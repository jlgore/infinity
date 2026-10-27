//! Claude via Cloudflare AI Gateway, called through the Workers `AI` binding.
//!
//! Requests go to `env.AI.gateway(id).run({ provider: "anthropic", endpoint:
//! "v1/messages", ... })`, which forwards the native Messages API body
//! unchanged (thinking signatures, tool use, SSE streaming). The binding
//! authenticates the call, and the gateway supplies the Anthropic credential
//! from its stored key (BYOK) or Unified Billing, so the Worker holds no
//! provider secret.

use std::collections::HashMap;

use async_trait::async_trait;
use futures_util::StreamExt;
use infinity_provider_protocol::message::{
    AssistantContent, Image, ImageSource, Message, Reasoning, ReasoningContent, ToolCall,
    ToolResultContent, UserContent,
};
use infinity_provider_protocol::{
    CompletionError, CompletionRequest, ErrorClass, FinalResponse, ModelEntry, ModelProvider,
    ModelStream, StreamChunk, ToolCallDeltaContent, Usage,
};
use serde_json::{Value, json};
use worker::send::SendFuture;
use worker::wasm_bindgen::{JsCast, JsValue};
use worker::wasm_bindgen_futures::JsFuture;
use worker::{js_sys, web_sys};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

const ANTHROPIC_VERSION: &str = "2026-10-21";

/// Static configuration for [`GatewayAnthropicProvider`], read from Worker
/// vars (see `lib.rs`).
#[derive(Clone, Debug)]
pub struct AnthropicConfig {
    /// AI Gateway ID (the gateway's name in the dashboard).
    pub gateway_id: String,
    pub model: String,
    pub max_tokens: u64,
    pub context_window: usize,
    /// `output_config.effort`; `None` uses the model's default.
    pub effort: Option<String>,
    /// `thinking.display` for adaptive thinking.
    pub thinking_display: String,
    /// Opt into server-side refusal fallbacks (`fallbacks: "default"`).
    pub refusal_fallbacks: bool,
    /// Extra `anthropic-beta` values.
    pub betas: Vec<String>,
}

pub struct GatewayAnthropicProvider {
    /// `None` when the binding is missing; every request then fails with a
    /// configuration error instead of the whole runtime failing to start.
    ai: Option<send_wrapper::SendWrapper<JsValue>>,
    config: AnthropicConfig,
}

impl GatewayAnthropicProvider {
    /// `ai` is the Workers AI binding object (`env.AI`).
    pub fn new(ai: Option<JsValue>, config: AnthropicConfig) -> Self {
        Self {
            ai: ai.map(send_wrapper::SendWrapper::new),
            config,
        }
    }

    pub fn model_entry(&self) -> ModelEntry {
        ModelEntry {
            model_id: self.config.model.clone(),
            display_name: self.config.model.clone(),
            context_window: self.config.context_window,
            max_output_tokens: Some(self.config.max_tokens),
            supports_image_input: true,
        }
    }

    fn request_body(
        &self,
        model_id: &str,
        request: &CompletionRequest,
        messages: Vec<Value>,
    ) -> Value {
        let mut body = json!({
            "model": model_id,
            "max_tokens": request.max_tokens.unwrap_or(self.config.max_tokens),
            "stream": true,
            "messages": messages,
            "thinking": { "type": "adaptive", "display": self.config.thinking_display },
            // Automatic prompt caching: the breakpoint follows the end of the
            // conversation as it grows.
            "cache_control": { "type": "ephemeral" },
        });
        if let Some(preamble) = request.preamble.as_deref().filter(|p| !p.is_empty()) {
            body["system"] = json!(preamble);
        }
        if !request.tools.is_empty() {
            body["tools"] = request
                .tools
                .iter()
                .map(|tool| {
                    json!({
                        "name": tool.name,
                        "description": tool.description,
                        "input_schema": tool.parameters,
                        "eager_input_streaming": true,
                    })
                })
                .collect();
        }
        if let Some(effort) = &self.config.effort {
            body["output_config"] = json!({ "effort": effort });
        }
        if self.config.refusal_fallbacks {
            body["fallbacks"] = json!("default");
        }
        if let (Some(Value::Object(extra)), Value::Object(target)) =
            (&request.additional_params, &mut body)
        {
            for (key, value) in extra {
                target.insert(key.clone(), value.clone());
            }
        }
        body
    }

    fn headers(&self) -> Value {
        let mut betas = self.config.betas.clone();
        if self.config.refusal_fallbacks {
            betas.push("server-side-fallback-2026-07-01".to_owned());
        }
        let mut headers = json!({
            "content-type": "application/json",
            "anthropic-version": ANTHROPIC_VERSION,
        });
        if !betas.is_empty() {
            headers["anthropic-beta"] = json!(betas.join(","));
        }
        headers
    }
}

/// An unsuccessful HTTP response from the gateway or Anthropic.
struct ApiError {
    status: u16,
    error_type: String,
    message: String,
}

impl ApiError {
    fn from_body(status: u16, body: &str) -> Self {
        let parsed: Option<Value> = serde_json::from_str(body).ok();
        let error = parsed.as_ref().and_then(|v| v.get("error"));
        Self {
            status,
            error_type: error
                .and_then(|e| e.get("type"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            message: error
                .and_then(|e| e.get("message"))
                .and_then(Value::as_str)
                .map_or_else(|| body.to_owned(), str::to_owned),
        }
    }

    /// A rejection of replayed thinking blocks (e.g. preserved thinking's
    /// history-editing check after compaction rewrote history).
    fn is_thinking_rejection(&self) -> bool {
        let msg = self.message.to_lowercase();
        self.status == 400
            && msg.contains("thinking")
            && ["signature", "modified", "edited", "invalid", "cannot"]
                .iter()
                .any(|w| msg.contains(w))
    }

    fn class(&self) -> ErrorClass {
        classify(self.status, &self.error_type, &self.message)
    }

    fn into_completion_error(self) -> CompletionError {
        CompletionError::provider(
            self.class(),
            format!(
                "Anthropic API error {} ({}): {}",
                self.status, self.error_type, self.message
            ),
        )
    }
}

fn classify(status: u16, error_type: &str, message: &str) -> ErrorClass {
    let msg = message.to_lowercase();
    match (status, error_type) {
        (429, _) | (_, "rate_limit_error") => ErrorClass::Throttled,
        (413, _) | (_, "request_too_large") => ErrorClass::ContextOverflow,
        (_, "overloaded_error" | "api_error") | (408 | 500 | 502 | 503 | 504 | 529, _) => {
            ErrorClass::Transient
        }
        (400, _)
            if msg.contains("prompt is too long")
                || msg.contains("too many tokens")
                || (msg.contains("context")
                    && (msg.contains("exceed") || msg.contains("window"))) =>
        {
            ErrorClass::ContextOverflow
        }
        _ => ErrorClass::Fatal,
    }
}

fn js_error(e: JsValue) -> CompletionError {
    CompletionError::provider(
        ErrorClass::Transient,
        format!(
            "AI Gateway binding error: {}",
            js_sys::JSON::stringify(&e)
                .map(String::from)
                .unwrap_or_default()
        ),
    )
}

/// Call `ai.gateway(gateway_id).run(...)` and return the raw response.
async fn gateway_run(
    ai: &JsValue,
    gateway_id: &str,
    headers: &Value,
    body: &Value,
) -> Result<web_sys::Response, CompletionError> {
    let gateway_fn: js_sys::Function = js_sys::Reflect::get(ai, &"gateway".into())
        .map_err(js_error)?
        .dyn_into()
        .map_err(js_error)?;
    let gateway = gateway_fn
        .call1(ai, &JsValue::from_str(gateway_id))
        .map_err(js_error)?;
    let run_fn: js_sys::Function = js_sys::Reflect::get(&gateway, &"run".into())
        .map_err(js_error)?
        .dyn_into()
        .map_err(js_error)?;
    let request = json!({
        "provider": "anthropic",
        "endpoint": "v1/messages",
        "headers": headers,
        "query": body,
    });
    let request = js_sys::JSON::parse(&request.to_string()).map_err(js_error)?;
    let promise: js_sys::Promise = run_fn
        .call1(&gateway, &request)
        .map_err(js_error)?
        .dyn_into()
        .map_err(js_error)?;
    JsFuture::from(promise)
        .await
        .map_err(js_error)?
        .dyn_into()
        .map_err(js_error)
}

async fn error_from_response(response: &web_sys::Response) -> ApiError {
    let body = match response.text() {
        Ok(promise) => JsFuture::from(promise)
            .await
            .ok()
            .and_then(|v| v.as_string())
            .unwrap_or_default(),
        Err(_) => String::new(),
    };
    ApiError::from_body(response.status(), &body)
}

#[async_trait]
impl ModelProvider for GatewayAnthropicProvider {
    async fn list_models(&self) -> Result<Vec<ModelEntry>, BoxError> {
        Ok(vec![self.model_entry()])
    }

    async fn invoke_model(
        &self,
        model_id: &str,
        request: CompletionRequest,
    ) -> Result<ModelStream, CompletionError> {
        let messages = convert_messages(&request.chat_history, true)?;
        let body = self.request_body(model_id, &request, messages);
        let headers = self.headers();
        let ai = self.ai.as_deref().cloned().ok_or_else(|| {
            CompletionError::provider(ErrorClass::Fatal, "the AI binding is not configured")
        })?;
        let gateway_id = self.config.gateway_id.clone();

        // JS futures are `!Send`; a Workers isolate is single-threaded.
        let response = SendFuture::new(async move {
            let response = gateway_run(&ai, &gateway_id, &headers, &body).await?;
            if response.ok() {
                return Ok(response);
            }
            let error = error_from_response(&response).await;
            if !error.is_thinking_rejection() {
                return Err(error.into_completion_error());
            }
            // Recover once without the replayed reasoning; the model answers
            // this turn without those blocks.
            tracing::warn!(
                "thinking blocks rejected ({}); retrying without them",
                error.message
            );
            let mut body = body;
            body["messages"] = Value::Array(convert_messages(&request.chat_history, false)?);
            let response = gateway_run(&ai, &gateway_id, &headers, &body).await?;
            if response.ok() {
                Ok(response)
            } else {
                Err(error_from_response(&response).await.into_completion_error())
            }
        })
        .await?;

        let body = response
            .body()
            .ok_or_else(|| CompletionError::ResponseError("response has no body".to_owned()))?;
        let bytes = wasm_streams::ReadableStream::from_raw(body.unchecked_into())
            .into_stream()
            .map(|chunk| {
                chunk
                    .map(|v| js_sys::Uint8Array::new(&v).to_vec())
                    .map_err(|e| format!("{e:?}"))
            });
        let stream = Box::pin(sse_stream(bytes));
        Ok(Box::pin(send_wrapper::SendWrapper::new(stream)))
    }
}

// ── Request conversion ──

fn image_block(image: &Image) -> Result<Value, CompletionError> {
    match &image.data {
        ImageSource::Base64(data) => {
            let media_type = image.media_type.ok_or_else(|| {
                CompletionError::provider(
                    ErrorClass::Fatal,
                    "base64 image content requires a media type",
                )
            })?;
            Ok(json!({
                "type": "image",
                "source": { "type": "base64", "media_type": media_type.to_mime_type(), "data": data },
            }))
        }
        ImageSource::Url(url) => Ok(json!({
            "type": "image",
            "source": { "type": "url", "url": url },
        })),
    }
}

fn user_block(content: &UserContent) -> Result<Option<Value>, CompletionError> {
    Ok(match content {
        UserContent::Text(text) if text.text.is_empty() => None,
        UserContent::Text(text) => Some(json!({ "type": "text", "text": text.text })),
        UserContent::Image(image) => Some(image_block(image)?),
        UserContent::ToolResult(result) => {
            let mut blocks = Vec::new();
            for part in &result.content {
                match part {
                    ToolResultContent::Text(text) if text.text.is_empty() => {}
                    ToolResultContent::Text(text) => {
                        blocks.push(json!({ "type": "text", "text": text.text }));
                    }
                    ToolResultContent::Image(image) => blocks.push(image_block(image)?),
                }
            }
            let mut block = json!({ "type": "tool_result", "tool_use_id": result.id });
            if !blocks.is_empty() {
                block["content"] = Value::Array(blocks);
            }
            Some(block)
        }
    })
}

fn reasoning_block(reasoning: &Reasoning) -> Option<Value> {
    if let Some(data) = reasoning.content.iter().find_map(|c| match c {
        ReasoningContent::Redacted { data } => Some(data),
        _ => None,
    }) {
        return Some(json!({ "type": "redacted_thinking", "data": data }));
    }
    // Unsigned reasoning (e.g. from another provider) cannot be replayed.
    let signature = reasoning.first_signature()?;
    Some(json!({
        "type": "thinking",
        "thinking": reasoning.first_text().unwrap_or_default(),
        "signature": signature,
    }))
}

fn assistant_block(content: &AssistantContent, include_thinking: bool) -> Option<Value> {
    match content {
        AssistantContent::Text(text) if text.text.is_empty() => None,
        AssistantContent::Text(text) => Some(json!({ "type": "text", "text": text.text })),
        AssistantContent::ToolCall(call) => {
            let input = match &call.function.arguments {
                Value::Object(_) => call.function.arguments.clone(),
                _ => json!({}),
            };
            Some(json!({
                "type": "tool_use",
                "id": call.id,
                "name": call.function.name,
                "input": input,
            }))
        }
        AssistantContent::Reasoning(reasoning) if include_thinking => reasoning_block(reasoning),
        AssistantContent::Reasoning(_) => None,
    }
}

/// Convert history to Messages API turns: drop empty blocks, merge
/// consecutive same-role turns (roles must alternate), put tool results
/// first in each user turn, and start with a user turn.
fn convert_messages(
    history: &[Message],
    include_thinking: bool,
) -> Result<Vec<Value>, CompletionError> {
    let mut turns: Vec<(&'static str, Vec<Value>)> = Vec::new();
    for message in history {
        let (role, blocks) = match message {
            Message::User { content } => (
                "user",
                content
                    .iter()
                    .map(user_block)
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>(),
            ),
            Message::Assistant { content } => (
                "assistant",
                content
                    .iter()
                    .filter_map(|c| assistant_block(c, include_thinking))
                    .collect(),
            ),
        };
        if blocks.is_empty() {
            continue;
        }
        match turns.last_mut() {
            Some((last_role, last_blocks)) if *last_role == role => last_blocks.extend(blocks),
            _ => turns.push((role, blocks)),
        }
    }
    if turns.first().is_some_and(|(role, _)| *role == "assistant") {
        // e.g. a history that starts with a compaction summary.
        turns.insert(
            0,
            (
                "user",
                vec![json!({ "type": "text", "text": "(conversation continues)" })],
            ),
        );
    }
    Ok(turns
        .into_iter()
        .map(|(role, mut blocks)| {
            if role == "user" {
                // Stable sort: tool results first, original order otherwise.
                blocks.sort_by_key(|b| b["type"] != "tool_result");
            }
            json!({ "role": role, "content": blocks })
        })
        .collect())
}

// ── Response streaming ──

enum Block {
    Text,
    Thinking {
        text: String,
        signature: Option<String>,
    },
    Redacted {
        data: String,
    },
    ToolUse {
        id: String,
        name: String,
        json: String,
    },
    Other,
}

#[derive(Default)]
struct StreamState {
    blocks: HashMap<u64, Block>,
    input_tokens: u64,
    cache_read: u64,
    cache_write: u64,
    output_tokens: u64,
    finished: bool,
}

impl StreamState {
    fn usage(&self) -> Usage {
        let input = self.input_tokens + self.cache_read + self.cache_write;
        Usage {
            input_tokens: input,
            output_tokens: self.output_tokens,
            total_tokens: input + self.output_tokens,
            cached_input_tokens: self.cache_read,
        }
    }

    fn record_usage(&mut self, usage: &Value) {
        let get = |key: &str| usage.get(key).and_then(Value::as_u64);
        if let Some(n) = get("input_tokens") {
            self.input_tokens = n;
        }
        if let Some(n) = get("cache_read_input_tokens") {
            self.cache_read = n;
        }
        if let Some(n) = get("cache_creation_input_tokens") {
            self.cache_write = n;
        }
        if let Some(n) = get("output_tokens") {
            self.output_tokens = n;
        }
    }

    /// Handle one SSE `data:` payload.
    fn handle(&mut self, event: &Value) -> Vec<Result<StreamChunk, CompletionError>> {
        let mut out = Vec::new();
        let index = event["index"].as_u64().unwrap_or(0);
        match event["type"].as_str().unwrap_or_default() {
            "message_start" => self.record_usage(&event["message"]["usage"]),
            "content_block_start" => {
                let block = &event["content_block"];
                let str_field = |key: &str| block[key].as_str().unwrap_or_default().to_owned();
                let state = match block["type"].as_str().unwrap_or_default() {
                    "text" => {
                        let text = str_field("text");
                        if !text.is_empty() {
                            out.push(Ok(StreamChunk::Text(text)));
                        }
                        Block::Text
                    }
                    "thinking" => Block::Thinking {
                        text: str_field("thinking"),
                        signature: block["signature"]
                            .as_str()
                            .filter(|s| !s.is_empty())
                            .map(str::to_owned),
                    },
                    "redacted_thinking" => Block::Redacted {
                        data: str_field("data"),
                    },
                    "tool_use" => {
                        let (id, name) = (str_field("id"), str_field("name"));
                        out.push(Ok(StreamChunk::ToolCallDelta {
                            id: id.clone(),
                            content: ToolCallDeltaContent::Name(name.clone()),
                        }));
                        Block::ToolUse {
                            id,
                            name,
                            json: String::new(),
                        }
                    }
                    other => {
                        tracing::debug!("ignoring content block of type {other}");
                        Block::Other
                    }
                };
                self.blocks.insert(index, state);
            }
            "content_block_delta" => {
                let delta = &event["delta"];
                let text = |key: &str| delta[key].as_str().unwrap_or_default();
                match (
                    delta["type"].as_str().unwrap_or_default(),
                    self.blocks.get_mut(&index),
                ) {
                    ("text_delta", _) if !text("text").is_empty() => {
                        out.push(Ok(StreamChunk::Text(text("text").to_owned())));
                    }
                    ("thinking_delta", Some(Block::Thinking { text: buf, .. })) => {
                        let piece = text("thinking");
                        buf.push_str(piece);
                        if !piece.is_empty() {
                            out.push(Ok(StreamChunk::ReasoningDelta {
                                id: None,
                                text: piece.to_owned(),
                            }));
                        }
                    }
                    ("signature_delta", Some(Block::Thinking { signature, .. })) => {
                        signature
                            .get_or_insert_with(String::new)
                            .push_str(text("signature"));
                    }
                    ("input_json_delta", Some(Block::ToolUse { id, json, .. })) => {
                        let piece = text("partial_json");
                        json.push_str(piece);
                        out.push(Ok(StreamChunk::ToolCallDelta {
                            id: id.clone(),
                            content: ToolCallDeltaContent::Delta(piece.to_owned()),
                        }));
                    }
                    _ => {}
                }
            }
            "content_block_stop" => match self.blocks.remove(&index) {
                Some(Block::Thinking { text, signature })
                    if signature.is_some() || !text.is_empty() =>
                {
                    out.push(Ok(StreamChunk::Reasoning(Reasoning::new_with_signature(
                        text, signature,
                    ))));
                }
                Some(Block::Redacted { data }) => out.push(Ok(StreamChunk::Reasoning(Reasoning {
                    id: None,
                    content: vec![ReasoningContent::Redacted { data }],
                }))),
                Some(Block::ToolUse { id, name, json }) => {
                    // Eager input streaming skips server-side validation, so
                    // parse strictly; a retry regenerates a malformed call.
                    let parsed = if json.trim().is_empty() {
                        Ok(json!({}))
                    } else {
                        serde_json::from_str::<Value>(&json)
                    };
                    out.push(match parsed {
                        Ok(args @ Value::Object(_)) => {
                            Ok(StreamChunk::ToolCall(ToolCall::new(id, name, args)))
                        }
                        _ => Err(CompletionError::provider(
                            ErrorClass::Transient,
                            format!("tool call {name} has malformed JSON input"),
                        )),
                    });
                }
                _ => {}
            },
            "message_delta" => {
                self.record_usage(&event["usage"]);
                if event["delta"]["stop_reason"] == "refusal" {
                    let category = event["delta"]["stop_details"]["category"]
                        .as_str()
                        .map(|c| format!(" (category: {c})"))
                        .unwrap_or_default();
                    out.push(Ok(StreamChunk::Text(format!(
                        "\n\n[The model declined to continue this response{category}.]"
                    ))));
                }
            }
            "message_stop" => {
                self.finished = true;
                out.push(Ok(StreamChunk::Final(FinalResponse {
                    usage: Some(self.usage()),
                })));
            }
            "error" => {
                let error = &event["error"];
                let error_type = error["type"].as_str().unwrap_or_default();
                let message = error["message"].as_str().unwrap_or_default();
                out.push(Err(CompletionError::provider(
                    classify(0, error_type, message),
                    format!("Anthropic stream error ({error_type}): {message}"),
                )));
            }
            _ => {} // ping
        }
        out
    }
}

/// Parse an SSE byte stream from the Messages API into stream chunks.
fn sse_stream(
    mut bytes: impl futures_util::Stream<Item = Result<Vec<u8>, String>> + Unpin,
) -> impl futures_util::Stream<Item = Result<StreamChunk, CompletionError>> {
    async_stream::stream! {
        let mut buffer: Vec<u8> = Vec::new();
        let mut state = StreamState::default();
        while let Some(chunk) = bytes.next().await {
            match chunk {
                // CRs only appear in SSE line endings (JSON escapes them).
                Ok(chunk) => buffer.extend(chunk.into_iter().filter(|b| *b != b'\r')),
                Err(e) => {
                    yield Err(CompletionError::ResponseError(e));
                    return;
                }
            }
            while let Some(end) = buffer.windows(2).position(|w| w == b"\n\n") {
                let raw: Vec<u8> = buffer.drain(..end + 2).collect();
                let raw = String::from_utf8_lossy(&raw);
                let data: String = raw
                    .lines()
                    .filter_map(|line| line.strip_prefix("data:"))
                    .map(str::trim_start)
                    .collect::<Vec<_>>()
                    .join("\n");
                if data.is_empty() {
                    continue;
                }
                let event: Value = match serde_json::from_str(&data) {
                    Ok(v) => v,
                    Err(e) => {
                        yield Err(CompletionError::ResponseError(format!("invalid SSE payload: {e}")));
                        return;
                    }
                };
                for item in state.handle(&event) {
                    let is_err = item.is_err();
                    yield item;
                    if is_err {
                        return;
                    }
                }
                if state.finished {
                    return;
                }
            }
        }
        if !state.finished {
            yield Err(CompletionError::ResponseError("stream ended before message_stop".to_owned()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use infinity_provider_protocol::message::{Text, ToolResult};

    #[test]
    fn merges_turns_and_orders_tool_results_first() {
        let history = vec![
            Message::assistant("summary"),
            Message::User {
                content: vec![UserContent::text("hi")],
            },
            Message::User {
                content: vec![UserContent::ToolResult(ToolResult {
                    id: "t1".into(),
                    call_id: None,
                    content: vec![ToolResultContent::Text(Text { text: "ok".into() })],
                })],
            },
        ];
        let turns = convert_messages(&history, true).expect("convert");
        assert_eq!(turns.len(), 3);
        assert_eq!(turns[0]["role"], "user");
        assert_eq!(turns[1]["role"], "assistant");
        assert_eq!(turns[2]["content"][0]["type"], "tool_result");
        assert_eq!(turns[2]["content"][1]["text"], "hi");
    }

    #[test]
    fn streams_text_thinking_and_tool_calls() {
        let mut state = StreamState::default();
        let events = [
            json!({"type":"message_start","message":{"usage":{"input_tokens":10,"cache_read_input_tokens":5,"output_tokens":1}}}),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"hmm"}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"sig"}}),
            json!({"type":"content_block_stop","index":0}),
            json!({"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"toolu_1","name":"sleep","input":{}}}),
            json!({"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"seconds\":"}}),
            json!({"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"5}"}}),
            json!({"type":"content_block_stop","index":1}),
            json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":20}}),
            json!({"type":"message_stop"}),
        ];
        let chunks: Vec<StreamChunk> = events
            .iter()
            .flat_map(|e| state.handle(e))
            .map(|c| c.expect("chunk"))
            .collect();
        assert!(
            chunks.iter().any(
                |c| matches!(c, StreamChunk::Reasoning(r) if r.first_signature() == Some("sig"))
            )
        );
        assert!(chunks.iter().any(|c| matches!(c, StreamChunk::ToolCall(t) if t.function.arguments == json!({"seconds": 5}))));
        let Some(StreamChunk::Final(FinalResponse { usage: Some(usage) })) = chunks.last() else {
            panic!("missing final chunk");
        };
        assert_eq!(usage.input_tokens, 15);
        assert_eq!(usage.cached_input_tokens, 5);
        assert_eq!(usage.output_tokens, 20);
    }

    #[test]
    fn classifies_errors() {
        assert_eq!(classify(429, "rate_limit_error", ""), ErrorClass::Throttled);
        assert_eq!(classify(529, "overloaded_error", ""), ErrorClass::Transient);
        assert_eq!(
            classify(
                400,
                "invalid_request_error",
                "prompt is too long: 1200000 tokens > 1000000 maximum"
            ),
            ErrorClass::ContextOverflow
        );
        assert_eq!(
            classify(400, "invalid_request_error", "bad"),
            ErrorClass::Fatal
        );
    }
}
