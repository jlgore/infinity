//! Infinity agent runtime on Cloudflare Workers.
//!
//! * `POST /threads/:id/messages` — send user text to a thread.
//! * `GET  /threads/:id/events?after=N[&tree=1]` — poll a thread's events.
//! * `POST /rap/callback[/:secret]` — RAP tool servers deliver results here.
//!
//! Each conversation tree (a root thread and every thread it spawns) is one
//! [`AgentTree`](tree::AgentTree) Durable Object, addressed by the root
//! thread ID. Claude is reached through AI Gateway via the `AI` binding.

mod anthropic;
mod config;
mod db;
mod http;
mod sender;
mod sleep;
mod tree;

use infinity_agent_core::message::InputMessage;
use infinity_rap_bridge::{CallbackDelivery, prepare_callback};
use rap_protocol::RapCallback;
use serde::Deserialize;
use serde_json::json;
use worker::{
    Context, Env, Headers, Method, Request, RequestInit, Response, Result, RouteContext, Router,
    event,
};

use crate::db::{ROOT_SEPARATOR, root_of};
use crate::tree::EnqueueRequest;

pub use tree::AgentTree;

const TREE_BINDING: &str = "AGENT_TREE";

#[event(fetch)]
async fn fetch(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    Router::new()
        .get("/", |_, _| Response::ok("infinity agent worker"))
        .post_async("/threads/:id/messages", post_message)
        .get_async("/threads/:id/events", get_events)
        .post_async("/rap/callback", rap_callback)
        .post_async("/rap/callback/:secret", rap_callback)
        .run(req, env)
        .await
}

/// Forward a request to the tree that owns `thread_id`.
async fn to_tree(
    env: &Env,
    thread_id: &str,
    method: Method,
    path_and_query: &str,
    body: Option<String>,
) -> Result<Response> {
    let stub = env
        .durable_object(TREE_BINDING)?
        .id_from_name(root_of(thread_id))?
        .get_stub()?;
    let mut init = RequestInit::new();
    init.with_method(method);
    if let Some(body) = body {
        let headers = Headers::new();
        headers.set("content-type", "application/json")?;
        init.with_headers(headers).with_body(Some(body.into()));
    }
    stub.fetch_with_request(Request::new_with_init(
        &format!("https://agent-tree{path_and_query}"),
        &init,
    )?)
    .await
}

/// Client routes require `Authorization: Bearer $API_TOKEN` unless the
/// deployment explicitly sets `ALLOW_UNAUTHENTICATED=true`.
fn authorize(req: &Request, ctx: &RouteContext<()>) -> Result<Option<Response>> {
    match config::get(&ctx.env, "API_TOKEN") {
        Some(token) => {
            let header = req.headers().get("authorization")?.unwrap_or_default();
            if header == format!("Bearer {token}") {
                Ok(None)
            } else {
                Response::error("unauthorized", 401).map(Some)
            }
        }
        None if config::get(&ctx.env, "ALLOW_UNAUTHENTICATED").as_deref() == Some("true") => {
            Ok(None)
        }
        None => Response::error("API_TOKEN is not configured", 500).map(Some),
    }
}

fn valid_thread_id(id: &str) -> bool {
    let root = root_of(id);
    !root.is_empty()
        && id.len() <= 200
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "-_.:".contains(c) || c == ROOT_SEPARATOR)
}

#[derive(Deserialize)]
struct PostMessage {
    text: String,
    /// Optional idempotency key for client retries.
    message_id: Option<String>,
    metadata: Option<serde_json::Value>,
}

async fn post_message(mut req: Request, ctx: RouteContext<()>) -> Result<Response> {
    if let Some(denied) = authorize(&req, &ctx)? {
        return Ok(denied);
    }
    let thread_id = ctx.param("id").cloned().unwrap_or_default();
    if !valid_thread_id(&thread_id) {
        return Response::error("invalid thread id", 400);
    }
    let body: PostMessage = match req.json().await {
        Ok(body) => body,
        Err(e) => return Response::error(format!("invalid body: {e}"), 400),
    };
    if body.text.trim().is_empty() {
        return Response::error("text must not be empty", 400);
    }

    let mut message = InputMessage::user_text(thread_id.as_str(), body.text);
    message.metadata = body.metadata;
    let request = EnqueueRequest {
        message,
        dedup_key: body.message_id.map(|id| format!("user-{id}")),
        origin: Some(req.url()?.origin().ascii_serialization()),
    };
    to_tree(
        &ctx.env,
        &thread_id,
        Method::Post,
        "/enqueue",
        Some(serde_json::to_string(&request)?),
    )
    .await
}

async fn get_events(req: Request, ctx: RouteContext<()>) -> Result<Response> {
    if let Some(denied) = authorize(&req, &ctx)? {
        return Ok(denied);
    }
    let thread_id = ctx.param("id").cloned().unwrap_or_default();
    if !valid_thread_id(&thread_id) {
        return Response::error("invalid thread id", 400);
    }
    let url = req.url()?;
    let mut query = url::form_urlencoded::Serializer::new(String::new());
    query.append_pair("thread", &thread_id);
    for (key, value) in url.query_pairs() {
        if matches!(key.as_ref(), "after" | "limit" | "tree") {
            query.append_pair(&key, &value);
        }
    }
    to_tree(
        &ctx.env,
        &thread_id,
        Method::Get,
        &format!("/events?{}", query.finish()),
        None,
    )
    .await
}

/// RAP callbacks (tool results, subscription events, OAuth challenges, user
/// choices). When `RAP_CALLBACK_SECRET` is set, only `/rap/callback/<secret>`
/// is accepted.
async fn rap_callback(mut req: Request, ctx: RouteContext<()>) -> Result<Response> {
    let expected = config::get(&ctx.env, "RAP_CALLBACK_SECRET");
    if expected.as_ref() != ctx.param("secret") {
        return Response::error("not found", 404);
    }

    let callback: RapCallback = match req.json().await {
        Ok(callback) => callback,
        Err(e) => {
            return Response::from_json(&json!({ "error": format!("invalid RAP callback: {e}") }))
                .map(|r| r.with_status(400));
        }
    };
    let CallbackDelivery::Input { message, dedup_id } = prepare_callback(callback) else {
        // View updates are display-only; there is no live client to update.
        return Response::from_json(&json!({ "ok": true }));
    };
    let thread_id = message.group_id.as_str().to_owned();
    if !valid_thread_id(&thread_id) {
        return Response::from_json(&json!({ "error": "invalid group_id" }))
            .map(|r| r.with_status(400));
    }
    let request = EnqueueRequest {
        message: *message,
        dedup_key: Some(dedup_id),
        origin: None,
    };
    let response = to_tree(
        &ctx.env,
        &thread_id,
        Method::Post,
        "/enqueue",
        Some(serde_json::to_string(&request)?),
    )
    .await?;
    if response.status_code() >= 400 {
        return Response::from_json(&json!({ "error": "Internal error" }))
            .map(|r| r.with_status(500));
    }
    Response::from_json(&json!({ "ok": true }))
}
