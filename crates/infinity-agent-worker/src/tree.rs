//! `AgentTree`: one Durable Object per conversation tree.
//!
//! Requests never run the agent directly. They append to the inbox and set
//! the alarm; the alarm handler drains the inbox through
//! [`AgentSystem::step`]. Alarm handlers never overlap and are retried by
//! the platform, so they give the same per-thread serialization and
//! at-least-once delivery the Lambda deployment gets from SQS FIFO (request
//! handlers could not, because the DO keeps accepting requests while a step
//! awaits the model stream).

use std::rc::Rc;
use std::sync::Arc;

use infinity_agent_core::ThreadId;
use infinity_agent_core::message::InputMessage;
use infinity_agent_core::system::{
    AgentEvent, AgentSystem, AgentSystemBuilder, EventCollector, NoDeferral, StaticModel,
    ThreadConfig, ThreadConfigSource,
};
use infinity_agent_core::tools::Tool;
use infinity_agent_core::tools::rap_tool::RapTool;
use infinity_provider_protocol::ModelProvider;
use rap_client::notifier::RapNotifier;
use rap_client::toolset_loader::ToolsetLoader;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use worker::{
    DurableObject, Env, Method, Request, Response, Result, SqlStorageValue, State, Url,
    console_error, console_warn, durable_object,
};

use crate::anthropic::GatewayAnthropicProvider;
use crate::config::WorkerConfig;
use crate::db::{Db, root_of};
use crate::http::FetchHttpClient;
use crate::sender::TreeSender;
use crate::sleep::{SleepTool, SleepUntilTool};

/// Inbox rows per `step` call.
const BATCH_SIZE: i64 = 25;
/// `step` rounds per alarm before yielding to a fresh alarm invocation.
const MAX_ROUNDS: usize = 20;
/// Failed deliveries of one inbox row before it is dropped.
const MAX_ATTEMPTS: i64 = 5;
const DEDUP_WINDOW_MS: i64 = 24 * 60 * 60 * 1000;

type System = AgentSystem<Db, Db, TreeSender, FetchHttpClient>;

/// Body of the DO's internal `POST /enqueue`.
#[derive(Serialize, Deserialize)]
pub struct EnqueueRequest {
    pub message: InputMessage,
    /// Idempotency key: a second enqueue with the same key within 24 hours
    /// is acknowledged and dropped.
    pub dedup_key: Option<String>,
    /// Public origin of the Worker, used to build the RAP callback URL when
    /// `PUBLIC_URL` is not configured.
    pub origin: Option<String>,
}

fn now_ms() -> i64 {
    worker::Date::now().as_millis() as i64
}

fn s(value: impl Into<String>) -> SqlStorageValue {
    SqlStorageValue::from(value.into())
}

#[durable_object]
pub struct AgentTree {
    state: State,
    env: Env,
    db: Db,
}

impl DurableObject for AgentTree {
    fn new(state: State, env: Env) -> Self {
        let db = Db::new(state.storage().sql());
        if let Err(e) = db.migrate() {
            console_error!("schema migration failed: {e}");
        }
        Self { state, env, db }
    }

    async fn fetch(&self, mut req: Request) -> Result<Response> {
        let url = req.url()?;
        match (req.method(), url.path()) {
            (Method::Post, "/enqueue") => self.enqueue(req.json().await?).await,
            (Method::Get, "/events") => self.events(&url),
            _ => Response::error("not found", 404),
        }
    }

    async fn alarm(&self) -> Result<Response> {
        self.drain().await;
        Response::ok("drained")
    }
}

impl AgentTree {
    async fn enqueue(&self, request: EnqueueRequest) -> Result<Response> {
        let thread_id = request.message.group_id.clone();
        let root = root_of(thread_id.as_str()).to_owned();
        self.db.kv_set("root", &root)?;
        if let Some(origin) = &request.origin {
            self.db.kv_set("origin", origin)?;
        }

        if let Some(key) = &request.dedup_key {
            let now = now_ms();
            self.db.run(
                "DELETE FROM dedup WHERE at < ?",
                vec![SqlStorageValue::from(now - DEDUP_WINDOW_MS)],
            )?;
            let seen: Option<i64> = self.db.value(
                "SELECT 1 AS v FROM dedup WHERE key = ?",
                vec![s(key.as_str())],
            )?;
            if seen.is_some() {
                return Response::from_json(&json!({ "queued": false, "duplicate": true }));
            }
            self.db.run(
                "INSERT INTO dedup (key, at) VALUES (?, ?)",
                vec![s(key.as_str()), SqlStorageValue::from(now)],
            )?;
        }

        TreeSender::new(self.db.clone(), root).enqueue(&request.message)?;
        self.state.storage().set_alarm(0i64).await?;
        Response::from_json(&json!({ "queued": true, "thread_id": thread_id }))
    }

    fn events(&self, url: &Url) -> Result<Response> {
        let param = |name: &str| {
            url.query_pairs()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.into_owned())
        };
        let after: i64 = param("after").and_then(|v| v.parse().ok()).unwrap_or(0);
        let limit: i64 = param("limit")
            .and_then(|v| v.parse().ok())
            .unwrap_or(200)
            .clamp(1, 1000);
        let whole_tree = param("tree").is_some_and(|v| v == "1" || v == "true");
        let thread = param("thread").unwrap_or_default();

        #[derive(Deserialize)]
        struct Row {
            seq: i64,
            thread_id: String,
            at: i64,
            data: String,
        }
        let rows: Vec<Row> = if whole_tree {
            self.db.rows(
                "SELECT seq, thread_id, at, data FROM events WHERE seq > ? ORDER BY seq LIMIT ?",
                vec![SqlStorageValue::from(after), SqlStorageValue::from(limit)],
            )?
        } else {
            self.db.rows(
                "SELECT seq, thread_id, at, data FROM events
                 WHERE thread_id = ? AND seq > ? ORDER BY seq LIMIT ?",
                vec![
                    s(thread),
                    SqlStorageValue::from(after),
                    SqlStorageValue::from(limit),
                ],
            )?
        };
        let next = rows.last().map_or(after, |r| r.seq);
        let events: Vec<Value> = rows
            .into_iter()
            .map(|r| {
                json!({
                    "seq": r.seq,
                    "thread_id": r.thread_id,
                    "at": r.at,
                    "event": serde_json::from_str::<Value>(&r.data).unwrap_or(Value::Null),
                })
            })
            .collect();
        Response::from_json(&json!({ "events": events, "next": next }))
    }

    /// Move due timers into the inbox.
    fn release_timers(&self) -> Result<()> {
        let now = SqlStorageValue::from(now_ms());
        self.db.run(
            "INSERT INTO inbox (message_id, data)
             SELECT 'timer-' || seq, data FROM timers WHERE due_at <= ? ORDER BY due_at, seq",
            vec![now.clone()],
        )?;
        self.db
            .run("DELETE FROM timers WHERE due_at <= ?", vec![now])?;
        Ok(())
    }

    async fn drain(&self) {
        if let Err(e) = self.try_drain().await {
            console_error!("drain failed: {e}");
            // Platform alarm retries would re-run the whole handler; schedule
            // our own retry instead so the inbox is never stranded.
            if let Err(e) = self.state.storage().set_alarm(5_000i64).await {
                console_error!("failed to schedule retry: {e}");
            }
        }
    }

    async fn try_drain(&self) -> Result<()> {
        let Some(root) = self.db.kv_get("root")? else {
            return Ok(());
        };
        let config = WorkerConfig::from_env(&self.env)?;
        let mut system = self.build_system(&root, &config)?;

        #[derive(Deserialize)]
        struct InboxRow {
            seq: i64,
            message_id: String,
            data: String,
        }

        for _ in 0..MAX_ROUNDS {
            self.release_timers()?;
            let rows: Vec<InboxRow> = self.db.rows(
                "SELECT seq, message_id, data FROM inbox ORDER BY seq LIMIT ?",
                vec![SqlStorageValue::from(BATCH_SIZE)],
            )?;
            if rows.is_empty() {
                break;
            }

            let seqs: Vec<i64> = rows.iter().map(|r| r.seq).collect();
            let mut inputs = Vec::new();
            for row in rows {
                match serde_json::from_str::<InputMessage>(&row.data) {
                    Ok(message) => inputs.push((message, row.message_id)),
                    Err(e) => console_warn!("dropping malformed inbox row {}: {e}", row.seq),
                }
            }

            let collector = EventCollector::new();
            let result = system.step(inputs, &collector, &mut NoDeferral).await;
            self.record_events(collector.take())?;

            let placeholders = vec!["?"; seqs.len()].join(",");
            let seq_values: Vec<SqlStorageValue> =
                seqs.iter().map(|s| SqlStorageValue::from(*s)).collect();
            match result {
                Ok(_) => self.db.run(
                    &format!("DELETE FROM inbox WHERE seq IN ({placeholders})"),
                    seq_values,
                )?,
                Err(e) => {
                    console_error!("step failed: {e}");
                    self.db.run(
                        &format!(
                            "UPDATE inbox SET attempts = attempts + 1 WHERE seq IN ({placeholders})"
                        ),
                        seq_values,
                    )?;
                    self.drop_poisoned(&e.to_string())?;
                    self.state.storage().set_alarm(5_000i64).await?;
                    return Ok(());
                }
            }
        }

        self.schedule_next().await
    }

    /// Drop rows that keep failing, leaving an error event on their thread.
    fn drop_poisoned(&self, error: &str) -> Result<()> {
        #[derive(Deserialize)]
        struct Row {
            data: String,
        }
        let rows: Vec<Row> = self.db.rows(
            "SELECT data FROM inbox WHERE attempts >= ?",
            vec![SqlStorageValue::from(MAX_ATTEMPTS)],
        )?;
        for row in rows {
            if let Ok(message) = serde_json::from_str::<InputMessage>(&row.data) {
                self.insert_event(
                    message.group_id.as_str(),
                    &json!({ "type": "error", "text": format!("Input dropped after {MAX_ATTEMPTS} failed attempts: {error}") }),
                )?;
            }
        }
        self.db.run(
            "DELETE FROM inbox WHERE attempts >= ?",
            vec![SqlStorageValue::from(MAX_ATTEMPTS)],
        )?;
        Ok(())
    }

    /// Point the alarm at the next pending work: now if the inbox has rows,
    /// else the earliest timer.
    async fn schedule_next(&self) -> Result<()> {
        let pending: Option<i64> = self.db.value("SELECT 1 AS v FROM inbox LIMIT 1", vec![])?;
        if pending.is_some() {
            return self.state.storage().set_alarm(0i64).await;
        }
        let next_timer: Option<Option<i64>> = self
            .db
            .value("SELECT MIN(due_at) AS v FROM timers", vec![])?;
        if let Some(due) = next_timer.flatten() {
            self.state
                .storage()
                .set_alarm((due - now_ms()).max(0))
                .await?;
        }
        Ok(())
    }

    fn insert_event(&self, thread_id: &str, event: &Value) -> Result<()> {
        self.db.run(
            "INSERT INTO events (thread_id, at, data) VALUES (?, ?, ?)",
            vec![
                s(thread_id),
                SqlStorageValue::from(now_ms()),
                s(event.to_string()),
            ],
        )?;
        Ok(())
    }

    /// Persist a step's events, coalescing streamed text and reasoning
    /// chunks into one event per run.
    fn record_events(&self, events: Vec<(ThreadId, AgentEvent)>) -> Result<()> {
        let mut pending: Option<(ThreadId, &'static str, String)> = None;
        let flush = |pending: &mut Option<(ThreadId, &'static str, String)>| -> Result<()> {
            if let Some((thread, kind, text)) = pending.take() {
                self.insert_event(thread.as_str(), &json!({ "type": kind, "text": text }))?;
            }
            Ok(())
        };

        for (thread, event) in events {
            let chunk = match &event {
                AgentEvent::TextChunk { text } => Some(("text", text)),
                AgentEvent::ThinkingChunk { text } => Some(("thinking", text)),
                _ => None,
            };
            if let Some((kind, text)) = chunk {
                match &mut pending {
                    Some((t, k, buf)) if *t == thread && *k == kind => buf.push_str(text),
                    _ => {
                        flush(&mut pending)?;
                        pending = Some((thread, kind, text.clone()));
                    }
                }
                continue;
            }
            flush(&mut pending)?;
            if let Some(value) = event_json(&event) {
                self.insert_event(thread.as_str(), &value)?;
            }
        }
        flush(&mut pending)
    }

    fn build_system(&self, root: &str, config: &WorkerConfig) -> Result<System> {
        let provider = Arc::new(GatewayAnthropicProvider::new(
            self.env
                .ai(&config.ai_binding)
                .ok()
                .map(|ai| ai.as_ref().clone()),
            config.anthropic.clone(),
        ));
        let entry = provider.model_entry();
        let model = StaticModel::from_entry(provider as Arc<dyn ModelProvider>, &entry);

        let callback_base = config
            .public_url
            .clone()
            .or(self.db.kv_get("origin")?)
            .unwrap_or_default();
        let callback_url = match &config.rap_callback_secret {
            Some(secret) => format!(
                "{}/rap/callback/{secret}",
                callback_base.trim_end_matches('/')
            ),
            None => format!("{}/rap/callback", callback_base.trim_end_matches('/')),
        };

        let http = FetchHttpClient::new(config.rap_auth_token.clone());
        let thread_config = WorkerThreadConfig {
            rap_notifier: RapNotifier::new(config.toolset_server_urls.clone(), http.clone()),
            toolset_loader: ToolsetLoader::new(http.clone(), self.db.clone()),
            toolset_server_urls: config.toolset_server_urls.clone(),
            extra_system_prompt: config.system_prompt.clone(),
            http,
        };

        Ok(AgentSystemBuilder::new(
            self.db.clone(),
            self.db.clone(),
            model,
            TreeSender::new(self.db.clone(), root),
        )
        .thread_config(thread_config)
        .callback_url(callback_url)
        .build())
    }
}

/// The JSON form of an event in the tree's event log (streamed chunks are
/// coalesced by `record_events`).
fn event_json(event: &AgentEvent) -> Option<Value> {
    Some(match event {
        AgentEvent::UserInput { text } => json!({ "type": "user_input", "text": text }),
        AgentEvent::ToolResult { segments } => {
            json!({ "type": "tool_result", "segments": segments })
        }
        AgentEvent::SubscriptionEvent { name, text } => {
            json!({ "type": "subscription_event", "name": name, "text": text })
        }
        AgentEvent::OAuthRequired { auth_url } => {
            json!({ "type": "oauth_required", "auth_url": auth_url })
        }
        AgentEvent::CompactionApplied => json!({ "type": "compaction_applied" }),
        AgentEvent::ToolCall {
            name,
            args,
            display_as,
        } => {
            json!({ "type": "tool_call", "name": name, "args": args, "display_as": display_as })
        }
        AgentEvent::CompletionFinished { usage } => {
            json!({ "type": "completion_finished", "usage": usage })
        }
        AgentEvent::UserChoiceRequired { choice } => {
            json!({ "type": "user_choice_required", "choice": choice })
        }
        AgentEvent::UserChoiceDismissed { choice_id } => {
            json!({ "type": "user_choice_dismissed", "choice_id": choice_id })
        }
        AgentEvent::Info { text } => json!({ "type": "info", "text": text }),
        AgentEvent::TextChunk { .. }
        | AgentEvent::ThinkingChunk { .. }
        | AgentEvent::ThinkingStarted
        | AgentEvent::ThinkingEnded
        | AgentEvent::CompletionStarted => return None,
    })
}

/// Per-thread configuration: RAP toolsets (manifests cached in the tree's
/// database) plus the alarm-backed sleep tools. The remaining built-in tools
/// come from the system builder.
struct WorkerThreadConfig {
    toolset_server_urls: Vec<String>,
    toolset_loader: ToolsetLoader<FetchHttpClient, Db>,
    http: FetchHttpClient,
    rap_notifier: RapNotifier<FetchHttpClient>,
    extra_system_prompt: Option<String>,
}

#[async_trait::async_trait(?Send)]
impl ThreadConfigSource<TreeSender, FetchHttpClient> for WorkerThreadConfig {
    async fn resolve(
        &self,
        thread_id: &ThreadId<str>,
    ) -> std::result::Result<
        ThreadConfig<TreeSender, FetchHttpClient>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let mut tools: Vec<Rc<dyn Tool<TreeSender>>> = Vec::new();
        if !self.toolset_server_urls.is_empty() {
            match self
                .toolset_loader
                .load_toolsets(&self.toolset_server_urls, thread_id)
                .await
            {
                Ok(loaded) => {
                    for toolset in loaded {
                        let endpoint = toolset.manifest.endpoint.clone();
                        for def in toolset.manifest.tools {
                            tools.push(Rc::new(RapTool {
                                descriptor: def.into(),
                                endpoint: endpoint.clone(),
                                http_client: self.http.clone(),
                                callback_url: None,
                            }));
                        }
                    }
                }
                Err(e) => console_warn!("failed to load RAP toolsets: {e}"),
            }
        }
        tools.push(Rc::new(SleepTool));
        tools.push(Rc::new(SleepUntilTool));

        Ok(ThreadConfig {
            tools,
            extra_system_prompt: self.extra_system_prompt.clone(),
            rap_notifier: Some(self.rap_notifier.clone()),
        })
    }
}
