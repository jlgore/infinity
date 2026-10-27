//! Durable Object SQLite storage for one conversation tree.
//!
//! Every thread of a tree (the root and all of its descendants) lives in the
//! same Durable Object, so this one database backs the conversation store,
//! the state store, the toolset cache, and the runtime's own inbox, timers,
//! and event log. The DO SQL API is synchronous, so none of these methods
//! hold JS futures and all of them satisfy the core traits' `Send` bounds.

use std::collections::HashSet;

use async_trait::async_trait;
use infinity_agent_core::ThreadId;
use infinity_agent_core::message::InfinityMessage;
use infinity_agent_core::system::UserChoice;
use infinity_agent_core::traits::{ConversationStore, SpawnContext, StateStore};
use infinity_provider_protocol::message::Message;
use rap_client::http::ToolsetCache;
use rap_protocol::{ChoiceId, ToolCallId};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use worker::{SqlStorage, SqlStorageValue};

/// Separates a root thread ID from a child's suffix. Child thread IDs are
/// `"{root}~{uuid}"`, so any thread ID names the Durable Object that owns it.
pub const ROOT_SEPARATOR: char = '~';

/// The root thread (and so the Durable Object name) that owns `thread_id`.
pub fn root_of(thread_id: &str) -> &str {
    thread_id
        .split_once(ROOT_SEPARATOR)
        .map_or(thread_id, |(root, _)| root)
}

#[derive(Debug)]
pub struct StoreError(pub String);

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for StoreError {}

impl From<worker::Error> for StoreError {
    fn from(e: worker::Error) -> Self {
        StoreError(e.to_string())
    }
}

impl From<StoreError> for worker::Error {
    fn from(e: StoreError) -> Self {
        worker::Error::RustError(e.0)
    }
}

impl From<serde_json::Error> for StoreError {
    fn from(e: serde_json::Error) -> Self {
        StoreError(e.to_string())
    }
}

const SCHEMA: &[&str] = &[
    // Conversation history. `ord` is one-based; see `load_history_up_to`.
    "CREATE TABLE IF NOT EXISTS messages (
        thread_id TEXT NOT NULL,
        ord INTEGER NOT NULL,
        message_id TEXT NOT NULL,
        data TEXT NOT NULL,
        PRIMARY KEY (thread_id, ord)
    )",
    "CREATE TABLE IF NOT EXISTS threads (
        thread_id TEXT PRIMARY KEY,
        parent_thread_id TEXT,
        root_thread_id TEXT NOT NULL,
        spawn_message_order INTEGER,
        spawn_tool_call_id TEXT,
        closed INTEGER NOT NULL DEFAULT 0,
        is_subscription_event INTEGER NOT NULL DEFAULT 0,
        is_compaction INTEGER NOT NULL DEFAULT 0,
        fresh_context INTEGER NOT NULL DEFAULT 0
    )",
    "CREATE TABLE IF NOT EXISTS compaction_summaries (
        thread_id TEXT NOT NULL,
        up_to_order INTEGER NOT NULL,
        summary TEXT NOT NULL,
        PRIMARY KEY (thread_id, up_to_order)
    )",
    "CREATE TABLE IF NOT EXISTS processed_ids (
        thread_id TEXT NOT NULL,
        message_id TEXT NOT NULL,
        PRIMARY KEY (thread_id, message_id)
    )",
    "CREATE TABLE IF NOT EXISTS metadata (thread_id TEXT PRIMARY KEY, data TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS subscriptions (
        thread_id TEXT NOT NULL,
        tool_call_id TEXT NOT NULL,
        PRIMARY KEY (thread_id, tool_call_id)
    )",
    "CREATE TABLE IF NOT EXISTS user_choices (
        thread_id TEXT NOT NULL,
        choice_id TEXT NOT NULL,
        data TEXT NOT NULL,
        PRIMARY KEY (thread_id, choice_id)
    )",
    "CREATE TABLE IF NOT EXISTS toolset_cache (cache_key TEXT PRIMARY KEY, json TEXT NOT NULL)",
    // Runtime: pending input (the per-tree equivalent of the SQS FIFO queue),
    // durable timers, delivery dedup keys, and the observable event log.
    "CREATE TABLE IF NOT EXISTS inbox (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        message_id TEXT NOT NULL,
        data TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0
    )",
    "CREATE TABLE IF NOT EXISTS timers (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        due_at INTEGER NOT NULL,
        data TEXT NOT NULL
    )",
    "CREATE INDEX IF NOT EXISTS timers_due_at ON timers (due_at)",
    "CREATE TABLE IF NOT EXISTS dedup (key TEXT PRIMARY KEY, at INTEGER NOT NULL)",
    "CREATE TABLE IF NOT EXISTS events (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        thread_id TEXT NOT NULL,
        at INTEGER NOT NULL,
        data TEXT NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
];

fn s(value: impl Into<String>) -> SqlStorageValue {
    SqlStorageValue::from(value.into())
}

fn i(value: i64) -> SqlStorageValue {
    SqlStorageValue::from(value)
}

#[derive(Deserialize)]
struct One<T> {
    v: T,
}

/// Handle to the tree's SQLite database. Cheap to clone.
#[derive(Clone)]
pub struct Db {
    sql: SqlStorage,
}

impl Db {
    pub fn new(sql: SqlStorage) -> Self {
        Self { sql }
    }

    pub fn migrate(&self) -> Result<(), StoreError> {
        for statement in SCHEMA {
            self.run(statement, vec![])?;
        }
        Ok(())
    }

    pub fn run(&self, query: &str, bindings: Vec<SqlStorageValue>) -> Result<(), StoreError> {
        // Drain the cursor so the statement fully executes.
        let _: Vec<serde_json::Value> = self.sql.exec(query, bindings)?.to_array()?;
        Ok(())
    }

    pub fn rows<T: DeserializeOwned>(
        &self,
        query: &str,
        bindings: Vec<SqlStorageValue>,
    ) -> Result<Vec<T>, StoreError> {
        Ok(self.sql.exec(query, bindings)?.to_array()?)
    }

    /// The single column `v` of the first row, if any.
    pub fn value<T: DeserializeOwned>(
        &self,
        query: &str,
        bindings: Vec<SqlStorageValue>,
    ) -> Result<Option<T>, StoreError> {
        Ok(self
            .rows::<One<T>>(query, bindings)?
            .into_iter()
            .next()
            .map(|row| row.v))
    }

    fn flag(&self, column: &str, thread_id: &str) -> Result<bool, StoreError> {
        let value: Option<i64> = self.value(
            &format!("SELECT {column} AS v FROM threads WHERE thread_id = ?"),
            vec![s(thread_id)],
        )?;
        Ok(value.unwrap_or(0) != 0)
    }

    fn set_flag(&self, column: &str, thread_id: &str) -> Result<(), StoreError> {
        self.run(
            &format!("UPDATE threads SET {column} = 1 WHERE thread_id = ?"),
            vec![s(thread_id)],
        )
    }

    pub fn kv_get(&self, key: &str) -> Result<Option<String>, StoreError> {
        self.value("SELECT value AS v FROM kv WHERE key = ?", vec![s(key)])
    }

    pub fn kv_set(&self, key: &str, value: &str) -> Result<(), StoreError> {
        self.run(
            "INSERT INTO kv (key, value) VALUES (?, ?)
             ON CONFLICT (key) DO UPDATE SET value = excluded.value",
            vec![s(key), s(value)],
        )
    }
}

#[async_trait]
impl ConversationStore for Db {
    type Error = StoreError;

    async fn ensure_root_thread(&self, thread_id: &ThreadId<str>) -> Result<(), StoreError> {
        self.run(
            "INSERT INTO threads (thread_id, parent_thread_id, root_thread_id)
             VALUES (?, NULL, ?) ON CONFLICT (thread_id) DO NOTHING",
            vec![s(thread_id.as_str()), s(thread_id.as_str())],
        )
    }

    async fn thread_exists(&self, thread_id: &ThreadId<str>) -> Result<bool, StoreError> {
        let found: Option<i64> = self.value(
            "SELECT 1 AS v FROM threads WHERE thread_id = ?",
            vec![s(thread_id.as_str())],
        )?;
        Ok(found.is_some())
    }

    async fn load_history_up_to(
        &self,
        session_id: &ThreadId<str>,
        start_from: Option<i64>,
        up_to: Option<i64>,
    ) -> Result<Vec<InfinityMessage>, StoreError> {
        // One-based orders with an exclusive start and inclusive end, so the
        // bounds read as leading-row counts (same convention as the DSQL
        // store).
        let mut query = String::from("SELECT data AS v FROM messages WHERE thread_id = ?");
        let mut bindings = vec![s(session_id.as_str())];
        if let Some(n) = start_from {
            query.push_str(" AND ord > ?");
            bindings.push(i(n));
        }
        if let Some(n) = up_to {
            query.push_str(" AND ord <= ?");
            bindings.push(i(n));
        }
        query.push_str(" ORDER BY ord ASC");

        let rows: Vec<One<String>> = self.rows(&query, bindings)?;
        Ok(rows
            .into_iter()
            .filter_map(|row| {
                serde_json::from_str::<InfinityMessage>(&row.v)
                    .ok()
                    .or_else(|| {
                        serde_json::from_str::<Message>(&row.v)
                            .ok()
                            .map(InfinityMessage::from_message)
                    })
            })
            .collect())
    }

    async fn append_messages(
        &self,
        session_id: &ThreadId<str>,
        messages: Vec<(InfinityMessage, String)>,
    ) -> Result<(), StoreError> {
        let max: Option<i64> = self.value(
            "SELECT MAX(ord) AS v FROM messages WHERE thread_id = ?",
            vec![s(session_id.as_str())],
        )?;
        let mut ord = max.unwrap_or(0);
        for (message, message_id) in messages {
            ord += 1;
            self.run(
                "INSERT INTO messages (thread_id, ord, message_id, data) VALUES (?, ?, ?, ?)
                 ON CONFLICT (thread_id, ord) DO NOTHING",
                vec![
                    s(session_id.as_str()),
                    i(ord),
                    s(message_id),
                    s(serde_json::to_string(&message)?),
                ],
            )?;
        }
        Ok(())
    }

    async fn spawn_thread(
        &self,
        parent_thread_id: &ThreadId<str>,
        spawn_tool_call_id: &ToolCallId<str>,
        is_for_subscription_event: bool,
        context: SpawnContext,
    ) -> Result<ThreadId, StoreError> {
        let (spawn_message_order, fresh_context) = match context {
            SpawnContext::InheritUpTo(order) => (order as i64, false),
            SpawnContext::Fresh => (0, true),
        };
        let root: String = self
            .value(
                "SELECT root_thread_id AS v FROM threads WHERE thread_id = ?",
                vec![s(parent_thread_id.as_str())],
            )?
            .ok_or_else(|| StoreError(format!("unknown parent thread {parent_thread_id}")))?;

        let new_id = format!("{root}{ROOT_SEPARATOR}{}", uuid::Uuid::new_v4());
        self.run(
            "INSERT INTO threads (thread_id, parent_thread_id, root_thread_id,
                 spawn_message_order, spawn_tool_call_id, is_subscription_event, fresh_context)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
            vec![
                s(new_id.as_str()),
                s(parent_thread_id.as_str()),
                s(root),
                i(spawn_message_order),
                s(spawn_tool_call_id.as_str()),
                i(i64::from(is_for_subscription_event)),
                i(i64::from(fresh_context)),
            ],
        )?;
        Ok(ThreadId::from(new_id))
    }

    async fn is_thread_closed(&self, thread_id: &ThreadId<str>) -> Result<bool, StoreError> {
        self.flag("closed", thread_id.as_str())
    }

    async fn close_thread(&self, thread_id: &ThreadId<str>) -> Result<(), StoreError> {
        self.set_flag("closed", thread_id.as_str())
    }

    async fn is_subscription_event_thread(
        &self,
        thread_id: &ThreadId<str>,
    ) -> Result<bool, StoreError> {
        self.flag("is_subscription_event", thread_id.as_str())
    }

    async fn is_fresh_context_thread(&self, thread_id: &ThreadId<str>) -> Result<bool, StoreError> {
        self.flag("fresh_context", thread_id.as_str())
    }

    async fn get_thread_parent_info(
        &self,
        thread_id: &ThreadId<str>,
    ) -> Result<Option<(ThreadId, ToolCallId)>, StoreError> {
        #[derive(Deserialize)]
        struct Row {
            parent_thread_id: Option<String>,
            spawn_tool_call_id: Option<String>,
        }
        let row = self
            .rows::<Row>(
                "SELECT parent_thread_id, spawn_tool_call_id FROM threads WHERE thread_id = ?",
                vec![s(thread_id.as_str())],
            )?
            .into_iter()
            .next();
        Ok(match row {
            Some(Row {
                parent_thread_id: Some(parent),
                spawn_tool_call_id: Some(call),
            }) => Some((ThreadId::from(parent), ToolCallId::from(call))),
            _ => None,
        })
    }

    async fn get_ancestor_chain(
        &self,
        thread_id: &ThreadId<str>,
    ) -> Result<Vec<(ThreadId, i64)>, StoreError> {
        #[derive(Deserialize)]
        struct Row {
            parent_thread_id: Option<String>,
            spawn_message_order: Option<i64>,
        }
        let mut chain = Vec::new();
        let mut current = thread_id.as_str().to_owned();
        loop {
            let row = self
                .rows::<Row>(
                    "SELECT parent_thread_id, spawn_message_order FROM threads WHERE thread_id = ?",
                    vec![s(current.as_str())],
                )?
                .into_iter()
                .next();
            match row.and_then(|r| r.parent_thread_id.map(|p| (p, r.spawn_message_order))) {
                Some((parent, order)) => {
                    chain.push((ThreadId::from(parent.clone()), order.unwrap_or(0)));
                    current = parent;
                }
                None => break,
            }
        }
        chain.reverse();
        Ok(chain)
    }

    async fn mark_thread_as_compaction(&self, thread_id: &ThreadId<str>) -> Result<(), StoreError> {
        self.set_flag("is_compaction", thread_id.as_str())
    }

    async fn is_compaction_thread(&self, thread_id: &ThreadId<str>) -> Result<bool, StoreError> {
        self.flag("is_compaction", thread_id.as_str())
    }

    async fn get_thread_spawn_order(
        &self,
        thread_id: &ThreadId<str>,
    ) -> Result<Option<i64>, StoreError> {
        let order: Option<Option<i64>> = self.value(
            "SELECT spawn_message_order AS v FROM threads WHERE thread_id = ?",
            vec![s(thread_id.as_str())],
        )?;
        Ok(order.flatten())
    }

    async fn save_compaction_summary(
        &self,
        thread_id: &ThreadId<str>,
        summary: &str,
        up_to_order: i64,
    ) -> Result<(), StoreError> {
        self.run(
            "INSERT INTO compaction_summaries (thread_id, up_to_order, summary) VALUES (?, ?, ?)
             ON CONFLICT (thread_id, up_to_order) DO UPDATE SET summary = excluded.summary",
            vec![s(thread_id.as_str()), i(up_to_order), s(summary)],
        )
    }

    async fn load_latest_compaction_summary_up_to(
        &self,
        thread_id: &ThreadId<str>,
        up_to_order: Option<i64>,
    ) -> Result<Option<(String, i64)>, StoreError> {
        #[derive(Deserialize)]
        struct Row {
            summary: String,
            up_to_order: i64,
        }
        let rows: Vec<Row> = match up_to_order {
            Some(n) => self.rows(
                "SELECT summary, up_to_order FROM compaction_summaries
                 WHERE thread_id = ? AND up_to_order <= ?
                 ORDER BY up_to_order DESC LIMIT 1",
                vec![s(thread_id.as_str()), i(n)],
            )?,
            None => self.rows(
                "SELECT summary, up_to_order FROM compaction_summaries
                 WHERE thread_id = ? ORDER BY up_to_order DESC LIMIT 1",
                vec![s(thread_id.as_str())],
            )?,
        };
        Ok(rows.into_iter().next().map(|r| (r.summary, r.up_to_order)))
    }
}

#[async_trait]
impl StateStore for Db {
    type Error = StoreError;

    async fn get_processed_ids(
        &self,
        thread_id: &ThreadId<str>,
    ) -> Result<HashSet<String>, StoreError> {
        let rows: Vec<One<String>> = self.rows(
            "SELECT message_id AS v FROM processed_ids WHERE thread_id = ?",
            vec![s(thread_id.as_str())],
        )?;
        Ok(rows.into_iter().map(|r| r.v).collect())
    }

    async fn add_processed_message_ids(
        &self,
        thread_id: &ThreadId<str>,
        message_ids: Vec<String>,
    ) -> Result<(), StoreError> {
        for id in message_ids {
            self.run(
                "INSERT INTO processed_ids (thread_id, message_id) VALUES (?, ?)
                 ON CONFLICT DO NOTHING",
                vec![s(thread_id.as_str()), s(id)],
            )?;
        }
        Ok(())
    }

    async fn get_metadata(
        &self,
        root_thread_id: &ThreadId<str>,
    ) -> Result<Option<serde_json::Value>, StoreError> {
        let data: Option<String> = self.value(
            "SELECT data AS v FROM metadata WHERE thread_id = ?",
            vec![s(root_thread_id.as_str())],
        )?;
        Ok(data.map(|d| serde_json::from_str(&d)).transpose()?)
    }

    async fn set_metadata(
        &self,
        root_thread_id: &ThreadId<str>,
        metadata: serde_json::Value,
    ) -> Result<(), StoreError> {
        self.run(
            "INSERT INTO metadata (thread_id, data) VALUES (?, ?)
             ON CONFLICT (thread_id) DO UPDATE SET data = excluded.data",
            vec![
                s(root_thread_id.as_str()),
                s(serde_json::to_string(&metadata)?),
            ],
        )
    }

    async fn get_active_subscriptions(
        &self,
        thread_id: &ThreadId<str>,
    ) -> Result<Vec<ToolCallId>, StoreError> {
        let rows: Vec<One<String>> = self.rows(
            "SELECT tool_call_id AS v FROM subscriptions WHERE thread_id = ?",
            vec![s(thread_id.as_str())],
        )?;
        Ok(rows.into_iter().map(|r| ToolCallId::from(r.v)).collect())
    }

    async fn add_active_subscription(
        &self,
        thread_id: &ThreadId<str>,
        tool_call_id: &ToolCallId<str>,
    ) -> Result<(), StoreError> {
        self.run(
            "INSERT INTO subscriptions (thread_id, tool_call_id) VALUES (?, ?)
             ON CONFLICT DO NOTHING",
            vec![s(thread_id.as_str()), s(tool_call_id.as_str())],
        )
    }

    async fn remove_active_subscription(
        &self,
        thread_id: &ThreadId<str>,
        tool_call_id: &ToolCallId<str>,
    ) -> Result<(), StoreError> {
        self.run(
            "DELETE FROM subscriptions WHERE thread_id = ? AND tool_call_id = ?",
            vec![s(thread_id.as_str()), s(tool_call_id.as_str())],
        )
    }

    async fn add_pending_user_choice(
        &self,
        thread_id: &ThreadId<str>,
        choice: UserChoice,
    ) -> Result<(), StoreError> {
        self.run(
            "INSERT INTO user_choices (thread_id, choice_id, data) VALUES (?, ?, ?)
             ON CONFLICT (thread_id, choice_id) DO UPDATE SET data = excluded.data",
            vec![
                s(thread_id.as_str()),
                s(choice.id.as_str()),
                s(serde_json::to_string(&choice)?),
            ],
        )
    }

    async fn remove_pending_user_choice(
        &self,
        thread_id: &ThreadId<str>,
        choice_id: &ChoiceId<str>,
    ) -> Result<(), StoreError> {
        self.run(
            "DELETE FROM user_choices WHERE thread_id = ? AND choice_id = ?",
            vec![s(thread_id.as_str()), s(choice_id.as_str())],
        )
    }

    async fn get_pending_user_choices(
        &self,
        thread_id: &ThreadId<str>,
    ) -> Result<Vec<UserChoice>, StoreError> {
        let rows: Vec<One<String>> = self.rows(
            "SELECT data AS v FROM user_choices WHERE thread_id = ?",
            vec![s(thread_id.as_str())],
        )?;
        rows.into_iter()
            .map(|r| Ok(serde_json::from_str(&r.v)?))
            .collect()
    }
}

#[async_trait]
impl ToolsetCache for Db {
    type Error = StoreError;

    async fn get_cached(&self, cache_key: &str) -> Result<Option<String>, StoreError> {
        self.value(
            "SELECT json AS v FROM toolset_cache WHERE cache_key = ?",
            vec![s(cache_key)],
        )
    }

    async fn put_cache(&self, cache_key: &str, json: &str) -> Result<(), StoreError> {
        self.run(
            "INSERT INTO toolset_cache (cache_key, json) VALUES (?, ?)
             ON CONFLICT (cache_key) DO UPDATE SET json = excluded.json",
            vec![s(cache_key), s(json)],
        )
    }
}
