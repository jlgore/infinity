//! The tree's loopback path: everything the runtime wants to happen later is
//! written to the inbox (or, when delayed, the timers table) and picked up by
//! the Durable Object's alarm.

use async_trait::async_trait;
use infinity_agent_core::message::InputMessage;
use infinity_agent_core::traits::InputSender;
use worker::SqlStorageValue;

use crate::db::{Db, StoreError, root_of};

#[derive(Clone)]
pub struct TreeSender {
    db: Db,
    root: String,
}

impl TreeSender {
    pub fn new(db: Db, root: impl Into<String>) -> Self {
        Self {
            db,
            root: root.into(),
        }
    }

    fn check_tree(&self, message: &InputMessage) -> Result<(), StoreError> {
        let target = root_of(message.group_id.as_str());
        if target != self.root {
            return Err(StoreError(format!(
                "message for thread {} does not belong to tree {}",
                message.group_id, self.root
            )));
        }
        Ok(())
    }

    /// Queue `message` for the next drain. Each row gets a fresh message ID,
    /// which stays stable across retries of the same row (like an SQS
    /// message ID), so the core's processed-ID dedup absorbs redelivery.
    pub fn enqueue(&self, message: &InputMessage) -> Result<(), StoreError> {
        self.check_tree(message)?;
        self.db.run(
            "INSERT INTO inbox (message_id, data) VALUES (?, ?)",
            vec![
                SqlStorageValue::from(uuid::Uuid::new_v4().to_string()),
                SqlStorageValue::from(serde_json::to_string(message)?),
            ],
        )
    }

    /// Deliver `message` once `due_at_ms` (Unix epoch milliseconds) passes.
    pub fn schedule(&self, message: &InputMessage, due_at_ms: i64) -> Result<(), StoreError> {
        self.check_tree(message)?;
        self.db.run(
            "INSERT INTO timers (due_at, data) VALUES (?, ?)",
            vec![
                SqlStorageValue::from(due_at_ms),
                SqlStorageValue::from(serde_json::to_string(message)?),
            ],
        )
    }
}

#[async_trait]
impl InputSender for TreeSender {
    type Error = StoreError;

    async fn send_to_input_queue(
        &self,
        message: InputMessage,
        _dedup_id: &str,
    ) -> Result<(), StoreError> {
        self.enqueue(&message)
    }
}
