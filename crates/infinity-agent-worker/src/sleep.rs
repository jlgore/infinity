//! `sleep` and `sleep_until` backed by Durable Object alarms: the wake-up
//! message is written to the timers table, and the alarm that drains the tree
//! is always set to the earliest due timer.

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use chrono_tz::Tz;
use infinity_agent_core::ThreadId;
use infinity_agent_core::message::{InputMessage, InputMessageContent};
use infinity_agent_core::tools::{Tool, ToolContext};
use infinity_provider_protocol::message::{Text, ToolResult, ToolResultContent, UserContent};

use crate::sender::TreeSender;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Build the tool-result message a sleep delivers when it wakes.
fn wakeup_message(
    id: &rap_protocol::ToolCallId<str>,
    call_id: Option<rap_protocol::ProviderCallId>,
    text: String,
    group_id: &ThreadId<str>,
) -> InputMessage {
    InputMessage {
        content: InputMessageContent::User(UserContent::ToolResult(ToolResult {
            id: id.as_str().to_owned(),
            call_id: call_id.map(|c| c.into_inner()),
            content: vec![ToolResultContent::Text(Text { text })],
        })),
        group_id: group_id.to_owned(),
        metadata: None,
        synthetic: None,
        display_as: None,
        subscription: false,
    }
}

fn deliver_at(
    context: &ToolContext<TreeSender>,
    message: &InputMessage,
    target: DateTime<Utc>,
) -> Result<(), BoxError> {
    if target <= Utc::now() {
        context.message_sender.enqueue(message)?;
    } else {
        context
            .message_sender
            .schedule(message, target.timestamp_millis())?;
    }
    Ok(())
}

pub struct SleepTool;

#[async_trait]
impl Tool<TreeSender> for SleepTool {
    fn name(&self) -> &str {
        "sleep"
    }

    fn is_passive(&self) -> bool {
        true
    }

    fn description(&self) -> &str {
        "Sleep for a specified number of seconds before continuing. Useful for waiting or delaying actions. This tool will automatically be interrupted by the system on user input, so you are free to invoke it in a loop as necessary."
    }

    fn parameters(&self) -> serde_json::Value {
        serde_json::json!({
            "type": "object",
            "properties": {
                "seconds": { "type": "number", "description": "Number of seconds to sleep" },
                "reason": { "type": "string", "description": "Human-readable reason for sleeping" }
            },
            "required": ["seconds"]
        })
    }

    fn display_script(&self) -> Option<&str> {
        Some(
            r#"let s = "Sleeping " + args.seconds + "s"; if args.reason != () { s += ": " + args.reason; } s"#,
        )
    }

    async fn execute(
        &self,
        args: serde_json::Value,
        id: rap_protocol::ToolCallId,
        call_id: Option<rap_protocol::ProviderCallId>,
        context: &ToolContext<TreeSender>,
    ) -> Result<(), BoxError> {
        let seconds = args["seconds"].as_f64().unwrap_or(0.0).max(0.0);
        let message = wakeup_message(
            &id,
            call_id,
            format!("Slept for {seconds} seconds"),
            &context.group_id,
        );
        let target = Utc::now() + chrono::Duration::milliseconds((seconds * 1000.0) as i64);
        deliver_at(context, &message, target)
    }
}

pub struct SleepUntilTool;

#[async_trait]
impl Tool<TreeSender> for SleepUntilTool {
    fn name(&self) -> &str {
        "sleep_until"
    }

    fn is_passive(&self) -> bool {
        true
    }

    fn description(&self) -> &str {
        "Sleep until a specific date and time in a given timezone. The agent will hibernate with zero resource usage and wake up at the specified time."
    }

    fn parameters(&self) -> serde_json::Value {
        serde_json::json!({
            "type": "object",
            "properties": {
                "date": { "type": "string", "description": "Target date in YYYY-MM-DD format" },
                "time": { "type": "string", "description": "Target time in HH:MM or HH:MM:SS 24-hour format" },
                "timezone": { "type": "string", "description": "IANA timezone name. Defaults to UTC." }
            },
            "required": ["date", "time"]
        })
    }

    fn display_script(&self) -> Option<&str> {
        Some(
            r#"let s = "Sleeping until " + args.date + " " + args.time; if args.timezone != () { s += " (" + args.timezone + ")"; } s"#,
        )
    }

    async fn execute(
        &self,
        args: serde_json::Value,
        id: rap_protocol::ToolCallId,
        call_id: Option<rap_protocol::ProviderCallId>,
        context: &ToolContext<TreeSender>,
    ) -> Result<(), BoxError> {
        let date_str = args["date"].as_str().unwrap_or("");
        let time_str = args["time"].as_str().unwrap_or("");
        let tz_str = args["timezone"].as_str().unwrap_or("UTC");

        let tz: Tz = tz_str
            .parse()
            .map_err(|_| format!("Invalid timezone: '{tz_str}'"))?;
        let date = NaiveDate::parse_from_str(date_str, "%Y-%m-%d")?;
        let time = NaiveTime::parse_from_str(time_str, "%H:%M:%S")
            .or_else(|_| NaiveTime::parse_from_str(time_str, "%H:%M"))?;
        let naive = NaiveDateTime::new(date, time);
        let target = naive
            .and_local_timezone(tz)
            .single()
            .ok_or(format!("Ambiguous datetime {naive} in {tz_str}"))?
            .with_timezone(&Utc);

        let text = if target <= Utc::now() {
            format!("Target time {date_str} {time_str} is in the past. Waking immediately.")
        } else {
            format!("Woke up at target time: {date_str} {time_str} ({tz_str})")
        };
        let message = wakeup_message(&id, call_id, text, &context.group_id);
        deliver_at(context, &message, target)
    }
}
