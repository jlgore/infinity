//! Worker configuration from bindings, vars, and secrets (see `wrangler.toml`).

use infinity_agent_core::tools::config::ToolsConfig;
use worker::{Env, Result};

use crate::anthropic::AnthropicConfig;

pub struct WorkerConfig {
    pub ai_binding: String,
    pub anthropic: AnthropicConfig,
    pub toolset_server_urls: Vec<String>,
    pub system_prompt: Option<String>,
    pub public_url: Option<String>,
    pub rap_callback_secret: Option<String>,
    pub rap_auth_token: Option<String>,
    /// How long event-log entries are kept; `None` keeps them forever.
    pub event_retention_ms: Option<i64>,
}

/// A non-empty var or secret.
pub fn get(env: &Env, name: &str) -> Option<String> {
    env.var(name)
        .map(|v| v.to_string())
        .or_else(|_| env.secret(name).map(|v| v.to_string()))
        .ok()
        .filter(|v| !v.is_empty())
}

fn parse<T: std::str::FromStr>(env: &Env, name: &str, default: T) -> T {
    get(env, name)
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

impl WorkerConfig {
    pub fn from_env(env: &Env) -> Result<Self> {
        let toolset_server_urls = match get(env, "TOOLS_CONFIG") {
            Some(json) => ToolsConfig::from_json(&json)
                .map_err(|e| worker::Error::RustError(format!("invalid TOOLS_CONFIG: {e}")))?
                .toolset_server_urls()
                .into_iter()
                .map(|(url, _)| url)
                .collect(),
            None => Vec::new(),
        };

        Ok(Self {
            ai_binding: get(env, "AI_BINDING").unwrap_or_else(|| "AI".to_owned()),
            anthropic: AnthropicConfig {
                gateway_id: get(env, "AI_GATEWAY_ID").unwrap_or_else(|| "default".to_owned()),
                model: get(env, "ANTHROPIC_MODEL").unwrap_or_else(|| "claude-opus-5-5".to_owned()),
                max_tokens: parse(env, "ANTHROPIC_MAX_TOKENS", 64_000),
                context_window: parse(env, "ANTHROPIC_CONTEXT_WINDOW", 1_000_000),
                effort: get(env, "ANTHROPIC_EFFORT"),
                thinking_display: get(env, "ANTHROPIC_THINKING_DISPLAY")
                    .unwrap_or_else(|| "summarized".to_owned()),
                refusal_fallbacks: parse(env, "ANTHROPIC_REFUSAL_FALLBACKS", true),
                betas: get(env, "ANTHROPIC_BETAS")
                    .map(|v| {
                        v.split(',')
                            .map(|b| b.trim().to_owned())
                            .filter(|b| !b.is_empty())
                            .collect()
                    })
                    .unwrap_or_default(),
            },
            toolset_server_urls,
            system_prompt: get(env, "SYSTEM_PROMPT"),
            public_url: get(env, "PUBLIC_URL"),
            rap_callback_secret: get(env, "RAP_CALLBACK_SECRET"),
            rap_auth_token: get(env, "RAP_AUTH_TOKEN"),
            event_retention_ms: match parse::<i64>(env, "EVENT_RETENTION_DAYS", 30) {
                days if days > 0 => Some(days * 24 * 60 * 60 * 1000),
                _ => None,
            },
        })
    }
}
