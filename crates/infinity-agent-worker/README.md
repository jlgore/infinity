# infinity-agent-worker

The Infinity runtime on Cloudflare Workers and Durable Objects: the same
`AgentSystem::step` the Lambda deployment runs, with Cloudflare-native
platform pieces.

| Runtime need | Lambda deployment | This crate |
|---|---|---|
| Per-thread ordering, cross-thread concurrency | SQS FIFO `MessageGroupId` | One `AgentTree` Durable Object per conversation tree; its alarm drains an inbox table |
| History and state | Aurora DSQL + DynamoDB | The Durable Object's SQLite storage |
| Loopback sender | SQS send | Insert into the inbox table |
| `sleep` / `sleep_until` | SQS delay queue + EventBridge Scheduler | Timers table + Durable Object alarm |
| RAP callbacks | `rap-receiver` Lambda | `POST /rap/callback/<secret>` |
| Model | Bedrock | Claude through **AI Gateway**, via the `AI` binding |

## How it works

A conversation tree is a root thread plus every thread it spawns. Child
thread IDs are `"{root}~{uuid}"`, so any thread ID names the Durable Object
that owns it, and child threads read their ancestors' history from the same
local database.

Requests never run the agent directly. They append to the tree's inbox and set
the alarm. The alarm handler drains the inbox through `AgentSystem::step`,
which runs the threads in a batch concurrently. Alarm handlers never overlap,
which gives the per-thread serialization SQS FIFO gave the Lambda
deployment. Request handlers could not provide it: a Durable Object keeps
accepting requests while a step awaits the model stream. A failed step is
retried (the core's processed-ID dedup absorbs redelivery). An input is
dropped after five failed attempts, leaving an `error` event on its thread.

## Claude via AI Gateway

Requests go to `env.AI.gateway(AI_GATEWAY_ID).run({ provider: "anthropic",
endpoint: "v1/messages", ... })`. The binding authenticates the call, and the
gateway supplies the Anthropic credential, so **the Worker holds no
Anthropic key**. The native Messages API body passes through unchanged:
adaptive thinking with signed thinking blocks replayed across turns,
streamed tool calls, and automatic prompt caching.

Before deploying:

1. Create an AI Gateway (its name is `AI_GATEWAY_ID`, default `default`).
2. Give it an Anthropic credential, either way:
   - **BYOK:** Provider Keys > Add API Key > Anthropic, under the `default`
     alias.
   - **Unified Billing:** load credits on the Cloudflare account.

Defaults: `claude-opus-5-5`, effort `high`, `max_tokens` 64000, and summarized
thinking so reasoning streams into the event log. Server-side refusal
fallbacks (`fallbacks: "default"`) are on; set
`ANTHROPIC_REFUSAL_FALLBACKS=false` to turn them off.

## Deploy

```sh
cd crates/infinity-agent-worker
npx wrangler secret put API_TOKEN            # bearer token for /threads/*
npx wrangler secret put RAP_CALLBACK_SECRET  # callbacks go to /rap/callback/<secret>
npx wrangler deploy                          # runs worker-build --release
```

`worker-build` needs `cargo install worker-build` and the
`wasm32-unknown-unknown` target. The workspace's `.cargo/config.toml` sets the
`getrandom` wasm backend.

## API

All `/threads/*` routes require `Authorization: Bearer $API_TOKEN`.

```sh
# Send user text (message_id is an optional idempotency key)
curl -X POST "$URL/threads/my-thread/messages" -H "authorization: Bearer $API_TOKEN" \
  -H 'content-type: application/json' -d '{"text":"Write a haiku about Rust","message_id":"m1"}'

# Poll events; pass the returned `next` as `after`. tree=1 includes child threads.
curl "$URL/threads/my-thread/events?after=0&tree=1" -H "authorization: Bearer $API_TOKEN"
```

Events are JSON objects with a `type` of `user_input`, `text`, `thinking`,
`tool_call`, `tool_result`, `subscription_event`, `oauth_required`,
`user_choice_required`, `user_choice_dismissed`, `compaction_applied`,
`completion_finished` (with token usage), `info`, or `error`. Streamed text
and reasoning are coalesced into one event per run.

Thread IDs may contain ASCII letters, digits, and `-_.:`; `~` is reserved
for child threads.

## Configuration

| Name | Kind | Default | Purpose |
|---|---|---|---|
| `AI` | binding | | AI Gateway access |
| `AGENT_TREE` | binding | | Durable Object namespace |
| `AI_GATEWAY_ID` | var | `default` | Gateway to route through |
| `ANTHROPIC_MODEL` | var | `claude-opus-5-5` | Model ID |
| `ANTHROPIC_EFFORT` | var | model default | `output_config.effort` |
| `ANTHROPIC_MAX_TOKENS` | var | `64000` | Per-response output cap |
| `ANTHROPIC_CONTEXT_WINDOW` | var | `1000000` | Compaction threshold input |
| `ANTHROPIC_THINKING_DISPLAY` | var | `summarized` | `thinking.display` |
| `ANTHROPIC_REFUSAL_FALLBACKS` | var | `true` | Server-side refusal fallbacks |
| `ANTHROPIC_BETAS` | var | | Extra `anthropic-beta` values, comma-separated |
| `TOOLS_CONFIG` | var | | `{"tool_sets":[{"type":"toolset_server","server_url":"..."}]}` |
| `SYSTEM_PROMPT` | var | | Appended to the built-in system prompt |
| `PUBLIC_URL` | var | request origin | Base of the RAP callback URL |
| `API_TOKEN` | secret | required | Client bearer token |
| `RAP_CALLBACK_SECRET` | secret | | Path secret for RAP callbacks |
| `RAP_AUTH_TOKEN` | secret | | Bearer token sent to RAP tool servers |
| `ALLOW_UNAUTHENTICATED` | var | | `true` disables the `API_TOKEN` check (local dev only) |

## Limitations

- Only `toolset_server` entries in `TOOLS_CONFIG` apply. Command-launched
  and stdio MCP toolsets need a process and are skipped; serve MCP servers
  over HTTP behind RAP instead.
- RAP view updates are acknowledged and dropped, as on Lambda.
- Clients poll for events; there is no push channel yet.
- The core's `tracing` output is not routed to the Workers console.
