//! Local (tokio + reqwest) callback reception and tool discovery.

use infinity_agent_core::system::local::ChannelSender;
use infinity_agent_core::tools::Tool;
use infinity_agent_core::tools::rap_tool::RapTool;
use infinity_agent_core::traits::InputSender;
use rap_client::callback_server::{bind_callback_listener, start_callback_server_on};
use rap_client::http::{InMemoryToolsetCache, SimpleHttpClient};
use rap_client::notifier::RapNotifier;
use rap_client::toolset_loader::ToolsetLoader;
use rap_protocol::RapViewUpdate;
use tokio::sync::mpsc;

use crate::{BoxError, CallbackDelivery, prepare_callback};

/// A bound RAP callback listener that has not started accepting requests.
pub struct RapCallbackBridge {
    listener: tokio::net::TcpListener,
    callback_url: String,
}

impl RapCallbackBridge {
    /// Bind a callback listener on localhost.
    pub async fn bind() -> Result<Self, BoxError> {
        let (listener, callback_url) = bind_callback_listener().await?;
        Ok(Self {
            listener,
            callback_url,
        })
    }

    /// Use an already-bound callback listener.
    pub fn from_listener(listener: tokio::net::TcpListener) -> Result<Self, BoxError> {
        let callback_url = format!("http://127.0.0.1:{}", listener.local_addr()?.port());
        Ok(Self {
            listener,
            callback_url,
        })
    }

    /// URL that RAP invocations should use for callbacks.
    pub fn callback_url(&self) -> &str {
        &self.callback_url
    }

    /// Start callback reception, forwarding agent inputs into `sender` and
    /// returning view updates on a channel.
    ///
    /// Each converted input is sent with its prepared deduplication ID, so
    /// HTTP retries of one callback are absorbed by the state store. Send
    /// failures are logged and do not stop the listener. Admission belongs
    /// to the receiving system: the local router consults the state store's
    /// existence and stopped-thread checks before spawning a driver, so an
    /// embedding using this form only needs to store or drop the returned view
    /// updates.
    pub fn serve_into<S>(
        self,
        sender: S,
    ) -> (
        mpsc::UnboundedReceiver<RapViewUpdate>,
        tokio::task::JoinHandle<()>,
    )
    where
        S: InputSender + 'static,
    {
        let (view_tx, view_rx) = mpsc::unbounded_channel();
        let task = start_callback_server_on(self.listener, move |callback| {
            let sender = sender.clone();
            let view_tx = view_tx.clone();
            async move {
                match prepare_callback(callback) {
                    CallbackDelivery::Input { message, dedup_id } => {
                        if let Err(error) = sender.send_to_input_queue(*message, &dedup_id).await {
                            tracing::warn!(%error, "failed to route RAP callback");
                        }
                    }
                    CallbackDelivery::ViewUpdate(update) => {
                        if view_tx.send(update).is_err() {
                            tracing::warn!("RAP view-update destination has stopped");
                        }
                    }
                }
            }
        });
        (view_rx, task)
    }
}

/// RAP tools discovered from one or more servers, ready for a local system.
///
/// Callback reception is configured separately through
/// [`RapCallbackBridge`]. Pass that bridge's URL to [`connect`](Self::connect),
/// then serve it into the local system's sender after the system is built.
pub struct RapToolSet {
    tools: Vec<RapTool<SimpleHttpClient>>,
    server_urls: Vec<String>,
}

impl RapToolSet {
    /// Discover tools from each server using an explicit callback destination.
    ///
    /// `session_id` scopes the in-memory manifest cache. Each server must
    /// expose `/.well-known/rap-toolset`. `callback_url` should name a bound
    /// [`RapCallbackBridge`] or another receiver for RAP callbacks.
    pub async fn connect(
        server_urls: impl IntoIterator<Item = String>,
        session_id: &rap_protocol::ThreadId<str>,
        callback_url: impl Into<String>,
    ) -> Result<Self, BoxError> {
        let server_urls: Vec<String> = server_urls.into_iter().collect();
        let callback_url = callback_url.into();
        let http = SimpleHttpClient::new();
        let loaded = ToolsetLoader::new(http.clone(), InMemoryToolsetCache::new())
            .load_toolsets(&server_urls, session_id)
            .await?;

        let mut tools = Vec::new();
        for toolset in loaded {
            let endpoint = toolset.manifest.endpoint;
            for definition in toolset.manifest.tools {
                tools.push(RapTool {
                    descriptor: definition.into(),
                    endpoint: endpoint.clone(),
                    http_client: http.clone(),
                    callback_url: Some(callback_url.clone()),
                });
            }
        }

        Ok(Self { tools, server_urls })
    }

    /// Build the discovered tools for registration on a local agent system.
    pub fn tools(&self) -> Vec<Box<dyn Tool<ChannelSender>>> {
        self.tools
            .iter()
            .cloned()
            .map(|tool| Box::new(tool) as Box<dyn Tool<ChannelSender>>)
            .collect()
    }

    /// Build the lifecycle notifier for the connected RAP servers.
    pub fn notifier(&self) -> RapNotifier<SimpleHttpClient> {
        RapNotifier::new(self.server_urls.clone(), SimpleHttpClient::new())
    }
}
