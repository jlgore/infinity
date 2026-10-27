/// HTTP and caching abstractions for RAP client operations.
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Abstraction over HTTP client for RAP tool invocation and toolset discovery.
#[async_trait]
pub trait HttpClient: Send + Sync + Clone {
    /// Error type returned by HTTP operations.
    type Error: std::error::Error + Send + Sync + 'static;

    /// POST a JSON body to the given URL. Returns the HTTP status code.
    async fn post(&self, url: &str, body: &str) -> Result<u16, Self::Error>;
    /// GET the given URL. Returns the HTTP status code and response body.
    async fn get(&self, url: &str) -> Result<(u16, Vec<u8>), Self::Error>;
}

/// Abstraction over toolset manifest caching.
#[async_trait]
pub trait ToolsetCache: Send + Sync {
    /// Error type returned by cache operations.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Look up a cached value by key.
    async fn get_cached(&self, cache_key: &str) -> Result<Option<String>, Self::Error>;
    /// Store a value in the cache.
    async fn put_cache(&self, cache_key: &str, json: &str) -> Result<(), Self::Error>;
}

// ── SimpleHttpClient ──

#[cfg(not(target_arch = "wasm32"))]
mod simple;
#[cfg(not(target_arch = "wasm32"))]
pub use simple::{SimpleHttpClient, SimpleHttpError};

// ── InMemoryToolsetCache ──

/// Error type for [`InMemoryToolsetCache`].
#[derive(Debug)]
pub struct CacheError(String);
impl std::fmt::Display for CacheError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl std::error::Error for CacheError {}

/// In-memory [`ToolsetCache`] backed by a `HashMap`. Useful for testing and CLI usage.
pub struct InMemoryToolsetCache {
    store: Arc<Mutex<HashMap<String, String>>>,
}

impl Default for InMemoryToolsetCache {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryToolsetCache {
    /// Create a new empty cache.
    pub fn new() -> Self {
        Self {
            store: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl ToolsetCache for InMemoryToolsetCache {
    type Error = CacheError;

    async fn get_cached(&self, key: &str) -> Result<Option<String>, CacheError> {
        Ok(self
            .store
            .lock()
            .expect("bug: cache mutex poisoned")
            .get(key)
            .cloned())
    }

    async fn put_cache(&self, key: &str, json: &str) -> Result<(), CacheError> {
        self.store
            .lock()
            .expect("bug: cache mutex poisoned")
            .insert(key.to_owned(), json.to_owned());
        Ok(())
    }
}
