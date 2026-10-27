//! Plain reqwest-based [`HttpClient`] (native targets only).

use async_trait::async_trait;

use super::HttpClient;

/// Error type for [`SimpleHttpClient`].
#[derive(Debug)]
pub struct SimpleHttpError(String);
impl std::fmt::Display for SimpleHttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl std::error::Error for SimpleHttpError {}

/// Plain reqwest-based [`HttpClient`] with no authentication.
#[derive(Clone)]
pub struct SimpleHttpClient {
    client: reqwest::Client,
}

impl Default for SimpleHttpClient {
    fn default() -> Self {
        Self::new()
    }
}

impl SimpleHttpClient {
    /// Create a new client with default settings.
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl HttpClient for SimpleHttpClient {
    type Error = SimpleHttpError;

    async fn post(&self, url: &str, body: &str) -> Result<u16, SimpleHttpError> {
        let resp = self
            .client
            .post(url)
            .header("content-type", "application/json")
            .body(body.to_owned())
            .send()
            .await
            .map_err(|e| SimpleHttpError(e.to_string()))?;
        Ok(resp.status().as_u16())
    }

    async fn get(&self, url: &str) -> Result<(u16, Vec<u8>), SimpleHttpError> {
        let resp = self
            .client
            .get(url)
            .header("accept", "application/json")
            .send()
            .await
            .map_err(|e| SimpleHttpError(e.to_string()))?;
        let status = resp.status().as_u16();
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| SimpleHttpError(e.to_string()))?;
        Ok((status, bytes.to_vec()))
    }
}
