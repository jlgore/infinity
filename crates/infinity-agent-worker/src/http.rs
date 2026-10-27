//! RAP HTTP client over the Workers `fetch` API.

use async_trait::async_trait;
use rap_client::http::HttpClient;
use worker::send::SendFuture;
use worker::{Fetch, Headers, Method, Request, RequestInit};

#[derive(Debug)]
pub struct FetchError(String);

impl std::fmt::Display for FetchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for FetchError {}

impl From<worker::Error> for FetchError {
    fn from(e: worker::Error) -> Self {
        FetchError(e.to_string())
    }
}

/// Plain `fetch` client for RAP tool servers. When `bearer_token` is set it
/// is sent as `Authorization: Bearer ...` on every request.
#[derive(Clone, Default)]
pub struct FetchHttpClient {
    bearer_token: Option<String>,
}

impl FetchHttpClient {
    pub fn new(bearer_token: Option<String>) -> Self {
        Self { bearer_token }
    }

    fn request(
        &self,
        method: Method,
        url: &str,
        content_type: &str,
        body: Option<&str>,
    ) -> Result<Request, FetchError> {
        let headers = Headers::new();
        headers.set(
            if body.is_some() {
                "content-type"
            } else {
                "accept"
            },
            content_type,
        )?;
        if let Some(token) = &self.bearer_token {
            headers.set("authorization", &format!("Bearer {token}"))?;
        }
        let mut init = RequestInit::new();
        init.with_method(method).with_headers(headers);
        if let Some(body) = body {
            init.with_body(Some(body.into()));
        }
        Ok(Request::new_with_init(url, &init)?)
    }
}

#[async_trait]
impl HttpClient for FetchHttpClient {
    type Error = FetchError;

    async fn post(&self, url: &str, body: &str) -> Result<u16, FetchError> {
        let request = self.request(Method::Post, url, "application/json", Some(body))?;
        // JS futures are `!Send`; a Workers isolate is single-threaded.
        SendFuture::new(async move {
            let response = Fetch::Request(request).send().await?;
            Ok(response.status_code())
        })
        .await
    }

    async fn get(&self, url: &str) -> Result<(u16, Vec<u8>), FetchError> {
        let request = self.request(Method::Get, url, "application/json", None)?;
        SendFuture::new(async move {
            let mut response = Fetch::Request(request).send().await?;
            let status = response.status_code();
            let body = response.bytes().await?;
            Ok((status, body))
        })
        .await
    }
}
