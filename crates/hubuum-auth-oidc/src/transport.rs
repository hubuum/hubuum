use std::time::Duration;

use hubuum_outbound_http::{OutboundHeaders, OutboundHttpError, OutboundMethod, OutboundRequest};
use openidconnect::{HttpRequest, HttpResponse, http};
use tokio::sync::Semaphore;

use crate::OidcError;

const RESPONSE_LIMIT: usize = 128 * 1024;
static REQUESTS: Semaphore = Semaphore::const_new(32);

/// No redirects, retries, proxies, permissive TLS or unbounded queue. Every call
/// resolves and screens all addresses through the shared transport.
#[derive(Clone, Copy)]
pub(crate) struct Transport {
    pub(crate) allow_private_targets: bool,
}

impl Transport {
    pub(crate) async fn send(self, request: HttpRequest) -> Result<HttpResponse, OidcError> {
        let _permit = REQUESTS.try_acquire().map_err(|_| OidcError::Unavailable)?;
        let method = match *request.method() {
            http::Method::GET => OutboundMethod::Get,
            http::Method::POST => OutboundMethod::Post,
            _ => return Err(OidcError::Configuration),
        };
        let mut headers = OutboundHeaders::new();
        for (name, value) in request.headers() {
            // The protocol library sets Content-Length; the actual transport
            // must calculate framing from the body it sends.
            if name == http::header::CONTENT_LENGTH {
                continue;
            }
            headers
                .insert(
                    name.as_str(),
                    value.to_str().map_err(|_| OidcError::Configuration)?,
                )
                .map_err(|_| OidcError::Configuration)?;
        }
        let url = request.uri().to_string();
        let body = String::from_utf8(request.into_body()).map_err(|_| OidcError::Configuration)?;
        self.send_to(method, headers, body, url).await
    }

    async fn send_to(
        self,
        method: OutboundMethod,
        headers: OutboundHeaders,
        body: String,
        url: String,
    ) -> Result<HttpResponse, OidcError> {
        let response = OutboundRequest::new(method, url, Duration::from_secs(5))
            .headers(headers)
            .body((!body.is_empty()).then_some(body))
            .max_response_bytes(RESPONSE_LIMIT)
            .require_complete_body()
            .allow_private_targets(self.allow_private_targets)
            .send()
            .await
            .map_err(|error| match error {
                OutboundHttpError::ResponseRead => OidcError::InvalidResponse,
                _ => OidcError::Unavailable,
            })?;
        if response.status_code() >= 500 || response.status_code() == 429 {
            return Err(OidcError::Unavailable);
        }
        if (300..400).contains(&response.status_code()) {
            return Err(OidcError::InvalidResponse);
        }
        let mut output = http::Response::builder().status(response.status_code());
        // Protocol clients only need Content-Type. Never propagate provider cookies.
        if let Some(content_type) = response
            .headers()
            .get("content-type")
            .and_then(|v| v.as_str())
        {
            output = output.header(http::header::CONTENT_TYPE, content_type);
        }
        output
            .body(response.body_preview().as_bytes().to_vec())
            .map_err(|_| OidcError::InvalidResponse)
    }

    pub(crate) async fn json(self, url: &str) -> Result<serde_json::Value, OidcError> {
        let request = http::Request::builder()
            .uri(url)
            .header(http::header::ACCEPT, "application/json")
            .body(Vec::new())
            .map_err(|_| OidcError::Configuration)?;
        let response = self.send(request).await?;
        if response.status() != http::StatusCode::OK {
            return Err(OidcError::InvalidResponse);
        }
        serde_json::from_slice(response.body()).map_err(|_| OidcError::InvalidResponse)
    }
}
