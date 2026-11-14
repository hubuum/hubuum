use std::time::Duration;

use hubuum_outbound_http::{OutboundHeaders, OutboundHttpError, OutboundMethod, OutboundRequest};
use oauth2::{HttpRequest, HttpResponse, http};
use tokio::sync::Semaphore;

use super::ProtocolError;

const RESPONSE_LIMIT: usize = 128 * 1024;
static REQUESTS: Semaphore = Semaphore::const_new(32);

/// No redirects, retries, proxies, permissive TLS or unbounded queue. Every call
/// resolves and screens all addresses through the shared transport.
#[derive(Clone, Copy)]
pub struct Transport {
    allow_private_targets: bool,
}

impl Transport {
    pub const fn new(allow_private_targets: bool) -> Self {
        Self {
            allow_private_targets,
        }
    }
    pub const fn allow_private_targets(self) -> bool {
        self.allow_private_targets
    }

    pub async fn send(self, request: HttpRequest) -> Result<HttpResponse, ProtocolError> {
        let _permit = REQUESTS
            .try_acquire()
            .map_err(|_| ProtocolError::Unavailable)?;
        let method = match *request.method() {
            http::Method::GET => OutboundMethod::Get,
            http::Method::POST => OutboundMethod::Post,
            http::Method::DELETE => OutboundMethod::Delete,
            _ => return Err(ProtocolError::Configuration),
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
                    value.to_str().map_err(|_| ProtocolError::Configuration)?,
                )
                .map_err(|_| ProtocolError::Configuration)?;
        }
        let url = request.uri().to_string();
        let body =
            String::from_utf8(request.into_body()).map_err(|_| ProtocolError::Configuration)?;
        self.send_to(method, headers, body, url).await
    }

    async fn send_to(
        self,
        method: OutboundMethod,
        headers: OutboundHeaders,
        body: String,
        url: String,
    ) -> Result<HttpResponse, ProtocolError> {
        let response = OutboundRequest::new(method, url, Duration::from_secs(5))
            .headers(headers)
            .body((!body.is_empty()).then_some(body))
            .max_response_bytes(RESPONSE_LIMIT)
            .require_complete_body()
            .allow_private_targets(self.allow_private_targets)
            .send()
            .await
            .map_err(|error| match error {
                OutboundHttpError::ResponseRead => ProtocolError::InvalidResponse,
                _ => ProtocolError::Unavailable,
            })?;
        if response.status_code() >= 500 || response.status_code() == 429 {
            return Err(ProtocolError::Unavailable);
        }
        if (300..400).contains(&response.status_code()) {
            return Err(ProtocolError::InvalidResponse);
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
            .map_err(|_| ProtocolError::InvalidResponse)
    }

    pub async fn json(self, url: &str) -> Result<serde_json::Value, ProtocolError> {
        let request = http::Request::builder()
            .uri(url)
            .header(http::header::ACCEPT, "application/json")
            .body(Vec::new())
            .map_err(|_| ProtocolError::Configuration)?;
        let response = self.send(request).await?;
        if response.status() != http::StatusCode::OK {
            return Err(ProtocolError::InvalidResponse);
        }
        serde_json::from_slice(response.body()).map_err(|_| ProtocolError::InvalidResponse)
    }
}
