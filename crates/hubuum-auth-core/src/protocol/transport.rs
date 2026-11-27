use std::time::Duration;

use hubuum_domain::OAuthProfile;
use hubuum_outbound_http::{OutboundHeaders, OutboundHttpError, OutboundMethod, OutboundRequest};
use oauth2::{HttpRequest, HttpResponse, http};
use tokio::sync::Semaphore;

use super::{ProtocolError, profile};

const RESPONSE_LIMIT: usize = 128 * 1024;
static REQUESTS: Semaphore = Semaphore::const_new(32);

fn temporary_status(status: u16) -> bool {
    matches!(status, 408 | 429 | 500..)
}

fn transport_error(error: OutboundHttpError) -> ProtocolError {
    match error {
        OutboundHttpError::InvalidResponseBody { status } if !temporary_status(status) => {
            ProtocolError::InvalidResponse
        }
        _ => ProtocolError::Unavailable,
    }
}

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
        self.send_with_dispatch(request, || true).await
    }

    pub async fn send_with_dispatch(
        self,
        request: HttpRequest,
        dispatch: impl FnOnce() -> bool,
    ) -> Result<HttpResponse, ProtocolError> {
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
        let skip_get_body = get_body_policy(request.extensions().get::<OAuthProfile>());
        let url = request.uri().to_string();
        let body =
            String::from_utf8(request.into_body()).map_err(|_| ProtocolError::Configuration)?;
        let request = OutboundRequest::new(method, url, Duration::from_secs(5))
            .headers(headers)
            .body((!body.is_empty()).then_some(body))
            .max_response_bytes(RESPONSE_LIMIT)
            .require_complete_body()
            .allow_private_targets(self.allow_private_targets);
        let request = if method == OutboundMethod::Get {
            request.skip_body_when(skip_get_body)
        } else {
            request
        };
        let response = request
            .send_with_dispatch(dispatch)
            .await
            .map_err(transport_error)?;
        Self::response(
            response.status_code(),
            response.headers(),
            response.body_preview().as_bytes().to_vec(),
        )
    }

    fn response(
        status: u16,
        headers: &serde_json::Value,
        body: Vec<u8>,
    ) -> Result<HttpResponse, ProtocolError> {
        if temporary_status(status) {
            return Err(ProtocolError::Unavailable);
        }
        if (300..400).contains(&status) {
            return Err(ProtocolError::InvalidResponse);
        }
        let mut output = http::Response::builder().status(status);
        // Preserve only protocol metadata, including GitHub's 403 rate-limit
        // signals. Never propagate provider cookies.
        for name in [
            "content-type",
            "retry-after",
            "x-ratelimit-remaining",
            "link",
        ] {
            if let Some(value) = headers.get(name).and_then(serde_json::Value::as_str) {
                output = output.header(name, value);
            }
        }
        output
            .body(body)
            .map_err(|_| ProtocolError::InvalidResponse)
    }

    pub async fn json(self, url: &str) -> Result<serde_json::Value, ProtocolError> {
        let request = http::Request::builder()
            .uri(url)
            .header(http::header::ACCEPT, "application/json")
            .body(Vec::new())
            .map_err(|_| ProtocolError::Configuration)?;
        Self::json_document(self.send(request).await?)
    }

    fn json_document(response: HttpResponse) -> Result<serde_json::Value, ProtocolError> {
        if response.status() != http::StatusCode::OK {
            return Err(ProtocolError::InvalidResponse);
        }
        profile::parse(response.body())
    }
}

fn get_body_policy(profile: Option<&OAuthProfile>) -> fn(u16, &serde_json::Value) -> bool {
    match profile {
        Some(OAuthProfile::Github) => github_profile_status_decided_by_headers,
        _ => |status, _| status == 401,
    }
}

fn github_profile_status_decided_by_headers(status: u16, headers: &serde_json::Value) -> bool {
    // Preserve a definitive bearer rejection or explicit rate-limit signal even
    // when the error page is oversized or unreadable. Unmarked 403s still need
    // their body for GitHub's secondary-rate-limit message.
    status == 401
        || status == 403
            && (headers
                .get("x-ratelimit-remaining")
                .and_then(serde_json::Value::as_str)
                == Some("0")
                || headers.get("retry-after").is_some())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signing_key_documents_reject_duplicate_key_identifiers() {
        let reply = HttpResponse::new(br#"{"keys":[{"kid":"a","kid":"b"}]}"#.to_vec());
        assert_eq!(
            Transport::json_document(reply),
            Err(ProtocolError::InvalidResponse)
        );
    }

    #[rstest::rstest]
    #[case::github_quota(Some(OAuthProfile::Github), 403, serde_json::json!({"x-ratelimit-remaining":"0"}), true)]
    #[case::github_retry(Some(OAuthProfile::Github), 403, serde_json::json!({"retry-after":"60"}), true)]
    #[case::github_unmarked(Some(OAuthProfile::Github), 403, serde_json::json!({}), false)]
    #[case::generic_retry(Some(OAuthProfile::JsonUser), 403, serde_json::json!({"retry-after":"60"}), false)]
    #[case::oidc_quota(None, 403, serde_json::json!({"x-ratelimit-remaining":"0"}), false)]
    #[case::github_unauthorized(Some(OAuthProfile::Github), 401, serde_json::json!({}), true)]
    #[case::generic_unauthorized(Some(OAuthProfile::JsonUser), 401, serde_json::json!({}), true)]
    #[case::oidc_unauthorized(None, 401, serde_json::json!({}), true)]
    fn get_body_is_skipped_only_for_definitive_headers(
        #[case] profile: Option<OAuthProfile>,
        #[case] status: u16,
        #[case] headers: serde_json::Value,
        #[case] expected: bool,
    ) {
        let skip = get_body_policy(profile.as_ref());
        assert_eq!(skip(status, &headers), expected);
    }

    #[test]
    fn provider_request_timeout_is_temporary() {
        assert_eq!(
            Transport::response(408, &serde_json::Value::Null, Vec::new()).unwrap_err(),
            ProtocolError::Unavailable
        );
    }

    #[test]
    fn unreadable_provider_timeout_is_temporary() {
        assert_eq!(
            transport_error(OutboundHttpError::InvalidResponseBody { status: 408 }),
            ProtocolError::Unavailable
        );
    }

    #[test]
    fn rate_limit_metadata_survives_transport_without_provider_cookies() {
        let response = Transport::response(
            403,
            &serde_json::json!({
                "content-type": "application/json",
                "retry-after": "60",
                "x-ratelimit-remaining": "0",
                "link": "</groups?page=2>; rel=next",
                "set-cookie": "provider-session=secret"
            }),
            Vec::new(),
        )
        .unwrap();
        let headers = response.headers();
        assert_eq!(headers.len(), 4);
        assert_eq!(headers["link"], "</groups?page=2>; rel=next");
        assert_eq!(headers["content-type"], "application/json");
        assert_eq!(headers["retry-after"], "60");
        assert_eq!(headers["x-ratelimit-remaining"], "0");
    }
}
