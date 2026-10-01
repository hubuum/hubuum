//! Shared hardened HTTP execution for event transports.
use hubuum_event_sinks_common::{SinkError, ensure_payload_within_limit};
use hubuum_outbound_http::{
    OutboundHeaders, OutboundMethod, OutboundRequest, OutboundResponse, validate_outbound_url,
};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HttpSinkSettings {
    max_timeout_ms: u64,
    max_response_bytes: usize,
    max_request_bytes: usize,
    allow_private_targets: bool,
    dangerous_accept_invalid_certs: bool,
    dangerous_allow_localhost: bool,
}

impl HttpSinkSettings {
    pub fn new(max_timeout_ms: u64, max_response_bytes: usize) -> Result<Self, SinkError> {
        if max_timeout_ms == 0 {
            return Err(SinkError::new(
                "HTTP sink maximum timeout must be greater than zero",
            ));
        }
        if max_response_bytes == 0 {
            return Err(SinkError::new(
                "HTTP sink maximum response size must be greater than zero",
            ));
        }

        Ok(Self {
            max_timeout_ms,
            max_response_bytes,
            max_request_bytes: max_response_bytes,
            allow_private_targets: false,
            dangerous_accept_invalid_certs: false,
            dangerous_allow_localhost: false,
        })
    }

    pub fn with_max_request_bytes(mut self, max_request_bytes: usize) -> Result<Self, SinkError> {
        if max_request_bytes == 0 {
            return Err(SinkError::new(
                "HTTP sink maximum request size must be greater than zero",
            ));
        }
        self.max_request_bytes = max_request_bytes;
        Ok(self)
    }

    pub fn allow_private_targets(mut self, allow_private_targets: bool) -> Self {
        self.allow_private_targets = allow_private_targets;
        self
    }

    pub fn dangerous_accept_invalid_certs(mut self, dangerous_accept_invalid_certs: bool) -> Self {
        self.dangerous_accept_invalid_certs = dangerous_accept_invalid_certs;
        self
    }

    pub fn dangerous_allow_localhost(mut self, dangerous_allow_localhost: bool) -> Self {
        self.dangerous_allow_localhost = dangerous_allow_localhost;
        self
    }
}

impl HttpSinkSettings {
    pub fn timeout_ms(&self, requested: Option<u64>) -> u64 {
        requested
            .unwrap_or(self.max_timeout_ms)
            .min(self.max_timeout_ms)
    }
    pub fn response_bytes(&self, requested: Option<usize>) -> usize {
        requested
            .unwrap_or(self.max_response_bytes)
            .min(self.max_response_bytes)
    }
    pub fn request_bytes(&self, requested: Option<usize>) -> usize {
        requested
            .unwrap_or(self.max_request_bytes)
            .min(self.max_request_bytes)
    }

    pub async fn post(
        &self,
        url: String,
        headers: OutboundHeaders,
        body: String,
    ) -> Result<OutboundResponse, SinkError> {
        ensure_payload_within_limit("HTTP sink", body.len(), self.max_request_bytes)
            .map_err(|_| SinkError::permanent("HTTP sink request exceeds its size limit"))?;
        OutboundRequest::new(
            OutboundMethod::Post,
            url,
            Duration::from_millis(self.max_timeout_ms),
        )
        .headers(headers)
        .body(Some(body))
        .max_response_bytes(self.max_response_bytes)
        .allow_private_targets(self.allow_private_targets)
        .dangerous_accept_invalid_certs(self.dangerous_accept_invalid_certs)
        .dangerous_allow_localhost(self.dangerous_allow_localhost)
        .send()
        .await
        .map_err(|_| SinkError::new("HTTP sink transport failed"))
    }

    pub fn bounded(
        &self,
        timeout: Option<u64>,
        response_bytes: Option<usize>,
        request_bytes: Option<usize>,
    ) -> Self {
        Self {
            max_timeout_ms: self.timeout_ms(timeout),
            max_response_bytes: self.response_bytes(response_bytes),
            max_request_bytes: self.request_bytes(request_bytes),
            ..*self
        }
    }
}

pub fn validate_url(url: &str) -> Result<(), SinkError> {
    validate_outbound_url(url).map(|_| ()).map_err(|_| {
        SinkError::permanent("Sink URL must be an HTTPS URL without embedded credentials")
    })
}

pub fn json_headers(token: Option<&str>) -> Result<OutboundHeaders, SinkError> {
    let mut headers = OutboundHeaders::new();
    headers
        .insert("content-type", "application/json")
        .map_err(|_| SinkError::permanent("Invalid JSON header"))?;
    if let Some(token) = token {
        headers
            .insert("authorization", &format!("Bearer {token}"))
            .map_err(|_| SinkError::permanent("Bot token is not a valid header"))?;
    }
    Ok(headers)
}

pub fn check_chat_status(response: &OutboundResponse) -> Result<(), SinkError> {
    check_status(
        response.status_code(),
        response
            .headers()
            .get("retry-after")
            .and_then(serde_json::Value::as_str),
    )
}

fn check_status(status: u16, retry_after: Option<&str>) -> Result<(), SinkError> {
    match status {
        429 => {
            let seconds = retry_after
                .and_then(|value| value.parse::<u32>().ok())
                .filter(|value| *value > 0)
                .unwrap_or(60);
            Err(SinkError::rate_limited(Duration::from_secs(u64::from(
                seconds,
            ))))
        }
        200..=299 => Ok(()),
        408 | 500..=599 => Err(SinkError::new(format!(
            "Provider temporarily unavailable (HTTP {})",
            status
        ))),
        status => Err(SinkError::permanent(format!(
            "Provider rejected notification (HTTP {status}); check credentials, destination and payload"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hubuum_event_sinks_common::SinkFailure;
    use rstest::rstest;

    #[rstest]
    #[case(Some("5"), 5)]
    #[case(Some("86401"), 86401)]
    #[case(None, 60)]
    #[case(Some("bad"), 60)]
    #[case(Some("-1"), 60)]
    #[case(Some("0"), 60)]
    fn rate_limit_uses_valid_delta_or_fallback(#[case] header: Option<&str>, #[case] seconds: u64) {
        assert_eq!(
            check_status(429, header).unwrap_err().failure(),
            SinkFailure::RateLimited(Duration::from_secs(seconds))
        );
    }

    #[rstest]
    #[case(400, SinkFailure::Permanent)]
    #[case(401, SinkFailure::Permanent)]
    #[case(403, SinkFailure::Permanent)]
    #[case(408, SinkFailure::Retryable)]
    #[case(500, SinkFailure::Retryable)]
    fn classifies_http_failure(#[case] status: u16, #[case] expected: SinkFailure) {
        assert_eq!(check_status(status, None).unwrap_err().failure(), expected);
    }
}
