//! Hardened HTTP execution for webhook deliveries.
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
