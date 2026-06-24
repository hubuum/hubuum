//! Hardened outbound HTTP primitives shared by Hubuum integrations.
//!
//! This crate owns the reusable security boundary for outbound calls: HTTPS-only
//! URL validation, embedded credential rejection, DNS resolution with IP
//! screening, resolver pinning, redirect refusal, response body caps, timeout
//! application, and sensitive response-header redaction.
//!
//! It intentionally does not own Hubuum-specific concerns such as auth secret
//! resolution, task/result persistence, API error types, permissions, template
//! rendering, or global app configuration. Callers pass concrete request
//! settings in explicitly and map `OutboundHttpError` into their public error
//! surface.
//!
//! The `dangerous_*` request toggles exist for tightly scoped test/internal
//! callers and should not be enabled from production paths.

use std::fmt;
use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, Instant};

use ipnet::IpNet;
use reqwest::Url;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};

#[derive(Debug)]
pub enum OutboundHttpError {
    InvalidUrl,
    NonHttpsUrl,
    EmbeddedCredentials,
    MissingHost,
    MissingKnownPort,
    DnsResolution { host: String },
    EmptyDnsResolution { host: String },
    DisallowedAddress { host: String, address: IpAddr },
    ClientBuild(String),
    ResponseRead(String),
    Timeout,
    Connect,
    Request(String),
    InvalidHeaderName { name: String },
    InvalidHeaderValue { name: String },
}

impl fmt::Display for OutboundHttpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidUrl => write!(f, "outbound URL is invalid"),
            Self::NonHttpsUrl => write!(f, "outbound URLs must use https"),
            Self::EmbeddedCredentials => {
                write!(f, "outbound URLs must not contain embedded credentials")
            }
            Self::MissingHost => write!(f, "outbound URL is missing a host"),
            Self::MissingKnownPort => write!(f, "outbound URL is missing a known port"),
            Self::DnsResolution { host } => write!(f, "failed to resolve outbound host '{host}'"),
            Self::EmptyDnsResolution { host } => {
                write!(f, "outbound host '{host}' did not resolve to any address")
            }
            Self::DisallowedAddress { host, address } => write!(
                f,
                "outbound host '{host}' resolves to a disallowed address ({address})"
            ),
            Self::ClientBuild(error) => write!(f, "HTTP client error: {error}"),
            Self::ResponseRead(error) => write!(f, "failed reading outbound response: {error}"),
            Self::Timeout => write!(f, "outbound call timed out"),
            Self::Connect => write!(f, "outbound connection failed"),
            Self::Request(error) => write!(f, "outbound call failed: {error}"),
            Self::InvalidHeaderName { name } => write!(f, "invalid outbound header name: {name}"),
            Self::InvalidHeaderValue { name } => {
                write!(f, "invalid outbound header value for {name}")
            }
        }
    }
}

impl std::error::Error for OutboundHttpError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutboundMethod {
    Get,
    Post,
    Patch,
    Delete,
}

impl OutboundMethod {
    fn as_reqwest(self) -> reqwest::Method {
        match self {
            OutboundMethod::Get => reqwest::Method::GET,
            OutboundMethod::Post => reqwest::Method::POST,
            OutboundMethod::Patch => reqwest::Method::PATCH,
            OutboundMethod::Delete => reqwest::Method::DELETE,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundUrlParts {
    url: Url,
    host: String,
    port: u16,
}

impl OutboundUrlParts {
    pub fn url(&self) -> &str {
        self.url.as_str()
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.port
    }
}

pub fn validate_outbound_url(url: &str) -> Result<OutboundUrlParts, OutboundHttpError> {
    if url.is_empty() || url.chars().any(|ch| ch.is_whitespace() || ch.is_control()) {
        return Err(OutboundHttpError::InvalidUrl);
    }

    let parsed = Url::parse(url).map_err(|_| OutboundHttpError::InvalidUrl)?;

    if parsed.scheme() != "https" {
        return Err(OutboundHttpError::NonHttpsUrl);
    }

    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(OutboundHttpError::EmbeddedCredentials);
    }

    let host = parsed
        .host_str()
        .filter(|host| !host.is_empty())
        .ok_or(OutboundHttpError::MissingHost)?
        .to_string();

    let port = parsed
        .port_or_known_default()
        .ok_or(OutboundHttpError::MissingKnownPort)?;

    Ok(OutboundUrlParts {
        url: parsed,
        host,
        port,
    })
}

fn blocked_outbound_nets() -> &'static [IpNet] {
    use std::sync::OnceLock;
    static NETS: OnceLock<Vec<IpNet>> = OnceLock::new();
    NETS.get_or_init(|| {
        [
            "0.0.0.0/8",
            "10.0.0.0/8",
            "100.64.0.0/10",
            "127.0.0.0/8",
            "169.254.0.0/16",
            "172.16.0.0/12",
            "192.0.0.0/24",
            "192.0.2.0/24",
            "192.168.0.0/16",
            "198.18.0.0/15",
            "198.51.100.0/24",
            "203.0.113.0/24",
            "224.0.0.0/4",
            "240.0.0.0/4",
            "255.255.255.255/32",
            "::/128",
            "::1/128",
            "::ffff:0:0/96",
            "64:ff9b::/96",
            "100::/64",
            "2001:db8::/32",
            "fc00::/7",
            "fe80::/10",
            "ff00::/8",
        ]
        .iter()
        .map(|net| net.parse().expect("static blocked net must parse"))
        .collect()
    })
}

pub fn ip_blocked(ip: IpAddr) -> bool {
    let ip = match ip {
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => IpAddr::V4(v4),
            None => IpAddr::V6(v6),
        },
        other => other,
    };

    blocked_outbound_nets().iter().any(|net| match (net, ip) {
        (IpNet::V4(net), IpAddr::V4(addr)) => net.contains(&addr),
        (IpNet::V6(net), IpAddr::V6(addr)) => net.contains(&addr),
        _ => false,
    })
}

pub async fn screen_host(
    host: &str,
    port: u16,
    allow_private_targets: bool,
    dangerous_allow_localhost: bool,
) -> Result<Vec<SocketAddr>, OutboundHttpError> {
    if dangerous_allow_localhost && host.eq_ignore_ascii_case("localhost") {
        return Ok(vec![SocketAddr::from(([127, 0, 0, 1], port))]);
    }

    let addrs: Vec<SocketAddr> = tokio::net::lookup_host((host, port))
        .await
        .map_err(|_| OutboundHttpError::DnsResolution {
            host: host.to_string(),
        })?
        .collect();

    if addrs.is_empty() {
        return Err(OutboundHttpError::EmptyDnsResolution {
            host: host.to_string(),
        });
    }

    if !allow_private_targets && let Some(blocked) = addrs.iter().find(|addr| ip_blocked(addr.ip()))
    {
        return Err(OutboundHttpError::DisallowedAddress {
            host: host.to_string(),
            address: blocked.ip(),
        });
    }

    Ok(addrs)
}

#[derive(Debug, Clone, Default)]
pub struct OutboundHeaders {
    inner: HeaderMap,
}

impl OutboundHeaders {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, name: &str, value: &str) -> Result<(), OutboundHttpError> {
        let header_name = HeaderName::from_bytes(name.as_bytes()).map_err(|_| {
            OutboundHttpError::InvalidHeaderName {
                name: name.to_string(),
            }
        })?;
        let header_value =
            HeaderValue::from_str(value).map_err(|_| OutboundHttpError::InvalidHeaderValue {
                name: name.to_string(),
            })?;
        self.inner.insert(header_name, header_value);
        Ok(())
    }

    fn into_inner(self) -> HeaderMap {
        self.inner
    }
}

pub struct OutboundRequest {
    method: OutboundMethod,
    url: String,
    headers: OutboundHeaders,
    body: Option<String>,
    timeout: Duration,
    max_response_bytes: usize,
    allow_private_targets: bool,
    dangerous_accept_invalid_certs: bool,
    dangerous_allow_localhost: bool,
}

impl OutboundRequest {
    pub fn new(method: OutboundMethod, url: impl Into<String>, timeout: Duration) -> Self {
        Self {
            method,
            url: url.into(),
            headers: OutboundHeaders::new(),
            body: None,
            timeout,
            max_response_bytes: 64 * 1024,
            allow_private_targets: false,
            dangerous_accept_invalid_certs: false,
            dangerous_allow_localhost: false,
        }
    }

    pub fn headers(mut self, headers: OutboundHeaders) -> Self {
        self.headers = headers;
        self
    }

    pub fn body(mut self, body: Option<String>) -> Self {
        self.body = body;
        self
    }

    pub fn max_response_bytes(mut self, max_response_bytes: usize) -> Self {
        self.max_response_bytes = max_response_bytes;
        self
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

    pub async fn send(self) -> Result<OutboundResponse, OutboundHttpError> {
        execute(self).await
    }
}

#[derive(Debug)]
pub struct OutboundResponse {
    status_code: u16,
    status_display: String,
    success: bool,
    headers: serde_json::Value,
    body_preview: String,
    duration_ms: i32,
    url: String,
}

impl OutboundResponse {
    pub fn status_code(&self) -> u16 {
        self.status_code
    }

    pub fn status_display(&self) -> &str {
        &self.status_display
    }

    pub fn is_success(&self) -> bool {
        self.success
    }

    pub fn headers(&self) -> &serde_json::Value {
        &self.headers
    }

    pub fn body_preview(&self) -> &str {
        &self.body_preview
    }

    pub fn duration_ms(&self) -> i32 {
        self.duration_ms
    }

    pub fn url(&self) -> &str {
        &self.url
    }
}

async fn execute(request: OutboundRequest) -> Result<OutboundResponse, OutboundHttpError> {
    let url_parts = validate_outbound_url(&request.url)?;
    let screened_addrs = screen_host(
        &url_parts.host,
        url_parts.port,
        request.allow_private_targets,
        request.dangerous_allow_localhost,
    )
    .await?;

    let client_builder = reqwest::Client::builder()
        .timeout(request.timeout)
        .redirect(reqwest::redirect::Policy::none())
        .resolve_to_addrs(&url_parts.host, &screened_addrs);
    let client_builder = if request.dangerous_accept_invalid_certs {
        client_builder.danger_accept_invalid_certs(true)
    } else {
        client_builder
    };
    let client = client_builder
        .build()
        .map_err(|error| OutboundHttpError::ClientBuild(error.to_string()))?;

    let mut request_builder = client
        .request(request.method.as_reqwest(), url_parts.url.clone())
        .headers(request.headers.into_inner());
    if let Some(body) = request.body {
        request_builder = request_builder.body(body);
    }

    let start = Instant::now();
    let response = request_builder.send().await.map_err(map_reqwest_error)?;
    let status = response.status();
    let headers = headers_to_json(response.headers());
    let body_preview = read_capped_body(response, request.max_response_bytes).await?;
    let duration_ms = i32::try_from(start.elapsed().as_millis()).unwrap_or(i32::MAX);

    Ok(OutboundResponse {
        status_code: status.as_u16(),
        status_display: status.to_string(),
        success: status.is_success(),
        headers,
        body_preview,
        duration_ms,
        url: url_parts.url.to_string(),
    })
}

async fn read_capped_body(
    mut response: reqwest::Response,
    limit: usize,
) -> Result<String, OutboundHttpError> {
    let mut buffer: Vec<u8> = Vec::new();
    while buffer.len() < limit {
        match response
            .chunk()
            .await
            .map_err(|error| OutboundHttpError::ResponseRead(error.to_string()))?
        {
            Some(chunk) => {
                let remaining = limit - buffer.len();
                let take = remaining.min(chunk.len());
                buffer.extend_from_slice(&chunk[..take]);
            }
            None => break,
        }
    }
    Ok(String::from_utf8_lossy(&buffer).replace('\0', "\u{FFFD}"))
}

fn is_sensitive_response_header(name: &str) -> bool {
    const SENSITIVE: [&str; 6] = [
        "set-cookie",
        "set-cookie2",
        "authorization",
        "proxy-authorization",
        "www-authenticate",
        "proxy-authenticate",
    ];
    SENSITIVE.contains(&name.to_ascii_lowercase().as_str())
}

fn headers_to_json(headers: &HeaderMap) -> serde_json::Value {
    let mut object = serde_json::Map::new();
    for (name, value) in headers {
        if is_sensitive_response_header(name.as_str()) {
            object.insert(
                name.to_string(),
                serde_json::Value::String("[redacted]".to_string()),
            );
        } else if let Ok(value) = value.to_str() {
            object.insert(
                name.to_string(),
                serde_json::Value::String(value.to_string()),
            );
        }
    }
    serde_json::Value::Object(object)
}

fn map_reqwest_error(error: reqwest::Error) -> OutboundHttpError {
    if error.is_timeout() {
        OutboundHttpError::Timeout
    } else if error.is_connect() {
        OutboundHttpError::Connect
    } else {
        OutboundHttpError::Request(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outbound_urls_must_be_https() {
        let parts = validate_outbound_url("https://example.com/hook").unwrap();
        assert_eq!(parts.url(), "https://example.com/hook");
        assert_eq!(parts.host, "example.com");
        assert_eq!(parts.port, 443);

        let custom_port = validate_outbound_url("https://example.com:8443/hook").unwrap();
        assert_eq!(custom_port.url(), "https://example.com:8443/hook");
        assert_eq!(custom_port.port, 8443);

        assert!(matches!(
            validate_outbound_url("http://example.com/hook"),
            Err(OutboundHttpError::NonHttpsUrl)
        ));
        assert!(validate_outbound_url("https://").is_err());
        assert!(validate_outbound_url("ftp://example.com").is_err());
        assert!(validate_outbound_url("https://example.com /hook").is_err());
    }

    #[test]
    fn outbound_urls_reject_embedded_credentials() {
        assert!(matches!(
            validate_outbound_url("https://user:pass@example.com/hook"),
            Err(OutboundHttpError::EmbeddedCredentials)
        ));
        assert!(matches!(
            validate_outbound_url("https://user@example.com/hook"),
            Err(OutboundHttpError::EmbeddedCredentials)
        ));
        assert!(matches!(
            validate_outbound_url("https://example.com@127.0.0.1/hook"),
            Err(OutboundHttpError::EmbeddedCredentials)
        ));
    }

    #[test]
    fn private_and_internal_ips_are_blocked() {
        use std::net::{Ipv4Addr, Ipv6Addr};

        assert!(ip_blocked(IpAddr::V4(Ipv4Addr::LOCALHOST)));
        assert!(ip_blocked(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5))));
        assert!(ip_blocked(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1))));
        assert!(ip_blocked(IpAddr::V4(Ipv4Addr::new(169, 254, 169, 254))));
        assert!(ip_blocked(IpAddr::V6(Ipv6Addr::LOCALHOST)));
        assert!(ip_blocked("fd00::1".parse::<IpAddr>().unwrap()));
        assert!(ip_blocked("::ffff:127.0.0.1".parse::<IpAddr>().unwrap()));

        assert!(!ip_blocked(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))));
        assert!(!ip_blocked(
            "2606:4700:4700::1111".parse::<IpAddr>().unwrap()
        ));
    }

    #[test]
    fn sensitive_response_headers_are_redacted() {
        let mut headers = HeaderMap::new();
        headers.insert("set-cookie", "secret=1".parse().unwrap());
        headers.insert("x-request-id", "abc".parse().unwrap());

        let json = headers_to_json(&headers);
        assert_eq!(json["set-cookie"], "[redacted]");
        assert_eq!(json["x-request-id"], "abc");
    }
}
