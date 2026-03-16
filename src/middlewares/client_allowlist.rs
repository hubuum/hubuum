use actix_service::{Service, Transform};
use actix_web::{
    Error, HttpRequest, dev::ServiceRequest, dev::ServiceResponse, error::ErrorForbidden,
};
use futures_util::future::{self, LocalBoxFuture, Ready};
use std::net::{IpAddr, SocketAddr};
use std::task::{Context, Poll};
use tracing::warn;

use crate::config::ClientAllowlist;

/// Middleware for enforcing client IP allowlist
#[derive(Clone)]
pub struct ClientAllowlistMiddleware {
    allowlist: ClientAllowlist,
    trust_ip_headers: bool,
}

impl ClientAllowlistMiddleware {
    #[allow(dead_code)]
    pub fn new(allowlist: ClientAllowlist) -> Self {
        Self {
            allowlist,
            trust_ip_headers: true,
        }
    }

    pub fn new_with_trust(allowlist: ClientAllowlist, trust_ip_headers: bool) -> Self {
        Self {
            allowlist,
            trust_ip_headers,
        }
    }
}

impl<S, B> Transform<S, ServiceRequest> for ClientAllowlistMiddleware
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Transform = ClientAllowlistMiddlewareService<S>;
    type InitError = ();
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        future::ready(Ok(ClientAllowlistMiddlewareService {
            service,
            allowlist: self.allowlist.clone(),
            trust_ip_headers: self.trust_ip_headers,
        }))
    }
}

pub struct ClientAllowlistMiddlewareService<S> {
    service: S,
    allowlist: ClientAllowlist,
    trust_ip_headers: bool,
}

impl<S, B> Service<ServiceRequest> for ClientAllowlistMiddlewareService<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let allowlist = self.allowlist.clone();
        let trust = self.trust_ip_headers;
        let client_ip = extract_client_ip(&req, trust);
        let fut = self.service.call(req);

        Box::pin(async move {
            match client_ip {
                Some(ip) if allowlist.allows(ip) => fut.await,
                Some(ip) => {
                    warn!(message = "Rejected request from disallowed IP", client_ip = %ip);
                    Err(ErrorForbidden("Client not allowed"))
                }
                None => {
                    warn!(message = "Rejected request with missing client IP");
                    Err(ErrorForbidden("Client not allowed"))
                }
            }
        })
    }
}

/// Extract the client IP from the request
pub fn extract_client_ip(req: &ServiceRequest, trust_headers: bool) -> Option<IpAddr> {
    extract_client_ip_parts(
        req.peer_addr(),
        req.connection_info().realip_remote_addr(),
        trust_headers,
    )
}

pub fn extract_client_ip_from_http_request(
    req: &HttpRequest,
    trust_headers: bool,
) -> Option<IpAddr> {
    extract_client_ip_parts(
        req.peer_addr(),
        req.connection_info().realip_remote_addr(),
        trust_headers,
    )
}

fn extract_client_ip_parts(
    peer_addr: Option<SocketAddr>,
    real_ip_remote_addr: Option<&str>,
    trust_headers: bool,
) -> Option<IpAddr> {
    let header_ip = if trust_headers {
        real_ip_remote_addr
            .and_then(|raw| raw.split(',').next())
            .and_then(|raw| {
                raw.trim()
                    .parse::<IpAddr>()
                    .ok()
                    .or_else(|| parse_socket(raw))
            })
    } else {
        None
    };

    header_ip.or_else(|| peer_addr.map(|addr| addr.ip()))
}

/// Parse a socket address or IPv6 address with brackets
fn parse_socket(raw: &str) -> Option<IpAddr> {
    if let Ok(sa) = raw.parse::<SocketAddr>() {
        return Some(sa.ip());
    }

    raw.trim_start_matches('[')
        .split(']')
        .next()
        .and_then(|ip| ip.parse::<IpAddr>().ok())
}
