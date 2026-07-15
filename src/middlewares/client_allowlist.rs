use actix_service::{Service, Transform};
use actix_web::{
    Error, HttpRequest, HttpResponse, body::EitherBody, dev::ServiceRequest, dev::ServiceResponse,
    http::header::HeaderMap,
};
use futures_util::future::{self, LocalBoxFuture, Ready};
use ipnet::IpNet;
use std::net::{IpAddr, SocketAddr};
use std::sync::Once;
use std::task::{Context, Poll};
use tracing::warn;

use crate::config::ClientAllowlist;

/// Policy for resolving the real client IP from a request that may have traversed
/// reverse proxies.
///
/// Forwarded headers (`X-Forwarded-For`) are attacker-controllable, so they are only
/// honored when `trust_headers` is set AND a trust mechanism is configured: either a
/// `trusted_proxies` allowlist (preferred) or a `hops` count. The client IP is resolved
/// from the *right* of the `[X-Forwarded-For..., peer]` hop chain, i.e. from the
/// connection peer inward, which is the part an attacker cannot forge.
#[derive(Clone, Debug, Default)]
pub struct ProxyTrust {
    trust_headers: bool,
    trusted_proxies: Vec<IpNet>,
    hops: usize,
}

impl ProxyTrust {
    /// Use the connection peer address only; never trust forwarded headers.
    pub fn peer_only() -> Self {
        Self::default()
    }

    pub fn new(trust_headers: bool, trusted_proxies: Vec<IpNet>, hops: usize) -> Self {
        Self {
            trust_headers,
            trusted_proxies,
            hops,
        }
    }
}

/// Middleware for enforcing client IP allowlist
#[derive(Clone)]
pub struct ClientAllowlistMiddleware {
    allowlist: ClientAllowlist,
    proxy_trust: ProxyTrust,
}

impl ClientAllowlistMiddleware {
    pub fn new(allowlist: ClientAllowlist) -> Self {
        Self {
            allowlist,
            proxy_trust: ProxyTrust::peer_only(),
        }
    }

    pub fn new_with_trust(allowlist: ClientAllowlist, proxy_trust: ProxyTrust) -> Self {
        Self {
            allowlist,
            proxy_trust,
        }
    }
}

impl<S, B> Transform<S, ServiceRequest> for ClientAllowlistMiddleware
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<EitherBody<B>>;
    type Error = Error;
    type Transform = ClientAllowlistMiddlewareService<S>;
    type InitError = ();
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        future::ready(Ok(ClientAllowlistMiddlewareService {
            service,
            allowlist: self.allowlist.clone(),
            proxy_trust: self.proxy_trust.clone(),
        }))
    }
}

pub struct ClientAllowlistMiddlewareService<S> {
    service: S,
    allowlist: ClientAllowlist,
    proxy_trust: ProxyTrust,
}

impl<S, B> Service<ServiceRequest> for ClientAllowlistMiddlewareService<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    B: 'static,
{
    type Response = ServiceResponse<EitherBody<B>>;
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&self, req: ServiceRequest) -> Self::Future {
        if is_probe_path(req.path()) {
            let fut = self.service.call(req);
            return Box::pin(async move { fut.await.map(ServiceResponse::map_into_left_body) });
        }

        let client_ip = extract_client_ip(&req, &self.proxy_trust);

        match client_ip {
            Some(ip) if self.allowlist.allows(ip) => {
                let fut = self.service.call(req);
                Box::pin(async move { fut.await.map(ServiceResponse::map_into_left_body) })
            }
            Some(ip) => {
                crate::observability::metrics::client_allowlist_rejected("disallowed_ip");
                warn!(message = "Rejected request from disallowed IP", client_ip = %ip);
                let response = req
                    .into_response(HttpResponse::Forbidden().body("Client not allowed"))
                    .map_into_right_body();
                Box::pin(async { Ok(response) })
            }
            None => {
                crate::observability::metrics::client_allowlist_rejected("missing_ip");
                warn!(message = "Rejected request with missing client IP");
                let response = req
                    .into_response(HttpResponse::Forbidden().body("Client not allowed"))
                    .map_into_right_body();
                Box::pin(async { Ok(response) })
            }
        }
    }
}

fn is_probe_path(path: &str) -> bool {
    matches!(path, "/healthz" | "/readyz")
}

/// Extract the client IP from the request
pub fn extract_client_ip(req: &ServiceRequest, policy: &ProxyTrust) -> Option<IpAddr> {
    resolve_client_ip(
        req.peer_addr().map(|addr| addr.ip()),
        &collect_forwarded_for(req.headers()),
        policy,
    )
}

pub fn extract_client_ip_from_http_request(
    req: &HttpRequest,
    policy: &ProxyTrust,
) -> Option<IpAddr> {
    resolve_client_ip(
        req.peer_addr().map(|addr| addr.ip()),
        &collect_forwarded_for(req.headers()),
        policy,
    )
}

/// Collect the ordered `X-Forwarded-For` chain as written by the proxies, left to right
/// (claimed client first, closest proxy last). Unparseable tokens are dropped.
fn collect_forwarded_for(headers: &HeaderMap) -> Vec<IpAddr> {
    headers
        .get_all("x-forwarded-for")
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(parse_ip_token)
        .collect()
}

/// Resolve the trustworthy client IP from the hop chain.
///
/// The full chain ordered from most to least trustworthy is `[peer, xff reversed...]`:
/// the connection peer is the only address we observe directly, and each forwarded entry
/// further from it is more attacker-controllable. We therefore resolve from that
/// trustworthy end:
///
/// * With a `trusted_proxies` allowlist, skip leading hops that are known proxies and
///   take the first untrusted hop as the client.
/// * Otherwise, with a `hops` count, skip that many proxy hops from the trustworthy end.
/// * If headers are trusted but neither mechanism is configured, forwarded values cannot
///   be trusted safely, so fall back to the peer address.
fn resolve_client_ip(
    peer: Option<IpAddr>,
    forwarded_for: &[IpAddr],
    policy: &ProxyTrust,
) -> Option<IpAddr> {
    if !policy.trust_headers {
        return peer;
    }

    // Forwarded headers are only meaningful relative to the connection peer: it is the one
    // hop we observe directly and the anchor from which trusted proxies are skipped. With
    // no peer the entire chain is attacker-controlled, so fail closed.
    let peer = peer?;

    // Chain from most trustworthy (peer / closest proxy) to least (claimed client).
    let mut chain: Vec<IpAddr> = Vec::with_capacity(forwarded_for.len() + 1);
    chain.push(peer);
    chain.extend(forwarded_for.iter().rev().copied());

    if !policy.trusted_proxies.is_empty() {
        if let Some(client) = chain
            .iter()
            .find(|ip| !ip_in_nets(**ip, &policy.trusted_proxies))
        {
            return Some(*client);
        }
        // Every hop is a trusted proxy; treat the furthest (claimed client) as client.
        return chain.last().copied();
    }

    if policy.hops > 0 {
        let index = policy.hops.min(chain.len() - 1);
        return Some(chain[index]);
    }

    // trust_headers is set but no trust mechanism is configured: ignore forwarded
    // headers and use the connection peer so spoofed values cannot take effect. This is
    // a static misconfiguration, so warn once per process rather than on every request to
    // avoid flooding logs under load.
    static WARN_UNCONFIGURED_TRUST: Once = Once::new();
    WARN_UNCONFIGURED_TRUST.call_once(|| {
        warn!(
            message = "trust_ip_headers is enabled but neither trusted_proxies nor trusted_proxy_hops is set; ignoring forwarded headers and using peer address"
        );
    });
    Some(peer)
}

/// Whether the given IP falls inside any of the provided networks.
fn ip_in_nets(ip: IpAddr, nets: &[IpNet]) -> bool {
    nets.iter().any(|net| match (net, ip) {
        (IpNet::V4(net), IpAddr::V4(addr)) => net.contains(&addr),
        (IpNet::V6(net), IpAddr::V6(addr)) => net.contains(&addr),
        _ => false,
    })
}

/// Parse one forwarded-for token into an IP address, tolerating surrounding whitespace,
/// `host:port` forms, and bracketed IPv6.
fn parse_ip_token(raw: &str) -> Option<IpAddr> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }
    raw.parse::<IpAddr>().ok().or_else(|| parse_socket(raw))
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

#[cfg(test)]
mod resolve_tests {
    use super::*;
    use std::str::FromStr;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    fn xff(entries: &[&str]) -> Vec<IpAddr> {
        entries.iter().map(|e| ip(e)).collect()
    }

    fn proxies(cidrs: &[&str]) -> Vec<IpNet> {
        cidrs.iter().map(|c| IpNet::from_str(c).unwrap()).collect()
    }

    #[test]
    fn peer_only_ignores_forwarded_headers() {
        let policy = ProxyTrust::peer_only();
        let client = resolve_client_ip(Some(ip("203.0.113.7")), &xff(&["10.0.0.42"]), &policy);
        assert_eq!(client, Some(ip("203.0.113.7")));
    }

    #[test]
    fn allowlist_skips_trusted_proxy_and_takes_client() {
        let policy = ProxyTrust {
            trust_headers: true,
            trusted_proxies: proxies(&["203.0.113.0/24"]),
            hops: 0,
        };
        // Peer is the trusted proxy; XFF claims the real client.
        let client = resolve_client_ip(Some(ip("203.0.113.7")), &xff(&["198.51.100.9"]), &policy);
        assert_eq!(client, Some(ip("198.51.100.9")));
    }

    #[test]
    fn allowlist_rejects_spoofed_prepended_client() {
        // Attacker sends "X-Forwarded-For: 9.9.9.9"; the trusted proxy appends the real
        // peer, yielding chain [proxy, real_client, spoof]. The spoof must not win.
        let policy = ProxyTrust {
            trust_headers: true,
            trusted_proxies: proxies(&["203.0.113.0/24"]),
            hops: 0,
        };
        let client = resolve_client_ip(
            Some(ip("203.0.113.7")),
            &xff(&["9.9.9.9", "198.51.100.9"]),
            &policy,
        );
        assert_eq!(client, Some(ip("198.51.100.9")));
    }

    #[test]
    fn rotating_spoofed_xff_cannot_change_resolved_client() {
        let policy = ProxyTrust {
            trust_headers: true,
            trusted_proxies: proxies(&["203.0.113.0/24"]),
            hops: 0,
        };
        let a = resolve_client_ip(
            Some(ip("203.0.113.7")),
            &xff(&["1.2.3.4", "198.51.100.9"]),
            &policy,
        );
        let b = resolve_client_ip(
            Some(ip("203.0.113.7")),
            &xff(&["5.6.7.8", "198.51.100.9"]),
            &policy,
        );
        assert_eq!(a, b);
        assert_eq!(a, Some(ip("198.51.100.9")));
    }

    #[test]
    fn hop_count_skips_configured_hops_from_the_right() {
        let policy = ProxyTrust {
            trust_headers: true,
            trusted_proxies: vec![],
            hops: 1,
        };
        // chain = [peer, 198.51.100.9, spoof]; skip 1 hop -> 198.51.100.9
        let client = resolve_client_ip(
            Some(ip("203.0.113.7")),
            &xff(&["9.9.9.9", "198.51.100.9"]),
            &policy,
        );
        assert_eq!(client, Some(ip("198.51.100.9")));
    }

    #[test]
    fn trust_enabled_with_no_peer_fails_closed() {
        // Without a connection peer to anchor the chain, forwarded headers are fully
        // attacker-controlled and must not resolve a client IP.
        let policy = ProxyTrust {
            trust_headers: true,
            trusted_proxies: proxies(&["203.0.113.0/24"]),
            hops: 0,
        };
        assert_eq!(
            resolve_client_ip(None, &xff(&["198.51.100.9"]), &policy),
            None
        );
    }

    #[test]
    fn trust_enabled_without_mechanism_falls_back_to_peer() {
        let policy = ProxyTrust {
            trust_headers: true,
            trusted_proxies: vec![],
            hops: 0,
        };
        let client = resolve_client_ip(Some(ip("203.0.113.7")), &xff(&["10.0.0.42"]), &policy);
        assert_eq!(client, Some(ip("203.0.113.7")));
    }

    #[test]
    fn parse_ip_token_handles_socket_and_bracketed_forms() {
        assert_eq!(parse_ip_token(" 198.51.100.9 "), Some(ip("198.51.100.9")));
        assert_eq!(parse_ip_token("198.51.100.9:443"), Some(ip("198.51.100.9")));
        assert_eq!(parse_ip_token("[2001:db8::1]:443"), Some(ip("2001:db8::1")));
        assert_eq!(parse_ip_token("not-an-ip"), None);
    }
}
