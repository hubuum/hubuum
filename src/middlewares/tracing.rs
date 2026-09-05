use std::borrow::Cow;
use std::net::IpAddr;
use std::task::{Context, Poll};
use std::time::Instant;

use actix_service::{Service, Transform};
use actix_web::{
    Error, HttpMessage,
    dev::{ServiceRequest, ServiceResponse},
    http::header::{HeaderName, HeaderValue},
};
use futures_util::future::{self, LocalBoxFuture, Ready};
#[cfg(feature = "integration-test-support")]
use tracing::{Dispatch, instrument::WithSubscriber};
use tracing::{Instrument, Level, Span, debug, error, field, info, span, warn};
use uuid::Uuid;

use crate::events::{CorrelationId, RequestProvenance};
use crate::observability::{metrics, tracing as telemetry};

use super::client_allowlist::{ProxyTrust, extract_client_ip};

const CORRELATION_ID: HeaderName = HeaderName::from_static("x-correlation-id");
const REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");
fn correlation_id_from_request(
    req: &ServiceRequest,
) -> Result<Option<CorrelationId>, &'static str> {
    let Some(value) = req.headers().get(&CORRELATION_ID) else {
        return Ok(None);
    };
    let value = value
        .to_str()
        .map_err(|_| "correlation ID must contain visible ASCII characters")?;
    CorrelationId::new(value)
        .map(Some)
        .map_err(|_| "correlation ID must contain 1 to 128 visible ASCII bytes without whitespace")
}

pub(crate) fn record_principal_on_current_span(principal_id: i32) {
    let span = Span::current();
    span.record("principal_id", principal_id);
    span.record("auth.principal.kind", "authenticated");
}

fn elapsed_millis(start_time: Instant) -> u64 {
    start_time
        .elapsed()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

// Middleware factory
#[derive(Clone)]
pub struct TracingMiddleware {
    proxy_trust: ProxyTrust,
    #[cfg(feature = "integration-test-support")]
    capture_dispatch: Option<Dispatch>,
}

impl Default for TracingMiddleware {
    fn default() -> Self {
        Self::new()
    }
}

impl TracingMiddleware {
    pub fn new() -> Self {
        Self {
            proxy_trust: ProxyTrust::peer_only(),
            #[cfg(feature = "integration-test-support")]
            capture_dispatch: None,
        }
    }

    pub fn new_with_trust(proxy_trust: ProxyTrust) -> Self {
        Self {
            proxy_trust,
            #[cfg(feature = "integration-test-support")]
            capture_dispatch: None,
        }
    }

    #[cfg(feature = "integration-test-support")]
    pub(crate) fn new_with_capture_dispatch(capture_dispatch: Dispatch) -> Self {
        Self {
            proxy_trust: ProxyTrust::peer_only(),
            capture_dispatch: Some(capture_dispatch),
        }
    }
}

impl<S, B> Transform<S, ServiceRequest> for TracingMiddleware
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Transform = TracingMiddlewareService<S>;
    type InitError = ();
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        future::ready(Ok(TracingMiddlewareService {
            service,
            proxy_trust: self.proxy_trust.clone(),
            #[cfg(feature = "integration-test-support")]
            capture_dispatch: self.capture_dispatch.clone(),
        }))
    }
}

pub struct TracingMiddlewareService<S> {
    service: S,
    proxy_trust: ProxyTrust,
    #[cfg(feature = "integration-test-support")]
    capture_dispatch: Option<Dispatch>,
}

impl<S, B> Service<ServiceRequest> for TracingMiddlewareService<S>
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
        #[cfg(feature = "integration-test-support")]
        let capture_dispatch = self.capture_dispatch.clone();
        #[cfg(feature = "integration-test-support")]
        let capture_guard = capture_dispatch
            .as_ref()
            .map(tracing::dispatcher::set_default);

        let request_id = Uuid::new_v4();
        let request_id_s = request_id.to_string();

        let (correlation_id, invalid_correlation_reason) = match correlation_id_from_request(&req) {
            Ok(correlation_id) => (correlation_id, None),
            Err(reason) => (None, Some(reason)),
        };
        let method = req.method().to_string();
        let path = req.path().to_string();
        let route = req
            .match_pattern()
            .map(Cow::Owned)
            .unwrap_or_else(|| route_group(&path));
        let client_ip = extract_client_ip(&req, &self.proxy_trust);
        let client_ip_s = client_ip.map(|ip| ip.to_string());
        let span = span!(
            Level::INFO,
            "http.server.request",
            otel.kind = "server",
            http.request.method = method.as_str(),
            http.route = route.as_ref(),
            http.response.status_code = field::Empty,
            client.network.category = client_network_category(client_ip),
            auth.principal.kind = "anonymous",
            request_id = %request_id_s,
            correlation_id = field::Empty,
            principal_id = field::Empty
        );
        let (remote_parent, invalid_trace_reason) =
            match telemetry::extract_remote_parent(req.headers()) {
                Ok(parent) => (parent, None),
                Err(reason) => (None, Some(reason)),
            };
        telemetry::set_remote_parent(&span, remote_parent);
        if let Some(correlation_id) = correlation_id.as_ref() {
            span.record("correlation_id", correlation_id.as_str());
        }
        if let Some(reason) = invalid_correlation_reason {
            span.in_scope(|| {
                tracing::warn!(message = "invalid correlation ID ignored", reason);
            });
        }
        if let Some(reason) = invalid_trace_reason {
            span.in_scope(|| {
                tracing::warn!(message = "invalid trace context ignored", reason);
            });
        }
        let trace_link = telemetry::trace_link_from_span(&span);
        req.extensions_mut().insert(
            RequestProvenance::new_with_client_ip(request_id, correlation_id.clone(), client_ip)
                .with_trace_link(trace_link),
        );

        let start_time = Instant::now();
        let in_flight_guard = metrics::http_request_started_for_route(&route);
        let fut = span.in_scope(|| self.service.call(req));

        let future = Box::pin(
            async move {
                let _in_flight_guard = in_flight_guard;
                let mut res = match fut.await {
                    Ok(res) => res,
                    Err(err) => {
                        let elapsed_time = start_time.elapsed();
                        metrics::http_request_finished(
                            &method,
                            &route,
                            err.as_response_error().status_code().as_u16(),
                            elapsed_time,
                        );
                        let elapsed_ms = elapsed_millis(start_time);
                        let status = err.as_response_error().status_code();
                        let status_code = status.as_u16();
                        Span::current().record("http.response.status_code", status_code);
                        if status.is_server_error() {
                            error!(
                                message = "request complete",
                                method = method.as_str(),
                                path = path.as_str(),
                                status = status_code,
                                client_ip = client_ip_s.as_deref(),
                                elapsed_ms,
                                error = %err,
                            );
                        } else {
                            warn!(
                                message = "request complete",
                                method = method.as_str(),
                                path = path.as_str(),
                                status = status_code,
                                client_ip = client_ip_s.as_deref(),
                                elapsed_ms,
                                error = %err,
                            );
                        }
                        return Err(err);
                    }
                };

                // Add the request ID and correlation ID to the response headers
                res.headers_mut().insert(
                    REQUEST_ID,
                    request_id_s
                        .parse()
                        .unwrap_or_else(|_| HeaderValue::from_static("<failed>")),
                );
                if let Some(correlation_id) = correlation_id {
                    res.headers_mut().insert(
                        CORRELATION_ID,
                        correlation_id
                            .as_str()
                            .parse()
                            .unwrap_or_else(|_| HeaderValue::from_static("<failed>")),
                    );
                }

                let elapsed_time = start_time.elapsed();
                metrics::http_request_finished(
                    &method,
                    &route,
                    res.status().as_u16(),
                    elapsed_time,
                );
                let elapsed_ms = elapsed_millis(start_time);
                let status = res.status();
                let status_code = status.as_u16();
                Span::current().record("http.response.status_code", status_code);
                if status.is_server_error() {
                    error!(
                        message = "request complete",
                        method = method.as_str(),
                        path = path.as_str(),
                        status = status_code,
                        client_ip = client_ip_s.as_deref(),
                        elapsed_ms,
                    );
                } else if status.is_client_error() {
                    warn!(
                        message = "request complete",
                        method = method.as_str(),
                        path = path.as_str(),
                        status = status_code,
                        client_ip = client_ip_s.as_deref(),
                        elapsed_ms,
                    );
                } else if status.is_success() && matches!(path.as_str(), "/healthz" | "/readyz") {
                    debug!(
                        message = "request complete",
                        method = method.as_str(),
                        path = path.as_str(),
                        status = status_code,
                        client_ip = client_ip_s.as_deref(),
                        elapsed_ms,
                    );
                } else {
                    info!(
                        message = "request complete",
                        method = method.as_str(),
                        path = path.as_str(),
                        status = status_code,
                        client_ip = client_ip_s.as_deref(),
                        elapsed_ms,
                    );
                }

                Ok(res)
            }
            .instrument(span),
        );

        #[cfg(feature = "integration-test-support")]
        {
            drop(capture_guard);
            if let Some(capture_dispatch) = capture_dispatch {
                return Box::pin(future.with_subscriber(capture_dispatch));
            }
        }

        future
    }
}

fn client_network_category(client_ip: Option<IpAddr>) -> &'static str {
    match client_ip {
        None => "unknown",
        Some(ip) if ip.is_loopback() => "loopback",
        Some(IpAddr::V4(ip)) if ip.is_private() || ip.is_link_local() => "private",
        Some(IpAddr::V6(ip))
            if ip.is_unique_local() || ip.is_unicast_link_local() || ip.is_unspecified() =>
        {
            "private"
        }
        Some(_) => "public",
    }
}

fn route_group(path: &str) -> Cow<'static, str> {
    match path {
        "/healthz" => Cow::Borrowed("/healthz"),
        "/readyz" => Cow::Borrowed("/readyz"),
        "/metrics" => Cow::Borrowed("/metrics"),
        "/api-doc/openapi.json" => Cow::Borrowed("/api-doc/openapi.json"),
        path if path.starts_with("/api/v1/") => Cow::Borrowed("/api/v1/{route}"),
        path if path.starts_with("/api/v0/") => Cow::Borrowed("/api/v0/{route}"),
        _ => Cow::Borrowed("unknown"),
    }
}
