use actix_service::{Service, Transform};
use actix_web::{
    Error, HttpMessage,
    dev::ServiceRequest,
    dev::ServiceResponse,
    http::header::{HeaderName, HeaderValue},
};
use futures_util::future::{self, LocalBoxFuture, Ready};
use std::task::{Context, Poll};
use std::time::Instant;
use tracing::{Instrument, Level, Span, error, field, info, span, warn};
use uuid::Uuid;

use crate::events::RequestProvenance;

use super::client_allowlist::{ProxyTrust, extract_client_ip};

const CORRELATION_ID: HeaderName = HeaderName::from_static("x-correlation-id");
const REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");

pub(crate) fn record_principal_on_current_span(principal_id: i32) {
    Span::current().record("principal", principal_id);
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
        }
    }

    pub fn new_with_trust(proxy_trust: ProxyTrust) -> Self {
        Self { proxy_trust }
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
        }))
    }
}

pub struct TracingMiddlewareService<S> {
    service: S,
    proxy_trust: ProxyTrust,
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
        let request_id = Uuid::new_v4();
        let request_id_s = request_id.to_string();

        // Extract the correlation ID from the request headers, could be None
        let correlation_id = req
            .headers()
            .get(&CORRELATION_ID)
            .and_then(|hv| hv.to_str().ok())
            .map(str::to_string);
        let span = span!(
            Level::INFO,
            "request",
            request_id = %request_id_s,
            correlation_id = field::Empty,
            principal = field::Empty
        );
        if let Some(correlation_id) = correlation_id.as_deref() {
            span.record("correlation_id", correlation_id);
        }

        let method = req.method().to_string();
        let path = req.path().to_string();
        let client_ip = extract_client_ip(&req, &self.proxy_trust);
        let client_ip_s = client_ip.map(|ip| ip.to_string());
        req.extensions_mut()
            .insert(RequestProvenance::new_with_client_ip(
                request_id,
                correlation_id.clone(),
                client_ip,
            ));

        let start_time = Instant::now();
        let fut = self.service.call(req);

        Box::pin(
            async move {
                let mut res = match fut.await {
                    Ok(res) => res,
                    Err(err) => {
                        let elapsed_ms = elapsed_millis(start_time);
                        error!(
                            message = "request complete",
                            method = method.as_str(),
                            path = path.as_str(),
                            client_ip = client_ip_s.as_deref(),
                            elapsed_ms,
                            error = %err,
                        );
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
                            .parse()
                            .unwrap_or_else(|_| HeaderValue::from_static("<failed>")),
                    );
                }

                let elapsed_ms = elapsed_millis(start_time);
                let status = res.status();
                let status_code = status.as_u16();
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
        )
    }
}
