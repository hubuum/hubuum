use actix_service::{Service, Transform};
use actix_web::{
    Error,
    dev::ServiceRequest,
    dev::ServiceResponse,
    http::header::{HeaderName, HeaderValue},
};
use futures_util::future::{self, LocalBoxFuture, Ready};
use std::task::{Context, Poll};
use std::time::Instant;
use tracing::{Instrument, Level, info, span};
use uuid::Uuid;

use super::client_allowlist::extract_client_ip;

const CORRELATION_ID: HeaderName = HeaderName::from_static("x-correlation-id");
const REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");

// Middleware factory
#[derive(Clone)]
pub struct TracingMiddleware {
    trust_ip_headers: bool,
}

impl Default for TracingMiddleware {
    fn default() -> Self {
        Self::new()
    }
}

impl TracingMiddleware {
    pub fn new() -> Self {
        Self {
            trust_ip_headers: true,
        }
    }

    pub fn new_with_trust(trust_ip_headers: bool) -> Self {
        Self { trust_ip_headers }
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
            trust_ip_headers: self.trust_ip_headers,
        }))
    }
}

pub struct TracingMiddlewareService<S> {
    service: S,
    trust_ip_headers: bool,
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
        let request_id = Uuid::new_v4().to_string(); // Generate a new UUID

        // Extract the correlation ID from the request headers, could be None
        let correlation_id = req
            .headers()
            .get(&CORRELATION_ID)
            .and_then(|hv| hv.to_str().ok())
            .map(str::to_string);

        let span = span!(Level::INFO, "request", request_id = %request_id, correlation_id = ?correlation_id);

        let method = req.method().to_string();
        let path = req.path().to_string();
        let client_ip = extract_client_ip(&req, self.trust_ip_headers);
        let client_ip_s = client_ip.map(|ip| ip.to_string());

        let start_time = Instant::now();
        info!(request_id = %request_id, correlation_id = ?correlation_id, message = "Request start", method = &method, path = &path, client_ip = client_ip_s.as_deref());

        let fut = self.service.call(req);

        Box::pin(
            async move {
                let mut res = fut.await?;

                // Add the request ID and correlation ID to the response headers
                res.headers_mut().insert(REQUEST_ID, request_id.parse().unwrap_or_else(|_| HeaderValue::from_static("<failed>")));
                if let Some(correlation_id) = correlation_id {
                    res.headers_mut().insert(
                        CORRELATION_ID,
                        correlation_id.parse().unwrap_or_else(|_| HeaderValue::from_static("<failed>")),
                    );
                }

                let elapsed_time = start_time.elapsed();
                info!(message = "Request end", method = &method, path = &path, client_ip = client_ip_s.as_deref(), run_time = ?elapsed_time);

                Ok(res)
            }
            .instrument(span),
        )
    }
}
