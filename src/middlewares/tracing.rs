use actix_service::{Service, Transform};
use actix_web::{
    dev::ServiceRequest,
    dev::ServiceResponse,
    http::header::{HeaderName, HeaderValue},
    Error,
};
use futures_util::future::{self, LocalBoxFuture, Ready};
use std::task::{Context, Poll};
use std::time::Instant;
use tracing::{info, span, Instrument, Level};
use uuid::Uuid;

const CORRELATION_ID: HeaderName = HeaderName::from_static("x-correlation-id");
const REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");

// Middleware factory
pub struct TracingMiddleware;

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
        future::ready(Ok(TracingMiddlewareService { service }))
    }
}

pub struct TracingMiddlewareService<S> {
    service: S,
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

        let start_time = Instant::now();
        info!(request_id = %request_id, correlation_id = ?correlation_id, message = "Request start", method = &method, path = &path);

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
                info!(message = "Request end", method = &method, path = &path, run_time = ?elapsed_time);

                Ok(res)
            }
            .instrument(span),
        )
    }
}
