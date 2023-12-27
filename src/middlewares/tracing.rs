use actix_service::{Service, Transform};
use actix_web::{dev::ServiceRequest, dev::ServiceResponse, Error};
use futures_util::future::{self, LocalBoxFuture, Ready};
use std::task::{Context, Poll};
use tracing::{info, span, Instrument, Level};
use uuid::Uuid;

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
        let span = span!(Level::INFO, "request", request_id = %request_id);

        let method = req.method().to_string();
        let path = req.path().to_string();

        info!(message = "Request start", request_id = %request_id, method = &method, path = &path);

        let fut = self.service.call(req);

        Box::pin(
            async move {
                let res = fut.await?;
                // Here you can manipulate the response if needed
                info!(message = "Request end", request_id = %request_id, method = &method, path = &path);

                Ok(res)
            }
            .instrument(span),
        )
    }
}
