use actix_service::{Service, Transform};

use crate::db::connection::DbPool;
use actix_web::{dev::ServiceRequest, dev::ServiceResponse, Error, HttpMessage};
use futures_util::future::{self, Ready};
use std::task::{Context, Poll};

pub struct DbPoolMiddleware {
    pool: DbPool,
}

impl DbPoolMiddleware {
    pub fn new(pool: DbPool) -> Self {
        DbPoolMiddleware { pool }
    }
}

impl<S, B> Transform<S, ServiceRequest> for DbPoolMiddleware
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    S::Future: 'static,
    B: 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type InitError = ();
    type Transform = DbPoolMiddlewareService<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        future::ready(Ok(DbPoolMiddlewareService {
            service,
            pool: self.pool.clone(),
        }))
    }
}

pub struct DbPoolMiddlewareService<S> {
    pub service: S,
    pub pool: DbPool,
}

impl<S, B> Service<ServiceRequest> for DbPoolMiddlewareService<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    B: 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&self, req: ServiceRequest) -> Self::Future {
        req.extensions_mut().insert(self.pool.clone());
        self.service.call(req)
    }
}
