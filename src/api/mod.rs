use actix_web::middleware::from_fn;
use actix_web::web::{self, PathConfig};

pub mod etag;
pub mod handlers;
pub mod locations;
pub mod openapi;
pub mod response;
pub mod routes;
pub mod v1;

pub fn config(cfg: &mut web::ServiceConfig) {
    // Map path-parameter extraction errors (e.g. an invalid id newtype) to `400` instead of
    // actix's default `404`. Registered here so both the production app and the test harness, which
    // share this `configure`, get the same edge behavior.
    cfg.app_data(PathConfig::default().error_handler(crate::errors::path_error_handler))
        .service(handlers::probes::healthz)
        .service(handlers::probes::readyz)
        // Runtime-managed settings are resolved once per request, so both the
        // production app and test harnesses apply the same request boundary.
        .service(
            web::scope("api/v1")
                .wrap(from_fn(crate::middlewares::apply_runtime_settings))
                .configure(v1::routes::config),
        )
        .service(
            web::scope("api/v0")
                .wrap(from_fn(crate::middlewares::apply_runtime_settings))
                .configure(routes::config),
        );
}
