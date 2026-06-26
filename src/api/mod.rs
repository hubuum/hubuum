use actix_web::web::{self, PathConfig};

pub mod handlers;
pub mod openapi;
pub mod routes;
pub mod v1;

pub fn config(cfg: &mut web::ServiceConfig) {
    // Map path-parameter extraction errors (e.g. an invalid id newtype) to `400` instead of
    // actix's default `404`. Registered here so both the production app and the test harness, which
    // share this `configure`, get the same edge behavior.
    cfg.app_data(PathConfig::default().error_handler(crate::errors::path_error_handler))
        .service(handlers::probes::healthz)
        .service(handlers::probes::readyz)
        .service(web::scope("api/v1").configure(v1::routes::config))
        .service(web::scope("api/v0").configure(routes::config));
}
