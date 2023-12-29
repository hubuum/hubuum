use actix_web::web;

pub mod handlers;
pub mod routes;
pub mod v1;

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(web::scope("api/v1").configure(v1::routes::config))
        .service(web::scope("api/v0").configure(routes::config));
}
