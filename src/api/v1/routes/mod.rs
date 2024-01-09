use actix_web::web;

pub mod iam;
pub mod namespaces;

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(web::scope("/iam").configure(iam::config))
        .service(web::scope("/namespaces").configure(namespaces::config));
}
