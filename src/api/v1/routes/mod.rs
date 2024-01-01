use actix_web::web;

pub mod iam;

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(web::scope("/iam").configure(iam::config));
}
