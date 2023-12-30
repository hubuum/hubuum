pub mod auth;
pub mod meta;

use actix_web::web;

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(web::scope("/auth").configure(auth::config))
        .service(web::scope("/meta").configure(meta::config));
}
