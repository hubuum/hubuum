use actix_web::web;

use crate::api::handlers::auth as auth_handlers;
pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(auth_handlers::login)
        .service(auth_handlers::logout)
        .service(auth_handlers::logout_all)
        .service(auth_handlers::logout_other)
        .service(auth_handlers::logout_token);
}
