use actix_web::web;

use crate::api::handlers::meta as meta_handlers;
pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(meta_handlers::get_db_state);
}
