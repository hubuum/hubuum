use actix_web::web;

use crate::api::v1::handlers::namespaces as namespaces_handlers;
pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(namespaces_handlers::get_namespaces)
        .service(namespaces_handlers::create_namespace);
}
