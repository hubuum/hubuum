use actix_web::web;

use crate::api::v1::handlers::exports;

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(exports::run_export)
        .service(exports::get_export)
        .service(exports::get_export_output);
}
