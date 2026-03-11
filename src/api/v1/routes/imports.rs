use actix_web::web;

use crate::api::v1::handlers::imports;

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(imports::create_import)
        .service(imports::get_import)
        .service(imports::get_import_results);
}
