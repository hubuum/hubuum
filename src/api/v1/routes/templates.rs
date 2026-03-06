use actix_web::web;

use crate::api::v1::handlers::templates;

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(templates::get_templates)
        .service(templates::create_template)
        .service(templates::get_template)
        .service(templates::patch_template)
        .service(templates::delete_template);
}
