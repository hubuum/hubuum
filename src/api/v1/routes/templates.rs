use actix_web::web;

use crate::api::v1::handlers::{events, templates};

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(templates::get_templates)
        .service(templates::create_template)
        .service(templates::get_template)
        .service(events::get_report_template_events)
        .service(templates::run_template_report)
        .service(templates::patch_template)
        .service(templates::delete_template);
}
