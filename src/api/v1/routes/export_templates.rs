use actix_web::web;

use crate::api::v1::handlers::{events, export_templates};

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(export_templates::get_templates)
        .service(export_templates::create_template)
        .service(export_templates::get_template)
        .service(events::get_export_template_events)
        .service(export_templates::get_template_history)
        .service(export_templates::get_template_as_of)
        .service(export_templates::run_template_export)
        .service(export_templates::patch_template)
        .service(export_templates::delete_template);
}
