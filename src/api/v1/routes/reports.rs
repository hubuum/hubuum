use actix_web::web;

use crate::api::v1::handlers::reports;

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(reports::run_report)
        .service(reports::get_report)
        .service(reports::get_report_output);
}
