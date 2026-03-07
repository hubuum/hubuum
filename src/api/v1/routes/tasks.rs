use actix_web::web;

use crate::api::v1::handlers::tasks;

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(tasks::get_task).service(tasks::get_task_events);
}
