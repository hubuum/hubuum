use actix_web::web;

use crate::api::v1::handlers::{events, remote_targets};

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(remote_targets::get_remote_targets)
        .service(remote_targets::create_remote_target)
        .service(remote_targets::get_remote_target)
        .service(events::get_remote_target_events)
        .service(remote_targets::patch_remote_target)
        .service(remote_targets::delete_remote_target)
        .service(remote_targets::invoke_remote_target);
}
