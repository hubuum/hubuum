use actix_web::web;

use crate::api::v1::handlers::remote_targets;

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(remote_targets::get_remote_targets)
        .service(remote_targets::create_remote_target)
        .service(remote_targets::get_remote_target)
        .service(remote_targets::get_remote_target_history)
        .service(remote_targets::get_remote_target_as_of)
        .service(remote_targets::patch_remote_target)
        .service(remote_targets::delete_remote_target)
        .service(remote_targets::invoke_remote_target);
}
