use actix_web::web;

use crate::api::v1::handlers::restores;

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(restores::create_restore_stage)
        .service(restores::confirm_restore_stage)
        .service(restores::get_restore_status);
}
