// src/routes/iam.rs

use actix_web::web;

use crate::handlers::iam as iam_handlers;
pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(iam_handlers::create_user)
        .service(iam_handlers::get_users)
        .service(iam_handlers::get_user)
        .service(iam_handlers::update_user)
        .service(iam_handlers::delete_user)
        .service(iam_handlers::create_group)
        .service(iam_handlers::get_group)
        .service(iam_handlers::get_groups)
        .service(iam_handlers::update_group)
        .service(iam_handlers::delete_group);
}
