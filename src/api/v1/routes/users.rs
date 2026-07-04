use actix_web::web;

use crate::api::v1::handlers::{events, users};
pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(users::create_user)
        .service(users::get_users)
        .service(users::get_user)
        .service(events::get_user_events)
        .service(users::update_user)
        .service(users::delete_user)
        .service(users::anonymize_user);
}
