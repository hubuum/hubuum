use actix_web::web;

use crate::api::v1::handlers::classes;
pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(classes::get_classes)
        .service(classes::get_class)
        .service(classes::create_class)
        .service(classes::update_class)
        .service(classes::delete_class)
        .service(classes::get_class_permissions);
}
