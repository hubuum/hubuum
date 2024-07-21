use actix_web::web;

use crate::api::v1::handlers::classes;
pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(classes::get_classes)
        .service(classes::get_class)
        .service(classes::create_class)
        .service(classes::update_class)
        .service(classes::delete_class)
        .service(classes::get_class_permissions)
        .service(classes::get_object_in_class)
        .service(classes::get_objects_in_class)
        .service(classes::patch_object_in_class)
        .service(classes::delete_object_in_class)
        .service(classes::create_object_in_class)
        .service(classes::get_class_relations)
        .service(classes::get_class_relations_transitive);
}
