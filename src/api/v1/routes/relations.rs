use actix_web::web;

use crate::api::v1::handlers::relations;
pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(relations::get_class_relations)
        .service(relations::get_class_relation)
        .service(relations::create_class_relation)
        .service(relations::delete_class_relation)
        .service(relations::get_object_relations)
        .service(relations::get_object_relation)
        .service(relations::create_object_relation)
        .service(relations::delete_object_relation);
}
