use actix_web::web;

use crate::api::v1::handlers::relations;
pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(relations::get_class_relations);
}
