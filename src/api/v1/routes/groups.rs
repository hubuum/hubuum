use actix_web::web;

use crate::api::v1::handlers::groups;
pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(groups::create_group)
        .service(groups::get_group)
        .service(groups::get_groups)
        .service(groups::update_group)
        .service(groups::delete_group)
        .service(groups::get_group_members)
        .service(groups::add_group_member)
        .service(groups::delete_group_member);
}
