use actix_web::web;

use crate::api::v1::handlers::{collections as collections_handlers, events};
pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(collections_handlers::get_collections)
        .service(collections_handlers::create_collection)
        .service(collections_handlers::get_collection)
        .service(events::get_collection_events)
        .service(collections_handlers::get_collection_history)
        .service(collections_handlers::get_collection_as_of)
        .service(collections_handlers::update_collection)
        .service(collections_handlers::delete_collection)
        .service(collections_handlers::get_collection_children)
        .service(collections_handlers::get_collection_ancestors)
        .service(collections_handlers::move_collection_parent)
        .service(collections_handlers::get_collection_permissions)
        .service(collections_handlers::get_collection_group_permissions)
        .service(collections_handlers::get_collection_effective_group_permissions)
        .service(collections_handlers::get_collection_group_permission)
        .service(collections_handlers::get_collection_principal_permissions)
        .service(collections_handlers::get_collection_effective_principal_permissions)
        .service(collections_handlers::get_collection_groups_with_permission)
        .service(collections_handlers::grant_collection_group_permissions)
        .service(collections_handlers::replace_collection_group_permissions)
        .service(collections_handlers::revoke_collection_group_permissions)
        .service(collections_handlers::grant_collection_group_permission)
        .service(collections_handlers::revoke_collection_group_permission);
}
