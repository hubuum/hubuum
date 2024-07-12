use crate::db::DbPool;
use crate::errors::ApiError;
use crate::extractors::{AdminAccess, UserAccess};
use crate::models::search::{parse_query_parameter, ParsedQueryParam};
use crate::models::{HubuumClassRelationID, Permissions};

use crate::check_permissions;
use crate::traits::{PermissionController, SelfAccessors};

use crate::utilities::response::json_response;
use tracing::debug;

use crate::traits::Search;

use actix_web::{get, http::StatusCode, patch, post, routes, web, HttpRequest, Responder};

#[routes]
#[get("")]
#[get("/")]
async fn get_class_relations(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = requestor.user;
    let query_string = req.query_string();

    let params = match parse_query_parameter(query_string) {
        Ok(params) => params,
        Err(e) => return Err(e),
    };

    debug!(message = "Listing class relations", user_id = user.id());

    let classes = user.search_class_relations(&pool, params).await?;

    Ok(json_response(classes, StatusCode::OK))
}

#[get("/{relation_id}")]
async fn get_class_relation(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    relation_id: web::Path<HubuumClassRelationID>,
) -> Result<impl Responder, ApiError> {
    let user = requestor.user;
    let relation_id = relation_id.into_inner();

    debug!(
        message = "Getting class relation",
        user_id = user.id(),
        relation_id = ?relation_id,
    );

    let namespaces = relation_id.namespace(&pool).await?;
    for namespace in [namespaces.0, namespaces.1] {
        check_permissions!(namespace, pool, user, Permissions::ReadClass);
    }

    let relation = relation_id.instance(&pool).await?;

    Ok(json_response(relation, StatusCode::OK))
}
