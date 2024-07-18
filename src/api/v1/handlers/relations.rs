use crate::db::DbPool;
use crate::errors::ApiError;
use crate::extractors::UserAccess;
use crate::models::search::parse_query_parameter;
use crate::models::{HubuumClassRelationID, Permissions};

use crate::check_permissions;
use crate::traits::{CanDelete, CanSave, NamespaceAccessors, SelfAccessors};

use crate::utilities::response::json_response;
use actix_web::delete;
use tracing::debug;

use crate::traits::Search;

use actix_web::{get, http::StatusCode, routes, web, HttpRequest, Responder};

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
        check_permissions!(namespace, pool, user, Permissions::ReadClassRelation);
    }

    let relation = relation_id.instance(&pool).await?;

    Ok(json_response(relation, StatusCode::OK))
}

#[routes]
#[post("")]
#[post("/")]
async fn create_class_relation(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    relation: web::Json<crate::models::NewHubuumClassRelation>,
) -> Result<impl Responder, ApiError> {
    let relation = relation.into_inner();
    let user = requestor.user;

    debug!(
        message = "Creating class relation",
        user_id = user.id(),
        from_class = relation.from_hubuum_class_id,
        to_class = relation.to_hubuum_class_id,
    );

    let namespaces = relation.namespace(&pool).await?;
    for namespace in [namespaces.0, namespaces.1] {
        check_permissions!(namespace, pool, user, Permissions::CreateClassRelation);
    }

    let relation = relation.save(&pool).await?;

    Ok(json_response(relation, StatusCode::CREATED))
}

#[delete("/{relation_id}")]
async fn delete_class_relation(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    relation_id: web::Path<HubuumClassRelationID>,
) -> Result<impl Responder, ApiError> {
    let user = requestor.user;
    let relation_id = relation_id.into_inner();

    debug!(
        message = "Deleting class relation",
        user_id = user.id(),
        relation_id = ?relation_id,
    );

    let namespaces = relation_id.namespace(&pool).await?;
    for namespace in [namespaces.0, namespaces.1] {
        check_permissions!(namespace, pool, user, Permissions::DeleteClassRelation);
    }

    relation_id.delete(&pool).await?;

    Ok(json_response("{}", StatusCode::NO_CONTENT))
}
