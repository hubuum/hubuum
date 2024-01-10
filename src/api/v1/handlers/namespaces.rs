use crate::errors::ApiError;
use actix_web::{delete, get, http::StatusCode, patch, post, web, Responder};

use crate::models::namespace::{NamespaceID, NewNamespaceRequest, UpdateNamespace};
use crate::models::permissions::{user_can_on_any, NamespacePermissions};
use crate::models::user::UserID;

use serde_json::json;

use crate::utilities::response::{json_response, json_response_created};

use crate::db::connection::DbPool;

use crate::extractors::{AdminAccess, UserAccess};

use tracing::debug;

#[get("")]
pub async fn get_namespaces(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
) -> Result<impl Responder, ApiError> {
    debug!(
        message = "Namespace list requested",
        requestor = requestor.user.username
    );

    let result = user_can_on_any(&pool, UserID(requestor.user.id), NamespacePermissions::Read)?;
    Ok(json_response(result, StatusCode::OK))
}

#[post("")]
pub async fn create_namespace(
    pool: web::Data<DbPool>,
    new_namespace_request: web::Json<NewNamespaceRequest>,
    requestor: AdminAccess,
) -> Result<impl Responder, ApiError> {
    let new_namespace_request = new_namespace_request.into_inner();
    debug!(
        message = "Namespace create requested",
        requestor = requestor.user.id,
        new_namespace = new_namespace_request.name
    );

    let created_namespace = new_namespace_request.save_and_grant_all(&pool)?;

    Ok(json_response_created(
        format!("/api/v1/namespaces/{}", created_namespace.id).as_str(),
    ))
}

#[get("/{namespace_id}")]
pub async fn get_namespace(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    namespace_id: web::Path<NamespaceID>,
) -> Result<impl Responder, ApiError> {
    debug!(
        message = "Namespace get requested",
        requestor = requestor.user.username,
        namespace_id = namespace_id.0
    );

    let namespace =
        namespace_id.user_can(&pool, UserID(requestor.user.id), NamespacePermissions::Read)?;

    Ok(json_response(namespace, StatusCode::OK))
}

#[patch("/{namespace_id}")]
pub async fn update_namespace(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    namespace_id: web::Path<NamespaceID>,
    update_data: web::Json<UpdateNamespace>,
) -> Result<impl Responder, ApiError> {
    debug!(
        message = "Namespace update requested",
        requestor = requestor.user.username,
        namespace_id = namespace_id.0
    );

    let namespace = namespace_id.user_can(
        &pool,
        UserID(requestor.user.id),
        NamespacePermissions::Update,
    )?;

    let updated_namespace = namespace.update(&pool, update_data.into_inner())?;

    Ok(json_response(updated_namespace, StatusCode::ACCEPTED))
}

#[delete("/{namespace_id}")]
pub async fn delete_namespace(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    namespace_id: web::Path<NamespaceID>,
) -> Result<impl Responder, ApiError> {
    debug!(
        message = "Namespace delete requested",
        requestor = requestor.user.username,
        namespace_id = namespace_id.0
    );

    let namespace = namespace_id.user_can(
        &pool,
        UserID(requestor.user.id),
        NamespacePermissions::Delete,
    )?;

    let delete_result = namespace.delete(&pool);

    match delete_result {
        Ok(elements) => Ok(json_response(json!(elements), StatusCode::NO_CONTENT)),
        Err(e) => Err(e),
    }
}
