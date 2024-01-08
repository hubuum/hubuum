use crate::errors::ApiError;
use actix_web::{delete, get, http::StatusCode, patch, post, web, Responder};

use crate::models::namespace::{user_can_on_all, Namespace, PermissionsForNamespaces};

use crate::utilities::response::{json_response, json_response_created};

use serde_json::json;

use crate::db::connection::DbPool;

use crate::extractors::{AdminAccess, AdminOrSelfAccess, UserAccess};

use tracing::debug;

#[get("/namespaces")]
pub async fn get_namespace(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
) -> Result<impl Responder, ApiError> {
    debug!(
        message = "Namespace list requested",
        requestor = requestor.user.username
    );

    let result = user_can_on_all(&pool, requestor.user.id, PermissionsForNamespaces::Read)?;
    Ok(json_response(result, StatusCode::OK))
}

#[post("/namespaces")]
pub async fn create_namespace(
    pool: web::Data<DbPool>,
    new_namespace: web::Json<Namespace>,
    requestor: AdminAccess,
) -> Result<impl Responder, ApiError> {
    debug!(
        message = "Namespace create requested",
        requestor = requestor.user.id,
        new_namespace = new_namespace.name.as_str()
    );

    let result = new_namespace.into_inner().save(&pool)?;

    Ok(json_response_created(
        format!("/api/v1/namespaces/{}", result.id).as_str(),
    ))
}
