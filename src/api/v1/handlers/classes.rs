use crate::db::DbPool;
use crate::errors::ApiError;
use crate::extractors::UserAccess;
use crate::models::namespace::NamespaceID;
use crate::utilities::response::{json_response, json_response_created};
use actix_web::delete;
use actix_web::{get, http::StatusCode, patch, post, web, Responder};
use tracing::debug;

use crate::models::traits::user::ClassAccessors;
use crate::models::{HubuumClassID, NewHubuumClass, Permissions, UpdateHubuumClass};
use crate::traits::{CanDelete, CanSave, CanUpdate, PermissionController, SelfAccessors};

// GET /api/v1/classes, list all classes the user may see.
#[get("")]
async fn list_classes(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
) -> Result<impl Responder, ApiError> {
    let user = requestor.user;
    debug!(message = "Listing classes", user_id = user.id());

    let classes = user.classes_read(&pool).await?;
    Ok(json_response(classes, StatusCode::OK))
}

#[post("")]
async fn create_class(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    class_data: web::Json<NewHubuumClass>,
) -> Result<impl Responder, ApiError> {
    let user = requestor.user;
    let class_data = class_data.into_inner();

    debug!(
        message = "Creating class",
        user_id = user.id(),
        class_name = class_data.name
    );

    let namespace_id = NamespaceID(class_data.namespace_id);
    if !namespace_id
        .user_can(&pool, user, Permissions::CreateClass)
        .await?
    {
        return Ok(json_response((), StatusCode::FORBIDDEN));
    }

    let class = class_data.save(&pool).await?;

    Ok(json_response_created(
        &class,
        format!("/api/v1/class/{}", class.id()).as_str(),
    ))
}

#[get("/{class_id}")]
async fn get_class(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    class_id: web::Path<HubuumClassID>,
) -> Result<impl Responder, ApiError> {
    let user = requestor.user;
    let class = class_id.into_inner();

    debug!(
        message = "Getting class",
        user_id = user.id(),
        class_id = class.id()
    );

    let class = class.instance(&pool).await?;
    if !class.user_can(&pool, user, Permissions::ReadClass).await? {
        return Ok(json_response((), StatusCode::FORBIDDEN));
    }

    Ok(json_response(class, StatusCode::OK))
}

#[patch("/{class_id}")]
async fn update_class(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    class_id: web::Path<HubuumClassID>,
    class_data: web::Json<UpdateHubuumClass>,
) -> Result<impl Responder, ApiError> {
    let user = requestor.user;
    let class_id = class_id.into_inner();
    let class_data = class_data.into_inner();

    debug!(
        message = "Updating class",
        user_id = user.id(),
        class_id = class_id.id()
    );

    let class = class_id.instance(&pool).await?;
    if !class
        .user_can(&pool, user, Permissions::UpdateClass)
        .await?
    {
        return Ok(json_response((), StatusCode::FORBIDDEN));
    }

    let class = class_data.update(&pool, class.id).await?;
    Ok(json_response(class, StatusCode::OK))
}

#[delete("/{class_id}")]
async fn delete_class(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    class_id: web::Path<HubuumClassID>,
) -> Result<impl Responder, ApiError> {
    let user = requestor.user;
    let class_id = class_id.into_inner();

    debug!(
        message = "Deleting class",
        user_id = user.id(),
        class_id = class_id.id()
    );

    let class = class_id.instance(&pool).await?;
    if !class
        .user_can(&pool, user, Permissions::DeleteClass)
        .await?
    {
        return Ok(json_response((), StatusCode::FORBIDDEN));
    }

    class.delete(&pool).await?;
    Ok(json_response((), StatusCode::NO_CONTENT))
}

#[get("/{class_id}/permissions")]
async fn get_class_permissions(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    class_id: web::Path<HubuumClassID>,
) -> Result<impl Responder, ApiError> {
    let user = requestor.user;
    let class_id = class_id.into_inner();

    debug!(
        message = "Getting class permissions",
        user_id = user.id(),
        class_id = class_id.id()
    );

    let class = class_id.instance(&pool).await?;
    if !class.user_can(&pool, user, Permissions::ReadClass).await? {
        return Ok(json_response((), StatusCode::FORBIDDEN));
    }

    // We need a groups_on for class.

    Ok(json_response((), StatusCode::NOT_IMPLEMENTED))
}
