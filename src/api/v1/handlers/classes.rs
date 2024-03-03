use actix_web::{delete, HttpRequest};
use actix_web::{get, http::StatusCode, patch, post, web, Responder};
use tracing::debug;

use crate::check_permissions;
use crate::db::DbPool;
use crate::errors::ApiError;
use crate::extractors::UserAccess;
use crate::utilities::response::{json_response, json_response_created};

use crate::models::traits::user::SearchClasses;
use crate::models::{HubuumClassID, NamespaceID, NewHubuumClass, Permissions, UpdateHubuumClass};
use crate::traits::{CanDelete, CanSave, CanUpdate, PermissionController, SelfAccessors};

use crate::models::search::parse_query_parameter;

// GET /api/v1/classes, list all classes the user may see.
#[get("")]
async fn get_classes(
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

    debug!(message = "Listing classes", user_id = user.id());

    let classes = user.search_classes(&pool, params).await?;

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

    let class = class_data.save(&pool).await?;
    check_permissions!(class, pool, user, Permissions::CreateClass);

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
    check_permissions!(class, pool, user, Permissions::ReadClass);

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
    check_permissions!(class, pool, user, Permissions::UpdateClass);

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
    check_permissions!(class, pool, user, Permissions::DeleteClass);

    class.delete(&pool).await?;
    Ok(json_response((), StatusCode::NO_CONTENT))
}

#[get("/{class_id}/permissions")]
async fn get_class_permissions(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    class_id: web::Path<HubuumClassID>,
) -> Result<impl Responder, ApiError> {
    use crate::models::groups_on;
    use crate::traits::NamespaceAccessors;

    let user = requestor.user;
    let class_id = class_id.into_inner();

    debug!(
        message = "Getting class permissions",
        user_id = user.id(),
        class_id = class_id.id()
    );

    let class = class_id.instance(&pool).await?;
    check_permissions!(class, pool, user, Permissions::ReadClass);

    let nid = class.namespace_id(&pool).await?;
    let permissions = groups_on(
        &pool,
        NamespaceID(nid),
        vec![
            Permissions::CreateClass,
            Permissions::UpdateClass,
            Permissions::ReadClass,
            Permissions::DeleteClass,
        ],
    )
    .await?;

    Ok(json_response(permissions, StatusCode::OK))
}
