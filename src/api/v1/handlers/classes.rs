use actix_web::{delete, HttpRequest};
use actix_web::{get, http::StatusCode, patch, post, routes, web, Responder};

use tracing::debug;

use crate::check_permissions;
use crate::db::DbPool;
use crate::errors::ApiError;
use crate::extractors::UserAccess;
use crate::utilities::response::{json_response, json_response_created};

use crate::models::{
    HubuumClassID, HubuumObjectID, NamespaceID, NewHubuumClass, NewHubuumObject, Permissions,
    UpdateHubuumClass, UpdateHubuumObject,
};
use crate::traits::{CanDelete, CanSave, CanUpdate, Search, SelfAccessors};

use crate::models::search::{parse_query_parameter, ParsedQueryParam};

// GET /api/v1/classes, list all classes the user may see.
#[routes]
#[get("")]
#[get("/")]
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

#[routes]
#[post("")]
#[post("/")]
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

    let namespace = NamespaceID(class_data.namespace_id).instance(&pool).await?;
    check_permissions!(namespace, pool, user, Permissions::CreateClass);
    let class = class_data.save(&pool).await?;

    Ok(json_response_created(
        &class,
        format!("/api/v1/classes/{}", class.id()).as_str(),
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

#[get("/{class_id}/relations/")]
async fn get_class_relations(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    class_id: web::Path<HubuumClassID>,
) -> Result<impl Responder, ApiError> {
    use crate::db::traits::SelfRelations;

    let user = requestor.user;
    let class_id = class_id.into_inner();

    debug!(
        message = "Getting class relations",
        user_id = user.id(),
        class_id = class_id.id()
    );

    let relations = class_id.relations(&pool).await?;
    Ok(json_response(relations, StatusCode::OK))
}

#[get("/{class_id}/relations/transitive/")]
async fn get_class_relations_transitive(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    class_id: web::Path<HubuumClassID>,
) -> Result<impl Responder, ApiError> {
    use crate::db::traits::SelfRelations;

    let user = requestor.user;
    let class_id = class_id.into_inner();

    debug!(
        message = "Getting class relations",
        user_id = user.id(),
        class_id = class_id.id()
    );

    let relations = class_id.transitive_relations(&pool).await?;
    Ok(json_response(relations, StatusCode::OK))
}

//
// Object API
//

#[get("/{class_id}/")]
async fn get_objects_in_class(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    class_id: web::Path<HubuumClassID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = requestor.user;
    let class = class_id.into_inner();
    let query_string = req.query_string();

    let mut params = match parse_query_parameter(query_string) {
        Ok(params) => params,
        Err(e) => return Err(e),
    };

    // Manually add a filter for the class itself to restrict the search
    // in order to restrict the search to the class.
    let class_filter = ParsedQueryParam {
        field: "hubuum_class_id".to_string(),
        operator: crate::models::search::SearchOperator::Equals { is_negated: false },
        value: class.id().to_string(),
    };
    params.push(class_filter);

    debug!(
        message = "Getting objects in class",
        user_id = user.id(),
        class_id = class.id(),
        query = query_string
    );

    let objects = user.search_objects(&pool, params).await?;

    Ok(json_response(objects, StatusCode::OK))
}

#[post("/{class_id}/")]
async fn create_object_in_class(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    class_id: web::Path<HubuumClassID>,
    object_data: web::Json<NewHubuumObject>,
) -> Result<impl Responder, ApiError> {
    let user = requestor.user;
    let class_id = class_id.into_inner();
    let object_data = object_data.into_inner();

    debug!(
        message = "Creating object in class",
        user_id = user.id(),
        class_id = class_id.id(),
        object_data = object_data.name,
    );

    check_permissions!(
        class_id.instance(&pool).await?,
        pool,
        user,
        Permissions::CreateClass
    );
    let object = object_data.save(&pool).await?;

    Ok(json_response_created(
        &object,
        &format!("/api/v1/classes/{}/{}", class_id.id(), object.id()),
    ))
}

#[get("/{class_id}/{object_id}")]
async fn get_object_in_class(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    paths: web::Path<(HubuumClassID, HubuumObjectID)>,
) -> Result<impl Responder, ApiError> {
    let user = requestor.user;
    let (class_id, object_id) = paths.into_inner();

    debug!(
        message = "Getting object in class",
        user_id = user.id(),
        class_id = class_id.id(),
        object_id = object_id.id()
    );

    println!(
        "Getting object in class: {} {}",
        class_id.id(),
        object_id.id()
    );

    // let class = class_id.instance(&pool).await?;
    // check_permissions!(class.namespace_id, pool, user, Permissions::ReadClass);

    let object = object_id.instance(&pool).await?;
    check_permissions!(object, pool, user, Permissions::ReadObject);

    Ok(json_response(object, StatusCode::OK))
}

#[patch("/{class_id}/{object_id}")]
async fn patch_object_in_class(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    paths: web::Path<(HubuumClassID, HubuumObjectID)>,
    object_data: web::Json<UpdateHubuumObject>,
) -> Result<impl Responder, ApiError> {
    let user = requestor.user;
    let (class_id, object_id) = paths.into_inner();
    let object_data = object_data.into_inner();

    debug!(
        message = "Updating object in class",
        user_id = user.id(),
        class_id = class_id.id(),
        object_id = object_id.id()
    );

    let object = object_id.instance(&pool).await?;
    check_permissions!(object, pool, user, Permissions::UpdateObject);

    let object = object_data.update(&pool, object.id).await?;
    Ok(json_response(object, StatusCode::OK))
}

#[delete("/{class_id}/{object_id}")]
async fn delete_object_in_class(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    paths: web::Path<(HubuumClassID, HubuumObjectID)>,
) -> Result<impl Responder, ApiError> {
    let user = requestor.user;
    let (class_id, object_id) = paths.into_inner();

    debug!(
        message = "Deleting object in class",
        user_id = user.id(),
        class_id = class_id.id(),
        object_id = object_id.id()
    );

    let object = object_id.instance(&pool).await?;
    check_permissions!(object, pool, user, Permissions::DeleteObject);

    object.delete(&pool).await?;
    Ok(json_response((), StatusCode::NO_CONTENT))
}
