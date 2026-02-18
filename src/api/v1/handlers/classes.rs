use actix_web::{delete, get, http::StatusCode, patch, post, routes, web, HttpRequest, Responder};

use tracing::{debug, info};

use crate::api::openapi::ApiErrorResponse;
use crate::can;
use crate::db::traits::{ClassRelation, ObjectRelationMemberships, UserPermissions};
use crate::db::DbPool;
use crate::errors::ApiError;
use crate::extractors::UserAccess;
use crate::models::traits::{ExpandNamespace, ToHubuumObjects};
use crate::utilities::response::{json_response, json_response_created};

use crate::models::{
    GroupPermission, HubuumClassExpanded, HubuumClassID, HubuumClassRelation,
    HubuumClassRelationID, HubuumClassRelationTransitive, HubuumObject, HubuumObjectID,
    HubuumObjectRelation, HubuumObjectWithPath, NamespaceID, NewHubuumClass,
    NewHubuumClassRelationFromClass, NewHubuumObject, NewHubuumObjectRelation, Permissions,
    UpdateHubuumClass, UpdateHubuumObject,
};
use crate::traits::{CanDelete, CanSave, CanUpdate, NamespaceAccessors, Search, SelfAccessors};

use super::check_if_object_in_class;
use crate::models::search::{parse_query_parameter, FilterField};

// GET /api/v1/classes, list all classes the user may see.
#[utoipa::path(
    get,
    path = "/api/v1/classes",
    tag = "classes",
    security(("bearer_auth" = [])),
    responses(
        (status = 200, description = "Classes matching optional query filters", body = [HubuumClassExpanded]),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse)
    )
)]
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

#[utoipa::path(
    post,
    path = "/api/v1/classes",
    tag = "classes",
    security(("bearer_auth" = [])),
    request_body = NewHubuumClass,
    responses(
        (status = 201, description = "Class created", body = HubuumClassExpanded),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 409, description = "Conflict", body = ApiErrorResponse)
    )
)]
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

    let namespace = NamespaceID(class_data.namespace_id);
    can!(&pool, user, [Permissions::CreateClass], namespace);

    let class = class_data
        .save(&pool)
        .await?
        .expand_namespace(&pool)
        .await?;

    Ok(json_response_created(
        &class,
        format!("/api/v1/classes/{}", class.id).as_str(),
    ))
}

#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID")
    ),
    responses(
        (status = 200, description = "Class", body = HubuumClassExpanded),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Class not found", body = ApiErrorResponse)
    )
)]
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
    can!(&pool, user, [Permissions::ReadClass], class);
    let class = class.expand_namespace(&pool).await?;

    Ok(json_response(class, StatusCode::OK))
}

#[utoipa::path(
    patch,
    path = "/api/v1/classes/{class_id}",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID")
    ),
    request_body = UpdateHubuumClass,
    responses(
        (status = 200, description = "Updated class", body = HubuumClassExpanded),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Class not found", body = ApiErrorResponse)
    )
)]
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
    can!(&pool, user, [Permissions::UpdateClass], class);

    let class = class_data
        .update(&pool, class.id)
        .await?
        .expand_namespace(&pool)
        .await?;
    Ok(json_response(class, StatusCode::OK))
}

#[utoipa::path(
    delete,
    path = "/api/v1/classes/{class_id}",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID")
    ),
    responses(
        (status = 204, description = "Class deleted"),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Class not found", body = ApiErrorResponse)
    )
)]
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
    can!(&pool, user, [Permissions::DeleteClass], class);

    class.delete(&pool).await?;
    Ok(json_response((), StatusCode::NO_CONTENT))
}

#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}/permissions",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID")
    ),
    responses(
        (status = 200, description = "Namespace-group permission mappings for class namespace", body = [GroupPermission]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Class not found", body = ApiErrorResponse)
    )
)]
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
    can!(&pool, user, [Permissions::ReadClass], class);

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

// Contextual get for class relations
#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}/relations",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID")
    ),
    responses(
        (status = 200, description = "Direct class relations from class", body = [HubuumClassRelation]),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Class not found", body = ApiErrorResponse)
    )
)]
#[get("/{class_id}/relations/")]
async fn get_class_relations(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    class_id: web::Path<HubuumClassID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    use crate::db::traits::SelfRelations;

    let user = requestor.user;
    let class_id = class_id.into_inner();
    let query_string = req.query_string();

    debug!(
        message = "Getting class relations",
        user_id = user.id(),
        class_id = class_id.id(),
        query_string = query_string
    );

    let mut params = parse_query_parameter(query_string)?;
    params.ensure_filter_exact(FilterField::ClassFrom, &class_id);

    // TODO: Migrate to user search for permissions.
    let relations = class_id.search_relations(&pool, &params).await?;
    Ok(json_response(relations, StatusCode::OK))
}

// Contextual post for class relations
#[utoipa::path(
    post,
    path = "/api/v1/classes/{class_id}/relations",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID")
    ),
    request_body = NewHubuumClassRelationFromClass,
    responses(
        (status = 201, description = "Class relation created", body = HubuumClassRelation),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 409, description = "Conflict", body = ApiErrorResponse)
    )
)]
#[post("/{class_id}/relations/")]
async fn create_class_relation(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    class_id: web::Path<HubuumClassID>,
    relation_data: web::Json<NewHubuumClassRelationFromClass>,
) -> Result<impl Responder, ApiError> {
    use crate::models::NewHubuumClassRelation;
    use crate::traits::NamespaceAccessors;

    let user = requestor.user;
    let class_id = class_id.into_inner();
    let partial_relation = relation_data.into_inner();

    debug!(
        message = "Creating class relation",
        user_id = user.id(),
        from_class = class_id.id(),
        to_class = partial_relation.to_hubuum_class_id,
    );

    let relation = NewHubuumClassRelation {
        from_hubuum_class_id: class_id.id(),
        to_hubuum_class_id: partial_relation.to_hubuum_class_id,
    };

    let ids = relation
        .namespace_id(&pool)
        .await
        .map(|(id0, id1)| (NamespaceID(id0), NamespaceID(id1)))?;
    can!(
        &pool,
        user,
        [Permissions::CreateClassRelation],
        ids.0,
        ids.1
    );

    let relation = relation.save(&pool).await?;

    Ok(json_response_created(
        relation,
        format!(
            "/api/v1/classes/{}/relations/{}",
            class_id.id(),
            relation.id()
        )
        .as_str(),
    ))
}

#[utoipa::path(
    delete,
    path = "/api/v1/classes/{class_id}/relations/{relation_id}",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID"),
        ("relation_id" = i32, Path, description = "Class relation ID")
    ),
    responses(
        (status = 204, description = "Class relation deleted"),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Class or relation not found", body = ApiErrorResponse)
    )
)]
#[delete("/{class_id}/relations/{relation_id}")]
async fn delete_class_relation(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    paths: web::Path<(HubuumClassID, HubuumClassRelationID)>,
) -> Result<impl Responder, ApiError> {
    use crate::traits::NamespaceAccessors;

    let user = requestor.user;
    let (class_id, relation_id) = paths.into_inner();

    debug!(
        message = "Deleting class relation",
        user_id = user.id(),
        class_id = class_id.id(),
        relation_id = relation_id.id()
    );

    let relation = relation_id.instance(&pool).await?;

    let ids = relation_id
        .namespace_id(&pool)
        .await
        .map(|(id0, id1)| (NamespaceID(id0), NamespaceID(id1)))?;

    can!(
        &pool,
        user,
        [Permissions::DeleteClassRelation],
        ids.0,
        ids.1
    );

    if relation.from_hubuum_class_id == class_id.id() {
        relation.delete(&pool).await?;
        Ok(json_response((), StatusCode::NO_CONTENT))
    } else {
        info!(
            message = "Relation ownership mismatch when deleting relation: from class does not match class",
            user_id = user.id(),
            class_id = class_id.id(),
            relation_id = relation_id.id(),
            relation_from_class = relation.from_hubuum_class_id,
            relation_to_class = relation.to_hubuum_class_id
        );
        Err(ApiError::BadRequest(format!(
            "Class {} is not the from-class of relation {}.",
            class_id.id(),
            relation.id,
        )))
    }
}

#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}/relations/transitive",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID")
    ),
    responses(
        (status = 200, description = "Transitive class relations", body = [HubuumClassRelationTransitive]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Class not found", body = ApiErrorResponse)
    )
)]
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

#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}/relations/transitive/class/{class_id_to}",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "From class ID"),
        ("class_id_to" = i32, Path, description = "To class ID")
    ),
    responses(
        (status = 200, description = "Transitive relations between classes", body = [HubuumClassRelationTransitive]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Class not found", body = ApiErrorResponse)
    )
)]
#[get("/{class_id}/relations/transitive/class/{class_id_to}")]
async fn get_class_relations_transitive_to_class(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    class_ids: web::Path<(HubuumClassID, HubuumClassID)>,
) -> Result<impl Responder, ApiError> {
    let user = requestor.user;
    let (class_id, class_id_to) = class_ids.into_inner();

    debug!(
        message = "Getting class relations to class",
        user_id = user.id(),
        class_id_from = class_id.id(),
        class_id_to = class_id_to.id()
    );

    let relations = class_id.relations_to(&pool, &class_id_to).await?;
    Ok(json_response(relations, StatusCode::OK))
}

//
// Object API
//

#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}/",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID")
    ),
    responses(
        (status = 200, description = "Objects in class", body = [HubuumObject]),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Class not found", body = ApiErrorResponse)
    )
)]
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
    params.ensure_filter_exact(FilterField::ClassId, &class);

    debug!(
        message = "Getting objects in class",
        user_id = user.id(),
        class_id = class.id(),
        query = query_string
    );

    let objects = user.search_objects(&pool, params).await?;

    Ok(json_response(objects, StatusCode::OK))
}

#[utoipa::path(
    post,
    path = "/api/v1/classes/{class_id}/",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID")
    ),
    request_body = NewHubuumObject,
    responses(
        (status = 201, description = "Object created in class", body = HubuumObject),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 409, description = "Conflict", body = ApiErrorResponse)
    )
)]
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

    can!(&pool, user, [Permissions::CreateObject], class_id);

    let object = object_data.save(&pool).await?;

    Ok(json_response_created(
        &object,
        &format!("/api/v1/classes/{}/{}", class_id.id(), object.id()),
    ))
}

#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}/{object_id}",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID"),
        ("object_id" = i32, Path, description = "Object ID")
    ),
    responses(
        (status = 200, description = "Object in class", body = HubuumObject),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Object not found", body = ApiErrorResponse)
    )
)]
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

    // Can you read objects in a class you can't read? Hm.
    // let class = class_id.instance(&pool).await?;
    // check_permissions!(class.namespace_id, pool, user, Permissions::ReadClass);

    let object = object_id.instance(&pool).await?;
    can!(&pool, user, [Permissions::ReadObject], object);

    Ok(json_response(object, StatusCode::OK))
}

#[utoipa::path(
    patch,
    path = "/api/v1/classes/{class_id}/{object_id}",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID"),
        ("object_id" = i32, Path, description = "Object ID")
    ),
    request_body = UpdateHubuumObject,
    responses(
        (status = 200, description = "Updated object", body = HubuumObject),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Object not found", body = ApiErrorResponse)
    )
)]
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
    can!(&pool, user, [Permissions::UpdateObject], object);

    let object = object_data.update(&pool, object.id).await?;
    Ok(json_response(object, StatusCode::OK))
}

#[utoipa::path(
    delete,
    path = "/api/v1/classes/{class_id}/{object_id}",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID"),
        ("object_id" = i32, Path, description = "Object ID")
    ),
    responses(
        (status = 204, description = "Object deleted"),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Object not found", body = ApiErrorResponse)
    )
)]
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
    can!(&pool, user, [Permissions::DeleteObject], object);

    object.delete(&pool).await?;
    Ok(json_response((), StatusCode::NO_CONTENT))
}

#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}/{from_object_id}/relations",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID"),
        ("from_object_id" = i32, Path, description = "Source object ID")
    ),
    responses(
        (status = 200, description = "Objects related to source object", body = [HubuumObjectWithPath]),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Class or object not found", body = ApiErrorResponse)
    )
)]
#[get("/{class_id}/{from_object_id}/relations/")]
async fn list_related_objects(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    paths: web::Path<(HubuumClassID, HubuumObjectID)>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = requestor.user;
    let (from_class, from_object) = paths.into_inner();
    let query_string = req.query_string();

    let params = match parse_query_parameter(query_string) {
        Ok(params) => params,
        Err(e) => return Err(e),
    };

    check_if_object_in_class(&pool, &from_class, &from_object).await?;

    debug!(
        message = "Getting objects related from class and object",
        user_id = user.id(),
        class_id = from_class.id(),
        object_id = from_object.id(),
        query = query_string,
    );

    // We could map this directly from the database? Meh.
    let hits = user
        .search_objects_related_to(&pool, from_object, params)
        .await?
        .to_descendant_objects_with_path();

    Ok(json_response(hits, StatusCode::OK))
}

#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}/{from_object_id}/relations/{to_class_id}/{to_object_id}",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Source class ID"),
        ("from_object_id" = i32, Path, description = "Source object ID"),
        ("to_class_id" = i32, Path, description = "Target class ID"),
        ("to_object_id" = i32, Path, description = "Target object ID")
    ),
    responses(
        (status = 200, description = "Object relation", body = HubuumObjectRelation),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Relation not found", body = ApiErrorResponse)
    )
)]
#[get("/{class_id}/{from_object_id}/relations/{to_class_id}/{to_object_id}")]
async fn get_object_relation_from_class_and_objects(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    paths: web::Path<(HubuumClassID, HubuumObjectID, HubuumClassID, HubuumObjectID)>,
) -> Result<impl Responder, ApiError> {
    let user = requestor.user;
    let (from_class, from_object, to_class, to_object) = paths.into_inner();

    debug!(
        message = "Getting object relation from class and objects",
        user_id = user.id(),
        class_id = from_class.id(),
        from_object_id = from_object.id(),
        to_object_id = to_object.id()
    );

    can!(
        &pool,
        user,
        [Permissions::ReadObjectRelation],
        from_class,
        from_object,
        to_class,
        to_object
    );

    check_if_object_in_class(&pool, &from_class, &from_object).await?;
    check_if_object_in_class(&pool, &to_class, &to_object).await?;

    match from_object
        .object_relation(&pool, &from_class, &to_object)
        .await
    {
        Ok(relation) => Ok(json_response(relation, StatusCode::OK)),
        Err(_) => Err(ApiError::NotFound(format!(
            "Object {} of class {} is not related to object {}",
            from_object.id(),
            from_class.id(),
            to_object.id()
        ))),
    }
}

#[utoipa::path(
    delete,
    path = "/api/v1/classes/{class_id}/{object_id}/relations/{to_class_id}/{to_object_id}",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Source class ID"),
        ("object_id" = i32, Path, description = "Source object ID"),
        ("to_class_id" = i32, Path, description = "Target class ID"),
        ("to_object_id" = i32, Path, description = "Target object ID")
    ),
    responses(
        (status = 204, description = "Object relation deleted"),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Relation not found", body = ApiErrorResponse)
    )
)]
#[delete("/{class_id}/{object_id}/relations/{to_class_id}/{to_object_id}")]
async fn delete_object_relation(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    paths: web::Path<(HubuumClassID, HubuumObjectID, HubuumClassID, HubuumObjectID)>,
) -> Result<impl Responder, ApiError> {
    let user = requestor.user;
    let (from_class, from_object, to_class, to_object) = paths.into_inner();

    check_if_object_in_class(&pool, &from_class, &from_object).await?;
    check_if_object_in_class(&pool, &to_class, &to_object).await?;

    debug!(
        message = "Deleting object relation",
        user_id = user.id(),
        from_class_id = from_class.id(),
        from_object_id = from_object.id(),
        to_class_id = to_class.id(),
        to_object_id = to_object.id()
    );

    can!(
        &pool,
        user,
        [Permissions::DeleteObjectRelation],
        from_class,
        from_object,
        to_class,
        to_object
    );

    let relation = from_class.direct_relation_to(&pool, &to_class).await?;

    if relation.is_none() {
        debug!(
            message = "Relation does not exist",
            user_id = user.id(),
            from_class_id = from_class.id(),
            from_object_id = from_object.id(),
            to_class_id = to_class.id(),
            to_object_id = to_object.id()
        );
        return Err(ApiError::NotFound(format!(
            "Class {} is not related to class {}",
            from_class.id(),
            to_class.id()
        )));
    }

    let relation = relation.expect("Relation should exist after is_none check");

    debug!(
        message = "Relation ID found",
        user_id = user.id(),
        class_id = from_class.id(),
        object_id = from_object.id(),
        relation_id = relation.id(),
        relation_id_actual = relation.id()
    );

    relation.delete(&pool).await?;
    Ok(json_response((), StatusCode::NO_CONTENT))
}

#[utoipa::path(
    post,
    path = "/api/v1/classes/{class_id}/{object_id}/relations/{to_class_id}/{to_object_id}",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Source class ID"),
        ("object_id" = i32, Path, description = "Source object ID"),
        ("to_class_id" = i32, Path, description = "Target class ID"),
        ("to_object_id" = i32, Path, description = "Target object ID")
    ),
    responses(
        (status = 201, description = "Object relation created", body = HubuumObjectRelation),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Class or object not found", body = ApiErrorResponse)
    )
)]
#[post("/{class_id}/{object_id}/relations/{to_class_id}/{to_object_id}")]
async fn create_object_relation(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    paths: web::Path<(HubuumClassID, HubuumObjectID, HubuumClassID, HubuumObjectID)>,
) -> Result<impl Responder, ApiError> {
    let user = requestor.user;
    let (from_class, from_object, to_class, to_object) = paths.into_inner();

    debug!(
        message = "Creating object relation",
        user_id = user.id(),
        from_class = from_class.id(),
        from_object = from_object.id(),
        to_class = to_class.id(),
        to_object = to_object.id()
    );

    can!(
        &pool,
        user,
        [Permissions::CreateObjectRelation],
        from_class,
        to_class
    );

    let is_related = from_class.direct_relation_to(&pool, &to_class).await?;

    if is_related.is_none() {
        debug!(
            message = "Relation does not exist",
            user_id = user.id(),
            from_class = from_class.id(),
            to_class = to_class.id()
        );
        return Err(ApiError::NotFound(format!(
            "Class {} is not related to class {}",
            from_class.id(),
            to_class.id()
        )));
    }

    let relation = is_related.expect("Relation should exist after is_none check");

    let relation = NewHubuumObjectRelation {
        class_relation_id: relation.id,
        from_hubuum_object_id: from_object.id(),
        to_hubuum_object_id: to_object.id(),
    };

    let relation = relation.save(&pool).await?;

    Ok(json_response_created(
        relation,
        format!(
            "/api/v1/classes/{}/{}/relations/{}/{}",
            from_class.id(),
            from_object.id(),
            to_class.id(),
            to_object.id()
        )
        .as_str(),
    ))
}
