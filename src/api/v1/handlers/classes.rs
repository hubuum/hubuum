use actix_web::{delete, HttpRequest};
use actix_web::{get, http::StatusCode, patch, post, routes, web, Responder};

use tracing::{debug, info};

use crate::check_permissions;
use crate::db::traits::{ClassRelation, ObjectRelationMemberships};
use crate::db::DbPool;
use crate::errors::ApiError;
use crate::extractors::UserAccess;
use crate::utilities::response::{json_response, json_response_created};

use crate::models::{
    HubuumClassID, HubuumClassRelationID, HubuumObjectID, HubuumObjectRelationID, NamespaceID,
    NewHubuumClass, NewHubuumClassRelationFromClass, NewHubuumObject, NewHubuumObjectRelation,
    NewHubuumObjectRelationFromClassAndObject, Permissions, UpdateHubuumClass, UpdateHubuumObject,
};
use crate::traits::{
    CanDelete, CanSave, CanUpdate, ClassAccessors, NamespaceAccessors, Search, SelfAccessors,
};

use crate::models::search::{parse_query_parameter, FilterField, ParsedQueryParam};

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

fn ensure_class_filter(
    params: &mut Vec<ParsedQueryParam>,
    field: FilterField,
    class_id: &HubuumClassID,
) {
    use crate::models::search::SearchOperator;
    if !params.iter().any(|p| p.field == field) {
        params.push(ParsedQueryParam {
            field,
            operator: SearchOperator::Equals { is_negated: false },
            value: class_id.id().to_string(),
        });
    }
}

// Contextual get for class relations
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
    ensure_class_filter(&mut params, FilterField::ClassFrom, &class_id);

    let relations = class_id.search_relations(&pool, &params).await?;
    Ok(json_response(relations, StatusCode::OK))
}

// Contextual post for class relations
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

    let namespaces = relation.namespace(&pool).await?;
    for namespace in [namespaces.0, namespaces.1] {
        check_permissions!(namespace, pool, user, Permissions::CreateClassRelation);
    }

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

    let namespaces = relation_id.namespace(&pool).await?;
    for namespace in [namespaces.0, namespaces.1] {
        check_permissions!(namespace, pool, user, Permissions::DeleteClassRelation);
    }

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
        field: FilterField::ClassId,
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

#[get("/{class_id}/{object_id}/relations/")]
async fn get_object_relations(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    paths: web::Path<(HubuumClassID, HubuumObjectID)>,
) -> Result<impl Responder, ApiError> {
    use crate::db::traits::ObjectRelationsFromUser;
    let user = requestor.user;
    let (class_id, object_id) = paths.into_inner();

    debug!(
        message = "Getting object relations",
        user_id = user.id(),
        class_id = class_id.id(),
        object_id = object_id.id()
    );

    let relations = user
        .get_related_objects(&pool, &object_id, &class_id)
        .await?;
    Ok(json_response(relations, StatusCode::OK))
}

#[get("/{class_id}/{from_object_id}/relations/class/{target_class}")]
async fn get_class_relation_from_classes_and_object(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    paths: web::Path<(HubuumClassID, HubuumObjectID, HubuumClassID)>,
) -> Result<impl Responder, ApiError> {
    let user = requestor.user;
    let (from_class, from_object, requested_class) = paths.into_inner();

    debug!(
        message = "Getting class relation from classes and object",
        user_id = user.id(),
        class_id = from_class.id(),
        object_id = from_object.id(),
        requested_class = requested_class.id()
    );

    let requested_class = requested_class.instance(&pool).await?;
    let from_object = from_object.instance(&pool).await?;

    for namespace in [
        from_class.namespace(&pool).await?,
        from_object.namespace(&pool).await?,
        requested_class.namespace(&pool).await?,
    ] {
        check_permissions!(namespace, pool, user, Permissions::ReadObjectRelation);
    }

    if from_object.hubuum_class_id != from_class.id() {
        debug!(
            message = "Object class mismatch",
            user_id = user.id(),
            class_id = from_class.id(),
            object_id = from_object.id(),
            object_class = from_object.hubuum_class_id
        );
        return Err(ApiError::BadRequest(format!(
            "Object {} is not of class {}",
            from_object.id(),
            from_class.id()
        )));
    }

    let class_relation = match from_class
        .direct_relation_to(&pool, &requested_class)
        .await?
    {
        Some(relation) => relation,
        None => {
            return Err(ApiError::NotFound(format!(
                "Class {} is not related to class {}",
                from_class.id(),
                requested_class.id()
            )))
        }
    };

    if !from_object
        .is_member_of_class_relation(&pool, &class_relation)
        .await?
    {
        return Err(ApiError::NotFound(format!(
            "Object {} is not a member of class relation {}",
            from_object.id(),
            class_relation.id()
        )));
    }

    Ok(json_response(class_relation, StatusCode::OK))
}

#[get("/{class_id}/{from_object_id}/relations/object/{to_object_id}")]
async fn get_object_relation_from_class_and_objects(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    paths: web::Path<(HubuumClassID, HubuumObjectID, HubuumObjectID)>,
) -> Result<impl Responder, ApiError> {
    let user = requestor.user;
    let (from_class, from_object, to_object) = paths.into_inner();

    debug!(
        message = "Getting object relation from class and objects",
        user_id = user.id(),
        class_id = from_class.id(),
        from_object_id = from_object.id(),
        to_object_id = to_object.id()
    );

    let to_object = to_object.instance(&pool).await?;
    let from_object = from_object.instance(&pool).await?;

    for namespace in [
        from_class.namespace(&pool).await?,
        from_object.namespace(&pool).await?,
        to_object.namespace(&pool).await?,
    ] {
        check_permissions!(namespace, pool, user, Permissions::ReadObjectRelation);
    }

    if from_object.hubuum_class_id != from_class.id() {
        debug!(
            message = "Object class mismatch",
            user_id = user.id(),
            class_id = from_class.id(),
            object_id = from_object.id(),
            object_class = from_object.hubuum_class_id
        );
        return Err(ApiError::BadRequest(format!(
            "Object {} is not of class {}",
            from_object.id(),
            from_class.id()
        )));
    }

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

#[delete("/{class_id}/{object_id}/relations/{relation_id}")]
async fn delete_object_relation(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    paths: web::Path<(HubuumClassID, HubuumObjectID, HubuumObjectRelationID)>,
) -> Result<impl Responder, ApiError> {
    let user = requestor.user;
    let (from_class, from_object, requested_relation) = paths.into_inner();

    debug!(
        message = "Deleting object relation",
        user_id = user.id(),
        class_id = from_class.id(),
        object_id = from_object.id(),
        relation_id = requested_relation.id()
    );

    for namespace in [
        from_class.namespace(&pool).await?,
        from_object.namespace(&pool).await?,
    ] {
        check_permissions!(namespace, pool, user, Permissions::DeleteObjectRelation);
    }

    let to_class = from_object.class(&pool).await?;
    let relation = from_class.direct_relation_to(&pool, &to_class).await?;

    if relation.is_none() {
        debug!(
            message = "Relation does not exist",
            user_id = user.id(),
            class_id = from_class.id(),
            object_id = from_object.id()
        );
        return Err(ApiError::NotFound(format!(
            "Class {} is not related to class {}",
            from_class.id(),
            to_class.id()
        )));
    }

    // Verifying that the relation fetched by looking up the relation of the class
    // and the target object class is the same as the relation requested to be deleted.
    let relation = relation.unwrap();
    if relation.from_hubuum_class_id != from_class.id() {
        debug!(
            message = "Relation ID mismatch",
            user_id = user.id(),
            class_id = from_class.id(),
            object_id = from_object.id(),
            relation_id = requested_relation.id(),
            relation_id_actual = relation.id()
        );
        return Err(ApiError::BadRequest(format!(
            "Relation ID {} does not match actual relation ID {}",
            requested_relation.id(),
            relation.id()
        )));
    }

    relation.delete(&pool).await?;
    Ok(json_response((), StatusCode::NO_CONTENT))
}

#[post("/{class_id}/{object_id}/relations/")]
async fn create_object_relation(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    paths: web::Path<(HubuumClassID, HubuumObjectID)>,
    relation_data: web::Json<NewHubuumObjectRelationFromClassAndObject>,
) -> Result<impl Responder, ApiError> {
    let user = requestor.user;
    let (from_class, from_object) = paths.into_inner();
    let partial_relation = relation_data.into_inner();
    let to_object = HubuumObjectID(partial_relation.to_hubuum_object_id);
    let to_class = to_object.class(&pool).await?;

    debug!(
        message = "Creating object relation",
        user_id = user.id(),
        from_class = from_class.id(),
        from_object = from_object.id(),
        to_class = to_class.id(),
        to_object = to_object.id()
    );

    for namespace in [
        to_class.namespace(&pool).await?,
        from_class.namespace(&pool).await?,
    ] {
        check_permissions!(namespace, pool, user, Permissions::CreateObjectRelation);
    }

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

    let relation = is_related.unwrap();

    let relation = NewHubuumObjectRelation {
        class_relation_id: relation.id,
        from_hubuum_object_id: from_object.id(),
        to_hubuum_object_id: to_object.id(),
    };

    let relation = relation.save(&pool).await?;

    Ok(json_response_created(
        relation,
        format!(
            "/api/v1/classes/{}/{}/relations/{}",
            from_class.id(),
            from_object.id(),
            relation.id()
        )
        .as_str(),
    ))
}
