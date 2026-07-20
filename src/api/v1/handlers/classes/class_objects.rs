use super::*;

//
// Object API
//

#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}/",
    tag = "classes",
    description = "Lists objects in the path class. Enabled computed fields can be filtered with computed.shared.<key> or computed.personal.<key> using the normal __operator suffix, and sorted with the same names. Computed querying supports at most two computed filter parameters and two explicit sort fields per request.",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID"),
        ("include" = Option<String>, Query, description = "Set to computed to enrich each object"),
        ("sort" = Option<String>, Query, description = "Sort by object fields or computed.shared.<key>/computed.personal.<key>; computed sorting supports at most two explicit sort fields")
    ),
    responses(
        (status = 200, description = "Objects in class, optionally enriched with computed fields", body = [crate::models::HubuumObjectReadResponse]),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Class not found", body = ApiErrorResponse)
    )
)]
#[get("/{class_id}/")]
async fn get_objects_in_class(
    pool: AppContext,
    requestor: Authenticated,
    class_id: web::Path<HubuumClassID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    list_objects_in_class(pool, requestor, class_id.into_inner(), None, req).await
}

#[utoipa::path(
    get,
    path = "/api/v1/classes/by-name/{class_name}/objects",
    tag = "classes",
    summary = "List objects in a class by class name",
    description = "Name-addressed alias for listing the current objects in a class. Numeric-looking class names remain names.",
    security(("bearer_auth" = [])),
    params(
        ("class_name" = String, Path, description = "Globally unique class name"),
        ("include" = Option<String>, Query, description = "Set to computed to enrich each object"),
        ("sort" = Option<String>, Query, description = "Sort by object fields or computed.shared.<key>/computed.personal.<key>; computed sorting supports at most two explicit sort fields")
    ),
    responses(
        (status = 200, description = "Objects in class, optionally enriched with computed fields", body = [crate::models::HubuumObjectReadResponse]),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Class not found", body = ApiErrorResponse)
    )
)]
#[get("/by-name/{class_name}/objects")]
async fn get_objects_in_class_by_name(
    pool: AppContext,
    requestor: Authenticated,
    class_name: web::Path<String>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let target = ClassSelector::by_name(class_name.into_inner())
        .resolve_class_target(&pool)
        .await?;
    let class_id = HubuumClassID::new(target.class().id)?;
    let class = target.class().clone();
    list_objects_in_class(pool, requestor, class_id, Some(class), req).await
}

async fn class_for_object_list(
    pool: &AppContext,
    class_id: &HubuumClassID,
    resolved_class: Option<&HubuumClass>,
) -> Result<HubuumClass, ApiError> {
    match resolved_class {
        Some(class) => Ok(class.clone()),
        None => class_id.instance(pool).await,
    }
}

async fn list_objects_in_class(
    pool: AppContext,
    requestor: Authenticated,
    class_id: HubuumClassID,
    resolved_class: Option<HubuumClass>,
    req: HttpRequest,
) -> Result<ApiResponse<Vec<HubuumObjectReadResponse>>, ApiError> {
    let query_string = req.query_string();
    let (mut params, include_computed) = parse_computed_object_list_query(query_string)?;

    // The path is authoritative even if the caller supplied a conflicting
    // class_id/classes filter.
    scope_object_query_to_class(&mut params, &class_id);

    debug!(
        message = "Getting objects in class",
        user_id = requestor.principal.id(),
        class_id = class_id.id(),
        query = query_string
    );

    let computed_querying = params
        .sort
        .iter()
        .any(|sort| sort.field.computed_query().is_some())
        || params
            .filters
            .iter()
            .any(|filter| filter.field.computed_query().is_some());

    if computed_querying {
        let class = class_for_object_list(&pool, &class_id, resolved_class.as_ref()).await?;
        return computed_objects::list_objects(&pool, &requestor, &class, params, include_computed)
            .await;
    }

    let (page, total_count) = load_raw_object_page(&pool, &requestor, &params).await?;
    if !include_computed {
        return object_read_page(page, total_count, effective_page_limit(&params)?, false);
    }

    let class = class_for_object_list(&pool, &class_id, resolved_class.as_ref()).await?;
    let personal_owner = computed_personal_owner(&pool, &requestor, &class).await?;
    let next_cursor = page.next_cursor;
    let enriched = enrich_objects_with_computed(&pool, page.items, personal_owner).await?;
    object_read_page(
        Page {
            items: enriched,
            next_cursor,
        },
        total_count,
        effective_page_limit(&params)?,
        true,
    )
}

async fn load_raw_object_page(
    pool: &AppContext,
    requestor: &Authenticated,
    params: &QueryOptions,
) -> Result<(Page<HubuumObject>, i64), ApiError> {
    let user = &requestor.principal;
    let (objects, total_count) = if pool.permission_backend().supports_sql_visibility_pushdown() {
        let total_count = if params.include_total {
            user.count_objects(pool, count_query_options(params), requestor.scopes())
                .await?
        } else {
            SKIPPED_TOTAL_COUNT
        };
        let objects = user
            .search_objects(
                pool,
                prepare_db_pagination::<HubuumObject>(params)?,
                requestor.scopes(),
            )
            .await?;
        (objects, total_count)
    } else if !scope_allows(requestor.scopes(), &[Permissions::ReadObject]) {
        (Vec::new(), 0)
    } else {
        let candidates = user
            .search_objects_from_backend_with_admin_status(
                pool,
                count_query_options(params),
                true,
                None,
            )
            .await?;
        let principal = PrincipalRef::load(pool, user).await?;
        let search_params = prepare_db_pagination::<HubuumObject>(params)?;
        let page = authorize_cursor_page(
            pool.permission_backend(),
            &principal,
            candidates,
            vec![Permissions::ReadObject],
            &search_params,
            |object| ResourceRef {
                kind: ResourceKind::Object,
                id: object.id,
                attrs: ResourceAttrs {
                    collection_id: Some(object.collection_id),
                    class_id: Some(object.hubuum_class_id),
                    name: Some(object.name.clone()),
                    ..Default::default()
                },
            },
        )
        .await?;
        (page.rows, page.total_count)
    };

    Ok((finalize_page(objects, params)?, total_count))
}
#[utoipa::path(
    post,
    path = "/api/v1/classes/{class_id}/",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID")
    ),
    request_body = NewHubuumObjectRequest,
    responses(
        (status = 201, description = "Object created in class", body = HubuumObject),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 409, description = "Conflict", body = ApiErrorResponse)
    )
)]
#[post("/{class_id}/")]
async fn create_object_in_class(
    pool: AppContext,
    requestor: Authenticated,
    class_id: web::Path<HubuumClassID>,
    object_data: web::Json<NewHubuumObjectRequest>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let target = ClassSelector::by_id(class_id.into_inner())
        .resolve_class_target(&pool)
        .await?;
    let object =
        create_object_in_resolved_class(&pool, &requestor, &req, target, object_data.into_inner())
            .await?;
    let location = api_locations::class_object(object.hubuum_class_id, object.id())?;
    Ok(ApiResponse::created(object, location))
}

#[utoipa::path(
    post,
    path = "/api/v1/classes/by-name/{class_name}/objects",
    tag = "classes",
    summary = "Create an object in a class by class name",
    description = "Name-addressed alias for object creation. The original class name and resolved ID are rechecked under the same transaction that creates the object.",
    security(("bearer_auth" = [])),
    params(("class_name" = String, Path, description = "Globally unique current class name")),
    request_body = NewHubuumObjectRequest,
    responses(
        (status = 201, description = "Object created in class", body = HubuumObject),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Class not found or concurrently renamed", body = ApiErrorResponse),
        (status = 409, description = "Conflict", body = ApiErrorResponse)
    )
)]
#[post("/by-name/{class_name}/objects")]
async fn create_object_in_class_by_name(
    pool: AppContext,
    requestor: Authenticated,
    class_name: web::Path<String>,
    object_data: web::Json<NewHubuumObjectRequest>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let target = ClassSelector::by_name(class_name.into_inner())
        .resolve_class_target(&pool)
        .await?;
    let object =
        create_object_in_resolved_class(&pool, &requestor, &req, target, object_data.into_inner())
            .await?;
    let location = api_locations::class_object(object.hubuum_class_id, object.id())?;
    Ok(ApiResponse::created(object, location))
}

async fn create_object_in_resolved_class(
    pool: &AppContext,
    requestor: &Authenticated,
    req: &HttpRequest,
    target: ResolvedClassTarget,
    object: NewHubuumObjectRequest,
) -> Result<HubuumObject, ApiError> {
    let object = object.into_domain_for_class(target.class())?;
    debug!(
        message = "Creating object in class",
        user_id = requestor.principal.id(),
        class_id = target.class().id,
        object_name = object.name,
    );

    can!(
        pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::CreateObject],
        target.class()
    );
    let event_context = requestor.event_context(req);
    object
        .create_object_in_resolved_class(pool, &target, &event_context)
        .await
}

async fn read_resolved_object(
    pool: &AppContext,
    requestor: &Authenticated,
    req: &HttpRequest,
    target: ResolvedObjectTarget,
) -> Result<ApiResponse<HubuumObjectReadResponse>, ApiError> {
    can!(
        pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ReadObject],
        target.object()
    );

    let (_, include_computed) = parse_computed_include(req.query_string())?;
    if include_computed {
        let personal_owner = computed_personal_owner(pool, requestor, target.class()).await?;
        let enriched =
            enrich_objects_with_computed(pool, vec![target.object().clone()], personal_owner)
                .await?
                .pop()
                .ok_or_else(|| {
                    ApiError::InternalServerError(
                        "Computed object enrichment returned no object".to_string(),
                    )
                })?;
        Ok(ApiResponse::new_private_no_store(
            HubuumObjectReadResponse::Computed(enriched),
            StatusCode::OK,
        ))
    } else {
        Ok(ApiResponse::new(
            HubuumObjectReadResponse::Raw(target.object().clone()),
            StatusCode::OK,
        ))
    }
}

#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}/{object_id}",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID"),
        ("object_id" = i32, Path, description = "Object ID"),
        ("include" = Option<String>, Query, description = "Set to computed to enrich the object")
    ),
    responses(
        (status = 200, description = "Object in class, optionally enriched with computed fields", body = crate::models::HubuumObjectReadResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Object not found", body = ApiErrorResponse)
    )
)]
#[get("/{class_id}/{object_id}")]
async fn get_object_in_class(
    pool: AppContext,
    requestor: Authenticated,
    paths: web::Path<(HubuumClassID, HubuumObjectID)>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let (class_id, object_id) = paths.into_inner();

    debug!(
        message = "Getting object in class",
        user_id = user.id(),
        class_id = class_id.id(),
        object_id = object_id.id()
    );

    let target = ObjectSelector::by_id(class_id, object_id)
        .resolve_object_target(&pool)
        .await?;
    read_resolved_object(&pool, &requestor, &req, target).await
}

#[utoipa::path(
    get,
    path = "/api/v1/classes/by-name/{class_name}/objects/by-name/{object_name}",
    tag = "classes",
    summary = "Get an object by class and object name",
    description = "Fully name-addressed alias for the object endpoint. Numeric-looking path values remain names.",
    security(("bearer_auth" = [])),
    params(
        ("class_name" = String, Path, description = "Globally unique class name"),
        ("object_name" = String, Path, description = "Object name, unique within the class"),
        ("include" = Option<String>, Query, description = "Set to computed to enrich the object")
    ),
    responses(
        (status = 200, description = "Object in class, optionally enriched with computed fields", body = crate::models::HubuumObjectReadResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Class/object name pair not found", body = ApiErrorResponse)
    )
)]
#[get("/by-name/{class_name}/objects/by-name/{object_name}")]
async fn get_object_in_class_by_name(
    pool: AppContext,
    requestor: Authenticated,
    paths: web::Path<(String, String)>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let (class_name, object_name) = paths.into_inner();
    let target = ObjectSelector::by_name(class_name, object_name)
        .resolve_object_target(&pool)
        .await?;
    read_resolved_object(&pool, &requestor, &req, target).await
}

async fn apply_resolved_object_update(
    pool: &AppContext,
    requestor: &Authenticated,
    req: &HttpRequest,
    target: ResolvedObjectTarget,
    update: UpdateHubuumObject,
) -> Result<HubuumObject, ApiError> {
    can!(
        pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::UpdateObject],
        target.object()
    );
    ensure_object_update_stays_in_path_class(&update, target.object())?;

    let event_context = requestor.event_context(req);
    update
        .update_resolved_object(pool, &target, &event_context)
        .await
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
    request_body = UpdateHubuumObjectRequest,
    responses(
        (status = 200, description = "Updated object", body = HubuumObject),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Object not found", body = ApiErrorResponse)
    )
)]
#[patch("/{class_id}/{object_id}")]
async fn patch_object_in_class(
    pool: AppContext,
    requestor: Authenticated,
    paths: web::Path<(HubuumClassID, HubuumObjectID)>,
    object_data: web::Json<UpdateHubuumObjectRequest>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let (class_id, object_id) = paths.into_inner();
    let object_data = object_data.into_inner().into_domain()?;

    debug!(
        message = "Updating object in class",
        user_id = user.id(),
        class_id = class_id.id(),
        object_id = object_id.id()
    );

    let target = ObjectSelector::by_id(class_id, object_id)
        .resolve_object_target(&pool)
        .await?;
    let object = apply_resolved_object_update(&pool, &requestor, &req, target, object_data).await?;
    Ok(ApiResponse::new(object, StatusCode::OK))
}

#[utoipa::path(
    patch,
    path = "/api/v1/classes/by-name/{class_name}/objects/by-name/{object_name}",
    tag = "classes",
    summary = "Update an object by class and object name",
    description = "Fully name-addressed alias for object update. The original natural key and resolved IDs are rechecked under the transactional row lock.",
    security(("bearer_auth" = [])),
    params(
        ("class_name" = String, Path, description = "Globally unique current class name"),
        ("object_name" = String, Path, description = "Current object name, unique within the class")
    ),
    request_body = UpdateHubuumObjectRequest,
    responses(
        (status = 200, description = "Updated object", body = HubuumObject),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Class/object pair not found or concurrently renamed", body = ApiErrorResponse)
    )
)]
#[patch("/by-name/{class_name}/objects/by-name/{object_name}")]
async fn patch_object_in_class_by_name(
    pool: AppContext,
    requestor: Authenticated,
    paths: web::Path<(String, String)>,
    object_data: web::Json<UpdateHubuumObjectRequest>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let (class_name, object_name) = paths.into_inner();
    let target = ObjectSelector::by_name(class_name, object_name)
        .resolve_object_target(&pool)
        .await?;
    let object = apply_resolved_object_update(
        &pool,
        &requestor,
        &req,
        target,
        object_data.into_inner().into_domain()?,
    )
    .await?;
    Ok(ApiResponse::new(object, StatusCode::OK))
}

async fn apply_object_data_patch(
    pool: &AppContext,
    requestor: &Authenticated,
    req: &HttpRequest,
    target: ResolvedObjectTarget,
    patch: ObjectDataPatchDocument,
) -> Result<HubuumObject, ApiError> {
    can!(
        pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::UpdateObject],
        target.object()
    );

    let event_context = requestor.event_context(req);
    patch.patch_object_data(pool, &target, &event_context).await
}

#[utoipa::path(
    patch,
    path = "/api/v1/classes/{class_id}/{object_id}/data",
    tag = "classes",
    summary = "Apply JSON Patch to object data",
    description = "Atomically applies an RFC 6902 JSON Patch document to the object's raw `data` document. Every `path` and `from` is an RFC 6901 JSON Pointer relative to the root of `data`; an empty path replaces the complete document. Operations are evaluated against the latest row-locked data. A successful change updates schema validation, computed-field materialization, history, timestamps, and the object update event in one transaction. A patch that produces no data change returns the current object without changing its timestamp or emitting an update event.",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID"),
        ("object_id" = i32, Path, description = "Object ID")
    ),
    request_body(
        content = ObjectDataPatchDocument,
        content_type = "application/json-patch+json",
        description = "RFC 6902 operation array. Supports add, remove, replace, move, copy, and test. Limited to 1,000 operations and 128 pointer segments per path/from."
    ),
    responses(
        (status = 200, description = "Updated object, or the unchanged object for a no-op patch", body = HubuumObject),
        (status = 400, description = "Malformed JSON or invalid JSON Patch structure, operation count, or pointer depth", body = ApiErrorResponse),
        (status = 401, description = "Missing or invalid authentication", body = ApiErrorResponse),
        (status = 403, description = "UpdateObject permission denied", body = ApiErrorResponse),
        (status = 404, description = "Class/object pair not found", body = ApiErrorResponse),
        (status = 406, description = "Final patched data fails the class JSON Schema", body = ApiErrorResponse),
        (status = 409, description = "A patch operation failed, including a failed test operation; no change was persisted", body = ApiErrorResponse),
        (status = 413, description = "JSON Patch request or resulting object data exceeds its resource limits", body = ApiErrorResponse),
        (status = 415, description = "Content-Type is not application/json-patch+json", body = ApiErrorResponse),
        (status = 500, description = "Persistence, computed-field materialization, or event emission failed and the transaction was rolled back", body = ApiErrorResponse)
    )
)]
#[patch("/{class_id}/{object_id}/data")]
async fn patch_object_data_in_class(
    pool: AppContext,
    requestor: Authenticated,
    paths: web::Path<(HubuumClassID, HubuumObjectID)>,
    patch: ObjectDataPatchPayload,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let (class_id, object_id) = paths.into_inner();

    debug!(
        message = "Applying JSON Patch to object data",
        user_id = user.id(),
        class_id = class_id.id(),
        object_id = object_id.id()
    );

    let target = ObjectSelector::by_id(class_id, object_id)
        .resolve_object_target(&pool)
        .await?;
    let object =
        apply_object_data_patch(&pool, &requestor, &req, target, patch.into_inner()).await?;
    Ok(ApiResponse::new(object, StatusCode::OK))
}

#[utoipa::path(
    patch,
    path = "/api/v1/classes/by-name/{class_name}/objects/by-name/{object_name}/data",
    tag = "classes",
    summary = "Apply JSON Patch to object data by object name",
    description = "Fully name-addressed alias for the atomic object-data JSON Patch endpoint. Class names are globally unique, object names are unique within their class, and both names are URL percent-decoded. The server resolves the natural key for authorization, then requires that same object ID, class ID, class name, and object name when taking the transactional row lock so a concurrent rename cannot redirect the patch. Numeric-looking names are always treated as names on this route. Every `path` and `from` remains relative to the root of the raw `data` document.",
    security(("bearer_auth" = [])),
    params(
        ("class_name" = String, Path, description = "Globally unique class name, URL percent-encoded as needed"),
        ("object_name" = String, Path, description = "Class-scoped object name, URL percent-encoded as needed")
    ),
    request_body(
        content = ObjectDataPatchDocument,
        content_type = "application/json-patch+json",
        description = "RFC 6902 operation array. Supports add, remove, replace, move, copy, and test. Limited to 1,000 operations and 128 pointer segments per path/from."
    ),
    responses(
        (status = 200, description = "Updated object, or the unchanged object for a no-op patch", body = HubuumObject),
        (status = 400, description = "Malformed JSON or invalid JSON Patch structure, operation count, or pointer depth", body = ApiErrorResponse),
        (status = 401, description = "Missing or invalid authentication", body = ApiErrorResponse),
        (status = 403, description = "UpdateObject permission denied", body = ApiErrorResponse),
        (status = 404, description = "Class-name/object-name pair not found or the object was concurrently renamed", body = ApiErrorResponse),
        (status = 406, description = "Final patched data fails the class JSON Schema", body = ApiErrorResponse),
        (status = 409, description = "A patch operation failed, including a failed test operation; no change was persisted", body = ApiErrorResponse),
        (status = 413, description = "JSON Patch request or resulting object data exceeds its resource limits", body = ApiErrorResponse),
        (status = 415, description = "Content-Type is not application/json-patch+json", body = ApiErrorResponse),
        (status = 500, description = "Persistence, computed-field materialization, or event emission failed and the transaction was rolled back", body = ApiErrorResponse)
    )
)]
#[patch("/by-name/{class_name}/objects/by-name/{object_name}/data")]
async fn patch_object_data_by_name_in_class(
    pool: AppContext,
    requestor: Authenticated,
    paths: web::Path<(String, String)>,
    patch: ObjectDataPatchPayload,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let (class_name, object_name) = paths.into_inner();
    let target = ObjectSelector::by_name(class_name, object_name)
        .resolve_object_target(&pool)
        .await?;

    debug!(
        message = "Applying JSON Patch to object data by name",
        user_id = user.id(),
        class_id = target.object().hubuum_class_id,
        object_id = target.object().id
    );

    let object =
        apply_object_data_patch(&pool, &requestor, &req, target, patch.into_inner()).await?;
    Ok(ApiResponse::new(object, StatusCode::OK))
}

async fn delete_resolved_object(
    pool: &AppContext,
    requestor: &Authenticated,
    req: &HttpRequest,
    target: ResolvedObjectTarget,
) -> Result<(), ApiError> {
    can!(
        pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::DeleteObject],
        target.object()
    );

    let event_context = requestor.event_context(req);
    target.delete_resolved_object(pool, &event_context).await
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
    pool: AppContext,
    requestor: Authenticated,
    paths: web::Path<(HubuumClassID, HubuumObjectID)>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let (class_id, object_id) = paths.into_inner();

    debug!(
        message = "Deleting object in class",
        user_id = user.id(),
        class_id = class_id.id(),
        object_id = object_id.id()
    );

    let target = ObjectSelector::by_id(class_id, object_id)
        .resolve_object_target(&pool)
        .await?;
    delete_resolved_object(&pool, &requestor, &req, target).await?;
    Ok(ApiResponse::no_content())
}

#[utoipa::path(
    delete,
    path = "/api/v1/classes/by-name/{class_name}/objects/by-name/{object_name}",
    tag = "classes",
    summary = "Delete an object by class and object name",
    description = "Fully name-addressed alias for object deletion. The original natural key and resolved IDs are rechecked under the transactional row lock.",
    security(("bearer_auth" = [])),
    params(
        ("class_name" = String, Path, description = "Globally unique current class name"),
        ("object_name" = String, Path, description = "Current object name, unique within the class")
    ),
    responses(
        (status = 204, description = "Object deleted"),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Class/object pair not found or concurrently renamed", body = ApiErrorResponse)
    )
)]
#[delete("/by-name/{class_name}/objects/by-name/{object_name}")]
async fn delete_object_in_class_by_name(
    pool: AppContext,
    requestor: Authenticated,
    paths: web::Path<(String, String)>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let (class_name, object_name) = paths.into_inner();
    let target = ObjectSelector::by_name(class_name, object_name)
        .resolve_object_target(&pool)
        .await?;
    delete_resolved_object(&pool, &requestor, &req, target).await?;
    Ok(ApiResponse::no_content())
}
