use super::*;

#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}/objects/{object_id}/related/objects",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID"),
        ("object_id" = i32, Path, description = "Object ID"),
        ("ignore_classes" = Option<String>, Query, description = "Comma-separated class IDs to exclude from the returned connected objects"),
        ("ignore_self_class" = Option<bool>, Query, description = "Exclude connected objects in the same class as the root object. Defaults to true")
    ),
    responses(
        (status = 200, description = "Objects connected to the object", body = [HubuumObjectWithPath]),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Class or object not found", body = ApiErrorResponse)
    )
)]
#[routes]
#[get("/{class_id}/objects/{object_id}/related/objects")]
#[get("/{class_id}/objects/{object_id}/related/objects/")]
async fn get_related_objects(
    pool: AppContext,
    requestor: Authenticated,
    paths: web::Path<(HubuumClassID, HubuumObjectID)>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let (class_id, object_id) = paths.into_inner();
    let target = ObjectSelector::by_id(class_id, object_id)
        .resolve_object_target(&pool)
        .await?;
    read_related_objects(pool, requestor, target, req).await
}

#[utoipa::path(
    get,
    path = "/api/v1/classes/by-name/{class_name}/objects/by-name/{object_name}/related/objects",
    tag = "classes",
    summary = "Get related objects by class and object name",
    security(("bearer_auth" = [])),
    params(
        ("class_name" = String, Path, description = "Globally unique class name"),
        ("object_name" = String, Path, description = "Object name, unique within the class"),
        ("ignore_classes" = Option<String>, Query, description = "Comma-separated class IDs to exclude from the returned connected objects"),
        ("ignore_self_class" = Option<bool>, Query, description = "Exclude connected objects in the same class as the root object. Defaults to true")
    ),
    responses(
        (status = 200, description = "Objects connected to the object", body = [HubuumObjectWithPath]),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Class/object name pair not found", body = ApiErrorResponse)
    )
)]
#[get("/by-name/{class_name}/objects/by-name/{object_name}/related/objects")]
async fn get_related_objects_by_name(
    pool: AppContext,
    requestor: Authenticated,
    paths: web::Path<(String, String)>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let (class_name, object_name) = paths.into_inner();
    let target = ObjectSelector::by_name(class_name, object_name)
        .resolve_object_target(&pool)
        .await?;
    read_related_objects(pool, requestor, target, req).await
}

async fn read_related_objects(
    pool: AppContext,
    requestor: Authenticated,
    target: ResolvedObjectTarget,
    req: HttpRequest,
) -> Result<ApiResponse<Vec<HubuumObjectWithPath>>, ApiError> {
    let user = &requestor.principal;
    let object = target.object();
    let query_string = req.query_string();

    let (mut params, related_options) = parse_related_objects_query(query_string)?;

    can!(
        &pool,
        user,
        requestor.scopes(),
        [Permissions::ReadObject],
        object
    );

    if related_options.ignore_self_class {
        params.filters.add_filter(
            FilterField::ClassId,
            SearchOperator::Equals { is_negated: true },
            &object.hubuum_class_id.to_string(),
        );
    }

    if !related_options.ignore_classes.is_empty() {
        params.filters.add_filter(
            FilterField::ClassId,
            SearchOperator::Equals { is_negated: true },
            &related_options
                .ignore_classes
                .iter()
                .map(i32::to_string)
                .collect::<Vec<_>>()
                .join(","),
        );
    }

    debug!(
        message = "Getting objects connected to object",
        user_id = user.id(),
        class_id = object.hubuum_class_id,
        object_id = object.id(),
        query = query_string,
        ignore_classes = ?related_options.ignore_classes,
        ignore_self_class = related_options.ignore_self_class,
    );

    let search_params = prepare_db_pagination::<RelatedObjectGraphRow>(&params)?;
    let (hits, total_count) = user
        .objects_related_to_page(&pool, object.clone(), search_params, requestor.scopes())
        .await?;

    ApiResponse::mapped_paginated(hits, total_count, &params, |page| {
        page.to_descendant_objects_with_path()
    })
}

#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}/objects/{object_id}/related/relations",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID"),
        ("object_id" = i32, Path, description = "Object ID")
    ),
    responses(
        (status = 200, description = "Direct relations touching the object", body = [HubuumObjectRelation]),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Class or object not found", body = ApiErrorResponse)
    )
)]
#[routes]
#[get("/{class_id}/objects/{object_id}/related/relations")]
#[get("/{class_id}/objects/{object_id}/related/relations/")]
async fn get_related_object_relations(
    pool: AppContext,
    requestor: Authenticated,
    paths: web::Path<(HubuumClassID, HubuumObjectID)>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let (class_id, object_id) = paths.into_inner();
    let target = ObjectSelector::by_id(class_id, object_id)
        .resolve_object_target(&pool)
        .await?;
    read_related_object_relations(pool, requestor, target, req).await
}

#[utoipa::path(
    get,
    path = "/api/v1/classes/by-name/{class_name}/objects/by-name/{object_name}/related/relations",
    tag = "classes",
    summary = "Get related object relations by class and object name",
    security(("bearer_auth" = [])),
    params(
        ("class_name" = String, Path, description = "Globally unique class name"),
        ("object_name" = String, Path, description = "Object name, unique within the class")
    ),
    responses(
        (status = 200, description = "Direct relations touching the object", body = [HubuumObjectRelation]),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Class/object name pair not found", body = ApiErrorResponse)
    )
)]
#[get("/by-name/{class_name}/objects/by-name/{object_name}/related/relations")]
async fn get_related_object_relations_by_name(
    pool: AppContext,
    requestor: Authenticated,
    paths: web::Path<(String, String)>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let (class_name, object_name) = paths.into_inner();
    let target = ObjectSelector::by_name(class_name, object_name)
        .resolve_object_target(&pool)
        .await?;
    read_related_object_relations(pool, requestor, target, req).await
}

async fn read_related_object_relations(
    pool: AppContext,
    requestor: Authenticated,
    target: ResolvedObjectTarget,
    req: HttpRequest,
) -> Result<ApiResponse<Vec<HubuumObjectRelation>>, ApiError> {
    let user = &requestor.principal;
    let params = parse_query_parameter(req.query_string())?;
    let object = target.object();
    can!(
        &pool,
        user,
        requestor.scopes(),
        [Permissions::ReadObject],
        object
    );

    debug!(
        message = "Getting direct relations touching object",
        user_id = user.id(),
        class_id = object.hubuum_class_id,
        object_id = object.id(),
        query = req.query_string(),
    );

    let (relations, total_count) = if pool.permission_backend().supports_sql_visibility_pushdown() {
        let search_params = prepare_db_pagination::<HubuumObjectRelation>(&params)?;
        user.object_relations_touching_page(
            &pool,
            object.clone(),
            search_params,
            requestor.scopes(),
        )
        .await?
    } else {
        let mut required = params.filters.permissions()?;
        required.ensure_contains(&[Permissions::ReadObjectRelation]);
        let required = required.iter().copied().collect::<Vec<_>>();
        if !scope_allows(requestor.scopes(), &required) {
            return ApiResponse::paginated(Vec::new(), 0, &params);
        }
        let mut candidate_options = count_query_options(&params);
        candidate_options.include_total = false;
        let (candidates, _) = user
            .object_relations_touching_page_from_backend_with_admin_status(
                &pool,
                object.clone(),
                candidate_options,
                true,
                None,
            )
            .await?;
        let resources = object_relation_authorization_resources(&pool, &candidates)
            .await?
            .into_iter()
            .map(|resource| (resource.id, resource))
            .collect::<HashMap<_, _>>();
        let principal = PrincipalRef::load(&pool, user).await?;
        let search_params = prepare_db_pagination::<HubuumObjectRelation>(&params)?;
        let page = authorize_cursor_page(
            pool.permission_backend(),
            &principal,
            candidates,
            required,
            &search_params,
            |relation| {
                resources
                    .get(&relation.id)
                    .expect("every relation candidate has an authorization resource")
                    .clone()
            },
        )
        .await?;
        (page.rows, page.total_count)
    };

    ApiResponse::paginated(relations, total_count, &params)
}

#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}/objects/{object_id}/related/graph",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID"),
        ("object_id" = i32, Path, description = "Object ID")
    ),
    responses(
        (status = 200, description = "Neighborhood graph for the object", body = RelatedObjectGraph),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Class or object not found", body = ApiErrorResponse)
    )
)]
#[routes]
#[get("/{class_id}/objects/{object_id}/related/graph")]
#[get("/{class_id}/objects/{object_id}/related/graph/")]
async fn get_related_object_graph(
    pool: AppContext,
    requestor: Authenticated,
    paths: web::Path<(HubuumClassID, HubuumObjectID)>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let (class_id, object_id) = paths.into_inner();
    let target = ObjectSelector::by_id(class_id, object_id)
        .resolve_object_target(&pool)
        .await?;
    read_related_object_graph(pool, requestor, target, req).await
}

#[utoipa::path(
    get,
    path = "/api/v1/classes/by-name/{class_name}/objects/by-name/{object_name}/related/graph",
    tag = "classes",
    summary = "Get the related-object graph by class and object name",
    security(("bearer_auth" = [])),
    params(
        ("class_name" = String, Path, description = "Globally unique class name"),
        ("object_name" = String, Path, description = "Object name, unique within the class")
    ),
    responses(
        (status = 200, description = "Neighborhood graph for the object", body = RelatedObjectGraph),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Class/object name pair not found", body = ApiErrorResponse)
    )
)]
#[get("/by-name/{class_name}/objects/by-name/{object_name}/related/graph")]
async fn get_related_object_graph_by_name(
    pool: AppContext,
    requestor: Authenticated,
    paths: web::Path<(String, String)>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let (class_name, object_name) = paths.into_inner();
    let target = ObjectSelector::by_name(class_name, object_name)
        .resolve_object_target(&pool)
        .await?;
    read_related_object_graph(pool, requestor, target, req).await
}

async fn read_related_object_graph(
    pool: AppContext,
    requestor: Authenticated,
    target: ResolvedObjectTarget,
    req: HttpRequest,
) -> Result<ApiResponse<RelatedObjectGraph>, ApiError> {
    let user = &requestor.principal;
    let object = target.object();
    let (params, graph_limit) =
        prepare_graph_query_options(parse_query_parameter(req.query_string())?)?;

    can!(
        &pool,
        user,
        requestor.scopes(),
        [Permissions::ReadObject],
        object
    );

    debug!(
        message = "Getting related object graph",
        user_id = user.id(),
        class_id = object.hubuum_class_id,
        object_id = object.id(),
        query = req.query_string(),
    );

    let root_object = object_with_root_path(object);
    let connected_objects = user
        .search_objects_related_to(&pool, object.clone(), params, requestor.scopes())
        .await?;
    ensure_graph_result_within_limit(&connected_objects, graph_limit, "objects")?;
    let mut objects = Vec::with_capacity(connected_objects.len() + 1);
    objects.push(root_object);
    objects.extend(connected_objects.to_descendant_objects_with_path());

    let object_ids = objects.iter().map(|item| item.id).collect::<Vec<_>>();
    let relations = user
        .search_object_relations_between_ids(&pool, &object_ids, requestor.scopes())
        .await?;

    Ok(ApiResponse::new(
        RelatedObjectGraph { objects, relations },
        StatusCode::OK,
    ))
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
    pool: AppContext,
    requestor: Authenticated,
    paths: web::Path<(HubuumClassID, HubuumObjectID, HubuumClassID, HubuumObjectID)>,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let (from_class, from_object, to_class, to_object) = paths.into_inner();

    debug!(
        message = "Getting object relation from class and objects",
        user_id = user.id(),
        class_id = from_class.id(),
        from_object_id = from_object.id(),
        to_object_id = to_object.id()
    );

    check_if_object_in_class(&pool, &from_class, &from_object).await?;
    check_if_object_in_class(&pool, &to_class, &to_object).await?;

    let relation = from_object
        .object_relation(&pool, &from_class, &to_object)
        .await
        .map_err(|_| {
            ApiError::NotFound(format!(
                "Object {} of class {} is not related to object {}",
                from_object.id(),
                from_class.id(),
                to_object.id()
            ))
        })?;
    if pool.permission_backend().uses_sql_permission_store() {
        can!(
            &pool,
            user,
            requestor.scopes(),
            [Permissions::ReadObjectRelation],
            from_class,
            from_object,
            to_class,
            to_object
        );
    } else {
        let resource = relation.to_resource_ref(&pool).await?;
        authorize_resources(
            pool.permission_backend(),
            &pool,
            user,
            requestor.scopes(),
            vec![Permissions::ReadObjectRelation],
            vec![resource],
        )
        .await?;
    }
    Ok(ApiResponse::new(relation, StatusCode::OK))
}

#[utoipa::path(
    delete,
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
        (status = 204, description = "Object relation deleted"),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Relation not found", body = ApiErrorResponse)
    )
)]
#[delete("/{class_id}/{from_object_id}/relations/{to_class_id}/{to_object_id}")]
async fn delete_object_relation(
    pool: AppContext,
    requestor: Authenticated,
    paths: web::Path<(HubuumClassID, HubuumObjectID, HubuumClassID, HubuumObjectID)>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
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

    let relation = from_object
        .object_relation(&pool, &from_class, &to_object)
        .await;

    if relation.is_err() {
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

    let relation = relation.expect("Relation should exist after is_err check");

    if pool.permission_backend().uses_sql_permission_store() {
        can!(
            &pool,
            user,
            requestor.scopes(),
            [Permissions::DeleteObjectRelation],
            from_class,
            from_object,
            to_class,
            to_object
        );
    } else {
        let resource = relation.to_resource_ref(&pool).await?;
        authorize_resources(
            pool.permission_backend(),
            &pool,
            user,
            requestor.scopes(),
            vec![Permissions::DeleteObjectRelation],
            vec![resource],
        )
        .await?;
    }

    debug!(
        message = "Relation ID found",
        user_id = user.id(),
        class_id = from_class.id(),
        object_id = from_object.id(),
        relation_id = relation.id(),
        relation_id_actual = relation.id()
    );

    let event_context = requestor.event_context(&req);
    relation.delete(&pool, &event_context).await?;
    Ok(ApiResponse::no_content())
}

#[utoipa::path(
    post,
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
        (status = 201, description = "Object relation created", body = HubuumObjectRelation),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Class or object not found", body = ApiErrorResponse),
        (status = 409, description = "Conflict", body = ApiErrorResponse)
    )
)]
#[post("/{class_id}/{from_object_id}/relations/{to_class_id}/{to_object_id}")]
async fn create_object_relation(
    pool: AppContext,
    requestor: Authenticated,
    paths: web::Path<(HubuumClassID, HubuumObjectID, HubuumClassID, HubuumObjectID)>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let (from_class, from_object, to_class, to_object) = paths.into_inner();

    debug!(
        message = "Creating object relation",
        user_id = user.id(),
        from_class = from_class.id(),
        from_object = from_object.id(),
        to_class = to_class.id(),
        to_object = to_object.id()
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

    if pool.permission_backend().uses_sql_permission_store() {
        can!(
            &pool,
            user,
            requestor.scopes(),
            [Permissions::CreateObjectRelation],
            from_class,
            to_class
        );
    } else {
        let resource = relation.to_resource_ref(&pool).await?;
        authorize_resources(
            pool.permission_backend(),
            &pool,
            user,
            requestor.scopes(),
            vec![Permissions::CreateObjectRelation],
            vec![resource],
        )
        .await?;
    }

    let event_context = requestor.event_context(&req);
    let relation = relation.save(&pool, &event_context).await?;

    let location = api_locations::object_relation(
        from_class.id(),
        from_object.id(),
        to_class.id(),
        to_object.id(),
    )?;
    Ok(ApiResponse::created(relation, location))
}
