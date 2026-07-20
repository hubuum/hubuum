use super::*;

#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}/related/classes",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID")
    ),
    responses(
        (status = 200, description = "Classes connected to the class", body = [HubuumClassWithPath]),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Class not found", body = ApiErrorResponse)
    )
)]
#[routes]
#[get("/{class_id}/related/classes")]
#[get("/{class_id}/related/classes/")]
async fn get_related_classes(
    pool: AppContext,
    requestor: Authenticated,
    class_id: web::Path<HubuumClassID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let target = ClassSelector::by_id(class_id.into_inner())
        .resolve_class_target(&pool)
        .await?;
    read_related_classes(pool, requestor, target, req).await
}

#[utoipa::path(
    get,
    path = "/api/v1/classes/by-name/{class_name}/related/classes",
    tag = "classes",
    summary = "Get related classes by class name",
    security(("bearer_auth" = [])),
    params(("class_name" = String, Path, description = "Globally unique class name")),
    responses(
        (status = 200, description = "Classes connected to the class", body = [HubuumClassWithPath]),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Class not found", body = ApiErrorResponse)
    )
)]
#[get("/by-name/{class_name}/related/classes")]
async fn get_related_classes_by_name(
    pool: AppContext,
    requestor: Authenticated,
    class_name: web::Path<String>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let target = ClassSelector::by_name(class_name.into_inner())
        .resolve_class_target(&pool)
        .await?;
    read_related_classes(pool, requestor, target, req).await
}

async fn read_related_classes(
    pool: AppContext,
    requestor: Authenticated,
    target: ResolvedClassTarget,
    req: HttpRequest,
) -> Result<ApiResponse<Vec<HubuumClassWithPath>>, ApiError> {
    let user = &requestor.principal;
    let params = parse_query_parameter(req.query_string())?;
    let class = target.class();
    can!(
        &pool,
        user,
        requestor.scopes(),
        [Permissions::ReadClass],
        class
    );

    let search_params = prepare_db_pagination::<ClassGraphRow>(&params)?;
    let (classes, total_count) = user
        .classes_related_to_page(&pool, class.clone(), search_params, requestor.scopes())
        .await?;

    ApiResponse::mapped_paginated(classes, total_count, &params, |page| {
        page.to_descendant_classes_with_path()
    })
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
#[routes]
#[post("/{class_id}/relations")]
#[post("/{class_id}/relations/")]
async fn create_class_relation(
    pool: AppContext,
    requestor: Authenticated,
    class_id: web::Path<HubuumClassID>,
    relation_data: web::Json<NewHubuumClassRelationFromClass>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    use crate::models::NewHubuumClassRelation;
    use crate::traits::CollectionAccessors;

    let user = &requestor.principal;
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
        forward_template_alias: partial_relation.forward_template_alias.clone(),
        reverse_template_alias: partial_relation.reverse_template_alias.clone(),
    };

    if pool.permission_backend().uses_sql_permission_store() {
        let ids = relation.collection_id(&pool).await?;
        can!(
            &pool,
            user,
            requestor.scopes(),
            [Permissions::CreateClassRelation],
            ids.0,
            ids.1
        );
    } else {
        let resource = relation.to_resource_ref(&pool).await?;
        authorize_resources(
            pool.permission_backend(),
            &pool,
            user,
            requestor.scopes(),
            vec![Permissions::CreateClassRelation],
            vec![resource],
        )
        .await?;
    }

    let event_context = requestor.event_context(&req);
    let relation = relation.save(&pool, &event_context).await?;

    let location = api_locations::class_relation(class_id.id(), relation.id())?;
    Ok(ApiResponse::created(relation, location))
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
    pool: AppContext,
    requestor: Authenticated,
    paths: web::Path<(HubuumClassID, HubuumClassRelationID)>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    use crate::traits::CollectionAccessors;

    let user = &requestor.principal;
    let (class_id, relation_id) = paths.into_inner();

    debug!(
        message = "Deleting class relation",
        user_id = user.id(),
        class_id = class_id.id(),
        relation_id = relation_id.id()
    );

    let relation = relation_id.instance(&pool).await?;

    if pool.permission_backend().uses_sql_permission_store() {
        let ids = relation_id.collection_id(&pool).await?;
        can!(
            &pool,
            user,
            requestor.scopes(),
            [Permissions::DeleteClassRelation],
            ids.0,
            ids.1
        );
    } else {
        let resource = relation.to_resource_ref(&pool).await?;
        authorize_resources(
            pool.permission_backend(),
            &pool,
            user,
            requestor.scopes(),
            vec![Permissions::DeleteClassRelation],
            vec![resource],
        )
        .await?;
    }

    if relation.from_hubuum_class_id == class_id.id()
        || relation.to_hubuum_class_id == class_id.id()
    {
        let event_context = requestor.event_context(&req);
        relation.delete(&pool, &event_context).await?;
        Ok(ApiResponse::no_content())
    } else {
        info!(
            message = "Relation membership mismatch when deleting relation: class does not match either endpoint",
            user_id = user.id(),
            class_id = class_id.id(),
            relation_id = relation_id.id(),
            relation_from_class = relation.from_hubuum_class_id,
            relation_to_class = relation.to_hubuum_class_id
        );
        Err(ApiError::BadRequest(format!(
            "Class {} is not part of relation {}.",
            class_id.id(),
            relation.id,
        )))
    }
}

#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}/related/relations",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID")
    ),
    responses(
        (status = 200, description = "Direct relations touching the class", body = [HubuumClassRelation]),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Class not found", body = ApiErrorResponse)
    )
)]
#[routes]
#[get("/{class_id}/related/relations")]
#[get("/{class_id}/related/relations/")]
async fn get_related_class_relations(
    pool: AppContext,
    requestor: Authenticated,
    class_id: web::Path<HubuumClassID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let target = ClassSelector::by_id(class_id.into_inner())
        .resolve_class_target(&pool)
        .await?;
    read_related_class_relations(pool, requestor, target, req).await
}

#[utoipa::path(
    get,
    path = "/api/v1/classes/by-name/{class_name}/related/relations",
    tag = "classes",
    summary = "Get related class relations by class name",
    security(("bearer_auth" = [])),
    params(("class_name" = String, Path, description = "Globally unique class name")),
    responses(
        (status = 200, description = "Direct relations touching the class", body = [HubuumClassRelation]),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Class not found", body = ApiErrorResponse)
    )
)]
#[get("/by-name/{class_name}/related/relations")]
async fn get_related_class_relations_by_name(
    pool: AppContext,
    requestor: Authenticated,
    class_name: web::Path<String>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let target = ClassSelector::by_name(class_name.into_inner())
        .resolve_class_target(&pool)
        .await?;
    read_related_class_relations(pool, requestor, target, req).await
}

async fn read_related_class_relations(
    pool: AppContext,
    requestor: Authenticated,
    target: ResolvedClassTarget,
    req: HttpRequest,
) -> Result<ApiResponse<Vec<HubuumClassRelation>>, ApiError> {
    let user = &requestor.principal;
    let params = parse_query_parameter(req.query_string())?;
    let class = target.class();
    can!(
        &pool,
        user,
        requestor.scopes(),
        [Permissions::ReadClass],
        class
    );

    debug!(
        message = "Getting direct relations touching class",
        user_id = user.id(),
        class_id = class.id
    );

    let (relations, total_count) = if pool.permission_backend().supports_sql_visibility_pushdown() {
        let search_params = prepare_db_pagination::<HubuumClassRelation>(&params)?;
        user.class_relations_touching_page(&pool, class.clone(), search_params, requestor.scopes())
            .await?
    } else {
        let mut required = params.filters.permissions()?;
        required.ensure_contains(&[Permissions::ReadClassRelation]);
        let required = required.iter().copied().collect::<Vec<_>>();
        if !scope_allows(requestor.scopes(), &required) {
            return ApiResponse::paginated(Vec::new(), 0, &params);
        }
        let mut candidate_options = count_query_options(&params);
        candidate_options.include_total = false;
        let (candidates, _) = user
            .class_relations_touching_page_from_backend_with_admin_status(
                &pool,
                class.clone(),
                candidate_options,
                true,
                None,
            )
            .await?;
        let resources = class_relation_authorization_resources(&pool, &candidates)
            .await?
            .into_iter()
            .map(|resource| (resource.id, resource))
            .collect::<HashMap<_, _>>();
        let principal = PrincipalRef::load(&pool, user).await?;
        let search_params = prepare_db_pagination::<HubuumClassRelation>(&params)?;
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
    path = "/api/v1/classes/{class_id}/related/graph",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID")
    ),
    responses(
        (status = 200, description = "Neighborhood graph for the class", body = RelatedClassGraph),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Class not found", body = ApiErrorResponse)
    )
)]
#[routes]
#[get("/{class_id}/related/graph")]
#[get("/{class_id}/related/graph/")]
async fn get_related_class_graph(
    pool: AppContext,
    requestor: Authenticated,
    class_id: web::Path<HubuumClassID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let target = ClassSelector::by_id(class_id.into_inner())
        .resolve_class_target(&pool)
        .await?;
    read_related_class_graph(pool, requestor, target, req).await
}

#[utoipa::path(
    get,
    path = "/api/v1/classes/by-name/{class_name}/related/graph",
    tag = "classes",
    summary = "Get the related-class graph by class name",
    security(("bearer_auth" = [])),
    params(("class_name" = String, Path, description = "Globally unique class name")),
    responses(
        (status = 200, description = "Neighborhood graph for the class", body = RelatedClassGraph),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Class not found", body = ApiErrorResponse)
    )
)]
#[get("/by-name/{class_name}/related/graph")]
async fn get_related_class_graph_by_name(
    pool: AppContext,
    requestor: Authenticated,
    class_name: web::Path<String>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let target = ClassSelector::by_name(class_name.into_inner())
        .resolve_class_target(&pool)
        .await?;
    read_related_class_graph(pool, requestor, target, req).await
}

async fn read_related_class_graph(
    pool: AppContext,
    requestor: Authenticated,
    target: ResolvedClassTarget,
    req: HttpRequest,
) -> Result<ApiResponse<RelatedClassGraph>, ApiError> {
    let user = &requestor.principal;
    let (params, graph_limit) =
        prepare_graph_query_options(parse_query_parameter(req.query_string())?)?;
    let class = target.class();
    can!(
        &pool,
        user,
        requestor.scopes(),
        [Permissions::ReadClass],
        class
    );

    let root_class = class_with_root_path(class);
    let connected_classes = user
        .search_classes_related_to(&pool, class.clone(), params, requestor.scopes())
        .await?;
    ensure_graph_result_within_limit(&connected_classes, graph_limit, "classes")?;
    let mut classes = Vec::with_capacity(connected_classes.len() + 1);
    classes.push(root_class);
    classes.extend(connected_classes.to_descendant_classes_with_path());

    let class_ids = classes.iter().map(|item| item.id).collect::<Vec<_>>();
    let relations = user
        .search_class_relations_between_ids(&pool, &class_ids, requestor.scopes())
        .await?;

    Ok(ApiResponse::new(
        RelatedClassGraph { classes, relations },
        StatusCode::OK,
    ))
}
