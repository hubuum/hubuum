use super::*;

#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}/history",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(("class_id" = i32, Path, description = "Class ID")),
    responses(
        (status = 200, description = "Class history", body = [HistoryResponse<HubuumClassHistory>]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Class not found", body = ApiErrorResponse)
    )
)]
#[get("/{class_id}/history")]
async fn get_class_history(
    pool: AppContext,
    requestor: Authenticated,
    class_id: web::Path<HubuumClassID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    use crate::api::v1::handlers::history::{
        HistoryResponse, authorize_history_page, can_read_deleted_history,
        history_candidate_query_options, readable_history_collection_ids, resolve_actor_usernames,
    };

    let user = &requestor.principal;
    let class_id = class_id.into_inner();
    let (entity_id, require_history) = match class_id.instance(&pool).await {
        Ok(instance) => {
            can!(
                &pool,
                user,
                requestor.scopes(),
                [Permissions::ReadClass],
                instance
            );
            (instance.id, false)
        }
        Err(ApiError::NotFound(_))
            if can_read_deleted_history(
                &pool,
                &requestor.principal,
                requestor.scopes().is_some(),
            )
            .await? =>
        {
            (class_id.id(), true)
        }
        Err(err) => return Err(err),
    };

    let params = parse_query_parameter(req.query_string())?;
    let search_params = prepare_db_pagination::<HubuumClassHistory>(&params)?;
    let (rows, total_count) = if require_history {
        class_history_paginated_with_total_count(
            entity_id,
            &pool,
            &search_params,
            HistoryCollectionFilter::All,
        )
        .await?
    } else if pool.permission_backend().supports_sql_visibility_pushdown() {
        let collection_ids = readable_history_collection_ids(
            &pool,
            user,
            requestor.scopes(),
            Permissions::ReadClass,
        )
        .await?;
        class_history_paginated_with_total_count(
            entity_id,
            &pool,
            &search_params,
            HistoryCollectionFilter::Visible(&collection_ids),
        )
        .await?
    } else {
        let candidate_params = history_candidate_query_options(&params);
        let (candidates, _) = class_history_paginated_with_total_count(
            entity_id,
            &pool,
            &candidate_params,
            HistoryCollectionFilter::All,
        )
        .await?;
        authorize_history_page(
            &pool,
            user,
            requestor.scopes(),
            Permissions::ReadClass,
            candidates,
            &search_params,
            |row| HistoryAuthorizationSnapshot::from(row),
        )
        .await?
    };
    if require_history && rows.is_empty() && params.cursor.is_none() {
        return Err(ApiError::NotFound(format!("class {entity_id} not found")));
    }

    let actor_ids = rows.iter().filter_map(|r| r.actor_id).collect();
    let actor_map = resolve_actor_usernames(&pool, actor_ids).await?;

    ApiResponse::mapped_paginated(rows, total_count, &params, move |rows| {
        rows.into_iter()
            .map(|row| {
                let actor_username = row.actor_id.and_then(|aid| actor_map.get(&aid).cloned());
                HistoryResponse {
                    entry: row,
                    actor_username,
                }
            })
            .collect()
    })
}

#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}/history/as-of",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID"),
        ("at" = String, Query, description = "RFC3339 timestamp")
    ),
    responses(
        (status = 200, description = "Class version at timestamp", body = HistoryResponse<HubuumClassHistory>),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Class or version not found", body = ApiErrorResponse)
    )
)]
#[get("/{class_id}/history/as-of")]
async fn get_class_as_of(
    pool: AppContext,
    requestor: Authenticated,
    class_id: web::Path<HubuumClassID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    use crate::api::v1::handlers::history::{
        HistoryResponse, authorize_history_snapshot, can_read_deleted_history, parse_as_of,
        resolve_actor_usernames,
    };

    let user = &requestor.principal;
    let class_id = class_id.into_inner();
    let (entity_id, deleted) = match class_id.instance(&pool).await {
        Ok(instance) => {
            can!(
                &pool,
                user,
                requestor.scopes(),
                [Permissions::ReadClass],
                instance
            );
            (instance.id, false)
        }
        Err(ApiError::NotFound(_))
            if can_read_deleted_history(
                &pool,
                &requestor.principal,
                requestor.scopes().is_some(),
            )
            .await? =>
        {
            (class_id.id(), true)
        }
        Err(err) => return Err(err),
    };

    let at = parse_as_of(req.query_string())?;
    let row = class_as_of(entity_id, at, &pool)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("no version of class {entity_id} at {at}")))?;

    if !deleted {
        authorize_history_snapshot(
            &pool,
            user,
            requestor.scopes(),
            Permissions::ReadClass,
            HistoryAuthorizationSnapshot::from(&row),
        )
        .await?;
    }

    let actor_map = resolve_actor_usernames(&pool, row.actor_id.into_iter().collect()).await?;
    let actor_username = row.actor_id.and_then(|aid| actor_map.get(&aid).cloned());
    Ok(ApiResponse::ok(HistoryResponse {
        entry: row,
        actor_username,
    }))
}

#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}/{object_id}/history",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID"),
        ("object_id" = i32, Path, description = "Object ID")
    ),
    responses(
        (status = 200, description = "Object history", body = [HistoryResponse<HubuumObjectHistory>]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Class or object not found", body = ApiErrorResponse)
    )
)]
#[get("/{class_id}/{object_id}/history")]
async fn get_object_history(
    pool: AppContext,
    requestor: Authenticated,
    paths: web::Path<(HubuumClassID, HubuumObjectID)>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    use crate::api::v1::handlers::history::{
        HistoryResponse, authorize_history_page, can_read_deleted_history,
        history_candidate_query_options, readable_history_collection_ids, resolve_actor_usernames,
    };

    let user = &requestor.principal;
    let (class_id, object_id) = paths.into_inner();

    let (entity_id, require_history) =
        match check_if_object_in_class(&pool, &class_id, &object_id).await {
            Ok(()) => {
                let object = object_id.instance(&pool).await?;
                can!(
                    &pool,
                    user,
                    requestor.scopes(),
                    [Permissions::ReadObject],
                    object
                );
                (object.id, false)
            }
            Err(ApiError::NotFound(_))
                if can_read_deleted_history(
                    &pool,
                    &requestor.principal,
                    requestor.scopes().is_some(),
                )
                .await? =>
            {
                (object_id.id(), true)
            }
            Err(err) => return Err(err),
        };

    let params = parse_query_parameter(req.query_string())?;
    let search_params = prepare_db_pagination::<HubuumObjectHistory>(&params)?;
    let (rows, total_count) = if require_history {
        object_history_paginated_with_total_count(
            entity_id,
            class_id.id(),
            &pool,
            &search_params,
            HistoryCollectionFilter::All,
        )
        .await?
    } else if pool.permission_backend().supports_sql_visibility_pushdown() {
        let collection_ids = readable_history_collection_ids(
            &pool,
            user,
            requestor.scopes(),
            Permissions::ReadObject,
        )
        .await?;
        object_history_paginated_with_total_count(
            entity_id,
            class_id.id(),
            &pool,
            &search_params,
            HistoryCollectionFilter::Visible(&collection_ids),
        )
        .await?
    } else {
        let candidate_params = history_candidate_query_options(&params);
        let (candidates, _) = object_history_paginated_with_total_count(
            entity_id,
            class_id.id(),
            &pool,
            &candidate_params,
            HistoryCollectionFilter::All,
        )
        .await?;
        authorize_history_page(
            &pool,
            user,
            requestor.scopes(),
            Permissions::ReadObject,
            candidates,
            &search_params,
            |row| HistoryAuthorizationSnapshot::from(row),
        )
        .await?
    };
    if require_history && rows.is_empty() && params.cursor.is_none() {
        return Err(ApiError::NotFound(format!("object {entity_id} not found")));
    }

    let actor_ids = rows.iter().filter_map(|r| r.actor_id).collect();
    let actor_map = resolve_actor_usernames(&pool, actor_ids).await?;

    ApiResponse::mapped_paginated(rows, total_count, &params, move |rows| {
        rows.into_iter()
            .map(|row| {
                let actor_username = row.actor_id.and_then(|aid| actor_map.get(&aid).cloned());
                HistoryResponse {
                    entry: row,
                    actor_username,
                }
            })
            .collect()
    })
}

#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}/{object_id}/history/as-of",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID"),
        ("object_id" = i32, Path, description = "Object ID"),
        ("at" = String, Query, description = "RFC3339 timestamp")
    ),
    responses(
        (status = 200, description = "Object version at timestamp", body = HistoryResponse<HubuumObjectHistory>),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Class, object, or version not found", body = ApiErrorResponse)
    )
)]
#[get("/{class_id}/{object_id}/history/as-of")]
async fn get_object_as_of(
    pool: AppContext,
    requestor: Authenticated,
    paths: web::Path<(HubuumClassID, HubuumObjectID)>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    use crate::api::v1::handlers::history::{
        HistoryResponse, authorize_history_snapshot, can_read_deleted_history, parse_as_of,
        resolve_actor_usernames,
    };

    let user = &requestor.principal;
    let (class_id, object_id) = paths.into_inner();

    let (entity_id, deleted) = match check_if_object_in_class(&pool, &class_id, &object_id).await {
        Ok(()) => {
            let object = object_id.instance(&pool).await?;
            can!(
                &pool,
                user,
                requestor.scopes(),
                [Permissions::ReadObject],
                object
            );
            (object.id, false)
        }
        Err(ApiError::NotFound(_))
            if can_read_deleted_history(
                &pool,
                &requestor.principal,
                requestor.scopes().is_some(),
            )
            .await? =>
        {
            (object_id.id(), true)
        }
        Err(err) => return Err(err),
    };

    let at = parse_as_of(req.query_string())?;
    let row = object_as_of(entity_id, class_id.id(), at, &pool)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("no version of object {entity_id} at {at}")))?;

    if !deleted {
        authorize_history_snapshot(
            &pool,
            user,
            requestor.scopes(),
            Permissions::ReadObject,
            HistoryAuthorizationSnapshot::from(&row),
        )
        .await?;
    }

    let actor_map = resolve_actor_usernames(&pool, row.actor_id.into_iter().collect()).await?;
    let actor_username = row.actor_id.and_then(|aid| actor_map.get(&aid).cloned());
    Ok(ApiResponse::ok(HistoryResponse {
        entry: row,
        actor_username,
    }))
}
