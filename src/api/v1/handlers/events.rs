use actix_web::{HttpRequest, Responder, get, web};

use crate::api::openapi::ApiErrorResponse;
use crate::api::response::ApiResponse;
use crate::db::DbPool;
use crate::db::traits::events::{list_events_with_total_count, parse_event_filters};
use crate::errors::ApiError;
use crate::events::{EntityType, EventResponse};
use crate::extractors::Authenticated;
use crate::models::Permissions;
use crate::models::collection::user_can_on_any;
use crate::models::search::parse_query_parameter_with_passthrough;
use crate::pagination::prepare_db_pagination;
use crate::traits::AuthzSubject;

#[utoipa::path(
    get,
    path = "/api/v1/events",
    tag = "events",
    security(("bearer_auth" = [])),
    params(
        ("entity_type" = Option<String>, Query, description = "Optional event entity type filter"),
        ("entity_id" = Option<i32>, Query, description = "Optional event entity id filter"),
        ("action" = Option<String>, Query, description = "Optional action filter"),
        ("actor_kind" = Option<String>, Query, description = "Optional actor kind filter"),
        ("actor_user_id" = Option<i32>, Query, description = "Optional actor principal id filter"),
        ("collection_id" = Option<i32>, Query, description = "Optional collection id filter"),
        ("occurred_after" = Option<String>, Query, description = "Optional lower occurred_at bound, RFC3339 or YYYY-MM-DD"),
        ("occurred_before" = Option<String>, Query, description = "Optional upper occurred_at bound, RFC3339 or YYYY-MM-DD"),
        ("limit" = Option<usize>, Query, description = "Cursor page size"),
        ("sort" = Option<String>, Query, description = "Comma-separated sort fields. Supported fields: id, occurred_at"),
        ("cursor" = Option<String>, Query, description = "Cursor token from X-Next-Cursor")
    ),
    responses(
        (status = 200, description = "Visible audit events", body = [EventResponse]),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse)
    )
)]
#[get("")]
pub async fn get_events(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    list_visible_events(pool, requestor, req, None).await
}

async fn list_visible_events(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    req: HttpRequest,
    entity_filter: Option<(EntityType, i32)>,
) -> Result<impl Responder, ApiError> {
    let filter_keys = [
        "entity_type",
        "entity_id",
        "action",
        "actor_kind",
        "actor_user_id",
        "collection_id",
        "occurred_after",
        "occurred_before",
    ];

    let (params, mut passthrough) =
        parse_query_parameter_with_passthrough(req.query_string(), &filter_keys)?;
    let mut filters = parse_event_filters(&mut passthrough)?;
    if let Some((entity_type, entity_id)) = entity_filter {
        if filters.entity_type.is_some() {
            return Err(ApiError::BadRequest(
                "entity_type is fixed by this route".to_string(),
            ));
        }
        if filters.entity_id.is_some() {
            return Err(ApiError::BadRequest(
                "entity_id is fixed by this route".to_string(),
            ));
        }
        filters.entity_type = Some(entity_type);
        filters.entity_id = Some(entity_id);
    }
    let search_params = prepare_db_pagination::<EventResponse>(&params)?;
    let visible_collections = user_can_on_any(
        &pool,
        &requestor.principal,
        Permissions::ReadAudit,
        requestor.scopes(),
    )
    .await?;
    let accessible_collection_ids = visible_collections
        .iter()
        .map(|collection| collection.id)
        .collect::<Vec<_>>();
    let include_collection_less =
        requestor.scopes().is_none() && requestor.principal.is_admin(&pool).await?;

    let (events, total_count) = list_events_with_total_count(
        &pool,
        &accessible_collection_ids,
        include_collection_less,
        &filters,
        &search_params,
    )
    .await?;
    ApiResponse::paginated(events, total_count, &params)
}

#[utoipa::path(
    get,
    path = "/api/v1/collections/{collection_id}/events",
    tag = "events",
    security(("bearer_auth" = [])),
    params(
        ("collection_id" = i32, Path, description = "Collection id"),
        ("action" = Option<String>, Query, description = "Optional action filter"),
        ("actor_kind" = Option<String>, Query, description = "Optional actor kind filter"),
        ("actor_user_id" = Option<i32>, Query, description = "Optional actor principal id filter"),
        ("occurred_after" = Option<String>, Query, description = "Optional lower occurred_at bound, RFC3339 or YYYY-MM-DD"),
        ("occurred_before" = Option<String>, Query, description = "Optional upper occurred_at bound, RFC3339 or YYYY-MM-DD"),
        ("limit" = Option<usize>, Query, description = "Cursor page size"),
        ("sort" = Option<String>, Query, description = "Comma-separated sort fields. Supported fields: id, occurred_at"),
        ("cursor" = Option<String>, Query, description = "Cursor token from X-Next-Cursor")
    ),
    responses(
        (status = 200, description = "Visible collection audit events", body = [EventResponse]),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse)
    )
)]
#[get("/{collection_id}/events")]
pub async fn get_collection_events(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    req: HttpRequest,
    collection_id: web::Path<i32>,
) -> Result<impl Responder, ApiError> {
    list_visible_events(
        pool,
        requestor,
        req,
        Some((EntityType::Collection, collection_id.into_inner())),
    )
    .await
}

#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}/events",
    tag = "events",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class id"),
        ("action" = Option<String>, Query, description = "Optional action filter"),
        ("actor_kind" = Option<String>, Query, description = "Optional actor kind filter"),
        ("actor_user_id" = Option<i32>, Query, description = "Optional actor principal id filter"),
        ("collection_id" = Option<i32>, Query, description = "Optional collection id filter"),
        ("occurred_after" = Option<String>, Query, description = "Optional lower occurred_at bound, RFC3339 or YYYY-MM-DD"),
        ("occurred_before" = Option<String>, Query, description = "Optional upper occurred_at bound, RFC3339 or YYYY-MM-DD"),
        ("limit" = Option<usize>, Query, description = "Cursor page size"),
        ("sort" = Option<String>, Query, description = "Comma-separated sort fields. Supported fields: id, occurred_at"),
        ("cursor" = Option<String>, Query, description = "Cursor token from X-Next-Cursor")
    ),
    responses(
        (status = 200, description = "Visible class audit events", body = [EventResponse]),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse)
    )
)]
#[get("/{class_id}/events")]
pub async fn get_class_events(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    req: HttpRequest,
    class_id: web::Path<i32>,
) -> Result<impl Responder, ApiError> {
    list_visible_events(
        pool,
        requestor,
        req,
        Some((EntityType::Class, class_id.into_inner())),
    )
    .await
}

#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}/{object_id}/events",
    tag = "events",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class id"),
        ("object_id" = i32, Path, description = "Object id"),
        ("action" = Option<String>, Query, description = "Optional action filter"),
        ("actor_kind" = Option<String>, Query, description = "Optional actor kind filter"),
        ("actor_user_id" = Option<i32>, Query, description = "Optional actor principal id filter"),
        ("collection_id" = Option<i32>, Query, description = "Optional collection id filter"),
        ("occurred_after" = Option<String>, Query, description = "Optional lower occurred_at bound, RFC3339 or YYYY-MM-DD"),
        ("occurred_before" = Option<String>, Query, description = "Optional upper occurred_at bound, RFC3339 or YYYY-MM-DD"),
        ("limit" = Option<usize>, Query, description = "Cursor page size"),
        ("sort" = Option<String>, Query, description = "Comma-separated sort fields. Supported fields: id, occurred_at"),
        ("cursor" = Option<String>, Query, description = "Cursor token from X-Next-Cursor")
    ),
    responses(
        (status = 200, description = "Visible object audit events", body = [EventResponse]),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse)
    )
)]
#[get("/{class_id}/{object_id}/events")]
pub async fn get_object_events(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    req: HttpRequest,
    path: web::Path<(i32, i32)>,
) -> Result<impl Responder, ApiError> {
    let (_, object_id) = path.into_inner();
    list_visible_events(pool, requestor, req, Some((EntityType::Object, object_id))).await
}

#[utoipa::path(
    get,
    path = "/api/v1/iam/users/{user_id}/events",
    tag = "events",
    security(("bearer_auth" = [])),
    params(
        ("user_id" = i32, Path, description = "User principal id"),
        ("action" = Option<String>, Query, description = "Optional action filter"),
        ("actor_kind" = Option<String>, Query, description = "Optional actor kind filter"),
        ("actor_user_id" = Option<i32>, Query, description = "Optional actor principal id filter"),
        ("collection_id" = Option<i32>, Query, description = "Optional collection id filter"),
        ("occurred_after" = Option<String>, Query, description = "Optional lower occurred_at bound, RFC3339 or YYYY-MM-DD"),
        ("occurred_before" = Option<String>, Query, description = "Optional upper occurred_at bound, RFC3339 or YYYY-MM-DD"),
        ("limit" = Option<usize>, Query, description = "Cursor page size"),
        ("sort" = Option<String>, Query, description = "Comma-separated sort fields. Supported fields: id, occurred_at"),
        ("cursor" = Option<String>, Query, description = "Cursor token from X-Next-Cursor")
    ),
    responses(
        (status = 200, description = "Visible user audit events", body = [EventResponse]),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse)
    )
)]
#[get("/{user_id}/events")]
pub async fn get_user_events(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    req: HttpRequest,
    user_id: web::Path<i32>,
) -> Result<impl Responder, ApiError> {
    list_visible_events(
        pool,
        requestor,
        req,
        Some((EntityType::User, user_id.into_inner())),
    )
    .await
}

#[utoipa::path(
    get,
    path = "/api/v1/iam/groups/{group_id}/events",
    tag = "events",
    security(("bearer_auth" = [])),
    params(
        ("group_id" = i32, Path, description = "Group principal id"),
        ("action" = Option<String>, Query, description = "Optional action filter"),
        ("actor_kind" = Option<String>, Query, description = "Optional actor kind filter"),
        ("actor_user_id" = Option<i32>, Query, description = "Optional actor principal id filter"),
        ("collection_id" = Option<i32>, Query, description = "Optional collection id filter"),
        ("occurred_after" = Option<String>, Query, description = "Optional lower occurred_at bound, RFC3339 or YYYY-MM-DD"),
        ("occurred_before" = Option<String>, Query, description = "Optional upper occurred_at bound, RFC3339 or YYYY-MM-DD"),
        ("limit" = Option<usize>, Query, description = "Cursor page size"),
        ("sort" = Option<String>, Query, description = "Comma-separated sort fields. Supported fields: id, occurred_at"),
        ("cursor" = Option<String>, Query, description = "Cursor token from X-Next-Cursor")
    ),
    responses(
        (status = 200, description = "Visible group audit events", body = [EventResponse]),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse)
    )
)]
#[get("/{group_id}/events")]
pub async fn get_group_events(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    req: HttpRequest,
    group_id: web::Path<i32>,
) -> Result<impl Responder, ApiError> {
    list_visible_events(
        pool,
        requestor,
        req,
        Some((EntityType::Group, group_id.into_inner())),
    )
    .await
}

#[utoipa::path(
    get,
    path = "/api/v1/export-templates/{template_id}/events",
    tag = "events",
    security(("bearer_auth" = [])),
    params(
        ("template_id" = i32, Path, description = "Export template id"),
        ("action" = Option<String>, Query, description = "Optional action filter"),
        ("actor_kind" = Option<String>, Query, description = "Optional actor kind filter"),
        ("actor_user_id" = Option<i32>, Query, description = "Optional actor principal id filter"),
        ("collection_id" = Option<i32>, Query, description = "Optional collection id filter"),
        ("occurred_after" = Option<String>, Query, description = "Optional lower occurred_at bound, RFC3339 or YYYY-MM-DD"),
        ("occurred_before" = Option<String>, Query, description = "Optional upper occurred_at bound, RFC3339 or YYYY-MM-DD"),
        ("limit" = Option<usize>, Query, description = "Cursor page size"),
        ("sort" = Option<String>, Query, description = "Comma-separated sort fields. Supported fields: id, occurred_at"),
        ("cursor" = Option<String>, Query, description = "Cursor token from X-Next-Cursor")
    ),
    responses(
        (status = 200, description = "Visible export template audit events", body = [EventResponse]),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse)
    )
)]
#[get("/{template_id}/events")]
pub async fn get_export_template_events(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    req: HttpRequest,
    template_id: web::Path<i32>,
) -> Result<impl Responder, ApiError> {
    list_visible_events(
        pool,
        requestor,
        req,
        Some((EntityType::ExportTemplate, template_id.into_inner())),
    )
    .await
}

#[utoipa::path(
    get,
    path = "/api/v1/remote-targets/{target_id}/events",
    tag = "events",
    security(("bearer_auth" = [])),
    params(
        ("target_id" = i32, Path, description = "Remote target id"),
        ("action" = Option<String>, Query, description = "Optional action filter"),
        ("actor_kind" = Option<String>, Query, description = "Optional actor kind filter"),
        ("actor_user_id" = Option<i32>, Query, description = "Optional actor principal id filter"),
        ("collection_id" = Option<i32>, Query, description = "Optional collection id filter"),
        ("occurred_after" = Option<String>, Query, description = "Optional lower occurred_at bound, RFC3339 or YYYY-MM-DD"),
        ("occurred_before" = Option<String>, Query, description = "Optional upper occurred_at bound, RFC3339 or YYYY-MM-DD"),
        ("limit" = Option<usize>, Query, description = "Cursor page size"),
        ("sort" = Option<String>, Query, description = "Comma-separated sort fields. Supported fields: id, occurred_at"),
        ("cursor" = Option<String>, Query, description = "Cursor token from X-Next-Cursor")
    ),
    responses(
        (status = 200, description = "Visible remote target audit events", body = [EventResponse]),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse)
    )
)]
#[get("/{target_id}/events")]
pub async fn get_remote_target_events(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    req: HttpRequest,
    target_id: web::Path<i32>,
) -> Result<impl Responder, ApiError> {
    list_visible_events(
        pool,
        requestor,
        req,
        Some((EntityType::RemoteTarget, target_id.into_inner())),
    )
    .await
}

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(get_events);
}
