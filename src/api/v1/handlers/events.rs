use actix_web::{HttpRequest, Responder, get, http::StatusCode, web};

use crate::api::openapi::ApiErrorResponse;
use crate::db::DbPool;
use crate::db::traits::events::{list_events_with_total_count, parse_event_filters};
use crate::errors::ApiError;
use crate::events::EventResponse;
use crate::extractors::Authenticated;
use crate::models::Permissions;
use crate::models::namespace::user_can_on_any;
use crate::models::search::parse_query_parameter_with_passthrough;
use crate::pagination::prepare_db_pagination;
use crate::traits::AuthzSubject;
use crate::utilities::response::paginated_json_response;

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
        ("namespace_id" = Option<i32>, Query, description = "Optional namespace id filter"),
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
    let (params, mut passthrough) = parse_query_parameter_with_passthrough(
        req.query_string(),
        &[
            "entity_type",
            "entity_id",
            "action",
            "actor_kind",
            "actor_user_id",
            "namespace_id",
            "occurred_after",
            "occurred_before",
        ],
    )?;
    let filters = parse_event_filters(&mut passthrough)?;
    let search_params = prepare_db_pagination::<EventResponse>(&params)?;
    let visible_namespaces = user_can_on_any(
        &pool,
        &requestor.principal,
        Permissions::ReadAudit,
        requestor.scopes(),
    )
    .await?;
    let accessible_namespace_ids = visible_namespaces
        .iter()
        .map(|namespace| namespace.id)
        .collect::<Vec<_>>();
    let include_namespace_less =
        requestor.scopes().is_none() && requestor.principal.is_admin(&pool).await?;

    let (events, total_count) = list_events_with_total_count(
        &pool,
        &accessible_namespace_ids,
        include_namespace_less,
        &filters,
        &search_params,
    )
    .await?;
    let events = events
        .into_iter()
        .map(EventResponse::from)
        .collect::<Vec<_>>();
    paginated_json_response(events, total_count, StatusCode::OK, &params)
}

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(get_events);
}
