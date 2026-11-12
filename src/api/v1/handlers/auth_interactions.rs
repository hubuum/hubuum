use actix_web::http::{StatusCode, header};
use actix_web::{HttpRequest, Responder, get, post, web};
use hubuum_domain::{InteractionClientId, InteractionId};
use hubuum_storage_core::StorageInteractionAccess;
use uuid::Uuid;

use crate::api::openapi::ApiErrorResponse;
use crate::api::response::ApiResponse;
use crate::errors::{ApiError, InteractionError};
use crate::events::{EventContext, RequestProvenance};
use crate::middlewares::rate_limit::client_ip_for_request;
use crate::models::auth_interaction::{
    AuthenticationMethods, InteractionBeginRequest, InteractionBegun, InteractionClientRequest,
    InteractionCompleted, InteractionCredentialsRequest, InteractionSecret, InteractionStatus,
};
use crate::permissions::AppContext;
use crate::services::{auth_interactions, credential_activity};

mod admission;
pub mod browser;

fn transport(req: &HttpRequest, creation: bool) -> Result<(), ApiError> {
    // These JSON endpoints are for CLI/BFF server-to-server calls. Browser
    // forms use a separate cookie/CSRF transport; credentials never reach JS.
    if req.headers().contains_key(header::ORIGIN)
        || req.headers().contains_key("sec-fetch-site")
        || !req.query_string().is_empty()
    {
        return Err(ApiError::BadRequest(
            "Use server-to-server interaction requests without query parameters".into(),
        ));
    }
    admission::admit(client_ip_for_request(req), creation)
}

fn access(
    req: &HttpRequest,
    id: &str,
    client: InteractionClientId,
) -> Result<StorageInteractionAccess, ApiError> {
    access_with_secret(req, id, client).map(|(access, _)| access)
}

fn access_with_secret(
    req: &HttpRequest,
    id: &str,
    client: InteractionClientId,
) -> Result<(StorageInteractionAccess, InteractionSecret), ApiError> {
    let mut headers = req.headers().get_all(header::AUTHORIZATION);
    let raw = headers
        .next()
        .and_then(|value| value.to_str().ok())
        .ok_or(InteractionError::Invalid)?;
    if headers.next().is_some() {
        return Err(InteractionError::Invalid.into());
    }
    let (scheme, secret) = raw.split_once(' ').ok_or(InteractionError::Invalid)?;
    if !scheme.eq_ignore_ascii_case("Hubuum-Interaction") {
        return Err(InteractionError::Invalid.into());
    }
    let secret = InteractionSecret::parse(secret)?;
    let id = Uuid::parse_str(id)
        .ok()
        .and_then(|id| InteractionId::new(id).ok())
        .ok_or(InteractionError::Invalid)?;
    Ok((
        StorageInteractionAccess::new(id, client, secret.digest()),
        secret,
    ))
}

fn event_context(req: &HttpRequest) -> EventContext {
    RequestProvenance::from_request(req)
        .map(|provenance| provenance.system_event_context())
        .unwrap_or_else(EventContext::system)
}

#[utoipa::path(get, path = "/api/v1/auth/methods", tag = "auth", responses(
    (status = 200, description = "Public password methods; interaction protocol version 1", body = AuthenticationMethods),
    (status = 429, description = "Request budget exhausted; honor Retry-After", body = ApiErrorResponse),
    (status = 503, description = "Provider configuration is unavailable", body = ApiErrorResponse)
))]
#[get("/methods")]
pub async fn methods(context: AppContext, req: HttpRequest) -> Result<impl Responder, ApiError> {
    admission::admit(client_ip_for_request(&req), false)?;
    Ok(ApiResponse::new_no_store(
        auth_interactions::methods(&context).await?,
        StatusCode::OK,
    ))
}

#[utoipa::path(post, path = "/api/v1/auth/interactions", tag = "auth", request_body = InteractionBeginRequest, responses(
    (status = 201, description = "Purpose-bound interaction created; secret is returned once", body = InteractionBegun),
    (status = 400, description = "Invalid purpose/delivery fields or transport", body = ApiErrorResponse),
    (status = 422, description = "Provider or delivery capability unsupported", body = ApiErrorResponse),
    (status = 429, description = "Bounded creation budget exhausted; honor Retry-After", body = ApiErrorResponse),
    (status = 503, description = "Provider configuration is unavailable", body = ApiErrorResponse)
))]
#[post("/interactions")]
pub async fn begin(
    context: AppContext,
    req: HttpRequest,
    input: web::Json<InteractionBeginRequest>,
    authenticated: Option<crate::extractors::Authenticated>,
) -> Result<impl Responder, ApiError> {
    transport(&req, true)?;
    Ok(ApiResponse::new_no_store(
        auth_interactions::begin(&context, input.into_inner(), authenticated.as_ref()).await?,
        StatusCode::CREATED,
    ))
}

#[utoipa::path(post, path = "/api/v1/auth/interactions/{id}/credentials", tag = "auth", security(("interaction_secret" = [])), params(("id" = String, Path)), request_body = InteractionCredentialsRequest, responses(
    (status = 200, description = "Password proof recorded; no session issued yet", body = InteractionStatus),
    (status = 400, description = "Invalid bounded credential input or transport", body = ApiErrorResponse),
    (status = 401, description = "Invalid interaction, client, secret or credentials", body = ApiErrorResponse),
    (status = 403, description = "Interaction denied or cancelled", body = ApiErrorResponse),
    (status = 409, description = "Consumed/failed interaction or changed configuration", body = ApiErrorResponse),
    (status = 410, description = "Expired interaction whose record is still retained; cleaned-up identifiers return 401", body = ApiErrorResponse),
    (status = 429, description = "Login or request budget exhausted; honor Retry-After", body = ApiErrorResponse),
    (status = 503, description = "Provider unavailable", body = ApiErrorResponse)
))]
#[post("/interactions/{id}/credentials")]
pub async fn credentials(
    context: AppContext,
    req: HttpRequest,
    id: web::Path<String>,
    input: web::Json<InteractionCredentialsRequest>,
) -> Result<impl Responder, ApiError> {
    transport(&req, false)?;
    let input = input.into_inner();
    let access = access(&req, &id, input.client_id.clone())?;
    let response =
        auth_interactions::credentials(&context, access, input, client_ip_for_request(&req))
            .await?;
    Ok(ApiResponse::new_no_store(response, StatusCode::OK))
}

#[utoipa::path(post, path = "/api/v1/auth/interactions/{id}/status", tag = "auth", security(("interaction_secret" = [])), params(("id" = String, Path)), request_body = InteractionClientRequest, responses(
    (status = 200, description = "Pending, ready or consumed metadata; no reusable proof or credentials", body = InteractionStatus),
    (status = 401, description = "Invalid interaction, client or secret", body = ApiErrorResponse),
    (status = 403, description = "Denied or cancelled", body = ApiErrorResponse),
    (status = 409, description = "Failed interaction or changed configuration", body = ApiErrorResponse),
    (status = 410, description = "Expired interaction whose record is still retained; cleaned-up identifiers return 401", body = ApiErrorResponse),
    (status = 429, description = "Early polling; increase interval and honor Retry-After", body = ApiErrorResponse)
))]
#[post("/interactions/{id}/status")]
pub async fn status(
    context: AppContext,
    req: HttpRequest,
    id: web::Path<String>,
    input: web::Json<InteractionClientRequest>,
) -> Result<impl Responder, ApiError> {
    transport(&req, false)?;
    let access = access(&req, &id, input.into_inner().client_id)?;
    Ok(ApiResponse::new_no_store(
        auth_interactions::status(&context, access).await?,
        StatusCode::OK,
    ))
}

#[utoipa::path(post, path = "/api/v1/auth/interactions/{id}/complete", tag = "auth", security(("interaction_secret" = [])), params(("id" = String, Path)), request_body = InteractionClientRequest, responses(
    (status = 200, description = "Single-use session issuance using the original client binding", body = InteractionCompleted),
    (status = 401, description = "Invalid interaction, client, secret or authority", body = ApiErrorResponse),
    (status = 403, description = "Denied or cancelled", body = ApiErrorResponse),
    (status = 409, description = "Not ready, consumed, failed or configuration changed; never replay issuance", body = ApiErrorResponse),
    (status = 410, description = "Expired interaction whose record is still retained; cleaned-up identifiers return 401", body = ApiErrorResponse),
    (status = 429, description = "Request budget exhausted; honor Retry-After", body = ApiErrorResponse)
))]
#[post("/interactions/{id}/complete")]
pub async fn complete(
    context: AppContext,
    req: HttpRequest,
    id: web::Path<String>,
    input: web::Json<InteractionClientRequest>,
) -> Result<impl Responder, ApiError> {
    transport(&req, false)?;
    let input = input.into_inner();
    let (access, secret) = access_with_secret(&req, &id, input.client_id)?;
    let session = auth_interactions::complete(
        &context,
        access,
        event_context(&req),
        credential_activity::request_source(&req),
        secret,
    )
    .await?;
    Ok(ApiResponse::new_no_store(session, StatusCode::OK))
}

#[utoipa::path(post, path = "/api/v1/auth/interactions/{id}/cancel", tag = "auth", security(("interaction_secret" = [])), params(("id" = String, Path)), request_body = InteractionClientRequest, responses(
    (status = 204, description = "Cancelled; repeated cancellation is harmless"),
    (status = 401, description = "Invalid interaction, client or secret", body = ApiErrorResponse),
    (status = 403, description = "Denied", body = ApiErrorResponse),
    (status = 409, description = "Consumed, failed or configuration changed", body = ApiErrorResponse),
    (status = 410, description = "Expired interaction whose record is still retained; cleaned-up identifiers return 401", body = ApiErrorResponse),
    (status = 429, description = "Request budget exhausted", body = ApiErrorResponse)
))]
#[post("/interactions/{id}/cancel")]
pub async fn cancel(
    context: AppContext,
    req: HttpRequest,
    id: web::Path<String>,
    input: web::Json<InteractionClientRequest>,
) -> Result<impl Responder, ApiError> {
    transport(&req, false)?;
    auth_interactions::cancel(&context, access(&req, &id, input.into_inner().client_id)?).await?;
    Ok(ApiResponse::no_content())
}

#[utoipa::path(post, path = "/api/v1/auth/interactions/{id}/issued-session/revoke", tag = "auth", security(("interaction_secret" = [])), params(("id" = String, Path)), request_body = InteractionClientRequest, responses(
    (status = 204, description = "Issued session family revoked, including rotated descendants; repeatable"),
    (status = 401, description = "Invalid interaction, client or secret", body = ApiErrorResponse),
    (status = 409, description = "No issued session", body = ApiErrorResponse),
    (status = 429, description = "Request budget exhausted", body = ApiErrorResponse)
))]
#[post("/interactions/{id}/issued-session/revoke")]
pub async fn revoke(
    context: AppContext,
    req: HttpRequest,
    id: web::Path<String>,
    input: web::Json<InteractionClientRequest>,
) -> Result<impl Responder, ApiError> {
    transport(&req, false)?;
    auth_interactions::revoke(
        &context,
        access(&req, &id, input.into_inner().client_id)?,
        event_context(&req),
    )
    .await?;
    Ok(ApiResponse::no_content())
}

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.configure(browser::config)
        .service(methods)
        .service(begin)
        .service(credentials)
        .service(status)
        .service(complete)
        .service(cancel)
        .service(revoke);
}
