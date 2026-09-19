use crate::api::openapi::ApiErrorResponse;
use crate::api::response::ApiResponse;
use crate::errors::ApiError;
use crate::extractors::{AccessEventContext, Authenticated};
use crate::middlewares::rate_limit::{
    LoginAttemptOutcome, begin_login_attempt, client_ip_for_request, finish_login_attempt,
};
use crate::models::credential_approval::{
    APPROVAL_HEADER, CredentialApprovalRecord, CredentialApprovalRequest,
    CredentialApprovalResponse, CredentialApprovalSecret, CredentialOperation,
};
use crate::permissions::AppContext;
use crate::services::credential_approvals::{self, ApprovedCredentialOperation, CredentialActor};
use actix_web::{HttpRequest, Responder, get, http::StatusCode, post, web};

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(create_approval).service(get_approval);
}

#[utoipa::path(post, path = "/api/v1/iam/credential-approvals", tag = "auth", security(("bearer_auth" = [])),
    request_body = CredentialApprovalRequest,
    responses((status = 201, description = "Single-use approval and retained evidence; no-store", body = CredentialApprovalResponse),
        (status = 400, description = "Invalid operation", body = ApiErrorResponse),
        (status = 401, description = "Authentication failed", body = ApiErrorResponse),
        (status = 403, description = "Human or administrative authority required", body = ApiErrorResponse),
        (status = 429, description = "Reauthentication throttled", body = ApiErrorResponse),
        (status = 503, description = "Authentication provider unavailable", body = ApiErrorResponse)))]
#[post("")]
pub async fn create_approval(
    context: AppContext,
    authenticated: Authenticated,
    req: HttpRequest,
    body: web::Json<CredentialApprovalRequest>,
) -> Result<impl Responder, ApiError> {
    let actor = CredentialActor::resolve(&context, &authenticated).await?;
    let input = body.into_inner();
    let operation_kind = input.operation.kind().as_str();
    let login = actor.password_login(&context, input.password).await?;
    let scope = login
        .identity_scope
        .as_deref()
        .unwrap_or(crate::models::LOCAL_IDENTITY_SCOPE);
    let Some(permit) = begin_login_attempt(scope, &login.name, client_ip_for_request(&req)).await?
    else {
        tracing::warn!(
            message = "Credential reauthentication throttled",
            actor_id = actor.id(),
            operation = operation_kind
        );
        return Err(ApiError::TooManyRequests(
            "Too many authentication attempts. Please try again later.".into(),
        ));
    };
    let proof = match credential_approvals::verify_password(&context, &actor, login).await {
        Ok(proof) => {
            finish_login_attempt(permit, LoginAttemptOutcome::Succeeded).await?;
            proof
        }
        Err(error) => {
            let outcome = if matches!(error, ApiError::Unauthorized(_)) {
                LoginAttemptOutcome::Failed
            } else {
                LoginAttemptOutcome::Aborted
            };
            finish_login_attempt(permit, outcome).await?;
            tracing::warn!(
                message = "Credential reauthentication rejected",
                actor_id = actor.id(),
                operation = operation_kind,
                reason = error.class()
            );
            return Err(error);
        }
    };
    let response = credential_approvals::issue(
        &context,
        &actor,
        input.operation,
        proof,
        &authenticated.event_context(&req),
    )
    .await?;
    Ok(ApiResponse::new_no_store(response, StatusCode::CREATED))
}

#[utoipa::path(get, path = "/api/v1/iam/credential-approvals/{approval_id}", tag = "auth", security(("bearer_auth" = [])),
    params(("approval_id" = i32, Path, description = "Retained approval identifier")),
    responses((status = 200, description = "Non-secret evidence, visible to actor or administrator", body = CredentialApprovalRecord),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Approval not found or not visible", body = ApiErrorResponse)))]
#[get("/{approval_id}")]
pub async fn get_approval(
    context: AppContext,
    authenticated: Authenticated,
    id: web::Path<i32>,
) -> Result<impl Responder, ApiError> {
    let actor = CredentialActor::resolve(&context, &authenticated).await?;
    Ok(ApiResponse::new_no_store(
        credential_approvals::get(&context, &actor, id.into_inner()).await?,
        StatusCode::OK,
    ))
}

pub(crate) async fn approve_request(
    context: &AppContext,
    authenticated: &Authenticated,
    req: &HttpRequest,
    operation: CredentialOperation,
) -> Result<ApprovedCredentialOperation, ApiError> {
    let actor = CredentialActor::resolve(context, authenticated).await?;
    let secret = (|| {
        if req.headers().get_all(APPROVAL_HEADER).count() != 1 {
            return Err(ApiError::ReauthenticationRequired);
        }
        let raw = req
            .headers()
            .get(APPROVAL_HEADER)
            .and_then(|header| header.to_str().ok())
            .ok_or(ApiError::ReauthenticationRequired)?;
        CredentialApprovalSecret::try_from(raw.to_string())
    })();
    let secret = match secret {
        Ok(secret) => secret,
        Err(error) => {
            tracing::warn!(
                message = "Credential approval missing or malformed",
                actor_id = actor.id(),
                operation = operation.kind().as_str()
            );
            return Err(error);
        }
    };
    credential_approvals::approve(context, &actor, operation, secret).await
}
