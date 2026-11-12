use actix_web::{HttpRequest, Responder, get, http::StatusCode, post, web};
use uuid::Uuid;

use crate::api::openapi::ApiErrorResponse;
use crate::api::response::ApiResponse;
use crate::errors::ApiError;
use crate::extractors::{AccessEventContext, Authenticated};
use crate::middlewares::rate_limit::{
    LoginAttemptOutcome, begin_login_attempt, client_ip_for_request, finish_login_attempt,
};
use crate::models::credential_approval::CredentialOperation;
use crate::models::security_candidate::{
    SecurityCandidateActivated, SecurityCandidateActivationRequest,
    SecurityCandidateLdapVerification, SecurityCandidateLdapVerificationResponse,
    SecurityCandidateReport, SecurityCandidateRequest,
};
use crate::permissions::AppContext;
use crate::services::credential_approvals::CredentialActor;
use crate::services::security_candidates;

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(create_candidate)
        .service(candidate_status)
        .service(verify_ldap)
        .service(activate_candidate);
}

#[utoipa::path(post, path = "/api/v1/admin/runtime-settings/security-candidates", tag = "admin", security(("bearer_auth" = [])),
    request_body = SecurityCandidateRequest,
    responses((status = 201, description = "Non-live candidate; receipt only after every required check passes", body = SecurityCandidateReport),
        (status = 400, description = "Invalid candidate", body = ApiErrorResponse), (status = 403, description = "Local unscoped human administrator required", body = ApiErrorResponse),
        (status = 412, description = "Settings revision changed", body = ApiErrorResponse), (status = 503, description = "Provider or secret unavailable", body = ApiErrorResponse)))]
#[post("/runtime-settings/security-candidates")]
pub async fn create_candidate(
    context: AppContext,
    authenticated: Authenticated,
    req: HttpRequest,
    body: web::Json<SecurityCandidateRequest>,
) -> Result<impl Responder, ApiError> {
    let actor = CredentialActor::resolve(&context, &authenticated).await?;
    let report = security_candidates::create(
        &context,
        &actor,
        body.into_inner(),
        authenticated.event_context(&req),
    )
    .await?;
    Ok(ApiResponse::new_no_store(report, StatusCode::CREATED))
}

#[utoipa::path(get, path = "/api/v1/admin/runtime-settings/security-candidates/{id}", tag = "admin", security(("bearer_auth" = [])),
    params(("id" = Uuid, Path, description = "Candidate ID")),
    responses((status = 200, description = "Owner-only sanitized checks; verification secrets are never redisclosed", body = SecurityCandidateReport),
        (status = 404, description = "Unknown candidate or different initiating credential", body = ApiErrorResponse)))]
#[get("/runtime-settings/security-candidates/{id}")]
pub async fn candidate_status(
    context: AppContext,
    authenticated: Authenticated,
    id: web::Path<Uuid>,
) -> Result<impl Responder, ApiError> {
    let actor = CredentialActor::resolve(&context, &authenticated).await?;
    let candidate = security_candidates::load(&context, &actor, id.into_inner()).await?;
    Ok(ApiResponse::new_no_store(
        SecurityCandidateReport::new(&candidate, None),
        StatusCode::OK,
    ))
}

#[utoipa::path(post, path = "/api/v1/admin/runtime-settings/security-candidates/{id}/verify-ldap", tag = "admin", security(("bearer_auth" = [])),
    params(("id" = Uuid, Path, description = "Candidate ID")), request_body = SecurityCandidateLdapVerification,
    responses((status = 200, description = "LDAP sign-in checked without provisioning, membership writes or session issuance", body = SecurityCandidateLdapVerificationResponse),
        (status = 401, description = "Verification failed", body = ApiErrorResponse), (status = 429, description = "Verification throttled", body = ApiErrorResponse),
        (status = 409, description = "Candidate changed or consumed", body = ApiErrorResponse), (status = 410, description = "Candidate expired", body = ApiErrorResponse)))]
#[post("/runtime-settings/security-candidates/{id}/verify-ldap")]
pub async fn verify_ldap(
    context: AppContext,
    authenticated: Authenticated,
    id: web::Path<Uuid>,
    req: HttpRequest,
    body: web::Json<SecurityCandidateLdapVerification>,
) -> Result<impl Responder, ApiError> {
    let actor = CredentialActor::resolve(&context, &authenticated).await?;
    let id = id.into_inner();
    let input = body.into_inner();
    let candidate = security_candidates::load(&context, &actor, id).await?;
    let report = SecurityCandidateReport::new(&candidate, None);
    let provider = report
        .providers
        .iter()
        .find(|provider| provider.provider_id == input.provider_id)
        .ok_or_else(|| ApiError::BadRequest("Candidate provider required".into()))?;
    if input.username.is_empty() || input.username.len() > 255 || input.password.len() > 4096 {
        return Err(ApiError::BadRequest(
            "Invalid verification credentials".into(),
        ));
    }
    let Some(permit) =
        begin_login_attempt(&provider.name, &input.username, client_ip_for_request(&req)).await?
    else {
        return Err(ApiError::TooManyRequests(
            "Too many verification attempts".into(),
        ));
    };
    let result = security_candidates::verify_ldap(&context, &actor, id, input).await;
    let outcome = match &result {
        Ok(_) => LoginAttemptOutcome::Succeeded,
        Err(ApiError::Unauthorized(_)) => LoginAttemptOutcome::Failed,
        Err(_) => LoginAttemptOutcome::Aborted,
    };
    finish_login_attempt(permit, outcome).await?;
    Ok(ApiResponse::new_no_store(result?, StatusCode::OK))
}

#[utoipa::path(post, path = "/api/v1/admin/runtime-settings/security-candidates/{id}/activate", tag = "admin", security(("bearer_auth" = [])),
    params(("id" = Uuid, Path, description = "Candidate ID"), ("X-Hubuum-Credential-Approval" = String, Header, description = "Fresh exact-operation approval")),
    request_body = SecurityCandidateActivationRequest,
    responses((status = 200, description = "Candidate, approval, revision and audit committed atomically", body = SecurityCandidateActivated),
        (status = 403, description = "Fresh approval or verification receipt required", body = ApiErrorResponse), (status = 409, description = "Candidate changed or consumed", body = ApiErrorResponse),
        (status = 410, description = "Candidate expired", body = ApiErrorResponse), (status = 412, description = "Settings revision changed", body = ApiErrorResponse)))]
#[post("/runtime-settings/security-candidates/{id}/activate")]
pub async fn activate_candidate(
    context: AppContext,
    authenticated: Authenticated,
    id: web::Path<Uuid>,
    req: HttpRequest,
    body: web::Json<SecurityCandidateActivationRequest>,
) -> Result<impl Responder, ApiError> {
    let actor = CredentialActor::resolve(&context, &authenticated).await?;
    let id = id.into_inner();
    let candidate = security_candidates::load(&context, &actor, id).await?;
    let request = body.into_inner();
    let operation = CredentialOperation::ActivateSecurityCandidate {
        candidate_id: id,
        group: candidate.assignment().group().into(),
        expected_revision: request.expected_revision,
    };
    let approved =
        super::credential_approvals::approve_request(&context, &authenticated, &req, operation)
            .await?;
    let result = security_candidates::activate(
        &context,
        &actor,
        id,
        request,
        approved,
        authenticated.event_context(&req),
    )
    .await?;
    Ok(ApiResponse::new_no_store(result, StatusCode::OK))
}
