//! Script-free, cookie-bound HTML transport. Client redemption stays server-to-server.
use actix_web::{
    HttpRequest, HttpResponse, Responder, ResponseError,
    cookie::{Cookie, SameSite, time::Duration},
    get,
    http::{StatusCode, header},
    post, web,
};
use hubuum_domain::InteractionId;
use hubuum_storage_core::StorageInteractionView;
use serde::Deserialize;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::config::auth_browser::BrowserSettings;
use crate::errors::{ApiError, InteractionError};
use crate::middlewares::rate_limit::client_ip_for_request;
use crate::models::auth_interaction::browser::{BrowserSession, BrowserUserCode};
use crate::permissions::AppContext;
use crate::services::auth_interactions::browser as service;

const ENTRY: &str = "/api/v1/auth/interactions/browser";

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CodeForm {
    csrf: String,
    user_code: String,
}
#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CredentialsForm {
    csrf: String,
    name: String,
    #[schema(write_only)]
    password: String,
}
#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DecisionForm {
    csrf: String,
    decision: Decision,
}
#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ProviderForm {
    csrf: String,
}

#[derive(Deserialize, ToSchema)]
pub struct OidcCallbackQuery {
    state: String,
    code: Option<String>,
    iss: Option<String>,
    error: Option<String>,
}
#[derive(Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
enum Decision {
    Approve,
    Deny,
}

mod pages;
use pages::{BrowserPage, RequestDisplay, response};

fn failure(error: ApiError) -> HttpResponse {
    let message = match &error {
        ApiError::AuthenticationInteraction(error) => error.message(),
        _ => "This request could not be completed. Return to your application and begin again.",
    };
    let mut result = response(error.status_code(), BrowserPage::Error { message });
    if let Some(retry) = error.error_response().headers().get(header::RETRY_AFTER) {
        result
            .headers_mut()
            .insert(header::RETRY_AFTER, retry.clone());
    }
    result
}
fn check_request(
    req: &HttpRequest,
    settings: &BrowserSettings,
    mutation: bool,
) -> Result<(), ApiError> {
    check_host(req, settings)?;
    let origin = settings.origin()?;
    if !req.query_string().is_empty()
        || mutation
            && (req.headers().get_all(header::ORIGIN).count() != 1
                || req
                    .headers()
                    .get(header::ORIGIN)
                    .and_then(|v| v.to_str().ok())
                    != Some(origin))
        || mutation
            && req.headers().contains_key("sec-fetch-site")
            && (req.headers().get_all("sec-fetch-site").count() != 1
                || req
                    .headers()
                    .get("sec-fetch-site")
                    .and_then(|v| v.to_str().ok())
                    != Some("same-origin"))
    {
        return Err(InteractionError::Invalid.into());
    }
    super::admission::admit(client_ip_for_request(req), false)
}
fn check_host(req: &HttpRequest, settings: &BrowserSettings) -> Result<(), ApiError> {
    let authority = settings
        .origin()?
        .split_once("://")
        .ok_or(InteractionError::Invalid)?
        .1;
    if req.headers().get_all(header::HOST).count() != 1
        || req
            .headers()
            .get(header::HOST)
            .and_then(|value| value.to_str().ok())
            != Some(authority)
    {
        return Err(InteractionError::Invalid.into());
    }
    Ok(())
}
fn cookie_name(settings: &BrowserSettings, id: Option<InteractionId>) -> String {
    format!(
        "{}hubuum.auth.{}",
        if settings.secure() { "__Host-" } else { "dev-" },
        id.map(|id| id.value().to_string())
            .unwrap_or_else(|| "entry".into())
    )
}
fn cookie(
    settings: &BrowserSettings,
    id: Option<InteractionId>,
    session: &BrowserSession,
) -> Cookie<'static> {
    Cookie::build(cookie_name(settings, id), session.value())
        .path("/")
        .http_only(true)
        .secure(settings.secure())
        .same_site(SameSite::Lax)
        .max_age(Duration::seconds(600))
        .finish()
}
fn session(
    req: &HttpRequest,
    settings: &BrowserSettings,
    id: Option<InteractionId>,
) -> Result<BrowserSession, ApiError> {
    let name = cookie_name(settings, id);
    let cookies = req.cookies().map_err(|_| InteractionError::Invalid)?;
    let mut matches = cookies.iter().filter(|cookie| cookie.name() == name);
    let value = matches.next().ok_or(InteractionError::Invalid)?;
    if matches.next().is_some() {
        return Err(InteractionError::Invalid.into());
    }
    BrowserSession::parse(value.value())
}
fn with_cookie(
    mut response: HttpResponse,
    cookie: Cookie<'static>,
) -> Result<HttpResponse, ApiError> {
    response
        .add_cookie(&cookie)
        .map_err(|_| InteractionError::Invalid)?;
    Ok(response)
}
fn redirect(location: String) -> HttpResponse {
    HttpResponse::SeeOther()
        .insert_header((header::LOCATION, location))
        .insert_header((header::CACHE_CONTROL, "no-store"))
        .insert_header((header::REFERRER_POLICY, "no-referrer"))
        .finish()
}
fn interaction_page(view: &StorageInteractionView, session: &BrowserSession) -> HttpResponse {
    let request = RequestDisplay::new(view);
    let page = match view.authenticated_name() {
        Some(name) => BrowserPage::Approval {
            request,
            csrf: session.csrf(),
            name,
        },
        None => BrowserPage::Credentials {
            request,
            csrf: session.csrf(),
        },
    };
    response(StatusCode::OK, page)
}

#[utoipa::path(get, path="/api/v1/auth/interactions/browser", tag="auth", responses((status=200, description="Hosted user-code entry; sets HttpOnly browser cookie", body=String, content_type="text/html"), (status=422, description="Browser transport is not configured")))]
#[get("/interactions/browser")]
pub async fn entry(context: AppContext, req: HttpRequest) -> Result<impl Responder, ApiError> {
    Ok((|| {
        let settings = service::settings(&context)?;
        check_request(&req, &settings, false)?;
        let session = BrowserSession::generate();
        with_cookie(
            response(
                StatusCode::OK,
                BrowserPage::Entry {
                    csrf: session.csrf(),
                },
            ),
            cookie(&settings, None, &session),
        )
    })()
    .unwrap_or_else(failure))
}

#[utoipa::path(post, path="/api/v1/auth/interactions/browser", tag="auth", request_body(content=CodeForm, content_type="application/x-www-form-urlencoded"), responses((status=303, description="Browser attached; redirect to sign-in"), (status=401, description="Invalid user code or browser binding")))]
#[post("/interactions/browser")]
pub async fn attach(
    context: AppContext,
    req: HttpRequest,
    form: web::Form<CodeForm>,
) -> Result<impl Responder, ApiError> {
    Ok(async {
        let settings = service::settings(&context)?;
        check_request(&req, &settings, true)?;
        super::admission::admit(client_ip_for_request(&req), true)?;
        let session = session(&req, &settings, None)?;
        session.validate_csrf(&form.csrf)?;
        let view =
            service::attach(&context, BrowserUserCode::parse(&form.user_code)?, &session).await?;
        let id = view.binding().id();
        let mut response = with_cookie(
            redirect(format!("{ENTRY}/{}", id.value())),
            cookie(&settings, Some(id), &session),
        )?;
        let mut old = cookie(&settings, None, &session);
        old.make_removal();
        response
            .add_cookie(&old)
            .map_err(|_| InteractionError::Invalid)?;
        Ok::<_, ApiError>(response)
    }
    .await
    .unwrap_or_else(failure))
}

#[utoipa::path(get, path="/api/v1/auth/interactions/browser/{id}", tag="auth", params(("id"=Uuid, Path)), responses((status=200, description="Bound sign-in or approval page", body=String, content_type="text/html"), (status=401, description="Invalid browser binding")))]
#[get("/interactions/browser/{id}")]
pub async fn resume(
    context: AppContext,
    req: HttpRequest,
    id: web::Path<Uuid>,
) -> Result<impl Responder, ApiError> {
    Ok(async {
        let settings = service::settings(&context)?;
        check_request(&req, &settings, false)?;
        let id = InteractionId::new(*id).map_err(|_| InteractionError::Invalid)?;
        let session = session(&req, &settings, Some(id))?;
        let view = service::inspect(&context, id, &session).await?;
        if view.authenticated_name().is_none()
            && crate::services::auth_interactions::oidc::available(&context, view.binding()).await?
        {
            return Ok(response(
                StatusCode::OK,
                BrowserPage::Provider {
                    request: RequestDisplay::new(&view),
                    csrf: session.csrf(),
                },
            ));
        }
        Ok::<_, ApiError>(interaction_page(&view, &session))
    }
    .await
    .unwrap_or_else(failure))
}

#[utoipa::path(post, path="/api/v1/auth/interactions/browser/{id}/provider", tag="auth", params(("id"=Uuid, Path)), request_body(content=ProviderForm, content_type="application/x-www-form-urlencoded"), responses((status=200, description="Continue at the selected identity provider", body=String, content_type="text/html"), (status=401, description="Invalid browser binding or request")))]
#[post("/interactions/browser/{id}/provider")]
pub async fn provider_start(
    context: AppContext,
    req: HttpRequest,
    id: web::Path<Uuid>,
    form: web::Form<ProviderForm>,
) -> Result<impl Responder, ApiError> {
    Ok(async {
        let settings = service::settings(&context)?;
        check_request(&req, &settings, true)?;
        let id = InteractionId::new(*id).map_err(|_| InteractionError::Invalid)?;
        let session = session(&req, &settings, Some(id))?;
        session.validate_csrf(&form.csrf)?;
        let url = crate::services::auth_interactions::oidc::start(&context, id, &session).await?;
        Ok::<_, ApiError>(response(
            StatusCode::OK,
            BrowserPage::ProviderRedirect { url: &url },
        ))
    }
    .await
    .unwrap_or_else(failure))
}

#[utoipa::path(get, path="/api/v1/auth/callbacks/{provider_id}", tag="auth", params(
    ("provider_id"=Uuid, Path),
    ("state"=String, Query, description="One-use provider correlation state"),
    ("code"=Option<String>, Query, description="Authorization code on success"),
    ("iss"=Option<String>, Query, description="Exact authorization-response issuer"),
    ("error"=Option<String>, Query, description="Provider error on denial")
), responses((status=303, description="OIDC identity verified; explicit Hubuum approval is required"), (status=401, description="Invalid, expired or replayed callback")))]
#[get("/callbacks/{provider_id}")]
pub async fn oidc_callback(
    context: AppContext,
    req: HttpRequest,
    provider_id: web::Path<Uuid>,
    query: web::Query<OidcCallbackQuery>,
) -> Result<impl Responder, ApiError> {
    Ok(async {
        let settings = service::settings(&context)?;
        check_host(&req, &settings)?;
        super::admission::admit(client_ip_for_request(&req), true)?;
        if req.query_string().len() > 8192 {
            return Err(InteractionError::Invalid.into());
        }
        let id = crate::services::auth_interactions::oidc::callback_id(&query.state)?;
        let session = session(&req, &settings, Some(id))?;
        let (id, rotated) = crate::services::auth_interactions::oidc::callback(
            &context,
            crate::services::auth_interactions::oidc::Callback {
                provider: *provider_id,
                state: &query.state,
                issuer: query.iss.as_deref(),
                code: query.code.as_deref(),
                error: query.error.as_deref(),
            },
            &session,
        )
        .await?;
        with_cookie(
            redirect(format!("{ENTRY}/{}", id.value())),
            cookie(&settings, Some(id), &rotated),
        )
    }
    .await
    .unwrap_or_else(failure))
}

#[utoipa::path(post, path="/api/v1/auth/interactions/browser/{id}/credentials", tag="auth", params(("id"=Uuid, Path)), request_body(content=CredentialsForm, content_type="application/x-www-form-urlencoded"), responses((status=303, description="Authenticated; challenge rotated; explicit approval required"), (status=401, description="Invalid credentials or browser binding")))]
#[post("/interactions/browser/{id}/credentials")]
pub async fn credentials(
    context: AppContext,
    req: HttpRequest,
    id: web::Path<Uuid>,
    form: web::Form<CredentialsForm>,
) -> Result<impl Responder, ApiError> {
    Ok(async {
        let settings = service::settings(&context)?;
        check_request(&req, &settings, true)?;
        let id = InteractionId::new(*id).map_err(|_| InteractionError::Invalid)?;
        let session = session(&req, &settings, Some(id))?;
        let form = form.into_inner();
        session.validate_csrf(&form.csrf)?;
        let (_, approval) = service::credentials(
            &context,
            id,
            &session,
            form.name,
            form.password,
            client_ip_for_request(&req),
        )
        .await?;
        with_cookie(
            redirect(format!("{ENTRY}/{}", id.value())),
            cookie(&settings, Some(id), &approval),
        )
    }
    .await
    .unwrap_or_else(failure))
}

#[utoipa::path(post, path="/api/v1/auth/interactions/browser/{id}/decision", tag="auth", params(("id"=Uuid, Path)), request_body(content=DecisionForm, content_type="application/x-www-form-urlencoded"), responses((status=200, description="Decision recorded; return to initiating client", body=String, content_type="text/html"), (status=303, description="Decision recorded; registered BFF handoff with ID and nonce only"), (status=401, description="Invalid browser binding")))]
#[post("/interactions/browser/{id}/decision")]
pub async fn decide(
    context: AppContext,
    req: HttpRequest,
    id: web::Path<Uuid>,
    form: web::Form<DecisionForm>,
) -> Result<impl Responder, ApiError> {
    Ok(async {
        let settings = service::settings(&context)?;
        check_request(&req, &settings, true)?;
        let id = InteractionId::new(*id).map_err(|_| InteractionError::Invalid)?;
        let session = session(&req, &settings, Some(id))?;
        session.validate_csrf(&form.csrf)?;
        let approve = matches!(form.decision, Decision::Approve);
        let destination = service::decide(&context, id, &session, approve).await?;
        let result = match destination {
            Some(destination) => redirect(destination),
            None => response(StatusCode::OK, BrowserPage::Complete { approved: approve }),
        };
        let mut old = cookie(&settings, Some(id), &session);
        old.make_removal();
        with_cookie(result, old)
    }
    .await
    .unwrap_or_else(failure))
}

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.app_data(web::FormConfig::default().limit(65536))
        .service(entry)
        .service(attach)
        .service(resume)
        .service(provider_start)
        .service(oidc_callback)
        .service(credentials)
        .service(decide);
}

#[cfg(test)]
mod tests;
