use crate::errors::ApiError;
use crate::events::{EventContext, RequestProvenance};
use crate::models::token::Token;
use crate::models::user::User;
use crate::models::{
    CollectionID, HubuumClassID, HubuumObjectID, MAX_OBJECT_DATA_PATCH_BYTES,
    MAX_PRINCIPAL_SETTINGS_PATCH_BYTES, ObjectDataPatchDocument, Permissions, PrincipalSettings,
    PrincipalSettingsPatch, PrincipalSettingsPatchDocument, TokenResourceScope, TokenScope,
};
use crate::permissions::{AppContext, PrincipalRef};
use crate::storage::{
    AuthenticatedToken, AuthenticationHuman, AuthenticationPrincipal, AuthenticationResourceScope,
    AuthenticationStorage, AuthenticationTokenScope, AuthenticationTokenScopeQuery, StorageContext,
    storage_handle,
};

use actix_web::{
    FromRequest, HttpMessage, HttpRequest, dev::Payload, error::JsonPayloadError, web::JsonBody,
};
use futures_util::future::{self, FutureExt};
use std::pin::Pin;
use tracing::debug;

use crate::middlewares::actor_context::ResolvedAuth;

const JSON_PATCH_MEDIA_TYPE: &str = "application/json-patch+json";
const JSON_MEDIA_TYPE: &str = "application/json";
const JSON_MERGE_PATCH_MEDIA_TYPE: &str = "application/merge-patch+json";
const SUPPORTED_PRINCIPAL_SETTINGS_PATCH_MEDIA_TYPES: &str =
    "application/json, application/merge-patch+json, or application/json-patch+json";

#[derive(Clone, Copy)]
struct PatchPayloadErrorContext {
    sentence_subject: &'static str,
    embedded_subject: &'static str,
    document_kind: &'static str,
    supported_media_types: &'static str,
}

const OBJECT_DATA_PATCH_ERROR_CONTEXT: PatchPayloadErrorContext = PatchPayloadErrorContext {
    sentence_subject: "JSON Patch",
    embedded_subject: "JSON Patch",
    document_kind: "JSON Patch",
    supported_media_types: JSON_PATCH_MEDIA_TYPE,
};

const PRINCIPAL_SETTINGS_JSON_PATCH_ERROR_CONTEXT: PatchPayloadErrorContext =
    PatchPayloadErrorContext {
        sentence_subject: "Principal settings patch",
        embedded_subject: "principal settings patch",
        document_kind: "JSON Patch",
        supported_media_types: SUPPORTED_PRINCIPAL_SETTINGS_PATCH_MEDIA_TYPES,
    };

const PRINCIPAL_SETTINGS_MERGE_PATCH_ERROR_CONTEXT: PatchPayloadErrorContext =
    PatchPayloadErrorContext {
        document_kind: "JSON Merge Patch",
        ..PRINCIPAL_SETTINGS_JSON_PATCH_ERROR_CONTEXT
    };

fn unsupported_patch_media_type(supported_media_types: &str) -> ApiError {
    ApiError::UnsupportedMediaType(format!("Content-Type must be {supported_media_types}"))
}

/// Strict JSON Patch request extractor for object-data patching.
pub struct ObjectDataPatchPayload(ObjectDataPatchDocument);

impl ObjectDataPatchPayload {
    pub fn into_inner(self) -> ObjectDataPatchDocument {
        self.0
    }
}

impl FromRequest for ObjectDataPatchPayload {
    type Error = ApiError;
    type Future = Pin<Box<dyn future::Future<Output = Result<Self, Self::Error>>>>;

    fn from_request(req: &HttpRequest, payload: &mut Payload) -> Self::Future {
        let content_type = req.mime_type().ok().flatten();

        if content_type.as_ref().map(|mime| mime.essence_str()) != Some(JSON_PATCH_MEDIA_TYPE) {
            return Box::pin(future::ready(Err(unsupported_patch_media_type(
                JSON_PATCH_MEDIA_TYPE,
            ))));
        }

        let body = JsonBody::<ObjectDataPatchDocument>::new(req, payload, None, true)
            .limit(MAX_OBJECT_DATA_PATCH_BYTES);
        Box::pin(async move {
            body.await
                .map(Self)
                .map_err(|error| patch_payload_error(error, OBJECT_DATA_PATCH_ERROR_CONTEXT))
        })
    }
}

/// Content-type-aware extractor for principal-settings merge and JSON Patch requests.
pub struct PrincipalSettingsPatchPayload(PrincipalSettingsPatch);

impl PrincipalSettingsPatchPayload {
    pub(crate) fn into_inner(self) -> PrincipalSettingsPatch {
        self.0
    }
}

impl FromRequest for PrincipalSettingsPatchPayload {
    type Error = ApiError;
    type Future = Pin<Box<dyn future::Future<Output = Result<Self, Self::Error>>>>;

    fn from_request(req: &HttpRequest, payload: &mut Payload) -> Self::Future {
        let content_type = req.mime_type().ok().flatten();

        match content_type.as_ref().map(|mime| mime.essence_str()) {
            Some(JSON_PATCH_MEDIA_TYPE) => {
                let body =
                    JsonBody::<PrincipalSettingsPatchDocument>::new(req, payload, None, true)
                        .limit(MAX_PRINCIPAL_SETTINGS_PATCH_BYTES);
                Box::pin(async move {
                    body.await
                        .map(|patch| Self(PrincipalSettingsPatch::JsonPatch(patch)))
                        .map_err(|error| {
                            patch_payload_error(error, PRINCIPAL_SETTINGS_JSON_PATCH_ERROR_CONTEXT)
                        })
                })
            }
            Some(JSON_MEDIA_TYPE | JSON_MERGE_PATCH_MEDIA_TYPE) => {
                let body = JsonBody::<PrincipalSettings>::new(req, payload, None, true)
                    .limit(MAX_PRINCIPAL_SETTINGS_PATCH_BYTES);
                Box::pin(async move {
                    body.await
                        .map(|patch| Self(PrincipalSettingsPatch::MergePatch(patch)))
                        .map_err(|error| {
                            patch_payload_error(error, PRINCIPAL_SETTINGS_MERGE_PATCH_ERROR_CONTEXT)
                        })
                })
            }
            _ => Box::pin(future::ready(Err(unsupported_patch_media_type(
                SUPPORTED_PRINCIPAL_SETTINGS_PATCH_MEDIA_TYPES,
            )))),
        }
    }
}

fn patch_payload_error(error: JsonPayloadError, context: PatchPayloadErrorContext) -> ApiError {
    match error {
        JsonPayloadError::OverflowKnownLength { length, limit } => {
            ApiError::PayloadTooLarge(format!(
                "{} payload is {length} bytes; the limit is {limit} bytes",
                context.sentence_subject
            ))
        }
        JsonPayloadError::Overflow { limit } => ApiError::PayloadTooLarge(format!(
            "{} payload exceeded the {limit} byte limit",
            context.sentence_subject
        )),
        JsonPayloadError::ContentType => {
            unsupported_patch_media_type(context.supported_media_types)
        }
        JsonPayloadError::Deserialize(error) => ApiError::BadRequest(format!(
            "Invalid {} document: {error}",
            context.document_kind
        )),
        JsonPayloadError::Serialize(error) => ApiError::InternalServerError(format!(
            "Unexpected {} serialization error: {error}",
            context.embedded_subject
        )),
        JsonPayloadError::Payload(error) => ApiError::BadRequest(format!(
            "Could not read {} payload: {error}",
            context.embedded_subject
        )),
        _ => ApiError::BadRequest(format!(
            "Could not read {} payload",
            context.embedded_subject
        )),
    }
}

/// The principal-centric authenticated context for resource and task flows.
///
/// This is the ONLY extractor that accepts scoped tokens — every authority
/// decision downstream threads `scopes()` into the authz pre-filter. Humans and
/// service accounts both authenticate here.
pub struct Authenticated {
    /// The raw bearer token (e.g. for current-token logout).
    pub token: Token,
    pub token_meta: AuthenticatedToken,
    pub principal: AuthenticationPrincipal,
    /// `None` = unscoped (full principal authority); `Some(..)` = the token's
    /// permission and/or resource narrowing boundary.
    pub scope: Option<TokenScope>,
}

impl Authenticated {
    /// The complete token scope, for passing into authorization entry points.
    pub fn scopes(&self) -> Option<&TokenScope> {
        self.scope.as_ref()
    }
}

/// A human user with a valid, **unscoped** token. Scoped tokens and service
/// accounts are rejected.
pub struct UserAccess {
    pub user: User,
}

/// A human admin with a valid, unscoped token.
pub struct AdminAccess {
    pub user: User,
}

/// A human admin, or the human user named by the `{principal_id}`/`{user_id}`
/// path segment, with a valid unscoped token.
pub struct AdminOrSelfAccess {
    pub user: User,
}

/// A human user with a valid unscoped token, for IAM / credential-management
/// endpoints (service-account CRUD, principal token management, admin logout).
/// Per-operation authorization (admin or owner-group) is decided in the handler.
/// Scoped automation tokens can never manage SAs, users, groups, or credentials.
pub struct ManagementAccess {
    pub user: User,
}

pub trait AccessEventContext {
    fn event_context(&self, req: &HttpRequest) -> EventContext;
}

impl AccessEventContext for Authenticated {
    fn event_context(&self, req: &HttpRequest) -> EventContext {
        user_event_context(req, self.principal.id().id())
    }
}

impl AccessEventContext for UserAccess {
    fn event_context(&self, req: &HttpRequest) -> EventContext {
        user_event_context(req, self.user.id)
    }
}

impl AccessEventContext for AdminAccess {
    fn event_context(&self, req: &HttpRequest) -> EventContext {
        user_event_context(req, self.user.id)
    }
}

impl AccessEventContext for AdminOrSelfAccess {
    fn event_context(&self, req: &HttpRequest) -> EventContext {
        user_event_context(req, self.user.id)
    }
}

impl AccessEventContext for ManagementAccess {
    fn event_context(&self, req: &HttpRequest) -> EventContext {
        user_event_context(req, self.user.id)
    }
}

fn user_event_context(req: &HttpRequest, actor_user_id: i32) -> EventContext {
    RequestProvenance::from_request(req)
        .map(|provenance| provenance.user_event_context(actor_user_id))
        .unwrap_or_else(|| {
            EventContext::user(
                hubuum_domain::PrincipalId::new(actor_user_id)
                    .expect("authenticated principal id must be positive"),
                None,
                None,
            )
        })
}

fn extract_token(req: &HttpRequest) -> Result<Token, ApiError> {
    req.headers()
        .get("Authorization")
        .and_then(|header| header.to_str().ok())
        .and_then(|header_str| {
            header_str
                .strip_prefix("Bearer ")
                .map(|header_str: &str| header_str.to_string())
        })
        .map(Token)
        .ok_or_else(|| ApiError::Unauthorized("No token provided".to_string()))
}

fn backend_from_req(req: &HttpRequest) -> Result<AppContext, ApiError> {
    AppContext::from_http_request(req)
}

async fn selected_backend_is_admin(context: &AppContext, user: &User) -> Result<bool, ApiError> {
    let principal = PrincipalRef::load(context, user).await?;
    context.permission_backend().is_admin(&principal).await
}

/// Build the full authenticated context (accepts scoped tokens).
async fn build_authenticated(
    backend: &impl StorageContext,
    token: Token,
) -> Result<Authenticated, ApiError> {
    let token_meta =
        crate::services::authentication::authenticate_bearer_token(backend, &token).await?;
    build_authenticated_from_meta(backend, token, token_meta).await
}

async fn build_authenticated_from_meta(
    backend: &impl StorageContext,
    token: Token,
    token_meta: AuthenticatedToken,
) -> Result<Authenticated, ApiError> {
    crate::auth::refresh_principal_if_needed(backend, token_meta.principal_id().id()).await?;
    let storage = storage_handle(backend);
    let identity = storage
        .load_authentication_identity(token_meta.principal_id())
        .await?;
    let (principal, _) = identity.into_parts();
    let scope = storage
        .load_authentication_token_scope(AuthenticationTokenScopeQuery::new(
            token_meta.id(),
            token_meta.is_permission_scoped(),
            token_meta.is_resource_scoped(),
        ))
        .await?
        .map(token_scope_from_storage)
        .transpose()?;
    Ok(Authenticated {
        token,
        token_meta,
        principal,
        scope,
    })
}

fn resolved_auth(req: &HttpRequest, token: &Token) -> Option<AuthenticatedToken> {
    match req.extensions().get::<ResolvedAuth>() {
        Some(ResolvedAuth::Authenticated {
            token: resolved_token,
            token_meta,
        }) if resolved_token.0 == token.0 => Some(token_meta.clone()),
        _ => None,
    }
}

/// Gate for human/IAM extractors: the token must be valid, **unscoped**, and
/// owned by a **human** principal. Returns the resolved `User`.
///
/// This is the privilege-separation keystone — it runs before any admin/self
/// decision, so a service account (even one in the admin group, even with an
/// unscoped token) can never act through a human/IAM extractor.
async fn human_unscoped_user_from_meta(
    backend: &impl StorageContext,
    token_meta: AuthenticatedToken,
) -> Result<User, ApiError> {
    if token_meta.is_scoped() {
        return Err(ApiError::Forbidden(
            "Scoped tokens cannot be used on human/management endpoints".to_string(),
        ));
    }

    crate::auth::refresh_principal_if_needed(backend, token_meta.principal_id().id()).await?;

    // Single round trip: fetch the backend-neutral principal projection and,
    // when human, a password-free human projection from the same snapshot.
    let storage = storage_handle(backend);
    let identity = storage
        .load_authentication_identity(token_meta.principal_id())
        .await?;
    let (principal, human) = identity.into_parts();
    if !principal.is_human() {
        return Err(ApiError::Forbidden(
            "Service accounts cannot use human/management endpoints".to_string(),
        ));
    }

    human
        .map(authentication_human_to_user)
        .ok_or_else(|| ApiError::Unauthorized("Invalid token".to_string()))
}

fn authentication_human_to_user(human: AuthenticationHuman) -> User {
    let (id, proper_name, email, created_at, updated_at, anonymized_at) = human.into_parts();
    User {
        id: id.id(),
        kind: "human".to_string(),
        password: None,
        proper_name,
        email,
        created_at,
        updated_at,
        anonymized_at,
    }
}

fn token_scope_from_storage(scope: AuthenticationTokenScope) -> Result<TokenScope, ApiError> {
    let (permissions, resources) = scope.into_parts();
    let permissions = permissions
        .map(|permissions| {
            permissions
                .iter()
                .map(|permission| Permissions::from_string(permission))
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?;
    let resources = resources
        .map(authentication_resources_from_storage)
        .transpose()?;

    TokenScope::from_stored_parts(permissions, resources)
}

fn authentication_resources_from_storage(
    resources: AuthenticationResourceScope,
) -> Result<Vec<TokenResourceScope>, ApiError> {
    let (collection_ids, class_ids, object_ids) = resources.into_parts();
    collection_ids
        .into_iter()
        .map(|id| {
            CollectionID::new(id.id())
                .map(TokenResourceScope::Collection)
                .map_err(ApiError::from)
        })
        .chain(class_ids.into_iter().map(|id| {
            HubuumClassID::new(id.id())
                .map(TokenResourceScope::Class)
                .map_err(ApiError::from)
        }))
        .chain(object_ids.into_iter().map(|id| {
            HubuumObjectID::new(id.id())
                .map(TokenResourceScope::Object)
                .map_err(ApiError::from)
        }))
        .collect()
}

async fn human_unscoped_user(
    backend: &impl StorageContext,
    token: &Token,
) -> Result<User, ApiError> {
    let token_meta =
        crate::services::authentication::authenticate_bearer_token(backend, token).await?;
    human_unscoped_user_from_meta(backend, token_meta).await
}

/// Resolve the self-target principal id from the path (`principal_id` preferred,
/// `user_id` accepted for not-yet-renamed routes).
fn self_target_id(path: &actix_web::dev::Path<actix_web::dev::Url>) -> Result<i32, ApiError> {
    if let Ok(id) = path.query("principal_id").parse::<i32>() {
        return Ok(id);
    }
    path.query("user_id")
        .parse::<i32>()
        .map_err(|_| ApiError::InternalServerError("Failed to parse principal id".into()))
}

impl FromRequest for Authenticated {
    type Error = ApiError;
    type Future = Pin<Box<dyn future::Future<Output = Result<Self, Self::Error>>>>;

    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        let backend = backend_from_req(req);
        let token_result = extract_token(req);
        let token_meta = token_result
            .as_ref()
            .ok()
            .and_then(|token| resolved_auth(req, token));
        async move {
            let backend = backend?;
            let token = token_result?;
            match token_meta {
                Some(token_meta) => {
                    build_authenticated_from_meta(&backend, token, token_meta).await
                }
                None => build_authenticated(&backend, token).await,
            }
        }
        .boxed_local()
    }
}

impl FromRequest for UserAccess {
    type Error = ApiError;
    type Future = Pin<Box<dyn future::Future<Output = Result<Self, Self::Error>>>>;

    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        let backend = backend_from_req(req);
        let token_result = extract_token(req);
        let token_meta = token_result
            .as_ref()
            .ok()
            .and_then(|token| resolved_auth(req, token));
        async move {
            let backend = backend?;
            let token = token_result?;
            let user = match token_meta {
                Some(token_meta) => human_unscoped_user_from_meta(&backend, token_meta).await?,
                None => human_unscoped_user(&backend, &token).await?,
            };
            Ok(UserAccess { user })
        }
        .boxed_local()
    }
}

impl FromRequest for ManagementAccess {
    type Error = ApiError;
    type Future = Pin<Box<dyn future::Future<Output = Result<Self, Self::Error>>>>;

    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        let backend = backend_from_req(req);
        let token_result = extract_token(req);
        let token_meta = token_result
            .as_ref()
            .ok()
            .and_then(|token| resolved_auth(req, token));
        async move {
            let backend = backend?;
            let token = token_result?;
            let user = match token_meta {
                Some(token_meta) => human_unscoped_user_from_meta(&backend, token_meta).await?,
                None => human_unscoped_user(&backend, &token).await?,
            };
            Ok(ManagementAccess { user })
        }
        .boxed_local()
    }
}

impl FromRequest for AdminAccess {
    type Error = ApiError;
    type Future = Pin<Box<dyn future::Future<Output = Result<Self, Self::Error>>>>;

    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        let backend = backend_from_req(req);
        let token_result = extract_token(req);
        let token_meta = token_result
            .as_ref()
            .ok()
            .and_then(|token| resolved_auth(req, token));
        async move {
            let backend = backend?;
            let token = token_result?;
            let user = match token_meta {
                Some(token_meta) => human_unscoped_user_from_meta(&backend, token_meta).await?,
                None => human_unscoped_user(&backend, &token).await?,
            };

            if selected_backend_is_admin(&backend, &user).await? {
                Ok(AdminAccess { user })
            } else {
                Err(ApiError::Forbidden("Permission denied".to_string()))
            }
        }
        .boxed_local()
    }
}

impl FromRequest for AdminOrSelfAccess {
    type Error = ApiError;
    type Future = Pin<Box<dyn future::Future<Output = Result<Self, Self::Error>>>>;

    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        let backend = backend_from_req(req);
        let token_result = extract_token(req);
        let token_meta = token_result
            .as_ref()
            .ok()
            .and_then(|token| resolved_auth(req, token));
        let path_info = req.match_info().clone();

        async move {
            let backend = backend?;
            let token = token_result?;
            let user = match token_meta {
                Some(token_meta) => human_unscoped_user_from_meta(&backend, token_meta).await?,
                None => human_unscoped_user(&backend, &token).await?,
            };
            let target_id = self_target_id(&path_info)?;

            if selected_backend_is_admin(&backend, &user).await? || user.id == target_id {
                Ok(AdminOrSelfAccess { user })
            } else {
                debug! {
                    message = "User attempted to access an admin-or-self resource.",
                    user_id = user.id,
                    target_id = target_id,
                };
                Err(ApiError::Forbidden("Permission denied".to_string()))
            }
        }
        .boxed_local()
    }
}
