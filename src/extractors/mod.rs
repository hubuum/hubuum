use crate::db::traits::Status;
use crate::db::traits::authz::{AuthzSubject, load_token_scopes};
use crate::db::{DbPool, with_connection};
use crate::errors::ApiError;
use crate::events::{EventContext, RequestProvenance};
use crate::models::permissions::Permissions;
use crate::models::principal::{Principal, load_principal_by_id};
use crate::models::token::{PrincipalToken, Token};
use crate::models::user::User;

use actix_web::{FromRequest, HttpMessage, HttpRequest, dev::Payload, web::Data};
use diesel::prelude::*;
use futures_util::future::{self, FutureExt};
use std::pin::Pin;
use tracing::debug;

use crate::middlewares::actor_context::ResolvedAuth;

/// The principal-centric authenticated context for resource and task flows.
///
/// This is the ONLY extractor that accepts scoped tokens — every authority
/// decision downstream threads `scopes()` into the authz pre-filter. Humans and
/// service accounts both authenticate here.
pub struct Authenticated {
    /// The raw bearer token (e.g. for current-token logout).
    pub token: Token,
    pub token_meta: PrincipalToken,
    pub principal: Principal,
    /// `None` = unscoped (full principal authority); `Some(..)` = the token's
    /// scope set (possibly empty = deny-all).
    pub scopes: Option<Vec<Permissions>>,
}

impl Authenticated {
    /// The token scope set as a slice, for passing into authz entry points.
    pub fn scopes(&self) -> Option<&[Permissions]> {
        self.scopes.as_deref()
    }
}

/// A human user with a valid, **unscoped** token. Scoped tokens and service
/// accounts are rejected.
pub struct UserAccess {
    pub user: User,
}

/// A human admin with a valid, unscoped token.
pub struct AdminAccess {
    pub token: Token,
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
    pub token: Token,
    pub user: User,
}

pub trait AccessEventContext {
    fn event_context(&self, req: &HttpRequest) -> EventContext;
}

impl AccessEventContext for Authenticated {
    fn event_context(&self, req: &HttpRequest) -> EventContext {
        user_event_context(req, self.principal.id)
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
        .unwrap_or_else(|| EventContext::user(actor_user_id, None, None))
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

fn pool_from_req(req: &HttpRequest) -> Result<Data<DbPool>, ApiError> {
    req.app_data::<Data<DbPool>>()
        .cloned()
        .ok_or_else(|| ApiError::InternalServerError("Pool not found".to_string()))
}

/// Build the full authenticated context (accepts scoped tokens).
async fn build_authenticated(pool: &DbPool, token: Token) -> Result<Authenticated, ApiError> {
    let token_meta = token.is_valid(pool).await?;
    build_authenticated_from_meta(pool, token, token_meta).await
}

async fn build_authenticated_from_meta(
    pool: &DbPool,
    token: Token,
    token_meta: PrincipalToken,
) -> Result<Authenticated, ApiError> {
    let principal = load_principal_by_id(pool, token_meta.principal_id).await?;
    let scopes = if token_meta.scoped {
        Some(load_token_scopes(pool, token_meta.id).await?)
    } else {
        None
    };
    Ok(Authenticated {
        token,
        token_meta,
        principal,
        scopes,
    })
}

fn resolved_auth(req: &HttpRequest, token: &Token) -> Option<PrincipalToken> {
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
    pool: &DbPool,
    token_meta: PrincipalToken,
) -> Result<User, ApiError> {
    if token_meta.scoped {
        return Err(ApiError::Forbidden(
            "Scoped tokens cannot be used on human/management endpoints".to_string(),
        ));
    }

    // Single round trip: fetch the principal and (when human) its `users` row
    // together, rather than a separate principal load followed by a user load.
    let (principal, user) = load_principal_with_user(pool, token_meta.principal_id).await?;
    if !principal.is_human() {
        return Err(ApiError::Forbidden(
            "Service accounts cannot use human/management endpoints".to_string(),
        ));
    }

    user.ok_or_else(|| ApiError::Unauthorized("Invalid token".to_string()))
}

async fn human_unscoped_user(pool: &DbPool, token: &Token) -> Result<User, ApiError> {
    let token_meta = token.is_valid(pool).await?;
    human_unscoped_user_from_meta(pool, token_meta).await
}

/// Load a principal and, when it is human, its `users` row in one left-joined
/// query. A service account simply has no `users` row, so the user is `None`.
async fn load_principal_with_user(
    pool: &DbPool,
    principal_id: i32,
) -> Result<(Principal, Option<User>), ApiError> {
    use crate::schema::{principals, users};

    with_connection(pool, |conn| {
        principals::table
            .left_join(users::table.on(users::id.eq(principals::id)))
            .filter(principals::id.eq(principal_id))
            .select((Principal::as_select(), Option::<User>::as_select()))
            .first::<(Principal, Option<User>)>(conn)
    })
}

/// Resolve the self-target principal id from the path. Principal routes use
/// `principal_id`; user routes use `user_id`.
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
        let pool = pool_from_req(req);
        let token_result = extract_token(req);
        let token_meta = token_result
            .as_ref()
            .ok()
            .and_then(|token| resolved_auth(req, token));
        async move {
            let pool = pool?;
            let token = token_result?;
            match token_meta {
                Some(token_meta) => build_authenticated_from_meta(&pool, token, token_meta).await,
                None => build_authenticated(&pool, token).await,
            }
        }
        .boxed_local()
    }
}

impl FromRequest for UserAccess {
    type Error = ApiError;
    type Future = Pin<Box<dyn future::Future<Output = Result<Self, Self::Error>>>>;

    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        let pool = pool_from_req(req);
        let token_result = extract_token(req);
        let token_meta = token_result
            .as_ref()
            .ok()
            .and_then(|token| resolved_auth(req, token));
        async move {
            let pool = pool?;
            let token = token_result?;
            let user = match token_meta {
                Some(token_meta) => human_unscoped_user_from_meta(&pool, token_meta).await?,
                None => human_unscoped_user(&pool, &token).await?,
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
        let pool = pool_from_req(req);
        let token_result = extract_token(req);
        let token_meta = token_result
            .as_ref()
            .ok()
            .and_then(|token| resolved_auth(req, token));
        async move {
            let pool = pool?;
            let token = token_result?;
            let user = match token_meta {
                Some(token_meta) => human_unscoped_user_from_meta(&pool, token_meta).await?,
                None => human_unscoped_user(&pool, &token).await?,
            };
            Ok(ManagementAccess { token, user })
        }
        .boxed_local()
    }
}

impl FromRequest for AdminAccess {
    type Error = ApiError;
    type Future = Pin<Box<dyn future::Future<Output = Result<Self, Self::Error>>>>;

    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        let pool = pool_from_req(req);
        let token_result = extract_token(req);
        let token_meta = token_result
            .as_ref()
            .ok()
            .and_then(|token| resolved_auth(req, token));
        async move {
            let pool = pool?;
            let token = token_result?;
            let user = match token_meta {
                Some(token_meta) => human_unscoped_user_from_meta(&pool, token_meta).await?,
                None => human_unscoped_user(&pool, &token).await?,
            };

            if user.is_admin(&pool).await? {
                Ok(AdminAccess { token, user })
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
        let pool = pool_from_req(req);
        let token_result = extract_token(req);
        let token_meta = token_result
            .as_ref()
            .ok()
            .and_then(|token| resolved_auth(req, token));
        let path_info = req.match_info().clone();

        async move {
            let pool = pool?;
            let token = token_result?;
            let user = match token_meta {
                Some(token_meta) => human_unscoped_user_from_meta(&pool, token_meta).await?,
                None => human_unscoped_user(&pool, &token).await?,
            };
            let target_id = self_target_id(&path_info)?;

            if user.is_admin(&pool).await? || user.id == target_id {
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
