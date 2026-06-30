use actix_web::body::{BoxBody, MessageBody};
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::middleware::Next;
use actix_web::web::Data;
use actix_web::{Error, HttpMessage};

use crate::db::traits::Status;
use crate::db::{with_actor_scope, DbPool};
use crate::models::token::Token;
use crate::models::user::User;
use crate::utilities::iam::get_user_by_id;

/// Outcome of resolving the bearer token once per request. Stored in request
/// extensions and consumed by the auth extractors so they never re-query.
#[derive(Clone)]
pub enum ResolvedAuth {
    Authenticated { token: Token, user: User },
    Missing,
    Invalid,
}

fn bearer_token(req: &ServiceRequest) -> Option<Token> {
    let header = req.headers().get("Authorization")?.to_str().ok()?;
    header.strip_prefix("Bearer ").map(|s| Token(s.to_string()))
}

async fn resolve_auth(req: &ServiceRequest) -> ResolvedAuth {
    let token = match bearer_token(req) {
        Some(token) => token,
        None => return ResolvedAuth::Missing,
    };
    let pool = match req.app_data::<Data<DbPool>>() {
        Some(pool) => pool.clone(),
        None => return ResolvedAuth::Invalid,
    };
    match token.is_valid(&pool).await {
        Ok(user_token) => match get_user_by_id(&pool, user_token.user_id) {
            Ok(user) => ResolvedAuth::Authenticated { token, user },
            Err(_) => ResolvedAuth::Invalid,
        },
        Err(_) => ResolvedAuth::Invalid,
    }
}

/// Resolve the requesting user once, stash the result in request extensions for
/// the extractors, and run the rest of the request inside a `with_actor_scope`
/// so every DB write attributes its history rows to that user.
pub async fn actor_context(
    req: ServiceRequest,
    next: Next<impl MessageBody + 'static>,
) -> Result<ServiceResponse<BoxBody>, Error> {
    let resolved = resolve_auth(&req).await;
    let actor = match &resolved {
        ResolvedAuth::Authenticated { user, .. } => Some(user.id),
        _ => None,
    };
    req.extensions_mut().insert(resolved);
    let res = with_actor_scope(actor, next.call(req)).await?;
    Ok(res.map_into_boxed_body())
}
