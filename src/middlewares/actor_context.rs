use actix_web::body::{BoxBody, MessageBody};
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::middleware::Next;
use actix_web::web::Data;
use actix_web::{Error, HttpMessage};

use crate::db::traits::Status;
use crate::db::{DbPool, with_actor_scope};
use crate::models::token::Token;

/// Outcome of resolving the bearer token once per request. Stored in request
/// extensions and consumed by the auth extractors so they never re-query.
enum ResolvedAuth {
    Authenticated { actor_id: i32 },
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
        Ok(token_meta) => ResolvedAuth::Authenticated {
            actor_id: token_meta.principal_id,
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
        ResolvedAuth::Authenticated { actor_id, .. } => Some(*actor_id),
        _ => None,
    };
    req.extensions_mut().insert(resolved);
    let res = with_actor_scope(actor, next.call(req)).await?;
    Ok(res.map_into_boxed_body())
}
