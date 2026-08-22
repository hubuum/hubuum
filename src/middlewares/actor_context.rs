use crate::events::MutationProvenance;
use crate::middlewares::tracing::record_principal_on_current_span;
use crate::models::token::Token;
use crate::permissions::AppContext;
use crate::storage::{AuthenticatedToken, with_mutation_provenance};
use actix_web::body::{BoxBody, MessageBody};
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::middleware::Next;
use actix_web::{Error, HttpMessage};

/// Outcome of resolving the bearer token once per request. Stored in request
/// extensions and consumed by the auth extractors so they never re-query.
#[derive(Clone)]
pub(crate) enum ResolvedAuth {
    Authenticated {
        token: Token,
        token_meta: AuthenticatedToken,
    },
    Missing,
    Invalid,
}

fn bearer_token(req: &ServiceRequest) -> Option<Token> {
    let header = req.headers().get("Authorization")?.to_str().ok()?;
    header.strip_prefix("Bearer ").map(|s| Token(s.to_string()))
}

fn is_public_path(path: &str) -> bool {
    matches!(path, "/healthz" | "/readyz" | "/api/v0/auth/login")
        || (path.starts_with("/api/v1/restores/") && path.ends_with("/status"))
        || path.starts_with("/api-doc/")
        || path == "/swagger-ui"
        || path.starts_with("/swagger-ui/")
}

async fn resolve_auth(req: &ServiceRequest, backend: &AppContext) -> ResolvedAuth {
    let token = match bearer_token(req) {
        Some(token) => token,
        None => return ResolvedAuth::Missing,
    };
    match crate::services::authentication::authenticate_bearer_token(backend, &token).await {
        Ok(token_meta) => ResolvedAuth::Authenticated { token, token_meta },
        Err(_) => ResolvedAuth::Invalid,
    }
}

/// Resolve the requesting user once, stash the result in request extensions for
/// the extractors, and run the rest of the request inside a typed provenance
/// scope so every DB write attributes its history rows to that user.
pub async fn actor_context(
    req: ServiceRequest,
    next: Next<impl MessageBody + 'static>,
) -> Result<ServiceResponse<BoxBody>, Error> {
    let backend = AppContext::from_http_request(req.request())?;
    let resolved = if is_public_path(req.path()) {
        ResolvedAuth::Missing
    } else {
        resolve_auth(&req, &backend).await
    };
    let actor = match &resolved {
        ResolvedAuth::Authenticated { token_meta, .. } => Some(token_meta.principal_id()),
        _ => None,
    };
    if let Some(principal_id) = actor {
        record_principal_on_current_span(principal_id.id());
    }
    req.extensions_mut().insert(resolved);
    let provenance = actor.map(MutationProvenance::user);
    let res = with_mutation_provenance(&backend, provenance, next.call(req)).await?;
    Ok(res.map_into_boxed_body())
}

#[cfg(test)]
mod tests {
    use super::is_public_path;

    #[test]
    fn public_paths_skip_authentication_resolution() {
        for path in [
            "/healthz",
            "/readyz",
            "/api/v0/auth/login",
            "/api-doc/openapi.json",
            "/swagger-ui",
            "/swagger-ui/index.html",
        ] {
            assert!(is_public_path(path), "expected {path} to be public");
        }
        assert!(!is_public_path("/api/v0/auth/logout"));
        assert!(!is_public_path("/api/v1/classes"));
    }
}
