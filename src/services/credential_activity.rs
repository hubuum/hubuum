use crate::{permissions::AppContext, storage::storage_handle};
use actix_web::{HttpMessage, HttpRequest};
use hubuum_domain::TokenId;
use hubuum_storage_core::{
    AuthenticationStorage, CredentialIssuanceMethod, ObservedSource, StorageCredentialCreation,
};

pub fn request_source(request: &HttpRequest) -> ObservedSource {
    request
        .extensions()
        .get::<ObservedSource>()
        .copied()
        .unwrap_or(ObservedSource::Unavailable)
}
pub async fn observe(context: &AppContext, token: TokenId, source: ObservedSource) {
    let result = async {
        let policy = crate::config::get_config()?.credential_activity_policy()?;
        storage_handle(context)
            .observe_credential_source(token, source, policy)
            .await?;
        Ok::<(), crate::errors::ApiError>(())
    }
    .await;
    if let Err(error) = result {
        tracing::warn!(
            message = "Credential activity recording failed",
            token_id = token.id(),
            reason = error.class()
        );
        crate::observability::metrics::token_authentication(
            "activity",
            "unknown",
            "recording_failed",
        );
    }
}
pub async fn creation(
    context: &AppContext,
    token: TokenId,
    mut creation: StorageCredentialCreation,
) {
    let result = async {
        if !crate::config::get_config()?.credential_activity_enabled {
            creation = StorageCredentialCreation::new(
                creation.issuer(),
                creation.method(),
                ObservedSource::Unavailable,
                creation.issued_at(),
            );
        }
        storage_handle(context)
            .record_credential_creation(token, creation)
            .await?;
        Ok::<(), crate::errors::ApiError>(())
    }
    .await;
    if let Err(error) = result {
        tracing::warn!(
            message = "Credential creation provenance recording failed",
            token_id = token.id(),
            reason = error.class()
        );
        crate::observability::metrics::token_authentication(
            "activity",
            "unknown",
            "recording_failed",
        );
    }
}

pub fn issuance_method(
    operation: &crate::models::credential_approval::CredentialOperation,
) -> CredentialIssuanceMethod {
    match operation {
        crate::models::credential_approval::CredentialOperation::RenewToken { .. } => {
            CredentialIssuanceMethod::Renewal
        }
        _ => CredentialIssuanceMethod::Creation,
    }
}
