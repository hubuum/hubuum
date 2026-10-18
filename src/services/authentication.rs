use chrono::SubsecRound;
use hubuum_domain::{PrincipalId, UserId};
use hubuum_events_core::{EventContext, EventSubject};
use tracing::{Instrument, field, info_span};

use crate::auth::AuthenticatedLogin;
use crate::errors::ApiError;
use crate::models::principal::load_principal_by_id;
use crate::models::token::configured_token_issuance_policy;
use crate::models::{IssuedToken, Token, configured_token_lifetime};
use crate::permissions::AppContext;
use crate::storage::{
    AuthenticationStorage, StorageAuthenticatedToken, StorageAuthenticationAttempt, StorageContext,
    StorageLogin, StorageTokenCreate, StorageTokenFormat, StorageTokenMigrationOutcome,
    StorageUser, TokenStorage, storage_handle,
};
use crate::utilities::auth::generate_token;

/// Commit a login token and immutable event-time subject using the configured
/// authorization policy, after provider authentication and identity synchronization.
pub(crate) async fn issue_login_token(
    context: &AppContext,
    authenticated: &AuthenticatedLogin,
    event_context: &EventContext,
) -> Result<IssuedToken, ApiError> {
    let user = authenticated.user();
    let principal = load_principal_by_id(context, user.id).await?;
    let is_admin = context.is_admin(user).await?;
    let subject = EventSubject::try_new(
        PrincipalId::new(principal.id)?,
        principal.name,
        principal.kind,
        authenticated.identity_scope().to_string(),
        authenticated.provider_kind().to_string(),
        is_admin,
    )
    .map_err(|error| ApiError::InternalServerError(error.to_string()))?;
    let token = generate_token();
    let request = StorageTokenCreate::for_login(
        StorageLogin::try_new(
            subject,
            StorageUser::try_new(
                UserId::new(user.id)?,
                user.password.clone(),
                user.proper_name.clone(),
                user.email.clone(),
                user.created_at.and_utc(),
                user.updated_at.and_utc(),
                user.anonymized_at.map(|at| at.and_utc()),
            )
            .map_err(|error| ApiError::InternalServerError(error.to_string()))?,
        )
        .map_err(|_| ApiError::Unauthorized("Authentication failed".into()))?,
        token.storage_digest()?,
        super::identity::token_policy(configured_token_issuance_policy()?),
        event_context,
    );
    let metadata = storage_handle(context)
        .create_token(request)
        .await?
        .into_value();
    let expires_at = metadata.expires_at().ok_or_else(|| {
        ApiError::InternalServerError(
            "newly issued token is missing its persisted expiry".to_string(),
        )
    })?;
    Ok(IssuedToken::new(token, expires_at.naive_utc()))
}

/// Validate one presented bearer token through the selected complete storage
/// backend.
///
/// Raw bearer material stays in the application authentication layer. The
/// storage contract receives only a redacted opaque lookup value and returns a
/// hash-free backend-neutral authentication projection.
pub async fn authenticate_bearer_token(
    context: &impl StorageContext,
    token: &Token,
) -> Result<StorageAuthenticatedToken, ApiError> {
    let plan = match token.credentials() {
        Ok(plan) => plan,
        Err(error) => {
            crate::observability::metrics::token_authentication(
                if token.0.starts_with("hbt") {
                    "versioned_unknown"
                } else {
                    "legacy"
                },
                "unknown",
                "rejected",
            );
            return Err(error);
        }
    };
    let format = match plan.format {
        StorageTokenFormat::Legacy => "legacy",
        StorageTokenFormat::Version1 => "version1",
    };
    let key_state = plan.key_state;
    let observed_at = chrono::Utc::now().naive_utc().trunc_subsecs(6);
    let legacy_valid_after = configured_token_lifetime()?.cutoff_from(observed_at)?;
    let attempt = StorageAuthenticationAttempt::try_candidates(
        plan.credentials,
        plan.migration_target,
        observed_at.and_utc(),
        legacy_valid_after.and_utc(),
    )
    .map_err(|error| ApiError::InternalServerError(error.to_string()))?;
    let span = info_span!(
        "auth.token_validation",
        auth.token.format = format,
        auth.token.key_state = key_state,
        auth.result = field::Empty,
    );
    match storage_handle(context)
        .authenticate_bearer_token(attempt)
        .instrument(span.clone())
        .await
    {
        Ok(authenticated) => {
            let outcome = match authenticated.migration_outcome() {
                StorageTokenMigrationOutcome::NotNeeded => "success",
                StorageTokenMigrationOutcome::Migrated => "migrated",
                StorageTokenMigrationOutcome::Conflict => "migration_conflict",
            };
            span.record("auth.result", outcome);
            crate::observability::metrics::token_authentication(format, key_state, outcome);
            Ok(authenticated)
        }
        Err(error) => {
            span.record("auth.result", "rejected");
            crate::observability::metrics::token_authentication(format, key_state, "rejected");
            Err(error.into())
        }
    }
}

/// A submitting principal whose external membership freshness and enabled
/// status have been checked for this execution. Workers require this proof
/// rather than accepting an arbitrary principal loaded from persistence.
#[derive(Debug)]
pub(crate) struct ExecutionPrincipal {
    principal: crate::models::Principal,
}

impl ExecutionPrincipal {
    pub(crate) async fn resolve(
        context: &impl StorageContext,
        principal_id: i32,
    ) -> Result<Self, ApiError> {
        crate::auth::refresh_principal_if_needed(context, principal_id).await?;
        let principal =
            crate::models::principal::load_principal_by_id(context, principal_id).await?;
        if crate::services::identity::is_service_account_disabled(context, principal_id).await? {
            return Err(ApiError::Forbidden(
                "Submitting service account is disabled; task will not run".to_string(),
            ));
        }
        Ok(Self { principal })
    }

    pub(crate) fn id(&self) -> i32 {
        self.principal.id
    }
}

impl crate::traits::PrincipalIdAccessor for ExecutionPrincipal {
    fn principal_id(&self) -> i32 {
        self.id()
    }
}

#[cfg(test)]
mod tests {
    use super::ExecutionPrincipal;
    use crate::storage::{ExternalIdentityStorage, StorageExternalUserSync, storage_handle};
    use crate::tests::TestContext;
    use diesel_async::RunQueryDsl;
    use hubuum_storage_postgres::with_transaction;

    #[actix_web::test]
    async fn execution_rejects_external_identity_without_a_refresh_provider() {
        let context = TestContext::new().await;
        let storage = storage_handle(&context.pool);
        let scope = context.scoped_name("unconfigured_execution_provider");
        let principal = storage
            .sync_external_user(
                StorageExternalUserSync::builder(
                    &scope,
                    "ldap",
                    context.scoped_name("external_subject"),
                    context.scoped_name("external_user"),
                )
                .build(),
            )
            .await
            .unwrap()
            .into_value();
        let loaded =
            crate::models::principal::load_principal_by_id(&context.pool, principal.id().id())
                .await;
        assert!(
            loaded.is_ok(),
            "the principal exists before its provider disappears"
        );
        let result = ExecutionPrincipal::resolve(&context.pool, principal.id().id()).await;
        assert!(
            result.is_err(),
            "loading a principal alone cannot authorize queued execution"
        );
        // Provider-owned users intentionally cannot be deleted through the API.
        with_transaction(&context.pool, async |connection| {
            diesel::sql_query("DELETE FROM principals WHERE id = $1")
                .bind::<diesel::sql_types::Integer, _>(principal.id().id())
                .execute(connection)
                .await?;
            diesel::sql_query("DELETE FROM identity_scopes WHERE name = $1")
                .bind::<diesel::sql_types::Text, _>(&scope)
                .execute(connection)
                .await?;
            Ok::<_, diesel::result::Error>(())
        })
        .await
        .unwrap();
    }
}
