use chrono::SubsecRound;
use tracing::{Instrument, field, info_span};

use crate::errors::ApiError;
use crate::models::{Token, configured_token_lifetime};
use crate::storage::{
    AuthenticationStorage, StorageAuthenticatedToken, StorageAuthenticationAttempt, StorageContext,
    StorageTokenFormat, StorageTokenMigrationOutcome, storage_handle,
};

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
