use std::collections::HashMap;

use hubuum_storage_core::StorageIdentityScope;

use crate::errors::ApiError;
use crate::models::{IdentityScope, ResourceRevision};
use crate::storage::StorageError;
use crate::storage::postgres::{PostgresPool, with_connection};

fn identity_scope_from_storage(scope: StorageIdentityScope) -> Result<IdentityScope, ApiError> {
    let revision = ResourceRevision::new(scope.revision()).map_err(|_| {
        ApiError::InternalServerError("Storage returned an invalid identity-scope revision".into())
    })?;
    Ok(IdentityScope {
        id: scope.id(),
        name: scope.name().to_string(),
        provider_kind: scope.provider_kind().to_string(),
        created_at: scope.created_at(),
        updated_at: scope.updated_at(),
        revision,
    })
}

pub async fn identity_scope_by_name(
    pool: &PostgresPool,
    scope_name: &str,
) -> Result<IdentityScope, ApiError> {
    let scope = with_connection(pool, async |connection| {
        hubuum_storage_postgres::operations::identity_scope::identity_scope_by_name_on_connection(
            connection, scope_name,
        )
        .await
        .map_err(StorageError::from)
        .map_err(ApiError::from)
    })
    .await?;
    identity_scope_from_storage(scope)
}

pub async fn identity_scope_names_by_ids(
    pool: &PostgresPool,
    scope_ids: &[i32],
) -> Result<HashMap<i32, String>, ApiError> {
    with_connection(pool, async |connection| {
        hubuum_storage_postgres::operations::identity_scope::identity_scope_names_by_ids_on_connection(
            connection,
            scope_ids,
        )
        .await
        .map_err(StorageError::from)
        .map_err(ApiError::from)
    })
    .await
}

pub async fn ensure_identity_scope(
    pool: &PostgresPool,
    scope_name: &str,
    provider: &str,
) -> Result<IdentityScope, ApiError> {
    let scope = with_connection(pool, async |connection| {
        hubuum_storage_postgres::operations::identity_scope::ensure_identity_scope_on_connection(
            connection, scope_name, provider,
        )
        .await
        .map_err(StorageError::from)
        .map_err(ApiError::from)
    })
    .await?;
    identity_scope_from_storage(scope)
}
