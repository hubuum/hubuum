use hubuum_auth_core::{AuthenticatedExternalUser, ExternalGroup, ExternalUserProfile};
use hubuum_storage_core::{
    AuthenticationTokenScope, StorageExternalGroup, StorageExternalPrincipalState,
    StorageExternalUserSync, StorageSyncedHuman,
};

use crate::errors::ApiError;
use crate::models::{
    CollectionID, HubuumClassID, HubuumObjectID, Permissions, TokenResourceScope, TokenScope,
};
use crate::storage::postgres::PostgresPool;

pub(crate) fn token_scope_from_storage(
    scope: AuthenticationTokenScope,
) -> Result<TokenScope, ApiError> {
    let (permissions, resources) = scope.into_parts();
    let permissions = permissions
        .map(|permissions| {
            permissions
                .into_iter()
                .map(|permission| Permissions::from_string(&permission))
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?;
    let resources = resources
        .map(|resources| {
            let (collections, classes, objects) = resources.into_parts();
            collections
                .into_iter()
                .map(|id| CollectionID::new(id).map(TokenResourceScope::Collection))
                .chain(
                    classes
                        .into_iter()
                        .map(|id| HubuumClassID::new(id).map(TokenResourceScope::Class)),
                )
                .chain(
                    objects
                        .into_iter()
                        .map(|id| HubuumObjectID::new(id).map(TokenResourceScope::Object)),
                )
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?;
    TokenScope::from_stored_parts(permissions, resources)
}

pub(crate) async fn external_principal_state(
    pool: &PostgresPool,
    principal_id: i32,
) -> Result<Option<StorageExternalPrincipalState>, ApiError> {
    Ok(
        super::external_identity::external_principal_state(pool, principal_id)
            .await?
            .map(|state| {
                StorageExternalPrincipalState::new(
                    state.identity_scope,
                    state.username,
                    state.external_subject,
                    state.last_sync_attempted_at,
                    state.last_sync_success_at,
                )
            }),
    )
}

pub(crate) async fn mark_external_sync_attempted(
    pool: &PostgresPool,
    principal_id: i32,
) -> Result<(), ApiError> {
    super::external_identity::mark_external_sync_attempted(pool, principal_id).await
}

fn external_group(group: &StorageExternalGroup) -> ExternalGroup {
    ExternalGroup {
        key: group.key().to_string(),
        name: group.name().to_string(),
        description: group.description().map(str::to_string),
    }
}

pub(crate) async fn sync_external_user(
    pool: &PostgresPool,
    request: StorageExternalUserSync,
) -> Result<StorageSyncedHuman, ApiError> {
    let authenticated = AuthenticatedExternalUser {
        profile: ExternalUserProfile {
            subject: request.subject().to_string(),
            name: request.name().to_string(),
            proper_name: request.proper_name().map(str::to_string),
            email: request.email().map(str::to_string),
        },
        groups: request.groups().iter().map(external_group).collect(),
    };
    let user = super::external_identity::sync_external_user(
        pool,
        request.identity_scope(),
        request.provider_kind(),
        authenticated,
    )
    .await?;
    Ok(StorageSyncedHuman::new(
        user.id,
        user.proper_name,
        user.email,
        user.created_at,
        user.updated_at,
        user.anonymized_at,
    ))
}
