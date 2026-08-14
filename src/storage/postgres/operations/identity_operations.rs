use hubuum_auth_core::{AuthenticatedExternalUser, ExternalGroup, ExternalUserProfile};
use hubuum_storage_core::{
    AuthenticationResourceScope, AuthenticationTokenScope, StorageExternalGroup,
    StorageExternalPrincipalState, StorageExternalUserSync, StorageIdentityPage,
    StorageServiceAccount, StorageServiceAccountCreate, StorageServiceAccountListItem,
    StorageServiceAccountListQuery, StorageServiceAccountMutation, StorageServiceAccountPoint,
    StorageServiceAccountUpdate, StorageSyncedHuman, StorageTokenCreate, StorageTokenHashRevoke,
    StorageTokenListQuery, StorageTokenListState, StorageTokenMetadata, StorageTokenRenew,
    StorageTokenRevoke,
};

use crate::errors::ApiError;
use crate::models::{
    CollectionID, GroupID, HubuumClassID, HubuumObjectID, NewServiceAccount, Permissions,
    PrincipalID, PrincipalTokenCreateParts, PrincipalTokenMetadata, ServiceAccount,
    ServiceAccountID, ServiceAccountWithName, TokenIssuancePolicy, TokenListState,
    TokenResourceScope, TokenScope, UpdateServiceAccount,
};
use crate::pagination::count_query_options;
use crate::storage::postgres::PostgresPool;
use crate::storage::postgres::operations::service_account::{
    DisableServiceAccount, SaveServiceAccount,
    delete_service_account as delete_service_account_record, update_service_account_record,
};
fn storage_service_account(account: ServiceAccount) -> StorageServiceAccount {
    StorageServiceAccount::new(
        account.id,
        account.description,
        account.owner_group_id,
        account.created_by,
        account.disabled_at,
        account.created_at,
        account.updated_at,
    )
}

fn storage_service_account_list_item(
    item: ServiceAccountWithName,
) -> StorageServiceAccountListItem {
    StorageServiceAccountListItem::new(
        storage_service_account(item.service_account),
        item.identity_scope,
        item.name,
        item.revision.get(),
    )
}

fn storage_token_state(state: StorageTokenListState) -> TokenListState {
    match state {
        StorageTokenListState::Active => TokenListState::Active,
        StorageTokenListState::Expired => TokenListState::Expired,
        StorageTokenListState::Revoked => TokenListState::Revoked,
        StorageTokenListState::All => TokenListState::All,
    }
}

fn storage_token_scope(scope: crate::models::TokenScopeDetails) -> AuthenticationTokenScope {
    let permissions = scope.permissions().map(|permissions| {
        permissions
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
    });
    let resources = scope.resources().map(|resources| {
        let mut collections = Vec::new();
        let mut classes = Vec::new();
        let mut objects = Vec::new();
        for resource in resources {
            match resource {
                TokenResourceScope::Collection(id) => collections.push(id.id()),
                TokenResourceScope::Class(id) => classes.push(id.id()),
                TokenResourceScope::Object(id) => objects.push(id.id()),
            }
        }
        AuthenticationResourceScope::new(collections, classes, objects)
    });
    AuthenticationTokenScope::new(permissions, resources)
}

fn storage_token_metadata(metadata: PrincipalTokenMetadata) -> StorageTokenMetadata {
    StorageTokenMetadata::builder(
        metadata.id.id(),
        metadata.principal_id.id(),
        metadata.issued,
        metadata.revision.get(),
    )
    .name(metadata.name)
    .description(metadata.description)
    .expires_at(metadata.expires_at)
    .last_used_at(metadata.last_used_at)
    .revoked_at(metadata.revoked_at)
    .active(metadata.active)
    .expired(metadata.expired)
    .scope(metadata.scope.map(storage_token_scope))
    .build()
}

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

pub(crate) async fn list_retained_tokens(
    pool: &PostgresPool,
    query: StorageTokenListQuery,
) -> Result<StorageIdentityPage<StorageTokenMetadata>, ApiError> {
    let (principal_id, options, state) = query.into_parts();
    let include_total = options.include_total;
    let (rows, total) =
        super::active_tokens::retained_token_metadata_by_principal_id_paginated_with_total_count(
            PrincipalID::new(principal_id)?,
            pool,
            &options,
            storage_token_state(state),
        )
        .await?;
    Ok(StorageIdentityPage::new(
        rows.into_iter().map(storage_token_metadata).collect(),
        include_total.then_some(total),
    ))
}

pub(crate) async fn load_service_account(
    pool: &PostgresPool,
    service_account_id: i32,
) -> Result<StorageServiceAccount, ApiError> {
    super::service_account::load_service_account_by_id(pool, service_account_id)
        .await
        .map(storage_service_account)
}

pub(crate) async fn load_service_account_point(
    pool: &PostgresPool,
    service_account_id: i32,
) -> Result<StorageServiceAccountPoint, ApiError> {
    let point =
        super::principal::load_service_account_point_response(pool, service_account_id).await?;
    let account = StorageServiceAccount::new(
        point.id,
        point.description,
        point.owner_group_id,
        point.created_by,
        point.disabled_at,
        point.created_at,
        point.updated_at,
    );
    Ok(StorageServiceAccountPoint::new(
        account,
        point.identity_scope_id,
        point.name,
        point.revision.get(),
    ))
}

pub(crate) async fn list_manageable_service_accounts(
    pool: &PostgresPool,
    query: StorageServiceAccountListQuery,
) -> Result<StorageIdentityPage<StorageServiceAccountListItem>, ApiError> {
    let (requestor_id, administrator, options) = query.into_parts();
    let requestor = PrincipalID::new(requestor_id)?;
    let total = if options.include_total {
        Some(
            super::service_account::count_manageable_service_accounts(
                pool,
                &requestor,
                administrator,
                count_query_options(&options),
            )
            .await?,
        )
    } else {
        None
    };
    let rows = super::service_account::search_manageable_service_accounts(
        pool,
        &requestor,
        administrator,
        options,
    )
    .await?
    .into_iter()
    .map(storage_service_account_list_item)
    .collect();
    Ok(StorageIdentityPage::new(rows, total))
}

pub(crate) async fn create_service_account(
    pool: &PostgresPool,
    request: StorageServiceAccountCreate,
) -> Result<StorageServiceAccount, ApiError> {
    let account = NewServiceAccount {
        identity_scope: None,
        name: request.name().to_string(),
        description: Some(request.description().to_string()),
        owner_group_id: GroupID::new(request.owner_group_id())?,
    };
    let saved = account
        .save(pool, request.created_by(), request.event_context())
        .await?;
    Ok(storage_service_account(saved))
}

pub(crate) async fn update_service_account(
    pool: &PostgresPool,
    request: StorageServiceAccountUpdate,
) -> Result<StorageServiceAccount, ApiError> {
    let update = UpdateServiceAccount {
        description: request.description().map(str::to_string),
        owner_group_id: request.owner_group_id(),
    };
    let id = ServiceAccountID::new(request.id())?;
    let updated =
        update_service_account_record(&update, pool, id.id(), Some(request.event_context()))
            .await?;
    Ok(storage_service_account(updated))
}

pub(crate) async fn disable_service_account(
    pool: &PostgresPool,
    request: StorageServiceAccountMutation,
) -> Result<StorageServiceAccount, ApiError> {
    let id = ServiceAccountID::new(request.id())?;
    let disabled = id.disable(pool, request.event_context()).await?;
    Ok(storage_service_account(disabled))
}

pub(crate) async fn delete_service_account(
    pool: &PostgresPool,
    request: StorageServiceAccountMutation,
) -> Result<(), ApiError> {
    let id = ServiceAccountID::new(request.id())?;
    delete_service_account_record(&id, pool, Some(request.event_context())).await
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

fn token_policy(
    policy: hubuum_storage_core::StorageTokenIssuancePolicy,
) -> Result<TokenIssuancePolicy, ApiError> {
    let (default_hours, maximum_hours) = policy.into_parts();
    Ok(TokenIssuancePolicy::from_hours(
        default_hours,
        maximum_hours,
    )?)
}

async fn storage_metadata_for_token(
    pool: &PostgresPool,
    token_id: i32,
) -> Result<StorageTokenMetadata, ApiError> {
    let metadata = super::token::principal_token_metadata_by_ids_db(pool, vec![token_id])
        .await?
        .pop()
        .ok_or_else(|| {
            ApiError::InternalServerError("Token metadata projection returned no token".to_string())
        })?;
    Ok(storage_token_metadata(metadata))
}

pub(crate) async fn create_token(
    pool: &PostgresPool,
    request: StorageTokenCreate,
) -> Result<StorageTokenMetadata, ApiError> {
    let (principal_id, token_hash, name, description, expires_at, scope, policy, context) =
        request.into_parts();
    let token = super::token::create_principal_token_hashed_db(
        pool,
        PrincipalTokenCreateParts {
            principal_id: PrincipalID::new(principal_id)?,
            name,
            description,
            expires_at,
            scope: scope.map(token_scope_from_storage).transpose()?,
        },
        token_hash,
        token_policy(policy)?,
        context.as_ref(),
    )
    .await?;
    storage_metadata_for_token(pool, token.id).await
}

pub(crate) async fn renew_token(
    pool: &PostgresPool,
    request: StorageTokenRenew,
) -> Result<StorageTokenMetadata, ApiError> {
    let (source_token_id, principal_id, token_hash, expires_at, policy, context) =
        request.into_parts();
    let token = super::token::renew_principal_token_hashed_db(
        pool,
        source_token_id,
        principal_id,
        token_hash,
        expires_at,
        token_policy(policy)?,
        context.as_ref(),
    )
    .await?;
    storage_metadata_for_token(pool, token.id).await
}

pub(crate) async fn load_token_metadata(
    pool: &PostgresPool,
    principal_id: i32,
    token_id: i32,
) -> Result<StorageTokenMetadata, ApiError> {
    super::token::principal_token_metadata_by_id_for_principal_db(pool, token_id, principal_id)
        .await
        .map(storage_token_metadata)
}

pub(crate) async fn load_token_metadata_batch(
    pool: &PostgresPool,
    token_ids: Vec<i32>,
) -> Result<Vec<StorageTokenMetadata>, ApiError> {
    Ok(
        super::token::principal_token_metadata_by_ids_db(pool, token_ids)
            .await?
            .into_iter()
            .map(storage_token_metadata)
            .collect(),
    )
}

pub(crate) async fn revoke_token(
    pool: &PostgresPool,
    request: StorageTokenRevoke,
) -> Result<usize, ApiError> {
    let (token_id, principal_id, context) = request.into_parts();
    super::token::revoke_token_by_id_for_principal_db(
        pool,
        token_id,
        principal_id,
        context.as_ref(),
    )
    .await
}

pub(crate) async fn revoke_token_by_hash(
    pool: &PostgresPool,
    request: StorageTokenHashRevoke,
) -> Result<usize, ApiError> {
    let (principal_id, token_hash) = request.into_parts();
    super::token::revoke_token_by_hash_db(pool, principal_id, token_hash).await
}

pub(crate) async fn revoke_all_principal_tokens(
    pool: &PostgresPool,
    principal_id: i32,
) -> Result<usize, ApiError> {
    super::token::revoke_all_tokens_for_principal_db(pool, PrincipalID::new(principal_id)?).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_state_mapping_is_complete() {
        assert_eq!(
            [
                StorageTokenListState::Active,
                StorageTokenListState::Expired,
                StorageTokenListState::Revoked,
                StorageTokenListState::All,
            ]
            .map(storage_token_state),
            [
                TokenListState::Active,
                TokenListState::Expired,
                TokenListState::Revoked,
                TokenListState::All,
            ]
        );
    }
}
