use std::collections::HashMap;

use hubuum_auth_core::AuthenticatedExternalUser;

use crate::errors::ApiError;
use crate::events::EventContext;
use crate::models::identity::{LOCAL_IDENTITY_SCOPE, LOCAL_PROVIDER_KIND};
use crate::models::search::QueryOptions;
use crate::models::{
    CollectionID, HubuumClassID, HubuumObjectID, IdentityScope, NewServiceAccount, Permissions,
    PrincipalGroup, PrincipalTokenMetadata, ResourceRevision, ServiceAccount,
    ServiceAccountWithName, TokenListState, TokenResourceScope, TokenScope, TokenScopeDetails,
    UpdateServiceAccount,
};
use crate::pagination::SKIPPED_TOTAL_COUNT;
use crate::storage::{
    AuthenticationTokenScope, IdentityStorage, StorageContext, StorageExternalGroup,
    StorageExternalPrincipalState, StorageExternalUserSync, StorageIdentityScope,
    StorageIdentityScopeEnsure, StoragePrincipalGroup, StorageServiceAccount,
    StorageServiceAccountCreate, StorageServiceAccountListItem, StorageServiceAccountListQuery,
    StorageServiceAccountMutation, StorageServiceAccountPoint, StorageServiceAccountUpdate,
    StorageSyncedHuman, StorageTokenListQuery, StorageTokenListState, StorageTokenMetadata,
    storage_handle,
};

fn revision(value: i64, resource: &str) -> Result<ResourceRevision, ApiError> {
    ResourceRevision::new(value).map_err(|_| {
        ApiError::InternalServerError(format!(
            "Storage backend returned an invalid {resource} revision"
        ))
    })
}

fn identity_scope_from_storage(scope: StorageIdentityScope) -> Result<IdentityScope, ApiError> {
    Ok(IdentityScope {
        id: scope.id(),
        name: scope.name().to_string(),
        provider_kind: scope.provider_kind().to_string(),
        created_at: scope.created_at(),
        updated_at: scope.updated_at(),
        revision: revision(scope.revision(), "identity scope")?,
    })
}

fn principal_group_from_storage(group: StoragePrincipalGroup) -> Result<PrincipalGroup, ApiError> {
    Ok(PrincipalGroup {
        principal_id: group.principal_id(),
        group_id: group.group_id(),
        created_at: group.created_at(),
        updated_at: group.updated_at(),
        revision: revision(group.revision(), "group membership")?,
    })
}

fn service_account_from_storage(account: StorageServiceAccount) -> ServiceAccount {
    ServiceAccount {
        id: account.id(),
        kind: crate::models::PrincipalKind::ServiceAccount
            .as_str()
            .to_string(),
        description: account.description().to_string(),
        owner_group_id: account.owner_group_id(),
        created_by: account.created_by(),
        disabled_at: account.disabled_at(),
        created_at: account.created_at(),
        updated_at: account.updated_at(),
    }
}

fn service_account_list_item_from_storage(
    item: StorageServiceAccountListItem,
) -> Result<ServiceAccountWithName, ApiError> {
    let (account, identity_scope, name, item_revision) = item.into_parts();
    Ok(ServiceAccountWithName {
        service_account: service_account_from_storage(account),
        identity_scope,
        name,
        revision: revision(item_revision, "service account")?,
    })
}

fn token_state(state: TokenListState) -> StorageTokenListState {
    match state {
        TokenListState::Active => StorageTokenListState::Active,
        TokenListState::Expired => StorageTokenListState::Expired,
        TokenListState::Revoked => StorageTokenListState::Revoked,
        TokenListState::All => StorageTokenListState::All,
    }
}

fn token_scope_from_storage(
    scope: AuthenticationTokenScope,
) -> Result<TokenScopeDetails, ApiError> {
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
    TokenScopeDetails::from_scope(TokenScope::from_stored_parts(permissions, resources)?)
}

fn token_metadata_from_storage(
    metadata: StorageTokenMetadata,
) -> Result<PrincipalTokenMetadata, ApiError> {
    let scope = metadata
        .scope()
        .cloned()
        .map(token_scope_from_storage)
        .transpose()?;
    Ok(PrincipalTokenMetadata {
        id: crate::models::TokenID::new(metadata.id())?,
        principal_id: crate::models::PrincipalID::new(metadata.principal_id())?,
        name: metadata.name().map(str::to_string),
        description: metadata.description().map(str::to_string),
        issued: metadata.issued(),
        expires_at: metadata.expires_at(),
        last_used_at: metadata.last_used_at(),
        revoked_at: metadata.revoked_at(),
        active: metadata.is_active(),
        expired: metadata.is_expired(),
        scope,
        revision: revision(metadata.revision(), "token")?,
    })
}

pub async fn ensure_identity_scope(
    context: &impl StorageContext,
    name: &str,
    provider_kind: &str,
) -> Result<IdentityScope, ApiError> {
    let request = StorageIdentityScopeEnsure::new(name, provider_kind);
    identity_scope_from_storage(
        storage_handle(context)
            .ensure_identity_scope(request)
            .await?,
    )
}

pub async fn identity_scope_name(
    context: &impl StorageContext,
    scope_id: i32,
) -> Result<String, ApiError> {
    Ok(storage_handle(context)
        .identity_scope_name(scope_id)
        .await?)
}

pub async fn identity_scope_names(
    context: &impl StorageContext,
    scope_ids: &[i32],
) -> Result<HashMap<i32, String>, ApiError> {
    Ok(storage_handle(context)
        .identity_scope_names(scope_ids.to_vec())
        .await?
        .into_iter()
        .collect())
}

pub async fn load_principal_group(
    context: &impl StorageContext,
    principal_id: i32,
    group_id: i32,
) -> Result<PrincipalGroup, ApiError> {
    principal_group_from_storage(
        storage_handle(context)
            .load_principal_group(principal_id, group_id)
            .await?,
    )
}

pub async fn list_retained_tokens(
    context: &impl StorageContext,
    principal_id: i32,
    options: QueryOptions,
    state: TokenListState,
) -> Result<(Vec<PrincipalTokenMetadata>, i64), ApiError> {
    let query = StorageTokenListQuery::new(principal_id, options, token_state(state));
    let (rows, total) = storage_handle(context)
        .list_retained_tokens(query)
        .await?
        .into_parts();
    Ok((
        rows.into_iter()
            .map(token_metadata_from_storage)
            .collect::<Result<Vec<_>, _>>()?,
        total.unwrap_or(SKIPPED_TOTAL_COUNT),
    ))
}

pub async fn is_human_owner_group_member(
    context: &impl StorageContext,
    principal_id: i32,
    owner_group_id: i32,
) -> Result<bool, ApiError> {
    Ok(storage_handle(context)
        .is_human_owner_group_member(principal_id, owner_group_id)
        .await?)
}

pub async fn principal_is_disabled(
    context: &impl StorageContext,
    principal_id: i32,
) -> Result<bool, ApiError> {
    Ok(storage_handle(context)
        .principal_is_disabled(principal_id)
        .await?)
}

pub async fn load_service_account(
    context: &impl StorageContext,
    service_account_id: i32,
) -> Result<ServiceAccount, ApiError> {
    Ok(service_account_from_storage(
        storage_handle(context)
            .load_service_account(service_account_id)
            .await?,
    ))
}

fn service_account_point_from_storage(
    point: StorageServiceAccountPoint,
) -> Result<crate::models::ServiceAccountPointResponse, ApiError> {
    let (account, identity_scope_id, name, point_revision) = point.into_parts();
    Ok(crate::models::ServiceAccountPointResponse::from_parts(
        service_account_from_storage(account),
        identity_scope_id,
        name,
        revision(point_revision, "service account")?,
    ))
}

pub async fn load_service_account_point(
    context: &impl StorageContext,
    service_account_id: i32,
) -> Result<crate::models::ServiceAccountPointResponse, ApiError> {
    service_account_point_from_storage(
        storage_handle(context)
            .load_service_account_point(service_account_id)
            .await?,
    )
}

pub async fn list_manageable_service_accounts(
    context: &impl StorageContext,
    requestor_id: i32,
    administrator: bool,
    options: QueryOptions,
) -> Result<(Vec<ServiceAccountWithName>, i64), ApiError> {
    let query = StorageServiceAccountListQuery::new(requestor_id, administrator, options);
    let (rows, total) = storage_handle(context)
        .list_manageable_service_accounts(query)
        .await?
        .into_parts();
    Ok((
        rows.into_iter()
            .map(service_account_list_item_from_storage)
            .collect::<Result<Vec<_>, _>>()?,
        total.unwrap_or(SKIPPED_TOTAL_COUNT),
    ))
}

pub async fn create_service_account(
    context: &impl StorageContext,
    account: &NewServiceAccount,
    created_by: Option<i32>,
    event_context: &EventContext,
) -> Result<ServiceAccount, ApiError> {
    let scope_name = account
        .identity_scope
        .as_deref()
        .unwrap_or(LOCAL_IDENTITY_SCOPE);
    if scope_name != LOCAL_IDENTITY_SCOPE {
        return Err(ApiError::BadRequest(
            "service accounts in non-local identity scopes are managed by their identity provider"
                .to_string(),
        ));
    }
    let request = StorageServiceAccountCreate::new(
        &account.name,
        account.description.clone().unwrap_or_default(),
        account.owner_group_id.id(),
        created_by,
        event_context.clone(),
    );
    Ok(service_account_from_storage(
        storage_handle(context)
            .create_service_account(request)
            .await?,
    ))
}

pub async fn update_service_account(
    context: &impl StorageContext,
    id: i32,
    update: &UpdateServiceAccount,
    event_context: &EventContext,
) -> Result<ServiceAccount, ApiError> {
    let request = StorageServiceAccountUpdate::new(
        id,
        update.description.clone(),
        update.owner_group_id,
        event_context.clone(),
    );
    Ok(service_account_from_storage(
        storage_handle(context)
            .update_service_account(request)
            .await?,
    ))
}

pub async fn disable_service_account(
    context: &impl StorageContext,
    id: i32,
    event_context: &EventContext,
) -> Result<ServiceAccount, ApiError> {
    let request = StorageServiceAccountMutation::new(id, event_context.clone());
    Ok(service_account_from_storage(
        storage_handle(context)
            .disable_service_account(request)
            .await?,
    ))
}

pub async fn delete_service_account(
    context: &impl StorageContext,
    id: i32,
    event_context: &EventContext,
) -> Result<(), ApiError> {
    let request = StorageServiceAccountMutation::new(id, event_context.clone());
    Ok(storage_handle(context)
        .delete_service_account(request)
        .await?)
}

#[derive(Clone, PartialEq, Eq)]
pub struct ExternalPrincipalState {
    pub identity_scope: String,
    pub username: String,
    pub external_subject: String,
    pub last_sync_attempted_at: Option<chrono::NaiveDateTime>,
    pub last_sync_success_at: Option<chrono::NaiveDateTime>,
}

fn external_state_from_storage(state: StorageExternalPrincipalState) -> ExternalPrincipalState {
    ExternalPrincipalState {
        identity_scope: state.identity_scope().to_string(),
        username: state.username().to_string(),
        external_subject: state.external_subject().to_string(),
        last_sync_attempted_at: state.last_sync_attempted_at(),
        last_sync_success_at: state.last_sync_success_at(),
    }
}

pub async fn external_principal_state(
    context: &impl StorageContext,
    principal_id: i32,
) -> Result<Option<ExternalPrincipalState>, ApiError> {
    Ok(storage_handle(context)
        .external_principal_state(principal_id)
        .await?
        .map(external_state_from_storage))
}

pub async fn mark_external_sync_attempted(
    context: &impl StorageContext,
    principal_id: i32,
) -> Result<(), ApiError> {
    Ok(storage_handle(context)
        .mark_external_sync_attempted(principal_id)
        .await?)
}

fn external_sync_request(
    scope_name: &str,
    provider_kind: &str,
    authenticated: AuthenticatedExternalUser,
) -> StorageExternalUserSync {
    let profile = authenticated.profile;
    StorageExternalUserSync::builder(scope_name, provider_kind, profile.subject, profile.name)
        .proper_name(profile.proper_name)
        .email(profile.email)
        .groups(
            authenticated
                .groups
                .into_iter()
                .map(|group| StorageExternalGroup::new(group.key, group.name, group.description))
                .collect(),
        )
        .build()
}

fn synced_human_from_storage(human: StorageSyncedHuman) -> crate::models::User {
    let (id, proper_name, email, created_at, updated_at, anonymized_at) = human.into_parts();
    crate::models::User {
        id,
        kind: crate::models::PrincipalKind::Human.as_str().to_string(),
        password: None,
        proper_name,
        email,
        created_at,
        updated_at,
        anonymized_at,
    }
}

pub async fn sync_external_user(
    context: &impl StorageContext,
    scope_name: &str,
    provider_kind: &str,
    authenticated: AuthenticatedExternalUser,
) -> Result<crate::models::User, ApiError> {
    let request = external_sync_request(scope_name, provider_kind, authenticated);
    Ok(synced_human_from_storage(
        storage_handle(context).sync_external_user(request).await?,
    ))
}

pub async fn ensure_local_identity_scope(
    context: &impl StorageContext,
) -> Result<IdentityScope, ApiError> {
    ensure_identity_scope(context, LOCAL_IDENTITY_SCOPE, LOCAL_PROVIDER_KIND).await
}
