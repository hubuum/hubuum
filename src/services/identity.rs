use std::collections::HashMap;

use hubuum_auth_core::AuthenticatedExternalUser;

use crate::errors::ApiError;
use crate::events::EventContext;
use crate::models::identity::{LOCAL_IDENTITY_SCOPE, LOCAL_PROVIDER_KIND};
use crate::models::search::QueryOptions;
use crate::models::{
    CollectionID, Group, HubuumClassID, HubuumObjectID, IdentityScope, NewServiceAccount,
    Permissions, PrincipalGroup, PrincipalToken, PrincipalTokenCreateRequest,
    PrincipalTokenMetadata, ResourceRevision, ServiceAccount, ServiceAccountWithName,
    TokenListState, TokenResourceScope, TokenScope, TokenScopeDetails, UpdateServiceAccount,
    UpdateUser, User, UserPointResponse, UserWithName,
};
use crate::pagination::SKIPPED_TOTAL_COUNT;
use crate::storage::{
    AuthenticationResourceScope, AuthenticationTokenScope, IdentityStorage, StorageContext,
    StorageExternalGroup, StorageExternalPrincipalState, StorageExternalUserSync,
    StorageIdentityGroup, StorageIdentityScope, StorageIdentityScopeEnsure,
    StorageLocalPasswordReset, StoragePrincipalGroup, StoragePrincipalGroupListQuery,
    StorageServiceAccount, StorageServiceAccountCreate, StorageServiceAccountListItem,
    StorageServiceAccountListQuery, StorageServiceAccountMutation, StorageServiceAccountPoint,
    StorageServiceAccountUpdate, StorageSyncedHuman, StorageTokenCreate, StorageTokenHashRevoke,
    StorageTokenIssuancePolicy, StorageTokenListQuery, StorageTokenListState, StorageTokenMetadata,
    StorageTokenRenew, StorageTokenRevoke, StorageUser, StorageUserCreate, StorageUserDelete,
    StorageUserListItem, StorageUserListQuery, StorageUserPasswordUpdate, StorageUserPoint,
    StorageUserUpdate, TokenStorage, UserStorage, storage_handle,
};

pub(crate) async fn reset_local_password(
    context: &impl StorageContext,
    principal_name: &str,
    new_password: String,
) -> Result<usize, ApiError> {
    let password_hash = crate::utilities::auth::hash_password_async(new_password)
        .await
        .map_err(|error| ApiError::HashError(format!("Failed to hash password: {error}")))?;
    Ok(storage_handle(context)
        .reset_local_password(StorageLocalPasswordReset::new(
            principal_name,
            password_hash,
        ))
        .await?)
}

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

pub(super) fn identity_group_from_storage(group: StorageIdentityGroup) -> Result<Group, ApiError> {
    Ok(Group {
        id: group.id(),
        groupname: group.name().to_string(),
        description: group.description().to_string(),
        created_at: group.created_at(),
        updated_at: group.updated_at(),
        identity_scope_id: group.identity_scope_id(),
        managed_by: group.managed_by().to_string(),
        external_key: group.external_key().map(ToString::to_string),
        last_sync_attempted_at: group.last_sync_attempted_at(),
        last_sync_success_at: group.last_sync_success_at(),
        revision: revision(group.revision(), "group")?,
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

fn user_from_storage(user: StorageUser) -> User {
    let (id, password, proper_name, email, created_at, updated_at, anonymized_at) =
        user.into_parts();
    User {
        id,
        kind: crate::models::PrincipalKind::Human.as_str().to_string(),
        password,
        proper_name,
        email,
        created_at,
        updated_at,
        anonymized_at,
    }
}

fn user_list_item_from_storage(item: StorageUserListItem) -> Result<UserWithName, ApiError> {
    let (user, scope, provider, name, managed, attempted, succeeded, item_revision) =
        item.into_parts();
    Ok(UserWithName::from_tuple((
        user_from_storage(user),
        scope,
        provider,
        name,
        managed,
        attempted,
        succeeded,
        revision(item_revision, "user")?,
    )))
}

fn user_point_from_storage(point: StorageUserPoint) -> Result<UserPointResponse, ApiError> {
    let (id, proper_name, email, created_at, updated_at, scope_id, managed, name, point_revision) =
        point.into_parts();
    Ok(UserPointResponse {
        id,
        identity_scope_id: scope_id,
        provider_managed: managed,
        name,
        proper_name,
        email,
        created_at,
        updated_at,
        revision: revision(point_revision, "user")?,
    })
}

fn token_scope_to_storage(scope: &TokenScope) -> AuthenticationTokenScope {
    let permissions = scope.permissions().map(|permissions| {
        permissions
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
    });
    let resources = scope.resource_ids().map(|resources| {
        AuthenticationResourceScope::new(
            resources.collection_ids().to_vec(),
            resources.class_ids().to_vec(),
            resources.object_ids().to_vec(),
        )
    });
    AuthenticationTokenScope::new(permissions, resources)
}

fn token_policy(policy: crate::models::TokenIssuancePolicy) -> StorageTokenIssuancePolicy {
    StorageTokenIssuancePolicy::new(
        policy.default_lifetime().hours(),
        policy.maximum_lifetime().hours(),
    )
}

pub async fn load_user(context: &impl StorageContext, id: i32) -> Result<User, ApiError> {
    Ok(user_from_storage(
        storage_handle(context).load_user(id).await?,
    ))
}

pub async fn load_user_by_name(
    context: &impl StorageContext,
    identity_scope: &str,
    name: &str,
) -> Result<User, ApiError> {
    Ok(user_from_storage(
        storage_handle(context)
            .load_user_by_name(identity_scope.to_string(), name.to_string())
            .await?,
    ))
}

pub async fn load_user_point(
    context: &impl StorageContext,
    id: i32,
) -> Result<UserPointResponse, ApiError> {
    user_point_from_storage(storage_handle(context).load_user_point(id).await?)
}

pub async fn list_users(
    context: &impl StorageContext,
    options: QueryOptions,
) -> Result<(Vec<UserWithName>, i64), ApiError> {
    let (rows, total) = storage_handle(context)
        .list_users(StorageUserListQuery::new(options))
        .await?
        .into_parts();
    Ok((
        rows.into_iter()
            .map(user_list_item_from_storage)
            .collect::<Result<Vec<_>, _>>()?,
        total.unwrap_or(SKIPPED_TOTAL_COUNT),
    ))
}

pub async fn create_user(
    context: &impl StorageContext,
    request: crate::models::NewUser,
    event_context: Option<&EventContext>,
) -> Result<User, ApiError> {
    let request = request.hash_password().await?;
    create_user_with_password_hash(context, request, event_context).await
}

pub(crate) async fn create_user_with_password_hash(
    context: &impl StorageContext,
    request: crate::models::NewUser,
    event_context: Option<&EventContext>,
) -> Result<User, ApiError> {
    let storage_request = StorageUserCreate::new(
        request.identity_scope,
        request.name,
        request.password,
        request.proper_name,
        request.email,
        event_context.cloned(),
    );
    Ok(user_from_storage(
        storage_handle(context).create_user(storage_request).await?,
    ))
}

pub async fn update_user(
    context: &impl StorageContext,
    id: i32,
    request: UpdateUser,
    event_context: Option<&EventContext>,
) -> Result<User, ApiError> {
    let request = request.hash_password().await?;
    let storage_request = StorageUserUpdate::new(
        id,
        request.password,
        request.proper_name,
        request.email,
        event_context.cloned(),
    );
    Ok(user_from_storage(
        storage_handle(context).update_user(storage_request).await?,
    ))
}

pub async fn set_user_password(
    context: &impl StorageContext,
    id: i32,
    password_hash: String,
) -> Result<usize, ApiError> {
    Ok(storage_handle(context)
        .set_user_password(StorageUserPasswordUpdate::new(id, password_hash))
        .await?)
}

pub async fn delete_user(
    context: &impl StorageContext,
    id: i32,
    event_context: Option<&EventContext>,
) -> Result<usize, ApiError> {
    Ok(storage_handle(context)
        .delete_user(StorageUserDelete::new(id, event_context.cloned()))
        .await?)
}

pub async fn anonymize_user(context: &impl StorageContext, id: i32) -> Result<(), ApiError> {
    Ok(storage_handle(context).anonymize_user(id).await?)
}

pub async fn create_token(
    context: &impl StorageContext,
    request: PrincipalTokenCreateRequest,
    issuance_policy: crate::models::TokenIssuancePolicy,
    event_context: Option<&EventContext>,
) -> Result<crate::models::IssuedToken, ApiError> {
    let parts = request.into_parts();
    let raw = crate::utilities::auth::generate_token();
    let storage_request = StorageTokenCreate::new(
        parts.principal_id.id(),
        raw.storage_hash(),
        token_policy(issuance_policy),
    )
    .name(parts.name)
    .description(parts.description)
    .expires_at(parts.expires_at)
    .scope(parts.scope.as_ref().map(token_scope_to_storage))
    .event_context(event_context.cloned());
    let metadata = token_metadata_from_storage(
        storage_handle(context)
            .create_token(storage_request)
            .await?,
    )?;
    let expires_at = metadata.expires_at.ok_or_else(|| {
        ApiError::InternalServerError(
            "newly issued token is missing its persisted expiry".to_string(),
        )
    })?;
    Ok(crate::models::IssuedToken::new(raw, expires_at))
}

pub async fn renew_token(
    context: &impl StorageContext,
    source_token_id: i32,
    principal_id: i32,
    expires_at: Option<chrono::NaiveDateTime>,
    issuance_policy: crate::models::TokenIssuancePolicy,
    event_context: Option<&EventContext>,
) -> Result<crate::models::IssuedToken, ApiError> {
    let raw = crate::utilities::auth::generate_token();
    let request = StorageTokenRenew::new(
        source_token_id,
        principal_id,
        raw.storage_hash(),
        expires_at,
        token_policy(issuance_policy),
        event_context.cloned(),
    );
    let metadata =
        token_metadata_from_storage(storage_handle(context).renew_token(request).await?)?;
    let expires_at = metadata.expires_at.ok_or_else(|| {
        ApiError::InternalServerError(
            "newly renewed token is missing its persisted expiry".to_string(),
        )
    })?;
    Ok(crate::models::IssuedToken::new(raw, expires_at))
}

pub async fn load_token_metadata(
    context: &impl StorageContext,
    principal_id: i32,
    token_id: i32,
) -> Result<PrincipalTokenMetadata, ApiError> {
    token_metadata_from_storage(
        storage_handle(context)
            .load_token_metadata(principal_id, token_id)
            .await?,
    )
}

pub async fn load_token_metadata_batch(
    context: &impl StorageContext,
    tokens: &[PrincipalToken],
) -> Result<Vec<PrincipalTokenMetadata>, ApiError> {
    storage_handle(context)
        .load_token_metadata_batch(tokens.iter().map(|token| token.id).collect())
        .await?
        .into_iter()
        .map(token_metadata_from_storage)
        .collect()
}

pub async fn revoke_token(
    context: &impl StorageContext,
    token_id: i32,
    principal_id: i32,
    event_context: Option<&EventContext>,
) -> Result<usize, ApiError> {
    Ok(storage_handle(context)
        .revoke_token(StorageTokenRevoke::new(
            token_id,
            principal_id,
            event_context.cloned(),
        ))
        .await?)
}

pub async fn revoke_token_by_hash(
    context: &impl StorageContext,
    principal_id: Option<i32>,
    token_hash: String,
) -> Result<usize, ApiError> {
    Ok(storage_handle(context)
        .revoke_token_by_hash(StorageTokenHashRevoke::new(principal_id, token_hash))
        .await?)
}

pub async fn revoke_all_principal_tokens(
    context: &impl StorageContext,
    principal_id: i32,
) -> Result<usize, ApiError> {
    Ok(storage_handle(context)
        .revoke_all_principal_tokens(principal_id)
        .await?)
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

pub async fn list_principal_groups(
    context: &impl StorageContext,
    principal_id: i32,
    options: QueryOptions,
) -> Result<(Vec<Group>, i64), ApiError> {
    let (rows, total) = storage_handle(context)
        .list_principal_groups(StoragePrincipalGroupListQuery::new(principal_id, options))
        .await?
        .into_parts();
    Ok((
        rows.into_iter()
            .map(identity_group_from_storage)
            .collect::<Result<Vec<_>, _>>()?,
        total.unwrap_or(SKIPPED_TOTAL_COUNT),
    ))
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
