use super::super::*;

#[async_trait]
impl AuthenticationStorage for PostgresStorage {
    async fn authenticate_bearer_token(
        &self,
        attempt: AuthenticationAttempt,
    ) -> Result<AuthenticatedToken, StorageError> {
        crate::operations::authentication::authenticate_bearer_token(self.runtime(), attempt)
            .await
            .map_err(StorageError::from)
    }

    async fn load_authentication_identity(
        &self,
        principal_id: i32,
    ) -> Result<AuthenticationIdentity, StorageError> {
        crate::operations::authentication::load_authentication_identity(
            self.runtime(),
            principal_id,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn load_authentication_token_scope(
        &self,
        query: AuthenticationTokenScopeQuery,
    ) -> Result<Option<AuthenticationTokenScope>, StorageError> {
        crate::operations::authentication::load_authentication_token_scope(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }
}

#[async_trait]
impl IdentityStorage for PostgresStorage {
    async fn default_admin_bootstrap_required(&self) -> Result<bool, StorageError> {
        crate::operations::bootstrap::default_admin_bootstrap_required(self.runtime())
            .await
            .map_err(StorageError::from)
    }

    async fn bootstrap_default_admin(
        &self,
        request: StorageDefaultAdminBootstrap,
    ) -> Result<bool, StorageError> {
        crate::operations::bootstrap::bootstrap_default_admin(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn reset_local_password(
        &self,
        request: StorageLocalPasswordReset,
    ) -> Result<usize, StorageError> {
        crate::operations::identity_credentials::reset_local_password(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn ensure_identity_scope(
        &self,
        request: StorageIdentityScopeEnsure,
    ) -> Result<StorageIdentityScope, StorageError> {
        crate::operations::identity_scope::ensure_identity_scope(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn identity_scope_name(&self, scope_id: i32) -> Result<String, StorageError> {
        crate::operations::identity_scope::identity_scope_name(self.runtime(), scope_id)
            .await
            .map_err(StorageError::from)
    }

    async fn identity_scope_names(
        &self,
        scope_ids: Vec<i32>,
    ) -> Result<Vec<(i32, String)>, StorageError> {
        crate::operations::identity_scope::identity_scope_names(self.runtime(), scope_ids)
            .await
            .map_err(StorageError::from)
    }

    async fn load_principal_group(
        &self,
        principal_id: i32,
        group_id: i32,
    ) -> Result<StoragePrincipalGroup, StorageError> {
        crate::operations::identity_principals::load_principal_group(
            self.runtime(),
            principal_id,
            group_id,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn list_principal_groups(
        &self,
        query: StoragePrincipalGroupListQuery,
    ) -> Result<StorageIdentityPage<StorageIdentityGroup>, StorageError> {
        crate::operations::group::list_principal_groups(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn list_groups(
        &self,
        query: StorageGroupListQuery,
    ) -> Result<StorageIdentityPage<StorageIdentityGroup>, StorageError> {
        crate::operations::group::list_groups(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn list_retained_tokens(
        &self,
        query: StorageTokenListQuery,
    ) -> Result<StorageIdentityPage<StorageTokenMetadata>, StorageError> {
        crate::operations::token::list_retained_tokens(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn is_human_owner_group_member(
        &self,
        principal_id: i32,
        owner_group_id: i32,
    ) -> Result<bool, StorageError> {
        crate::operations::identity_principals::is_human_owner_group_member(
            self.runtime(),
            principal_id,
            owner_group_id,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn principal_is_disabled(&self, principal_id: i32) -> Result<bool, StorageError> {
        crate::operations::identity_principals::principal_is_disabled(self.runtime(), principal_id)
            .await
            .map_err(StorageError::from)
    }

    async fn load_service_account(
        &self,
        service_account_id: i32,
    ) -> Result<StorageServiceAccount, StorageError> {
        crate::operations::service_account::load_service_account(self.runtime(), service_account_id)
            .await
            .map_err(StorageError::from)
    }

    async fn load_service_account_point(
        &self,
        service_account_id: i32,
    ) -> Result<StorageServiceAccountPoint, StorageError> {
        crate::operations::service_account::load_service_account_point(
            self.runtime(),
            service_account_id,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn list_manageable_service_accounts(
        &self,
        query: StorageServiceAccountListQuery,
    ) -> Result<StorageIdentityPage<StorageServiceAccountListItem>, StorageError> {
        crate::operations::service_account::list_manageable_service_accounts(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn create_service_account(
        &self,
        request: StorageServiceAccountCreate,
    ) -> Result<StorageServiceAccount, StorageError> {
        crate::operations::service_account::create_service_account(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn update_service_account(
        &self,
        request: StorageServiceAccountUpdate,
    ) -> Result<StorageServiceAccount, StorageError> {
        crate::operations::service_account::update_service_account(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn disable_service_account(
        &self,
        request: StorageServiceAccountMutation,
    ) -> Result<StorageServiceAccountDisableOutcome, StorageError> {
        crate::operations::service_account::disable_service_account(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn delete_service_account(
        &self,
        request: StorageServiceAccountMutation,
    ) -> Result<(), StorageError> {
        crate::operations::service_account::delete_service_account(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn external_principal_state(
        &self,
        principal_id: i32,
    ) -> Result<Option<StorageExternalPrincipalState>, StorageError> {
        crate::operations::external_identity::external_principal_state(self.runtime(), principal_id)
            .await
            .map_err(StorageError::from)
    }

    async fn mark_external_sync_attempted(&self, principal_id: i32) -> Result<(), StorageError> {
        crate::operations::external_identity::mark_external_sync_attempted(
            self.runtime(),
            principal_id,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn sync_external_user(
        &self,
        request: StorageExternalUserSync,
    ) -> Result<StorageSyncedHuman, StorageError> {
        crate::operations::external_identity::sync_external_user(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }
}

#[async_trait]
impl UserStorage for PostgresStorage {
    async fn load_user(&self, id: i32) -> Result<StorageUser, StorageError> {
        crate::operations::user::load_user(self.runtime(), id)
            .await
            .map_err(StorageError::from)
    }

    async fn load_user_by_name(
        &self,
        identity_scope: String,
        name: String,
    ) -> Result<StorageUser, StorageError> {
        crate::operations::user::load_user_by_name(self.runtime(), identity_scope, name)
            .await
            .map_err(StorageError::from)
    }

    async fn load_user_point(&self, id: i32) -> Result<StorageUserPoint, StorageError> {
        crate::operations::user::load_user_point(self.runtime(), id)
            .await
            .map_err(StorageError::from)
    }

    async fn list_users(
        &self,
        query: StorageUserListQuery,
    ) -> Result<StorageIdentityPage<StorageUserListItem>, StorageError> {
        crate::operations::user::list_users(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn create_user(&self, request: StorageUserCreate) -> Result<StorageUser, StorageError> {
        crate::operations::user::create_user(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn update_user(&self, request: StorageUserUpdate) -> Result<StorageUser, StorageError> {
        crate::operations::user::update_user(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn set_user_password(
        &self,
        request: StorageUserPasswordUpdate,
    ) -> Result<usize, StorageError> {
        crate::operations::user::set_user_password(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn delete_user(&self, request: StorageUserDelete) -> Result<usize, StorageError> {
        crate::operations::user::delete_user(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn anonymize_user(&self, id: i32) -> Result<(), StorageError> {
        crate::operations::user::anonymize_user(self.runtime(), id)
            .await
            .map_err(StorageError::from)
    }
}

#[async_trait]
impl TokenStorage for PostgresStorage {
    async fn create_token(
        &self,
        request: StorageTokenCreate,
    ) -> Result<StorageTokenMetadata, StorageError> {
        crate::operations::token::create_token(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn renew_token(
        &self,
        request: StorageTokenRenew,
    ) -> Result<StorageTokenMetadata, StorageError> {
        crate::operations::token::renew_token(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn load_token_metadata(
        &self,
        principal_id: i32,
        token_id: i32,
        observation: StorageTokenObservation,
    ) -> Result<StorageTokenMetadata, StorageError> {
        crate::operations::token::load_token_metadata(
            self.runtime(),
            principal_id,
            token_id,
            observation,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn load_token_metadata_batch(
        &self,
        token_ids: Vec<i32>,
        observation: StorageTokenObservation,
    ) -> Result<Vec<StorageTokenMetadata>, StorageError> {
        crate::operations::token::load_token_metadata_batch(self.runtime(), token_ids, observation)
            .await
            .map_err(StorageError::from)
    }

    async fn revoke_token(&self, request: StorageTokenRevoke) -> Result<usize, StorageError> {
        crate::operations::token::revoke_token(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn revoke_token_by_hash(
        &self,
        request: StorageTokenHashRevoke,
    ) -> Result<usize, StorageError> {
        crate::operations::token::revoke_token_by_hash(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn revoke_all_principal_tokens(&self, principal_id: i32) -> Result<usize, StorageError> {
        crate::operations::token::revoke_all_principal_tokens(self.runtime(), principal_id)
            .await
            .map_err(StorageError::from)
    }
}

#[async_trait]
impl AuthorizationStorage for PostgresStorage {
    async fn load_authorization_principal(
        &self,
        principal_id: i32,
    ) -> Result<AuthorizationPrincipal, StorageError> {
        crate::operations::authorization::load_authorization_principal(self.runtime(), principal_id)
            .await
            .map_err(StorageError::from)
    }

    async fn authorization_principal_is_group_member(
        &self,
        query: AuthorizationGroupMembershipQuery,
    ) -> Result<bool, StorageError> {
        crate::operations::authorization::authorization_principal_is_group_member(
            self.runtime(),
            query,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn load_authorization_classes(
        &self,
        query: AuthorizationResourceIds,
    ) -> Result<Vec<AuthorizationClassResource>, StorageError> {
        crate::operations::authorization::load_authorization_classes(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn load_authorization_objects(
        &self,
        query: AuthorizationResourceIds,
    ) -> Result<Vec<AuthorizationObjectResource>, StorageError> {
        crate::operations::authorization::load_authorization_objects(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn authorize_local_collection(
        &self,
        query: AuthorizationCollectionAccessQuery,
    ) -> Result<bool, StorageError> {
        crate::operations::authorization::authorize_local_collection(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn authorize_local_collections(
        &self,
        query: AuthorizationCollectionsAccessQuery,
    ) -> Result<bool, StorageError> {
        crate::operations::authorization::authorize_local_collections(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn local_authorized_collections(
        &self,
        query: AuthorizationCollectionsQuery,
    ) -> Result<Vec<AuthorizationCollection>, StorageError> {
        crate::operations::authorization::local_authorized_collections(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn list_authorization_collection_candidates(
        &self,
    ) -> Result<Vec<AuthorizationCollection>, StorageError> {
        crate::operations::authorization::list_authorization_collection_candidates(self.runtime())
            .await
            .map_err(StorageError::from)
    }

    async fn list_authorization_group_candidates(
        &self,
        query_options: QueryOptions,
    ) -> Result<Vec<AuthorizationGroup>, StorageError> {
        crate::operations::authorization::list_authorization_group_candidates(
            self.runtime(),
            query_options,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn authorization_policy_snapshot(
        &self,
    ) -> Result<Vec<AuthorizationPolicySnapshotRow>, StorageError> {
        crate::operations::authorization::authorization_policy_snapshot(self.runtime())
            .await
            .map_err(StorageError::from)
    }

    async fn list_local_collection_grants(
        &self,
        query: AuthorizationCollectionGrantListQuery,
    ) -> Result<AuthorizationGroupGrantPage, StorageError> {
        crate::operations::authorization::list_local_collection_grants(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn get_local_collection_grant(
        &self,
        key: AuthorizationGrantKey,
    ) -> Result<Option<AuthorizationGrant>, StorageError> {
        crate::operations::authorization::get_local_collection_grant(self.runtime(), key)
            .await
            .map_err(StorageError::from)
    }

    async fn load_local_collection_permission_set(
        &self,
        query: AuthorizationPermissionSetQuery,
    ) -> Result<AuthorizationPermissionSet, StorageError> {
        crate::operations::authorization::load_local_collection_permission_set(
            self.runtime(),
            query,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn apply_local_collection_grant(
        &self,
        mutation: AuthorizationGrantMutation,
    ) -> Result<AuthorizationGrant, StorageError> {
        crate::operations::authorization::apply_local_collection_grant(self.runtime(), mutation)
            .await
            .map_err(StorageError::from)
    }

    async fn revoke_local_collection_grant(
        &self,
        mutation: AuthorizationGrantMutation,
    ) -> Result<AuthorizationGrant, StorageError> {
        crate::operations::authorization::revoke_local_collection_grant(self.runtime(), mutation)
            .await
            .map_err(StorageError::from)
    }

    async fn revoke_all_local_collection_grants(
        &self,
        request: AuthorizationGrantDelete,
    ) -> Result<(), StorageError> {
        crate::operations::authorization::revoke_all_local_collection_grants(
            self.runtime(),
            request,
        )
        .await
        .map_err(StorageError::from)
    }
}
