use super::super::*;

#[async_trait]
impl AuthenticationStorage for PostgresStorage {
    async fn authenticate_bearer_token(
        &self,
        credential: AuthenticationCredential,
    ) -> Result<AuthenticatedToken, StorageError> {
        operations::authentication::authenticate_bearer_token(&self.pool, credential)
            .await
            .map_err(map_postgres_error)
    }

    async fn load_authentication_identity(
        &self,
        principal_id: i32,
    ) -> Result<AuthenticationIdentity, StorageError> {
        operations::authentication::load_authentication_identity(&self.pool, principal_id)
            .await
            .map_err(map_postgres_error)
    }

    async fn load_authentication_token_scope(
        &self,
        query: AuthenticationTokenScopeQuery,
    ) -> Result<Option<AuthenticationTokenScope>, StorageError> {
        operations::authentication::load_authentication_token_scope(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }
}

#[async_trait]
impl IdentityStorage for PostgresStorage {
    async fn default_admin_bootstrap_required(&self) -> Result<bool, StorageError> {
        operations::bootstrap::default_admin_bootstrap_required(&self.pool)
            .await
            .map_err(map_postgres_error)
    }

    async fn bootstrap_default_admin(
        &self,
        request: StorageDefaultAdminBootstrap,
    ) -> Result<bool, StorageError> {
        operations::bootstrap::bootstrap_default_admin(&self.pool, request)
            .await
            .map_err(map_postgres_error)
    }

    async fn reset_local_password(
        &self,
        request: StorageLocalPasswordReset,
    ) -> Result<usize, StorageError> {
        operations::identity_operations::reset_local_password(&self.pool, request)
            .await
            .map_err(map_postgres_error)
    }

    async fn ensure_identity_scope(
        &self,
        request: StorageIdentityScopeEnsure,
    ) -> Result<StorageIdentityScope, StorageError> {
        operations::identity_operations::ensure_identity_scope(&self.pool, request)
            .await
            .map_err(map_postgres_error)
    }

    async fn identity_scope_name(&self, scope_id: i32) -> Result<String, StorageError> {
        operations::identity_operations::identity_scope_name(&self.pool, scope_id)
            .await
            .map_err(map_postgres_error)
    }

    async fn identity_scope_names(
        &self,
        scope_ids: Vec<i32>,
    ) -> Result<Vec<(i32, String)>, StorageError> {
        operations::identity_operations::identity_scope_names(&self.pool, scope_ids)
            .await
            .map_err(map_postgres_error)
    }

    async fn load_principal_group(
        &self,
        principal_id: i32,
        group_id: i32,
    ) -> Result<StoragePrincipalGroup, StorageError> {
        operations::identity_operations::load_principal_group(&self.pool, principal_id, group_id)
            .await
            .map_err(map_postgres_error)
    }

    async fn list_principal_groups(
        &self,
        query: StoragePrincipalGroupListQuery,
    ) -> Result<StorageIdentityPage<StorageIdentityGroup>, StorageError> {
        operations::identity_operations::list_principal_groups(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn list_groups(
        &self,
        query: StorageGroupListQuery,
    ) -> Result<StorageIdentityPage<StorageIdentityGroup>, StorageError> {
        operations::identity_operations::list_groups(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn list_retained_tokens(
        &self,
        query: StorageTokenListQuery,
    ) -> Result<StorageIdentityPage<StorageTokenMetadata>, StorageError> {
        operations::identity_operations::list_retained_tokens(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn is_human_owner_group_member(
        &self,
        principal_id: i32,
        owner_group_id: i32,
    ) -> Result<bool, StorageError> {
        operations::identity_operations::is_human_owner_group_member(
            &self.pool,
            principal_id,
            owner_group_id,
        )
        .await
        .map_err(map_postgres_error)
    }

    async fn principal_is_disabled(&self, principal_id: i32) -> Result<bool, StorageError> {
        operations::identity_operations::principal_is_disabled(&self.pool, principal_id)
            .await
            .map_err(map_postgres_error)
    }

    async fn load_service_account(
        &self,
        service_account_id: i32,
    ) -> Result<StorageServiceAccount, StorageError> {
        operations::identity_operations::load_service_account(&self.pool, service_account_id)
            .await
            .map_err(map_postgres_error)
    }

    async fn load_service_account_point(
        &self,
        service_account_id: i32,
    ) -> Result<StorageServiceAccountPoint, StorageError> {
        operations::identity_operations::load_service_account_point(&self.pool, service_account_id)
            .await
            .map_err(map_postgres_error)
    }

    async fn list_manageable_service_accounts(
        &self,
        query: StorageServiceAccountListQuery,
    ) -> Result<StorageIdentityPage<StorageServiceAccountListItem>, StorageError> {
        operations::identity_operations::list_manageable_service_accounts(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn create_service_account(
        &self,
        request: StorageServiceAccountCreate,
    ) -> Result<StorageServiceAccount, StorageError> {
        operations::identity_operations::create_service_account(&self.pool, request)
            .await
            .map_err(map_postgres_error)
    }

    async fn update_service_account(
        &self,
        request: StorageServiceAccountUpdate,
    ) -> Result<StorageServiceAccount, StorageError> {
        operations::identity_operations::update_service_account(&self.pool, request)
            .await
            .map_err(map_postgres_error)
    }

    async fn disable_service_account(
        &self,
        request: StorageServiceAccountMutation,
    ) -> Result<StorageServiceAccount, StorageError> {
        operations::identity_operations::disable_service_account(&self.pool, request)
            .await
            .map_err(map_postgres_error)
    }

    async fn delete_service_account(
        &self,
        request: StorageServiceAccountMutation,
    ) -> Result<(), StorageError> {
        operations::identity_operations::delete_service_account(&self.pool, request)
            .await
            .map_err(map_postgres_error)
    }

    async fn external_principal_state(
        &self,
        principal_id: i32,
    ) -> Result<Option<StorageExternalPrincipalState>, StorageError> {
        operations::identity_operations::external_principal_state(&self.pool, principal_id)
            .await
            .map_err(map_postgres_error)
    }

    async fn mark_external_sync_attempted(&self, principal_id: i32) -> Result<(), StorageError> {
        operations::identity_operations::mark_external_sync_attempted(&self.pool, principal_id)
            .await
            .map_err(map_postgres_error)
    }

    async fn sync_external_user(
        &self,
        request: StorageExternalUserSync,
    ) -> Result<StorageSyncedHuman, StorageError> {
        operations::identity_operations::sync_external_user(&self.pool, request)
            .await
            .map_err(map_postgres_error)
    }
}

#[async_trait]
impl UserStorage for PostgresStorage {
    async fn load_user(&self, id: i32) -> Result<StorageUser, StorageError> {
        operations::identity_operations::load_user(&self.pool, id)
            .await
            .map_err(map_postgres_error)
    }

    async fn load_user_by_name(
        &self,
        identity_scope: String,
        name: String,
    ) -> Result<StorageUser, StorageError> {
        operations::identity_operations::load_user_by_name(&self.pool, identity_scope, name)
            .await
            .map_err(map_postgres_error)
    }

    async fn load_user_point(&self, id: i32) -> Result<StorageUserPoint, StorageError> {
        operations::identity_operations::load_user_point(&self.pool, id)
            .await
            .map_err(map_postgres_error)
    }

    async fn list_users(
        &self,
        query: StorageUserListQuery,
    ) -> Result<StorageIdentityPage<StorageUserListItem>, StorageError> {
        operations::identity_operations::list_users(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn create_user(&self, request: StorageUserCreate) -> Result<StorageUser, StorageError> {
        operations::identity_operations::create_user(&self.pool, request)
            .await
            .map_err(map_postgres_error)
    }

    async fn update_user(&self, request: StorageUserUpdate) -> Result<StorageUser, StorageError> {
        operations::identity_operations::update_user(&self.pool, request)
            .await
            .map_err(map_postgres_error)
    }

    async fn set_user_password(
        &self,
        request: StorageUserPasswordUpdate,
    ) -> Result<usize, StorageError> {
        operations::identity_operations::set_user_password(&self.pool, request)
            .await
            .map_err(map_postgres_error)
    }

    async fn delete_user(&self, request: StorageUserDelete) -> Result<usize, StorageError> {
        operations::identity_operations::delete_user(&self.pool, request)
            .await
            .map_err(map_postgres_error)
    }

    async fn anonymize_user(&self, id: i32) -> Result<(), StorageError> {
        operations::identity_operations::anonymize_user(&self.pool, id)
            .await
            .map_err(map_postgres_error)
    }
}

#[async_trait]
impl TokenStorage for PostgresStorage {
    async fn create_token(
        &self,
        request: StorageTokenCreate,
    ) -> Result<StorageTokenMetadata, StorageError> {
        operations::identity_operations::create_token(&self.pool, request)
            .await
            .map_err(map_postgres_error)
    }

    async fn renew_token(
        &self,
        request: StorageTokenRenew,
    ) -> Result<StorageTokenMetadata, StorageError> {
        operations::identity_operations::renew_token(&self.pool, request)
            .await
            .map_err(map_postgres_error)
    }

    async fn load_token_metadata(
        &self,
        principal_id: i32,
        token_id: i32,
    ) -> Result<StorageTokenMetadata, StorageError> {
        operations::identity_operations::load_token_metadata(&self.pool, principal_id, token_id)
            .await
            .map_err(map_postgres_error)
    }

    async fn load_token_metadata_batch(
        &self,
        token_ids: Vec<i32>,
    ) -> Result<Vec<StorageTokenMetadata>, StorageError> {
        operations::identity_operations::load_token_metadata_batch(&self.pool, token_ids)
            .await
            .map_err(map_postgres_error)
    }

    async fn revoke_token(&self, request: StorageTokenRevoke) -> Result<usize, StorageError> {
        operations::identity_operations::revoke_token(&self.pool, request)
            .await
            .map_err(map_postgres_error)
    }

    async fn revoke_token_by_hash(
        &self,
        request: StorageTokenHashRevoke,
    ) -> Result<usize, StorageError> {
        operations::identity_operations::revoke_token_by_hash(&self.pool, request)
            .await
            .map_err(map_postgres_error)
    }

    async fn revoke_all_principal_tokens(&self, principal_id: i32) -> Result<usize, StorageError> {
        operations::identity_operations::revoke_all_principal_tokens(&self.pool, principal_id)
            .await
            .map_err(map_postgres_error)
    }
}

#[async_trait]
impl AuthorizationStorage for PostgresStorage {
    async fn load_authorization_principal(
        &self,
        principal_id: i32,
    ) -> Result<AuthorizationPrincipal, StorageError> {
        operations::authorization::load_authorization_principal(&self.pool, principal_id)
            .await
            .map_err(map_postgres_error)
    }

    async fn authorization_principal_is_group_member(
        &self,
        query: AuthorizationGroupMembershipQuery,
    ) -> Result<bool, StorageError> {
        operations::authorization::authorization_principal_is_group_member(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn load_authorization_classes(
        &self,
        query: AuthorizationResourceIds,
    ) -> Result<Vec<AuthorizationClassResource>, StorageError> {
        operations::authorization::load_authorization_classes(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn load_authorization_objects(
        &self,
        query: AuthorizationResourceIds,
    ) -> Result<Vec<AuthorizationObjectResource>, StorageError> {
        operations::authorization::load_authorization_objects(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn authorize_local_collection(
        &self,
        query: AuthorizationCollectionAccessQuery,
    ) -> Result<bool, StorageError> {
        operations::authorization::authorize_local_collection(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn authorize_local_collections(
        &self,
        query: AuthorizationCollectionsAccessQuery,
    ) -> Result<bool, StorageError> {
        operations::authorization::authorize_local_collections(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn local_authorized_collections(
        &self,
        query: AuthorizationCollectionsQuery,
    ) -> Result<Vec<AuthorizationCollection>, StorageError> {
        operations::authorization::local_authorized_collections(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn list_authorization_collection_candidates(
        &self,
    ) -> Result<Vec<AuthorizationCollection>, StorageError> {
        operations::authorization::list_authorization_collection_candidates(&self.pool)
            .await
            .map_err(map_postgres_error)
    }

    async fn list_authorization_group_candidates(
        &self,
        query_options: QueryOptions,
    ) -> Result<Vec<AuthorizationGroup>, StorageError> {
        operations::authorization::list_authorization_group_candidates(&self.pool, query_options)
            .await
            .map_err(map_postgres_error)
    }

    async fn authorization_policy_snapshot(
        &self,
    ) -> Result<Vec<AuthorizationPolicySnapshotRow>, StorageError> {
        operations::authorization::authorization_policy_snapshot(&self.pool)
            .await
            .map_err(map_postgres_error)
    }

    async fn list_local_collection_grants(
        &self,
        query: AuthorizationCollectionGrantListQuery,
    ) -> Result<AuthorizationGroupGrantPage, StorageError> {
        operations::authorization::list_local_collection_grants(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn get_local_collection_grant(
        &self,
        key: AuthorizationGrantKey,
    ) -> Result<Option<AuthorizationGrant>, StorageError> {
        operations::authorization::get_local_collection_grant(&self.pool, key)
            .await
            .map_err(map_postgres_error)
    }

    async fn load_local_collection_permission_set(
        &self,
        query: AuthorizationPermissionSetQuery,
    ) -> Result<AuthorizationPermissionSet, StorageError> {
        operations::authorization::load_local_collection_permission_set(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn apply_local_collection_grant(
        &self,
        mutation: AuthorizationGrantMutation,
    ) -> Result<AuthorizationGrant, StorageError> {
        operations::authorization::apply_local_collection_grant(&self.pool, mutation)
            .await
            .map_err(map_postgres_error)
    }

    async fn revoke_local_collection_grant(
        &self,
        mutation: AuthorizationGrantMutation,
    ) -> Result<AuthorizationGrant, StorageError> {
        operations::authorization::revoke_local_collection_grant(&self.pool, mutation)
            .await
            .map_err(map_postgres_error)
    }

    async fn revoke_all_local_collection_grants(
        &self,
        request: AuthorizationGrantDelete,
    ) -> Result<(), StorageError> {
        operations::authorization::revoke_all_local_collection_grants(&self.pool, request)
            .await
            .map_err(map_postgres_error)
    }
}
