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

    async fn get_authentication_identity(
        &self,
        principal_id: PrincipalId,
    ) -> Result<AuthenticationIdentity, StorageError> {
        crate::operations::authentication::get_authentication_identity(
            self.runtime(),
            principal_id.id(),
        )
        .await
        .map_err(StorageError::from)
    }

    async fn get_authentication_token_scope(
        &self,
        query: AuthenticationTokenScopeQuery,
    ) -> Result<Option<AuthenticationTokenScope>, StorageError> {
        crate::operations::authentication::get_authentication_token_scope(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }
}

#[async_trait]
impl LocalIdentityCredentialStorage for PostgresStorage {
    async fn is_default_admin_bootstrap_required(&self) -> Result<bool, StorageError> {
        crate::operations::bootstrap::is_default_admin_bootstrap_required(self.runtime())
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
}

#[async_trait]
impl IdentityScopeStorage for PostgresStorage {
    async fn ensure_identity_scope(
        &self,
        request: StorageIdentityScopeEnsure,
    ) -> Result<StorageIdentityScope, StorageError> {
        crate::operations::identity_scope::ensure_identity_scope(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn resolve_identity_scope_name(
        &self,
        scope_id: IdentityScopeId,
    ) -> Result<String, StorageError> {
        crate::operations::identity_scope::resolve_identity_scope_name(
            self.runtime(),
            scope_id.id(),
        )
        .await
        .map_err(StorageError::from)
    }

    async fn resolve_identity_scope_names(
        &self,
        scope_ids: Vec<IdentityScopeId>,
    ) -> Result<Vec<(IdentityScopeId, String)>, StorageError> {
        let scope_ids = scope_ids.into_iter().map(IdentityScopeId::id).collect();
        crate::operations::identity_scope::resolve_identity_scope_names(self.runtime(), scope_ids)
            .await?
            .into_iter()
            .map(|(id, name)| {
                IdentityScopeId::new(id)
                    .map(|id| (id, name))
                    .map_err(|error| StorageError::internal(error.to_string()))
            })
            .collect()
    }
}

#[async_trait]
impl IdentityMembershipStorage for PostgresStorage {
    async fn get_principal_group(
        &self,
        principal_id: PrincipalId,
        group_id: GroupId,
    ) -> Result<StoragePrincipalGroup, StorageError> {
        crate::operations::identity_principals::get_principal_group(
            self.runtime(),
            principal_id.id(),
            group_id.id(),
        )
        .await
        .map_err(StorageError::from)
    }

    async fn list_principal_groups(
        &self,
        query: StoragePrincipalGroupListQuery,
    ) -> Result<StoragePage<StorageIdentityGroup>, StorageError> {
        crate::operations::group::list_principal_groups(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn is_human_owner_group_member(
        &self,
        principal_id: PrincipalId,
        owner_group_id: GroupId,
    ) -> Result<bool, StorageError> {
        crate::operations::identity_principals::is_human_owner_group_member(
            self.runtime(),
            principal_id.id(),
            owner_group_id.id(),
        )
        .await
        .map_err(StorageError::from)
    }
}

#[async_trait]
impl ServiceAccountStorage for PostgresStorage {
    async fn is_service_account_disabled(
        &self,
        principal_id: PrincipalId,
    ) -> Result<bool, StorageError> {
        crate::operations::identity_principals::is_service_account_disabled(
            self.runtime(),
            principal_id.id(),
        )
        .await
        .map_err(StorageError::from)
    }

    async fn get_service_account(
        &self,
        service_account_id: ServiceAccountId,
    ) -> Result<StorageServiceAccount, StorageError> {
        crate::operations::service_account::get_service_account(
            self.runtime(),
            service_account_id.id(),
        )
        .await
        .map_err(StorageError::from)
    }

    async fn get_service_account_point(
        &self,
        service_account_id: ServiceAccountId,
    ) -> Result<StorageServiceAccountPoint, StorageError> {
        crate::operations::service_account::get_service_account_point(
            self.runtime(),
            service_account_id.id(),
        )
        .await
        .map_err(StorageError::from)
    }

    async fn list_manageable_service_accounts(
        &self,
        query: StorageServiceAccountListQuery,
    ) -> Result<StoragePage<StorageServiceAccountListItem>, StorageError> {
        crate::operations::service_account::list_manageable_service_accounts(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn create_service_account(
        &self,
        request: StorageServiceAccountCreate,
    ) -> Result<MutationOutcome<StorageServiceAccount>, StorageError> {
        crate::operations::service_account::create_service_account(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn update_service_account(
        &self,
        request: StorageServiceAccountUpdate,
    ) -> Result<MutationOutcome<StorageServiceAccount>, StorageError> {
        crate::operations::service_account::update_service_account(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn disable_service_account(
        &self,
        request: StorageServiceAccountMutation,
    ) -> Result<MutationOutcome<StorageServiceAccountDisableOutcome>, StorageError> {
        crate::operations::service_account::disable_service_account(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn delete_service_account(
        &self,
        request: StorageServiceAccountMutation,
    ) -> Result<MutationOutcome<()>, StorageError> {
        crate::operations::service_account::delete_service_account(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }
}

#[async_trait]
impl ExternalIdentityStorage for PostgresStorage {
    async fn get_external_principal_state(
        &self,
        principal_id: PrincipalId,
    ) -> Result<Option<StorageExternalPrincipalState>, StorageError> {
        crate::operations::external_identity::get_external_principal_state(
            self.runtime(),
            principal_id.id(),
        )
        .await
        .map_err(StorageError::from)
    }

    async fn mark_external_sync_attempted(
        &self,
        principal_id: PrincipalId,
    ) -> Result<(), StorageError> {
        crate::operations::external_identity::mark_external_sync_attempted(
            self.runtime(),
            principal_id.id(),
        )
        .await
        .map_err(StorageError::from)
    }

    async fn sync_external_user(
        &self,
        request: StorageExternalUserSync,
    ) -> Result<MutationOutcome<StorageSyncedHuman>, StorageError> {
        crate::operations::external_identity::sync_external_user(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }
}

#[async_trait]
impl UserStorage for PostgresStorage {
    async fn get_user(&self, id: UserId) -> Result<StorageUser, StorageError> {
        crate::operations::user::get_user(self.runtime(), id.id())
            .await
            .map_err(StorageError::from)
    }

    async fn get_user_by_name(
        &self,
        identity_scope: String,
        name: String,
    ) -> Result<StorageUser, StorageError> {
        crate::operations::user::get_user_by_name(self.runtime(), identity_scope, name)
            .await
            .map_err(StorageError::from)
    }

    async fn get_user_point(&self, id: UserId) -> Result<StorageUserPoint, StorageError> {
        crate::operations::user::get_user_point(self.runtime(), id.id())
            .await
            .map_err(StorageError::from)
    }

    async fn list_users(
        &self,
        query: StorageUserListQuery,
    ) -> Result<StoragePage<StorageUserListItem>, StorageError> {
        crate::operations::user::list_users(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn create_user(
        &self,
        request: StorageUserCreate,
    ) -> Result<MutationOutcome<StorageUser>, StorageError> {
        crate::operations::user::create_user(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn update_user(
        &self,
        request: StorageUserUpdate,
    ) -> Result<MutationOutcome<StorageUser>, StorageError> {
        crate::operations::user::update_user(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn set_user_password(
        &self,
        request: StorageUserPasswordUpdate,
    ) -> Result<MutationOutcome<usize>, StorageError> {
        crate::operations::user::set_user_password(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn delete_user(
        &self,
        request: StorageUserDelete,
    ) -> Result<MutationOutcome<usize>, StorageError> {
        crate::operations::user::delete_user(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn anonymize_user(
        &self,
        request: StorageUserAnonymize,
    ) -> Result<MutationOutcome<()>, StorageError> {
        crate::operations::user::anonymize_user(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }
}

#[async_trait]
impl TokenStorage for PostgresStorage {
    async fn list_retained_tokens(
        &self,
        query: StorageTokenListQuery,
    ) -> Result<StoragePage<StorageTokenMetadata>, StorageError> {
        crate::operations::token::list_retained_tokens(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn create_token(
        &self,
        request: StorageTokenCreate,
    ) -> Result<MutationOutcome<StorageTokenMetadata>, StorageError> {
        crate::operations::token::create_token(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn renew_token(
        &self,
        request: StorageTokenRenew,
    ) -> Result<MutationOutcome<StorageTokenMetadata>, StorageError> {
        crate::operations::token::renew_token(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn get_token_metadata(
        &self,
        principal_id: PrincipalId,
        token_id: TokenId,
        observation: StorageTokenObservation,
    ) -> Result<StorageTokenMetadata, StorageError> {
        crate::operations::token::get_token_metadata(
            self.runtime(),
            principal_id.id(),
            token_id.id(),
            observation,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn get_token_metadata_by_ids(
        &self,
        token_ids: Vec<TokenId>,
        observation: StorageTokenObservation,
    ) -> Result<Vec<StorageTokenMetadata>, StorageError> {
        let token_ids = token_ids.into_iter().map(TokenId::id).collect();
        crate::operations::token::get_token_metadata_by_ids(self.runtime(), token_ids, observation)
            .await
            .map_err(StorageError::from)
    }

    async fn revoke_token(
        &self,
        request: StorageTokenRevoke,
    ) -> Result<MutationOutcome<usize>, StorageError> {
        crate::operations::token::revoke_token(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn revoke_token_by_hash(
        &self,
        request: StorageTokenHashRevoke,
    ) -> Result<MutationOutcome<usize>, StorageError> {
        crate::operations::token::revoke_token_by_hash(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn revoke_all_principal_tokens(
        &self,
        request: StoragePrincipalTokensRevoke,
    ) -> Result<MutationOutcome<usize>, StorageError> {
        crate::operations::token::revoke_all_principal_tokens(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }
}

#[async_trait]
impl AuthorizationDataStorage for PostgresStorage {
    async fn get_authorization_principal(
        &self,
        principal_id: PrincipalId,
    ) -> Result<AuthorizationPrincipal, StorageError> {
        crate::operations::authorization::get_authorization_principal(
            self.runtime(),
            principal_id.id(),
        )
        .await
        .map_err(StorageError::from)
    }

    async fn is_authorization_principal_group_member(
        &self,
        query: AuthorizationGroupMembershipQuery,
    ) -> Result<bool, StorageError> {
        crate::operations::authorization::is_authorization_principal_group_member(
            self.runtime(),
            query,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn list_authorization_classes(
        &self,
        query: AuthorizationResourceIds,
    ) -> Result<Vec<AuthorizationClassResource>, StorageError> {
        crate::operations::authorization::list_authorization_classes(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn list_authorization_objects(
        &self,
        query: AuthorizationResourceIds,
    ) -> Result<Vec<AuthorizationObjectResource>, StorageError> {
        crate::operations::authorization::list_authorization_objects(self.runtime(), query)
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

    async fn list_local_authorized_collections(
        &self,
        query: AuthorizationCollectionsQuery,
    ) -> Result<Vec<AuthorizationCollection>, StorageError> {
        crate::operations::authorization::list_local_authorized_collections(self.runtime(), query)
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

    async fn get_authorization_policy_snapshot(
        &self,
    ) -> Result<Vec<AuthorizationPolicySnapshotRow>, StorageError> {
        crate::operations::authorization::get_authorization_policy_snapshot(self.runtime())
            .await
            .map_err(StorageError::from)
    }

    async fn list_local_collection_grants(
        &self,
        query: AuthorizationCollectionGrantListQuery,
    ) -> Result<StoragePage<AuthorizationGroupGrant>, StorageError> {
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

    async fn get_local_collection_permission_set(
        &self,
        query: AuthorizationPermissionSetQuery,
    ) -> Result<AuthorizationPermissionSet, StorageError> {
        crate::operations::authorization::get_local_collection_permission_set(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn apply_local_collection_grant(
        &self,
        mutation: AuthorizationGrantMutation,
    ) -> Result<MutationOutcome<AuthorizationGrant>, StorageError> {
        crate::operations::authorization::apply_local_collection_grant(self.runtime(), mutation)
            .await
            .map_err(StorageError::from)
    }

    async fn revoke_local_collection_grant(
        &self,
        mutation: AuthorizationGrantMutation,
    ) -> Result<MutationOutcome<AuthorizationGrant>, StorageError> {
        crate::operations::authorization::revoke_local_collection_grant(self.runtime(), mutation)
            .await
            .map_err(StorageError::from)
    }

    async fn revoke_all_local_collection_grants(
        &self,
        request: AuthorizationGrantDelete,
    ) -> Result<MutationOutcome<()>, StorageError> {
        crate::operations::authorization::revoke_all_local_collection_grants(
            self.runtime(),
            request,
        )
        .await
        .map_err(StorageError::from)
    }
}
