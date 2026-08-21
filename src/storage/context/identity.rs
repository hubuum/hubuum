use super::*;

#[async_trait]
impl AuthenticationStorage for StorageHandle {
    async fn authenticate_bearer_token(
        &self,
        attempt: AuthenticationAttempt,
    ) -> Result<AuthenticatedToken, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Authentication,
            "authenticate_bearer_token",
            async {
                dispatch_backend!(self, |backend| {
                    backend.authenticate_bearer_token(attempt).await
                })
            },
        )
        .await
    }

    async fn get_authentication_identity(
        &self,
        principal_id: PrincipalId,
    ) -> Result<AuthenticationIdentity, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Authentication,
            "get_identity",
            async {
                dispatch_backend!(self, |backend| {
                    backend.get_authentication_identity(principal_id).await
                })
            },
        )
        .await
    }

    async fn get_authentication_token_scope(
        &self,
        query: AuthenticationTokenScopeQuery,
    ) -> Result<Option<AuthenticationTokenScope>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Authentication,
            "get_token_scope",
            async {
                dispatch_backend!(self, |backend| {
                    backend.get_authentication_token_scope(query).await
                })
            },
        )
        .await
    }
}

#[async_trait]
impl LocalIdentityCredentialStorage for StorageHandle {
    async fn is_default_admin_bootstrap_required(&self) -> Result<bool, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::LocalIdentityCredentials,
            "is_default_admin_bootstrap_required",
            async {
                dispatch_backend!(self, |backend| {
                    backend.is_default_admin_bootstrap_required().await
                })
            },
        )
        .await
    }

    async fn bootstrap_default_admin(
        &self,
        request: StorageDefaultAdminBootstrap,
    ) -> Result<bool, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::LocalIdentityCredentials,
            "bootstrap_default_admin",
            async {
                dispatch_backend!(self, |backend| {
                    backend.bootstrap_default_admin(request).await
                })
            },
        )
        .await
    }

    async fn reset_local_password(
        &self,
        request: StorageLocalPasswordReset,
    ) -> Result<usize, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::LocalIdentityCredentials,
            "reset_local_password",
            async {
                dispatch_backend!(self, |backend| {
                    backend.reset_local_password(request).await
                })
            },
        )
        .await
    }
}

#[async_trait]
impl IdentityScopeStorage for StorageHandle {
    async fn ensure_identity_scope(
        &self,
        request: StorageIdentityScopeEnsure,
    ) -> Result<StorageIdentityScope, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::IdentityScopes,
            "ensure_scope",
            async {
                dispatch_backend!(self, |backend| {
                    backend.ensure_identity_scope(request).await
                })
            },
        )
        .await
    }

    async fn resolve_identity_scope_name(
        &self,
        scope_id: IdentityScopeId,
    ) -> Result<String, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::IdentityScopes,
            "resolve_scope_name",
            async {
                dispatch_backend!(self, |backend| {
                    backend.resolve_identity_scope_name(scope_id).await
                })
            },
        )
        .await
    }

    async fn resolve_identity_scope_names(
        &self,
        scope_ids: Vec<IdentityScopeId>,
    ) -> Result<Vec<(IdentityScopeId, String)>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::IdentityScopes,
            "resolve_scope_names",
            async {
                dispatch_backend!(self, |backend| {
                    backend.resolve_identity_scope_names(scope_ids).await
                })
            },
        )
        .await
    }
}

#[async_trait]
impl IdentityMembershipStorage for StorageHandle {
    async fn get_principal_group(
        &self,
        principal_id: PrincipalId,
        group_id: GroupId,
    ) -> Result<StoragePrincipalGroup, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::IdentityMembership,
            "get_membership",
            async {
                dispatch_backend!(self, |backend| {
                    backend.get_principal_group(principal_id, group_id).await
                })
            },
        )
        .await
    }

    async fn list_principal_groups(
        &self,
        query: StoragePrincipalGroupListQuery,
    ) -> Result<StoragePage<StorageIdentityGroup>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::IdentityMembership,
            "list_principal_groups",
            async {
                dispatch_backend!(self, |backend| {
                    backend.list_principal_groups(query).await
                })
            },
        )
        .await
    }

    async fn is_human_owner_group_member(
        &self,
        principal_id: PrincipalId,
        owner_group_id: GroupId,
    ) -> Result<bool, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::IdentityMembership,
            "human_owner_group_member",
            async {
                dispatch_backend!(self, |backend| {
                    backend
                        .is_human_owner_group_member(principal_id, owner_group_id)
                        .await
                })
            },
        )
        .await
    }
}

#[async_trait]
impl ServiceAccountStorage for StorageHandle {
    async fn is_service_account_disabled(
        &self,
        principal_id: PrincipalId,
    ) -> Result<bool, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::ServiceAccounts,
            "is_service_account_disabled",
            async {
                dispatch_backend!(self, |backend| {
                    backend.is_service_account_disabled(principal_id).await
                })
            },
        )
        .await
    }

    async fn get_service_account(
        &self,
        service_account_id: ServiceAccountId,
    ) -> Result<StorageServiceAccount, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::ServiceAccounts,
            "get_service_account",
            async {
                dispatch_backend!(self, |backend| {
                    backend.get_service_account(service_account_id).await
                })
            },
        )
        .await
    }

    async fn get_service_account_point(
        &self,
        service_account_id: ServiceAccountId,
    ) -> Result<StorageServiceAccountPoint, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::ServiceAccounts,
            "get_service_account_point",
            async {
                dispatch_backend!(self, |backend| {
                    backend.get_service_account_point(service_account_id).await
                })
            },
        )
        .await
    }

    async fn list_manageable_service_accounts(
        &self,
        query: StorageServiceAccountListQuery,
    ) -> Result<StoragePage<StorageServiceAccountListItem>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::ServiceAccounts,
            "list_service_accounts",
            async {
                dispatch_backend!(self, |backend| {
                    backend.list_manageable_service_accounts(query).await
                })
            },
        )
        .await
    }

    async fn create_service_account(
        &self,
        request: StorageServiceAccountCreate,
    ) -> Result<MutationOutcome<StorageServiceAccount>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::ServiceAccounts,
            "create_service_account",
            async {
                dispatch_backend!(self, |backend| {
                    backend.create_service_account(request).await
                })
            },
        )
        .await
    }

    async fn update_service_account(
        &self,
        request: StorageServiceAccountUpdate,
    ) -> Result<MutationOutcome<StorageServiceAccount>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::ServiceAccounts,
            "update_service_account",
            async {
                dispatch_backend!(self, |backend| {
                    backend.update_service_account(request).await
                })
            },
        )
        .await
    }

    async fn disable_service_account(
        &self,
        request: StorageServiceAccountMutation,
    ) -> Result<MutationOutcome<StorageServiceAccountDisableOutcome>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::ServiceAccounts,
            "disable_service_account",
            async {
                dispatch_backend!(self, |backend| {
                    backend.disable_service_account(request).await
                })
            },
        )
        .await
    }

    async fn delete_service_account(
        &self,
        request: StorageServiceAccountMutation,
    ) -> Result<MutationOutcome<()>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::ServiceAccounts,
            "delete_service_account",
            async {
                dispatch_backend!(self, |backend| {
                    backend.delete_service_account(request).await
                })
            },
        )
        .await
    }
}

#[async_trait]
impl ExternalIdentityStorage for StorageHandle {
    async fn get_external_principal_state(
        &self,
        principal_id: PrincipalId,
    ) -> Result<Option<StorageExternalPrincipalState>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::ExternalIdentity,
            "get_external_state",
            async {
                dispatch_backend!(self, |backend| {
                    backend.get_external_principal_state(principal_id).await
                })
            },
        )
        .await
    }

    async fn mark_external_sync_attempted(
        &self,
        principal_id: PrincipalId,
    ) -> Result<(), StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::ExternalIdentity,
            "mark_external_sync_attempted",
            async {
                dispatch_backend!(self, |backend| {
                    backend.mark_external_sync_attempted(principal_id).await
                })
            },
        )
        .await
    }

    async fn sync_external_user(
        &self,
        request: StorageExternalUserSync,
    ) -> Result<MutationOutcome<StorageSyncedHuman>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::ExternalIdentity,
            "sync_external_user",
            async {
                dispatch_backend!(self, |backend| {
                    backend.sync_external_user(request).await
                })
            },
        )
        .await
    }
}

#[async_trait]
impl UserStorage for StorageHandle {
    async fn get_user(&self, id: UserId) -> Result<StorageUser, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Users,
            "get",
            async { dispatch_backend!(self, |backend| backend.get_user(id).await) },
        )
        .await
    }

    async fn get_user_by_name(
        &self,
        identity_scope: String,
        name: String,
    ) -> Result<StorageUser, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Users,
            "get_by_name",
            async {
                dispatch_backend!(self, |backend| {
                    backend.get_user_by_name(identity_scope, name).await
                })
            },
        )
        .await
    }

    async fn get_user_point(&self, id: UserId) -> Result<StorageUserPoint, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Users,
            "get_point",
            async { dispatch_backend!(self, |backend| backend.get_user_point(id).await) },
        )
        .await
    }

    async fn list_users(
        &self,
        query: StorageUserListQuery,
    ) -> Result<StoragePage<StorageUserListItem>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Users,
            "list",
            async { dispatch_backend!(self, |backend| backend.list_users(query).await) },
        )
        .await
    }

    async fn create_user(
        &self,
        request: StorageUserCreate,
    ) -> Result<MutationOutcome<StorageUser>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Users,
            "create",
            async { dispatch_backend!(self, |backend| backend.create_user(request).await) },
        )
        .await
    }

    async fn update_user(
        &self,
        request: StorageUserUpdate,
    ) -> Result<MutationOutcome<StorageUser>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Users,
            "update",
            async { dispatch_backend!(self, |backend| backend.update_user(request).await) },
        )
        .await
    }

    async fn set_user_password(
        &self,
        request: StorageUserPasswordUpdate,
    ) -> Result<MutationOutcome<usize>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Users,
            "set_password",
            async {
                dispatch_backend!(self, |backend| { backend.set_user_password(request).await })
            },
        )
        .await
    }

    async fn delete_user(
        &self,
        request: StorageUserDelete,
    ) -> Result<MutationOutcome<usize>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Users,
            "delete",
            async { dispatch_backend!(self, |backend| backend.delete_user(request).await) },
        )
        .await
    }

    async fn anonymize_user(
        &self,
        request: StorageUserAnonymize,
    ) -> Result<MutationOutcome<()>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Users,
            "anonymize",
            async { dispatch_backend!(self, |backend| backend.anonymize_user(request).await) },
        )
        .await
    }
}

#[async_trait]
impl TokenStorage for StorageHandle {
    async fn list_retained_tokens(
        &self,
        query: StorageTokenListQuery,
    ) -> Result<StoragePage<StorageTokenMetadata>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Tokens,
            "list_retained",
            async {
                dispatch_backend!(self, |backend| {
                    backend.list_retained_tokens(query).await
                })
            },
        )
        .await
    }

    async fn create_token(
        &self,
        request: StorageTokenCreate,
    ) -> Result<MutationOutcome<StorageTokenMetadata>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Tokens,
            "create",
            async { dispatch_backend!(self, |backend| backend.create_token(request).await) },
        )
        .await
    }

    async fn renew_token(
        &self,
        request: StorageTokenRenew,
    ) -> Result<MutationOutcome<StorageTokenMetadata>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Tokens,
            "renew",
            async { dispatch_backend!(self, |backend| backend.renew_token(request).await) },
        )
        .await
    }

    async fn get_token_metadata(
        &self,
        principal_id: PrincipalId,
        token_id: TokenId,
        observation: StorageTokenObservation,
    ) -> Result<StorageTokenMetadata, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Tokens,
            "get_metadata",
            async {
                dispatch_backend!(self, |backend| {
                    backend
                        .get_token_metadata(principal_id, token_id, observation)
                        .await
                })
            },
        )
        .await
    }

    async fn get_token_metadata_by_ids(
        &self,
        token_ids: Vec<TokenId>,
        observation: StorageTokenObservation,
    ) -> Result<Vec<StorageTokenMetadata>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Tokens,
            "get_metadata_batch",
            async {
                dispatch_backend!(self, |backend| {
                    backend
                        .get_token_metadata_by_ids(token_ids, observation)
                        .await
                })
            },
        )
        .await
    }

    async fn revoke_token(
        &self,
        request: StorageTokenRevoke,
    ) -> Result<MutationOutcome<usize>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Tokens,
            "revoke",
            async { dispatch_backend!(self, |backend| backend.revoke_token(request).await) },
        )
        .await
    }

    async fn revoke_token_by_hash(
        &self,
        request: StorageTokenHashRevoke,
    ) -> Result<MutationOutcome<usize>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Tokens,
            "revoke_by_hash",
            async {
                dispatch_backend!(self, |backend| {
                    backend.revoke_token_by_hash(request).await
                })
            },
        )
        .await
    }

    async fn revoke_all_principal_tokens(
        &self,
        request: StoragePrincipalTokensRevoke,
    ) -> Result<MutationOutcome<usize>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Tokens,
            "revoke_all",
            async {
                dispatch_backend!(self, |backend| backend
                    .revoke_all_principal_tokens(request)
                    .await)
            },
        )
        .await
    }
}

#[async_trait]
impl AuthorizationDataStorage for StorageHandle {
    async fn get_authorization_principal(
        &self,
        principal_id: PrincipalId,
    ) -> Result<AuthorizationPrincipal, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::AuthorizationData,
            "get_principal",
            async {
                dispatch_backend!(self, |backend| {
                    backend.get_authorization_principal(principal_id).await
                })
            },
        )
        .await
    }

    async fn is_authorization_principal_group_member(
        &self,
        query: AuthorizationGroupMembershipQuery,
    ) -> Result<bool, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::AuthorizationData,
            "principal_is_group_member",
            async {
                dispatch_backend!(self, |backend| {
                    backend.is_authorization_principal_group_member(query).await
                })
            },
        )
        .await
    }

    async fn list_authorization_classes(
        &self,
        query: AuthorizationResourceIds,
    ) -> Result<Vec<AuthorizationClassResource>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::AuthorizationData,
            "get_classes",
            async {
                dispatch_backend!(self, |backend| {
                    backend.list_authorization_classes(query).await
                })
            },
        )
        .await
    }

    async fn list_authorization_objects(
        &self,
        query: AuthorizationResourceIds,
    ) -> Result<Vec<AuthorizationObjectResource>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::AuthorizationData,
            "get_objects",
            async {
                dispatch_backend!(self, |backend| {
                    backend.list_authorization_objects(query).await
                })
            },
        )
        .await
    }

    async fn authorize_local_collection(
        &self,
        query: AuthorizationCollectionAccessQuery,
    ) -> Result<bool, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::AuthorizationData,
            "authorize_local_collection",
            async {
                dispatch_backend!(self, |backend| {
                    backend.authorize_local_collection(query).await
                })
            },
        )
        .await
    }

    async fn authorize_local_collections(
        &self,
        query: AuthorizationCollectionsAccessQuery,
    ) -> Result<bool, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::AuthorizationData,
            "authorize_local_collections",
            async {
                dispatch_backend!(self, |backend| {
                    backend.authorize_local_collections(query).await
                })
            },
        )
        .await
    }

    async fn list_local_authorized_collections(
        &self,
        query: AuthorizationCollectionsQuery,
    ) -> Result<Vec<AuthorizationCollection>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::AuthorizationData,
            "list_local_authorized_collections",
            async {
                dispatch_backend!(self, |backend| {
                    backend.list_local_authorized_collections(query).await
                })
            },
        )
        .await
    }

    async fn list_authorization_collection_candidates(
        &self,
    ) -> Result<Vec<AuthorizationCollection>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::AuthorizationData,
            "list_collection_candidates",
            async {
                dispatch_backend!(self, |backend| {
                    backend.list_authorization_collection_candidates().await
                })
            },
        )
        .await
    }

    async fn list_authorization_group_candidates(
        &self,
        query_options: QueryOptions,
    ) -> Result<Vec<AuthorizationGroup>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::AuthorizationData,
            "list_group_candidates",
            async {
                dispatch_backend!(self, |backend| {
                    backend
                        .list_authorization_group_candidates(query_options)
                        .await
                })
            },
        )
        .await
    }

    async fn get_authorization_policy_snapshot(
        &self,
    ) -> Result<Vec<AuthorizationPolicySnapshotRow>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::AuthorizationData,
            "policy_snapshot",
            async {
                dispatch_backend!(self, |backend| {
                    backend.get_authorization_policy_snapshot().await
                })
            },
        )
        .await
    }

    async fn list_local_collection_grants(
        &self,
        query: AuthorizationCollectionGrantListQuery,
    ) -> Result<StoragePage<AuthorizationGroupGrant>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::AuthorizationData,
            "list_local_collection_grants",
            async {
                dispatch_backend!(self, |backend| {
                    backend.list_local_collection_grants(query).await
                })
            },
        )
        .await
    }

    async fn get_local_collection_grant(
        &self,
        key: AuthorizationGrantKey,
    ) -> Result<Option<AuthorizationGrant>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::AuthorizationData,
            "get_local_collection_grant",
            async {
                dispatch_backend!(self, |backend| {
                    backend.get_local_collection_grant(key).await
                })
            },
        )
        .await
    }

    async fn get_local_collection_permission_set(
        &self,
        query: AuthorizationPermissionSetQuery,
    ) -> Result<AuthorizationPermissionSet, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::AuthorizationData,
            "get_local_collection_permission_set",
            async {
                dispatch_backend!(self, |backend| {
                    backend.get_local_collection_permission_set(query).await
                })
            },
        )
        .await
    }

    async fn apply_local_collection_grant(
        &self,
        mutation: AuthorizationGrantMutation,
    ) -> Result<MutationOutcome<AuthorizationGrant>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::AuthorizationData,
            "apply_local_collection_grant",
            async {
                dispatch_backend!(self, |backend| {
                    backend.apply_local_collection_grant(mutation).await
                })
            },
        )
        .await
    }

    async fn revoke_local_collection_grant(
        &self,
        mutation: AuthorizationGrantMutation,
    ) -> Result<MutationOutcome<AuthorizationGrant>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::AuthorizationData,
            "revoke_local_collection_grant",
            async {
                dispatch_backend!(self, |backend| {
                    backend.revoke_local_collection_grant(mutation).await
                })
            },
        )
        .await
    }

    async fn revoke_all_local_collection_grants(
        &self,
        request: AuthorizationGrantDelete,
    ) -> Result<MutationOutcome<()>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::AuthorizationData,
            "revoke_all_local_collection_grants",
            async {
                dispatch_backend!(self, |backend| {
                    backend.revoke_all_local_collection_grants(request).await
                })
            },
        )
        .await
    }
}
