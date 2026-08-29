use super::*;

#[async_trait]
impl AuthenticationStorage for StorageHandle {
    async fn authenticate_bearer_token(
        &self,
        attempt: StorageAuthenticationAttempt,
    ) -> Result<StorageAuthenticatedToken, StorageError> {
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
    ) -> Result<StorageAuthenticationIdentity, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Authentication,
            "get_authentication_identity",
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
        query: StorageAuthenticationTokenScopeQuery,
    ) -> Result<Option<StorageAuthenticationTokenScope>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Authentication,
            "get_authentication_token_scope",
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
            StorageCapability::LocalIdentityCredential,
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
            StorageCapability::LocalIdentityCredential,
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
    ) -> Result<StorageMutationOutcome<usize>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::LocalIdentityCredential,
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
            StorageCapability::IdentityScope,
            "ensure_identity_scope",
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
            StorageCapability::IdentityScope,
            "resolve_identity_scope_name",
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
            StorageCapability::IdentityScope,
            "resolve_identity_scope_names",
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
impl GroupMembershipStorage for StorageHandle {
    async fn get_principal_group(
        &self,
        principal_id: PrincipalId,
        group_id: GroupId,
    ) -> Result<StoragePrincipalGroup, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::GroupMembership,
            "get_principal_group",
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
            StorageCapability::GroupMembership,
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
            StorageCapability::GroupMembership,
            "is_human_owner_group_member",
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

    async fn load_group_member_principals(
        &self,
        group_id: GroupId,
    ) -> Result<Vec<StoragePrincipal>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::GroupMembership,
            "load_group_member_principals",
            async {
                dispatch_backend!(self, |backend| {
                    backend.load_group_member_principals(group_id).await
                })
            },
        )
        .await
    }

    async fn list_group_members(
        &self,
        group_id: GroupId,
        query_options: QueryOptions,
    ) -> Result<StoragePage<StorageGroupMember>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::GroupMembership,
            "list_group_members",
            async {
                dispatch_backend!(self, |backend| {
                    backend.list_group_members(group_id, query_options).await
                })
            },
        )
        .await
    }

    async fn add_group_member(
        &self,
        principal_id: PrincipalId,
        group_id: GroupId,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StoragePrincipalGroup>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::GroupMembership,
            "add_group_member",
            async {
                dispatch_backend!(self, |backend| {
                    backend
                        .add_group_member(principal_id, group_id, context)
                        .await
                })
            },
        )
        .await
    }

    async fn remove_group_member(
        &self,
        principal_id: PrincipalId,
        group_id: GroupId,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::GroupMembership,
            "remove_group_member",
            async {
                dispatch_backend!(self, |backend| {
                    backend
                        .remove_group_member(principal_id, group_id, context)
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
            StorageCapability::ServiceAccount,
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
            StorageCapability::ServiceAccount,
            "get_service_account",
            async {
                dispatch_backend!(self, |backend| {
                    backend.get_service_account(service_account_id).await
                })
            },
        )
        .await
    }

    async fn get_service_account_details(
        &self,
        service_account_id: ServiceAccountId,
    ) -> Result<StorageServiceAccountDetails, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::ServiceAccount,
            "get_service_account_details",
            async {
                dispatch_backend!(self, |backend| {
                    backend
                        .get_service_account_details(service_account_id)
                        .await
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
            StorageCapability::ServiceAccount,
            "list_manageable_service_accounts",
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
    ) -> Result<StorageMutationOutcome<StorageServiceAccount>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::ServiceAccount,
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
    ) -> Result<StorageMutationOutcome<StorageServiceAccount>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::ServiceAccount,
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
    ) -> Result<StorageMutationOutcome<StorageServiceAccountDisableOutcome>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::ServiceAccount,
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
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::ServiceAccount,
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
            "get_external_principal_state",
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
    ) -> Result<StorageMutationOutcome<StorageSyncedHuman>, StorageError> {
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
            StorageCapability::User,
            "get_user",
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
            StorageCapability::User,
            "get_user_by_name",
            async {
                dispatch_backend!(self, |backend| {
                    backend.get_user_by_name(identity_scope, name).await
                })
            },
        )
        .await
    }

    async fn get_user_details(&self, id: UserId) -> Result<StorageUserDetails, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::User,
            "get_user_details",
            async { dispatch_backend!(self, |backend| backend.get_user_details(id).await) },
        )
        .await
    }

    async fn list_users(
        &self,
        query: StorageUserListQuery,
    ) -> Result<StoragePage<StorageUserListItem>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::User,
            "list_users",
            async { dispatch_backend!(self, |backend| backend.list_users(query).await) },
        )
        .await
    }

    async fn create_user(
        &self,
        request: StorageUserCreate,
    ) -> Result<StorageMutationOutcome<StorageUser>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::User,
            "create_user",
            async { dispatch_backend!(self, |backend| backend.create_user(request).await) },
        )
        .await
    }

    async fn update_user(
        &self,
        request: StorageUserUpdate,
    ) -> Result<StorageMutationOutcome<StorageUser>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::User,
            "update_user",
            async { dispatch_backend!(self, |backend| backend.update_user(request).await) },
        )
        .await
    }

    async fn set_user_password(
        &self,
        request: StorageUserPasswordUpdate,
    ) -> Result<StorageMutationOutcome<usize>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::User,
            "set_user_password",
            async {
                dispatch_backend!(self, |backend| { backend.set_user_password(request).await })
            },
        )
        .await
    }

    async fn delete_user(
        &self,
        request: StorageUserDelete,
    ) -> Result<StorageMutationOutcome<usize>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::User,
            "delete_user",
            async { dispatch_backend!(self, |backend| backend.delete_user(request).await) },
        )
        .await
    }

    async fn anonymize_user(
        &self,
        request: StorageUserAnonymize,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::User,
            "anonymize_user",
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
            StorageCapability::Token,
            "list_retained_tokens",
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
    ) -> Result<StorageMutationOutcome<StorageTokenMetadata>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Token,
            "create_token",
            async { dispatch_backend!(self, |backend| backend.create_token(request).await) },
        )
        .await
    }

    async fn renew_token(
        &self,
        request: StorageTokenRenew,
    ) -> Result<StorageMutationOutcome<StorageTokenMetadata>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Token,
            "renew_token",
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
            StorageCapability::Token,
            "get_token_metadata",
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

    async fn load_token_metadata_by_ids(
        &self,
        token_ids: Vec<TokenId>,
        observation: StorageTokenObservation,
    ) -> Result<Vec<StorageTokenMetadata>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Token,
            "load_token_metadata_by_ids",
            async {
                dispatch_backend!(self, |backend| {
                    backend
                        .load_token_metadata_by_ids(token_ids, observation)
                        .await
                })
            },
        )
        .await
    }

    async fn revoke_token(
        &self,
        request: StorageTokenRevoke,
    ) -> Result<StorageMutationOutcome<usize>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Token,
            "revoke_token",
            async { dispatch_backend!(self, |backend| backend.revoke_token(request).await) },
        )
        .await
    }

    async fn revoke_token_by_hash(
        &self,
        request: StorageTokenHashRevoke,
    ) -> Result<StorageMutationOutcome<usize>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Token,
            "revoke_token_by_hash",
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
    ) -> Result<StorageMutationOutcome<usize>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Token,
            "revoke_all_principal_tokens",
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
    ) -> Result<StorageAuthorizationPrincipal, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::AuthorizationData,
            "get_authorization_principal",
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
        query: StorageAuthorizationGroupMembershipQuery,
    ) -> Result<bool, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::AuthorizationData,
            "is_authorization_principal_group_member",
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
        query: StorageAuthorizationResourceIds,
    ) -> Result<Vec<StorageAuthorizationClassResource>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::AuthorizationData,
            "list_authorization_classes",
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
        query: StorageAuthorizationResourceIds,
    ) -> Result<Vec<StorageAuthorizationObjectResource>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::AuthorizationData,
            "list_authorization_objects",
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
        query: StorageAuthorizationCollectionAccessQuery,
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
        query: StorageAuthorizationCollectionsAccessQuery,
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
        query: StorageAuthorizationCollectionsQuery,
    ) -> Result<Vec<StorageAuthorizationCollection>, StorageError> {
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

    async fn load_authorization_collection_candidates(
        &self,
    ) -> Result<Vec<StorageAuthorizationCollection>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::AuthorizationData,
            "load_authorization_collection_candidates",
            async {
                dispatch_backend!(self, |backend| {
                    backend.load_authorization_collection_candidates().await
                })
            },
        )
        .await
    }

    async fn load_authorization_group_candidates(
        &self,
        query: StorageAuthorizationGroupCandidateQuery,
    ) -> Result<Vec<StorageAuthorizationGroup>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::AuthorizationData,
            "load_authorization_group_candidates",
            async {
                dispatch_backend!(self, |backend| {
                    backend.load_authorization_group_candidates(query).await
                })
            },
        )
        .await
    }

    async fn get_authorization_policy_snapshot(
        &self,
    ) -> Result<Vec<StorageAuthorizationPolicySnapshotRow>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::AuthorizationData,
            "get_authorization_policy_snapshot",
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
        query: StorageAuthorizationCollectionGrantListQuery,
    ) -> Result<StoragePage<StorageAuthorizationGroupGrant>, StorageError> {
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
        key: StorageAuthorizationGrantKey,
    ) -> Result<Option<StorageAuthorizationGrant>, StorageError> {
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
        query: StorageAuthorizationPermissionSetQuery,
    ) -> Result<StorageAuthorizationPermissionSet, StorageError> {
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
        mutation: StorageAuthorizationGrantMutation,
    ) -> Result<StorageMutationOutcome<StorageAuthorizationGrant>, StorageError> {
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
        mutation: StorageAuthorizationGrantMutation,
    ) -> Result<StorageMutationOutcome<StorageAuthorizationGrant>, StorageError> {
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
        request: StorageAuthorizationGrantDelete,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
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
