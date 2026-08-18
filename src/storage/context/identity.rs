use super::*;

#[async_trait]
impl AuthenticationStorage for StorageHandle {
    async fn authenticate_bearer_token(
        &self,
        attempt: AuthenticationAttempt,
    ) -> Result<AuthenticatedToken, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "authentication",
            "authenticate_bearer_token",
            async {
                dispatch_backend!(self, |backend| {
                    backend.authenticate_bearer_token(attempt).await
                })
            },
        )
        .await
    }

    async fn load_authentication_identity(
        &self,
        principal_id: i32,
    ) -> Result<AuthenticationIdentity, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "authentication",
            "load_identity",
            async {
                dispatch_backend!(self, |backend| {
                    backend.load_authentication_identity(principal_id).await
                })
            },
        )
        .await
    }

    async fn load_authentication_token_scope(
        &self,
        query: AuthenticationTokenScopeQuery,
    ) -> Result<Option<AuthenticationTokenScope>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "authentication",
            "load_token_scope",
            async {
                dispatch_backend!(self, |backend| {
                    backend.load_authentication_token_scope(query).await
                })
            },
        )
        .await
    }
}

#[async_trait]
impl IdentityStorage for StorageHandle {
    async fn default_admin_bootstrap_required(&self) -> Result<bool, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "identity",
            "default_admin_bootstrap_required",
            async {
                dispatch_backend!(self, |backend| {
                    backend.default_admin_bootstrap_required().await
                })
            },
        )
        .await
    }

    async fn bootstrap_default_admin(
        &self,
        request: StorageDefaultAdminBootstrap,
    ) -> Result<bool, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "identity",
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
        observe_storage_call(
            self.backend_name(),
            "identity",
            "reset_local_password",
            async {
                dispatch_backend!(self, |backend| {
                    backend.reset_local_password(request).await
                })
            },
        )
        .await
    }

    async fn ensure_identity_scope(
        &self,
        request: StorageIdentityScopeEnsure,
    ) -> Result<StorageIdentityScope, StorageError> {
        observe_storage_call(self.backend_name(), "identity", "ensure_scope", async {
            dispatch_backend!(self, |backend| {
                backend.ensure_identity_scope(request).await
            })
        })
        .await
    }

    async fn identity_scope_name(&self, scope_id: i32) -> Result<String, StorageError> {
        observe_storage_call(self.backend_name(), "identity", "load_scope_name", async {
            dispatch_backend!(self, |backend| {
                backend.identity_scope_name(scope_id).await
            })
        })
        .await
    }

    async fn identity_scope_names(
        &self,
        scope_ids: Vec<i32>,
    ) -> Result<Vec<(i32, String)>, StorageError> {
        observe_storage_call(self.backend_name(), "identity", "load_scope_names", async {
            dispatch_backend!(self, |backend| {
                backend.identity_scope_names(scope_ids).await
            })
        })
        .await
    }

    async fn load_principal_group(
        &self,
        principal_id: i32,
        group_id: i32,
    ) -> Result<StoragePrincipalGroup, StorageError> {
        observe_storage_call(self.backend_name(), "identity", "load_membership", async {
            dispatch_backend!(self, |backend| {
                backend.load_principal_group(principal_id, group_id).await
            })
        })
        .await
    }

    async fn list_principal_groups(
        &self,
        query: StoragePrincipalGroupListQuery,
    ) -> Result<StorageIdentityPage<StorageIdentityGroup>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "identity",
            "list_principal_groups",
            async {
                dispatch_backend!(self, |backend| {
                    backend.list_principal_groups(query).await
                })
            },
        )
        .await
    }

    async fn list_groups(
        &self,
        query: StorageGroupListQuery,
    ) -> Result<StorageIdentityPage<StorageIdentityGroup>, StorageError> {
        observe_storage_call(self.backend_name(), "identity", "list_groups", async {
            dispatch_backend!(self, |backend| backend.list_groups(query).await)
        })
        .await
    }

    async fn list_retained_tokens(
        &self,
        query: StorageTokenListQuery,
    ) -> Result<StorageIdentityPage<StorageTokenMetadata>, StorageError> {
        observe_storage_call(self.backend_name(), "identity", "list_tokens", async {
            dispatch_backend!(self, |backend| {
                backend.list_retained_tokens(query).await
            })
        })
        .await
    }

    async fn is_human_owner_group_member(
        &self,
        principal_id: i32,
        owner_group_id: i32,
    ) -> Result<bool, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "identity",
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

    async fn principal_is_disabled(&self, principal_id: i32) -> Result<bool, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "identity",
            "principal_is_disabled",
            async {
                dispatch_backend!(self, |backend| {
                    backend.principal_is_disabled(principal_id).await
                })
            },
        )
        .await
    }

    async fn load_service_account(
        &self,
        service_account_id: i32,
    ) -> Result<StorageServiceAccount, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "identity",
            "load_service_account",
            async {
                dispatch_backend!(self, |backend| {
                    backend.load_service_account(service_account_id).await
                })
            },
        )
        .await
    }

    async fn load_service_account_point(
        &self,
        service_account_id: i32,
    ) -> Result<StorageServiceAccountPoint, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "identity",
            "load_service_account_point",
            async {
                dispatch_backend!(self, |backend| {
                    backend.load_service_account_point(service_account_id).await
                })
            },
        )
        .await
    }

    async fn list_manageable_service_accounts(
        &self,
        query: StorageServiceAccountListQuery,
    ) -> Result<StorageIdentityPage<StorageServiceAccountListItem>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "identity",
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
    ) -> Result<StorageServiceAccount, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "identity",
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
    ) -> Result<StorageServiceAccount, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "identity",
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
    ) -> Result<StorageServiceAccountDisableOutcome, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "identity",
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
    ) -> Result<(), StorageError> {
        observe_storage_call(
            self.backend_name(),
            "identity",
            "delete_service_account",
            async {
                dispatch_backend!(self, |backend| {
                    backend.delete_service_account(request).await
                })
            },
        )
        .await
    }

    async fn external_principal_state(
        &self,
        principal_id: i32,
    ) -> Result<Option<StorageExternalPrincipalState>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "identity",
            "load_external_state",
            async {
                dispatch_backend!(self, |backend| {
                    backend.external_principal_state(principal_id).await
                })
            },
        )
        .await
    }

    async fn mark_external_sync_attempted(&self, principal_id: i32) -> Result<(), StorageError> {
        observe_storage_call(
            self.backend_name(),
            "identity",
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
    ) -> Result<StorageSyncedHuman, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "identity",
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
    async fn load_user(&self, id: i32) -> Result<StorageUser, StorageError> {
        observe_storage_call(self.backend_name(), "user", "load", async {
            dispatch_backend!(self, |backend| backend.load_user(id).await)
        })
        .await
    }

    async fn load_user_by_name(
        &self,
        identity_scope: String,
        name: String,
    ) -> Result<StorageUser, StorageError> {
        observe_storage_call(self.backend_name(), "user", "load_by_name", async {
            dispatch_backend!(self, |backend| {
                backend.load_user_by_name(identity_scope, name).await
            })
        })
        .await
    }

    async fn load_user_point(&self, id: i32) -> Result<StorageUserPoint, StorageError> {
        observe_storage_call(self.backend_name(), "user", "load_point", async {
            dispatch_backend!(self, |backend| backend.load_user_point(id).await)
        })
        .await
    }

    async fn list_users(
        &self,
        query: StorageUserListQuery,
    ) -> Result<StorageIdentityPage<StorageUserListItem>, StorageError> {
        observe_storage_call(self.backend_name(), "user", "list", async {
            dispatch_backend!(self, |backend| backend.list_users(query).await)
        })
        .await
    }

    async fn create_user(&self, request: StorageUserCreate) -> Result<StorageUser, StorageError> {
        observe_storage_call(self.backend_name(), "user", "create", async {
            dispatch_backend!(self, |backend| backend.create_user(request).await)
        })
        .await
    }

    async fn update_user(&self, request: StorageUserUpdate) -> Result<StorageUser, StorageError> {
        observe_storage_call(self.backend_name(), "user", "update", async {
            dispatch_backend!(self, |backend| backend.update_user(request).await)
        })
        .await
    }

    async fn set_user_password(
        &self,
        request: StorageUserPasswordUpdate,
    ) -> Result<usize, StorageError> {
        observe_storage_call(self.backend_name(), "user", "set_password", async {
            dispatch_backend!(self, |backend| { backend.set_user_password(request).await })
        })
        .await
    }

    async fn delete_user(&self, request: StorageUserDelete) -> Result<usize, StorageError> {
        observe_storage_call(self.backend_name(), "user", "delete", async {
            dispatch_backend!(self, |backend| backend.delete_user(request).await)
        })
        .await
    }

    async fn anonymize_user(&self, request: StorageUserAnonymize) -> Result<(), StorageError> {
        observe_storage_call(self.backend_name(), "user", "anonymize", async {
            dispatch_backend!(self, |backend| backend.anonymize_user(request).await)
        })
        .await
    }
}

#[async_trait]
impl TokenStorage for StorageHandle {
    async fn create_token(
        &self,
        request: StorageTokenCreate,
    ) -> Result<StorageTokenMetadata, StorageError> {
        observe_storage_call(self.backend_name(), "token", "create", async {
            dispatch_backend!(self, |backend| backend.create_token(request).await)
        })
        .await
    }

    async fn renew_token(
        &self,
        request: StorageTokenRenew,
    ) -> Result<StorageTokenMetadata, StorageError> {
        observe_storage_call(self.backend_name(), "token", "renew", async {
            dispatch_backend!(self, |backend| backend.renew_token(request).await)
        })
        .await
    }

    async fn load_token_metadata(
        &self,
        principal_id: i32,
        token_id: i32,
        observation: StorageTokenObservation,
    ) -> Result<StorageTokenMetadata, StorageError> {
        observe_storage_call(self.backend_name(), "token", "load_metadata", async {
            dispatch_backend!(self, |backend| {
                backend
                    .load_token_metadata(principal_id, token_id, observation)
                    .await
            })
        })
        .await
    }

    async fn load_token_metadata_batch(
        &self,
        token_ids: Vec<i32>,
        observation: StorageTokenObservation,
    ) -> Result<Vec<StorageTokenMetadata>, StorageError> {
        observe_storage_call(self.backend_name(), "token", "load_metadata_batch", async {
            dispatch_backend!(self, |backend| {
                backend
                    .load_token_metadata_batch(token_ids, observation)
                    .await
            })
        })
        .await
    }

    async fn revoke_token(&self, request: StorageTokenRevoke) -> Result<usize, StorageError> {
        observe_storage_call(self.backend_name(), "token", "revoke", async {
            dispatch_backend!(self, |backend| backend.revoke_token(request).await)
        })
        .await
    }

    async fn revoke_token_by_hash(
        &self,
        request: StorageTokenHashRevoke,
    ) -> Result<usize, StorageError> {
        observe_storage_call(self.backend_name(), "token", "revoke_by_hash", async {
            dispatch_backend!(self, |backend| {
                backend.revoke_token_by_hash(request).await
            })
        })
        .await
    }

    async fn revoke_all_principal_tokens(
        &self,
        request: StoragePrincipalTokensRevoke,
    ) -> Result<usize, StorageError> {
        observe_storage_call(self.backend_name(), "token", "revoke_all", async {
            dispatch_backend!(self, |backend| backend
                .revoke_all_principal_tokens(request)
                .await)
        })
        .await
    }
}

#[async_trait]
impl AuthorizationStorage for StorageHandle {
    async fn load_authorization_principal(
        &self,
        principal_id: i32,
    ) -> Result<AuthorizationPrincipal, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "authorization",
            "load_principal",
            async {
                dispatch_backend!(self, |backend| {
                    backend.load_authorization_principal(principal_id).await
                })
            },
        )
        .await
    }

    async fn authorization_principal_is_group_member(
        &self,
        query: AuthorizationGroupMembershipQuery,
    ) -> Result<bool, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "authorization",
            "principal_is_group_member",
            async {
                dispatch_backend!(self, |backend| {
                    backend.authorization_principal_is_group_member(query).await
                })
            },
        )
        .await
    }

    async fn load_authorization_classes(
        &self,
        query: AuthorizationResourceIds,
    ) -> Result<Vec<AuthorizationClassResource>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "authorization",
            "load_classes",
            async {
                dispatch_backend!(self, |backend| {
                    backend.load_authorization_classes(query).await
                })
            },
        )
        .await
    }

    async fn load_authorization_objects(
        &self,
        query: AuthorizationResourceIds,
    ) -> Result<Vec<AuthorizationObjectResource>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "authorization",
            "load_objects",
            async {
                dispatch_backend!(self, |backend| {
                    backend.load_authorization_objects(query).await
                })
            },
        )
        .await
    }

    async fn authorize_local_collection(
        &self,
        query: AuthorizationCollectionAccessQuery,
    ) -> Result<bool, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "authorization",
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
        observe_storage_call(
            self.backend_name(),
            "authorization",
            "authorize_local_collections",
            async {
                dispatch_backend!(self, |backend| {
                    backend.authorize_local_collections(query).await
                })
            },
        )
        .await
    }

    async fn local_authorized_collections(
        &self,
        query: AuthorizationCollectionsQuery,
    ) -> Result<Vec<AuthorizationCollection>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "authorization",
            "local_authorized_collections",
            async {
                dispatch_backend!(self, |backend| {
                    backend.local_authorized_collections(query).await
                })
            },
        )
        .await
    }

    async fn list_authorization_collection_candidates(
        &self,
    ) -> Result<Vec<AuthorizationCollection>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "authorization",
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
        observe_storage_call(
            self.backend_name(),
            "authorization",
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

    async fn authorization_policy_snapshot(
        &self,
    ) -> Result<Vec<AuthorizationPolicySnapshotRow>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "authorization",
            "policy_snapshot",
            async {
                dispatch_backend!(self, |backend| {
                    backend.authorization_policy_snapshot().await
                })
            },
        )
        .await
    }

    async fn list_local_collection_grants(
        &self,
        query: AuthorizationCollectionGrantListQuery,
    ) -> Result<AuthorizationGroupGrantPage, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "authorization",
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
        observe_storage_call(
            self.backend_name(),
            "authorization",
            "get_local_collection_grant",
            async {
                dispatch_backend!(self, |backend| {
                    backend.get_local_collection_grant(key).await
                })
            },
        )
        .await
    }

    async fn load_local_collection_permission_set(
        &self,
        query: AuthorizationPermissionSetQuery,
    ) -> Result<AuthorizationPermissionSet, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "authorization",
            "load_local_collection_permission_set",
            async {
                dispatch_backend!(self, |backend| {
                    backend.load_local_collection_permission_set(query).await
                })
            },
        )
        .await
    }

    async fn apply_local_collection_grant(
        &self,
        mutation: AuthorizationGrantMutation,
    ) -> Result<AuthorizationGrant, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "authorization",
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
    ) -> Result<AuthorizationGrant, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "authorization",
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
    ) -> Result<(), StorageError> {
        observe_storage_call(
            self.backend_name(),
            "authorization",
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
