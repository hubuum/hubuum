use super::*;

#[async_trait]
impl AuthenticationStorage for MemoryStorage {
    async fn authenticate_bearer_token(
        &self,
        attempt: StorageAuthenticationAttempt,
    ) -> Result<StorageAuthenticatedToken, StorageError> {
        let (credentials, migration_target, observed_at, legacy_valid_after) = attempt.into_parts();
        let observation = StorageTokenObservation::try_new(observed_at, legacy_valid_after)
            .map_err(invalid_contract_value)?;
        let mut state = self.state.write().await;
        let matching = state
            .tokens
            .values()
            .filter(|record| {
                credentials
                    .iter()
                    .any(|credential| record.matches_credential(credential))
            })
            .map(|record| record.id)
            .collect::<Vec<_>>();
        if matching.len() > 1 {
            return Err(StorageError::internal(
                "multiple token rows matched one bearer credential",
            ));
        }
        let token_id = matching
            .first()
            .copied()
            .ok_or_else(|| StorageError::authentication_required("Invalid bearer token"))?;
        let token = state
            .tokens
            .get(&token_id.id())
            .cloned()
            .ok_or_else(|| StorageError::internal("authenticated token disappeared"))?;
        let metadata = token.metadata(observation)?;
        if !metadata.is_active() {
            return Err(StorageError::authentication_required(
                "Bearer token is expired or revoked",
            ));
        }
        let principal = state
            .principals
            .get(&token.principal_id.id())
            .ok_or_else(|| {
                StorageError::authentication_required("Token principal was not found")
            })?;
        if principal.kind().is_service_account()
            && state
                .service_accounts
                .get(&principal.id().id())
                .is_some_and(|account| account.is_disabled())
        {
            return Err(StorageError::authentication_required(
                "Token principal is disabled",
            ));
        }
        let permission_scoped = token
            .scope
            .clone()
            .map(StorageAuthenticationTokenScope::into_parts)
            .is_some_and(|parts| parts.0.is_some());
        let resource_scoped = token
            .scope
            .clone()
            .map(StorageAuthenticationTokenScope::into_parts)
            .is_some_and(|parts| parts.1.is_some());
        let migration_target = migration_target.filter(|_| {
            token.token_format == StorageTokenFormat::Legacy && token.token_hash_key_id.is_none()
        });
        if migration_target.as_ref().is_some_and(|target| {
            state.tokens.values().any(|candidate| {
                candidate.id != token_id && target.matches_lookup_value(&candidate.token_hash)
            })
        }) {
            return Err(StorageError::internal(
                "legacy token migration target conflicts with another token row",
            ));
        }
        let record = state
            .tokens
            .get_mut(&token_id.id())
            .ok_or_else(|| StorageError::internal("authenticated token disappeared"))?;
        let migration_outcome = if let Some(target) = migration_target {
            record.migrate_legacy_digest(target);
            StorageTokenMigrationOutcome::Migrated
        } else {
            StorageTokenMigrationOutcome::NotNeeded
        };
        record.last_used_at = Some(observed_at);
        StorageAuthenticatedToken::builder(
            token.id,
            token.principal_id,
            token.issued,
            token.revision,
        )
        .name(token.name)
        .description(token.description)
        .expires_at(token.expires_at)
        .last_used_at(Some(observed_at))
        .permission_scoped(permission_scoped)
        .resource_scoped(resource_scoped)
        .migration_outcome(migration_outcome)
        .try_build()
        .map_err(invalid_contract_value)
    }

    async fn get_authentication_identity(
        &self,
        principal_id: PrincipalId,
    ) -> Result<StorageAuthenticationIdentity, StorageError> {
        let state = self.state.read().await;
        let principal = state.principals.get(&principal_id.id()).ok_or_else(|| {
            StorageError::not_found(format!("Principal {} was not found", principal_id.id()))
        })?;
        let projection = StorageAuthenticationPrincipal::new(
            principal.id(),
            principal.kind(),
            principal.name(),
            principal.identity_scope_id(),
        );
        let human = if principal.kind().is_human() {
            let parts = state
                .users
                .get(&principal_id.id())
                .ok_or_else(|| StorageError::internal("human principal has no user record"))?
                .user
                .clone()
                .into_parts();
            Some(
                StorageAuthenticationHuman::try_new(
                    parts.id(),
                    parts.proper_name().map(ToOwned::to_owned),
                    parts.email().map(ToOwned::to_owned),
                    parts.created_at(),
                    parts.updated_at(),
                    parts.anonymized_at(),
                )
                .map_err(invalid_contract_value)?,
            )
        } else {
            None
        };
        StorageAuthenticationIdentity::try_new(projection, human).map_err(invalid_contract_value)
    }

    async fn get_authentication_token_scope(
        &self,
        query: StorageAuthenticationTokenScopeQuery,
    ) -> Result<Option<StorageAuthenticationTokenScope>, StorageError> {
        if !query.is_scoped() {
            return Ok(None);
        }
        let persisted = self
            .state
            .read()
            .await
            .tokens
            .get(&query.token_id().id())
            .and_then(|record| record.scope.clone());
        let (persisted_permissions, persisted_resources) = persisted
            .map(StorageAuthenticationTokenScope::into_parts)
            .unwrap_or_default();
        Ok(Some(StorageAuthenticationTokenScope::new(
            query
                .is_permission_scoped()
                .then(|| persisted_permissions.unwrap_or_default()),
            query.is_resource_scoped().then(|| {
                persisted_resources.unwrap_or_else(StorageAuthenticationResourceScope::default)
            }),
        )))
    }
}

#[async_trait]
impl LocalIdentityCredentialStorage for MemoryStorage {
    async fn is_default_admin_bootstrap_required(&self) -> Result<bool, StorageError> {
        Ok(self.state.read().await.users.is_empty())
    }

    async fn bootstrap_default_admin(
        &self,
        _request: StorageDefaultAdminBootstrap,
    ) -> Result<bool, StorageError> {
        Ok(false)
    }

    async fn reset_local_password(
        &self,
        request: StorageLocalPasswordReset,
    ) -> Result<StorageMutationOutcome<usize>, StorageError> {
        let (name, password_hash, context) = request.into_parts();
        let mut state = self.state.write().await;
        let user_id = state
            .users
            .iter()
            .find(|(_, record)| {
                record.name == name
                    && state
                        .identity_scopes
                        .get(&record.identity_scope_id.id())
                        .is_some_and(|scope| scope.name() == LOCAL_IDENTITY_SCOPE)
            })
            .map(|(id, _)| *id)
            .ok_or_else(|| StorageError::not_found(format!("User '{name}' was not found")))?;
        let record = state
            .users
            .get(&user_id)
            .cloned()
            .ok_or_else(|| StorageError::internal("password reset user disappeared"))?;
        let parts = record.user.into_parts();
        let now = Utc::now();
        let user = StorageUser::try_new(
            parts.id(),
            Some(password_hash),
            parts.proper_name().map(ToOwned::to_owned),
            parts.email().map(ToOwned::to_owned),
            parts.created_at(),
            now,
            parts.anonymized_at(),
        )
        .map_err(invalid_contract_value)?;
        state
            .users
            .get_mut(&user_id)
            .expect("password reset user exists")
            .user = user;
        let principal = state
            .principals
            .get(&user_id)
            .cloned()
            .ok_or_else(|| StorageError::internal("password reset principal is missing"))?;
        state.principals.insert(
            user_id,
            advanced_principal(
                &principal,
                principal.name(),
                principal.settings().clone(),
                now,
            )?,
        );
        for token in state.tokens.values_mut() {
            if token.principal_id.id() == user_id && token.revoked_at.is_none() {
                token.revoked_at = Some(now);
            }
        }
        let receipt = state.append_simple_event(
            EntityType::User,
            user_id,
            Some(&name),
            Action::Updated,
            &context,
            format!("User '{name}' password reset"),
        )?;
        Ok(StorageMutationOutcome::committed(1, receipt))
    }
}

#[async_trait]
impl IdentityScopeStorage for MemoryStorage {
    async fn ensure_identity_scope(
        &self,
        request: StorageIdentityScopeEnsure,
    ) -> Result<StorageIdentityScope, StorageError> {
        let mut state = self.state.write().await;
        if let Some(scope) = state.identity_scope_by_name(request.name()) {
            if scope.provider_kind() != request.provider_kind() {
                return Err(StorageError::conflict(format!(
                    "Identity scope '{}' uses provider kind '{}'",
                    request.name(),
                    scope.provider_kind()
                )));
            }
            return Ok(scope.clone());
        }
        let id = IdentityScopeId::new(state.next_identity_scope_id)
            .expect("memory identity scope id is positive");
        state.next_identity_scope_id += 1;
        let now = Utc::now();
        let scope = StorageIdentityScope::try_new(
            id,
            request.name(),
            request.provider_kind(),
            now,
            now,
            ResourceRevision::INITIAL,
        )
        .map_err(invalid_contract_value)?;
        state.identity_scopes.insert(id.id(), scope.clone());
        Ok(scope)
    }

    async fn resolve_identity_scope_name(
        &self,
        scope_id: IdentityScopeId,
    ) -> Result<String, StorageError> {
        self.state
            .read()
            .await
            .identity_scopes
            .get(&scope_id.id())
            .map(|scope| scope.name().to_string())
            .ok_or_else(|| {
                StorageError::not_found(format!("Identity scope {} was not found", scope_id.id()))
            })
    }

    async fn resolve_identity_scope_names(
        &self,
        scope_ids: Vec<IdentityScopeId>,
    ) -> Result<Vec<(IdentityScopeId, String)>, StorageError> {
        let state = self.state.read().await;
        scope_ids
            .into_iter()
            .map(|id| {
                state
                    .identity_scopes
                    .get(&id.id())
                    .map(|scope| (id, scope.name().to_string()))
                    .ok_or_else(|| {
                        StorageError::not_found(format!("Identity scope {} was not found", id.id()))
                    })
            })
            .collect()
    }
}

#[async_trait]
impl GroupMembershipStorage for MemoryStorage {
    async fn get_principal_group(
        &self,
        principal_id: PrincipalId,
        group_id: GroupId,
    ) -> Result<StoragePrincipalGroup, StorageError> {
        self.state
            .read()
            .await
            .memberships
            .get(&(principal_id.id(), group_id.id()))
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!(
                    "Principal {} is not a member of group {}",
                    principal_id.id(),
                    group_id.id()
                ))
            })
    }

    async fn list_principal_groups(
        &self,
        query: StoragePrincipalGroupListQuery,
    ) -> Result<StoragePage<StorageIdentityGroup>, StorageError> {
        let (principal_id, options) = query.into_parts();
        let state = self.state.read().await;
        let rows = state
            .memberships
            .values()
            .filter(|membership| membership.principal_id() == principal_id)
            .filter_map(|membership| state.groups.get(&membership.group_id().id()).cloned())
            .collect();
        page(rows, &options)
    }

    async fn is_human_owner_group_member(
        &self,
        principal_id: PrincipalId,
        owner_group_id: GroupId,
    ) -> Result<bool, StorageError> {
        let state = self.state.read().await;
        Ok(state
            .principals
            .get(&principal_id.id())
            .is_some_and(|principal| principal.kind().is_human())
            && state
                .memberships
                .contains_key(&(principal_id.id(), owner_group_id.id())))
    }

    async fn load_group_member_principals(
        &self,
        group_id: GroupId,
    ) -> Result<Vec<StoragePrincipal>, StorageError> {
        let state = self.state.read().await;
        Ok(state
            .memberships
            .values()
            .filter(|membership| membership.group_id() == group_id)
            .filter_map(|membership| {
                state
                    .principals
                    .get(&membership.principal_id().id())
                    .cloned()
            })
            .collect())
    }

    async fn list_group_members(
        &self,
        group_id: GroupId,
        query_options: QueryOptions,
    ) -> Result<StoragePage<StorageGroupMember>, StorageError> {
        let state = self.state.read().await;
        let rows = state
            .memberships
            .values()
            .filter(|membership| membership.group_id() == group_id)
            .map(|membership| {
                let principal = state
                    .principals
                    .get(&membership.principal_id().id())
                    .cloned()
                    .ok_or_else(|| StorageError::internal("membership principal is missing"))?;
                StorageGroupMember::try_new(membership.clone(), principal)
                    .map_err(invalid_contract_value)
            })
            .collect::<Result<Vec<_>, _>>()?;
        page(rows, &query_options)
    }

    async fn add_group_member(
        &self,
        principal_id: PrincipalId,
        group_id: GroupId,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StoragePrincipalGroup>, StorageError> {
        let mut state = self.state.write().await;
        if !state.principals.contains_key(&principal_id.id()) {
            return Err(StorageError::not_found(format!(
                "Principal {} was not found",
                principal_id.id()
            )));
        }
        if !state.groups.contains_key(&group_id.id()) {
            return Err(StorageError::not_found(format!(
                "Group {} was not found",
                group_id.id()
            )));
        }
        if let Some(existing) = state.memberships.get(&(principal_id.id(), group_id.id())) {
            return Ok(StorageMutationOutcome::unchanged(existing.clone()));
        }
        let now = Utc::now();
        let membership = StoragePrincipalGroup::try_new(
            principal_id,
            group_id,
            now,
            now,
            ResourceRevision::INITIAL,
        )
        .map_err(invalid_contract_value)?;
        state
            .memberships
            .insert((principal_id.id(), group_id.id()), membership.clone());
        let receipt = state.append_simple_event(
            EntityType::UserGroup,
            principal_id.id(),
            None,
            Action::Added,
            context,
            format!(
                "Principal {} added to group {}",
                principal_id.id(),
                group_id.id()
            ),
        )?;
        Ok(StorageMutationOutcome::committed(membership, receipt))
    }

    async fn remove_group_member(
        &self,
        principal_id: PrincipalId,
        group_id: GroupId,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        let mut state = self.state.write().await;
        if state
            .memberships
            .remove(&(principal_id.id(), group_id.id()))
            .is_none()
        {
            return Ok(StorageMutationOutcome::unchanged(()));
        }
        let receipt = state.append_simple_event(
            EntityType::UserGroup,
            principal_id.id(),
            None,
            Action::Removed,
            context,
            format!(
                "Principal {} removed from group {}",
                principal_id.id(),
                group_id.id()
            ),
        )?;
        Ok(StorageMutationOutcome::committed((), receipt))
    }
}

#[async_trait]
impl ServiceAccountStorage for MemoryStorage {
    async fn is_service_account_disabled(
        &self,
        principal_id: PrincipalId,
    ) -> Result<bool, StorageError> {
        self.state
            .read()
            .await
            .service_accounts
            .get(&principal_id.id())
            .map(StorageServiceAccount::is_disabled)
            .ok_or_else(|| {
                StorageError::not_found(format!(
                    "Service account {} was not found",
                    principal_id.id()
                ))
            })
    }

    async fn get_service_account(
        &self,
        service_account_id: ServiceAccountId,
    ) -> Result<StorageServiceAccount, StorageError> {
        self.state
            .read()
            .await
            .service_accounts
            .get(&service_account_id.id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!(
                    "Service account {} was not found",
                    service_account_id.id()
                ))
            })
    }

    async fn get_service_account_details(
        &self,
        service_account_id: ServiceAccountId,
    ) -> Result<StorageServiceAccountDetails, StorageError> {
        let state = self.state.read().await;
        let account = state
            .service_accounts
            .get(&service_account_id.id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!(
                    "Service account {} was not found",
                    service_account_id.id()
                ))
            })?;
        let principal = state
            .principals
            .get(&service_account_id.id())
            .ok_or_else(|| StorageError::internal("service-account principal is missing"))?;
        Ok(StorageServiceAccountDetails::new(
            account,
            principal.identity_scope_id(),
            principal.name(),
            principal.revision(),
        ))
    }

    async fn list_manageable_service_accounts(
        &self,
        query: StorageServiceAccountListQuery,
    ) -> Result<StoragePage<StorageServiceAccountListItem>, StorageError> {
        let (requestor_id, administrator, options) = query.into_parts();
        let state = self.state.read().await;
        let rows = state
            .service_accounts
            .values()
            .filter(|account| {
                administrator
                    || account.created_by() == Some(requestor_id)
                    || state
                        .memberships
                        .contains_key(&(requestor_id.id(), account.owner_group_id().id()))
            })
            .map(|account| {
                let principal = state.principals.get(&account.id().id()).ok_or_else(|| {
                    StorageError::internal("service-account principal is missing")
                })?;
                let scope = state
                    .identity_scopes
                    .get(&principal.identity_scope_id().id())
                    .ok_or_else(|| {
                        StorageError::internal("service-account identity scope is missing")
                    })?;
                Ok(StorageServiceAccountListItem::new(
                    account.clone(),
                    scope.name(),
                    principal.name(),
                    principal.revision(),
                ))
            })
            .collect::<Result<Vec<_>, StorageError>>()?;
        page(rows, &options)
    }

    async fn create_service_account(
        &self,
        request: StorageServiceAccountCreate,
    ) -> Result<StorageMutationOutcome<StorageServiceAccount>, StorageError> {
        let (name, description, owner_group_id, created_by, context) = request.into_parts();
        let mut state = self.state.write().await;
        if !state.groups.contains_key(&owner_group_id.id()) {
            return Err(StorageError::not_found(format!(
                "Owner group {} was not found",
                owner_group_id.id()
            )));
        }
        let scope = state
            .identity_scope_by_name(LOCAL_IDENTITY_SCOPE)
            .cloned()
            .ok_or_else(|| StorageError::internal("local identity scope is missing"))?;
        if state.principals.values().any(|principal| {
            principal.identity_scope_id() == scope.id() && principal.name() == name
        }) {
            return Err(StorageError::conflict(format!(
                "Principal '{name}' already exists"
            )));
        }
        let principal_id = PrincipalId::new(state.next_principal_id)
            .expect("memory service-account principal id is positive");
        state.next_principal_id += 1;
        let id = ServiceAccountId::new(principal_id.id())
            .expect("memory service-account id is positive");
        let now = Utc::now();
        let metadata = StorageRecordMetadata::try_new(
            ResourceId::new(id.id()).expect("service-account resource id is positive"),
            now,
            now,
            ResourceRevision::INITIAL,
        )
        .map_err(invalid_contract_value)?;
        let principal = StoragePrincipal::builder(
            metadata,
            PrincipalKind::ServiceAccount,
            name.clone(),
            scope.id(),
        )
        .try_build()
        .map_err(invalid_contract_value)?;
        let account = StorageServiceAccount::try_new(
            id,
            description,
            owner_group_id,
            created_by,
            None,
            now,
            now,
        )
        .map_err(invalid_contract_value)?;
        state.principals.insert(principal_id.id(), principal);
        state.service_accounts.insert(id.id(), account.clone());
        let receipt = state.append_simple_event(
            EntityType::ServiceAccount,
            id.id(),
            Some(&name),
            Action::Created,
            &context,
            format!("Service account '{name}' created"),
        )?;
        Ok(StorageMutationOutcome::committed(account, receipt))
    }

    async fn update_service_account(
        &self,
        request: StorageServiceAccountUpdate,
    ) -> Result<StorageMutationOutcome<StorageServiceAccount>, StorageError> {
        let (id, description, owner_group_id, context) = request.into_parts();
        let mut state = self.state.write().await;
        let current = state
            .service_accounts
            .get(&id.id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!("Service account {} was not found", id.id()))
            })?;
        if description.is_none() && owner_group_id.is_none() {
            return Ok(StorageMutationOutcome::unchanged(current));
        }
        let owner_group_id = owner_group_id.unwrap_or(current.owner_group_id());
        if !state.groups.contains_key(&owner_group_id.id()) {
            return Err(StorageError::not_found(format!(
                "Owner group {} was not found",
                owner_group_id.id()
            )));
        }
        let now = Utc::now();
        let updated = StorageServiceAccount::try_new(
            id,
            description.unwrap_or_else(|| current.description().to_string()),
            owner_group_id,
            current.created_by(),
            current.disabled_at(),
            current.created_at(),
            now,
        )
        .map_err(invalid_contract_value)?;
        state.service_accounts.insert(id.id(), updated.clone());
        let principal = state
            .principals
            .get(&id.id())
            .cloned()
            .ok_or_else(|| StorageError::internal("service-account principal is missing"))?;
        state.principals.insert(
            id.id(),
            advanced_principal(
                &principal,
                principal.name(),
                principal.settings().clone(),
                now,
            )?,
        );
        let receipt = state.append_simple_event(
            EntityType::ServiceAccount,
            id.id(),
            Some(principal.name()),
            Action::Updated,
            &context,
            format!("Service account '{}' updated", principal.name()),
        )?;
        Ok(StorageMutationOutcome::committed(updated, receipt))
    }

    async fn disable_service_account(
        &self,
        request: StorageServiceAccountMutation,
    ) -> Result<StorageMutationOutcome<StorageServiceAccountDisableOutcome>, StorageError> {
        let (id, context) = request.into_parts();
        let mut state = self.state.write().await;
        let current = state
            .service_accounts
            .get(&id.id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!("Service account {} was not found", id.id()))
            })?;
        if current.is_disabled() {
            return Ok(StorageMutationOutcome::unchanged(
                StorageServiceAccountDisableOutcome::new(current, Vec::new()),
            ));
        }
        let now = Utc::now();
        let disabled = StorageServiceAccount::try_new(
            id,
            current.description(),
            current.owner_group_id(),
            current.created_by(),
            Some(now),
            current.created_at(),
            now,
        )
        .map_err(invalid_contract_value)?;
        state.service_accounts.insert(id.id(), disabled.clone());
        for token in state.tokens.values_mut() {
            if token.principal_id.id() == id.id() && token.revoked_at.is_none() {
                token.revoked_at = Some(now);
            }
        }
        let cancelled = state.cancel_queued_tasks_for_principal(
            PrincipalId::new(id.id()).expect("service account principal id is positive"),
        )?;
        let principal = state
            .principals
            .get(&id.id())
            .cloned()
            .ok_or_else(|| StorageError::internal("service-account principal is missing"))?;
        state.principals.insert(
            id.id(),
            advanced_principal(
                &principal,
                principal.name(),
                principal.settings().clone(),
                now,
            )?,
        );
        let receipt = state.append_simple_event(
            EntityType::ServiceAccount,
            id.id(),
            Some(principal.name()),
            Action::Disabled,
            &context,
            format!("Service account '{}' disabled", principal.name()),
        )?;
        Ok(StorageMutationOutcome::committed(
            StorageServiceAccountDisableOutcome::new(disabled, cancelled),
            receipt,
        ))
    }

    async fn delete_service_account(
        &self,
        request: StorageServiceAccountMutation,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        let (id, context) = request.into_parts();
        let mut state = self.state.write().await;
        let Some(account) = state.service_accounts.remove(&id.id()) else {
            return Ok(StorageMutationOutcome::unchanged(()));
        };
        let name = state
            .principals
            .remove(&id.id())
            .map(|principal| principal.name().to_string())
            .unwrap_or_else(|| id.to_string());
        state
            .memberships
            .retain(|(principal_id, _), _| *principal_id != id.id());
        state
            .tokens
            .retain(|_, token| token.principal_id.id() != id.id());
        for task in state.tasks.values_mut() {
            if task
                .submitted_by
                .is_some_and(|principal| principal.id() == id.id())
            {
                task.submitted_by = None;
            }
        }
        let receipt = state.append_simple_event(
            EntityType::ServiceAccount,
            account.id().id(),
            Some(&name),
            Action::Deleted,
            &context,
            format!("Service account '{name}' deleted"),
        )?;
        Ok(StorageMutationOutcome::committed((), receipt))
    }
}

#[async_trait]
impl ExternalIdentityStorage for MemoryStorage {
    async fn get_external_principal_state(
        &self,
        principal_id: PrincipalId,
    ) -> Result<Option<StorageExternalPrincipalState>, StorageError> {
        let state = self.state.read().await;
        let Some(record) = state.users.get(&principal_id.id()) else {
            return Ok(None);
        };
        if !record.provider_managed {
            return Ok(None);
        }
        let scope = state
            .identity_scopes
            .get(&record.identity_scope_id.id())
            .ok_or_else(|| StorageError::internal("external user identity scope is missing"))?;
        StorageExternalPrincipalState::try_new(
            scope.name(),
            record.name.clone(),
            record
                .external_subject
                .clone()
                .ok_or_else(|| StorageError::internal("external user subject is missing"))?,
            record.last_sync_attempted_at,
            record.last_sync_success_at,
        )
        .map(Some)
        .map_err(invalid_contract_value)
    }

    async fn mark_external_sync_attempted(
        &self,
        principal_id: PrincipalId,
    ) -> Result<(), StorageError> {
        let mut state = self.state.write().await;
        let current = state
            .users
            .get(&principal_id.id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!("Principal {} was not found", principal_id.id()))
            })?;
        if !current.provider_managed {
            return Ok(());
        }
        let now = Utc::now();
        state
            .users
            .get_mut(&principal_id.id())
            .expect("external user exists")
            .last_sync_attempted_at = Some(now);
        let principal = state
            .principals
            .get(&principal_id.id())
            .cloned()
            .ok_or_else(|| StorageError::internal("external principal is missing"))?;
        let revision = principal
            .revision()
            .checked_advance()
            .map_err(|error| StorageError::internal(error.to_string()))?;
        let metadata = StorageRecordMetadata::try_new(
            ResourceId::new(principal_id.id()).expect("principal resource id is positive"),
            principal.created_at(),
            now,
            revision,
        )
        .map_err(invalid_contract_value)?;
        let updated = StoragePrincipal::builder(
            metadata,
            principal.kind(),
            principal.name(),
            principal.identity_scope_id(),
        )
        .provider_managed(true)
        .settings(principal.settings().clone())
        .external_subject(principal.external_subject().map(ToOwned::to_owned))
        .last_sync_attempted_at(Some(now))
        .last_sync_success_at(principal.last_sync_success_at())
        .try_build()
        .map_err(invalid_contract_value)?;
        state.principals.insert(principal_id.id(), updated);
        Ok(())
    }

    async fn sync_external_user(
        &self,
        request: StorageExternalUserSync,
    ) -> Result<StorageMutationOutcome<StorageSyncedHuman>, StorageError> {
        let (scope_name, provider_kind, subject, name, proper_name, email, groups) =
            request.into_parts();
        let mut state = self.state.write().await;
        let scope = if let Some(scope) = state.identity_scope_by_name(&scope_name).cloned() {
            if scope.provider_kind() != provider_kind {
                return Err(StorageError::conflict(format!(
                    "Identity scope '{scope_name}' uses provider kind '{}'",
                    scope.provider_kind()
                )));
            }
            scope
        } else {
            let id = IdentityScopeId::new(state.next_identity_scope_id)
                .expect("memory identity scope id is positive");
            state.next_identity_scope_id += 1;
            let now = Utc::now();
            let scope = StorageIdentityScope::try_new(
                id,
                scope_name.clone(),
                provider_kind.clone(),
                now,
                now,
                ResourceRevision::INITIAL,
            )
            .map_err(invalid_contract_value)?;
            state.identity_scopes.insert(id.id(), scope.clone());
            scope
        };
        let existing_id = state
            .users
            .iter()
            .find(|(_, record)| {
                record.identity_scope_id == scope.id()
                    && (record.external_subject.as_deref() == Some(subject.as_str())
                        || record.name == name)
            })
            .map(|(id, _)| *id);
        let now = Utc::now();
        let principal_id = if let Some(id) = existing_id {
            let record = state
                .users
                .get(&id)
                .cloned()
                .expect("selected external user exists");
            let parts = record.user.into_parts();
            let user = StorageUser::try_new(
                parts.id(),
                None,
                proper_name.clone(),
                email.clone(),
                parts.created_at(),
                now,
                parts.anonymized_at(),
            )
            .map_err(invalid_contract_value)?;
            let record = state.users.get_mut(&id).expect("external user exists");
            record.user = user;
            record.name = name.clone();
            record.provider_managed = true;
            record.external_subject = Some(subject.clone());
            record.last_sync_attempted_at = Some(now);
            record.last_sync_success_at = Some(now);
            let current = state
                .principals
                .get(&id)
                .cloned()
                .ok_or_else(|| StorageError::internal("external principal is missing"))?;
            let revision = current
                .revision()
                .checked_advance()
                .map_err(|error| StorageError::internal(error.to_string()))?;
            let metadata = StorageRecordMetadata::try_new(
                ResourceId::new(id).expect("principal resource id is positive"),
                current.created_at(),
                now,
                revision,
            )
            .map_err(invalid_contract_value)?;
            let principal =
                StoragePrincipal::builder(metadata, PrincipalKind::Human, name.clone(), scope.id())
                    .provider_managed(true)
                    .settings(current.settings().clone())
                    .external_subject(Some(subject.clone()))
                    .last_sync_attempted_at(Some(now))
                    .last_sync_success_at(Some(now))
                    .try_build()
                    .map_err(invalid_contract_value)?;
            state.principals.insert(id, principal);
            PrincipalId::new(id).expect("external principal id is positive")
        } else {
            let principal_id =
                PrincipalId::new(state.next_principal_id).expect("memory principal id is positive");
            state.next_principal_id += 1;
            let metadata = StorageRecordMetadata::try_new(
                ResourceId::new(principal_id.id()).expect("principal resource id is positive"),
                now,
                now,
                ResourceRevision::INITIAL,
            )
            .map_err(invalid_contract_value)?;
            let principal =
                StoragePrincipal::builder(metadata, PrincipalKind::Human, name.clone(), scope.id())
                    .provider_managed(true)
                    .external_subject(Some(subject.clone()))
                    .last_sync_attempted_at(Some(now))
                    .last_sync_success_at(Some(now))
                    .try_build()
                    .map_err(invalid_contract_value)?;
            let user_id = UserId::new(principal_id.id()).expect("external user id is positive");
            let user = StorageUser::try_new(
                user_id,
                None,
                proper_name.clone(),
                email.clone(),
                now,
                now,
                None,
            )
            .map_err(invalid_contract_value)?;
            state.principals.insert(principal_id.id(), principal);
            state.users.insert(
                user_id.id(),
                MemoryUserRecord {
                    user,
                    identity_scope_id: scope.id(),
                    name: name.clone(),
                    provider_managed: true,
                    external_subject: Some(subject.clone()),
                    last_sync_attempted_at: Some(now),
                    last_sync_success_at: Some(now),
                },
            );
            principal_id
        };

        let mut current_external_groups = BTreeSet::new();
        for external_group in groups {
            let group = if let Some(group) = state.groups.values().find(|group| {
                group.identity_scope_id() == scope.id()
                    && (group.external_key() == Some(external_group.key())
                        || group.name() == external_group.name())
            }) {
                group.clone()
            } else {
                let group_id = GroupId::new(state.next_group_id)
                    .expect("memory external group id is positive");
                state.next_group_id += 1;
                let metadata = StorageRecordMetadata::try_new(
                    ResourceId::new(group_id.id()).expect("group resource id is positive"),
                    now,
                    now,
                    ResourceRevision::INITIAL,
                )
                .map_err(invalid_contract_value)?;
                let group = StorageIdentityGroup::builder(
                    metadata,
                    external_group.name(),
                    external_group.description().unwrap_or_default(),
                    scope.id(),
                    provider_kind.clone(),
                )
                .external_key(Some(external_group.key().to_string()))
                .last_sync_attempted_at(Some(now))
                .last_sync_success_at(Some(now))
                .try_build()
                .map_err(invalid_contract_value)?;
                state.groups.insert(group_id.id(), group.clone());
                group
            };
            let key = (principal_id.id(), group.id().id());
            current_external_groups.insert(key);
            if let std::collections::btree_map::Entry::Vacant(entry) = state.memberships.entry(key)
            {
                let membership = StoragePrincipalGroup::try_new(
                    principal_id,
                    group.id(),
                    now,
                    now,
                    ResourceRevision::INITIAL,
                )
                .map_err(invalid_contract_value)?;
                entry.insert(membership);
            }
        }
        let stale_groups = state
            .external_memberships
            .iter()
            .filter(|(id, _)| *id == principal_id.id())
            .filter(|key| !current_external_groups.contains(key))
            .copied()
            .collect::<Vec<_>>();
        for key in stale_groups {
            state.external_memberships.remove(&key);
            state.memberships.remove(&key);
        }
        state.external_memberships.extend(current_external_groups);
        let user = state
            .users
            .get(&principal_id.id())
            .ok_or_else(|| StorageError::internal("synchronized user is missing"))?
            .user
            .clone()
            .into_parts();
        let synced = StorageSyncedHuman::try_new(
            user.id(),
            user.proper_name().map(ToOwned::to_owned),
            user.email().map(ToOwned::to_owned),
            user.created_at(),
            user.updated_at(),
            user.anonymized_at(),
        )
        .map_err(invalid_contract_value)?;
        let receipt = state.append_simple_event(
            EntityType::ExternalIdentitySync,
            principal_id.id(),
            Some(&name),
            Action::Succeeded,
            &EventContext::system(),
            format!("External identity '{name}' synchronized"),
        )?;
        Ok(StorageMutationOutcome::committed(synced, receipt))
    }
}

#[async_trait]
impl UserStorage for MemoryStorage {
    async fn get_user(&self, id: UserId) -> Result<StorageUser, StorageError> {
        self.state
            .read()
            .await
            .users
            .get(&id.id())
            .map(|record| record.user.clone())
            .ok_or_else(|| StorageError::not_found(format!("User {} was not found", id.id())))
    }

    async fn get_user_by_name(
        &self,
        identity_scope: String,
        name: String,
    ) -> Result<StorageUser, StorageError> {
        let state = self.state.read().await;
        state
            .users
            .values()
            .find(|record| {
                record.name == name
                    && state
                        .identity_scopes
                        .get(&record.identity_scope_id.id())
                        .is_some_and(|scope| scope.name() == identity_scope)
            })
            .map(|record| record.user.clone())
            .ok_or_else(|| {
                StorageError::not_found(format!(
                    "User '{name}' was not found in identity scope '{identity_scope}'"
                ))
            })
    }

    async fn get_user_details(&self, id: UserId) -> Result<StorageUserDetails, StorageError> {
        let state = self.state.read().await;
        let record = state
            .users
            .get(&id.id())
            .ok_or_else(|| StorageError::not_found(format!("User {} was not found", id.id())))?;
        state.user_details(record)
    }

    async fn list_users(
        &self,
        query: StorageUserListQuery,
    ) -> Result<StoragePage<StorageUserListItem>, StorageError> {
        let options = query.into_options();
        let state = self.state.read().await;
        let rows = state
            .users
            .values()
            .filter(|record| {
                options.filters().as_slice().iter().all(|filter| {
                    let equal = match filter.field {
                        FilterField::Id => {
                            record.user.clone().into_parts().id().to_string() == filter.value
                        }
                        FilterField::Name => record.name == filter.value,
                        _ => true,
                    };
                    match filter.operator {
                        SearchOperator::Equals { is_negated } => equal != is_negated,
                        _ => true,
                    }
                })
            })
            .map(|record| state.user_list_item(record))
            .collect::<Result<Vec<_>, _>>()?;
        page(rows, &options)
    }

    async fn create_user(
        &self,
        request: StorageUserCreate,
    ) -> Result<StorageMutationOutcome<StorageUser>, StorageError> {
        let (identity_scope, name, password_hash, proper_name, email, context) =
            request.into_parts();
        let identity_scope = identity_scope.unwrap_or_else(|| LOCAL_IDENTITY_SCOPE.to_string());
        let mut state = self.state.write().await;
        let scope = state
            .identity_scope_by_name(&identity_scope)
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!("Identity scope '{identity_scope}' was not found"))
            })?;
        if state
            .users
            .values()
            .any(|record| record.identity_scope_id == scope.id() && record.name == name.as_str())
        {
            return Err(StorageError::conflict(format!(
                "User '{name}' already exists in identity scope '{identity_scope}'"
            )));
        }
        let principal_id =
            PrincipalId::new(state.next_principal_id).expect("memory principal id is positive");
        state.next_principal_id += 1;
        let user_id = UserId::new(principal_id.id()).expect("memory user id is positive");
        let now = Utc::now();
        let metadata = StorageRecordMetadata::try_new(
            ResourceId::new(principal_id.id()).expect("user resource id is positive"),
            now,
            now,
            ResourceRevision::INITIAL,
        )
        .map_err(invalid_contract_value)?;
        let principal =
            StoragePrincipal::builder(metadata, PrincipalKind::Human, name.clone(), scope.id())
                .try_build()
                .map_err(invalid_contract_value)?;
        let user = StorageUser::try_new(
            user_id,
            Some(password_hash),
            proper_name,
            email,
            now,
            now,
            None,
        )
        .map_err(invalid_contract_value)?;
        state.principals.insert(principal_id.id(), principal);
        state.users.insert(
            user_id.id(),
            MemoryUserRecord {
                user: user.clone(),
                identity_scope_id: scope.id(),
                name: name.clone(),
                provider_managed: false,
                external_subject: None,
                last_sync_attempted_at: None,
                last_sync_success_at: None,
            },
        );
        let receipt = state.append_simple_event(
            EntityType::User,
            user_id.id(),
            Some(&name),
            Action::Created,
            &context,
            format!("User '{name}' created"),
        )?;
        Ok(StorageMutationOutcome::committed(user, receipt))
    }

    async fn update_user(
        &self,
        request: StorageUserUpdate,
    ) -> Result<StorageMutationOutcome<StorageUser>, StorageError> {
        let (id, password_hash, proper_name, email, context) = request.into_parts();
        let mut state = self.state.write().await;
        let current =
            state.users.get(&id.id()).cloned().ok_or_else(|| {
                StorageError::not_found(format!("User {} was not found", id.id()))
            })?;
        if password_hash.is_none() && proper_name.is_none() && email.is_none() {
            return Ok(StorageMutationOutcome::unchanged(current.user));
        }
        let parts = current.user.into_parts();
        let now = Utc::now();
        let user = StorageUser::try_new(
            id,
            password_hash.or_else(|| parts.password_hash().map(ToOwned::to_owned)),
            proper_name.or_else(|| parts.proper_name().map(ToOwned::to_owned)),
            email.or_else(|| parts.email().map(ToOwned::to_owned)),
            parts.created_at(),
            now,
            parts.anonymized_at(),
        )
        .map_err(invalid_contract_value)?;
        let record = state.users.get_mut(&id.id()).expect("updated user exists");
        record.user = user.clone();
        let principal = state
            .principals
            .get(&id.id())
            .cloned()
            .ok_or_else(|| StorageError::internal("updated user principal is missing"))?;
        state.principals.insert(
            id.id(),
            advanced_principal(
                &principal,
                principal.name(),
                principal.settings().clone(),
                now,
            )?,
        );
        let receipt = state.append_simple_event(
            EntityType::User,
            id.id(),
            Some(principal.name()),
            Action::Updated,
            &context,
            format!("User '{}' updated", principal.name()),
        )?;
        Ok(StorageMutationOutcome::committed(user, receipt))
    }

    async fn set_user_password(
        &self,
        request: StorageUserPasswordUpdate,
    ) -> Result<StorageMutationOutcome<usize>, StorageError> {
        let (id, password_hash, context) = request.into_parts();
        let outcome = self
            .update_user(StorageUserUpdate::new(
                id,
                Some(password_hash),
                None,
                None,
                context,
            ))
            .await?;
        let now = Utc::now();
        let mut state = self.state.write().await;
        let mut revoked = 0_usize;
        for token in state.tokens.values_mut() {
            if token.principal_id.id() == id.id() && token.revoked_at.is_none() {
                token.revoked_at = Some(now);
                revoked += 1;
            }
        }
        Ok(outcome.map(|_| revoked))
    }

    async fn delete_user(
        &self,
        request: StorageUserDelete,
    ) -> Result<StorageMutationOutcome<usize>, StorageError> {
        let (id, context) = request.into_parts();
        let mut state = self.state.write().await;
        let Some(record) = state.users.remove(&id.id()) else {
            return Ok(StorageMutationOutcome::unchanged(0));
        };
        state.principals.remove(&id.id());
        state
            .memberships
            .retain(|(principal_id, _), _| *principal_id != id.id());
        state
            .tokens
            .retain(|_, token| token.principal_id.id() != id.id());
        let receipt = state.append_simple_event(
            EntityType::User,
            id.id(),
            Some(&record.name),
            Action::Deleted,
            &context,
            format!("User '{}' deleted", record.name),
        )?;
        Ok(StorageMutationOutcome::committed(1, receipt))
    }

    async fn anonymize_user(
        &self,
        request: StorageUserAnonymize,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        let (id, context) = request.into_parts();
        let mut state = self.state.write().await;
        let current =
            state.users.get(&id.id()).cloned().ok_or_else(|| {
                StorageError::not_found(format!("User {} was not found", id.id()))
            })?;
        let parts = current.user.into_parts();
        if parts.anonymized_at().is_some() {
            return Ok(StorageMutationOutcome::unchanged(()));
        }
        let now = Utc::now();
        let anonymous_name = format!("anonymized-{}", id.id());
        let user = StorageUser::try_new(id, None, None, None, parts.created_at(), now, Some(now))
            .map_err(invalid_contract_value)?;
        let record = state
            .users
            .get_mut(&id.id())
            .expect("anonymized user exists");
        record.user = user;
        record.name = anonymous_name.clone();
        let principal = state
            .principals
            .get(&id.id())
            .cloned()
            .ok_or_else(|| StorageError::internal("anonymized user principal is missing"))?;
        state.principals.insert(
            id.id(),
            advanced_principal(
                &principal,
                anonymous_name,
                principal.settings().clone(),
                now,
            )?,
        );
        let receipt = state.append_simple_event(
            EntityType::User,
            id.id(),
            None,
            Action::Updated,
            &context,
            format!("User {} anonymized", id.id()),
        )?;
        Ok(StorageMutationOutcome::committed((), receipt))
    }
}

#[async_trait]
impl TokenStorage for MemoryStorage {
    async fn list_retained_tokens(
        &self,
        query: StorageTokenListQuery,
    ) -> Result<StoragePage<StorageTokenMetadata>, StorageError> {
        let (principal_id, options, list_state, observation) = query.into_parts();
        let state = self.state.read().await;
        let rows = state
            .tokens
            .values()
            .filter(|record| record.principal_id == principal_id)
            .map(|record| record.metadata(observation))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter(|metadata| token_state_matches(metadata, list_state))
            .collect();
        page(rows, &options)
    }

    async fn token_key_usage(
        &self,
        observation: StorageTokenObservation,
    ) -> Result<Vec<StorageTokenKeyUsage>, StorageError> {
        #[derive(Default)]
        struct Usage {
            active: i64,
            revoked: i64,
            expired: i64,
            latest_validation: Option<DateTime<Utc>>,
            earliest_expiry: Option<DateTime<Utc>>,
            latest_expiry: Option<DateTime<Utc>>,
        }

        let state = self.state.read().await;
        let mut usage = BTreeMap::<Option<StorageTokenHashKeyId>, Usage>::new();
        for token in state.tokens.values() {
            let metadata = token.metadata(observation)?;
            let item = usage.entry(token.token_hash_key_id.clone()).or_default();
            if metadata.revoked_at().is_some() {
                item.revoked += 1;
            } else if metadata.is_expired() {
                item.expired += 1;
            } else {
                item.active += 1;
            }
            item.latest_validation = item.latest_validation.max(token.last_used_at);
            item.earliest_expiry = match (item.earliest_expiry, token.expires_at) {
                (Some(current), Some(candidate)) => Some(current.min(candidate)),
                (None, candidate) => candidate,
                (current, None) => current,
            };
            item.latest_expiry = item.latest_expiry.max(token.expires_at);
        }
        usage
            .into_iter()
            .map(|(key_id, usage)| {
                StorageTokenKeyUsage::try_new(
                    key_id,
                    usage.active,
                    usage.revoked,
                    usage.expired,
                    usage.latest_validation,
                    usage.earliest_expiry,
                    usage.latest_expiry,
                )
                .map_err(invalid_contract_value)
            })
            .collect()
    }

    async fn create_token(
        &self,
        request: StorageTokenCreate,
    ) -> Result<StorageMutationOutcome<StorageTokenMetadata>, StorageError> {
        let request = request.into_parts();
        let mut state = self.state.write().await;
        if !state.principals.contains_key(&request.principal_id().id()) {
            return Err(StorageError::not_found(format!(
                "Principal {} was not found",
                request.principal_id().id()
            )));
        }
        if state
            .tokens
            .values()
            .any(|token| request.digest().matches_lookup_value(&token.token_hash))
        {
            return Err(StorageError::conflict("Token credential already exists"));
        }
        let id = TokenId::new(state.next_token_id).expect("memory token id is positive");
        state.next_token_id += 1;
        let issued = Utc::now();
        let (default_lifetime_hours, maximum_lifetime_hours) = request.policy().into_parts();
        let maximum_expiry = issued
            + chrono::Duration::try_hours(maximum_lifetime_hours)
                .ok_or_else(|| StorageError::invalid_input("Token lifetime is too large"))?;
        let expires_at = request.expires_at().unwrap_or_else(|| {
            issued
                + chrono::Duration::try_hours(default_lifetime_hours)
                    .expect("validated token lifetime fits chrono duration")
        });
        if expires_at > maximum_expiry || expires_at <= issued {
            return Err(StorageError::invalid_input(
                "Token expiry is outside the issuance policy",
            ));
        }
        let (token_hash, token_format, token_hash_algorithm, token_hash_key_id) =
            request.digest().clone().into_parts();
        let record = MemoryTokenRecord {
            id,
            principal_id: request.principal_id(),
            token_hash,
            token_format,
            token_hash_algorithm,
            token_hash_key_id,
            name: request.name().map(ToOwned::to_owned),
            description: request.description().map(ToOwned::to_owned),
            issued,
            expires_at: Some(expires_at),
            last_used_at: None,
            revoked_at: None,
            scope: request.scope().cloned(),
            revision: ResourceRevision::INITIAL,
        };
        let observation =
            StorageTokenObservation::try_new(issued, issued).map_err(invalid_contract_value)?;
        let metadata = record.metadata(observation)?;
        state.tokens.insert(id.id(), record);
        let receipt = state.append_simple_event(
            EntityType::Token,
            id.id(),
            None,
            Action::Created,
            request.event_context(),
            format!("Token {} created", id.id()),
        )?;
        Ok(StorageMutationOutcome::committed(metadata, receipt))
    }

    async fn renew_token(
        &self,
        request: StorageTokenRenew,
    ) -> Result<StorageMutationOutcome<StorageTokenMetadata>, StorageError> {
        let (source_id, principal_id, digest, expires_at, policy, context) = request.into_parts();
        let source = self
            .state
            .read()
            .await
            .tokens
            .get(&source_id.id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!("Token {} was not found", source_id.id()))
            })?;
        if source.principal_id != principal_id {
            return Err(StorageError::not_found(format!(
                "Token {} was not found for principal {}",
                source_id.id(),
                principal_id.id()
            )));
        }
        self.create_token(
            StorageTokenCreate::new(principal_id, digest, policy, context)
                .name(source.name)
                .description(source.description)
                .expires_at(expires_at)
                .scope(source.scope),
        )
        .await
    }

    async fn get_token_metadata(
        &self,
        principal_id: PrincipalId,
        token_id: TokenId,
        observation: StorageTokenObservation,
    ) -> Result<StorageTokenMetadata, StorageError> {
        let state = self.state.read().await;
        let token = state.tokens.get(&token_id.id()).ok_or_else(|| {
            StorageError::not_found(format!("Token {} was not found", token_id.id()))
        })?;
        if token.principal_id != principal_id {
            return Err(StorageError::not_found(format!(
                "Token {} was not found for principal {}",
                token_id.id(),
                principal_id.id()
            )));
        }
        token.metadata(observation)
    }

    async fn load_token_metadata_by_ids(
        &self,
        token_ids: Vec<TokenId>,
        observation: StorageTokenObservation,
    ) -> Result<Vec<StorageTokenMetadata>, StorageError> {
        let state = self.state.read().await;
        token_ids
            .into_iter()
            .map(|id| {
                state
                    .tokens
                    .get(&id.id())
                    .ok_or_else(|| {
                        StorageError::not_found(format!("Token {} was not found", id.id()))
                    })?
                    .metadata(observation)
            })
            .collect()
    }

    async fn revoke_token(
        &self,
        request: StorageTokenRevoke,
    ) -> Result<StorageMutationOutcome<usize>, StorageError> {
        let (token_id, principal_id, context) = request.into_parts();
        let mut state = self.state.write().await;
        let token = state.tokens.get_mut(&token_id.id()).ok_or_else(|| {
            StorageError::not_found(format!("Token {} was not found", token_id.id()))
        })?;
        if token.principal_id != principal_id {
            return Err(StorageError::not_found(format!(
                "Token {} was not found for principal {}",
                token_id.id(),
                principal_id.id()
            )));
        }
        if token.revoked_at.is_some() {
            return Ok(StorageMutationOutcome::unchanged(0));
        }
        token.revoked_at = Some(Utc::now());
        token.revision = token
            .revision
            .checked_advance()
            .map_err(|error| StorageError::internal(error.to_string()))?;
        let receipt = state.append_simple_event(
            EntityType::Token,
            token_id.id(),
            None,
            Action::Revoked,
            &context,
            format!("Token {} revoked", token_id.id()),
        )?;
        Ok(StorageMutationOutcome::committed(1, receipt))
    }

    async fn revoke_token_by_hash(
        &self,
        request: StorageTokenHashRevoke,
    ) -> Result<StorageMutationOutcome<usize>, StorageError> {
        let (principal_id, credentials, context) = request.into_parts();
        let token_id = self
            .state
            .read()
            .await
            .tokens
            .values()
            .find(|token| {
                credentials
                    .iter()
                    .any(|credential| token.matches_credential(credential))
                    && principal_id.is_none_or(|id| token.principal_id == id)
            })
            .map(|token| token.id);
        let Some(token_id) = token_id else {
            return Ok(StorageMutationOutcome::unchanged(0));
        };
        let owner = self
            .state
            .read()
            .await
            .tokens
            .get(&token_id.id())
            .expect("token selected by id exists")
            .principal_id;
        self.revoke_token(StorageTokenRevoke::new(token_id, owner, context))
            .await
    }

    async fn revoke_all_principal_tokens(
        &self,
        request: StoragePrincipalTokensRevoke,
    ) -> Result<StorageMutationOutcome<usize>, StorageError> {
        let (principal_id, context) = request.into_parts();
        let mut state = self.state.write().await;
        let now = Utc::now();
        let mut revoked = Vec::new();
        for token in state.tokens.values_mut() {
            if token.principal_id == principal_id && token.revoked_at.is_none() {
                token.revoked_at = Some(now);
                token.revision = token
                    .revision
                    .checked_advance()
                    .map_err(|error| StorageError::internal(error.to_string()))?;
                revoked.push(token.id);
            }
        }
        if revoked.is_empty() {
            return Ok(StorageMutationOutcome::unchanged(0));
        }
        let receipts = revoked
            .into_iter()
            .map(|token_id| {
                state.append_simple_event(
                    EntityType::Token,
                    token_id.id(),
                    None,
                    Action::Revoked,
                    &context,
                    format!("Token {} revoked", token_id.id()),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let count = receipts.len();
        let audits = StorageAuditReceipts::try_from_vec(receipts)
            .map_err(|error| StorageError::internal(error.to_string()))?;
        Ok(StorageMutationOutcome::committed_with_audits(count, audits))
    }
}

#[async_trait]
impl AuthorizationDataStorage for MemoryStorage {
    async fn get_authorization_principal(
        &self,
        principal_id: PrincipalId,
    ) -> Result<StorageAuthorizationPrincipal, StorageError> {
        let state = self.state.read().await;
        if !state.principals.contains_key(&principal_id.id()) {
            return Err(StorageError::not_found(format!(
                "Principal {} was not found",
                principal_id.id()
            )));
        }
        Ok(StorageAuthorizationPrincipal::new(
            principal_id,
            principal_group_ids(&state, principal_id),
        ))
    }

    async fn is_authorization_principal_group_member(
        &self,
        query: StorageAuthorizationGroupMembershipQuery,
    ) -> Result<bool, StorageError> {
        let state = self.state.read().await;
        Ok(state.groups.values().any(|group| {
            group.name() == query.group_name()
                && state
                    .identity_scopes
                    .get(&group.identity_scope_id().id())
                    .is_some_and(|scope| scope.name() == query.identity_scope())
                && state
                    .memberships
                    .contains_key(&(query.principal_id().id(), group.id().id()))
        }))
    }

    async fn list_authorization_classes(
        &self,
        query: StorageAuthorizationResourceIds,
    ) -> Result<Vec<StorageAuthorizationClassResource>, StorageError> {
        let state = self.state.read().await;
        Ok(query
            .ids()
            .iter()
            .filter_map(|id| state.classes.get(&id.id()))
            .map(|class| StorageAuthorizationClassResource::new(class.id(), class.collection_id()))
            .collect())
    }

    async fn list_authorization_objects(
        &self,
        query: StorageAuthorizationResourceIds,
    ) -> Result<Vec<StorageAuthorizationObjectResource>, StorageError> {
        let state = self.state.read().await;
        Ok(query
            .ids()
            .iter()
            .filter_map(|id| state.objects.get(&id.id()))
            .map(|object| {
                StorageAuthorizationObjectResource::new(
                    object.id(),
                    object.collection_id(),
                    object.class_id(),
                    object.name(),
                )
            })
            .collect())
    }

    async fn authorize_local_collection(
        &self,
        query: StorageAuthorizationCollectionAccessQuery,
    ) -> Result<bool, StorageError> {
        let state = self.state.read().await;
        Ok(principal_has_collection_permissions(
            &state,
            query.principal_id(),
            query.collection_id(),
            query.permissions(),
        ))
    }

    async fn authorize_local_collections(
        &self,
        query: StorageAuthorizationCollectionsAccessQuery,
    ) -> Result<bool, StorageError> {
        let state = self.state.read().await;
        Ok(query.collection_ids().iter().all(|collection_id| {
            principal_has_collection_permissions(
                &state,
                query.principal_id(),
                *collection_id,
                query.permissions(),
            )
        }))
    }

    async fn list_local_authorized_collections(
        &self,
        query: StorageAuthorizationCollectionsQuery,
    ) -> Result<Vec<StorageAuthorizationCollection>, StorageError> {
        let state = self.state.read().await;
        state
            .collections
            .values()
            .filter(|collection| {
                principal_has_collection_permissions(
                    &state,
                    query.principal_id(),
                    collection.id(),
                    query.permissions(),
                )
            })
            .map(authorization_collection)
            .collect()
    }

    async fn load_authorization_collection_candidates(
        &self,
        query: StorageAuthorizationCollectionCandidateQuery,
    ) -> Result<StorageCandidatePage<StorageAuthorizationCollection>, StorageError> {
        let state = self.state.read().await;
        let limit = query.page_limit();
        let mut rows = state
            .collections
            .values()
            .filter(|collection| query.after_id().is_none_or(|id| collection.id() > id))
            .take(limit.get().saturating_add(1))
            .map(authorization_collection)
            .collect::<Result<Vec<_>, _>>()?;
        let has_more = rows.len() > limit.get();
        rows.truncate(limit.get());
        StorageCandidatePage::try_new(rows, has_more, limit).map_err(invalid_contract_value)
    }

    async fn load_authorization_group_candidates(
        &self,
        query: StorageAuthorizationGroupCandidateQuery,
    ) -> Result<StorageCandidatePage<StorageAuthorizationGroup>, StorageError> {
        let state = self.state.read().await;
        let limit = query.page_limit();
        let mut rows = state
            .groups
            .values()
            .filter(|group| {
                resource_filters_match(
                    query.options(),
                    group.id().id(),
                    group.name(),
                    group.description(),
                )
            })
            .take(limit.get().saturating_add(1))
            .map(authorization_group)
            .collect::<Result<Vec<_>, _>>()?;
        let has_more = rows.len() > limit.get();
        rows.truncate(limit.get());
        StorageCandidatePage::try_new(rows, has_more, limit).map_err(invalid_contract_value)
    }

    async fn get_authorization_policy_snapshot(
        &self,
    ) -> Result<Vec<StorageAuthorizationPolicySnapshotRow>, StorageError> {
        let state = self.state.read().await;
        state
            .authorization_grants
            .values()
            .map(|grant| authorization_policy_row(&state, grant))
            .collect()
    }

    async fn list_local_collection_grants(
        &self,
        query: StorageAuthorizationCollectionGrantListQuery,
    ) -> Result<StoragePage<StorageAuthorizationGroupGrant>, StorageError> {
        let state = self.state.read().await;
        let rows = state
            .authorization_grants
            .values()
            .filter(|grant| {
                grant.collection_id() == query.collection_id()
                    && permissions_include(grant.permissions(), query.required_permissions())
            })
            .map(|grant| authorization_group_grant(&state, grant))
            .collect::<Result<Vec<_>, _>>()?;
        page(rows, query.query_options())
    }

    async fn get_local_collection_grant(
        &self,
        key: StorageAuthorizationGrantKey,
    ) -> Result<Option<StorageAuthorizationGrant>, StorageError> {
        Ok(self
            .state
            .read()
            .await
            .authorization_grants
            .get(&(key.collection_id().id(), key.group_id().id()))
            .cloned())
    }

    async fn get_local_collection_permission_set(
        &self,
        query: StorageAuthorizationPermissionSetQuery,
    ) -> Result<StorageAuthorizationPermissionSet, StorageError> {
        let state = self.state.read().await;
        let collection = state
            .collections
            .get(&query.collection_id().id())
            .ok_or_else(|| {
                StorageError::not_found(format!(
                    "Collection {} was not found",
                    query.collection_id().id()
                ))
            })?;
        let grants = state
            .authorization_grants
            .values()
            .filter(|grant| {
                grant.collection_id() == query.collection_id()
                    && query.group_id().is_none_or(|id| grant.group_id() == id)
            })
            .cloned()
            .collect();
        StorageAuthorizationPermissionSet::try_new(collection.id(), collection.revision(), grants)
            .map_err(invalid_contract_value)
    }

    async fn apply_local_collection_grant(
        &self,
        mutation: StorageAuthorizationGrantMutation,
    ) -> Result<StorageMutationOutcome<StorageAuthorizationGrant>, StorageError> {
        let key = mutation.key();
        let mut state = self.state.write().await;
        if !state.collections.contains_key(&key.collection_id().id()) {
            return Err(StorageError::not_found(format!(
                "Collection {} was not found",
                key.collection_id().id()
            )));
        }
        if !state.groups.contains_key(&key.group_id().id()) {
            return Err(StorageError::not_found(format!(
                "Group {} was not found",
                key.group_id().id()
            )));
        }
        let map_key = (key.collection_id().id(), key.group_id().id());
        let current = state.authorization_grants.get(&map_key).cloned();
        let mut permissions = if mutation.replace_existing() {
            Vec::new()
        } else {
            current
                .as_ref()
                .map(|grant| grant.permissions().to_vec())
                .unwrap_or_default()
        };
        permissions.extend_from_slice(mutation.permissions());
        permissions.sort_unstable();
        permissions.dedup();
        if current
            .as_ref()
            .is_some_and(|grant| grant.permissions() == permissions)
        {
            return Ok(StorageMutationOutcome::unchanged(
                current.expect("grant exists"),
            ));
        }
        let now = Utc::now();
        let (id, created_at) = current.as_ref().map_or_else(
            || {
                let id = AuthorizationGrantId::new(state.next_authorization_grant_id)
                    .expect("memory authorization grant ids are positive");
                state.next_authorization_grant_id += 1;
                (id, now)
            },
            |grant| (grant.id(), grant.created_at()),
        );
        let grant = StorageAuthorizationGrant::try_new(
            id,
            key.collection_id(),
            key.group_id(),
            permissions,
            created_at,
            now,
        )
        .map_err(invalid_contract_value)?;
        state.authorization_grants.insert(map_key, grant.clone());
        let receipt = state.append_simple_event(
            EntityType::Permission,
            id.id(),
            None,
            Action::Granted,
            mutation.event_context(),
            format!(
                "Collection {} permissions granted to group {}",
                key.collection_id().id(),
                key.group_id().id()
            ),
        )?;
        Ok(StorageMutationOutcome::committed(grant, receipt))
    }

    async fn revoke_local_collection_grant(
        &self,
        mutation: StorageAuthorizationGrantMutation,
    ) -> Result<StorageMutationOutcome<StorageAuthorizationGrant>, StorageError> {
        let key = mutation.key();
        let mut state = self.state.write().await;
        let map_key = (key.collection_id().id(), key.group_id().id());
        let Some(current) = state.authorization_grants.get(&map_key).cloned() else {
            return Err(StorageError::not_found(format!(
                "Collection {} has no grant for group {}",
                key.collection_id().id(),
                key.group_id().id()
            )));
        };
        let permissions = current
            .permissions()
            .iter()
            .copied()
            .filter(|permission| !mutation.permissions().contains(permission))
            .collect::<Vec<_>>();
        if permissions == current.permissions() {
            return Ok(StorageMutationOutcome::unchanged(current));
        }
        let grant = StorageAuthorizationGrant::try_new(
            current.id(),
            current.collection_id(),
            current.group_id(),
            permissions,
            current.created_at(),
            Utc::now(),
        )
        .map_err(invalid_contract_value)?;
        state.authorization_grants.insert(map_key, grant.clone());
        let receipt = state.append_simple_event(
            EntityType::Permission,
            grant.id().id(),
            None,
            Action::Revoked,
            mutation.event_context(),
            format!(
                "Collection {} permissions revoked from group {}",
                key.collection_id().id(),
                key.group_id().id()
            ),
        )?;
        Ok(StorageMutationOutcome::committed(grant, receipt))
    }

    async fn revoke_all_local_collection_grants(
        &self,
        request: StorageAuthorizationGrantDelete,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        let key = request.key();
        let mut state = self.state.write().await;
        let Some(grant) = state
            .authorization_grants
            .remove(&(key.collection_id().id(), key.group_id().id()))
        else {
            return Ok(StorageMutationOutcome::unchanged(()));
        };
        let receipt = state.append_simple_event(
            EntityType::Permission,
            grant.id().id(),
            None,
            Action::Revoked,
            request.event_context(),
            format!(
                "All collection {} permissions revoked from group {}",
                key.collection_id().id(),
                key.group_id().id()
            ),
        )?;
        Ok(StorageMutationOutcome::committed((), receipt))
    }
}
#[async_trait]
impl GroupStorage for MemoryStorage {
    async fn list_groups(
        &self,
        query: StorageGroupListQuery,
    ) -> Result<StoragePage<StorageIdentityGroup>, StorageError> {
        let options = query.into_options();
        let rows = self.state.read().await.groups.values().cloned().collect();
        page(rows, &options)
    }

    async fn get_group(&self, group_id: GroupId) -> Result<StorageIdentityGroup, StorageError> {
        self.state
            .read()
            .await
            .groups
            .get(&group_id.id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!("Group {} was not found", group_id.id()))
            })
    }

    async fn resolve_group_identity_scope_name(
        &self,
        group_id: GroupId,
    ) -> Result<String, StorageError> {
        let state = self.state.read().await;
        let group = state.groups.get(&group_id.id()).ok_or_else(|| {
            StorageError::not_found(format!("Group {} was not found", group_id.id()))
        })?;
        state
            .identity_scopes
            .get(&group.identity_scope_id().id())
            .map(|scope| scope.name().to_string())
            .ok_or_else(|| StorageError::internal("group identity scope is missing"))
    }

    async fn create_group(
        &self,
        command: StorageGroupCreate,
        context: &EventContext,
    ) -> Result<crate::StorageMutationOutcome<StorageIdentityGroup>, StorageError> {
        let (identity_scope, name, description) = command.into_parts();
        let identity_scope = identity_scope.unwrap_or_else(|| LOCAL_IDENTITY_SCOPE.to_string());
        let mut state = self.state.write().await;
        let scope = state
            .identity_scope_by_name(&identity_scope)
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!("Identity scope '{identity_scope}' was not found"))
            })?;
        if state
            .groups
            .values()
            .any(|group| group.identity_scope_id() == scope.id() && group.name() == name.as_str())
        {
            return Err(StorageError::conflict(format!(
                "Group '{name}' already exists in identity scope '{identity_scope}'"
            )));
        }
        let id = GroupId::new(state.next_group_id).expect("memory group id is positive");
        state.next_group_id += 1;
        let now = Utc::now();
        let metadata = StorageRecordMetadata::try_new(
            ResourceId::new(id.id()).expect("group resource id is positive"),
            now,
            now,
            ResourceRevision::INITIAL,
        )
        .map_err(invalid_contract_value)?;
        let group = StorageIdentityGroup::builder(
            metadata,
            name.clone(),
            description.unwrap_or_default(),
            scope.id(),
            LOCAL_PROVIDER_KIND,
        )
        .try_build()
        .map_err(invalid_contract_value)?;
        state.groups.insert(id.id(), group.clone());
        let receipt = state.append_simple_event(
            EntityType::Group,
            id.id(),
            Some(&name),
            Action::Created,
            context,
            format!("Group '{name}' created"),
        )?;
        Ok(StorageMutationOutcome::committed(group, receipt))
    }

    async fn update_group(
        &self,
        group_id: GroupId,
        update: StorageGroupUpdate,
        context: &EventContext,
    ) -> Result<crate::StorageMutationOutcome<StorageIdentityGroup>, StorageError> {
        let mut state = self.state.write().await;
        let current = state.groups.get(&group_id.id()).cloned().ok_or_else(|| {
            StorageError::not_found(format!("Group {} was not found", group_id.id()))
        })?;
        let Some(name) = update.into_name() else {
            return Ok(StorageMutationOutcome::unchanged(current));
        };
        if name == current.name() {
            return Ok(StorageMutationOutcome::unchanged(current));
        }
        if state.groups.values().any(|group| {
            group.id() != group_id
                && group.identity_scope_id() == current.identity_scope_id()
                && group.name() == name.as_str()
        }) {
            return Err(StorageError::conflict(format!(
                "Group '{name}' already exists"
            )));
        }
        let now = Utc::now();
        let revision = current
            .revision()
            .checked_advance()
            .map_err(|error| StorageError::internal(error.to_string()))?;
        let metadata = StorageRecordMetadata::try_new(
            ResourceId::new(group_id.id()).expect("group resource id is positive"),
            current.created_at(),
            now,
            revision,
        )
        .map_err(invalid_contract_value)?;
        let group = StorageIdentityGroup::builder(
            metadata,
            name.clone(),
            current.description(),
            current.identity_scope_id(),
            current.managed_by(),
        )
        .external_key(current.external_key().map(ToOwned::to_owned))
        .last_sync_attempted_at(current.last_sync_attempted_at())
        .last_sync_success_at(current.last_sync_success_at())
        .try_build()
        .map_err(invalid_contract_value)?;
        state.groups.insert(group_id.id(), group.clone());
        let receipt = state.append_simple_event(
            EntityType::Group,
            group_id.id(),
            Some(&name),
            Action::Updated,
            context,
            format!("Group '{name}' updated"),
        )?;
        Ok(StorageMutationOutcome::committed(group, receipt))
    }

    async fn delete_group(
        &self,
        group_id: GroupId,
        context: &EventContext,
    ) -> Result<crate::StorageMutationOutcome<usize>, StorageError> {
        let mut state = self.state.write().await;
        let Some(group) = state.groups.remove(&group_id.id()) else {
            return Ok(StorageMutationOutcome::unchanged(0));
        };
        state
            .memberships
            .retain(|(_, member_group_id), _| *member_group_id != group_id.id());
        let receipt = state.append_simple_event(
            EntityType::Group,
            group_id.id(),
            Some(group.name()),
            Action::Deleted,
            context,
            format!("Group '{}' deleted", group.name()),
        )?;
        Ok(StorageMutationOutcome::committed(1, receipt))
    }
}

#[async_trait]
impl PrincipalStorage for MemoryStorage {
    async fn get_principal(
        &self,
        principal_id: PrincipalId,
    ) -> Result<StoragePrincipal, StorageError> {
        self.state
            .read()
            .await
            .principals
            .get(&principal_id.id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!("Principal {} was not found", principal_id.id()))
            })
    }

    async fn get_principal_settings(
        &self,
        principal_id: PrincipalId,
    ) -> Result<StoragePrincipalSettings, StorageError> {
        let principal = self.get_principal(principal_id).await?;
        StoragePrincipalSettings::try_new(
            principal_id,
            principal.revision(),
            principal.settings().clone(),
        )
        .map_err(invalid_contract_value)
    }

    async fn update_principal_settings(
        &self,
        principal_id: PrincipalId,
        mutation: StoragePrincipalSettingsMutation,
        context: &EventContext,
    ) -> Result<crate::StorageMutationOutcome<StoragePrincipalSettings>, StorageError> {
        let mut state = self.state.write().await;
        let current = state
            .principals
            .get(&principal_id.id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!("Principal {} was not found", principal_id.id()))
            })?;
        let mut document = current.settings().clone();
        match mutation {
            StoragePrincipalSettingsMutation::Replace(replacement) => document = replacement,
            StoragePrincipalSettingsMutation::MergePatch(patch) => {
                json_patch::merge(&mut document, &patch);
            }
            StoragePrincipalSettingsMutation::JsonPatch(patch) => {
                let patch = serde_json::from_value::<json_patch::Patch>(patch)
                    .map_err(|error| StorageError::invalid_input(error.to_string()))?;
                json_patch::patch(&mut document, &patch)
                    .map_err(|error| StorageError::invalid_input(error.to_string()))?;
            }
            StoragePrincipalSettingsMutation::Reset => document = serde_json::json!({}),
        }
        if !document.is_object() {
            return Err(StorageError::invalid_input(
                "Principal settings must be a JSON object",
            ));
        }
        if document == *current.settings() {
            let settings =
                StoragePrincipalSettings::try_new(principal_id, current.revision(), document)
                    .map_err(invalid_contract_value)?;
            return Ok(StorageMutationOutcome::unchanged(settings));
        }
        let updated = advanced_principal(&current, current.name(), document.clone(), Utc::now())?;
        let settings =
            StoragePrincipalSettings::try_new(principal_id, updated.revision(), document)
                .map_err(invalid_contract_value)?;
        state.principals.insert(principal_id.id(), updated);
        let entity_type = if current.kind().is_human() {
            EntityType::User
        } else {
            EntityType::ServiceAccount
        };
        let receipt = state.append_simple_event(
            entity_type,
            principal_id.id(),
            Some(current.name()),
            Action::Updated,
            context,
            format!("Principal '{}' settings updated", current.name()),
        )?;
        Ok(StorageMutationOutcome::committed(settings, receipt))
    }
}

#[async_trait]
impl CollectionAuthorizationQueryStorage for MemoryStorage {
    async fn load_principal_collection_permissions(
        &self,
        query: StorageAuthorizationPrincipalCollectionQuery,
    ) -> Result<Vec<StorageAuthorizationGroupGrant>, StorageError> {
        let state = self.state.read().await;
        let group_ids = principal_group_ids(&state, query.principal_id());
        state
            .authorization_grants
            .values()
            .filter(|grant| {
                grant.collection_id() == query.collection_id()
                    && group_ids.contains(&grant.group_id())
            })
            .map(|grant| authorization_group_grant(&state, grant))
            .collect()
    }

    async fn list_all_principal_collection_permissions(
        &self,
        principal_id: PrincipalId,
    ) -> Result<Vec<StorageAuthorizationPolicySnapshotRow>, StorageError> {
        let state = self.state.read().await;
        let group_ids = principal_group_ids(&state, principal_id);
        state
            .authorization_grants
            .values()
            .filter(|grant| group_ids.contains(&grant.group_id()))
            .map(|grant| authorization_policy_row(&state, grant))
            .collect()
    }

    async fn list_principal_collection_permissions(
        &self,
        query: StorageAuthorizationPrincipalCollectionPageQuery,
    ) -> Result<StoragePage<StorageAuthorizationGroupGrant>, StorageError> {
        let state = self.state.read().await;
        let group_ids = principal_group_ids(&state, query.principal().principal_id());
        let rows = state
            .authorization_grants
            .values()
            .filter(|grant| {
                grant.collection_id() == query.principal().collection_id()
                    && group_ids.contains(&grant.group_id())
            })
            .map(|grant| authorization_group_grant(&state, grant))
            .collect::<Result<Vec<_>, _>>()?;
        page(rows, query.query_options())
    }

    async fn list_effective_principal_collection_permissions(
        &self,
        query: StorageAuthorizationPrincipalCollectionQuery,
    ) -> Result<Vec<StorageAuthorizationEffectiveGroupGrant>, StorageError> {
        let state = self.state.read().await;
        let group_ids = principal_group_ids(&state, query.principal_id());
        state
            .authorization_grants
            .values()
            .filter(|grant| {
                grant.collection_id() == query.collection_id()
                    && group_ids.contains(&grant.group_id())
            })
            .map(|grant| authorization_effective_group_grant(&state, grant))
            .collect()
    }

    async fn list_visible_collections(
        &self,
        query: StorageAuthorizationCollectionVisibilityQuery,
    ) -> Result<Vec<StorageAuthorizationCollection>, StorageError> {
        let (principal_id, is_admin, permission, scope) = query.into_parts();
        let (scope_permissions, scope_resources) = scope
            .map(StorageAuthenticationTokenScope::into_parts)
            .unwrap_or((None, None));
        if scope_permissions
            .as_ref()
            .is_some_and(|permissions| !permissions.contains(&permission))
        {
            return Ok(Vec::new());
        }
        let resources = scope_resources.map(StorageAuthenticationResourceScope::into_parts);
        let state = self.state.read().await;
        state
            .collections
            .values()
            .filter(|collection| {
                let resource_allowed =
                    resources
                        .as_ref()
                        .is_none_or(|(collection_ids, class_ids, object_ids)| {
                            collection_ids.contains(&collection.id())
                                || class_ids.iter().any(|class_id| {
                                    state.classes.get(&class_id.id()).is_some_and(|class| {
                                        class.collection_id() == collection.id()
                                    })
                                })
                                || object_ids.iter().any(|object_id| {
                                    state.objects.get(&object_id.id()).is_some_and(|object| {
                                        object.collection_id() == collection.id()
                                    })
                                })
                        });
                resource_allowed
                    && (is_admin
                        || principal_has_collection_permissions(
                            &state,
                            principal_id,
                            collection.id(),
                            &[permission],
                        ))
            })
            .map(authorization_collection)
            .collect()
    }

    async fn has_group_collection_permission(
        &self,
        query: StorageAuthorizationGroupCollectionQuery,
    ) -> Result<bool, StorageError> {
        Ok(self
            .state
            .read()
            .await
            .authorization_grants
            .get(&(query.collection_id().id(), query.group_id().id()))
            .is_some_and(|grant| grant.permissions().contains(&query.permission())))
    }

    async fn list_effective_group_collection_permissions(
        &self,
        collection_id: CollectionId,
        group_id: GroupId,
    ) -> Result<Vec<StorageAuthorizationEffectiveGroupGrant>, StorageError> {
        let state = self.state.read().await;
        state
            .authorization_grants
            .get(&(collection_id.id(), group_id.id()))
            .into_iter()
            .map(|grant| authorization_effective_group_grant(&state, grant))
            .collect()
    }

    async fn load_groups_with_collection_permission(
        &self,
        query: StorageAuthorizationCollectionGroupsQuery,
    ) -> Result<Vec<StorageAuthorizationGroup>, StorageError> {
        let state = self.state.read().await;
        state
            .authorization_grants
            .values()
            .filter(|grant| {
                grant.collection_id() == query.collection_id()
                    && grant.permissions().contains(&query.permission())
            })
            .map(|grant| {
                state
                    .groups
                    .get(&grant.group_id().id())
                    .ok_or_else(|| StorageError::internal("authorization grant group is missing"))
                    .and_then(authorization_group)
            })
            .collect()
    }

    async fn list_groups_with_collection_permission(
        &self,
        query: StorageAuthorizationCollectionGroupsPageQuery,
    ) -> Result<StoragePage<StorageAuthorizationGroup>, StorageError> {
        let state = self.state.read().await;
        let groups = query.groups();
        let rows = state
            .authorization_grants
            .values()
            .filter(|grant| {
                grant.collection_id() == groups.collection_id()
                    && grant.permissions().contains(&groups.permission())
            })
            .map(|grant| {
                state
                    .groups
                    .get(&grant.group_id().id())
                    .ok_or_else(|| StorageError::internal("authorization grant group is missing"))
                    .and_then(authorization_group)
            })
            .collect::<Result<Vec<_>, _>>()?;
        page(rows, query.query_options())
    }
}
