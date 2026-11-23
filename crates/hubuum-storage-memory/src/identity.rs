use super::*;
use hubuum_domain::ExternalIdentity;
use hubuum_storage_core::StorageExternalInteractionAuthentication;
use hubuum_storage_core::StorageExternalSyncFailure;
use hubuum_storage_core::StorageTokenRevocations;
use hubuum_storage_core::StorageUpstreamClaim;
use hubuum_storage_core::StorageUpstreamClaimOutcome;
use hubuum_storage_core::StorageUpstreamRenewal;
use hubuum_storage_core::{
    StorageInteractionAccess, StorageInteractionAction, StorageInteractionCommand,
    StorageInteractionComplete, StorageInteractionCompletion, StorageInteractionCreate,
    StorageInteractionOutcome, StorageInteractionView,
};
use hubuum_storage_core::{
    StorageUpstreamFailure, StorageUpstreamRevocation, StorageUpstreamSession,
};
use serde_json::{Value, json};

mod authorization;
mod credentials;

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
        let id = IdentityScopeId::new(state.identity_scope_ids.allocate()?)
            .expect("memory identity scope id is positive");
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
        let principal_id = PrincipalId::new(state.principal_ids.allocate()?)
            .expect("memory service-account principal id is positive");
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
        state.invalidate_principal_sessions(id.id());
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
        state.cleanup_credential_activity();
        for task in state.tasks.values_mut() {
            task.control.forget_actor(
                PrincipalId::new(id.id())
                    .map_err(|error| StorageError::internal(error.to_string()))?,
            );
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
    async fn upstream_approval_session(
        &self,
        proof: hubuum_domain::FreshExternalAuthentication,
    ) -> Result<Option<hubuum_storage_core::StorageUpstreamApproval>, StorageError> {
        let state = self.state.read().await;
        let principal = proof
            .authentication()
            .binding()
            .authority()
            .actor()
            .ok_or_else(|| StorageError::authentication_required("Approval actor missing"))?
            .principal_id();
        state
            .upstream_sessions
            .iter()
            .filter(|(_, record)| {
                record.principal() == principal && state.upstream_eligible(record, Utc::now())
            })
            .max_by_key(|(id, _)| (state.sessions.get(id).map(|session| session.absolute), **id))
            .map(|(_, record)| {
                hubuum_storage_core::StorageUpstreamApproval::new(record, proof)
                    .map_err(invalid_contract_value)
            })
            .transpose()
    }
    async fn fail_upstream_renewal(
        &self,
        claim: StorageUpstreamClaim,
        failure: StorageUpstreamFailure,
    ) -> Result<(), StorageError> {
        let mut live = self.state.write().await;
        let mut state = live.clone();
        let original = claim.session();
        let record = state
            .upstream_sessions
            .get(&original.session().value())
            .ok_or_else(|| StorageError::authentication_required("Upstream session changed"))?;
        let now = Utc::now();
        if !state.upstream_eligible(record, now)
            || !record.accepts(&claim, now)
            || state
                .users
                .get(&original.principal().id())
                .is_some_and(|user| {
                    user.last_sync_success_at
                        .is_some_and(|at| at > claim.started_at())
                })
        {
            return Err(StorageError::authentication_required(
                "Upstream session changed",
            ));
        }
        let provider = state
            .runtime_settings
            .provider_binding(original.scope())
            .ok_or_else(|| StorageError::authentication_required("Upstream provider changed"))?;
        let record = state
            .upstream_sessions
            .get_mut(&original.session().value())
            .expect("checked session");
        if !record.defer_credential_retry(&claim, provider, failure, now) {
            state.upstream_sessions.remove(&original.session().value());
        }
        if let Some(failure) = failure.sync_failure() {
            state.external_sync_failure(original.principal(), failure)?;
        }
        *live = state;
        Ok(())
    }
    async fn claim_upstream_credential(
        &self,
        revision: ResourceRevision,
        principal: PrincipalId,
    ) -> Result<StorageUpstreamClaimOutcome, StorageError> {
        let mut state = self.state.write().await;
        state
            .runtime_settings
            .fence_authentication_revision(revision)?;
        let now = Utc::now();
        let selected = state
            .upstream_sessions
            .iter()
            .filter(|(_, record)| {
                record.principal() == principal
                    && record.has_credential_or_live_lease(now)
                    && state.upstream_eligible(record, now)
            })
            .max_by_key(|(id, record)| {
                (
                    record.has_live_lease(now),
                    !record.retry_deferred(now),
                    state.sessions.get(id).map(|session| session.absolute),
                    std::cmp::Reverse(**id),
                )
            })
            .map(|(id, _)| *id);
        let Some(id) = selected else {
            return Ok(StorageUpstreamClaimOutcome::Unavailable);
        };
        Ok(state
            .upstream_sessions
            .get_mut(&id)
            .expect("selected session")
            .claim(now))
    }
    async fn finish_upstream_renewal(
        &self,
        request: StorageUpstreamRenewal,
    ) -> Result<(), StorageError> {
        let mut live = self.state.write().await;
        let mut state = live.clone();
        let original = request.claim().session();
        let mut record = state
            .upstream_sessions
            .get(&original.session().value())
            .cloned()
            .ok_or_else(|| StorageError::authentication_required("Upstream session changed"))?;
        if !state.upstream_eligible(&record, Utc::now()) {
            return Err(StorageError::authentication_required(
                "Upstream session changed",
            ));
        }
        record
            .renewed(
                request.claim(),
                request.proof(),
                request.credential().cloned(),
                Utc::now(),
            )
            .map_err(|_| StorageError::authentication_required("Upstream session changed"))?;
        let current = state
            .users
            .get(&original.principal().id())
            .ok_or_else(|| StorageError::authentication_required("Upstream principal changed"))?;
        if current.external_subject.as_deref()
            != Some(request.proof().identity().subject().as_str())
            || current
                .last_sync_success_at
                .is_some_and(|at| at > request.proof().verified_at())
        {
            return Err(StorageError::authentication_required(
                "Upstream identity changed",
            ));
        }
        let authentication =
            StorageExternalInteractionAuthentication::Login(request.proof().clone());
        let admitted = authentication
            .admit(Some(original.principal()))
            .map_err(invalid_contract_value)?;
        let user = state
            .sync_external_user(
                admitted.synchronization(),
                request.proof().memberships().provider_groups().is_none(),
                Some(request.proof().verified_at()),
            )?
            .into_value();
        if user.id().id() != original.principal().id() {
            return Err(StorageError::authentication_required(
                "Upstream identity changed",
            ));
        }
        state
            .upstream_sessions
            .insert(record.session().value(), record);
        *live = state;
        Ok(())
    }
    async fn upstream_logout_credentials(
        &self,
        target: StorageUpstreamRevocation,
    ) -> Result<Vec<StorageUpstreamSession>, StorageError> {
        let state = self.state.read().await;
        Ok(state.logout_grants(target))
    }

    async fn find_external_identity(
        &self,
        identity: ExternalIdentity,
    ) -> Result<Option<PrincipalId>, StorageError> {
        self.state.read().await.identity_principal(&identity)
    }

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

    async fn record_external_sync_failure(
        &self,
        revision: ResourceRevision,
        principal_id: PrincipalId,
        failure: StorageExternalSyncFailure,
    ) -> Result<(), StorageError> {
        let mut live = self.state.write().await;
        live.runtime_settings
            .fence_authentication_revision(revision)?;
        let mut state = live.clone();
        state.external_sync_failure(principal_id, failure)?;
        *live = state;
        Ok(())
    }

    async fn sync_external_user(
        &self,
        revision: ResourceRevision,
        request: StorageExternalUserSync,
    ) -> Result<StorageMutationOutcome<StorageSyncedHuman>, StorageError> {
        let mut live = self.state.write().await;
        live.runtime_settings
            .fence_authentication_revision(revision)?;
        let mut staged = live.clone();
        let result = staged.sync_external_user(request, false, None)?;
        *live = staged;
        Ok(result)
    }
}

fn source_membership(source: &StorageBackupRow) -> Option<(i32, i32)> {
    let id = |field| i32::try_from(source.get(field)?.as_i64()?).ok();
    Some((id("principal_id")?, id("group_id")?))
}

impl MemoryState {
    pub(super) fn logout_grants(
        &self,
        target: StorageUpstreamRevocation,
    ) -> Vec<StorageUpstreamSession> {
        self.upstream_sessions
            .values()
            .filter(|record| {
                record.credential().is_some()
                    && self
                        .sessions
                        .get(&record.session().value())
                        .is_some_and(|session| !session.revoked)
                    && match &target {
                        StorageUpstreamRevocation::Sessions(SessionRevocation::All(id)) => {
                            record.principal() == *id
                        }
                        StorageUpstreamRevocation::Sessions(SessionRevocation::Session(id)) => {
                            record.session() == *id
                        }
                        StorageUpstreamRevocation::Sessions(SessionRevocation::Current(id)) => {
                            self.session_for_token(*id) == Some(record.session().value())
                        }
                        StorageUpstreamRevocation::Provider(provider) => {
                            record.binding().provider().external_id() == Some(provider.id())
                                && record.namespace() == provider.namespace()
                        }
                    }
            })
            .cloned()
            .collect()
    }

    pub(super) fn identity_principal(
        &self,
        identity: &ExternalIdentity,
    ) -> Result<Option<PrincipalId>, StorageError> {
        if !self
            .runtime_settings
            .provider_binding(identity.scope().as_str())
            .is_some_and(|provider| {
                Some(provider.id()) == identity.provider().external_id()
                    && provider.kind().as_str() == identity.provider_kind()
                    && provider.namespace() == identity.namespace()
            })
        {
            return Ok(None);
        }
        let scope = self.identity_scopes.values().find(|scope| {
            scope.name() == identity.scope().as_str()
                && scope.provider_kind() == identity.provider_kind()
        });
        let Some(scope) = scope else {
            return Ok(None);
        };
        self.users
            .iter()
            .find(|(_, record)| {
                record.identity_scope_id == scope.id()
                    && record.provider_managed
                    && record.external_subject.as_deref() == Some(identity.subject().as_str())
            })
            .map(|(id, _)| {
                PrincipalId::new(*id).map_err(|error| StorageError::internal(error.to_string()))
            })
            .transpose()
    }

    fn owns_external_membership(&self, key: (i32, i32), scope: IdentityScopeId) -> bool {
        let sources = self.membership_sources.iter().filter(|source| {
            source_membership(source) == Some(key)
                && source.get("source").and_then(Value::as_str) == Some(EXTERNAL_MEMBERSHIP_SOURCE)
        });
        if sources.clone().count() == 0 {
            self.groups
                .get(&key.1)
                .is_some_and(|group| group.identity_scope_id() == scope)
        } else {
            sources.clone().any(|source| {
                source.get("source_scope_id").and_then(Value::as_i64) == Some(i64::from(scope.id()))
            })
        }
    }
    pub(super) fn clear_provider_memberships(&mut self, scope: IdentityScopeId) {
        // Imported memberships may carry implicit provenance through the
        // group's scope. Explicit surviving sources take precedence.
        let mut affected: BTreeSet<_> = self
            .external_memberships
            .iter()
            .copied()
            .filter(|(_, group)| {
                self.groups
                    .get(group)
                    .is_some_and(|g| g.identity_scope_id() == scope)
            })
            .collect();
        self.membership_sources.retain(|row| {
            let owned = row.get("source").and_then(Value::as_str)
                == Some(EXTERNAL_MEMBERSHIP_SOURCE)
                && row.get("source_scope_id").and_then(Value::as_i64)
                    == Some(i64::from(scope.id()));
            if owned && let Some(key) = source_membership(row) {
                affected.insert(key);
            }
            !owned
        });
        let mut lost_effective = affected.clone();
        for row in &self.membership_sources {
            if let Some(key) = source_membership(row) {
                lost_effective.remove(&key);
                if row.get("source").and_then(Value::as_str) == Some(EXTERNAL_MEMBERSHIP_SOURCE) {
                    affected.remove(&key);
                }
            }
        }
        for key in affected {
            self.external_memberships.remove(&key);
        }
        for key in lost_effective {
            self.memberships.remove(&key);
        }
    }

    fn remove_provider_membership(&mut self, key: (i32, i32), scope: IdentityScopeId) {
        self.membership_sources.retain(|source| {
            !(source_membership(source) == Some(key)
                && source.get("source").and_then(Value::as_str) == Some(EXTERNAL_MEMBERSHIP_SOURCE)
                && source.get("source_scope_id").and_then(Value::as_i64)
                    == Some(i64::from(scope.id())))
        });
        let retained = self
            .membership_sources
            .iter()
            .filter(|source| source_membership(source) == Some(key));
        if !retained.clone().any(|source| {
            source.get("source").and_then(Value::as_str) == Some(EXTERNAL_MEMBERSHIP_SOURCE)
        }) {
            self.external_memberships.remove(&key);
        }
        if retained.count() == 0 {
            self.memberships.remove(&key);
        }
    }
    pub(super) fn external_sync_failure(
        &mut self,
        principal_id: PrincipalId,
        failure: StorageExternalSyncFailure,
    ) -> Result<(), StorageError> {
        let current = self.users.get(&principal_id.id()).cloned().ok_or_else(|| {
            StorageError::not_found(format!("Principal {} was not found", principal_id.id()))
        })?;
        if !current.provider_managed {
            return Ok(());
        }
        let now = Utc::now();
        if failure == StorageExternalSyncFailure::Rejected {
            // Match PostgreSQL's non-authenticating rejection marker. Clearing it
            // during recovery advances the snapshot instead of reviving old logins.
            let parts = current.user.into_parts();
            let record = self
                .users
                .get_mut(&principal_id.id())
                .expect("external user exists");
            record.user = StorageUser::try_new(
                parts.id(),
                Some("!external-identity-rejected-no-login".into()),
                parts.proper_name().map(ToOwned::to_owned),
                parts.email().map(ToOwned::to_owned),
                parts.created_at(),
                now,
                parts.anonymized_at(),
            )
            .map_err(invalid_contract_value)?;
            record.last_sync_success_at = None;
            self.invalidate_principal_sessions(principal_id.id());
            for token in self
                .tokens
                .values_mut()
                .filter(|token| token.principal_id == principal_id)
            {
                if token.revoked_at.is_none() {
                    token.revision = token
                        .revision
                        .checked_advance()
                        .map_err(|error| StorageError::internal(error.to_string()))?;
                    token.revoked_at = Some(now);
                }
            }
        }
        self.users
            .get_mut(&principal_id.id())
            .expect("external user exists")
            .last_sync_attempted_at = Some(now);
        let principal = self
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
        .last_sync_success_at(if failure == StorageExternalSyncFailure::Rejected {
            None
        } else {
            principal.last_sync_success_at()
        })
        .try_build()
        .map_err(invalid_contract_value)?;
        self.principals.insert(principal_id.id(), updated);
        Ok(())
    }

    pub(super) fn sync_external_user(
        &mut self,
        request: StorageExternalUserSync,
        preserve_memberships: bool,
        observed_at: Option<DateTime<Utc>>,
    ) -> Result<StorageMutationOutcome<StorageSyncedHuman>, StorageError> {
        let (scope_name, provider_kind, subject, name, proper_name, email, groups) =
            request.into_parts();
        let state = self;
        let scope = if let Some(scope) = state.identity_scope_by_name(&scope_name).cloned() {
            if scope.provider_kind() != provider_kind {
                return Err(StorageError::conflict(format!(
                    "Identity scope '{scope_name}' uses provider kind '{}'",
                    scope.provider_kind()
                )));
            }
            scope
        } else {
            let id = IdentityScopeId::new(state.identity_scope_ids.allocate()?)
                .expect("memory identity scope id is positive");
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
        // A name is not evidence that two external identities are the same user.
        // Check all principals, including non-human and unmanaged collisions.
        if state.principals.values().any(|principal| {
            principal.identity_scope_id() == scope.id()
                && ((principal.name() == name
                    && principal.external_subject() != Some(subject.as_str()))
                    || ((principal.name() == name
                        || principal.external_subject() == Some(subject.as_str()))
                        && (!principal.provider_managed()
                            || principal.kind() != PrincipalKind::Human)))
        }) {
            return Err(StorageError::conflict(
                "identity scope already contains a different principal with this name",
            ));
        }
        let existing_id = state
            .users
            .iter()
            .find(|(_, record)| {
                record.identity_scope_id == scope.id()
                    && record.external_subject.as_deref() == Some(subject.as_str())
            })
            .map(|(id, _)| *id);
        let now = Utc::now();
        let sync_time = observed_at.unwrap_or(now);
        let principal_id = if let Some(id) = existing_id {
            let record = state
                .users
                .get(&id)
                .cloned()
                .expect("selected external user exists");
            let parts = record.user.into_parts();
            // PostgreSQL suppresses unchanged user writes. Preserve the same
            // credential snapshot when another login only refreshes sync metadata.
            let updated_at = if parts.password_hash().is_none()
                && parts.proper_name() == proper_name.as_deref()
                && parts.email() == email.as_deref()
            {
                parts.updated_at()
            } else {
                now
            };
            let user = StorageUser::try_new(
                parts.id(),
                None,
                proper_name.clone(),
                email.clone(),
                parts.created_at(),
                updated_at,
                parts.anonymized_at(),
            )
            .map_err(invalid_contract_value)?;
            let record = state.users.get_mut(&id).expect("external user exists");
            record.user = user;
            record.name = name.clone();
            record.provider_managed = true;
            record.external_subject = Some(subject.clone());
            record.last_sync_attempted_at = Some(sync_time);
            record.last_sync_success_at = Some(sync_time);
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
                    .last_sync_attempted_at(Some(sync_time))
                    .last_sync_success_at(Some(sync_time))
                    .try_build()
                    .map_err(invalid_contract_value)?;
            state.principals.insert(id, principal);
            PrincipalId::new(id).expect("external principal id is positive")
        } else {
            let principal_id = PrincipalId::new(state.principal_ids.allocate()?)
                .expect("memory principal id is positive");
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
                    .last_sync_attempted_at(Some(sync_time))
                    .last_sync_success_at(Some(sync_time))
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
                    last_sync_attempted_at: Some(sync_time),
                    last_sync_success_at: Some(sync_time),
                },
            );
            principal_id
        };

        if !preserve_memberships {
            let mut current_external_groups = BTreeSet::new();
            for external_group in groups {
                if state.groups.values().any(|group| {
                    group.identity_scope_id() == scope.id()
                        && group.name() == external_group.name()
                        && group.external_key() != Some(external_group.key())
                }) {
                    return Err(StorageError::conflict("External group name collision"));
                }
                let group = if let Some(group) = state.groups.values().find(|group| {
                    group.identity_scope_id() == scope.id()
                        && group.external_key() == Some(external_group.key())
                }) {
                    group.clone()
                } else {
                    let group_id = GroupId::new(state.group_ids.allocate()?)
                        .expect("memory external group id is positive");
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
                    .last_sync_attempted_at(Some(sync_time))
                    .last_sync_success_at(Some(sync_time))
                    .try_build()
                    .map_err(invalid_contract_value)?;
                    state.groups.insert(group_id.id(), group.clone());
                    group
                };
                let key = (principal_id.id(), group.id().id());
                current_external_groups.insert(key);
                if !state.membership_sources.iter().any(|source| {
                    source_membership(source) == Some(key)
                        && source.get("source").and_then(Value::as_str)
                            == Some(EXTERNAL_MEMBERSHIP_SOURCE)
                        && source.get("source_scope_id").and_then(Value::as_i64)
                            == Some(i64::from(scope.id().id()))
                        && source.get("source_key").and_then(Value::as_str)
                            == Some(external_group.key())
                }) {
                    state.membership_sources.push(StorageBackupRow::try_from_value(json!({
                        "principal_id":key.0,"group_id":key.1,"source":EXTERNAL_MEMBERSHIP_SOURCE,
                        "source_scope_id":scope.id().id(),"source_key":external_group.key(),
                        "created_at":sync_time,"updated_at":sync_time,
                    })).map_err(invalid_contract_value)?);
                }
                if let std::collections::btree_map::Entry::Vacant(entry) =
                    state.memberships.entry(key)
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
                .filter(|key| state.owns_external_membership(**key, scope.id()))
                .filter(|key| !current_external_groups.contains(key))
                .copied()
                .collect::<Vec<_>>();
            for key in stale_groups {
                state.remove_provider_membership(key, scope.id());
            }
            state.external_memberships.extend(current_external_groups);
        }
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
        let string_filters = options
            .filters()
            .iter()
            .filter(|filter| {
                matches!(
                    filter.field(),
                    FilterField::Name
                        | FilterField::Username
                        | FilterField::ProperName
                        | FilterField::Email
                        | FilterField::IdentityScope
                )
            })
            .map(|filter| Ok((filter.field(), StringFilter::new(filter)?)))
            .collect::<Result<Vec<_>, StorageError>>()?;
        let state = self.state.read().await;
        let rows = state
            .users
            .values()
            .filter(|record| {
                let parts = record.user.clone().into_parts();
                if !string_filters.iter().all(|(field, filter)| {
                    let actual = match field {
                        FilterField::Name | FilterField::Username => Some(record.name.as_str()),
                        FilterField::ProperName => parts.proper_name(),
                        FilterField::Email => parts.email(),
                        FilterField::IdentityScope => state
                            .identity_scopes
                            .get(&record.identity_scope_id.id())
                            .map(|scope| scope.name()),
                        _ => unreachable!("compiled user string filter"),
                    };
                    filter.matches(actual)
                }) {
                    return false;
                }
                options.filters().as_slice().iter().all(|filter| {
                    if *filter.field() != FilterField::Id {
                        return true;
                    }
                    let equal = parts.id().to_string() == filter.value();
                    match *filter.operator() {
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
        let mut live = self.state.write().await;
        let mut staged = live.clone();
        let result = create_user_in_state(&mut staged, request)?;
        *live = staged;
        Ok(result)
    }

    async fn update_user(
        &self,
        request: StorageUserUpdate,
    ) -> Result<StorageMutationOutcome<StorageUser>, StorageError> {
        let mut live = self.state.write().await;
        let mut staged = live.clone();
        let result = update_user_in_state(&mut staged, request)?;
        *live = staged;
        Ok(result)
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
        state.invalidate_principal_sessions(id.id());
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
        if state
            .users
            .get(&id.id())
            .is_some_and(|user| user.provider_managed)
        {
            return Err(StorageError::permission_denied(
                "Provider-managed users are read-only in Hubuum",
            ));
        }
        let Some(record) = state.users.remove(&id.id()) else {
            return Ok(StorageMutationOutcome::unchanged(0));
        };
        let actor =
            PrincipalId::new(id.id()).map_err(|error| StorageError::internal(error.to_string()))?;
        for task in state.tasks.values_mut() {
            task.control.forget_actor(actor);
        }
        state.principals.remove(&id.id());
        state
            .sessions
            .retain(|_, session| session.principal_id().id() != id.id());
        state
            .memberships
            .retain(|(principal_id, _), _| *principal_id != id.id());
        state
            .tokens
            .retain(|_, token| token.principal_id.id() != id.id());
        state.cleanup_credential_activity();
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
        if current.provider_managed {
            return Err(StorageError::permission_denied(
                "Provider-managed users are read-only in Hubuum",
            ));
        }
        let parts = current.user.into_parts();
        if parts.anonymized_at().is_some() {
            return Ok(StorageMutationOutcome::unchanged(()));
        }
        let now = Utc::now();
        state.invalidate_principal_sessions(id.id());
        for token in state.tokens.values_mut() {
            if token.principal_id.id() == id.id() && token.revoked_at.is_none() {
                token.revision = token
                    .revision
                    .checked_advance()
                    .map_err(|error| StorageError::internal(error.to_string()))?;
                token.revoked_at = Some(now);
            }
        }
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
impl GroupStorage for MemoryStorage {
    async fn list_groups(
        &self,
        query: StorageGroupListQuery,
    ) -> Result<StoragePage<StorageIdentityGroup>, StorageError> {
        let options = query.into_options();
        let filters = ResourceFilters::for_groups(&options)?;
        let state = self.state.read().await;
        let rows = state
            .groups
            .values()
            .filter(|group| {
                filters.matches_with_identity_scope(
                    group.id().id(),
                    group.name(),
                    group.description(),
                    state
                        .identity_scopes
                        .get(&group.identity_scope_id().id())
                        .map(|scope| scope.name()),
                )
            })
            .cloned()
            .collect();
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
        let id = GroupId::new(state.group_ids.allocate()?).expect("memory group id is positive");
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

fn create_user_in_state(
    state: &mut MemoryState,
    mut request: StorageUserCreate,
) -> Result<StorageMutationOutcome<StorageUser>, StorageError> {
    let claim = request.take_credential_claim();
    let (identity_scope, name, password_hash, proper_name, email, context) = request.into_parts();
    let identity_scope = identity_scope.unwrap_or_else(|| LOCAL_IDENTITY_SCOPE.to_string());
    let approval_audit = state.consume_credential_claim(
        claim.as_ref(),
        StorageCredentialOperation::CreateUser,
        None,
        &context,
    )?;
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
        PrincipalId::new(state.principal_ids.allocate()?).expect("memory principal id is positive");
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
    Ok(super::credential_approval::outcome(
        user,
        receipt,
        approval_audit,
    ))
}

fn update_user_in_state(
    state: &mut MemoryState,
    mut request: StorageUserUpdate,
) -> Result<StorageMutationOutcome<StorageUser>, StorageError> {
    let claim = request.take_credential_claim();
    let (id, password_hash, proper_name, email, context) = request.into_parts();
    let approval_audit = state.consume_credential_claim(
        claim.as_ref(),
        StorageCredentialOperation::UpdateUser,
        Some(PrincipalId::new(id.id()).expect("validated user ID")),
        &context,
    )?;
    let current = state
        .users
        .get(&id.id())
        .cloned()
        .ok_or_else(|| StorageError::not_found(format!("User {} was not found", id.id())))?;
    if password_hash.is_none() && proper_name.is_none() && email.is_none() {
        return Ok(match approval_audit {
            Some(audit) => StorageMutationOutcome::committed(current.user, audit),
            None => StorageMutationOutcome::unchanged(current.user),
        });
    }
    let password_changed = password_hash.is_some();
    if password_changed {
        state.invalidate_principal_sessions(id.id());
        for token in state.tokens.values_mut() {
            if token.principal_id.id() == id.id() && token.revoked_at.is_none() {
                token.revoked_at = Some(Utc::now());
            }
        }
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
    Ok(super::credential_approval::outcome(
        user,
        receipt,
        approval_audit,
    ))
}

impl MemoryState {
    fn revoke_owned_token(
        &mut self,
        request: StorageTokenRevoke,
    ) -> Result<StorageMutationOutcome<StorageTokenRevocations>, StorageError> {
        let (token_id, principal_id, context) = request.into_parts();
        let token = self.tokens.get_mut(&token_id.id()).ok_or_else(|| {
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
            return Ok(StorageMutationOutcome::unchanged(
                StorageTokenRevocations::default(),
            ));
        }
        if self.session_for_token(token_id).is_some() {
            return self.revoke_sessions(SessionRevocation::Current(token_id), context);
        }
        let token = self
            .tokens
            .get_mut(&token_id.id())
            .expect("validated token exists");
        token.revoked_at = Some(Utc::now());
        token.revision = token
            .revision
            .checked_advance()
            .map_err(|error| StorageError::internal(error.to_string()))?;
        let receipt = self.append_simple_event(
            EntityType::Token,
            token_id.id(),
            None,
            Action::Revoked,
            &context,
            format!("Token {} revoked", token_id.id()),
        )?;
        Ok(StorageMutationOutcome::committed(
            StorageTokenRevocations::new(1, Vec::new()),
            receipt,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exhausted_group_ids_roll_back_external_user_synchronization() {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(async {
                let storage = MemoryStorage::new();
                storage.state.write().await.group_ids.observe(i32::MAX);
                let sync = StorageExternalUserSync::builder(
                    "directory",
                    LDAP_PROVIDER_KIND,
                    "alice",
                    "alice",
                )
                .groups(vec![StorageExternalGroup::new(
                    "operators",
                    "Operators",
                    None,
                )])
                .build();
                assert!(
                    storage
                        .sync_external_user(ResourceRevision::INITIAL, sync)
                        .await
                        .is_err()
                );
                let state = storage.state.read().await;
                assert!(state.identity_scope_by_name("directory").is_none());
                assert!(
                    !state
                        .principals
                        .values()
                        .any(|principal| principal.name() == "alice")
                );
            });
    }
}
