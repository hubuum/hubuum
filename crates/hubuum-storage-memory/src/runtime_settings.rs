use super::*;

/// Process-local runtime settings. Like the rest of this adapter, values are
/// shared only by handles over one in-memory store and are lost on restart.
#[derive(Clone)]
pub(super) struct MemoryRuntimeSettings {
    snapshot: StorageRuntimeSettingsSnapshot,
    changes: Vec<StorageRuntimeSettingsChange>,
    candidates: BTreeMap<Uuid, StorageSecurityCandidate>,
    bindings: BTreeMap<String, StorageProviderIdentityBinding>,
}

impl MemoryRuntimeSettings {
    pub(super) fn new() -> Self {
        Self {
            snapshot: StorageRuntimeSettingsSnapshot::initial(Utc::now()),
            changes: Vec::new(),
            candidates: BTreeMap::new(),
            bindings: BTreeMap::new(),
        }
    }

    pub(super) fn authentication_revision(&self) -> ResourceRevision {
        self.snapshot
            .record(StorageRuntimeSettingsGroup::Authentication)
            .revision()
    }

    pub(super) fn provider_binding(&self, name: &str) -> Option<&StorageProviderIdentityBinding> {
        self.bindings.get(name)
    }
    pub(super) fn verify_candidate(
        &mut self,
        candidate: StorageSecurityCandidate,
    ) -> Result<(), StorageError> {
        self.candidates
            .get_mut(&candidate.id())
            .ok_or_else(|| StorageError::not_found("Security candidate not found"))?
            .accept_verification(candidate, Utc::now())
            .map_err(|error| StorageError::conflict(error.to_string()))
    }
    pub(super) fn discard_candidates(&mut self) {
        self.candidates.clear();
    }

    fn update(
        &mut self,
        request: &StorageRuntimeSettingsUpdate,
    ) -> Result<StorageRuntimeSettingsUpdateOutcome, StorageError> {
        let current = self.snapshot.record(request.group()).clone();
        if current.revision() != request.expected_revision() {
            return Err(StorageError::revision_conflict(
                format!(
                    "The {} runtime settings revision changed",
                    request.group().as_str()
                ),
                current.revision(),
            ));
        }
        if current.assignment() == request.assignment() {
            return Ok(StorageRuntimeSettingsUpdateOutcome::Unchanged(current));
        }
        let revision = current
            .revision()
            .checked_advance()
            .map_err(|error| StorageError::internal(error.to_string()))?;
        let changed_at = Utc::now();
        let change = StorageRuntimeSettingsChange::try_new(
            revision,
            current.assignment().clone(),
            request.assignment().clone(),
            changed_at,
            StorageRuntimeSettingsActor::from_context(request.context()),
        )
        .map_err(|error| StorageError::internal(error.to_string()))?;
        let record = StorageRuntimeSettingsRecord::new(
            request.assignment().clone(),
            revision,
            changed_at,
            request.context().actor_user_id(),
        );
        let records = self
            .snapshot
            .records()
            .iter()
            .map(|existing| {
                if existing.group() == request.group() {
                    record.clone()
                } else {
                    existing.clone()
                }
            })
            .collect();
        self.snapshot = StorageRuntimeSettingsSnapshot::try_new(records)
            .map_err(|error| StorageError::internal(error.to_string()))?;
        self.changes.push(change);
        Ok(StorageRuntimeSettingsUpdateOutcome::Committed(record))
    }

    fn changes(
        &self,
        query: StorageRuntimeSettingsHistoryQuery,
    ) -> Vec<StorageRuntimeSettingsChange> {
        self.changes
            .iter()
            .rev()
            .filter(|change| change.group() == query.group())
            .filter(|change| {
                query
                    .before()
                    .is_none_or(|before| change.revision() < before)
            })
            .take(query.limit())
            .cloned()
            .collect()
    }
}

#[async_trait]
impl RuntimeSettingsStorage for MemoryStorage {
    async fn recover_security_settings(
        &self,
        request: StorageSecurityRecovery,
    ) -> Result<[StorageRuntimeSettingsUpdateOutcome; 2], StorageError> {
        let mut live = self.state.write().await;
        let mut staged = live.clone();
        staged.require_local_recovery_administrator(
            request.administrator(),
            request.recovery_group(),
        )?;
        let authentication = staged.runtime_settings.update(&request.updates()[0])?;
        let authorization = staged.runtime_settings.update(&request.updates()[1])?;
        let scopes: Vec<_> = staged
            .identity_scopes
            .values()
            .filter(|scope| scope.name() != "local")
            .map(|scope| scope.id())
            .collect();
        staged.invalidate_provider_credentials(&scopes);
        staged.runtime_settings.discard_candidates();
        staged.auth_interactions.clear();
        staged.upstream_sessions.clear();
        for (_, metadata) in staged.credential_approvals.values_mut() {
            metadata.invalidate(Utc::now());
        }
        staged.append_simple_event(
            EntityType::Authentication,
            request.administrator().id(),
            None,
            Action::Updated,
            &EventContext::system(),
            "Offline security recovery: local authorization and local-only authentication",
        )?;
        *live = staged;
        Ok([authentication, authorization])
    }

    async fn purge_security_candidates(&self) -> Result<usize, StorageError> {
        let mut state = self.state.write().await;
        let candidates = &mut state.runtime_settings.candidates;
        let before = candidates.len();
        candidates.retain(|_, candidate| candidate.expires_at() > Utc::now());
        Ok(before - candidates.len())
    }

    async fn authentication_provider_binding(
        &self,
        name: IdentityScopeName,
    ) -> Result<Option<StorageProviderIdentityBinding>, StorageError> {
        Ok(self
            .state
            .read()
            .await
            .runtime_settings
            .bindings
            .get(name.as_str())
            .cloned())
    }

    async fn create_security_candidate(
        &self,
        candidate: StorageSecurityCandidate,
    ) -> Result<(), StorageError> {
        let mut state = self.state.write().await;
        let settings = &mut state.runtime_settings;
        let now = Utc::now();
        settings
            .candidates
            .retain(|_, candidate| candidate.expires_at() > now);
        let revision = settings
            .snapshot
            .record(candidate.assignment().group())
            .revision();
        if revision != candidate.base_revision() {
            return Err(StorageError::revision_conflict(
                "Security settings changed",
                revision,
            ));
        }
        if candidate.expires_at() <= now
            || candidate.created_at() > now
            || candidate.checks().is_empty()
        {
            return Err(StorageError::invalid_input(
                "Invalid security candidate lifetime or checks",
            ));
        }
        if settings.candidates.len() >= MAX_SECURITY_CANDIDATES
            || settings
                .candidates
                .values()
                .filter(|stored| stored.owner().actor_id() == candidate.owner().actor_id())
                .count()
                >= MAX_SECURITY_CANDIDATES_PER_ACTOR
        {
            return Err(StorageError::conflict(
                "Too many active security candidates",
            ));
        }
        if settings.candidates.contains_key(&candidate.id()) {
            return Err(StorageError::conflict("Candidate already exists"));
        }
        settings.candidates.insert(candidate.id(), candidate);
        Ok(())
    }

    async fn get_security_candidate(
        &self,
        id: Uuid,
        owner: StorageSecurityCandidateOwner,
    ) -> Result<StorageSecurityCandidate, StorageError> {
        self.state
            .read()
            .await
            .runtime_settings
            .candidates
            .get(&id)
            .filter(|candidate| candidate.owner() == owner)
            .cloned()
            .ok_or_else(|| StorageError::not_found("Security candidate not found"))
    }

    async fn verify_security_candidate(
        &self,
        candidate: StorageSecurityCandidate,
    ) -> Result<(), StorageError> {
        self.state
            .write()
            .await
            .runtime_settings
            .verify_candidate(candidate)
    }

    async fn activate_security_candidate(
        &self,
        request: StorageSecurityCandidateActivation,
    ) -> Result<StorageRuntimeSettingsUpdateOutcome, StorageError> {
        let mut live = self.state.write().await;
        let mut staged = live.clone();
        staged.consume_credential_claim(
            Some(request.claim()),
            StorageCredentialOperation::ActivateSecurityCandidate,
            None,
            request.update().context(),
        )?;
        staged.require_local_recovery_administrator(
            request.claim().actor_id(),
            request.recovery_group(),
        )?;
        let previous = staged
            .runtime_settings
            .snapshot
            .record(request.update().group())
            .assignment()
            .clone();
        let candidate = staged
            .runtime_settings
            .candidates
            .get_mut(&request.candidate_id())
            .ok_or_else(|| StorageError::not_found("Security candidate not found"))?;
        candidate
            .activate(&request, Utc::now())
            .map_err(|error| StorageError::conflict(error.to_string()))?;
        if let Some(StorageRuntimeSettingsValue::Authentication(providers)) =
            candidate.verification_assignment().value()
        {
            for provider in providers.providers() {
                if staged.identity_scopes.values().any(|scope| {
                    scope.name() == provider.name()
                        && scope.provider_kind() != provider.kind().as_str()
                }) {
                    return Err(StorageError::conflict(
                        "Existing identity scope belongs to another provider kind",
                    ));
                }
                let binding = StorageProviderIdentityBinding::from_provider(provider)
                    .map_err(invalid_contract_value)?;
                if staged
                    .runtime_settings
                    .bindings
                    .get(binding.name())
                    .is_some_and(|old| old != &binding)
                    || staged
                        .runtime_settings
                        .bindings
                        .values()
                        .any(|old| old.id() == binding.id() && old.name() != binding.name())
                {
                    return Err(StorageError::conflict(
                        "Provider identity namespace is immutable; use a new scope",
                    ));
                }
                staged
                    .runtime_settings
                    .bindings
                    .insert(binding.name().to_owned(), binding);
            }
        }
        let revoked_scopes: Vec<_> =
            if request.update().group() == StorageRuntimeSettingsGroup::Authentication {
                staged
                    .identity_scopes
                    .values()
                    .filter(|scope| {
                        scope.name() != "local"
                            && !candidate.retains_authenticated_provider(&previous, scope.name())
                    })
                    .map(|scope| scope.id())
                    .collect()
            } else {
                Vec::new()
            };
        let result = staged.runtime_settings.update(request.update())?;
        staged.invalidate_provider_credentials(&revoked_scopes);
        let revision = staged.runtime_settings.authentication_revision();
        staged
            .upstream_sessions
            .retain(|_, record| record.binding().revision() == revision);
        *live = staged;
        Ok(result)
    }

    async fn load_runtime_settings(&self) -> Result<StorageRuntimeSettingsSnapshot, StorageError> {
        Ok(self.state.read().await.runtime_settings.snapshot.clone())
    }

    async fn update_runtime_settings(
        &self,
        request: StorageRuntimeSettingsUpdate,
    ) -> Result<StorageRuntimeSettingsUpdateOutcome, StorageError> {
        self.state.write().await.runtime_settings.update(&request)
    }

    async fn list_runtime_settings_changes(
        &self,
        query: StorageRuntimeSettingsHistoryQuery,
    ) -> Result<Vec<StorageRuntimeSettingsChange>, StorageError> {
        Ok(self.state.read().await.runtime_settings.changes(query))
    }
}

impl MemoryState {
    fn invalidate_provider_credentials(&mut self, scopes: &[IdentityScopeId]) {
        let principals: Vec<_> = self
            .principals
            .values()
            .filter(|principal| scopes.contains(&principal.identity_scope_id()))
            .map(|principal| principal.id())
            .collect();
        for principal in &principals {
            self.invalidate_principal_sessions(principal.id());
        }
        for token in self
            .tokens
            .values_mut()
            .filter(|token| principals.contains(&token.principal_id) && token.revoked_at.is_none())
        {
            token.revoked_at = Some(Utc::now());
            token.revision = token
                .revision
                .checked_advance()
                .expect("credential revision");
        }
        for (claim, metadata) in self.credential_approvals.values_mut() {
            if principals.contains(&claim.actor_id()) {
                metadata.invalidate(Utc::now());
            }
        }
    }

    pub(super) fn require_local_recovery_administrator(
        &self,
        actor: PrincipalId,
        group: &str,
    ) -> Result<(), StorageError> {
        let valid_user = self.users.get(&actor.id()).is_some_and(|user| {
            let parts = user.user.clone().into_parts();
            !user.provider_managed
                && parts.password_hash().is_some()
                && parts.anonymized_at().is_none()
                && self
                    .identity_scopes
                    .get(&user.identity_scope_id.id())
                    .is_some_and(|scope| scope.name() == "local")
        });
        let valid_grant = self.groups.values().any(|candidate| {
            candidate.name() == group
                && candidate.managed_by() == "local"
                && self
                    .identity_scopes
                    .get(&candidate.identity_scope_id().id())
                    .is_some_and(|scope| scope.name() == "local")
                && self
                    .memberships
                    .contains_key(&(actor.id(), candidate.id().id()))
        });
        if !valid_user || !valid_grant {
            return Err(StorageError::permission_denied(
                "Local recovery administrator access changed",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod external_identity_tests;
