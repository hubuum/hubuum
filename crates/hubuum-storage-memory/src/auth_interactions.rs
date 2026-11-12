use super::*;
use hubuum_storage_core::{
    MAX_ACTIVE_AUTH_INTERACTIONS, MAX_ACTIVE_AUTH_INTERACTIONS_PER_ACTOR,
    MAX_ACTIVE_AUTH_INTERACTIONS_PER_CLIENT, MAX_AUTH_INTERACTIONS, StorageInteractionAccess,
    StorageInteractionAction, StorageInteractionCommand, StorageInteractionComplete,
    StorageInteractionCompletion, StorageInteractionCreate, StorageInteractionEvidence,
    StorageInteractionIssuance, StorageInteractionLogin, StorageInteractionOutcome,
    StorageInteractionRecord, StorageInteractionView,
};

fn invalid() -> StorageError {
    StorageError::authentication_required("Invalid authentication interaction")
}
fn rejected(error: InteractionTransitionError) -> StorageInteractionOutcome {
    StorageInteractionOutcome::Rejected(error)
}

impl MemoryState {
    fn interaction_actor_valid(
        &self,
        record: &StorageInteractionRecord,
        now: DateTime<Utc>,
    ) -> bool {
        let Some(actor) = record.lifecycle().binding().authority().actor() else {
            return true;
        };
        let Some(lease) = record.actor_lease() else {
            return false;
        };
        let Some(token) = self.tokens.get(&actor.token_id().id()) else {
            return false;
        };
        let valid = now < lease.valid_until()
            && token.principal_id == actor.principal_id()
            && token.revoked_at.is_none()
            && token.scope.is_none()
            && token.issued <= record.lifecycle().created_at()
            && token
                .expires_at
                .is_none_or(|expiry| expiry >= lease.valid_until() && expiry > now)
            && self
                .users
                .get(&actor.principal_id().id())
                .is_some_and(|user| user.user.clone().into_parts().anonymized_at().is_none());
        valid
            && lease.recovery_group().is_none_or(|group| {
                self.require_local_recovery_administrator(actor.principal_id(), group)
                    .is_ok()
            })
    }

    fn interaction_fence(
        &self,
        record: &mut StorageInteractionRecord,
        now: DateTime<Utc>,
    ) -> Option<InteractionTransitionError> {
        if !matches!(
            record.lifecycle().state(now),
            InteractionState::Pending | InteractionState::Ready
        ) {
            return None;
        }
        if record.lifecycle().binding().revision()
            != self.runtime_settings.authentication_revision()
        {
            record.invalidate();
            return Some(InteractionTransitionError::ConfigurationChanged);
        }
        if !self.interaction_actor_valid(record, now) {
            record.invalidate();
            return Some(InteractionTransitionError::Invalid);
        }
        None
    }

    fn store_interaction(&mut self, record: StorageInteractionRecord) -> Result<(), StorageError> {
        record.to_document().map_err(invalid_contract_value)?;
        self.auth_interactions
            .insert(record.lifecycle().binding().id().value(), record);
        Ok(())
    }

    pub(super) fn create_interaction(
        &mut self,
        request: StorageInteractionCreate,
    ) -> Result<StorageInteractionView, StorageError> {
        self.purge_interactions(128)?;
        let now = Utc::now();
        let mut record = request.into_record();
        if record.lifecycle().created_at() > now
            || record.lifecycle().expires_at() <= now
            || self.interaction_fence(&mut record, now).is_some()
        {
            return Err(invalid());
        }
        if let Some(provider) = record.provider()
            && self
                .runtime_settings
                .provider_binding(provider.name())
                .is_some_and(|stored| stored != provider)
        {
            return Err(invalid());
        }
        let active: Vec<_> = self
            .auth_interactions
            .values()
            .filter(|value| {
                matches!(
                    value.lifecycle().state(now),
                    InteractionState::Pending | InteractionState::Ready
                )
            })
            .collect();
        let binding = record.lifecycle().binding();
        if self.auth_interactions.len() >= MAX_AUTH_INTERACTIONS
            || active.len() >= MAX_ACTIVE_AUTH_INTERACTIONS
            || active
                .iter()
                .filter(|value| value.lifecycle().binding().client() == binding.client())
                .count()
                >= MAX_ACTIVE_AUTH_INTERACTIONS_PER_CLIENT
            || binding.authority().actor().is_some_and(|actor| {
                active
                    .iter()
                    .filter(|value| {
                        value
                            .lifecycle()
                            .binding()
                            .authority()
                            .actor()
                            .is_some_and(|other| other.principal_id() == actor.principal_id())
                    })
                    .count()
                    >= MAX_ACTIVE_AUTH_INTERACTIONS_PER_ACTOR
            })
        {
            return Err(StorageError::conflict(
                "Too many authentication interactions",
            ));
        }
        if self.auth_interactions.contains_key(&binding.id().value())
            || record.user_code().is_some_and(|code| {
                self.auth_interactions
                    .values()
                    .any(|value| value.user_code() == Some(code))
            })
        {
            return Err(StorageError::conflict(
                "Authentication interaction collision",
            ));
        }
        let view = record.view(now);
        self.store_interaction(record)?;
        Ok(view)
    }

    pub(super) fn bind_interaction(
        &mut self,
        code: InteractionDigest,
        browser: InteractionBrowserBinding,
    ) -> Result<StorageInteractionOutcome, StorageError> {
        let mut record = self
            .auth_interactions
            .values()
            .find(|record| record.user_code() == Some(&code))
            .cloned()
            .ok_or_else(invalid)?;
        let now = Utc::now();
        let result = if let Some(error) = self.interaction_fence(&mut record, now) {
            rejected(error)
        } else {
            record
                .bind_browser(browser, now)
                .map(StorageInteractionOutcome::View)
                .unwrap_or_else(rejected)
        };
        record.discard_inactive_evidence(now);
        self.store_interaction(record)?;
        Ok(result)
    }

    fn interaction_login_valid(
        &self,
        record: &StorageInteractionRecord,
        login: &StorageInteractionLogin,
        subject: &str,
    ) -> bool {
        let Some(user) = self.users.get(&login.subject().principal_id().id()) else {
            return false;
        };
        let Some(scope) = self.identity_scopes.get(&user.identity_scope_id.id()) else {
            return false;
        };
        if user.provider_managed && user.last_sync_success_at.is_none()
            || login.restore_login(user.user.clone()).is_err()
            || scope.name() != login.subject().identity_scope()
            || scope.provider_kind() != login.subject().authentication_provider_kind()
        {
            return false;
        }
        match record.provider() {
            Some(provider) => {
                provider.name() == scope.name()
                    && provider.kind().as_str() == scope.provider_kind()
                    && user.external_subject.as_deref() == Some(subject)
            }
            None => {
                scope.name() == "local"
                    && subject == login.subject().principal_id().id().to_string()
            }
        }
    }

    pub(super) fn advance_interaction(
        &mut self,
        command: StorageInteractionCommand,
    ) -> Result<StorageInteractionOutcome, StorageError> {
        let mut record = self
            .auth_interactions
            .get(&command.authorization().id().value())
            .cloned()
            .ok_or_else(invalid)?;
        if !record.authorizes(&command) {
            return Err(invalid());
        }
        let now = Utc::now();
        let result = if let Some(error) = self.interaction_fence(&mut record, now) {
            rejected(error)
        } else if let StorageInteractionAction::Authenticate { authentication, .. } =
            command.action()
            && let StorageInteractionEvidence::Login(login) = authentication.evidence()
            && !self.interaction_login_valid(&record, login, authentication.proof().subject())
        {
            return Err(invalid());
        } else {
            record.apply(command, now)
        };
        self.store_interaction(record)?;
        Ok(result)
    }

    pub(super) fn complete_interaction(
        &mut self,
        request: StorageInteractionComplete,
    ) -> Result<StorageInteractionCompletion, StorageError> {
        let mut record = self
            .auth_interactions
            .get(&request.access().id().value())
            .cloned()
            .ok_or_else(invalid)?;
        if !record.allows(request.access()) {
            return Err(invalid());
        }
        let now = Utc::now();
        if let Some(error) = self.interaction_fence(&mut record, now) {
            self.store_interaction(record)?;
            return Ok(StorageInteractionCompletion::Rejected(error));
        }
        let evidence = match record.ready(request.access(), now) {
            Ok(evidence) => evidence.clone(),
            Err(error) => {
                self.store_interaction(record)?;
                return Ok(StorageInteractionCompletion::Rejected(error));
            }
        };
        if !record.accepts_issuance(request.issuance()) {
            return Ok(StorageInteractionCompletion::Rejected(
                InteractionTransitionError::Invalid,
            ));
        }
        if let StorageInteractionEvidence::Login(login) = &evidence {
            let proof = record
                .lifecycle()
                .authenticated_subject()
                .ok_or_else(invalid)?;
            if !self.interaction_login_valid(&record, login, proof.subject()) {
                return Err(invalid());
            }
        }
        let (outcome, issued) = match (evidence, request.issuance()) {
            (
                StorageInteractionEvidence::Login(evidence),
                StorageInteractionIssuance::Session {
                    credentials,
                    policy,
                    context,
                },
            ) => {
                let user = self
                    .users
                    .get(&evidence.subject().principal_id().id())
                    .ok_or_else(invalid)?
                    .user
                    .clone();
                let login = evidence.restore_login(user).map_err(|_| invalid())?;
                let session = self.create_session(StorageSessionCreate::new(
                    login,
                    SessionClientId::try_new(record.lifecycle().binding().client().as_str())
                        .map_err(invalid_contract_value)?,
                    credentials.clone(),
                    *policy,
                    context.clone(),
                ))?;
                let id = session.clone().into_value().session_id().value();
                (
                    StorageInteractionCompletion::Session(session),
                    InteractionIssuedResult::Session(id),
                )
            }
            (
                StorageInteractionEvidence::Login(evidence),
                StorageInteractionIssuance::Approval { claim, context },
            ) => {
                let user = self
                    .users
                    .get(&evidence.subject().principal_id().id())
                    .ok_or_else(invalid)?
                    .user
                    .clone();
                evidence.restore_login(user).map_err(|_| invalid())?;
                let approval =
                    self.create_credential_approval(StorageCredentialApprovalCreate::new(
                        claim.clone(),
                        evidence.authenticated_at(),
                        context.clone(),
                    ))?;
                let id = approval.clone().into_value().id();
                (
                    StorageInteractionCompletion::Approval(approval),
                    InteractionIssuedResult::Approval(id),
                )
            }
            (
                StorageInteractionEvidence::Verification(candidate),
                StorageInteractionIssuance::ProviderVerification,
            ) => {
                let id = candidate.id();
                self.runtime_settings.verify_candidate(*candidate)?;
                (
                    StorageInteractionCompletion::ProviderVerification(id),
                    InteractionIssuedResult::ProviderVerification(id),
                )
            }
            _ => {
                return Ok(StorageInteractionCompletion::Rejected(
                    InteractionTransitionError::Invalid,
                ));
            }
        };
        let completed = Utc::now();
        if !self.interaction_actor_valid(&record, completed) {
            return Err(invalid());
        }
        record.consume(issued, completed).map_err(|_| invalid())?;
        self.store_interaction(record)?;
        Ok(outcome)
    }

    pub(super) fn revoke_interaction(
        &mut self,
        access: StorageInteractionAccess,
        context: EventContext,
    ) -> Result<StorageMutationOutcome<usize>, StorageError> {
        let record = self
            .auth_interactions
            .get(&access.id().value())
            .ok_or_else(invalid)?;
        if !record.allows(&access) || Utc::now() >= record.retain_until() {
            return Err(invalid());
        }
        let Some(InteractionIssuedResult::Session(id)) = record.lifecycle().issued_result() else {
            return Err(StorageError::conflict(
                "Interaction has not issued a session",
            ));
        };
        self.revoke_sessions(SessionRevocation::Session(AuthSessionId::new(id)), context)
    }

    pub(super) fn purge_interactions(&mut self, limit: usize) -> Result<usize, StorageError> {
        if limit > 4096 {
            return Err(StorageError::invalid_input(
                "Interaction cleanup limit must be at most 4096",
            ));
        }
        let now = Utc::now();
        let keys: Vec<_> = self
            .auth_interactions
            .iter()
            .filter(|(_, record)| record.retain_until() <= now)
            .map(|(id, _)| *id)
            .take(limit)
            .collect();
        for id in &keys {
            self.auth_interactions.remove(id);
        }
        Ok(keys.len())
    }
}
