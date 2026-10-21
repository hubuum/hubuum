use super::*;

#[derive(Clone)]
pub(super) struct MemorySession {
    login: StorageLogin,
    client: SessionClientId,
    policy: SessionPolicy,
    pub(super) absolute: DateTime<Utc>,
    pub(super) idle: DateTime<Utc>,
    retain_until: DateTime<Utc>,
    pub(super) revoked: bool,
    access_ids: Vec<TokenId>,
    pub(super) refresh: BTreeMap<String, (StorageTokenDigest, DateTime<Utc>, bool)>,
}
impl MemorySession {
    pub(super) fn active(&self, now: DateTime<Utc>) -> bool {
        !self.revoked && now < self.idle && now < self.absolute
    }
    pub(super) fn principal_id(&self) -> PrincipalId {
        self.login.subject().principal_id()
    }
}

fn denied() -> StorageError {
    StorageError::authentication_required("Authentication failed")
}
impl MemoryState {
    pub(super) fn invalidate_principal_sessions(&mut self, principal: i32) {
        for session in self.sessions.values_mut() {
            if session.principal_id().id() == principal {
                session.revoked = true;
            }
        }
    }

    pub(super) fn session_for_token(&self, token: TokenId) -> Option<Uuid> {
        self.sessions
            .iter()
            .find(|(_, s)| s.access_ids.contains(&token))
            .map(|(id, _)| *id)
    }

    fn session_issue(
        &mut self,
        id: Uuid,
        session: &mut MemorySession,
        credentials: &SessionCredentials,
        context: &EventContext,
        login: bool,
    ) -> Result<(StorageSessionIssued, StorageAuditReceipts), StorageError> {
        let now = Utc::now();
        let (access_expiry, refresh_expiry) = session
            .policy
            .deadlines(now, session.absolute)
            .map_err(invalid_contract_value)?;
        if self
            .sessions
            .values()
            .any(|s| s.refresh.contains_key(credentials.refresh().lookup_value()))
            || session
                .refresh
                .contains_key(credentials.refresh().lookup_value())
        {
            return Err(StorageError::conflict("Refresh credential already exists"));
        }
        let policy = StorageTokenIssuancePolicy::try_new(1, 8784)
            .map_err(|e| StorageError::invalid_input(e.to_string()))?;
        let request = if login {
            StorageTokenCreate::for_login(
                session.login.clone(),
                credentials.access().clone(),
                policy,
                context,
            )
        } else {
            StorageTokenCreate::new(
                session.principal_id(),
                credentials.access().clone(),
                policy,
                context.clone(),
            )
        }
        .expires_at(Some(access_expiry));
        let (token, audits) = match super::identity::create_token_in_state(self, request)? {
            StorageMutationOutcome::Committed { value, audits } => (value, audits),
            StorageMutationOutcome::Unchanged(_) => {
                return Err(StorageError::internal("session issuance did not commit"));
            }
        };
        session.access_ids.push(token.id());
        session.refresh.insert(
            credentials.refresh().lookup_value().to_owned(),
            (credentials.refresh().clone(), refresh_expiry, false),
        );
        session.idle = refresh_expiry;
        Ok((
            StorageSessionIssued::new(
                AuthSessionId::new(id),
                session.principal_id(),
                token.id(),
                now,
                access_expiry,
                refresh_expiry,
                session.absolute,
            ),
            audits,
        ))
    }
    pub(super) fn create_session(
        &mut self,
        request: StorageSessionCreate,
    ) -> Result<StorageMutationOutcome<StorageSessionIssued>, StorageError> {
        let now = Utc::now();
        let absolute = now + Duration::seconds(request.policy().absolute_seconds());
        let id = Uuid::new_v4();
        let mut session = MemorySession {
            login: request.login().clone(),
            client: request.client().clone(),
            policy: request.policy(),
            absolute,
            idle: now + Duration::seconds(request.policy().idle_seconds()),
            retain_until: absolute + Duration::seconds(request.policy().retention_seconds()),
            revoked: false,
            access_ids: Vec::new(),
            refresh: BTreeMap::new(),
        };
        let (issued, audits) = self.session_issue(
            id,
            &mut session,
            request.credentials(),
            request.context(),
            true,
        )?;
        self.sessions.insert(id, session);
        Ok(StorageMutationOutcome::committed_with_audits(
            issued, audits,
        ))
    }
    pub(super) fn refresh_owner(
        &self,
        credential: &RefreshCredential,
    ) -> Result<PrincipalId, StorageError> {
        self.sessions
            .values()
            .find(|s| {
                s.refresh
                    .get(credential.digest().lookup_value())
                    .is_some_and(|(digest, _, _)| digest == credential.digest())
            })
            .map(MemorySession::principal_id)
            .ok_or_else(denied)
    }
    fn revoke_family(&mut self, session: &mut MemorySession) -> Result<(), StorageError> {
        session.revoked = true;
        let now = Utc::now();
        for id in &session.access_ids {
            if let Some(token) = self.tokens.get_mut(&id.id())
                && token.revoked_at.is_none()
            {
                token.revision = token
                    .revision
                    .checked_advance()
                    .map_err(|error| StorageError::internal(error.to_string()))?;
                token.revoked_at = Some(now);
            }
        }
        Ok(())
    }
    pub(super) fn rotate_session(
        &mut self,
        request: StorageSessionRotate,
    ) -> Result<StorageMutationOutcome<StorageSessionRotation>, StorageError> {
        let key = request.presented().digest().lookup_value();
        let Some(id) = self
            .sessions
            .iter()
            .find(|(_, s)| s.refresh.contains_key(key))
            .map(|(id, _)| *id)
        else {
            return Ok(StorageMutationOutcome::unchanged(
                StorageSessionRotation::Rejected,
            ));
        };
        let mut session = self.sessions.get(&id).cloned().ok_or_else(denied)?;
        let (digest, expires, consumed) = session.refresh.get(key).cloned().ok_or_else(denied)?;
        if &digest != request.presented().digest() || &session.client != request.client() {
            return Ok(StorageMutationOutcome::unchanged(
                StorageSessionRotation::Rejected,
            ));
        }
        if consumed {
            self.revoke_family(&mut session)?;
            let audit = self.append_simple_event(
                EntityType::Authentication,
                session.principal_id().id(),
                None,
                Action::Revoked,
                request.context(),
                "Interactive session refresh replay",
            )?;
            self.sessions.insert(id, session);
            return Ok(StorageMutationOutcome::committed(
                StorageSessionRotation::Replayed,
                audit,
            ));
        }
        let now = Utc::now();
        let valid_user = self
            .users
            .get(&session.principal_id().id())
            .is_some_and(|u| {
                u.user.clone().into_parts().anonymized_at().is_none()
                    && (!u.provider_managed || u.last_sync_success_at.is_some())
            });
        if !valid_user || !session.active(now) || now >= expires {
            return Ok(StorageMutationOutcome::unchanged(
                StorageSessionRotation::Rejected,
            ));
        }
        session.refresh.get_mut(key).ok_or_else(denied)?.2 = true;
        let (issued, issuance_audits) = self.session_issue(
            id,
            &mut session,
            request.credentials(),
            request.context(),
            false,
        )?;
        let audit = self.append_simple_event(
            EntityType::Authentication,
            issued.access_id().id(),
            None,
            Action::Updated,
            request.context(),
            "Interactive session refreshed",
        )?;
        self.sessions.insert(id, session);
        let mut audits = issuance_audits.into_vec();
        audits.push(audit);
        Ok(StorageMutationOutcome::committed_with_audits(
            StorageSessionRotation::Issued(issued),
            StorageAuditReceipts::try_from_vec(audits).map_err(invalid_contract_value)?,
        ))
    }
    pub(super) fn revoke_sessions(
        &mut self,
        target: SessionRevocation,
        context: EventContext,
    ) -> Result<StorageMutationOutcome<usize>, StorageError> {
        let ids: Vec<_> = self
            .sessions
            .iter()
            .filter(|(_, s)| {
                !s.revoked
                    && match target {
                        SessionRevocation::All(id) => s.principal_id() == id,
                        SessionRevocation::Current(id) => s.access_ids.contains(&id),
                    }
            })
            .map(|(id, _)| *id)
            .collect();
        let mut audits = Vec::new();
        for id in &ids {
            let mut session = self.sessions.get(id).cloned().ok_or_else(denied)?;
            self.revoke_family(&mut session)?;
            audits.push(self.append_simple_event(
                EntityType::Authentication,
                session.principal_id().id(),
                None,
                Action::Revoked,
                &context,
                "Interactive session logout",
            )?);
            self.sessions.insert(*id, session);
        }
        if audits.is_empty() {
            return Ok(StorageMutationOutcome::unchanged(0));
        }
        Ok(StorageMutationOutcome::committed_with_audits(
            ids.len(),
            StorageAuditReceipts::try_from_vec(audits).map_err(invalid_contract_value)?,
        ))
    }
    pub(super) fn session_allows_token(&self, id: TokenId, now: DateTime<Utc>) -> bool {
        self.sessions
            .values()
            .find(|s| s.access_ids.contains(&id))
            .is_none_or(|s| s.active(now))
    }
    pub(super) fn purge_sessions(&mut self, limit: usize) -> usize {
        let now = Utc::now();
        let ids: Vec<_> = self
            .sessions
            .iter()
            .filter(|(_, s)| {
                s.retain_until < now
                    && s.access_ids
                        .iter()
                        .all(|id| !self.tokens.contains_key(&id.id()))
            })
            .take(limit)
            .map(|(id, _)| *id)
            .collect();
        for id in &ids {
            if let Some(session) = self.sessions.remove(id) {
                for token in session.access_ids {
                    self.tokens.remove(&token.id());
                }
            }
        }
        ids.len()
    }
}
