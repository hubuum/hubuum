use super::*;

impl MemoryState {
    pub(super) fn issue_token(
        &mut self,
        mut request: StorageTokenCreate,
    ) -> Result<StorageMutationOutcome<StorageTokenMetadata>, StorageError> {
        let claim = request.take_credential_claim();
        let request = request.into_parts();
        if let Some(login) = request.login() {
            if login
                .authentication_revision()
                .is_some_and(|expected| expected != self.runtime_settings.authentication_revision())
            {
                return Err(StorageError::authentication_required(
                    "Authentication configuration changed; sign in again",
                ));
            }
            let matches = self
                .users
                .get(&request.principal_id().id())
                .is_some_and(|record| {
                    login.matches_user(&record.user)
                        && (!record.provider_managed || record.last_sync_success_at.is_some())
                });
            if !matches {
                return Err(StorageError::authentication_required(
                    "Authentication failed",
                ));
            }
        }
        let approval_audit = self.consume_credential_claim(
            claim.as_ref(),
            StorageCredentialOperation::CreateToken,
            Some(request.principal_id()),
            request.event_context(),
        )?;
        if !self.principals.contains_key(&request.principal_id().id()) {
            return Err(StorageError::not_found(format!(
                "Principal {} was not found",
                request.principal_id().id()
            )));
        }
        if self
            .service_accounts
            .get(&request.principal_id().id())
            .is_some_and(|account| account.disabled_at().is_some())
        {
            return Err(StorageError::conflict("Service account is disabled"));
        }
        if self
            .tokens
            .values()
            .any(|token| request.digest().matches_lookup_value(&token.token_hash))
        {
            return Err(StorageError::conflict("Token credential already exists"));
        }
        let id = TokenId::new(self.token_ids.allocate()?).expect("memory token id is positive");
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
        self.tokens.insert(id.id(), record);
        let receipt = self.append_simple_event(
            EntityType::Token,
            id.id(),
            None,
            Action::Created,
            request.event_context(),
            format!("Token {} created", id.id()),
        )?;
        let mut additional_audits = approval_audit.into_iter().collect::<Vec<_>>();
        if let Some(subject) = request.login_subject() {
            additional_audits.push(append_memory_event!(
                self,
                EntityType::Authentication,
                subject.principal_id().id(),
                Some(subject.name()),
                None,
                Action::Succeeded,
                request.event_context(),
                subject.successful_login_document(),
                None,
                None,
            )?);
        }
        Ok(StorageMutationOutcome::committed_with_audits(
            metadata,
            StorageAuditReceipts::new(receipt, additional_audits),
        ))
    }
}
