use super::*;

fn denied() -> StorageError {
    StorageError::reauthentication_required()
}
#[derive(Clone, Default)]
pub(super) struct CredentialApprovals {
    ids: IdSequence,
    by_digest: BTreeMap<String, i32>,
    records: BTreeMap<i32, (StorageCredentialClaim, StorageCredentialApprovalMetadata)>,
}

impl CredentialApprovals {
    pub(super) fn metadata(&self, id: i32) -> Option<&StorageCredentialApprovalMetadata> {
        self.records.get(&id).map(|(_, metadata)| metadata)
    }

    pub(super) fn consumed_for_restore(
        &self,
        id: RestoreJobId,
    ) -> Option<&StorageCredentialApprovalMetadata> {
        self.records
            .values()
            .map(|(_, metadata)| metadata)
            .filter(|metadata| {
                metadata.restore_job_id() == Some(id) && metadata.consumed_at().is_some()
            })
            .max_by_key(|metadata| metadata.consumed_at())
    }

    pub(super) fn invalidated_for_restore(&self, at: DateTime<Utc>) -> Self {
        let mut restored = self.clone();
        for (_, metadata) in restored.records.values_mut() {
            metadata.invalidate(at);
        }
        restored
    }

    fn create(
        &mut self,
        claim: StorageCredentialClaim,
        authenticated_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> Result<StorageCredentialApprovalMetadata, StorageError> {
        if self.by_digest.contains_key(claim.secret_digest().as_str()) {
            return Err(StorageError::conflict("Approval already exists"));
        }
        let id = self.ids.next()?;
        let metadata =
            StorageCredentialApprovalMetadata::new(id, &claim, authenticated_at, expires_at)
                .map_err(invalid_contract_value)?;
        self.by_digest
            .insert(claim.secret_digest().as_str().to_string(), id);
        self.records.insert(id, (claim, metadata.clone()));
        self.ids.observe(id);
        Ok(metadata)
    }

    fn consume(
        &mut self,
        claim: &StorageCredentialClaim,
    ) -> Result<StorageCredentialApprovalMetadata, StorageError> {
        let id = *self
            .by_digest
            .get(claim.secret_digest().as_str())
            .ok_or_else(denied)?;
        let (stored, metadata) = self.records.get_mut(&id).ok_or_else(denied)?;
        if stored.actor_id() != claim.actor_id()
            || stored.token_id() != claim.token_id()
            || stored.operation() != claim.operation()
            || stored.target_id() != claim.target_id()
            || stored.restore_job_id() != claim.restore_job_id()
            || stored.request_digest() != claim.request_digest()
        {
            return Err(denied());
        }
        metadata.consume(Utc::now()).map_err(|_| denied())?;
        Ok(metadata.clone())
    }
}

impl MemoryState {
    fn check_approval_actor(
        &self,
        claim: &StorageCredentialClaim,
    ) -> Result<Option<DateTime<Utc>>, StorageError> {
        let token = self.tokens.get(&claim.token_id().id()).ok_or_else(denied)?;
        let actor = self.users.get(&claim.actor_id().id()).ok_or_else(denied)?;
        let valid = token.principal_id == claim.actor_id()
            && token.scope.is_none()
            && token.revoked_at.is_none()
            && actor.user.clone().into_parts().anonymized_at().is_none()
            && token
                .expires_at
                .map_or(token.issued > claim.legacy_valid_after(), |expiry| {
                    expiry > Utc::now()
                });
        if !valid {
            return Err(denied());
        }
        Ok(token.expires_at)
    }
    pub(super) fn create_credential_approval(
        &mut self,
        request: StorageCredentialApprovalCreate,
    ) -> Result<StorageMutationOutcome<StorageCredentialApprovalMetadata>, StorageError> {
        let (claim, authenticated_at, context) = request.into_parts();
        if context.actor_user_id() != Some(claim.actor_id()) {
            return Err(denied());
        }
        let mut expires_at =
            authenticated_at + Duration::seconds(CREDENTIAL_APPROVAL_LIFETIME_SECONDS);
        if let Some(expiry) = self.check_approval_actor(&claim)? {
            expires_at = expires_at.min(expiry);
        }
        if authenticated_at > Utc::now() || expires_at <= Utc::now() {
            return Err(denied());
        }
        let metadata = self
            .credential_approvals
            .create(claim, authenticated_at, expires_at)?;
        let receipt = self.approval_audit(&metadata, Action::Created, &context)?;
        Ok(StorageMutationOutcome::committed(metadata, receipt))
    }
    pub(super) fn consume_credential_claim(
        &mut self,
        claim: Option<&StorageCredentialClaim>,
        expected: StorageCredentialOperation,
        target: Option<PrincipalId>,
        context: &EventContext,
    ) -> Result<Option<StorageAuditReceipt>, StorageError> {
        let Some(claim) = claim else {
            return Ok(None);
        };
        if claim.operation() != expected
            || claim.target_id() != target
            || context.actor_user_id() != Some(claim.actor_id())
        {
            return Err(denied());
        }
        self.check_approval_actor(claim)?;
        let metadata = self.credential_approvals.consume(claim)?;
        Ok(Some(self.approval_audit(
            &metadata,
            Action::Succeeded,
            context,
        )?))
    }
    fn approval_audit(
        &mut self,
        metadata: &StorageCredentialApprovalMetadata,
        action: Action,
        context: &EventContext,
    ) -> Result<StorageAuditReceipt, StorageError> {
        let document = AuditDocument::try_new(format!("Credential approval {} {}", metadata.id(), action.as_str()), None,
            Some(serde_json::json!(metadata)), serde_json::json!({"approval_id": metadata.id(), "operation": metadata.operation().as_str(), "target_id": metadata.target_id()}))
            .map_err(|error| StorageError::internal(error.to_string()))?;
        self.append_event_record(MemoryEventAppend {
            entity_type: EntityType::CredentialApproval,
            entity_id: metadata.id(),
            entity_name: None,
            collection_id: None,
            action,
            context,
            document,
            before_revision: None,
            after_revision: None,
        })
    }
}
pub(super) fn outcome<T>(
    value: T,
    audit: StorageAuditReceipt,
    approval: Option<StorageAuditReceipt>,
) -> StorageMutationOutcome<T> {
    StorageMutationOutcome::committed_with_audits(
        value,
        StorageAuditReceipts::new(audit, approval.into_iter().collect()),
    )
}
