use hubuum_artifacts::{
    ArtifactError, ArtifactIdentity, ArtifactLimits, ArtifactResult, ArtifactStore,
    VerifiedArtifact, stage_producer,
};
use hubuum_storage_core::StorageErrorKind;

use crate::errors::ApiError;
use crate::models::{RestoreValidationSummary, TokenScope};
use crate::permissions::AppContext;
use crate::restores::replay::{ReplayDocument, capture};
use crate::storage::StorageBackupBudget;
use crate::traits::AuthzSubject;

/// An authorized, captured backup with immutable rows in the existing replay
/// spool. Staging consumes this exact snapshot; it never recaptures or publishes
/// a task result. The caller retains responsibility for live authorization,
/// ownership fences, reservations, and atomic task completion.
pub struct BackupArtifactProducer {
    document: ReplayDocument,
    budget: StorageBackupBudget,
    summary: RestoreValidationSummary,
}

impl BackupArtifactProducer {
    pub async fn capture(
        context: &AppContext,
        subject: &impl AuthzSubject,
        scopes: Option<&TokenScope>,
        include_history: bool,
        budget: StorageBackupBudget,
    ) -> Result<Self, ApiError> {
        super::authorize_backup_request(context, subject, scopes).await?;
        let document = capture(context, include_history, budget).await?;
        let summary = document.summary()?;
        Ok(Self {
            document,
            budget,
            summary,
        })
    }

    pub fn summary(&self) -> &RestoreValidationSummary {
        &self.summary
    }

    /// Serialize row-by-row with bounded backpressure, then independently verify
    /// durable chunks. The replay spool is dropped before this method returns.
    /// The returned proof is deliberately not a published output reference.
    pub async fn stage<S: ArtifactStore + ?Sized>(
        self,
        store: &S,
        identity: ArtifactIdentity,
        limits: ArtifactLimits,
    ) -> ArtifactResult<(VerifiedArtifact, RestoreValidationSummary)> {
        stage_producer(store, identity, limits, move |sink| {
            self.budget
                .serialize_into(&self.document, sink)
                .map_err(|error| match error.kind() {
                    StorageErrorKind::InputTooLarge => ArtifactError::Limit(error.to_string()),
                    _ => ArtifactError::Backend(error.to_string()),
                })?;
            Ok(self.summary)
        })
        .await
    }
}

#[cfg(test)]
mod tests;
