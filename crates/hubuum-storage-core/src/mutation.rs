use hubuum_domain::ResourceRevision;
use hubuum_events_core::{Action, EntityType, EventId, EventSequence};

use crate::StorageError;

/// Durable proof returned by a backend for one committed audited mutation.
///
/// The receipt deliberately excludes event snapshots, metadata, summaries,
/// and actor details. Callers authorized to inspect those values use
/// [`crate::AuditEventStorage`] with the receipt's stable identifiers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuditReceipt {
    sequence: EventSequence,
    event_id: EventId,
    entity_type: EntityType,
    action: Action,
    before_revision: Option<ResourceRevision>,
    after_revision: Option<ResourceRevision>,
}

/// One or more audit receipts produced by a single atomic mutation.
///
/// The first receipt is stored separately so an instance can never represent
/// an empty committed audit set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuditReceipts {
    first: AuditReceipt,
    additional: Vec<AuditReceipt>,
}

impl AuditReceipts {
    #[must_use]
    pub const fn single(first: AuditReceipt) -> Self {
        Self {
            first,
            additional: Vec::new(),
        }
    }

    #[must_use]
    pub const fn new(first: AuditReceipt, additional: Vec<AuditReceipt>) -> Self {
        Self { first, additional }
    }

    /// Build a non-empty receipt set without permitting an invalid committed
    /// mutation to cross the storage boundary.
    pub fn try_from_vec(receipts: Vec<AuditReceipt>) -> Result<Self, StorageError> {
        let mut receipts = receipts.into_iter();
        let first = receipts.next().ok_or_else(|| {
            StorageError::internal("committed audited mutation returned no audit receipts")
        })?;
        Ok(Self::new(first, receipts.collect()))
    }

    #[must_use]
    pub const fn first(&self) -> &AuditReceipt {
        &self.first
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        1 + self.additional.len()
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        false
    }

    pub fn iter(&self) -> impl Iterator<Item = &AuditReceipt> {
        std::iter::once(&self.first).chain(self.additional.iter())
    }

    #[must_use]
    pub fn into_vec(self) -> Vec<AuditReceipt> {
        let mut receipts = Vec::with_capacity(self.len());
        receipts.push(self.first);
        receipts.extend(self.additional);
        receipts
    }
}

impl AuditReceipt {
    #[must_use]
    pub fn new(
        sequence: EventSequence,
        event_id: EventId,
        entity_type: EntityType,
        action: Action,
        before_revision: Option<ResourceRevision>,
        after_revision: Option<ResourceRevision>,
    ) -> Self {
        Self {
            sequence,
            event_id,
            entity_type,
            action,
            before_revision,
            after_revision,
        }
    }

    #[must_use]
    pub const fn sequence(&self) -> EventSequence {
        self.sequence
    }

    #[must_use]
    pub const fn event_id(&self) -> EventId {
        self.event_id
    }

    #[must_use]
    pub const fn entity_type(&self) -> EntityType {
        self.entity_type
    }

    #[must_use]
    pub const fn action(&self) -> Action {
        self.action
    }

    #[must_use]
    pub const fn before_revision(&self) -> Option<ResourceRevision> {
        self.before_revision
    }

    #[must_use]
    pub const fn after_revision(&self) -> Option<ResourceRevision> {
        self.after_revision
    }
}

/// Explicit result of an ordinary audited mutation.
///
/// `Committed` proves that the backend returned a durable audit receipt from
/// the same atomic operation as the state change. `Unchanged` is reserved for
/// a genuine no-op and therefore carries no audit receipt.
#[must_use]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MutationOutcome<T> {
    Unchanged(T),
    Committed { value: T, audits: AuditReceipts },
}

impl<T> MutationOutcome<T> {
    pub const fn unchanged(value: T) -> Self {
        Self::Unchanged(value)
    }

    pub const fn committed(value: T, audit: AuditReceipt) -> Self {
        Self::Committed {
            value,
            audits: AuditReceipts::single(audit),
        }
    }

    pub const fn committed_with_audits(value: T, audits: AuditReceipts) -> Self {
        Self::Committed { value, audits }
    }

    #[must_use]
    pub const fn is_committed(&self) -> bool {
        matches!(self, Self::Committed { .. })
    }

    #[must_use]
    pub const fn audits(&self) -> Option<&AuditReceipts> {
        match self {
            Self::Unchanged(_) => None,
            Self::Committed { audits, .. } => Some(audits),
        }
    }

    #[must_use]
    pub fn value(&self) -> &T {
        match self {
            Self::Unchanged(value) | Self::Committed { value, .. } => value,
        }
    }

    pub fn into_value(self) -> T {
        match self {
            Self::Unchanged(value) | Self::Committed { value, .. } => value,
        }
    }

    pub fn map<U>(self, map: impl FnOnce(T) -> U) -> MutationOutcome<U> {
        match self {
            Self::Unchanged(value) => MutationOutcome::Unchanged(map(value)),
            Self::Committed { value, audits } => MutationOutcome::Committed {
                value: map(value),
                audits,
            },
        }
    }

    /// Transform the mutation value while preserving its audit proof.
    ///
    /// This is the fallible counterpart to [`Self::map`] and is useful at a
    /// backend boundary where persisted primitives are validated into domain
    /// types before they leave the adapter.
    pub fn try_map<U, E>(
        self,
        map: impl FnOnce(T) -> Result<U, E>,
    ) -> Result<MutationOutcome<U>, E> {
        match self {
            Self::Unchanged(value) => map(value).map(MutationOutcome::Unchanged),
            Self::Committed { value, audits } => {
                map(value).map(|value| MutationOutcome::Committed { value, audits })
            }
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (T, Option<AuditReceipts>) {
        match self {
            Self::Unchanged(value) => (value, None),
            Self::Committed { value, audits } => (value, Some(audits)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn receipt() -> AuditReceipt {
        AuditReceipt::new(
            EventSequence::new(7).unwrap(),
            EventId::from(uuid::Uuid::nil()),
            EntityType::Collection,
            Action::Updated,
            Some(ResourceRevision::new(2).unwrap()),
            Some(ResourceRevision::new(3).unwrap()),
        )
    }

    #[test]
    fn committed_outcome_preserves_value_and_receipt() {
        let outcome = MutationOutcome::committed(41, receipt());

        assert_eq!(outcome.value(), &41);
        assert_eq!(outcome.audits().map(AuditReceipts::first), Some(&receipt()));
        assert_eq!(outcome.audits().map(AuditReceipts::len), Some(1));
        assert!(outcome.is_committed());
    }

    #[test]
    fn unchanged_outcome_has_no_receipt() {
        let outcome = MutationOutcome::unchanged(41);

        assert_eq!(outcome.into_parts(), (41, None));
    }

    #[test]
    fn audit_receipts_are_non_empty() {
        let receipts = AuditReceipts::new(receipt(), vec![receipt()]);

        assert!(!receipts.is_empty());
        assert_eq!(receipts.len(), 2);
        assert_eq!(receipts.iter().count(), 2);
    }

    #[test]
    fn empty_audit_receipt_vectors_are_rejected() {
        let error = AuditReceipts::try_from_vec(Vec::new()).unwrap_err();

        assert_eq!(error.kind(), crate::StorageErrorKind::Internal);
    }
}
