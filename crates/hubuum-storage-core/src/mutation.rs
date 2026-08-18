use hubuum_events_core::{EventId, EventSequence};
use uuid::Uuid;

/// Durable proof returned by a backend for one committed audited mutation.
///
/// The receipt deliberately excludes event snapshots, metadata, summaries,
/// and actor details. Callers authorized to inspect those values use
/// [`crate::AuditEventStorage`] with the receipt's stable identifiers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuditReceipt {
    sequence: EventSequence,
    event_id: EventId,
    entity_type: String,
    action: String,
    before_revision: Option<i64>,
    after_revision: Option<i64>,
}

impl AuditReceipt {
    #[must_use]
    pub fn new(
        sequence: EventSequence,
        event_id: Uuid,
        entity_type: impl Into<String>,
        action: impl Into<String>,
        before_revision: Option<i64>,
        after_revision: Option<i64>,
    ) -> Self {
        Self {
            sequence,
            event_id: EventId::from(event_id),
            entity_type: entity_type.into(),
            action: action.into(),
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
    pub fn entity_type(&self) -> &str {
        &self.entity_type
    }

    #[must_use]
    pub fn action(&self) -> &str {
        &self.action
    }

    #[must_use]
    pub const fn before_revision(&self) -> Option<i64> {
        self.before_revision
    }

    #[must_use]
    pub const fn after_revision(&self) -> Option<i64> {
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
    Committed { value: T, audit: AuditReceipt },
}

impl<T> MutationOutcome<T> {
    pub const fn unchanged(value: T) -> Self {
        Self::Unchanged(value)
    }

    pub const fn committed(value: T, audit: AuditReceipt) -> Self {
        Self::Committed { value, audit }
    }

    #[must_use]
    pub const fn is_committed(&self) -> bool {
        matches!(self, Self::Committed { .. })
    }

    #[must_use]
    pub const fn audit(&self) -> Option<&AuditReceipt> {
        match self {
            Self::Unchanged(_) => None,
            Self::Committed { audit, .. } => Some(audit),
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
            Self::Committed { value, audit } => MutationOutcome::Committed {
                value: map(value),
                audit,
            },
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (T, Option<AuditReceipt>) {
        match self {
            Self::Unchanged(value) => (value, None),
            Self::Committed { value, audit } => (value, Some(audit)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn receipt() -> AuditReceipt {
        AuditReceipt::new(
            EventSequence::new(7).unwrap(),
            Uuid::nil(),
            "collection",
            "updated",
            Some(2),
            Some(3),
        )
    }

    #[test]
    fn committed_outcome_preserves_value_and_receipt() {
        let outcome = MutationOutcome::committed(41, receipt());

        assert_eq!(outcome.value(), &41);
        assert_eq!(outcome.audit(), Some(&receipt()));
        assert!(outcome.is_committed());
    }

    #[test]
    fn unchanged_outcome_has_no_receipt() {
        let outcome = MutationOutcome::unchanged(41);

        assert_eq!(outcome.into_parts(), (41, None));
    }
}
