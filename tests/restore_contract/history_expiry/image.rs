//! Independent expectations from the full logical backup, before expiry admission.
use super::*;
use StorageBackupHistorySection as History;
use StorageBackupStateSection as State;

#[derive(Debug, Eq, PartialEq)]
pub(super) struct FamilyImage {
    current: Vec<StorageRecordIdentity>,
    open: Vec<StorageRecordIdentity>,
    closed: Vec<StorageRecordIdentity>,
}

impl FamilyImage {
    pub(super) fn read(backup: StorageBackupSnapshot, family: Family) -> Self {
        let (state_section, history_section) = match family {
            Family::Collection => (State::Collections, History::CollectionHistory),
            Family::Class => (State::Classes, History::ClassHistory),
            Family::Object => (State::Objects, History::ObjectHistory),
            Family::ClassRelation => (State::ClassRelations, History::ClassRelationHistory),
            Family::ObjectRelation => (State::ObjectRelations, History::ObjectRelationHistory),
            Family::ExportTemplate => (State::ExportTemplates, History::ExportTemplateHistory),
            Family::RemoteTarget => (State::RemoteTargets, History::RemoteTargetHistory),
            Family::ComputedDefinitions => (
                State::ComputedFieldDefinitions,
                History::ComputedFieldHistory,
            ),
            Family::ClassSchema => (State::ClassSchemaRevisions, History::ClassSchemaHistory),
        };
        let (state, history) = backup.into_parts();
        let history = history.expect("full history backup");
        let identity = |row: &StorageBackupRow| {
            let evidence: StorageIdentityEvidence = serde_json::from_value(
                row.get("logical_identity")
                    .expect("fixture identity")
                    .clone(),
            )
            .unwrap();
            evidence
                .subject()
                .and_then(StorageIdentitySubject::record_identity)
                .expect("original resource and version")
        };
        let mut image = Self {
            current: state[&state_section].iter().map(identity).collect(),
            open: Vec::new(),
            closed: Vec::new(),
        };
        for row in &history[&history_section] {
            let closed = family == Family::ClassSchema
                || row.get("valid_to").is_some_and(|value| !value.is_null())
                || row.get("operation").and_then(Value::as_str) == Some("delete");
            if closed {
                image.closed.push(identity(row));
            } else {
                image.open.push(identity(row));
            }
        }
        for rows in [&mut image.current, &mut image.open, &mut image.closed] {
            rows.sort_by_key(|id| (id.resource(), id.version()));
        }
        image
    }

    pub(super) fn newly_closed_since(&self, before: &Self) -> Vec<StorageRecordIdentity> {
        self.closed
            .iter()
            .copied()
            .filter(|id| !before.closed.contains(id))
            .collect()
    }

    pub(super) fn remove_expected(&mut self, expected: &[StorageRecordIdentity]) {
        let before = self.closed.len();
        self.closed.retain(|id| !expected.contains(id));
        assert_eq!(
            before - self.closed.len(),
            expected.len(),
            "each removal must correspond to one backed-up row"
        );
        assert!(
            !self.current.is_empty(),
            "exercise preservation of live state"
        );
    }
}
