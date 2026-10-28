//! Identity assignment for observations synthesized at a confirmed restore boundary.

use std::collections::BTreeMap;

use hubuum_domain::{IdentityReference, LegacyIdentityCohort, VersionIdentity};
use serde_json::{Value, json};

use super::identities::derived_uuid;
use super::{
    StorageBackupHistorySection as History, StorageBackupReplay, StorageBackupRow,
    StorageBackupStateSection as State,
};
use crate::{
    StorageCapturedIdentity as Captured, StorageError, StorageIdentityEvidence as Evidence,
    StorageIdentityResourceKind as Family, StorageIdentitySubject as Subject,
};

fn invalid(message: impl Into<String>) -> StorageError {
    StorageError::invalid_input(message)
}

fn evidence(row: &StorageBackupRow) -> Result<Evidence, StorageError> {
    serde_json::from_value(
        row.get("logical_identity")
            .cloned()
            .ok_or_else(|| invalid("Synthesized baseline source has no identity"))?,
    )
    .map_err(|error| invalid(error.to_string()))
}

pub(super) fn attach(row: &mut StorageBackupRow, identity: Evidence) -> Result<(), StorageError> {
    row.0.insert(
        "logical_identity".into(),
        serde_json::to_value(identity).map_err(|error| invalid(error.to_string()))?,
    );
    Ok(())
}

pub(super) struct BaselineIdentities {
    cohort: LegacyIdentityCohort,
    classes: BTreeMap<i64, Evidence>,
    definitions: BTreeMap<i64, Evidence>,
}

impl BaselineIdentities {
    pub(super) fn new(
        source: &StorageBackupReplay,
        cohort: LegacyIdentityCohort,
    ) -> Result<Self, StorageError> {
        let index = |section| {
            source
                .state_rows(section)
                .map(|row| {
                    let row = row?;
                    let id = row
                        .get("id")
                        .and_then(Value::as_i64)
                        .ok_or_else(|| invalid("Baseline source has no id"))?;
                    Ok((id, evidence(&row)?))
                })
                .collect::<Result<BTreeMap<_, _>, StorageError>>()
        };
        Ok(Self {
            cohort,
            classes: index(State::Classes)?,
            definitions: index(State::ComputedFieldDefinitions)?,
        })
    }

    fn version(&self, namespace: &str, key: Value) -> VersionIdentity {
        VersionIdentity::from_bytes(derived_uuid(self.cohort, namespace, &key))
            .expect("derived RFC UUID")
    }

    pub(super) fn resource(&self, row: &mut StorageBackupRow) -> Result<(), StorageError> {
        let source = evidence(row)?;
        let Some(Subject::Resource { identity, version }) = source.subject() else {
            return Err(invalid("Resource baseline has no resource identity"));
        };
        let subject = Subject::Resource {
            identity,
            version: self.version("restore-resource-baseline", json!(version)),
        };
        attach(
            row,
            Evidence::try_new(Some(subject), source.references().clone())
                .map_err(|error| error.into_request_error())?,
        )
    }

    pub(super) fn history(
        &self,
        section: History,
        row: &mut StorageBackupRow,
    ) -> Result<(), StorageError> {
        match section {
            History::ComputedFieldHistory => self.computed(row),
            History::ClassSchemaHistory => self.schema(row),
            _ => self.resource(row),
        }
    }

    fn computed(&self, row: &mut StorageBackupRow) -> Result<(), StorageError> {
        let class_id = row
            .get("class_id")
            .and_then(Value::as_i64)
            .ok_or_else(|| invalid("Computed baseline has no class"))?;
        let source = self
            .classes
            .get(&class_id)
            .ok_or_else(|| invalid("Computed baseline class is missing"))?;
        let Some(Subject::Resource { identity, .. }) = source.subject() else {
            return Err(invalid("Computed baseline class has no identity"));
        };
        let mut references = BTreeMap::from([(
            "/class_id".into(),
            Captured::Resource {
                family: Family::Class,
                reference: identity,
            },
        )]);
        let definitions = row
            .get("definitions")
            .and_then(Value::as_array)
            .ok_or_else(|| invalid("Computed baseline definitions are missing"))?;
        for (index, definition) in definitions.iter().enumerate() {
            let id = definition
                .get("id")
                .and_then(Value::as_i64)
                .ok_or_else(|| invalid("Computed baseline definition has no id"))?;
            let source = self
                .definitions
                .get(&id)
                .ok_or_else(|| invalid("Computed baseline definition is missing"))?;
            let Some(Subject::Resource {
                identity: definition_identity,
                version,
            }) = source.subject()
            else {
                return Err(invalid("Computed definition has no identity"));
            };
            references.insert(
                format!("/definitions/{index}/id"),
                Captured::Resource {
                    family: Family::ComputedDefinition,
                    reference: definition_identity,
                },
            );
            references.insert(
                format!("/definitions/{index}/revision"),
                Captured::Version {
                    reference: IdentityReference::Known { identity: version },
                },
            );
            let class = source
                .references()
                .get("/class_id")
                .copied()
                .ok_or_else(|| invalid("Computed definition has no captured owner"))?;
            references.insert(format!("/definitions/{index}/class_id"), class);
        }
        let version = self.version(
            "restore-computed-baseline",
            json!([identity, row.get("history_entry_id")]),
        );
        attach(
            row,
            Evidence::try_new(Some(Subject::Resource { identity, version }), references)
                .map_err(|error| error.into_request_error())?,
        )
    }

    fn schema(&self, row: &mut StorageBackupRow) -> Result<(), StorageError> {
        let snapshot = StorageBackupRow::try_from_value(
            row.get("snapshot")
                .cloned()
                .ok_or_else(|| invalid("Schema baseline has no snapshot"))?,
        )
        .map_err(|error| error.into_request_error())?;
        let source = evidence(&snapshot)?;
        let Some(Subject::Resource { identity, version }) = source.subject() else {
            return Err(invalid("Schema baseline has no class identity"));
        };
        let mut references = BTreeMap::from([
            (
                "/class_id".into(),
                Captured::Resource {
                    family: Family::Class,
                    reference: identity,
                },
            ),
            (
                "/snapshot/revision".into(),
                Captured::Version {
                    reference: IdentityReference::Known { identity: version },
                },
            ),
        ]);
        references.extend(
            source
                .references()
                .iter()
                .map(|(path, captured)| (format!("/snapshot{path}"), *captured)),
        );
        let version = self.version("restore-schema-history-baseline", json!(version));
        attach(
            row,
            Evidence::try_new(Some(Subject::Resource { identity, version }), references)
                .map_err(|error| error.into_request_error())?,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{StorageBackupBudget, StorageBackupSnapshot, StorageIdentityUpgrade};
    use rstest::rstest;

    fn source() -> crate::StorageIdentityReplay {
        let mut state = State::ALL
            .iter()
            .copied()
            .map(|section| (section, Vec::new()))
            .collect::<BTreeMap<_, _>>();
        state.get_mut(&State::Collections).unwrap().push(StorageBackupRow::try_from_value(json!({
            "id": 1, "revision": 1, "created_at": "2026-01-01T00:00:00Z", "updated_at": "2026-01-01T00:00:00Z",
            "name": "root", "description": "root", "parent_collection_id": null,
        })).unwrap());
        state
            .get_mut(&State::CollectionAuthorization)
            .unwrap()
            .push(
                StorageBackupRow::try_from_value(json!({"collection_id": 1, "revision": 1}))
                    .unwrap(),
            );
        StorageBackupReplay::from_snapshot(
            StorageBackupSnapshot::try_new(state, None).unwrap(),
            StorageBackupBudget::new(1_000_000, 100).unwrap(),
        )
        .unwrap()
        .upgrade_logical_identities(StorageIdentityUpgrade::Legacy(
            LegacyIdentityCohort::generate(),
        ))
        .unwrap()
    }

    #[test]
    fn synthesized_baseline_preserves_incarnation_and_changes_version() {
        let source = source();
        let before = evidence(
            &source
                .replay()
                .state_rows(State::Collections)
                .next()
                .unwrap()
                .unwrap(),
        )
        .unwrap()
        .subject()
        .unwrap()
        .record_identity()
        .unwrap();
        let prepared = source
            .replay()
            .identity_restore_boundary(LegacyIdentityCohort::generate(), chrono::Utc::now())
            .unwrap();
        let current = evidence(
            &prepared
                .replay()
                .state_rows(State::Collections)
                .next()
                .unwrap()
                .unwrap(),
        )
        .unwrap()
        .subject()
        .unwrap()
        .record_identity()
        .unwrap();
        let history = evidence(
            &prepared
                .replay()
                .history_rows(History::CollectionHistory)
                .next()
                .unwrap()
                .unwrap(),
        )
        .unwrap()
        .subject()
        .unwrap()
        .record_identity()
        .unwrap();
        assert_eq!(current, history);
        assert_eq!(current.resource(), before.resource());
        assert_ne!(current.version(), before.version());
    }

    #[rstest]
    #[case::retry(true)]
    #[case::new_restore(false)]
    fn baseline_version_is_stable_only_within_one_confirmed_stage(#[case] retry: bool) {
        let source = source();
        let first = LegacyIdentityCohort::generate();
        let second = if retry {
            first
        } else {
            LegacyIdentityCohort::generate()
        };
        let at = chrono::Utc::now();
        let identity = |cohort| {
            let prepared = source
                .replay()
                .identity_restore_boundary(cohort, at)
                .unwrap();
            evidence(
                &prepared
                    .replay()
                    .history_rows(History::CollectionHistory)
                    .next()
                    .unwrap()
                    .unwrap(),
            )
            .unwrap()
        };
        assert_eq!(identity(first) == identity(second), retry);
    }
}
