use super::view::{BackupView, ReadError};
use crate::{
    MAX_QUERY_USAGE_DECLARATIONS, StorageBackupStateSection as Section,
    StorageQueryUsageDeclaration,
};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

pub(super) fn validate(snapshot: &impl BackupView) -> Result<(), ReadError> {
    let mut classes = BTreeSet::new();
    for row in snapshot.state_rows(Section::Classes)? {
        if let Some(id) = row?.get("id").and_then(Value::as_i64) {
            classes.insert(id);
        }
    }
    let mut ids = BTreeSet::new();
    let mut patterns = BTreeSet::new();
    let mut counts = BTreeMap::new();
    for row in snapshot.state_rows(Section::QueryUsageDeclarations)? {
        let row = row?;
        let record = StorageQueryUsageDeclaration::from_snapshot(row.into_owned().into_value())?;
        let class_id = record.class_id().id();
        let count = counts.entry(class_id).or_insert(0);
        *count += 1;
        let pattern_key = (
            class_id,
            record.pattern().path().canonical().to_string(),
            serde_json::to_string(&record.pattern().value_type()).expect("scalar enum"),
        );
        if !classes.contains(&i64::from(class_id))
            || !ids.insert(record.metadata().id().id())
            || !patterns.insert(pattern_key)
            || *count > MAX_QUERY_USAGE_DECLARATIONS
        {
            return Err(ReadError::invalid(
                "Backup query usage declarations violate class, identity, uniqueness or quota invariants",
            ));
        }
    }
    Ok(())
}
