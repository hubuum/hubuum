use std::collections::{BTreeMap, HashSet};

use chrono::{DateTime, NaiveDateTime, SecondsFormat, Utc};
use diesel::QueryableByName;
use diesel::sql_types::Jsonb;
use diesel_async::RunQueryDsl;
use hubuum_events_core::CorrelationId;
use hubuum_storage_core::{
    StorageBackupHistorySection, StorageBackupHistorySections, StorageBackupRow,
    StorageBackupSnapshot, StorageBackupStateSection, StorageBackupStateSections,
};
use serde_json::{Map, Value};

use crate::{PostgresConnection, PostgresRuntime, PostgresStorageError};

#[derive(QueryableByName)]
struct JsonRows {
    #[diesel(sql_type = Jsonb)]
    rows: Value,
}

pub(crate) const fn state_table(section: StorageBackupStateSection) -> &'static str {
    match section {
        StorageBackupStateSection::IdentityScopes => "identity_scopes",
        StorageBackupStateSection::Groups => "groups",
        StorageBackupStateSection::Principals => "principals",
        StorageBackupStateSection::Users => "users",
        StorageBackupStateSection::ServiceAccounts => "service_accounts",
        StorageBackupStateSection::GroupMemberships => "group_memberships",
        StorageBackupStateSection::GroupMembershipSources => "group_membership_sources",
        StorageBackupStateSection::Collections => "collections",
        StorageBackupStateSection::CollectionAuthorization => "collection_authorization_state",
        StorageBackupStateSection::CollectionHierarchy => "collection_closure",
        StorageBackupStateSection::CollectionPermissionGrants => "permissions",
        StorageBackupStateSection::Classes => "hubuumclass",
        StorageBackupStateSection::ComputedFieldDefinitions => "computed_field_definitions",
        StorageBackupStateSection::ClassRelations => "hubuumclass_relation",
        StorageBackupStateSection::Objects => "hubuumobject",
        StorageBackupStateSection::ObjectRelations => "hubuumobject_relation",
        StorageBackupStateSection::ExportTemplates => "export_templates",
        StorageBackupStateSection::RemoteTargets => "remote_targets",
        StorageBackupStateSection::EventSinks => "event_sinks",
        StorageBackupStateSection::EventSubscriptions => "event_subscriptions",
    }
}

pub(crate) const fn history_table(section: StorageBackupHistorySection) -> &'static str {
    match section {
        StorageBackupHistorySection::CollectionHistory => "collections_history",
        StorageBackupHistorySection::ClassHistory => "hubuumclass_history",
        StorageBackupHistorySection::ClassRelationHistory => "hubuumclass_relation_history",
        StorageBackupHistorySection::ObjectHistory => "hubuumobject_history",
        StorageBackupHistorySection::ObjectRelationHistory => "hubuumobject_relation_history",
        StorageBackupHistorySection::ExportTemplateHistory => "export_templates_history",
        StorageBackupHistorySection::RemoteTargetHistory => "remote_targets_history",
        StorageBackupHistorySection::TerminalTasks => "tasks",
        StorageBackupHistorySection::ImportResults => "import_task_results",
        StorageBackupHistorySection::ExportOutputs => "export_task_outputs",
        StorageBackupHistorySection::RemoteCallResults => "remote_call_results",
        StorageBackupHistorySection::AuditEvents => "events",
        StorageBackupHistorySection::TerminalEventDeliveries => "event_deliveries",
    }
}

const HISTORY_FIELD_MAPPINGS: &[(&str, &str)] = &[
    ("op", "operation"),
    ("history_id", "history_entry_id"),
    ("actor_id", "actor_principal_id"),
    ("initiator_user_id", "initiator_principal_id"),
];

const CLASS_RELATION_FIELD_MAPPINGS: &[(&str, &str)] = &[
    ("from_hubuum_class_id", "from_class_id"),
    ("to_hubuum_class_id", "to_class_id"),
];

const OBJECT_FIELD_MAPPINGS: &[(&str, &str)] = &[("hubuum_class_id", "class_id")];

const OBJECT_RELATION_FIELD_MAPPINGS: &[(&str, &str)] = &[
    ("from_hubuum_object_id", "from_object_id"),
    ("to_hubuum_object_id", "to_object_id"),
];

const PERMISSION_FIELDS: &[&str] = &[
    "read_collection",
    "update_collection",
    "delete_collection",
    "delegate_collection",
    "create_class",
    "read_class",
    "update_class",
    "delete_class",
    "create_object",
    "read_object",
    "update_object",
    "delete_object",
    "create_class_relation",
    "read_class_relation",
    "update_class_relation",
    "delete_class_relation",
    "create_object_relation",
    "read_object_relation",
    "update_object_relation",
    "delete_object_relation",
    "read_template",
    "create_template",
    "update_template",
    "delete_template",
    "read_remote_target",
    "create_remote_target",
    "update_remote_target",
    "delete_remote_target",
    "execute_remote_target",
    "read_audit",
    "manage_event_subscription",
];

fn rename_fields(
    row: &mut Map<String, Value>,
    mappings: &[(&str, &str)],
    to_logical: bool,
) -> Result<(), PostgresStorageError> {
    for &(physical, logical) in mappings {
        let (source, target) = if to_logical {
            (physical, logical)
        } else {
            (logical, physical)
        };
        if let Some(value) = row.remove(source)
            && row.insert(target.to_string(), value).is_some()
        {
            let message = format!("Backup row contains both '{source}' and '{target}'");
            return Err(if to_logical {
                PostgresStorageError::database(message)
            } else {
                PostgresStorageError::invalid_input(message)
            });
        }
    }
    Ok(())
}

fn is_timestamp_field(field: &str) -> bool {
    field.ends_with("_at") || matches!(field, "issued" | "valid_from" | "valid_to")
}

fn logicalize_timestamps(row: &mut Map<String, Value>) -> Result<(), PostgresStorageError> {
    for (field, value) in row {
        if !is_timestamp_field(field) || value.is_null() {
            continue;
        }
        let timestamp = value.as_str().ok_or_else(|| {
            PostgresStorageError::database(format!(
                "Persisted timestamp field '{field}' is not a string"
            ))
        })?;
        let utc = DateTime::parse_from_rfc3339(timestamp)
            .map(|value| value.with_timezone(&Utc))
            .or_else(|_| {
                NaiveDateTime::parse_from_str(timestamp, "%Y-%m-%dT%H:%M:%S%.f")
                    .map(|value| value.and_utc())
            })
            .map_err(|error| {
                PostgresStorageError::database(format!(
                    "Persisted timestamp field '{field}' is invalid: {error}"
                ))
            })?;
        *value = Value::String(utc.to_rfc3339_opts(SecondsFormat::AutoSi, true));
    }
    Ok(())
}

fn physicalize_timestamps(row: &mut Map<String, Value>) -> Result<(), PostgresStorageError> {
    for (field, value) in row {
        if !is_timestamp_field(field) || value.is_null() {
            continue;
        }
        let timestamp = value.as_str().ok_or_else(|| {
            PostgresStorageError::invalid_input(format!(
                "Logical timestamp field '{field}' is not a string"
            ))
        })?;
        let utc = DateTime::parse_from_rfc3339(timestamp).map_err(|error| {
            PostgresStorageError::invalid_input(format!(
                "Logical timestamp field '{field}' must be RFC 3339 with an offset: {error}"
            ))
        })?;
        if matches!(field.as_str(), "valid_from" | "valid_to") {
            *value = Value::String(
                utc.with_timezone(&Utc)
                    .to_rfc3339_opts(SecondsFormat::AutoSi, true),
            );
        } else {
            *value = Value::String(
                utc.with_timezone(&Utc)
                    .naive_utc()
                    .format("%Y-%m-%dT%H:%M:%S%.f")
                    .to_string(),
            );
        }
    }
    Ok(())
}

fn logicalize_permission_grant(row: &mut Map<String, Value>) -> Result<(), PostgresStorageError> {
    let mut permissions = Vec::new();
    for permission in PERMISSION_FIELDS {
        let field = format!("has_{permission}");
        match row.remove(&field) {
            Some(Value::Bool(true)) => permissions.push(Value::String((*permission).to_string())),
            Some(Value::Bool(false)) => {}
            Some(_) => {
                return Err(PostgresStorageError::database(format!(
                    "Persisted permission field '{field}' is not Boolean"
                )));
            }
            None => {
                return Err(PostgresStorageError::database(format!(
                    "Persisted permission row is missing '{field}'"
                )));
            }
        }
    }
    row.insert("permissions".to_string(), Value::Array(permissions));
    Ok(())
}

fn physicalize_permission_grant(row: &mut Map<String, Value>) -> Result<(), PostgresStorageError> {
    let permissions = row
        .remove("permissions")
        .and_then(|value| value.as_array().cloned())
        .ok_or_else(|| {
            PostgresStorageError::invalid_input(
                "Logical collection permission grant must contain a permissions array",
            )
        })?;
    for permission in PERMISSION_FIELDS {
        row.insert(format!("has_{permission}"), Value::Bool(false));
    }
    let mut seen = HashSet::new();
    for permission in permissions {
        let permission = permission.as_str().ok_or_else(|| {
            PostgresStorageError::invalid_input(
                "Logical collection permission names must be strings",
            )
        })?;
        if !PERMISSION_FIELDS.contains(&permission) {
            return Err(PostgresStorageError::invalid_input(format!(
                "Logical collection permission grant contains unknown permission '{permission}'"
            )));
        }
        if !seen.insert(permission.to_string()) {
            return Err(PostgresStorageError::invalid_input(format!(
                "Logical collection permission grant repeats permission '{permission}'"
            )));
        }
        row.insert(format!("has_{permission}"), Value::Bool(true));
    }
    Ok(())
}

fn reject_physical_fields(
    row: &Map<String, Value>,
    mappings: &[(&str, &str)],
) -> Result<(), PostgresStorageError> {
    if let Some((field, _)) = mappings.iter().find(|(field, _)| row.contains_key(*field)) {
        return Err(PostgresStorageError::invalid_input(format!(
            "Logical backup row contains adapter-private field '{field}'"
        )));
    }
    Ok(())
}

fn logicalize_history_operation(row: &mut Map<String, Value>) -> Result<(), PostgresStorageError> {
    let operation = row
        .get_mut("operation")
        .ok_or_else(|| PostgresStorageError::database("Persisted history row has no operation"))?;
    let logical = match operation.as_str() {
        Some("I") => "create",
        Some("U") => "update",
        Some("D") => "delete",
        _ => {
            return Err(PostgresStorageError::database(
                "Persisted history row has an invalid operation",
            ));
        }
    };
    *operation = Value::String(logical.to_string());
    Ok(())
}

fn physicalize_history_operation(row: &mut Map<String, Value>) -> Result<(), PostgresStorageError> {
    let operation = row.get_mut("operation").ok_or_else(|| {
        PostgresStorageError::invalid_input("Logical history row has no operation")
    })?;
    let physical = match operation.as_str() {
        Some("create") => "I",
        Some("update") => "U",
        Some("delete") => "D",
        _ => {
            return Err(PostgresStorageError::invalid_input(
                "Logical history operation must be 'create', 'update', or 'delete'",
            ));
        }
    };
    *operation = Value::String(physical.to_string());
    Ok(())
}

fn state_field_mappings(
    section: StorageBackupStateSection,
) -> &'static [(&'static str, &'static str)] {
    match section {
        StorageBackupStateSection::Groups => &[("groupname", "name")],
        StorageBackupStateSection::ComputedFieldDefinitions => {
            &[("owner_user_id", "owner_principal_id")]
        }
        StorageBackupStateSection::ClassRelations => CLASS_RELATION_FIELD_MAPPINGS,
        StorageBackupStateSection::Objects => OBJECT_FIELD_MAPPINGS,
        StorageBackupStateSection::ObjectRelations => OBJECT_RELATION_FIELD_MAPPINGS,
        _ => &[],
    }
}

fn history_field_mappings(
    section: StorageBackupHistorySection,
) -> &'static [(&'static str, &'static str)] {
    match section {
        StorageBackupHistorySection::ClassRelationHistory => CLASS_RELATION_FIELD_MAPPINGS,
        StorageBackupHistorySection::ObjectHistory => OBJECT_FIELD_MAPPINGS,
        StorageBackupHistorySection::ObjectRelationHistory => OBJECT_RELATION_FIELD_MAPPINGS,
        StorageBackupHistorySection::AuditEvents => &[
            ("actor_user_id", "actor_principal_id"),
            ("initiator_user_id", "initiator_principal_id"),
        ],
        StorageBackupHistorySection::TerminalTasks => {
            &[("initiator_user_id", "initiator_principal_id")]
        }
        _ => &[],
    }
}

fn history_private_fields(section: StorageBackupHistorySection) -> &'static [&'static str] {
    match section {
        StorageBackupHistorySection::TerminalTasks => &[
            "idempotency_key",
            "submitted_token_id",
            "lease_token",
            "lease_expires_at",
        ],
        StorageBackupHistorySection::AuditEvents => &["fanout_locked_until", "fanout_claim_token"],
        StorageBackupHistorySection::TerminalEventDeliveries => &["locked_until", "claim_token"],
        _ => &[],
    }
}

pub(crate) fn state_row_to_logical(
    section: StorageBackupStateSection,
    row: &mut Value,
) -> Result<(), PostgresStorageError> {
    let row = row.as_object_mut().ok_or_else(|| {
        PostgresStorageError::database("PostgreSQL backup row is not a JSON object")
    })?;
    rename_fields(row, state_field_mappings(section), true)?;
    if section == StorageBackupStateSection::Users {
        row.remove("password");
    }
    if section == StorageBackupStateSection::CollectionPermissionGrants {
        logicalize_permission_grant(row)?;
    }
    logicalize_timestamps(row)?;
    Ok(())
}

pub(crate) fn history_row_to_logical(
    section: StorageBackupHistorySection,
    row: &mut Value,
) -> Result<(), PostgresStorageError> {
    let row = row.as_object_mut().ok_or_else(|| {
        PostgresStorageError::database("PostgreSQL backup history row is not a JSON object")
    })?;
    for field in history_private_fields(section) {
        row.remove(*field);
    }
    if matches!(
        section,
        StorageBackupHistorySection::CollectionHistory
            | StorageBackupHistorySection::ClassHistory
            | StorageBackupHistorySection::ClassRelationHistory
            | StorageBackupHistorySection::ObjectHistory
            | StorageBackupHistorySection::ObjectRelationHistory
            | StorageBackupHistorySection::ExportTemplateHistory
            | StorageBackupHistorySection::RemoteTargetHistory
    ) {
        rename_fields(row, HISTORY_FIELD_MAPPINGS, true)?;
        logicalize_history_operation(row)?;
    }
    rename_fields(row, history_field_mappings(section), true)
        .and_then(|()| logicalize_timestamps(row))
}

pub(crate) fn state_row_to_postgres(
    section: StorageBackupStateSection,
    mut row: Value,
) -> Result<Value, PostgresStorageError> {
    let object = row.as_object_mut().ok_or_else(|| {
        PostgresStorageError::invalid_input("Logical backup state row is not a JSON object")
    })?;
    reject_physical_fields(object, state_field_mappings(section))?;
    if section == StorageBackupStateSection::Users && object.contains_key("password") {
        return Err(PostgresStorageError::invalid_input(
            "Logical user backup rows must not contain password storage fields",
        ));
    }
    if section == StorageBackupStateSection::CollectionPermissionGrants
        && object.keys().any(|field| field.starts_with("has_"))
    {
        return Err(PostgresStorageError::invalid_input(
            "Logical permission rows must use the permissions array",
        ));
    }
    rename_fields(object, state_field_mappings(section), false)?;
    if section == StorageBackupStateSection::CollectionPermissionGrants {
        physicalize_permission_grant(object)?;
    }
    physicalize_timestamps(object)?;
    Ok(row)
}

pub(crate) fn history_row_to_postgres(
    section: StorageBackupHistorySection,
    mut row: Value,
) -> Result<Value, PostgresStorageError> {
    let object = row.as_object_mut().ok_or_else(|| {
        PostgresStorageError::invalid_input("Logical backup history row is not a JSON object")
    })?;
    if let Some(field) = history_private_fields(section)
        .iter()
        .find(|field| object.contains_key(**field))
    {
        return Err(PostgresStorageError::invalid_input(format!(
            "Logical backup history row contains adapter-private field '{field}'"
        )));
    }
    if section == StorageBackupHistorySection::AuditEvents {
        normalize_legacy_event_correlation_id(object);
    }
    if matches!(
        section,
        StorageBackupHistorySection::CollectionHistory
            | StorageBackupHistorySection::ClassHistory
            | StorageBackupHistorySection::ClassRelationHistory
            | StorageBackupHistorySection::ObjectHistory
            | StorageBackupHistorySection::ObjectRelationHistory
            | StorageBackupHistorySection::ExportTemplateHistory
            | StorageBackupHistorySection::RemoteTargetHistory
    ) {
        reject_physical_fields(object, HISTORY_FIELD_MAPPINGS)?;
        physicalize_history_operation(object)?;
        rename_fields(object, HISTORY_FIELD_MAPPINGS, false)?;
    }
    reject_physical_fields(object, history_field_mappings(section))?;
    rename_fields(object, history_field_mappings(section), false)?;
    physicalize_timestamps(object)?;
    Ok(row)
}

fn normalize_legacy_event_correlation_id(row: &mut Map<String, Value>) {
    let invalid = row
        .get("correlation_id")
        .and_then(Value::as_str)
        .is_some_and(|value| CorrelationId::new(value).is_err());
    if invalid {
        row.insert("correlation_id".to_string(), Value::Null);
    }
}

fn validate_snapshot_table(table: &str) -> Result<(), PostgresStorageError> {
    let known_table = StorageBackupStateSection::ALL
        .iter()
        .copied()
        .map(state_table)
        .chain(
            StorageBackupHistorySection::ALL
                .iter()
                .copied()
                .map(history_table),
        )
        .any(|known| known == table);
    if !known_table {
        return Err(PostgresStorageError::database(
            "Refused an unknown backup snapshot table",
        ));
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum SnapshotFilter {
    All,
    TerminalTasks,
    TerminalTaskResults,
    HistoryEvents,
    TerminalDeliveries,
}

impl SnapshotFilter {
    fn sql(self, table: &str) -> Result<Option<&'static str>, PostgresStorageError> {
        match self {
            Self::All => Ok(None),
            Self::TerminalTasks if table == "tasks" => Ok(Some(
                "status IN ('succeeded', 'partially_succeeded', 'failed', 'cancelled')",
            )),
            Self::TerminalTaskResults
                if matches!(
                    table,
                    "import_task_results" | "export_task_outputs" | "remote_call_results"
                ) =>
            {
                Ok(Some(
                    "task_id IN (SELECT id FROM tasks WHERE status IN \
                     ('succeeded', 'partially_succeeded', 'failed', 'cancelled'))",
                ))
            }
            Self::HistoryEvents if table == "events" => Ok(Some(
                "entity_type <> 'task' OR entity_id IN \
                 (SELECT id FROM tasks WHERE status IN \
                 ('succeeded', 'partially_succeeded', 'failed', 'cancelled'))",
            )),
            Self::TerminalDeliveries if table == "event_deliveries" => Ok(Some(
                "status IN ('succeeded', 'dead') AND event_id IN \
                 (SELECT id FROM events WHERE entity_type <> 'task' OR entity_id IN \
                 (SELECT id FROM tasks WHERE status IN \
                 ('succeeded', 'partially_succeeded', 'failed', 'cancelled')))",
            )),
            _ => Err(PostgresStorageError::database(
                "Refused an invalid backup snapshot filter/table combination",
            )),
        }
    }
}

async fn load_json_rows(
    conn: &mut PostgresConnection,
    table: &str,
    filter: SnapshotFilter,
) -> Result<Vec<Value>, PostgresStorageError> {
    validate_snapshot_table(table)?;
    // The only formatted components are a table identifier from the closed
    // list above and a predicate selected from fixed internal variants.
    let predicate = filter
        .sql(table)?
        .map(|value| format!(" WHERE {value}"))
        .unwrap_or_default();
    let query = format!(
        "SELECT COALESCE(jsonb_agg(to_jsonb(snapshot_row) ORDER BY to_jsonb(snapshot_row)::text), '[]'::jsonb) AS rows \
         FROM {table} snapshot_row{predicate}"
    );
    let value = diesel::sql_query(query)
        .get_result::<JsonRows>(conn)
        .await?
        .rows;
    value.as_array().cloned().ok_or_else(|| {
        PostgresStorageError::database(format!("Backup query for {table} did not return an array"))
    })
}

async fn snapshot_state(
    conn: &mut PostgresConnection,
) -> Result<StorageBackupStateSections, PostgresStorageError> {
    let mut sections = BTreeMap::new();
    for section in StorageBackupStateSection::ALL.iter().copied() {
        let table = state_table(section);
        let mut rows = load_json_rows(conn, table, SnapshotFilter::All).await?;
        for row in &mut rows {
            state_row_to_logical(section, row)?;
        }
        sections.insert(section, backup_rows(rows)?);
    }
    Ok(sections)
}

async fn load_logical_history_rows(
    conn: &mut PostgresConnection,
    section: StorageBackupHistorySection,
    filter: SnapshotFilter,
) -> Result<Vec<Value>, PostgresStorageError> {
    let mut rows = load_json_rows(conn, history_table(section), filter).await?;
    for row in &mut rows {
        history_row_to_logical(section, row)?;
    }
    Ok(rows)
}

async fn snapshot_history(
    conn: &mut PostgresConnection,
) -> Result<StorageBackupHistorySections, PostgresStorageError> {
    let mut sections = BTreeMap::new();
    for section in [
        StorageBackupHistorySection::CollectionHistory,
        StorageBackupHistorySection::ClassHistory,
        StorageBackupHistorySection::ClassRelationHistory,
        StorageBackupHistorySection::ObjectHistory,
        StorageBackupHistorySection::ObjectRelationHistory,
        StorageBackupHistorySection::ExportTemplateHistory,
        StorageBackupHistorySection::RemoteTargetHistory,
    ] {
        let rows = load_logical_history_rows(conn, section, SnapshotFilter::All).await?;
        sections.insert(section, backup_rows(rows)?);
    }
    let mut tasks = load_json_rows(conn, "tasks", SnapshotFilter::TerminalTasks).await?;
    for task in &mut tasks {
        history_row_to_logical(StorageBackupHistorySection::TerminalTasks, task)?;
    }
    sections.insert(
        StorageBackupHistorySection::TerminalTasks,
        backup_rows(tasks)?,
    );
    sections.insert(
        StorageBackupHistorySection::ImportResults,
        backup_rows(
            load_logical_history_rows(
                conn,
                StorageBackupHistorySection::ImportResults,
                SnapshotFilter::TerminalTaskResults,
            )
            .await?,
        )?,
    );
    sections.insert(
        StorageBackupHistorySection::ExportOutputs,
        backup_rows(
            load_logical_history_rows(
                conn,
                StorageBackupHistorySection::ExportOutputs,
                SnapshotFilter::TerminalTaskResults,
            )
            .await?,
        )?,
    );
    sections.insert(
        StorageBackupHistorySection::RemoteCallResults,
        backup_rows(
            load_logical_history_rows(
                conn,
                StorageBackupHistorySection::RemoteCallResults,
                SnapshotFilter::TerminalTaskResults,
            )
            .await?,
        )?,
    );
    let mut events = load_json_rows(conn, "events", SnapshotFilter::HistoryEvents).await?;
    for event in &mut events {
        if let Some(object) = event.as_object_mut()
            && object.get("dispatched_at").is_none_or(Value::is_null)
        {
            object.insert(
                "dispatched_at".to_string(),
                object.get("occurred_at").cloned().unwrap_or(Value::Null),
            );
        }
    }
    for event in &mut events {
        history_row_to_logical(StorageBackupHistorySection::AuditEvents, event)?;
    }
    sections.insert(
        StorageBackupHistorySection::AuditEvents,
        backup_rows(events)?,
    );
    sections.insert(
        StorageBackupHistorySection::TerminalEventDeliveries,
        backup_rows(
            load_logical_history_rows(
                conn,
                StorageBackupHistorySection::TerminalEventDeliveries,
                SnapshotFilter::TerminalDeliveries,
            )
            .await?,
        )?,
    );
    Ok(sections)
}

fn backup_rows(rows: Vec<Value>) -> Result<Vec<StorageBackupRow>, PostgresStorageError> {
    rows.into_iter()
        .map(|row| {
            crate::validate_persisted("backup section row", StorageBackupRow::try_from_value(row))
        })
        .collect()
}

pub async fn capture_backup_snapshot(
    runtime: &PostgresRuntime,
    include_history: bool,
) -> Result<StorageBackupSnapshot, PostgresStorageError> {
    runtime
        .with_read_only_snapshot(async |conn| -> Result<_, PostgresStorageError> {
            let state = snapshot_state(conn).await?;
            let history = if include_history {
                Some(snapshot_history(conn).await?)
            } else {
                None
            };
            crate::validate_persisted(
                "backup snapshot",
                StorageBackupSnapshot::try_new(state, history),
            )
        })
        .await
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde_json::{Map, Value, json};

    use super::{
        PERMISSION_FIELDS, SnapshotFilter, history_row_to_logical, history_row_to_postgres,
        state_row_to_logical, state_row_to_postgres, validate_snapshot_table,
    };
    use hubuum_storage_core::{StorageBackupHistorySection, StorageBackupStateSection};

    #[rstest]
    #[case::known("collections", true)]
    #[case::private_mapping("hubuumclass", true)]
    #[case::injected("collections; DROP TABLE users", false)]
    fn snapshot_tables_use_a_closed_allowlist(#[case] table: &str, #[case] accepted: bool) {
        assert_eq!(validate_snapshot_table(table).is_ok(), accepted);
    }

    #[rstest]
    #[case::all_collections(SnapshotFilter::All, "collections", true)]
    #[case::task_filter_on_collection(SnapshotFilter::TerminalTasks, "collections", false)]
    fn snapshot_filters_are_validated_for_their_table(
        #[case] filter: SnapshotFilter,
        #[case] table: &str,
        #[case] accepted: bool,
    ) {
        assert_eq!(filter.sql(table).is_ok(), accepted);
    }

    #[test]
    fn class_relation_rows_use_domain_field_names() {
        let physical = json!({
            "id": 3,
            "from_hubuum_class_id": 4,
            "to_hubuum_class_id": 5
        });
        let mut logical = physical.clone();

        state_row_to_logical(StorageBackupStateSection::ClassRelations, &mut logical).unwrap();

        assert_eq!(logical.get("from_class_id"), Some(&json!(4)));
        assert_eq!(logical.get("to_class_id"), Some(&json!(5)));
        assert_eq!(
            state_row_to_postgres(StorageBackupStateSection::ClassRelations, logical).unwrap(),
            physical
        );
    }

    #[test]
    fn permission_rows_use_a_semantic_permission_list() {
        let mut object = Map::from_iter([
            ("id".to_string(), json!(3)),
            ("collection_id".to_string(), json!(4)),
            ("group_id".to_string(), json!(5)),
        ]);
        for permission in PERMISSION_FIELDS {
            object.insert(
                format!("has_{permission}"),
                Value::Bool(*permission == "read_collection"),
            );
        }
        let physical = Value::Object(object);
        let mut logical = physical.clone();

        state_row_to_logical(
            StorageBackupStateSection::CollectionPermissionGrants,
            &mut logical,
        )
        .unwrap();

        assert_eq!(
            logical.get("permissions"),
            Some(&json!(["read_collection"]))
        );
        assert!(
            logical
                .as_object()
                .unwrap()
                .keys()
                .all(|field| !field.starts_with("has_"))
        );
        assert_eq!(
            state_row_to_postgres(
                StorageBackupStateSection::CollectionPermissionGrants,
                logical,
            )
            .unwrap(),
            physical
        );
    }

    #[test]
    fn history_rows_use_principal_and_operation_names() {
        let physical = json!({
            "op": "U",
            "history_id": 9,
            "actor_id": 7,
            "initiator_user_id": 8
        });
        let mut logical = physical.clone();

        history_row_to_logical(StorageBackupHistorySection::ClassHistory, &mut logical).unwrap();

        assert_eq!(logical.get("operation"), Some(&json!("update")));
        assert_eq!(logical.get("history_entry_id"), Some(&json!(9)));
        assert_eq!(logical.get("actor_principal_id"), Some(&json!(7)));
        assert_eq!(logical.get("initiator_principal_id"), Some(&json!(8)));
        assert_eq!(
            history_row_to_postgres(StorageBackupHistorySection::ClassHistory, logical).unwrap(),
            physical
        );
    }

    #[test]
    fn restore_rejects_adapter_private_fields() {
        let error = state_row_to_postgres(
            StorageBackupStateSection::Users,
            json!({"id": 4, "password": "postgres-hash"}),
        )
        .unwrap_err();

        assert_eq!(
            error.kind(),
            hubuum_storage_core::StorageErrorKind::InvalidInput
        );
    }

    #[test]
    fn restore_rejects_duplicate_permissions() {
        let error = state_row_to_postgres(
            StorageBackupStateSection::CollectionPermissionGrants,
            json!({"permissions": ["read_collection", "read_collection"]}),
        )
        .unwrap_err();

        assert_eq!(
            error.kind(),
            hubuum_storage_core::StorageErrorKind::InvalidInput
        );
    }

    #[test]
    fn user_rows_omit_password_storage_fields() {
        let mut row = json!({"id": 4, "password": null});

        state_row_to_logical(StorageBackupStateSection::Users, &mut row).unwrap();

        assert!(row.get("password").is_none());
    }

    #[test]
    fn logical_rows_use_explicit_utc_timestamps() {
        let physical = json!({"id": 4, "created_at": "2026-08-21T12:34:56.123456"});
        let mut logical = physical.clone();

        state_row_to_logical(StorageBackupStateSection::Collections, &mut logical).unwrap();

        assert_eq!(
            logical.get("created_at"),
            Some(&json!("2026-08-21T12:34:56.123456Z"))
        );
        assert_eq!(
            state_row_to_postgres(StorageBackupStateSection::Collections, logical).unwrap(),
            physical
        );
    }

    #[rstest]
    #[case::whitespace("legacy correlation")]
    #[case::overlong(&"x".repeat(129))]
    fn restore_normalizes_legacy_invalid_event_correlation_ids(#[case] correlation_id: &str) {
        let restored = history_row_to_postgres(
            StorageBackupHistorySection::AuditEvents,
            json!({"correlation_id": correlation_id}),
        )
        .unwrap();

        assert_eq!(restored.get("correlation_id"), Some(&Value::Null));
    }
}
