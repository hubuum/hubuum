use super::view::{BackupView, ReadError, Rows};
use crate::{
    StorageBackupHistorySection, StorageBackupRow, StorageBackupStateSection,
    StorageClassSchemaPolicy,
};
use chrono::DateTime;
use hubuum_computed_fields::{
    Definition, FieldKey, MAX_PERSONAL_DEFINITIONS, MAX_SHARED_DEFINITIONS, Operation, ResultType,
    SEMANTICS_VERSION,
};
use hubuum_domain::{JsonSchemaLimits, LOCAL_IDENTITY_SCOPE, LOCAL_PROVIDER_KIND};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

const COMPUTED_FIELD_VISIBILITY_SHARED: &str = "shared";
const COMPUTED_FIELD_VISIBILITY_PERSONAL: &str = "personal";

pub(super) fn validate_view(
    snapshot: &impl BackupView,
    schema_limits: JsonSchemaLimits,
) -> Result<(), ReadError> {
    validate_backup_timestamps(snapshot)?;
    validate_required_seed_rows(snapshot)?;
    validate_backup_state_references(snapshot)?;
    validate_backup_class_schemas(snapshot, schema_limits)?;
    validate_computed_field_definitions(snapshot)
}

fn validate_row_timestamps(section: &str, rows: &[StorageBackupRow]) -> Result<(), ReadError> {
    for row in rows {
        for (field, value) in row.fields() {
            if !(field.ends_with("_at")
                || matches!(field.as_str(), "issued" | "valid_from" | "valid_to"))
                || value.is_null()
            {
                continue;
            }
            let timestamp = value.as_str().ok_or_else(|| {
                ReadError::invalid(format!(
                    "Full backup section '{section}' contains a non-string timestamp in '{field}'"
                ))
            })?;
            DateTime::parse_from_rfc3339(timestamp).map_err(|_| {
                ReadError::invalid(format!(
                    "Full backup section '{section}' contains an invalid RFC 3339 timestamp in '{field}'"
                ))
            })?;
        }
    }
    Ok(())
}

fn validate_backup_timestamps(document: &impl BackupView) -> Result<(), ReadError> {
    for section in StorageBackupStateSection::ALL {
        if document.has_state(*section) {
            for row in document.state_rows(*section)? {
                validate_row_timestamps(section.as_str(), std::slice::from_ref(row?.as_ref()))?;
            }
        }
    }
    if document.includes_history() {
        for section in StorageBackupHistorySection::ALL {
            for row in document.history_rows(*section)? {
                validate_row_timestamps(
                    &format!("history.{}", section.as_str()),
                    std::slice::from_ref(row?.as_ref()),
                )?;
            }
        }
    }
    Ok(())
}

fn required_positive_id(
    section: &str,
    row: &StorageBackupRow,
    field: &str,
) -> Result<i64, ReadError> {
    row.get(field)
        .and_then(Value::as_i64)
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            ReadError::invalid(format!(
                "Full backup section '{section}' contains an invalid {field}"
            ))
        })
}

fn optional_positive_id(
    section: &str,
    row: &StorageBackupRow,
    field: &str,
) -> Result<Option<i64>, ReadError> {
    match row.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(_) => required_positive_id(section, row, field).map(Some),
    }
}

fn section_ids(
    document: &impl BackupView,
    section: StorageBackupStateSection,
) -> Result<HashSet<i64>, ReadError> {
    let rows = required_state_section(document, section)?;
    let ids = rows
        .iter()
        .map(|row| required_positive_id(section.as_str(), row?.as_ref(), "id"))
        .collect::<Result<HashSet<_>, _>>()?;
    if ids.len() != rows.len() {
        return Err(ReadError::invalid(format!(
            "Full backup section '{section}' contains duplicate identifiers"
        )));
    }
    Ok(ids)
}

fn require_reference(
    section: &str,
    field: &str,
    value: i64,
    targets: &HashSet<i64>,
) -> Result<(), ReadError> {
    if targets.contains(&value) {
        Ok(())
    } else {
        Err(ReadError::invalid(format!(
            "Full backup section '{section}' contains a {field} that does not reference a retained row"
        )))
    }
}

fn validate_required_reference(
    section: &str,
    row: &StorageBackupRow,
    field: &str,
    targets: &HashSet<i64>,
) -> Result<i64, ReadError> {
    let value = required_positive_id(section, row, field)?;
    require_reference(section, field, value, targets)?;
    Ok(value)
}

fn validate_optional_reference(
    section: &str,
    row: &StorageBackupRow,
    field: &str,
    targets: &HashSet<i64>,
) -> Result<Option<i64>, ReadError> {
    let value = optional_positive_id(section, row, field)?;
    if let Some(value) = value {
        require_reference(section, field, value, targets)?;
    }
    Ok(value)
}

fn validate_backup_state_references(document: &impl BackupView) -> Result<(), ReadError> {
    let identity_scopes = section_ids(document, StorageBackupStateSection::IdentityScopes)?;
    let groups = section_ids(document, StorageBackupStateSection::Groups)?;
    let principals = section_ids(document, StorageBackupStateSection::Principals)?;
    let collections = section_ids(document, StorageBackupStateSection::Collections)?;
    let classes = section_ids(document, StorageBackupStateSection::Classes)?;
    let class_relations = section_ids(document, StorageBackupStateSection::ClassRelations)?;
    let objects = section_ids(document, StorageBackupStateSection::Objects)?;
    let sinks = section_ids(document, StorageBackupStateSection::EventSinks)?;

    for row in required_state_section(document, StorageBackupStateSection::Groups)? {
        let row = row?;
        validate_required_reference("groups", &row, "identity_scope_id", &identity_scopes)?;
    }
    for row in required_state_section(document, StorageBackupStateSection::Principals)? {
        let row = row?;
        validate_required_reference("principals", &row, "identity_scope_id", &identity_scopes)?;
    }
    for section in [
        StorageBackupStateSection::Users,
        StorageBackupStateSection::ServiceAccounts,
    ] {
        for row in required_state_section(document, section)? {
            let row = row?;
            validate_required_reference(section.as_str(), &row, "id", &principals)?;
        }
    }
    for row in required_state_section(document, StorageBackupStateSection::ServiceAccounts)? {
        let row = row?;
        validate_required_reference("service_accounts", &row, "owner_group_id", &groups)?;
        validate_optional_reference("service_accounts", &row, "created_by", &principals)?;
    }
    for row in required_state_section(document, StorageBackupStateSection::GroupMemberships)? {
        let row = row?;
        validate_required_reference("group_memberships", &row, "principal_id", &principals)?;
        validate_required_reference("group_memberships", &row, "group_id", &groups)?;
    }
    for row in required_state_section(document, StorageBackupStateSection::GroupMembershipSources)?
    {
        let row = row?;
        validate_required_reference(
            "group_membership_sources",
            &row,
            "principal_id",
            &principals,
        )?;
        validate_required_reference("group_membership_sources", &row, "group_id", &groups)?;
        validate_required_reference(
            "group_membership_sources",
            &row,
            "source_scope_id",
            &identity_scopes,
        )?;
    }
    for row in required_state_section(document, StorageBackupStateSection::Collections)? {
        let row = row?;
        validate_optional_reference("collections", &row, "parent_collection_id", &collections)?;
    }
    for row in required_state_section(document, StorageBackupStateSection::CollectionAuthorization)?
    {
        let row = row?;
        validate_required_reference(
            "collection_authorization",
            &row,
            "collection_id",
            &collections,
        )?;
    }
    for row in required_state_section(document, StorageBackupStateSection::CollectionHierarchy)? {
        let row = row?;
        validate_required_reference(
            "collection_hierarchy",
            &row,
            "ancestor_collection_id",
            &collections,
        )?;
        validate_required_reference(
            "collection_hierarchy",
            &row,
            "descendant_collection_id",
            &collections,
        )?;
    }
    for row in required_state_section(
        document,
        StorageBackupStateSection::CollectionPermissionGrants,
    )? {
        let row = row?;
        validate_required_reference(
            "collection_permission_grants",
            &row,
            "collection_id",
            &collections,
        )?;
        validate_required_reference("collection_permission_grants", &row, "group_id", &groups)?;
    }

    let class_collections = required_state_section(document, StorageBackupStateSection::Classes)?
        .iter()
        .map(|row| {
            let row = row?;
            let id = required_positive_id("classes", &row, "id")?;
            let collection_id =
                validate_required_reference("classes", &row, "collection_id", &collections)?;
            Ok((id, collection_id))
        })
        .collect::<Result<HashMap<_, _>, ReadError>>()?;
    for row in required_state_section(
        document,
        StorageBackupStateSection::ComputedFieldDefinitions,
    )? {
        let row = row?;
        validate_required_reference("computed_field_definitions", &row, "class_id", &classes)?;
        validate_optional_reference(
            "computed_field_definitions",
            &row,
            "owner_principal_id",
            &principals,
        )?;
        validate_optional_reference(
            "computed_field_definitions",
            &row,
            "created_by",
            &principals,
        )?;
        validate_optional_reference(
            "computed_field_definitions",
            &row,
            "updated_by",
            &principals,
        )?;
    }

    let relation_classes =
        required_state_section(document, StorageBackupStateSection::ClassRelations)?
            .iter()
            .map(|row| {
                let row = row?;
                let id = required_positive_id("class_relations", &row, "id")?;
                let from = validate_required_reference(
                    "class_relations",
                    &row,
                    "from_class_id",
                    &classes,
                )?;
                let to =
                    validate_required_reference("class_relations", &row, "to_class_id", &classes)?;
                Ok((id, (from, to)))
            })
            .collect::<Result<HashMap<_, _>, ReadError>>()?;

    let object_classes = required_state_section(document, StorageBackupStateSection::Objects)?
        .iter()
        .map(|row| {
            let row = row?;
            let id = required_positive_id("objects", &row, "id")?;
            let class_id = validate_required_reference("objects", &row, "class_id", &classes)?;
            let collection_id =
                validate_required_reference("objects", &row, "collection_id", &collections)?;
            if class_collections.get(&class_id) != Some(&collection_id) {
                return Err(ReadError::invalid(
                    "Full backup object collection does not match its class collection".to_string(),
                ));
            }
            Ok((id, class_id))
        })
        .collect::<Result<HashMap<_, _>, ReadError>>()?;

    for row in required_state_section(document, StorageBackupStateSection::ObjectRelations)? {
        let row = row?;
        let from_object =
            validate_required_reference("object_relations", &row, "from_object_id", &objects)?;
        let to_object =
            validate_required_reference("object_relations", &row, "to_object_id", &objects)?;
        let relation_id = validate_required_reference(
            "object_relations",
            &row,
            "class_relation_id",
            &class_relations,
        )?;
        let endpoints = (
            object_classes.get(&from_object).copied(),
            object_classes.get(&to_object).copied(),
        );
        let expected = relation_classes.get(&relation_id).copied();
        if expected.is_none_or(|expected| {
            endpoints != (Some(expected.0), Some(expected.1))
                && endpoints != (Some(expected.1), Some(expected.0))
        }) {
            return Err(ReadError::invalid(
                "Full backup object relation endpoints do not match its class relation".to_string(),
            ));
        }
    }

    for row in required_state_section(document, StorageBackupStateSection::ExportTemplates)? {
        let row = row?;
        validate_required_reference("export_templates", &row, "collection_id", &collections)?;
        validate_optional_reference("export_templates", &row, "class_id", &classes)?;
    }
    for row in required_state_section(document, StorageBackupStateSection::RemoteTargets)? {
        let row = row?;
        validate_required_reference("remote_targets", &row, "collection_id", &collections)?;
        validate_optional_reference("remote_targets", &row, "class_id", &classes)?;
    }
    for row in required_state_section(document, StorageBackupStateSection::EventSinks)? {
        let row = row?;
        validate_optional_reference("event_sinks", &row, "collection_id", &collections)?;
    }
    if document.has_state(StorageBackupStateSection::EventSinkCollectionGrants) {
        let grants = document.state_rows(StorageBackupStateSection::EventSinkCollectionGrants)?;
        for row in grants {
            let row = row?;
            validate_required_reference("event_sink_collection_grants", &row, "sink_id", &sinks)?;
            validate_required_reference(
                "event_sink_collection_grants",
                &row,
                "collection_id",
                &collections,
            )?;
        }
    }
    for row in required_state_section(document, StorageBackupStateSection::EventSubscriptions)? {
        let row = row?;
        validate_optional_reference("event_subscriptions", &row, "collection_id", &collections)?;
        validate_required_reference("event_subscriptions", &row, "sink_id", &sinks)?;
    }
    Ok(())
}

fn validate_backup_class_schemas(
    document: &impl BackupView,
    schema_limits: JsonSchemaLimits,
) -> Result<(), ReadError> {
    let current_classes = required_state_section(document, StorageBackupStateSection::Classes)?;
    let historical_classes = document.history_rows(StorageBackupHistorySection::ClassHistory)?;
    for row in current_classes.iter().chain(historical_classes) {
        let row = row?;
        let schema_policy = StorageClassSchemaPolicy::try_from_parts(
            row.get("json_schema")
                .filter(|value| !value.is_null())
                .cloned(),
            row.get("validate_schema")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        )
        .map_err(|error| {
            let class_id = row.get("id").and_then(Value::as_i64);
            ReadError::invalid(format!(
                "Full backup class {} contains an invalid schema policy: {error}",
                class_id
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "with unknown id".to_string())
            ))
        })?;
        let Some(schema) = schema_policy.json_schema() else {
            continue;
        };
        let validation = if schema_policy.validates_schema() {
            schema_limits.validate_schema_for_instances(schema)
        } else {
            schema_limits.validate_schema(schema)
        };
        validation.map_err(|error| {
            let class_id = row.get("id").and_then(Value::as_i64);
            ReadError::invalid(format!(
                "Full backup class {} contains an invalid JSON schema: {error}",
                class_id
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "with unknown id".to_string())
            ))
        })?;
    }
    Ok(())
}

fn validate_computed_field_definitions(document: &impl BackupView) -> Result<(), ReadError> {
    let mut shared_counts = HashMap::<i32, usize>::new();
    let mut personal_counts = HashMap::<(i32, i32), usize>::new();
    let mut shared_keys = HashSet::<(i32, String)>::new();
    let mut personal_keys = HashSet::<(i32, i32, String)>::new();
    for row in required_state_section(
        document,
        StorageBackupStateSection::ComputedFieldDefinitions,
    )? {
        let row = row?;
        let object = row.fields();
        let string = |field: &str| {
            object
                .get(field)
                .and_then(Value::as_str)
                .map(str::to_string)
                .ok_or_else(|| {
                    ReadError::invalid(format!(
                        "Full backup computed-field definition has an invalid {field}"
                    ))
                })
        };
        let result_type = serde_json::from_value::<ResultType>(Value::String(string(
            "result_type",
        )?))
        .map_err(|_| {
            ReadError::invalid(
                "Full backup computed-field definition has an invalid result_type".to_string(),
            )
        })?;
        let key = string("key")?;
        let operation = object.get("operation").cloned().ok_or_else(|| {
            ReadError::invalid("Full backup computed-field definition is missing operation")
        })?;
        let operation: Operation = serde_json::from_value(operation).map_err(|e| {
            ReadError::invalid(format!(
                "Full backup contains an invalid computed-field definition: {e}"
            ))
        })?;
        let enabled = object
            .get("enabled")
            .and_then(Value::as_bool)
            .ok_or_else(|| {
                ReadError::invalid(
                    "Full backup computed-field definition has an invalid enabled flag",
                )
            })?;
        let field_key = FieldKey::new(&key).map_err(|e| ReadError::invalid(e.to_string()))?;
        Definition::new(
            field_key,
            string("label")?,
            string("description")?,
            operation,
            result_type,
            enabled,
        )
        .map_err(|e| {
            ReadError::invalid(format!(
                "Full backup contains an invalid computed-field definition: {e}"
            ))
        })?;

        let positive_id = |field: &str| {
            object
                .get(field)
                .and_then(Value::as_i64)
                .and_then(|value| i32::try_from(value).ok())
                .filter(|value| *value > 0)
                .ok_or_else(|| {
                    ReadError::invalid(format!(
                        "Full backup computed-field definition has an invalid {field}"
                    ))
                })
        };
        let class_id = positive_id("class_id")?;
        object
            .get("revision")
            .and_then(Value::as_i64)
            .filter(|value| (1..i64::MAX).contains(value))
            .ok_or_else(|| {
                ReadError::invalid(
                    "Full backup computed-field definition has an invalid revision".to_string(),
                )
            })?;
        let semantics_version = object
            .get("semantics_version")
            .and_then(Value::as_i64)
            .and_then(|value| i16::try_from(value).ok())
            .ok_or_else(|| {
                ReadError::invalid(
                    "Full backup computed-field definition has an invalid semantics_version"
                        .to_string(),
                )
            })?;
        if semantics_version != SEMANTICS_VERSION {
            return Err(ReadError::invalid(format!(
                "Full backup computed-field definition uses unsupported semantics version {semantics_version}"
            )));
        }
        let visibility = string("visibility")?;
        match visibility.as_str() {
            COMPUTED_FIELD_VISIBILITY_SHARED => {
                if object
                    .get("owner_principal_id")
                    .is_some_and(|value| !value.is_null())
                {
                    return Err(ReadError::invalid(
                        "Full backup shared computed-field definition must not have an owner_principal_id"
                            .to_string(),
                    ));
                }
                if !shared_keys.insert((class_id, key)) {
                    return Err(ReadError::invalid(format!(
                        "Full backup class {class_id} contains a duplicate shared computed-field key"
                    )));
                }
                let count = shared_counts.entry(class_id).or_default();
                *count += 1;
                if *count > MAX_SHARED_DEFINITIONS {
                    return Err(ReadError::invalid(format!(
                        "Full backup class {class_id} has more than {MAX_SHARED_DEFINITIONS} shared computed fields"
                    )));
                }
            }
            COMPUTED_FIELD_VISIBILITY_PERSONAL => {
                let owner_id = positive_id("owner_principal_id")?;
                if !personal_keys.insert((owner_id, class_id, key)) {
                    return Err(ReadError::invalid(format!(
                        "Full backup user {owner_id} contains a duplicate personal computed-field key for class {class_id}"
                    )));
                }
                let count = personal_counts.entry((owner_id, class_id)).or_default();
                *count += 1;
                if *count > MAX_PERSONAL_DEFINITIONS {
                    return Err(ReadError::invalid(format!(
                        "Full backup user {owner_id} has more than {MAX_PERSONAL_DEFINITIONS} personal computed fields for class {class_id}"
                    )));
                }
            }
            _ => {
                return Err(ReadError::invalid(
                    "Full backup computed-field definition has an invalid visibility".to_string(),
                ));
            }
        }
    }
    Ok(())
}

fn required_state_section(
    document: &impl BackupView,
    section: StorageBackupStateSection,
) -> Result<Rows<'_>, ReadError> {
    document.state_rows(section)
}

fn validate_required_seed_rows(document: &impl BackupView) -> Result<(), ReadError> {
    let mut local_count = 0;
    for row in document.state_rows(StorageBackupStateSection::IdentityScopes)? {
        let row = row?;
        if row.get("name").and_then(Value::as_str) == Some(LOCAL_IDENTITY_SCOPE) {
            local_count += 1;
            if row.get("provider_kind").and_then(Value::as_str) != Some(LOCAL_PROVIDER_KIND) {
                return Err(ReadError::invalid(format!(
                    "Full backup must contain exactly one '{LOCAL_IDENTITY_SCOPE}' identity scope with provider kind '{LOCAL_PROVIDER_KIND}'"
                )));
            }
        }
    }
    if local_count != 1 {
        return Err(ReadError::invalid(format!(
            "Full backup must contain exactly one '{LOCAL_IDENTITY_SCOPE}' identity scope with provider kind '{LOCAL_PROVIDER_KIND}'"
        )));
    }
    let mut root = None;
    for row in document.state_rows(StorageBackupStateSection::Collections)? {
        let row = row?;
        if row.get("parent_collection_id").is_some_and(Value::is_null) {
            if root.is_some() {
                return Err(ReadError::invalid(
                    "Full backup must contain exactly one root collection",
                ));
            }
            root = Some(row.get("id").and_then(Value::as_i64).ok_or_else(|| {
                ReadError::invalid("Full backup root collection has an invalid id")
            })?);
        }
    }
    let root_id = root.ok_or_else(|| {
        ReadError::invalid("Full backup must contain exactly one root collection")
    })?;
    let mut has_root_closure = false;
    for row in document.state_rows(StorageBackupStateSection::CollectionHierarchy)? {
        let row = row?;
        has_root_closure |= row.get("ancestor_collection_id").and_then(Value::as_i64)
            == Some(root_id)
            && row.get("descendant_collection_id").and_then(Value::as_i64) == Some(root_id)
            && row.get("depth").and_then(Value::as_i64) == Some(0);
    }
    if !has_root_closure {
        return Err(ReadError::invalid(
            "Full backup must contain the root collection's depth-zero closure row",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StorageBackupSnapshot;
    use rstest::rstest;
    use serde_json::json;
    use std::collections::BTreeMap;

    fn computed_definition(class_id: i32, owner_id: Option<i32>, key: String) -> serde_json::Value {
        json!({
            "class_id": class_id,
            "visibility": if owner_id.is_some() { "personal" } else { "shared" },
            "owner_principal_id": owner_id,
            "key": key,
            "label": "Restored field",
            "description": "",
            "operation": {"type": "first_non_null", "paths": ["/value"]},
            "result_type": "string",
            "enabled": true,
            "revision": 1,
            "semantics_version": 1
        })
    }

    fn snapshot_with_computed_definitions(definitions: Vec<Value>) -> StorageBackupSnapshot {
        StorageBackupSnapshot {
            schema_limits: JsonSchemaLimits::default(),
            state_sections: BTreeMap::from([(
                StorageBackupStateSection::ComputedFieldDefinitions,
                definitions
                    .into_iter()
                    .map(StorageBackupRow::try_from_value)
                    .collect::<Result<_, _>>()
                    .unwrap(),
            )]),
            history_sections: None,
        }
    }

    #[rstest]
    #[case::shared(MAX_SHARED_DEFINITIONS, None)]
    #[case::personal(MAX_PERSONAL_DEFINITIONS, Some(7))]
    fn restore_rejects_computed_definition_scope_over_capacity(
        #[case] maximum: usize,
        #[case] owner_id: Option<i32>,
    ) {
        let definitions = (0..=maximum)
            .map(|index| computed_definition(42, owner_id, format!("field_{index}")))
            .collect();
        let error =
            validate_computed_field_definitions(&snapshot_with_computed_definitions(definitions))
                .unwrap_err();
        assert!(error.to_string().contains(&maximum.to_string()));
    }

    #[test]
    fn restore_rejects_duplicate_computed_definition_keys() {
        let definitions = vec![
            computed_definition(42, None, "duplicate".to_string()),
            computed_definition(42, None, "duplicate".to_string()),
        ];

        let error =
            validate_computed_field_definitions(&snapshot_with_computed_definitions(definitions))
                .unwrap_err();

        assert!(error.to_string().contains("duplicate"));
    }
}
