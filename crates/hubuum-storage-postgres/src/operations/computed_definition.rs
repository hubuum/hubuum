//! Adapter-private representation and validation of persisted computed fields.

use chrono::NaiveDateTime;
use diesel::{Queryable, Selectable};
use hubuum_computed_fields::{
    Definition, EvaluationLimits, EvaluationResult, FieldKey, Operation, ResultType, evaluate,
};
use hubuum_domain::{ClassId, PrincipalId};
use hubuum_query::ComputedQueryValueType;
use hubuum_storage_core::{
    StorageComputedFieldDefinition, StorageComputedFieldDefinitionContent,
    StorageComputedFieldDefinitionInput, StorageComputedFieldProvenance,
    StorageComputedFieldVisibility,
};

use crate::revision::record_metadata;
use crate::{PostgresRevision, PostgresStorageError};

pub(crate) const PERSONAL_VISIBILITY: &str = "personal";
pub(crate) const SHARED_VISIBILITY: &str = "shared";

#[derive(Clone, Queryable, Selectable)]
#[diesel(table_name = crate::schema::computed_field_definitions)]
pub(crate) struct ComputedDefinitionRow {
    id: i32,
    class_id: i32,
    visibility: String,
    owner_user_id: Option<i32>,
    key: String,
    label: String,
    description: String,
    operation: serde_json::Value,
    result_type: String,
    enabled: bool,
    revision: PostgresRevision,
    semantics_version: i16,
    created_by: Option<i32>,
    updated_by: Option<i32>,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
}

impl std::fmt::Debug for ComputedDefinitionRow {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ComputedDefinitionRow")
            .field("id", &self.id)
            .field("class_id", &self.class_id)
            .field("visibility", &self.visibility)
            .field("result_type", &self.result_type)
            .field("enabled", &self.enabled)
            .field("revision", &self.revision)
            .field("definition", &"[redacted]")
            .finish()
    }
}

impl ComputedDefinitionRow {
    pub(crate) const fn id(&self) -> i32 {
        self.id
    }

    pub(crate) const fn class_id(&self) -> i32 {
        self.class_id
    }

    pub(crate) fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn operation(&self) -> &serde_json::Value {
        &self.operation
    }

    pub(crate) fn label(&self) -> &str {
        &self.label
    }

    pub(crate) fn description(&self) -> &str {
        &self.description
    }

    pub(crate) fn result_type_name(&self) -> &str {
        &self.result_type
    }

    pub(crate) const fn enabled(&self) -> bool {
        self.enabled
    }

    pub(crate) const fn revision(&self) -> PostgresRevision {
        self.revision
    }

    pub(crate) const fn created_at(&self) -> NaiveDateTime {
        self.created_at
    }

    pub(crate) const fn updated_at(&self) -> NaiveDateTime {
        self.updated_at
    }

    pub(crate) fn is_shared(&self) -> bool {
        self.visibility == SHARED_VISIBILITY
    }

    pub(crate) fn is_personal_for(&self, owner_id: i32) -> bool {
        self.visibility == PERSONAL_VISIBILITY && self.owner_user_id == Some(owner_id)
    }

    pub(crate) fn result_type(&self) -> Result<ResultType, PostgresStorageError> {
        result_type_from_database(&self.result_type)
            .ok_or_else(|| invalid_definition(self.id, "unknown result type"))
    }

    pub(crate) fn query_value_type(&self) -> Result<ComputedQueryValueType, PostgresStorageError> {
        Ok(match self.result_type()? {
            ResultType::String => ComputedQueryValueType::String,
            ResultType::Number => ComputedQueryValueType::Number,
            ResultType::Integer => ComputedQueryValueType::Integer,
            ResultType::Boolean => ComputedQueryValueType::Boolean,
            ResultType::Object => ComputedQueryValueType::Object,
            ResultType::Array => ComputedQueryValueType::Array,
        })
    }

    pub(crate) fn evaluator_definition(&self) -> Result<Definition, PostgresStorageError> {
        let operation = serde_json::from_value::<Operation>(self.operation.clone())
            .map_err(|error| invalid_definition(self.id, format!("invalid operation: {error}")))?;
        let key =
            FieldKey::new(self.key.clone()).map_err(|error| invalid_definition(self.id, error))?;
        Definition::new(
            key,
            self.label.clone(),
            self.description.clone(),
            operation,
            self.result_type()?,
            self.enabled,
        )
        .map_err(|error| invalid_definition(self.id, error))
    }

    pub(crate) fn into_storage(
        self,
    ) -> Result<StorageComputedFieldDefinition, PostgresStorageError> {
        let visibility = match (self.visibility.as_str(), self.owner_user_id) {
            (SHARED_VISIBILITY, None) => StorageComputedFieldVisibility::Shared,
            (PERSONAL_VISIBILITY, Some(owner_id)) => StorageComputedFieldVisibility::Personal {
                owner_id: PrincipalId::new(owner_id)?,
            },
            (visibility, owner_id) => {
                return Err(PostgresStorageError::database(format!(
                    "Computed-field definition {} has invalid visibility '{visibility}' and owner {owner_id:?}",
                    self.id
                )));
            }
        };
        Ok(StorageComputedFieldDefinition::new(
            record_metadata(self.id, self.created_at, self.updated_at, self.revision)?,
            ClassId::new(self.class_id)?,
            visibility,
            StorageComputedFieldDefinitionContent::new(
                StorageComputedFieldDefinitionInput::new(
                    self.key,
                    self.label,
                    self.operation,
                    self.result_type,
                )
                .with_description(self.description)
                .with_enabled(self.enabled),
                self.semantics_version,
            ),
            StorageComputedFieldProvenance::new(
                self.created_by.map(PrincipalId::new).transpose()?,
                self.updated_by.map(PrincipalId::new).transpose()?,
            ),
        ))
    }

    pub(crate) fn snapshot(&self) -> serde_json::Value {
        serde_json::json!({
            "id": self.id,
            "class_id": self.class_id,
            "visibility": self.visibility,
            "owner_user_id": self.owner_user_id,
            "key": self.key,
            "label": self.label,
            "description": self.description,
            "operation": self.operation,
            "result_type": self.result_type,
            "enabled": self.enabled,
            "revision": self.revision.get(),
            "semantics_version": self.semantics_version,
            "created_by": self.created_by,
            "updated_by": self.updated_by,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        })
    }
}

pub(crate) fn evaluate_definitions(
    data: &serde_json::Value,
    definitions: &[ComputedDefinitionRow],
    maximum: usize,
) -> Result<EvaluationResult, PostgresStorageError> {
    let definitions = definitions
        .iter()
        .filter(|definition| definition.enabled())
        .map(ComputedDefinitionRow::evaluator_definition)
        .collect::<Result<Vec<_>, _>>()?;
    evaluate(data, &definitions, maximum, EvaluationLimits::standard()).map_err(|error| {
        PostgresStorageError::internal(format!("Computed-field evaluation failed: {error}"))
    })
}

fn result_type_from_database(value: &str) -> Option<ResultType> {
    Some(match value {
        "string" => ResultType::String,
        "number" => ResultType::Number,
        "integer" => ResultType::Integer,
        "boolean" => ResultType::Boolean,
        "object" => ResultType::Object,
        "array" => ResultType::Array,
        _ => return None,
    })
}

fn invalid_definition(id: i32, detail: impl std::fmt::Display) -> PostgresStorageError {
    PostgresStorageError::internal(format!(
        "Computed-field definition {id} is invalid: {detail}"
    ))
}
