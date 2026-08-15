//! Adapter-private representation and validation of persisted computed fields.

use diesel::{Queryable, Selectable};
use hubuum_computed_fields::{
    Definition, EvaluationLimits, EvaluationResult, FieldKey, Operation, ResultType, evaluate,
};
use hubuum_query::ComputedQueryValueType;

use crate::PostgresStorageError;

pub(crate) const PERSONAL_VISIBILITY: &str = "personal";
pub(crate) const SHARED_VISIBILITY: &str = "shared";

#[derive(Clone, Debug, Queryable, Selectable)]
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
}

impl ComputedDefinitionRow {
    pub(crate) const fn class_id(&self) -> i32 {
        self.class_id
    }

    pub(crate) fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn operation(&self) -> &serde_json::Value {
        &self.operation
    }

    pub(crate) fn result_type_name(&self) -> &str {
        &self.result_type
    }

    pub(crate) const fn enabled(&self) -> bool {
        self.enabled
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
