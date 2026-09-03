use std::collections::BTreeMap;

use chrono::NaiveDateTime;
use hubuum_computed_fields::{Definition, FieldError, FieldKey, Operation, ResultType};
use serde::{Deserialize, Serialize};
use utoipa::{PartialSchema, ToSchema};

use crate::errors::ApiError;
use crate::models::search::{ComputedQueryValueType, FilterField, SortParam};
use crate::models::{HubuumClassID, HubuumObject, HubuumObjectID, ResourceRevision};
use crate::pagination::{CursorPaginated, CursorValue};

pub const COMPUTED_FIELD_VISIBILITY_SHARED: &str = "shared";
pub const COMPUTED_FIELD_VISIBILITY_PERSONAL: &str = "personal";

pub use hubuum_domain::ComputedFieldDefinitionId as ComputedFieldDefinitionID;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ComputedResultType {
    String,
    Number,
    Integer,
    Boolean,
    Object,
    Array,
}

impl ComputedResultType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::String => "string",
            Self::Number => "number",
            Self::Integer => "integer",
            Self::Boolean => "boolean",
            Self::Object => "object",
            Self::Array => "array",
        }
    }

    pub fn from_db(value: &str) -> Result<Self, ApiError> {
        match value {
            "string" => Ok(Self::String),
            "number" => Ok(Self::Number),
            "integer" => Ok(Self::Integer),
            "boolean" => Ok(Self::Boolean),
            "object" => Ok(Self::Object),
            "array" => Ok(Self::Array),
            _ => Err(ApiError::InternalServerError(format!(
                "Unknown computed result type '{value}'"
            ))),
        }
    }
}

impl From<ComputedResultType> for ResultType {
    fn from(value: ComputedResultType) -> Self {
        match value {
            ComputedResultType::String => Self::String,
            ComputedResultType::Number => Self::Number,
            ComputedResultType::Integer => Self::Integer,
            ComputedResultType::Boolean => Self::Boolean,
            ComputedResultType::Object => Self::Object,
            ComputedResultType::Array => Self::Array,
        }
    }
}

impl From<ResultType> for ComputedResultType {
    fn from(value: ResultType) -> Self {
        match value {
            ResultType::String => Self::String,
            ResultType::Number => Self::Number,
            ResultType::Integer => Self::Integer,
            ResultType::Boolean => Self::Boolean,
            ResultType::Object => Self::Object,
            ResultType::Array => Self::Array,
        }
    }
}

impl From<ComputedResultType> for ComputedQueryValueType {
    fn from(value: ComputedResultType) -> Self {
        match value {
            ComputedResultType::String => Self::String,
            ComputedResultType::Number => Self::Number,
            ComputedResultType::Integer => Self::Integer,
            ComputedResultType::Boolean => Self::Boolean,
            ComputedResultType::Object => Self::Object,
            ComputedResultType::Array => Self::Array,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ComputedFieldDefinition {
    pub id: i32,
    pub class_id: i32,
    pub visibility: String,
    pub owner_user_id: Option<i32>,
    pub key: String,
    pub label: String,
    pub description: String,
    #[schema(value_type = Object)]
    pub operation: serde_json::Value,
    pub result_type: String,
    pub enabled: bool,
    pub revision: ResourceRevision,
    pub semantics_version: i16,
    pub created_by: Option<i32>,
    pub updated_by: Option<i32>,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}

impl ComputedFieldDefinition {
    pub fn is_shared(&self) -> bool {
        self.visibility == COMPUTED_FIELD_VISIBILITY_SHARED
    }

    pub fn is_personal_for(&self, owner_id: i32) -> bool {
        self.visibility == COMPUTED_FIELD_VISIBILITY_PERSONAL
            && self.owner_user_id == Some(owner_id)
    }
}

impl CursorPaginated for ComputedFieldDefinition {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(
            field,
            FilterField::Id
                | FilterField::Name
                | FilterField::ClassId
                | FilterField::CreatedAt
                | FilterField::UpdatedAt
                | FilterField::Revision
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        Ok(match field {
            FilterField::Id => CursorValue::Integer(self.id as i64),
            FilterField::Name => CursorValue::String(self.key.clone()),
            FilterField::ClassId => CursorValue::Integer(self.class_id as i64),
            FilterField::CreatedAt => CursorValue::DateTime(self.created_at),
            FilterField::UpdatedAt => CursorValue::DateTime(self.updated_at),
            FilterField::Revision => CursorValue::Integer(self.revision.get()),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{field}' is not orderable for computed fields"
                )));
            }
        })
    }

    fn default_sort() -> Vec<SortParam> {
        vec![SortParam {
            field: FilterField::Id,
            descending: false,
        }]
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        Self::default_sort()
    }
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ComputedFieldDefinitionRequest {
    pub key: String,
    pub label: String,
    #[serde(default)]
    pub description: String,
    #[schema(value_type = Object)]
    pub operation: serde_json::Value,
    pub result_type: ComputedResultType,
    #[serde(default = "enabled_by_default")]
    pub enabled: bool,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct PersonalComputedFieldDefinitionRequest {
    pub class_id: i32,
    #[serde(flatten)]
    pub definition: ComputedFieldDefinitionRequest,
}

const fn enabled_by_default() -> bool {
    true
}

impl ComputedFieldDefinitionRequest {
    pub fn validate(&self) -> Result<Definition, ApiError> {
        validated_definition(
            &self.key,
            &self.label,
            &self.description,
            &self.operation,
            self.result_type,
            self.enabled,
        )
        .map_err(|error| ApiError::BadRequest(error.to_string()))
    }
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ComputedFieldDefinitionPatch {
    pub key: Option<String>,
    pub label: Option<String>,
    pub description: Option<String>,
    #[schema(value_type = Option<Object>)]
    pub operation: Option<serde_json::Value>,
    pub result_type: Option<ComputedResultType>,
    pub enabled: Option<bool>,
}

fn validated_definition(
    key: &str,
    label: &str,
    description: &str,
    operation: &serde_json::Value,
    result_type: ComputedResultType,
    enabled: bool,
) -> Result<Definition, Box<dyn std::error::Error + Send + Sync>> {
    let operation: Operation = serde_json::from_value(operation.clone())?;
    let key = FieldKey::new(key)?;
    Ok(Definition::new(
        key,
        label,
        description,
        operation,
        result_type.into(),
        enabled,
    )?)
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ClassComputationState {
    pub class_id: i32,
    pub evaluation_revision: i64,
    pub rebuild_status: String,
    pub active_task_id: Option<i32>,
    pub last_error: Option<String>,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}

impl ClassComputationState {
    pub fn ready_without_definitions(class_id: i32) -> Self {
        let now = chrono::Utc::now().naive_utc();
        Self {
            class_id,
            evaluation_revision: 0,
            rebuild_status: "ready".to_string(),
            active_task_id: None,
            last_error: None,
            created_at: now,
            updated_at: now,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectComputedData {
    pub object_id: i32,
    pub class_id: i32,
    pub evaluation_revision: i64,
    pub source_data_sha256: String,
    pub values: serde_json::Value,
    pub errors: serde_json::Value,
    pub computed_at: NaiveDateTime,
}

#[derive(Debug, Clone)]
pub struct NewObjectComputedData {
    pub object_id: i32,
    pub class_id: i32,
    pub evaluation_revision: i64,
    pub source_data_sha256: String,
    pub values: serde_json::Value,
    pub errors: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ComputedFieldListResponse {
    pub definitions: Vec<ComputedFieldDefinition>,
    pub state: ClassComputationState,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ComputedFieldMutationResponse {
    pub definition: ComputedFieldDefinition,
    pub state: ClassComputationState,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ComputedFieldDeleteResponse {
    pub deleted_definition_id: i32,
    pub state: ClassComputationState,
}

#[derive(Debug, Clone)]
pub enum ComputedFieldPreviewSource {
    Object(HubuumObjectID),
    Inline(serde_json::Value),
}

#[derive(Debug, Clone)]
pub struct ComputedFieldPreviewRequest {
    class_id: Option<HubuumClassID>,
    definition: ComputedFieldDefinitionRequest,
    source: ComputedFieldPreviewSource,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
struct ComputedFieldPreviewWire {
    /// Required by the personal preview route and ignored on class-scoped routes.
    #[schema(value_type = Option<i32>)]
    class_id: Option<HubuumClassID>,
    definition: ComputedFieldDefinitionRequest,
    #[schema(value_type = Option<i32>)]
    object_id: Option<HubuumObjectID>,
    #[schema(value_type = Option<Object>)]
    data: Option<serde_json::Value>,
}

impl<'de> Deserialize<'de> for ComputedFieldPreviewRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = ComputedFieldPreviewWire::deserialize(deserializer)?;
        let source = match (wire.object_id, wire.data) {
            (Some(object_id), None) => ComputedFieldPreviewSource::Object(object_id),
            (None, Some(data)) => ComputedFieldPreviewSource::Inline(data),
            _ => {
                return Err(serde::de::Error::custom(
                    "preview requires exactly one of object_id or data",
                ));
            }
        };
        Ok(Self {
            class_id: wire.class_id,
            definition: wire.definition,
            source,
        })
    }
}

impl PartialSchema for ComputedFieldPreviewRequest {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        ComputedFieldPreviewWire::schema()
    }
}

impl ToSchema for ComputedFieldPreviewRequest {}

impl ComputedFieldPreviewRequest {
    pub const fn class_id(&self) -> Option<HubuumClassID> {
        self.class_id
    }

    pub const fn definition(&self) -> &ComputedFieldDefinitionRequest {
        &self.definition
    }

    pub const fn source(&self) -> &ComputedFieldPreviewSource {
        &self.source
    }
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ComputedFieldErrorResponse {
    pub code: String,
    pub path: Option<String>,
    pub message: String,
}

impl From<FieldError> for ComputedFieldErrorResponse {
    fn from(error: FieldError) -> Self {
        Self {
            code: error.code.as_str().to_string(),
            path: error.path,
            message: error.message,
        }
    }
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ComputedFieldPreviewResponse {
    pub value: serde_json::Value,
    pub error: Option<ComputedFieldErrorResponse>,
}

#[derive(Debug, Clone, Default, Serialize, ToSchema)]
pub struct ComputedScopeResponse {
    pub values: BTreeMap<String, serde_json::Value>,
    pub errors: BTreeMap<String, ComputedFieldErrorResponse>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct SharedComputedScopeResponse {
    pub revision: i64,
    pub materialization_stale: bool,
    pub values: BTreeMap<String, serde_json::Value>,
    pub errors: BTreeMap<String, ComputedFieldErrorResponse>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ComputedObjectScopesResponse {
    pub shared: SharedComputedScopeResponse,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub personal: Option<ComputedScopeResponse>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct HubuumObjectComputedResponse {
    #[serde(flatten)]
    pub object: HubuumObject,
    pub computed: ComputedObjectScopesResponse,
}

impl CursorPaginated for HubuumObjectComputedResponse {
    fn supports_sort(field: &FilterField) -> bool {
        field.computed_query().is_some() || HubuumObject::supports_sort(field)
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        let Some(computed_query) = field.computed_query() else {
            return self.object.cursor_value(field);
        };
        let values = match computed_query.scope() {
            crate::models::search::ComputedFieldScope::Shared => &self.computed.shared.values,
            crate::models::search::ComputedFieldScope::Personal => {
                &self
                    .computed
                    .personal
                    .as_ref()
                    .ok_or_else(|| {
                        ApiError::BadRequest(
                            "Personal computed fields are unavailable for this principal"
                                .to_string(),
                        )
                    })?
                    .values
            }
        };
        let value = values.get(computed_query.key()).ok_or_else(|| {
            ApiError::InternalServerError(format!(
                "Computed sort field '{}' is missing from the enriched object",
                computed_query.key()
            ))
        })?;
        if value.is_null() {
            return Ok(CursorValue::Null);
        }
        let value_type = computed_query.value_type().ok_or_else(|| {
            ApiError::InternalServerError(format!(
                "Computed sort field '{}' was not resolved",
                computed_query.key()
            ))
        })?;
        match value_type {
            ComputedQueryValueType::String => value
                .as_str()
                .map(|value| CursorValue::String(value.to_string())),
            ComputedQueryValueType::Number | ComputedQueryValueType::Integer => value
                .is_number()
                .then(|| CursorValue::Decimal(value.to_string())),
            ComputedQueryValueType::Boolean => value.as_bool().map(CursorValue::Boolean),
            ComputedQueryValueType::Object if value.is_object() => {
                Some(CursorValue::Json(value.clone()))
            }
            ComputedQueryValueType::Array if value.is_array() => {
                Some(CursorValue::Json(value.clone()))
            }
            ComputedQueryValueType::Object | ComputedQueryValueType::Array => None,
        }
        .ok_or_else(|| {
            ApiError::InternalServerError(format!(
                "Computed sort field '{}' does not match its declared result type",
                computed_query.key()
            ))
        })
    }

    fn default_sort() -> Vec<SortParam> {
        HubuumObject::default_sort()
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        HubuumObject::tie_breaker_sort()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_validation_uses_typed_evaluator_contract() {
        let request = ComputedFieldDefinitionRequest {
            key: "average_load".to_string(),
            label: "Average load".to_string(),
            description: String::new(),
            operation: serde_json::json!({
                "type": "average",
                "paths": ["/load/one", "/load/five"]
            }),
            result_type: ComputedResultType::Number,
            enabled: true,
        };

        assert!(request.validate().is_ok());
    }

    #[test]
    fn invalid_operation_shape_is_a_bad_request() {
        let request = ComputedFieldDefinitionRequest {
            key: "average_load".to_string(),
            label: "Average load".to_string(),
            description: String::new(),
            operation: serde_json::json!({"type": "average", "paths": []}),
            result_type: ComputedResultType::Number,
            enabled: true,
        };

        assert!(matches!(request.validate(), Err(ApiError::BadRequest(_))));
    }
}
