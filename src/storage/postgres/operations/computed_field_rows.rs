use chrono::NaiveDateTime;
use diesel::{Insertable, Queryable, Selectable};

use crate::errors::ApiError;
use crate::models::search::{FilterField, SortParam};
use crate::models::{
    ClassComputationState, ComputedFieldDefinition, NewComputedFieldDefinition,
    NewObjectComputedData, ObjectComputedData, ResourceRevision,
};
use crate::pagination::{
    CursorPaginated, CursorSqlField, CursorSqlMapping, CursorSqlType, CursorValue,
};
use crate::schema::{class_computation_state, computed_field_definitions, object_computed_data};

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = computed_field_definitions)]
pub struct ComputedFieldDefinitionRow {
    pub id: i32,
    pub class_id: i32,
    pub visibility: String,
    pub owner_user_id: Option<i32>,
    pub key: String,
    pub label: String,
    pub description: String,
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

impl ComputedFieldDefinitionRow {
    pub fn evaluator_definition(&self) -> Result<hubuum_computed_fields::Definition, ApiError> {
        ComputedFieldDefinition::from(self.clone()).evaluator_definition()
    }

    pub fn is_shared(&self) -> bool {
        self.visibility == crate::models::COMPUTED_FIELD_VISIBILITY_SHARED
    }

    pub fn is_personal_for(&self, owner_id: i32) -> bool {
        self.visibility == crate::models::COMPUTED_FIELD_VISIBILITY_PERSONAL
            && self.owner_user_id == Some(owner_id)
    }
}

impl From<ComputedFieldDefinitionRow> for ComputedFieldDefinition {
    fn from(row: ComputedFieldDefinitionRow) -> Self {
        Self {
            id: row.id,
            class_id: row.class_id,
            visibility: row.visibility,
            owner_user_id: row.owner_user_id,
            key: row.key,
            label: row.label,
            description: row.description,
            operation: row.operation,
            result_type: row.result_type,
            enabled: row.enabled,
            revision: row.revision,
            semantics_version: row.semantics_version,
            created_by: row.created_by,
            updated_by: row.updated_by,
            created_at: row.created_at,
            updated_at: row.updated_at,
        }
    }
}

impl CursorPaginated for ComputedFieldDefinitionRow {
    fn supports_sort(field: &FilterField) -> bool {
        ComputedFieldDefinition::supports_sort(field)
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
        ComputedFieldDefinition::default_sort()
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        Self::default_sort()
    }
}

impl CursorSqlMapping for ComputedFieldDefinitionRow {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::Id => CursorSqlField {
                column: "computed_field_definitions.id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Name => CursorSqlField {
                column: "computed_field_definitions.key",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::ClassId => CursorSqlField {
                column: "computed_field_definitions.class_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::CreatedAt => CursorSqlField {
                column: "computed_field_definitions.created_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::UpdatedAt => CursorSqlField {
                column: "computed_field_definitions.updated_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::Revision => CursorSqlField {
                column: "computed_field_definitions.revision",
                sql_type: CursorSqlType::BigInt,
                nullable: false,
            },
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{field}' is not orderable for computed fields"
                )));
            }
        })
    }
}

#[derive(Debug, Clone, Insertable)]
#[diesel(table_name = computed_field_definitions)]
pub struct NewComputedFieldDefinitionRow {
    pub class_id: i32,
    pub visibility: String,
    pub owner_user_id: Option<i32>,
    pub key: String,
    pub label: String,
    pub description: String,
    pub operation: serde_json::Value,
    pub result_type: String,
    pub enabled: bool,
    pub semantics_version: i16,
    pub created_by: Option<i32>,
    pub updated_by: Option<i32>,
}

impl From<NewComputedFieldDefinition> for NewComputedFieldDefinitionRow {
    fn from(row: NewComputedFieldDefinition) -> Self {
        Self {
            class_id: row.class_id,
            visibility: row.visibility,
            owner_user_id: row.owner_user_id,
            key: row.key,
            label: row.label,
            description: row.description,
            operation: row.operation,
            result_type: row.result_type,
            enabled: row.enabled,
            semantics_version: row.semantics_version,
            created_by: row.created_by,
            updated_by: row.updated_by,
        }
    }
}

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = class_computation_state)]
pub struct ClassComputationStateRow {
    pub class_id: i32,
    pub evaluation_revision: i64,
    pub rebuild_status: String,
    pub active_task_id: Option<i32>,
    pub last_error: Option<String>,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}

impl ClassComputationStateRow {
    pub fn ready_without_definitions(class_id: i32) -> Self {
        ClassComputationState::ready_without_definitions(class_id).into()
    }
}

impl From<ClassComputationStateRow> for ClassComputationState {
    fn from(row: ClassComputationStateRow) -> Self {
        Self {
            class_id: row.class_id,
            evaluation_revision: row.evaluation_revision,
            rebuild_status: row.rebuild_status,
            active_task_id: row.active_task_id,
            last_error: row.last_error,
            created_at: row.created_at,
            updated_at: row.updated_at,
        }
    }
}

impl From<ClassComputationState> for ClassComputationStateRow {
    fn from(state: ClassComputationState) -> Self {
        Self {
            class_id: state.class_id,
            evaluation_revision: state.evaluation_revision,
            rebuild_status: state.rebuild_status,
            active_task_id: state.active_task_id,
            last_error: state.last_error,
            created_at: state.created_at,
            updated_at: state.updated_at,
        }
    }
}

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = object_computed_data)]
pub struct ObjectComputedDataRow {
    pub object_id: i32,
    pub class_id: i32,
    pub evaluation_revision: i64,
    pub source_data_sha256: String,
    pub values: serde_json::Value,
    pub errors: serde_json::Value,
    pub computed_at: NaiveDateTime,
}

impl From<ObjectComputedDataRow> for ObjectComputedData {
    fn from(row: ObjectComputedDataRow) -> Self {
        Self {
            object_id: row.object_id,
            class_id: row.class_id,
            evaluation_revision: row.evaluation_revision,
            source_data_sha256: row.source_data_sha256,
            values: row.values,
            errors: row.errors,
            computed_at: row.computed_at,
        }
    }
}

#[derive(Debug, Clone, Insertable)]
#[diesel(table_name = object_computed_data)]
pub struct NewObjectComputedDataRow {
    pub object_id: i32,
    pub class_id: i32,
    pub evaluation_revision: i64,
    pub source_data_sha256: String,
    pub values: serde_json::Value,
    pub errors: serde_json::Value,
}

impl From<NewObjectComputedData> for NewObjectComputedDataRow {
    fn from(row: NewObjectComputedData) -> Self {
        Self {
            object_id: row.object_id,
            class_id: row.class_id,
            evaluation_revision: row.evaluation_revision,
            source_data_sha256: row.source_data_sha256,
            values: row.values,
            errors: row.errors,
        }
    }
}
