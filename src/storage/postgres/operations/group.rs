//! Transitional application projections for root-owned PostgreSQL workflows.
//!
//! Group lifecycle and membership persistence live in
//! `hubuum-storage-postgres`. This row remains only while the remaining
//! application workflows that join groups are moved behind storage contracts.

use crate::errors::ApiError;
use crate::models::search::{FilterField, SortParam};
use crate::models::{Group, ResourceRevision};
use crate::pagination::{CursorSqlField, CursorSqlMapping, CursorSqlType};
use crate::traits::{CursorPaginated, CursorValue};
use diesel::{Queryable, Selectable};

#[derive(Debug, Queryable, Selectable, Clone)]
#[diesel(table_name = crate::schema::groups)]
pub(crate) struct GroupRow {
    pub(crate) id: i32,
    pub(crate) groupname: String,
    pub(crate) description: String,
    pub(crate) created_at: chrono::NaiveDateTime,
    pub(crate) updated_at: chrono::NaiveDateTime,
    pub(crate) identity_scope_id: i32,
    pub(crate) managed_by: String,
    pub(crate) external_key: Option<String>,
    pub(crate) last_sync_attempted_at: Option<chrono::NaiveDateTime>,
    pub(crate) last_sync_success_at: Option<chrono::NaiveDateTime>,
    pub(crate) revision: crate::storage::postgres::PostgresRevision,
}

impl From<GroupRow> for Group {
    fn from(row: GroupRow) -> Self {
        Self {
            id: row.id,
            groupname: row.groupname,
            description: row.description,
            created_at: row.created_at,
            updated_at: row.updated_at,
            identity_scope_id: row.identity_scope_id,
            managed_by: row.managed_by,
            external_key: row.external_key,
            last_sync_attempted_at: row.last_sync_attempted_at,
            last_sync_success_at: row.last_sync_success_at,
            revision: ResourceRevision::from(row.revision),
        }
    }
}

impl CursorPaginated for GroupRow {
    fn supports_sort(field: &FilterField) -> bool {
        Group::supports_sort(field)
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        Ok(match field {
            FilterField::Id => CursorValue::Integer(self.id as i64),
            FilterField::Name | FilterField::Groupname => {
                CursorValue::String(self.groupname.clone())
            }
            FilterField::Description => CursorValue::String(self.description.clone()),
            FilterField::CreatedAt => CursorValue::DateTime(self.created_at),
            FilterField::UpdatedAt => CursorValue::DateTime(self.updated_at),
            FilterField::Revision => CursorValue::Integer(self.revision.get()),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for groups",
                    field
                )));
            }
        })
    }

    fn default_sort() -> Vec<SortParam> {
        Group::default_sort()
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        Group::tie_breaker_sort()
    }
}

impl CursorSqlMapping for GroupRow {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::Id => cursor_field("groups.id", CursorSqlType::Integer),
            FilterField::Name | FilterField::Groupname => {
                cursor_field("groups.groupname", CursorSqlType::String)
            }
            FilterField::Description => cursor_field("groups.description", CursorSqlType::String),
            FilterField::CreatedAt => cursor_field("groups.created_at", CursorSqlType::DateTime),
            FilterField::UpdatedAt => cursor_field("groups.updated_at", CursorSqlType::DateTime),
            FilterField::Revision => cursor_field("groups.revision", CursorSqlType::BigInt),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for groups",
                    field
                )));
            }
        })
    }
}

const fn cursor_field(column: &'static str, sql_type: CursorSqlType) -> CursorSqlField {
    CursorSqlField {
        column,
        sql_type,
        nullable: false,
    }
}

pub async fn principal_group_by_ids(
    pool: &crate::storage::postgres::PostgresPool,
    principal_id: i32,
    group_id: i32,
) -> Result<crate::models::PrincipalGroup, ApiError> {
    let runtime = hubuum_storage_postgres::PostgresRuntime::new(pool.clone());
    hubuum_storage_postgres::operations::group::load_principal_group(
        &runtime,
        principal_id,
        group_id,
    )
    .await
    .map_err(hubuum_storage_core::StorageError::from)
    .map_err(ApiError::from)
    .and_then(crate::services::storage_boundary::principal_group_from_storage)
}
