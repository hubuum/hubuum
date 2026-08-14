//! Transitional root projections used by legacy tests and service-account SQL.
//!
//! Bearer-token lifecycle behavior lives in `hubuum-storage-postgres`. The
//! remaining row projection supports legacy test helpers until they move to
//! backend-neutral compatibility fixtures.

use crate::pagination::{CursorSqlField, CursorSqlMapping, CursorSqlType};
use crate::storage::postgres::prelude::*;

use crate::errors::ApiError;
use crate::models::PrincipalToken;
use crate::models::search::{FilterField, SortParam};
use crate::traits::{CursorPaginated, CursorValue};

#[derive(Queryable, Selectable, Clone)]
#[diesel(table_name = crate::schema::tokens)]
pub(crate) struct PrincipalTokenRow {
    pub(crate) id: i32,
    pub(crate) token: String,
    pub(crate) principal_id: i32,
    pub(crate) name: Option<String>,
    pub(crate) description: Option<String>,
    pub(crate) issued: chrono::NaiveDateTime,
    pub(crate) expires_at: Option<chrono::NaiveDateTime>,
    pub(crate) last_used_at: Option<chrono::NaiveDateTime>,
    pub(crate) revoked_at: Option<chrono::NaiveDateTime>,
    pub(crate) permission_scoped: bool,
    pub(crate) resource_scoped: bool,
    pub(crate) revision: PostgresRevision,
}

impl From<PrincipalTokenRow> for PrincipalToken {
    fn from(row: PrincipalTokenRow) -> Self {
        Self {
            id: row.id,
            token: row.token,
            principal_id: row.principal_id,
            name: row.name,
            description: row.description,
            issued: row.issued,
            expires_at: row.expires_at,
            last_used_at: row.last_used_at,
            revoked_at: row.revoked_at,
            permission_scoped: row.permission_scoped,
            resource_scoped: row.resource_scoped,
            revision: row.revision.into_domain(),
        }
    }
}

impl CursorPaginated for PrincipalTokenRow {
    fn supports_sort(field: &FilterField) -> bool {
        PrincipalToken::supports_sort(field)
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        PrincipalToken::from(self.clone()).cursor_value(field)
    }

    fn default_sort() -> Vec<SortParam> {
        PrincipalToken::default_sort()
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        PrincipalToken::tie_breaker_sort()
    }
}

impl CursorSqlMapping for PrincipalTokenRow {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::Id => CursorSqlField {
                column: "tokens.id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Name => CursorSqlField {
                column: "tokens.name",
                sql_type: CursorSqlType::String,
                nullable: true,
            },
            FilterField::IssuedAt => CursorSqlField {
                column: "tokens.issued",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::ExpiresAt => CursorSqlField {
                column: "tokens.expires_at",
                sql_type: CursorSqlType::DateTime,
                nullable: true,
            },
            FilterField::LastUsedAt => CursorSqlField {
                column: "tokens.last_used_at",
                sql_type: CursorSqlType::DateTime,
                nullable: true,
            },
            FilterField::Revision => CursorSqlField {
                column: "tokens.revision",
                sql_type: CursorSqlType::BigInt,
                nullable: false,
            },
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for tokens",
                    field
                )));
            }
        })
    }
}
