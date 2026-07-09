//! Shared building blocks for the per-resource history read API:
//! a response wrapper that adds the actor's username and `as_of` query parsing.

use chrono::{DateTime, Utc};
use serde::Serialize;
use utoipa::ToSchema;

use crate::db::DbPool;
use crate::db::traits::authz::AuthzSubject;
use crate::errors::ApiError;
use crate::extractors::Authenticated;

pub use crate::db::traits::history::resolve_actor_usernames;

/// A serialized history row plus the resolved username of its actor (if any).
#[derive(Serialize, ToSchema)]
pub struct HistoryResponse<T: Serialize + ToSchema> {
    #[serde(flatten)]
    pub entry: T,
    pub actor_username: Option<String>,
}

/// Deleted resources have no live row to authorize against. Only unscoped
/// admins may use the history endpoints as a compliance/audit surface for
/// those tombstones; ordinary callers must pass the normal live-resource check.
pub async fn can_read_deleted_history(
    pool: &DbPool,
    requestor: &Authenticated,
) -> Result<bool, ApiError> {
    if requestor.scopes().is_some() {
        return Ok(false);
    }
    requestor.principal.is_admin(pool).await
}

/// Parse the required `at=<rfc3339>` query parameter for the as-of endpoint.
pub fn parse_as_of(query_string: &str) -> Result<DateTime<Utc>, ApiError> {
    let (_opts, passthrough) =
        crate::models::search::parse_query_parameter_with_passthrough(query_string, &["at"])?;
    let at = passthrough
        .get("at")
        .and_then(|values| values.first())
        .ok_or_else(|| ApiError::BadRequest("missing required 'at' parameter".into()))?;
    DateTime::parse_from_rfc3339(at)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|_| ApiError::BadRequest(format!("invalid rfc3339 timestamp: {at}")))
}

/// Implement `CursorPaginated` + `CursorSqlMapping` for a history Queryable
/// type. `$table` is the history table name as it appears in SQL (used to
/// build fully-qualified column references for the keyset/ORDER BY clauses).
#[macro_export]
macro_rules! impl_history_pagination {
    ($ty:ty, $table:literal) => {
        impl $crate::traits::CursorPaginated for $ty {
            fn supports_sort(field: &$crate::models::search::FilterField) -> bool {
                matches!(field, $crate::models::search::FilterField::HistoryId)
            }

            fn cursor_value(
                &self,
                field: &$crate::models::search::FilterField,
            ) -> Result<$crate::traits::CursorValue, $crate::errors::ApiError> {
                Ok(match field {
                    $crate::models::search::FilterField::HistoryId => {
                        $crate::traits::CursorValue::Integer(self.history_id)
                    }
                    other => {
                        return Err($crate::errors::ApiError::BadRequest(format!(
                            "Field '{}' is not orderable for history",
                            other
                        )));
                    }
                })
            }

            fn default_sort() -> Vec<$crate::models::search::SortParam> {
                vec![$crate::models::search::SortParam {
                    field: $crate::models::search::FilterField::HistoryId,
                    descending: true,
                }]
            }

            fn tie_breaker_sort() -> Vec<$crate::models::search::SortParam> {
                vec![$crate::models::search::SortParam {
                    field: $crate::models::search::FilterField::HistoryId,
                    descending: true,
                }]
            }
        }

        impl $crate::traits::CursorSqlMapping for $ty {
            fn sql_field(
                field: &$crate::models::search::FilterField,
            ) -> Result<$crate::traits::CursorSqlField, $crate::errors::ApiError> {
                Ok(match field {
                    $crate::models::search::FilterField::HistoryId => {
                        $crate::traits::CursorSqlField {
                            column: concat!($table, ".history_id"),
                            sql_type: $crate::traits::CursorSqlType::Integer,
                            nullable: false,
                        }
                    }
                    other => {
                        return Err($crate::errors::ApiError::BadRequest(format!(
                            "Field '{}' is not orderable for history",
                            other
                        )));
                    }
                })
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_as_of_reads_rfc3339() {
        let dt = parse_as_of("at=2026-01-02T03:04:05Z").unwrap();
        assert_eq!(
            dt,
            DateTime::parse_from_rfc3339("2026-01-02T03:04:05Z").unwrap()
        );
    }

    #[test]
    fn parse_as_of_requires_param() {
        assert!(matches!(
            parse_as_of("foo=bar"),
            Err(ApiError::BadRequest(_))
        ));
    }

    #[test]
    fn parse_as_of_rejects_garbage() {
        assert!(matches!(
            parse_as_of("at=not-a-date"),
            Err(ApiError::BadRequest(_))
        ));
    }
}
