//! Shared building blocks for the per-resource history read API:
//! a response wrapper that adds the actor's username, actor-id resolution,
//! `as_of` query parsing, and macros that implement cursor pagination and the
//! DB fetch functions for each history table.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use diesel::prelude::*;
use serde::Serialize;

use crate::db::{with_connection, DbPool};
use crate::errors::ApiError;

/// A serialized history row plus the resolved username of its actor (if any).
#[derive(Serialize)]
#[allow(dead_code)]
pub struct HistoryResponse<T: Serialize> {
    #[serde(flatten)]
    pub entry: T,
    pub actor_username: Option<String>,
}

/// Parse the required `at=<rfc3339>` query parameter for the as-of endpoint.
#[allow(dead_code)]
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

/// Batch-resolve a set of actor ids to usernames (anonymized users keep their
/// tombstoned username; ids with no matching user are simply absent).
#[allow(dead_code)]
pub async fn resolve_actor_usernames(
    pool: &DbPool,
    mut actor_ids: Vec<i32>,
) -> Result<HashMap<i32, String>, ApiError> {
    use crate::schema::users::dsl::{id, username, users};
    actor_ids.sort_unstable();
    actor_ids.dedup();
    if actor_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let rows: Vec<(i32, String)> = with_connection(pool, |conn| {
        users
            .filter(id.eq_any(&actor_ids))
            .select((id, username))
            .load(conn)
    })?;
    Ok(rows.into_iter().collect())
}

/// Implement `CursorPaginated` + `CursorSqlMapping` for a history Queryable
/// type. `$table` is the history table name as it appears in SQL (used to
/// build fully-qualified column references for the keyset/ORDER BY clauses).
#[macro_export]
macro_rules! impl_history_pagination {
    ($ty:ty, $table:literal) => {
        impl $crate::traits::CursorPaginated for $ty {
            fn supports_sort(field: &$crate::models::search::FilterField) -> bool {
                matches!(
                    field,
                    $crate::models::search::FilterField::ValidFrom
                        | $crate::models::search::FilterField::HistoryId
                )
            }

            fn cursor_value(
                &self,
                field: &$crate::models::search::FilterField,
            ) -> Result<$crate::traits::CursorValue, $crate::errors::ApiError> {
                Ok(match field {
                    $crate::models::search::FilterField::ValidFrom => {
                        $crate::traits::CursorValue::DateTime(self.valid_from.naive_utc())
                    }
                    $crate::models::search::FilterField::HistoryId => {
                        $crate::traits::CursorValue::Integer(self.history_id)
                    }
                    other => {
                        return Err($crate::errors::ApiError::BadRequest(format!(
                            "Field '{}' is not orderable for history",
                            other
                        )))
                    }
                })
            }

            fn default_sort() -> Vec<$crate::models::search::SortParam> {
                vec![$crate::models::search::SortParam {
                    field: $crate::models::search::FilterField::ValidFrom,
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
                    $crate::models::search::FilterField::ValidFrom => {
                        $crate::traits::CursorSqlField {
                            column: concat!($table, ".valid_from"),
                            sql_type: $crate::traits::CursorSqlType::DateTime,
                            nullable: false,
                        }
                    }
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
                        )))
                    }
                })
            }
        }
    };
}

/// Generate the two DB fetch functions for a history table:
/// - `$paginate_fn(entity_id, pool, &QueryOptions) -> (Vec<$ty>, i64)` — a page
///   of versions for one entity plus the total version count.
/// - `$as_of_fn(entity_id, at, pool) -> Option<$ty>` — the version valid at `at`.
///
/// `$schema` is the diesel schema module path, e.g. `crate::schema::hubuumclass_history`.
#[macro_export]
macro_rules! history_db_fns {
    ($paginate_fn:ident, $as_of_fn:ident, $($schema:tt)::+, $ty:ty) => {
        pub async fn $paginate_fn(
            entity_id: i32,
            pool: &$crate::db::DbPool,
            query_options: &$crate::models::search::QueryOptions,
        ) -> Result<(Vec<$ty>, i64), $crate::errors::ApiError> {
            use diesel::prelude::*;
            use $($schema)::+::dsl::*;
            let total = $crate::db::with_connection(pool, |conn| {
                $($schema)::+::table
                    .filter(id.eq(entity_id))
                    .count()
                    .get_result::<i64>(conn)
            })?;
            let mut query = $($schema)::+::table.into_boxed().filter(id.eq(entity_id));
            $crate::apply_query_options!(query, query_options, $ty);
            let items =
                $crate::db::with_connection(pool, |conn| query.load::<$ty>(conn))?;
            Ok((items, total))
        }

        pub async fn $as_of_fn(
            entity_id: i32,
            at: chrono::DateTime<chrono::Utc>,
            pool: &$crate::db::DbPool,
        ) -> Result<Option<$ty>, $crate::errors::ApiError> {
            use diesel::prelude::*;
            use $($schema)::+::dsl::*;
            $crate::db::with_connection(pool, |conn| {
                $($schema)::+::table
                    .into_boxed()
                    .filter(id.eq(entity_id))
                    .filter(valid_from.le(at))
                    .filter(valid_to.is_null().or(valid_to.gt(at)))
                    .order(history_id.desc())
                    .first::<$ty>(conn)
                    .optional()
            })
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_as_of_reads_rfc3339() {
        let dt = parse_as_of("at=2026-01-02T03:04:05Z").unwrap();
        assert_eq!(dt, DateTime::parse_from_rfc3339("2026-01-02T03:04:05Z").unwrap());
    }

    #[test]
    fn parse_as_of_requires_param() {
        assert!(matches!(parse_as_of("foo=bar"), Err(ApiError::BadRequest(_))));
    }

    #[test]
    fn parse_as_of_rejects_garbage() {
        assert!(matches!(parse_as_of("at=not-a-date"), Err(ApiError::BadRequest(_))));
    }
}
