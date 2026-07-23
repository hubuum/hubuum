//! Shared building blocks for the per-resource history read API:
//! a response wrapper that adds the actor's username and `as_of` query parsing.

use chrono::{DateTime, Utc};
use serde::Serialize;
use utoipa::ToSchema;

use crate::db::traits::authz::scope_allows;
use crate::errors::ApiError;
use crate::models::collection::user_can_on_any;
use crate::models::search::QueryOptions;
use crate::models::{HistoryAuthorizationSnapshot, Permissions, TokenScope};
use crate::pagination::count_query_options;
use crate::permissions::visibility::authorize_cursor_page;
use crate::permissions::{AppContext, PrincipalRef, authorize_resources};
use crate::traits::{AuthzSubject, CursorPaginated};

pub use crate::db::traits::history::resolve_actor_usernames;

/// A serialized history row plus the resolved username of its actor (if any).
#[derive(Serialize, ToSchema)]
pub struct HistoryResponse<T: Serialize + ToSchema> {
    #[serde(flatten)]
    pub entry: T,
    pub actor_username: Option<String>,
}

/// Authorize one historical resource shape, including its stored attributes.
pub async fn authorize_history_snapshot<S>(
    context: &AppContext,
    subject: &S,
    scopes: Option<&TokenScope>,
    permission: Permissions,
    snapshot: HistoryAuthorizationSnapshot,
) -> Result<(), ApiError>
where
    S: AuthzSubject + ?Sized,
{
    authorize_resources(
        context.permission_backend(),
        context,
        subject,
        scopes,
        vec![permission],
        vec![snapshot.into_resource()],
    )
    .await
}

/// Filter complete historical candidates through the configured policy backend,
/// then count and paginate only the visible rows.
pub async fn authorize_history_page<S, T, F>(
    context: &AppContext,
    subject: &S,
    scopes: Option<&TokenScope>,
    permission: Permissions,
    candidates: Vec<T>,
    query_options: &QueryOptions,
    to_snapshot: F,
) -> Result<(Vec<T>, i64), ApiError>
where
    S: AuthzSubject + ?Sized,
    T: CursorPaginated,
    F: Fn(&T) -> HistoryAuthorizationSnapshot,
{
    if !scope_allows(scopes, &[permission]) {
        return Err(ApiError::Forbidden("Permission denied".to_string()));
    }
    let principal = PrincipalRef::load(context, subject).await?;
    let page = authorize_cursor_page(
        context.permission_backend(),
        &principal,
        candidates,
        scopes,
        vec![permission],
        query_options,
        |candidate| {
            to_snapshot(candidate)
                .into_resource()
                .normalized_for_permission(permission)
        },
    )
    .await?;
    Ok((page.rows, page.total_count))
}

/// Load every non-permission-filtered candidate before external authorization.
pub fn history_candidate_query_options(query_options: &QueryOptions) -> QueryOptions {
    let mut candidates = count_query_options(query_options);
    candidates.include_total = false;
    candidates
}

/// Resolve the collection ids visible through the local SQL permission store.
pub async fn readable_history_collection_ids<S>(
    context: &AppContext,
    subject: &S,
    scopes: Option<&TokenScope>,
    permission: Permissions,
) -> Result<Vec<i32>, ApiError>
where
    S: AuthzSubject + ?Sized,
{
    let mut collection_ids = user_can_on_any(context, subject, permission, scopes)
        .await?
        .into_iter()
        .map(|collection| collection.id)
        .collect::<Vec<_>>();
    collection_ids.sort_unstable();
    collection_ids.dedup();
    Ok(collection_ids)
}

/// Deleted resources have no live row to authorize against. Only unscoped
/// admins may use the history endpoints as a compliance/audit surface for
/// those tombstones; ordinary callers must pass the normal live-resource check.
pub async fn can_read_deleted_history<S>(
    context: &AppContext,
    subject: &S,
    token_scoped: bool,
) -> Result<bool, ApiError>
where
    S: AuthzSubject + ?Sized,
{
    if token_scoped {
        return Ok(false);
    }
    let principal = PrincipalRef::load(context, subject).await?;
    context.permission_backend().is_admin(&principal).await
}

/// Parse the required `at=<rfc3339>` query parameter for the as-of endpoint.
pub fn parse_as_of(query_string: &str) -> Result<DateTime<Utc>, ApiError> {
    let (_opts, passthrough) =
        crate::models::search::parse_query_parameter_with_passthrough(query_string, &["at"])?;
    let at = passthrough
        .get("at")
        .and_then(|values| values.as_slice().first())
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
    use std::sync::Arc;

    use super::*;
    use crate::models::HubuumClassHistory;
    use crate::models::search::parse_query_parameter;
    use crate::pagination::prepare_db_pagination;
    use crate::permissions::test_support::{MockAllowRule, MockTreetopBackend};
    use crate::permissions::{ResourceAttrs, ResourceKind};
    use crate::tests::{TestContext, create_test_group};

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

    #[actix_web::test]
    async fn deleted_history_admin_check_uses_the_configured_backend() {
        let test = TestContext::new().await;
        let policy_group = create_test_group(&test.pool).await;
        policy_group
            .add_member_without_events(&test.pool, &test.normal_user)
            .await
            .unwrap();

        let allow_backend = MockTreetopBackend::new();
        allow_backend.add_admin_rule(policy_group.id);
        let allow_context = AppContext::new(test.pool.get_ref().clone(), Arc::new(allow_backend));
        assert!(
            can_read_deleted_history(&allow_context, &test.normal_user, false)
                .await
                .unwrap()
        );

        let deny_context = AppContext::new(
            test.pool.get_ref().clone(),
            Arc::new(MockTreetopBackend::new()),
        );
        assert!(
            !can_read_deleted_history(&deny_context, &test.admin_user, false)
                .await
                .unwrap()
        );

        policy_group
            .delete_without_events(&test.pool)
            .await
            .unwrap();
    }

    #[actix_web::test]
    async fn historical_resource_attributes_are_sent_to_external_backends() {
        let test = TestContext::new().await;
        let policy_group = create_test_group(&test.pool).await;
        policy_group
            .add_member_without_events(&test.pool, &test.normal_user)
            .await
            .unwrap();

        let backend = Arc::new(MockTreetopBackend::new());
        backend.add_rule(MockAllowRule {
            group_id: policy_group.id,
            action: Permissions::ReadClass,
            resource_kind: ResourceKind::Class,
            resource_id: Some(41),
            attrs: ResourceAttrs {
                collection_id: Some(73),
                name: Some("visible-version".to_string()),
                ..Default::default()
            },
        });
        let context = AppContext::new(test.pool.get_ref().clone(), backend.clone());
        let timestamp = Utc::now();
        let visible = HubuumClassHistory {
            id: 41,
            name: "visible-version".to_string(),
            collection_id: 73,
            json_schema: None,
            validate_schema: false,
            description: "visible".to_string(),
            created_at: timestamp.naive_utc(),
            updated_at: timestamp.naive_utc(),
            op: "U".to_string(),
            valid_from: timestamp,
            valid_to: None,
            actor_id: None,
            history_id: 2,
        };
        authorize_history_snapshot(
            &context,
            &test.normal_user,
            None,
            Permissions::ReadClass,
            HistoryAuthorizationSnapshot::from(&visible),
        )
        .await
        .unwrap();

        let hidden = HubuumClassHistory {
            name: "hidden-version".to_string(),
            description: "hidden".to_string(),
            history_id: 1,
            ..visible.clone()
        };
        let params = parse_query_parameter("limit=10").unwrap();
        let query_options = prepare_db_pagination::<HubuumClassHistory>(&params).unwrap();
        let (rows, total_count) = authorize_history_page(
            &context,
            &test.normal_user,
            None,
            Permissions::ReadClass,
            vec![visible, hidden],
            &query_options,
            |row| HistoryAuthorizationSnapshot::from(row),
        )
        .await
        .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].name, "visible-version");
        assert_eq!(total_count, 1);
        assert_eq!(backend.authorization_batch_sizes(), vec![1, 2]);

        policy_group
            .delete_without_events(&test.pool)
            .await
            .unwrap();
    }
}
