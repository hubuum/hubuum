//! Shared building blocks for the per-resource history read API:
//! a response wrapper that adds the actor's username and `as_of` query parsing.

use chrono::{DateTime, Utc};
use serde::Serialize;
use utoipa::ToSchema;

use crate::db::traits::UserPermissions;
use crate::errors::ApiError;
use crate::models::{CollectionID, HistoryAuthorizationSnapshot, Permissions};
use crate::permissions::{AppContext, PrincipalRef, authorize_resources};
use crate::traits::AuthzSubject;

pub use crate::db::traits::history::resolve_actor_usernames;

/// A serialized history row plus the resolved username of its actor (if any).
#[derive(Serialize, ToSchema)]
pub struct HistoryResponse<T: Serialize + ToSchema> {
    #[serde(flatten)]
    pub entry: T,
    pub actor_username: Option<String>,
}

/// Require the requested permission on every permission-relevant historical
/// resource shape before returning a history page.
///
/// The local backend can decide the distinct collection set in one SQL query.
/// External policy backends receive the complete historical resource
/// snapshots so policies that use names or class IDs retain their semantics.
pub async fn authorize_history_snapshots<S>(
    context: &AppContext,
    subject: &S,
    scopes: Option<&[Permissions]>,
    permission: Permissions,
    snapshots: Vec<HistoryAuthorizationSnapshot>,
) -> Result<(), ApiError>
where
    S: AuthzSubject + ?Sized,
{
    if context.permission_backend().uses_sql_permission_store() {
        let mut collection_ids = snapshots
            .iter()
            .map(HistoryAuthorizationSnapshot::collection_id)
            .collect::<Vec<_>>();
        collection_ids.sort_unstable();
        collection_ids.dedup();
        let collections = collection_ids
            .into_iter()
            .map(CollectionID::new)
            .collect::<Result<Vec<_>, _>>()?;
        return subject
            .can(context, [permission], collections, scopes)
            .await;
    }

    authorize_resources(
        context.permission_backend(),
        context,
        subject,
        scopes,
        vec![permission],
        snapshots
            .into_iter()
            .map(HistoryAuthorizationSnapshot::into_resource)
            .collect(),
    )
    .await
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
        let visible = HistoryAuthorizationSnapshot::class(41, 73, "visible-version".to_string());
        authorize_history_snapshots(
            &context,
            &test.normal_user,
            None,
            Permissions::ReadClass,
            vec![visible.clone()],
        )
        .await
        .unwrap();

        let result = authorize_history_snapshots(
            &context,
            &test.normal_user,
            None,
            Permissions::ReadClass,
            vec![
                visible,
                HistoryAuthorizationSnapshot::class(41, 73, "hidden-version".to_string()),
            ],
        )
        .await;

        assert!(matches!(result, Err(ApiError::Forbidden(_))));
        assert_eq!(backend.authorization_batch_sizes(), vec![1, 2]);

        policy_group
            .delete_without_events(&test.pool)
            .await
            .unwrap();
    }
}
