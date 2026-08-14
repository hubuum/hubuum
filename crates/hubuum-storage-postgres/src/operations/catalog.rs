//! PostgreSQL implementation of ordinary catalog listing and filtering.

use diesel::prelude::{ExpressionMethods, QueryDsl};
use diesel::{JoinOnDsl, Queryable, Selectable, SelectableHelper};
use diesel_async::RunQueryDsl;
use hubuum_query::{FilterField, ParsedQueryParam, QueryOptions};
use hubuum_storage_core::{
    AuthorizationPermission, CatalogListQuery, CatalogPage, StorageCollection,
    StorageRecordMetadata, UnifiedSearchResourceScope,
};

use crate::cursor::{CursorSqlField, CursorSqlType};
use crate::{PostgresRevision, PostgresRuntime, PostgresStorageError};

#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::schema::collections)]
struct CollectionCatalogRow {
    id: i32,
    name: String,
    description: String,
    created_at: chrono::NaiveDateTime,
    updated_at: chrono::NaiveDateTime,
    parent_collection_id: Option<i32>,
    revision: PostgresRevision,
}

impl CollectionCatalogRow {
    fn into_storage(self) -> StorageCollection {
        StorageCollection::new(
            StorageRecordMetadata::new(
                self.id,
                self.created_at,
                self.updated_at,
                self.revision.get(),
            ),
            self.name,
            self.description,
            self.parent_collection_id,
        )
    }
}

/// List visible collections, applying PostgreSQL filters, cursor paging, and
/// the optional exact count entirely inside the adapter.
pub async fn list_collections(
    runtime: &PostgresRuntime,
    query: CatalogListQuery,
) -> Result<CatalogPage<StorageCollection>, PostgresStorageError> {
    let include_total = query.options().include_total;
    if !query
        .visibility()
        .allows_permissions(&[AuthorizationPermission::ReadCollection])
    {
        return Ok(CatalogPage::new(Vec::new(), include_total.then_some(0)));
    }

    let (options, visibility) = query.into_parts();
    validate_permission_filters(&options.filters)?;
    let principal_id = visibility.principal_id();
    let is_admin = visibility.is_admin();
    let resource_scope = visibility.resources().cloned();

    runtime
        .with_read_only_snapshot(async move |connection| {
            let total = if include_total {
                let count_query = collection_query(principal_id, is_admin, resource_scope.as_ref());
                let count_query = apply_collection_filters(count_query, &options)?;
                Some(count_query.count().get_result::<i64>(connection).await?)
            } else {
                None
            };

            let row_query = collection_query(principal_id, is_admin, resource_scope.as_ref());
            let mut row_query = apply_collection_filters(row_query, &options)?;
            let fields = collection_cursor_fields(&options)?;
            crate::apply_query_options_with_fields!(row_query, options, fields);
            let rows = row_query
                .select(CollectionCatalogRow::as_select())
                .load::<CollectionCatalogRow>(connection)
                .await?
                .into_iter()
                .map(CollectionCatalogRow::into_storage)
                .collect();

            Ok::<_, PostgresStorageError>(CatalogPage::new(rows, total))
        })
        .await
}

fn collection_query<'query>(
    principal_id: i32,
    is_admin: bool,
    resource_scope: Option<&'query UnifiedSearchResourceScope>,
) -> crate::schema::collections::BoxedQuery<'query, diesel::pg::Pg> {
    use crate::schema::collection_closure;
    use crate::schema::collections;
    use crate::schema::group_memberships;
    use crate::schema::permissions;

    let mut query = collections::table.into_boxed();
    if !is_admin {
        let principal_groups = group_memberships::table
            .filter(group_memberships::principal_id.eq(principal_id))
            .select(group_memberships::group_id);
        let visible_collections = permissions::table
            .filter(permissions::group_id.eq_any(principal_groups))
            .filter(permissions::has_read_collection.eq(true))
            .inner_join(
                collection_closure::table
                    .on(permissions::collection_id.eq(collection_closure::ancestor_collection_id)),
            )
            .select(collection_closure::descendant_collection_id)
            .distinct();
        query = query.filter(collections::id.eq_any(visible_collections));
    }
    if let Some(scope) = resource_scope {
        query = query.filter(collections::id.eq_any(scope.collection_ids()));
    }
    query
}

fn apply_collection_filters<'query>(
    mut query: crate::schema::collections::BoxedQuery<'query, diesel::pg::Pg>,
    options: &QueryOptions,
) -> Result<crate::schema::collections::BoxedQuery<'query, diesel::pg::Pg>, PostgresStorageError> {
    use crate::schema::collections;

    for parameter in &options.filters {
        match &parameter.field {
            FilterField::Id => {
                crate::postgres_integer_filter!(query, parameter, collections::id)
            }
            FilterField::CreatedAt => {
                crate::postgres_datetime_filter!(query, parameter, collections::created_at)
            }
            FilterField::UpdatedAt => {
                crate::postgres_datetime_filter!(query, parameter, collections::updated_at)
            }
            FilterField::Revision => {
                crate::postgres_revision_filter!(query, parameter, collections::revision)
            }
            FilterField::Name => {
                crate::postgres_string_filter!(query, parameter, collections::name)
            }
            FilterField::Description => {
                crate::postgres_string_filter!(query, parameter, collections::description)
            }
            FilterField::Permissions => {}
            other => {
                return Err(PostgresStorageError::bad_request(format!(
                    "Field '{other}' isn't searchable (or does not exist) for collections"
                )));
            }
        }
    }
    Ok(query)
}

fn validate_permission_filters(
    filters: &[ParsedQueryParam],
) -> Result<Vec<AuthorizationPermission>, PostgresStorageError> {
    filters
        .iter()
        .filter(|parameter| parameter.field == FilterField::Permissions)
        .map(|parameter| {
            AuthorizationPermission::from_name(&parameter.value)
                .map_err(|error| PostgresStorageError::bad_request(error.to_string()))
        })
        .collect()
}

fn collection_cursor_fields(
    options: &QueryOptions,
) -> Result<Vec<CursorSqlField>, PostgresStorageError> {
    options
        .sort
        .iter()
        .map(|sort| {
            Ok(match sort.field {
                FilterField::Id => CursorSqlField {
                    column: "collections.id",
                    sql_type: CursorSqlType::Integer,
                    nullable: false,
                },
                FilterField::Name => CursorSqlField {
                    column: "collections.name",
                    sql_type: CursorSqlType::String,
                    nullable: false,
                },
                FilterField::Description => CursorSqlField {
                    column: "collections.description",
                    sql_type: CursorSqlType::String,
                    nullable: false,
                },
                FilterField::CreatedAt => CursorSqlField {
                    column: "collections.created_at",
                    sql_type: CursorSqlType::DateTime,
                    nullable: false,
                },
                FilterField::UpdatedAt => CursorSqlField {
                    column: "collections.updated_at",
                    sql_type: CursorSqlType::DateTime,
                    nullable: false,
                },
                FilterField::Revision => CursorSqlField {
                    column: "collections.revision",
                    sql_type: CursorSqlType::BigInt,
                    nullable: false,
                },
                ref other => {
                    return Err(PostgresStorageError::bad_request(format!(
                        "Field '{other}' is not orderable for collections"
                    )));
                }
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use hubuum_query::{FilterField, ParsedQueryParam, SearchOperator};

    use super::validate_permission_filters;

    #[test]
    fn permission_filters_are_validated_inside_the_adapter() {
        let error = validate_permission_filters(&[ParsedQueryParam {
            field: FilterField::Permissions,
            operator: SearchOperator::Equals { is_negated: false },
            value: "DefinitelyNotAPermission".to_string(),
        }])
        .unwrap_err();

        assert!(error.to_string().contains("Invalid permission"));
    }
}
