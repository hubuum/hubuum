//! PostgreSQL implementation of ordinary catalog listing and filtering.

use std::collections::HashMap;

use diesel::prelude::{BoolExpressionMethods, ExpressionMethods, QueryDsl};
use diesel::{JoinOnDsl, Queryable, Selectable, SelectableHelper};
use diesel_async::RunQueryDsl;
use hubuum_query::{FilterField, ParsedQueryParam, QueryOptions};
use hubuum_storage_core::{
    AuthorizationPermission, CatalogListQuery, CatalogPage, StorageClass, StorageCollection,
    StorageObject, UnifiedSearchResourceScope,
};

use crate::cursor::{CursorSqlField, CursorSqlType};
use crate::operations::class::ClassRow;
use crate::operations::dynamic_sql::BoundSqlPredicate;
use crate::operations::json_filter::json_predicate;
use crate::operations::object::ObjectRow;
use crate::operations::related_filter::related_object_filter_predicate;
use crate::operations::visibility::{authorized_collection_ids, required_permissions};
use crate::revision::record_metadata;
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
            record_metadata(self.id, self.created_at, self.updated_at, self.revision),
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
    let include_total = query.options().include_total();
    if !query
        .visibility()
        .allows_permissions(&[AuthorizationPermission::ReadCollection])
    {
        return Ok(CatalogPage::new(Vec::new(), include_total.then_some(0)));
    }

    let (options, visibility) = query.into_parts();
    validate_permission_filters(options.filters())?;
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

/// List visible classes, including their collection projection, with all
/// authorization, filters, paging, and counting executed by PostgreSQL.
pub async fn list_classes(
    runtime: &PostgresRuntime,
    query: CatalogListQuery,
) -> Result<CatalogPage<StorageClass>, PostgresStorageError> {
    let include_total = query.options().include_total();
    let (options, visibility) = query.into_parts();
    let permissions = required_permissions(
        &options,
        [
            AuthorizationPermission::ReadCollection,
            AuthorizationPermission::ReadClass,
        ],
    )?;
    if !visibility.allows_permissions(&permissions) {
        return Ok(CatalogPage::new(Vec::new(), include_total.then_some(0)));
    }

    runtime
        .with_read_only_snapshot(async move |connection| {
            let collection_ids =
                authorized_collection_ids(connection, &visibility, &permissions).await?;
            let build_query = || class_query(&collection_ids, visibility.resources());
            let total = if include_total {
                let query = apply_class_filters(build_query(), &options)?;
                Some(query.count().get_result::<i64>(connection).await?)
            } else {
                None
            };

            let mut query = apply_class_filters(build_query(), &options)?;
            let fields = class_cursor_fields(&options)?;
            crate::apply_query_options_with_fields!(query, options, fields);
            tracing::debug!(
                operation = "list_classes",
                filter_count = options.filters().len(),
                sort_count = options.sort().len(),
                has_cursor = options.cursor().is_some(),
                include_total,
                "executing PostgreSQL catalog query"
            );
            let rows = query
                .select(ClassRow::as_select())
                .load::<ClassRow>(connection)
                .await?;
            let collections = load_collection_map_for_classes(connection, &rows).await?;
            let classes = rows
                .into_iter()
                .map(|row| class_to_storage(row, &collections))
                .collect::<Result<Vec<_>, _>>()?;

            Ok::<_, PostgresStorageError>(CatalogPage::new(classes, total))
        })
        .await
}

/// List visible objects with ordinary scalar, JSON, and relation-graph
/// filters, stable cursor paging, and an optional exact count.
pub async fn list_objects(
    runtime: &PostgresRuntime,
    query: CatalogListQuery,
) -> Result<CatalogPage<StorageObject>, PostgresStorageError> {
    let include_total = query.options().include_total();
    let (options, visibility) = query.into_parts();
    let permissions = required_permissions(
        &options,
        [
            AuthorizationPermission::ReadCollection,
            AuthorizationPermission::ReadObject,
        ],
    )?;
    if !visibility.allows_permissions(&permissions) {
        return Ok(CatalogPage::new(Vec::new(), include_total.then_some(0)));
    }
    reject_computed_object_query(&options)?;

    runtime
        .with_read_only_snapshot(async move |connection| {
            let collection_ids =
                authorized_collection_ids(connection, &visibility, &permissions).await?;
            let related_predicate =
                related_object_filter_predicate(connection, options.filters(), &visibility).await?;
            let build_query = || object_query(&collection_ids, visibility.resources());
            let total = if include_total {
                let query =
                    apply_object_filters(build_query(), &options, related_predicate.clone())?;
                Some(query.count().get_result::<i64>(connection).await?)
            } else {
                None
            };

            let mut query = apply_object_filters(build_query(), &options, related_predicate)?;
            let fields = object_cursor_fields(&options)?;
            crate::apply_query_options_with_fields!(query, options, fields);
            tracing::debug!(
                operation = "list_objects",
                filter_count = options.filters().len(),
                sort_count = options.sort().len(),
                has_cursor = options.cursor().is_some(),
                include_total,
                "executing PostgreSQL catalog query"
            );
            let rows = query
                .select(ObjectRow::as_select())
                .load::<ObjectRow>(connection)
                .await?
                .into_iter()
                .map(ObjectRow::into_storage)
                .collect();

            Ok::<_, PostgresStorageError>(CatalogPage::new(rows, total))
        })
        .await
}

fn reject_computed_object_query(options: &QueryOptions) -> Result<(), PostgresStorageError> {
    if options
        .filters()
        .iter()
        .any(|filter| filter.field.computed_query().is_some())
        || options
            .sort()
            .iter()
            .any(|sort| sort.field.computed_query().is_some())
    {
        return Err(PostgresStorageError::bad_request(
            "Computed object queries require a resolved query plan",
        ));
    }
    Ok(())
}

pub(crate) fn object_query<'query>(
    collection_ids: &'query [i32],
    resource_scope: Option<&'query UnifiedSearchResourceScope>,
) -> crate::schema::hubuumobject::BoxedQuery<'query, diesel::pg::Pg> {
    use crate::schema::hubuumobject;

    let mut query = hubuumobject::table
        .filter(hubuumobject::collection_id.eq_any(collection_ids))
        .into_boxed();
    if let Some(scope) = resource_scope {
        query = query.filter(
            hubuumobject::collection_id
                .eq_any(scope.collection_ids())
                .or(hubuumobject::hubuum_class_id.eq_any(scope.class_ids()))
                .or(hubuumobject::id.eq_any(scope.object_ids())),
        );
    }
    query
}

pub(crate) fn apply_object_filters<'query>(
    mut query: crate::schema::hubuumobject::BoxedQuery<'query, diesel::pg::Pg>,
    options: &QueryOptions,
    related_predicate: Option<BoundSqlPredicate>,
) -> Result<crate::schema::hubuumobject::BoxedQuery<'query, diesel::pg::Pg>, PostgresStorageError> {
    use crate::schema::hubuumobject;

    if let Some(predicate) = related_predicate {
        query = query.filter(predicate);
    }
    for parameter in options.filters() {
        if parameter.field.related_query().is_some() {
            continue;
        }
        if parameter.field.computed_query().is_some() {
            continue;
        }
        match &parameter.field {
            FilterField::Id => {
                crate::postgres_integer_filter!(query, parameter, hubuumobject::id)
            }
            FilterField::Collections | FilterField::CollectionId => {
                crate::postgres_integer_filter!(query, parameter, hubuumobject::collection_id)
            }
            FilterField::CreatedAt => {
                crate::postgres_datetime_filter!(query, parameter, hubuumobject::created_at)
            }
            FilterField::UpdatedAt => {
                crate::postgres_datetime_filter!(query, parameter, hubuumobject::updated_at)
            }
            FilterField::Revision => {
                crate::postgres_revision_filter!(query, parameter, hubuumobject::revision)
            }
            FilterField::Name => {
                crate::postgres_string_filter!(query, parameter, hubuumobject::name)
            }
            FilterField::Description => {
                crate::postgres_string_filter!(query, parameter, hubuumobject::description)
            }
            FilterField::Classes | FilterField::ClassId => {
                crate::postgres_integer_filter!(query, parameter, hubuumobject::hubuum_class_id)
            }
            FilterField::JsonData => {
                query = query.filter(json_predicate(parameter, "hubuumobject.data")?);
            }
            FilterField::Permissions => {}
            other => {
                return Err(PostgresStorageError::bad_request(format!(
                    "Field '{other}' isn't searchable (or does not exist) for objects"
                )));
            }
        }
    }
    Ok(query)
}

fn object_cursor_fields(
    options: &QueryOptions,
) -> Result<Vec<CursorSqlField>, PostgresStorageError> {
    options
        .sort()
        .iter()
        .map(|sort| object_cursor_field(&sort.field))
        .collect()
}

pub(crate) fn object_cursor_field(
    field: &FilterField,
) -> Result<CursorSqlField, PostgresStorageError> {
    Ok(match field {
        FilterField::Id => CursorSqlField {
            column: "hubuumobject.id",
            sql_type: CursorSqlType::Integer,
            nullable: false,
        },
        FilterField::Name => CursorSqlField {
            column: "hubuumobject.name",
            sql_type: CursorSqlType::String,
            nullable: false,
        },
        FilterField::Description => CursorSqlField {
            column: "hubuumobject.description",
            sql_type: CursorSqlType::String,
            nullable: false,
        },
        FilterField::Collections | FilterField::CollectionId => CursorSqlField {
            column: "hubuumobject.collection_id",
            sql_type: CursorSqlType::Integer,
            nullable: false,
        },
        FilterField::ClassId | FilterField::Classes => CursorSqlField {
            column: "hubuumobject.hubuum_class_id",
            sql_type: CursorSqlType::Integer,
            nullable: false,
        },
        FilterField::CreatedAt => CursorSqlField {
            column: "hubuumobject.created_at",
            sql_type: CursorSqlType::DateTime,
            nullable: false,
        },
        FilterField::UpdatedAt => CursorSqlField {
            column: "hubuumobject.updated_at",
            sql_type: CursorSqlType::DateTime,
            nullable: false,
        },
        FilterField::Revision => CursorSqlField {
            column: "hubuumobject.revision",
            sql_type: CursorSqlType::BigInt,
            nullable: false,
        },
        other => {
            return Err(PostgresStorageError::bad_request(format!(
                "Field '{other}' is not orderable for objects"
            )));
        }
    })
}

fn class_query<'query>(
    collection_ids: &'query [i32],
    resource_scope: Option<&'query UnifiedSearchResourceScope>,
) -> crate::schema::hubuumclass::BoxedQuery<'query, diesel::pg::Pg> {
    use crate::schema::hubuumclass;

    let mut query = hubuumclass::table
        .filter(hubuumclass::collection_id.eq_any(collection_ids))
        .into_boxed();
    if let Some(scope) = resource_scope {
        query = query.filter(
            hubuumclass::collection_id
                .eq_any(scope.collection_ids())
                .or(hubuumclass::id.eq_any(scope.class_ids())),
        );
    }
    query
}

fn apply_class_filters<'query>(
    mut query: crate::schema::hubuumclass::BoxedQuery<'query, diesel::pg::Pg>,
    options: &QueryOptions,
) -> Result<crate::schema::hubuumclass::BoxedQuery<'query, diesel::pg::Pg>, PostgresStorageError> {
    use crate::schema::hubuumclass;

    for parameter in options.filters() {
        match &parameter.field {
            FilterField::Id => {
                crate::postgres_integer_filter!(query, parameter, hubuumclass::id)
            }
            FilterField::Collections => {
                crate::postgres_integer_filter!(query, parameter, hubuumclass::collection_id)
            }
            FilterField::CreatedAt => {
                crate::postgres_datetime_filter!(query, parameter, hubuumclass::created_at)
            }
            FilterField::UpdatedAt => {
                crate::postgres_datetime_filter!(query, parameter, hubuumclass::updated_at)
            }
            FilterField::Revision => {
                crate::postgres_revision_filter!(query, parameter, hubuumclass::revision)
            }
            FilterField::Name => {
                crate::postgres_string_filter!(query, parameter, hubuumclass::name)
            }
            FilterField::Description => {
                crate::postgres_string_filter!(query, parameter, hubuumclass::description)
            }
            FilterField::ValidateSchema => {
                crate::postgres_boolean_filter!(query, parameter, hubuumclass::validate_schema)
            }
            FilterField::JsonSchema => {
                query = query.filter(json_predicate(parameter, "hubuumclass.json_schema")?);
            }
            FilterField::Permissions => {}
            other => {
                return Err(PostgresStorageError::bad_request(format!(
                    "Field '{other}' isn't searchable (or does not exist) for classes"
                )));
            }
        }
    }
    Ok(query)
}

fn class_cursor_fields(
    options: &QueryOptions,
) -> Result<Vec<CursorSqlField>, PostgresStorageError> {
    options
        .sort()
        .iter()
        .map(|sort| {
            Ok(match sort.field {
                FilterField::Id => CursorSqlField {
                    column: "hubuumclass.id",
                    sql_type: CursorSqlType::Integer,
                    nullable: false,
                },
                FilterField::Name => CursorSqlField {
                    column: "hubuumclass.name",
                    sql_type: CursorSqlType::String,
                    nullable: false,
                },
                FilterField::Description => CursorSqlField {
                    column: "hubuumclass.description",
                    sql_type: CursorSqlType::String,
                    nullable: false,
                },
                FilterField::Collections | FilterField::CollectionId => CursorSqlField {
                    column: "hubuumclass.collection_id",
                    sql_type: CursorSqlType::Integer,
                    nullable: false,
                },
                FilterField::CreatedAt => CursorSqlField {
                    column: "hubuumclass.created_at",
                    sql_type: CursorSqlType::DateTime,
                    nullable: false,
                },
                FilterField::UpdatedAt => CursorSqlField {
                    column: "hubuumclass.updated_at",
                    sql_type: CursorSqlType::DateTime,
                    nullable: false,
                },
                FilterField::Revision => CursorSqlField {
                    column: "hubuumclass.revision",
                    sql_type: CursorSqlType::BigInt,
                    nullable: false,
                },
                ref other => {
                    return Err(PostgresStorageError::bad_request(format!(
                        "Field '{other}' is not orderable for classes"
                    )));
                }
            })
        })
        .collect()
}

async fn load_collection_map_for_classes(
    connection: &mut crate::PostgresConnection,
    classes: &[ClassRow],
) -> Result<HashMap<i32, StorageCollection>, PostgresStorageError> {
    use crate::schema::collections;

    let mut collection_ids = classes
        .iter()
        .map(|class| class.collection_id)
        .collect::<Vec<_>>();
    collection_ids.sort_unstable();
    collection_ids.dedup();
    if collection_ids.is_empty() {
        return Ok(HashMap::new());
    }
    Ok(collections::table
        .filter(collections::id.eq_any(collection_ids))
        .select(CollectionCatalogRow::as_select())
        .load::<CollectionCatalogRow>(connection)
        .await?
        .into_iter()
        .map(|row| {
            let collection = row.into_storage();
            (collection.id(), collection)
        })
        .collect())
}

fn class_to_storage(
    row: ClassRow,
    collections: &HashMap<i32, StorageCollection>,
) -> Result<StorageClass, PostgresStorageError> {
    let collection = collections
        .get(&row.collection_id)
        .cloned()
        .ok_or_else(|| {
            PostgresStorageError::database(format!(
                "class {} references missing collection {}",
                row.id, row.collection_id
            ))
        })?;
    Ok(StorageClass::builder(
        record_metadata(row.id, row.created_at, row.updated_at, row.revision),
        row.name,
        collection,
        row.description,
    )
    .json_schema(row.json_schema)
    .validate_schema(row.validate_schema)
    .build())
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

    for parameter in options.filters() {
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
        .sort()
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
