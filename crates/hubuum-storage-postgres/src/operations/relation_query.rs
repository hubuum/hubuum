//! PostgreSQL relation listing and endpoint-visibility queries.

use crate::cursor::{
    CursorSqlField, CursorSqlType, cursor_filter_sql_for_fields, order_sql_clause_for_field,
};
use crate::operations::dynamic_sql::SqlValue;
use crate::operations::json_filter::json_filter_sql;
use crate::operations::relation::{ClassRelationRow, ObjectRelationRow};
use crate::operations::visibility::{authorized_collection_ids, required_permissions};
use crate::revision::record_metadata_from_raw_revision;
use crate::{PostgresConnection, PostgresRuntime, PostgresStorageError};
use diesel::dsl::not;
use diesel::prelude::{BoolExpressionMethods, ExpressionMethods, QueryDsl};
use diesel::{QueryableByName, SelectableHelper};
use diesel_async::RunQueryDsl;
use hubuum_domain::{ClassId, CollectionId, ObjectId};
use hubuum_query::{DataType, FilterField, Operator, ParsedQueryParam, QueryOptions, SortParam};
use hubuum_storage_core::{
    StorageAuthorizationPermission, StorageBidirectionalRelatedObjectsQuery, StorageClassGraphRow,
    StorageClassRelation, StorageGraphClass, StorageGraphObject, StorageGraphResource,
    StorageObjectGraphRow, StorageObjectRelation, StorageObjectRelationsTouchingIdsQuery,
    StoragePage, StorageRelatedDirection, StorageRelatedObjectForRootRow,
    StorageRelatedObjectIncludeRow, StorageRelatedObjectsForRootsQuery, StorageRelatedSort,
    StorageRelationGraphQuery, StorageRelationIdsQuery, StorageRelationListQuery,
    StorageRelationTouchingQuery, StorageVisibility,
};

const CLASS_RELATION_PERMISSION: StorageAuthorizationPermission =
    StorageAuthorizationPermission::ReadClassRelation;
const OBJECT_RELATION_PERMISSION: StorageAuthorizationPermission =
    StorageAuthorizationPermission::ReadObjectRelation;

macro_rules! bind_raw_sql_query {
    ($spec:expr) => {{
        let spec = $spec.into_indexed_sql();
        let mut query = diesel::sql_query(spec.sql).into_boxed();
        for value in spec.bind_variables {
            query = match value {
                SqlValue::Integer(value) => query.bind::<diesel::sql_types::Integer, _>(value),
                SqlValue::BigInteger(value) => query.bind::<diesel::sql_types::BigInt, _>(value),
                SqlValue::String(value) => query.bind::<diesel::sql_types::Text, _>(value),
                SqlValue::Boolean(value) => query.bind::<diesel::sql_types::Bool, _>(value),
                SqlValue::DateTime(value) => query.bind::<diesel::sql_types::Timestamp, _>(value),
            };
        }
        query
    }};
}

/// List visible class relations with stable cursor paging and an optional count.
pub async fn list_class_relations(
    runtime: &PostgresRuntime,
    query: StorageRelationListQuery,
) -> Result<StoragePage<StorageClassRelation>, PostgresStorageError> {
    let include_total = query.options().include_total();
    let (options, visibility) = query.into_parts();
    let permissions = required_permissions(&options, [CLASS_RELATION_PERMISSION])?;
    if !visibility.allows_permissions(&permissions) {
        return crate::persisted_page(Vec::new(), include_total.then_some(0));
    }

    runtime
        .with_read_only_snapshot(async move |connection| {
            let collection_ids =
                authorized_collection_ids(connection, &visibility, &permissions).await?;
            let (from_name_ids, to_name_ids) =
                class_name_filter_ids(connection, &options, &visibility).await?;
            let build_query = || {
                build_class_relation_query(
                    &options,
                    &visibility,
                    &collection_ids,
                    None,
                    from_name_ids.as_deref(),
                    to_name_ids.as_deref(),
                )
            };
            let total = if include_total {
                Some(build_query()?.count().get_result::<i64>(connection).await?)
            } else {
                None
            };
            let mut records = build_query()?;
            let fields = relation_cursor_fields(&options, RelationKind::Class)?;
            crate::apply_query_options_with_fields!(
                records,
                options,
                fields,
                crate::cursor::CursorTieBreaker::new(
                    FilterField::Id,
                    false,
                    relation_cursor_field(&FilterField::Id, RelationKind::Class)?,
                )
            );
            let rows = records
                .select(ClassRelationRow::as_select())
                .load::<ClassRelationRow>(connection)
                .await?
                .into_iter()
                .map(ClassRelationRow::into_storage)
                .collect::<Result<Vec<_>, _>>()?;
            crate::persisted_page(rows, total)
        })
        .await
}

/// List visible object relations with stable cursor paging and an optional count.
pub async fn list_object_relations(
    runtime: &PostgresRuntime,
    query: StorageRelationListQuery,
) -> Result<StoragePage<StorageObjectRelation>, PostgresStorageError> {
    let include_total = query.options().include_total();
    let (options, visibility) = query.into_parts();
    let permissions = required_permissions(&options, [OBJECT_RELATION_PERMISSION])?;
    if !visibility.allows_permissions(&permissions) {
        return crate::persisted_page(Vec::new(), include_total.then_some(0));
    }

    runtime
        .with_read_only_snapshot(async move |connection| {
            let collection_ids =
                authorized_collection_ids(connection, &visibility, &permissions).await?;
            let build_query =
                || build_object_relation_query(&options, &visibility, &collection_ids, None);
            let total = if include_total {
                Some(build_query()?.count().get_result::<i64>(connection).await?)
            } else {
                None
            };
            let mut records = build_query()?;
            let fields = relation_cursor_fields(&options, RelationKind::Object)?;
            crate::apply_query_options_with_fields!(
                records,
                options,
                fields,
                crate::cursor::CursorTieBreaker::new(
                    FilterField::Id,
                    false,
                    relation_cursor_field(&FilterField::Id, RelationKind::Object)?,
                )
            );
            let rows = records
                .select(ObjectRelationRow::as_select())
                .load::<ObjectRelationRow>(connection)
                .await?
                .into_iter()
                .map(ObjectRelationRow::into_storage)
                .collect::<Result<Vec<_>, _>>()?;
            crate::persisted_page(rows, total)
        })
        .await
}

/// List class relations touching one class.
pub async fn list_class_relations_touching(
    runtime: &PostgresRuntime,
    query: StorageRelationTouchingQuery,
) -> Result<StoragePage<StorageClassRelation>, PostgresStorageError> {
    let include_total = query.options().include_total();
    let (class_id, options, visibility) = query.into_parts();
    validate_positive_id(class_id.id(), "class id")?;
    let permissions = required_permissions(&options, [CLASS_RELATION_PERMISSION])?;
    if !visibility.allows_permissions(&permissions) {
        return crate::persisted_page(Vec::new(), include_total.then_some(0));
    }

    runtime
        .with_read_only_snapshot(async move |connection| {
            let collection_ids =
                authorized_collection_ids(connection, &visibility, &permissions).await?;
            let build_query = || {
                build_class_relation_query(
                    &options,
                    &visibility,
                    &collection_ids,
                    Some(class_id.id()),
                    None,
                    None,
                )
            };
            let total = if include_total {
                Some(build_query()?.count().get_result::<i64>(connection).await?)
            } else {
                None
            };
            let mut records = build_query()?;
            let fields = relation_cursor_fields(&options, RelationKind::Class)?;
            crate::apply_query_options_with_fields!(
                records,
                options,
                fields,
                crate::cursor::CursorTieBreaker::new(
                    FilterField::Id,
                    false,
                    relation_cursor_field(&FilterField::Id, RelationKind::Class)?,
                )
            );
            let rows = records
                .select(ClassRelationRow::as_select())
                .load::<ClassRelationRow>(connection)
                .await?
                .into_iter()
                .map(ClassRelationRow::into_storage)
                .collect::<Result<Vec<_>, _>>()?;
            crate::persisted_page(rows, total)
        })
        .await
}

/// List object relations touching one object.
pub async fn list_object_relations_touching(
    runtime: &PostgresRuntime,
    query: StorageRelationTouchingQuery,
) -> Result<StoragePage<StorageObjectRelation>, PostgresStorageError> {
    let include_total = query.options().include_total();
    let (object_id, options, visibility) = query.into_parts();
    validate_positive_id(object_id.id(), "object id")?;
    let permissions = required_permissions(&options, [OBJECT_RELATION_PERMISSION])?;
    if !visibility.allows_permissions(&permissions) {
        return crate::persisted_page(Vec::new(), include_total.then_some(0));
    }

    runtime
        .with_read_only_snapshot(async move |connection| {
            let collection_ids =
                authorized_collection_ids(connection, &visibility, &permissions).await?;
            let build_query = || {
                build_object_relation_query(
                    &options,
                    &visibility,
                    &collection_ids,
                    Some(object_id.id()),
                )
            };
            let total = if include_total {
                Some(build_query()?.count().get_result::<i64>(connection).await?)
            } else {
                None
            };
            let mut records = build_query()?;
            let fields = relation_cursor_fields(&options, RelationKind::Object)?;
            crate::apply_query_options_with_fields!(
                records,
                options,
                fields,
                crate::cursor::CursorTieBreaker::new(
                    FilterField::Id,
                    false,
                    relation_cursor_field(&FilterField::Id, RelationKind::Object)?,
                )
            );
            let rows = records
                .select(ObjectRelationRow::as_select())
                .load::<ObjectRelationRow>(connection)
                .await?
                .into_iter()
                .map(ObjectRelationRow::into_storage)
                .collect::<Result<Vec<_>, _>>()?;
            crate::persisted_page(rows, total)
        })
        .await
}

/// Return visible class relations touching any supplied class id.
pub async fn list_class_relations_touching_ids(
    runtime: &PostgresRuntime,
    query: StorageRelationIdsQuery,
) -> Result<Vec<StorageClassRelation>, PostgresStorageError> {
    class_relations_for_ids(runtime, query, IdMatch::Touching).await
}

/// Return visible class relations whose endpoints are both supplied ids.
pub async fn list_class_relations_between_ids(
    runtime: &PostgresRuntime,
    query: StorageRelationIdsQuery,
) -> Result<Vec<StorageClassRelation>, PostgresStorageError> {
    class_relations_for_ids(runtime, query, IdMatch::Between).await
}

/// Return visible object relations whose endpoints are both supplied ids.
pub async fn list_object_relations_between_ids(
    runtime: &PostgresRuntime,
    query: StorageRelationIdsQuery,
) -> Result<Vec<StorageObjectRelation>, PostgresStorageError> {
    let (ids, visibility) = query.into_parts();
    if ids.is_empty() || !visibility.allows_permissions(&[OBJECT_RELATION_PERMISSION]) {
        return Ok(Vec::new());
    }
    let ids = ids.into_iter().map(|id| id.id()).collect::<Vec<_>>();
    runtime
        .with_connection(async move |connection| {
            let collection_ids =
                authorized_collection_ids(connection, &visibility, &[OBJECT_RELATION_PERMISSION])
                    .await?;
            let options = empty_options();
            let rows = build_object_relation_query(&options, &visibility, &collection_ids, None)?
                .filter(crate::schema::hubuumobject_relation::from_hubuum_object_id.eq_any(&ids))
                .filter(crate::schema::hubuumobject_relation::to_hubuum_object_id.eq_any(&ids))
                .order(crate::schema::hubuumobject_relation::id.asc())
                .select(ObjectRelationRow::as_select())
                .load::<ObjectRelationRow>(connection)
                .await?;
            rows.into_iter()
                .map(ObjectRelationRow::into_storage)
                .collect::<Result<Vec<_>, _>>()
        })
        .await
}

/// Return a bounded set of visible object relations touching supplied ids.
pub async fn list_object_relations_touching_ids(
    runtime: &PostgresRuntime,
    query: StorageObjectRelationsTouchingIdsQuery,
) -> Result<Vec<StorageObjectRelation>, PostgresStorageError> {
    let (ids, excluded_ids, max_results, visibility) = query.into_parts();
    if ids.is_empty()
        || max_results == 0
        || !visibility.allows_permissions(&[OBJECT_RELATION_PERMISSION])
    {
        return Ok(Vec::new());
    }
    let max_results = i64::try_from(max_results).map_err(|_| {
        PostgresStorageError::invalid_input("Object-relation result limit is too large")
    })?;
    let ids = ids.into_iter().map(|id| id.id()).collect::<Vec<_>>();
    let excluded_ids = excluded_ids
        .into_iter()
        .map(|id| id.id())
        .collect::<Vec<_>>();
    runtime
        .with_connection(async move |connection| {
            let collection_ids =
                authorized_collection_ids(connection, &visibility, &[OBJECT_RELATION_PERMISSION])
                    .await?;
            let options = empty_options();
            let mut records =
                build_object_relation_query(&options, &visibility, &collection_ids, None)?.filter(
                    crate::schema::hubuumobject_relation::from_hubuum_object_id
                        .eq_any(&ids)
                        .or(crate::schema::hubuumobject_relation::to_hubuum_object_id.eq_any(&ids)),
                );
            if !excluded_ids.is_empty() {
                records = records.filter(not(
                    crate::schema::hubuumobject_relation::id.eq_any(excluded_ids)
                ));
            }
            let rows = records
                .order(crate::schema::hubuumobject_relation::id.asc())
                .limit(max_results)
                .select(ObjectRelationRow::as_select())
                .load::<ObjectRelationRow>(connection)
                .await?;
            rows.into_iter()
                .map(ObjectRelationRow::into_storage)
                .collect::<Result<Vec<_>, _>>()
        })
        .await
}

/// Return classes reachable from one class, with PostgreSQL-native filtering and paging.
pub async fn list_related_classes(
    runtime: &PostgresRuntime,
    query: StorageRelationGraphQuery,
) -> Result<StoragePage<StorageClassGraphRow>, PostgresStorageError> {
    let include_total = query.options().include_total();
    let (root_id, options, visibility) = query.into_parts();
    validate_positive_id(root_id.id(), "class id")?;
    let permissions = required_permissions(
        &options,
        [
            StorageAuthorizationPermission::ReadClass,
            StorageAuthorizationPermission::ReadClassRelation,
        ],
    )?;
    if !visibility.allows_permissions(&permissions) {
        return crate::persisted_page(Vec::new(), include_total.then_some(0));
    }

    runtime
        .with_read_only_snapshot(async move |connection| {
            let collection_ids =
                authorized_collection_ids(connection, &visibility, &permissions).await?;
            if collection_ids.is_empty() {
                return crate::persisted_page(Vec::new(), include_total.then_some(0));
            }
            let base = build_related_graph_query_spec(
                GraphKind::Class,
                root_id.id(),
                &collection_ids,
                &options,
                &visibility,
            )?;
            let total = if include_total {
                Some(
                    bind_raw_sql_query!(base.clone().into_count_query("related_classes_count"))
                        .get_result::<CountRow>(connection)
                        .await?
                        .count,
                )
            } else {
                None
            };
            let paged = apply_raw_sql_pagination(base, &options, GraphKind::Class)?;
            tracing::debug!(
                operation = "list_related_classes",
                filter_count = options.filters().len(),
                sort_count = options.sort().len(),
                has_cursor = options.cursor().is_some(),
                include_total,
                "executing PostgreSQL relation graph query"
            );
            let rows = bind_raw_sql_query!(paged)
                .get_results::<ClassGraphQueryRow>(connection)
                .await?
                .into_iter()
                .map(ClassGraphQueryRow::into_storage)
                .collect::<Result<Vec<_>, _>>()?;
            crate::persisted_page(rows, total)
        })
        .await
}

/// Return objects reachable from one object, with PostgreSQL-native filtering and paging.
pub async fn list_related_objects(
    runtime: &PostgresRuntime,
    query: StorageRelationGraphQuery,
) -> Result<StoragePage<StorageObjectGraphRow>, PostgresStorageError> {
    let include_total = query.options().include_total();
    let (root_id, options, visibility) = query.into_parts();
    validate_positive_id(root_id.id(), "object id")?;
    let permissions = required_permissions(
        &options,
        [
            StorageAuthorizationPermission::ReadObject,
            StorageAuthorizationPermission::ReadObjectRelation,
        ],
    )?;
    if !visibility.allows_permissions(&permissions) {
        return crate::persisted_page(Vec::new(), include_total.then_some(0));
    }

    runtime
        .with_read_only_snapshot(async move |connection| {
            let collection_ids =
                authorized_collection_ids(connection, &visibility, &permissions).await?;
            if collection_ids.is_empty() {
                return crate::persisted_page(Vec::new(), include_total.then_some(0));
            }
            let base = build_related_graph_query_spec(
                GraphKind::Object,
                root_id.id(),
                &collection_ids,
                &options,
                &visibility,
            )?;
            let total = if include_total {
                Some(
                    bind_raw_sql_query!(base.clone().into_count_query("related_objects_count"))
                        .get_result::<CountRow>(connection)
                        .await?
                        .count,
                )
            } else {
                None
            };
            let paged = apply_raw_sql_pagination(base, &options, GraphKind::Object)?;
            tracing::debug!(
                operation = "list_related_objects",
                filter_count = options.filters().len(),
                sort_count = options.sort().len(),
                has_cursor = options.cursor().is_some(),
                include_total,
                "executing PostgreSQL relation graph query"
            );
            let rows = bind_raw_sql_query!(paged)
                .get_results::<ObjectGraphQueryRow>(connection)
                .await?
                .into_iter()
                .map(ObjectGraphQueryRow::into_storage)
                .collect::<Result<Vec<_>, _>>()?;
            crate::persisted_page(rows, total)
        })
        .await
}

/// Walk directional object relations for several roots in one bounded query.
pub async fn list_related_objects_for_roots(
    runtime: &PostgresRuntime,
    query: StorageRelatedObjectsForRootsQuery,
) -> Result<Vec<StorageRelatedObjectIncludeRow>, PostgresStorageError> {
    let (
        root_ids,
        class_id,
        class_relation_id,
        direction,
        sort,
        max_depth,
        limit,
        preserve_alternative_paths,
        visibility,
    ) = query.into_parts();
    if root_ids.is_empty() {
        return Ok(Vec::new());
    }
    validate_graph_bounds(max_depth, limit)?;
    let permissions = [
        StorageAuthorizationPermission::ReadObject,
        StorageAuthorizationPermission::ReadObjectRelation,
    ];
    if !visibility.allows_permissions(&permissions) {
        return Ok(Vec::new());
    }
    let root_ids = root_ids
        .into_iter()
        .map(|root_id| root_id.id())
        .collect::<Vec<_>>();
    let class_relation_id = class_relation_id.map(|relation_id| relation_id.id());
    let class_id = class_id.id();

    runtime
        .with_connection(async move |connection| {
            let collection_ids =
                authorized_collection_ids(connection, &visibility, &permissions).await?;
            if collection_ids.is_empty() {
                return Ok(Vec::new());
            }
            let spec = build_root_graph_walk_query(RootGraphWalkSpec {
                root_ids: &root_ids,
                collection_ids: &collection_ids,
                visibility: &visibility,
                max_depth,
                per_root_limit: limit,
                edges: GraphWalkEdges::Directional {
                    direction,
                    class_relation_id,
                },
                ranking: GraphWalkRanking::ByTargetClass { class_id, sort },
                projection: GraphWalkProjection::AncestorAndDescendant,
                preserve_alternative_paths,
            });
            tracing::debug!(
                operation = "list_related_objects_for_roots",
                root_count = root_ids.len(),
                max_depth,
                per_root_limit = limit,
                "executing PostgreSQL relation graph query"
            );
            let rows = bind_raw_sql_query!(spec)
                .get_results::<RelatedObjectIncludeQueryRow>(connection)
                .await?;
            rows.into_iter()
                .map(RelatedObjectIncludeQueryRow::into_storage)
                .collect::<Result<Vec<_>, _>>()
        })
        .await
}

/// Walk bidirectional object relations for several roots in one bounded query.
pub async fn list_bidirectionally_related_objects_for_roots(
    runtime: &PostgresRuntime,
    query: StorageBidirectionalRelatedObjectsQuery,
) -> Result<Vec<StorageRelatedObjectForRootRow>, PostgresStorageError> {
    let (root_ids, max_depth, per_root_cap, preserve_alternative_paths, visibility) =
        query.into_parts();
    if root_ids.is_empty() {
        return Ok(Vec::new());
    }
    validate_graph_bounds(max_depth, per_root_cap)?;
    let permissions = [
        StorageAuthorizationPermission::ReadObject,
        StorageAuthorizationPermission::ReadObjectRelation,
    ];
    if !visibility.allows_permissions(&permissions) {
        return Ok(Vec::new());
    }
    let root_ids = root_ids
        .into_iter()
        .map(|root_id| root_id.id())
        .collect::<Vec<_>>();

    runtime
        .with_connection(async move |connection| {
            let collection_ids =
                authorized_collection_ids(connection, &visibility, &permissions).await?;
            if collection_ids.is_empty() {
                return Ok(Vec::new());
            }
            let spec = build_root_graph_walk_query(RootGraphWalkSpec {
                root_ids: &root_ids,
                collection_ids: &collection_ids,
                visibility: &visibility,
                max_depth,
                per_root_limit: per_root_cap,
                edges: GraphWalkEdges::Bidirectional,
                ranking: GraphWalkRanking::ByDescendant,
                projection: GraphWalkProjection::DescendantOnly,
                preserve_alternative_paths,
            });
            tracing::debug!(
                operation = "list_bidirectionally_related_objects_for_roots",
                root_count = root_ids.len(),
                max_depth,
                per_root_limit = per_root_cap,
                "executing PostgreSQL relation graph query"
            );
            let rows = bind_raw_sql_query!(spec)
                .get_results::<RelatedObjectForRootQueryRow>(connection)
                .await?;
            rows.into_iter()
                .map(RelatedObjectForRootQueryRow::into_storage)
                .collect::<Result<Vec<_>, _>>()
        })
        .await
}

#[derive(Clone, Debug)]
struct RawSqlQuerySpec {
    sql: String,
    bind_variables: Vec<SqlValue>,
}

impl RawSqlQuerySpec {
    fn into_count_query(self, alias: &str) -> Self {
        Self {
            sql: format!("SELECT COUNT(*) AS count FROM ({}) AS {alias}", self.sql),
            bind_variables: self.bind_variables,
        }
    }

    fn into_indexed_sql(self) -> Self {
        let mut parameter = 0;
        let mut sql = String::with_capacity(self.sql.len());
        for character in self.sql.chars() {
            if character == '?' {
                parameter += 1;
                sql.push('$');
                sql.push_str(&parameter.to_string());
            } else {
                sql.push(character);
            }
        }
        Self {
            sql,
            bind_variables: self.bind_variables,
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum GraphKind {
    Class,
    Object,
}

impl GraphKind {
    const fn resource_label(self) -> &'static str {
        match self {
            Self::Class => "related classes",
            Self::Object => "related objects",
        }
    }

    const fn default_id_field(self) -> FilterField {
        match self {
            Self::Class => FilterField::ClassId,
            Self::Object => FilterField::Id,
        }
    }
}

fn build_related_graph_query_spec(
    kind: GraphKind,
    root_id: i32,
    collection_ids: &[i32],
    options: &QueryOptions,
    visibility: &StorageVisibility,
) -> Result<RawSqlQuerySpec, PostgresStorageError> {
    let mut bind_variables = vec![SqlValue::Integer(root_id)];
    let collection_array_sql = sql_integer_array(collection_ids, &mut bind_variables);
    let max_depth = related_depth_upper_bound(options.filters())?;
    let depth_sql = if let Some(max_depth) = max_depth {
        bind_variables.push(SqlValue::Integer(max_depth));
        "?"
    } else {
        "NULL"
    };
    let mut sql = match kind {
        GraphKind::Class => format!(
            "SELECT list_related_classes.*, ancestor.revision AS ancestor_revision, descendant.revision AS descendant_revision FROM get_bidirectionally_related_classes(?, {collection_array_sql}, {depth_sql}) AS list_related_classes JOIN hubuumclass ancestor ON ancestor.id = list_related_classes.ancestor_class_id JOIN hubuumclass descendant ON descendant.id = list_related_classes.descendant_class_id"
        ),
        GraphKind::Object => format!(
            "SELECT list_related_objects.*, ancestor.revision AS ancestor_revision, descendant.revision AS descendant_revision FROM get_bidirectionally_related_objects(?, {collection_array_sql}, {depth_sql}) AS list_related_objects JOIN hubuumobject ancestor ON ancestor.id = list_related_objects.ancestor_object_id JOIN hubuumobject descendant ON descendant.id = list_related_objects.descendant_object_id"
        ),
    };

    let mut clauses = Vec::new();
    append_graph_scope_clause(kind, &mut clauses, visibility, &mut bind_variables);
    for parameter in options.filters() {
        if let Some(clause) = build_graph_filter_clause(kind, parameter, &mut bind_variables)? {
            clauses.push(clause);
        }
    }
    if !clauses.is_empty() {
        sql.push_str("\nWHERE ");
        sql.push_str(&clauses.join("\n  AND "));
    }
    Ok(RawSqlQuerySpec {
        sql,
        bind_variables,
    })
}

fn append_graph_scope_clause(
    kind: GraphKind,
    clauses: &mut Vec<String>,
    visibility: &StorageVisibility,
    bind_variables: &mut Vec<SqlValue>,
) {
    let Some(scope) = visibility.resources() else {
        return;
    };
    let collection_ids = scope
        .collection_ids()
        .iter()
        .map(|id| id.id())
        .collect::<Vec<_>>();
    let class_ids = scope
        .class_ids()
        .iter()
        .map(|id| id.id())
        .collect::<Vec<_>>();
    let collection_sql = sql_integer_array(&collection_ids, bind_variables);
    let class_sql = sql_integer_array(&class_ids, bind_variables);
    match kind {
        GraphKind::Class => clauses.push(format!(
            "NOT EXISTS (SELECT 1 FROM unnest(list_related_classes.path) AS path_class_id JOIN hubuumclass path_class ON path_class.id = path_class_id WHERE NOT (path_class.collection_id = ANY({collection_sql}) OR path_class.id = ANY({class_sql})))"
        )),
        GraphKind::Object => {
            let object_ids = scope
                .object_ids()
                .iter()
                .map(|id| id.id())
                .collect::<Vec<_>>();
            let object_sql = sql_integer_array(&object_ids, bind_variables);
            clauses.push(format!(
                "NOT EXISTS (SELECT 1 FROM unnest(list_related_objects.path) AS path_object_id JOIN hubuumobject path_object ON path_object.id = path_object_id WHERE NOT (path_object.collection_id = ANY({collection_sql}) OR path_object.hubuum_class_id = ANY({class_sql}) OR path_object.id = ANY({object_sql})))"
            ));
        }
    }
}

fn build_graph_filter_clause(
    kind: GraphKind,
    parameter: &ParsedQueryParam,
    bind_variables: &mut Vec<SqlValue>,
) -> Result<Option<String>, PostgresStorageError> {
    if parameter.field == FilterField::Permissions {
        return Ok(None);
    }
    if matches!(kind, GraphKind::Object)
        && matches!(
            parameter.field,
            FilterField::JsonDataFrom | FilterField::JsonDataTo
        )
    {
        let expression = if parameter.field == FilterField::JsonDataFrom {
            "list_related_objects.ancestor_data"
        } else {
            "list_related_objects.descendant_data"
        };
        let component = json_filter_sql(parameter, expression)?;
        bind_variables.extend(component.bind_variables);
        return Ok(Some(format!("({})", component.sql)));
    }

    let column = graph_column(kind, &parameter.field).ok_or_else(|| {
        PostgresStorageError::invalid_input(format!(
            "Field '{}' isn't searchable (or does not exist) for {}",
            parameter.field,
            kind.resource_label()
        ))
    })?;
    let (operator, negated) = parameter.operator.op_and_neg();
    let wrap = |sql: String| {
        if negated { format!("NOT ({sql})") } else { sql }
    };
    let clause = if graph_numeric_field(&parameter.field) {
        ensure_operator_type(parameter, DataType::NumericOrDate)?;
        let values = integer_values(parameter)?;
        if values.is_empty() {
            return Err(filter_requires_value(parameter));
        }
        let min = values.iter().copied().min().expect("nonempty values");
        let max = values.iter().copied().max().expect("nonempty values");
        match operator {
            Operator::Equals => {
                if matches!(kind, GraphKind::Object) && values.len() > 50 {
                    return Err(PostgresStorageError::invalid_input(format!(
                        "Operator 'equals' is limited to 50 values, got {} (use between?)",
                        values.len()
                    )));
                }
                wrap(format!(
                    "{column} = ANY({})",
                    sql_integer_array(&values, bind_variables)
                ))
            }
            Operator::Gt => bind_comparison(column, ">", max, negated, bind_variables),
            Operator::Gte => bind_comparison(column, ">=", max, negated, bind_variables),
            Operator::Lt => bind_comparison(column, "<", min, negated, bind_variables),
            Operator::Lte => bind_comparison(column, "<=", min, negated, bind_variables),
            Operator::Between if values.len() == 2 => {
                bind_variables.extend(values.iter().copied().map(SqlValue::Integer));
                wrap(format!("{column} BETWEEN ? AND ?"))
            }
            Operator::Between => return Err(between_requires_two(parameter)),
            _ => return Err(unsupported_operator(parameter, "numeric")),
        }
    } else if graph_string_field(&parameter.field) {
        ensure_operator_type(parameter, DataType::String)?;
        let (sql_operator, value) = match operator {
            Operator::Equals => ("=", parameter.value.clone()),
            Operator::IEquals => ("ILIKE", parameter.value.clone()),
            Operator::Contains => ("LIKE", format!("%{}%", parameter.value)),
            Operator::IContains => ("ILIKE", format!("%{}%", parameter.value)),
            Operator::StartsWith => ("LIKE", format!("{}%", parameter.value)),
            Operator::IStartsWith => ("ILIKE", format!("{}%", parameter.value)),
            Operator::EndsWith => ("LIKE", format!("%{}", parameter.value)),
            Operator::IEndsWith => ("ILIKE", format!("%{}", parameter.value)),
            Operator::Like => ("LIKE", parameter.value.clone()),
            Operator::Regex => ("~", parameter.value.clone()),
            _ => return Err(unsupported_operator(parameter, "string")),
        };
        bind_variables.push(SqlValue::String(value));
        wrap(format!("{column} {sql_operator} ?"))
    } else if graph_datetime_field(&parameter.field) {
        ensure_operator_type(parameter, DataType::NumericOrDate)?;
        let values = datetime_values(parameter)?;
        if values.is_empty() {
            return Err(filter_requires_value(parameter));
        }
        let min = values.iter().copied().min().expect("nonempty values");
        let max = values.iter().copied().max().expect("nonempty values");
        match operator {
            Operator::Equals => wrap(format!(
                "{column} = ANY({})",
                sql_datetime_array(&values, bind_variables)
            )),
            Operator::Gt => bind_datetime_comparison(column, ">", max, negated, bind_variables),
            Operator::Gte => bind_datetime_comparison(column, ">=", max, negated, bind_variables),
            Operator::Lt => bind_datetime_comparison(column, "<", min, negated, bind_variables),
            Operator::Lte => bind_datetime_comparison(column, "<=", min, negated, bind_variables),
            Operator::Between if values.len() == 2 => {
                bind_variables.extend(values.iter().copied().map(SqlValue::DateTime));
                wrap(format!("{column} BETWEEN ? AND ?"))
            }
            Operator::Between => return Err(between_requires_two(parameter)),
            _ => return Err(unsupported_operator(parameter, "date")),
        }
    } else if parameter.field == FilterField::Path {
        ensure_operator_type(parameter, DataType::Array)?;
        let values = integer_values(parameter)?;
        if matches!(kind, GraphKind::Object) && values.is_empty() {
            return Err(filter_requires_value(parameter));
        }
        let array = sql_integer_array(&values, bind_variables);
        match operator {
            Operator::Contains => wrap(format!("{column} @> {array}")),
            Operator::Equals => wrap(format!("{column} = {array}")),
            _ => return Err(unsupported_operator(parameter, "array")),
        }
    } else {
        return Err(PostgresStorageError::invalid_input(format!(
            "Field '{}' isn't searchable (or does not exist) for {}",
            parameter.field,
            kind.resource_label()
        )));
    };
    Ok(Some(clause))
}

fn graph_column(kind: GraphKind, field: &FilterField) -> Option<&'static str> {
    match (kind, field) {
        (
            GraphKind::Class,
            FilterField::Id | FilterField::ClassTo | FilterField::ClassId | FilterField::Classes,
        ) => Some("list_related_classes.descendant_class_id"),
        (GraphKind::Class, FilterField::ClassFrom) => {
            Some("list_related_classes.ancestor_class_id")
        }
        (GraphKind::Object, FilterField::Id | FilterField::ObjectTo) => {
            Some("list_related_objects.descendant_object_id")
        }
        (GraphKind::Object, FilterField::ObjectFrom) => {
            Some("list_related_objects.ancestor_object_id")
        }
        (GraphKind::Object, FilterField::ClassFrom) => {
            Some("list_related_objects.ancestor_class_id")
        }
        (GraphKind::Object, FilterField::ClassId | FilterField::Classes | FilterField::ClassTo) => {
            Some("list_related_objects.descendant_class_id")
        }
        (
            GraphKind::Class,
            FilterField::Collections | FilterField::CollectionId | FilterField::CollectionsTo,
        ) => Some("list_related_classes.descendant_collection_id"),
        (GraphKind::Class, FilterField::CollectionsFrom) => {
            Some("list_related_classes.ancestor_collection_id")
        }
        (
            GraphKind::Object,
            FilterField::Collections | FilterField::CollectionId | FilterField::CollectionsTo,
        ) => Some("list_related_objects.descendant_collection_id"),
        (GraphKind::Object, FilterField::CollectionsFrom) => {
            Some("list_related_objects.ancestor_collection_id")
        }
        (GraphKind::Class, FilterField::Name | FilterField::NameTo) => {
            Some("list_related_classes.descendant_name")
        }
        (GraphKind::Class, FilterField::NameFrom) => Some("list_related_classes.ancestor_name"),
        (GraphKind::Object, FilterField::Name | FilterField::NameTo) => {
            Some("list_related_objects.descendant_name")
        }
        (GraphKind::Object, FilterField::NameFrom) => Some("list_related_objects.ancestor_name"),
        (GraphKind::Class, FilterField::Description | FilterField::DescriptionTo) => {
            Some("list_related_classes.descendant_description")
        }
        (GraphKind::Class, FilterField::DescriptionFrom) => {
            Some("list_related_classes.ancestor_description")
        }
        (GraphKind::Object, FilterField::Description | FilterField::DescriptionTo) => {
            Some("list_related_objects.descendant_description")
        }
        (GraphKind::Object, FilterField::DescriptionFrom) => {
            Some("list_related_objects.ancestor_description")
        }
        (GraphKind::Class, FilterField::CreatedAt | FilterField::CreatedAtTo) => {
            Some("list_related_classes.descendant_created_at")
        }
        (GraphKind::Class, FilterField::CreatedAtFrom) => {
            Some("list_related_classes.ancestor_created_at")
        }
        (GraphKind::Object, FilterField::CreatedAt | FilterField::CreatedAtTo) => {
            Some("list_related_objects.descendant_created_at")
        }
        (GraphKind::Object, FilterField::CreatedAtFrom) => {
            Some("list_related_objects.ancestor_created_at")
        }
        (GraphKind::Class, FilterField::UpdatedAt | FilterField::UpdatedAtTo) => {
            Some("list_related_classes.descendant_updated_at")
        }
        (GraphKind::Class, FilterField::UpdatedAtFrom) => {
            Some("list_related_classes.ancestor_updated_at")
        }
        (GraphKind::Object, FilterField::UpdatedAt | FilterField::UpdatedAtTo) => {
            Some("list_related_objects.descendant_updated_at")
        }
        (GraphKind::Object, FilterField::UpdatedAtFrom) => {
            Some("list_related_objects.ancestor_updated_at")
        }
        (GraphKind::Class, FilterField::Depth) => Some("list_related_classes.depth"),
        (GraphKind::Class, FilterField::Path) => Some("list_related_classes.path"),
        (GraphKind::Object, FilterField::Depth) => Some("list_related_objects.depth"),
        (GraphKind::Object, FilterField::Path) => Some("list_related_objects.path"),
        _ => None,
    }
}

fn graph_numeric_field(field: &FilterField) -> bool {
    matches!(
        field,
        FilterField::Id
            | FilterField::ObjectFrom
            | FilterField::ObjectTo
            | FilterField::ClassFrom
            | FilterField::ClassTo
            | FilterField::ClassId
            | FilterField::Classes
            | FilterField::Collections
            | FilterField::CollectionId
            | FilterField::CollectionsFrom
            | FilterField::CollectionsTo
            | FilterField::Depth
    )
}

fn graph_string_field(field: &FilterField) -> bool {
    matches!(
        field,
        FilterField::Name
            | FilterField::NameFrom
            | FilterField::NameTo
            | FilterField::Description
            | FilterField::DescriptionFrom
            | FilterField::DescriptionTo
    )
}

fn graph_datetime_field(field: &FilterField) -> bool {
    matches!(
        field,
        FilterField::CreatedAt
            | FilterField::CreatedAtFrom
            | FilterField::CreatedAtTo
            | FilterField::UpdatedAt
            | FilterField::UpdatedAtFrom
            | FilterField::UpdatedAtTo
    )
}

fn ensure_operator_type(
    parameter: &ParsedQueryParam,
    data_type: DataType,
) -> Result<(), PostgresStorageError> {
    if parameter.operator.is_applicable_to(data_type) {
        Ok(())
    } else {
        Err(PostgresStorageError::invalid_input(format!(
            "Operator '{:?}' is not applicable to field '{}'",
            parameter.operator, parameter.field
        )))
    }
}

fn integer_values(parameter: &ParsedQueryParam) -> Result<Vec<i32>, PostgresStorageError> {
    hubuum_query::parse_integer_list(&parameter.value)
        .map_err(|error| PostgresStorageError::invalid_input(error.to_string()))
}

fn datetime_values(
    parameter: &ParsedQueryParam,
) -> Result<Vec<chrono::NaiveDateTime>, PostgresStorageError> {
    hubuum_query::parse_datetime_list(&parameter.value)
        .map_err(|error| PostgresStorageError::invalid_input(error.to_string()))
}

fn filter_requires_value(parameter: &ParsedQueryParam) -> PostgresStorageError {
    PostgresStorageError::invalid_input(format!(
        "Searching on field '{}' requires a value",
        parameter.field
    ))
}

fn between_requires_two(parameter: &ParsedQueryParam) -> PostgresStorageError {
    PostgresStorageError::invalid_input(format!(
        "Operator 'between' requires 2 values (min,max) for field '{}'",
        parameter.field
    ))
}

fn unsupported_operator(parameter: &ParsedQueryParam, kind: &str) -> PostgresStorageError {
    PostgresStorageError::invalid_input(format!(
        "Operator '{:?}' not implemented for field '{}' (type: {kind})",
        parameter.operator, parameter.field
    ))
}

fn bind_comparison(
    column: &str,
    operator: &str,
    value: i32,
    negated: bool,
    bind_variables: &mut Vec<SqlValue>,
) -> String {
    bind_variables.push(SqlValue::Integer(value));
    let clause = format!("{column} {operator} ?");
    if negated {
        format!("NOT ({clause})")
    } else {
        clause
    }
}

fn bind_datetime_comparison(
    column: &str,
    operator: &str,
    value: chrono::NaiveDateTime,
    negated: bool,
    bind_variables: &mut Vec<SqlValue>,
) -> String {
    bind_variables.push(SqlValue::DateTime(value));
    let clause = format!("{column} {operator} ?");
    if negated {
        format!("NOT ({clause})")
    } else {
        clause
    }
}

fn sql_datetime_array(
    values: &[chrono::NaiveDateTime],
    bind_variables: &mut Vec<SqlValue>,
) -> String {
    bind_variables.extend(values.iter().copied().map(SqlValue::DateTime));
    let placeholders = std::iter::repeat_n("?", values.len())
        .collect::<Vec<_>>()
        .join(", ");
    format!("ARRAY[{placeholders}]::timestamp[]")
}

fn related_depth_upper_bound(
    filters: &[ParsedQueryParam],
) -> Result<Option<i32>, PostgresStorageError> {
    let mut upper_bound = None;
    for filter in filters {
        if filter.field != FilterField::Depth {
            continue;
        }
        let values = integer_values(filter)?;
        let Some(min) = values.iter().copied().min() else {
            continue;
        };
        let max = values.iter().copied().max().unwrap_or(min);
        let candidate = match &filter.operator {
            hubuum_query::SearchOperator::Equals { is_negated: false } => Some(max),
            hubuum_query::SearchOperator::Lt { is_negated: false } => Some(min.saturating_sub(1)),
            hubuum_query::SearchOperator::Lte { is_negated: false } => Some(min),
            hubuum_query::SearchOperator::Between { is_negated: false } => Some(max),
            _ => None,
        };
        if let Some(candidate) = candidate {
            upper_bound =
                Some(upper_bound.map_or(candidate, |current: i32| current.min(candidate)));
        }
    }
    Ok(upper_bound)
}

fn apply_raw_sql_pagination(
    mut spec: RawSqlQuerySpec,
    options: &QueryOptions,
    kind: GraphKind,
) -> Result<RawSqlQuerySpec, PostgresStorageError> {
    let sorts = normalized_graph_sorts(kind, options.sort())?;
    let fields = sorts
        .iter()
        .map(|sort| graph_cursor_field(kind, &sort.field))
        .collect::<Result<Vec<_>, _>>()?;
    if let Some(cursor) = cursor_filter_sql_for_fields(
        &sorts,
        &fields,
        options.cursor().map(|cursor| cursor.as_str()),
    )? {
        if spec.sql.contains("\nWHERE ") {
            spec.sql.push_str("\n  AND ");
        } else {
            spec.sql.push_str("\nWHERE ");
        }
        spec.sql.push_str(&cursor);
    }
    let order = sorts
        .iter()
        .zip(&fields)
        .map(|(sort, field)| order_sql_clause_for_field(sort, field))
        .collect::<Vec<_>>()
        .join(", ");
    spec.sql.push_str("\nORDER BY ");
    spec.sql.push_str(&order);
    if let Some(limit) = options.limit() {
        spec.sql.push_str(&format!("\nLIMIT {limit}"));
    }
    Ok(spec)
}

fn normalized_graph_sorts(
    kind: GraphKind,
    requested: &[SortParam],
) -> Result<Vec<SortParam>, PostgresStorageError> {
    let default = || {
        vec![
            SortParam {
                field: FilterField::Path,
                descending: false,
            },
            SortParam {
                field: kind.default_id_field(),
                descending: false,
            },
        ]
    };
    let mut sorts = if requested.is_empty() {
        default()
    } else {
        requested.to_vec()
    };
    for sort in &sorts {
        graph_cursor_field(kind, &sort.field)?;
    }
    for tie_breaker in default() {
        if !sorts
            .iter()
            .any(|existing| existing.field == tie_breaker.field)
        {
            sorts.push(tie_breaker);
        }
    }
    Ok(sorts)
}

fn graph_cursor_field(
    kind: GraphKind,
    field: &FilterField,
) -> Result<CursorSqlField, PostgresStorageError> {
    let column = graph_column(kind, field).ok_or_else(|| {
        PostgresStorageError::invalid_input(format!(
            "Field '{field}' is not orderable for {}",
            kind.resource_label()
        ))
    })?;
    let sql_type = if graph_numeric_field(field) {
        CursorSqlType::Integer
    } else if graph_string_field(field) {
        CursorSqlType::String
    } else if graph_datetime_field(field) {
        CursorSqlType::DateTime
    } else if *field == FilterField::Path {
        CursorSqlType::IntegerArray
    } else {
        return Err(PostgresStorageError::invalid_input(format!(
            "Field '{field}' is not orderable for {}",
            kind.resource_label()
        )));
    };
    Ok(CursorSqlField {
        column,
        sql_type,
        nullable: false,
    })
}

#[derive(QueryableByName)]
struct CountRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    count: i64,
}

#[derive(Clone, Copy)]
enum GraphWalkEdges {
    Bidirectional,
    Directional {
        direction: StorageRelatedDirection,
        class_relation_id: Option<i32>,
    },
}

#[derive(Clone, Copy)]
enum GraphWalkRanking {
    ByDescendant,
    ByTargetClass {
        class_id: i32,
        sort: StorageRelatedSort,
    },
}

#[derive(Clone, Copy)]
enum GraphWalkProjection {
    DescendantOnly,
    AncestorAndDescendant,
}

struct RootGraphWalkSpec<'a> {
    root_ids: &'a [i32],
    collection_ids: &'a [i32],
    visibility: &'a StorageVisibility,
    max_depth: i32,
    per_root_limit: i32,
    edges: GraphWalkEdges,
    ranking: GraphWalkRanking,
    projection: GraphWalkProjection,
    preserve_alternative_paths: bool,
}

fn build_root_graph_walk_query(spec: RootGraphWalkSpec<'_>) -> RawSqlQuerySpec {
    let mut bind_variables = Vec::new();
    let collection_array_sql = sql_integer_array(spec.collection_ids, &mut bind_variables);
    let valid_scope_objects_sql = scoped_objects_sql(spec.visibility, &mut bind_variables);
    let root_array_sql = sql_integer_array(spec.root_ids, &mut bind_variables);
    let object_edges_sql = object_edges_sql(spec.edges, &mut bind_variables);
    bind_variables.extend([
        SqlValue::Integer(spec.max_depth),
        SqlValue::Integer(spec.max_depth),
    ]);

    let deduplicated_walk_sql = if spec.preserve_alternative_paths {
        r#"    SELECT
        root_object_id,
        ancestor_object_id,
        descendant_object_id,
        depth,
        path
    FROM graph_walk"#
    } else {
        r#"    SELECT DISTINCT ON (root_object_id, descendant_object_id)
        root_object_id,
        ancestor_object_id,
        descendant_object_id,
        depth,
        path
    FROM graph_walk
    ORDER BY root_object_id ASC, descendant_object_id ASC, depth ASC, path ASC"#
    };

    let ranked_walk_sql = match spec.ranking {
        GraphWalkRanking::ByDescendant => r#"    SELECT
        deduped_walk.*,
        row_number() OVER (
            PARTITION BY root_object_id
            ORDER BY descendant_object_id ASC, depth ASC, path ASC
        ) AS related_rank
    FROM deduped_walk"#
            .to_string(),
        GraphWalkRanking::ByTargetClass { class_id, sort } => {
            bind_variables.push(SqlValue::Integer(class_id));
            format!(
                r#"    SELECT
        deduped_walk.*,
        row_number() OVER (
            PARTITION BY deduped_walk.root_object_id
            ORDER BY {}
        ) AS related_rank
    FROM deduped_walk
    JOIN hubuumobject target_object
      ON target_object.id = deduped_walk.descendant_object_id
    WHERE target_object.hubuum_class_id = ?"#,
                related_include_order_sql(sort)
            )
        }
    };
    bind_variables.push(SqlValue::Integer(spec.per_root_limit));

    let final_select_sql = match spec.projection {
        GraphWalkProjection::DescendantOnly => {
            r#"SELECT
    ranked_walk.root_object_id,
    target_object.id AS descendant_object_id,
    ranked_walk.depth,
    ranked_walk.path,
    target_object.name AS descendant_name,
    target_object.collection_id AS descendant_collection_id,
    target_object.hubuum_class_id AS descendant_class_id,
    target_object.description AS descendant_description,
    target_object.data AS descendant_data,
    target_object.created_at AS descendant_created_at,
    target_object.updated_at AS descendant_updated_at,
    target_object.revision AS descendant_revision
FROM ranked_walk
JOIN hubuumobject target_object
  ON target_object.id = ranked_walk.descendant_object_id
WHERE ranked_walk.related_rank <= ?
  AND target_object.collection_id IN (SELECT collection_id FROM valid_collections)
  AND target_object.id IN (SELECT object_id FROM valid_scope_objects)
ORDER BY ranked_walk.root_object_id ASC, ranked_walk.related_rank ASC"#
        }
        GraphWalkProjection::AncestorAndDescendant => {
            r#"SELECT
    ranked_walk.root_object_id,
    source_object.id AS ancestor_object_id,
    target_object.id AS descendant_object_id,
    ranked_walk.depth,
    ranked_walk.path,
    source_object.name AS ancestor_name,
    target_object.name AS descendant_name,
    source_object.collection_id AS ancestor_collection_id,
    target_object.collection_id AS descendant_collection_id,
    source_object.hubuum_class_id AS ancestor_class_id,
    target_object.hubuum_class_id AS descendant_class_id,
    source_object.description AS ancestor_description,
    target_object.description AS descendant_description,
    source_object.data AS ancestor_data,
    target_object.data AS descendant_data,
    source_object.created_at AS ancestor_created_at,
    target_object.created_at AS descendant_created_at,
    source_object.updated_at AS ancestor_updated_at,
    target_object.updated_at AS descendant_updated_at,
    source_object.revision AS ancestor_revision,
    target_object.revision AS descendant_revision
FROM ranked_walk
JOIN hubuumobject source_object
  ON source_object.id = ranked_walk.ancestor_object_id
JOIN hubuumobject target_object
  ON target_object.id = ranked_walk.descendant_object_id
WHERE ranked_walk.related_rank <= ?
  AND source_object.collection_id IN (SELECT collection_id FROM valid_collections)
  AND target_object.collection_id IN (SELECT collection_id FROM valid_collections)
  AND source_object.id IN (SELECT object_id FROM valid_scope_objects)
  AND target_object.id IN (SELECT object_id FROM valid_scope_objects)
ORDER BY ranked_walk.root_object_id ASC, ranked_walk.related_rank ASC"#
        }
    };

    RawSqlQuerySpec {
        sql: format!(
            r#"
WITH RECURSIVE
valid_collections AS (
    SELECT unnest({collection_array_sql}) AS collection_id
),
valid_scope_objects AS (
    {valid_scope_objects_sql}
),
root_objects AS (
    SELECT scoped_root.root_object_id
    FROM unnest({root_array_sql}) AS scoped_root(root_object_id)
    WHERE scoped_root.root_object_id IN (SELECT object_id FROM valid_scope_objects)
),
object_edges AS NOT MATERIALIZED (
{object_edges_sql}
),
graph_walk AS (
    SELECT
        root_objects.root_object_id,
        root_objects.root_object_id AS ancestor_object_id,
        object_edges.target_object_id AS descendant_object_id,
        1 AS depth,
        ARRAY[root_objects.root_object_id, object_edges.target_object_id] AS path
    FROM root_objects
    JOIN object_edges
      ON object_edges.source_object_id = root_objects.root_object_id
    JOIN hubuumobject target_object
      ON target_object.id = object_edges.target_object_id
    WHERE ? >= 1
      AND target_object.collection_id IN (SELECT collection_id FROM valid_collections)
      AND target_object.id IN (SELECT object_id FROM valid_scope_objects)

    UNION ALL

    SELECT
        graph_walk.root_object_id,
        graph_walk.ancestor_object_id,
        object_edges.target_object_id AS descendant_object_id,
        graph_walk.depth + 1,
        graph_walk.path || object_edges.target_object_id
    FROM graph_walk
    JOIN object_edges
      ON object_edges.source_object_id = graph_walk.descendant_object_id
    JOIN hubuumobject target_object
      ON target_object.id = object_edges.target_object_id
    WHERE NOT (object_edges.target_object_id = ANY(graph_walk.path))
      AND graph_walk.depth < ?
      AND target_object.collection_id IN (SELECT collection_id FROM valid_collections)
      AND target_object.id IN (SELECT object_id FROM valid_scope_objects)
),
deduped_walk AS (
{deduplicated_walk_sql}
),
ranked_walk AS (
{ranked_walk_sql}
)
{final_select_sql}
"#
        ),
        bind_variables,
    }
}

fn scoped_objects_sql(
    visibility: &StorageVisibility,
    bind_variables: &mut Vec<SqlValue>,
) -> String {
    let Some(scope) = visibility.resources() else {
        return "SELECT id AS object_id FROM hubuumobject".to_string();
    };
    let collection_id_values = scope
        .collection_ids()
        .iter()
        .map(|id| id.id())
        .collect::<Vec<_>>();
    let class_id_values = scope
        .class_ids()
        .iter()
        .map(|id| id.id())
        .collect::<Vec<_>>();
    let object_id_values = scope
        .object_ids()
        .iter()
        .map(|id| id.id())
        .collect::<Vec<_>>();
    let collection_ids = sql_integer_array(&collection_id_values, bind_variables);
    let class_ids = sql_integer_array(&class_id_values, bind_variables);
    let object_ids = sql_integer_array(&object_id_values, bind_variables);
    format!(
        "SELECT id AS object_id FROM hubuumobject WHERE collection_id = ANY({collection_ids}) OR hubuum_class_id = ANY({class_ids}) OR id = ANY({object_ids})"
    )
}

fn object_edges_sql(edges: GraphWalkEdges, bind_variables: &mut Vec<SqlValue>) -> String {
    match edges {
        GraphWalkEdges::Bidirectional => r#"    SELECT from_hubuum_object_id AS source_object_id, to_hubuum_object_id AS target_object_id
    FROM hubuumobject_relation

    UNION ALL

    SELECT to_hubuum_object_id AS source_object_id, from_hubuum_object_id AS target_object_id
    FROM hubuumobject_relation"#
            .to_string(),
        GraphWalkEdges::Directional {
            direction,
            class_relation_id,
        } => {
            let mut selects = Vec::new();
            if matches!(
                direction,
                StorageRelatedDirection::Any | StorageRelatedDirection::Outgoing
            ) {
                selects.push(directional_edge_sql(
                    "from_hubuum_object_id",
                    "to_hubuum_object_id",
                    class_relation_id,
                    bind_variables,
                ));
            }
            if matches!(
                direction,
                StorageRelatedDirection::Any | StorageRelatedDirection::Incoming
            ) {
                selects.push(directional_edge_sql(
                    "to_hubuum_object_id",
                    "from_hubuum_object_id",
                    class_relation_id,
                    bind_variables,
                ));
            }
            selects.join("\n\n    UNION ALL\n\n")
        }
    }
}

fn directional_edge_sql(
    source_column: &str,
    target_column: &str,
    class_relation_id: Option<i32>,
    bind_variables: &mut Vec<SqlValue>,
) -> String {
    let relation_filter = if let Some(class_relation_id) = class_relation_id {
        bind_variables.push(SqlValue::Integer(class_relation_id));
        "  AND hubuumobject_relation.class_relation_id = ?\n"
    } else {
        ""
    };
    format!(
        r#"    SELECT
        hubuumobject_relation.{source_column} AS source_object_id,
        hubuumobject_relation.{target_column} AS target_object_id
    FROM hubuumobject_relation
    JOIN hubuumobject source_edge_object
      ON source_edge_object.id = hubuumobject_relation.{source_column}
    JOIN hubuumobject target_edge_object
      ON target_edge_object.id = hubuumobject_relation.{target_column}
    WHERE source_edge_object.collection_id IN (SELECT collection_id FROM valid_collections)
      AND target_edge_object.collection_id IN (SELECT collection_id FROM valid_collections)
{relation_filter}"#
    )
}

fn related_include_order_sql(sort: StorageRelatedSort) -> &'static str {
    match sort {
        StorageRelatedSort::Path => "deduped_walk.path ASC, deduped_walk.descendant_object_id ASC",
        StorageRelatedSort::Name => {
            "target_object.name ASC, target_object.id ASC, deduped_walk.path ASC"
        }
        StorageRelatedSort::CreatedAt => {
            "target_object.created_at ASC, target_object.id ASC, deduped_walk.path ASC"
        }
    }
}

fn sql_integer_array(values: &[i32], bind_variables: &mut Vec<SqlValue>) -> String {
    bind_variables.extend(values.iter().copied().map(SqlValue::Integer));
    let placeholders = std::iter::repeat_n("?", values.len())
        .collect::<Vec<_>>()
        .join(", ");
    format!("ARRAY[{placeholders}]::integer[]")
}

fn validate_graph_bounds(max_depth: i32, per_root_limit: i32) -> Result<(), PostgresStorageError> {
    if max_depth < 0 {
        return Err(PostgresStorageError::invalid_input(
            "relation graph depth cannot be negative",
        ));
    }
    if per_root_limit < 0 {
        return Err(PostgresStorageError::invalid_input(
            "relation graph result limit cannot be negative",
        ));
    }
    Ok(())
}

#[derive(Clone, QueryableByName)]
struct ClassGraphQueryRow {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    ancestor_class_id: i32,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    descendant_class_id: i32,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    depth: i32,
    #[diesel(sql_type = diesel::sql_types::Array<diesel::sql_types::Integer>)]
    path: Vec<i32>,
    #[diesel(sql_type = diesel::sql_types::Text)]
    ancestor_name: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    descendant_name: String,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    ancestor_collection_id: i32,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    descendant_collection_id: i32,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Jsonb>)]
    ancestor_json_schema: Option<serde_json::Value>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Jsonb>)]
    descendant_json_schema: Option<serde_json::Value>,
    #[diesel(sql_type = diesel::sql_types::Bool)]
    ancestor_validate_schema: bool,
    #[diesel(sql_type = diesel::sql_types::Bool)]
    descendant_validate_schema: bool,
    #[diesel(sql_type = diesel::sql_types::Text)]
    ancestor_description: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    descendant_description: String,
    #[diesel(sql_type = diesel::sql_types::Timestamp)]
    ancestor_created_at: chrono::NaiveDateTime,
    #[diesel(sql_type = diesel::sql_types::Timestamp)]
    descendant_created_at: chrono::NaiveDateTime,
    #[diesel(sql_type = diesel::sql_types::Timestamp)]
    ancestor_updated_at: chrono::NaiveDateTime,
    #[diesel(sql_type = diesel::sql_types::Timestamp)]
    descendant_updated_at: chrono::NaiveDateTime,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    ancestor_revision: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    descendant_revision: i64,
}

impl ClassGraphQueryRow {
    fn into_storage(self) -> Result<StorageClassGraphRow, PostgresStorageError> {
        let ancestor_resource = StorageGraphResource::new(
            record_metadata_from_raw_revision(
                self.ancestor_class_id,
                self.ancestor_created_at,
                self.ancestor_updated_at,
                self.ancestor_revision,
            )?,
            self.ancestor_name,
            CollectionId::new(self.ancestor_collection_id)?,
            self.ancestor_description,
        );
        let descendant_resource = StorageGraphResource::new(
            record_metadata_from_raw_revision(
                self.descendant_class_id,
                self.descendant_created_at,
                self.descendant_updated_at,
                self.descendant_revision,
            )?,
            self.descendant_name,
            CollectionId::new(self.descendant_collection_id)?,
            self.descendant_description,
        );
        crate::validate_persisted(
            "class graph row",
            StorageClassGraphRow::try_new(
                StorageGraphClass::new(
                    ancestor_resource,
                    self.ancestor_json_schema,
                    self.ancestor_validate_schema,
                ),
                StorageGraphClass::new(
                    descendant_resource,
                    self.descendant_json_schema,
                    self.descendant_validate_schema,
                ),
                self.depth,
                self.path
                    .into_iter()
                    .map(ClassId::new)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
        )
    }
}

#[derive(Clone, QueryableByName)]
struct ObjectGraphQueryRow {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    ancestor_object_id: i32,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    descendant_object_id: i32,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    depth: i32,
    #[diesel(sql_type = diesel::sql_types::Array<diesel::sql_types::Integer>)]
    path: Vec<i32>,
    #[diesel(sql_type = diesel::sql_types::Text)]
    ancestor_name: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    descendant_name: String,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    ancestor_collection_id: i32,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    descendant_collection_id: i32,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    ancestor_class_id: i32,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    descendant_class_id: i32,
    #[diesel(sql_type = diesel::sql_types::Text)]
    ancestor_description: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    descendant_description: String,
    #[diesel(sql_type = diesel::sql_types::Jsonb)]
    ancestor_data: serde_json::Value,
    #[diesel(sql_type = diesel::sql_types::Jsonb)]
    descendant_data: serde_json::Value,
    #[diesel(sql_type = diesel::sql_types::Timestamp)]
    ancestor_created_at: chrono::NaiveDateTime,
    #[diesel(sql_type = diesel::sql_types::Timestamp)]
    descendant_created_at: chrono::NaiveDateTime,
    #[diesel(sql_type = diesel::sql_types::Timestamp)]
    ancestor_updated_at: chrono::NaiveDateTime,
    #[diesel(sql_type = diesel::sql_types::Timestamp)]
    descendant_updated_at: chrono::NaiveDateTime,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    ancestor_revision: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    descendant_revision: i64,
}

impl ObjectGraphQueryRow {
    fn into_storage(self) -> Result<StorageObjectGraphRow, PostgresStorageError> {
        let ancestor = GraphObjectParts {
            id: self.ancestor_object_id,
            name: self.ancestor_name,
            collection_id: self.ancestor_collection_id,
            class_id: self.ancestor_class_id,
            description: self.ancestor_description,
            data: self.ancestor_data,
            created_at: self.ancestor_created_at,
            updated_at: self.ancestor_updated_at,
            revision: self.ancestor_revision,
        }
        .into_storage()?;
        let descendant = GraphObjectParts {
            id: self.descendant_object_id,
            name: self.descendant_name,
            collection_id: self.descendant_collection_id,
            class_id: self.descendant_class_id,
            description: self.descendant_description,
            data: self.descendant_data,
            created_at: self.descendant_created_at,
            updated_at: self.descendant_updated_at,
            revision: self.descendant_revision,
        }
        .into_storage()?;
        crate::validate_persisted(
            "object graph row",
            StorageObjectGraphRow::try_new(
                ancestor,
                descendant,
                self.depth,
                self.path
                    .into_iter()
                    .map(ObjectId::new)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
        )
    }
}

#[derive(QueryableByName)]
struct RelatedObjectIncludeQueryRow {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    root_object_id: i32,
    #[diesel(embed)]
    graph: ObjectGraphQueryRow,
}

impl RelatedObjectIncludeQueryRow {
    fn into_storage(self) -> Result<StorageRelatedObjectIncludeRow, PostgresStorageError> {
        crate::validate_persisted(
            "related object include row",
            StorageRelatedObjectIncludeRow::try_new(
                ObjectId::new(self.root_object_id)?,
                self.graph.into_storage()?,
            ),
        )
    }
}

#[derive(QueryableByName)]
struct RelatedObjectForRootQueryRow {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    root_object_id: i32,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    descendant_object_id: i32,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    depth: i32,
    #[diesel(sql_type = diesel::sql_types::Array<diesel::sql_types::Integer>)]
    path: Vec<i32>,
    #[diesel(sql_type = diesel::sql_types::Text)]
    descendant_name: String,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    descendant_collection_id: i32,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    descendant_class_id: i32,
    #[diesel(sql_type = diesel::sql_types::Text)]
    descendant_description: String,
    #[diesel(sql_type = diesel::sql_types::Jsonb)]
    descendant_data: serde_json::Value,
    #[diesel(sql_type = diesel::sql_types::Timestamp)]
    descendant_created_at: chrono::NaiveDateTime,
    #[diesel(sql_type = diesel::sql_types::Timestamp)]
    descendant_updated_at: chrono::NaiveDateTime,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    descendant_revision: i64,
}

impl RelatedObjectForRootQueryRow {
    fn into_storage(self) -> Result<StorageRelatedObjectForRootRow, PostgresStorageError> {
        let descendant = GraphObjectParts {
            id: self.descendant_object_id,
            name: self.descendant_name,
            collection_id: self.descendant_collection_id,
            class_id: self.descendant_class_id,
            description: self.descendant_description,
            data: self.descendant_data,
            created_at: self.descendant_created_at,
            updated_at: self.descendant_updated_at,
            revision: self.descendant_revision,
        }
        .into_storage()?;
        crate::validate_persisted(
            "related object for-root row",
            StorageRelatedObjectForRootRow::try_new(
                ObjectId::new(self.root_object_id)?,
                descendant,
                self.depth,
                self.path
                    .into_iter()
                    .map(ObjectId::new)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
        )
    }
}

struct GraphObjectParts {
    id: i32,
    name: String,
    collection_id: i32,
    class_id: i32,
    description: String,
    data: serde_json::Value,
    created_at: chrono::NaiveDateTime,
    updated_at: chrono::NaiveDateTime,
    revision: i64,
}

impl GraphObjectParts {
    fn into_storage(self) -> Result<StorageGraphObject, PostgresStorageError> {
        let metadata = record_metadata_from_raw_revision(
            self.id,
            self.created_at,
            self.updated_at,
            self.revision,
        )?;
        let resource = StorageGraphResource::new(
            metadata,
            self.name,
            CollectionId::new(self.collection_id)?,
            self.description,
        );
        Ok(StorageGraphObject::new(
            resource,
            ClassId::new(self.class_id)?,
            self.data,
        ))
    }
}

#[derive(Clone, Copy)]
enum IdMatch {
    Touching,
    Between,
}

async fn class_relations_for_ids(
    runtime: &PostgresRuntime,
    query: StorageRelationIdsQuery,
    mode: IdMatch,
) -> Result<Vec<StorageClassRelation>, PostgresStorageError> {
    let (ids, visibility) = query.into_parts();
    if ids.is_empty() || !visibility.allows_permissions(&[CLASS_RELATION_PERMISSION]) {
        return Ok(Vec::new());
    }
    let ids = ids.into_iter().map(|id| id.id()).collect::<Vec<_>>();
    runtime
        .with_connection(async move |connection| {
            let collection_ids =
                authorized_collection_ids(connection, &visibility, &[CLASS_RELATION_PERMISSION])
                    .await?;
            let options = empty_options();
            let records = build_class_relation_query(
                &options,
                &visibility,
                &collection_ids,
                None,
                None,
                None,
            )?;
            let records = match mode {
                IdMatch::Touching => records.filter(
                    crate::schema::hubuumclass_relation::from_hubuum_class_id
                        .eq_any(&ids)
                        .or(crate::schema::hubuumclass_relation::to_hubuum_class_id.eq_any(&ids)),
                ),
                IdMatch::Between => records
                    .filter(crate::schema::hubuumclass_relation::from_hubuum_class_id.eq_any(&ids))
                    .filter(crate::schema::hubuumclass_relation::to_hubuum_class_id.eq_any(&ids)),
            };
            let rows = records
                .order(crate::schema::hubuumclass_relation::id.asc())
                .select(ClassRelationRow::as_select())
                .load::<ClassRelationRow>(connection)
                .await?;
            rows.into_iter()
                .map(ClassRelationRow::into_storage)
                .collect::<Result<Vec<_>, _>>()
        })
        .await
}

fn build_class_relation_query<'query>(
    options: &'query QueryOptions,
    visibility: &'query StorageVisibility,
    collection_ids: &'query [i32],
    touching_id: Option<i32>,
    from_name_ids: Option<&'query [i32]>,
    to_name_ids: Option<&'query [i32]>,
) -> Result<
    crate::schema::hubuumclass_relation::BoxedQuery<'query, diesel::pg::Pg>,
    PostgresStorageError,
> {
    use crate::schema::{hubuumclass, hubuumclass_relation};

    let visible_class_ids = || {
        hubuumclass::table
            .select(hubuumclass::id)
            .filter(hubuumclass::collection_id.eq_any(collection_ids))
    };
    let mut records = hubuumclass_relation::table
        .filter(hubuumclass_relation::from_hubuum_class_id.eq_any(visible_class_ids()))
        .filter(hubuumclass_relation::to_hubuum_class_id.eq_any(visible_class_ids()))
        .into_boxed();
    if let Some(touching_id) = touching_id {
        records = records.filter(
            hubuumclass_relation::from_hubuum_class_id
                .eq(touching_id)
                .or(hubuumclass_relation::to_hubuum_class_id.eq(touching_id)),
        );
    }
    if let Some(scope) = visibility.resources() {
        let scoped_class_ids = || {
            hubuumclass::table.select(hubuumclass::id).filter(
                hubuumclass::collection_id
                    .eq_any(
                        scope
                            .collection_ids()
                            .iter()
                            .map(|id| id.id())
                            .collect::<Vec<_>>(),
                    )
                    .or(hubuumclass::id.eq_any(
                        scope
                            .class_ids()
                            .iter()
                            .map(|id| id.id())
                            .collect::<Vec<_>>(),
                    )),
            )
        };
        records = records
            .filter(hubuumclass_relation::from_hubuum_class_id.eq_any(scoped_class_ids()))
            .filter(hubuumclass_relation::to_hubuum_class_id.eq_any(scoped_class_ids()));
    }
    if let Some(ids) = from_name_ids {
        records = records.filter(hubuumclass_relation::from_hubuum_class_id.eq_any(ids));
    }
    if let Some(ids) = to_name_ids {
        records = records.filter(hubuumclass_relation::to_hubuum_class_id.eq_any(ids));
    }
    for parameter in options.filters() {
        match parameter.field {
            FilterField::Id => {
                crate::postgres_integer_filter!(records, parameter, hubuumclass_relation::id)
            }
            FilterField::ClassFrom => crate::postgres_integer_filter!(
                records,
                parameter,
                hubuumclass_relation::from_hubuum_class_id
            ),
            FilterField::ClassTo => crate::postgres_integer_filter!(
                records,
                parameter,
                hubuumclass_relation::to_hubuum_class_id
            ),
            FilterField::CreatedAt => crate::postgres_datetime_filter!(
                records,
                parameter,
                hubuumclass_relation::created_at
            ),
            FilterField::UpdatedAt => crate::postgres_datetime_filter!(
                records,
                parameter,
                hubuumclass_relation::updated_at
            ),
            FilterField::Revision => {
                crate::postgres_revision_filter!(records, parameter, hubuumclass_relation::revision)
            }
            FilterField::Permissions | FilterField::ClassFromName | FilterField::ClassToName => {}
            _ => {
                return Err(PostgresStorageError::invalid_input(format!(
                    "Field '{}' isn't searchable (or does not exist) for class relations",
                    parameter.field
                )));
            }
        }
    }
    Ok(records)
}

fn build_object_relation_query<'query>(
    options: &'query QueryOptions,
    visibility: &'query StorageVisibility,
    collection_ids: &'query [i32],
    touching_id: Option<i32>,
) -> Result<
    crate::schema::hubuumobject_relation::BoxedQuery<'query, diesel::pg::Pg>,
    PostgresStorageError,
> {
    use crate::schema::{hubuumobject, hubuumobject_relation};

    let visible_object_ids = || {
        hubuumobject::table
            .select(hubuumobject::id)
            .filter(hubuumobject::collection_id.eq_any(collection_ids))
    };
    let mut records = hubuumobject_relation::table
        .filter(hubuumobject_relation::from_hubuum_object_id.eq_any(visible_object_ids()))
        .filter(hubuumobject_relation::to_hubuum_object_id.eq_any(visible_object_ids()))
        .into_boxed();
    if let Some(touching_id) = touching_id {
        records = records.filter(
            hubuumobject_relation::from_hubuum_object_id
                .eq(touching_id)
                .or(hubuumobject_relation::to_hubuum_object_id.eq(touching_id)),
        );
    }
    if let Some(scope) = visibility.resources() {
        let scoped_object_ids = || {
            hubuumobject::table.select(hubuumobject::id).filter(
                hubuumobject::collection_id
                    .eq_any(
                        scope
                            .collection_ids()
                            .iter()
                            .map(|id| id.id())
                            .collect::<Vec<_>>(),
                    )
                    .or(hubuumobject::hubuum_class_id.eq_any(
                        scope
                            .class_ids()
                            .iter()
                            .map(|id| id.id())
                            .collect::<Vec<_>>(),
                    ))
                    .or(hubuumobject::id.eq_any(
                        scope
                            .object_ids()
                            .iter()
                            .map(|id| id.id())
                            .collect::<Vec<_>>(),
                    )),
            )
        };
        records = records
            .filter(hubuumobject_relation::from_hubuum_object_id.eq_any(scoped_object_ids()))
            .filter(hubuumobject_relation::to_hubuum_object_id.eq_any(scoped_object_ids()));
    }
    for parameter in options.filters() {
        match parameter.field {
            FilterField::Id => {
                crate::postgres_integer_filter!(records, parameter, hubuumobject_relation::id)
            }
            FilterField::ClassRelation => crate::postgres_integer_filter!(
                records,
                parameter,
                hubuumobject_relation::class_relation_id
            ),
            FilterField::ObjectFrom => crate::postgres_integer_filter!(
                records,
                parameter,
                hubuumobject_relation::from_hubuum_object_id
            ),
            FilterField::ObjectTo => crate::postgres_integer_filter!(
                records,
                parameter,
                hubuumobject_relation::to_hubuum_object_id
            ),
            FilterField::CreatedAt => crate::postgres_datetime_filter!(
                records,
                parameter,
                hubuumobject_relation::created_at
            ),
            FilterField::UpdatedAt => crate::postgres_datetime_filter!(
                records,
                parameter,
                hubuumobject_relation::updated_at
            ),
            FilterField::Revision => crate::postgres_revision_filter!(
                records,
                parameter,
                hubuumobject_relation::revision
            ),
            FilterField::Permissions => {}
            _ => {
                return Err(PostgresStorageError::invalid_input(format!(
                    "Field '{}' isn't searchable (or does not exist) for object relations",
                    parameter.field
                )));
            }
        }
    }
    Ok(records)
}

async fn class_name_filter_ids(
    connection: &mut PostgresConnection,
    options: &QueryOptions,
    visibility: &StorageVisibility,
) -> Result<(Option<Vec<i32>>, Option<Vec<i32>>), PostgresStorageError> {
    let from = options
        .filters()
        .iter()
        .find(|parameter| parameter.field == FilterField::ClassFromName);
    let to = options
        .filters()
        .iter()
        .find(|parameter| parameter.field == FilterField::ClassToName);
    if from.is_none() && to.is_none() {
        return Ok((None, None));
    }
    let class_permissions = [
        StorageAuthorizationPermission::ReadCollection,
        StorageAuthorizationPermission::ReadClass,
    ];
    if !visibility.allows_permissions(&class_permissions) {
        return Ok((from.map(|_| Vec::new()), to.map(|_| Vec::new())));
    }
    let collection_ids =
        authorized_collection_ids(connection, visibility, &class_permissions).await?;
    let mut load = async |parameter: &hubuum_query::ParsedQueryParam| {
        use crate::schema::hubuumclass;

        let mut classes = hubuumclass::table
            .filter(hubuumclass::collection_id.eq_any(&collection_ids))
            .into_boxed();
        crate::postgres_string_filter!(classes, parameter, hubuumclass::name);
        if let Some(scope) = visibility.resources() {
            classes = classes.filter(
                hubuumclass::collection_id
                    .eq_any(
                        scope
                            .collection_ids()
                            .iter()
                            .map(|id| id.id())
                            .collect::<Vec<_>>(),
                    )
                    .or(hubuumclass::id.eq_any(
                        scope
                            .class_ids()
                            .iter()
                            .map(|id| id.id())
                            .collect::<Vec<_>>(),
                    )),
            );
        }
        classes
            .select(hubuumclass::id)
            .load::<i32>(connection)
            .await
            .map_err(PostgresStorageError::from)
    };
    let from_ids = match from {
        Some(parameter) => Some(load(parameter).await?),
        None => None,
    };
    let to_ids = match to {
        Some(parameter) => Some(load(parameter).await?),
        None => None,
    };
    Ok((from_ids, to_ids))
}

fn empty_options() -> QueryOptions {
    QueryOptions::new(Vec::new(), Vec::new(), None, None, false)
        .expect("empty query options must be valid")
}

#[derive(Clone, Copy)]
enum RelationKind {
    Class,
    Object,
}

fn relation_cursor_fields(
    options: &QueryOptions,
    kind: RelationKind,
) -> Result<Vec<CursorSqlField>, PostgresStorageError> {
    options
        .sort()
        .iter()
        .map(|sort| relation_cursor_field(&sort.field, kind))
        .collect()
}

fn relation_cursor_field(
    field: &FilterField,
    kind: RelationKind,
) -> Result<CursorSqlField, PostgresStorageError> {
    let (column, sql_type) = match (kind, field) {
        (RelationKind::Class, FilterField::Id) => {
            ("hubuumclass_relation.id", CursorSqlType::Integer)
        }
        (RelationKind::Object, FilterField::Id) => {
            ("hubuumobject_relation.id", CursorSqlType::Integer)
        }
        (RelationKind::Class, FilterField::ClassFrom) => (
            "hubuumclass_relation.from_hubuum_class_id",
            CursorSqlType::Integer,
        ),
        (RelationKind::Class, FilterField::ClassTo) => (
            "hubuumclass_relation.to_hubuum_class_id",
            CursorSqlType::Integer,
        ),
        (RelationKind::Object, FilterField::ClassRelation) => (
            "hubuumobject_relation.class_relation_id",
            CursorSqlType::Integer,
        ),
        (RelationKind::Object, FilterField::ObjectFrom) => (
            "hubuumobject_relation.from_hubuum_object_id",
            CursorSqlType::Integer,
        ),
        (RelationKind::Object, FilterField::ObjectTo) => (
            "hubuumobject_relation.to_hubuum_object_id",
            CursorSqlType::Integer,
        ),
        (RelationKind::Class, FilterField::CreatedAt) => {
            ("hubuumclass_relation.created_at", CursorSqlType::DateTime)
        }
        (RelationKind::Object, FilterField::CreatedAt) => {
            ("hubuumobject_relation.created_at", CursorSqlType::DateTime)
        }
        (RelationKind::Class, FilterField::UpdatedAt) => {
            ("hubuumclass_relation.updated_at", CursorSqlType::DateTime)
        }
        (RelationKind::Object, FilterField::UpdatedAt) => {
            ("hubuumobject_relation.updated_at", CursorSqlType::DateTime)
        }
        (RelationKind::Class, FilterField::Revision) => {
            ("hubuumclass_relation.revision", CursorSqlType::BigInt)
        }
        (RelationKind::Object, FilterField::Revision) => {
            ("hubuumobject_relation.revision", CursorSqlType::BigInt)
        }
        _ => {
            return Err(PostgresStorageError::invalid_input(format!(
                "Field '{field}' is not orderable for relations"
            )));
        }
    };
    Ok(CursorSqlField {
        column,
        sql_type,
        nullable: false,
    })
}

fn validate_positive_id(id: i32, label: &str) -> Result<(), PostgresStorageError> {
    if id <= 0 {
        return Err(PostgresStorageError::invalid_input(format!(
            "{label} must be greater than zero"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use hubuum_domain::PrincipalId;

    use super::*;

    #[test]
    fn root_graph_walk_keeps_bidirectional_edges_inline() {
        let root_ids = [1];
        let collection_ids = [2];
        let visibility = StorageVisibility::new(
            PrincipalId::new(3).unwrap(),
            true,
            None::<Vec<StorageAuthorizationPermission>>,
            None,
        );

        let query = build_root_graph_walk_query(RootGraphWalkSpec {
            root_ids: &root_ids,
            collection_ids: &collection_ids,
            visibility: &visibility,
            max_depth: 4,
            per_root_limit: 250,
            edges: GraphWalkEdges::Bidirectional,
            ranking: GraphWalkRanking::ByDescendant,
            projection: GraphWalkProjection::DescendantOnly,
            preserve_alternative_paths: false,
        });

        assert!(query.sql.contains("object_edges AS NOT MATERIALIZED"));
    }
}
