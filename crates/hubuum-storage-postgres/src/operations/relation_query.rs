//! PostgreSQL relation listing and endpoint-visibility queries.

use diesel::dsl::not;
use diesel::prelude::{BoolExpressionMethods, ExpressionMethods, QueryDsl};
use diesel::{JoinOnDsl, QueryableByName, SelectableHelper};
use diesel_async::RunQueryDsl;
use hubuum_query::{FilterField, QueryOptions};
use hubuum_storage_core::{
    AuthorizationPermission, BidirectionalRelatedObjectsQuery, ObjectRelationsTouchingIdsQuery,
    RelatedObjectsForRootsQuery, RelationIdsQuery, RelationListQuery, RelationPage,
    RelationTouchingQuery, StorageClassRelation, StorageGraphObject, StorageGraphResource,
    StorageObjectGraphRow, StorageObjectRelation, StorageRecordMetadata, StorageRelatedDirection,
    StorageRelatedObjectForRootRow, StorageRelatedObjectIncludeRow, StorageRelatedSort,
    StorageVisibility,
};

use crate::cursor::{CursorSqlField, CursorSqlType};
use crate::operations::authorization::apply_permission_filter;
use crate::operations::relation::{ClassRelationRow, ObjectRelationRow};
use crate::{PostgresConnection, PostgresRuntime, PostgresStorageError};

const CLASS_RELATION_PERMISSION: AuthorizationPermission =
    AuthorizationPermission::ReadClassRelation;
const OBJECT_RELATION_PERMISSION: AuthorizationPermission =
    AuthorizationPermission::ReadObjectRelation;

macro_rules! bind_integer_query {
    ($spec:expr) => {{
        let spec = $spec.into_indexed_sql();
        let mut query = diesel::sql_query(spec.sql).into_boxed();
        for value in spec.bind_variables {
            query = query.bind::<diesel::sql_types::Integer, _>(value);
        }
        query
    }};
}

/// List visible class relations with stable cursor paging and an optional count.
pub async fn list_class_relations(
    runtime: &PostgresRuntime,
    query: RelationListQuery,
) -> Result<RelationPage<StorageClassRelation>, PostgresStorageError> {
    let include_total = query.options().include_total;
    let (options, visibility) = query.into_parts();
    let permissions = required_permissions(&options, [CLASS_RELATION_PERMISSION])?;
    if !visibility.allows_permissions(&permissions) {
        return Ok(RelationPage::new(Vec::new(), include_total.then_some(0)));
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
            crate::apply_query_options_with_fields!(records, options, fields);
            let rows = records
                .select(ClassRelationRow::as_select())
                .load::<ClassRelationRow>(connection)
                .await?
                .into_iter()
                .map(ClassRelationRow::into_storage)
                .collect();
            Ok::<_, PostgresStorageError>(RelationPage::new(rows, total))
        })
        .await
}

/// List visible object relations with stable cursor paging and an optional count.
pub async fn list_object_relations(
    runtime: &PostgresRuntime,
    query: RelationListQuery,
) -> Result<RelationPage<StorageObjectRelation>, PostgresStorageError> {
    let include_total = query.options().include_total;
    let (options, visibility) = query.into_parts();
    let permissions = required_permissions(&options, [OBJECT_RELATION_PERMISSION])?;
    if !visibility.allows_permissions(&permissions) {
        return Ok(RelationPage::new(Vec::new(), include_total.then_some(0)));
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
            crate::apply_query_options_with_fields!(records, options, fields);
            let rows = records
                .select(ObjectRelationRow::as_select())
                .load::<ObjectRelationRow>(connection)
                .await?
                .into_iter()
                .map(ObjectRelationRow::into_storage)
                .collect();
            Ok::<_, PostgresStorageError>(RelationPage::new(rows, total))
        })
        .await
}

/// List class relations touching one class.
pub async fn list_class_relations_touching(
    runtime: &PostgresRuntime,
    query: RelationTouchingQuery,
) -> Result<RelationPage<StorageClassRelation>, PostgresStorageError> {
    let include_total = query.options().include_total;
    let (class_id, options, visibility) = query.into_parts();
    validate_positive_id(class_id, "class id")?;
    let permissions = required_permissions(&options, [CLASS_RELATION_PERMISSION])?;
    if !visibility.allows_permissions(&permissions) {
        return Ok(RelationPage::new(Vec::new(), include_total.then_some(0)));
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
                    Some(class_id),
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
            crate::apply_query_options_with_fields!(records, options, fields);
            let rows = records
                .select(ClassRelationRow::as_select())
                .load::<ClassRelationRow>(connection)
                .await?
                .into_iter()
                .map(ClassRelationRow::into_storage)
                .collect();
            Ok::<_, PostgresStorageError>(RelationPage::new(rows, total))
        })
        .await
}

/// List object relations touching one object.
pub async fn list_object_relations_touching(
    runtime: &PostgresRuntime,
    query: RelationTouchingQuery,
) -> Result<RelationPage<StorageObjectRelation>, PostgresStorageError> {
    let include_total = query.options().include_total;
    let (object_id, options, visibility) = query.into_parts();
    validate_positive_id(object_id, "object id")?;
    let permissions = required_permissions(&options, [OBJECT_RELATION_PERMISSION])?;
    if !visibility.allows_permissions(&permissions) {
        return Ok(RelationPage::new(Vec::new(), include_total.then_some(0)));
    }

    runtime
        .with_read_only_snapshot(async move |connection| {
            let collection_ids =
                authorized_collection_ids(connection, &visibility, &permissions).await?;
            let build_query = || {
                build_object_relation_query(&options, &visibility, &collection_ids, Some(object_id))
            };
            let total = if include_total {
                Some(build_query()?.count().get_result::<i64>(connection).await?)
            } else {
                None
            };
            let mut records = build_query()?;
            let fields = relation_cursor_fields(&options, RelationKind::Object)?;
            crate::apply_query_options_with_fields!(records, options, fields);
            let rows = records
                .select(ObjectRelationRow::as_select())
                .load::<ObjectRelationRow>(connection)
                .await?
                .into_iter()
                .map(ObjectRelationRow::into_storage)
                .collect();
            Ok::<_, PostgresStorageError>(RelationPage::new(rows, total))
        })
        .await
}

/// Return visible class relations touching any supplied class id.
pub async fn class_relations_touching_ids(
    runtime: &PostgresRuntime,
    query: RelationIdsQuery,
) -> Result<Vec<StorageClassRelation>, PostgresStorageError> {
    class_relations_for_ids(runtime, query, IdMatch::Touching).await
}

/// Return visible class relations whose endpoints are both supplied ids.
pub async fn class_relations_between_ids(
    runtime: &PostgresRuntime,
    query: RelationIdsQuery,
) -> Result<Vec<StorageClassRelation>, PostgresStorageError> {
    class_relations_for_ids(runtime, query, IdMatch::Between).await
}

/// Return visible object relations whose endpoints are both supplied ids.
pub async fn object_relations_between_ids(
    runtime: &PostgresRuntime,
    query: RelationIdsQuery,
) -> Result<Vec<StorageObjectRelation>, PostgresStorageError> {
    let (ids, visibility) = query.into_parts();
    if ids.is_empty() || !visibility.allows_permissions(&[OBJECT_RELATION_PERMISSION]) {
        return Ok(Vec::new());
    }
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
            Ok::<_, PostgresStorageError>(
                rows.into_iter()
                    .map(ObjectRelationRow::into_storage)
                    .collect(),
            )
        })
        .await
}

/// Return a bounded set of visible object relations touching supplied ids.
pub async fn object_relations_touching_ids(
    runtime: &PostgresRuntime,
    query: ObjectRelationsTouchingIdsQuery,
) -> Result<Vec<StorageObjectRelation>, PostgresStorageError> {
    let (ids, excluded_ids, max_results, visibility) = query.into_parts();
    if ids.is_empty()
        || max_results == 0
        || !visibility.allows_permissions(&[OBJECT_RELATION_PERMISSION])
    {
        return Ok(Vec::new());
    }
    let max_results = i64::try_from(max_results).map_err(|_| {
        PostgresStorageError::bad_request("Object-relation result limit is too large")
    })?;
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
            Ok::<_, PostgresStorageError>(
                rows.into_iter()
                    .map(ObjectRelationRow::into_storage)
                    .collect(),
            )
        })
        .await
}

/// Walk directional object relations for several roots in one bounded query.
pub async fn related_objects_for_roots(
    runtime: &PostgresRuntime,
    query: RelatedObjectsForRootsQuery,
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
        AuthorizationPermission::ReadObject,
        AuthorizationPermission::ReadObjectRelation,
    ];
    if !visibility.allows_permissions(&permissions) {
        return Ok(Vec::new());
    }

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
                operation = "related_objects_for_roots",
                root_count = root_ids.len(),
                max_depth,
                per_root_limit = limit,
                "executing PostgreSQL relation graph query"
            );
            let rows = bind_integer_query!(spec)
                .get_results::<RelatedObjectIncludeQueryRow>(connection)
                .await?;
            Ok::<_, PostgresStorageError>(
                rows.into_iter()
                    .map(RelatedObjectIncludeQueryRow::into_storage)
                    .collect(),
            )
        })
        .await
}

/// Walk bidirectional object relations for several roots in one bounded query.
pub async fn bidirectionally_related_objects_for_roots(
    runtime: &PostgresRuntime,
    query: BidirectionalRelatedObjectsQuery,
) -> Result<Vec<StorageRelatedObjectForRootRow>, PostgresStorageError> {
    let (root_ids, max_depth, per_root_cap, preserve_alternative_paths, visibility) =
        query.into_parts();
    if root_ids.is_empty() {
        return Ok(Vec::new());
    }
    validate_graph_bounds(max_depth, per_root_cap)?;
    let permissions = [
        AuthorizationPermission::ReadObject,
        AuthorizationPermission::ReadObjectRelation,
    ];
    if !visibility.allows_permissions(&permissions) {
        return Ok(Vec::new());
    }

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
                operation = "bidirectionally_related_objects_for_roots",
                root_count = root_ids.len(),
                max_depth,
                per_root_limit = per_root_cap,
                "executing PostgreSQL relation graph query"
            );
            let rows = bind_integer_query!(spec)
                .get_results::<RelatedObjectForRootQueryRow>(connection)
                .await?;
            Ok::<_, PostgresStorageError>(
                rows.into_iter()
                    .map(RelatedObjectForRootQueryRow::into_storage)
                    .collect(),
            )
        })
        .await
}

#[derive(Debug)]
struct IntegerQuerySpec {
    sql: String,
    bind_variables: Vec<i32>,
}

impl IntegerQuerySpec {
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

fn build_root_graph_walk_query(spec: RootGraphWalkSpec<'_>) -> IntegerQuerySpec {
    let mut bind_variables = Vec::new();
    let collection_array_sql = sql_integer_array(spec.collection_ids, &mut bind_variables);
    let valid_scope_objects_sql = scoped_objects_sql(spec.visibility, &mut bind_variables);
    let root_array_sql = sql_integer_array(spec.root_ids, &mut bind_variables);
    let object_edges_sql = object_edges_sql(spec.edges, &mut bind_variables);
    bind_variables.extend([spec.max_depth, spec.max_depth]);

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
            bind_variables.push(class_id);
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
    bind_variables.push(spec.per_root_limit);

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

    IntegerQuerySpec {
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
object_edges AS (
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

fn scoped_objects_sql(visibility: &StorageVisibility, bind_variables: &mut Vec<i32>) -> String {
    let Some(scope) = visibility.resources() else {
        return "SELECT id AS object_id FROM hubuumobject".to_string();
    };
    let collection_ids = sql_integer_array(scope.collection_ids(), bind_variables);
    let class_ids = sql_integer_array(scope.class_ids(), bind_variables);
    let object_ids = sql_integer_array(scope.object_ids(), bind_variables);
    format!(
        "SELECT id AS object_id FROM hubuumobject WHERE collection_id = ANY({collection_ids}) OR hubuum_class_id = ANY({class_ids}) OR id = ANY({object_ids})"
    )
}

fn object_edges_sql(edges: GraphWalkEdges, bind_variables: &mut Vec<i32>) -> String {
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
    bind_variables: &mut Vec<i32>,
) -> String {
    let relation_filter = if let Some(class_relation_id) = class_relation_id {
        bind_variables.push(class_relation_id);
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

fn sql_integer_array(values: &[i32], bind_variables: &mut Vec<i32>) -> String {
    bind_variables.extend_from_slice(values);
    let placeholders = std::iter::repeat_n("?", values.len())
        .collect::<Vec<_>>()
        .join(", ");
    format!("ARRAY[{placeholders}]::integer[]")
}

fn validate_graph_bounds(max_depth: i32, per_root_limit: i32) -> Result<(), PostgresStorageError> {
    if max_depth < 0 {
        return Err(PostgresStorageError::bad_request(
            "relation graph depth cannot be negative",
        ));
    }
    if per_root_limit < 0 {
        return Err(PostgresStorageError::bad_request(
            "relation graph result limit cannot be negative",
        ));
    }
    Ok(())
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
    fn into_storage(self) -> StorageObjectGraphRow {
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
        .into_storage();
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
        .into_storage();
        StorageObjectGraphRow::new(ancestor, descendant, self.depth, self.path)
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
    fn into_storage(self) -> StorageRelatedObjectIncludeRow {
        StorageRelatedObjectIncludeRow::new(self.root_object_id, self.graph.into_storage())
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
    fn into_storage(self) -> StorageRelatedObjectForRootRow {
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
        .into_storage();
        StorageRelatedObjectForRootRow::new(self.root_object_id, descendant, self.depth, self.path)
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
    fn into_storage(self) -> StorageGraphObject {
        let metadata =
            StorageRecordMetadata::new(self.id, self.created_at, self.updated_at, self.revision);
        let resource =
            StorageGraphResource::new(metadata, self.name, self.collection_id, self.description);
        StorageGraphObject::new(resource, self.class_id, self.data)
    }
}

#[derive(Clone, Copy)]
enum IdMatch {
    Touching,
    Between,
}

async fn class_relations_for_ids(
    runtime: &PostgresRuntime,
    query: RelationIdsQuery,
    mode: IdMatch,
) -> Result<Vec<StorageClassRelation>, PostgresStorageError> {
    let (ids, visibility) = query.into_parts();
    if ids.is_empty() || !visibility.allows_permissions(&[CLASS_RELATION_PERMISSION]) {
        return Ok(Vec::new());
    }
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
            Ok::<_, PostgresStorageError>(
                rows.into_iter()
                    .map(ClassRelationRow::into_storage)
                    .collect(),
            )
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
                    .eq_any(scope.collection_ids())
                    .or(hubuumclass::id.eq_any(scope.class_ids())),
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
    for parameter in &options.filters {
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
                return Err(PostgresStorageError::bad_request(format!(
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
                    .eq_any(scope.collection_ids())
                    .or(hubuumobject::hubuum_class_id.eq_any(scope.class_ids()))
                    .or(hubuumobject::id.eq_any(scope.object_ids())),
            )
        };
        records = records
            .filter(hubuumobject_relation::from_hubuum_object_id.eq_any(scoped_object_ids()))
            .filter(hubuumobject_relation::to_hubuum_object_id.eq_any(scoped_object_ids()));
    }
    for parameter in &options.filters {
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
                return Err(PostgresStorageError::bad_request(format!(
                    "Field '{}' isn't searchable (or does not exist) for object relations",
                    parameter.field
                )));
            }
        }
    }
    Ok(records)
}

async fn authorized_collection_ids(
    connection: &mut PostgresConnection,
    visibility: &StorageVisibility,
    permissions: &[AuthorizationPermission],
) -> Result<Vec<i32>, PostgresStorageError> {
    use crate::schema::{
        collection_closure, collections, group_memberships, permissions as grants,
    };

    if visibility.is_admin() {
        return collections::table
            .select(collections::id)
            .load(connection)
            .await
            .map_err(PostgresStorageError::from);
    }
    let group_ids = group_memberships::table
        .filter(group_memberships::principal_id.eq(visibility.principal_id()))
        .select(group_memberships::group_id);
    let mut records = grants::table
        .filter(grants::group_id.eq_any(group_ids))
        .into_boxed();
    for permission in permissions.iter().copied() {
        apply_permission_filter!(records, permission, true);
    }
    records
        .inner_join(
            collection_closure::table
                .on(grants::collection_id.eq(collection_closure::ancestor_collection_id)),
        )
        .select(collection_closure::descendant_collection_id)
        .distinct()
        .load(connection)
        .await
        .map_err(PostgresStorageError::from)
}

async fn class_name_filter_ids(
    connection: &mut PostgresConnection,
    options: &QueryOptions,
    visibility: &StorageVisibility,
) -> Result<(Option<Vec<i32>>, Option<Vec<i32>>), PostgresStorageError> {
    let from = options
        .filters
        .iter()
        .find(|parameter| parameter.field == FilterField::ClassFromName);
    let to = options
        .filters
        .iter()
        .find(|parameter| parameter.field == FilterField::ClassToName);
    if from.is_none() && to.is_none() {
        return Ok((None, None));
    }
    let class_permissions = [
        AuthorizationPermission::ReadCollection,
        AuthorizationPermission::ReadClass,
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
                    .eq_any(scope.collection_ids())
                    .or(hubuumclass::id.eq_any(scope.class_ids())),
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

fn required_permissions(
    options: &QueryOptions,
    baseline: impl IntoIterator<Item = AuthorizationPermission>,
) -> Result<Vec<AuthorizationPermission>, PostgresStorageError> {
    let mut permissions = baseline.into_iter().collect::<Vec<_>>();
    for parameter in &options.filters {
        if parameter.field == FilterField::Permissions {
            permissions.push(
                AuthorizationPermission::from_name(&parameter.value)
                    .map_err(|error| PostgresStorageError::bad_request(error.to_string()))?,
            );
        }
    }
    permissions.sort_unstable();
    permissions.dedup();
    Ok(permissions)
}

fn empty_options() -> QueryOptions {
    QueryOptions {
        filters: Vec::new(),
        sort: Vec::new(),
        limit: None,
        cursor: None,
        include_total: false,
    }
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
        .sort
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
            return Err(PostgresStorageError::bad_request(format!(
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
        return Err(PostgresStorageError::bad_request(format!(
            "{label} must be greater than zero"
        )));
    }
    Ok(())
}
