use super::{
    RawSqlQuerySpec, RelatedObjectForRootQueryRow, RelatedObjectIncludeQueryRow, sql_integer_array,
    validate_graph_bounds,
};
use crate::operations::dynamic_sql::SqlValue;
use crate::operations::visibility::authorized_collection_ids;
use crate::{PostgresRuntime, PostgresStorageError};
use diesel_async::RunQueryDsl;
use hubuum_query::TraversalBudget;
use hubuum_storage_core::{
    StorageAuthorizationPermission, StorageBidirectionalRelatedObjectsQuery,
    StorageRelatedDirection, StorageRelatedObjectForRootRow, StorageRelatedObjectIncludeRow,
    StorageRelatedObjectsForRootsQuery, StorageRelatedSort, StorageVisibility,
};

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
        traversal,
        limit,
        preserve_alternative_paths,
        visibility,
    ) = query.into_parts();
    if root_ids.is_empty() {
        return Ok(Vec::new());
    }
    let max_depth = traversal.max_depth();
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
        .with_read_connection(async move |connection| {
            let collection_ids =
                authorized_collection_ids(connection, &visibility, &permissions).await?;
            if collection_ids.is_empty() {
                return Ok(Vec::new());
            }
            let spec = build_root_graph_walk_query(RootGraphWalkSpec {
                root_ids: &root_ids,
                collection_ids: &collection_ids,
                visibility: &visibility,
                traversal,
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
    let (root_ids, traversal, per_root_cap, preserve_alternative_paths, visibility) =
        query.into_parts();
    if root_ids.is_empty() {
        return Ok(Vec::new());
    }
    let max_depth = traversal.max_depth();
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
        .with_read_connection(async move |connection| {
            let collection_ids =
                authorized_collection_ids(connection, &visibility, &permissions).await?;
            if collection_ids.is_empty() {
                return Ok(Vec::new());
            }
            let spec = build_root_graph_walk_query(RootGraphWalkSpec {
                root_ids: &root_ids,
                collection_ids: &collection_ids,
                visibility: &visibility,
                traversal,
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
    traversal: TraversalBudget,
    per_root_limit: i32,
    edges: GraphWalkEdges,
    ranking: GraphWalkRanking,
    projection: GraphWalkProjection,
    preserve_alternative_paths: bool,
}

fn build_root_graph_walk_query(spec: RootGraphWalkSpec<'_>) -> RawSqlQuerySpec {
    let max_work_rows = spec.traversal.max_work_rows();
    let work_lookahead = max_work_rows + 1;
    let mut bind_variables = Vec::new();
    let collection_array_sql = sql_integer_array(spec.collection_ids, &mut bind_variables);
    let valid_scope_objects_sql = scoped_objects_sql(spec.visibility, &mut bind_variables);
    let root_array_sql = sql_integer_array(spec.root_ids, &mut bind_variables);
    let base_object_edges_sql = object_neighbors_sql(
        spec.edges,
        "root_objects.root_object_id",
        &mut bind_variables,
    );
    bind_variables.push(SqlValue::Integer(spec.traversal.max_depth()));
    let recursive_object_edges_sql = object_neighbors_sql(
        spec.edges,
        "graph_walk.descendant_object_id",
        &mut bind_variables,
    );
    bind_variables.push(SqlValue::Integer(spec.traversal.max_depth()));

    let deduplicated_walk_sql = if spec.preserve_alternative_paths {
        r#"    SELECT
        root_object_id,
        ancestor_object_id,
        descendant_object_id,
        depth,
        path
    FROM bounded_walk CROSS JOIN walk_budget
    WHERE walk_budget.allowed"#
    } else {
        r#"    SELECT DISTINCT ON (root_object_id, descendant_object_id)
        root_object_id,
        ancestor_object_id,
        descendant_object_id,
        depth,
        path
    FROM bounded_walk CROSS JOIN walk_budget
    WHERE walk_budget.allowed
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
valid_scope_objects AS NOT MATERIALIZED (
    {valid_scope_objects_sql}
),
root_objects AS (
    SELECT scoped_root.root_object_id
    FROM unnest({root_array_sql}) AS scoped_root(root_object_id)
    JOIN hubuumobject root_object
      ON root_object.id = scoped_root.root_object_id
    WHERE scoped_root.root_object_id IN (SELECT object_id FROM valid_scope_objects)
      AND root_object.collection_id IN (SELECT collection_id FROM valid_collections)
),
graph_walk AS (
    SELECT
        root_objects.root_object_id,
        root_objects.root_object_id AS ancestor_object_id,
        object_edges.target_object_id AS descendant_object_id,
        1 AS depth,
        ARRAY[root_objects.root_object_id, object_edges.target_object_id] AS path
    FROM root_objects
    JOIN LATERAL (
{base_object_edges_sql}
    ) object_edges ON TRUE
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
    JOIN LATERAL (
{recursive_object_edges_sql}
    ) object_edges ON TRUE
    JOIN hubuumobject target_object
      ON target_object.id = object_edges.target_object_id
    WHERE NOT (object_edges.target_object_id = ANY(graph_walk.path))
      AND graph_walk.depth < ?
      AND target_object.collection_id IN (SELECT collection_id FROM valid_collections)
      AND target_object.id IN (SELECT object_id FROM valid_scope_objects)
),
bounded_walk AS MATERIALIZED (
    SELECT * FROM graph_walk LIMIT {work_lookahead}
),
walk_budget AS MATERIALIZED (
    SELECT hubuum_require_graph_budget(count(*), {max_work_rows}) AS allowed FROM bounded_walk
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

fn object_neighbors_sql(
    edges: GraphWalkEdges,
    source_object_sql: &str,
    bind_variables: &mut Vec<SqlValue>,
) -> String {
    match edges {
        GraphWalkEdges::Bidirectional => format!(
            r#"        SELECT hubuumobject_relation.to_hubuum_object_id AS target_object_id
        FROM hubuumobject_relation
        WHERE hubuumobject_relation.from_hubuum_object_id = {source_object_sql}

        UNION ALL

        SELECT hubuumobject_relation.from_hubuum_object_id AS target_object_id
        FROM hubuumobject_relation
        WHERE hubuumobject_relation.to_hubuum_object_id = {source_object_sql}"#
        ),
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
                    source_object_sql,
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
                    source_object_sql,
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
    source_object_sql: &str,
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
        r#"        SELECT hubuumobject_relation.{target_column} AS target_object_id
        FROM hubuumobject_relation
        WHERE hubuumobject_relation.{source_column} = {source_object_sql}
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

#[cfg(test)]
mod tests {
    use hubuum_domain::PrincipalId;

    use super::*;

    #[test]
    fn root_graph_walk_uses_correlated_bidirectional_edge_lookups() {
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
            traversal: TraversalBudget::new(4, hubuum_query::MAX_TRAVERSAL_WORK_ROWS).unwrap(),
            per_root_limit: 250,
            edges: GraphWalkEdges::Bidirectional,
            ranking: GraphWalkRanking::ByDescendant,
            projection: GraphWalkProjection::DescendantOnly,
            preserve_alternative_paths: false,
        });

        assert_eq!(query.sql.matches("JOIN LATERAL").count(), 2);
        assert!(
            query.sql.contains(
                "hubuumobject_relation.from_hubuum_object_id = root_objects.root_object_id"
            )
        );
        assert!(
            query.sql.contains(
                "hubuumobject_relation.to_hubuum_object_id = root_objects.root_object_id"
            )
        );
        assert!(query.sql.contains(
            "hubuumobject_relation.from_hubuum_object_id = graph_walk.descendant_object_id"
        ));
        assert!(query.sql.contains(
            "hubuumobject_relation.to_hubuum_object_id = graph_walk.descendant_object_id"
        ));
        assert!(
            query
                .sql
                .contains("valid_scope_objects AS NOT MATERIALIZED")
        );
    }

    #[test]
    fn root_graph_walk_keeps_directional_edge_filters_on_each_lookup() {
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
            traversal: TraversalBudget::new(3, hubuum_query::MAX_TRAVERSAL_WORK_ROWS).unwrap(),
            per_root_limit: 50,
            edges: GraphWalkEdges::Directional {
                direction: StorageRelatedDirection::Any,
                class_relation_id: Some(11),
            },
            ranking: GraphWalkRanking::ByTargetClass {
                class_id: 4,
                sort: StorageRelatedSort::Path,
            },
            projection: GraphWalkProjection::AncestorAndDescendant,
            preserve_alternative_paths: false,
        });

        assert_eq!(query.sql.matches("JOIN LATERAL").count(), 2);
        assert_eq!(
            query
                .sql
                .matches("hubuumobject_relation.class_relation_id = ?")
                .count(),
            4
        );
        assert_eq!(query.sql.matches('?').count(), query.bind_variables.len());
    }
}
