//! Explicit application adapter hooks for benchmark targets.
//!
//! The root library is an internal application composition crate. These
//! helpers keep benchmark-only construction out of normal service APIs while
//! allowing benchmarks to exercise the same observed storage boundary.

use std::sync::Arc;

use crate::services::CollectionService;
use crate::storage::{ApplicationStorageObserver, CollectionStorage, ObservedStorage};

#[cfg(feature = "postgres-bench")]
use crate::services::Services;
#[cfg(feature = "postgres-bench")]
use crate::storage::{BenchmarkStorageContext, StorageHandle};
#[cfg(feature = "postgres-bench")]
use crate::{
    errors::ApiError,
    models::{
        ExportIncludeRelatedDirection, ExportIncludeRelatedQuery, ExportIncludeRelatedSort,
        HubuumClassID, HubuumObject, RelatedObjectForRootRow, RelatedObjectIncludeRow,
        STRUCTURED_SEARCH_VERSION, StructuredClassSelector, StructuredRelatedPredicate,
        StructuredSearchExpression, StructuredSearchField, StructuredSearchFieldPredicate,
        StructuredSearchOperator, StructuredSearchRequest, StructuredSearchTarget,
    },
};
#[cfg(feature = "postgres-bench")]
use hubuum_storage_postgres::PostgresPool;
#[cfg(feature = "postgres-bench")]
use serde_json::Value;

/// Build the collection service around the production observability wrapper.
///
/// Deterministic benchmarks provide a fixed storage capability so the
/// measurement covers only application-side service, diagnostics, and DTO
/// conversion work.
#[must_use]
pub fn observed_collection_service<S>(storage: S) -> CollectionService
where
    S: CollectionStorage + 'static,
{
    CollectionService::new(Arc::new(ObservedStorage::new(
        storage,
        "benchmark",
        Arc::new(ApplicationStorageObserver),
    )))
}

/// Compose a PostgreSQL pool into the same opaque context used by the
/// application boundary.
#[cfg(feature = "postgres-bench")]
#[must_use]
pub fn storage_for_postgres(pool: PostgresPool) -> BenchmarkStorageContext {
    StorageHandle::postgres(pool)
}

/// Build resource-family services from an already-composed benchmark context.
#[cfg(feature = "postgres-bench")]
#[must_use]
pub fn services_for_storage(storage: &BenchmarkStorageContext) -> Services {
    Services::from_storage(storage.clone())
}

/// Run the production PostgreSQL structured-related-object query used by the
/// self-contained storage benchmark.
///
/// Fixture construction resolves both class IDs before the timed region. The
/// synthetic administrator ID is safe because SQL-backed administrator
/// visibility does not consult group membership, while the catalog adapter
/// still applies its normal collection, class, object, and relation scopes.
#[cfg(feature = "postgres-bench")]
#[doc(hidden)]
pub async fn structured_related_object_search(
    storage: &BenchmarkStorageContext,
    source_class_id: HubuumClassID,
    target_class_id: HubuumClassID,
    target_operator: StructuredSearchOperator,
    target_value: &str,
    depth: u8,
) -> Result<Vec<HubuumObject>, ApiError> {
    let request = StructuredSearchRequest {
        version: STRUCTURED_SEARCH_VERSION,
        target: StructuredSearchTarget::Object {
            class: Some(StructuredClassSelector::Id {
                id: source_class_id,
            }),
        },
        filter: Some(StructuredSearchExpression::Related {
            predicate: StructuredRelatedPredicate {
                class: StructuredClassSelector::Id {
                    id: target_class_id,
                },
                filters: vec![StructuredSearchFieldPredicate {
                    field: StructuredSearchField::Name,
                    operator: target_operator,
                    path: None,
                    value: Some(Value::String(target_value.to_string())),
                }],
                depth,
            },
        }),
        sort: Vec::new(),
        limit: Some(250),
        cursor: None,
        include_total: false,
    };
    // This request is constructed from typed benchmark inputs above. Do not
    // call the public HTTP validator here: page-limit validation reads the
    // application CLI configuration, which must remain uninitialized while
    // Criterion owns the benchmark process arguments.
    let options = request.query_options(Some(source_class_id), None)?;
    let (rows, total) =
        crate::services::catalog::list_objects(storage, 1, true, None, options).await?;
    debug_assert!(total.is_none(), "benchmark search must skip exact counts");
    Ok(rows)
}

/// Run the production bidirectional multi-root graph walk used by
/// relation-aware export hydration.
#[cfg(feature = "postgres-bench")]
#[doc(hidden)]
pub async fn template_multi_root_bidirectional_objects(
    storage: &BenchmarkStorageContext,
    root_object_ids: &[i32],
    max_depth: i32,
    per_root_cap: i32,
) -> Result<Vec<RelatedObjectForRootRow>, ApiError> {
    crate::services::relation_queries::list_bidirectionally_related_objects_for_roots(
        storage,
        crate::services::relation_queries::RelationAccess::new(1, true, None),
        root_object_ids,
        max_depth,
        per_root_cap,
        false,
    )
    .await
}

/// Run the production directional multi-root graph walk used by
/// `include.related_objects` during object exports.
#[cfg(feature = "postgres-bench")]
#[doc(hidden)]
pub async fn template_related_include_objects(
    storage: &BenchmarkStorageContext,
    root_object_ids: &[i32],
    target_class_id: HubuumClassID,
    max_depth: i32,
    per_root_limit: i32,
) -> Result<Vec<RelatedObjectIncludeRow>, ApiError> {
    crate::services::relation_queries::list_related_objects_for_roots(
        storage,
        crate::services::relation_queries::RelationAccess::new(1, true, None),
        root_object_ids,
        ExportIncludeRelatedQuery {
            class_id: target_class_id.id(),
            class_relation_id: None,
            direction: ExportIncludeRelatedDirection::Any,
            sort: ExportIncludeRelatedSort::Path,
            max_depth,
            limit: per_root_limit,
        },
        false,
    )
    .await
}
