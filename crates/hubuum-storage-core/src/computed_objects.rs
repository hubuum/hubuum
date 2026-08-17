use std::collections::BTreeMap;
use std::fmt;

use async_trait::async_trait;
use hubuum_query::QueryOptions;

use crate::{StorageError, StorageObject, StorageVisibility};

/// Visibility already established for a computed-object query.
///
/// Local-policy backends receive the ordinary principal and token visibility.
/// External policy engines authorize object candidates in the application
/// layer and pass the resulting bounded identifier set back to storage.
#[derive(Clone, PartialEq)]
pub enum ComputedObjectVisibility {
    Storage(StorageVisibility),
    AuthorizedObjectIds {
        principal_id: i32,
        object_ids: Vec<i32>,
    },
}

impl ComputedObjectVisibility {
    #[must_use]
    pub const fn storage(visibility: StorageVisibility) -> Self {
        Self::Storage(visibility)
    }

    #[must_use]
    pub fn authorized_object_ids(
        principal_id: i32,
        object_ids: impl IntoIterator<Item = i32>,
    ) -> Self {
        let mut object_ids = object_ids.into_iter().collect::<Vec<_>>();
        object_ids.sort_unstable();
        object_ids.dedup();
        Self::AuthorizedObjectIds {
            principal_id,
            object_ids,
        }
    }
}

impl fmt::Debug for ComputedObjectVisibility {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(visibility) => {
                formatter.debug_tuple("Storage").field(visibility).finish()
            }
            Self::AuthorizedObjectIds { object_ids, .. } => formatter
                .debug_struct("AuthorizedObjectIds")
                .field("object_count", &object_ids.len())
                .finish_non_exhaustive(),
        }
    }
}

/// Computed values the backend must attach to a computed-object result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ComputedObjectProjection {
    None,
    All,
    CursorBoundary { page_limit: usize },
}

/// Client-visible and execution forms of one computed-object query.
#[derive(Clone, PartialEq)]
pub struct ComputedObjectQueryOptions {
    requested: QueryOptions,
    execution: QueryOptions,
}

impl ComputedObjectQueryOptions {
    /// Preserve both the client-visible query and the backend execution query.
    ///
    /// The application may increase the execution limit to discover whether a
    /// next page exists. The adapter resolves computed field types in both
    /// copies, but the result must retain the requested limit for response
    /// pagination.
    #[must_use]
    pub const fn new(requested: QueryOptions, execution: QueryOptions) -> Self {
        Self {
            requested,
            execution,
        }
    }

    #[must_use]
    pub const fn requested(&self) -> &QueryOptions {
        &self.requested
    }

    #[must_use]
    pub fn into_parts(self) -> (QueryOptions, QueryOptions) {
        (self.requested, self.execution)
    }
}

/// One computed filter/sort query with authorization already selected by the
/// application layer.
#[derive(Clone, PartialEq)]
pub struct ComputedObjectListQuery {
    class_id: i32,
    personal_owner_id: Option<i32>,
    options: ComputedObjectQueryOptions,
    visibility: ComputedObjectVisibility,
    projection: ComputedObjectProjection,
}

impl ComputedObjectListQuery {
    #[must_use]
    pub const fn new(
        class_id: i32,
        personal_owner_id: Option<i32>,
        options: ComputedObjectQueryOptions,
        visibility: ComputedObjectVisibility,
        projection: ComputedObjectProjection,
    ) -> Self {
        Self {
            class_id,
            personal_owner_id,
            options,
            visibility,
            projection,
        }
    }

    #[must_use]
    pub const fn options(&self) -> &QueryOptions {
        self.options.requested()
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        i32,
        Option<i32>,
        ComputedObjectQueryOptions,
        ComputedObjectVisibility,
        ComputedObjectProjection,
    ) {
        (
            self.class_id,
            self.personal_owner_id,
            self.options,
            self.visibility,
            self.projection,
        )
    }
}

impl fmt::Debug for ComputedObjectListQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ComputedObjectListQuery")
            .field("filter_count", &self.options.requested.filters().len())
            .field("sort_count", &self.options.requested.sort().len())
            .field("limit", &self.options.requested.limit())
            .field("has_cursor", &self.options.requested.cursor().is_some())
            .field("include_total", &self.options.requested.include_total())
            .field("visibility", &self.visibility)
            .field("projection", &self.projection)
            .finish_non_exhaustive()
    }
}

/// Backend-neutral request to enrich an existing object page.
#[derive(Clone, PartialEq)]
pub struct ComputedObjectEnrichmentQuery {
    objects: Vec<StorageObject>,
    personal_owner_id: Option<i32>,
}

impl ComputedObjectEnrichmentQuery {
    #[must_use]
    pub const fn new(objects: Vec<StorageObject>, personal_owner_id: Option<i32>) -> Self {
        Self {
            objects,
            personal_owner_id,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (Vec<StorageObject>, Option<i32>) {
        (self.objects, self.personal_owner_id)
    }
}

impl fmt::Debug for ComputedObjectEnrichmentQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ComputedObjectEnrichmentQuery")
            .field("object_count", &self.objects.len())
            .field("has_personal_owner", &self.personal_owner_id.is_some())
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct StorageComputedFieldError {
    code: String,
    path: Option<String>,
    message: String,
}

impl StorageComputedFieldError {
    #[must_use]
    pub const fn new(code: String, path: Option<String>, message: String) -> Self {
        Self {
            code,
            path,
            message,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (String, Option<String>, String) {
        (self.code, self.path, self.message)
    }
}

#[derive(Clone, Default, PartialEq)]
pub struct StorageComputedScope {
    values: BTreeMap<String, serde_json::Value>,
    errors: BTreeMap<String, StorageComputedFieldError>,
}

impl StorageComputedScope {
    #[must_use]
    pub const fn new(
        values: BTreeMap<String, serde_json::Value>,
        errors: BTreeMap<String, StorageComputedFieldError>,
    ) -> Self {
        Self { values, errors }
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        BTreeMap<String, serde_json::Value>,
        BTreeMap<String, StorageComputedFieldError>,
    ) {
        (self.values, self.errors)
    }
}

#[derive(Clone, PartialEq)]
pub struct StorageSharedComputedScope {
    revision: i64,
    materialization_stale: bool,
    scope: StorageComputedScope,
}

impl StorageSharedComputedScope {
    #[must_use]
    pub const fn new(
        revision: i64,
        materialization_stale: bool,
        scope: StorageComputedScope,
    ) -> Self {
        Self {
            revision,
            materialization_stale,
            scope,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (i64, bool, StorageComputedScope) {
        (self.revision, self.materialization_stale, self.scope)
    }
}

#[derive(Clone, PartialEq)]
pub struct StorageComputedObject {
    object: StorageObject,
    shared: StorageSharedComputedScope,
    personal: Option<StorageComputedScope>,
}

impl StorageComputedObject {
    #[must_use]
    pub const fn new(
        object: StorageObject,
        shared: StorageSharedComputedScope,
        personal: Option<StorageComputedScope>,
    ) -> Self {
        Self {
            object,
            shared,
            personal,
        }
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        StorageObject,
        StorageSharedComputedScope,
        Option<StorageComputedScope>,
    ) {
        (self.object, self.shared, self.personal)
    }
}

/// One backend-selected computed-object page and any requested computed
/// projections. Projections are keyed by the object embedded in each DTO.
#[derive(Clone, PartialEq)]
pub struct ComputedObjectPage {
    rows: Vec<StorageObject>,
    total: Option<i64>,
    computed: Vec<StorageComputedObject>,
    resolved_options: QueryOptions,
}

impl ComputedObjectPage {
    #[must_use]
    pub const fn new(
        rows: Vec<StorageObject>,
        total: Option<i64>,
        computed: Vec<StorageComputedObject>,
        resolved_options: QueryOptions,
    ) -> Self {
        Self {
            rows,
            total,
            computed,
            resolved_options,
        }
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        Vec<StorageObject>,
        Option<i64>,
        Vec<StorageComputedObject>,
        QueryOptions,
    ) {
        (self.rows, self.total, self.computed, self.resolved_options)
    }
}

/// Mandatory backend contract for computed filtering, sorting, paging,
/// counting, and computed-value enrichment.
#[async_trait]
pub trait ComputedObjectStorage: Send + Sync {
    async fn list_computed_objects(
        &self,
        query: ComputedObjectListQuery,
    ) -> Result<ComputedObjectPage, StorageError>;

    async fn enrich_objects_with_computed(
        &self,
        query: ComputedObjectEnrichmentQuery,
    ) -> Result<Vec<StorageComputedObject>, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use hubuum_query::{FilterField, ParsedQueryParam, SearchOperator};

    #[test]
    fn query_debug_redacts_filter_values_ids_and_cursors() {
        let query = ComputedObjectListQuery::new(
            7,
            Some(9),
            ComputedObjectQueryOptions::new(
                QueryOptions::new(
                    vec![ParsedQueryParam::from_parts(
                        FilterField::Name,
                        SearchOperator::Equals { is_negated: false },
                        "secret object",
                    )],
                    Vec::new(),
                    Some(20),
                    Some("secret cursor".to_string()),
                    true,
                )
                .unwrap(),
                QueryOptions::new(
                    vec![ParsedQueryParam::from_parts(
                        FilterField::Name,
                        SearchOperator::Equals { is_negated: false },
                        "secret object",
                    )],
                    Vec::new(),
                    Some(21),
                    Some("secret cursor".to_string()),
                    true,
                )
                .unwrap(),
            ),
            ComputedObjectVisibility::authorized_object_ids(42, [11, 12]),
            ComputedObjectProjection::All,
        );

        let debug = format!("{query:?}");

        assert!(debug.contains("filter_count: 1"));
        assert!(debug.contains("object_count: 2"));
        assert!(!debug.contains("secret object"));
        assert!(!debug.contains("secret cursor"));
        for id in [7, 9, 11, 12, 42] {
            assert!(!debug.contains(&id.to_string()));
        }
    }
}
