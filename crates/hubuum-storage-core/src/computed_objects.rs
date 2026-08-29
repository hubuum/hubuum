use std::collections::{BTreeMap, HashSet};
use std::fmt;

use async_trait::async_trait;
use hubuum_domain::{ClassId, ObjectId, PrincipalId};
use hubuum_query::{FilterField, QueryOptions, SortParam};

use crate::page::validate_page_total;
use crate::{
    StorageComputationRevision, StorageError, StorageObject, StorageValidationError,
    StorageVisibility,
};

/// Visibility already established for a computed-object query.
///
/// Local-policy backends receive the ordinary principal and token visibility.
/// External policy engines authorize object candidates in the application
/// layer and pass the resulting bounded identifier set back to storage.
#[derive(Clone, PartialEq)]
pub enum StorageComputedObjectVisibility {
    Storage(StorageVisibility),
    AuthorizedObjectIds {
        principal_id: PrincipalId,
        object_ids: Vec<ObjectId>,
    },
}

impl StorageComputedObjectVisibility {
    #[must_use]
    pub const fn storage(visibility: StorageVisibility) -> Self {
        Self::Storage(visibility)
    }

    #[must_use]
    pub fn authorized_object_ids(
        principal_id: PrincipalId,
        object_ids: impl IntoIterator<Item = ObjectId>,
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

impl fmt::Debug for StorageComputedObjectVisibility {
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
pub enum StorageComputedObjectProjection {
    None,
    All,
    CursorBoundary,
}

/// Client-visible and execution forms of one computed-object query.
#[derive(Clone, PartialEq)]
pub struct StorageComputedObjectQueryOptions {
    requested: QueryOptions,
    execution: QueryOptions,
    effective_page_limit: usize,
}

impl StorageComputedObjectQueryOptions {
    /// Preserve both the client-visible query and the backend execution query.
    ///
    /// The application normalizes the execution sort and expands its limit by
    /// exactly one row to discover whether a next page exists. The adapter
    /// resolves computed field types in both copies, but the result retains
    /// the requested query and effective page limit for response pagination.
    pub fn try_new(
        requested: QueryOptions,
        execution: QueryOptions,
        effective_page_limit: usize,
    ) -> Result<Self, StorageError> {
        if effective_page_limit == 0 {
            return Err(StorageError::invalid_input(
                "Computed-object page limit must be greater than zero",
            ));
        }
        if requested
            .limit()
            .is_some_and(|requested_limit| effective_page_limit > requested_limit)
        {
            return Err(StorageError::invalid_input(
                "Computed-object effective page limit must not exceed the requested limit",
            ));
        }
        let execution_limit = effective_page_limit.checked_add(1).ok_or_else(|| {
            StorageError::input_too_large(
                "Computed-object page limit cannot be expanded for cursor pagination",
            )
        })?;
        if execution.limit() != Some(execution_limit) {
            return Err(StorageError::invalid_input(
                "Computed-object execution limit must exceed the page limit by exactly one",
            ));
        }
        if requested.filters() != execution.filters() {
            return Err(StorageError::invalid_input(
                "Computed-object requested and execution queries must use the same filters",
            ));
        }
        if requested.cursor() != execution.cursor() {
            return Err(StorageError::invalid_input(
                "Computed-object requested and execution queries must use the same cursor",
            ));
        }
        if requested.include_total() != execution.include_total() {
            return Err(StorageError::invalid_input(
                "Computed-object requested and execution queries must use the same count intent",
            ));
        }
        let mut expected_sort = if requested.sort().is_empty() {
            vec![SortParam::new(FilterField::Id, false)]
        } else {
            requested.sort().iter().cloned().collect()
        };
        if !expected_sort
            .iter()
            .any(|sort| sort.field() == &FilterField::Id)
        {
            expected_sort.push(SortParam::new(FilterField::Id, false));
        }
        if execution.sort().as_slice() != expected_sort {
            return Err(StorageError::invalid_input(
                "Computed-object execution sort must be the normalized requested sort with an ID tie-breaker",
            ));
        }
        Ok(Self {
            requested,
            execution,
            effective_page_limit,
        })
    }

    #[must_use]
    pub const fn requested(&self) -> &QueryOptions {
        &self.requested
    }

    #[must_use]
    pub const fn effective_page_limit(&self) -> usize {
        self.effective_page_limit
    }

    #[must_use]
    pub fn into_parts(self) -> (QueryOptions, QueryOptions, usize) {
        (self.requested, self.execution, self.effective_page_limit)
    }
}

/// One computed filter/sort query with authorization already selected by the
/// application layer.
#[derive(Clone, PartialEq)]
pub struct StorageComputedObjectListQuery {
    class_id: ClassId,
    personal_owner_id: Option<PrincipalId>,
    options: StorageComputedObjectQueryOptions,
    visibility: StorageComputedObjectVisibility,
    projection: StorageComputedObjectProjection,
}

impl StorageComputedObjectListQuery {
    #[must_use]
    pub const fn new(
        class_id: ClassId,
        personal_owner_id: Option<PrincipalId>,
        options: StorageComputedObjectQueryOptions,
        visibility: StorageComputedObjectVisibility,
        projection: StorageComputedObjectProjection,
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
        ClassId,
        Option<PrincipalId>,
        StorageComputedObjectQueryOptions,
        StorageComputedObjectVisibility,
        StorageComputedObjectProjection,
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

impl fmt::Debug for StorageComputedObjectListQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageComputedObjectListQuery")
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
pub struct StorageComputedObjectEnrichmentQuery {
    objects: Vec<StorageObject>,
    personal_owner_id: Option<PrincipalId>,
}

impl StorageComputedObjectEnrichmentQuery {
    #[must_use]
    pub const fn new(objects: Vec<StorageObject>, personal_owner_id: Option<PrincipalId>) -> Self {
        Self {
            objects,
            personal_owner_id,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (Vec<StorageObject>, Option<PrincipalId>) {
        (self.objects, self.personal_owner_id)
    }
}

impl fmt::Debug for StorageComputedObjectEnrichmentQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageComputedObjectEnrichmentQuery")
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
    revision: StorageComputationRevision,
    materialization_stale: bool,
    scope: StorageComputedScope,
}

impl StorageSharedComputedScope {
    #[must_use]
    pub const fn new(
        revision: StorageComputationRevision,
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
    pub fn into_parts(self) -> (StorageComputationRevision, bool, StorageComputedScope) {
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
/// projections. Every projected object must exactly match a row in the page,
/// and each object can have at most one projection.
#[derive(Clone, PartialEq)]
pub struct StorageComputedObjectPage {
    rows: Vec<StorageObject>,
    total: Option<i64>,
    computed: Vec<StorageComputedObject>,
    resolved_options: QueryOptions,
}

impl StorageComputedObjectPage {
    pub fn try_new(
        rows: Vec<StorageObject>,
        total: Option<i64>,
        computed: Vec<StorageComputedObject>,
        resolved_options: QueryOptions,
    ) -> Result<Self, StorageValidationError> {
        validate_page_total(rows.len(), total)?;
        let mut projected_ids = HashSet::with_capacity(computed.len());
        for projection in &computed {
            if !projected_ids.insert(projection.object.id()) {
                return Err(StorageValidationError::invalid(
                    "Computed-object page projections must have unique object IDs",
                ));
            }
            if !rows.iter().any(|row| row == &projection.object) {
                return Err(StorageValidationError::invalid(
                    "Computed-object page projections must exactly match a returned row",
                ));
            }
        }
        Ok(Self {
            rows,
            total,
            computed,
            resolved_options,
        })
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
        query: StorageComputedObjectListQuery,
    ) -> Result<StorageComputedObjectPage, StorageError>;

    async fn enrich_objects_with_computed(
        &self,
        query: StorageComputedObjectEnrichmentQuery,
    ) -> Result<Vec<StorageComputedObject>, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use hubuum_domain::{CollectionId, ResourceId, ResourceRevision};
    use hubuum_query::{FilterField, ParsedQueryParam, SearchOperator};

    fn object(id: i32, name: &str) -> StorageObject {
        let now = Utc::now();
        StorageObject::new(
            crate::StorageRecordMetadata::try_new(
                ResourceId::new(id).unwrap(),
                now,
                now,
                ResourceRevision::INITIAL,
            )
            .unwrap(),
            name,
            CollectionId::new(1).unwrap(),
            ClassId::new(1).unwrap(),
            serde_json::json!({}),
            "description",
        )
    }

    fn projection(object: StorageObject) -> StorageComputedObject {
        StorageComputedObject::new(
            object,
            StorageSharedComputedScope::new(
                StorageComputationRevision::try_new(0).unwrap(),
                false,
                StorageComputedScope::default(),
            ),
            None,
        )
    }

    fn computed_query(filter: &str, cursor: Option<&str>, include_total: bool) -> QueryOptions {
        QueryOptions::new(
            vec![ParsedQueryParam::from_parts(
                FilterField::Name,
                SearchOperator::Equals { is_negated: false },
                filter,
            )],
            Vec::new(),
            Some(20),
            cursor.map(str::to_string),
            include_total,
        )
        .unwrap()
    }

    fn computed_execution_query(
        filter: &str,
        cursor: Option<&str>,
        include_total: bool,
    ) -> QueryOptions {
        QueryOptions::new(
            vec![ParsedQueryParam::from_parts(
                FilterField::Name,
                SearchOperator::Equals { is_negated: false },
                filter,
            )],
            vec![SortParam::new(FilterField::Id, false)],
            Some(21),
            cursor.map(str::to_string),
            include_total,
        )
        .unwrap()
    }

    #[test]
    fn computed_query_rejects_different_execution_filters() {
        let requested = computed_query("requested", None, true);
        let execution = computed_execution_query("execution", None, true);

        let error = StorageComputedObjectQueryOptions::try_new(requested, execution, 20)
            .err()
            .expect("requested and execution filters must disagree");

        assert_eq!(error.kind(), crate::StorageErrorKind::InvalidInput);
    }

    #[test]
    fn computed_query_rejects_a_different_execution_cursor() {
        let requested = computed_query("object", Some("requested"), true);
        let execution = computed_execution_query("object", Some("execution"), true);

        let error = StorageComputedObjectQueryOptions::try_new(requested, execution, 20)
            .err()
            .expect("requested and execution cursors must disagree");

        assert_eq!(error.kind(), crate::StorageErrorKind::InvalidInput);
    }

    #[test]
    fn computed_query_rejects_different_count_intent() {
        let requested = computed_query("object", None, true);
        let execution = computed_execution_query("object", None, false);

        let error = StorageComputedObjectQueryOptions::try_new(requested, execution, 20)
            .err()
            .expect("requested and execution count intent must disagree");

        assert_eq!(error.kind(), crate::StorageErrorKind::InvalidInput);
    }

    #[test]
    fn computed_query_rejects_an_execution_limit_that_does_not_fetch_one_extra_row() {
        let requested = computed_query("object", None, true);
        let mut execution = computed_execution_query("object", None, true);
        execution.set_limit(Some(1)).unwrap();

        let error = StorageComputedObjectQueryOptions::try_new(requested, execution, 20)
            .err()
            .expect("execution query must fetch exactly one extra row");

        assert_eq!(error.kind(), crate::StorageErrorKind::InvalidInput);
    }

    #[test]
    fn computed_query_accepts_a_policy_capped_effective_page_limit() {
        let requested = QueryOptions::new(Vec::new(), Vec::new(), Some(100), None, false).unwrap();
        let execution = QueryOptions::new(
            Vec::new(),
            vec![SortParam::new(FilterField::Id, false)],
            Some(21),
            None,
            false,
        )
        .unwrap();

        let options = StorageComputedObjectQueryOptions::try_new(requested, execution, 20)
            .expect("application policy may cap the client-requested page limit");

        assert_eq!(options.effective_page_limit(), 20);
    }

    #[test]
    fn computed_query_rejects_an_effective_limit_above_the_requested_limit() {
        let requested = computed_query("object", None, true);
        let mut execution = computed_execution_query("object", None, true);
        execution.set_limit(Some(101)).unwrap();

        let error = StorageComputedObjectQueryOptions::try_new(requested, execution, 100)
            .err()
            .expect("an effective limit may cap but not expand the requested limit");

        assert_eq!(error.kind(), crate::StorageErrorKind::InvalidInput);
    }

    #[test]
    fn computed_query_rejects_an_execution_sort_unrelated_to_the_requested_sort() {
        let requested = computed_query("object", None, true);
        let execution = QueryOptions::new(
            requested.filters().iter().cloned().collect(),
            vec![SortParam::new(FilterField::Name, false)],
            Some(21),
            None,
            true,
        )
        .unwrap();

        let error = StorageComputedObjectQueryOptions::try_new(requested, execution, 20)
            .err()
            .expect("execution query must retain the normalized requested ordering");

        assert_eq!(error.kind(), crate::StorageErrorKind::InvalidInput);
    }

    #[test]
    fn computed_page_rejects_a_projection_not_in_the_returned_rows() {
        let error = StorageComputedObjectPage::try_new(
            vec![object(1, "returned")],
            None,
            vec![projection(object(1, "different"))],
            QueryOptions::empty(),
        )
        .err()
        .expect("a projection must match a returned row");

        assert_eq!(
            error.kind(),
            crate::StorageValidationErrorKind::InvalidValue
        );
    }

    #[test]
    fn computed_page_rejects_duplicate_projection_ids() {
        let row = object(1, "returned");
        let error = StorageComputedObjectPage::try_new(
            vec![row.clone()],
            None,
            vec![projection(row.clone()), projection(row)],
            QueryOptions::empty(),
        )
        .err()
        .expect("projection object IDs must be unique");

        assert_eq!(
            error.kind(),
            crate::StorageValidationErrorKind::InvalidValue
        );
    }

    #[test]
    fn query_debug_redacts_filter_values_ids_and_cursors() {
        let query = StorageComputedObjectListQuery::new(
            ClassId::new(7).unwrap(),
            PrincipalId::new(9).ok(),
            StorageComputedObjectQueryOptions::try_new(
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
                    vec![SortParam::new(FilterField::Id, false)],
                    Some(21),
                    Some("secret cursor".to_string()),
                    true,
                )
                .unwrap(),
                20,
            )
            .unwrap(),
            StorageComputedObjectVisibility::authorized_object_ids(
                PrincipalId::new(42).unwrap(),
                [ObjectId::new(11).unwrap(), ObjectId::new(12).unwrap()],
            ),
            StorageComputedObjectProjection::All,
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
