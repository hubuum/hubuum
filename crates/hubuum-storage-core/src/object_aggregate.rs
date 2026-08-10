use std::fmt;

use async_trait::async_trait;
use hubuum_query::QueryOptions;
use serde_json::Value;

use crate::{AuthorizationPermission, StorageError, StorageVisibility};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObjectAggregateAuthorizationMode {
    Storage,
    Delegated,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageObjectAggregateSort {
    DimensionsAscending,
    DimensionsDescending,
    ObjectCountAscending,
    ObjectCountDescending,
}

#[derive(Clone, PartialEq, Eq)]
pub struct StorageObjectAggregateTarget {
    class_id: i32,
    class_name: String,
    collection_id: i32,
}

impl StorageObjectAggregateTarget {
    #[must_use]
    pub const fn new(class_id: i32, class_name: String, collection_id: i32) -> Self {
        Self {
            class_id,
            class_name,
            collection_id,
        }
    }

    #[must_use]
    pub const fn class_id(&self) -> i32 {
        self.class_id
    }

    #[must_use]
    pub fn class_name(&self) -> &str {
        &self.class_name
    }

    #[must_use]
    pub const fn collection_id(&self) -> i32 {
        self.collection_id
    }
}

impl fmt::Debug for StorageObjectAggregateTarget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageObjectAggregateTarget")
            .field("class_id", &"[redacted]")
            .field("class_name", &"[redacted]")
            .field("collection_id", &"[redacted]")
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct StorageObjectAggregateSpec {
    dimensions: Vec<String>,
    measures: Vec<String>,
    sort: StorageObjectAggregateSort,
}

impl StorageObjectAggregateSpec {
    #[must_use]
    pub fn new(
        dimensions: impl IntoIterator<Item = String>,
        measures: impl IntoIterator<Item = String>,
        sort: StorageObjectAggregateSort,
    ) -> Self {
        Self {
            dimensions: dimensions.into_iter().collect(),
            measures: measures.into_iter().collect(),
            sort,
        }
    }

    #[must_use]
    pub fn dimensions(&self) -> &[String] {
        &self.dimensions
    }

    #[must_use]
    pub fn measures(&self) -> &[String] {
        &self.measures
    }

    #[must_use]
    pub const fn sort(&self) -> StorageObjectAggregateSort {
        self.sort
    }
}

impl fmt::Debug for StorageObjectAggregateSpec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageObjectAggregateSpec")
            .field("dimension_count", &self.dimensions.len())
            .field("measure_count", &self.measures.len())
            .field("sort", &self.sort)
            .finish()
    }
}

#[derive(Clone, PartialEq)]
pub struct ObjectAggregateStorageQuery {
    target: StorageObjectAggregateTarget,
    options: QueryOptions,
    spec: StorageObjectAggregateSpec,
    personal_owner_id: Option<i32>,
    required_permissions: Vec<AuthorizationPermission>,
    visibility: StorageVisibility,
    cursor_max_encoded_bytes: usize,
    authorization_mode: ObjectAggregateAuthorizationMode,
}

pub struct ObjectAggregateStorageQueryBuilder {
    target: StorageObjectAggregateTarget,
    options: QueryOptions,
    spec: StorageObjectAggregateSpec,
    personal_owner_id: Option<i32>,
    required_permissions: Option<Vec<AuthorizationPermission>>,
    visibility: StorageVisibility,
    cursor_max_encoded_bytes: Option<usize>,
    authorization_mode: ObjectAggregateAuthorizationMode,
}

impl ObjectAggregateStorageQuery {
    #[must_use]
    pub const fn builder(
        target: StorageObjectAggregateTarget,
        options: QueryOptions,
        spec: StorageObjectAggregateSpec,
        visibility: StorageVisibility,
    ) -> ObjectAggregateStorageQueryBuilder {
        ObjectAggregateStorageQueryBuilder {
            target,
            options,
            spec,
            personal_owner_id: None,
            required_permissions: None,
            visibility,
            cursor_max_encoded_bytes: None,
            authorization_mode: ObjectAggregateAuthorizationMode::Storage,
        }
    }

    #[must_use]
    pub const fn options(&self) -> &QueryOptions {
        &self.options
    }

    #[must_use]
    pub const fn target(&self) -> &StorageObjectAggregateTarget {
        &self.target
    }

    #[must_use]
    pub const fn spec(&self) -> &StorageObjectAggregateSpec {
        &self.spec
    }

    #[must_use]
    pub const fn personal_owner_id(&self) -> Option<i32> {
        self.personal_owner_id
    }

    #[must_use]
    pub fn required_permissions(&self) -> &[AuthorizationPermission] {
        &self.required_permissions
    }

    #[must_use]
    pub const fn visibility(&self) -> &StorageVisibility {
        &self.visibility
    }

    #[must_use]
    pub const fn cursor_max_encoded_bytes(&self) -> usize {
        self.cursor_max_encoded_bytes
    }

    #[must_use]
    pub const fn authorization_mode(&self) -> ObjectAggregateAuthorizationMode {
        self.authorization_mode
    }
}

impl ObjectAggregateStorageQueryBuilder {
    #[must_use]
    pub const fn personal_owner_id(mut self, personal_owner_id: Option<i32>) -> Self {
        self.personal_owner_id = personal_owner_id;
        self
    }

    #[must_use]
    pub fn required_permissions(
        mut self,
        required_permissions: impl IntoIterator<Item = AuthorizationPermission>,
    ) -> Self {
        self.required_permissions = Some(required_permissions.into_iter().collect());
        self
    }

    #[must_use]
    pub const fn cursor_max_encoded_bytes(mut self, cursor_max_encoded_bytes: usize) -> Self {
        self.cursor_max_encoded_bytes = Some(cursor_max_encoded_bytes);
        self
    }

    #[must_use]
    pub const fn authorization_mode(
        mut self,
        authorization_mode: ObjectAggregateAuthorizationMode,
    ) -> Self {
        self.authorization_mode = authorization_mode;
        self
    }

    pub fn build(self) -> Result<ObjectAggregateStorageQuery, StorageError> {
        let required_permissions = self.required_permissions.ok_or_else(|| {
            StorageError::internal("Object aggregate query is missing required permissions")
        })?;
        if required_permissions.is_empty() {
            return Err(StorageError::bad_request(
                "Object aggregate query requires at least one permission",
            ));
        }
        let cursor_max_encoded_bytes = self.cursor_max_encoded_bytes.ok_or_else(|| {
            StorageError::internal("Object aggregate query is missing its cursor budget")
        })?;
        if cursor_max_encoded_bytes == 0 {
            return Err(StorageError::bad_request(
                "Object aggregate cursor budget must be positive",
            ));
        }
        Ok(ObjectAggregateStorageQuery {
            target: self.target,
            options: self.options,
            spec: self.spec,
            personal_owner_id: self.personal_owner_id,
            required_permissions,
            visibility: self.visibility,
            cursor_max_encoded_bytes,
            authorization_mode: self.authorization_mode,
        })
    }
}

impl fmt::Debug for ObjectAggregateStorageQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ObjectAggregateStorageQuery")
            .field("target", &self.target)
            .field("spec", &self.spec)
            .field("filter_count", &self.options.filters.len())
            .field("limit", &self.options.limit)
            .field("has_cursor", &self.options.cursor.is_some())
            .field("include_total", &self.options.include_total)
            .field("has_personal_owner", &self.personal_owner_id.is_some())
            .field("permission_count", &self.required_permissions.len())
            .field("visibility", &self.visibility)
            .field("cursor_max_encoded_bytes", &self.cursor_max_encoded_bytes)
            .field("authorization_mode", &self.authorization_mode)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct StorageObjectAggregateAuthorizationTarget {
    class_id: i32,
    class_name: String,
    collection_id: i32,
    collection_name: String,
}

impl StorageObjectAggregateAuthorizationTarget {
    #[must_use]
    pub const fn new(
        class_id: i32,
        class_name: String,
        collection_id: i32,
        collection_name: String,
    ) -> Self {
        Self {
            class_id,
            class_name,
            collection_id,
            collection_name,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (i32, String, i32, String) {
        (
            self.class_id,
            self.class_name,
            self.collection_id,
            self.collection_name,
        )
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct StorageObjectAggregateAuthorizationCandidate {
    id: i32,
    name: String,
    collection_id: i32,
    class_id: i32,
}

impl StorageObjectAggregateAuthorizationCandidate {
    #[must_use]
    pub const fn new(id: i32, name: String, collection_id: i32, class_id: i32) -> Self {
        Self {
            id,
            name,
            collection_id,
            class_id,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (i32, String, i32, i32) {
        (self.id, self.name, self.collection_id, self.class_id)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageObjectAggregateMeasureState {
    Value,
    Empty,
}

#[derive(Clone, PartialEq)]
pub struct StorageObjectAggregateMeasureValue {
    state: StorageObjectAggregateMeasureState,
    value_count: i64,
    skipped_count: i64,
    value: Option<Value>,
}

impl StorageObjectAggregateMeasureValue {
    #[must_use]
    pub const fn new(
        state: StorageObjectAggregateMeasureState,
        value_count: i64,
        skipped_count: i64,
        value: Option<Value>,
    ) -> Self {
        Self {
            state,
            value_count,
            skipped_count,
            value,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (StorageObjectAggregateMeasureState, i64, i64, Option<Value>) {
        (self.state, self.value_count, self.skipped_count, self.value)
    }
}

#[derive(Clone, PartialEq)]
pub struct StorageObjectAggregateRow {
    measures: Vec<StorageObjectAggregateMeasureValue>,
    object_count: i64,
    sort_key: Value,
}

impl StorageObjectAggregateRow {
    #[must_use]
    pub const fn new(
        measures: Vec<StorageObjectAggregateMeasureValue>,
        object_count: i64,
        sort_key: Value,
    ) -> Self {
        Self {
            measures,
            object_count,
            sort_key,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (Vec<StorageObjectAggregateMeasureValue>, i64, Value) {
        (self.measures, self.object_count, self.sort_key)
    }
}

#[derive(Clone, PartialEq)]
pub struct StorageObjectAggregatePage {
    rows: Vec<StorageObjectAggregateRow>,
    total: Option<i64>,
    next_cursor: Option<String>,
}

impl StorageObjectAggregatePage {
    #[must_use]
    pub const fn new(
        rows: Vec<StorageObjectAggregateRow>,
        total: Option<i64>,
        next_cursor: Option<String>,
    ) -> Self {
        Self {
            rows,
            total,
            next_cursor,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (Vec<StorageObjectAggregateRow>, Option<i64>, Option<String>) {
        (self.rows, self.total, self.next_cursor)
    }
}

/// Delegated policy decisions required while a backend streams an aggregate.
///
/// Implementations receive only storage-owned DTOs. Decisions must retain the
/// input order and cardinality so the storage backend can reject malformed
/// policy responses before using them.
#[async_trait]
pub trait ObjectAggregateAuthorizer: Send + Sync {
    async fn authorize_target(
        &self,
        target: StorageObjectAggregateAuthorizationTarget,
        required_permissions: Vec<AuthorizationPermission>,
    ) -> Result<bool, StorageError>;

    async fn authorize_objects(
        &self,
        candidates: Vec<StorageObjectAggregateAuthorizationCandidate>,
        required_permissions: Vec<AuthorizationPermission>,
    ) -> Result<Vec<bool>, StorageError>;
}

/// Mandatory backend contract for object aggregation.
#[async_trait]
pub trait ObjectAggregateStorage: Send + Sync {
    async fn aggregate_objects(
        &self,
        query: ObjectAggregateStorageQuery,
        authorizer: Option<&dyn ObjectAggregateAuthorizer>,
    ) -> Result<StorageObjectAggregatePage, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StorageErrorKind;
    use hubuum_query::{FilterField, ParsedQueryParam, SearchOperator};

    fn empty_query_options() -> QueryOptions {
        QueryOptions {
            filters: Vec::new(),
            sort: Vec::new(),
            limit: None,
            cursor: None,
            include_total: false,
        }
    }

    #[test]
    fn aggregate_query_debug_redacts_target_filters_and_cursor() {
        let query = ObjectAggregateStorageQuery::builder(
            StorageObjectAggregateTarget::new(7, "secret class".to_string(), 9),
            QueryOptions {
                filters: vec![ParsedQueryParam {
                    field: FilterField::Name,
                    operator: SearchOperator::Equals { is_negated: false },
                    value: "secret object".to_string(),
                }],
                sort: Vec::new(),
                limit: Some(20),
                cursor: Some("secret cursor".to_string()),
                include_total: true,
            },
            StorageObjectAggregateSpec::new(
                ["secret dimension".to_string()],
                ["secret measure".to_string()],
                StorageObjectAggregateSort::DimensionsAscending,
            ),
            StorageVisibility::new(73, false, None::<[AuthorizationPermission; 0]>, None),
        )
        .personal_owner_id(Some(42))
        .required_permissions([AuthorizationPermission::ReadObject])
        .cursor_max_encoded_bytes(1_000)
        .authorization_mode(ObjectAggregateAuthorizationMode::Delegated)
        .build()
        .unwrap();

        let debug = format!("{query:?}");

        for secret in [
            "secret class",
            "secret object",
            "secret cursor",
            "secret dimension",
            "secret measure",
            "42",
            "73",
        ] {
            assert!(!debug.contains(secret));
        }
    }

    #[test]
    fn aggregate_query_requires_permissions() {
        let result = ObjectAggregateStorageQuery::builder(
            StorageObjectAggregateTarget::new(7, "class".to_string(), 9),
            empty_query_options(),
            StorageObjectAggregateSpec::new(
                ["name".to_string()],
                [],
                StorageObjectAggregateSort::DimensionsAscending,
            ),
            StorageVisibility::new(73, false, None::<[AuthorizationPermission; 0]>, None),
        )
        .required_permissions([])
        .cursor_max_encoded_bytes(1_000)
        .build();

        assert_eq!(result.unwrap_err().kind(), StorageErrorKind::BadRequest);
    }

    #[test]
    fn aggregate_query_requires_a_positive_cursor_budget() {
        let result = ObjectAggregateStorageQuery::builder(
            StorageObjectAggregateTarget::new(7, "class".to_string(), 9),
            empty_query_options(),
            StorageObjectAggregateSpec::new(
                ["name".to_string()],
                [],
                StorageObjectAggregateSort::DimensionsAscending,
            ),
            StorageVisibility::new(73, false, None::<[AuthorizationPermission; 0]>, None),
        )
        .required_permissions([AuthorizationPermission::ReadObject])
        .cursor_max_encoded_bytes(0)
        .build();

        assert_eq!(result.unwrap_err().kind(), StorageErrorKind::BadRequest);
    }
}
