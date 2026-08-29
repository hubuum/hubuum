use std::collections::HashSet;
use std::fmt;
use std::str::FromStr;

use async_trait::async_trait;
use base64::Engine;
use hubuum_domain::{ClassId, CollectionId, ObjectId, PrincipalId};
use hubuum_query::{ComputedFieldScope, JsonFieldPath, QueryOptions};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::page::validate_page_total;
use crate::{AuthorizationPermission, StorageError, StorageVisibility};

/// Authorization strategy for one object-aggregate operation.
///
/// The delegated variant carries the required authorizer so callers cannot
/// select delegated authorization without supplying it, or attach an
/// authorizer to storage-owned authorization.
pub enum ObjectAggregateAuthorization<'authorizer> {
    Storage,
    Delegated(&'authorizer dyn ObjectAggregateAuthorizer),
}

impl fmt::Debug for ObjectAggregateAuthorization<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Storage => "Storage",
            Self::Delegated(_) => "Delegated",
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageObjectAggregateSort {
    DimensionsAscending,
    DimensionsDescending,
    ObjectCountAscending,
    ObjectCountDescending,
}

const MAX_OBJECT_AGGREGATE_DIMENSIONS: usize = 3;
const MAX_OBJECT_AGGREGATE_MEASURES: usize = 4;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageObjectAggregateScalarField {
    Name,
    Description,
    CollectionId,
    CreatedAt,
    UpdatedAt,
}

impl StorageObjectAggregateScalarField {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Name => "name",
            Self::Description => "description",
            Self::CollectionId => "collection_id",
            Self::CreatedAt => "created_at",
            Self::UpdatedAt => "updated_at",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageComputedFieldSelector {
    scope: ComputedFieldScope,
    key: String,
}

impl StorageComputedFieldSelector {
    pub fn new(scope: ComputedFieldScope, key: impl Into<String>) -> Result<Self, StorageError> {
        let key = key.into();
        let valid = !key.is_empty()
            && key.len() <= 64
            && key
                .bytes()
                .enumerate()
                .all(|(index, byte)| match (index, byte) {
                    (0, b'a'..=b'z') => true,
                    (0, _) => false,
                    (_, b'a'..=b'z' | b'0'..=b'9' | b'_') => true,
                    (_, _) => false,
                });
        if !valid {
            return Err(StorageError::invalid_input(format!(
                "Invalid computed aggregate field key '{key}'"
            )));
        }
        Ok(Self { scope, key })
    }

    #[must_use]
    pub const fn scope(&self) -> ComputedFieldScope {
        self.scope
    }

    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    #[must_use]
    pub fn canonical(&self) -> String {
        format!("computed.{}.{}", self.scope.as_str(), self.key)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StorageObjectAggregateDimension {
    Scalar(StorageObjectAggregateScalarField),
    JsonData(JsonFieldPath),
    Computed(StorageComputedFieldSelector),
}

impl StorageObjectAggregateDimension {
    #[must_use]
    pub fn canonical(&self) -> String {
        match self {
            Self::Scalar(field) => field.as_str().to_string(),
            Self::JsonData(path) => format!("json_data.{}", path.canonical()),
            Self::Computed(selector) => selector.canonical(),
        }
    }

    #[must_use]
    pub const fn computed_selector(&self) -> Option<&StorageComputedFieldSelector> {
        match self {
            Self::Computed(selector) => Some(selector),
            Self::Scalar(_) | Self::JsonData(_) => None,
        }
    }
}

impl FromStr for StorageObjectAggregateDimension {
    type Err = StorageError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let scalar = match value {
            "name" => Some(StorageObjectAggregateScalarField::Name),
            "description" => Some(StorageObjectAggregateScalarField::Description),
            "collection_id" => Some(StorageObjectAggregateScalarField::CollectionId),
            "created_at" => Some(StorageObjectAggregateScalarField::CreatedAt),
            "updated_at" => Some(StorageObjectAggregateScalarField::UpdatedAt),
            _ => None,
        };
        if let Some(scalar) = scalar {
            return Ok(Self::Scalar(scalar));
        }
        if let Some(path) = value.strip_prefix("json_data.") {
            return JsonFieldPath::new(path)
                .map(Self::JsonData)
                .map_err(|error| StorageError::invalid_input(error.to_string()));
        }
        if let Some(key) = value.strip_prefix("computed.shared.") {
            return StorageComputedFieldSelector::new(ComputedFieldScope::Shared, key)
                .map(Self::Computed);
        }
        if let Some(key) = value.strip_prefix("computed.personal.") {
            return StorageComputedFieldSelector::new(ComputedFieldScope::Personal, key)
                .map(Self::Computed);
        }
        Err(StorageError::invalid_input(format!(
            "Invalid object aggregate dimension '{value}'; use an allowed object field, json_data path, or computed selector"
        )))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageObjectAggregateMeasureOperation {
    Sum,
    Average,
    Min,
    Max,
}

impl StorageObjectAggregateMeasureOperation {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sum => "sum",
            Self::Average => "average",
            Self::Min => "min",
            Self::Max => "max",
        }
    }
}

impl FromStr for StorageObjectAggregateMeasureOperation {
    type Err = StorageError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "sum" => Ok(Self::Sum),
            "average" => Ok(Self::Average),
            "min" => Ok(Self::Min),
            "max" => Ok(Self::Max),
            _ => Err(StorageError::invalid_input(format!(
                "Invalid object aggregate operation '{value}'; use sum, average, min, or max"
            ))),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StorageObjectAggregateMeasureField {
    JsonData(JsonFieldPath),
    Computed(StorageComputedFieldSelector),
}

impl StorageObjectAggregateMeasureField {
    #[must_use]
    pub fn canonical(&self) -> String {
        match self {
            Self::JsonData(path) => format!("json_data.{}", path.canonical()),
            Self::Computed(selector) => selector.canonical(),
        }
    }

    #[must_use]
    pub const fn computed_selector(&self) -> Option<&StorageComputedFieldSelector> {
        match self {
            Self::Computed(selector) => Some(selector),
            Self::JsonData(_) => None,
        }
    }
}

impl FromStr for StorageObjectAggregateMeasureField {
    type Err = StorageError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if let Some(path) = value.strip_prefix("json_data.") {
            return JsonFieldPath::new(path)
                .map(Self::JsonData)
                .map_err(|error| StorageError::invalid_input(error.to_string()));
        }
        if let Some(key) = value.strip_prefix("computed.shared.") {
            return StorageComputedFieldSelector::new(ComputedFieldScope::Shared, key)
                .map(Self::Computed);
        }
        if let Some(key) = value.strip_prefix("computed.personal.") {
            return StorageComputedFieldSelector::new(ComputedFieldScope::Personal, key)
                .map(Self::Computed);
        }
        Err(StorageError::invalid_input(format!(
            "Invalid object aggregate measure field '{value}'; use a json_data path or computed selector"
        )))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageObjectAggregateMeasure {
    operation: StorageObjectAggregateMeasureOperation,
    field: StorageObjectAggregateMeasureField,
}

impl StorageObjectAggregateMeasure {
    #[must_use]
    pub const fn new(
        operation: StorageObjectAggregateMeasureOperation,
        field: StorageObjectAggregateMeasureField,
    ) -> Self {
        Self { operation, field }
    }

    #[must_use]
    pub const fn operation(&self) -> StorageObjectAggregateMeasureOperation {
        self.operation
    }

    #[must_use]
    pub const fn field(&self) -> &StorageObjectAggregateMeasureField {
        &self.field
    }

    #[must_use]
    pub const fn computed_selector(&self) -> Option<&StorageComputedFieldSelector> {
        self.field.computed_selector()
    }

    #[must_use]
    pub fn canonical(&self) -> String {
        format!("{}:{}", self.operation.as_str(), self.field.canonical())
    }
}

impl FromStr for StorageObjectAggregateMeasure {
    type Err = StorageError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (operation, field) = value.split_once(':').ok_or_else(|| {
            StorageError::invalid_input(format!(
                "Invalid object aggregate measure '{value}'; use operation:field"
            ))
        })?;
        Ok(Self::new(
            StorageObjectAggregateMeasureOperation::from_str(operation)?,
            StorageObjectAggregateMeasureField::from_str(field)?,
        ))
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct StorageObjectAggregateTarget {
    class_id: ClassId,
    class_name: String,
    collection_id: CollectionId,
}

impl StorageObjectAggregateTarget {
    #[must_use]
    pub const fn new(class_id: ClassId, class_name: String, collection_id: CollectionId) -> Self {
        Self {
            class_id,
            class_name,
            collection_id,
        }
    }

    #[must_use]
    pub const fn class_id(&self) -> ClassId {
        self.class_id
    }

    #[must_use]
    pub fn class_name(&self) -> &str {
        &self.class_name
    }

    #[must_use]
    pub const fn collection_id(&self) -> CollectionId {
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
    dimensions: Vec<StorageObjectAggregateDimension>,
    measures: Vec<StorageObjectAggregateMeasure>,
    sort: StorageObjectAggregateSort,
}

impl StorageObjectAggregateSpec {
    pub fn new(
        dimensions: impl IntoIterator<Item = StorageObjectAggregateDimension>,
        measures: impl IntoIterator<Item = StorageObjectAggregateMeasure>,
        sort: StorageObjectAggregateSort,
    ) -> Result<Self, StorageError> {
        let dimensions = dimensions.into_iter().collect::<Vec<_>>();
        let measures = measures.into_iter().collect::<Vec<_>>();
        if dimensions.is_empty() && measures.is_empty() {
            return Err(StorageError::invalid_input(
                "Object aggregation requires at least one group_by dimension or aggregate measure",
            ));
        }
        if dimensions.len() > MAX_OBJECT_AGGREGATE_DIMENSIONS {
            return Err(StorageError::invalid_input(format!(
                "Object aggregation supports at most {MAX_OBJECT_AGGREGATE_DIMENSIONS} group_by dimensions"
            )));
        }
        if measures.len() > MAX_OBJECT_AGGREGATE_MEASURES {
            return Err(StorageError::invalid_input(format!(
                "Object aggregation supports at most {MAX_OBJECT_AGGREGATE_MEASURES} aggregate measures"
            )));
        }
        let mut seen = HashSet::with_capacity(dimensions.len());
        if let Some(duplicate) = dimensions
            .iter()
            .map(StorageObjectAggregateDimension::canonical)
            .find(|field| !seen.insert(field.clone()))
        {
            return Err(StorageError::invalid_input(format!(
                "Duplicate object aggregate dimension '{duplicate}'"
            )));
        }
        let mut seen = HashSet::with_capacity(measures.len());
        if let Some(duplicate) = measures
            .iter()
            .map(StorageObjectAggregateMeasure::canonical)
            .find(|measure| !seen.insert(measure.clone()))
        {
            return Err(StorageError::invalid_input(format!(
                "Duplicate object aggregate measure '{duplicate}'"
            )));
        }
        Ok(Self {
            dimensions,
            measures,
            sort,
        })
    }

    #[must_use]
    pub fn dimensions(&self) -> &[StorageObjectAggregateDimension] {
        &self.dimensions
    }

    #[must_use]
    pub fn measures(&self) -> &[StorageObjectAggregateMeasure] {
        &self.measures
    }

    #[must_use]
    pub const fn sort(&self) -> StorageObjectAggregateSort {
        self.sort
    }

    #[must_use]
    pub fn has_computed_field(&self) -> bool {
        self.dimensions
            .iter()
            .any(|dimension| dimension.computed_selector().is_some())
            || self
                .measures
                .iter()
                .any(|measure| measure.computed_selector().is_some())
    }

    #[must_use]
    pub fn requires_object_data(&self) -> bool {
        self.dimensions.iter().any(|dimension| {
            matches!(
                dimension,
                StorageObjectAggregateDimension::JsonData(_)
                    | StorageObjectAggregateDimension::Computed(_)
            )
        }) || !self.measures.is_empty()
    }

    pub fn computed_selectors(&self) -> impl Iterator<Item = &StorageComputedFieldSelector> {
        self.dimensions
            .iter()
            .filter_map(StorageObjectAggregateDimension::computed_selector)
            .chain(
                self.measures
                    .iter()
                    .filter_map(StorageObjectAggregateMeasure::computed_selector),
            )
    }

    fn dimension_names(&self) -> Vec<String> {
        self.dimensions
            .iter()
            .map(StorageObjectAggregateDimension::canonical)
            .collect()
    }

    fn measure_names(&self) -> Vec<String> {
        self.measures
            .iter()
            .map(StorageObjectAggregateMeasure::canonical)
            .collect()
    }

    pub fn decode_cursor(
        &self,
        cursor: &str,
        maximum_encoded_bytes: usize,
    ) -> Result<StorageObjectAggregateCursor, StorageError> {
        if cursor.len() > maximum_encoded_bytes {
            return Err(StorageError::input_too_large(format!(
                "aggregate cursor exceeds the replay-safe limit of {maximum_encoded_bytes} bytes for this request"
            )));
        }
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(cursor)
            .map_err(|error| {
                StorageError::invalid_input(format!("invalid aggregate cursor: {error}"))
            })?;
        let token: StorageObjectAggregateCursorToken =
            serde_json::from_slice(&bytes).map_err(|error| {
                StorageError::invalid_input(format!("invalid aggregate cursor: {error}"))
            })?;
        if token.version != 1
            || token.dimensions != self.dimension_names()
            || token.measures != self.measure_names()
            || token.sort != self.sort
        {
            return Err(StorageError::invalid_input(
                "aggregate cursor does not match the current dimensions, measures, and sort",
            ));
        }
        let sort_key_is_valid = token.sort_key.as_array().is_some_and(|values| {
            values.len() == self.dimensions.len()
                && values
                    .iter()
                    .zip(&self.dimensions)
                    .all(|(value, dimension)| valid_cursor_dimension_value(value, dimension))
        });
        if !sort_key_is_valid || token.object_count <= 0 {
            return Err(StorageError::invalid_input(
                "aggregate cursor contains invalid ordering values",
            ));
        }
        // The contract-specific shape was checked above; retain the shared
        // constructor check so this type cannot gain a second invalid path.
        StorageObjectAggregateCursor::try_new(token.sort_key, token.object_count).map_err(|_| {
            StorageError::invalid_input("aggregate cursor contains invalid ordering values")
        })
    }

    pub fn encode_cursor(
        &self,
        row: &StorageObjectAggregateRow,
        maximum_encoded_bytes: usize,
    ) -> Result<String, StorageError> {
        let token = StorageObjectAggregateCursorToken {
            version: 1,
            dimensions: self.dimension_names(),
            measures: self.measure_names(),
            sort: self.sort,
            sort_key: row.sort_key.clone(),
            object_count: row.object_count,
        };
        let bytes = serde_json::to_vec(&token).map_err(|error| {
            StorageError::internal(format!("failed to serialize aggregate cursor: {error}"))
        })?;
        let cursor = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
        if cursor.len() > maximum_encoded_bytes {
            return Err(StorageError::input_too_large(format!(
                "aggregate value at the page boundary produces a cursor larger than the replay-safe limit of {maximum_encoded_bytes} bytes for this request; shorten the filters, narrow the grouping dimensions, or use a page limit that does not end on this value"
            )));
        }
        Ok(cursor)
    }
}

fn valid_cursor_dimension_value(
    value: &Value,
    dimension: &StorageObjectAggregateDimension,
) -> bool {
    let Some(pair) = value.as_array().filter(|pair| pair.len() == 2) else {
        return false;
    };
    let Some(state) = pair[0].as_u64() else {
        return false;
    };
    match state {
        0 => valid_cursor_present_value(&pair[1], dimension),
        1 => !matches!(dimension, StorageObjectAggregateDimension::Scalar(_)) && pair[1].is_null(),
        2 => matches!(dimension, StorageObjectAggregateDimension::JsonData(_)) && pair[1].is_null(),
        3 => matches!(dimension, StorageObjectAggregateDimension::Computed(_)) && pair[1].is_null(),
        _ => false,
    }
}

fn valid_cursor_present_value(value: &Value, dimension: &StorageObjectAggregateDimension) -> bool {
    match dimension {
        StorageObjectAggregateDimension::Scalar(StorageObjectAggregateScalarField::Name)
        | StorageObjectAggregateDimension::Scalar(StorageObjectAggregateScalarField::Description) => {
            value.is_string()
        }
        StorageObjectAggregateDimension::Scalar(
            StorageObjectAggregateScalarField::CollectionId,
        ) => value
            .as_i64()
            .and_then(|value| i32::try_from(value).ok())
            .is_some_and(|value| value > 0),
        StorageObjectAggregateDimension::Scalar(
            StorageObjectAggregateScalarField::CreatedAt
            | StorageObjectAggregateScalarField::UpdatedAt,
        ) => value
            .as_str()
            .is_some_and(|value| value.parse::<chrono::NaiveDateTime>().is_ok()),
        StorageObjectAggregateDimension::JsonData(_)
        | StorageObjectAggregateDimension::Computed(_) => !value.is_null(),
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct StorageObjectAggregateCursorToken {
    version: u8,
    dimensions: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    measures: Vec<String>,
    sort: StorageObjectAggregateSort,
    sort_key: Value,
    object_count: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StorageObjectAggregateCursor {
    sort_key: Value,
    object_count: i64,
}

impl StorageObjectAggregateCursor {
    pub fn try_new(sort_key: Value, object_count: i64) -> Result<Self, StorageError> {
        let Some(dimension_values) = sort_key.as_array() else {
            return Err(StorageError::internal(
                "An object aggregate cursor sort key must be an array",
            ));
        };
        if dimension_values.len() > MAX_OBJECT_AGGREGATE_DIMENSIONS {
            return Err(StorageError::internal(
                "An object aggregate cursor has too many dimension values",
            ));
        }
        if object_count <= 0 {
            return Err(StorageError::internal(
                "An object aggregate cursor object count must be positive",
            ));
        }
        Ok(Self {
            sort_key,
            object_count,
        })
    }

    #[must_use]
    pub const fn sort_key(&self) -> &Value {
        &self.sort_key
    }

    #[must_use]
    pub const fn object_count(&self) -> i64 {
        self.object_count
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
    personal_owner_id: Option<PrincipalId>,
    required_permissions: Vec<AuthorizationPermission>,
    visibility: StorageVisibility,
    page_limit: usize,
    cursor_max_encoded_bytes: usize,
}

pub struct ObjectAggregateStorageQueryBuilder {
    target: StorageObjectAggregateTarget,
    options: QueryOptions,
    spec: StorageObjectAggregateSpec,
    personal_owner_id: Option<PrincipalId>,
    required_permissions: Option<Vec<AuthorizationPermission>>,
    visibility: StorageVisibility,
    page_limit: Option<usize>,
    cursor_max_encoded_bytes: Option<usize>,
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
            page_limit: None,
            cursor_max_encoded_bytes: None,
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
    pub const fn personal_owner_id(&self) -> Option<PrincipalId> {
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
    pub const fn page_limit(&self) -> usize {
        self.page_limit
    }
}

impl ObjectAggregateStorageQueryBuilder {
    #[must_use]
    pub const fn personal_owner_id(mut self, personal_owner_id: Option<PrincipalId>) -> Self {
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
    pub const fn page_limit(mut self, page_limit: usize) -> Self {
        self.page_limit = Some(page_limit);
        self
    }

    pub fn build(self) -> Result<ObjectAggregateStorageQuery, StorageError> {
        let required_permissions = self.required_permissions.ok_or_else(|| {
            StorageError::internal("Object aggregate query is missing required permissions")
        })?;
        if required_permissions.is_empty() {
            return Err(StorageError::invalid_input(
                "Object aggregate query requires at least one permission",
            ));
        }
        let page_limit = self.page_limit.ok_or_else(|| {
            StorageError::internal("Object aggregate query is missing its page limit")
        })?;
        if page_limit == 0 {
            return Err(StorageError::invalid_input(
                "Object aggregate page limit must be positive",
            ));
        }
        let cursor_max_encoded_bytes = self.cursor_max_encoded_bytes.ok_or_else(|| {
            StorageError::internal("Object aggregate query is missing its cursor budget")
        })?;
        if cursor_max_encoded_bytes == 0 {
            return Err(StorageError::invalid_input(
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
            page_limit,
            cursor_max_encoded_bytes,
        })
    }
}

impl fmt::Debug for ObjectAggregateStorageQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ObjectAggregateStorageQuery")
            .field("target", &self.target)
            .field("spec", &self.spec)
            .field("filter_count", &self.options.filters().len())
            .field("limit", &self.options.limit())
            .field("has_cursor", &self.options.cursor().is_some())
            .field("include_total", &self.options.include_total())
            .field("has_personal_owner", &self.personal_owner_id.is_some())
            .field("permission_count", &self.required_permissions.len())
            .field("visibility", &self.visibility)
            .field("page_limit", &self.page_limit)
            .field("cursor_max_encoded_bytes", &self.cursor_max_encoded_bytes)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct StorageObjectAggregateAuthorizationTarget {
    class_id: ClassId,
    class_name: String,
    collection_id: CollectionId,
    collection_name: String,
}

impl StorageObjectAggregateAuthorizationTarget {
    #[must_use]
    pub const fn new(
        class_id: ClassId,
        class_name: String,
        collection_id: CollectionId,
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
    pub fn into_parts(self) -> (ClassId, String, CollectionId, String) {
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
    id: ObjectId,
    name: String,
    collection_id: CollectionId,
    class_id: ClassId,
}

impl StorageObjectAggregateAuthorizationCandidate {
    #[must_use]
    pub const fn new(
        id: ObjectId,
        name: String,
        collection_id: CollectionId,
        class_id: ClassId,
    ) -> Self {
        Self {
            id,
            name,
            collection_id,
            class_id,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (ObjectId, String, CollectionId, ClassId) {
        (self.id, self.name, self.collection_id, self.class_id)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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
    pub fn try_new(
        state: StorageObjectAggregateMeasureState,
        value_count: i64,
        skipped_count: i64,
        value: Option<Value>,
    ) -> Result<Self, StorageError> {
        if value_count < 0 || skipped_count < 0 {
            return Err(StorageError::internal(
                "Object aggregate measure counts must not be negative",
            ));
        }
        let value_matches_state = match state {
            StorageObjectAggregateMeasureState::Value => {
                value_count > 0 && value.as_ref().is_some_and(Value::is_number)
            }
            StorageObjectAggregateMeasureState::Empty => value_count == 0 && value.is_none(),
        };
        if !value_matches_state {
            return Err(StorageError::internal(
                "Object aggregate measure state contradicts its value and count",
            ));
        }
        Ok(Self {
            state,
            value_count,
            skipped_count,
            value,
        })
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
    pub fn try_new(
        measures: Vec<StorageObjectAggregateMeasureValue>,
        object_count: i64,
        sort_key: Value,
    ) -> Result<Self, StorageError> {
        if object_count <= 0 {
            return Err(StorageError::internal(
                "An object aggregate row object count must be positive",
            ));
        }
        let Some(dimension_values) = sort_key.as_array() else {
            return Err(StorageError::internal(
                "An object aggregate row sort key must be an array",
            ));
        };
        if dimension_values.len() > MAX_OBJECT_AGGREGATE_DIMENSIONS {
            return Err(StorageError::internal(
                "An object aggregate row has too many dimension values",
            ));
        }
        if measures.len() > MAX_OBJECT_AGGREGATE_MEASURES {
            return Err(StorageError::internal(
                "An object aggregate row has too many measures",
            ));
        }
        if measures.iter().any(|measure| {
            measure.value_count.checked_add(measure.skipped_count) != Some(object_count)
        }) {
            return Err(StorageError::internal(
                "Object aggregate measure counts must add up to the row object count",
            ));
        }
        Ok(Self {
            measures,
            object_count,
            sort_key,
        })
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
    pub fn try_new(
        rows: Vec<StorageObjectAggregateRow>,
        total: Option<i64>,
        next_cursor: Option<String>,
    ) -> Result<Self, StorageError> {
        validate_page_total(rows.len(), total)?;
        if next_cursor.as_ref().is_some_and(String::is_empty) {
            return Err(StorageError::internal(
                "An object aggregate page cursor must not be empty",
            ));
        }
        Ok(Self {
            rows,
            total,
            next_cursor,
        })
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
///
/// A backend may retain one native read snapshot and a scarce storage resource
/// while awaiting these callbacks so authorization, computed values,
/// aggregation, totals, and paging observe one consistent candidate set.
/// Implementations must therefore keep policy work bounded and cancellation
/// safe. Authorization latency extends the lifetime of that snapshot and its
/// native resource. A callback must not re-enter the same storage backend when
/// doing so could exhaust its resource pool or deadlock on the retained
/// snapshot or connection.
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
        authorization: ObjectAggregateAuthorization<'_>,
    ) -> Result<StorageObjectAggregatePage, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StorageErrorKind;
    use hubuum_query::{FilterField, ParsedQueryParam, SearchOperator};

    const CURSOR_BUDGET: usize = 1_024;

    fn aggregate_spec(
        dimension: &str,
        measures: impl IntoIterator<Item = &'static str>,
    ) -> StorageObjectAggregateSpec {
        StorageObjectAggregateSpec::new(
            [StorageObjectAggregateDimension::from_str(dimension).unwrap()],
            measures
                .into_iter()
                .map(|measure| StorageObjectAggregateMeasure::from_str(measure).unwrap()),
            StorageObjectAggregateSort::DimensionsAscending,
        )
        .unwrap()
    }

    fn aggregate_row(sort_key: Value) -> StorageObjectAggregateRow {
        StorageObjectAggregateRow::try_new(Vec::new(), 1, sort_key).unwrap()
    }

    fn encoded_cursor(dimension: &str, sort_key: Value, object_count: i64) -> String {
        let token = StorageObjectAggregateCursorToken {
            version: 1,
            dimensions: vec![dimension.to_string()],
            measures: Vec::new(),
            sort: StorageObjectAggregateSort::DimensionsAscending,
            sort_key,
            object_count,
        };
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(serde_json::to_vec(&token).unwrap())
    }

    fn empty_query_options() -> QueryOptions {
        QueryOptions::new(Vec::new(), Vec::new(), None, None, false).unwrap()
    }

    #[test]
    fn aggregate_cursor_is_bound_to_dimension_contract() {
        let first = aggregate_spec("name", []);
        let second = aggregate_spec("description", []);
        let cursor = first
            .encode_cursor(
                &aggregate_row(serde_json::json!([[0, "router"]])),
                CURSOR_BUDGET,
            )
            .unwrap();

        let error = second.decode_cursor(&cursor, CURSOR_BUDGET).unwrap_err();

        assert_eq!(error.kind(), StorageErrorKind::InvalidInput);
        assert!(error.to_string().contains("does not match"));
    }

    #[test]
    fn aggregate_cursor_is_bound_to_measure_contract() {
        let first = aggregate_spec("name", ["sum:json_data.cost"]);
        let second = aggregate_spec("name", ["max:json_data.cost"]);
        let cursor = first
            .encode_cursor(
                &aggregate_row(serde_json::json!([[0, "router"]])),
                CURSOR_BUDGET,
            )
            .unwrap();

        let error = second.decode_cursor(&cursor, CURSOR_BUDGET).unwrap_err();

        assert_eq!(error.kind(), StorageErrorKind::InvalidInput);
        assert!(error.to_string().contains("does not match"));
    }

    #[test]
    fn aggregate_cursor_refuses_an_unreplayable_boundary_value() {
        let spec = aggregate_spec("json_data.large", []);
        let row = aggregate_row(serde_json::json!([[0, "x".repeat(CURSOR_BUDGET)]]));

        let error = spec.encode_cursor(&row, CURSOR_BUDGET).unwrap_err();

        assert_eq!(error.kind(), StorageErrorKind::InputTooLarge);
        assert!(error.to_string().contains("replay-safe limit"));
    }

    #[test]
    fn aggregate_cursor_rejects_oversized_input_before_decoding() {
        let spec = aggregate_spec("name", []);

        let error = spec
            .decode_cursor(&"a".repeat(CURSOR_BUDGET + 1), CURSOR_BUDGET)
            .unwrap_err();

        assert_eq!(error.kind(), StorageErrorKind::InputTooLarge);
        assert!(error.to_string().contains("replay-safe limit"));
    }

    #[test]
    fn aggregate_cursor_rejects_invalid_ordering_shape() {
        let spec = aggregate_spec("name", []);
        let cursor = encoded_cursor("name", serde_json::json!([[0]]), 1);

        let error = spec.decode_cursor(&cursor, CURSOR_BUDGET).unwrap_err();

        assert_eq!(error.kind(), StorageErrorKind::InvalidInput);
        assert!(error.to_string().contains("invalid ordering values"));
    }

    #[test]
    fn aggregate_cursor_rejects_wrong_scalar_value_type() {
        let spec = aggregate_spec("collection_id", []);
        let cursor = encoded_cursor("collection_id", serde_json::json!([[0, "42"]]), 1);

        let error = spec.decode_cursor(&cursor, CURSOR_BUDGET).unwrap_err();

        assert_eq!(error.kind(), StorageErrorKind::InvalidInput);
        assert!(error.to_string().contains("invalid ordering values"));
    }

    #[test]
    fn aggregate_output_constructors_reject_contradictory_values() {
        assert!(StorageObjectAggregateCursor::try_new(serde_json::json!({}), 1).is_err());
        assert!(StorageObjectAggregateCursor::try_new(serde_json::json!([]), 0).is_err());
        assert!(
            StorageObjectAggregateMeasureValue::try_new(
                StorageObjectAggregateMeasureState::Value,
                0,
                1,
                Some(serde_json::json!(3)),
            )
            .is_err()
        );
        assert!(
            StorageObjectAggregateMeasureValue::try_new(
                StorageObjectAggregateMeasureState::Empty,
                0,
                1,
                Some(serde_json::json!(3)),
            )
            .is_err()
        );

        let measure = StorageObjectAggregateMeasureValue::try_new(
            StorageObjectAggregateMeasureState::Value,
            1,
            0,
            Some(serde_json::json!(3)),
        )
        .unwrap();
        assert!(
            StorageObjectAggregateRow::try_new(
                vec![measure],
                2,
                serde_json::json!([[0, "router"]]),
            )
            .is_err()
        );
        assert!(
            StorageObjectAggregatePage::try_new(
                vec![aggregate_row(serde_json::json!([[0, "router"]]))],
                Some(0),
                None,
            )
            .is_err()
        );
        assert!(
            StorageObjectAggregatePage::try_new(Vec::new(), Some(0), Some(String::new())).is_err()
        );
    }

    #[test]
    fn aggregate_cursor_accepts_typed_scalar_value() {
        let spec = aggregate_spec("created_at", []);
        let cursor = encoded_cursor(
            "created_at",
            serde_json::json!([[0, "2026-07-20T12:34:56.123456"]]),
            1,
        );

        let decoded = spec.decode_cursor(&cursor, CURSOR_BUDGET).unwrap();

        assert_eq!(decoded.object_count(), 1);
        assert_eq!(
            decoded.sort_key(),
            &serde_json::json!([[0, "2026-07-20T12:34:56.123456"]])
        );
    }

    #[test]
    fn aggregate_query_debug_redacts_target_filters_and_cursor() {
        let query = ObjectAggregateStorageQuery::builder(
            StorageObjectAggregateTarget::new(
                ClassId::new(7).unwrap(),
                "secret class".to_string(),
                CollectionId::new(9).unwrap(),
            ),
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
            StorageObjectAggregateSpec::new(
                [
                    StorageObjectAggregateDimension::from_str("json_data.secret_dimension")
                        .unwrap(),
                ],
                [
                    StorageObjectAggregateMeasure::from_str("sum:json_data.secret_measure")
                        .unwrap(),
                ],
                StorageObjectAggregateSort::DimensionsAscending,
            )
            .unwrap(),
            StorageVisibility::new(
                hubuum_domain::PrincipalId::new(73).unwrap(),
                false,
                None::<[AuthorizationPermission; 0]>,
                None,
            ),
        )
        .personal_owner_id(PrincipalId::new(42).ok())
        .required_permissions([AuthorizationPermission::ReadObject])
        .page_limit(20)
        .cursor_max_encoded_bytes(1_000)
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
            StorageObjectAggregateTarget::new(
                ClassId::new(7).unwrap(),
                "class".to_string(),
                CollectionId::new(9).unwrap(),
            ),
            empty_query_options(),
            StorageObjectAggregateSpec::new(
                [StorageObjectAggregateDimension::Scalar(
                    StorageObjectAggregateScalarField::Name,
                )],
                [],
                StorageObjectAggregateSort::DimensionsAscending,
            )
            .unwrap(),
            StorageVisibility::new(
                hubuum_domain::PrincipalId::new(73).unwrap(),
                false,
                None::<[AuthorizationPermission; 0]>,
                None,
            ),
        )
        .required_permissions([])
        .page_limit(20)
        .cursor_max_encoded_bytes(1_000)
        .build();

        assert_eq!(result.unwrap_err().kind(), StorageErrorKind::InvalidInput);
    }

    #[test]
    fn aggregate_query_requires_a_positive_cursor_budget() {
        let result = ObjectAggregateStorageQuery::builder(
            StorageObjectAggregateTarget::new(
                ClassId::new(7).unwrap(),
                "class".to_string(),
                CollectionId::new(9).unwrap(),
            ),
            empty_query_options(),
            StorageObjectAggregateSpec::new(
                [StorageObjectAggregateDimension::Scalar(
                    StorageObjectAggregateScalarField::Name,
                )],
                [],
                StorageObjectAggregateSort::DimensionsAscending,
            )
            .unwrap(),
            StorageVisibility::new(
                hubuum_domain::PrincipalId::new(73).unwrap(),
                false,
                None::<[AuthorizationPermission; 0]>,
                None,
            ),
        )
        .required_permissions([AuthorizationPermission::ReadObject])
        .page_limit(20)
        .cursor_max_encoded_bytes(0)
        .build();

        assert_eq!(result.unwrap_err().kind(), StorageErrorKind::InvalidInput);
    }
}
