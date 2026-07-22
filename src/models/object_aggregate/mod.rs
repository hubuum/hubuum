use std::collections::HashSet;
use std::str::FromStr;

use base64::Engine;
use hubuum_computed_fields::FieldKey;
use hubuum_query::JsonFieldPath;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::errors::ApiError;
use crate::models::search::{
    ComputedFieldScope, FilterField, QueryOptions, QueryParamsExt, SearchOperator,
    parse_query_parameter_with_computed_filters_and_passthrough,
};
use crate::models::{CollectionID, HubuumClass, HubuumClassID, Permissions, TokenScope, UserID};

pub const MAX_OBJECT_AGGREGATE_DIMENSIONS: usize = 3;
pub const MAX_OBJECT_AGGREGATE_MEASURES: usize = 4;
const COMMON_HTTP_LINE_LIMIT_BYTES: usize = 8 * 1024;
const NEXT_CURSOR_HEADER_PREFIX: &str = "X-Next-Cursor: ";
const HTTP_LINE_TERMINATOR: &str = "\r\n";
const HTTP_GET_REQUEST_LINE_PREFIX: &str = "GET ";
const HTTP_1_1_REQUEST_LINE_SUFFIX: &str = " HTTP/1.1\r\n";
const CURSOR_QUERY_PREFIX: &str = "cursor=";
/// Absolute cursor cap after reserving the response header framing from a
/// common 8 KiB HTTP line limit. Individual requests usually have a smaller
/// replay budget because their path and non-cursor query parameters also count.
pub const MAX_OBJECT_AGGREGATE_CURSOR_LENGTH: usize =
    COMMON_HTTP_LINE_LIMIT_BYTES - NEXT_CURSOR_HEADER_PREFIX.len() - HTTP_LINE_TERMINATOR.len();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectAggregateCursorBudget {
    max_encoded_bytes: usize,
}

impl ObjectAggregateCursorBudget {
    pub fn for_request_target(path: &str, query_string: &str) -> Result<Self, ApiError> {
        let base_query_length = query_string
            .split('&')
            .filter(|parameter| !is_cursor_query_parameter(parameter))
            .map(str::len)
            .reduce(|total, length| total.saturating_add(1).saturating_add(length))
            .unwrap_or_default();
        let query_separator_length = usize::from(base_query_length > 0);
        let replay_overhead = path
            .len()
            .saturating_add(HTTP_GET_REQUEST_LINE_PREFIX.len())
            .saturating_add(HTTP_1_1_REQUEST_LINE_SUFFIX.len())
            .saturating_add(1)
            .saturating_add(base_query_length)
            .saturating_add(query_separator_length)
            .saturating_add(CURSOR_QUERY_PREFIX.len());
        let max_encoded_bytes = COMMON_HTTP_LINE_LIMIT_BYTES
            .saturating_sub(replay_overhead)
            .min(MAX_OBJECT_AGGREGATE_CURSOR_LENGTH);
        if max_encoded_bytes == 0 {
            return Err(ApiError::PayloadTooLarge(
                "Object aggregate request target leaves no room for a replay-safe cursor; shorten the filters"
                    .to_string(),
            ));
        }
        Ok(Self { max_encoded_bytes })
    }

    pub(crate) const fn max_encoded_bytes(self) -> usize {
        self.max_encoded_bytes
    }
}

fn is_cursor_query_parameter(parameter: &str) -> bool {
    parameter
        .split_once('=')
        .is_some_and(|(key, _)| key == "cursor")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectAggregateScalarField {
    Name,
    Description,
    CollectionId,
    CreatedAt,
    UpdatedAt,
}

impl ObjectAggregateScalarField {
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

pub type ObjectAggregateJsonPath = JsonFieldPath;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComputedFieldSelector {
    scope: ComputedFieldScope,
    key: FieldKey,
}

impl ComputedFieldSelector {
    pub fn new(scope: ComputedFieldScope, key: &str) -> Result<Self, ApiError> {
        let key = FieldKey::new(key).map_err(|error| {
            ApiError::BadRequest(format!(
                "Invalid computed group selector key '{key}': {error}"
            ))
        })?;
        Ok(Self { scope, key })
    }

    pub const fn scope(&self) -> ComputedFieldScope {
        self.scope
    }

    pub fn key(&self) -> &str {
        self.key.as_str()
    }

    pub fn canonical(&self) -> String {
        format!("computed.{}.{}", self.scope.as_str(), self.key)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectAggregateDimension {
    Scalar(ObjectAggregateScalarField),
    JsonData(ObjectAggregateJsonPath),
    Computed(ComputedFieldSelector),
}

impl ObjectAggregateDimension {
    pub fn canonical(&self) -> String {
        match self {
            Self::Scalar(field) => field.as_str().to_string(),
            Self::JsonData(path) => format!("json_data.{}", path.canonical()),
            Self::Computed(selector) => selector.canonical(),
        }
    }

    pub const fn computed_selector(&self) -> Option<&ComputedFieldSelector> {
        match self {
            Self::Computed(selector) => Some(selector),
            Self::Scalar(_) | Self::JsonData(_) => None,
        }
    }
}

impl FromStr for ObjectAggregateDimension {
    type Err = ApiError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let scalar = match value {
            "name" => Some(ObjectAggregateScalarField::Name),
            "description" => Some(ObjectAggregateScalarField::Description),
            "collection_id" => Some(ObjectAggregateScalarField::CollectionId),
            "created_at" => Some(ObjectAggregateScalarField::CreatedAt),
            "updated_at" => Some(ObjectAggregateScalarField::UpdatedAt),
            _ => None,
        };
        if let Some(scalar) = scalar {
            return Ok(Self::Scalar(scalar));
        }
        if let Some(path) = value.strip_prefix("json_data.") {
            return Ok(Self::JsonData(ObjectAggregateJsonPath::new(path)?));
        }
        if let Some(key) = value.strip_prefix("computed.shared.") {
            return Ok(Self::Computed(ComputedFieldSelector::new(
                ComputedFieldScope::Shared,
                key,
            )?));
        }
        if let Some(key) = value.strip_prefix("computed.personal.") {
            return Ok(Self::Computed(ComputedFieldSelector::new(
                ComputedFieldScope::Personal,
                key,
            )?));
        }
        Err(ApiError::BadRequest(format!(
            "Invalid object aggregate dimension '{value}'; use an allowed object field, json_data path, or computed selector"
        )))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ObjectAggregateMeasureOperation {
    Sum,
    Average,
    Min,
    Max,
}

impl ObjectAggregateMeasureOperation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sum => "sum",
            Self::Average => "average",
            Self::Min => "min",
            Self::Max => "max",
        }
    }
}

impl FromStr for ObjectAggregateMeasureOperation {
    type Err = ApiError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "sum" => Ok(Self::Sum),
            "average" => Ok(Self::Average),
            "min" => Ok(Self::Min),
            "max" => Ok(Self::Max),
            _ => Err(ApiError::BadRequest(format!(
                "Invalid object aggregate operation '{value}'; use sum, average, min, or max"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectAggregateMeasureField {
    JsonData(ObjectAggregateJsonPath),
    Computed(ComputedFieldSelector),
}

impl ObjectAggregateMeasureField {
    pub fn canonical(&self) -> String {
        match self {
            Self::JsonData(path) => format!("json_data.{}", path.canonical()),
            Self::Computed(selector) => selector.canonical(),
        }
    }

    pub const fn computed_selector(&self) -> Option<&ComputedFieldSelector> {
        match self {
            Self::Computed(selector) => Some(selector),
            Self::JsonData(_) => None,
        }
    }
}

impl FromStr for ObjectAggregateMeasureField {
    type Err = ApiError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if let Some(path) = value.strip_prefix("json_data.") {
            return Ok(Self::JsonData(ObjectAggregateJsonPath::new(path)?));
        }
        if let Some(key) = value.strip_prefix("computed.shared.") {
            return Ok(Self::Computed(ComputedFieldSelector::new(
                ComputedFieldScope::Shared,
                key,
            )?));
        }
        if let Some(key) = value.strip_prefix("computed.personal.") {
            return Ok(Self::Computed(ComputedFieldSelector::new(
                ComputedFieldScope::Personal,
                key,
            )?));
        }
        Err(ApiError::BadRequest(format!(
            "Invalid object aggregate measure field '{value}'; use a json_data path or computed selector"
        )))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectAggregateMeasure {
    operation: ObjectAggregateMeasureOperation,
    field: ObjectAggregateMeasureField,
}

impl ObjectAggregateMeasure {
    pub const fn operation(&self) -> ObjectAggregateMeasureOperation {
        self.operation
    }

    pub const fn field(&self) -> &ObjectAggregateMeasureField {
        &self.field
    }

    pub const fn computed_selector(&self) -> Option<&ComputedFieldSelector> {
        self.field.computed_selector()
    }

    pub fn canonical(&self) -> String {
        format!("{}:{}", self.operation.as_str(), self.field.canonical())
    }
}

impl FromStr for ObjectAggregateMeasure {
    type Err = ApiError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (operation, field) = value.split_once(':').ok_or_else(|| {
            ApiError::BadRequest(format!(
                "Invalid object aggregate measure '{value}'; use operation:field"
            ))
        })?;
        Ok(Self {
            operation: ObjectAggregateMeasureOperation::from_str(operation)?,
            field: ObjectAggregateMeasureField::from_str(field)?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectAggregateSort {
    DimensionsAscending,
    DimensionsDescending,
    ObjectCountAscending,
    ObjectCountDescending,
}

impl ObjectAggregateSort {
    pub const fn orders_by_count(self) -> bool {
        matches!(
            self,
            Self::ObjectCountAscending | Self::ObjectCountDescending
        )
    }

    pub const fn descending(self) -> bool {
        matches!(
            self,
            Self::DimensionsDescending | Self::ObjectCountDescending
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectAggregateSpec {
    dimensions: Vec<ObjectAggregateDimension>,
    measures: Vec<ObjectAggregateMeasure>,
    sort: ObjectAggregateSort,
}

impl ObjectAggregateSpec {
    pub fn new(
        dimensions: Vec<ObjectAggregateDimension>,
        sort: ObjectAggregateSort,
    ) -> Result<Self, ApiError> {
        Self::with_measures(dimensions, Vec::new(), sort)
    }

    pub fn with_measures(
        dimensions: Vec<ObjectAggregateDimension>,
        measures: Vec<ObjectAggregateMeasure>,
        sort: ObjectAggregateSort,
    ) -> Result<Self, ApiError> {
        if dimensions.is_empty() && measures.is_empty() {
            return Err(ApiError::BadRequest(
                "Object aggregation requires at least one group_by dimension or aggregate measure"
                    .to_string(),
            ));
        }
        if dimensions.len() > MAX_OBJECT_AGGREGATE_DIMENSIONS {
            return Err(ApiError::BadRequest(format!(
                "Object aggregation supports at most {MAX_OBJECT_AGGREGATE_DIMENSIONS} group_by dimensions"
            )));
        }
        if measures.len() > MAX_OBJECT_AGGREGATE_MEASURES {
            return Err(ApiError::BadRequest(format!(
                "Object aggregation supports at most {MAX_OBJECT_AGGREGATE_MEASURES} aggregate measures"
            )));
        }
        let mut seen = HashSet::with_capacity(dimensions.len());
        if let Some(duplicate) = dimensions
            .iter()
            .map(ObjectAggregateDimension::canonical)
            .find(|field| !seen.insert(field.clone()))
        {
            return Err(ApiError::BadRequest(format!(
                "Duplicate object aggregate dimension '{duplicate}'"
            )));
        }
        let mut seen = HashSet::with_capacity(measures.len());
        if let Some(duplicate) = measures
            .iter()
            .map(ObjectAggregateMeasure::canonical)
            .find(|measure| !seen.insert(measure.clone()))
        {
            return Err(ApiError::BadRequest(format!(
                "Duplicate object aggregate measure '{duplicate}'"
            )));
        }
        Ok(Self {
            dimensions,
            measures,
            sort,
        })
    }

    pub fn dimensions(&self) -> &[ObjectAggregateDimension] {
        &self.dimensions
    }

    pub fn measures(&self) -> &[ObjectAggregateMeasure] {
        &self.measures
    }

    pub const fn sort(&self) -> ObjectAggregateSort {
        self.sort
    }

    pub fn has_computed_dimension(&self) -> bool {
        self.dimensions
            .iter()
            .any(|dimension| dimension.computed_selector().is_some())
    }

    pub fn has_computed_field(&self) -> bool {
        self.has_computed_dimension()
            || self
                .measures
                .iter()
                .any(|measure| measure.computed_selector().is_some())
    }

    pub(crate) fn requires_object_data(&self) -> bool {
        self.dimensions.iter().any(|dimension| {
            matches!(
                dimension,
                ObjectAggregateDimension::JsonData(_) | ObjectAggregateDimension::Computed(_)
            )
        }) || !self.measures.is_empty()
    }

    pub fn has_personal_computed_dimension(&self) -> bool {
        self.dimensions.iter().any(|dimension| {
            dimension
                .computed_selector()
                .is_some_and(|selector| selector.scope() == ComputedFieldScope::Personal)
        })
    }

    pub fn has_personal_computed_field(&self) -> bool {
        self.has_personal_computed_dimension()
            || self.measures.iter().any(|measure| {
                measure
                    .computed_selector()
                    .is_some_and(|selector| selector.scope() == ComputedFieldScope::Personal)
            })
    }

    pub(crate) fn computed_selectors(&self) -> impl Iterator<Item = &ComputedFieldSelector> {
        self.dimensions
            .iter()
            .filter_map(ObjectAggregateDimension::computed_selector)
            .chain(
                self.measures
                    .iter()
                    .filter_map(ObjectAggregateMeasure::computed_selector),
            )
    }

    fn dimension_names(&self) -> Vec<String> {
        self.dimensions
            .iter()
            .map(ObjectAggregateDimension::canonical)
            .collect()
    }

    fn measure_names(&self) -> Vec<String> {
        self.measures
            .iter()
            .map(ObjectAggregateMeasure::canonical)
            .collect()
    }

    pub(crate) fn decode_cursor(
        &self,
        cursor: &str,
        budget: ObjectAggregateCursorBudget,
    ) -> Result<DecodedObjectAggregateCursor, ApiError> {
        if cursor.len() > budget.max_encoded_bytes() {
            return Err(ApiError::PayloadTooLarge(format!(
                "aggregate cursor exceeds the replay-safe limit of {} bytes for this request",
                budget.max_encoded_bytes()
            )));
        }
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(cursor)
            .map_err(|error| ApiError::BadRequest(format!("invalid aggregate cursor: {error}")))?;
        let token: ObjectAggregateCursorToken = serde_json::from_slice(&bytes)
            .map_err(|error| ApiError::BadRequest(format!("invalid aggregate cursor: {error}")))?;
        if token.version != 1
            || token.dimensions != self.dimension_names()
            || token.measures != self.measure_names()
            || token.sort != self.sort
        {
            return Err(ApiError::BadRequest(
                "aggregate cursor does not match the current dimensions, measures, and sort"
                    .to_string(),
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
            return Err(ApiError::BadRequest(
                "aggregate cursor contains invalid ordering values".to_string(),
            ));
        }
        Ok(DecodedObjectAggregateCursor {
            sort_key: token.sort_key,
            object_count: token.object_count,
        })
    }

    pub(crate) fn encode_cursor(
        &self,
        row: &ObjectAggregateRow,
        budget: ObjectAggregateCursorBudget,
    ) -> Result<String, ApiError> {
        let token = ObjectAggregateCursorToken {
            version: 1,
            dimensions: self.dimension_names(),
            measures: self.measure_names(),
            sort: self.sort,
            sort_key: row.sort_key.clone(),
            object_count: row.object_count,
        };
        let bytes = serde_json::to_vec(&token).map_err(|error| {
            ApiError::InternalServerError(format!("failed to serialize aggregate cursor: {error}"))
        })?;
        let cursor = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
        if cursor.len() > budget.max_encoded_bytes() {
            return Err(ApiError::PayloadTooLarge(format!(
                "aggregate value at the page boundary produces a cursor larger than the replay-safe limit of {} bytes for this request; shorten the filters, narrow the grouping dimensions, or use a page limit that does not end on this value",
                budget.max_encoded_bytes()
            )));
        }
        Ok(cursor)
    }
}

fn valid_cursor_dimension_value(
    value: &serde_json::Value,
    dimension: &ObjectAggregateDimension,
) -> bool {
    let Some(pair) = value.as_array().filter(|pair| pair.len() == 2) else {
        return false;
    };
    let Some(state) = pair[0].as_u64() else {
        return false;
    };
    match state {
        0 => valid_cursor_present_value(&pair[1], dimension),
        1 => !matches!(dimension, ObjectAggregateDimension::Scalar(_)) && pair[1].is_null(),
        2 => matches!(dimension, ObjectAggregateDimension::JsonData(_)) && pair[1].is_null(),
        3 => matches!(dimension, ObjectAggregateDimension::Computed(_)) && pair[1].is_null(),
        _ => false,
    }
}

fn valid_cursor_present_value(
    value: &serde_json::Value,
    dimension: &ObjectAggregateDimension,
) -> bool {
    match dimension {
        ObjectAggregateDimension::Scalar(ObjectAggregateScalarField::Name)
        | ObjectAggregateDimension::Scalar(ObjectAggregateScalarField::Description) => {
            value.is_string()
        }
        ObjectAggregateDimension::Scalar(ObjectAggregateScalarField::CollectionId) => value
            .as_i64()
            .and_then(|value| i32::try_from(value).ok())
            .is_some_and(|value| value > 0),
        ObjectAggregateDimension::Scalar(
            ObjectAggregateScalarField::CreatedAt | ObjectAggregateScalarField::UpdatedAt,
        ) => value
            .as_str()
            .is_some_and(|value| value.parse::<chrono::NaiveDateTime>().is_ok()),
        ObjectAggregateDimension::JsonData(_) | ObjectAggregateDimension::Computed(_) => {
            !value.is_null()
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ObjectAggregateCursorToken {
    version: u8,
    dimensions: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    measures: Vec<String>,
    sort: ObjectAggregateSort,
    sort_key: serde_json::Value,
    object_count: i64,
}

#[derive(Debug)]
pub(crate) struct DecodedObjectAggregateCursor {
    pub sort_key: serde_json::Value,
    pub object_count: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ObjectAggregateValueState {
    Value,
    Null,
    Missing,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ObjectAggregateMeasureState {
    Value,
    Empty,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ObjectAggregateMeasureValue {
    field: String,
    operation: ObjectAggregateMeasureOperation,
    state: ObjectAggregateMeasureState,
    value_count: i64,
    skipped_count: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<Value>)]
    value: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct ObjectAggregateDatabaseMeasureValue {
    state: ObjectAggregateMeasureState,
    value_count: i64,
    skipped_count: i64,
    value: Option<serde_json::Value>,
}

impl ObjectAggregateMeasureValue {
    pub fn field(&self) -> &str {
        &self.field
    }

    pub const fn operation(&self) -> ObjectAggregateMeasureOperation {
        self.operation
    }

    pub const fn state(&self) -> ObjectAggregateMeasureState {
        self.state
    }

    pub const fn value_count(&self) -> i64 {
        self.value_count
    }

    pub const fn skipped_count(&self) -> i64 {
        self.skipped_count
    }

    pub fn value(&self) -> Option<&serde_json::Value> {
        self.value.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ObjectAggregateDimensionValue {
    field: String,
    state: ObjectAggregateValueState,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<Value>)]
    value: Option<serde_json::Value>,
}

impl ObjectAggregateDimensionValue {
    pub fn field(&self) -> &str {
        &self.field
    }

    pub const fn state(&self) -> ObjectAggregateValueState {
        self.state
    }

    pub fn value(&self) -> Option<&serde_json::Value> {
        self.value.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, ToSchema)]
pub struct ObjectAggregateRow {
    dimensions: Vec<ObjectAggregateDimensionValue>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    measures: Vec<ObjectAggregateMeasureValue>,
    object_count: i64,
    #[serde(skip)]
    #[schema(ignore)]
    sort_key: serde_json::Value,
}

impl ObjectAggregateRow {
    pub(crate) fn from_database(
        spec: &ObjectAggregateSpec,
        measures: serde_json::Value,
        object_count: i64,
        sort_key: serde_json::Value,
    ) -> Result<Self, ApiError> {
        let dimensions = dimensions_from_sort_key(spec, &sort_key)?;
        let database_measures = serde_json::from_value::<Vec<ObjectAggregateDatabaseMeasureValue>>(
            measures,
        )
        .map_err(|error| {
            ApiError::InternalServerError(format!(
                "Database returned invalid object aggregate measures: {error}"
            ))
        })?;
        if database_measures.len() != spec.measures().len() {
            return Err(ApiError::InternalServerError(
                "Database returned an object aggregate row with the wrong measure count"
                    .to_string(),
            ));
        }
        let measures = spec
            .measures()
            .iter()
            .zip(database_measures)
            .map(|(measure, value)| ObjectAggregateMeasureValue {
                field: measure.field().canonical(),
                operation: measure.operation(),
                state: value.state,
                value_count: value.value_count,
                skipped_count: value.skipped_count,
                value: value.value,
            })
            .collect::<Vec<_>>();
        if object_count <= 0 || !sort_key.is_array() {
            return Err(ApiError::InternalServerError(
                "Database returned invalid object aggregate ordering data".to_string(),
            ));
        }
        if measures.iter().any(|measure| {
            measure.value_count < 0
                || measure.skipped_count < 0
                || measure.value_count + measure.skipped_count != object_count
                || match measure.state {
                    ObjectAggregateMeasureState::Value => {
                        measure.value_count == 0
                            || !measure
                                .value
                                .as_ref()
                                .is_some_and(serde_json::Value::is_number)
                    }
                    ObjectAggregateMeasureState::Empty => {
                        measure.value_count != 0 || measure.value.is_some()
                    }
                }
        }) {
            return Err(ApiError::InternalServerError(
                "Database returned invalid object aggregate measure data".to_string(),
            ));
        }
        Ok(Self {
            dimensions,
            measures,
            object_count,
            sort_key,
        })
    }

    pub fn dimensions(&self) -> &[ObjectAggregateDimensionValue] {
        &self.dimensions
    }

    pub fn measures(&self) -> &[ObjectAggregateMeasureValue] {
        &self.measures
    }

    pub const fn object_count(&self) -> i64 {
        self.object_count
    }
}

fn dimensions_from_sort_key(
    spec: &ObjectAggregateSpec,
    sort_key: &serde_json::Value,
) -> Result<Vec<ObjectAggregateDimensionValue>, ApiError> {
    let values = sort_key.as_array().ok_or_else(|| {
        ApiError::InternalServerError(
            "Database returned a non-array object aggregate sort key".to_string(),
        )
    })?;
    if values.len() != spec.dimensions().len() {
        return Err(ApiError::InternalServerError(
            "Database returned an object aggregate sort key with the wrong dimension count"
                .to_string(),
        ));
    }
    spec.dimensions()
        .iter()
        .zip(values)
        .map(|(dimension, item)| dimension_from_sort_item(dimension, item))
        .collect()
}

fn dimension_from_sort_item(
    dimension: &ObjectAggregateDimension,
    item: &serde_json::Value,
) -> Result<ObjectAggregateDimensionValue, ApiError> {
    let pair = item
        .as_array()
        .filter(|pair| pair.len() == 2)
        .ok_or_else(|| {
            ApiError::InternalServerError(
                "Database returned an invalid object aggregate sort key".to_string(),
            )
        })?;
    let state = pair[0].as_i64().ok_or_else(|| {
        ApiError::InternalServerError(
            "Database returned an invalid object aggregate value state".to_string(),
        )
    })?;
    let (state, value) = match state {
        0 => (ObjectAggregateValueState::Value, Some(pair[1].clone())),
        1 => (ObjectAggregateValueState::Null, None),
        2 => (ObjectAggregateValueState::Missing, None),
        3 => (ObjectAggregateValueState::Unavailable, None),
        _ => {
            return Err(ApiError::InternalServerError(
                "Database returned an unknown object aggregate value state".to_string(),
            ));
        }
    };
    Ok(ObjectAggregateDimensionValue {
        field: dimension.canonical(),
        state,
        value,
    })
}

#[derive(Debug)]
pub struct ObjectAggregateQuery {
    query_options: QueryOptions,
    spec: ObjectAggregateSpec,
}

#[derive(Debug, Clone)]
pub struct ObjectAggregateTarget {
    class_id: HubuumClassID,
    class_name: String,
    collection_id: CollectionID,
}

impl ObjectAggregateTarget {
    pub fn from_class(class: &HubuumClass) -> Result<Self, ApiError> {
        Ok(Self {
            class_id: HubuumClassID::new(class.id)?,
            class_name: class.name.clone(),
            collection_id: CollectionID::new(class.collection_id)?,
        })
    }

    pub(crate) fn into_parts(self) -> (HubuumClassID, String, CollectionID) {
        (self.class_id, self.class_name, self.collection_id)
    }
}

pub struct ObjectAggregatePage {
    rows: Vec<ObjectAggregateRow>,
    total_count: i64,
    next_cursor: Option<String>,
}

impl ObjectAggregatePage {
    pub(crate) fn new(
        rows: Vec<ObjectAggregateRow>,
        total_count: i64,
        next_cursor: Option<String>,
    ) -> Self {
        Self {
            rows,
            total_count,
            next_cursor,
        }
    }

    pub fn into_parts(self) -> (Vec<ObjectAggregateRow>, i64, Option<String>) {
        (self.rows, self.total_count, self.next_cursor)
    }
}

impl ObjectAggregateQuery {
    pub fn into_parts(self) -> (QueryOptions, ObjectAggregateSpec) {
        (self.query_options, self.spec)
    }

    pub fn query_options(&self) -> &QueryOptions {
        &self.query_options
    }

    pub const fn spec(&self) -> &ObjectAggregateSpec {
        &self.spec
    }

    pub fn has_computed_filter(&self) -> bool {
        self.query_options
            .filters
            .iter()
            .any(|filter| filter.field.computed_query().is_some())
    }

    pub fn has_personal_computed_filter(&self) -> bool {
        self.query_options.filters.iter().any(|filter| {
            filter
                .field
                .computed_query()
                .is_some_and(|field| field.scope() == ComputedFieldScope::Personal)
        })
    }

    pub fn uses_computed_values(&self) -> bool {
        self.spec.has_computed_field() || self.has_computed_filter()
    }

    pub fn requires_personal_owner(&self) -> bool {
        self.spec.has_personal_computed_field() || self.has_personal_computed_filter()
    }
}

pub struct ObjectAggregateBackendRequest {
    target: ObjectAggregateTarget,
    query_options: QueryOptions,
    spec: ObjectAggregateSpec,
    personal_owner_id: Option<UserID>,
    authorization: ObjectAggregateAuthorization,
    cursor_budget: ObjectAggregateCursorBudget,
}

pub struct ObjectAggregateBackendRequestBuilder {
    target: ObjectAggregateTarget,
    query: ObjectAggregateQuery,
    personal_owner_id: Option<UserID>,
    authorization: Option<ObjectAggregateAuthorization>,
    cursor_budget: Option<ObjectAggregateCursorBudget>,
}

pub(crate) struct ObjectAggregateBackendParts {
    pub target: ObjectAggregateTarget,
    pub query_options: QueryOptions,
    pub spec: ObjectAggregateSpec,
    pub personal_owner_id: Option<UserID>,
    pub authorization: ObjectAggregateAuthorization,
    pub cursor_budget: ObjectAggregateCursorBudget,
}

pub struct ObjectAggregateAuthorization {
    required_permissions: Vec<Permissions>,
    token_scopes: Option<TokenScope>,
}

impl ObjectAggregateAuthorization {
    pub fn new(
        required_permissions: Vec<Permissions>,
        token_scopes: Option<TokenScope>,
    ) -> Result<Self, ApiError> {
        if !required_permissions.contains(&Permissions::ReadObject)
            || !required_permissions.contains(&Permissions::ReadCollection)
        {
            return Err(ApiError::BadRequest(
                "Object aggregation authorization must require ReadObject and ReadCollection"
                    .to_string(),
            ));
        }
        Ok(Self {
            required_permissions,
            token_scopes,
        })
    }

    pub(crate) fn into_parts(self) -> (Vec<Permissions>, Option<TokenScope>) {
        (self.required_permissions, self.token_scopes)
    }
}

impl ObjectAggregateBackendRequest {
    pub fn builder(
        target: ObjectAggregateTarget,
        query: ObjectAggregateQuery,
    ) -> ObjectAggregateBackendRequestBuilder {
        ObjectAggregateBackendRequestBuilder {
            target,
            query,
            personal_owner_id: None,
            authorization: None,
            cursor_budget: None,
        }
    }

    pub(crate) fn into_parts(self) -> ObjectAggregateBackendParts {
        ObjectAggregateBackendParts {
            target: self.target,
            query_options: self.query_options,
            spec: self.spec,
            personal_owner_id: self.personal_owner_id,
            authorization: self.authorization,
            cursor_budget: self.cursor_budget,
        }
    }
}

impl ObjectAggregateBackendRequestBuilder {
    pub fn personal_owner(mut self, owner_id: UserID) -> Self {
        self.personal_owner_id = Some(owner_id);
        self
    }

    pub fn authorization(mut self, authorization: ObjectAggregateAuthorization) -> Self {
        self.authorization = Some(authorization);
        self
    }

    pub fn cursor_budget(mut self, cursor_budget: ObjectAggregateCursorBudget) -> Self {
        self.cursor_budget = Some(cursor_budget);
        self
    }

    pub fn build(mut self) -> Result<ObjectAggregateBackendRequest, ApiError> {
        let authorization = self.authorization.ok_or_else(|| {
            ApiError::InternalServerError(
                "Object aggregate backend request is missing authorization".to_string(),
            )
        })?;
        let cursor_budget = self.cursor_budget.ok_or_else(|| {
            ApiError::InternalServerError(
                "Object aggregate backend request is missing a cursor transport budget".to_string(),
            )
        })?;
        self.query.query_options.filters.add_filter(
            FilterField::ClassId,
            SearchOperator::Equals { is_negated: false },
            &self.target.class_id.id().to_string(),
        );
        self.query.query_options.filters.add_filter(
            FilterField::CollectionId,
            SearchOperator::Equals { is_negated: false },
            &self.target.collection_id.id().to_string(),
        );
        let (query_options, spec) = self.query.into_parts();
        let requires_personal_owner = spec.has_personal_computed_field()
            || query_options.filters.iter().any(|filter| {
                filter
                    .field
                    .computed_query()
                    .is_some_and(|field| field.scope() == ComputedFieldScope::Personal)
            });
        if requires_personal_owner != self.personal_owner_id.is_some() {
            return Err(ApiError::InternalServerError(
                "Personal computed aggregation requires exactly one typed owner".to_string(),
            ));
        }
        Ok(ObjectAggregateBackendRequest {
            target: self.target,
            query_options,
            spec,
            personal_owner_id: self.personal_owner_id,
            authorization,
            cursor_budget,
        })
    }
}

pub fn parse_object_aggregate_query(query_string: &str) -> Result<ObjectAggregateQuery, ApiError> {
    let (query_options, mut passthrough) =
        parse_query_parameter_with_computed_filters_and_passthrough(
            query_string,
            &["group_by", "aggregate", "sort"],
        )?;
    if !query_options.sort.is_empty() {
        return Err(ApiError::BadRequest(
            "Object-list sort fields are not valid for grouped results; use sort=dimensions.asc|desc or sort=object_count.asc|desc"
                .to_string(),
        ));
    }

    let dimensions = passthrough
        .remove("group_by")
        .unwrap_or_default()
        .into_iter()
        .map(|value| ObjectAggregateDimension::from_str(&value))
        .collect::<Result<Vec<_>, _>>()?;
    let measures = passthrough
        .remove("aggregate")
        .unwrap_or_default()
        .into_iter()
        .map(|value| ObjectAggregateMeasure::from_str(&value))
        .collect::<Result<Vec<_>, _>>()?;

    let sort_values = passthrough.remove("sort").unwrap_or_default();
    if sort_values.len() > 1 {
        return Err(ApiError::BadRequest("duplicate aggregate sort".to_string()));
    }
    let sort = match sort_values.first().map(String::as_str) {
        None | Some("dimensions") | Some("dimensions.asc") => {
            ObjectAggregateSort::DimensionsAscending
        }
        Some("dimensions.desc") => ObjectAggregateSort::DimensionsDescending,
        Some("object_count") | Some("object_count.asc") => {
            ObjectAggregateSort::ObjectCountAscending
        }
        Some("object_count.desc") => ObjectAggregateSort::ObjectCountDescending,
        Some(value) => {
            return Err(ApiError::BadRequest(format!(
                "Invalid object aggregate sort '{value}'; use dimensions.asc|desc or object_count.asc|desc"
            )));
        }
    };

    Ok(ObjectAggregateQuery {
        query_options,
        spec: ObjectAggregateSpec::with_measures(dimensions, measures, sort)?,
    })
}

#[cfg(test)]
mod tests;
