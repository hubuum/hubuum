use std::collections::HashSet;
use std::str::FromStr;

use base64::Engine;
use hubuum_computed_fields::FieldKey;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::errors::ApiError;
use crate::models::search::{
    ComputedFieldScope, FilterField, QueryOptions, QueryParamsExt, SearchOperator,
    parse_query_parameter_with_passthrough,
};
use crate::models::{CollectionID, HubuumClass, HubuumClassID, Permissions, UserID};

pub const MIN_OBJECT_GROUP_DIMENSIONS: usize = 1;
pub const MAX_OBJECT_GROUP_DIMENSIONS: usize = 3;
const COMMON_HTTP_LINE_LIMIT_BYTES: usize = 8 * 1024;
const NEXT_CURSOR_HEADER_PREFIX: &str = "X-Next-Cursor: ";
const HTTP_LINE_TERMINATOR: &str = "\r\n";
const HTTP_GET_REQUEST_LINE_PREFIX: &str = "GET ";
const HTTP_1_1_REQUEST_LINE_SUFFIX: &str = " HTTP/1.1\r\n";
const CURSOR_QUERY_PREFIX: &str = "cursor=";
/// Absolute cursor cap after reserving the response header framing from a
/// common 8 KiB HTTP line limit. Individual requests usually have a smaller
/// replay budget because their path and non-cursor query parameters also count.
pub const MAX_OBJECT_GROUP_CURSOR_LENGTH: usize =
    COMMON_HTTP_LINE_LIMIT_BYTES - NEXT_CURSOR_HEADER_PREFIX.len() - HTTP_LINE_TERMINATOR.len();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectGroupCursorBudget {
    max_encoded_bytes: usize,
}

impl ObjectGroupCursorBudget {
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
            .min(MAX_OBJECT_GROUP_CURSOR_LENGTH);
        if max_encoded_bytes == 0 {
            return Err(ApiError::PayloadTooLarge(
                "Object group request target leaves no room for a replay-safe cursor; shorten the filters"
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
pub enum ObjectGroupScalarField {
    Name,
    Description,
    CollectionId,
    CreatedAt,
    UpdatedAt,
}

impl ObjectGroupScalarField {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectGroupJsonPath {
    segments: Vec<String>,
}

impl ObjectGroupJsonPath {
    pub fn new(value: &str) -> Result<Self, ApiError> {
        let segments = value.split(',').map(str::to_string).collect::<Vec<_>>();
        let valid = !value.is_empty()
            && segments.iter().all(|segment| {
                !segment.is_empty()
                    && segment
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'$'))
            });
        if !valid {
            return Err(ApiError::BadRequest(format!(
                "Invalid object group JSON path '{value}'; use non-empty comma-separated ASCII path segments"
            )));
        }
        Ok(Self { segments })
    }

    pub fn segments(&self) -> &[String] {
        &self.segments
    }

    pub fn canonical(&self) -> String {
        self.segments.join(",")
    }
}

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
pub enum ObjectGroupDimension {
    Scalar(ObjectGroupScalarField),
    JsonData(ObjectGroupJsonPath),
    Computed(ComputedFieldSelector),
}

impl ObjectGroupDimension {
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

impl FromStr for ObjectGroupDimension {
    type Err = ApiError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let scalar = match value {
            "name" => Some(ObjectGroupScalarField::Name),
            "description" => Some(ObjectGroupScalarField::Description),
            "collection_id" => Some(ObjectGroupScalarField::CollectionId),
            "created_at" => Some(ObjectGroupScalarField::CreatedAt),
            "updated_at" => Some(ObjectGroupScalarField::UpdatedAt),
            _ => None,
        };
        if let Some(scalar) = scalar {
            return Ok(Self::Scalar(scalar));
        }
        if let Some(path) = value.strip_prefix("json_data.") {
            return Ok(Self::JsonData(ObjectGroupJsonPath::new(path)?));
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
            "Invalid object group dimension '{value}'; use an allowed object field, json_data path, or computed selector"
        )))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectGroupSort {
    DimensionsAscending,
    DimensionsDescending,
    ObjectCountAscending,
    ObjectCountDescending,
}

impl ObjectGroupSort {
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
pub struct ObjectGroupSpec {
    dimensions: Vec<ObjectGroupDimension>,
    sort: ObjectGroupSort,
}

impl ObjectGroupSpec {
    pub fn new(
        dimensions: Vec<ObjectGroupDimension>,
        sort: ObjectGroupSort,
    ) -> Result<Self, ApiError> {
        if !(MIN_OBJECT_GROUP_DIMENSIONS..=MAX_OBJECT_GROUP_DIMENSIONS).contains(&dimensions.len())
        {
            return Err(ApiError::BadRequest(format!(
                "Object grouping requires between {MIN_OBJECT_GROUP_DIMENSIONS} and {MAX_OBJECT_GROUP_DIMENSIONS} group_by dimensions"
            )));
        }
        let mut seen = HashSet::with_capacity(dimensions.len());
        if let Some(duplicate) = dimensions
            .iter()
            .map(ObjectGroupDimension::canonical)
            .find(|field| !seen.insert(field.clone()))
        {
            return Err(ApiError::BadRequest(format!(
                "Duplicate object group dimension '{duplicate}'"
            )));
        }
        Ok(Self { dimensions, sort })
    }

    pub fn dimensions(&self) -> &[ObjectGroupDimension] {
        &self.dimensions
    }

    pub const fn sort(&self) -> ObjectGroupSort {
        self.sort
    }

    pub fn has_computed_dimension(&self) -> bool {
        self.dimensions
            .iter()
            .any(|dimension| dimension.computed_selector().is_some())
    }

    pub(crate) fn requires_object_data(&self) -> bool {
        self.dimensions.iter().any(|dimension| {
            matches!(
                dimension,
                ObjectGroupDimension::JsonData(_) | ObjectGroupDimension::Computed(_)
            )
        })
    }

    pub fn has_personal_computed_dimension(&self) -> bool {
        self.dimensions.iter().any(|dimension| {
            dimension
                .computed_selector()
                .is_some_and(|selector| selector.scope() == ComputedFieldScope::Personal)
        })
    }

    fn dimension_names(&self) -> Vec<String> {
        self.dimensions
            .iter()
            .map(ObjectGroupDimension::canonical)
            .collect()
    }

    pub(crate) fn decode_cursor(
        &self,
        cursor: &str,
        budget: ObjectGroupCursorBudget,
    ) -> Result<DecodedObjectGroupCursor, ApiError> {
        if cursor.len() > budget.max_encoded_bytes() {
            return Err(ApiError::PayloadTooLarge(format!(
                "group cursor exceeds the replay-safe limit of {} bytes for this request",
                budget.max_encoded_bytes()
            )));
        }
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(cursor)
            .map_err(|error| ApiError::BadRequest(format!("invalid group cursor: {error}")))?;
        let token: ObjectGroupCursorToken = serde_json::from_slice(&bytes)
            .map_err(|error| ApiError::BadRequest(format!("invalid group cursor: {error}")))?;
        if token.version != 1
            || token.dimensions != self.dimension_names()
            || token.sort != self.sort
        {
            return Err(ApiError::BadRequest(
                "group cursor does not match the current dimensions and sort".to_string(),
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
                "group cursor contains invalid ordering values".to_string(),
            ));
        }
        Ok(DecodedObjectGroupCursor {
            sort_key: token.sort_key,
            object_count: token.object_count,
        })
    }

    pub(crate) fn encode_cursor(
        &self,
        row: &ObjectGroupRow,
        budget: ObjectGroupCursorBudget,
    ) -> Result<String, ApiError> {
        let token = ObjectGroupCursorToken {
            version: 1,
            dimensions: self.dimension_names(),
            sort: self.sort,
            sort_key: row.sort_key.clone(),
            object_count: row.object_count,
        };
        let bytes = serde_json::to_vec(&token).map_err(|error| {
            ApiError::InternalServerError(format!("failed to serialize group cursor: {error}"))
        })?;
        let cursor = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
        if cursor.len() > budget.max_encoded_bytes() {
            return Err(ApiError::PayloadTooLarge(format!(
                "group value at the page boundary produces a cursor larger than the replay-safe limit of {} bytes for this request; shorten the filters, narrow the grouping dimensions, or use a page limit that does not end on this value",
                budget.max_encoded_bytes()
            )));
        }
        Ok(cursor)
    }
}

fn valid_cursor_dimension_value(
    value: &serde_json::Value,
    dimension: &ObjectGroupDimension,
) -> bool {
    let Some(pair) = value.as_array().filter(|pair| pair.len() == 2) else {
        return false;
    };
    let Some(state) = pair[0].as_u64() else {
        return false;
    };
    match state {
        0 => valid_cursor_present_value(&pair[1], dimension),
        1 => !matches!(dimension, ObjectGroupDimension::Scalar(_)) && pair[1].is_null(),
        2 => matches!(dimension, ObjectGroupDimension::JsonData(_)) && pair[1].is_null(),
        3 => matches!(dimension, ObjectGroupDimension::Computed(_)) && pair[1].is_null(),
        _ => false,
    }
}

fn valid_cursor_present_value(value: &serde_json::Value, dimension: &ObjectGroupDimension) -> bool {
    match dimension {
        ObjectGroupDimension::Scalar(ObjectGroupScalarField::Name)
        | ObjectGroupDimension::Scalar(ObjectGroupScalarField::Description) => value.is_string(),
        ObjectGroupDimension::Scalar(ObjectGroupScalarField::CollectionId) => value
            .as_i64()
            .and_then(|value| i32::try_from(value).ok())
            .is_some_and(|value| value > 0),
        ObjectGroupDimension::Scalar(
            ObjectGroupScalarField::CreatedAt | ObjectGroupScalarField::UpdatedAt,
        ) => value
            .as_str()
            .is_some_and(|value| value.parse::<chrono::NaiveDateTime>().is_ok()),
        ObjectGroupDimension::JsonData(_) | ObjectGroupDimension::Computed(_) => !value.is_null(),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ObjectGroupCursorToken {
    version: u8,
    dimensions: Vec<String>,
    sort: ObjectGroupSort,
    sort_key: serde_json::Value,
    object_count: i64,
}

#[derive(Debug)]
pub(crate) struct DecodedObjectGroupCursor {
    pub sort_key: serde_json::Value,
    pub object_count: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ObjectGroupValueState {
    Value,
    Null,
    Missing,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ObjectGroupDimensionValue {
    field: String,
    state: ObjectGroupValueState,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<Value>)]
    value: Option<serde_json::Value>,
}

impl ObjectGroupDimensionValue {
    pub fn field(&self) -> &str {
        &self.field
    }

    pub const fn state(&self) -> ObjectGroupValueState {
        self.state
    }

    pub fn value(&self) -> Option<&serde_json::Value> {
        self.value.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, ToSchema)]
pub struct ObjectGroupRow {
    dimensions: Vec<ObjectGroupDimensionValue>,
    object_count: i64,
    #[serde(skip)]
    #[schema(ignore)]
    sort_key: serde_json::Value,
}

impl ObjectGroupRow {
    pub(crate) fn from_database(
        dimensions: serde_json::Value,
        object_count: i64,
        sort_key: serde_json::Value,
    ) -> Result<Self, ApiError> {
        let dimensions = serde_json::from_value::<Vec<ObjectGroupDimensionValue>>(dimensions)
            .map_err(|error| {
                ApiError::InternalServerError(format!(
                    "Database returned invalid object group dimensions: {error}"
                ))
            })?;
        if object_count < 0 || !sort_key.is_array() {
            return Err(ApiError::InternalServerError(
                "Database returned invalid object group ordering data".to_string(),
            ));
        }
        Ok(Self {
            dimensions,
            object_count,
            sort_key,
        })
    }

    pub fn dimensions(&self) -> &[ObjectGroupDimensionValue] {
        &self.dimensions
    }

    pub const fn object_count(&self) -> i64 {
        self.object_count
    }
}

#[derive(Debug)]
pub struct ObjectGroupQuery {
    query_options: QueryOptions,
    spec: ObjectGroupSpec,
}

#[derive(Debug, Clone)]
pub struct ObjectGroupTarget {
    class_id: HubuumClassID,
    class_name: String,
    collection_id: CollectionID,
}

impl ObjectGroupTarget {
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

pub struct ObjectGroupPage {
    rows: Vec<ObjectGroupRow>,
    total_count: i64,
    next_cursor: Option<String>,
}

impl ObjectGroupPage {
    pub(crate) fn new(
        rows: Vec<ObjectGroupRow>,
        total_count: i64,
        next_cursor: Option<String>,
    ) -> Self {
        Self {
            rows,
            total_count,
            next_cursor,
        }
    }

    pub fn into_parts(self) -> (Vec<ObjectGroupRow>, i64, Option<String>) {
        (self.rows, self.total_count, self.next_cursor)
    }
}

impl ObjectGroupQuery {
    pub fn into_parts(self) -> (QueryOptions, ObjectGroupSpec) {
        (self.query_options, self.spec)
    }

    pub fn query_options(&self) -> &QueryOptions {
        &self.query_options
    }

    pub const fn spec(&self) -> &ObjectGroupSpec {
        &self.spec
    }
}

pub struct ObjectGroupBackendRequest {
    target: ObjectGroupTarget,
    query_options: QueryOptions,
    spec: ObjectGroupSpec,
    personal_owner_id: Option<UserID>,
    authorization: ObjectGroupAuthorization,
    cursor_budget: ObjectGroupCursorBudget,
}

pub struct ObjectGroupBackendRequestBuilder {
    target: ObjectGroupTarget,
    query: ObjectGroupQuery,
    personal_owner_id: Option<UserID>,
    authorization: Option<ObjectGroupAuthorization>,
    cursor_budget: Option<ObjectGroupCursorBudget>,
}

pub(crate) struct ObjectGroupBackendParts {
    pub target: ObjectGroupTarget,
    pub query_options: QueryOptions,
    pub spec: ObjectGroupSpec,
    pub personal_owner_id: Option<UserID>,
    pub authorization: ObjectGroupAuthorization,
    pub cursor_budget: ObjectGroupCursorBudget,
}

pub struct ObjectGroupAuthorization {
    required_permissions: Vec<Permissions>,
    token_scopes: Option<Vec<Permissions>>,
}

impl ObjectGroupAuthorization {
    pub fn new(
        required_permissions: Vec<Permissions>,
        token_scopes: Option<Vec<Permissions>>,
    ) -> Result<Self, ApiError> {
        if !required_permissions.contains(&Permissions::ReadObject) {
            return Err(ApiError::BadRequest(
                "Object grouping authorization must require ReadObject".to_string(),
            ));
        }
        Ok(Self {
            required_permissions,
            token_scopes,
        })
    }

    pub(crate) fn into_parts(self) -> (Vec<Permissions>, Option<Vec<Permissions>>) {
        (self.required_permissions, self.token_scopes)
    }
}

impl ObjectGroupBackendRequest {
    pub fn builder(
        target: ObjectGroupTarget,
        query: ObjectGroupQuery,
    ) -> ObjectGroupBackendRequestBuilder {
        ObjectGroupBackendRequestBuilder {
            target,
            query,
            personal_owner_id: None,
            authorization: None,
            cursor_budget: None,
        }
    }

    pub(crate) fn into_parts(self) -> ObjectGroupBackendParts {
        ObjectGroupBackendParts {
            target: self.target,
            query_options: self.query_options,
            spec: self.spec,
            personal_owner_id: self.personal_owner_id,
            authorization: self.authorization,
            cursor_budget: self.cursor_budget,
        }
    }
}

impl ObjectGroupBackendRequestBuilder {
    pub fn personal_owner(mut self, owner_id: UserID) -> Self {
        self.personal_owner_id = Some(owner_id);
        self
    }

    pub fn authorization(mut self, authorization: ObjectGroupAuthorization) -> Self {
        self.authorization = Some(authorization);
        self
    }

    pub fn cursor_budget(mut self, cursor_budget: ObjectGroupCursorBudget) -> Self {
        self.cursor_budget = Some(cursor_budget);
        self
    }

    pub fn build(mut self) -> Result<ObjectGroupBackendRequest, ApiError> {
        let authorization = self.authorization.ok_or_else(|| {
            ApiError::InternalServerError(
                "Object group backend request is missing authorization".to_string(),
            )
        })?;
        let cursor_budget = self.cursor_budget.ok_or_else(|| {
            ApiError::InternalServerError(
                "Object group backend request is missing a cursor transport budget".to_string(),
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
        if spec.has_personal_computed_dimension() != self.personal_owner_id.is_some() {
            return Err(ApiError::InternalServerError(
                "Personal computed grouping requires exactly one typed owner".to_string(),
            ));
        }
        Ok(ObjectGroupBackendRequest {
            target: self.target,
            query_options,
            spec,
            personal_owner_id: self.personal_owner_id,
            authorization,
            cursor_budget,
        })
    }
}

pub fn parse_object_group_query(query_string: &str) -> Result<ObjectGroupQuery, ApiError> {
    let (query_options, mut passthrough) =
        parse_query_parameter_with_passthrough(query_string, &["group_by", "sort"])?;
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
        .map(|value| ObjectGroupDimension::from_str(&value))
        .collect::<Result<Vec<_>, _>>()?;

    let sort_values = passthrough.remove("sort").unwrap_or_default();
    if sort_values.len() > 1 {
        return Err(ApiError::BadRequest("duplicate group sort".to_string()));
    }
    let sort = match sort_values.first().map(String::as_str) {
        None | Some("dimensions") | Some("dimensions.asc") => ObjectGroupSort::DimensionsAscending,
        Some("dimensions.desc") => ObjectGroupSort::DimensionsDescending,
        Some("object_count") | Some("object_count.asc") => ObjectGroupSort::ObjectCountAscending,
        Some("object_count.desc") => ObjectGroupSort::ObjectCountDescending,
        Some(value) => {
            return Err(ApiError::BadRequest(format!(
                "Invalid object group sort '{value}'; use dimensions.asc|desc or object_count.asc|desc"
            )));
        }
    };

    Ok(ObjectGroupQuery {
        query_options,
        spec: ObjectGroupSpec::new(dimensions, sort)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cursor_budget() -> ObjectGroupCursorBudget {
        ObjectGroupCursorBudget::for_request_target(
            "/api/v1/classes/1/object-aggregates",
            "group_by=name",
        )
        .unwrap()
    }

    fn encoded_cursor(dimension: &str, sort_key: serde_json::Value, object_count: i64) -> String {
        let token = ObjectGroupCursorToken {
            version: 1,
            dimensions: vec![dimension.to_string()],
            sort: ObjectGroupSort::DimensionsAscending,
            sort_key,
            object_count,
        };
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(serde_json::to_vec(&token).unwrap())
    }

    #[test]
    fn parses_ordered_multidimensional_group_query() {
        let query = parse_object_group_query(
            "json_data__equals=status=active&group_by=json_data.location,country&group_by=computed.shared.lifecycle&sort=object_count.desc&limit=50",
        )
        .unwrap();
        let (options, spec) = query.into_parts();
        assert_eq!(options.filters.len(), 1);
        assert_eq!(options.limit, Some(50));
        assert_eq!(spec.sort(), ObjectGroupSort::ObjectCountDescending);
        assert_eq!(
            spec.dimension_names(),
            vec![
                "json_data.location,country".to_string(),
                "computed.shared.lifecycle".to_string()
            ]
        );
    }

    #[test]
    fn rejects_missing_group_dimensions() {
        let error = parse_object_group_query("").unwrap_err();
        assert!(error.to_string().contains("between 1 and 3"));
    }

    #[test]
    fn rejects_more_than_three_group_dimensions() {
        let error = parse_object_group_query(
            "group_by=name&group_by=description&group_by=collection_id&group_by=created_at",
        )
        .unwrap_err();
        assert!(error.to_string().contains("between 1 and 3"));
    }

    #[test]
    fn rejects_empty_or_malformed_json_paths() {
        for query in [
            "group_by=json_data.",
            "group_by=json_data.location,,country",
            "group_by=json_data.location%20country",
        ] {
            let error = parse_object_group_query(query).unwrap_err();
            assert!(error.to_string().contains("JSON path"));
        }
    }

    #[test]
    fn rejects_object_list_sort_fields() {
        let error = parse_object_group_query("group_by=name&order_by=created_at.desc").unwrap_err();
        assert!(error.to_string().contains("Object-list sort fields"));
    }

    #[test]
    fn cursor_is_bound_to_dimension_and_sort_spec() {
        let first = ObjectGroupSpec::new(
            vec![ObjectGroupDimension::from_str("name").unwrap()],
            ObjectGroupSort::DimensionsAscending,
        )
        .unwrap();
        let second = ObjectGroupSpec::new(
            vec![ObjectGroupDimension::from_str("description").unwrap()],
            ObjectGroupSort::DimensionsAscending,
        )
        .unwrap();
        let row = ObjectGroupRow::from_database(
            serde_json::json!([{"field": "name", "state": "value", "value": "a"}]),
            1,
            serde_json::json!([[0, "a"]]),
        )
        .unwrap();
        let budget = cursor_budget();
        let cursor = first.encode_cursor(&row, budget).unwrap();
        let error = second.decode_cursor(&cursor, budget).unwrap_err();
        assert!(error.to_string().contains("does not match"));
    }

    #[test]
    fn refuses_to_emit_an_unreplayable_group_cursor() {
        let spec = ObjectGroupSpec::new(
            vec![ObjectGroupDimension::from_str("json_data.large").unwrap()],
            ObjectGroupSort::DimensionsAscending,
        )
        .unwrap();
        let large_value = "x".repeat(MAX_OBJECT_GROUP_CURSOR_LENGTH);
        let row = ObjectGroupRow::from_database(
            serde_json::json!([{
                "field": "json_data.large",
                "state": "value",
                "value": large_value.clone(),
            }]),
            1,
            serde_json::json!([[0, large_value]]),
        )
        .unwrap();

        let error = spec.encode_cursor(&row, cursor_budget()).unwrap_err();

        assert!(matches!(error, ApiError::PayloadTooLarge(_)));
        assert!(error.to_string().contains("replay-safe limit"));
    }

    #[test]
    fn cursor_budget_accounts_for_the_complete_replay_target() {
        let short = ObjectGroupCursorBudget::for_request_target(
            "/api/v1/classes/1/object-aggregates",
            "group_by=name",
        )
        .unwrap();
        let long = ObjectGroupCursorBudget::for_request_target(
            "/api/v1/classes/1/object-aggregates",
            &format!("name__contains={}&group_by=name", "x".repeat(5_000)),
        )
        .unwrap();

        assert!(long.max_encoded_bytes() < short.max_encoded_bytes());
        assert_eq!(
            MAX_OBJECT_GROUP_CURSOR_LENGTH
                + NEXT_CURSOR_HEADER_PREFIX.len()
                + HTTP_LINE_TERMINATOR.len(),
            COMMON_HTTP_LINE_LIMIT_BYTES
        );
    }

    #[test]
    fn existing_cursor_does_not_reduce_its_replacement_budget() {
        let without_cursor = ObjectGroupCursorBudget::for_request_target(
            "/api/v1/classes/1/object-aggregates",
            "group_by=name&limit=1",
        )
        .unwrap();
        let with_cursor = ObjectGroupCursorBudget::for_request_target(
            "/api/v1/classes/1/object-aggregates",
            "group_by=name&cursor=opaque&limit=1",
        )
        .unwrap();

        assert_eq!(with_cursor, without_cursor);
    }

    #[test]
    fn cursor_emission_uses_the_request_specific_budget() {
        let spec = ObjectGroupSpec::new(
            vec![ObjectGroupDimension::from_str("json_data.large").unwrap()],
            ObjectGroupSort::DimensionsAscending,
        )
        .unwrap();
        let boundary_value = "x".repeat(1_000);
        let row = ObjectGroupRow::from_database(
            serde_json::json!([{
                "field": "json_data.large",
                "state": "value",
                "value": boundary_value.clone(),
            }]),
            1,
            serde_json::json!([[0, boundary_value]]),
        )
        .unwrap();
        let budget = ObjectGroupCursorBudget::for_request_target(
            "/api/v1/classes/1/object-aggregates",
            &format!(
                "name__contains={}&group_by=json_data.large",
                "x".repeat(7_000)
            ),
        )
        .unwrap();

        let error = spec.encode_cursor(&row, budget).unwrap_err();

        assert!(matches!(error, ApiError::PayloadTooLarge(_)));
        assert!(error.to_string().contains("for this request"));
    }

    #[test]
    fn rejects_an_oversized_group_cursor_before_decoding() {
        let spec = ObjectGroupSpec::new(
            vec![ObjectGroupDimension::from_str("name").unwrap()],
            ObjectGroupSort::DimensionsAscending,
        )
        .unwrap();

        let error = spec
            .decode_cursor(
                &"a".repeat(MAX_OBJECT_GROUP_CURSOR_LENGTH + 1),
                cursor_budget(),
            )
            .unwrap_err();

        assert!(matches!(error, ApiError::PayloadTooLarge(_)));
        assert!(error.to_string().contains("replay-safe limit"));
    }

    #[rstest::rstest]
    #[case(serde_json::json!([null]), 1)]
    #[case(serde_json::json!([[0]]), 1)]
    #[case(serde_json::json!([[0, null]]), 1)]
    #[case(serde_json::json!([[1, null]]), 1)]
    #[case(serde_json::json!([[4, "value"]]), 1)]
    #[case(serde_json::json!([[0, "value"]]), 0)]
    fn rejects_cursor_with_invalid_ordering_values(
        #[case] sort_key: serde_json::Value,
        #[case] object_count: i64,
    ) {
        let spec = ObjectGroupSpec::new(
            vec![ObjectGroupDimension::from_str("name").unwrap()],
            ObjectGroupSort::DimensionsAscending,
        )
        .unwrap();

        let error = spec
            .decode_cursor(
                &encoded_cursor("name", sort_key, object_count),
                cursor_budget(),
            )
            .unwrap_err();

        assert!(error.to_string().contains("invalid ordering values"));
    }

    #[rstest::rstest]
    #[case("name", serde_json::json!(42))]
    #[case("description", serde_json::json!(false))]
    #[case("collection_id", serde_json::json!("42"))]
    #[case("collection_id", serde_json::json!(0))]
    #[case("created_at", serde_json::json!(true))]
    #[case("updated_at", serde_json::json!("not-a-timestamp"))]
    fn rejects_cursor_with_wrong_scalar_value_type(
        #[case] dimension: &str,
        #[case] value: serde_json::Value,
    ) {
        let spec = ObjectGroupSpec::new(
            vec![ObjectGroupDimension::from_str(dimension).unwrap()],
            ObjectGroupSort::DimensionsAscending,
        )
        .unwrap();

        let error = spec
            .decode_cursor(
                &encoded_cursor(dimension, serde_json::json!([[0, value]]), 1),
                cursor_budget(),
            )
            .unwrap_err();

        assert!(error.to_string().contains("invalid ordering values"));
    }

    #[rstest::rstest]
    #[case("name", serde_json::json!("router"))]
    #[case("description", serde_json::json!("edge device"))]
    #[case("collection_id", serde_json::json!(42))]
    #[case("created_at", serde_json::json!("2026-07-20T12:34:56.123456"))]
    #[case("updated_at", serde_json::json!("2026-07-20T12:34:56"))]
    fn accepts_cursor_with_correct_scalar_value_type(
        #[case] dimension: &str,
        #[case] value: serde_json::Value,
    ) {
        let spec = ObjectGroupSpec::new(
            vec![ObjectGroupDimension::from_str(dimension).unwrap()],
            ObjectGroupSort::DimensionsAscending,
        )
        .unwrap();

        spec.decode_cursor(
            &encoded_cursor(dimension, serde_json::json!([[0, value]]), 1),
            cursor_budget(),
        )
        .unwrap();
    }

    #[test]
    fn rejects_computed_source_filters() {
        let error = parse_object_group_query(
            "computed.shared.lifecycle__equals=active&group_by=description",
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("Computed fields are not supported")
        );
    }
}
