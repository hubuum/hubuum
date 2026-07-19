use std::collections::HashSet;
use std::str::FromStr;

use base64::Engine;
use hubuum_computed_fields::FieldKey;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::errors::ApiError;
use crate::models::search::{QueryOptions, parse_query_parameter_with_passthrough};

pub const MIN_OBJECT_GROUP_DIMENSIONS: usize = 1;
pub const MAX_OBJECT_GROUP_DIMENSIONS: usize = 3;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComputedFieldScope {
    Shared,
    Personal,
}

impl ComputedFieldScope {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Shared => "shared",
            Self::Personal => "personal",
        }
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

    pub(crate) fn decode_cursor(&self, cursor: &str) -> Result<DecodedObjectGroupCursor, ApiError> {
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
        if token
            .sort_key
            .as_array()
            .is_none_or(|values| values.len() != self.dimensions.len())
            || token.object_count < 0
        {
            return Err(ApiError::BadRequest(
                "group cursor contains invalid ordering values".to_string(),
            ));
        }
        Ok(DecodedObjectGroupCursor {
            sort_key: token.sort_key,
            object_count: token.object_count,
        })
    }

    pub(crate) fn encode_cursor(&self, row: &ObjectGroupRow) -> Result<String, ApiError> {
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
        Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
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
}

pub struct ObjectGroupBackendRequest {
    class_id: i32,
    candidates: Vec<crate::models::HubuumObject>,
    query_options: QueryOptions,
    spec: ObjectGroupSpec,
    personal_owner_id: Option<i32>,
}

impl ObjectGroupBackendRequest {
    pub fn new(
        class_id: i32,
        candidates: Vec<crate::models::HubuumObject>,
        query_options: QueryOptions,
        spec: ObjectGroupSpec,
        personal_owner_id: Option<i32>,
    ) -> Result<Self, ApiError> {
        if class_id <= 0 {
            return Err(ApiError::BadRequest(
                "Object group class id must be positive".to_string(),
            ));
        }
        Ok(Self {
            class_id,
            candidates,
            query_options,
            spec,
            personal_owner_id,
        })
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        i32,
        Vec<crate::models::HubuumObject>,
        QueryOptions,
        ObjectGroupSpec,
        Option<i32>,
    ) {
        (
            self.class_id,
            self.candidates,
            self.query_options,
            self.spec,
            self.personal_owner_id,
        )
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
        let cursor = first.encode_cursor(&row).unwrap();
        let error = second.decode_cursor(&cursor).unwrap_err();
        assert!(error.to_string().contains("does not match"));
    }
}
