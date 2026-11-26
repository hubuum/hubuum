//! App-neutral query parsing and search parameter types.
//!
//! This crate intentionally does not depend on Actix, Diesel, app models,
//! permissions, pagination config, or Hubuum API errors. The application maps
//! [`QueryError`] into its public error surface at the boundary.

mod parser;
pub use parser::{
    decode_query_parameter_pair, decode_query_parameter_pairs, parse_query_parameter,
    parse_query_parameter_with_computed_and_related_filters_and_passthrough,
    parse_query_parameter_with_computed_filters_and_passthrough,
    parse_query_parameter_with_passthrough,
};
mod cursor_codec;
pub use cursor_codec::{
    CursorCodecError, CursorValue, QueryContinuation, decode_cursor_values, encode_cursor_values,
};
mod values;
pub use values::{
    parse_boolean_value, parse_datetime_list, parse_integer_list, parse_integer_list_with_limit,
    parse_positive_bigint_list_with_limit,
};

mod traversal;
pub use traversal::{MAX_TRAVERSAL_DEPTH, MAX_TRAVERSAL_WORK_ROWS, TraversalBudget};

use chrono::{DateTime, NaiveDate};
use hubuum_regex::Pattern;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

/// Maximum number of unique integers accepted from one list or range filter.
///
/// This bound is enforced while ranges are expanded so compact inputs cannot
/// force unbounded allocation before an endpoint applies its operator-specific
/// limits.
pub const MAX_INTEGER_FILTER_VALUES: usize = 1_024;

/// Maximum number of top-level `key=value` components accepted by the common
/// query parser.
pub const MAX_QUERY_PARAMETERS: usize = 128;

/// Maximum number of resource filters accepted by one parsed query.
pub const MAX_QUERY_FILTERS: usize = 64;

/// Maximum number of requested sort fields. Cursor predicate construction is
/// quadratic in this value, so keep it deliberately small and explicit.
pub const MAX_QUERY_SORT_FIELDS: usize = 8;

/// Maximum number of independent related-object predicates accepted by one
/// object-list query.
pub const MAX_RELATED_FILTER_GROUPS: usize = 4;

/// Maximum relationship depth accepted by the GET query grammar.
pub const MAX_RELATED_FILTER_DEPTH: u8 = 10;

/// Relationship depth used when a related filter group omits `depth__lte`.
pub const DEFAULT_RELATED_FILTER_DEPTH: u8 = 1;

/// Maximum length of the caller-selected name that correlates related filters.
pub const MAX_RELATED_FILTER_ALIAS_LENGTH: usize = 64;

/// Maximum encoded cursor size accepted by the shared query boundary.
pub const MAX_ENCODED_CURSOR_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryError {
    BadRequest(String),
    InvalidIntegerRange(String),
}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueryError::BadRequest(message) | QueryError::InvalidIntegerRange(message) => {
                f.write_str(message)
            }
        }
    }
}

impl std::error::Error for QueryError {}

fn validate_related_filter_groups(filters: &[ParsedQueryParam]) -> Result<(), QueryError> {
    #[derive(Default)]
    struct GroupCounts {
        class_selectors: usize,
        depth_filters: usize,
    }

    let mut aliases = HashMap::<&str, GroupCounts>::new();

    for filter in filters {
        let Some(field) = filter.field.related_query() else {
            continue;
        };
        let counts = aliases.entry(field.alias()).or_default();
        match field.target() {
            RelatedFilterTarget::Class(_) => {
                counts.class_selectors += 1;
                if filter.operator != (SearchOperator::Equals { is_negated: false }) {
                    return Err(QueryError::BadRequest(format!(
                        "Related filter group '{}' requires an unnegated equality class selector",
                        field.alias()
                    )));
                }
            }
            RelatedFilterTarget::Depth => {
                counts.depth_filters += 1;
                if filter.operator != (SearchOperator::Lte { is_negated: false }) {
                    return Err(QueryError::BadRequest(format!(
                        "Related filter group '{}' only supports depth__lte",
                        field.alias()
                    )));
                }
                let depth = filter.value().parse::<u8>().map_err(|_| {
                    QueryError::BadRequest(format!(
                        "Related filter depth must be an integer from 1 to {MAX_RELATED_FILTER_DEPTH}"
                    ))
                })?;
                if !(1..=MAX_RELATED_FILTER_DEPTH).contains(&depth) {
                    return Err(QueryError::BadRequest(format!(
                        "Related filter depth must be an integer from 1 to {MAX_RELATED_FILTER_DEPTH}"
                    )));
                }
            }
            RelatedFilterTarget::Object(field) => {
                if let Some(data_type) = field.data_type()
                    && !filter.operator.is_applicable_to(data_type)
                {
                    return Err(QueryError::BadRequest(format!(
                        "Operator '{}' is not applicable to related object field '{}'",
                        filter.operator,
                        field.as_str()
                    )));
                }
            }
        }
    }

    if aliases.len() > MAX_RELATED_FILTER_GROUPS {
        return Err(QueryError::BadRequest(format!(
            "query accepts at most {MAX_RELATED_FILTER_GROUPS} related filter groups"
        )));
    }
    for (alias, counts) in aliases {
        if counts.class_selectors != 1 {
            return Err(QueryError::BadRequest(format!(
                "Related filter group '{alias}' requires exactly one class.id or class.name selector"
            )));
        }
        if counts.depth_filters > 1 {
            return Err(QueryError::BadRequest(format!(
                "Related filter group '{alias}' accepts at most one depth__lte filter"
            )));
        }
    }

    Ok(())
}

fn is_computed_field_name(key: &str) -> bool {
    [
        "computed.shared.",
        "computed.public.",
        "computed.personal.",
        "computed.private.",
    ]
    .iter()
    .any(|prefix| key.starts_with(prefix))
}

/// A bounded collection of query filters.
///
/// The private representation prevents callers from bypassing the same
/// resource limit enforced by the query-string parser.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct QueryFilters(Vec<ParsedQueryParam>);

impl QueryFilters {
    pub fn new(filters: Vec<ParsedQueryParam>) -> Result<Self, QueryError> {
        if filters.len() > MAX_QUERY_FILTERS {
            return Err(QueryError::BadRequest(format!(
                "query accepts at most {MAX_QUERY_FILTERS} filters"
            )));
        }
        if filters
            .iter()
            .any(|filter| filter.field.related_query().is_some())
        {
            validate_related_filter_groups(&filters)?;
        }
        Ok(Self(filters))
    }

    #[must_use]
    pub fn as_slice(&self) -> &[ParsedQueryParam] {
        &self.0
    }

    #[must_use]
    pub fn into_vec(self) -> Vec<ParsedQueryParam> {
        self.0
    }

    /// Remove filters while preserving all construction-time invariants.
    pub fn try_retain(
        &mut self,
        predicate: impl FnMut(&ParsedQueryParam) -> bool,
    ) -> Result<(), QueryError> {
        if !self
            .0
            .iter()
            .any(|filter| filter.field.related_query().is_some())
        {
            self.0.retain(predicate);
            return Ok(());
        }

        let previous = self.0.clone();
        self.0.retain(predicate);
        if self
            .0
            .iter()
            .any(|filter| filter.field.related_query().is_some())
            && let Err(error) = validate_related_filter_groups(&self.0)
        {
            self.0 = previous;
            return Err(error);
        }
        Ok(())
    }

    /// Add one filter while enforcing the same bounds and related-filter
    /// invariants as construction.
    pub fn try_push(&mut self, parameter: ParsedQueryParam) -> Result<(), QueryError> {
        if self.0.len() >= MAX_QUERY_FILTERS {
            return Err(QueryError::BadRequest(format!(
                "query accepts at most {MAX_QUERY_FILTERS} filters"
            )));
        }
        let validate_related = parameter.field.related_query().is_some();
        self.0.push(parameter);
        if validate_related && let Err(error) = validate_related_filter_groups(&self.0) {
            self.0.pop();
            return Err(error);
        }
        Ok(())
    }

    /// Resolve the value type of every computed field without exposing
    /// unrestricted mutable access to the bounded collection.
    pub fn try_resolve_computed_fields<E>(
        &mut self,
        mut resolver: impl FnMut(&ComputedQueryField) -> Result<ComputedQueryValueType, E>,
    ) -> Result<(), E> {
        for parameter in &mut self.0 {
            if let Some(field) = parameter.field.computed_query_mut() {
                let value_type = resolver(field)?;
                field.resolve(value_type);
            }
        }
        Ok(())
    }
}

impl TryFrom<Vec<ParsedQueryParam>> for QueryFilters {
    type Error = QueryError;

    fn try_from(filters: Vec<ParsedQueryParam>) -> Result<Self, Self::Error> {
        Self::new(filters)
    }
}

impl std::ops::Deref for QueryFilters {
    type Target = [ParsedQueryParam];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<'a> IntoIterator for &'a QueryFilters {
    type Item = &'a ParsedQueryParam;
    type IntoIter = std::slice::Iter<'a, ParsedQueryParam>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl IntoIterator for QueryFilters {
    type Item = ParsedQueryParam;
    type IntoIter = std::vec::IntoIter<ParsedQueryParam>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// A bounded, duplicate-free sort specification.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct QuerySort(Vec<SortParam>);

impl QuerySort {
    pub fn new(sort: Vec<SortParam>) -> Result<Self, QueryError> {
        if sort.len() > MAX_QUERY_SORT_FIELDS {
            return Err(QueryError::BadRequest(format!(
                "query accepts at most {MAX_QUERY_SORT_FIELDS} sort fields"
            )));
        }
        for (index, parameter) in sort.iter().enumerate() {
            if parameter.field.related_query().is_some() {
                return Err(QueryError::BadRequest(
                    "Related fields cannot be used for sorting".to_string(),
                ));
            }
            if sort[..index]
                .iter()
                .any(|existing| existing.field == parameter.field)
            {
                return Err(QueryError::BadRequest(format!(
                    "duplicate sort field '{}'",
                    parameter.field
                )));
            }
        }
        Ok(Self(sort))
    }

    #[must_use]
    pub fn as_slice(&self) -> &[SortParam] {
        &self.0
    }

    #[must_use]
    pub fn into_vec(self) -> Vec<SortParam> {
        self.0
    }

    /// Add one deterministic pagination tie-breaker to a caller-bounded sort.
    ///
    /// The parser limit applies to caller-selected fields; storage adapters
    /// may need one additional unique field to guarantee stable cursor order.
    pub fn append_tie_breaker(&mut self, parameter: SortParam) -> Result<(), QueryError> {
        if self.0.len() > MAX_QUERY_SORT_FIELDS {
            return Err(QueryError::BadRequest(format!(
                "effective query sort accepts at most {} fields",
                MAX_QUERY_SORT_FIELDS + 1
            )));
        }
        if parameter.field.related_query().is_some() {
            return Err(QueryError::BadRequest(
                "Related fields cannot be used for sorting".to_string(),
            ));
        }
        if self
            .0
            .iter()
            .any(|existing| existing.field == parameter.field)
        {
            return Err(QueryError::BadRequest(format!(
                "duplicate sort field '{}'",
                parameter.field
            )));
        }
        self.0.push(parameter);
        Ok(())
    }

    /// Resolve the value type of every computed sort field without exposing
    /// unrestricted mutable access to the bounded collection.
    pub fn try_resolve_computed_fields<E>(
        &mut self,
        mut resolver: impl FnMut(&ComputedQueryField) -> Result<ComputedQueryValueType, E>,
    ) -> Result<(), E> {
        for parameter in &mut self.0 {
            if let Some(field) = parameter.field.computed_query_mut() {
                let value_type = resolver(field)?;
                field.resolve(value_type);
            }
        }
        Ok(())
    }
}

impl TryFrom<Vec<SortParam>> for QuerySort {
    type Error = QueryError;

    fn try_from(sort: Vec<SortParam>) -> Result<Self, Self::Error> {
        Self::new(sort)
    }
}

impl std::ops::Deref for QuerySort {
    type Target = [SortParam];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<'a> IntoIterator for &'a QuerySort {
    type Item = &'a SortParam;
    type IntoIter = std::slice::Iter<'a, SortParam>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl IntoIterator for QuerySort {
    type Item = SortParam;
    type IntoIter = std::vec::IntoIter<SortParam>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// A cursor token bounded before it reaches a decoder or storage adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryCursor(String);

#[derive(Debug, Clone, PartialEq, Eq)]
enum QueryPosition {
    Token(QueryCursor),
    Continuation(QueryContinuation),
}

impl QueryCursor {
    pub fn new(cursor: String) -> Result<Self, QueryError> {
        if cursor.len() > MAX_ENCODED_CURSOR_BYTES {
            return Err(QueryError::BadRequest(format!(
                "cursor exceeds the maximum encoded size of {MAX_ENCODED_CURSOR_BYTES} bytes"
            )));
        }
        Ok(Self(cursor))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }
}

impl TryFrom<String> for QueryCursor {
    type Error = QueryError;

    fn try_from(cursor: String) -> Result<Self, Self::Error> {
        Self::new(cursor)
    }
}

impl std::ops::Deref for QueryCursor {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl AsRef<str> for QueryCursor {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for QueryCursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Semantic field addressed by a recursive structured-search predicate.
///
/// The HTTP layer validates which fields are available for a resource kind;
/// storage adapters map the validated semantic field to their private schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StructuredQueryField {
    Id,
    Name,
    Description,
    CollectionId,
    CreatedAt,
    UpdatedAt,
    Revision,
    JsonData,
    ValidateSchema,
    JsonSchema,
    IdentityScope,
    ProperName,
    Email,
    OccurredAt,
    EntityType,
    EntityId,
    EntityName,
    Action,
    ActorKind,
    ActorUserId,
    InitiatorUserId,
    Summary,
    Metadata,
    ManagedBy,
    ExternalKey,
    LastSyncAttemptedAt,
    LastSyncSuccessAt,
    OwnerGroupId,
    CreatedBy,
    DisabledAt,
}

/// Backend-neutral recursive predicate prepared by an API or service layer.
#[derive(Debug, Clone, PartialEq)]
pub enum StructuredQueryExpression {
    And(Vec<Self>),
    Or(Vec<Self>),
    Not(Box<Self>),
    Field {
        field: StructuredQueryField,
        parameter: ParsedQueryParam,
    },
    Related(Vec<ParsedQueryParam>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryOptions {
    filters: QueryFilters,
    sort: QuerySort,
    limit: Option<usize>,
    cursor: Option<QueryPosition>,
    include_total: bool,
    structured_filter: Option<StructuredQueryExpression>,
}

impl QueryOptions {
    fn from_parsed_parts(
        filters: Vec<ParsedQueryParam>,
        sort: Vec<SortParam>,
        limit: Option<usize>,
        cursor: Option<String>,
        include_total: bool,
    ) -> Result<Self, QueryError> {
        validate_query_limit(limit)?;
        Ok(Self {
            filters: QueryFilters(filters),
            sort: QuerySort(sort),
            limit,
            cursor: cursor
                .map(QueryCursor::new)
                .transpose()?
                .map(QueryPosition::Token),
            include_total,
            structured_filter: None,
        })
    }

    pub fn new(
        filters: Vec<ParsedQueryParam>,
        sort: Vec<SortParam>,
        limit: Option<usize>,
        cursor: Option<String>,
        include_total: bool,
    ) -> Result<Self, QueryError> {
        validate_query_limit(limit)?;
        Ok(Self {
            filters: QueryFilters::new(filters)?,
            sort: QuerySort::new(sort)?,
            limit,
            cursor: cursor
                .map(QueryCursor::new)
                .transpose()?
                .map(QueryPosition::Token),
            include_total,
            structured_filter: None,
        })
    }

    #[must_use]
    pub fn empty() -> Self {
        Self {
            filters: QueryFilters::default(),
            sort: QuerySort::default(),
            limit: None,
            cursor: None,
            include_total: true,
            structured_filter: None,
        }
    }

    #[must_use]
    pub const fn filters(&self) -> &QueryFilters {
        &self.filters
    }

    #[must_use]
    pub const fn structured_filter(&self) -> Option<&StructuredQueryExpression> {
        self.structured_filter.as_ref()
    }

    pub fn set_structured_filter(&mut self, filter: Option<StructuredQueryExpression>) {
        self.structured_filter = filter;
    }

    /// Mutably access the bounded filter collection.
    ///
    /// The returned type exposes only invariant-preserving mutations.
    pub const fn filters_mut(&mut self) -> &mut QueryFilters {
        &mut self.filters
    }

    pub fn set_filters(&mut self, filters: QueryFilters) {
        self.filters = filters;
    }

    #[must_use]
    pub const fn sort(&self) -> &QuerySort {
        &self.sort
    }

    /// Mutably access the bounded sort collection.
    ///
    /// The returned type exposes only invariant-preserving mutations.
    pub const fn sort_mut(&mut self) -> &mut QuerySort {
        &mut self.sort
    }

    /// Mutably access both invariant-preserving query collections.
    pub const fn filters_and_sort_mut(&mut self) -> (&mut QueryFilters, &mut QuerySort) {
        (&mut self.filters, &mut self.sort)
    }

    pub fn set_sort(&mut self, sort: QuerySort) {
        self.sort = sort;
    }

    #[must_use]
    pub const fn limit(&self) -> Option<usize> {
        self.limit
    }

    pub fn set_limit(&mut self, limit: Option<usize>) -> Result<(), QueryError> {
        validate_query_limit(limit)?;
        self.limit = limit;
        Ok(())
    }

    /// Return the encoded client token only. Ordinary storage page readers must
    /// use [`Self::cursor_values`] to also honor internal continuations.
    #[must_use]
    pub const fn cursor(&self) -> Option<&QueryCursor> {
        match &self.cursor {
            Some(QueryPosition::Token(cursor)) => Some(cursor),
            _ => None,
        }
    }

    #[must_use]
    pub const fn continuation(&self) -> Option<&QueryContinuation> {
        match &self.cursor {
            Some(QueryPosition::Continuation(continuation)) => Some(continuation),
            _ => None,
        }
    }

    #[must_use]
    pub const fn has_cursor(&self) -> bool {
        self.cursor.is_some()
    }

    /// Resolve either a bounded public token or an already validated storage
    /// continuation against the execution sort (including its tie-breaker).
    pub fn cursor_values(
        &self,
        sorts: &[SortParam],
    ) -> Result<Option<Cow<'_, [CursorValue]>>, CursorCodecError> {
        match &self.cursor {
            Some(QueryPosition::Token(cursor)) => {
                decode_cursor_values(cursor, sorts).map(|values| Some(Cow::Owned(values)))
            }
            Some(QueryPosition::Continuation(continuation)) => continuation
                .values_for(sorts)
                .map(|values| Some(Cow::Borrowed(values))),
            None => Ok(None),
        }
    }

    pub fn set_continuation(&mut self, continuation: QueryContinuation) {
        self.cursor = Some(QueryPosition::Continuation(continuation));
    }

    pub fn set_cursor(&mut self, cursor: Option<String>) -> Result<(), QueryError> {
        self.cursor = cursor
            .map(QueryCursor::new)
            .transpose()?
            .map(QueryPosition::Token);
        Ok(())
    }

    pub fn set_validated_cursor(&mut self, cursor: Option<QueryCursor>) {
        self.cursor = cursor.map(QueryPosition::Token);
    }

    pub fn clear_cursor(&mut self) {
        self.cursor = None;
    }

    #[must_use]
    pub const fn include_total(&self) -> bool {
        self.include_total
    }

    pub const fn set_include_total(&mut self, include_total: bool) {
        self.include_total = include_total;
    }

    /// Resolve computed filter and sort types while retaining all query-shape
    /// invariants.
    pub fn try_resolve_computed_fields<E>(
        &mut self,
        mut resolver: impl FnMut(&ComputedQueryField) -> Result<ComputedQueryValueType, E>,
    ) -> Result<(), E> {
        self.filters.try_resolve_computed_fields(&mut resolver)?;
        self.sort.try_resolve_computed_fields(resolver)
    }
}

fn validate_query_limit(limit: Option<usize>) -> Result<(), QueryError> {
    let Some(limit) = limit else {
        return Ok(());
    };
    if limit == 0 {
        return Err(QueryError::BadRequest(
            "limit must be greater than 0".to_string(),
        ));
    }
    i64::try_from(limit).map_err(|_| {
        QueryError::BadRequest("query limit exceeds the supported range".to_string())
    })?;
    Ok(())
}

/// A validated, comma-separated path into a JSON object.
///
/// Paths are kept app-neutral here so query filters, aggregate dimensions, and
/// future query-planning features share one grammar. Each segment must be
/// non-empty and contain only ASCII letters, digits, `_`, or `$`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct JsonFieldPathRef<'a> {
    canonical: &'a str,
}

impl<'a> JsonFieldPathRef<'a> {
    pub fn new(value: &'a str) -> Result<Self, QueryError> {
        let valid = !value.is_empty()
            && value.split(',').all(|segment| {
                !segment.is_empty()
                    && segment
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'$'))
            });
        if !valid {
            return Err(invalid_json_field_path(value));
        }
        Ok(Self { canonical: value })
    }

    pub fn segments(self) -> impl Iterator<Item = &'a str> {
        self.canonical.split(',')
    }

    pub fn canonical(self) -> &'a str {
        self.canonical
    }
}

impl fmt::Display for JsonFieldPathRef<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.canonical)
    }
}

/// An owned [`JsonFieldPathRef`].
///
/// Use the borrowed representation while parsing request-local values and this
/// owned representation when a validated path must be retained in a model.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct JsonFieldPath {
    canonical: String,
}

impl JsonFieldPath {
    pub fn new(value: &str) -> Result<Self, QueryError> {
        JsonFieldPathRef::new(value)?;
        Ok(Self {
            canonical: value.to_string(),
        })
    }

    pub fn segments(&self) -> impl Iterator<Item = &str> {
        self.as_ref().segments()
    }

    pub fn canonical(&self) -> &str {
        &self.canonical
    }

    pub fn as_ref(&self) -> JsonFieldPathRef<'_> {
        JsonFieldPathRef {
            canonical: &self.canonical,
        }
    }
}

impl FromStr for JsonFieldPath {
    type Err = QueryError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl fmt::Display for JsonFieldPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.canonical)
    }
}

fn invalid_json_field_path(value: &str) -> QueryError {
    QueryError::BadRequest(format!(
        "Invalid JSON path '{value}'; use non-empty comma-separated ASCII path segments containing only letters, digits, '_', or '$'"
    ))
}

#[derive(Debug, Clone, PartialEq)]
pub struct SortParam {
    pub field: FilterField,
    pub descending: bool,
}

impl SortParam {
    #[must_use]
    pub const fn new(field: FilterField, descending: bool) -> Self {
        Self { field, descending }
    }

    #[must_use]
    pub const fn field(&self) -> &FilterField {
        &self.field
    }

    #[must_use]
    pub const fn descending(&self) -> bool {
        self.descending
    }

    #[must_use]
    pub fn into_parts(self) -> (FilterField, bool) {
        (self.field, self.descending)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComputedQueryValueType {
    String,
    Number,
    Integer,
    Boolean,
    Object,
    Array,
}

impl ComputedQueryValueType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::String => "string",
            Self::Number => "number",
            Self::Integer => "integer",
            Self::Boolean => "boolean",
            Self::Object => "object",
            Self::Array => "array",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ComputedQueryField {
    scope: ComputedFieldScope,
    key: String,
    value_type: Option<ComputedQueryValueType>,
}

impl ComputedQueryField {
    fn unresolved(scope: ComputedFieldScope, key: &str) -> Result<Self, QueryError> {
        let valid_key = !key.is_empty()
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
        if !valid_key {
            return Err(QueryError::BadRequest(format!(
                "Invalid computed field key: '{key}'"
            )));
        }
        Ok(Self {
            scope,
            key: key.to_string(),
            value_type: None,
        })
    }

    pub const fn scope(&self) -> ComputedFieldScope {
        self.scope
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn resolve(&mut self, value_type: ComputedQueryValueType) {
        self.value_type = Some(value_type);
    }

    pub const fn value_type(&self) -> Option<ComputedQueryValueType> {
        self.value_type
    }
}

impl PartialEq for ComputedQueryField {
    fn eq(&self, other: &Self) -> bool {
        self.scope == other.scope && self.key == other.key
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedQueryParam {
    field: FilterField,
    operator: SearchOperator,
    operand: QueryOperand,
}

#[derive(Debug, Clone, PartialEq)]
enum QueryOperand {
    Text(String),
    Regex(Box<RegexOperand>),
}

#[derive(Debug, Clone, PartialEq)]
struct RegexOperand {
    value: String,
    pattern: Pattern,
}

impl RegexOperand {
    fn new(field: &FilterField, value: String) -> Result<Self, QueryError> {
        let source = if field_has_json_operand(field) {
            let (path, source) = value.split_once('=').ok_or_else(|| {
                QueryError::BadRequest("Expected exactly two parts of key=value".into())
            })?;
            JsonFieldPathRef::new(path)?;
            source
        } else {
            &value
        };
        let pattern =
            Pattern::new(source).map_err(|error| QueryError::BadRequest(error.to_string()))?;
        Ok(Self { value, pattern })
    }
}

impl ParsedQueryParam {
    pub fn new(
        field: &str,
        operator: Option<SearchOperator>,
        value: &str,
    ) -> Result<Self, QueryError> {
        Self::from_parts(
            FilterField::from_str(field)?,
            operator.unwrap_or(SearchOperator::Equals { is_negated: false }),
            value,
        )
    }

    #[inline]
    pub fn from_parts(
        field: FilterField,
        operator: SearchOperator,
        value: impl Into<String>,
    ) -> Result<Self, QueryError> {
        let value = value.into();
        let operand = if matches!(operator, SearchOperator::Regex { .. }) {
            QueryOperand::Regex(Box::new(RegexOperand::new(&field, value)?))
        } else {
            QueryOperand::Text(value)
        };
        Ok(Self {
            field,
            operator,
            operand,
        })
    }

    #[must_use]
    pub const fn field(&self) -> &FilterField {
        &self.field
    }

    #[must_use]
    pub const fn operator(&self) -> &SearchOperator {
        &self.operator
    }

    #[must_use]
    pub fn value(&self) -> &str {
        match &self.operand {
            QueryOperand::Text(value) => value,
            QueryOperand::Regex(operand) => &operand.value,
        }
    }

    /// Return the proof carried from query construction without parsing again.
    pub fn regex_pattern(&self) -> Option<&Pattern> {
        match &self.operand {
            QueryOperand::Regex(operand) => Some(&operand.pattern),
            QueryOperand::Text(_) => None,
        }
    }

    /// Remap aliases or related fields while preserving the validated operand.
    /// Changing between plain and JSON regex operands would change the source
    /// expression and therefore requires constructing a new parameter instead.
    pub fn with_field(mut self, field: FilterField) -> Result<Self, QueryError> {
        if self.regex_pattern().is_some()
            && field_has_json_operand(&self.field) != field_has_json_operand(&field)
        {
            return Err(QueryError::BadRequest(
                "Cannot remap between text and JSON regex operands".into(),
            ));
        }
        self.field = field;
        Ok(self)
    }

    #[must_use]
    pub fn into_parts(self) -> (FilterField, SearchOperator, String) {
        let value = match self.operand {
            QueryOperand::Text(value) => value,
            QueryOperand::Regex(operand) => operand.value,
        };
        (self.field, self.operator, value)
    }

    pub fn is_permission(&self) -> bool {
        self.field == FilterField::Permissions
    }

    pub fn is_collection(&self) -> bool {
        self.field == FilterField::Collections
    }

    pub fn is_json_schema(&self) -> bool {
        self.field == FilterField::JsonSchema
    }

    pub fn is_json_data(&self) -> bool {
        matches!(
            self.field,
            FilterField::JsonData | FilterField::JsonDataFrom | FilterField::JsonDataTo
        )
    }

    pub fn is_json(&self) -> bool {
        self.is_json_schema() || self.is_json_data()
    }
}

fn field_has_json_operand(field: &FilterField) -> bool {
    matches!(
        field,
        FilterField::JsonSchema
            | FilterField::JsonData
            | FilterField::JsonDataFrom
            | FilterField::JsonDataTo
    ) || field.related_query().is_some_and(|related| {
        related.target() == RelatedFilterTarget::Object(RelatedObjectField::JsonData)
    })
}

#[derive(Debug, PartialEq, Clone)]
pub enum Operator {
    Equals,
    IEquals,
    Contains,
    IContains,
    StartsWith,
    IStartsWith,
    EndsWith,
    IEndsWith,
    Like,
    Regex,
    Gt,
    Gte,
    Lt,
    Lte,
    Between,
    WithinNetwork,
    ContainsNetwork,
    ContainsIp,
    OverlapsNetwork,
    InetEquals,
    In,
    All,
    ArrayLength,
    HasKey,
    IsNull,
}

impl fmt::Display for Operator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let op = match self {
            Operator::Equals => "equals",
            Operator::IEquals => "iequals",
            Operator::Contains => "contains",
            Operator::IContains => "icontains",
            Operator::StartsWith => "startswith",
            Operator::IStartsWith => "istartswith",
            Operator::EndsWith => "endswith",
            Operator::IEndsWith => "iendswith",
            Operator::Like => "like",
            Operator::Regex => "regex",
            Operator::Gt => "gt",
            Operator::Gte => "gte",
            Operator::Lt => "lt",
            Operator::Lte => "lte",
            Operator::Between => "between",
            Operator::WithinNetwork => "within_network",
            Operator::ContainsNetwork => "contains_network",
            Operator::ContainsIp => "contains_ip",
            Operator::OverlapsNetwork => "overlaps_network",
            Operator::InetEquals => "inet_equals",
            Operator::In => "in",
            Operator::All => "all",
            Operator::ArrayLength => "array_length",
            Operator::HasKey => "has_key",
            Operator::IsNull => "is_null",
        };
        f.write_str(op)
    }
}

impl Operator {
    pub fn is_ip_operator(&self) -> bool {
        matches!(
            self,
            Operator::WithinNetwork
                | Operator::ContainsNetwork
                | Operator::ContainsIp
                | Operator::OverlapsNetwork
                | Operator::InetEquals
        )
    }

    pub fn is_json_structure_operator(&self) -> bool {
        matches!(
            self,
            Operator::In
                | Operator::All
                | Operator::ArrayLength
                | Operator::HasKey
                | Operator::IsNull
        )
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum DataType {
    String,
    NumericOrDate,
    Boolean,
    Array,
}

#[derive(Debug, PartialEq, Clone)]
pub enum SearchOperator {
    Equals { is_negated: bool },
    IEquals { is_negated: bool },
    Contains { is_negated: bool },
    IContains { is_negated: bool },
    StartsWith { is_negated: bool },
    IStartsWith { is_negated: bool },
    EndsWith { is_negated: bool },
    IEndsWith { is_negated: bool },
    Like { is_negated: bool },
    Regex { is_negated: bool },
    Gt { is_negated: bool },
    Gte { is_negated: bool },
    Lt { is_negated: bool },
    Lte { is_negated: bool },
    Between { is_negated: bool },
    WithinNetwork { is_negated: bool },
    ContainsNetwork { is_negated: bool },
    ContainsIp { is_negated: bool },
    OverlapsNetwork { is_negated: bool },
    InetEquals { is_negated: bool },
    In { is_negated: bool },
    All { is_negated: bool },
    ArrayLength { is_negated: bool },
    HasKey { is_negated: bool },
    IsNull { is_negated: bool },
}

impl fmt::Display for SearchOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (op, neg) = self.op_and_neg();
        let neg_str = if neg { "not_" } else { "" };
        write!(f, "{neg_str}{op}")
    }
}

impl SearchOperator {
    pub fn is_applicable_to(&self, data_type: DataType) -> bool {
        type SO = SearchOperator;
        match self {
            SO::Equals { .. } => true,
            SO::Gt { .. }
            | SO::Gte { .. }
            | SO::Lt { .. }
            | SO::Lte { .. }
            | SO::Between { .. } => matches!(data_type, DataType::NumericOrDate),
            SO::WithinNetwork { .. }
            | SO::ContainsNetwork { .. }
            | SO::ContainsIp { .. }
            | SO::OverlapsNetwork { .. }
            | SO::InetEquals { .. } => false,
            SO::In { .. } => {
                matches!(data_type, DataType::String)
                    || matches!(data_type, DataType::NumericOrDate)
            }
            SO::IsNull { .. } => true,
            SO::All { .. } | SO::ArrayLength { .. } | SO::HasKey { .. } => false,
            SO::Contains { .. } => {
                matches!(data_type, DataType::String) || matches!(data_type, DataType::Array)
            }
            _ => matches!(data_type, DataType::String),
        }
    }

    pub fn op_and_neg(&self) -> (Operator, bool) {
        match self {
            SearchOperator::Equals { is_negated } => (Operator::Equals, *is_negated),
            SearchOperator::IEquals { is_negated } => (Operator::IEquals, *is_negated),
            SearchOperator::Contains { is_negated } => (Operator::Contains, *is_negated),
            SearchOperator::IContains { is_negated } => (Operator::IContains, *is_negated),
            SearchOperator::StartsWith { is_negated } => (Operator::StartsWith, *is_negated),
            SearchOperator::IStartsWith { is_negated } => (Operator::IStartsWith, *is_negated),
            SearchOperator::EndsWith { is_negated } => (Operator::EndsWith, *is_negated),
            SearchOperator::IEndsWith { is_negated } => (Operator::IEndsWith, *is_negated),
            SearchOperator::Like { is_negated } => (Operator::Like, *is_negated),
            SearchOperator::Regex { is_negated } => (Operator::Regex, *is_negated),
            SearchOperator::Gt { is_negated } => (Operator::Gt, *is_negated),
            SearchOperator::Gte { is_negated } => (Operator::Gte, *is_negated),
            SearchOperator::Lt { is_negated } => (Operator::Lt, *is_negated),
            SearchOperator::Lte { is_negated } => (Operator::Lte, *is_negated),
            SearchOperator::Between { is_negated } => (Operator::Between, *is_negated),
            SearchOperator::WithinNetwork { is_negated } => (Operator::WithinNetwork, *is_negated),
            SearchOperator::ContainsNetwork { is_negated } => {
                (Operator::ContainsNetwork, *is_negated)
            }
            SearchOperator::ContainsIp { is_negated } => (Operator::ContainsIp, *is_negated),
            SearchOperator::OverlapsNetwork { is_negated } => {
                (Operator::OverlapsNetwork, *is_negated)
            }
            SearchOperator::InetEquals { is_negated } => (Operator::InetEquals, *is_negated),
            SearchOperator::In { is_negated } => (Operator::In, *is_negated),
            SearchOperator::All { is_negated } => (Operator::All, *is_negated),
            SearchOperator::ArrayLength { is_negated } => (Operator::ArrayLength, *is_negated),
            SearchOperator::HasKey { is_negated } => (Operator::HasKey, *is_negated),
            SearchOperator::IsNull { is_negated } => (Operator::IsNull, *is_negated),
        }
    }

    pub fn new_from_string(operator: &str) -> Result<Self, QueryError> {
        type SO = SearchOperator;
        let mut negated = false;
        let operator = match operator {
            operator if operator.starts_with("not_") => {
                negated = true;
                operator.trim_start_matches("not_")
            }
            operator => operator,
        };

        match operator {
            "equals" => Ok(SO::Equals {
                is_negated: negated,
            }),
            "iequals" => Ok(SO::IEquals {
                is_negated: negated,
            }),
            "contains" => Ok(SO::Contains {
                is_negated: negated,
            }),
            "icontains" => Ok(SO::IContains {
                is_negated: negated,
            }),
            "startswith" => Ok(SO::StartsWith {
                is_negated: negated,
            }),
            "istartswith" => Ok(SO::IStartsWith {
                is_negated: negated,
            }),
            "endswith" => Ok(SO::EndsWith {
                is_negated: negated,
            }),
            "iendswith" => Ok(SO::IEndsWith {
                is_negated: negated,
            }),
            "like" => Ok(SO::Like {
                is_negated: negated,
            }),
            "regex" => Ok(SO::Regex {
                is_negated: negated,
            }),
            "gt" => Ok(SO::Gt {
                is_negated: negated,
            }),
            "gte" => Ok(SO::Gte {
                is_negated: negated,
            }),
            "lt" => Ok(SO::Lt {
                is_negated: negated,
            }),
            "lte" => Ok(SO::Lte {
                is_negated: negated,
            }),
            "between" => Ok(SO::Between {
                is_negated: negated,
            }),
            "within_network" => Ok(SO::WithinNetwork {
                is_negated: negated,
            }),
            "contains_network" => Ok(SO::ContainsNetwork {
                is_negated: negated,
            }),
            "contains_ip" => Ok(SO::ContainsIp {
                is_negated: negated,
            }),
            "overlaps_network" => Ok(SO::OverlapsNetwork {
                is_negated: negated,
            }),
            "inet_equals" => Ok(SO::InetEquals {
                is_negated: negated,
            }),
            "in" | "any" => Ok(SO::In {
                is_negated: negated,
            }),
            "all" => Ok(SO::All {
                is_negated: negated,
            }),
            "array_length" => Ok(SO::ArrayLength {
                is_negated: negated,
            }),
            "has_key" => Ok(SO::HasKey {
                is_negated: negated,
            }),
            "is_null" => Ok(SO::IsNull {
                is_negated: negated,
            }),
            _ => Err(QueryError::BadRequest(format!(
                "Invalid search operator: '{operator}'"
            ))),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum QueryScalarType {
    String,
    Numeric,
    Date,
    Boolean,
    None,
}

pub fn infer_query_scalar_type_from_schema(
    schema: &serde_json::Value,
    key: &str,
) -> Option<QueryScalarType> {
    let path = JsonFieldPathRef::new(key).ok()?;
    use serde_json::Value;
    let mut current_schema = schema;

    for key in path.segments() {
        match current_schema {
            Value::Object(map) => {
                if let Some(sub_schema) = map.get("properties").and_then(|p| p.get(key)) {
                    current_schema = sub_schema;
                } else {
                    current_schema = map.get("items")?;
                }
            }
            _ => return None,
        }
    }

    if let Some(Value::String(format_str)) = current_schema.get("format")
        && matches!(format_str.as_ref(), "date-time" | "date")
    {
        return Some(QueryScalarType::Date);
    }

    match current_schema.get("type") {
        Some(Value::String(type_str)) => match type_str.as_ref() {
            "string" => Some(QueryScalarType::String),
            "number" | "integer" => Some(QueryScalarType::Numeric),
            "boolean" => Some(QueryScalarType::Boolean),
            _ => None,
        },
        _ => None,
    }
}

pub fn infer_query_scalar_type(value: &str, operator: Operator) -> Option<QueryScalarType> {
    match operator {
        Operator::Equals => infer_scalar_type_from_value(
            value,
            &[
                QueryScalarType::Date,
                QueryScalarType::Boolean,
                QueryScalarType::Numeric,
                QueryScalarType::None,
                QueryScalarType::String,
            ],
        ),
        Operator::Contains => Some(QueryScalarType::String),
        Operator::Gt | Operator::Gte | Operator::Lt | Operator::Lte => {
            infer_scalar_type_from_value(value, &[QueryScalarType::Date, QueryScalarType::Numeric])
        }
        Operator::Between => {
            let parts = value.split(',').collect::<Vec<&str>>();
            if parts.len() != 2 {
                return None;
            }
            let lval = infer_scalar_type_from_value(
                parts[0],
                &[QueryScalarType::Date, QueryScalarType::Numeric],
            );
            let rval = infer_scalar_type_from_value(
                parts[1],
                &[QueryScalarType::Date, QueryScalarType::Numeric],
            );
            if lval.is_none() || rval.is_none() || lval != rval {
                return None;
            }
            lval
        }
        Operator::IEquals
        | Operator::IContains
        | Operator::StartsWith
        | Operator::IStartsWith
        | Operator::EndsWith
        | Operator::IEndsWith
        | Operator::Like
        | Operator::Regex => Some(QueryScalarType::String),
        Operator::WithinNetwork
        | Operator::ContainsNetwork
        | Operator::ContainsIp
        | Operator::OverlapsNetwork
        | Operator::InetEquals => None,
        Operator::In => Some(QueryScalarType::String),
        Operator::All | Operator::ArrayLength | Operator::HasKey | Operator::IsNull => None,
    }
}

pub fn infer_scalar_type_from_value(
    value: &str,
    accepted_types: &[QueryScalarType],
) -> Option<QueryScalarType> {
    for t in accepted_types {
        match t {
            QueryScalarType::String => return Some(QueryScalarType::String),
            QueryScalarType::Numeric => {
                if value.parse::<f64>().is_ok() {
                    return Some(QueryScalarType::Numeric);
                }
            }
            QueryScalarType::Date => {
                if DateTime::parse_from_rfc3339(value).is_ok()
                    || NaiveDate::parse_from_str(value, "%Y-%m-%d").is_ok()
                {
                    return Some(QueryScalarType::Date);
                }
            }
            QueryScalarType::Boolean => {
                if value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("false") {
                    return Some(QueryScalarType::Boolean);
                }
            }
            QueryScalarType::None => {
                if value.is_empty() || value.eq_ignore_ascii_case("null") {
                    return Some(QueryScalarType::None);
                }
            }
        }
    }

    None
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelatedClassField {
    Id,
    Name,
}

impl RelatedClassField {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Id => "id",
            Self::Name => "name",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelatedObjectField {
    Id,
    Name,
    Description,
    CollectionId,
    CreatedAt,
    UpdatedAt,
    Revision,
    JsonData,
}

impl RelatedObjectField {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Id => "id",
            Self::Name => "name",
            Self::Description => "description",
            Self::CollectionId => "collection_id",
            Self::CreatedAt => "created_at",
            Self::UpdatedAt => "updated_at",
            Self::Revision => "revision",
            Self::JsonData => "json_data",
        }
    }

    pub const fn data_type(self) -> Option<DataType> {
        match self {
            Self::Id | Self::CollectionId | Self::Revision => Some(DataType::NumericOrDate),
            Self::Name | Self::Description => Some(DataType::String),
            Self::CreatedAt | Self::UpdatedAt => Some(DataType::NumericOrDate),
            // JSON operators depend on the addressed value and are validated
            // by the application-level JSON predicate compiler.
            Self::JsonData => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelatedFilterTarget {
    Class(RelatedClassField),
    Object(RelatedObjectField),
    Depth,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelatedQueryField {
    alias: String,
    target: RelatedFilterTarget,
}

impl RelatedQueryField {
    fn parse(value: &str) -> Result<Self, QueryError> {
        let segments = value.split('.').collect::<Vec<_>>();
        if segments.first() != Some(&"related") {
            return Err(invalid_related_field(value));
        }
        let Some(alias) = segments.get(1).copied() else {
            return Err(invalid_related_field(value));
        };
        let valid_alias = !alias.is_empty()
            && alias.len() <= MAX_RELATED_FILTER_ALIAS_LENGTH
            && alias
                .bytes()
                .enumerate()
                .all(|(index, byte)| match (index, byte) {
                    (0, b'A'..=b'Z' | b'a'..=b'z' | b'_') => true,
                    (0, _) => false,
                    (_, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_') => true,
                    (_, _) => false,
                });
        if !valid_alias {
            return Err(QueryError::BadRequest(format!(
                "Invalid related filter alias: '{alias}'"
            )));
        }

        let target = match segments.as_slice() {
            ["related", _, "class", "id"] => RelatedFilterTarget::Class(RelatedClassField::Id),
            ["related", _, "class", "name"] => RelatedFilterTarget::Class(RelatedClassField::Name),
            ["related", _, "object", "id"] => RelatedFilterTarget::Object(RelatedObjectField::Id),
            ["related", _, "object", "name"] => {
                RelatedFilterTarget::Object(RelatedObjectField::Name)
            }
            ["related", _, "object", "description"] => {
                RelatedFilterTarget::Object(RelatedObjectField::Description)
            }
            ["related", _, "object", "collection_id"] => {
                RelatedFilterTarget::Object(RelatedObjectField::CollectionId)
            }
            ["related", _, "object", "created_at"] => {
                RelatedFilterTarget::Object(RelatedObjectField::CreatedAt)
            }
            ["related", _, "object", "updated_at"] => {
                RelatedFilterTarget::Object(RelatedObjectField::UpdatedAt)
            }
            ["related", _, "object", "revision"] => {
                RelatedFilterTarget::Object(RelatedObjectField::Revision)
            }
            ["related", _, "object", "json_data"] => {
                RelatedFilterTarget::Object(RelatedObjectField::JsonData)
            }
            ["related", _, "depth"] => RelatedFilterTarget::Depth,
            _ => return Err(invalid_related_field(value)),
        };

        Ok(Self {
            alias: alias.to_string(),
            target,
        })
    }

    pub fn alias(&self) -> &str {
        &self.alias
    }

    pub const fn target(&self) -> RelatedFilterTarget {
        self.target
    }
}

impl fmt::Display for RelatedQueryField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "related.{}.", self.alias)?;
        match self.target {
            RelatedFilterTarget::Class(field) => write!(f, "class.{}", field.as_str()),
            RelatedFilterTarget::Object(field) => write!(f, "object.{}", field.as_str()),
            RelatedFilterTarget::Depth => f.write_str("depth"),
        }
    }
}

fn invalid_related_field(value: &str) -> QueryError {
    QueryError::BadRequest(format!("Invalid related search field: '{value}'"))
}

macro_rules! filter_fields {
    ($(($variant:ident, $str_rep:expr)),* $(,)?) => {
        #[derive(Debug, PartialEq, Clone)]
        pub enum FilterField {
            $($variant),*,
            Computed(Box<ComputedQueryField>),
            Related(Box<RelatedQueryField>),
        }

        impl FromStr for FilterField {
            type Err = QueryError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                match s {
                    $($str_rep => Ok(FilterField::$variant),)*
                    _ => {
                        if let Some(key) = s
                            .strip_prefix("computed.shared.")
                            .or_else(|| s.strip_prefix("computed.public."))
                        {
                            return ComputedQueryField::unresolved(ComputedFieldScope::Shared, key)
                                .map(Box::new)
                                .map(FilterField::Computed);
                        }
                        if let Some(key) = s
                            .strip_prefix("computed.personal.")
                            .or_else(|| s.strip_prefix("computed.private."))
                        {
                            return ComputedQueryField::unresolved(
                                ComputedFieldScope::Personal,
                                key,
                            )
                            .map(Box::new)
                            .map(FilterField::Computed);
                        }
                        if s.starts_with("related.") {
                            return RelatedQueryField::parse(s)
                                .map(Box::new)
                                .map(FilterField::Related);
                        }
                        Err(QueryError::BadRequest(format!(
                            "Invalid search field: '{}'",
                            s
                        )))
                    }
                }
            }
        }

        impl fmt::Display for FilterField {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                match self {
                    $(FilterField::$variant => f.write_str($str_rep),)*
                    FilterField::Computed(field) => {
                        write!(f, "computed.{}.{}", field.scope().as_str(), field.key())
                    }
                    FilterField::Related(field) => field.fmt(f),
                }
            }
        }

        impl FilterField {
            pub fn computed_query(&self) -> Option<&ComputedQueryField> {
                match self {
                    FilterField::Computed(field) => Some(field),
                    _ => None,
                }
            }

            pub fn computed_query_mut(&mut self) -> Option<&mut ComputedQueryField> {
                match self {
                    FilterField::Computed(field) => Some(field),
                    _ => None,
                }
            }

            pub fn related_query(&self) -> Option<&RelatedQueryField> {
                match self {
                    FilterField::Related(field) => Some(field),
                    _ => None,
                }
            }
        }
    }
}

filter_fields!(
    (Id, "id"),
    (Collections, "collections"),
    (CollectionId, "collection_id"),
    (Name, "name"),
    (IdentityScope, "identity_scope"),
    (Groupname, "groupname"),
    (Username, "username"),
    (ProperName, "proper_name"),
    (Description, "description"),
    (Email, "email"),
    (ValidateSchema, "validate_schema"),
    (JsonSchema, "json_schema"),
    (JsonData, "json_data"),
    (Permissions, "permissions"),
    (Classes, "classes"),
    (ClassId, "class_id"),
    (CreatedAt, "created_at"),
    (UpdatedAt, "updated_at"),
    (Revision, "revision"),
    (BeforeRevision, "before_revision"),
    (AfterRevision, "after_revision"),
    (OccurredAt, "occurred_at"),
    (NextAttemptAt, "next_attempt_at"),
    (StartedAt, "started_at"),
    (FinishedAt, "finished_at"),
    (IssuedAt, "issued_at"),
    (ExpiresAt, "expires_at"),
    (LastUsedAt, "last_used_at"),
    (Kind, "kind"),
    (Status, "status"),
    (SubmittedBy, "submitted_by"),
    (NameFrom, "from_name"),
    (NameTo, "to_name"),
    (DescriptionFrom, "from_description"),
    (DescriptionTo, "to_description"),
    (ObjectFrom, "from_objects"),
    (ObjectTo, "to_objects"),
    (ClassTo, "to_classes"),
    (ClassFrom, "from_classes"),
    (ClassToName, "to_class_name"),
    (ClassFromName, "from_class_name"),
    (CollectionsFrom, "from_collections"),
    (CollectionsTo, "to_collections"),
    (JsonDataFrom, "from_json_data"),
    (JsonDataTo, "to_json_data"),
    (CreatedAtFrom, "from_created_at"),
    (CreatedAtTo, "to_created_at"),
    (UpdatedAtFrom, "from_updated_at"),
    (UpdatedAtTo, "to_updated_at"),
    (ClassRelation, "class_relation"),
    (Depth, "depth"),
    (Path, "path"),
    (ValidFrom, "valid_from"),
    (HistoryId, "history_id"),
);

#[cfg(test)]
mod tests;
