//! App-neutral query parsing and search parameter types.
//!
//! This crate intentionally does not depend on Actix, Diesel, app models,
//! permissions, pagination config, or Hubuum API errors. The application maps
//! [`QueryError`] into its public error surface at the boundary.

use chrono::{DateTime, NaiveDate};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
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

pub fn parse_query_parameter(qs: &str) -> Result<QueryOptions, QueryError> {
    let (query_options, _) = parse_query_parameter_with_passthrough(qs, &[])?;
    Ok(query_options)
}

pub fn parse_query_parameter_with_passthrough(
    qs: &str,
    passthrough_keys: &[&str],
) -> Result<(QueryOptions, HashMap<String, Vec<String>>), QueryError> {
    parse_query_parameter_with_options(qs, passthrough_keys, QueryParserFeatures::STANDARD)
}

/// Parse query parameters for a resource that explicitly supports filtering
/// on computed fields.
pub fn parse_query_parameter_with_computed_filters_and_passthrough(
    qs: &str,
    passthrough_keys: &[&str],
) -> Result<(QueryOptions, HashMap<String, Vec<String>>), QueryError> {
    parse_query_parameter_with_options(qs, passthrough_keys, QueryParserFeatures::COMPUTED)
}

/// Parse query parameters for an object resource that supports both computed
/// fields and bounded, named related-object filter groups.
pub fn parse_query_parameter_with_computed_and_related_filters_and_passthrough(
    qs: &str,
    passthrough_keys: &[&str],
) -> Result<(QueryOptions, HashMap<String, Vec<String>>), QueryError> {
    parse_query_parameter_with_options(
        qs,
        passthrough_keys,
        QueryParserFeatures::COMPUTED_AND_RELATED,
    )
}

#[derive(Debug, Clone, Copy)]
struct QueryParserFeatures {
    computed_filters: bool,
    related_filters: bool,
}

impl QueryParserFeatures {
    const STANDARD: Self = Self {
        computed_filters: false,
        related_filters: false,
    };
    const COMPUTED: Self = Self {
        computed_filters: true,
        related_filters: false,
    };
    const COMPUTED_AND_RELATED: Self = Self {
        computed_filters: true,
        related_filters: true,
    };
}

fn parse_query_parameter_with_options(
    qs: &str,
    passthrough_keys: &[&str],
    features: QueryParserFeatures,
) -> Result<(QueryOptions, HashMap<String, Vec<String>>), QueryError> {
    let mut filters = Vec::new();
    let mut sort = Vec::new();
    let mut limit = None;
    let mut cursor = None;
    let mut include_total = None;
    let mut passthrough = HashMap::<String, Vec<String>>::new();
    let passthrough_keys = passthrough_keys.iter().copied().collect::<HashSet<_>>();

    if qs.is_empty() {
        return Ok((
            QueryOptions {
                filters,
                sort,
                limit,
                cursor,
                include_total: true,
            },
            passthrough,
        ));
    }

    for (index, chunk) in qs.split('&').enumerate() {
        if index >= MAX_QUERY_PARAMETERS {
            return Err(QueryError::BadRequest(format!(
                "query accepts at most {MAX_QUERY_PARAMETERS} parameters"
            )));
        }
        let (key, value) = decode_query_parameter_pair(chunk)?;
        if passthrough_keys.contains(key.as_ref()) {
            passthrough
                .entry(key.into_owned())
                .or_default()
                .push(value.into_owned());
            continue;
        }

        match key.as_ref() {
            "limit" => {
                if limit.is_some() {
                    return Err(QueryError::BadRequest("duplicate limit".into()));
                }
                let parsed_limit = value
                    .parse::<usize>()
                    .map_err(|e| QueryError::BadRequest(format!("bad limit: {e}")))?;
                limit = Some(parsed_limit);
            }
            "cursor" => {
                if cursor.is_some() {
                    return Err(QueryError::BadRequest("duplicate cursor".into()));
                }
                cursor = Some(value.into_owned());
            }
            "include_total" => {
                if include_total.is_some() {
                    return Err(QueryError::BadRequest("duplicate include_total".into()));
                }
                include_total = Some(parse_boolean(&value)?);
            }
            "sort" | "order_by" => {
                for piece in value.split(',') {
                    if sort.len() >= MAX_QUERY_SORT_FIELDS {
                        return Err(QueryError::BadRequest(format!(
                            "query accepts at most {MAX_QUERY_SORT_FIELDS} sort fields"
                        )));
                    }
                    let parsed = parse_sort_param(piece)?;
                    if sort
                        .iter()
                        .any(|existing: &SortParam| existing.field == parsed.field)
                    {
                        return Err(QueryError::BadRequest(format!(
                            "duplicate sort field '{}'",
                            parsed.field
                        )));
                    }
                    sort.push(parsed);
                }
            }
            _ => {
                if filters.len() >= MAX_QUERY_FILTERS {
                    return Err(QueryError::BadRequest(format!(
                        "query accepts at most {MAX_QUERY_FILTERS} filters"
                    )));
                }
                filters.push(parse_single_filter(key.as_ref(), value.as_ref(), features)?);
            }
        }
    }

    if features.related_filters {
        validate_related_filter_groups(&filters)?;
    }

    Ok((
        QueryOptions {
            filters,
            sort,
            limit,
            cursor,
            include_total: include_total.unwrap_or(true),
        },
        passthrough,
    ))
}

fn parse_sort_param(piece: &str) -> Result<SortParam, QueryError> {
    let leading_descending = piece.starts_with('-');
    let field_name = piece.trim_start_matches('-');

    // A complete computed name takes precedence over direction suffix syntax.
    // This keeps valid keys named `asc` or `desc` addressable. Callers can use
    // the leading `-` form or append another suffix to set their direction.
    match FilterField::from_str(field_name) {
        Ok(field) => related_sort_error(field).map(|field| SortParam {
            field,
            descending: leading_descending,
        }),
        Err(original_error) => {
            let (field_name, suffix_descending) =
                if let Some(field_name) = field_name.strip_suffix(".asc") {
                    (field_name, false)
                } else if let Some(field_name) = field_name.strip_suffix(".desc") {
                    (field_name, true)
                } else {
                    return Err(original_error);
                };
            Ok(SortParam {
                field: related_sort_error(FilterField::from_str(field_name)?)?,
                descending: leading_descending || suffix_descending,
            })
        }
    }
}

fn related_sort_error(field: FilterField) -> Result<FilterField, QueryError> {
    if field.related_query().is_some() {
        return Err(QueryError::BadRequest(
            "Related fields cannot be used for sorting".to_string(),
        ));
    }
    Ok(field)
}

/// Decode an `application/x-www-form-urlencoded` query string into owned pairs.
///
/// Both percent escapes and `+` space encoding are handled consistently for
/// keys and values so endpoint-specific parsers can reuse the common transport
/// grammar without depending on a web framework.
pub fn decode_query_parameter_pairs(qs: &str) -> Result<Vec<(String, String)>, QueryError> {
    let mut pairs = Vec::new();
    if qs.is_empty() {
        return Ok(pairs);
    }

    for chunk in qs.split('&') {
        let (key, value) = decode_query_parameter_pair(chunk)?;
        pairs.push((key.into_owned(), value.into_owned()));
    }

    Ok(pairs)
}

/// Decode one `key=value` query-string component.
///
/// Components that need no form or percent decoding remain borrowed. This lets
/// streaming endpoint parsers share the transport grammar without allocating a
/// complete intermediate pair list.
pub fn decode_query_parameter_pair(
    chunk: &str,
) -> Result<(Cow<'_, str>, Cow<'_, str>), QueryError> {
    let (raw_key, raw_value) = chunk
        .split_once('=')
        .ok_or_else(|| QueryError::BadRequest(format!("Invalid query parameter: '{chunk}'")))?;

    let key = decode_query_component(raw_key, chunk, "key")?;
    let value = decode_query_component(raw_value, chunk, "value")?;
    Ok((key, value))
}

fn decode_query_component<'a>(
    raw: &'a str,
    chunk: &str,
    component: &str,
) -> Result<Cow<'a, str>, QueryError> {
    let decoded = if raw.contains('+') {
        let form_encoded = raw.replace('+', " ");
        percent_encoding::percent_decode(form_encoded.as_bytes())
            .decode_utf8()
            .map(|value| Cow::Owned(value.into_owned()))
    } else {
        percent_encoding::percent_decode(raw.as_bytes()).decode_utf8()
    };

    decoded.map_err(|e| {
        QueryError::BadRequest(format!(
            "Invalid query parameter: '{chunk}', invalid {component}: {e}",
        ))
    })
}

fn parse_boolean(value: &str) -> Result<bool, QueryError> {
    match value.to_lowercase().as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(QueryError::BadRequest(format!(
            "Invalid boolean value: '{value}'"
        ))),
    }
}

fn parse_single_filter(
    key: &str,
    value: &str,
    features: QueryParserFeatures,
) -> Result<ParsedQueryParam, QueryError> {
    if value.is_empty() {
        return Err(QueryError::BadRequest(format!(
            "Invalid query parameter: '{key}', no value",
        )));
    }

    let (field_name, operator) = if is_computed_field_name(key) || key.starts_with("related.") {
        // Computed keys and related aliases may themselves contain double
        // underscores. Only a recognized terminal operator suffix is syntax;
        // every other double underscore remains part of the key.
        match key.rsplit_once("__") {
            Some((field_name, suffix)) => match SearchOperator::new_from_string(suffix) {
                Ok(operator) => (field_name, operator),
                Err(_) => (key, SearchOperator::new_from_string("equals")?),
            },
            None => (key, SearchOperator::new_from_string("equals")?),
        }
    } else {
        match key.split_once("__") {
            Some((field_name, operator)) => {
                (field_name, SearchOperator::new_from_string(operator)?)
            }
            None => (key, SearchOperator::new_from_string("equals")?),
        }
    };

    let field = FilterField::from_str(field_name)?;
    if field.computed_query().is_some() && !features.computed_filters {
        return Err(QueryError::BadRequest(
            "Computed fields are not supported in this filter context".to_string(),
        ));
    }
    if field.related_query().is_some() && !features.related_filters {
        return Err(QueryError::BadRequest(
            "Related fields are not supported in this filter context".to_string(),
        ));
    }

    Ok(ParsedQueryParam {
        field,
        operator,
        value: value.to_string(),
    })
}

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
                let depth = filter.value.parse::<u8>().map_err(|_| {
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

#[derive(Debug, Clone, PartialEq)]
pub struct QueryOptions {
    pub filters: Vec<ParsedQueryParam>,
    pub sort: Vec<SortParam>,
    pub limit: Option<usize>,
    pub cursor: Option<String>,
    pub include_total: bool,
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
    pub field: FilterField,
    pub operator: SearchOperator,
    pub value: String,
}

impl ParsedQueryParam {
    pub fn new(
        field: &str,
        operator: Option<SearchOperator>,
        value: &str,
    ) -> Result<Self, QueryError> {
        Ok(Self {
            field: FilterField::from_str(field)?,
            operator: operator.unwrap_or(SearchOperator::Equals { is_negated: false }),
            value: value.to_string(),
        })
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
pub enum SQLMappedType {
    String,
    Numeric,
    Date,
    Boolean,
    None,
}

#[derive(Debug, PartialEq, Clone)]
pub struct JsonbFieldType {
    pub value: String,
    pub mapping: SQLMappedType,
    pub operator: Operator,
}

pub fn get_jsonb_field_type_from_json_schema(
    schema: &serde_json::Value,
    key: &str,
) -> Option<SQLMappedType> {
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
        return Some(SQLMappedType::Date);
    }

    match current_schema.get("type") {
        Some(Value::String(type_str)) => match type_str.as_ref() {
            "string" => Some(SQLMappedType::String),
            "number" | "integer" => Some(SQLMappedType::Numeric),
            "boolean" => Some(SQLMappedType::Boolean),
            _ => None,
        },
        _ => None,
    }
}

pub fn get_jsonb_field_type_from_value_and_operator(
    value: &str,
    operator: Operator,
) -> Option<SQLMappedType> {
    match operator {
        Operator::Equals => get_sql_mapped_type_from_value(
            value,
            &[
                SQLMappedType::Date,
                SQLMappedType::Boolean,
                SQLMappedType::Numeric,
                SQLMappedType::None,
                SQLMappedType::String,
            ],
        ),
        Operator::Contains => Some(SQLMappedType::String),
        Operator::Gt | Operator::Gte | Operator::Lt | Operator::Lte => {
            get_sql_mapped_type_from_value(value, &[SQLMappedType::Date, SQLMappedType::Numeric])
        }
        Operator::Between => {
            let parts = value.split(',').collect::<Vec<&str>>();
            if parts.len() != 2 {
                return None;
            }
            let lval = get_sql_mapped_type_from_value(
                parts[0],
                &[SQLMappedType::Date, SQLMappedType::Numeric],
            );
            let rval = get_sql_mapped_type_from_value(
                parts[1],
                &[SQLMappedType::Date, SQLMappedType::Numeric],
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
        | Operator::Regex => Some(SQLMappedType::String),
        Operator::WithinNetwork
        | Operator::ContainsNetwork
        | Operator::ContainsIp
        | Operator::OverlapsNetwork
        | Operator::InetEquals => None,
        Operator::In => Some(SQLMappedType::String),
        Operator::All | Operator::ArrayLength | Operator::HasKey | Operator::IsNull => None,
    }
}

pub fn get_sql_mapped_type_from_value(
    value: &str,
    accepted_types: &[SQLMappedType],
) -> Option<SQLMappedType> {
    for t in accepted_types {
        match t {
            SQLMappedType::String => return Some(SQLMappedType::String),
            SQLMappedType::Numeric => {
                if value.parse::<f64>().is_ok() {
                    return Some(SQLMappedType::Numeric);
                }
            }
            SQLMappedType::Date => {
                if DateTime::parse_from_rfc3339(value).is_ok()
                    || NaiveDate::parse_from_str(value, "%Y-%m-%d").is_ok()
                {
                    return Some(SQLMappedType::Date);
                }
            }
            SQLMappedType::Boolean => {
                if value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("false") {
                    return Some(SQLMappedType::Boolean);
                }
            }
            SQLMappedType::None => {
                if value.is_empty() || value.eq_ignore_ascii_case("null") {
                    return Some(SQLMappedType::None);
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
            /// Return the JSONB column addressed by this query field.
            ///
            /// Most filter fields map to typed SQL columns and therefore have no
            /// JSONB column. Keeping that distinction in the return type prevents
            /// callers from turning an ordinary parsed field into a panic.
            pub fn json_column(&self) -> Option<&'static str> {
                match self {
                    FilterField::JsonSchema => Some("json_schema"),
                    FilterField::JsonData
                    | FilterField::JsonDataFrom
                    | FilterField::JsonDataTo => Some("data"),
                    _ => None,
                }
            }

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

/// Parse a bounded comma-separated list of positive signed 64-bit values.
/// Revision filters deliberately do not accept range expansion syntax: range
/// comparisons use the `gt`/`gte`/`lt`/`lte`/`between` operators instead.
pub fn parse_positive_bigint_list_with_limit(
    input: &str,
    max_values: usize,
) -> Result<Vec<i64>, QueryError> {
    let mut values = Vec::new();
    for segment in input.split(',') {
        if segment.is_empty() {
            return Err(QueryError::BadRequest(
                "Revision filters cannot contain an empty value".to_string(),
            ));
        }
        if values.len() == max_values {
            return Err(QueryError::BadRequest(format!(
                "Revision filter contains more than {max_values} values"
            )));
        }
        let value = segment.parse::<i64>().map_err(|_| {
            QueryError::BadRequest(format!(
                "Invalid revision '{segment}': expected a positive int64"
            ))
        })?;
        if value <= 0 {
            return Err(QueryError::BadRequest(format!(
                "Invalid revision '{segment}': expected a positive int64"
            )));
        }
        values.push(value);
    }
    Ok(values)
}

pub fn parse_integer_list(input: &str) -> Result<Vec<i32>, QueryError> {
    parse_integer_list_with_limit(input, MAX_INTEGER_FILTER_VALUES)
}

/// Parse an integer list while enforcing a caller-provided unique-value cap.
///
/// The limit is checked during insertion rather than after collecting and
/// sorting, which keeps very large ranges and heavily duplicated inputs
/// bounded in both memory and work.
pub fn parse_integer_list_with_limit(
    input: &str,
    max_values: usize,
) -> Result<Vec<i32>, QueryError> {
    let mut numbers = IntegerAccumulator::new(max_values);

    for segment in input.split(',') {
        if let Some((start, negative_end)) = segment.split_once("--") {
            if negative_end.contains("--") {
                return Err(QueryError::InvalidIntegerRange(format!(
                    "Invalid format: '{segment}'"
                )));
            }
            let start = start.parse::<i32>().map_err(|_| {
                QueryError::InvalidIntegerRange(format!("Invalid start of range: '{start}'"))
            })?;
            let end = format!("-{negative_end}").parse::<i32>().map_err(|_| {
                QueryError::InvalidIntegerRange(format!("Invalid end of range: '{negative_end}'"))
            })?;
            insert_integer_range(&mut numbers, start, end, input, segment)?;
        } else if let Some(idx) = segment.find('-') {
            if idx == 0 {
                let value = segment.parse::<i32>().map_err(|_| {
                    QueryError::InvalidIntegerRange(format!("Invalid number: '{segment}'"))
                })?;
                insert_integer(&mut numbers, value, input)?;
            } else {
                let (start, end) = segment.split_at(idx);
                let end = &end[1..];
                let start = start.parse::<i32>().map_err(|_| {
                    QueryError::InvalidIntegerRange(format!("Invalid start of range: '{start}'"))
                })?;
                let end = end.parse::<i32>().map_err(|_| {
                    QueryError::InvalidIntegerRange(format!("Invalid end of range: '{end}'"))
                })?;
                insert_integer_range(&mut numbers, start, end, input, segment)?;
            }
        } else {
            let value = segment.parse::<i32>().map_err(|_| {
                QueryError::InvalidIntegerRange(format!("Invalid number: '{segment}'"))
            })?;
            insert_integer(&mut numbers, value, input)?;
        }
    }

    Ok(numbers.finish())
}

enum IntegerAccumulator {
    Values {
        values: Vec<i32>,
        max_values: usize,
    },
    Unique {
        values: HashSet<i32>,
        max_values: usize,
    },
}

impl IntegerAccumulator {
    fn new(max_values: usize) -> Self {
        Self::Values {
            values: Vec::new(),
            max_values,
        }
    }

    fn insert(&mut self, value: i32, input: &str) -> Result<(), QueryError> {
        if let Self::Values { values, max_values } = self {
            if values.len() < *max_values {
                values.push(value);
                return Ok(());
            }
            self.promote();
        }

        let Self::Unique { values, max_values } = self else {
            unreachable!("integer accumulator must be promoted")
        };
        values.insert(value);
        if values.len() > *max_values {
            return Err(integer_filter_limit_error(input, *max_values));
        }
        Ok(())
    }

    fn extend_range(&mut self, start: i32, end: i32, input: &str) -> Result<(), QueryError> {
        if let Self::Values { values, max_values } = self {
            let range_len = (i64::from(end) - i64::from(start) + 1) as u64;
            let available = max_values.saturating_sub(values.len()) as u64;
            if range_len <= available {
                values.extend(start..=end);
                return Ok(());
            }
            self.promote();
        }

        for value in start..=end {
            self.insert(value, input)?;
        }
        Ok(())
    }

    fn promote(&mut self) {
        let Self::Values { values, max_values } = self else {
            return;
        };
        let max_values = *max_values;
        let unique = std::mem::take(values).into_iter().collect();
        *self = Self::Unique {
            values: unique,
            max_values,
        };
    }

    fn finish(self) -> Vec<i32> {
        let mut values = match self {
            Self::Values { values, .. } => values,
            Self::Unique { values, .. } => values.into_iter().collect(),
        };
        values.sort_unstable();
        values.dedup();
        values
    }
}

fn insert_integer_range(
    numbers: &mut IntegerAccumulator,
    start: i32,
    end: i32,
    input: &str,
    segment: &str,
) -> Result<(), QueryError> {
    if start > end {
        return Err(QueryError::InvalidIntegerRange(format!(
            "Range start is greater than end: '{segment}'"
        )));
    }
    numbers.extend_range(start, end, input)
}

fn insert_integer(
    numbers: &mut IntegerAccumulator,
    value: i32,
    input: &str,
) -> Result<(), QueryError> {
    numbers.insert(value, input)
}

fn integer_filter_limit_error(input: &str, max_values: usize) -> QueryError {
    QueryError::InvalidIntegerRange(format!(
        "Integer filter '{input}' expands to more than {max_values} unique values"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filter_field_keeps_ordinary_variants_compact() {
        assert!(
            std::mem::size_of::<FilterField>() <= 2 * std::mem::size_of::<usize>(),
            "FilterField should keep computed sort state behind indirection"
        );
    }

    #[test]
    fn filter_field_json_column_mapping_is_total() {
        assert_eq!(FilterField::JsonSchema.json_column(), Some("json_schema"));
        assert_eq!(FilterField::JsonData.json_column(), Some("data"));
        assert_eq!(FilterField::JsonDataFrom.json_column(), Some("data"));
        assert_eq!(FilterField::JsonDataTo.json_column(), Some("data"));
        assert_eq!(FilterField::Id.json_column(), None);

        let computed = FilterField::from_str("computed.shared.rank").unwrap();
        assert_eq!(computed.json_column(), None);
    }

    #[test]
    fn parses_filters_sort_cursor_and_limit() {
        let parsed = parse_query_parameter(
            "name__not_icontains=archived&limit=10&cursor=abc&sort=-created_at,name.asc",
        )
        .unwrap();

        assert_eq!(parsed.filters.len(), 1);
        assert_eq!(parsed.limit, Some(10));
        assert_eq!(parsed.cursor.as_deref(), Some("abc"));
        assert_eq!(parsed.sort.len(), 2);
        assert!(parsed.sort[0].descending);
        assert!(!parsed.sort[1].descending);
    }

    #[test]
    fn parses_named_related_filter_groups() {
        let (parsed, _) =
            parse_query_parameter_with_computed_and_related_filters_and_passthrough(
                "related.room.class.name=Room&related.room.object.name__iequals=foo&related.room.depth__lte=5&related.owner.class.id=42&related.owner.object.revision__gt=4",
                &[],
            )
            .unwrap();

        assert_eq!(parsed.filters.len(), 5);
        let room = parsed.filters[0].field.related_query().unwrap();
        assert_eq!(room.alias(), "room");
        assert_eq!(
            room.target(),
            RelatedFilterTarget::Class(RelatedClassField::Name)
        );
        assert_eq!(
            parsed.filters[1].field.related_query().unwrap().target(),
            RelatedFilterTarget::Object(RelatedObjectField::Name)
        );
        assert_eq!(
            parsed.filters[2].field.related_query().unwrap().target(),
            RelatedFilterTarget::Depth
        );
    }

    fn related_parse_error(query: &str) -> QueryError {
        parse_query_parameter_with_computed_and_related_filters_and_passthrough(query, &[])
            .unwrap_err()
    }

    #[test]
    fn related_filter_group_requires_a_class_selector() {
        let error = related_parse_error("related.room.object.name=foo");

        assert!(
            error
                .to_string()
                .contains("exactly one class.id or class.name")
        );
    }

    #[test]
    fn related_filter_group_rejects_multiple_class_selectors() {
        let error = related_parse_error("related.room.class.name=Room&related.room.class.id=2");

        assert!(
            error
                .to_string()
                .contains("exactly one class.id or class.name")
        );
    }

    #[test]
    fn related_filters_enforce_the_group_count_limit() {
        let groups = (0..=MAX_RELATED_FILTER_GROUPS)
            .map(|index| format!("related.g{index}.class.name=Class{index}"))
            .collect::<Vec<_>>()
            .join("&");
        let error = related_parse_error(&groups);

        assert!(
            error
                .to_string()
                .contains("at most 4 related filter groups")
        );
    }

    #[test]
    fn related_filter_depth_rejects_zero() {
        let error = related_parse_error("related.room.class.name=Room&related.room.depth__lte=0");

        assert!(error.to_string().contains("integer from 1 to 10"));
    }

    #[test]
    fn related_filter_depth_rejects_values_above_the_limit() {
        let error = related_parse_error("related.room.class.name=Room&related.room.depth__lte=11");

        assert!(error.to_string().contains("integer from 1 to 10"));
    }

    #[test]
    fn related_filter_depth_rejects_non_integer_values() {
        let error = related_parse_error("related.room.class.name=Room&related.room.depth__lte=x");

        assert!(error.to_string().contains("integer from 1 to 10"));
    }

    #[test]
    fn related_filter_rejects_an_invalid_alias() {
        assert!(
            related_parse_error("related.1room.class.name=Room")
                .to_string()
                .contains("alias")
        );
    }

    #[test]
    fn related_filter_rejects_an_unknown_class_field() {
        let error = related_parse_error("related.room.class.description=Room");

        assert!(error.to_string().contains("Invalid related search field"));
    }

    #[test]
    fn related_filter_class_selector_requires_equality() {
        let error = related_parse_error("related.room.class.name__icontains=Room");

        assert!(error.to_string().contains("unnegated equality"));
    }

    #[test]
    fn related_filter_depth_requires_the_lte_suffix() {
        let error = related_parse_error("related.room.class.name=Room&related.room.depth=2");

        assert!(error.to_string().contains("only supports depth__lte"));
    }

    #[test]
    fn related_filter_depth_rejects_other_operators() {
        let error = related_parse_error("related.room.class.name=Room&related.room.depth__gte=2");

        assert!(error.to_string().contains("only supports depth__lte"));
    }

    #[test]
    fn standard_parser_rejects_related_filters() {
        let error = parse_query_parameter("related.room.class.name=Room").unwrap_err();

        assert!(error.to_string().contains("not supported"));
    }

    #[test]
    fn parser_rejects_related_sort_fields() {
        let error = parse_query_parameter("sort=related.room.object.name").unwrap_err();

        assert!(error.to_string().contains("cannot be used for sorting"));
    }

    #[test]
    fn related_filter_alias_may_contain_double_underscores() {
        let (parsed, _) = parse_query_parameter_with_computed_and_related_filters_and_passthrough(
            "related.room__west.class.name=Room&related.room__west.object.name__contains=foo",
            &[],
        )
        .unwrap();

        assert_eq!(
            parsed.filters[0].field.related_query().unwrap().alias(),
            "room__west"
        );
        assert_eq!(
            parsed.filters[1].operator,
            SearchOperator::Contains { is_negated: false }
        );
    }

    #[test]
    fn common_parser_rejects_more_than_the_parameter_limit() {
        let query = std::iter::repeat_n("local=value", MAX_QUERY_PARAMETERS + 1)
            .collect::<Vec<_>>()
            .join("&");

        let error = parse_query_parameter_with_passthrough(&query, &["local"]).unwrap_err();

        assert_eq!(
            error,
            QueryError::BadRequest(format!(
                "query accepts at most {MAX_QUERY_PARAMETERS} parameters"
            ))
        );
    }

    #[test]
    fn common_parser_accepts_the_parameter_limit() {
        let query = std::iter::repeat_n("local=value", MAX_QUERY_PARAMETERS)
            .collect::<Vec<_>>()
            .join("&");

        let (_, passthrough) = parse_query_parameter_with_passthrough(&query, &["local"]).unwrap();

        assert_eq!(passthrough["local"].len(), MAX_QUERY_PARAMETERS);
    }

    #[test]
    fn common_parser_rejects_more_than_the_filter_limit() {
        let query = std::iter::repeat_n("name__contains=value", MAX_QUERY_FILTERS + 1)
            .collect::<Vec<_>>()
            .join("&");

        let error = parse_query_parameter(&query).unwrap_err();

        assert_eq!(
            error,
            QueryError::BadRequest(format!("query accepts at most {MAX_QUERY_FILTERS} filters"))
        );
    }

    #[test]
    fn common_parser_accepts_the_filter_limit() {
        let query = std::iter::repeat_n("name__contains=value", MAX_QUERY_FILTERS)
            .collect::<Vec<_>>()
            .join("&");

        let parsed = parse_query_parameter(&query).unwrap();

        assert_eq!(parsed.filters.len(), MAX_QUERY_FILTERS);
    }

    #[test]
    fn common_parser_rejects_more_than_the_sort_limit() {
        let fields = [
            "id",
            "name",
            "description",
            "created_at",
            "updated_at",
            "collection_id",
            "class_id",
            "username",
            "proper_name",
        ];
        assert_eq!(fields.len(), MAX_QUERY_SORT_FIELDS + 1);

        let error = parse_query_parameter(&format!("sort={}", fields.join(","))).unwrap_err();

        assert_eq!(
            error,
            QueryError::BadRequest(format!(
                "query accepts at most {MAX_QUERY_SORT_FIELDS} sort fields"
            ))
        );
    }

    #[test]
    fn common_parser_accepts_the_sort_limit() {
        let fields = [
            "id",
            "name",
            "description",
            "created_at",
            "updated_at",
            "collection_id",
            "class_id",
            "username",
        ];
        assert_eq!(fields.len(), MAX_QUERY_SORT_FIELDS);

        let parsed = parse_query_parameter(&format!("sort={}", fields.join(","))).unwrap();

        assert_eq!(parsed.sort.len(), MAX_QUERY_SORT_FIELDS);
    }

    #[test]
    fn common_parser_rejects_duplicate_sort_fields() {
        let error = parse_query_parameter("sort=id.asc,-id").unwrap_err();

        assert_eq!(
            error,
            QueryError::BadRequest("duplicate sort field 'id'".to_string())
        );
    }

    #[test]
    fn parses_revision_filters_and_rejects_invalid_positive_bigints() {
        let parsed = parse_query_parameter(
            "revision__between=10,20&before_revision__gte=3&after_revision__in=4,5&sort=-revision",
        )
        .unwrap();

        assert_eq!(parsed.filters[0].field, FilterField::Revision);
        assert_eq!(parsed.filters[1].field, FilterField::BeforeRevision);
        assert_eq!(parsed.filters[2].field, FilterField::AfterRevision);
        assert_eq!(parsed.sort[0].field, FilterField::Revision);
        assert!(parsed.sort[0].descending);
        assert_eq!(
            parse_positive_bigint_list_with_limit(&i64::MAX.to_string(), 1).unwrap(),
            vec![i64::MAX]
        );
        for invalid in ["0", "-1", "9223372036854775808", "x", "1,"] {
            assert!(parse_positive_bigint_list_with_limit(invalid, 8).is_err());
        }
        assert!(parse_positive_bigint_list_with_limit("1,2,3", 2).is_err());
    }

    #[test]
    fn parses_shared_and_personal_computed_queries() {
        let parsed = parse_query_parameter(
            "sort=computed.shared.display_name.asc,computed.personal.my_rank.desc",
        )
        .unwrap();

        let shared = parsed.sort[0].field.computed_query().unwrap();
        assert_eq!(shared.scope(), ComputedFieldScope::Shared);
        assert_eq!(shared.key(), "display_name");
        assert!(!parsed.sort[0].descending);
        let personal = parsed.sort[1].field.computed_query().unwrap();
        assert_eq!(personal.scope(), ComputedFieldScope::Personal);
        assert_eq!(personal.key(), "my_rank");
        assert!(parsed.sort[1].descending);
    }

    #[test]
    fn computed_query_preserves_a_key_named_asc() {
        let parsed = parse_query_parameter("sort=computed.shared.asc").unwrap();

        let computed = parsed.sort[0].field.computed_query().unwrap();
        assert_eq!(computed.key(), "asc");
        assert!(!parsed.sort[0].descending);
    }

    #[test]
    fn computed_query_preserves_a_key_named_desc() {
        let parsed = parse_query_parameter("sort=computed.shared.desc").unwrap();

        let computed = parsed.sort[0].field.computed_query().unwrap();
        assert_eq!(computed.key(), "desc");
        assert!(!parsed.sort[0].descending);
    }

    #[test]
    fn computed_query_direction_can_be_set_for_direction_named_keys() {
        let parsed = parse_query_parameter(
            "sort=computed.shared.asc.desc,-computed.shared.desc,computed.personal.desc.asc",
        )
        .unwrap();

        assert_eq!(parsed.sort[0].field.computed_query().unwrap().key(), "asc");
        assert!(parsed.sort[0].descending);
        assert_eq!(parsed.sort[1].field.computed_query().unwrap().key(), "desc");
        assert!(parsed.sort[1].descending);
        assert_eq!(parsed.sort[2].field.computed_query().unwrap().key(), "desc");
        assert!(!parsed.sort[2].descending);
    }

    #[test]
    fn accepts_public_and_private_computed_query_aliases() {
        let parsed = parse_query_parameter(
            "sort=computed.public.display_name,computed.private.my_rank.desc",
        )
        .unwrap();

        assert_eq!(
            parsed.sort[0].field.computed_query().unwrap().scope(),
            ComputedFieldScope::Shared
        );
        assert_eq!(
            parsed.sort[1].field.computed_query().unwrap().scope(),
            ComputedFieldScope::Personal
        );
    }

    #[test]
    fn rejects_computed_filters_without_resource_opt_in() {
        let error = parse_query_parameter("computed.shared.display_name=router").unwrap_err();

        assert_eq!(
            error,
            QueryError::BadRequest(
                "Computed fields are not supported in this filter context".to_string()
            )
        );
    }

    #[test]
    fn resource_opt_in_accepts_computed_filters_and_aliases() {
        let (parsed, _) = parse_query_parameter_with_computed_filters_and_passthrough(
            "computed.public.display_name__icontains=edge&computed.private.rank__gte=2",
            &[],
        )
        .unwrap();

        let shared = parsed.filters[0].field.computed_query().unwrap();
        assert_eq!(shared.scope(), ComputedFieldScope::Shared);
        assert_eq!(shared.key(), "display_name");
        assert_eq!(
            parsed.filters[0].operator,
            SearchOperator::IContains { is_negated: false }
        );
        let personal = parsed.filters[1].field.computed_query().unwrap();
        assert_eq!(personal.scope(), ComputedFieldScope::Personal);
        assert_eq!(personal.key(), "rank");
        assert_eq!(
            parsed.filters[1].operator,
            SearchOperator::Gte { is_negated: false }
        );
    }

    #[test]
    fn computed_filter_preserves_double_underscores_in_the_key() {
        let (parsed, _) = parse_query_parameter_with_computed_filters_and_passthrough(
            "computed.shared.display__name=router",
            &[],
        )
        .unwrap();

        let computed = parsed.filters[0].field.computed_query().unwrap();
        assert_eq!(computed.key(), "display__name");
        assert_eq!(
            parsed.filters[0].operator,
            SearchOperator::Equals { is_negated: false }
        );
    }

    #[test]
    fn computed_filter_recognizes_only_a_terminal_operator_suffix() {
        let (parsed, _) = parse_query_parameter_with_computed_filters_and_passthrough(
            "computed.shared.display__name__icontains=edge",
            &[],
        )
        .unwrap();

        let computed = parsed.filters[0].field.computed_query().unwrap();
        assert_eq!(computed.key(), "display__name");
        assert_eq!(
            parsed.filters[0].operator,
            SearchOperator::IContains { is_negated: false }
        );
    }

    #[test]
    fn ordinary_filter_still_rejects_an_unknown_operator() {
        let error = parse_query_parameter("name__unknown=router").unwrap_err();

        assert_eq!(error.to_string(), "Invalid search operator: 'unknown'");
    }

    #[test]
    fn rejects_invalid_computed_query_keys() {
        let error = parse_query_parameter("sort=computed.shared.Invalid-Key").unwrap_err();
        assert_eq!(
            error.to_string(),
            "Invalid computed field key: 'Invalid-Key'"
        );
    }

    #[test]
    fn include_total_defaults_to_true() {
        let parsed = parse_query_parameter("").unwrap();

        assert!(parsed.include_total);
    }

    #[test]
    fn include_total_accepts_false() {
        let parsed = parse_query_parameter("include_total=false").unwrap();

        assert!(!parsed.include_total);
    }

    #[test]
    fn include_total_rejects_duplicates() {
        let error = parse_query_parameter("include_total=true&include_total=false").unwrap_err();

        assert_eq!(
            error,
            QueryError::BadRequest("duplicate include_total".to_string())
        );
    }

    #[test]
    fn include_total_rejects_invalid_boolean() {
        let error = parse_query_parameter("include_total=yes").unwrap_err();

        assert_eq!(
            error,
            QueryError::BadRequest("Invalid boolean value: 'yes'".to_string())
        );
    }

    #[test]
    fn passthrough_preserves_repeated_values() {
        let (parsed, passthrough) =
            parse_query_parameter_with_passthrough("name=router&local=one&local=two", &["local"])
                .unwrap();

        assert_eq!(parsed.filters.len(), 1);
        assert_eq!(passthrough["local"], ["one", "two"]);
    }

    #[test]
    fn decodes_form_encoded_filter_values() {
        let parsed = parse_query_parameter("name=core+router%2Fedge").unwrap();

        assert_eq!(parsed.filters[0].value, "core router/edge");
    }

    #[test]
    fn shared_query_decoder_decodes_keys_and_values() {
        let pairs = decode_query_parameter_pairs("search+term=core+router%2Fedge").unwrap();

        assert_eq!(
            pairs,
            [("search term".to_string(), "core router/edge".to_string())]
        );
    }

    #[test]
    fn shared_query_decoder_accepts_an_empty_query_string() {
        assert_eq!(decode_query_parameter_pairs("").unwrap(), []);
    }

    #[test]
    fn shared_query_decoder_borrows_unchanged_components() {
        let (key, value) = decode_query_parameter_pair("kind=object").unwrap();

        assert!(matches!(key, Cow::Borrowed("kind")));
        assert!(matches!(value, Cow::Borrowed("object")));
    }

    #[test]
    fn parses_integer_ranges() {
        assert_eq!(
            parse_integer_list("1-4,3,8,-4--2").unwrap(),
            vec![-4, -3, -2, 1, 2, 3, 4, 8]
        );
    }

    #[test]
    fn rejects_integer_ranges_above_the_expansion_limit() {
        let error = parse_integer_list("0-2147483647").unwrap_err();

        assert_eq!(
            error,
            QueryError::InvalidIntegerRange(format!(
                "Integer filter '0-2147483647' expands to more than {MAX_INTEGER_FILTER_VALUES} unique values"
            ))
        );
    }

    #[test]
    fn integer_list_limit_counts_unique_values() {
        assert_eq!(
            parse_integer_list_with_limit("1-3,1-3,2", 3).unwrap(),
            vec![1, 2, 3]
        );
    }

    #[test]
    fn rejects_an_integer_list_when_the_unique_value_limit_is_zero() {
        let error = parse_integer_list_with_limit("1", 0).unwrap_err();

        assert_eq!(
            error,
            QueryError::InvalidIntegerRange(
                "Integer filter '1' expands to more than 0 unique values".to_string()
            )
        );
    }

    #[test]
    fn json_path_preserves_valid_nested_segments() {
        let path = JsonFieldPath::new("network,address_v4,$value").unwrap();

        assert_eq!(
            path.segments().collect::<Vec<_>>(),
            ["network", "address_v4", "$value"]
        );
        assert_eq!(path.canonical(), "network,address_v4,$value");
        assert_eq!(path.to_string(), "network,address_v4,$value");
    }

    #[test]
    fn json_path_rejects_empty_segments() {
        let error = JsonFieldPath::new("network,,address").unwrap_err();

        assert_eq!(
            error,
            QueryError::BadRequest(
                "Invalid JSON path 'network,,address'; use non-empty comma-separated ASCII path segments containing only letters, digits, '_', or '$'"
                    .to_string()
            )
        );
    }

    #[test]
    fn infers_jsonb_type_from_value_and_operator() {
        assert_eq!(
            get_jsonb_field_type_from_value_and_operator("2024-01-15", Operator::Equals),
            Some(SQLMappedType::Date)
        );
        assert_eq!(
            get_jsonb_field_type_from_value_and_operator("router", Operator::IContains),
            Some(SQLMappedType::String)
        );
    }
}
