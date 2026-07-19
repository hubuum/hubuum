//! App-neutral query parsing and search parameter types.
//!
//! This crate intentionally does not depend on Actix, Diesel, app models,
//! permissions, pagination config, or Hubuum API errors. The application maps
//! [`QueryError`] into its public error surface at the boundary.

use chrono::{DateTime, NaiveDate};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::str::FromStr;

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
    parse_query_parameter_with_options(qs, passthrough_keys, false)
}

/// Parse query parameters for a resource that explicitly supports filtering
/// on computed fields.
pub fn parse_query_parameter_with_computed_filters_and_passthrough(
    qs: &str,
    passthrough_keys: &[&str],
) -> Result<(QueryOptions, HashMap<String, Vec<String>>), QueryError> {
    parse_query_parameter_with_options(qs, passthrough_keys, true)
}

fn parse_query_parameter_with_options(
    qs: &str,
    passthrough_keys: &[&str],
    allow_computed_filters: bool,
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

    for (key, value) in decode_query_parameter_pairs(qs)? {
        if passthrough_keys.contains(key.as_str()) {
            passthrough.entry(key).or_default().push(value);
            continue;
        }

        match key.as_str() {
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
                cursor = Some(value);
            }
            "include_total" => {
                if include_total.is_some() {
                    return Err(QueryError::BadRequest("duplicate include_total".into()));
                }
                include_total = Some(parse_boolean(&value)?);
            }
            "sort" | "order_by" => {
                for piece in value.split(',') {
                    let descending = piece.starts_with('-') || piece.ends_with(".desc");
                    let field_name = piece
                        .trim_start_matches('-')
                        .trim_end_matches(".asc")
                        .trim_end_matches(".desc");
                    let field = FilterField::from_str(field_name)?;
                    sort.push(SortParam { field, descending });
                }
            }
            _ => filters.push(parse_single_filter(&key, &value, allow_computed_filters)?),
        }
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

fn decode_query_parameter_pairs(qs: &str) -> Result<Vec<(String, String)>, QueryError> {
    let mut pairs = Vec::new();

    for chunk in qs.split('&') {
        let parts: Vec<_> = chunk.splitn(2, '=').collect();
        if parts.len() != 2 {
            return Err(QueryError::BadRequest(format!(
                "Invalid query parameter: '{chunk}'"
            )));
        }

        let key = decode_query_component(parts[0], chunk, "key")?;
        let value = decode_query_component(parts[1], chunk, "value")?;
        pairs.push((key, value));
    }

    Ok(pairs)
}

fn decode_query_component(raw: &str, chunk: &str, component: &str) -> Result<String, QueryError> {
    let decoded = if raw.contains('+') {
        let form_encoded = raw.replace('+', " ");
        percent_encoding::percent_decode(form_encoded.as_bytes())
            .decode_utf8()
            .map(|value| value.into_owned())
    } else {
        percent_encoding::percent_decode(raw.as_bytes())
            .decode_utf8()
            .map(|value| value.into_owned())
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
    allow_computed_filters: bool,
) -> Result<ParsedQueryParam, QueryError> {
    let field_and_op: Vec<&str> = key.splitn(2, "__").collect();

    if value.is_empty() {
        return Err(QueryError::BadRequest(format!(
            "Invalid query parameter: '{key}', no value",
        )));
    }

    let operator = if field_and_op.len() == 1 {
        SearchOperator::new_from_string("equals")?
    } else {
        SearchOperator::new_from_string(field_and_op[1])?
    };

    let field = FilterField::from_str(field_and_op[0])?;
    if field.computed_sort().is_some() && !allow_computed_filters {
        return Err(QueryError::BadRequest(
            "Computed fields are not supported in this filter context".to_string(),
        ));
    }

    Ok(ParsedQueryParam {
        field,
        operator,
        value: value.to_string(),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatementTimeoutMs(u64);

impl StatementTimeoutMs {
    pub fn new(milliseconds: u64) -> Option<Self> {
        (milliseconds > 0).then_some(Self(milliseconds))
    }

    pub fn as_millis(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryOptions {
    pub filters: Vec<ParsedQueryParam>,
    pub sort: Vec<SortParam>,
    pub limit: Option<usize>,
    pub cursor: Option<String>,
    pub include_total: bool,
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
pub enum ComputedSortValueType {
    String,
    Number,
    Integer,
    Boolean,
    Object,
    Array,
}

impl ComputedSortValueType {
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
pub struct ComputedSortField {
    scope: ComputedFieldScope,
    key: String,
    sql_expression: Option<String>,
    value_type: Option<ComputedSortValueType>,
}

impl ComputedSortField {
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
            sql_expression: None,
            value_type: None,
        })
    }

    pub const fn scope(&self) -> ComputedFieldScope {
        self.scope
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn resolve(&mut self, sql_expression: String, value_type: ComputedSortValueType) {
        self.sql_expression = Some(sql_expression);
        self.value_type = Some(value_type);
    }

    pub fn sql_expression(&self) -> Option<&str> {
        self.sql_expression.as_deref()
    }

    pub const fn value_type(&self) -> Option<ComputedSortValueType> {
        self.value_type
    }
}

impl PartialEq for ComputedSortField {
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
    use serde_json::Value;
    let mut current_schema = schema;

    for key in key.split(',') {
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

macro_rules! filter_fields {
    ($(($variant:ident, $str_rep:expr)),* $(,)?) => {
        #[derive(Debug, PartialEq, Clone)]
        pub enum FilterField {
            $($variant),*,
            Computed(Box<ComputedSortField>),
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
                            return ComputedSortField::unresolved(ComputedFieldScope::Shared, key)
                                .map(Box::new)
                                .map(FilterField::Computed);
                        }
                        if let Some(key) = s
                            .strip_prefix("computed.personal.")
                            .or_else(|| s.strip_prefix("computed.private."))
                        {
                            return ComputedSortField::unresolved(
                                ComputedFieldScope::Personal,
                                key,
                            )
                            .map(Box::new)
                            .map(FilterField::Computed);
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
                }
            }
        }

        impl FilterField {
            pub fn table_field(&self) -> &'static str {
                match self {
                    FilterField::JsonSchema => "json_schema",
                    FilterField::JsonData
                    | FilterField::JsonDataFrom
                    | FilterField::JsonDataTo => "data",
                    FilterField::Computed(_) => {
                        panic!("computed sort fields should not be used as table fields")
                    }
                    _ => panic!("{:?} should not be used as a table field", self),
                }
            }

            pub fn computed_sort(&self) -> Option<&ComputedSortField> {
                match self {
                    FilterField::Computed(field) => Some(field),
                    _ => None,
                }
            }

            pub fn computed_sort_mut(&mut self) -> Option<&mut ComputedSortField> {
                match self {
                    FilterField::Computed(field) => Some(field),
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

pub fn parse_integer_list(input: &str) -> Result<Vec<i32>, QueryError> {
    let mut numbers = Vec::new();

    for segment in input.split(',') {
        if segment.contains("--") {
            let parts: Vec<&str> = segment.split("--").collect();
            if parts.len() != 2 {
                return Err(QueryError::InvalidIntegerRange(format!(
                    "Invalid format: '{segment}'"
                )));
            }
            let start = parts[0].parse::<i32>().map_err(|_| {
                QueryError::InvalidIntegerRange(format!("Invalid start of range: '{}'", parts[0]))
            })?;
            let end = format!("-{}", parts[1]).parse::<i32>().map_err(|_| {
                QueryError::InvalidIntegerRange(format!("Invalid end of range: '{}'", parts[1]))
            })?;
            if start > end {
                return Err(QueryError::InvalidIntegerRange(format!(
                    "Range start is greater than end: '{segment}'"
                )));
            }
            numbers.extend(start..=end);
        } else if let Some(idx) = segment.find('-') {
            if idx == 0 {
                numbers.push(segment.parse::<i32>().map_err(|_| {
                    QueryError::InvalidIntegerRange(format!("Invalid number: '{segment}'"))
                })?);
            } else {
                let (start, end) = segment.split_at(idx);
                let end = &end[1..];
                let start = start.parse::<i32>().map_err(|_| {
                    QueryError::InvalidIntegerRange(format!("Invalid start of range: '{start}'"))
                })?;
                let end = end.parse::<i32>().map_err(|_| {
                    QueryError::InvalidIntegerRange(format!("Invalid end of range: '{end}'"))
                })?;
                if start > end {
                    return Err(QueryError::InvalidIntegerRange(format!(
                        "Range start is greater than end: '{segment}'"
                    )));
                }
                numbers.extend(start..=end);
            }
        } else {
            numbers.push(segment.parse::<i32>().map_err(|_| {
                QueryError::InvalidIntegerRange(format!("Invalid number: '{segment}'"))
            })?);
        }
    }

    numbers.sort_unstable();
    numbers.dedup();
    Ok(numbers)
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
    fn parses_shared_and_personal_computed_sorts() {
        let parsed = parse_query_parameter(
            "sort=computed.shared.display_name.asc,computed.personal.my_rank.desc",
        )
        .unwrap();

        let shared = parsed.sort[0].field.computed_sort().unwrap();
        assert_eq!(shared.scope(), ComputedFieldScope::Shared);
        assert_eq!(shared.key(), "display_name");
        assert!(!parsed.sort[0].descending);
        let personal = parsed.sort[1].field.computed_sort().unwrap();
        assert_eq!(personal.scope(), ComputedFieldScope::Personal);
        assert_eq!(personal.key(), "my_rank");
        assert!(parsed.sort[1].descending);
    }

    #[test]
    fn accepts_public_and_private_computed_sort_aliases() {
        let parsed = parse_query_parameter(
            "sort=computed.public.display_name,computed.private.my_rank.desc",
        )
        .unwrap();

        assert_eq!(
            parsed.sort[0].field.computed_sort().unwrap().scope(),
            ComputedFieldScope::Shared
        );
        assert_eq!(
            parsed.sort[1].field.computed_sort().unwrap().scope(),
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

        let shared = parsed.filters[0].field.computed_sort().unwrap();
        assert_eq!(shared.scope(), ComputedFieldScope::Shared);
        assert_eq!(shared.key(), "display_name");
        assert_eq!(
            parsed.filters[0].operator,
            SearchOperator::IContains { is_negated: false }
        );
        let personal = parsed.filters[1].field.computed_sort().unwrap();
        assert_eq!(personal.scope(), ComputedFieldScope::Personal);
        assert_eq!(personal.key(), "rank");
        assert_eq!(
            parsed.filters[1].operator,
            SearchOperator::Gte { is_negated: false }
        );
    }

    #[test]
    fn rejects_invalid_computed_sort_keys() {
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
    fn parses_integer_ranges() {
        assert_eq!(
            parse_integer_list("1-4,3,8,-4--2").unwrap(),
            vec![-4, -3, -2, 1, 2, 3, 4, 8]
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
