//! Compile direct object predicates once, before examining any stored rows.

use std::cmp::Ordering;
use std::str::FromStr;

use bigdecimal::BigDecimal;
use chrono::{NaiveDateTime, TimeZone, Utc};
use hubuum_query::{
    FilterField, JsonFieldPath, Operator, ParsedQueryParam, QueryFilters, QueryScalarType,
    infer_query_scalar_type, parse_boolean_value, parse_datetime_list, parse_integer_list,
};
use hubuum_storage_core::{StorageError, StorageObject};
use ipnet::IpNet;
use regex::Regex;
use serde_json::Value;

pub(super) struct ObjectFilters(Vec<ObjectFilter>);

struct ObjectFilter {
    field: FilterField,
    path: Option<JsonFieldPath>,
    operator: Operator,
    negated: bool,
    operand: Operand,
}

enum Operand {
    Text(String),
    Pattern(Regex),
    Numbers(Vec<BigDecimal>),
    Dates(Vec<NaiveDateTime>),
    Boolean(bool),
    Items(Vec<String>),
    Network(IpNet),
    Null,
}

impl ObjectFilters {
    pub(super) fn new(filters: &QueryFilters) -> Result<Self, StorageError> {
        filters
            .iter()
            .filter(|filter| {
                filter.field.computed_query().is_none()
                    && filter.field.related_query().is_none()
                    && filter.field != FilterField::Permissions
            })
            .map(ObjectFilter::new)
            .collect::<Result<_, _>>()
            .map(Self)
    }

    pub(super) fn matches(&self, object: &StorageObject) -> bool {
        self.0.iter().all(|filter| filter.matches(object))
    }
}

impl ObjectFilter {
    fn new(filter: &ParsedQueryParam) -> Result<Self, StorageError> {
        let (mut operator, mut negated) = filter.operator.op_and_neg();
        if operator == Operator::IsNull && filter.field != FilterField::JsonData {
            negated ^= !parse_boolean_value(&filter.value).map_err(invalid)?;
        }
        let (path, value) = if filter.field == FilterField::JsonData {
            let (path, value) = if operator == Operator::IsNull {
                (filter.value.as_str(), "")
            } else {
                filter.value.split_once('=').ok_or_else(|| {
                    StorageError::invalid_input("Expected exactly two parts of key=value")
                })?
            };
            (Some(JsonFieldPath::new(path).map_err(invalid)?), value)
        } else {
            (None, filter.value.as_str())
        };
        let scalar = match filter.field {
            FilterField::JsonData => infer_query_scalar_type(value, operator.clone()),
            FilterField::Id
            | FilterField::ClassId
            | FilterField::Classes
            | FilterField::CollectionId
            | FilterField::Collections
            | FilterField::Revision => Some(QueryScalarType::Numeric),
            FilterField::CreatedAt | FilterField::UpdatedAt => Some(QueryScalarType::Date),
            FilterField::Name | FilterField::Description => Some(QueryScalarType::String),
            _ => {
                return Err(StorageError::invalid_input(
                    "Invalid direct object filter field",
                ));
            }
        };
        let operand = match operator {
            Operator::IsNull => Operand::Null,
            Operator::HasKey => Operand::Text(value.to_string()),
            Operator::ArrayLength => Operand::Numbers(vec![BigDecimal::from(
                value.parse::<i32>().map_err(invalid)?,
            )]),
            Operator::In if filter.field == FilterField::Revision => Operand::Numbers(
                hubuum_query::parse_positive_bigint_list_with_limit(
                    value,
                    hubuum_query::MAX_INTEGER_FILTER_VALUES,
                )
                .map_err(invalid)?
                .into_iter()
                .map(BigDecimal::from)
                .collect(),
            ),
            Operator::In if scalar == Some(QueryScalarType::Numeric) => Operand::Numbers(
                parse_integer_list(value)
                    .map_err(invalid)?
                    .into_iter()
                    .map(BigDecimal::from)
                    .collect(),
            ),
            Operator::In if scalar == Some(QueryScalarType::Date) => {
                Operand::Dates(parse_datetime_list(value).map_err(invalid)?)
            }
            Operator::In | Operator::All => {
                Operand::Items(value.split(',').map(str::to_string).collect())
            }
            _ if operator.is_ip_operator() => Operand::Network(
                parse_network(value)
                    .ok_or_else(|| StorageError::invalid_input("Invalid IP/CIDR"))?,
            ),
            _ => match scalar {
                Some(QueryScalarType::Numeric) => {
                    let numbers = if filter.field == FilterField::Revision {
                        hubuum_query::parse_positive_bigint_list_with_limit(
                            value,
                            hubuum_query::MAX_INTEGER_FILTER_VALUES,
                        )
                        .map_err(invalid)?
                        .into_iter()
                        .map(BigDecimal::from)
                        .collect()
                    } else {
                        parse_integer_list(value)
                            .map_err(invalid)?
                            .into_iter()
                            .map(BigDecimal::from)
                            .collect()
                    };
                    Operand::Numbers(numbers)
                }
                Some(QueryScalarType::Date) => {
                    Operand::Dates(parse_datetime_list(value).map_err(invalid)?)
                }
                Some(QueryScalarType::Boolean) => {
                    Operand::Boolean(parse_boolean_value(value).map_err(invalid)?)
                }
                Some(QueryScalarType::String | QueryScalarType::None) => match operator {
                    Operator::Equals => Operand::Text(value.to_string()),
                    Operator::Regex => Operand::Pattern(Regex::new(value).map_err(invalid)?),
                    Operator::IEquals
                    | Operator::Contains
                    | Operator::IContains
                    | Operator::StartsWith
                    | Operator::IStartsWith
                    | Operator::EndsWith
                    | Operator::IEndsWith
                    | Operator::Like => Operand::Pattern(like_pattern(value, &operator)?),
                    _ => {
                        return Err(StorageError::invalid_input(
                            "Invalid operator for text filter",
                        ));
                    }
                },
                None => {
                    return Err(StorageError::invalid_input(
                        "Invalid JSON type/operator combination",
                    ));
                }
            },
        };
        if path.is_none()
            && filter.field != FilterField::Revision
            && operator == Operator::Equals
            && matches!(operand, Operand::Numbers(_) | Operand::Dates(_))
        {
            operator = Operator::In;
        }
        if matches!(
            operand,
            Operand::Numbers(_) | Operand::Dates(_) | Operand::Boolean(_)
        ) && operator != Operator::ArrayLength
            && operator != Operator::In
        {
            if !matches!(
                operator,
                Operator::Equals
                    | Operator::Gt
                    | Operator::Gte
                    | Operator::Lt
                    | Operator::Lte
                    | Operator::Between
            ) {
                return Err(StorageError::invalid_input(
                    "Invalid operator for typed filter",
                ));
            }
            let length = match &operand {
                Operand::Numbers(values) => values.len(),
                Operand::Dates(values) => values.len(),
                _ => 1,
            };
            if (operator == Operator::Between && length != 2)
                || ((path.is_some() || filter.field == FilterField::Revision)
                    && operator != Operator::Between
                    && length != 1)
            {
                return Err(StorageError::invalid_input(
                    "Invalid number of typed filter operands",
                ));
            }
        }
        Ok(Self {
            field: filter.field.clone(),
            path,
            operator,
            negated,
            operand,
        })
    }

    fn matches(&self, object: &StorageObject) -> bool {
        let value = match &self.path {
            Some(path) => path
                .segments()
                .try_fold(object.data(), |value, segment| match value {
                    Value::Array(values) => segment
                        .parse::<usize>()
                        .ok()
                        .and_then(|index| values.get(index)),
                    _ => value.get(segment),
                })
                .cloned(),
            None => Some(match self.field {
                FilterField::Id => Value::from(object.id().id()),
                FilterField::ClassId | FilterField::Classes => Value::from(object.class_id().id()),
                FilterField::CollectionId | FilterField::Collections => {
                    Value::from(object.collection_id().id())
                }
                FilterField::Revision => Value::from(object.revision().get()),
                FilterField::Name => Value::from(object.name()),
                FilterField::Description => Value::from(object.description()),
                FilterField::CreatedAt => Value::from(object.created_at().to_rfc3339()),
                FilterField::UpdatedAt => Value::from(object.updated_at().to_rfc3339()),
                _ => return false,
            }),
        };
        let text = value.as_ref().and_then(json_text);
        let matched = match &self.operand {
            Operand::Null => Some(text.is_none()),
            Operand::Text(expected) if self.operator == Operator::HasKey => {
                value.as_ref().map(|value| {
                    value
                        .as_object()
                        .is_some_and(|object| object.contains_key(expected))
                        || value.as_array().is_some_and(|array| {
                            array.iter().any(|value| value.as_str() == Some(expected))
                        })
                        || value.as_str() == Some(expected)
                })
            }
            Operand::Text(expected) => text.as_ref().map(|actual| actual == expected),
            Operand::Pattern(pattern) => text.as_ref().map(|actual| pattern.is_match(actual)),
            Operand::Numbers(expected) if self.operator == Operator::ArrayLength => {
                let Some(array) = value.as_ref().and_then(Value::as_array) else {
                    return false;
                };
                Some(expected[0] == array.len() as u64)
            }
            Operand::Numbers(expected) => text
                .as_deref()
                .and_then(|value| BigDecimal::from_str(value.trim()).ok())
                .map(|actual| compare(&actual, expected, &self.operator)),
            Operand::Dates(expected) => text
                .as_deref()
                .and_then(parse_date)
                .map(|actual| compare(&actual, expected, &self.operator)),
            Operand::Boolean(expected) => text
                .as_deref()
                .and_then(parse_stored_boolean)
                .map(|actual| compare(&actual, &[*expected], &self.operator)),
            Operand::Network(expected) => {
                text.as_deref()
                    .and_then(parse_network)
                    .map(|actual| match self.operator {
                        Operator::InetEquals => actual == *expected,
                        Operator::WithinNetwork => network_contains(expected, &actual),
                        Operator::ContainsNetwork => network_contains(&actual, expected),
                        Operator::ContainsIp => {
                            actual.prefix_len() < expected.prefix_len()
                                && network_contains(&actual, expected)
                        }
                        Operator::OverlapsNetwork => {
                            network_contains(&actual, expected)
                                || network_contains(expected, &actual)
                        }
                        _ => false,
                    })
            }
            Operand::Items(expected) => {
                let items = value.as_ref().and_then(Value::as_array).map(|array| {
                    array
                        .iter()
                        .filter_map(json_text)
                        .collect::<std::collections::BTreeSet<_>>()
                });
                if self.operator == Operator::All {
                    value.as_ref().map(|_| {
                        items.is_some_and(|items| {
                            items.iter().filter(|item| expected.contains(item)).count()
                                == expected.len()
                        })
                    })
                } else if text.as_ref().is_some_and(|text| expected.contains(text))
                    || items.is_some_and(|items| items.iter().any(|item| expected.contains(item)))
                {
                    Some(true)
                } else {
                    text.as_ref().map(|_| false)
                }
            }
        };
        // SQL NULL and failed try_* conversions never match, including negation.
        matched.is_some_and(|matched| matched != self.negated)
    }
}

fn compare<T: Ord>(actual: &T, expected: &[T], operator: &Operator) -> bool {
    if *operator == Operator::In {
        return expected.contains(actual);
    }
    match operator {
        Operator::Equals => actual == &expected[0],
        Operator::Gt => expected.iter().max().is_some_and(|value| actual > value),
        Operator::Gte => expected.iter().max().is_some_and(|value| actual >= value),
        Operator::Lt => expected.iter().min().is_some_and(|value| actual < value),
        Operator::Lte => expected.iter().min().is_some_and(|value| actual <= value),
        Operator::Between => actual >= &expected[0] && actual <= &expected[1],
        _ => false,
    }
}

fn json_text(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(value) => Some(value.clone()),
        _ => Some(value.to_string()),
    }
}

fn parse_date(value: &str) -> Option<NaiveDateTime> {
    parse_datetime_list(value)
        .ok()
        .and_then(|values| values.first().copied())
        .or_else(|| {
            NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")
                .ok()
                .map(|value| Utc.from_utc_datetime(&value).naive_utc())
        })
}

fn parse_stored_boolean(value: &str) -> Option<bool> {
    let value = value.trim().to_ascii_lowercase();
    if value == "1" {
        return Some(true);
    }
    if value == "0" {
        return Some(false);
    }
    if value.is_empty() {
        return None;
    }
    let yes = ["true", "yes", "on"]
        .iter()
        .any(|word| word.starts_with(&value));
    let no = ["false", "no", "off"]
        .iter()
        .any(|word| word.starts_with(&value));
    match (yes, no) {
        (true, false) => Some(true),
        (false, true) => Some(false),
        _ => None,
    }
}

fn parse_network(value: &str) -> Option<IpNet> {
    value
        .parse()
        .ok()
        .or_else(|| value.parse::<std::net::IpAddr>().ok().map(IpNet::from))
}

fn network_contains(outer: &IpNet, inner: &IpNet) -> bool {
    outer.prefix_len().cmp(&inner.prefix_len()) != Ordering::Greater
        && outer.contains(&inner.network())
}

fn like_pattern(value: &str, operator: &Operator) -> Result<Regex, StorageError> {
    let insensitive = matches!(
        operator,
        Operator::IEquals | Operator::IContains | Operator::IStartsWith | Operator::IEndsWith
    );
    let mut pattern = String::from(if insensitive { "(?is)\\A" } else { "(?s)\\A" });
    if matches!(
        operator,
        Operator::Contains
            | Operator::IContains
            | Operator::Like
            | Operator::EndsWith
            | Operator::IEndsWith
    ) {
        pattern.push_str(".*");
    }
    let mut chars = value.chars();
    while let Some(character) = chars.next() {
        match character {
            '%' => pattern.push_str(".*"),
            '_' => pattern.push('.'),
            '\\' => {
                let escaped = chars.next().ok_or_else(|| {
                    StorageError::invalid_input("LIKE pattern ends with an escape")
                })?;
                pattern.push_str(&regex::escape(&escaped.to_string()));
            }
            _ => pattern.push_str(&regex::escape(&character.to_string())),
        }
    }
    if matches!(
        operator,
        Operator::Contains
            | Operator::IContains
            | Operator::Like
            | Operator::StartsWith
            | Operator::IStartsWith
    ) {
        pattern.push_str(".*");
    }
    pattern.push_str("\\z");
    Regex::new(&pattern).map_err(invalid)
}

fn invalid(error: impl std::fmt::Display) -> StorageError {
    StorageError::invalid_input(error.to_string())
}
