use super::{
    FilterField, MAX_QUERY_FILTERS, MAX_QUERY_PARAMETERS, MAX_QUERY_SORT_FIELDS, ParsedQueryParam,
    QueryError, QueryOptions, SearchOperator, SortParam, is_computed_field_name,
    parse_boolean_value, validate_related_filter_groups,
};
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    str::FromStr,
};

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
            QueryOptions::from_parsed_parts(filters, sort, limit, cursor, true)?,
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
                include_total = Some(parse_boolean_value(&value)?);
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
        QueryOptions::from_parsed_parts(
            filters,
            sort,
            limit,
            cursor,
            include_total.unwrap_or(true),
        )?,
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

fn parse_single_filter(
    key: &str,
    value: &str,
    features: QueryParserFeatures,
) -> Result<ParsedQueryParam, QueryError> {
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

    if value.is_empty() && !matches!(operator, SearchOperator::Regex { .. }) {
        return Err(QueryError::BadRequest(format!(
            "Invalid query parameter: '{key}', no value",
        )));
    }

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

    ParsedQueryParam::from_parts(field, operator, value)
}
