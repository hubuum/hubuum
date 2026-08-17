use chrono::NaiveDateTime;
use std::collections::HashMap;
#[cfg(test)]
use std::str::FromStr;

pub use hubuum_query::{
    ComputedFieldScope, ComputedQueryValueType, DEFAULT_RELATED_FILTER_DEPTH, DataType,
    FilterField, MAX_RELATED_FILTER_DEPTH, MAX_RELATED_FILTER_GROUPS, Operator, ParsedQueryParam,
    QueryFilters, QueryOptions, RelatedClassField, RelatedFilterTarget, RelatedObjectField,
    RelatedQueryField, SearchOperator, SortParam, decode_query_parameter_pairs,
};
#[cfg(test)]
use hubuum_query::{
    QueryScalarType, infer_query_scalar_type, infer_query_scalar_type_from_schema,
    infer_scalar_type_from_value,
};

use crate::errors::ApiError;
use crate::models::permissions::{Permissions, PermissionsList};
use crate::pagination::validate_page_limit;
use crate::traits::SelfAccessors;
use crate::utilities::extensions::CustomStringExtensions;

#[cfg(test)]
use crate::storage::postgres::operations::search::{
    ParsedQueryParamSqlExt, SQLComponent, SQLValue, json_column,
};

use super::{HubuumClass, HubuumClassID};

/// ## Parse a query string into search parameters
///
/// ## Arguments
///
/// * `query_string` - A string that contains the query parameters
///
/// ## Returns
///
pub fn parse_query_parameter(qs: &str) -> Result<QueryOptions, ApiError> {
    let (query_options, _) = parse_query_parameter_with_passthrough(qs, &[])?;
    Ok(query_options)
}

pub fn parse_query_parameter_with_passthrough(
    qs: &str,
    passthrough_keys: &[&str],
) -> Result<(QueryOptions, HashMap<String, Vec<String>>), ApiError> {
    let (mut query_options, passthrough) =
        hubuum_query::parse_query_parameter_with_passthrough(qs, passthrough_keys)?;
    let limit = query_options.limit().map(validate_page_limit).transpose()?;
    query_options.set_limit(limit);
    Ok((query_options, passthrough))
}

pub fn parse_query_parameter_with_computed_filters_and_passthrough(
    qs: &str,
    passthrough_keys: &[&str],
) -> Result<(QueryOptions, HashMap<String, Vec<String>>), ApiError> {
    let (mut query_options, passthrough) =
        hubuum_query::parse_query_parameter_with_computed_filters_and_passthrough(
            qs,
            passthrough_keys,
        )?;
    let limit = query_options.limit().map(validate_page_limit).transpose()?;
    query_options.set_limit(limit);
    Ok((query_options, passthrough))
}

pub fn parse_query_parameter_with_computed_and_related_filters_and_passthrough(
    qs: &str,
    passthrough_keys: &[&str],
) -> Result<(QueryOptions, HashMap<String, Vec<String>>), ApiError> {
    let (mut query_options, passthrough) =
        hubuum_query::parse_query_parameter_with_computed_and_related_filters_and_passthrough(
            qs,
            passthrough_keys,
        )?;
    let limit = query_options.limit().map(validate_page_limit).transpose()?;
    query_options.set_limit(limit);
    Ok((query_options, passthrough))
}

impl From<hubuum_query::QueryError> for ApiError {
    fn from(error: hubuum_query::QueryError) -> Self {
        match error {
            hubuum_query::QueryError::BadRequest(message) => ApiError::BadRequest(message),
            hubuum_query::QueryError::InvalidIntegerRange(message) => {
                ApiError::InvalidIntegerRange(message)
            }
        }
    }
}

pub trait QueryOptionsExt {
    /// ## Ensure that a filter is present in the query options
    ///
    /// This function checks if a filter with the given field and identifier is
    /// already present in the filters list. If not, it adds a new filter with
    /// the given field and identifier.
    ///
    /// ### Arguments
    ///
    /// * `field` - The field to check for
    /// * `operator` - The operator to check for
    /// * `identifier` - The identifier to add if the filter is not present
    ///
    /// ### Returns
    ///
    /// * None
    fn ensure_filter<I, T>(
        &mut self,
        field: FilterField,
        operator: SearchOperator,
        identifier: &I,
    ) -> Result<bool, ApiError>
    where
        I: SelfAccessors<T>;

    fn ensure_filter_exact(
        &mut self,
        field: FilterField,
        identifier: &HubuumClassID,
    ) -> Result<bool, ApiError>;
}

impl QueryOptionsExt for QueryOptions {
    fn ensure_filter<I, T>(
        &mut self,
        field: FilterField,
        operator: SearchOperator,
        identifier: &I,
    ) -> Result<bool, ApiError>
    where
        I: SelfAccessors<T>,
    {
        let id_string = identifier.id().to_string();
        self.filters_mut()
            .ensure_filter(field, operator, &id_string)
    }

    /// ## Ensure that an equality filter is present in the query options
    ///
    /// This function checks if an equality filter with the given field and identifier is already
    /// present in the filters list. If not, it adds a new equality filter with the given field and identifier.
    ///
    /// ### Arguments
    ///
    /// * `field` - The field to check for
    /// * `identifier` - The identifier to add if the filter is not present
    ///
    /// ### Returns
    ///
    /// * bool - true if the filter was added, false if it already existed
    fn ensure_filter_exact(
        &mut self,
        field: FilterField,
        identifier: &HubuumClassID,
    ) -> Result<bool, ApiError> {
        self.ensure_filter::<_, HubuumClass>(
            field,
            SearchOperator::Equals { is_negated: false },
            identifier,
        )
    }
}

pub trait ParsedQueryParamExt {
    /// ## Coerce the value into a Permissions enum
    ///
    /// ### Returns
    ///
    /// * A Permissions enum or ApiError::BadRequest if the value is invalid
    fn value_as_permission(&self) -> Result<Permissions, ApiError>;

    /// ## Coerce the value into a list of integers
    ///
    /// Accepts the format given to the [`as_integer`] trait.
    ///
    /// ### Returns
    ///
    /// * A vector of integers or ApiError::BadRequest if the value is invalid
    fn value_as_integer(&self) -> Result<Vec<i32>, ApiError>;

    /// Coerce a bounded list of positive BIGINT resource revisions.
    fn value_as_revision(&self) -> Result<Vec<i64>, ApiError>;

    /// ## Coerce the value into a list of dates
    ///
    /// Accepts a comma separated list of RFC3339 dates.
    /// https://www.rfc-editor.org/rfc/rfc3339
    ///     
    /// ### Returns
    ///
    /// * A vector of NaiveDateTime or ApiError::BadRequest if the value is invalid
    fn value_as_date(&self) -> Result<Vec<NaiveDateTime>, ApiError>;

    /// ## Coerce the value into a boolean
    ///
    /// Accepted values are "true" and "false" (case insensitive)
    ///
    /// ### Returns
    ///
    /// * A boolean or ApiError::BadRequest if the value is invalid
    fn value_as_boolean(&self) -> Result<bool, ApiError>;
}

impl ParsedQueryParamExt for ParsedQueryParam {
    fn value_as_permission(&self) -> Result<Permissions, ApiError> {
        self.value.as_permission()
    }

    fn value_as_integer(&self) -> Result<Vec<i32>, ApiError> {
        self.value.as_integer()
    }

    fn value_as_revision(&self) -> Result<Vec<i64>, ApiError> {
        hubuum_query::parse_positive_bigint_list_with_limit(&self.value, 50).map_err(Into::into)
    }

    fn value_as_date(&self) -> Result<Vec<NaiveDateTime>, ApiError> {
        self.value.as_date()
    }

    fn value_as_boolean(&self) -> Result<bool, ApiError> {
        self.value.as_boolean()
    }
}

pub trait QueryParamsExt {
    /// ## Get a list of permissions from a list of parsed query parameters
    ///
    /// Iterate over the parsed query parameters and filter out the ones that are permissions,
    /// defined as having the `field` set as "permissions". For each value of each parsed query
    /// parameter, attempt to parse it into a Permissions enum. If the value is not a valid
    /// permission, return an ApiError::BadRequest.
    ///
    /// ### Returns    
    ///
    /// * A PermissionsList of Permissions or ApiError::BadRequest if the permissions are invalid
    fn permissions(&self) -> Result<PermissionsList, ApiError>;

    /// ## Get a list of all JSON Schema elements in a list of parsed query parameters
    ///
    /// Iterate over the parsed query parameters and filter out the ones that are JSON Schemas,
    /// defined as having the `field` set as "json_schema". Also validates both keys and values
    /// and their matching to the operator.
    fn json_schemas(&self) -> Result<Vec<&ParsedQueryParam>, ApiError>;

    /// ## Get a list of all JSON Data elements in a list of parsed query parameters
    ///
    /// Iterate over the parsed query parameters and filter out the ones that are JSON Data,
    /// defined as having the `field` set as "json_data". Also validates both keys and values
    /// and their matching to the operator.
    fn json_datas(&self, filter: FilterField) -> Result<Vec<&ParsedQueryParam>, ApiError>;

    /// ## Add a filter to the query options
    ///
    /// Blindly add a filter to the query params. This may lead to duplicate filters.
    ///
    /// ### Arguments
    ///
    /// * `field` - The field to add
    /// * `operator` - The operator to add
    ///
    /// ### Returns
    ///
    /// * None
    fn add_filter(
        &mut self,
        field: FilterField,
        operator: SearchOperator,
        value: &str,
    ) -> Result<(), ApiError>;

    /// ## Ensure a filter is present in the query options
    ///
    /// This function checks if a filter with the given field and operator exists in the list of
    /// parsed query parameters. If not, it adds a new filter with the given field and operator.
    ///
    /// ### Arguments
    ///
    /// * `field` - The field to check for
    /// * `operator` - The operator to check for
    /// * `value` - The value to check for
    ///
    /// ### Returns
    ///
    /// * true if the filter was added, false if it already exists
    fn ensure_filter(
        &mut self,
        field: FilterField,
        operator: SearchOperator,
        value: &str,
    ) -> Result<bool, ApiError>;

    /// ## Check if a filter exists
    ///
    /// This function checks if a filter with the given field and operator exists in the list of
    /// parsed query parameters.
    ///
    /// ### Arguments
    ///
    /// * `field` - The field to check for
    /// * `operator` - The operator to check for
    ///
    /// ### Returns
    ///
    /// * true if the filter exists, false if it does not
    fn filter_exists(&self, field: FilterField, operator: SearchOperator) -> bool;
}

impl QueryParamsExt for Vec<ParsedQueryParam> {
    /// ## Get a list of all Permissions in a list of parsed query parameters
    ///
    /// Iterate over the parsed query parameters and filter out the ones that are permissions,
    /// defined as having the `field` set as "permissions". For each value of a matching parsed query
    /// parameter, attempt to parse it into a Permissions enum.
    ///
    /// Note that the list is not sorted and duplicates are removed.
    ///
    /// If any value is not a valid permission, return an ApiError::BadRequest.
    fn permissions(&self) -> Result<PermissionsList, ApiError> {
        self.iter()
            .filter(|param| param.is_permission())
            .map(ParsedQueryParam::value_as_permission)
            .collect::<Result<Vec<_>, _>>()
            .map(PermissionsList::new)
    }
    /// ## Get a list of all JSON schema entries in a list of parsed query parameters
    ///
    /// Iterate over the parsed query parameters and filter out the ones that are JSON Schemas,
    /// defined as having the `field` set as "json_schema".
    fn json_schemas(&self) -> Result<Vec<&ParsedQueryParam>, ApiError> {
        let json_schema: Vec<&ParsedQueryParam> =
            self.iter().filter(|p| p.is_json_schema()).collect();

        Ok(json_schema)
    }

    /// ## Get a list of all JSON data entries in a list of parsed query parameters
    ///
    /// Iterate over the parsed query parameters and filter out the ones that are JSON Schemas,
    /// defined as having the `field` set as "json_data".
    fn json_datas(&self, field: FilterField) -> Result<Vec<&ParsedQueryParam>, ApiError> {
        let json_schema: Vec<&ParsedQueryParam> = self
            .iter()
            .filter(|p| p.is_json_data() && p.field == field)
            .collect();

        Ok(json_schema)
    }

    fn add_filter(
        &mut self,
        field: FilterField,
        operator: SearchOperator,
        value: &str,
    ) -> Result<(), ApiError> {
        self.push(ParsedQueryParam {
            field,
            operator,
            value: value.to_string(),
        });
        Ok(())
    }

    fn filter_exists(&self, field: FilterField, operator: SearchOperator) -> bool {
        self.iter()
            .any(|p| p.field == field && p.operator == operator)
    }

    fn ensure_filter(
        &mut self,
        field: FilterField,
        operator: SearchOperator,
        value: &str,
    ) -> Result<bool, ApiError> {
        if !self.filter_exists(field.clone(), operator.clone()) {
            self.add_filter(field, operator, value)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

impl QueryParamsExt for QueryFilters {
    fn permissions(&self) -> Result<PermissionsList, ApiError> {
        self.iter()
            .filter(|param| param.is_permission())
            .map(ParsedQueryParam::value_as_permission)
            .collect::<Result<Vec<_>, _>>()
            .map(PermissionsList::new)
    }

    fn json_schemas(&self) -> Result<Vec<&ParsedQueryParam>, ApiError> {
        Ok(self
            .iter()
            .filter(|parameter| parameter.is_json_schema())
            .collect())
    }

    fn json_datas(&self, field: FilterField) -> Result<Vec<&ParsedQueryParam>, ApiError> {
        Ok(self
            .iter()
            .filter(|parameter| parameter.is_json_data() && parameter.field == field)
            .collect())
    }

    fn add_filter(
        &mut self,
        field: FilterField,
        operator: SearchOperator,
        value: &str,
    ) -> Result<(), ApiError> {
        self.try_push(ParsedQueryParam {
            field,
            operator,
            value: value.to_string(),
        })?;
        Ok(())
    }

    fn filter_exists(&self, field: FilterField, operator: SearchOperator) -> bool {
        self.iter()
            .any(|parameter| parameter.field == field && parameter.operator == operator)
    }

    fn ensure_filter(
        &mut self,
        field: FilterField,
        operator: SearchOperator,
        value: &str,
    ) -> Result<bool, ApiError> {
        if self.filter_exists(field.clone(), operator.clone()) {
            return Ok(false);
        }
        self.add_filter(field, operator, value)?;
        Ok(true)
    }
}

#[cfg(test)]
mod test {
    use std::vec;

    use rstest::rstest;

    use super::*;

    fn pq(field: &str, operator: SearchOperator, value: &str) -> ParsedQueryParam {
        ParsedQueryParam {
            field: FilterField::from_str(field).unwrap(),
            operator,
            value: value.to_string(),
        }
    }

    #[test]
    fn test_empty_query_string_returns_empty_vec() {
        let result = parse_query_parameter("").unwrap();
        assert!(result.filters().is_empty());
    }

    #[test]
    fn test_query_string_without_equal_sign_returns_error() {
        let result = parse_query_parameter("name");
        assert!(result.is_err());
    }

    #[rstest]
    #[case("1", vec![1])]
    #[case("2,4", vec![2, 4])]
    #[case("3,3,3,6", vec![3, 6])]
    #[case("4,1,4,1,5", vec![1, 4, 5])]
    #[case("1-4", vec![1, 2, 3, 4])]
    #[case("2-4", vec![2, 3, 4])]
    #[case("3-4", vec![3, 4])]
    #[case("4-4", vec![4])]
    #[case("1,2,3,4", vec![1, 2, 3, 4])]
    #[case("1-4,6-8", vec![1, 2, 3, 4, 6, 7, 8])]
    #[case("1,2,3-5,7", vec![1, 2, 3, 4, 5, 7])]
    #[case("1-4,3,3,8", vec![1, 2, 3, 4, 8])]
    #[case("-4--2", vec![-4, -3, -2])]
    #[case("-90", vec![-90])]
    fn test_parse_integer_list(#[case] input: &str, #[case] expected: Vec<i32>) {
        assert_eq!(input.as_integer(), Ok(expected));
    }

    #[rstest]
    #[case("1-")]
    #[case("-4--6")]
    #[case("1-2-3")]
    fn test_parse_integer_list_failures(#[case] input: &str) {
        assert!(input.as_integer().is_err());
    }

    #[test]
    fn oversized_integer_range_maps_to_the_public_validation_error() {
        let error = "0-2147483647".as_integer().unwrap_err();

        assert!(matches!(error, ApiError::InvalidIntegerRange(_)));
        assert!(error.to_string().contains("more than 1024 unique values"));
    }

    #[rstest]
    #[case(
        "name__icontains=foo&description=bar&invalid",
        "Invalid query parameter: 'invalid'"
    )]
    #[case(
        "name__icontains=foo&description=bar&invalid=",
        "Invalid query parameter: 'invalid', no value"
    )]
    #[case(
        "name__icontains=foo&description=bar&invalid=foo&name__invalid=bar",
        "Invalid search field: 'invalid'"
    )]
    fn test_query_string_bad_request(#[case] query: &str, #[case] expected: &str) {
        assert_eq!(
            parse_query_parameter(query),
            Err(ApiError::BadRequest(expected.to_string()))
        );
    }

    #[rstest]
    #[case(
        "name__icontains=foo&description=bar",
        vec![
            pq("name", SearchOperator::IContains { is_negated: false }, "foo"),
            pq("description", SearchOperator::Equals { is_negated: false }, "bar"),
        ]
    )]
    #[case(
        "name__contains=foo&description__icontains=bar&created_at__gte=2021-01-01&updated_at__lte=2021-12-31",
        vec![
            pq("name", SearchOperator::Contains { is_negated: false }, "foo"),
            pq("description", SearchOperator::IContains { is_negated: false }, "bar"),
            pq("created_at", SearchOperator::Gte { is_negated: false }, "2021-01-01"),
            pq("updated_at", SearchOperator::Lte { is_negated: false }, "2021-12-31"),
        ]
    )]
    #[case(
        "name__not_icontains=foo&description=bar&permissions=CanRead&validate_schema=true",
        vec![
            pq("name", SearchOperator::IContains { is_negated: true }, "foo"),
            pq("description", SearchOperator::Equals { is_negated: false }, "bar"),
            pq("permissions", SearchOperator::Equals { is_negated: false }, "CanRead"),
            pq("validate_schema", SearchOperator::Equals { is_negated: false }, "true"),
        ]
    )]
    #[case(
        "json_data__within_network=network,address=10.0.0.0/24&json_data__contains_ip=network,address=10.0.0.10",
        vec![
            pq("json_data", SearchOperator::WithinNetwork { is_negated: false }, "network,address=10.0.0.0/24"),
            pq("json_data", SearchOperator::ContainsIp { is_negated: false }, "network,address=10.0.0.10"),
        ]
    )]
    fn test_query_string_parsing(#[case] query: &str, #[case] expected: Vec<ParsedQueryParam>) {
        assert_eq!(
            parse_query_parameter(query).unwrap().filters().as_slice(),
            expected
        );
    }

    #[rstest]
    #[case("=value")]
    #[case("network,,address=value")]
    #[case(",network=value")]
    #[case("network,=value")]
    #[case("network address=value")]
    fn json_filter_rejects_malformed_paths(#[case] value: &str) {
        let error = pq(
            "json_data",
            SearchOperator::Equals { is_negated: false },
            value,
        )
        .as_json_sql()
        .unwrap_err();

        assert!(matches!(error, ApiError::BadRequest(_)));
        assert!(error.to_string().contains("JSON path"));
    }

    #[test]
    fn json_is_null_filter_uses_the_shared_path_validation() {
        let error = pq(
            "json_data",
            SearchOperator::IsNull { is_negated: false },
            "network,,address",
        )
        .as_json_sql()
        .unwrap_err();

        assert!(matches!(error, ApiError::BadRequest(_)));
        assert!(error.to_string().contains("JSON path"));
    }

    #[test]
    fn test_json_schema_sql_query_text_generation() {
        let field = "json_schema";
        let test_cases = vec![
            (
                pq(
                    "json_schema",
                    SearchOperator::Equals { is_negated: false },
                    "key=foo",
                ),
                format!("{field} #>> '{{key}}' = ?"),
                SQLValue::String("foo".to_string()),
            ),
            (
                pq(
                    "json_schema",
                    SearchOperator::IEquals { is_negated: true },
                    "key=foo",
                ),
                format!("NOT {field} #>> '{{key}}' ILIKE ?"),
                SQLValue::String("foo".to_string()),
            ),
            (
                pq(
                    "json_schema",
                    SearchOperator::Gt { is_negated: false },
                    "key,subkey=3",
                ),
                format!(
                    "try_numeric({field} #>> '{{key,subkey}}') IS NOT NULL AND try_numeric({field} #>> '{{key,subkey}}') > ?"
                ),
                SQLValue::Integer(3),
            ),
        ];

        for (param, expected, sqlvalue) in test_cases {
            let result = param.as_json_sql();
            assert_eq!(
                result.unwrap(),
                SQLComponent {
                    sql: expected.to_string(),
                    bind_variables: vec![sqlvalue]
                },
                "Failed test case for param: {param:?}",
            );
        }
    }

    #[test]
    fn test_json_schema_sql_query_ip_generation() {
        let field = "json_schema";
        let test_cases = vec![
            (
                pq(
                    "json_schema",
                    SearchOperator::WithinNetwork { is_negated: false },
                    "key,subkey=10.0.0.0/24",
                ),
                format!(
                    "try_inet({field} #>> '{{key,subkey}}') IS NOT NULL AND try_inet({field} #>> '{{key,subkey}}') <<= ?::inet"
                ),
                SQLValue::String("10.0.0.0/24".to_string()),
            ),
            (
                pq(
                    "json_schema",
                    SearchOperator::ContainsIp { is_negated: false },
                    "key=10.0.0.10",
                ),
                format!(
                    "try_inet({field} #>> '{{key}}') IS NOT NULL AND try_inet({field} #>> '{{key}}') >> ?::inet"
                ),
                SQLValue::String("10.0.0.10".to_string()),
            ),
            (
                pq(
                    "json_schema",
                    SearchOperator::InetEquals { is_negated: true },
                    "key=10.0.0.10",
                ),
                format!(
                    "try_inet({field} #>> '{{key}}') IS NOT NULL AND NOT (try_inet({field} #>> '{{key}}') = ?::inet)"
                ),
                SQLValue::String("10.0.0.10/32".to_string()),
            ),
        ];

        for (param, expected, sqlvalue) in test_cases {
            let result = param.as_json_sql();
            assert_eq!(
                result.unwrap(),
                SQLComponent {
                    sql: expected.to_string(),
                    bind_variables: vec![sqlvalue]
                },
                "Failed test case for param: {param:?}",
            );
        }
    }

    #[test]
    fn test_json_schema_sql_query_ip_validation() {
        let test_cases = vec![
            pq(
                "json_schema",
                SearchOperator::WithinNetwork { is_negated: false },
                "key=not-an-ip",
            ),
            pq(
                "json_schema",
                SearchOperator::ContainsIp { is_negated: false },
                "key=10.0.0.0/24",
            ),
        ];

        for param in test_cases {
            let result = param.as_json_sql();
            assert!(
                result.is_err(),
                "Expected bad request for param: {param:?}, got {result:?}",
            );
        }
    }

    #[test]
    fn test_json_schema_sql_query_date_generation() {
        let field = "json_schema";
        let test_cases = vec![
            (
                pq(
                    "json_schema",
                    SearchOperator::Equals { is_negated: false },
                    "key=2021-01-01",
                ),
                format!(
                    "try_timestamp({field} #>> '{{key}}') IS NOT NULL AND try_timestamp({field} #>> '{{key}}') = ?"
                ),
                SQLValue::Date("2021-01-01".as_date().unwrap()[0]),
            ),
            (
                pq(
                    "json_schema",
                    SearchOperator::Gt { is_negated: false },
                    "key,subkey=2021-01-01",
                ),
                format!(
                    "try_timestamp({field} #>> '{{key,subkey}}') IS NOT NULL AND try_timestamp({field} #>> '{{key,subkey}}') > ?"
                ),
                SQLValue::Date("2021-01-01".as_date().unwrap()[0]),
            ),
            (
                pq(
                    "json_schema",
                    SearchOperator::Gt { is_negated: true },
                    "key,subkey=2021-01-01",
                ),
                format!(
                    "try_timestamp({field} #>> '{{key,subkey}}') IS NOT NULL AND NOT (try_timestamp({field} #>> '{{key,subkey}}') > ?)"
                ),
                SQLValue::Date("2021-01-01".as_date().unwrap()[0]),
            ),
            (
                pq(
                    "json_schema",
                    SearchOperator::Between { is_negated: false },
                    "key=2021-01-01,2021-01-31",
                ),
                format!(
                    "try_timestamp({field} #>> '{{key}}') IS NOT NULL AND try_timestamp({field} #>> '{{key}}') BETWEEN ? AND ?"
                ),
                SQLValue::Date("2021-01-01,2021-01-31".as_date().unwrap()[0]),
            ),
        ];

        for (index, (param, expected, sqlvalue)) in test_cases.into_iter().enumerate() {
            let result = param.as_json_sql();
            let expected_bindings = if index == 3 {
                "2021-01-01,2021-01-31"
                    .as_date()
                    .unwrap()
                    .into_iter()
                    .map(SQLValue::Date)
                    .collect::<Vec<_>>()
            } else {
                vec![sqlvalue]
            };
            assert_eq!(
                result.unwrap(),
                SQLComponent {
                    sql: expected.to_string(),
                    bind_variables: expected_bindings
                },
                "Failed test case for param: {param:?}",
            );
        }
    }

    #[test]
    fn test_json_schema_sql_query_numerical_generation() {
        let field = "json_schema";
        let test_cases = vec![
            (
                pq(
                    "json_schema",
                    SearchOperator::Equals { is_negated: false },
                    "key=3",
                ),
                format!(
                    "try_numeric({field} #>> '{{key}}') IS NOT NULL AND try_numeric({field} #>> '{{key}}') = ?"
                ),
                SQLValue::Integer(3),
            ),
            (
                pq(
                    "json_schema",
                    SearchOperator::Gt { is_negated: false },
                    "key,subkey=3",
                ),
                format!(
                    "try_numeric({field} #>> '{{key,subkey}}') IS NOT NULL AND try_numeric({field} #>> '{{key,subkey}}') > ?"
                ),
                SQLValue::Integer(3),
            ),
            (
                pq(
                    "json_schema",
                    SearchOperator::Gt { is_negated: true },
                    "key,subkey=3",
                ),
                format!(
                    "try_numeric({field} #>> '{{key,subkey}}') IS NOT NULL AND NOT (try_numeric({field} #>> '{{key,subkey}}') > ?)"
                ),
                SQLValue::Integer(3),
            ),
            (
                pq(
                    "json_schema",
                    SearchOperator::Between { is_negated: false },
                    "key=3,5",
                ),
                format!(
                    "try_numeric({field} #>> '{{key}}') IS NOT NULL AND try_numeric({field} #>> '{{key}}') BETWEEN ? AND ?"
                ),
                SQLValue::Integer(3),
            ),
        ];

        for (index, (param, expected, sqlvalue)) in test_cases.into_iter().enumerate() {
            let result = param.as_json_sql();
            let expected_bindings = if index == 3 {
                vec![SQLValue::Integer(3), SQLValue::Integer(5)]
            } else {
                vec![sqlvalue]
            };
            assert_eq!(
                result.unwrap(),
                SQLComponent {
                    sql: expected.to_string(),
                    bind_variables: expected_bindings
                },
                "Failed test case for param: {param:?}",
            );
        }
    }

    #[test]
    fn test_json_schema_sql_query_boolean_generation() {
        let field = "json_schema";
        let test_cases = vec![
            (
                pq(
                    "json_schema",
                    SearchOperator::Equals { is_negated: false },
                    "key=true",
                ),
                format!(
                    "try_boolean({field} #>> '{{key}}') IS NOT NULL AND try_boolean({field} #>> '{{key}}') = ?"
                ),
                SQLValue::Boolean(true),
            ),
            (
                pq(
                    "json_schema",
                    SearchOperator::Equals { is_negated: true },
                    "key=false",
                ),
                format!(
                    "try_boolean({field} #>> '{{key}}') IS NOT NULL AND NOT (try_boolean({field} #>> '{{key}}') = ?)"
                ),
                SQLValue::Boolean(false),
            ),
        ];

        for (param, expected, sqlvalue) in test_cases {
            let result = param.as_json_sql();
            assert_eq!(
                result.unwrap(),
                SQLComponent {
                    sql: expected.to_string(),
                    bind_variables: vec![sqlvalue]
                },
                "Failed test case for param: {param:?}",
            );
        }
    }

    #[test]
    fn test_json_schema_sql_generation_wrapping() {
        let field = "json_schema";
        let test_cases = vec![
            (
                pq(
                    "json_schema",
                    SearchOperator::Equals { is_negated: false },
                    "key,subkey=3",
                ),
                format!(
                    "try_numeric({field} #>> '{{key,subkey}}') IS NOT NULL AND try_numeric({field} #>> '{{key,subkey}}') = ?"
                ),
            ),
            (
                pq(
                    "json_schema",
                    SearchOperator::Equals { is_negated: false },
                    "key,subkey,subsubkey=3",
                ),
                format!(
                    "try_numeric({field} #>> '{{key,subkey,subsubkey}}') IS NOT NULL AND try_numeric({field} #>> '{{key,subkey,subsubkey}}') = ?"
                ),
            ),
            (
                pq(
                    "json_schema",
                    SearchOperator::Equals { is_negated: true },
                    "key,subkey,subsubkey,subsubsubkey=3",
                ),
                format!(
                    "try_numeric({field} #>> '{{key,subkey,subsubkey,subsubsubkey}}') IS NOT NULL AND NOT (try_numeric({field} #>> '{{key,subkey,subsubkey,subsubsubkey}}') = ?)"
                ),
            ),
        ];

        for (param, expected) in test_cases {
            let result = param.as_json_sql();
            assert_eq!(
                result.unwrap().sql,
                expected.to_string(),
                "Failed test case for param: {param:?}",
            );
        }
    }

    #[test]
    fn test_json_field_type_from_schema() {
        let schema = serde_json::json!({
            "type": "object",
            "properties": {
                "name": {
                    "type": "string"
                },
                "age": {
                    "type": "number"
                },
                "is_active": {
                    "type": "boolean"
                },
                "date_of_birth": {
                    "type": "string",
                    "format": "date"
                },
                "last_updated": {
                    "type": "string",
                    "format": "date-time"
                },
                "address": {
                    "type": "object",
                    "properties": {
                        "street": {
                            "type": "string"
                        },
                        "city": {
                            "type": "string"
                        },
                        "zip": {
                            "type": "number"
                        }
                    }
                }
            }
        });

        let test_cases = vec![
            ("name", QueryScalarType::String),
            ("age", QueryScalarType::Numeric),
            ("is_active", QueryScalarType::Boolean),
            ("date_of_birth", QueryScalarType::Date),
            ("last_updated", QueryScalarType::Date),
            ("address,street", QueryScalarType::String),
            ("address,city", QueryScalarType::String),
            ("address,zip", QueryScalarType::Numeric),
        ];

        for (key, expected) in test_cases {
            let result = infer_query_scalar_type_from_schema(&schema, key);
            assert_eq!(result, Some(expected), "Failed test case for key: {key}");
        }
    }

    #[test]
    fn test_json_field_type_from_schema_failures() {
        let schema = serde_json::json!({
            "type": "object",
            "properties": {
                "name": {
                    "type": "string"
                },
                "age": {
                    "type": "number"
                },
                "is_active": {
                    "type": "boolean"
                },
                "address": {
                    "type": "object",
                    "properties": {
                        "street": {
                            "type": "string"
                        },
                        "city": {
                            "type": "string"
                        },
                        "zip": {
                            "type": "number"
                        }
                    }
                }
            }
        });

        let test_cases = vec!["invalid", "address,invalid", "address,zip,invalid"];

        for key in test_cases {
            let result = infer_query_scalar_type_from_schema(&schema, key);
            assert_eq!(result, None, "Failed test case for key: {key}");
        }
    }

    #[test]
    fn test_infer_scalar_type_from_value() {
        let test_cases = vec![
            ("foo", QueryScalarType::String),
            ("3", QueryScalarType::Numeric),
            ("3.14", QueryScalarType::Numeric),
            ("2021-01-01", QueryScalarType::Date),
            ("2021-01-01T00:00:00Z", QueryScalarType::Date),
            ("true", QueryScalarType::Boolean),
            ("false", QueryScalarType::Boolean),
            ("null", QueryScalarType::None),
        ];

        for (value, expected) in test_cases {
            let result = infer_scalar_type_from_value(
                value,
                &[
                    QueryScalarType::Date,
                    QueryScalarType::Numeric,
                    QueryScalarType::Boolean,
                    QueryScalarType::None,
                    QueryScalarType::String,
                ],
            );
            assert_eq!(
                result,
                Some(expected),
                "Failed test case for value: '{value}'"
            );
        }
    }

    #[test]
    fn test_infer_scalar_type_from_value_and_operator() {
        let test_cases = vec![
            ("foo", Operator::Equals, Some(QueryScalarType::String)),
            ("3", Operator::Equals, Some(QueryScalarType::Numeric)),
            ("2021-01-01", Operator::Equals, Some(QueryScalarType::Date)),
            ("true", Operator::Equals, Some(QueryScalarType::Boolean)),
            ("FALSe", Operator::Equals, Some(QueryScalarType::Boolean)),
            ("null", Operator::Equals, Some(QueryScalarType::None)),
            ("true", Operator::Equals, Some(QueryScalarType::Boolean)),
            ("2021-01-01", Operator::Gt, Some(QueryScalarType::Date)),
            ("3", Operator::Gt, Some(QueryScalarType::Numeric)),
            (
                "2021-01-01,2021-01-31",
                Operator::Between,
                Some(QueryScalarType::Date),
            ),
            ("3,5", Operator::Between, Some(QueryScalarType::Numeric)),
            ("3", Operator::Contains, Some(QueryScalarType::String)),
            ("null", Operator::Gt, None),
            ("foo", Operator::Gt, None),
        ];

        for (value, operator, expected) in test_cases {
            let result = infer_query_scalar_type(value, operator.clone());
            assert_eq!(
                result, expected,
                "Failed test case for value: '{value}', operator: '{operator:?}'"
            );
        }
    }

    #[test]
    fn test_new_from_string() {
        type SO = SearchOperator;

        let test_cases = vec![
            ("equals", SO::Equals { is_negated: false }),
            ("iequals", SO::IEquals { is_negated: false }),
            ("contains", SO::Contains { is_negated: false }),
            ("icontains", SO::IContains { is_negated: false }),
            ("startswith", SO::StartsWith { is_negated: false }),
            ("istartswith", SO::IStartsWith { is_negated: false }),
            ("endswith", SO::EndsWith { is_negated: false }),
            ("iendswith", SO::IEndsWith { is_negated: false }),
            ("like", SO::Like { is_negated: false }),
            ("regex", SO::Regex { is_negated: false }),
            ("gt", SO::Gt { is_negated: false }),
            ("gte", SO::Gte { is_negated: false }),
            ("lt", SO::Lt { is_negated: false }),
            ("lte", SO::Lte { is_negated: false }),
            ("between", SO::Between { is_negated: false }),
            ("within_network", SO::WithinNetwork { is_negated: false }),
            (
                "contains_network",
                SO::ContainsNetwork { is_negated: false },
            ),
            ("contains_ip", SO::ContainsIp { is_negated: false }),
            (
                "overlaps_network",
                SO::OverlapsNetwork { is_negated: false },
            ),
            ("inet_equals", SO::InetEquals { is_negated: false }),
            ("not_equals", SO::Equals { is_negated: true }),
            ("not_iequals", SO::IEquals { is_negated: true }),
            ("not_contains", SO::Contains { is_negated: true }),
            ("not_icontains", SO::IContains { is_negated: true }),
            ("not_startswith", SO::StartsWith { is_negated: true }),
            ("not_istartswith", SO::IStartsWith { is_negated: true }),
            ("not_endswith", SO::EndsWith { is_negated: true }),
            ("not_iendswith", SO::IEndsWith { is_negated: true }),
            ("not_like", SO::Like { is_negated: true }),
            ("not_regex", SO::Regex { is_negated: true }),
            ("not_gt", SO::Gt { is_negated: true }),
            ("not_gte", SO::Gte { is_negated: true }),
            ("not_lt", SO::Lt { is_negated: true }),
            ("not_lte", SO::Lte { is_negated: true }),
            ("not_within_network", SO::WithinNetwork { is_negated: true }),
            (
                "not_contains_network",
                SO::ContainsNetwork { is_negated: true },
            ),
            ("not_contains_ip", SO::ContainsIp { is_negated: true }),
            (
                "not_overlaps_network",
                SO::OverlapsNetwork { is_negated: true },
            ),
            ("not_inet_equals", SO::InetEquals { is_negated: true }),
        ];

        for (input, expected) in test_cases {
            let result = SO::new_from_string(input);
            assert_eq!(
                result,
                Ok(expected),
                "Failed test case for input: '{input}'",
            );
        }
    }

    #[test]
    fn test_is_applicable_to() {
        type SO = SearchOperator;
        type DT = DataType;

        let test_cases = vec![
            (SO::Equals { is_negated: false }, DT::String, true),
            (SO::Equals { is_negated: false }, DT::NumericOrDate, true),
            (SO::Equals { is_negated: false }, DT::Boolean, true),
            (SO::IEquals { is_negated: false }, DT::String, true),
            (SO::IEquals { is_negated: false }, DT::NumericOrDate, false),
            (SO::IEquals { is_negated: false }, DT::Boolean, false),
            (SO::Contains { is_negated: false }, DT::String, true),
            (SO::Contains { is_negated: false }, DT::NumericOrDate, false),
            (SO::Contains { is_negated: false }, DT::Boolean, false),
            (SO::IContains { is_negated: false }, DT::String, true),
            (
                SO::IContains { is_negated: false },
                DT::NumericOrDate,
                false,
            ),
            (SO::IContains { is_negated: false }, DT::Boolean, false),
            (SO::StartsWith { is_negated: false }, DT::String, true),
            (
                SO::StartsWith { is_negated: false },
                DT::NumericOrDate,
                false,
            ),
            (SO::StartsWith { is_negated: false }, DT::Boolean, false),
            (SO::IStartsWith { is_negated: false }, DT::String, true),
            (
                SO::IStartsWith { is_negated: false },
                DT::NumericOrDate,
                false,
            ),
            (SO::IStartsWith { is_negated: false }, DT::Boolean, false),
            (SO::EndsWith { is_negated: false }, DT::String, true),
            (SO::EndsWith { is_negated: false }, DT::NumericOrDate, false),
            (SO::EndsWith { is_negated: false }, DT::Boolean, false),
            (SO::IEndsWith { is_negated: false }, DT::String, true),
            (
                SO::IEndsWith { is_negated: false },
                DT::NumericOrDate,
                false,
            ),
            (SO::IEndsWith { is_negated: false }, DT::Boolean, false),
            (SO::Like { is_negated: false }, DT::String, true),
            (SO::Like { is_negated: false }, DT::NumericOrDate, false),
            (SO::Like { is_negated: false }, DT::Boolean, false),
            (SO::Regex { is_negated: false }, DT::String, true),
            (SO::Regex { is_negated: false }, DT::NumericOrDate, false),
            (SO::Regex { is_negated: false }, DT::Boolean, false),
            (SO::Gt { is_negated: false }, DT::String, false),
            (SO::Gt { is_negated: false }, DT::NumericOrDate, true),
            (SO::Gt { is_negated: false }, DT::Boolean, false),
            (SO::Gte { is_negated: false }, DT::String, false),
            (SO::Gte { is_negated: false }, DT::NumericOrDate, true),
            (SO::Gte { is_negated: false }, DT::Boolean, false),
            (SO::Lt { is_negated: false }, DT::String, false),
            (SO::Lt { is_negated: false }, DT::NumericOrDate, true),
            (SO::Lt { is_negated: false }, DT::Boolean, false),
            (SO::Lte { is_negated: false }, DT::String, false),
            (SO::Lte { is_negated: false }, DT::NumericOrDate, true),
            (SO::Lte { is_negated: false }, DT::Boolean, false),
            (SO::Between { is_negated: false }, DT::String, false),
            (SO::Between { is_negated: false }, DT::NumericOrDate, true),
            (SO::Between { is_negated: false }, DT::Boolean, false),
        ];

        for (operator, data_type, expected) in test_cases {
            let result = operator.is_applicable_to(data_type);
            assert_eq!(
                result, expected,
                "Failed test case for operator: '{operator:?}', data_type: '{data_type:?}'",
            );
        }
    }

    #[test]
    fn test_parse_query_parameter_with_cursor() {
        let query_options =
            parse_query_parameter("limit=2&sort=id.desc&cursor=test-cursor").unwrap();

        assert_eq!(query_options.limit(), Some(2));
        assert_eq!(query_options.sort().len(), 1);
        assert_eq!(query_options.sort()[0].field, FilterField::Id);
        assert!(query_options.sort()[0].descending);
        assert_eq!(
            query_options.cursor().map(|cursor| cursor.as_str()),
            Some("test-cursor")
        );
    }

    #[test]
    fn parse_query_parameter_preserves_total_count_opt_out() {
        let options = parse_query_parameter("include_total=false").unwrap();

        assert!(!options.include_total());
    }

    #[test]
    fn test_parse_query_parameter_with_passthrough_extracts_endpoint_local_values() {
        let (query_options, passthrough) = parse_query_parameter_with_passthrough(
            "name__contains=alpha&ignore_classes=1,2&ignore_self_class=false&sort=id.asc",
            &["ignore_classes", "ignore_self_class"],
        )
        .unwrap();

        assert_eq!(query_options.filters().len(), 1);
        assert_eq!(query_options.filters()[0].field, FilterField::Name);
        assert_eq!(
            query_options.filters()[0].operator,
            SearchOperator::Contains { is_negated: false }
        );
        assert_eq!(query_options.filters()[0].value, "alpha");
        assert_eq!(query_options.sort().len(), 1);
        assert_eq!(
            passthrough.get("ignore_classes"),
            Some(&vec!["1,2".to_string()])
        );
        assert_eq!(
            passthrough.get("ignore_self_class"),
            Some(&vec!["false".to_string()])
        );
    }

    #[test]
    fn test_parse_query_parameter_with_passthrough_preserves_repeated_local_keys() {
        let (_, passthrough) = parse_query_parameter_with_passthrough(
            "ignore_self_class=true&ignore_self_class=false",
            &["ignore_self_class"],
        )
        .unwrap();

        assert_eq!(
            passthrough.get("ignore_self_class"),
            Some(&vec!["true".to_string(), "false".to_string()])
        );
    }

    #[test]
    fn test_parse_query_parameter_rejects_duplicate_cursor() {
        let error = parse_query_parameter("cursor=one&cursor=two").unwrap_err();
        assert!(matches!(error, ApiError::BadRequest(_)));
        assert_eq!(error.to_string(), "duplicate cursor");
    }

    // Covers docs/querying.md "Sorting" (`order_by` is accepted as an alias).
    #[test]
    fn docs_parse_query_parameter_accepts_order_by_alias() {
        let query_options = parse_query_parameter("order_by=name.desc,id.asc").unwrap();

        assert_eq!(query_options.sort().len(), 2);
        assert_eq!(query_options.sort()[0].field, FilterField::Name);
        assert!(query_options.sort()[0].descending);
        assert_eq!(query_options.sort()[1].field, FilterField::Id);
        assert!(!query_options.sort()[1].descending);
    }

    // Covers docs/querying.md "Query syntax" (`field=value` means `field__equals=value`).
    #[test]
    fn docs_parse_query_parameter_plain_filter_defaults_to_equals() {
        let query_options = parse_query_parameter("name=alpha").unwrap();

        assert_eq!(query_options.filters().len(), 1);
        assert_eq!(query_options.filters()[0].field, FilterField::Name);
        assert_eq!(
            query_options.filters()[0].operator,
            SearchOperator::Equals { is_negated: false }
        );
        assert_eq!(query_options.filters()[0].value, "alpha");
    }

    #[test]
    fn test_parse_query_parameter_decodes_keys_values_and_plus() {
        let query_options = parse_query_parameter("name%5F%5Fcontains=alpha+beta").unwrap();

        assert_eq!(query_options.filters().len(), 1);
        assert_eq!(query_options.filters()[0].field, FilterField::Name);
        assert_eq!(
            query_options.filters()[0].operator,
            SearchOperator::Contains { is_negated: false }
        );
        assert_eq!(query_options.filters()[0].value, "alpha beta");
    }

    // Covers docs/querying.md "Negation" (`not_` works with `between`).
    #[test]
    fn docs_parse_query_parameter_accepts_negated_between_filter() {
        let query_options = parse_query_parameter(
            "created_at__not_between=2026-01-01T00:00:00Z,2026-02-01T00:00:00Z",
        )
        .unwrap();

        assert_eq!(query_options.filters().len(), 1);
        assert_eq!(query_options.filters()[0].field, FilterField::CreatedAt);
        assert_eq!(
            query_options.filters()[0].operator,
            SearchOperator::Between { is_negated: true }
        );
        assert_eq!(
            query_options.filters()[0].value,
            "2026-01-01T00:00:00Z,2026-02-01T00:00:00Z"
        );
    }

    // Covers docs/querying.md "JSON filtering" (`json_data` aliases target object JSON payload data).
    #[test]
    fn docs_json_data_aliases_map_to_object_data_column() {
        assert_eq!(json_column(&FilterField::JsonData), Some("data"));
        assert_eq!(json_column(&FilterField::JsonDataFrom), Some("data"));
        assert_eq!(json_column(&FilterField::JsonDataTo), Some("data"));
    }

    #[test]
    fn non_json_field_cannot_abort_json_sql_generation() {
        let error = pq(
            "name",
            SearchOperator::Equals { is_negated: false },
            "value",
        )
        .as_json_sql()
        .unwrap_err();

        assert!(matches!(error, ApiError::InternalServerError(_)));
        assert_eq!(error.to_string(), "Attempt to filter 'name' as JSON!");
    }

    #[test]
    fn test_parse_query_parameter_rejects_zero_limit() {
        let error = parse_query_parameter("limit=0").unwrap_err();
        assert!(matches!(error, ApiError::BadRequest(_)));
        assert_eq!(error.to_string(), "limit must be greater than 0");
    }

    // Covers docs/querying.md "Cursor pagination" (maximum page size is `250`).
    #[test]
    fn docs_parse_query_parameter_clamps_limit_above_maximum() {
        let query_options = parse_query_parameter("limit=251").unwrap();
        assert_eq!(query_options.limit(), Some(250));
    }
}
