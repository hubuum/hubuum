//! Reusable PostgreSQL filter expansion for backend-neutral query values.

diesel::infix_operator!(RegexMatch, " ~ ", backend: diesel::pg::Pg);

#[doc(hidden)]
pub fn regex_match<T, U, ST>(left: T, right: U) -> RegexMatch<T, U::Expression>
where
    T: diesel::expression::Expression<SqlType = ST>,
    U: diesel::expression::AsExpression<ST>,
    ST: diesel::sql_types::SqlType + diesel::expression::TypedExpressionType,
{
    RegexMatch::new(left, right.as_expression())
}

#[macro_export]
macro_rules! postgres_is_null_filter {
    ($query:ident, $param:expr, $operator:expr, $field:expr) => {{
        let is_null = hubuum_query::parse_boolean_value(&$param.value)
            .map_err(|error| $crate::PostgresStorageError::bad_request(error.to_string()))?;
        let (_, negated) = $operator.op_and_neg();
        if is_null != negated {
            $query = $query.filter($field.is_null());
        } else {
            $query = $query.filter($field.is_not_null());
        }
    }};
}

#[macro_export]
macro_rules! postgres_string_filter {
    ($query:ident, $param:expr, $field:expr) => {{
        use diesel::dsl::not;
        use diesel::{PgTextExpressionMethods, TextExpressionMethods};
        use hubuum_query::{DataType, Operator};

        let operator = $param.operator.clone();
        let (operation, negated) = operator.op_and_neg();
        if operation == Operator::IsNull {
            $crate::postgres_is_null_filter!($query, $param, operator, $field);
        } else {
            if !operator.is_applicable_to(DataType::String) {
                return Err($crate::PostgresStorageError::bad_request(format!(
                    "Operator '{operator:?}' is not applicable to field '{}'",
                    $param.field
                )));
            }
            let value = $param.value.clone();
            if value.is_empty() {
                return Err($crate::PostgresStorageError::bad_request(format!(
                    "Searching on field '{}' requires a value",
                    $param.field
                )));
            }
            match (operation, negated) {
                (Operator::Equals, false) => $query = $query.filter($field.eq(value)),
                (Operator::Equals, true) => $query = $query.filter(not($field.eq(value))),
                (Operator::In, false) => {
                    $query = $query.filter(
                        $field.eq_any(value.split(',').map(str::to_string).collect::<Vec<_>>()),
                    );
                }
                (Operator::In, true) => {
                    $query = $query.filter(not(
                        $field.eq_any(value.split(',').map(str::to_string).collect::<Vec<_>>())
                    ));
                }
                (Operator::IEquals, false) => $query = $query.filter($field.ilike(value)),
                (Operator::IEquals, true) => $query = $query.filter(not($field.ilike(value))),
                (Operator::Contains, false) => {
                    $query = $query.filter($field.like(format!("%{value}%")))
                }
                (Operator::Contains, true) => {
                    $query = $query.filter(not($field.like(format!("%{value}%"))))
                }
                (Operator::IContains, false) => {
                    $query = $query.filter($field.ilike(format!("%{value}%")))
                }
                (Operator::IContains, true) => {
                    $query = $query.filter(not($field.ilike(format!("%{value}%"))))
                }
                (Operator::StartsWith, false) => {
                    $query = $query.filter($field.like(format!("{value}%")))
                }
                (Operator::StartsWith, true) => {
                    $query = $query.filter(not($field.like(format!("{value}%"))))
                }
                (Operator::IStartsWith, false) => {
                    $query = $query.filter($field.ilike(format!("{value}%")))
                }
                (Operator::IStartsWith, true) => {
                    $query = $query.filter(not($field.ilike(format!("{value}%"))))
                }
                (Operator::EndsWith, false) => {
                    $query = $query.filter($field.like(format!("%{value}")))
                }
                (Operator::EndsWith, true) => {
                    $query = $query.filter(not($field.like(format!("%{value}"))))
                }
                (Operator::IEndsWith, false) => {
                    $query = $query.filter($field.ilike(format!("%{value}")))
                }
                (Operator::IEndsWith, true) => {
                    $query = $query.filter(not($field.ilike(format!("%{value}"))))
                }
                (Operator::Like, false) => $query = $query.filter($field.like(value)),
                (Operator::Like, true) => $query = $query.filter(not($field.like(value))),
                (Operator::Regex, false) => {
                    $query = $query.filter($crate::filters::regex_match($field, value))
                }
                (Operator::Regex, true) => {
                    $query = $query.filter(not($crate::filters::regex_match($field, value)))
                }
                _ => {
                    return Err($crate::PostgresStorageError::bad_request(format!(
                        "Operator '{operator:?}' not implemented for field '{}' (type: string)",
                        $param.field
                    )));
                }
            }
        }
    }};
}

#[macro_export]
macro_rules! postgres_datetime_filter {
    ($query:ident, $param:expr, $field:expr) => {{
        use diesel::dsl::not;
        use hubuum_query::{DataType, Operator};

        let operator = $param.operator.clone();
        let (operation, negated) = operator.op_and_neg();
        if operation == Operator::IsNull {
            $crate::postgres_is_null_filter!($query, $param, operator, $field);
        } else {
            if !operator.is_applicable_to(DataType::NumericOrDate) {
                return Err($crate::PostgresStorageError::bad_request(format!(
                    "Operator '{operator:?}' is not applicable to field '{}'",
                    $param.field
                )));
            }
            let values = hubuum_query::parse_datetime_list(&$param.value)
                .map_err(|error| $crate::PostgresStorageError::bad_request(error.to_string()))?;
            let Some(minimum) = values.iter().min().copied() else {
                return Err($crate::PostgresStorageError::bad_request(format!(
                    "Searching on field '{}' requires a value",
                    $param.field
                )));
            };
            let maximum = values.iter().max().copied().unwrap_or(minimum);
            if operation == Operator::Between && values.len() != 2 {
                return Err($crate::PostgresStorageError::bad_request(format!(
                    "Operator 'between' requires 2 values (min,max) for field '{}'",
                    $param.field
                )));
            }
            match (operation, negated) {
                (Operator::Equals | Operator::In, false) => {
                    $query = $query.filter($field.eq_any(values))
                }
                (Operator::Equals | Operator::In, true) => {
                    $query = $query.filter(not($field.eq_any(values)))
                }
                (Operator::Gt, false) => $query = $query.filter($field.gt(maximum)),
                (Operator::Gt, true) => $query = $query.filter($field.le(maximum)),
                (Operator::Gte, false) => $query = $query.filter($field.ge(maximum)),
                (Operator::Gte, true) => $query = $query.filter($field.lt(maximum)),
                (Operator::Lt, false) => $query = $query.filter($field.lt(minimum)),
                (Operator::Lt, true) => $query = $query.filter($field.ge(minimum)),
                (Operator::Lte, false) => $query = $query.filter($field.le(minimum)),
                (Operator::Lte, true) => $query = $query.filter($field.gt(minimum)),
                (Operator::Between, false) => {
                    $query = $query.filter($field.between(values[0], values[1]))
                }
                (Operator::Between, true) => {
                    $query = $query.filter(not($field.between(values[0], values[1])))
                }
                _ => {
                    return Err($crate::PostgresStorageError::bad_request(format!(
                        "Operator '{operator:?}' not implemented for field '{}' (type: date)",
                        $param.field
                    )));
                }
            }
        }
    }};
}
