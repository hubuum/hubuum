use diesel::expression::{AppearsOnTable, Expression, SelectableExpression, ValidGrouping};
use diesel::pg::Pg;
use diesel::query_builder::{AstPass, QueryFragment, QueryId};
use diesel::result::QueryResult;
use diesel::sql_types::{Bool, Integer, Text, Timestamp};

use crate::errors::ApiError;
use crate::models::search::{ParsedQueryParam, ParsedQueryParamExt, SQLComponent, SQLValue};

#[derive(Debug, Clone, PartialEq)]
pub struct JsonSqlPredicate {
    sql: String,
    bind_variables: Vec<SQLValue>,
}

impl Expression for JsonSqlPredicate {
    type SqlType = Bool;
}

impl QueryId for JsonSqlPredicate {
    type QueryId = ();

    const HAS_STATIC_QUERY_ID: bool = false;
}

impl QueryFragment<Pg> for JsonSqlPredicate {
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.unsafe_to_cache_prepared();
        let mut sql_parts = self.sql.split('?');
        if let Some(first_part) = sql_parts.next() {
            out.push_sql(first_part);
        }
        for (bind_variable, sql_part) in self.bind_variables.iter().zip(sql_parts) {
            bind_sql_value(&mut out, bind_variable)?;
            out.push_sql(sql_part);
        }
        Ok(())
    }
}

impl<QS> SelectableExpression<QS> for JsonSqlPredicate {}
impl<QS> AppearsOnTable<QS> for JsonSqlPredicate {}

impl<GB> ValidGrouping<GB> for JsonSqlPredicate {
    type IsAggregate = diesel::expression::is_aggregate::Never;
}

fn bind_sql_value<'b>(out: &mut AstPass<'_, 'b, Pg>, value: &'b SQLValue) -> QueryResult<()> {
    match value {
        SQLValue::String(value) => out.push_bind_param::<Text, _>(value),
        SQLValue::Integer(value) => out.push_bind_param::<Integer, _>(value),
        SQLValue::Date(value) => out.push_bind_param::<Timestamp, _>(value),
        SQLValue::Boolean(value) => out.push_bind_param::<Bool, _>(value),
    }
}

fn into_predicate(component: SQLComponent) -> Result<JsonSqlPredicate, ApiError> {
    let placeholder_count = component
        .sql
        .chars()
        .filter(|character| *character == '?')
        .count();
    if placeholder_count != component.bind_variables.len() {
        return Err(ApiError::InternalServerError(format!(
            "JSON SQL predicate has {placeholder_count} placeholders but {} bind values",
            component.bind_variables.len()
        )));
    }
    Ok(JsonSqlPredicate {
        sql: component.sql,
        bind_variables: component.bind_variables,
    })
}

pub trait JsonPredicateExt {
    fn as_json_predicate(&self) -> Result<JsonSqlPredicate, ApiError>;
}

impl JsonPredicateExt for ParsedQueryParam {
    fn as_json_predicate(&self) -> Result<JsonSqlPredicate, ApiError> {
        into_predicate(self.as_json_sql()?)
    }
}
