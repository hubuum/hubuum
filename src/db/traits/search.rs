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
        let mut start = 0;
        for (bind_variable, offset) in self
            .bind_variables
            .iter()
            .zip(bind_placeholder_offsets(&self.sql))
        {
            out.push_sql(&self.sql[start..offset]);
            bind_sql_value(&mut out, bind_variable)?;
            start = offset + 1;
        }
        out.push_sql(&self.sql[start..]);
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

pub(crate) fn dynamic_sql_predicate(component: SQLComponent) -> Result<JsonSqlPredicate, ApiError> {
    let placeholder_count = bind_placeholder_offsets(&component.sql).len();
    if placeholder_count != component.bind_variables.len() {
        return Err(ApiError::InternalServerError(format!(
            "Dynamic SQL predicate has {placeholder_count} placeholders but {} bind values",
            component.bind_variables.len()
        )));
    }
    Ok(JsonSqlPredicate {
        sql: component.sql,
        bind_variables: component.bind_variables,
    })
}

fn bind_placeholder_offsets(sql: &str) -> Vec<usize> {
    let mut offsets = Vec::new();
    let mut characters = sql.char_indices().peekable();
    let mut in_single_quoted_string = false;
    while let Some((offset, character)) = characters.next() {
        if character == '\'' {
            if in_single_quoted_string
                && characters
                    .peek()
                    .is_some_and(|(_, next_character)| *next_character == '\'')
            {
                let _ = characters.next();
            } else {
                in_single_quoted_string = !in_single_quoted_string;
            }
        } else if character == '?' && !in_single_quoted_string {
            offsets.push(offset);
        }
    }
    offsets
}

pub trait JsonPredicateExt {
    fn as_json_predicate(&self) -> Result<JsonSqlPredicate, ApiError>;
}

impl JsonPredicateExt for ParsedQueryParam {
    fn as_json_predicate(&self) -> Result<JsonSqlPredicate, ApiError> {
        dynamic_sql_predicate(self.as_json_sql()?)
    }
}

#[cfg(test)]
mod tests {
    use super::bind_placeholder_offsets;

    #[test]
    fn bind_placeholders_ignore_question_marks_in_sql_strings() {
        let sql = "scope = '[{\"path\":\"/answer?\"}]' AND escaped = 'it''s?' AND value = ?";

        assert_eq!(bind_placeholder_offsets(sql), vec![sql.len() - 1]);
    }
}
