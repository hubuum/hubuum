//! Typed bind values and Diesel predicates for adapter-owned dynamic SQL.

use std::iter::from_fn;

use diesel::expression::{AppearsOnTable, Expression, SelectableExpression, ValidGrouping};
use diesel::pg::Pg;
use diesel::query_builder::{AstPass, QueryFragment, QueryId};
use diesel::result::QueryResult;
use diesel::sql_types::{BigInt, Bool, Integer, Text, Timestamp};

use crate::PostgresStorageError;

#[derive(Clone, Debug)]
pub(crate) enum SqlValue {
    Integer(i32),
    BigInteger(i64),
    String(String),
    Boolean(bool),
    DateTime(chrono::NaiveDateTime),
}

pub(crate) struct SqlComponent {
    pub(crate) sql: String,
    pub(crate) bind_variables: Vec<SqlValue>,
}

#[derive(Debug, Clone)]
pub(crate) struct BoundSqlPredicate {
    sql: String,
    bind_variables: Vec<SqlValue>,
}

impl Expression for BoundSqlPredicate {
    type SqlType = Bool;
}

impl QueryId for BoundSqlPredicate {
    type QueryId = ();

    const HAS_STATIC_QUERY_ID: bool = false;
}

impl QueryFragment<Pg> for BoundSqlPredicate {
    fn walk_ast<'bind>(&'bind self, mut output: AstPass<'_, 'bind, Pg>) -> QueryResult<()> {
        output.unsafe_to_cache_prepared();
        let mut start = 0;
        for (bind_variable, offset) in self
            .bind_variables
            .iter()
            .zip(bind_placeholder_offsets(&self.sql))
        {
            output.push_sql(&self.sql[start..offset]);
            match bind_variable {
                SqlValue::Integer(value) => output.push_bind_param::<Integer, _>(value)?,
                SqlValue::BigInteger(value) => output.push_bind_param::<BigInt, _>(value)?,
                SqlValue::String(value) => output.push_bind_param::<Text, _>(value)?,
                SqlValue::Boolean(value) => output.push_bind_param::<Bool, _>(value)?,
                SqlValue::DateTime(value) => output.push_bind_param::<Timestamp, _>(value)?,
            }
            start = offset + 1;
        }
        output.push_sql(&self.sql[start..]);
        Ok(())
    }
}

impl<QuerySource> SelectableExpression<QuerySource> for BoundSqlPredicate {}
impl<QuerySource> AppearsOnTable<QuerySource> for BoundSqlPredicate {}

impl<GroupBy> ValidGrouping<GroupBy> for BoundSqlPredicate {
    type IsAggregate = diesel::expression::is_aggregate::Never;
}

pub(crate) fn bound_sql_predicate(
    component: SqlComponent,
) -> Result<BoundSqlPredicate, PostgresStorageError> {
    let placeholder_count = bind_placeholder_offsets(&component.sql).count();
    if placeholder_count != component.bind_variables.len() {
        return Err(PostgresStorageError::internal(format!(
            "Dynamic SQL predicate has {placeholder_count} placeholders but {} bind values",
            component.bind_variables.len()
        )));
    }
    Ok(BoundSqlPredicate {
        sql: component.sql,
        bind_variables: component.bind_variables,
    })
}

pub(crate) fn indexed_bind_placeholders(sql: &str) -> String {
    let mut indexed = String::with_capacity(sql.len());
    let mut start = 0;
    for (index, offset) in bind_placeholder_offsets(sql).enumerate() {
        indexed.push_str(&sql[start..offset]);
        indexed.push('$');
        indexed.push_str(&(index + 1).to_string());
        start = offset + 1;
    }
    indexed.push_str(&sql[start..]);
    indexed
}

fn bind_placeholder_offsets(sql: &str) -> impl Iterator<Item = usize> + '_ {
    let mut characters = sql.char_indices().peekable();
    let mut in_single_quoted_string = false;
    from_fn(move || {
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
                return Some(offset);
            }
        }
        None
    })
}

#[cfg(test)]
mod tests {
    use super::{bind_placeholder_offsets, indexed_bind_placeholders};

    #[test]
    fn placeholders_ignore_question_marks_in_sql_strings() {
        let sql = "scope = '[{\"path\":\"/answer?\"}]' AND escaped = 'it''s?' AND value = ?";

        assert_eq!(
            bind_placeholder_offsets(sql).collect::<Vec<_>>(),
            vec![sql.len() - 1]
        );
    }

    #[test]
    fn placeholders_are_numbered_without_touching_quoted_question_marks() {
        assert_eq!(
            indexed_bind_placeholders("SELECT ?, '?', ?"),
            "SELECT $1, '?', $2"
        );
    }
}
