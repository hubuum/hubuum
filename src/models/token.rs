use argon2::password_hash::rand_core::le;
use chrono::NaiveDateTime;

use chrono::{Duration, Utc};

use diesel::prelude::*;
use diesel::sql_types::{Integer, Text, Timestamp};
use diesel::QueryableByName;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::db::DbPool;
use crate::errors::ApiError;
use crate::models::search::{FilterField, SortParam};
use crate::schema::tokens;
use crate::traits::{
    CursorPaginated, CursorSqlField, CursorSqlMapping, CursorSqlType, CursorValue,
};

#[derive(
    Serialize, Deserialize, Queryable, Insertable, Selectable, QueryableByName, Clone, ToSchema,
)]
#[diesel(table_name = tokens)]
pub struct UserToken {
    #[diesel(sql_type = Text)]
    pub token: String,
    #[diesel(sql_type = Integer)]
    pub user_id: i32,
    #[diesel(sql_type = Timestamp)]
    pub issued: NaiveDateTime,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Token(pub String);

impl Token {
    pub fn get_token(&self) -> String {
        self.0.clone()
    }

    /// Return a string where we only expose the first three and last three characters.
    /// The middle part is replaced with "..."
    pub fn obfuscate(&self) -> String {
        let len = self.0.len();
        if len > 6 {
            let start = &self.0[..3];
            let end = &self.0[len - 3..];
            format!("{start}...{end}")
        } else {
            "...".to_string()
        }
    }

    pub async fn delete(&self, pool: &DbPool) -> Result<(), ApiError> {
        use crate::schema::tokens::dsl::{token, tokens};

        let mut conn = pool.get()?;
        diesel::delete(tokens.filter(token.eq(&self.0))).execute(&mut conn)?;
        Ok(())
    }
}

impl CursorPaginated for UserToken {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(field, FilterField::IssuedAt | FilterField::Name)
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        Ok(match field {
            FilterField::IssuedAt => CursorValue::DateTime(self.issued),
            FilterField::Name => CursorValue::String(self.token.clone()),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for user tokens",
                    field
                )))
            }
        })
    }

    fn default_sort() -> Vec<SortParam> {
        vec![
            SortParam {
                field: FilterField::IssuedAt,
                descending: true,
            },
            SortParam {
                field: FilterField::Name,
                descending: false,
            },
        ]
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        vec![SortParam {
            field: FilterField::Name,
            descending: false,
        }]
    }
}

impl CursorSqlMapping for UserToken {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::IssuedAt => CursorSqlField {
                column: "tokens.issued",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::Name => CursorSqlField {
                column: "tokens.token",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for user tokens",
                    field
                )))
            }
        })
    }
}
