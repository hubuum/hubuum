use chrono::NaiveDateTime;

use crate::errors::ApiError;
use chrono::{Duration, Utc};

use crate::schema::tokens;
use diesel::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
pub struct Token(pub String);

impl Token {
    pub fn get_token(&self) -> String {
        self.0.clone()
    }

    pub fn is_valid(&self, conn: &mut PgConnection) -> Result<UserToken, ApiError> {
        is_valid_token(conn, &self.0)
    }
    /// Return a string where we only expose the first three and last three characters.
    /// The middle part is replaced with "..."
    pub fn obfuscate(&self) -> String {
        let len = self.0.len();
        if len > 6 {
            let start = &self.0[..3];
            let end = &self.0[len - 3..];
            format!("{}...{}", start, end)
        } else {
            "...".to_string()
        }
    }

    pub fn delete(&self, conn: &mut PgConnection) -> Result<(), ApiError> {
        use crate::schema::tokens::dsl::{token, tokens};

        diesel::delete(tokens.filter(token.eq(&self.0))).execute(conn)?;
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Queryable, Insertable)]
#[diesel(table_name = tokens)]
pub struct UserToken {
    pub token: String,
    pub user_id: i32,
    pub issued: NaiveDateTime,
}

fn timestamp_for_valid_token() -> chrono::NaiveDateTime {
    Utc::now().naive_utc() - Duration::hours(24)
}

pub fn is_valid_token(conn: &mut PgConnection, token: &str) -> Result<UserToken, ApiError> {
    use crate::schema::tokens::dsl::{issued, token as token_column, tokens};

    let since = timestamp_for_valid_token();

    let token_result = tokens
        .filter(token_column.eq(token))
        .filter(issued.gt(since))
        .first::<UserToken>(conn);

    match token_result {
        Ok(token) => Ok(token),
        Err(_) => Err(ApiError::Unauthorized("Invalid token".to_string())),
    }
}

pub fn valid_tokens_for_user(
    conn: &mut PgConnection,
    user_id: i32,
) -> Result<Vec<UserToken>, ApiError> {
    use crate::schema::tokens::dsl::{issued, tokens, user_id as user_id_column};

    let since = timestamp_for_valid_token();
    tokens
        .filter(user_id_column.eq(user_id))
        .filter(issued.gt(since))
        .load::<UserToken>(conn)
        .map_err(|e| ApiError::DatabaseError(e.to_string()))
}
