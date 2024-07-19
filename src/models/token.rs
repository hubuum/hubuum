use argon2::password_hash::rand_core::le;
use chrono::NaiveDateTime;

use chrono::{Duration, Utc};

use diesel::prelude::*;
use diesel::sql_types::{Integer, Text, Timestamp};
use diesel::QueryableByName;
use serde::{Deserialize, Serialize};

use crate::errors::ApiError;
use crate::schema::tokens;

#[derive(Serialize, Deserialize, Queryable, Insertable, Selectable, QueryableByName, Clone)]
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
            format!("{}...{}", start, end)
        } else {
            "...".to_string()
        }
    }

    pub async fn delete(&self, conn: &mut PgConnection) -> Result<(), ApiError> {
        use crate::schema::tokens::dsl::{token, tokens};

        diesel::delete(tokens.filter(token.eq(&self.0))).execute(conn)?;
        Ok(())
    }
}
