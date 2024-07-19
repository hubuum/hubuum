use crate::db::{with_connection, DbPool};
use crate::models::user::User;

use diesel::prelude::*;

use crate::schema::users::dsl::*;

use crate::errors::ApiError;

pub fn get_user_by_id(pool: &DbPool, user_id: i32) -> Result<User, ApiError> {
    with_connection(pool, |conn| {
        Ok(users
            .filter(crate::schema::users::dsl::id.eq(user_id))
            .first::<User>(conn)?)
    })
}
