use crate::models::user::User;

use diesel::prelude::*;

use crate::schema::users::dsl::*;

use crate::errors::ApiError;

use diesel::r2d2::ConnectionManager;
use r2d2::PooledConnection;

pub fn get_user_by_id(
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
    user_id: i32,
) -> Result<User, ApiError> {
    users
        .filter(crate::schema::users::dsl::id.eq(user_id))
        .first::<User>(conn)
        .map_err(|e| ApiError::DatabaseError(e.to_string()))
}
