use crate::models::user::User;

use diesel::prelude::*;

use crate::schema::users::dsl::*;

use crate::errors::ApiError;

use diesel::r2d2::ConnectionManager;
use r2d2::PooledConnection;

use tracing::debug;

pub fn get_user_by_id(
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
    user_id: i32,
) -> Result<User, ApiError> {
    users
        .filter(crate::schema::users::dsl::id.eq(user_id))
        .first::<User>(conn)
        .map_err(|e| ApiError::DatabaseError(e.to_string()))
}

pub fn add_user_to_group(
    conn: &mut PgConnection,
    user_id: i32,
    group_id: i32,
) -> Result<usize, ApiError> {
    debug!(
        message = "Adding user to group",
        user_id = user_id,
        group_id = group_id
    );
    diesel::insert_into(crate::schema::user_groups::table)
        .values((
            crate::schema::user_groups::user_id.eq(user_id),
            crate::schema::user_groups::group_id.eq(group_id),
        ))
        .execute(conn)
        .map_err(|e| ApiError::DatabaseError(e.to_string()))
}

pub fn delete_user_from_group(
    conn: &mut PgConnection,
    user_id: i32,
    group_id: i32,
) -> Result<usize, ApiError> {
    debug!(
        message = "Deleting user from group",
        user_id = user_id,
        group_id = group_id
    );
    diesel::delete(crate::schema::user_groups::table)
        .filter(crate::schema::user_groups::user_id.eq(user_id))
        .filter(crate::schema::user_groups::group_id.eq(group_id))
        .execute(conn)
        .map_err(|e| ApiError::DatabaseError(e.to_string()))
}
