// To run during init:
// If we have no users and no groups, create a default admin user and a default admin group.

use crate::models::group::NewGroup;
use crate::models::user::{NewUser, User};

use diesel::prelude::*;

use crate::schema::groups::dsl::*;
use crate::schema::users::dsl::*;

use crate::errors::ApiError;

use crate::utilities::auth::generate_token;
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

pub fn get_user_by_username(
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
    uname: &str,
) -> Result<User, ApiError> {
    crate::schema::users::dsl::users
        .filter(username.eq(uname))
        .first::<User>(conn)
        .map_err(|e| ApiError::DatabaseError(e.to_string()))
}

pub fn add_group(conn: &mut PgConnection, group_name: &str, desc: &str) -> Result<usize, ApiError> {
    debug!(
        message = "Creating group",
        group_name = group_name,
        description = desc
    );
    diesel::insert_into(groups)
        .values(NewGroup {
            groupname: group_name.to_string(),
            description: desc.to_string(),
        })
        .execute(conn)
        .map_err(|e| ApiError::DatabaseError(e.to_string()))
}

pub fn add_user(conn: &mut PgConnection, new_user: &NewUser) -> Result<usize, ApiError> {
    debug!(
        message = "Creating user",
        username = new_user.username,
        email = new_user.email.as_ref().unwrap_or(&"".to_string())
    );

    diesel::insert_into(users)
        .values(new_user)
        .execute(conn)
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

pub fn add_token_for_user(
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
    user_id_for_token: i32,
) -> Result<String, ApiError> {
    use crate::schema::tokens::dsl::*;

    let generated_token = generate_token();

    let expire_when = chrono::Utc::now()
        .checked_add_signed(chrono::Duration::days(1))
        .expect("valid timestamp")
        .naive_utc();

    diesel::insert_into(crate::schema::tokens::table)
        .values((
            user_id.eq(&user_id_for_token),
            token.eq(&generated_token),
            expires.eq(&expire_when),
        ))
        .execute(conn)
        .map_err(|e| ApiError::DatabaseError(e.to_string()))
        .map(|_| generated_token)
}
