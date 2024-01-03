pub mod api;

use crate::models::user::NewUser;
use crate::utilities::auth::hash_password;
use crate::utilities::iam::add_user;
use diesel::prelude::*;
use diesel::r2d2::ConnectionManager;
use r2d2::PooledConnection;

// Used in tests, flagged as dead code
#[allow(dead_code)]
pub async fn create_test_user(
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
    username: &str,
    plaintextpassword: &str,
) -> i32 {
    let hashed_password = hash_password(plaintextpassword).unwrap();
    let new_user = NewUser {
        username: username.to_string(),
        email: Some(username.to_string() + "@nowhere"),
        password: hashed_password,
    };
    add_user(conn, &new_user).expect("Failed to create test user");

    use crate::schema::users::dsl::{id, username as username_column, users};
    users
        .filter(username_column.eq(username))
        .select(id)
        .first::<i32>(conn)
        .expect("Failed to get user id")
}
