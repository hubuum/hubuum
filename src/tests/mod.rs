#![allow(dead_code)]
// We allow dead code here because all of this is used in tests and it is
// thus marked as dead. Doh.

pub mod api;
pub mod db;

use diesel::prelude::*;

use crate::db::connection::DbPool;
use crate::errors::ApiError;
use crate::models::user::{NewUser, User};

fn create_test_user(pool: &DbPool) -> User {
    let result = NewUser {
        username: "testuser".to_string(),
        password: "testpassword".to_string(),
        email: None,
    }
    .save(pool);

    assert!(
        result.is_ok(),
        "Failed to create test user: {:?}",
        result.err().unwrap()
    );

    result.unwrap()
}

fn cleanup(pool: &DbPool) -> Result<(), ApiError> {
    use crate::schema::groups::dsl::*;
    use crate::schema::namespaces::dsl::*;
    use crate::schema::users::dsl::*;

    let mut conn = pool.get().expect("Failed to get db connection");

    diesel::delete(users).execute(&mut conn)?;
    diesel::delete(groups).execute(&mut conn)?;
    diesel::delete(namespaces).execute(&mut conn)?;
    Ok(())
}
