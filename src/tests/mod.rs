#![allow(dead_code)]
// We allow dead code here because all of this is used in tests and it is
// thus marked as dead. Doh.

pub mod acl;
pub mod api;

use diesel::prelude::*;

use crate::config::{get_config, AppConfig};

use crate::db::connection::DbPool;
use crate::errors::ApiError;
use crate::models::group::GroupID;
use crate::models::group::{Group, NewGroup};
use crate::models::namespace::{Namespace, NewNamespace};
use crate::models::permissions::Assignee;
use crate::models::user::{NewUser, User};

use crate::utilities::auth::generate_random_password;

fn create_user_with_params(pool: &DbPool, username: &str, password: &str) -> User {
    let result = NewUser {
        username: username.to_string(),
        password: password.to_string(),
        email: None,
    }
    .save(pool);

    assert!(
        result.is_ok(),
        "Failed to create user: {:?}",
        result.err().unwrap()
    );

    result.unwrap()
}

fn create_test_user(pool: &DbPool) -> User {
    let username = "admin".to_string() + &generate_random_password(16);
    create_user_with_params(pool, &username, "testpassword")
}

fn create_test_admin(pool: &DbPool) -> User {
    let username = "user".to_string() + &generate_random_password(16);
    let user = create_user_with_params(pool, &username, "testadminpassword");
    let admin_group = ensure_admin_group(pool);

    let result = admin_group.add_member(&user, pool);

    if result.is_ok() {
        user
    } else {
        panic!("Failed to add user to admin group: {:?}", result.err())
    }
}

fn ensure_admin_group(pool: &DbPool) -> Group {
    use crate::schema::groups::dsl::*;

    let mut conn = pool.get().expect("Failed to get db connection");

    let result = groups
        .filter(groupname.eq("admin"))
        .first::<Group>(&mut conn);

    if let Ok(group) = result {
        return group;
    }

    let result = NewGroup {
        groupname: "admin".to_string(),
        description: Some("Admin group".to_string()),
    }
    .save(pool);

    if let Err(e) = result {
        panic!("Failed to create admin group: {:?}", e);
    }

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

pub fn get_config_sync() -> AppConfig {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime");
    rt.block_on(async { get_config().await }).clone()
}

pub fn create_namespace(pool: &DbPool, ns_name: &str) -> Result<Namespace, ApiError> {
    let admin_group = ensure_admin_group(pool);
    let assignee = Assignee::Group(GroupID(admin_group.id));

    let ns = NewNamespace {
        name: ns_name.to_string(),
        description: "Test namespace".to_string(),
    }
    .save_and_grant_all_to(pool, assignee);
    ns
}
