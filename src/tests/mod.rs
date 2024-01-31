#![allow(dead_code)]
// We allow dead code here because all of this is used in tests and it is
// thus marked as dead. Doh.

pub mod acl;
pub mod api;
pub mod api_operations;
pub mod asserts;

use actix_web::web;
use diesel::prelude::*;

use crate::config::{get_config, AppConfig};
use crate::db::init_pool;
use crate::db::DbPool;
use crate::errors::ApiError;
use crate::models::group::GroupID;
use crate::models::group::{Group, NewGroup};
use crate::models::namespace::{Namespace, NewNamespace};
use crate::models::user::{NewUser, User};

use crate::utilities::auth::generate_random_password;

pub async fn create_user_with_params(pool: &DbPool, username: &str, password: &str) -> User {
    let result = NewUser {
        username: username.to_string(),
        password: password.to_string(),
        email: None,
    }
    .save(pool)
    .await;

    assert!(
        result.is_ok(),
        "Failed to create user: {:?}",
        result.err().unwrap()
    );

    result.unwrap()
}

pub async fn create_test_user(pool: &DbPool) -> User {
    let username = "admin".to_string() + &generate_random_password(16);
    create_user_with_params(pool, &username, "testpassword").await
}

pub async fn create_test_admin(pool: &DbPool) -> User {
    let username = "user".to_string() + &generate_random_password(16);
    let user = create_user_with_params(pool, &username, "testadminpassword").await;
    let admin_group = ensure_admin_group(pool).await;

    let result = admin_group.add_member(&user, pool).await;

    if result.is_ok() {
        user
    } else {
        panic!("Failed to add user to admin group: {:?}", result.err())
    }
}

pub async fn create_test_group(pool: &DbPool) -> Group {
    let groupname = "group".to_string() + &generate_random_password(16);
    let result = NewGroup {
        groupname: groupname.to_string(),
        description: Some("Test group".to_string()),
    }
    .save(pool)
    .await;

    assert!(
        result.is_ok(),
        "Failed to create group: {:?}",
        result.err().unwrap()
    );

    result.unwrap()
}

pub async fn ensure_user(pool: &DbPool, uname: &str) -> User {
    use crate::schema::users::dsl::*;

    let mut conn = pool.get().expect("Failed to get db connection");

    let result = users.filter(username.eq(uname)).first::<User>(&mut conn);

    if let Ok(user) = result {
        return user;
    }

    let result = NewUser {
        username: uname.to_string(),
        password: "testpassword".to_string(),
        email: None,
    }
    .save(pool)
    .await;

    if let Err(e) = result {
        match e {
            ApiError::Conflict(_) => {
                return users
                    .filter(username.eq(uname))
                    .first::<User>(&mut conn)
                    .expect("Failed to fetch user after conflict");
            }
            _ => panic!("Failed to create user '{}': {:?}", uname, e),
        }
    }

    result.unwrap()
}

pub async fn ensure_admin_user(pool: &DbPool) -> User {
    let user = ensure_user(pool, "admin").await;

    let admin_group = ensure_admin_group(pool).await;

    let _ = admin_group.add_member(&user, pool).await;

    user
}

pub async fn ensure_normal_user(pool: &DbPool) -> User {
    ensure_user(pool, "normal").await
}

pub async fn ensure_admin_group(pool: &DbPool) -> Group {
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
    .save(pool)
    .await;

    if let Err(e) = result {
        match e {
            ApiError::Conflict(_) => {
                return groups
                    .filter(groupname.eq("admin"))
                    .first::<Group>(&mut conn)
                    .expect("Failed to fetch user after conflict");
            }
            _ => panic!("Failed to create admin group: {:?}", e),
        }
    }

    result.unwrap()
}

pub fn get_config_sync() -> AppConfig {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime");
    rt.block_on(async { get_config().await }).clone()
}

pub async fn get_pool_and_config() -> (DbPool, AppConfig) {
    let config = get_config().await.clone();
    let pool = init_pool(&config.database_url, 3);

    (pool, config)
}

pub async fn create_namespace(pool: &DbPool, ns_name: &str) -> Result<Namespace, ApiError> {
    let admin_group = ensure_admin_group(pool).await;
    let assignee = GroupID(admin_group.id);

    NewNamespace {
        name: ns_name.to_string(),
        description: "Test namespace".to_string(),
    }
    .save_and_grant_all_to(pool, assignee)
    .await
}

/// Initialize useful data for tests
///
/// This function will ensure that the following exists:
/// * An admin user (with the username "admin")
/// * A normal user (with the username "normal")
///
/// However, as the users are not that interesting, we simply
/// return the tokens for the users.
///
/// ## Returns
/// * pool - The database pool
/// * admin_token_string - The token for the admin user
/// * normal_token_string - The token for the normal user
async fn setup_pool_and_tokens() -> (web::Data<DbPool>, String, String) {
    let config = get_config().await;
    let pool = web::Data::new(init_pool(&config.database_url, 3));
    let admin_token_string = ensure_admin_user(&pool)
        .await
        .create_token(&pool)
        .await
        .unwrap()
        .get_token();
    let normal_token_string = ensure_normal_user(&pool)
        .await
        .create_token(&pool)
        .await
        .unwrap()
        .get_token();

    (pool, admin_token_string, normal_token_string)
}
