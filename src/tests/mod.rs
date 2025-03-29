#![allow(dead_code)]
// We allow dead code here because all of this is used in tests and it is
// thus marked as dead. Doh.

pub mod acl;
pub mod api;
pub mod api_operations;
pub mod asserts;
pub mod constants;
pub mod search;
pub mod validation;

use actix_web::web;
use diesel::prelude::*;

use crate::config::{get_config, AppConfig};
use crate::db::init_pool;
use crate::db::DbPool;
use crate::errors::ApiError;
use crate::models::group::GroupID;
use crate::models::group::{Group, NewGroup};
use crate::models::namespace::{Namespace, NewNamespaceWithAssignee};
use crate::models::user::{NewUser, User};

use crate::utilities::auth::generate_random_password;

use crate::traits::CanSave;

use lazy_static::lazy_static;
use once_cell::sync::Lazy;
use std::sync::Arc;
use tokio::runtime::Builder;

lazy_static! {
    static ref POOL: DbPool = {
        let config = get_config_sync();
        init_pool(&config.database_url, 20)
    };
}

static CONFIG: Lazy<Arc<AppConfig>> = Lazy::new(|| {
    let rt = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime");
    Arc::new(rt.block_on(async { get_config().await.clone() }))
});

pub fn get_config_sync() -> Arc<AppConfig> {
    CONFIG.clone()
}

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

/// Create a test user with a random username
pub async fn create_test_user(pool: &DbPool) -> User {
    let username = "user".to_string() + &generate_random_password(16);
    create_user_with_params(pool, &username, "testpassword").await
}

/// Create a test admin user with a random username.
///
/// The user will be added to the admin group.
pub async fn create_test_admin(pool: &DbPool) -> User {
    let username = "admin".to_string() + &generate_random_password(16);
    let user = create_user_with_params(pool, &username, "testadminpassword").await;
    let admin_group = ensure_admin_group(pool).await;

    let result = admin_group.add_member(pool, &user).await;

    if result.is_ok() {
        user
    } else {
        panic!("Failed to add user to admin group: {:?}", result.err())
    }
}

/// Create a test group with a random name
pub async fn create_test_group(pool: &DbPool) -> Group {
    create_groups_with_prefix(pool, &generate_random_password(16), 1)
        .await
        .remove(0)
}

pub async fn create_groups_with_prefix(
    pool: &DbPool,
    prefix: &str,
    num_groups: usize,
) -> Vec<Group> {
    let mut groups = Vec::new();

    for i in 0..num_groups {
        let groupname = format!("{}-group-{}", prefix, i);
        let result = NewGroup {
            groupname: groupname.to_string(),
            description: Some(groupname.clone()),
        }
        .save(pool)
        .await;

        assert!(
            result.is_ok(),
            "Failed to create group: {:?}",
            result.err().unwrap()
        );

        groups.push(result.unwrap());
    }

    groups
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

    let _ = admin_group.add_member(pool, &user).await;

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

pub async fn get_pool_and_config() -> (DbPool, AppConfig) {
    let config = get_config().await.clone();
    let pool = POOL.clone();

    (pool, config)
}

pub async fn create_namespace(pool: &DbPool, ns_name: &str) -> Result<Namespace, ApiError> {
    let admin_group = ensure_admin_group(pool).await;
    let assignee = GroupID(admin_group.id);

    NewNamespaceWithAssignee {
        name: ns_name.to_string(),
        description: "Test namespace".to_string(),
        group_id: assignee.0,
    }
    .save(pool)
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
pub async fn setup_pool_and_tokens() -> (web::Data<DbPool>, String, String) {
    let pool = web::Data::new(POOL.clone());
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

pub fn generate_all_subsets<T: Clone>(items: &[T]) -> Vec<Vec<T>> {
    let num_items = items.len();
    let num_subsets = 2usize.pow(num_items as u32);
    let mut subsets: Vec<Vec<T>> = Vec::with_capacity(num_subsets);

    // Iterate over each possible subset
    for subset_index in 0..num_subsets {
        let mut current_subset: Vec<T> = Vec::new();

        // Determine which items are in the current subset
        for (offset, item) in items.iter().enumerate() {
            if subset_index & (1 << offset) != 0 {
                current_subset.push(item.clone());
            }
        }

        subsets.push(current_subset);
    }

    subsets
}

#[cfg(test)]
mod test {

    use super::*;
    use crate::{models::namespace::UpdateNamespace, traits::CanDelete, traits::CanUpdate};

    #[actix_rt::test]
    async fn test_updated_and_created_at() {
        let (pool, _) = get_pool_and_config().await;
        let namespace = create_namespace(&pool, "test_updated_at").await.unwrap();
        let original_updated_at = namespace.updated_at;
        let original_created_at = namespace.created_at;

        let update = UpdateNamespace {
            name: Some("test update 2".to_string()),
            description: None,
        };

        let updated_namespace = update.update(&pool, namespace.id).await.unwrap();
        let new_created_at = updated_namespace.created_at;
        let new_updated_at = updated_namespace.updated_at;

        assert_eq!(updated_namespace.id, namespace.id);
        assert_eq!(updated_namespace.name, "test update 2");
        assert_eq!(original_created_at, new_created_at);
        assert_ne!(original_updated_at, new_updated_at);
        assert!(new_updated_at > original_updated_at);

        updated_namespace.delete(&pool).await.unwrap();
    }
}
