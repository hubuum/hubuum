// To run during init:
// If we have no users and no groups, create a default admin user and a default admin group.

use crate::models::user::NewUser;

use crate::db::DbPool;

use diesel::prelude::*;

use crate::schema::groups::dsl::*;
use crate::schema::users::dsl::*;

use crate::models::group::NewGroup;
use crate::utilities::auth::generate_random_password;

use tracing::{debug, error, trace, warn};

pub type InitError = String;
pub type InitResult = Result<(), InitError>;

pub async fn init(pool: DbPool) -> InitResult {
    let mut conn = match pool.get() {
        Ok(conn) => conn,
        Err(e) => {
            let err_msg = format!(
                "Failed to get database connection during initialization: {}",
                e
            );
            error!(message = &err_msg);
            return Err(err_msg);
        }
    };

    let users_count = match users.count().get_result::<i64>(&mut conn) {
        Ok(count) => count,
        Err(e) => {
            let err_msg = format!("Failed to count users during initialization: {}", e);
            error!(message = &err_msg);
            return Err(err_msg);
        }
    };

    let groups_count = match groups.count().get_result::<i64>(&mut conn) {
        Ok(count) => count,
        Err(e) => {
            let err_msg = format!("Failed to count groups during initialization: {}", e);
            error!(message = &err_msg);
            return Err(err_msg);
        }
    };

    if users_count != 0 || groups_count != 0 {
        trace!("Users and/or groups found. Skipping default admin user and group creation.");
        return Ok(());
    }

    debug!(message = "No users or groups found. Creating default admin user and group.");

    let adm_group = match (NewGroup {
        groupname: "admin".to_string(),
        description: Some("Default admin group.".to_string()),
    }
    .save(&pool))
    .await
    {
        Ok(group) => group,
        Err(e) => {
            let err_msg = format!("Error creating default admin group: {}", e);
            error!(message = &err_msg);
            return Err(err_msg);
        }
    };

    let default_password = generate_random_password(32);

    let adm_user = match (NewUser {
        username: "admin".to_string(),
        email: None,
        password: default_password.clone(),
    }
    .save(&pool))
    .await
    {
        Ok(user) => user,
        Err(e) => {
            let err_msg = format!("Error creating default admin user: {}", e);
            error!(message = &err_msg);
            return Err(err_msg);
        }
    };

    match adm_group.add_member(&pool, &adm_user).await {
        Ok(_) => {}
        Err(e) => {
            let err_msg = format!(
                "Error adding default admin user to default admin group: {}",
                e
            );
            error!(message = &err_msg);
            return Err(err_msg);
        }
    }

    warn!(
        message = "Created admin user",
        username = adm_user.username,
        password = default_password
    );
    Ok(())
}
