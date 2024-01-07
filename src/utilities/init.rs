// To run during init:
// If we have no users and no groups, create a default admin user and a default admin group.

use crate::models::user::NewUser;

use crate::db::connection::DbPool;

use diesel::prelude::*;

use crate::schema::groups::dsl::*;
use crate::schema::users::dsl::*;

use crate::models::group::NewGroup;
use crate::utilities::auth::generate_random_password;

use tracing::{debug, error, trace, warn};

pub async fn init(pool: DbPool) {
    let mut conn = pool.get().expect("couldn't get db connection from pool");

    let users_count = users.count().get_result::<i64>(&mut conn).unwrap_or(0);
    let groups_count = groups.count().get_result::<i64>(&mut conn).unwrap_or(0);

    if users_count != 0 || groups_count != 0 {
        trace!("Users and/or groups found. Skipping default admin user and group creation.");
        return;
    }

    debug!(message = "No users or groups found. Creating default admin user and group.");

    let adm_group = match (NewGroup {
        groupname: "admin".to_string(),
        description: Some("Default admin group.".to_string()),
    }
    .save(&pool))
    {
        Ok(group) => group,
        Err(e) => {
            error!(
                message = "Error creating default admin group",
                error = e.to_string()
            );
            return;
        }
    };

    let default_password = generate_random_password(32);

    let adm_user = match (NewUser {
        username: "admin".to_string(),
        email: Some("".to_string()),
        password: default_password.clone(),
    }
    .save(&pool))
    {
        Ok(user) => user,
        Err(e) => {
            error!(
                message = "Error creating default admin user",
                error = e.to_string()
            );
            return;
        }
    };

    match adm_group.add_member(&adm_user, &pool) {
        Ok(_) => {}
        Err(e) => {
            error!(
                message = "Error adding default admin user to default admin group",
                error = e.to_string()
            );
            return;
        }
    }

    warn!(
        message = "Created admin user",
        username = adm_user.username,
        password = default_password
    );
}
