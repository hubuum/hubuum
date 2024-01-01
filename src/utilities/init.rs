// To run during init:
// If we have no users and no groups, create a default admin user and a default admin group.

use crate::models::user::NewUser;

use crate::db::connection::DbPool;

use diesel::prelude::*;

use crate::schema::groups::dsl::*;
use crate::schema::users::dsl::*;

use crate::utilities::auth::{generate_random_password, hash_password};

use crate::utilities::iam::{add_group, add_user, add_user_to_group};

use tracing::{debug, error, trace, warn};

pub async fn init(pool: DbPool) {
    let mut conn = pool.get().expect("couldn't get db connection from pool");

    let users_count = users.count().get_result::<i64>(&mut conn).unwrap_or(0);
    let groups_count = groups.count().get_result::<i64>(&mut conn).unwrap_or(0);

    if users_count == 0 && groups_count == 0 {
        debug!(message = "No users or groups found. Creating default admin user and group.");

        if let Err(e) = add_group(&mut conn, "admin", "Default admin group.") {
            error!(
                message = "Error creating default admin group",
                error = e.to_string()
            );
            return;
        }

        let default_password = generate_random_password(32);
        let hashed_password = match hash_password(&default_password) {
            Ok(hash) => hash,
            Err(e) => {
                error!(
                    message = "Error hashing default admin user password.",
                    error = e.to_string()
                );
                return;
            }
        };

        let new_user = NewUser {
            username: "admin".to_string(),
            email: Some("".to_string()),
            password: hashed_password,
        };

        if let Err(e) = add_user(&mut conn, &new_user) {
            error!(
                message = "Error creating default admin user",
                error = e.to_string()
            );
            return;
        }

        let admin_group_id = match groups
            .filter(groupname.eq("admin"))
            .select(crate::schema::groups::id)
            .first::<i32>(&mut conn)
        {
            Ok(group_id) => group_id,
            Err(e) => {
                error!(message = "Error finding admin group", error = e.to_string());
                return;
            }
        };

        let admin_user_id = match users
            .filter(username.eq("admin"))
            .select(crate::schema::users::id)
            .first::<i32>(&mut conn)
        {
            Ok(user_id) => user_id,
            Err(e) => {
                error!(message = "Error finding admin user", error = e.to_string());
                return;
            }
        };

        if let Err(e) = add_user_to_group(&mut conn, admin_user_id, admin_group_id) {
            error!(
                message = "Error adding default admin user to default admin group",
                error = e.to_string()
            );
            return;
        }

        warn!(
            message = "Created admin user",
            username = new_user.username,
            password = default_password
        );
    } else {
        trace!("Users and/or groups found. Skipping default admin user and group creation.");
    }
}
