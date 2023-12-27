// To run during init:
// If we have no users and no groups, create a default admin user and a default admin group.

use crate::models::group::NewGroup;
use crate::models::user::NewUser;

use crate::db::connection::DbPool;

use diesel::prelude::*;

use crate::schema::groups::dsl::*;
use crate::schema::users::dsl::*;

use crate::utilities::auth::{generate_random_password, hash_password};

use tracing::{debug, error, trace, warn};

// Assuming necessary imports and models are already defined in your crate

// Helper function to add a group
fn add_group(conn: &mut PgConnection, group_name: &str, desc: &str) -> QueryResult<usize> {
    diesel::insert_into(groups)
        .values(NewGroup {
            groupname: group_name.to_string(),
            description: desc.to_string(),
        })
        .execute(conn)
}

// Helper function to add a user
fn add_user(conn: &mut PgConnection, new_user: &NewUser) -> QueryResult<usize> {
    diesel::insert_into(users).values(new_user).execute(conn)
}

// Helper function to add a user to a group
fn add_user_to_group(conn: &mut PgConnection, user_id: i32, group_id: i32) -> QueryResult<usize> {
    diesel::insert_into(crate::schema::user_groups::table)
        .values((
            crate::schema::user_groups::user_id.eq(user_id),
            crate::schema::user_groups::group_id.eq(group_id),
        ))
        .execute(conn)
}

pub async fn init(pool: DbPool) {
    let mut conn = pool.get().expect("couldn't get db connection from pool");

    let users_count = users.count().get_result::<i64>(&mut conn).unwrap_or(0);
    let groups_count = groups.count().get_result::<i64>(&mut conn).unwrap_or(0);

    if users_count == 0 && groups_count == 0 {
        debug!("No users or groups found. Creating default admin user and group.");

        if let Err(e) = add_group(&mut conn, "admin", "Default admin group.") {
            error!("Error creating default admin group: {}", e);
            return;
        }

        let default_password = generate_random_password(32);
        let hashed_password = match hash_password(&default_password) {
            Ok(hash) => hash,
            Err(_) => {
                error!("Error hashing default admin user password.");
                return;
            }
        };

        let new_user = NewUser {
            username: "admin".to_string(),
            email: Some("".to_string()),
            password: hashed_password,
        };

        if let Err(e) = add_user(&mut conn, &new_user) {
            error!("Error creating default admin user: {}", e);
            return;
        }

        let admin_group_id = match groups
            .filter(groupname.eq("admin"))
            .select(crate::schema::groups::id)
            .first::<i32>(&mut conn)
        {
            Ok(group_id) => group_id,
            Err(e) => {
                error!("Error finding admin group: {}", e);
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
                error!("Error finding admin user: {}", e);
                return;
            }
        };

        if let Err(e) = add_user_to_group(&mut conn, admin_user_id, admin_group_id) {
            error!(
                "Error adding default admin user to default admin group: {}",
                e
            );
            return;
        }

        warn!(
            "Created admin user: {} : {}.",
            new_user.username, default_password
        );
    } else {
        trace!("Users and/or groups found. Skipping default admin user and group creation.");
    }
}
