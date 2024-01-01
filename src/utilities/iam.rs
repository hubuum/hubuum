// To run during init:
// If we have no users and no groups, create a default admin user and a default admin group.

use crate::models::group::NewGroup;
use crate::models::user::NewUser;

use diesel::prelude::*;

use crate::schema::groups::dsl::*;
use crate::schema::users::dsl::*;

use tracing::debug;

// Helper function to add a group
pub fn add_group(conn: &mut PgConnection, group_name: &str, desc: &str) -> QueryResult<usize> {
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
}

// Helper function to add a user
pub fn add_user(conn: &mut PgConnection, new_user: &NewUser) -> QueryResult<usize> {
    debug!(
        message = "Creating user",
        username = new_user.username,
        email = new_user.email.as_ref().unwrap_or(&"".to_string())
    );
    diesel::insert_into(users).values(new_user).execute(conn)
}

// Helper function to add a user to a group
pub fn add_user_to_group(
    conn: &mut PgConnection,
    user_id: i32,
    group_id: i32,
) -> QueryResult<usize> {
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
}
