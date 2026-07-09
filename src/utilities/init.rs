// To run during init:
// If we have no users and no groups, create a default admin user and a default admin group.

use crate::models::user::NewUser;

use crate::db::DbPool;

use crate::db::traits::group::count_group_records;
use crate::db::traits::identity::ensure_identity_scope;
use crate::db::traits::user::count_user_records;
use crate::models::group::NewGroup;
use crate::models::identity::{LOCAL_IDENTITY_SCOPE, LOCAL_PROVIDER_KIND};
use crate::utilities::auth::generate_random_password;

use tracing::{debug, error, trace, warn};

pub type InitError = String;
pub type InitResult = Result<(), InitError>;

pub async fn init(pool: DbPool) -> InitResult {
    if let Err(e) = ensure_identity_scope(&pool, LOCAL_IDENTITY_SCOPE, LOCAL_PROVIDER_KIND).await {
        let err_msg = format!("Failed to ensure local identity scope: {}", e);
        error!(message = &err_msg);
        return Err(err_msg);
    }
    if let Err(e) = crate::auth::ensure_configured_identity_scopes(&pool).await {
        let err_msg = format!("Failed to ensure configured identity scopes: {}", e);
        error!(message = &err_msg);
        return Err(err_msg);
    }

    let users_count = match count_user_records(&pool) {
        Ok(count) => count,
        Err(e) => {
            let err_msg = format!("Failed to count users during initialization: {}", e);
            error!(message = &err_msg);
            return Err(err_msg);
        }
    };

    let groups_count = match count_group_records(&pool) {
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
    let admin_groupname = crate::config::get_config()
        .map(|config| config.admin_groupname.clone())
        .map_err(|e| {
            let err_msg = format!("Failed to load config during initialization: {}", e);
            error!(message = &err_msg);
            err_msg
        })?;

    let adm_group = match NewGroup::new(&admin_groupname, Some("Default admin group."))
        .await
        .save_without_events(&pool)
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
        identity_scope: None,
        name: "admin".to_string(),
        email: None,
        proper_name: Some("Administrator".to_string()),
        password: default_password.clone(),
    }
    .save_without_events(&pool))
    .await
    {
        Ok(user) => user,
        Err(e) => {
            let err_msg = format!("Error creating default admin user: {}", e);
            error!(message = &err_msg);
            return Err(err_msg);
        }
    };

    match adm_group.add_member_without_events(&pool, &adm_user).await {
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
        message = "Created default admin user; reset password with hubuum-admin",
        username = "admin",
        reset_command = "hubuum-admin --reset-password admin"
    );
    let _ = &adm_user;
    Ok(())
}
