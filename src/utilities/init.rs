// To run during init:
// If we have no users and no groups, create a default admin user and a default admin group.

use crate::db::DbPool;

use crate::db::traits::bootstrap::bootstrap_default_admin;
use crate::db::traits::identity::ensure_identity_scope;
use crate::models::identity::{LOCAL_IDENTITY_SCOPE, LOCAL_PROVIDER_KIND};
use crate::utilities::auth::{generate_random_password, hash_password};

use tracing::{error, warn};

pub type InitError = String;
pub type InitResult = Result<(), InitError>;

#[derive(Clone, Debug)]
pub struct InitializationSettings {
    admin_groupname: String,
}

impl InitializationSettings {
    pub fn new(admin_groupname: impl Into<String>) -> Result<Self, String> {
        let admin_groupname = admin_groupname.into();
        if admin_groupname.trim().is_empty() {
            return Err("administrator group name must not be empty".to_string());
        }
        Ok(Self { admin_groupname })
    }
}

pub async fn init(pool: DbPool, settings: &InitializationSettings) -> InitResult {
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

    let default_password = generate_random_password(32);
    let hashed_password = hash_password(&default_password)
        .map_err(|error| format!("Failed to hash default administrator password: {error}"))?;
    let created = bootstrap_default_admin(&pool, &settings.admin_groupname, &hashed_password)
        .await
        .map_err(|error| format!("Failed to bootstrap default administrator: {error}"))?;

    if created {
        warn!(
            message = "Created default admin user; reset password with hubuum-admin",
            username = "admin",
            reset_command = "hubuum-admin --reset-password admin"
        );
    }
    Ok(())
}
