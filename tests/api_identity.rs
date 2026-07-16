#![cfg(feature = "integration-test-support")]

pub use hubuum::*;

#[path = "support/mod.rs"]
pub mod tests;

#[path = "api/v1/auth.rs"]
mod api_auth;
#[path = "api/v1/groups.rs"]
mod api_groups;
#[path = "api/v1/principal_settings.rs"]
mod api_principal_settings;
#[path = "api/v1/service_accounts.rs"]
mod api_service_accounts;
#[path = "api/v1/users.rs"]
mod api_users;
