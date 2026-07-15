#![cfg(feature = "integration-test-support")]

pub use hubuum::*;

#[path = "support/mod.rs"]
pub mod tests;

#[path = "api/v1/backups.rs"]
mod api_backups;
#[path = "api/v1/export_templates.rs"]
mod api_export_templates;
#[path = "api/v1/exports.rs"]
mod api_exports;
#[path = "api/v1/imports.rs"]
mod api_imports;
#[path = "api/v1/remote_targets.rs"]
mod api_remote_targets;
#[path = "api/v1/restores.rs"]
mod api_restores;
#[path = "api/v1/tasks.rs"]
mod api_tasks;
