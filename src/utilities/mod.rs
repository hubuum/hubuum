pub mod auth;
pub mod db;
pub mod extensions;
pub mod iam;
pub mod init;
pub mod reporting;
pub mod response;
pub mod tasks;

pub fn is_valid_log_level(level: &str) -> bool {
    matches!(level, "error" | "warn" | "info" | "debug" | "trace")
}
