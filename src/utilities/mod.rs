pub mod auth;
pub mod db;
pub mod iam;
pub mod init;
pub mod response;
pub mod test;

pub fn is_valid_log_level(level: &str) -> bool {
    matches!(level, "error" | "warn" | "info" | "debug" | "trace")
}
