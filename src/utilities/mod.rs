pub mod aliases;
pub mod auth;
pub mod db;
pub mod exporting;
pub mod extensions;
pub mod init;

pub fn is_valid_log_level(level: &str) -> bool {
    matches!(level, "error" | "warn" | "info" | "debug" | "trace")
}
