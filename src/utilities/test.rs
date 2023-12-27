// Do not warn about dead code in this file.
#![allow(dead_code)]
pub fn test_database_url() -> String {
    #[cfg(feature = "postgres")]
    {
        return std::env::var("HUBUUM_POSTGRES_URL")
            .unwrap_or_else(|_| "postgres://test:test@localhost/test".into());
    }
    #[cfg(feature = "mysql")]
    {
        return std::env::var("HUBUUM_MYSQL_URL")
            .unwrap_or_else(|_| "mysql://test:test@localhost/test".into());
    }
    #[cfg(feature = "sqlite")]
    {
        return std::env::var("HUBUUM_SQLITE_URL").unwrap_or_else(|_| "sqlite://:memory:".into());
    }
    #[cfg(not(any(feature = "postgres", feature = "mysql", feature = "sqlite")))]
    {
        panic!("No database feature enabled")
    }
}
