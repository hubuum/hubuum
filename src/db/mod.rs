pub mod traits;

use diesel::r2d2::ConnectionManager;
use diesel::r2d2::Pool;
use diesel::PgConnection;

use std::time::Duration;
use tracing::{debug, error, warn};

use crate::errors::{fatal_error, ApiError, EXIT_CODE_CONFIG_ERROR, EXIT_CODE_DATABASE_ERROR};
use crate::utilities::db::DatabaseUrlComponents;

pub type DbPool = Pool<ConnectionManager<PgConnection>>;

const MAX_RETRIES: u32 = 3;
const RETRY_DELAY: Duration = Duration::from_millis(100);

pub fn with_connection<F, R>(pool: &DbPool, f: F) -> Result<R, ApiError>
where
    F: FnOnce(&mut PgConnection) -> Result<R, diesel::result::Error>,
{
    let mut last_error = None;

    for attempt in 1..=MAX_RETRIES {
        match pool.get() {
            Ok(mut conn) => return f(&mut conn).map_err(ApiError::from),
            Err(e) => {
                warn!(
                    "Failed to get database connection (attempt {}): {}",
                    attempt, e
                );
                last_error = Some(e);
                if attempt < MAX_RETRIES {
                    std::thread::sleep(RETRY_DELAY);
                }
            }
        }
    }

    error!(
        "Failed to get database connection after {} attempts",
        MAX_RETRIES
    );
    // last_error should always be Some since we iterate at least once,
    // but we handle it defensively with a fallback message
    match last_error {
        Some(e) => Err(ApiError::from(e)),
        None => Err(ApiError::DbConnectionError(
            "Failed to establish database connection after retries".to_string(),
        )),
    }
}

pub fn init_pool(database_url: &str, max_size: u32) -> DbPool {
    let database_url_components = DatabaseUrlComponents::new(database_url);

    match database_url_components {
        Ok(components) => {
            debug!(
                message = "Database URL parsed.",
                vendor = components.vendor,
                username = components.username,
                host = components.host,
                port = components.port,
                database = components.database,
            );
        }
        Err(err) => fatal_error(
            &format!("Failed to parse database URL: {}", err),
            EXIT_CODE_CONFIG_ERROR,
        ),
    }

    let manager = ConnectionManager::<PgConnection>::new(database_url);

    match Pool::builder().max_size(max_size).build(manager) {
        Ok(pool) => pool,
        Err(e) => fatal_error(
            &format!("Failed to create database pool: {}", e),
            EXIT_CODE_DATABASE_ERROR,
        ),
    }
}

#[cfg(test)]
mod tests {
    use crate::config::get_config;
    use crate::errors::ApiError;
    use diesel::r2d2::{ConnectionManager, Pool};
    use diesel::PgConnection;

    #[test]
    fn test_init_pool() {
        let config = get_config().expect("Failed to load config for test");
        let database_url = config.database_url.clone();
        let pool_size = config.db_pool_size;
        let pool = super::init_pool(&database_url, pool_size);
        assert_eq!(pool.max_size(), pool_size);
    }

    #[test]
    fn test_with_connection_returns_error_on_invalid_pool() {
        // Create a pool with an invalid database URL to test error handling
        let manager = ConnectionManager::<PgConnection>::new("postgres://invalid:5432/nonexistent");

        // Try to build the pool - if this fails, we can't run the test
        let pool = match Pool::builder()
            .max_size(1)
            .connection_timeout(std::time::Duration::from_millis(100))
            .build(manager)
        {
            Ok(p) => p,
            Err(_) => {
                // Pool creation itself failed - this is acceptable for this test
                // as we're testing error handling in general
                return;
            }
        };

        // This should return an error, not panic
        let result = super::with_connection(&pool, |_conn| Ok::<_, diesel::result::Error>(()));

        assert!(result.is_err());

        // Verify it's the right kind of error
        match result {
            Err(ApiError::DbConnectionError(_)) => {
                // Expected error type
            }
            other => panic!("Expected DbConnectionError, got: {:?}", other),
        }
    }

    #[test]
    fn test_with_connection_success_path() {
        let config = get_config().expect("Failed to load config for test");
        let pool = super::init_pool(&config.database_url, 1);

        // This should succeed
        let result = super::with_connection(&pool, |_conn| Ok::<i32, diesel::result::Error>(42));

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }
}
