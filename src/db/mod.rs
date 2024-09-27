pub mod traits;

use diesel::r2d2::ConnectionManager;
use diesel::r2d2::Pool;
use diesel::PgConnection;

use std::time::Duration;
use tracing::{debug, error, warn};

use crate::errors::ApiError;
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
    Err(ApiError::from(last_error.unwrap()))
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
        Err(err) => panic!("{}", err),
    }

    let manager = ConnectionManager::<PgConnection>::new(database_url);

    Pool::builder()
        .max_size(max_size)
        .build(manager)
        .expect("Failed to create pool")
}

#[cfg(test)]
mod tests {
    use crate::tests::get_config_sync;

    #[test]
    fn test_init_pool() {
        let database_url = get_config_sync().database_url.clone();
        let pool_size = get_config_sync().db_pool_size;
        let pool = super::init_pool(&database_url, pool_size);
        assert_eq!(pool.max_size(), pool_size);
    }
}
