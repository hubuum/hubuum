// src/db/connection.rs

use diesel::r2d2::ConnectionManager;
use diesel::r2d2::Pool;
use diesel::PgConnection;
use tracing::debug;

use crate::utilities::db::DatabaseUrlComponents;

pub type DbPool = Pool<ConnectionManager<PgConnection>>;

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
    use crate::config::get_config;

    #[test]
    fn test_init_pool() {
        let database_url = get_config().database_url.clone();
        let pool_size = get_config().db_pool_size;
        let pool = super::init_pool(&database_url, pool_size);
        assert_eq!(pool.max_size(), pool_size);
    }
}
