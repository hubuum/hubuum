use urlparse::urlparse;

use crate::errors::ApiError;
use diesel::result::{DatabaseErrorKind, Error as DieselError};

pub fn handle_diesel_error(e: DieselError, message: &str) -> ApiError {
    match e {
        DieselError::NotFound => ApiError::Conflict(message.to_string()),
        DieselError::DatabaseError(DatabaseErrorKind::UniqueViolation, _) => {
            ApiError::Conflict(message.to_string())
        }
        _ => ApiError::DatabaseError(e.to_string()),
    }
}
#[derive(Debug)]
pub struct DatabaseUrlComponents {
    pub vendor: String,
    pub username: String,
    pub password: String,
    pub host: String,
    pub port: u16,
    pub database: String,
}

impl DatabaseUrlComponents {
    pub fn new(database_url: &str) -> Result<DatabaseUrlComponents, String> {
        let url = urlparse(database_url);

        let lowercase = url.scheme.to_lowercase();
        let scheme = lowercase.as_str();
        let port = match scheme {
            "postgres" => Ok(5432),
            "mysql" => Ok(3306),
            "sqlite" => Ok(0),
            _ => Err("Unsupported database type".to_string()),
        }?;

        let username = url.username.unwrap_or_default().to_string();
        let password = url.password.unwrap_or_default().to_string();
        let host = url.hostname.ok_or("Missing host".to_string())?.to_string();
        let path = url.path.trim_start_matches('/').to_string();

        Ok(DatabaseUrlComponents {
            vendor: scheme.to_lowercase(),
            username,
            password,
            host,
            port,
            database: path,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_database_url(url: &str, expected: Result<DatabaseUrlComponents, &str>) {
        let result = DatabaseUrlComponents::new(url);

        match expected {
            Ok(expected_components) => match result {
                Ok(components) => {
                    assert_eq!(components.vendor, expected_components.vendor);
                    assert_eq!(components.username, expected_components.username);
                    assert_eq!(components.password, expected_components.password);
                    assert_eq!(components.host, expected_components.host);
                    assert_eq!(components.port, expected_components.port);
                    assert_eq!(components.database, expected_components.database);
                }
                Err(err) => panic!("Unexpected error: {}", err),
            },
            Err(expected_err) => {
                assert!(result.is_err());
                assert_eq!(result.unwrap_err(), expected_err);
            }
        }
    }

    #[test]
    fn test_database_url_variants() {
        // Test with all fields present
        test_database_url(
            "postgres://test:test@localhost:5432/testdb",
            Ok(DatabaseUrlComponents {
                vendor: "postgres".to_string(),
                username: "test".to_string(),
                password: "test".to_string(),
                host: "localhost".to_string(),
                port: 5432,
                database: "testdb".to_string(),
            }),
        );

        // Test with missing username and password
        test_database_url(
            "postgres://localhost:5432/testdb",
            Ok(DatabaseUrlComponents {
                vendor: "postgres".to_string(),
                username: "".to_string(),
                password: "".to_string(),
                host: "localhost".to_string(),
                port: 5432,
                database: "testdb".to_string(),
            }),
        );

        // Test with missing port
        test_database_url(
            "postgres://test:test@localhost/testdb",
            Ok(DatabaseUrlComponents {
                vendor: "postgres".to_string(),
                username: "test".to_string(),
                password: "test".to_string(),
                host: "localhost".to_string(),
                port: 5432,
                database: "testdb".to_string(),
            }),
        );

        // Test with unsupported database type
        test_database_url(
            "mongodb://test:test@localhost:5432/testdb",
            Err("Unsupported database type"),
        );
    }
}
