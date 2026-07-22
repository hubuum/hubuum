use std::fmt;
use std::str::FromStr;

use urlparse::urlparse;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseVendor {
    Postgres,
    MySql,
    Sqlite,
}

impl DatabaseVendor {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
            Self::MySql => "mysql",
            Self::Sqlite => "sqlite",
        }
    }

    const fn default_port(self) -> u16 {
        match self {
            Self::Postgres => 5432,
            Self::MySql => 3306,
            Self::Sqlite => 0,
        }
    }
}

impl fmt::Display for DatabaseVendor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for DatabaseVendor {
    type Err = DatabaseUrlParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "postgres" => Ok(Self::Postgres),
            "mysql" => Ok(Self::MySql),
            "sqlite" => Ok(Self::Sqlite),
            _ => Err(DatabaseUrlParseError::UnsupportedDatabaseType),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseUrlParseError {
    UnsupportedDatabaseType,
    MissingHost,
}

impl fmt::Display for DatabaseUrlParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedDatabaseType => formatter.write_str("Unsupported database type"),
            Self::MissingHost => formatter.write_str("Missing host"),
        }
    }
}

impl std::error::Error for DatabaseUrlParseError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseUrlComponents {
    vendor: DatabaseVendor,
    username: String,
    host: String,
    port: u16,
    database: String,
}

impl DatabaseUrlComponents {
    pub fn vendor(&self) -> DatabaseVendor {
        self.vendor
    }

    pub fn username(&self) -> &str {
        &self.username
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn database(&self) -> &str {
        &self.database
    }
}

impl FromStr for DatabaseUrlComponents {
    type Err = DatabaseUrlParseError;

    fn from_str(database_url: &str) -> Result<Self, Self::Err> {
        let url = urlparse(database_url);
        let vendor = url.scheme.parse::<DatabaseVendor>()?;
        let username = url.username.unwrap_or_default().to_string();
        let host = url
            .hostname
            .ok_or(DatabaseUrlParseError::MissingHost)?
            .to_string();
        let database = url.path.trim_start_matches('/').to_string();

        Ok(Self {
            vendor,
            username,
            host,
            port: vendor.default_port(),
            database,
        })
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(
        "postgres://test:test@localhost:5432/testdb",
        DatabaseVendor::Postgres,
        "test",
        "localhost",
        5432,
        "testdb"
    )]
    #[case(
        "postgres://localhost:5432/testdb",
        DatabaseVendor::Postgres,
        "",
        "localhost",
        5432,
        "testdb"
    )]
    #[case(
        "mysql://test:test@example.internal/example",
        DatabaseVendor::MySql,
        "test",
        "example.internal",
        3306,
        "example"
    )]
    fn parses_supported_database_urls(
        #[case] database_url: &str,
        #[case] vendor: DatabaseVendor,
        #[case] username: &str,
        #[case] host: &str,
        #[case] port: u16,
        #[case] database: &str,
    ) {
        let components = database_url.parse::<DatabaseUrlComponents>().unwrap();

        assert_eq!(components.vendor(), vendor);
        assert_eq!(components.username(), username);
        assert_eq!(components.host(), host);
        assert_eq!(components.port(), port);
        assert_eq!(components.database(), database);
    }

    #[test]
    fn rejects_an_unsupported_database_type() {
        let error = "mongodb://test:test@localhost:5432/testdb"
            .parse::<DatabaseUrlComponents>()
            .unwrap_err();

        assert_eq!(error, DatabaseUrlParseError::UnsupportedDatabaseType);
    }

    #[test]
    fn rejects_a_database_url_without_a_host() {
        let error = "postgres:///testdb"
            .parse::<DatabaseUrlComponents>()
            .unwrap_err();

        assert_eq!(error, DatabaseUrlParseError::MissingHost);
    }
}
