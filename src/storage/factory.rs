//! Application composition for complete storage backends.
//!
//! Process entry points supply validated, backend-neutral settings and receive
//! an opaque [`StorageHandle`]. Adapter selection, endpoint diagnostics, and
//! backend-specific initialization errors remain inside this module.

use std::fmt;

use hubuum_storage_postgres::{PostgresPoolBuildError, PostgresPoolSettings, build_postgres_pool};
use tracing::info;

use super::{StorageBackendKind, StorageError, StorageErrorKind, StorageHandle};

pub(crate) struct StorageSettings {
    adapter: StorageAdapterSettings,
}

enum StorageAdapterSettings {
    Postgresql(PostgresPoolSettings),
}

impl StorageSettings {
    pub(crate) fn builder(connection_url: impl Into<String>) -> StorageSettingsBuilder {
        StorageSettingsBuilder {
            connection_url: connection_url.into(),
            max_connections: None,
            statement_timeout_ms: 0,
            acquire_timeout_ms: None,
        }
    }
}

impl fmt::Debug for StorageSettings {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.adapter {
            StorageAdapterSettings::Postgresql(settings) => formatter
                .debug_struct("StorageSettings")
                .field("backend", &StorageBackendKind::Postgresql.as_str())
                .field("connection_url", &"<redacted>")
                .field("max_connections", &settings.max_size())
                .field("statement_timeout_ms", &settings.statement_timeout_ms())
                .field("acquire_timeout_ms", &settings.acquire_timeout_ms())
                .finish(),
        }
    }
}

pub(crate) struct StorageSettingsBuilder {
    connection_url: String,
    max_connections: Option<u32>,
    statement_timeout_ms: u64,
    acquire_timeout_ms: Option<u64>,
}

impl StorageSettingsBuilder {
    #[must_use]
    pub(crate) fn max_connections(mut self, max_connections: u32) -> Self {
        self.max_connections = Some(max_connections);
        self
    }

    #[must_use]
    pub(crate) fn statement_timeout_ms(mut self, statement_timeout_ms: u64) -> Self {
        self.statement_timeout_ms = statement_timeout_ms;
        self
    }

    #[must_use]
    pub(crate) fn acquire_timeout_ms(mut self, acquire_timeout_ms: u64) -> Self {
        self.acquire_timeout_ms = Some(acquire_timeout_ms);
        self
    }

    pub(crate) fn build(self) -> Result<StorageSettings, StorageError> {
        let settings = PostgresPoolSettings::builder(self.connection_url)
            .max_size(self.max_connections.ok_or_else(|| {
                StorageError::new(
                    StorageErrorKind::InvalidInput,
                    "storage maximum connection count is required",
                    None,
                )
            })?)
            .statement_timeout_ms(self.statement_timeout_ms)
            .acquire_timeout_ms(self.acquire_timeout_ms.ok_or_else(|| {
                StorageError::new(
                    StorageErrorKind::InvalidInput,
                    "storage acquire timeout is required",
                    None,
                )
            })?)
            .build()
            .map_err(storage_initialization_error)?;
        Ok(StorageSettings {
            adapter: StorageAdapterSettings::Postgresql(settings),
        })
    }
}

pub(crate) fn initialize_storage(
    settings: &StorageSettings,
) -> Result<StorageHandle, StorageError> {
    match &settings.adapter {
        StorageAdapterSettings::Postgresql(settings) => {
            let endpoint = settings.endpoint();
            info!(
                message = "storage backend configured",
                backend = StorageBackendKind::Postgresql.as_str(),
                username = endpoint.username(),
                host = endpoint.host(),
                port = endpoint.port(),
                database = endpoint.database(),
                max_connections = settings.max_size(),
                acquire_timeout_ms = settings.acquire_timeout_ms(),
                statement_timeout_ms = settings.statement_timeout_ms(),
            );
            let pool = build_postgres_pool(settings).map_err(storage_initialization_error)?;
            let operational_pool_settings =
                PostgresPoolSettings::builder(settings.connection_url().to_string())
                    .max_size(1)
                    .statement_timeout_ms(settings.statement_timeout_ms())
                    .acquire_timeout_ms(settings.acquire_timeout_ms())
                    .build()
                    .map_err(storage_initialization_error)?;
            Ok(StorageHandle::postgres_with_operational_pool_settings(
                pool,
                operational_pool_settings,
            ))
        }
    }
}

fn storage_initialization_error(error: PostgresPoolBuildError) -> StorageError {
    let kind = match error {
        PostgresPoolBuildError::InvalidSettings(_)
        | PostgresPoolBuildError::InvalidUrl(_)
        | PostgresPoolBuildError::UnsupportedDatabaseType
        | PostgresPoolBuildError::UnsupportedTlsMode(_) => StorageErrorKind::InvalidInput,
        PostgresPoolBuildError::Tls(_) => StorageErrorKind::Unavailable,
    };
    StorageError::new(kind, error.to_string(), None)
}

#[cfg(feature = "embedded-migrations")]
pub(crate) fn run_storage_migrations(settings: &StorageSettings) -> Result<usize, StorageError> {
    match &settings.adapter {
        StorageAdapterSettings::Postgresql(settings) => {
            super::postgres::run_embedded_migrations(settings.connection_url())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_debug_redacts_the_connection_url() {
        let settings =
            StorageSettings::builder("postgres://secret-user:secret-password@example.test/hubuum")
                .max_connections(4)
                .statement_timeout_ms(500)
                .acquire_timeout_ms(1_000)
                .build()
                .expect("settings should be valid");

        let debug = format!("{settings:?}");
        assert!(!debug.contains("secret-user"));
        assert!(!debug.contains("secret-password"));
        assert!(debug.contains("max_connections: 4"));
    }

    #[test]
    fn settings_require_backend_neutral_pool_limits() {
        let error = StorageSettings::builder("postgres://localhost/hubuum")
            .build()
            .expect_err("missing connection limits should fail");

        assert_eq!(error.kind(), StorageErrorKind::InvalidInput);
    }
}
