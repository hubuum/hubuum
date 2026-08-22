use std::fmt;
use std::fs::{self, File};
use std::io::{self, Read};
use std::net::IpAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::pooled_connection::bb8::{Pool, PooledConnection};
use diesel_async::pooled_connection::{AsyncDieselConnectionManager, ManagerConfig};
use futures_util::FutureExt;
use rustls::pki_types::{CertificateDer, pem::PemObject};
use rustls::{ClientConfig, RootCertStore};
use rustls_platform_verifier::BuilderVerifierExt;
use urlparse::urlparse;

const MAX_CERTIFICATE_BUNDLE_BYTES: usize = 4 * 1024 * 1024;

pub type PostgresConnection = AsyncPgConnection;
pub type PostgresPool = Pool<PostgresConnection>;
pub type PostgresPooledConnection<'a> = PooledConnection<'a, PostgresConnection>;

/// Non-secret PostgreSQL endpoint details suitable for diagnostics.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PostgresEndpoint {
    username: String,
    host: String,
    port: u16,
    database: String,
}

impl PostgresEndpoint {
    #[must_use]
    pub fn username(&self) -> &str {
        &self.username
    }

    #[must_use]
    pub fn host(&self) -> &str {
        &self.host
    }

    #[must_use]
    pub const fn port(&self) -> u16 {
        self.port
    }

    #[must_use]
    pub fn database(&self) -> &str {
        &self.database
    }
}

/// Validated settings required to construct a PostgreSQL pool.
pub struct PostgresPoolSettings {
    database_url: String,
    endpoint: PostgresEndpoint,
    max_size: u32,
    statement_timeout_ms: u64,
    acquire_timeout_ms: u64,
}

impl PostgresPoolSettings {
    #[must_use]
    pub fn builder(database_url: impl Into<String>) -> PostgresPoolSettingsBuilder {
        PostgresPoolSettingsBuilder {
            database_url: database_url.into(),
            max_size: None,
            statement_timeout_ms: 0,
            acquire_timeout_ms: None,
        }
    }

    #[must_use]
    pub fn endpoint(&self) -> &PostgresEndpoint {
        &self.endpoint
    }

    #[must_use]
    pub const fn max_size(&self) -> u32 {
        self.max_size
    }

    #[must_use]
    pub const fn statement_timeout_ms(&self) -> u64 {
        self.statement_timeout_ms
    }

    #[must_use]
    pub const fn acquire_timeout_ms(&self) -> u64 {
        self.acquire_timeout_ms
    }

    /// Credential-bearing connection URL for adapter-internal setup such as
    /// embedded migrations. Do not include this value in diagnostics.
    #[must_use]
    pub fn connection_url(&self) -> &str {
        &self.database_url
    }
}

pub struct PostgresPoolSettingsBuilder {
    database_url: String,
    max_size: Option<u32>,
    statement_timeout_ms: u64,
    acquire_timeout_ms: Option<u64>,
}

impl PostgresPoolSettingsBuilder {
    #[must_use]
    pub fn max_size(mut self, max_size: u32) -> Self {
        self.max_size = Some(max_size);
        self
    }

    #[must_use]
    pub fn statement_timeout_ms(mut self, statement_timeout_ms: u64) -> Self {
        self.statement_timeout_ms = statement_timeout_ms;
        self
    }

    #[must_use]
    pub fn acquire_timeout_ms(mut self, acquire_timeout_ms: u64) -> Self {
        self.acquire_timeout_ms = Some(acquire_timeout_ms);
        self
    }

    pub fn build(self) -> Result<PostgresPoolSettings, PostgresPoolBuildError> {
        let max_size = self
            .max_size
            .ok_or(PostgresPoolBuildError::InvalidSettings(
                "PostgreSQL pool size is required",
            ))?;
        let acquire_timeout_ms =
            self.acquire_timeout_ms
                .ok_or(PostgresPoolBuildError::InvalidSettings(
                    "PostgreSQL pool acquire timeout is required",
                ))?;
        if self.database_url.trim().is_empty() {
            return Err(PostgresPoolBuildError::InvalidSettings(
                "PostgreSQL URL must not be empty",
            ));
        }
        if max_size == 0 {
            return Err(PostgresPoolBuildError::InvalidSettings(
                "PostgreSQL pool size must be greater than zero",
            ));
        }
        if acquire_timeout_ms == 0 {
            return Err(PostgresPoolBuildError::InvalidSettings(
                "PostgreSQL pool acquire timeout must be greater than zero",
            ));
        }
        let endpoint = parse_postgres_endpoint(&self.database_url)?;

        Ok(PostgresPoolSettings {
            database_url: self.database_url,
            endpoint,
            max_size,
            statement_timeout_ms: self.statement_timeout_ms,
            acquire_timeout_ms,
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum PostgresPoolBuildError {
    InvalidSettings(&'static str),
    InvalidUrl(&'static str),
    UnsupportedDatabaseType,
    UnsupportedTlsMode(String),
    Tls(String),
}

impl fmt::Display for PostgresPoolBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSettings(message) | Self::InvalidUrl(message) => {
                formatter.write_str(message)
            }
            Self::UnsupportedDatabaseType => formatter.write_str("Unsupported database type"),
            Self::UnsupportedTlsMode(mode) => write!(
                formatter,
                "unsupported PostgreSQL sslmode '{mode}'; expected disable, prefer, or require"
            ),
            Self::Tls(message) => {
                write!(formatter, "PostgreSQL TLS configuration failed: {message}")
            }
        }
    }
}

impl std::error::Error for PostgresPoolBuildError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PostgresTlsMode {
    Disable,
    Verify,
}

/// Construct the lazy PostgreSQL connection pool described by `settings`.
pub fn build_postgres_pool(
    settings: &PostgresPoolSettings,
) -> Result<PostgresPool, PostgresPoolBuildError> {
    let tls_mode = postgres_tls_mode(&settings.database_url, settings.endpoint.host())?;
    let mut manager_config = ManagerConfig::<PostgresConnection>::default();
    let tls_config = match tls_mode {
        PostgresTlsMode::Disable => None,
        PostgresTlsMode::Verify => Some(postgres_tls_config()?),
    };
    let statement_timeout_ms = settings.statement_timeout_ms;
    manager_config.custom_setup = Box::new(move |url| {
        let tls_config = tls_config.clone();
        async move {
            let mut connection = establish_postgres_connection(url, tls_config).await?;

            if statement_timeout_ms > 0 {
                diesel::sql_query("SELECT set_config('statement_timeout', $1, false)")
                    .bind::<diesel::sql_types::Text, _>(statement_timeout_ms.to_string())
                    .execute(&mut connection)
                    .await
                    .map_err(|error| {
                        diesel::result::ConnectionError::BadConnection(error.to_string())
                    })?;
            }
            Ok(connection)
        }
        .boxed()
    });
    let manager = AsyncDieselConnectionManager::<PostgresConnection>::new_with_config(
        &settings.database_url,
        manager_config,
    );

    Ok(Pool::builder()
        .max_size(settings.max_size)
        .connection_timeout(Duration::from_millis(settings.acquire_timeout_ms))
        .build_unchecked(manager))
}

fn parse_postgres_endpoint(database_url: &str) -> Result<PostgresEndpoint, PostgresPoolBuildError> {
    let parsed = urlparse(database_url);
    if !parsed.scheme.eq_ignore_ascii_case("postgres")
        && !parsed.scheme.eq_ignore_ascii_case("postgresql")
    {
        return Err(PostgresPoolBuildError::UnsupportedDatabaseType);
    }
    let host = parsed.hostname.filter(|host| !host.is_empty()).ok_or(
        PostgresPoolBuildError::InvalidUrl("PostgreSQL URL must include a host"),
    )?;

    Ok(PostgresEndpoint {
        username: parsed.username.unwrap_or_default().to_string(),
        host: host.to_string(),
        port: 5432,
        database: parsed.path.trim_start_matches('/').to_string(),
    })
}

fn postgres_tls_mode(
    database_url: &str,
    host: &str,
) -> Result<PostgresTlsMode, PostgresPoolBuildError> {
    let mut explicit_mode = None;
    if let Some((_, query)) = database_url.split_once('?') {
        for (key, value) in query
            .split('&')
            .filter_map(|parameter| parameter.split_once('='))
        {
            if key.eq_ignore_ascii_case("sslmode") && explicit_mode.replace(value).is_some() {
                return Err(PostgresPoolBuildError::InvalidUrl(
                    "PostgreSQL sslmode must not be repeated",
                ));
            }
        }
    }

    match explicit_mode {
        Some("disable") => Ok(PostgresTlsMode::Disable),
        Some("prefer" | "require") => Ok(PostgresTlsMode::Verify),
        Some(mode) => Err(PostgresPoolBuildError::UnsupportedTlsMode(mode.to_string())),
        None if is_loopback_postgres_host(host) => Ok(PostgresTlsMode::Disable),
        None => Ok(PostgresTlsMode::Verify),
    }
}

fn is_loopback_postgres_host(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

fn postgres_tls_config() -> Result<ClientConfig, PostgresPoolBuildError> {
    let builder = ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|error| PostgresPoolBuildError::Tls(error.to_string()))?;

    let Some(root_cert_path) = std::env::var_os("PGSSLROOTCERT") else {
        return builder
            .with_platform_verifier()
            .map(|builder| builder.with_no_client_auth())
            .map_err(|error| PostgresPoolBuildError::Tls(error.to_string()));
    };

    if root_cert_path == "system" {
        return builder
            .with_platform_verifier()
            .map(|builder| builder.with_no_client_auth())
            .map_err(|error| PostgresPoolBuildError::Tls(error.to_string()));
    }

    let certificate_bytes = read_bounded_regular_file(
        Path::new(&root_cert_path),
        "PostgreSQL root certificate bundle",
        MAX_CERTIFICATE_BUNDLE_BYTES,
    )
    .map_err(|error| PostgresPoolBuildError::Tls(error.to_string()))?;
    let certificates = CertificateDer::pem_slice_iter(&certificate_bytes);
    let mut roots = RootCertStore::empty();
    let mut root_count = 0usize;
    for certificate in certificates {
        roots
            .add(certificate.map_err(|error| PostgresPoolBuildError::Tls(error.to_string()))?)
            .map_err(|error| PostgresPoolBuildError::Tls(error.to_string()))?;
        root_count += 1;
    }
    if root_count == 0 {
        return Err(PostgresPoolBuildError::Tls(format!(
            "PGSSLROOTCERT contains no certificates: {}",
            root_cert_path.to_string_lossy()
        )));
    }

    Ok(builder.with_root_certificates(roots).with_no_client_auth())
}

async fn establish_postgres_connection(
    database_url: &str,
    tls_config: Option<ClientConfig>,
) -> Result<PostgresConnection, diesel::result::ConnectionError> {
    if let Some(tls_config) = tls_config {
        let tls = tokio_postgres_rustls::MakeRustlsConnect::new(tls_config);
        let (client, connection) = tokio_postgres::connect(database_url, tls)
            .await
            .map_err(|error| diesel::result::ConnectionError::BadConnection(error.to_string()))?;
        PostgresConnection::try_from_client_and_connection(client, connection).await
    } else {
        let (client, connection) = tokio_postgres::connect(database_url, tokio_postgres::NoTls)
            .await
            .map_err(|error| diesel::result::ConnectionError::BadConnection(error.to_string()))?;
        PostgresConnection::try_from_client_and_connection(client, connection).await
    }
}

fn read_bounded_regular_file(
    path: &Path,
    description: &str,
    max_bytes: usize,
) -> io::Result<Vec<u8>> {
    let max_bytes_u64 = u64::try_from(max_bytes).unwrap_or(u64::MAX);
    let metadata = fs::metadata(path)?;
    if !metadata.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{description} {path:?} must be a regular file"),
        ));
    }
    if metadata.len() > max_bytes_u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{description} {path:?} exceeds the {max_bytes}-byte limit"),
        ));
    }

    let file = File::open(path)?;
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(max_bytes_u64.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() > max_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{description} {path:?} exceeds the {max_bytes}-byte limit"),
        ));
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    fn settings(database_url: &str) -> Result<PostgresPoolSettings, PostgresPoolBuildError> {
        PostgresPoolSettings::builder(database_url)
            .max_size(1)
            .acquire_timeout_ms(100)
            .build()
    }

    #[rstest]
    #[case::empty_url("", 1, "PostgreSQL URL must not be empty")]
    #[case::zero_pool_size(
        "postgres://localhost/hubuum",
        0,
        "PostgreSQL pool size must be greater than zero"
    )]
    fn settings_reject_invalid_values(
        #[case] database_url: &str,
        #[case] max_size: u32,
        #[case] expected: &str,
    ) {
        let builder = PostgresPoolSettings::builder(database_url)
            .max_size(max_size)
            .acquire_timeout_ms(100);

        let Err(error) = builder.build() else {
            panic!("invalid PostgreSQL settings unexpectedly succeeded");
        };
        assert_eq!(error.to_string(), expected);
    }

    #[rstest]
    #[case::localhost("localhost")]
    #[case::ipv4("127.0.0.1")]
    #[case::ipv6("::1")]
    fn implicit_loopback_urls_disable_tls(#[case] host: &str) {
        assert_eq!(
            postgres_tls_mode("postgres://postgres@localhost/hubuum", host),
            Ok(PostgresTlsMode::Disable)
        );
    }

    #[test]
    fn endpoint_metadata_excludes_credentials() {
        let settings = settings("postgres://admin:secret@db.example.com/hubuum").unwrap();

        assert_eq!(settings.endpoint().username(), "admin");
        assert_eq!(settings.endpoint().host(), "db.example.com");
        assert_eq!(settings.endpoint().database(), "hubuum");
        assert!(!format!("{:?}", settings.endpoint()).contains("secret"));
    }

    #[test]
    fn unsupported_database_scheme_preserves_the_administrative_diagnostic() {
        let error = settings("mongodb://localhost/hubuum")
            .err()
            .expect("non-PostgreSQL scheme must be rejected");

        assert_eq!(error, PostgresPoolBuildError::UnsupportedDatabaseType);
        assert_eq!(error.to_string(), "Unsupported database type");
    }

    #[test]
    fn unsupported_sslmode_is_rejected_without_exposing_the_url() {
        let settings =
            settings("postgres://admin:secret@db.example.com/hubuum?sslmode=verify-full").unwrap();
        let error = build_postgres_pool(&settings).unwrap_err();

        assert_eq!(
            error.to_string(),
            "unsupported PostgreSQL sslmode 'verify-full'; expected disable, prefer, or require"
        );
        assert!(!error.to_string().contains("secret"));
    }
}
