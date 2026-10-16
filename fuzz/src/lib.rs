//! Configuration checks for the standalone native fuzz fixture.

#[cfg(any(test, feature = "postgres"))]
use tokio_postgres::{Config, config::Host};

/// Parse the dedicated fixture URL and reject non-loopback destinations.
#[cfg(any(test, feature = "postgres"))]
pub fn database_config(url: &str) -> Result<Config, &'static str> {
    let config: Config = url.parse().map_err(|_| "invalid fuzz database URL")?;
    if config.get_dbname() != Some("hubuum_regex_fuzz") {
        return Err("the fuzz database must be named hubuum_regex_fuzz");
    }
    if config.get_hosts().is_empty()
        || !config.get_hosts().iter().all(|host| {
            matches!(host, Host::Tcp(name) if matches!(name.as_str(), "localhost" | "127.0.0.1" | "::1"))
        })
        || !config.get_hostaddrs().iter().all(|ip| ip.is_loopback())
    {
        return Err("the fuzz database must use a loopback host and hostaddr");
    }
    Ok(config)
}

#[cfg(test)]
mod tests;
