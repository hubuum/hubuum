use std::time::Duration;

use actix_rt::time::sleep;
use tracing::{error, info};

use crate::config::{
    DEFAULT_TOKEN_LIFETIME_HOURS, DEFAULT_TOKEN_RETENTION_DAYS,
    DEFAULT_TOKEN_RETENTION_PURGE_BATCH_SIZE, DEFAULT_TOKEN_RETENTION_PURGE_ENABLED,
    DEFAULT_TOKEN_RETENTION_PURGE_INTERVAL_SECONDS, get_config,
};
use crate::db::DbPool;
use crate::db::traits::token_retention::{TokenRetentionSettings, purge_expired_token_batch};
use crate::errors::ApiError;
use crate::lifecycle::{ShutdownSignal, spawn_background_worker};
use crate::restores::{MaintenanceActivityGuard, maintenance_state};

static TOKEN_RETENTION_WORKER: std::sync::Once = std::sync::Once::new();

#[derive(Debug, Clone)]
struct TokenRetentionWorkerConfig {
    enabled: bool,
    settings: TokenRetentionSettings,
    interval: Duration,
}

fn configured_token_retention_worker() -> TokenRetentionWorkerConfig {
    get_config()
        .map(|config| TokenRetentionWorkerConfig {
            enabled: config.token_retention_purge_enabled,
            settings: TokenRetentionSettings {
                retention_days: config.token_retention_days,
                token_lifetime_hours: config.token_lifetime_hours,
                batch_size: config.token_retention_purge_batch_size,
            },
            interval: Duration::from_secs(config.token_retention_purge_interval_seconds),
        })
        .unwrap_or(TokenRetentionWorkerConfig {
            enabled: DEFAULT_TOKEN_RETENTION_PURGE_ENABLED,
            settings: TokenRetentionSettings {
                retention_days: DEFAULT_TOKEN_RETENTION_DAYS,
                token_lifetime_hours: DEFAULT_TOKEN_LIFETIME_HOURS,
                batch_size: DEFAULT_TOKEN_RETENTION_PURGE_BATCH_SIZE,
            },
            interval: Duration::from_secs(DEFAULT_TOKEN_RETENTION_PURGE_INTERVAL_SECONDS),
        })
}

pub async fn process_token_retention_batch(
    pool: &DbPool,
    settings: TokenRetentionSettings,
) -> Result<usize, ApiError> {
    let _activity = MaintenanceActivityGuard::begin();
    if maintenance_state(pool).await? != "normal" {
        return Ok(0);
    }
    purge_expired_token_batch(pool, settings).await
}

fn retention_worker_should_continue(result: &Result<usize, ApiError>) -> bool {
    match result {
        Ok(deleted) if *deleted > 0 => {
            info!(
                message = "Expired token retention batch completed",
                deleted_tokens = deleted
            );
            true
        }
        Ok(_) => false,
        Err(error) => {
            error!(message = "Token retention worker iteration failed", error = %error);
            false
        }
    }
}

async fn token_retention_worker_loop(
    pool: DbPool,
    config: TokenRetentionWorkerConfig,
    shutdown: ShutdownSignal,
) {
    loop {
        let result = tokio::select! {
            biased;
            _ = shutdown.requested() => break,
            result = process_token_retention_batch(&pool, config.settings) => result,
        };
        if retention_worker_should_continue(&result) {
            continue;
        }
        tokio::select! {
            biased;
            _ = shutdown.requested() => break,
            _ = sleep(config.interval) => {}
        }
    }
}

fn spawn_token_retention_worker_loop(pool: DbPool, config: TokenRetentionWorkerConfig) {
    spawn_background_worker("token-retention-worker", move |shutdown| {
        info!(
            message = "Starting token retention worker loop",
            retention_days = config.settings.retention_days,
            token_lifetime_hours = config.settings.token_lifetime_hours,
            batch_size = config.settings.batch_size,
            interval = ?config.interval,
        );
        let system = actix_rt::System::new();
        system.block_on(token_retention_worker_loop(pool, config, shutdown));
    });
}

pub fn ensure_token_retention_worker_running(pool: DbPool) {
    if get_config().is_ok_and(|config| !config.runtime_role.runs_background_workers()) {
        return;
    }
    let config = configured_token_retention_worker();
    if !config.enabled {
        return;
    }

    TOKEN_RETENTION_WORKER.call_once(move || {
        info!(
            message = "Initializing token retention worker",
            retention_days = config.settings.retention_days,
            token_lifetime_hours = config.settings.token_lifetime_hours,
            batch_size = config.settings.batch_size,
            interval = ?config.interval,
        );
        spawn_token_retention_worker_loop(pool, config);
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retention_worker_retries_immediately_after_deleting_rows() {
        assert!(retention_worker_should_continue(&Ok(1)));
    }

    #[test]
    fn retention_worker_sleeps_after_an_empty_batch() {
        assert!(!retention_worker_should_continue(&Ok(0)));
    }

    #[test]
    fn retention_worker_sleeps_after_an_error() {
        assert!(!retention_worker_should_continue(&Err(
            ApiError::InternalServerError("boom".to_string()),
        )));
    }
}
