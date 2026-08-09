use std::time::Duration;

use actix_rt::time::sleep;
use tracing::{error, info};

use crate::config::get_config;
use crate::db::traits::token_retention::purge_expired_token_batch;
use crate::db::{DbCallSite, DbPool, with_db_call_site};
use crate::errors::ApiError;
use crate::lifecycle::{ShutdownSignal, spawn_background_worker};
use crate::models::TokenRetentionSettings;
use crate::restores::MaintenanceActivityGuard;
use crate::traits::{BackendContext, backend_pool};

static TOKEN_RETENTION_WORKER: std::sync::Once = std::sync::Once::new();

#[derive(Debug, Clone)]
struct TokenRetentionWorkerConfig {
    enabled: bool,
    settings: TokenRetentionSettings,
    interval: Duration,
}

fn configured_token_retention_worker() -> Result<TokenRetentionWorkerConfig, ApiError> {
    let config = get_config()?;
    Ok(TokenRetentionWorkerConfig {
        enabled: config.token_retention_purge_enabled,
        settings: config.token_retention_settings()?,
        interval: Duration::from_secs(config.token_retention_purge_interval_seconds),
    })
}

pub async fn process_token_retention_batch(
    pool: &DbPool,
    settings: TokenRetentionSettings,
) -> Result<usize, ApiError> {
    let _activity = MaintenanceActivityGuard::begin();
    purge_expired_token_batch(pool, settings).await
}

fn retention_worker_should_continue(result: &Result<usize, ApiError>) -> bool {
    match result {
        Ok(deleted) if *deleted > 0 => {
            info!(
                message = "Terminal token retention batch completed",
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
            result = with_db_call_site(
                DbCallSite::TokenRetention,
                process_token_retention_batch(&pool, config.settings),
            ) => result,
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
            retention_days = config.settings.retention_period().days(),
            token_lifetime_hours = config.settings.token_lifetime().hours(),
            batch_size = config.settings.batch_size().get(),
            interval = ?config.interval,
        );
        let system = actix_rt::System::new();
        system.block_on(token_retention_worker_loop(pool, config, shutdown));
    });
}

pub fn ensure_token_retention_worker_running<C>(backend: C)
where
    C: BackendContext,
{
    let pool = backend_pool(&backend).clone();
    if get_config().is_ok_and(|config| !config.runtime_role.runs_background_workers()) {
        return;
    }
    let config = match configured_token_retention_worker() {
        Ok(config) => config,
        Err(error) => {
            error!(
                message = "Token retention worker configuration is invalid",
                error = %error
            );
            return;
        }
    };
    if !config.enabled {
        return;
    }

    TOKEN_RETENTION_WORKER.call_once(move || {
        info!(
            message = "Initializing token retention worker",
            retention_days = config.settings.retention_period().days(),
            token_lifetime_hours = config.settings.token_lifetime().hours(),
            batch_size = config.settings.batch_size().get(),
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
