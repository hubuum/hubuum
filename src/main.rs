#[cfg(test)]
#[path = "tests/container_build.rs"]
mod container_build;

use actix_web::{App, HttpServer, middleware::from_fn, web, web::Data, web::JsonConfig};
#[cfg(feature = "swagger-ui")]
use utoipa::OpenApi;
#[cfg(feature = "swagger-ui")]
use utoipa_swagger_ui::SwaggerUi;

use std::time::Duration;
use tracing::{error, info, warn};

use hubuum::api::openapi::openapi_json as openapi_json_handler;
use hubuum::backups::BackupSettings;
#[cfg(test)]
use hubuum::config::get_config;
#[cfg(not(test))]
use hubuum::config::initialize_config;
use hubuum::config::running::RunningConfig;
use hubuum::config::token_hash_key_is_ephemeral;
use hubuum::config::{AppConfig, LoginRateLimitBackendKind};
use hubuum::db::{DatabasePoolSettings, init_pool_with_settings};
use hubuum::errors::{
    EXIT_CODE_CONFIG_ERROR, EXIT_CODE_DATABASE_ERROR, EXIT_CODE_INIT_ERROR,
    EXIT_CODE_PERMISSION_BACKEND_ERROR, EXIT_CODE_TLS_ERROR, fatal_error, json_error_handler,
};
use hubuum::events::{
    ensure_event_delivery_worker_running, ensure_event_fanout_worker_running,
    ensure_event_retention_worker_running,
};
use hubuum::lifecycle::{
    background_worker_count, shutdown_background_workers, wait_for_background_worker_exit,
};
use hubuum::middlewares::rate_limit::{
    LoginRateLimitStoreSettings, initialize_login_rate_limit_store,
};
use hubuum::permissions::{AppContext, build_permission_backend};
use hubuum::restores::{RestoreSettings, ensure_restore_coordinator_running};
use hubuum::tasks::{
    TaskWorkerSettings, ensure_task_worker_running_with_settings, initialize_task_worker_settings,
};
use hubuum::utilities::is_valid_log_level;
use hubuum::{api, db, logger, middlewares, observability, tls, utilities};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    if let Err(e) = tls::install_default_crypto_provider() {
        fatal_error(
            &format!("Failed to initialize TLS cryptography: {e}"),
            EXIT_CODE_INIT_ERROR,
        );
    }

    #[cfg(not(test))]
    let config = match initialize_config() {
        Ok(cfg) => cfg.clone(),
        Err(e) => fatal_error(
            &format!("Failed to load configuration: {}", e),
            EXIT_CODE_CONFIG_ERROR,
        ),
    };
    #[cfg(test)]
    let config = match get_config() {
        Ok(cfg) => cfg.clone(),
        Err(e) => fatal_error(
            &format!("Failed to load configuration: {}", e),
            EXIT_CODE_CONFIG_ERROR,
        ),
    };
    if !is_valid_log_level(&config.log_level) {
        fatal_error(
            &format!("Invalid log level: {}", config.log_level),
            EXIT_CODE_CONFIG_ERROR,
        );
    }
    if let Err(err) = logger::init_json_logging(&config.log_level) {
        fatal_error(&err, EXIT_CODE_CONFIG_ERROR);
    }

    if token_hash_key_is_ephemeral() {
        warn!(
            message = "HUBUUM_TOKEN_HASH_KEY is not set; using ephemeral in-memory key. Existing tokens will be invalid after restart.",
            recommendation = "Set HUBUUM_TOKEN_HASH_KEY to a stable secret to preserve token validity across restarts"
        );
    }

    if config.metrics_enabled
        && let Err(e) = observability::metrics::init()
    {
        fatal_error(
            &format!("Failed to initialize metrics: {}", e),
            EXIT_CODE_INIT_ERROR,
        );
    }
    utilities::auth::initialize_dummy_password_hash();
    let database_settings = DatabasePoolSettings::builder(config.database_url.clone())
        .max_size(config.db_pool_size)
        .statement_timeout_ms(config.db_statement_timeout_ms)
        .acquire_timeout_ms(config.db_pool_acquire_timeout_ms)
        .build()
        .unwrap_or_else(|error| fatal_error(&error, EXIT_CODE_CONFIG_ERROR));
    let pool = init_pool_with_settings(&database_settings);
    db::ensure_database_schema_ready(&pool)
        .await
        .unwrap_or_else(|error| {
            fatal_error(
                &format!("Database schema is not ready: {error}"),
                EXIT_CODE_DATABASE_ERROR,
            )
        });

    let backup_settings = BackupSettings::new(
        config.backup_output_retention_hours,
        config.backup_max_active_tasks_per_user,
        config.backup_max_output_bytes,
    )
    .unwrap_or_else(|error| fatal_error(&error, EXIT_CODE_CONFIG_ERROR));
    let restore_settings = RestoreSettings::new(
        config.restore_stage_retention_minutes,
        config.restore_max_upload_bytes,
    )
    .unwrap_or_else(|error| fatal_error(&error, EXIT_CODE_CONFIG_ERROR));

    let task_worker_count = if config.runtime_role.runs_background_workers() {
        config.task_workers
    } else {
        0
    };
    let task_worker_settings = TaskWorkerSettings::new(
        task_worker_count,
        Duration::from_millis(config.task_poll_interval_ms),
        Duration::from_secs(config.task_lease_seconds),
        Duration::from_secs(config.task_heartbeat_seconds),
        Duration::from_secs(config.task_recovery_interval_seconds),
        Duration::from_secs(config.export_output_cleanup_interval_seconds),
    )
    .unwrap_or_else(|error| fatal_error(&error, EXIT_CODE_CONFIG_ERROR));
    initialize_task_worker_settings(task_worker_settings)
        .unwrap_or_else(|error| fatal_error(&error, EXIT_CODE_INIT_ERROR));

    let initialization_settings =
        utilities::init::InitializationSettings::new(config.admin_groupname.clone())
            .unwrap_or_else(|error| fatal_error(&error, EXIT_CODE_CONFIG_ERROR));
    if let Err(e) = utilities::init::init(pool.clone(), &initialization_settings).await {
        fatal_error(
            &format!("Critical database initialization failed: {}", e),
            EXIT_CODE_INIT_ERROR,
        );
    }

    let permission_backend = build_permission_backend(&config, pool.clone())
        .await
        .unwrap_or_else(|error| {
            fatal_error(
                &format!("Failed to initialize permission backend: {error}"),
                EXIT_CODE_PERMISSION_BACKEND_ERROR,
            )
        });
    let app_context = AppContext::new(pool.clone(), permission_backend);
    let authorization_backend = app_context.permission_backend().kind();

    // Every process that can serve or originate work must participate in the
    // restore drain barrier. In particular, API-only replicas need their own
    // heartbeat even though they do not run task or event workers.
    ensure_restore_coordinator_running(app_context.db_pool.clone());

    let active_event_sinks =
        match db::traits::event_subscription::enabled_event_sink_count(&pool).await {
            Ok(count) => Some(count),
            Err(error) => {
                warn!(
                    message = "failed to count active event sinks for startup metadata",
                    error = %error,
                );
                None
            }
        };

    if !config.runtime_role.serves_http() {
        start_background_workers(&app_context, &backup_settings);
        info!(
            message = "worker startup",
            version = env!("CARGO_PKG_VERSION"),
            git_sha = logger::build_git_sha(),
            runtime_role = config.runtime_role.as_str(),
            task_workers = config.task_workers,
            event_fanout_workers = config.event_fanout_workers,
            event_delivery_workers = config.event_delivery_workers,
            db_backend = "postgresql",
            authorization_backend,
            active_event_sinks,
        );
        let worker_exit = tokio::select! {
            shutdown = wait_for_shutdown_signal() => {
                shutdown?;
                None
            }
            exit = wait_for_background_worker_exit() => Some(exit),
        };
        if let Some(exit) = &worker_exit {
            error!(
                message = "Background worker supervision failed",
                reason = %exit,
            );
        }
        shutdown_background_workers(Duration::from_secs(30)).await;
        drop(app_context);
        drop(pool);
        if let Some(exit) = worker_exit {
            return Err(std::io::Error::other(format!(
                "Background worker supervision failed: {exit}"
            )));
        }
        return Ok(());
    }

    let login_rate_limit_store_settings = login_rate_limit_store_settings(&config)
        .unwrap_or_else(|error| fatal_error(&error, EXIT_CODE_CONFIG_ERROR));
    initialize_login_rate_limit_store(login_rate_limit_store_settings)
        .await
        .unwrap_or_else(|error| {
            fatal_error(
                &format!("Failed to initialize login rate-limit store: {error}"),
                EXIT_CODE_INIT_ERROR,
            )
        });

    let client_allowlist = config.client_allowlist.clone();
    let proxy_trust = middlewares::ProxyTrust::new(
        config.trust_ip_headers,
        config.trusted_proxies.nets().to_vec(),
        config.trusted_proxy_hops,
    );
    let running_config = RunningConfig::from(&config);
    let metrics_enabled = config.metrics_enabled;
    let metrics_path = config.metrics_path.clone();
    let app_pool = pool.clone();
    let server_app_context = app_context.clone();
    let background_worker_context = app_context.clone();
    let app_backup_settings = backup_settings.clone();
    let app_restore_settings = restore_settings.clone();

    let server = HttpServer::new(move || {
        let app = App::new()
            .wrap(from_fn(middlewares::actor_context))
            .wrap(from_fn(middlewares::reject_during_maintenance))
            // Actix runs the last registered middleware first. Reject disallowed
            // clients before bearer-token resolution can touch the database.
            .wrap(middlewares::ClientAllowlistMiddleware::new_with_trust(
                client_allowlist.clone(),
                proxy_trust.clone(),
            ))
            .wrap(middlewares::TracingMiddleware::new_with_trust(
                proxy_trust.clone(),
            ))
            .app_data(Data::new(proxy_trust.clone()))
            .app_data(Data::new(running_config.clone()))
            .app_data(Data::new(app_backup_settings.clone()))
            .app_data(Data::new(app_restore_settings.clone()))
            .app_data(Data::new(app_pool.clone()))
            .app_data(Data::new(server_app_context.clone()))
            .app_data(JsonConfig::default().error_handler(json_error_handler))
            .route("/api-doc/openapi.json", web::get().to(openapi_json_handler));

        let app = if metrics_enabled {
            app.route(
                metrics_path.as_str(),
                web::get().to(observability::metrics::scrape),
            )
        } else {
            app
        };

        #[cfg(feature = "swagger-ui")]
        let app = app.service(
            SwaggerUi::new("/swagger-ui/{_:.*}")
                .url("/api-doc/openapi.json", api::openapi::ApiDoc::openapi()),
        );

        app.configure(api::config)
    });

    let bind_address = format!("{}:{}", config.bind_ip, config.port);

    let server = match (&config.tls_cert_path, &config.tls_key_path) {
        (Some(cert), Some(key)) => match tls::configure_server(
            server,
            &bind_address,
            cert,
            key,
            config.tls_key_passphrase.as_deref(),
            config.tls_backend,
        ) {
            Ok(srv) => srv,
            Err(e) => fatal_error(
                &format!("Failed to configure TLS server: {}", e),
                EXIT_CODE_TLS_ERROR,
            ),
        },
        (Some(_), None) => fatal_error(
            "TLS certificate specified but key is missing. Please provide both --tls-cert-path and --tls-key-path",
            EXIT_CODE_TLS_ERROR,
        ),
        (None, Some(_)) => fatal_error(
            "TLS key specified but certificate is missing. Please provide both --tls-cert-path and --tls-key-path",
            EXIT_CODE_TLS_ERROR,
        ),
        _ => server.bind(&bind_address)?,
    };

    if config.runtime_role.runs_background_workers() {
        start_background_workers(&background_worker_context, &backup_settings);
    }

    info!(
        message = "server startup",
        version = env!("CARGO_PKG_VERSION"),
        git_sha = logger::build_git_sha(),
        runtime_role = config.runtime_role.as_str(),
        bind_address = bind_address.as_str(),
        tls = config.tls_cert_path.is_some() && config.tls_key_path.is_some(),
        log_format = "json",
        log_level = config.log_level.as_str(),
        actix_workers = config.actix_workers,
        task_workers = config.task_workers,
        event_fanout_workers = config.event_fanout_workers,
        event_delivery_workers = config.event_delivery_workers,
        db_backend = "postgresql",
        authorization_backend,
        login_rate_limit_backend = config.login_rate_limit_backend.as_str(),
        active_event_sinks,
    );

    let server = server.workers(config.actix_workers).run();
    let result = if background_worker_count() > 0 {
        tokio::select! {
            result = server => result,
            exit = wait_for_background_worker_exit() => {
                error!(
                    message = "Background worker supervision failed",
                    reason = %exit,
                );
                Err(std::io::Error::other(format!(
                    "Background worker supervision failed: {exit}"
                )))
            }
        }
    } else {
        server.await
    };
    shutdown_background_workers(Duration::from_secs(30)).await;
    drop(app_context);
    drop(pool);
    result
}

fn start_background_workers(context: &AppContext, backup_settings: &BackupSettings) {
    ensure_task_worker_running_with_settings(context.clone(), backup_settings.clone());
    ensure_event_fanout_worker_running(context.db_pool.clone());
    ensure_event_delivery_worker_running(context.db_pool.clone());
    ensure_event_retention_worker_running(context.db_pool.clone());
}

fn login_rate_limit_store_settings(
    config: &AppConfig,
) -> Result<LoginRateLimitStoreSettings, String> {
    match config.login_rate_limit_backend {
        LoginRateLimitBackendKind::Memory => Ok(LoginRateLimitStoreSettings::in_memory()),
        LoginRateLimitBackendKind::Valkey => {
            #[cfg(feature = "login-rate-limit-valkey")]
            {
                let url = config.login_rate_limit_valkey_url.clone().ok_or_else(|| {
                    "login rate-limit Valkey URL is required for the Valkey backend".to_string()
                })?;
                LoginRateLimitStoreSettings::valkey(
                    url,
                    config.login_rate_limit_valkey_prefix.clone(),
                    Duration::from_millis(config.login_rate_limit_valkey_io_timeout_ms),
                )
            }
            #[cfg(not(feature = "login-rate-limit-valkey"))]
            {
                Err("the Valkey login rate-limit backend is not compiled in".to_string())
            }
        }
    }
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() -> std::io::Result<()> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result,
        _ = terminate.recv() => Ok(()),
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() -> std::io::Result<()> {
    tokio::signal::ctrl_c().await
}
