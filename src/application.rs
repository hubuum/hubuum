use actix_web::{
    App, HttpServer,
    dev::{HttpServiceFactory, Server},
    middleware::from_fn,
    web,
    web::Data,
    web::JsonConfig,
};
#[cfg(feature = "swagger-ui")]
use utoipa::OpenApi;
#[cfg(feature = "swagger-ui")]
use utoipa_swagger_ui::SwaggerUi;

use std::time::Duration;
use tracing::{error, info, warn};

use crate::api::openapi::openapi_json as openapi_json_handler;
use crate::backups::BackupSettings;
#[cfg(test)]
use crate::config::get_config;
#[cfg(not(test))]
use crate::config::initialize_config;
use crate::config::running::RunningConfig;
use crate::config::token_hash_key_is_ephemeral;
use crate::config::{AppConfig, ClientAllowlist, LoginRateLimitBackendKind, MetricsPath};
use crate::errors::{
    EXIT_CODE_CONFIG_ERROR, EXIT_CODE_DATABASE_ERROR, EXIT_CODE_INIT_ERROR,
    EXIT_CODE_PERMISSION_BACKEND_ERROR, EXIT_CODE_TLS_ERROR, fatal_error, json_error_handler,
};
use crate::events::{
    ensure_event_delivery_worker_running, ensure_event_fanout_worker_running,
    ensure_event_retention_worker_running,
};
use crate::lifecycle::{
    background_worker_count, shutdown_background_workers, wait_for_background_worker_exit,
};
use crate::middlewares::rate_limit::{
    LoginRateLimitStoreSettings, initialize_login_rate_limit_store,
};
use crate::permissions::{AppContext, build_permission_backend};
use crate::restores::{RestoreSettings, ensure_restore_coordinator_running};
use crate::services::event_administration::count_enabled_event_sinks;
use crate::storage::{
    OperationalStateStorage, StorageBackendKind, StorageSettings, initialize_storage,
};
use crate::tasks::{ensure_task_worker_running_with_settings, initialize_task_worker_settings};
use crate::token_retention::ensure_token_retention_worker_running;
use crate::utilities::is_valid_log_level;
use crate::{api, logger, middlewares, observability, tls, utilities};

/// Build and run the configured Hubuum process until it shuts down.
///
/// This is the workspace-internal composition boundary used by the server
/// binary. It is not a supported third-party embedding API.
pub async fn run_runtime_from_environment() -> std::io::Result<()> {
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

    if config.metrics_enabled {
        if let Err(e) = observability::metrics::init() {
            fatal_error(
                &format!("Failed to initialize metrics: {}", e),
                EXIT_CODE_INIT_ERROR,
            );
        }
        observability::metrics::runtime_identity(config.runtime_role);
    }
    utilities::auth::initialize_dummy_password_hash();
    let storage_settings = match config.storage_backend {
        StorageBackendKind::Postgres => StorageSettings::postgres(config.database_url.clone())
            .max_connections(config.db_pool_size)
            .statement_timeout_ms(config.db_statement_timeout_ms)
            .acquire_timeout_ms(config.db_pool_acquire_timeout_ms)
            .build()
            .unwrap_or_else(|error| fatal_error(&error.to_string(), EXIT_CODE_CONFIG_ERROR)),
        StorageBackendKind::Memory => StorageSettings::memory(),
    };
    let storage = initialize_storage(&storage_settings)
        .unwrap_or_else(|error| fatal_error(&error.to_string(), EXIT_CODE_CONFIG_ERROR));
    let readiness = storage
        .get_readiness_snapshot()
        .await
        .unwrap_or_else(|error| {
            fatal_error(
                &format!("Storage backend is not ready: {error}"),
                EXIT_CODE_DATABASE_ERROR,
            )
        });
    if !readiness.storage_is_ready() {
        fatal_error(
            "Storage backend schema is not ready",
            EXIT_CODE_DATABASE_ERROR,
        );
    }

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

    let task_worker_settings = config
        .task_worker_settings()
        .unwrap_or_else(|error| fatal_error(&error.to_string(), EXIT_CODE_CONFIG_ERROR));
    initialize_task_worker_settings(task_worker_settings)
        .unwrap_or_else(|error| fatal_error(&error, EXIT_CODE_INIT_ERROR));

    let initialization_settings =
        utilities::init::InitializationSettings::new(config.admin_groupname.clone())
            .unwrap_or_else(|error| fatal_error(&error, EXIT_CODE_CONFIG_ERROR));
    if let Err(e) = utilities::init::init(&storage, &initialization_settings).await {
        fatal_error(
            &format!("Critical storage initialization failed: {}", e),
            EXIT_CODE_INIT_ERROR,
        );
    }

    let permission_backend = build_permission_backend(&config, storage.clone())
        .await
        .unwrap_or_else(|error| {
            fatal_error(
                &format!("Failed to initialize permission backend: {error}"),
                EXIT_CODE_PERMISSION_BACKEND_ERROR,
            )
        });
    let app_context = AppContext::new(storage, permission_backend);
    let storage_backend = app_context.storage_backend_descriptor();
    if config.metrics_enabled {
        observability::metrics::storage_backend_identity(storage_backend.kind().as_str());
    }
    let authorization_backend = app_context.permission_backend().kind();

    // Every process that can serve or originate work must participate in the
    // restore drain barrier. In particular, API-only replicas need their own
    // heartbeat even though they do not run task or event workers.
    ensure_restore_coordinator_running(app_context.clone_backend());

    let active_event_sinks = match count_enabled_event_sinks(&app_context).await {
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
        let metrics_server = start_worker_metrics_server(&config, app_context.clone())?;
        start_background_workers(&app_context, &backup_settings);
        info!(
            message = "worker startup",
            version = env!("CARGO_PKG_VERSION"),
            git_sha = logger::build_git_sha(),
            runtime_role = config.runtime_role.as_str(),
            task_workers = config.task_workers,
            event_fanout_workers = config.event_fanout_workers,
            event_delivery_workers = config.event_delivery_workers,
            db_backend = storage_backend.kind().as_str(),
            storage_backend = storage_backend.kind().as_str(),
            authorization_backend,
            active_event_sinks,
            metrics_listener = metrics_server.is_some(),
        );
        let supervision_result = supervise_worker_process(metrics_server).await;
        if let Err(error) = &supervision_result {
            error!(
                message = "Worker process supervision failed",
                reason = %error,
            );
        }
        shutdown_background_workers(Duration::from_secs(30)).await;
        drop(app_context);
        supervision_result?;
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
    let running_config = RunningConfig::from_app_config_and_storage(&config, storage_backend);
    let metrics_enabled = config.metrics_enabled;
    let metrics_path = config.metrics_path.clone();
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
        db_backend = storage_backend.kind().as_str(),
        storage_backend = storage_backend.kind().as_str(),
        authorization_backend,
        login_rate_limit_backend = config.login_rate_limit_backend.as_str(),
        active_event_sinks,
    );

    let server = server.workers(config.actix_workers).run();
    let result = if background_worker_count() > 0 {
        tokio::select! {
            result = server => result,
            error = wait_for_background_worker_failure() => {
                error!(
                    message = "Background worker supervision failed",
                    reason = %error,
                );
                Err(error)
            }
        }
    } else {
        server.await
    };
    shutdown_background_workers(Duration::from_secs(30)).await;
    drop(app_context);
    result
}

fn start_background_workers(context: &AppContext, backup_settings: &BackupSettings) {
    ensure_task_worker_running_with_settings(context.clone(), backup_settings.clone());
    ensure_event_fanout_worker_running(context.clone_backend());
    ensure_event_delivery_worker_running(context.clone_backend());
    ensure_event_retention_worker_running(context.clone_backend());
    ensure_token_retention_worker_running(context.clone_backend());
}

async fn wait_for_background_worker_failure() -> std::io::Error {
    let exit = wait_for_background_worker_exit().await;
    std::io::Error::other(format!("Background worker supervision failed: {exit}"))
}

async fn supervise_worker_process(metrics_server: Option<Server>) -> std::io::Result<()> {
    let Some(metrics_server) = metrics_server else {
        return tokio::select! {
            shutdown = wait_for_shutdown_signal() => shutdown,
            error = wait_for_background_worker_failure() => Err(error),
        };
    };

    let metrics_server_handle = metrics_server.handle();
    let result = tokio::select! {
        shutdown = wait_for_shutdown_signal() => shutdown,
        error = wait_for_background_worker_failure() => Err(error),
        result = metrics_server => match result {
            Ok(()) => Err(std::io::Error::other(
                "Worker metrics HTTP server stopped unexpectedly",
            )),
            Err(error) => Err(error),
        },
    };
    metrics_server_handle.stop(true).await;
    result
}

fn worker_metrics_service(
    client_allowlist: ClientAllowlist,
    proxy_trust: middlewares::ProxyTrust,
    metrics_path: MetricsPath,
    context: AppContext,
) -> impl HttpServiceFactory {
    web::scope("")
        .wrap(middlewares::ClientAllowlistMiddleware::new_with_trust(
            client_allowlist,
            proxy_trust.clone(),
        ))
        .wrap(middlewares::TracingMiddleware::new_with_trust(proxy_trust))
        .app_data(Data::new(context))
        .route(
            metrics_path.as_str(),
            web::get().to(observability::metrics::scrape),
        )
}

fn start_worker_metrics_server(
    config: &AppConfig,
    context: AppContext,
) -> std::io::Result<Option<Server>> {
    if !config.metrics_enabled {
        return Ok(None);
    }

    let client_allowlist = config.client_allowlist.clone();
    let proxy_trust = middlewares::ProxyTrust::new(
        config.trust_ip_headers,
        config.trusted_proxies.nets().to_vec(),
        config.trusted_proxy_hops,
    );
    let metrics_path = config.metrics_path.clone();
    let server = HttpServer::new(move || {
        App::new().service(worker_metrics_service(
            client_allowlist.clone(),
            proxy_trust.clone(),
            metrics_path.clone(),
            context.clone(),
        ))
    });
    let bind_address = format!("{}:{}", config.bind_ip, config.port);
    let server = match (&config.tls_cert_path, &config.tls_key_path) {
        (Some(cert), Some(key)) => tls::configure_server(
            server,
            &bind_address,
            cert,
            key,
            config.tls_key_passphrase.as_deref(),
            config.tls_backend,
        )?,
        (Some(_), None) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "TLS certificate specified but key is missing",
            ));
        }
        (None, Some(_)) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "TLS key specified but certificate is missing",
            ));
        }
        _ => server.bind(&bind_address)?,
    };

    // The worker supervisor owns SIGINT/SIGTERM handling and initiates a
    // graceful stop through the server handle. Letting Actix install a second
    // signal handler creates a race that can misclassify normal shutdown as an
    // unexpected metrics-server exit.
    Ok(Some(server.disable_signals().workers(1).run()))
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use actix_web::{http::StatusCode, test as actix_test};

    use crate::permissions::LocalPermissionBackend;
    use crate::storage::StorageHandle;
    use hubuum_storage_postgres::{PostgresPool, PostgresPoolSettings, build_postgres_pool};

    use super::*;

    fn unreachable_pool() -> PostgresPool {
        let settings = PostgresPoolSettings::builder(
            "postgres://hubuum:hubuum@127.0.0.1:1/hubuum_worker_metrics_unreachable",
        )
        .max_size(1)
        .statement_timeout_ms(0)
        .acquire_timeout_ms(5)
        .build()
        .expect("unreachable test pool settings should be valid");
        build_postgres_pool(&settings).expect("unreachable test pool must be constructible")
    }

    fn unreachable_context() -> AppContext {
        let pool = unreachable_pool();
        crate::tests::app_context_with_permission_backend(
            pool.clone(),
            Arc::new(LocalPermissionBackend::new(
                StorageHandle::postgres(pool),
                "admin".to_string(),
            )),
        )
    }

    #[actix_web::test]
    async fn worker_metrics_service_instruments_scrape_requests() {
        observability::metrics::init().expect("metrics should initialize");
        let app = actix_test::init_service(App::new().service(worker_metrics_service(
            ClientAllowlist::Any,
            middlewares::ProxyTrust::peer_only(),
            MetricsPath::new("/metrics").expect("metrics path should be valid"),
            unreachable_context(),
        )))
        .await;
        let metrics_request = || {
            actix_test::TestRequest::get()
                .uri("/metrics")
                .peer_addr(
                    "127.0.0.1:4242"
                        .parse()
                        .expect("test peer address should parse"),
                )
                .to_request()
        };

        let response = actix_test::call_service(&app, metrics_request()).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().contains_key("x-request-id"));
        actix_test::read_body(response).await;

        let response = actix_test::call_service(&app, metrics_request()).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().contains_key("x-request-id"));
        let body = actix_test::read_body(response).await;
        let body = std::str::from_utf8(&body).expect("metrics body should be UTF-8");

        assert!(body.contains(
            "hubuum_http_requests_total{method=\"GET\",route=\"/metrics\",status_code=\"200\",status_family=\"2xx\"}"
        ));
        assert!(body.contains("hubuum_http_requests_in_flight{route=\"/metrics\"} 1"));
    }
}
