#![allow(async_fn_in_trait)]

mod api;
mod auth;
mod config;
pub mod db;
mod errors;
pub mod events;
mod exports;
mod extractors;
mod lifecycle;
mod logger;
mod macros;
mod middlewares;
mod models;
mod observability;
mod pagination;
pub mod permissions;
mod schema;
mod tasks;
#[cfg(test)]
mod tests;
mod tls;
mod traits;
mod utilities;

use actix_web::{App, HttpServer, middleware::from_fn, web, web::Data, web::JsonConfig};
use db::{DatabasePoolSettings, init_pool_with_settings};
#[cfg(feature = "swagger-ui")]
use utoipa::OpenApi;
#[cfg(feature = "swagger-ui")]
use utoipa_swagger_ui::SwaggerUi;

use std::time::Duration;
use tracing::{info, warn};

use crate::api::openapi::openapi_json as openapi_json_handler;
#[cfg(test)]
use crate::config::get_config;
#[cfg(not(test))]
use crate::config::initialize_config;
use crate::config::running::RunningConfig;
use crate::config::token_hash_key_is_ephemeral;
use crate::errors::{
    EXIT_CODE_CONFIG_ERROR, EXIT_CODE_INIT_ERROR, EXIT_CODE_PERMISSION_BACKEND_ERROR,
    EXIT_CODE_TLS_ERROR, fatal_error, json_error_handler,
};
use crate::events::{
    ensure_event_delivery_worker_running, ensure_event_fanout_worker_running,
    ensure_event_retention_worker_running,
};
use crate::lifecycle::shutdown_background_workers;
use crate::permissions::{AppContext, build_permission_backend};
use crate::tasks::ensure_task_worker_running;
use crate::utilities::is_valid_log_level;

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

    if let Err(e) = utilities::init::init(pool.clone()).await {
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

    let server = HttpServer::new(move || {
        let app = App::new()
            .wrap(from_fn(middlewares::actor_context))
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
            .app_data(Data::new(app_pool.clone()))
            .app_data(Data::new(app_context.clone()))
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

    ensure_task_worker_running(pool.clone());
    ensure_event_fanout_worker_running(pool.clone());
    ensure_event_delivery_worker_running(pool.clone());
    ensure_event_retention_worker_running(pool.clone());

    info!(
        message = "server startup",
        version = env!("CARGO_PKG_VERSION"),
        git_sha = logger::build_git_sha(),
        bind_address = bind_address.as_str(),
        tls = config.tls_cert_path.is_some() && config.tls_key_path.is_some(),
        log_format = "json",
        log_level = config.log_level.as_str(),
        actix_workers = config.actix_workers,
        task_workers = config.task_workers,
        event_fanout_workers = config.event_fanout_workers,
        event_delivery_workers = config.event_delivery_workers,
        db_backend = "postgresql",
        authorization_backend = "database_permissions",
        active_event_sinks,
    );

    let result = server.workers(config.actix_workers).run().await;
    shutdown_background_workers(Duration::from_secs(30)).await;
    drop(pool);
    result
}
