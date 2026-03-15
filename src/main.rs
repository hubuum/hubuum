mod api;
mod config;
mod db;
mod errors;
mod extractors;
mod logger;
mod macros;
mod middlewares;
mod models;
mod pagination;
mod schema;
mod tasks;
mod tests;
mod tls;
mod traits;
mod utilities;

use actix_web::{App, HttpServer, middleware::Logger, web, web::Data, web::JsonConfig};
use db::init_pool;
#[cfg(feature = "swagger-ui")]
use utoipa::OpenApi;
#[cfg(feature = "swagger-ui")]
use utoipa_swagger_ui::SwaggerUi;

use tracing::{debug, info, warn};
use tracing_subscriber::{
    filter::EnvFilter, fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt,
};

use crate::api::openapi::openapi_json as openapi_json_handler;
use crate::config::{get_config, token_hash_key_is_ephemeral};
use crate::errors::{
    EXIT_CODE_CONFIG_ERROR, EXIT_CODE_INIT_ERROR, EXIT_CODE_TLS_ERROR, fatal_error,
    json_error_handler,
};
use crate::tasks::ensure_task_worker_running;
use crate::utilities::is_valid_log_level;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Clone the config to prevent the mutex from being locked
    // See https://rust-lang.github.io/rust-clippy/master/index.html#await_holding_lock
    let config = match get_config() {
        Ok(cfg) => cfg.clone(),
        Err(e) => fatal_error(
            &format!("Failed to load configuration: {}", e),
            EXIT_CODE_CONFIG_ERROR,
        ),
    };
    let filter = if is_valid_log_level(&config.log_level) {
        EnvFilter::try_new(&config.log_level).unwrap_or_else(|_e| {
            fatal_error(
                &format!("Error parsing log level: {}", &config.log_level),
                EXIT_CODE_CONFIG_ERROR,
            )
        })
    } else {
        fatal_error(
            &format!("Invalid log level: {}", config.log_level),
            EXIT_CODE_CONFIG_ERROR,
        )
    };

    tracing_subscriber::registry()
        .with(filter)
        .with(
            tracing_subscriber::fmt::layer()
                .json()
                .with_span_events(FmtSpan::CLOSE)
                .event_format(logger::HubuumLoggingFormat),
        )
        .init();

    if token_hash_key_is_ephemeral() {
        warn!(
            message = "HUBUUM_TOKEN_HASH_KEY is not set; using ephemeral in-memory key. Existing tokens will be invalid after restart.",
            recommendation = "Set HUBUUM_TOKEN_HASH_KEY to a stable secret to preserve token validity across restarts"
        );
    }

    debug!(
        message = "Starting server",
        bind_ip = %config.bind_ip,
        port = config.port,
        ssl = config.tls_cert_path.is_some() && config.tls_key_path.is_some(),
        log_level = %config.log_level,
        actix_workers = config.actix_workers,
        task_workers = config.task_workers,
        task_poll_interval_ms = config.task_poll_interval_ms,
        db_pool_size = config.db_pool_size,
    );

    let pool = init_pool(&config.database_url, config.db_pool_size);

    if let Err(e) = utilities::init::init(pool.clone()).await {
        fatal_error(
            &format!("Critical database initialization failed: {}", e),
            EXIT_CODE_INIT_ERROR,
        );
    }

    ensure_task_worker_running(pool.clone());

    let client_allowlist = config.client_allowlist.clone();
    let trust_ip_headers = config.trust_ip_headers;

    let server = HttpServer::new(move || {
        let app = App::new()
            .wrap(middlewares::ClientAllowlistMiddleware::new_with_trust(
                client_allowlist.clone(),
                trust_ip_headers,
            ))
            .wrap(middlewares::TracingMiddleware::new_with_trust(
                trust_ip_headers,
            ))
            .wrap(Logger::default())
            .app_data(Data::new(pool.clone()))
            .app_data(JsonConfig::default().error_handler(json_error_handler))
            .route("/api-doc/openapi.json", web::get().to(openapi_json_handler));

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
        _ => {
            info!("Server binding to http://{}", bind_address);
            server.bind(bind_address)?
        }
    };

    server.workers(config.actix_workers).run().await
}
