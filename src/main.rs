#[macro_use]
extern crate lazy_static;

mod api;
mod config;
mod db;
mod errors;
mod extractors;
mod logger;
mod macros;
mod middlewares;
mod models;
mod schema;
mod tests;
mod traits;
mod utilities;

use actix_web::{middleware::Logger, web::Data, web::JsonConfig, App, HttpServer};
use db::init_pool;
use tracing::{debug, warn};
use tracing_subscriber::{
    filter::EnvFilter, fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt,
};

use crate::config::get_config;
use crate::errors::json_error_handler;
use crate::utilities::is_valid_log_level;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Clone the config to prevent the mutex from being locked
    // See https://rust-lang.github.io/rust-clippy/master/index.html#await_holding_lock
    let config = get_config().await.clone();
    let filter = if is_valid_log_level(&config.log_level) {
        EnvFilter::try_new(&config.log_level).unwrap_or_else(|_e| {
            warn!("Error parsing log level: {}", &config.log_level);
            std::process::exit(1);
        })
    } else {
        warn!("Invalid log level: {}", config.log_level);
        std::process::exit(1);
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

    debug!(
        message = "Starting server",
        bind_ip = %config.bind_ip,
        port = config.port,
        log_level = %config.log_level,
        actix_workers = config.actix_workers,
        db_pool_size = config.db_pool_size,
    );

    let pool = init_pool(&config.database_url, config.db_pool_size);

    utilities::init::init(pool.clone()).await;

    HttpServer::new(move || {
        App::new()
            .wrap(middlewares::tracing::TracingMiddleware)
            .wrap(Logger::default())
            .app_data(Data::new(pool.clone()))
            .app_data(JsonConfig::default().error_handler(json_error_handler))
            .configure(api::config)
    })
    .bind(format!("{}:{}", config.bind_ip, config.port))?
    .workers(config.actix_workers)
    .run()
    .await
}
