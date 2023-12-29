use actix_web::{web::Data, App, HttpServer};

use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use actix_web::middleware::Logger;

mod api;
mod config;
mod db;
mod errors;
mod extractors;
mod middlewares;
mod models;
mod schema;
mod utilities;

use db::connection::init_pool;
use tracing::{debug, warn};

use crate::config::AppConfig;
use crate::utilities::is_valid_log_level;
use clap::Parser;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let config = AppConfig::parse();
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
                .json() // Enable JSON output
                .with_span_events(FmtSpan::CLOSE),
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

    let database_url: String = config.database_url;
    let pool = init_pool(&database_url, config.db_pool_size);

    utilities::init::init(pool.clone()).await;

    HttpServer::new(move || {
        App::new()
            .wrap(middlewares::tracing::TracingMiddleware)
            .wrap(Logger::default())
            .app_data(Data::new(pool.clone()))
            .configure(api::config)
    })
    .bind(format!("{}:{}", config.bind_ip, config.port))?
    .workers(config.actix_workers)
    .run()
    .await
}
