use actix_web::{web::Data, App, HttpServer};

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

use crate::db::connection::init_pool;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .json() // Enable JSON output
                .with_span_events(FmtSpan::CLOSE),
        )
        .init();

    let database_url =
        std::env::var("HUBUUM_DATABASE_URL").expect("HUBUUM_DATABASE_URL must be set");
    let pool = init_pool(&database_url);

    utilities::init::init(pool.clone()).await;

    HttpServer::new(move || {
        App::new()
            .wrap(middlewares::tracing::TracingMiddleware)
            .wrap(Logger::default())
            .app_data(Data::new(pool.clone()))
            .configure(api::config)
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}
