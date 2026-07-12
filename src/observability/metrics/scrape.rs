use actix_web::{HttpResponse, Responder, http::header, web};
use prometheus::{Encoder, TextEncoder};

use crate::db::DbPool;
use crate::errors::ApiError;

use super::Metrics;
use super::{db, event, get, inventory, login, task};

pub async fn scrape(pool: web::Data<DbPool>) -> Result<impl Responder, ApiError> {
    let metrics = get()?;
    refresh_scrape_gauges(metrics, &pool).await;

    let encoder = TextEncoder::new();
    let metric_families = metrics.registry.gather();
    let mut body = Vec::new();
    encoder
        .encode(&metric_families, &mut body)
        .map_err(|error| {
            ApiError::InternalServerError(format!("Failed to encode metrics: {error}"))
        })?;

    Ok(HttpResponse::Ok()
        .insert_header((header::CONTENT_TYPE, encoder.format_type()))
        .body(body))
}

async fn refresh_scrape_gauges(metrics: &Metrics, pool: &DbPool) {
    db::refresh_pool_gauges(metrics, pool);
    login::refresh_login_limiter_gauges(metrics).await;
    if let Ok(_refresh_guard) = metrics.db_refresh_lock.try_lock() {
        inventory::refresh_inventory_gauges(metrics, pool).await;
        task::refresh_task_gauges(metrics, pool).await;
        event::refresh_event_gauges(metrics, pool).await;
    }
}
