use crate::api as prod_api;
use crate::backups::BackupSettings;
use crate::config::{
    DEFAULT_BACKUP_MAX_ACTIVE_TASKS_PER_USER, DEFAULT_BACKUP_MAX_OUTPUT_BYTES,
    DEFAULT_BACKUP_OUTPUT_RETENTION_HOURS, DEFAULT_RESTORE_MAX_UPLOAD_BYTES,
    DEFAULT_RESTORE_STAGE_RETENTION_MINUTES,
};
use crate::db::DbPool;
use crate::middlewares::tracing::TracingMiddleware;
use crate::permissions::{AppContext, LocalPermissionBackend};
use crate::restores::RestoreSettings;
use actix_web::{App, http, test, web::Data};
use serde::Serialize;
use std::sync::Arc;

pub fn app_context(pool: &DbPool) -> Data<AppContext> {
    let config = crate::tests::integration_test_config()
        .expect("integration test configuration must be valid");
    let permissions = LocalPermissionBackend::new(pool.clone(), config.admin_groupname.clone());
    Data::new(AppContext::new(pool.clone(), Arc::new(permissions)))
}

fn create_token_header(token: &str) -> (http::header::HeaderName, String) {
    (http::header::AUTHORIZATION, format!("Bearer {token}"))
}

fn backup_settings() -> BackupSettings {
    BackupSettings::new(
        DEFAULT_BACKUP_OUTPUT_RETENTION_HOURS,
        DEFAULT_BACKUP_MAX_ACTIVE_TASKS_PER_USER,
        DEFAULT_BACKUP_MAX_OUTPUT_BYTES,
    )
    .expect("default backup settings must be valid")
}

fn restore_settings() -> RestoreSettings {
    RestoreSettings::new(
        DEFAULT_RESTORE_STAGE_RETENTION_MINUTES,
        DEFAULT_RESTORE_MAX_UPLOAD_BYTES,
    )
    .expect("default restore settings must be valid")
}

pub async fn get_request_with_correlation(
    pool: &DbPool,
    token: &str,
    endpoint: &str,
    correlation_id: Option<&str>,
) -> actix_web::dev::ServiceResponse {
    let headers = correlation_id
        .map(|value| {
            vec![(
                http::header::HeaderName::from_static("x-correlation-id"),
                value.to_string(),
            )]
        })
        .unwrap_or_default();
    get_request_with_headers(pool, token, endpoint, headers).await
}

pub async fn get_request_with_headers(
    pool: &DbPool,
    token: &str,
    endpoint: &str,
    headers: Vec<(http::header::HeaderName, String)>,
) -> actix_web::dev::ServiceResponse {
    let app = test::init_service(
        App::new()
            .wrap(actix_web::middleware::from_fn(
                crate::middlewares::actor_context,
            ))
            .wrap(TracingMiddleware::new())
            .app_data(Data::new(backup_settings()))
            .app_data(Data::new(restore_settings()))
            .app_data(Data::new(pool.clone()))
            .app_data(app_context(pool))
            .configure(prod_api::config),
    )
    .await;

    let mut request = test::TestRequest::get()
        .insert_header(create_token_header(token))
        .uri(endpoint);
    for (name, value) in headers {
        request = request.insert_header((name, value));
    }
    request.send_request(&app).await.map_into_boxed_body()
}

pub async fn get_request(
    pool: &DbPool,
    token: &str,
    endpoint: &str,
) -> actix_web::dev::ServiceResponse {
    get_request_with_correlation(pool, token, endpoint, None).await
}

pub async fn post_request_with_headers<T>(
    pool: &DbPool,
    token: &str,
    endpoint: &str,
    content: T,
    headers: Vec<(http::header::HeaderName, String)>,
) -> actix_web::dev::ServiceResponse
where
    T: Serialize,
{
    let app = test::init_service(
        App::new()
            .wrap(actix_web::middleware::from_fn(
                crate::middlewares::actor_context,
            ))
            .wrap(TracingMiddleware::new())
            .app_data(Data::new(backup_settings()))
            .app_data(Data::new(restore_settings()))
            .app_data(Data::new(pool.clone()))
            .app_data(app_context(pool))
            .configure(prod_api::config),
    )
    .await;

    let mut req = test::TestRequest::post()
        .insert_header(create_token_header(token))
        .uri(endpoint);

    for (name, value) in headers {
        req = req.insert_header((name, value));
    }

    req.set_json(&content)
        .send_request(&app)
        .await
        .map_into_boxed_body()
}

pub async fn post_request<T>(
    pool: &DbPool,
    token: &str,
    endpoint: &str,
    content: T,
) -> actix_web::dev::ServiceResponse
where
    T: Serialize,
{
    post_request_with_headers(pool, token, endpoint, content, vec![]).await
}

pub async fn delete_request(
    pool: &DbPool,
    token: &str,
    endpoint: &str,
) -> actix_web::dev::ServiceResponse {
    let app = test::init_service(
        App::new()
            .wrap(actix_web::middleware::from_fn(
                crate::middlewares::actor_context,
            ))
            .wrap(TracingMiddleware::new())
            .app_data(Data::new(pool.clone()))
            .app_data(app_context(pool))
            .configure(prod_api::config),
    )
    .await;

    test::TestRequest::delete()
        .insert_header(create_token_header(token))
        .uri(endpoint)
        .send_request(&app)
        .await
        .map_into_boxed_body()
}

pub async fn patch_request<T>(
    pool: &DbPool,
    token: &str,
    endpoint: &str,
    content: T,
) -> actix_web::dev::ServiceResponse
where
    T: Serialize,
{
    let app = test::init_service(
        App::new()
            .wrap(actix_web::middleware::from_fn(
                crate::middlewares::actor_context,
            ))
            .wrap(TracingMiddleware::new())
            .app_data(Data::new(pool.clone()))
            .app_data(app_context(pool))
            .configure(prod_api::config),
    )
    .await;

    test::TestRequest::patch()
        .insert_header(create_token_header(token))
        .uri(endpoint)
        .set_json(&content) // Make sure to reference content
        .send_request(&app)
        .await
        .map_into_boxed_body()
}

pub async fn put_request<T>(
    pool: &DbPool,
    token: &str,
    endpoint: &str,
    content: T,
) -> actix_web::dev::ServiceResponse
where
    T: Serialize,
{
    let app = test::init_service(
        App::new()
            .wrap(actix_web::middleware::from_fn(
                crate::middlewares::actor_context,
            ))
            .wrap(TracingMiddleware::new())
            .app_data(Data::new(pool.clone()))
            .app_data(app_context(pool))
            .configure(prod_api::config),
    )
    .await;

    test::TestRequest::put()
        .insert_header(create_token_header(token))
        .uri(endpoint)
        .set_json(&content)
        .send_request(&app)
        .await
        .map_into_boxed_body()
}
