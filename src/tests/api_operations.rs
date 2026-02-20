use crate::api as prod_api;
use crate::db::DbPool;
use crate::middlewares::tracing::TracingMiddleware;
use actix_web::{http, test, web::Data, App};
use serde::Serialize;

fn create_token_header(token: &str) -> (http::header::HeaderName, String) {
    (http::header::AUTHORIZATION, format!("Bearer {token}"))
}

pub async fn get_request_with_correlation(
    pool: &DbPool,
    token: &str,
    endpoint: &str,
    correlation_id: Option<&str>,
) -> actix_web::dev::ServiceResponse {
    let app = test::init_service(
        App::new()
            .wrap(TracingMiddleware)
            .app_data(Data::new(pool.clone()))
            .configure(prod_api::config),
    )
    .await;

    if let Some(correlation_id) = correlation_id {
        test::TestRequest::get()
            .insert_header(create_token_header(token))
            .insert_header((
                http::header::HeaderName::from_static("x-correlation-id"),
                correlation_id,
            ))
            .uri(endpoint)
            .send_request(&app)
            .await
    } else {
        test::TestRequest::get()
            .insert_header(create_token_header(token))
            .uri(endpoint)
            .send_request(&app)
            .await
    }
}

pub async fn get_request(
    pool: &DbPool,
    token: &str,
    endpoint: &str,
) -> actix_web::dev::ServiceResponse {
    get_request_with_correlation(pool, token, endpoint, None).await
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
    let app = test::init_service(
        App::new()
            .wrap(TracingMiddleware)
            .app_data(Data::new(pool.clone()))
            .configure(prod_api::config),
    )
    .await;

    test::TestRequest::post()
        .insert_header(create_token_header(token))
        .uri(endpoint)
        .set_json(&content) // Make sure to reference content
        .send_request(&app)
        .await
}

pub async fn delete_request(
    pool: &DbPool,
    token: &str,
    endpoint: &str,
) -> actix_web::dev::ServiceResponse {
    let app = test::init_service(
        App::new()
            .wrap(TracingMiddleware)
            .app_data(Data::new(pool.clone()))
            .configure(prod_api::config),
    )
    .await;

    test::TestRequest::delete()
        .insert_header(create_token_header(token))
        .uri(endpoint)
        .send_request(&app)
        .await
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
            .wrap(TracingMiddleware)
            .app_data(Data::new(pool.clone()))
            .configure(prod_api::config),
    )
    .await;

    test::TestRequest::patch()
        .insert_header(create_token_header(token))
        .uri(endpoint)
        .set_json(&content) // Make sure to reference content
        .send_request(&app)
        .await
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
            .wrap(TracingMiddleware)
            .app_data(Data::new(pool.clone()))
            .configure(prod_api::config),
    )
    .await;

    test::TestRequest::put()
        .insert_header(create_token_header(token))
        .uri(endpoint)
        .set_json(&content)
        .send_request(&app)
        .await
}
