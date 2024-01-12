use crate::api as prod_api;
use crate::db::connection::DbPool;
use actix_web::{http, test, web, App};
use serde::Serialize;

fn create_token_header(token: &str) -> (http::header::HeaderName, String) {
    (http::header::AUTHORIZATION, format!("Bearer {}", token))
}

pub async fn get_request(
    pool: &web::Data<DbPool>,
    token: &str,
    endpoint: &str,
) -> actix_web::dev::ServiceResponse {
    let app = test::init_service(
        App::new()
            .app_data(pool.clone())
            .configure(prod_api::config),
    )
    .await;

    test::TestRequest::get()
        .insert_header(create_token_header(&token))
        .uri(endpoint)
        .send_request(&app)
        .await
}

pub async fn post_request<T>(
    pool: &web::Data<DbPool>,
    token: &str,
    endpoint: &str,
    content: T,
) -> actix_web::dev::ServiceResponse
where
    T: Serialize,
{
    let app = test::init_service(
        App::new()
            .app_data(pool.clone())
            .configure(prod_api::config),
    )
    .await;

    test::TestRequest::post()
        .insert_header(create_token_header(&token))
        .uri(endpoint)
        .set_json(&content) // Make sure to reference content
        .send_request(&app)
        .await
}

pub async fn delete_request(
    pool: &web::Data<DbPool>,
    token: &str,
    endpoint: &str,
) -> actix_web::dev::ServiceResponse {
    let app = test::init_service(
        App::new()
            .app_data(pool.clone())
            .configure(prod_api::config),
    )
    .await;

    test::TestRequest::delete()
        .insert_header(create_token_header(&token))
        .uri(endpoint)
        .send_request(&app)
        .await
}

pub async fn patch_request<T>(
    pool: &web::Data<DbPool>,
    token: &str,
    endpoint: &str,
    content: T,
) -> actix_web::dev::ServiceResponse
where
    T: Serialize,
{
    let app = test::init_service(
        App::new()
            .app_data(pool.clone())
            .configure(prod_api::config),
    )
    .await;

    test::TestRequest::patch()
        .insert_header(create_token_header(&token))
        .uri(endpoint)
        .set_json(&content) // Make sure to reference content
        .send_request(&app)
        .await
}
