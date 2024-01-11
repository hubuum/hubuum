use crate::api as prod_api;
use crate::db::connection::DbPool;
use actix_web::{http, test, web, App};
use serde::Serialize;

pub async fn send_get_request(
    app: &web::Data<DbPool>,
    token: &str,
    endpoint: &str,
) -> actix_web::dev::ServiceResponse {
    let app =
        test::init_service(App::new().app_data(app.clone()).configure(prod_api::config)).await;

    test::TestRequest::get()
        .insert_header((http::header::AUTHORIZATION, format!("Bearer {}", token)))
        .uri(endpoint)
        .send_request(&app)
        .await
}

pub async fn send_post_request<T>(
    app: &web::Data<DbPool>,
    token: &str,
    endpoint: &str,
    content: T,
) -> actix_web::dev::ServiceResponse
where
    T: Serialize,
{
    let app =
        test::init_service(App::new().app_data(app.clone()).configure(prod_api::config)).await;

    test::TestRequest::post()
        .insert_header((http::header::AUTHORIZATION, format!("Bearer {}", token)))
        .uri(endpoint)
        .set_json(&content) // Make sure to reference content
        .send_request(&app)
        .await
}

pub async fn send_delete_request(
    app: &web::Data<DbPool>,
    token: &str,
    endpoint: &str,
) -> actix_web::dev::ServiceResponse {
    let app =
        test::init_service(App::new().app_data(app.clone()).configure(prod_api::config)).await;

    test::TestRequest::delete()
        .insert_header((http::header::AUTHORIZATION, format!("Bearer {}", token)))
        .uri(endpoint)
        .send_request(&app)
        .await
}
