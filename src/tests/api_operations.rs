use crate::api as prod_api;
use actix_web::{http, test, web, App};

pub async fn send_get_request(
    app: &web::Data<crate::db::connection::DbPool>,
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
