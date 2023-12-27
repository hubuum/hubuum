use actix_web::{http::StatusCode, HttpResponse, Responder};
use serde::Serialize;
use serde_json::json;

use tracing::debug;

pub fn json_response<T: Serialize>(data: T, status: StatusCode) -> HttpResponse {
    HttpResponse::build(status).json(data)
}

pub fn handle_result<T, E>(
    result: Result<T, E>,
    success_status: StatusCode,
    error_status: StatusCode,
) -> impl Responder
where
    T: Serialize,
    E: std::fmt::Debug,
{
    match result {
        Ok(data) => {
            debug!(message = "Handling result: OK", status = ?success_status);
            json_response(&data, success_status)
        }
        Err(err) => {
            debug!(message = "Handling result: Error", error = ?err, status = ?error_status);
            let error = json!({ "error": "An error occurred" });
            json_response(error, error_status)
        }
    }
}
