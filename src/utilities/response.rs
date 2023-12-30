use actix_web::{http::StatusCode, HttpResponse};
use serde::Serialize;
use serde_json::json;

use tracing::debug;

/// Create a JSON response with the given data and status code
///
/// ## Arguments
///
/// * `data` - The json data to be serialized and sent in the response (use `json!` macro)
/// * `status` - The HTTP status code to be sent in the response
pub fn json_response<T: Serialize>(data: T, status: StatusCode) -> HttpResponse {
    debug!(message = "Creating JSON response", status = ?status);

    if status == StatusCode::NO_CONTENT {
        return HttpResponse::build(status).finish();
    }
    HttpResponse::build(status).json(data)
}

pub fn handle_result<T, E>(
    result: Result<T, E>,
    success_status: StatusCode,
    error_status: StatusCode,
) -> HttpResponse
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
