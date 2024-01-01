use actix_web::{http::StatusCode, HttpResponse, ResponseError};
use serde::Serialize;

use std::collections::HashMap;
use tracing::debug;

use lazy_static::lazy_static;
use std::collections::HashSet;

lazy_static! {
    /// A set of status codes that should not have a body
    static ref NO_CONTENT_STATUS_CODES: HashSet<StatusCode> = {
        let mut m = HashSet::new();
        m.insert(StatusCode::NO_CONTENT);
        m.insert(StatusCode::RESET_CONTENT);
        m.insert(StatusCode::NOT_MODIFIED);
        m
    };
}

/// Create a JSON response with the given data and status code
///
/// ## Arguments
///
/// * `data` - The json data to be serialized and sent in the response (use `json!` macro)
/// * `status` - The HTTP status code to be sent in the response
pub fn json_response<T: Serialize>(data: T, status: StatusCode) -> HttpResponse {
    debug!(message = "Creating JSON response without extra headers", status = ?status);
    json_response_with_header(data, status, None)
}

pub fn json_response_with_header<T: Serialize>(
    data: T,
    status: StatusCode,
    headers: Option<HashMap<&str, &str>>,
) -> HttpResponse {
    let mut response_builder = HttpResponse::build(status);

    if let Some(hdrs) = headers {
        for (key, value) in hdrs {
            debug!(message = "Adding response header", key = key, value = value);
            response_builder.insert_header((key, value));
        }
    }

    if NO_CONTENT_STATUS_CODES.contains(&status) {
        response_builder.finish()
    } else {
        response_builder.json(data)
    }
}

pub fn json_response_created(location: &str) -> HttpResponse {
    let mut headers = HashMap::new();
    headers.insert("Location", location);

    json_response_with_header(serde_json::Value::Null, StatusCode::CREATED, Some(headers))
}

pub fn handle_result_with_modifier<T, E, F>(
    result: Result<T, E>,
    success_status: StatusCode,
    error_status: StatusCode,
    success_modifier: Option<F>,
) -> HttpResponse
where
    T: Serialize,
    E: ResponseError,
    F: FnOnce(HttpResponse) -> HttpResponse,
{
    match result {
        Ok(data) => {
            debug!(message = "Handling result: OK", status = ?success_status);
            let response = json_response(&data, success_status);
            if let Some(modifier) = success_modifier {
                modifier(response)
            } else {
                response
            }
        }
        Err(err) => {
            debug!(message = "Handling result: Error", error = ?err, status = ?error_status);
            err.error_response()
        }
    }
}

pub fn handle_result<T, E>(
    result: Result<T, E>,
    success_status: StatusCode,
    error_status: StatusCode,
) -> HttpResponse
where
    T: Serialize,
    E: ResponseError,
{
    handle_result_with_modifier(
        result,
        success_status,
        error_status,
        None::<fn(HttpResponse) -> HttpResponse>,
    )
}
