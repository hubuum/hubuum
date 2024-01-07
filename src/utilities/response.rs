use actix_web::{http::StatusCode, HttpResponse};
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
        m.insert(StatusCode::CREATED);
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
        debug!(message = "Empty result requested", status = ?status);
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
