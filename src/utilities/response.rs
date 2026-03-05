use actix_web::{HttpResponse, http::StatusCode};
use serde::Serialize;

use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::collections::HashSet;
use tracing::debug;

use crate::errors::ApiError;
use crate::models::search::QueryOptions;
use crate::pagination::{CursorPaginated, finalize_page, next_cursor_header};

static NO_CONTENT_STATUS_CODES: Lazy<HashSet<StatusCode>> = Lazy::new(|| {
    let mut m = HashSet::new();
    m.insert(StatusCode::NO_CONTENT);
    m.insert(StatusCode::RESET_CONTENT);
    m.insert(StatusCode::NOT_MODIFIED);
    m
});

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
    headers: Option<HashMap<String, String>>,
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

pub fn json_response_created<T: Serialize>(object: T, location: &str) -> HttpResponse {
    let mut headers = HashMap::new();
    headers.insert("Location".to_string(), location.to_string());

    json_response_with_header(object, StatusCode::CREATED, Some(headers))
}

pub fn paginated_json_response<T>(
    data: Vec<T>,
    status: StatusCode,
    query_options: &QueryOptions,
) -> Result<HttpResponse, ApiError>
where
    T: Serialize + CursorPaginated,
{
    let page = finalize_page(data, query_options)?;
    Ok(json_response_with_header(
        page.items,
        status,
        next_cursor_header(&page.next_cursor),
    ))
}

pub fn paginated_json_mapped_response<T, U, F>(
    data: Vec<T>,
    status: StatusCode,
    query_options: &QueryOptions,
    map: F,
) -> Result<HttpResponse, ApiError>
where
    T: CursorPaginated,
    U: Serialize,
    F: FnOnce(Vec<T>) -> Vec<U>,
{
    let page = finalize_page(data, query_options)?;
    Ok(json_response_with_header(
        map(page.items),
        status,
        next_cursor_header(&page.next_cursor),
    ))
}
