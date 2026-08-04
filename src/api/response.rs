use actix_web::{
    HttpRequest, HttpResponse, Responder,
    body::BoxBody,
    http::{StatusCode, header, header::HeaderValue},
};
use serde::Serialize;

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::LazyLock;
use tracing::debug;

use crate::api::etag::RevisionedResource;
use crate::api::openapi::MessageResponse;
use crate::errors::ApiError;
use crate::models::search::QueryOptions;
use crate::pagination::{CursorPaginated, effective_page_limit, finalize_page, pagination_headers};

static NO_CONTENT_STATUS_CODES: LazyLock<HashSet<StatusCode>> = LazyLock::new(|| {
    let mut m = HashSet::new();
    m.insert(StatusCode::NO_CONTENT);
    m.insert(StatusCode::RESET_CONTENT);
    m.insert(StatusCode::NOT_MODIFIED);
    m
});

pub enum ApiResponse<T> {
    Json {
        data: T,
        status: StatusCode,
        headers: Option<HashMap<String, String>>,
    },
    Empty {
        status: StatusCode,
        headers: Option<HashMap<String, String>>,
    },
    Created {
        data: T,
        location: ResponseLocation,
    },
}

impl<T> ApiResponse<T> {
    pub fn new(data: T, status: StatusCode) -> Self {
        Self::Json {
            data,
            status,
            headers: None,
        }
    }

    pub fn new_with_headers(data: T, status: StatusCode, headers: HashMap<String, String>) -> Self {
        Self::Json {
            data,
            status,
            headers: Some(headers),
        }
    }

    pub fn new_no_store(data: T, status: StatusCode) -> Self {
        Self::Json {
            data,
            status,
            headers: Some(HashMap::from([(
                header::CACHE_CONTROL.to_string(),
                "no-store".to_string(),
            )])),
        }
    }

    pub fn new_private_no_store(data: T, status: StatusCode) -> Self {
        Self::Json {
            data,
            status,
            headers: Some(HashMap::from([(
                header::CACHE_CONTROL.to_string(),
                "private, no-store".to_string(),
            )])),
        }
    }

    pub fn ok(data: T) -> Self {
        Self::new(data, StatusCode::OK)
    }

    pub fn accepted(data: T) -> Self {
        Self::new(data, StatusCode::ACCEPTED)
    }

    pub fn revisioned(data: T, status: StatusCode) -> Result<Self, ApiError>
    where
        T: RevisionedResource,
    {
        let etag = data.entity_tag()?;
        Ok(Self::new_with_etag(data, status, etag))
    }

    pub fn new_with_resource_etag<R>(
        data: T,
        status: StatusCode,
        resource: &R,
    ) -> Result<Self, ApiError>
    where
        R: RevisionedResource,
    {
        Ok(Self::new_with_etag(data, status, resource.entity_tag()?))
    }

    pub fn new_with_etag(data: T, status: StatusCode, etag: impl ToString) -> Self {
        Self::new_with_headers(data, status, etag_header(etag))
    }

    pub fn ok_revisioned(data: T) -> Result<Self, ApiError>
    where
        T: RevisionedResource,
    {
        Self::revisioned(data, StatusCode::OK)
    }

    pub fn accepted_revisioned(data: T) -> Result<Self, ApiError>
    where
        T: RevisionedResource,
    {
        Self::revisioned(data, StatusCode::ACCEPTED)
    }

    pub fn created(data: T, location: ResponseLocation) -> Self {
        Self::Created { data, location }
    }

    pub fn created_revisioned(data: T, location: ResponseLocation) -> Result<Self, ApiError>
    where
        T: RevisionedResource,
    {
        let mut headers = location_header(location);
        headers.insert(header::ETAG.to_string(), data.entity_tag()?.to_string());
        Ok(Self::Json {
            data,
            status: StatusCode::CREATED,
            headers: Some(headers),
        })
    }

    pub fn accepted_at(data: T, location: ResponseLocation) -> Self {
        Self::Json {
            data,
            status: StatusCode::ACCEPTED,
            headers: Some(location_header(location)),
        }
    }

    pub fn paginated_items(
        data: T,
        next_cursor: &Option<String>,
        total_count: i64,
        effective_limit: usize,
        no_store: bool,
    ) -> Self {
        let mut headers = pagination_headers(next_cursor, total_count, effective_limit);
        if no_store {
            headers.insert(
                header::CACHE_CONTROL.to_string(),
                "private, no-store".to_string(),
            );
        }
        Self::Json {
            data,
            status: StatusCode::OK,
            headers: Some(headers),
        }
    }
}

impl<T> ApiResponse<Vec<T>>
where
    T: CursorPaginated,
{
    pub fn paginated(
        data: Vec<T>,
        total_count: i64,
        query_options: &QueryOptions,
    ) -> Result<Self, ApiError> {
        let page = finalize_page(data, query_options)?;
        let effective_limit = effective_page_limit(query_options)?;
        Ok(Self::Json {
            data: page.items,
            status: StatusCode::OK,
            headers: Some(pagination_headers(
                &page.next_cursor,
                total_count,
                effective_limit,
            )),
        })
    }
}

impl<U> ApiResponse<Vec<U>> {
    pub fn mapped_paginated<T, F>(
        data: Vec<T>,
        total_count: i64,
        query_options: &QueryOptions,
        map: F,
    ) -> Result<Self, ApiError>
    where
        T: CursorPaginated,
        F: FnOnce(Vec<T>) -> Vec<U>,
    {
        let page = finalize_page(data, query_options)?;
        let effective_limit = effective_page_limit(query_options)?;
        Ok(Self::Json {
            data: map(page.items),
            status: StatusCode::OK,
            headers: Some(pagination_headers(
                &page.next_cursor,
                total_count,
                effective_limit,
            )),
        })
    }
}

impl ApiResponse<()> {
    pub fn ok_empty() -> Self {
        Self::new((), StatusCode::OK)
    }

    pub fn created_empty() -> Self {
        Self::new((), StatusCode::CREATED)
    }

    pub fn no_content() -> Self {
        Self::Empty {
            status: StatusCode::NO_CONTENT,
            headers: None,
        }
    }

    pub fn no_content_with_etag(etag: impl ToString) -> Self {
        Self::Empty {
            status: StatusCode::NO_CONTENT,
            headers: Some(etag_header(etag)),
        }
    }

    pub fn not_found_empty() -> Self {
        Self::new((), StatusCode::NOT_FOUND)
    }
}

fn etag_header(etag: impl ToString) -> HashMap<String, String> {
    HashMap::from([(header::ETAG.to_string(), etag.to_string())])
}

impl ApiResponse<MessageResponse> {
    pub fn message(message: impl Into<String>) -> Self {
        Self::ok(MessageResponse::new(message))
    }
}

impl<T: Serialize> Responder for ApiResponse<T> {
    type Body = BoxBody;

    fn respond_to(self, _req: &HttpRequest) -> HttpResponse<Self::Body> {
        match self {
            Self::Json {
                data,
                status,
                headers,
            } => {
                let mut response_builder = HttpResponse::build(status);
                insert_headers(&mut response_builder, headers);

                if NO_CONTENT_STATUS_CODES.contains(&status) {
                    debug!(message = "Empty result requested", status = ?status);
                    response_builder.finish()
                } else {
                    response_builder.json(data)
                }
            }
            Self::Empty { status, headers } => {
                let mut response_builder = HttpResponse::build(status);
                insert_headers(&mut response_builder, headers);
                response_builder.finish()
            }
            Self::Created { data, location } => HttpResponse::Created()
                .insert_header((header::LOCATION, location.as_str()))
                .json(data),
        }
    }
}

fn insert_headers(
    response_builder: &mut actix_web::HttpResponseBuilder,
    headers: Option<HashMap<String, String>>,
) {
    if let Some(headers) = headers {
        for (key, value) in headers {
            debug!(message = "Adding response header", key = key);
            response_builder.insert_header((key, value));
        }
    }
}

fn location_header(location: ResponseLocation) -> HashMap<String, String> {
    HashMap::from([(header::LOCATION.to_string(), location.as_str().to_string())])
}

pub struct ResponseLocation(String);

impl ResponseLocation {
    pub fn new(value: impl Into<String>) -> Result<Self, ApiError> {
        let value = value.into();
        if !value.starts_with('/') {
            return Err(ApiError::InternalServerError(
                "Response location must be an absolute path".to_string(),
            ));
        }
        HeaderValue::from_str(&value).map_err(|_| {
            ApiError::InternalServerError(
                "Response location must be a valid header value".to_string(),
            )
        })?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logger::{HubuumLoggingFormat, test_support::JsonLogWriter};
    use actix_web::test as actix_test;
    use tracing_subscriber::layer::SubscriberExt;

    #[test]
    fn response_header_debug_log_omits_the_value() {
        let header_name = "x-sensitive-response-data";
        let header_value = "secret-pagination-cursor";
        let writer = JsonLogWriter::default();
        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .json()
                .with_writer(writer.clone())
                .event_format(HubuumLoggingFormat),
        );
        let request = actix_test::TestRequest::default().to_http_request();

        let response = tracing::subscriber::with_default(subscriber, || {
            ApiResponse::new_with_headers(
                (),
                StatusCode::OK,
                HashMap::from([(header_name.to_string(), header_value.to_string())]),
            )
            .respond_to(&request)
        });

        assert_eq!(
            response.headers().get(header_name),
            Some(&HeaderValue::from_static(header_value))
        );

        let logs = writer.raw_output();
        assert!(logs.contains("Adding response header"));
        assert!(logs.contains(header_name));
        assert!(!logs.contains(header_value));
    }
}
