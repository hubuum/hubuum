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

use crate::errors::ApiError;
use crate::models::search::QueryOptions;
use crate::pagination::{CursorPaginated, finalize_page, pagination_headers};

static NO_CONTENT_STATUS_CODES: LazyLock<HashSet<StatusCode>> = LazyLock::new(|| {
    let mut m = HashSet::new();
    m.insert(StatusCode::NO_CONTENT);
    m.insert(StatusCode::RESET_CONTENT);
    m.insert(StatusCode::NOT_MODIFIED);
    m
});

pub struct JsonResponse<T> {
    data: T,
    status: StatusCode,
    headers: Option<HashMap<String, String>>,
}

impl<T> JsonResponse<T> {
    pub fn new(data: T, status: StatusCode) -> Self {
        Self {
            data,
            status,
            headers: None,
        }
    }

    pub fn ok(data: T) -> Self {
        Self::new(data, StatusCode::OK)
    }

    pub fn with_headers(
        data: T,
        status: StatusCode,
        headers: Option<HashMap<String, String>>,
    ) -> Self {
        Self {
            data,
            status,
            headers,
        }
    }
}

impl<T: Serialize> Responder for JsonResponse<T> {
    type Body = BoxBody;

    fn respond_to(self, _req: &HttpRequest) -> HttpResponse<Self::Body> {
        let mut response_builder = HttpResponse::build(self.status);

        if let Some(headers) = self.headers {
            for (key, value) in headers {
                debug!(message = "Adding response header", key = key, value = value);
                response_builder.insert_header((key, value));
            }
        }

        if NO_CONTENT_STATUS_CODES.contains(&self.status) {
            debug!(message = "Empty result requested", status = ?self.status);
            response_builder.finish()
        } else {
            response_builder.json(self.data)
        }
    }
}

impl JsonResponse<()> {
    pub fn no_content() -> Self {
        Self::new((), StatusCode::NO_CONTENT)
    }
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

pub struct CreatedJsonResponse<T> {
    data: T,
    location: ResponseLocation,
}

impl<T> CreatedJsonResponse<T> {
    pub fn new(data: T, location: ResponseLocation) -> Self {
        Self { data, location }
    }
}

impl<T: Serialize> Responder for CreatedJsonResponse<T> {
    type Body = BoxBody;

    fn respond_to(self, _req: &HttpRequest) -> HttpResponse<Self::Body> {
        HttpResponse::Created()
            .insert_header((header::LOCATION, self.location.as_str()))
            .json(self.data)
    }
}

pub struct PaginatedJsonResponse<T> {
    data: Vec<T>,
    status: StatusCode,
    headers: HashMap<String, String>,
}

impl<T> PaginatedJsonResponse<T>
where
    T: CursorPaginated,
{
    pub fn new(
        data: Vec<T>,
        total_count: i64,
        status: StatusCode,
        query_options: &QueryOptions,
    ) -> Result<Self, ApiError> {
        let page = finalize_page(data, query_options)?;
        Ok(Self {
            data: page.items,
            status,
            headers: pagination_headers(&page.next_cursor, total_count),
        })
    }
}

impl<T: Serialize> Responder for PaginatedJsonResponse<T> {
    type Body = BoxBody;

    fn respond_to(self, req: &HttpRequest) -> HttpResponse<Self::Body> {
        JsonResponse::with_headers(self.data, self.status, Some(self.headers)).respond_to(req)
    }
}

pub struct MappedPaginatedJsonResponse<U> {
    data: Vec<U>,
    status: StatusCode,
    headers: HashMap<String, String>,
}

impl<U> MappedPaginatedJsonResponse<U> {
    pub fn new<T, F>(
        data: Vec<T>,
        total_count: i64,
        status: StatusCode,
        query_options: &QueryOptions,
        map: F,
    ) -> Result<Self, ApiError>
    where
        T: CursorPaginated,
        F: FnOnce(Vec<T>) -> Vec<U>,
    {
        let page = finalize_page(data, query_options)?;
        Ok(Self {
            data: map(page.items),
            status,
            headers: pagination_headers(&page.next_cursor, total_count),
        })
    }
}

impl<U: Serialize> Responder for MappedPaginatedJsonResponse<U> {
    type Body = BoxBody;

    fn respond_to(self, req: &HttpRequest) -> HttpResponse<Self::Body> {
        JsonResponse::with_headers(self.data, self.status, Some(self.headers)).respond_to(req)
    }
}
