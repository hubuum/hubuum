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

use crate::api::openapi::MessageResponse;
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

pub enum ApiResponse<T> {
    Json {
        data: T,
        status: StatusCode,
        headers: Option<HashMap<String, String>>,
    },
    Empty {
        status: StatusCode,
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

    pub fn ok(data: T) -> Self {
        Self::new(data, StatusCode::OK)
    }

    pub fn accepted(data: T) -> Self {
        Self::new(data, StatusCode::ACCEPTED)
    }

    pub fn created(data: T, location: ResponseLocation) -> Self {
        Self::Created { data, location }
    }

    pub fn accepted_at(data: T, location: ResponseLocation) -> Self {
        Self::Json {
            data,
            status: StatusCode::ACCEPTED,
            headers: Some(location_header(location)),
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
        Ok(Self::Json {
            data: page.items,
            status: StatusCode::OK,
            headers: Some(pagination_headers(&page.next_cursor, total_count)),
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
        Ok(Self::Json {
            data: map(page.items),
            status: StatusCode::OK,
            headers: Some(pagination_headers(&page.next_cursor, total_count)),
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
        }
    }

    pub fn not_found_empty() -> Self {
        Self::new((), StatusCode::NOT_FOUND)
    }
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
            Self::Empty { status } => HttpResponse::build(status).finish(),
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
            debug!(message = "Adding response header", key = key, value = value);
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
