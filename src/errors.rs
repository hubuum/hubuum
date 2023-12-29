use actix_web::{http::StatusCode, HttpResponse, ResponseError};
use serde::Serialize;
use serde_json::json;
use std::fmt;

#[derive(Debug, Serialize)]
pub enum ApiError {
    Unauthorized(String),
    InternalServerError(String),
    Forbidden(String),
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ApiError::Forbidden(ref message) => write!(f, "{}", message),
            ApiError::InternalServerError(ref message) => write!(f, "{}", message),
            ApiError::Unauthorized(ref message) => write!(f, "{}", message),
        }
    }
}

impl ResponseError for ApiError {
    fn error_response(&self) -> HttpResponse {
        match self {
            ApiError::Forbidden(ref message) => {
                HttpResponse::Forbidden().json(json!({ "error": "Forbidden", "message": message }))
            }
            ApiError::InternalServerError(ref message) => HttpResponse::InternalServerError()
                .json(json!({ "error": "Internal Server Error", "message": message })),
            ApiError::Unauthorized(ref message) => HttpResponse::Unauthorized()
                .json(json!({ "error": "Unauthorized", "message": message })),
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            ApiError::Forbidden(_) => StatusCode::FORBIDDEN,
            ApiError::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            ApiError::InternalServerError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
