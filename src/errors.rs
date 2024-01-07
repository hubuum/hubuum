use actix_web::{http::StatusCode, HttpResponse, ResponseError};
use diesel::r2d2::PoolError;
use diesel::result::{DatabaseErrorKind, Error as DieselError};
use serde::Serialize;
use serde_json::json;
use std::fmt;

#[derive(Debug, Serialize)]
pub enum ApiError {
    Unauthorized(String),
    InternalServerError(String),
    Forbidden(String),
    DatabaseError(String),
    Conflict(String),
    NotFound(String),
    DbConnectionError(String),
    HashError(String),
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ApiError::HashError(ref message) => write!(f, "{}", message),
            ApiError::NotFound(ref message) => write!(f, "{}", message),
            ApiError::Conflict(ref message) => write!(f, "{}", message),
            ApiError::Forbidden(ref message) => write!(f, "{}", message),
            ApiError::InternalServerError(ref message) => write!(f, "{}", message),
            ApiError::Unauthorized(ref message) => write!(f, "{}", message),
            ApiError::DatabaseError(ref message) => write!(f, "{}", message),
            ApiError::DbConnectionError(ref message) => write!(f, "{}", message),
        }
    }
}

impl ResponseError for ApiError {
    fn error_response(&self) -> HttpResponse {
        match self {
            ApiError::Conflict(ref message) => {
                HttpResponse::Conflict().json(json!({ "error": "Conflict", "message": message }))
            }
            ApiError::Forbidden(ref message) => {
                HttpResponse::Forbidden().json(json!({ "error": "Forbidden", "message": message }))
            }
            ApiError::InternalServerError(ref message) => HttpResponse::InternalServerError()
                .json(json!({ "error": "Internal Server Error", "message": message })),
            ApiError::Unauthorized(ref message) => HttpResponse::Unauthorized()
                .json(json!({ "error": "Unauthorized", "message": message })),
            ApiError::DbConnectionError(ref message) => HttpResponse::InternalServerError()
                .json(json!({ "error": "Database Connection Error", "message": message })),
            ApiError::DatabaseError(ref message) => HttpResponse::InternalServerError()
                .json(json!({ "error": "Database Error", "message": message })),
            ApiError::HashError(ref message) => HttpResponse::InternalServerError()
                .json(json!({ "error": "Hash Error", "message": message })),
            ApiError::NotFound(ref message) => {
                HttpResponse::NotFound().json(json!({ "error": "Not Found", "message": message }))
            }
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            ApiError::Conflict(_) => StatusCode::CONFLICT,
            ApiError::Forbidden(_) => StatusCode::FORBIDDEN,
            ApiError::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            ApiError::InternalServerError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ApiError::DatabaseError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ApiError::DbConnectionError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ApiError::HashError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ApiError::NotFound(_) => StatusCode::NOT_FOUND,
        }
    }
}

impl From<argon2::Error> for ApiError {
    fn from(e: argon2::Error) -> Self {
        ApiError::HashError(e.to_string())
    }
}

impl From<PoolError> for ApiError {
    fn from(e: PoolError) -> Self {
        ApiError::DbConnectionError(e.to_string())
    }
}
impl From<DieselError> for ApiError {
    fn from(e: DieselError) -> Self {
        match e {
            DieselError::NotFound => ApiError::NotFound(e.to_string()),
            DieselError::DatabaseError(DatabaseErrorKind::UniqueViolation, _) => {
                ApiError::Conflict(e.to_string())
            }
            _ => ApiError::DatabaseError(e.to_string()),
        }
    }
}

pub trait ApiErrorMappable {
    fn map_to_api_error(&self, message: &str) -> ApiError;
}

impl ApiErrorMappable for argon2::Error {
    fn map_to_api_error(&self, message: &str) -> ApiError {
        ApiError::HashError(message.to_string())
    }
}

impl ApiErrorMappable for PoolError {
    fn map_to_api_error(&self, message: &str) -> ApiError {
        ApiError::DbConnectionError(message.to_string())
    }
}

impl ApiErrorMappable for DieselError {
    fn map_to_api_error(&self, message: &str) -> ApiError {
        match self {
            DieselError::NotFound => ApiError::NotFound(message.to_string()),
            DieselError::DatabaseError(DatabaseErrorKind::UniqueViolation, _) => {
                ApiError::Conflict(message.to_string())
            }
            _ => ApiError::DatabaseError(self.to_string()),
        }
    }
}

pub fn map_error<E: ApiErrorMappable + std::fmt::Debug>(error: E, message: &str) -> ApiError {
    error.map_to_api_error(message)
}
