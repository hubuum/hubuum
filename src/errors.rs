use actix_web::{
    error::JsonPayloadError, http::StatusCode, HttpRequest, HttpResponse, ResponseError,
};
use diesel::r2d2::PoolError;
use diesel::result::{DatabaseErrorKind, Error as DieselError};
use serde::Serialize;
use serde_json::json;
use std::fmt;

use tracing::{debug, error};

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
    BadRequest(String),
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
            ApiError::BadRequest(ref message) => write!(f, "{}", message),
        }
    }
}

impl ResponseError for ApiError {
    fn error_response(&self) -> HttpResponse {
        match self {
            ApiError::Conflict(ref message) => {
                HttpResponse::Conflict().json(json!({ "error": "Conflict", "message": message}))
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
            ApiError::BadRequest(ref message) => HttpResponse::BadRequest()
                .json(json!({ "error": "Bad Request", "message": message })),
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
            ApiError::BadRequest(_) => StatusCode::BAD_REQUEST,
        }
    }
}

impl From<argon2::Error> for ApiError {
    fn from(e: argon2::Error) -> Self {
        error!(message = "Error hashing password", error = ?e);
        ApiError::HashError(e.to_string())
    }
}

impl From<PoolError> for ApiError {
    fn from(e: PoolError) -> Self {
        error!(message = "Unable to get a connection from the pool", error = ?e);
        ApiError::DbConnectionError(e.to_string())
    }
}
impl From<DieselError> for ApiError {
    fn from(e: DieselError) -> Self {
        match e {
            DieselError::NotFound => {
                debug!(message = "Entity not found", error = ?e);
                ApiError::NotFound(e.to_string())
            }
            DieselError::DatabaseError(DatabaseErrorKind::UniqueViolation, _) => {
                debug!(message = "Unique constraint not met", error = ?e);
                ApiError::Conflict(e.to_string())
            }
            DieselError::DatabaseError(DatabaseErrorKind::ForeignKeyViolation, _) => {
                debug!(message = "Unable to resolve foreign key", error = ?e);
                ApiError::Conflict(e.to_string())
            }
            DieselError::DatabaseError(DatabaseErrorKind::CheckViolation, _) => {
                ApiError::BadRequest(e.to_string())
            }
            _ => {
                error!(message = "Database error", error = ?e);
                ApiError::DatabaseError(e.to_string())
            }
        }
    }
}

/// Ensure that json deserialization errors are reported as a bad request and
/// that the error itself is returned as json.
pub fn json_error_handler(err: JsonPayloadError, _: &HttpRequest) -> actix_web::Error {
    let error_message = format!("Json deserialize error: {}", err);
    ApiError::BadRequest(error_message).into()
}
