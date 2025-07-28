use actix_web::{
    error::JsonPayloadError, http::StatusCode, HttpRequest, HttpResponse, ResponseError,
};
use diesel::r2d2::PoolError;
use diesel::result::{DatabaseErrorKind, Error as DieselError};
use serde::Serialize;
use serde_json::json;
use std::fmt;
use std::num::ParseIntError;

use tracing::{debug, error};

#[derive(Debug, Serialize, PartialEq)]
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
    OperatorMismatch(String),
    InvalidIntegerRange(String),
    ValidationError(String),
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ApiError::HashError(ref message) => write!(f, "{message}"),
            ApiError::NotFound(ref message) => write!(f, "{message}"),
            ApiError::Conflict(ref message) => write!(f, "{message}"),
            ApiError::Forbidden(ref message) => write!(f, "{message}"),
            ApiError::InternalServerError(ref message) => write!(f, "{message}"),
            ApiError::Unauthorized(ref message) => write!(f, "{message}"),
            ApiError::DatabaseError(ref message) => write!(f, "{message}"),
            ApiError::DbConnectionError(ref message) => write!(f, "{message}"),
            ApiError::BadRequest(ref message) => write!(f, "{message}"),
            ApiError::OperatorMismatch(ref message) => write!(f, "{message}"),
            ApiError::InvalidIntegerRange(ref message) => write!(f, "{message}"),
            ApiError::ValidationError(ref message) => write!(f, "{message}"),
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
            ApiError::OperatorMismatch(ref message) => HttpResponse::BadRequest()
                .json(json!({ "error": "Operator Mismatch", "message": message })),
            ApiError::InvalidIntegerRange(ref message) => HttpResponse::BadRequest()
                .json(json!({ "error": "Invalid Integer Range", "message": message })),
            ApiError::ValidationError(ref message) => HttpResponse::NotAcceptable()
                .json(json!({ "error": "Validation Error", "message": message })),
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
            ApiError::OperatorMismatch(_) => StatusCode::BAD_REQUEST,
            ApiError::InvalidIntegerRange(_) => StatusCode::BAD_REQUEST,
            ApiError::ValidationError(_) => StatusCode::NOT_ACCEPTABLE,
        }
    }
}

impl From<serde_json::Error> for ApiError {
    fn from(e: serde_json::Error) -> Self {
        error!(message = "Error parsing input as json", error = ?e);
        ApiError::BadRequest(e.to_string())
    }
}

impl From<chrono::ParseError> for ApiError {
    fn from(e: chrono::ParseError) -> Self {
        error!(message = "Error parsing date", error = ?e);
        ApiError::BadRequest(e.to_string())
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

impl From<ParseIntError> for ApiError {
    fn from(e: ParseIntError) -> Self {
        error!(message = "Error parsing integer", error = ?e);
        ApiError::BadRequest(e.to_string())
    }
}

impl From<DieselError> for ApiError {
    fn from(e: DieselError) -> Self {
        match e {
            DieselError::NotFound => {
                let message = "Entity not found".to_string();
                debug!(message = message, error = ?e);
                ApiError::NotFound(message)
            }
            DieselError::DatabaseError(DatabaseErrorKind::UniqueViolation, _) => {
                let message = "Unique constraint not met".to_string();
                debug!(message = message, error = ?e);
                ApiError::Conflict(message)
            }
            DieselError::DatabaseError(DatabaseErrorKind::ForeignKeyViolation, _) => {
                let message = "Attempt to associate to a non-existent entity".to_string();
                debug!(message = message, error = ?e);
                ApiError::NotFound(message)
            }
            DieselError::DatabaseError(DatabaseErrorKind::CheckViolation, _) => {
                let message = "Check constraint not met".to_string();
                debug!(message = message, error = ?e);
                ApiError::BadRequest(message)
            }
            DieselError::DatabaseError(DatabaseErrorKind::Unknown, ref info) => {
                let message = info.message();
                if message.starts_with("Invalid object relation:") {
                    debug!(message = message, error = ?e);
                    return ApiError::BadRequest(message.to_string());
                }
                error!(message = "Database error", error = ?e);
                ApiError::DatabaseError(e.to_string())
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
    let error_message = format!("Json deserialize error: {err}");
    ApiError::BadRequest(error_message).into()
}
