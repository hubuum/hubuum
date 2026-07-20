use actix_web::{
    HttpRequest, HttpResponse, ResponseError, error::JsonPayloadError, http::StatusCode,
};
use diesel::result::{DatabaseErrorKind, Error as DieselError};
use diesel_async::pooled_connection::bb8::RunError as PoolError;
use serde::Serialize;
use serde_json::json;
use std::fmt;
use std::num::ParseIntError;

use tracing::{debug, error};

use crate::observability::metrics;

const PUBLIC_INTERNAL_ERROR: &str = "An internal error occurred";
const PUBLIC_SERVICE_UNAVAILABLE: &str = "Service temporarily unavailable";
const PUBLIC_PERMISSION_BACKEND_UNAVAILABLE: &str = "Permission backend temporarily unavailable";

// Exit codes for startup/initialization failures.
// These help shell scripts and orchestration systems determine the failure mode.
pub const EXIT_CODE_GENERIC_ERROR: i32 = 1; // Non-specific/generic errors
pub const EXIT_CODE_CONFIG_ERROR: i32 = 2; // Config/validation
pub const EXIT_CODE_DATABASE_ERROR: i32 = 3; // Database connection/pool error
pub const EXIT_CODE_INIT_ERROR: i32 = 4; // Critical initialization error (admin user/group)
pub const EXIT_CODE_TLS_ERROR: i32 = 5; // TLS setup error
pub const EXIT_CODE_PERMISSION_BACKEND_ERROR: i32 = 6; // Permission backend unavailable

/// Log a fatal error and exit the process with the specified exit code.
/// This provides a consistent way to handle unrecoverable errors during startup.
///
/// The `#[track_caller]` attribute captures the location of the caller, so logs
/// will show where the error originated, not this function's location.
#[track_caller]
pub fn fatal_error(message: &str, exit_code: i32) -> ! {
    let location = std::panic::Location::caller();
    error!(
        message = message,
        exit_code = exit_code,
        file = location.file(),
        line = location.line(),
    );
    eprintln!(
        "Fatal startup error at {}:{}: {} (exit code {})",
        location.file(),
        location.line(),
        message,
        exit_code
    );
    #[cfg(test)]
    panic!(
        "Fatal startup error at {}:{}: {} (exit code {})",
        location.file(),
        location.line(),
        message,
        exit_code
    );
    #[cfg(not(test))]
    std::process::exit(exit_code);
}

#[derive(Debug, Serialize, PartialEq)]
pub enum ApiError {
    Unauthorized(String),
    InternalServerError(String),
    Forbidden(String),
    NotAcceptable(String),
    UnsupportedMediaType(String),
    PayloadTooLarge(String),
    NotImplemented(String),
    PermissionBackendUnavailable(String),
    DatabaseError(String),
    Conflict(String),
    TooManyRequests(String),
    ServiceUnavailable(String),
    NotFound(String),
    Gone(String),
    DbConnectionError(String),
    HashError(String),
    BadRequest(String),
    OperatorMismatch(String),
    InvalidIntegerRange(String),
    ValidationError(String),
}

impl ApiError {
    pub fn class(&self) -> &'static str {
        match self {
            ApiError::Unauthorized(_) => "unauthorized",
            ApiError::InternalServerError(_) => "internal_server_error",
            ApiError::Forbidden(_) => "forbidden",
            ApiError::NotAcceptable(_) => "not_acceptable",
            ApiError::UnsupportedMediaType(_) => "unsupported_media_type",
            ApiError::PayloadTooLarge(_) => "payload_too_large",
            ApiError::DatabaseError(_) => "database_error",
            ApiError::Conflict(_) => "conflict",
            ApiError::TooManyRequests(_) => "too_many_requests",
            ApiError::ServiceUnavailable(_) => "service_unavailable",
            ApiError::NotImplemented(_) => "not_implemented",
            ApiError::PermissionBackendUnavailable(_) => "permission_backend_unavailable",
            ApiError::NotFound(_) => "not_found",
            ApiError::Gone(_) => "gone",
            ApiError::DbConnectionError(_) => "db_connection_error",
            ApiError::HashError(_) => "hash_error",
            ApiError::BadRequest(_) => "bad_request",
            ApiError::OperatorMismatch(_) => "operator_mismatch",
            ApiError::InvalidIntegerRange(_) => "invalid_integer_range",
            ApiError::ValidationError(_) => "validation_error",
        }
    }

    /// Return an appropriate exit code for startup/initialization errors based on error type.
    /// Failure modes:
    /// - Configuration/validation errors (BadRequest, ValidationError, etc.) → EXIT_CODE_CONFIG_ERROR (2)
    /// - Database errors (DatabaseError, DbConnectionError) → EXIT_CODE_DATABASE_ERROR (3)
    /// - Other errors → EXIT_CODE_GENERIC_ERROR (1)
    pub fn exit_code(&self) -> i32 {
        match self {
            // Config/validation errors: misconfiguration or bad input
            ApiError::BadRequest(_)
            | ApiError::ValidationError(_)
            | ApiError::OperatorMismatch(_)
            | ApiError::InvalidIntegerRange(_) => EXIT_CODE_CONFIG_ERROR,
            // Database errors: can't connect or pool exhausted
            ApiError::DatabaseError(_) | ApiError::DbConnectionError(_) => EXIT_CODE_DATABASE_ERROR,
            ApiError::PermissionBackendUnavailable(_) => EXIT_CODE_PERMISSION_BACKEND_ERROR,
            // Generic errors: should not occur during startup
            _ => EXIT_CODE_GENERIC_ERROR,
        }
    }

    /// Message safe to expose over any public transport, including response
    /// bodies and streaming events.
    pub fn public_message(&self) -> &str {
        match self {
            ApiError::InternalServerError(_)
            | ApiError::DatabaseError(_)
            | ApiError::DbConnectionError(_)
            | ApiError::HashError(_) => PUBLIC_INTERNAL_ERROR,
            ApiError::ServiceUnavailable(_) => PUBLIC_SERVICE_UNAVAILABLE,
            ApiError::PermissionBackendUnavailable(_) => PUBLIC_PERMISSION_BACKEND_UNAVAILABLE,
            ApiError::Unauthorized(message)
            | ApiError::Forbidden(message)
            | ApiError::NotAcceptable(message)
            | ApiError::UnsupportedMediaType(message)
            | ApiError::PayloadTooLarge(message)
            | ApiError::Conflict(message)
            | ApiError::TooManyRequests(message)
            | ApiError::NotFound(message)
            | ApiError::Gone(message)
            | ApiError::NotImplemented(message)
            | ApiError::BadRequest(message)
            | ApiError::OperatorMismatch(message)
            | ApiError::InvalidIntegerRange(message)
            | ApiError::ValidationError(message) => message,
        }
    }
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ApiError::HashError(message) => write!(f, "{message}"),
            ApiError::NotFound(message) => write!(f, "{message}"),
            ApiError::Gone(message) => write!(f, "{message}"),
            ApiError::Conflict(message) => write!(f, "{message}"),
            ApiError::TooManyRequests(message) => write!(f, "{message}"),
            ApiError::ServiceUnavailable(message) => write!(f, "{message}"),
            ApiError::NotImplemented(message) => write!(f, "{message}"),
            ApiError::PermissionBackendUnavailable(message) => write!(f, "{message}"),
            ApiError::Forbidden(message) => write!(f, "{message}"),
            ApiError::InternalServerError(message) => write!(f, "{message}"),
            ApiError::Unauthorized(message) => write!(f, "{message}"),
            ApiError::DatabaseError(message) => write!(f, "{message}"),
            ApiError::DbConnectionError(message) => write!(f, "{message}"),
            ApiError::BadRequest(message) => write!(f, "{message}"),
            ApiError::OperatorMismatch(message) => write!(f, "{message}"),
            ApiError::InvalidIntegerRange(message) => write!(f, "{message}"),
            ApiError::ValidationError(message) => write!(f, "{message}"),
            ApiError::NotAcceptable(message) => write!(f, "{message}"),
            ApiError::UnsupportedMediaType(message) => write!(f, "{message}"),
            ApiError::PayloadTooLarge(message) => write!(f, "{message}"),
        }
    }
}

impl ResponseError for ApiError {
    fn error_response(&self) -> HttpResponse {
        metrics::api_error(self.class());
        match self {
            ApiError::Conflict(message) => {
                HttpResponse::Conflict().json(json!({ "error": "Conflict", "message": message}))
            }
            ApiError::TooManyRequests(message) => HttpResponse::TooManyRequests()
                .json(json!({ "error": "Too Many Requests", "message": message })),
            ApiError::ServiceUnavailable(_) => HttpResponse::ServiceUnavailable().json(
                json!({ "error": "Service Unavailable", "message": PUBLIC_SERVICE_UNAVAILABLE }),
            ),
            ApiError::NotImplemented(message) => HttpResponse::NotImplemented()
                .json(json!({ "error": "Not Implemented", "message": message })),
            ApiError::PermissionBackendUnavailable(_) => HttpResponse::ServiceUnavailable()
                .append_header(("Retry-After", "5"))
                .json(json!({
                    "error": "Permission Backend Unavailable",
                    "message": PUBLIC_PERMISSION_BACKEND_UNAVAILABLE
                })),
            ApiError::Forbidden(message) => {
                HttpResponse::Forbidden().json(json!({ "error": "Forbidden", "message": message }))
            }
            ApiError::NotAcceptable(message) => HttpResponse::NotAcceptable()
                .json(json!({ "error": "Not Acceptable", "message": message })),
            ApiError::UnsupportedMediaType(message) => HttpResponse::UnsupportedMediaType()
                .json(json!({ "error": "Unsupported Media Type", "message": message })),
            ApiError::PayloadTooLarge(message) => HttpResponse::PayloadTooLarge()
                .json(json!({ "error": "Payload Too Large", "message": message })),
            ApiError::InternalServerError(_)
            | ApiError::DbConnectionError(_)
            | ApiError::DatabaseError(_)
            | ApiError::HashError(_) => HttpResponse::InternalServerError().json(
                json!({ "error": "Internal Server Error", "message": PUBLIC_INTERNAL_ERROR }),
            ),
            ApiError::Unauthorized(message) => HttpResponse::Unauthorized()
                .json(json!({ "error": "Unauthorized", "message": message })),
            ApiError::NotFound(message) => {
                HttpResponse::NotFound().json(json!({ "error": "Not Found", "message": message }))
            }
            ApiError::Gone(message) => {
                HttpResponse::Gone().json(json!({ "error": "Gone", "message": message }))
            }
            ApiError::BadRequest(message) => HttpResponse::BadRequest()
                .json(json!({ "error": "Bad Request", "message": message })),
            ApiError::OperatorMismatch(message) => HttpResponse::BadRequest()
                .json(json!({ "error": "Operator Mismatch", "message": message })),
            ApiError::InvalidIntegerRange(message) => HttpResponse::BadRequest()
                .json(json!({ "error": "Invalid Integer Range", "message": message })),
            ApiError::ValidationError(message) => HttpResponse::NotAcceptable()
                .json(json!({ "error": "Validation Error", "message": message })),
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            ApiError::Conflict(_) => StatusCode::CONFLICT,
            ApiError::TooManyRequests(_) => StatusCode::TOO_MANY_REQUESTS,
            ApiError::ServiceUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            ApiError::NotImplemented(_) => StatusCode::NOT_IMPLEMENTED,
            ApiError::PermissionBackendUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            ApiError::Forbidden(_) => StatusCode::FORBIDDEN,
            ApiError::NotAcceptable(_) => StatusCode::NOT_ACCEPTABLE,
            ApiError::UnsupportedMediaType(_) => StatusCode::UNSUPPORTED_MEDIA_TYPE,
            ApiError::PayloadTooLarge(_) => StatusCode::PAYLOAD_TOO_LARGE,
            ApiError::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            ApiError::InternalServerError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ApiError::DatabaseError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ApiError::DbConnectionError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ApiError::HashError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ApiError::NotFound(_) => StatusCode::NOT_FOUND,
            ApiError::Gone(_) => StatusCode::GONE,
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

/// Ensure that json deserialization errors are exported as a bad request and
/// that the error itself is returned as json.
pub fn json_error_handler(err: JsonPayloadError, _: &HttpRequest) -> actix_web::Error {
    metrics::extraction_failure("json");
    let error_message = format!("Json deserialize error: {err}");
    ApiError::BadRequest(error_message).into()
}

/// Ensure that path-parameter errors are exported as a bad request rather than actix's default
/// `404`. The validating `Deserialize` impls on the id newtypes already surface a clear message
/// (e.g. "Invalid collection id '0': must be a positive integer"); this maps that to a `400` so an
/// invalid id is rejected at the edge as the contract promises.
pub fn path_error_handler(err: actix_web::error::PathError, _: &HttpRequest) -> actix_web::Error {
    metrics::extraction_failure("path");
    ApiError::BadRequest(err.to_string()).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exit_codes_are_distinct() {
        // Verify all exit codes are unique
        let codes = [
            EXIT_CODE_CONFIG_ERROR,
            EXIT_CODE_DATABASE_ERROR,
            EXIT_CODE_INIT_ERROR,
            EXIT_CODE_GENERIC_ERROR,
        ];

        for i in 0..codes.len() {
            for j in (i + 1)..codes.len() {
                assert_ne!(
                    codes[i], codes[j],
                    "Exit codes should be distinct: {} vs {}",
                    codes[i], codes[j]
                );
            }
        }
    }

    #[test]
    fn test_api_error_exit_code_mapping() {
        // Test that different error types map to expected exit codes
        let config_error = ApiError::BadRequest("config error".to_string());
        assert_eq!(config_error.exit_code(), EXIT_CODE_CONFIG_ERROR);

        let db_error = ApiError::DbConnectionError("connection failed".to_string());
        assert_eq!(db_error.exit_code(), EXIT_CODE_DATABASE_ERROR);

        let generic_error = ApiError::InternalServerError("internal error".to_string());
        assert_eq!(generic_error.exit_code(), EXIT_CODE_GENERIC_ERROR);
    }

    #[test]
    fn test_fatal_error_type_signature() {
        // This test verifies that fatal_error has the correct type signature
        // and can be called with expected parameters (compile-time check)
        fn _test_fatal_error_compiles() -> ! {
            fatal_error("test message", EXIT_CODE_CONFIG_ERROR)
        }

        // We don't actually call it (would exit), just verify it compiles
    }

    #[tokio::test]
    async fn test_api_error_from_pool_error() {
        let pool = crate::db::init_pool("postgres://invalid:5432/nonexistent", 1);
        let result = crate::db::with_connection(&pool, async |_conn| Ok::<(), ApiError>(())).await;
        match result {
            Err(ApiError::DbConnectionError(_)) => {}
            Err(other) => panic!("Expected DbConnectionError from pool error, got: {other:?}"),
            Ok(_) => panic!("Expected pool connection to fail"),
        }
    }

    #[test]
    fn test_api_error_from_diesel_not_found() {
        // Test that Diesel NotFound error converts correctly
        let diesel_error = DieselError::NotFound;
        let api_error = ApiError::from(diesel_error);

        match api_error {
            ApiError::NotFound(_) => {
                // Expected
            }
            _ => panic!("Expected NotFound error, got: {:?}", api_error),
        }
    }

    #[test]
    fn test_api_error_http_status_codes() {
        // Verify that errors map to correct HTTP status codes
        assert_eq!(
            ApiError::NotFound("test".to_string()).status_code(),
            StatusCode::NOT_FOUND
        );

        assert_eq!(
            ApiError::BadRequest("test".to_string()).status_code(),
            StatusCode::BAD_REQUEST
        );

        assert_eq!(
            ApiError::Unauthorized("test".to_string()).status_code(),
            StatusCode::UNAUTHORIZED
        );

        assert_eq!(
            ApiError::Forbidden("test".to_string()).status_code(),
            StatusCode::FORBIDDEN
        );

        assert_eq!(
            ApiError::Conflict("test".to_string()).status_code(),
            StatusCode::CONFLICT
        );

        assert_eq!(
            ApiError::TooManyRequests("test".to_string()).status_code(),
            StatusCode::TOO_MANY_REQUESTS
        );

        assert_eq!(
            ApiError::ServiceUnavailable("test".to_string()).status_code(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[actix_web::test]
    async fn internal_error_responses_do_not_expose_details() {
        use actix_web::body::to_bytes;

        for error in [
            ApiError::InternalServerError("secret internal path".to_string()),
            ApiError::DatabaseError("password=database-secret".to_string()),
            ApiError::DbConnectionError("postgres://user:secret@db/app".to_string()),
            ApiError::HashError("hash implementation detail".to_string()),
        ] {
            let response = error.error_response();
            let body = to_bytes(response.into_body()).await.unwrap();
            let body = std::str::from_utf8(&body).unwrap();
            assert!(body.contains(PUBLIC_INTERNAL_ERROR));
            assert!(!body.contains("secret"));
            assert!(!body.contains("implementation detail"));
        }
    }

    #[actix_web::test]
    async fn service_unavailable_response_does_not_expose_details() {
        use actix_web::body::to_bytes;

        let response = ApiError::ServiceUnavailable(
            "database host db.internal.example rejected password".to_string(),
        )
        .error_response();
        let body = to_bytes(response.into_body()).await.unwrap();
        let body = std::str::from_utf8(&body).unwrap();
        assert!(body.contains(PUBLIC_SERVICE_UNAVAILABLE));
        assert!(!body.contains("db.internal.example"));
    }
}
