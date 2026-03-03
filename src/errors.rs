use actix_web::{
    HttpRequest, HttpResponse, ResponseError, error::JsonPayloadError, http::StatusCode,
};
use diesel::r2d2::PoolError;
use diesel::result::{DatabaseErrorKind, Error as DieselError};
use serde::Serialize;
use serde_json::json;
use std::fmt;
use std::num::ParseIntError;

use tracing::{debug, error};

// Exit codes for startup/initialization failures.
// These help shell scripts and orchestration systems determine the failure mode.
pub const EXIT_CODE_GENERIC_ERROR: i32 = 1; // Non-specific/generic errors
pub const EXIT_CODE_CONFIG_ERROR: i32 = 2; // Config/validation
pub const EXIT_CODE_DATABASE_ERROR: i32 = 3; // Database connection/pool error
pub const EXIT_CODE_INIT_ERROR: i32 = 4; // Critical initialization error (admin user/group)
pub const EXIT_CODE_TLS_ERROR: i32 = 5; // TLS setup error

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
    std::process::exit(exit_code);
}

#[derive(Debug, Serialize, PartialEq)]
pub enum ApiError {
    Unauthorized(String),
    InternalServerError(String),
    Forbidden(String),
    NotAcceptable(String),
    PayloadTooLarge(String),
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

impl ApiError {
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
            // Generic errors: should not occur during startup
            _ => EXIT_CODE_GENERIC_ERROR,
        }
    }
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ApiError::HashError(message) => write!(f, "{message}"),
            ApiError::NotFound(message) => write!(f, "{message}"),
            ApiError::Conflict(message) => write!(f, "{message}"),
            ApiError::Forbidden(message) => write!(f, "{message}"),
            ApiError::InternalServerError(message) => write!(f, "{message}"),
            ApiError::Unauthorized(message) => write!(f, "{message}"),
            ApiError::DatabaseError(message) => write!(f, "{message}"),
            ApiError::DbConnectionError(message) => write!(f, "{message}"),
            ApiError::BadRequest(message) => write!(f, "{message}"),
            ApiError::OperatorMismatch(message) => write!(f, "{message}"),
            ApiError::InvalidIntegerRange(message) => write!(f, "{message}"),
            ApiError::ValidationError(message) => write!(f, "{message}"),
        }
    }
}

impl ResponseError for ApiError {
    fn error_response(&self) -> HttpResponse {
        match self {
            ApiError::Conflict(message) => {
                HttpResponse::Conflict().json(json!({ "error": "Conflict", "message": message}))
            }
            ApiError::Forbidden(message) => {
                HttpResponse::Forbidden().json(json!({ "error": "Forbidden", "message": message }))
            }
            ApiError::NotAcceptable(message) => HttpResponse::NotAcceptable()
                .json(json!({ "error": "Not Acceptable", "message": message })),
            ApiError::PayloadTooLarge(message) => HttpResponse::PayloadTooLarge()
                .json(json!({ "error": "Payload Too Large", "message": message })),
            ApiError::InternalServerError(message) => HttpResponse::InternalServerError()
                .json(json!({ "error": "Internal Server Error", "message": message })),
            ApiError::Unauthorized(message) => HttpResponse::Unauthorized()
                .json(json!({ "error": "Unauthorized", "message": message })),
            ApiError::DbConnectionError(message) => HttpResponse::InternalServerError()
                .json(json!({ "error": "Database Connection Error", "message": message })),
            ApiError::DatabaseError(message) => HttpResponse::InternalServerError()
                .json(json!({ "error": "Database Error", "message": message })),
            ApiError::HashError(message) => HttpResponse::InternalServerError()
                .json(json!({ "error": "Hash Error", "message": message })),
            ApiError::NotFound(message) => {
                HttpResponse::NotFound().json(json!({ "error": "Not Found", "message": message }))
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
            ApiError::Forbidden(_) => StatusCode::FORBIDDEN,
            ApiError::NotAcceptable(_) => StatusCode::NOT_ACCEPTABLE,
            ApiError::PayloadTooLarge(_) => StatusCode::PAYLOAD_TOO_LARGE,
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

    #[test]
    fn test_api_error_from_pool_error() {
        // Test that PoolError converts to DbConnectionError
        use diesel::PgConnection;
        use diesel::r2d2::ConnectionManager;

        let manager = ConnectionManager::<PgConnection>::new("postgres://invalid:5432/nonexistent");
        let pool_result = diesel::r2d2::Pool::builder()
            .max_size(1)
            .connection_timeout(std::time::Duration::from_millis(1))
            .build(manager);

        if let Ok(pool) = pool_result {
            let result = crate::db::with_connection(&pool, |_conn| Ok::<(), ApiError>(()));
            match result {
                Err(ApiError::DbConnectionError(_)) => {
                    // Expected
                }
                Err(other) => panic!("Expected DbConnectionError from PoolError, got: {other:?}"),
                Ok(_) => panic!("Expected pool connection to fail"),
            }
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
    }
}
