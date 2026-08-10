use crate::errors::ApiError;
use treetop_client::TreetopError;

/// Convert a treetop-client error into our ApiError.
///
/// Mapping rationale (per the spec, Phase 4 Errors and Observability section):
/// - Transport errors and Api 5xx → `PermissionBackendUnavailable` (HTTP 503).
///   The Treetop server is reachable-but-failing or unreachable; the client
///   should retry.
/// - Api 4xx (other than auth-related) → `InternalServerError`. These are
///   our bug — we sent something the server rejected. Surface as 500 to the
///   caller; ops should investigate.
/// - Deserialization → `PermissionBackendUnavailable`. The server returned
///   something we couldn't parse; treat as availability problem.
/// - InvalidUrl / Configuration → `InternalServerError`. Misconfiguration
///   detected after startup; should have failed at boot.
pub fn treetop_to_api_error(err: TreetopError) -> ApiError {
    match err {
        TreetopError::Transport(error) => {
            let category = if error.is_timeout() {
                "timeout"
            } else if error.is_connect() {
                "connection failure"
            } else {
                "transport failure"
            };
            ApiError::PermissionBackendUnavailable(format!("treetop {category}"))
        }
        TreetopError::Api { status, .. } if status.as_u16() >= 500 => {
            ApiError::PermissionBackendUnavailable(format!("treetop returned {status}"))
        }
        TreetopError::Api { status, .. } => {
            ApiError::InternalServerError(format!("treetop returned {status}"))
        }
        TreetopError::Deserialization(_) => ApiError::PermissionBackendUnavailable(
            "treetop response could not be decoded".to_string(),
        ),
        TreetopError::InvalidUrl(_) => {
            ApiError::InternalServerError("treetop URL is invalid".to_string())
        }
        TreetopError::Configuration(_) => {
            ApiError::InternalServerError("treetop client configuration is invalid".to_string())
        }
        _ => ApiError::PermissionBackendUnavailable("treetop client failure".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, from_str};

    use super::*;

    // Note: We cannot easily construct TreetopError::Transport, Api, or InvalidUrl
    // variants in unit tests because they depend on types from reqwest, http, and url
    // crates that are not direct dependencies of this crate. The error mapping logic
    // is straightforward pattern matching and will be exercised in integration tests
    // where the treetop-client actually returns these errors.
    //
    // We test the variants we CAN construct: Deserialization and Configuration.

    #[test]
    fn deserialization_maps_to_unavailable() {
        let err = TreetopError::Deserialization(from_str::<Value>("not json").unwrap_err());
        let api = treetop_to_api_error(err);
        assert!(
            matches!(api, ApiError::PermissionBackendUnavailable(_)),
            "Deserialization errors should map to PermissionBackendUnavailable"
        );
        assert_eq!(
            api,
            ApiError::PermissionBackendUnavailable(
                "treetop response could not be decoded".to_string()
            )
        );
    }

    #[test]
    fn configuration_maps_to_internal_error() {
        let err = TreetopError::Configuration("missing url".to_string());
        let api = treetop_to_api_error(err);
        assert!(
            matches!(api, ApiError::InternalServerError(_)),
            "Configuration errors should map to InternalServerError"
        );
        assert_eq!(
            api,
            ApiError::InternalServerError("treetop client configuration is invalid".to_string())
        );
    }

    // The Transport, Api, and InvalidUrl variant mappings are tested in
    // integration tests where the treetop-client actually returns these errors
    // from real HTTP operations.
}
