//! Stable, sanitized client recovery reasons for the interaction protocol.
use actix_web::http::StatusCode;
use hubuum_domain::InteractionTransitionError;
use serde::Serialize;

use super::ApiError;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum InteractionError {
    Invalid,
    ConfigurationChanged,
    Expired,
    Consumed,
    Cancelled,
    Denied,
    Failed,
    Pending,
    SlowDown { retry_after_seconds: u32 },
    Unsupported,
    ProviderUnavailable,
}
impl InteractionError {
    pub const fn reason(&self) -> &'static str {
        match self {
            Self::Invalid => "interaction_invalid",
            Self::ConfigurationChanged => "configuration_changed",
            Self::Expired => "interaction_expired",
            Self::Consumed => "interaction_consumed",
            Self::Cancelled => "interaction_cancelled",
            Self::Denied => "access_denied",
            Self::Failed => "interaction_failed",
            Self::Pending => "interaction_pending",
            Self::SlowDown { .. } => "slow_down",
            Self::Unsupported => "capability_unsupported",
            Self::ProviderUnavailable => "provider_unavailable",
        }
    }
    pub const fn message(&self) -> &'static str {
        match self {
            Self::Invalid => "Invalid authentication interaction or credentials",
            Self::ConfigurationChanged => "Authentication configuration changed; begin again",
            Self::Expired => "Authentication interaction expired; begin again",
            Self::Consumed => "Authentication interaction was already consumed",
            Self::Cancelled => "Authentication interaction was cancelled",
            Self::Denied => "Authentication interaction was denied",
            Self::Failed => "Authentication interaction failed; begin again",
            Self::Pending => "Authentication interaction is not ready",
            Self::SlowDown { .. } => "Slow down and honor Retry-After",
            Self::Unsupported => "This authentication capability is not supported",
            Self::ProviderUnavailable => "Authentication provider temporarily unavailable",
        }
    }
    pub const fn status(&self) -> StatusCode {
        match self {
            Self::Invalid => StatusCode::UNAUTHORIZED,
            Self::ConfigurationChanged | Self::Consumed | Self::Pending | Self::Failed => {
                StatusCode::CONFLICT
            }
            Self::Expired => StatusCode::GONE,
            Self::Cancelled | Self::Denied => StatusCode::FORBIDDEN,
            Self::SlowDown { .. } => StatusCode::TOO_MANY_REQUESTS,
            Self::Unsupported => StatusCode::UNPROCESSABLE_ENTITY,
            Self::ProviderUnavailable => StatusCode::SERVICE_UNAVAILABLE,
        }
    }
    pub const fn retry_after(&self) -> Option<u32> {
        match self {
            Self::SlowDown {
                retry_after_seconds,
            } => Some(*retry_after_seconds),
            Self::ProviderUnavailable => Some(5),
            _ => None,
        }
    }
}
impl From<InteractionError> for ApiError {
    fn from(error: InteractionError) -> Self {
        Self::AuthenticationInteraction(error)
    }
}
impl From<InteractionTransitionError> for ApiError {
    fn from(error: InteractionTransitionError) -> Self {
        match error {
            InteractionTransitionError::InvalidLifetime | InteractionTransitionError::Invalid => {
                InteractionError::Invalid
            }
            InteractionTransitionError::ConfigurationChanged => {
                InteractionError::ConfigurationChanged
            }
            InteractionTransitionError::Expired => InteractionError::Expired,
            InteractionTransitionError::Consumed => InteractionError::Consumed,
            InteractionTransitionError::Cancelled => InteractionError::Cancelled,
            InteractionTransitionError::Denied => InteractionError::Denied,
            InteractionTransitionError::Failed => InteractionError::Failed,
            InteractionTransitionError::Pending => InteractionError::Pending,
            InteractionTransitionError::SlowDown {
                retry_after_seconds,
            } => InteractionError::SlowDown {
                retry_after_seconds,
            },
        }
        .into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{ResponseError, body::to_bytes};
    use rstest::rstest;

    #[rstest]
    #[case::invalid(InteractionError::Invalid, 401, "interaction_invalid", None)]
    #[case::consumed(InteractionError::Consumed, 409, "interaction_consumed", None)]
    #[case::cancelled(InteractionError::Cancelled, 403, "interaction_cancelled", None)]
    #[case::failed(InteractionError::Failed, 409, "interaction_failed", None)]
    #[case::pending(InteractionError::Pending, 409, "interaction_pending", None)]
    #[case::unsupported(InteractionError::Unsupported, 422, "capability_unsupported", None)]
    #[case::expired(InteractionError::Expired, 410, "interaction_expired", None)]
    #[case::changed(
        InteractionError::ConfigurationChanged,
        409,
        "configuration_changed",
        None
    )]
    #[case::denied(InteractionError::Denied, 403, "access_denied", None)]
    #[case::slow(InteractionError::SlowDown { retry_after_seconds: 15 }, 429, "slow_down", Some("15"))]
    #[case::unavailable(
        InteractionError::ProviderUnavailable,
        503,
        "provider_unavailable",
        Some("5")
    )]
    #[actix_web::test]
    async fn ordinary_api_errors_preserve_recovery_reasons(
        #[case] error: InteractionError,
        #[case] status: u16,
        #[case] reason: &str,
        #[case] retry: Option<&str>,
    ) {
        crate::observability::metrics::init().unwrap();
        let response = ApiError::from(error).error_response();
        assert_eq!(response.status().as_u16(), status);
        assert_eq!(response.headers().get("cache-control").unwrap(), "no-store");
        assert_eq!(
            response
                .headers()
                .get("retry-after")
                .map(|value| value.to_str().unwrap()),
            retry
        );
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body()).await.unwrap()).unwrap();
        assert_eq!(body["reason"], reason);
    }
}
