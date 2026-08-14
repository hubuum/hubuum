use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
#[cfg(feature = "openapi")]
use utoipa::ToSchema;

/// Durable lifecycle state for one event delivery.
///
/// The textual representation is shared by storage adapters, API models, and
/// operational reporting so each layer cannot silently invent different
/// persisted status values.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "openapi", derive(ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum EventDeliveryStatus {
    Pending,
    InFlight,
    Succeeded,
    Failed,
    Dead,
}

impl EventDeliveryStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::InFlight => "in_flight",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Dead => "dead",
        }
    }
}

impl fmt::Display for EventDeliveryStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for EventDeliveryStatus {
    type Err = EventDeliveryStatusParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "pending" => Ok(Self::Pending),
            "in_flight" => Ok(Self::InFlight),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "dead" => Ok(Self::Dead),
            _ => Err(EventDeliveryStatusParseError(value.to_string())),
        }
    }
}

/// Failure to decode a persisted event-delivery status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventDeliveryStatusParseError(String);

impl fmt::Display for EventDeliveryStatusParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Unsupported event delivery status: '{}'", self.0)
    }
}

impl std::error::Error for EventDeliveryStatusParseError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persisted_statuses_round_trip() {
        for status in [
            EventDeliveryStatus::Pending,
            EventDeliveryStatus::InFlight,
            EventDeliveryStatus::Succeeded,
            EventDeliveryStatus::Failed,
            EventDeliveryStatus::Dead,
        ] {
            assert_eq!(status.as_str().parse(), Ok(status));
        }
    }

    #[test]
    fn unknown_status_is_rejected() {
        assert_eq!(
            "retrying".parse::<EventDeliveryStatus>().unwrap_err(),
            EventDeliveryStatusParseError("retrying".to_string())
        );
    }
}
