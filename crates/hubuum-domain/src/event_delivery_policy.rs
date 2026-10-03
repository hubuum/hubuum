use serde::{Deserialize, Serialize};

/// An optional minimum interval between admissions to one sink.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "RawPolicy")]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct EventDeliveryPolicy {
    #[cfg_attr(feature = "openapi", schema(minimum = 1, maximum = 86_400_000))]
    min_interval_ms: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPolicy {
    #[serde(default)]
    min_interval_ms: Option<u64>,
}

impl TryFrom<RawPolicy> for EventDeliveryPolicy {
    type Error = String;
    fn try_from(raw: RawPolicy) -> Result<Self, Self::Error> {
        Self::new(raw.min_interval_ms)
    }
}
impl EventDeliveryPolicy {
    pub fn new(min_interval_ms: Option<u64>) -> Result<Self, String> {
        if min_interval_ms.is_some_and(|value| value == 0 || value > 86_400_000) {
            return Err("min_interval_ms must be between 1 and 86400000, or null".to_string());
        }
        Ok(Self { min_interval_ms })
    }
    pub const fn min_interval_ms(self) -> Option<u64> {
        self.min_interval_ms
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub enum EventDeliveryPurpose {
    #[default]
    Event,
    Test,
}
impl EventDeliveryPurpose {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Event => "event",
            Self::Test => "test",
        }
    }
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "event" => Some(Self::Event),
            "test" => Some(Self::Test),
            _ => None,
        }
    }
}
