use std::fmt;

use crate::errors::ApiError;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MaintenanceState {
    Normal,
    Draining,
}

impl MaintenanceState {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Draining => "draining",
        }
    }

    pub(crate) const fn is_normal(self) -> bool {
        matches!(self, Self::Normal)
    }

    pub(crate) fn from_db(value: &str) -> Result<Self, ApiError> {
        match value {
            "normal" => Ok(Self::Normal),
            "draining" => Ok(Self::Draining),
            _ => Err(ApiError::InternalServerError(format!(
                "Unknown maintenance state '{value}'"
            ))),
        }
    }
}

impl fmt::Display for MaintenanceState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::MaintenanceState;

    #[rstest]
    #[case("normal", MaintenanceState::Normal)]
    #[case("draining", MaintenanceState::Draining)]
    fn database_values_map_to_maintenance_states(
        #[case] value: &str,
        #[case] expected: MaintenanceState,
    ) {
        assert_eq!(MaintenanceState::from_db(value).unwrap(), expected);
    }

    #[test]
    fn unknown_database_value_is_rejected() {
        assert!(MaintenanceState::from_db("unknown").is_err());
    }
}
