use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MaintenanceState {
    Normal,
    Draining,
    RecoveryQuarantine,
}

impl MaintenanceState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Draining => "draining",
            Self::RecoveryQuarantine => "recovery_quarantine",
        }
    }

    pub const fn is_normal(self) -> bool {
        matches!(self, Self::Normal)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceStateParseError {
    value: String,
}

impl fmt::Display for MaintenanceStateParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "unknown maintenance state '{}'", self.value)
    }
}

impl std::error::Error for MaintenanceStateParseError {}

impl TryFrom<&str> for MaintenanceState {
    type Error = MaintenanceStateParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "normal" => Ok(Self::Normal),
            "draining" => Ok(Self::Draining),
            "recovery_quarantine" => Ok(Self::RecoveryQuarantine),
            _ => Err(MaintenanceStateParseError {
                value: value.to_string(),
            }),
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
    #[case("recovery_quarantine", MaintenanceState::RecoveryQuarantine)]
    fn database_values_map_to_maintenance_states(
        #[case] value: &str,
        #[case] expected: MaintenanceState,
    ) {
        assert_eq!(MaintenanceState::try_from(value).unwrap(), expected);
    }

    #[test]
    fn unknown_database_value_is_rejected() {
        assert!(MaintenanceState::try_from("unknown").is_err());
    }
}
