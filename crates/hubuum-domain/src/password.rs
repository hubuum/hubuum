use std::fmt;

/// A nonempty plaintext password within the supported authentication input limit.
/// Whitespace and Unicode are preserved exactly; passwords are never trimmed.
pub struct PlaintextPassword(String);

impl PlaintextPassword {
    pub const MAX_CHARACTERS: usize = 4096;

    pub fn new(value: String) -> Result<Self, PasswordValidationError> {
        if value.is_empty() {
            return Err(PasswordValidationError::Empty);
        }
        if value.chars().take(Self::MAX_CHARACTERS + 1).count() > Self::MAX_CHARACTERS {
            return Err(PasswordValidationError::TooLong);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for PlaintextPassword {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PlaintextPassword(<redacted>)")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PasswordValidationError {
    Empty,
    TooLong,
}

impl fmt::Display for PasswordValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Empty => "password must not be empty",
            Self::TooLong => "password must not exceed 4096 characters",
        })
    }
}

impl std::error::Error for PasswordValidationError {}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case::empty("".into(), false)]
    #[case::spaces("  ".into(), true)]
    #[case::unicode_limit("é".repeat(4096), true)]
    #[case::too_long("a".repeat(4097), false)]
    fn validates_password_without_normalizing(#[case] input: String, #[case] valid: bool) {
        let result = PlaintextPassword::new(input.clone());
        assert_eq!(result.is_ok(), valid);
        if let Ok(password) = result {
            assert_eq!(password.as_str(), input);
        }
    }
}
