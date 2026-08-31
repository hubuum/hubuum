use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretErrorKind {
    InvalidReference,
    InvalidProviderConfiguration,
    ProviderNotConfigured,
    NotFound,
    PermissionDenied,
    InvalidValue,
    TooLarge,
    UnsafePath,
    UnsupportedVersion,
    ChangedDuringRead,
    Timeout,
    Unavailable,
    Internal,
}

#[derive(Clone, PartialEq, Eq)]
pub struct SecretError {
    kind: SecretErrorKind,
    message: String,
}

impl SecretError {
    pub fn new(kind: SecretErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub fn kind(&self) -> SecretErrorKind {
        self.kind
    }
}

impl fmt::Debug for SecretError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SecretError")
            .field("kind", &self.kind)
            .field("message", &self.message)
            .finish()
    }
}

impl fmt::Display for SecretError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for SecretError {}
