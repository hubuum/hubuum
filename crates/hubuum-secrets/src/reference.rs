use std::fmt;

use crate::{SecretError, SecretErrorKind, SecretVersion};

const MAX_NAME_BYTES: usize = 128;
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SecretName(String);

impl SecretName {
    pub fn new(value: impl Into<String>) -> Result<Self, SecretError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > MAX_NAME_BYTES
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        {
            return Err(SecretError::new(
                SecretErrorKind::InvalidReference,
                "secret names must contain 1-128 ASCII letters, numbers, underscores, or hyphens",
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretName(<redacted>)")
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SecretProviderKind {
    Environment,
    File,
}

impl fmt::Debug for SecretProviderKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl SecretProviderKind {
    pub const fn environment() -> Self {
        Self::Environment
    }

    pub const fn file() -> Self {
        Self::File
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Environment => "environment",
            Self::File => "file",
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub enum SecretVersionSelector {
    Latest,
    Exact(SecretVersion),
}

impl fmt::Debug for SecretVersionSelector {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Latest => formatter.write_str("Latest"),
            Self::Exact(_) => formatter.write_str("Exact(<redacted>)"),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct SecretRef {
    provider: SecretProviderKind,
    name: SecretName,
    version: SecretVersionSelector,
}

impl SecretRef {
    pub fn new(provider: SecretProviderKind, name: SecretName) -> Self {
        Self {
            provider,
            name,
            version: SecretVersionSelector::Latest,
        }
    }

    pub fn with_version(mut self, version: SecretVersionSelector) -> Self {
        self.version = version;
        self
    }

    pub fn provider(&self) -> &SecretProviderKind {
        &self.provider
    }

    pub fn name(&self) -> &SecretName {
        &self.name
    }

    pub fn version(&self) -> &SecretVersionSelector {
        &self.version
    }
}

impl fmt::Debug for SecretRef {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SecretRef")
            .field("provider", &self.provider)
            .field("name", &"<redacted>")
            .field("version", &self.version)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn references_reject_paths_and_debug_without_names() {
        assert!(SecretName::new("../database-password").is_err());
        let reference = SecretRef::new(
            SecretProviderKind::environment(),
            SecretName::new("database-password").unwrap(),
        );

        let debug = format!("{reference:?}");
        assert!(!debug.contains("database-password"));
        assert!(debug.contains("environment"));
    }
}
