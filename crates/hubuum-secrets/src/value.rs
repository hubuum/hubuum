use std::fmt;
use std::sync::Arc;

use zeroize::Zeroize;

use crate::{SecretError, SecretErrorKind};

pub struct SecretValue(Box<[u8]>);

impl SecretValue {
    pub fn new(value: impl Into<Vec<u8>>) -> Result<Self, SecretError> {
        let value = value.into();
        if value.is_empty() {
            return Err(SecretError::new(
                SecretErrorKind::InvalidValue,
                "resolved secret value must not be empty",
            ));
        }
        Ok(Self(value.into_boxed_slice()))
    }

    pub fn expose(&self) -> &[u8] {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn expose_utf8(&self) -> Result<&str, SecretError> {
        std::str::from_utf8(self.expose()).map_err(|_| {
            SecretError::new(
                SecretErrorKind::InvalidValue,
                "resolved secret is not valid UTF-8",
            )
        })
    }
}

impl Drop for SecretValue {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

impl fmt::Debug for SecretValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretValue(<redacted>)")
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct SecretVersion(Arc<str>);

impl SecretVersion {
    pub fn new(value: impl Into<String>) -> Result<Self, SecretError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 128
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
        {
            return Err(SecretError::new(
                SecretErrorKind::InvalidValue,
                "secret versions must contain 1-128 safe ASCII characters",
            ));
        }
        Ok(Self(value.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretVersion(<redacted>)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_do_not_print_secret_bytes() {
        let value = SecretValue::new(b"canary-secret".to_vec()).unwrap();
        assert_eq!(format!("{value:?}"), "SecretValue(<redacted>)");
        assert!(!format!("{value:?}").contains("canary-secret"));
    }
}
