//! Credential envelopes and keyed digests, independent of configuration and storage.

use std::fmt;

use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("Invalid credential")]
pub struct CredentialError;

#[derive(Clone)]
pub struct VersionedCredential {
    raw: String,
    key_id: String,
}

impl VersionedCredential {
    fn parse(raw: String, prefix: &str) -> Result<Self, CredentialError> {
        let mut parts = raw.split('.');
        if parts.next() != Some(prefix) {
            return Err(CredentialError);
        }
        let key_id = parts.next().ok_or(CredentialError)?;
        let secret = parts.next().ok_or(CredentialError)?;
        if key_id.is_empty() || !is_valid_secret(secret) || parts.next().is_some() {
            return Err(CredentialError);
        }
        let key_id = key_id.to_owned();
        Ok(Self { raw, key_id })
    }

    pub fn parse_refresh(raw: String) -> Result<Self, CredentialError> {
        Self::parse(raw, "hbr1")
    }

    pub fn issue_refresh(key_id: &str, secret: &str) -> Result<Self, CredentialError> {
        Self::parse_refresh(format!("hbr1.{key_id}.{secret}"))
    }

    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    pub fn as_str(&self) -> &str {
        &self.raw
    }
}

impl fmt::Debug for VersionedCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("VersionedCredential(<redacted>)")
    }
}

#[derive(Clone)]
pub struct BearerCredential {
    value: BearerValue,
}

#[derive(Clone)]
enum BearerValue {
    Legacy(String),
    Versioned(VersionedCredential),
}

impl BearerCredential {
    pub fn parse(raw: String) -> Result<Self, CredentialError> {
        if raw.is_empty() || raw.starts_with("hbr") {
            return Err(CredentialError);
        }
        let value = if raw.starts_with("hbt") {
            BearerValue::Versioned(VersionedCredential::parse(raw, "hbt1")?)
        } else {
            BearerValue::Legacy(raw)
        };
        Ok(Self { value })
    }
    pub fn issue(key_id: &str, secret: &str) -> Result<Self, CredentialError> {
        Self::parse(format!("hbt1.{key_id}.{secret}"))
    }
    pub fn key_id(&self) -> Option<&str> {
        match &self.value {
            BearerValue::Legacy(_) => None,
            BearerValue::Versioned(value) => Some(value.key_id()),
        }
    }
    pub fn as_str(&self) -> &str {
        match &self.value {
            BearerValue::Legacy(raw) => raw,
            BearerValue::Versioned(value) => value.as_str(),
        }
    }
}

impl fmt::Debug for BearerCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BearerCredential(<redacted>)")
    }
}

fn is_valid_secret(value: &str) -> bool {
    value.len() == 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub fn keyed_digest(raw: &str, key: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts keys of any length");
    mac.update(raw.as_bytes());
    let digest = mac.finalize().into_bytes();
    let mut encoded = String::with_capacity(digest.len() * 2);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in digest {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refresh_credentials_cannot_authenticate_as_bearers() {
        let refresh = VersionedCredential::issue_refresh("key", &"a".repeat(128)).unwrap();
        assert!(BearerCredential::parse(refresh.as_str().to_owned()).is_err());
    }

    #[test]
    fn unknown_versions_cannot_fall_back_to_legacy() {
        assert!(BearerCredential::parse(format!("hbt2.key.{}", "a".repeat(128))).is_err());
    }

    #[test]
    fn debug_redacts_credential_secrets() {
        let credential = BearerCredential::issue("key", &"a".repeat(128)).unwrap();
        assert_eq!(format!("{credential:?}"), "BearerCredential(<redacted>)");
    }
}
