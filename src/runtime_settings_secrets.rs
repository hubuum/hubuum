use std::fmt;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use hubuum_secrets::SecretValue;
use ring::{
    aead::{self, Aad, LessSafeKey, Nonce, UnboundKey},
    rand::{SecureRandom, SystemRandom},
};
use serde::{Deserialize, Serialize};

const ENVELOPE_VERSION: u8 = 1;
const KEY_BYTES: usize = 32;
const NONCE_BYTES: usize = 12;

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EncryptedSecret {
    version: u8,
    key_ref: String,
    key_version: String,
    wrapped_key_nonce: String,
    wrapped_key: String,
    data_nonce: String,
    ciphertext: String,
}

impl EncryptedSecret {
    pub(crate) fn key_ref(&self) -> &str {
        &self.key_ref
    }

    pub(crate) fn key_version(&self) -> &str {
        &self.key_version
    }
}

impl fmt::Debug for EncryptedSecret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EncryptedSecret")
            .field("version", &self.version)
            .field("key_ref", &"<redacted>")
            .field("key_version", &"<redacted>")
            .field("wrapped_key_nonce", &"<redacted>")
            .field("wrapped_key", &"<redacted>")
            .field("data_nonce", &"<redacted>")
            .field("ciphertext", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SecretProtectionError {
    InvalidKey,
    InvalidEnvelope,
    RandomnessUnavailable,
    EncryptionFailed,
    AuthenticationFailed,
}

impl fmt::Display for SecretProtectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidKey => "runtime settings encryption key must be 32 bytes",
            Self::InvalidEnvelope => "runtime settings encrypted secret is invalid",
            Self::RandomnessUnavailable => "runtime settings encryption randomness is unavailable",
            Self::EncryptionFailed => "runtime settings secret encryption failed",
            Self::AuthenticationFailed => "runtime settings secret authentication failed",
        })
    }
}

impl std::error::Error for SecretProtectionError {}

pub(crate) fn encrypt(
    key_ref: String,
    key_version: String,
    wrapping_key: &SecretValue,
    context: &str,
    plaintext: &SecretValue,
) -> Result<EncryptedSecret, SecretProtectionError> {
    validate_key(wrapping_key)?;
    let data_key = random_secret(KEY_BYTES)?;
    let data_nonce = random_nonce()?;
    let wrapped_key_nonce = random_nonce()?;
    let data_aad = associated_data(context, &key_ref, &key_version);

    let ciphertext = seal(
        data_key.expose(),
        &data_nonce,
        &data_aad,
        plaintext.expose(),
    )?;
    let wrapped_key = seal(
        wrapping_key.expose(),
        &wrapped_key_nonce,
        &data_aad,
        data_key.expose(),
    )?;

    Ok(EncryptedSecret {
        version: ENVELOPE_VERSION,
        key_ref,
        key_version,
        wrapped_key_nonce: STANDARD.encode(wrapped_key_nonce),
        wrapped_key: STANDARD.encode(wrapped_key),
        data_nonce: STANDARD.encode(data_nonce),
        ciphertext: STANDARD.encode(ciphertext),
    })
}

pub(crate) fn decrypt(
    envelope: &EncryptedSecret,
    wrapping_key: &SecretValue,
    context: &str,
) -> Result<SecretValue, SecretProtectionError> {
    validate_key(wrapping_key)?;
    if envelope.version != ENVELOPE_VERSION {
        return Err(SecretProtectionError::InvalidEnvelope);
    }
    let data_aad = associated_data(context, &envelope.key_ref, &envelope.key_version);
    let wrapped_key_nonce = decode_nonce(&envelope.wrapped_key_nonce)?;
    let data_nonce = decode_nonce(&envelope.data_nonce)?;
    let mut wrapped_key = decode(&envelope.wrapped_key)?;
    let data_key = open(
        wrapping_key.expose(),
        &wrapped_key_nonce,
        &data_aad,
        &mut wrapped_key,
    )?;
    if data_key.len() != KEY_BYTES {
        return Err(SecretProtectionError::InvalidEnvelope);
    }
    let mut ciphertext = decode(&envelope.ciphertext)?;
    open(data_key.expose(), &data_nonce, &data_aad, &mut ciphertext)
}

fn validate_key(key: &SecretValue) -> Result<(), SecretProtectionError> {
    if key.len() == KEY_BYTES {
        Ok(())
    } else {
        Err(SecretProtectionError::InvalidKey)
    }
}

fn associated_data(context: &str, key_ref: &str, key_version: &str) -> Vec<u8> {
    format!("hubuum-runtime-secret:v{ENVELOPE_VERSION}:{context}:{key_ref}:{key_version}")
        .into_bytes()
}

fn random_secret(length: usize) -> Result<SecretValue, SecretProtectionError> {
    let mut bytes = vec![0; length];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| SecretProtectionError::RandomnessUnavailable)?;
    SecretValue::new(bytes).map_err(|_| SecretProtectionError::EncryptionFailed)
}

fn random_nonce() -> Result<[u8; NONCE_BYTES], SecretProtectionError> {
    let mut nonce = [0; NONCE_BYTES];
    SystemRandom::new()
        .fill(&mut nonce)
        .map_err(|_| SecretProtectionError::RandomnessUnavailable)?;
    Ok(nonce)
}

fn seal(
    key: &[u8],
    nonce: &[u8; NONCE_BYTES],
    aad: &[u8],
    plaintext: &[u8],
) -> Result<Vec<u8>, SecretProtectionError> {
    let key = UnboundKey::new(&aead::AES_256_GCM, key)
        .map(LessSafeKey::new)
        .map_err(|_| SecretProtectionError::InvalidKey)?;
    let mut ciphertext = plaintext.to_vec();
    key.seal_in_place_append_tag(
        Nonce::assume_unique_for_key(*nonce),
        Aad::from(aad),
        &mut ciphertext,
    )
    .map_err(|_| SecretProtectionError::EncryptionFailed)?;
    Ok(ciphertext)
}

fn open(
    key: &[u8],
    nonce: &[u8; NONCE_BYTES],
    aad: &[u8],
    ciphertext: &mut Vec<u8>,
) -> Result<SecretValue, SecretProtectionError> {
    let key = UnboundKey::new(&aead::AES_256_GCM, key)
        .map(LessSafeKey::new)
        .map_err(|_| SecretProtectionError::InvalidKey)?;
    let plaintext = key
        .open_in_place(
            Nonce::assume_unique_for_key(*nonce),
            Aad::from(aad),
            ciphertext,
        )
        .map_err(|_| SecretProtectionError::AuthenticationFailed)?;
    let length = plaintext.len();
    ciphertext.truncate(length);
    let plaintext = std::mem::take(ciphertext);
    SecretValue::new(plaintext).map_err(|_| SecretProtectionError::InvalidEnvelope)
}

fn decode(value: &str) -> Result<Vec<u8>, SecretProtectionError> {
    STANDARD
        .decode(value)
        .map_err(|_| SecretProtectionError::InvalidEnvelope)
}

fn decode_nonce(value: &str) -> Result<[u8; NONCE_BYTES], SecretProtectionError> {
    decode(value)?
        .try_into()
        .map_err(|_| SecretProtectionError::InvalidEnvelope)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_round_trips_and_authenticates_its_context() {
        let wrapping_key = SecretValue::new(vec![7; KEY_BYTES]).unwrap();
        let plaintext = SecretValue::new(b"bind-password".to_vec()).unwrap();
        let envelope = encrypt(
            "key-v1".to_string(),
            "version-1".to_string(),
            &wrapping_key,
            "ldap:directory:bind_password",
            &plaintext,
        )
        .unwrap();
        let decrypted = decrypt(&envelope, &wrapping_key, "ldap:directory:bind_password").unwrap();
        assert_eq!(decrypted.expose(), b"bind-password");
        assert_eq!(
            decrypt(&envelope, &wrapping_key, "ldap:other:bind_password",).unwrap_err(),
            SecretProtectionError::AuthenticationFailed
        );
    }

    #[test]
    fn envelope_debug_redacts_key_metadata_and_ciphertext() {
        let wrapping_key = SecretValue::new(vec![7; KEY_BYTES]).unwrap();
        let plaintext = SecretValue::new(b"bind-password".to_vec()).unwrap();
        let envelope = encrypt(
            "active-key".to_string(),
            "version-1".to_string(),
            &wrapping_key,
            "ldap:directory:bind_password",
            &plaintext,
        )
        .unwrap();
        let debug = format!("{envelope:?}");
        assert!(!debug.contains("active-key"));
        assert!(!debug.contains("version-1"));
        assert!(!debug.contains("bind-password"));
    }
}
