use std::fmt;
use std::sync::LazyLock;

use hubuum_secrets::{SecretError, SecretErrorKind, SecretValue};
use hubuum_storage_core::{MAX_TOKEN_HASH_KEYS, StorageTokenHashKeyId};
use sha2::{Digest, Sha256};
use uuid::Uuid;

const ACTIVE_KEY_ID_ENVIRONMENT: &str = "HUBUUM_TOKEN_HASH_ACTIVE_KEY_ID";
const PREVIOUS_KEY_IDS_ENVIRONMENT: &str = "HUBUUM_TOKEN_HASH_PREVIOUS_KEY_IDS";
const REQUIRE_STABLE_KEY_ENVIRONMENT: &str = "HUBUUM_REQUIRE_STABLE_TOKEN_HASH_KEY";
const LEGACY_KEY_ID: &str = "legacy";
const EPHEMERAL_KEY_ID: &str = "ephemeral";
const MINIMUM_KEY_BYTES: usize = 32;

#[derive(Debug, Clone)]
pub enum TokenHashKeyConfigError {
    Invalid(&'static str),
    Secret(SecretError),
}

impl fmt::Display for TokenHashKeyConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message) => formatter.write_str(message),
            Self::Secret(error) => write!(formatter, "token hash key resolution failed: {error}"),
        }
    }
}

impl std::error::Error for TokenHashKeyConfigError {}

impl From<SecretError> for TokenHashKeyConfigError {
    fn from(error: SecretError) -> Self {
        Self::Secret(error)
    }
}

struct TokenHashKey {
    id: StorageTokenHashKeyId,
    material: SecretValue,
}

/// Validated process-wide token-hash rotation configuration.
pub struct TokenHashKeyRing {
    keys: Vec<TokenHashKey>,
    stable: bool,
    identity: String,
    require_stable: bool,
}

impl fmt::Debug for TokenHashKeyRing {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TokenHashKeyRing")
            .field("active_key_id", &self.active_key_id())
            .field("key_count", &self.keys.len())
            .field("stable", &self.stable)
            .field("identity", &self.identity)
            .field("require_stable", &self.require_stable)
            .finish()
    }
}

impl TokenHashKeyRing {
    fn from_environment() -> Result<Self, TokenHashKeyConfigError> {
        let require_stable = parse_bool_environment(REQUIRE_STABLE_KEY_ENVIRONMENT)?;
        let active = environment_value(ACTIVE_KEY_ID_ENVIRONMENT)?;
        let previous = parse_previous_key_ids()?;

        let Some(active) = active else {
            if !previous.is_empty() {
                return Err(TokenHashKeyConfigError::Invalid(
                    "HUBUUM_TOKEN_HASH_ACTIVE_KEY_ID is required when previous token hash keys are configured",
                ));
            }
            return match crate::secrets::resolve_token_hash_key() {
                Ok(material) => Self::try_new(
                    vec![(key_id(LEGACY_KEY_ID)?, material)],
                    true,
                    require_stable,
                ),
                Err(error) if error.kind() == SecretErrorKind::NotFound && !require_stable => {
                    let material = format!("{}{}", Uuid::new_v4(), Uuid::new_v4()).into_bytes();
                    Self::try_new(
                        vec![(key_id(EPHEMERAL_KEY_ID)?, material)],
                        false,
                        require_stable,
                    )
                }
                Err(error) => Err(error.into()),
            };
        };

        let active = key_id(&active)?;
        let mut ids = Vec::with_capacity(previous.len() + 1);
        ids.push(active);
        for value in previous {
            ids.push(key_id(&value)?);
        }
        validate_ids(&ids)?;
        let aliases = ids
            .iter()
            .map(|id| id.as_str().to_string())
            .collect::<Vec<_>>();
        let resolved = crate::secrets::resolve_token_hash_key_group(&aliases)?;
        let text_source = crate::secrets::token_hash_secrets_are_text()?;
        let materials = resolved
            .iter()
            .map(|secret| {
                if text_source {
                    secret
                        .value()
                        .expose_utf8()
                        .map(|value| value.trim().as_bytes().to_vec())
                } else {
                    Ok(secret.value().expose().to_vec())
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        Self::try_new(
            ids.into_iter().zip(materials).collect(),
            true,
            require_stable,
        )
    }

    pub(crate) fn try_new(
        entries: Vec<(StorageTokenHashKeyId, Vec<u8>)>,
        stable: bool,
        require_stable: bool,
    ) -> Result<Self, TokenHashKeyConfigError> {
        if entries.is_empty() || entries.len() > MAX_TOKEN_HASH_KEYS {
            return Err(TokenHashKeyConfigError::Invalid(
                "the token hash key ring must contain one active key and at most seven previous keys",
            ));
        }
        validate_ids(&entries.iter().map(|(id, _)| id.clone()).collect::<Vec<_>>())?;
        let keys = entries
            .into_iter()
            .map(|(id, material)| {
                SecretValue::new(material)
                    .map(|material| TokenHashKey { id, material })
                    .map_err(TokenHashKeyConfigError::from)
            })
            .collect::<Result<Vec<_>, _>>()?;
        if require_stable && !stable {
            return Err(TokenHashKeyConfigError::Invalid(
                "a stable token hash key is required for this deployment",
            ));
        }
        if keys
            .iter()
            .any(|key| key.material.len() < MINIMUM_KEY_BYTES)
        {
            return Err(TokenHashKeyConfigError::Invalid(
                "token hash keys must contain at least 32 bytes",
            ));
        }
        if keys.iter().enumerate().any(|(index, key)| {
            keys[index + 1..]
                .iter()
                .any(|other| key.material.expose() == other.material.expose())
        }) {
            return Err(TokenHashKeyConfigError::Invalid(
                "token hash key material must be unique within the key ring",
            ));
        }
        let identity = ring_identity(&keys);
        Ok(Self {
            keys,
            stable,
            identity,
            require_stable,
        })
    }

    #[must_use]
    pub fn active_key_id(&self) -> &StorageTokenHashKeyId {
        &self.keys[0].id
    }

    pub fn previous_key_ids(&self) -> impl Iterator<Item = &StorageTokenHashKeyId> {
        self.keys[1..].iter().map(|key| &key.id)
    }

    pub fn key_ids(&self) -> impl Iterator<Item = &StorageTokenHashKeyId> {
        self.keys.iter().map(|key| &key.id)
    }

    #[must_use]
    pub fn is_stable(&self) -> bool {
        self.stable
    }

    #[must_use]
    pub fn identity(&self) -> &str {
        &self.identity
    }

    #[must_use]
    pub fn requires_stable_key(&self) -> bool {
        self.require_stable
    }

    #[must_use]
    pub fn active_key_bytes(&self) -> &[u8] {
        self.keys[0].material.expose()
    }

    #[must_use]
    pub fn key_bytes(&self, id: &StorageTokenHashKeyId) -> Option<&[u8]> {
        self.keys
            .iter()
            .find(|key| &key.id == id)
            .map(|key| key.material.expose())
    }

    pub(crate) fn keys(&self) -> impl Iterator<Item = (&StorageTokenHashKeyId, &[u8])> {
        self.keys.iter().map(|key| (&key.id, key.material.expose()))
    }
}

static TOKEN_HASH_KEY_RING: LazyLock<Result<TokenHashKeyRing, TokenHashKeyConfigError>> =
    LazyLock::new(TokenHashKeyRing::from_environment);

pub fn token_hash_key_ring() -> Result<&'static TokenHashKeyRing, TokenHashKeyConfigError> {
    TOKEN_HASH_KEY_RING.as_ref().map_err(Clone::clone)
}

pub fn token_hash_key_bytes() -> &'static [u8] {
    token_hash_key_ring()
        .expect("token hash key-ring configuration must be validated at startup")
        .active_key_bytes()
}

pub fn token_hash_key_is_ephemeral() -> bool {
    token_hash_key_ring()
        .map(|ring| !ring.is_stable())
        .unwrap_or(false)
}

fn key_id(value: &str) -> Result<StorageTokenHashKeyId, TokenHashKeyConfigError> {
    StorageTokenHashKeyId::try_new(value).map_err(|_| {
        TokenHashKeyConfigError::Invalid(
            "token hash key IDs must use 1-32 lowercase ASCII letters, numbers, or interior hyphens",
        )
    })
}

fn validate_ids(ids: &[StorageTokenHashKeyId]) -> Result<(), TokenHashKeyConfigError> {
    if ids.is_empty() || ids.len() > MAX_TOKEN_HASH_KEYS {
        return Err(TokenHashKeyConfigError::Invalid(
            "the token hash key ring must contain one active key and at most seven previous keys",
        ));
    }
    if ids
        .iter()
        .enumerate()
        .any(|(index, id)| ids[index + 1..].contains(id))
    {
        return Err(TokenHashKeyConfigError::Invalid(
            "token hash key IDs must be unique within the key ring",
        ));
    }
    Ok(())
}

fn environment_value(name: &str) -> Result<Option<String>, TokenHashKeyConfigError> {
    match std::env::var(name) {
        Ok(value) if value.trim().is_empty() => Err(TokenHashKeyConfigError::Invalid(
            "configured token hash key metadata must not be empty",
        )),
        Ok(value) => Ok(Some(value.trim().to_string())),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(TokenHashKeyConfigError::Invalid(
            "token hash key metadata must contain valid Unicode",
        )),
    }
}

fn parse_previous_key_ids() -> Result<Vec<String>, TokenHashKeyConfigError> {
    let Some(value) = environment_value(PREVIOUS_KEY_IDS_ENVIRONMENT)? else {
        return Ok(Vec::new());
    };
    let ids = value
        .split(',')
        .map(str::trim)
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    if ids.iter().any(String::is_empty) {
        return Err(TokenHashKeyConfigError::Invalid(
            "previous token hash key IDs must not contain empty entries",
        ));
    }
    Ok(ids)
}

fn parse_bool_environment(name: &str) -> Result<bool, TokenHashKeyConfigError> {
    match environment_value(name)?.as_deref() {
        None | Some("false" | "0") => Ok(false),
        Some("true" | "1") => Ok(true),
        Some(_) => Err(TokenHashKeyConfigError::Invalid(
            "HUBUUM_REQUIRE_STABLE_TOKEN_HASH_KEY must be true, false, 1, or 0",
        )),
    }
}

fn ring_identity(keys: &[TokenHashKey]) -> String {
    let mut hasher = Sha256::new();
    for key in keys {
        hasher.update(key.id.as_str().as_bytes());
        hasher.update([0]);
        hasher.update(key.material.expose());
        hasher.update([0xff]);
    }
    let digest = hasher.finalize();
    digest[..12]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(value: &str) -> StorageTokenHashKeyId {
        StorageTokenHashKeyId::try_new(value).unwrap()
    }

    #[test]
    fn key_ring_rejects_duplicate_material() {
        let result = TokenHashKeyRing::try_new(
            vec![(id("active"), vec![1; 32]), (id("previous"), vec![1; 32])],
            true,
            false,
        );

        assert!(matches!(result, Err(TokenHashKeyConfigError::Invalid(_))));
    }

    #[test]
    fn key_ring_rejects_duplicate_ids() {
        let result = TokenHashKeyRing::try_new(
            vec![(id("active"), vec![1; 32]), (id("active"), vec![2; 32])],
            true,
            false,
        );

        assert!(matches!(result, Err(TokenHashKeyConfigError::Invalid(_))));
    }

    #[test]
    fn key_ring_rejects_short_material() {
        let result = TokenHashKeyRing::try_new(vec![(id("active"), vec![1; 31])], true, false);

        assert!(matches!(result, Err(TokenHashKeyConfigError::Invalid(_))));
    }

    #[test]
    fn key_ring_rejects_more_than_the_bounded_key_count() {
        let entries = (0_u8..=8)
            .map(|index| (id(&format!("key-{index}")), vec![index; 32]))
            .collect();

        assert!(matches!(
            TokenHashKeyRing::try_new(entries, true, false),
            Err(TokenHashKeyConfigError::Invalid(_))
        ));
    }

    #[test]
    fn key_ring_identity_changes_with_material_without_exposing_it() {
        let first =
            TokenHashKeyRing::try_new(vec![(id("active"), vec![1; 32])], true, false).unwrap();
        let second =
            TokenHashKeyRing::try_new(vec![(id("active"), vec![2; 32])], true, false).unwrap();

        assert_ne!(first.identity(), second.identity());
        assert!(!format!("{first:?}").contains(&"01".repeat(32)));
    }

    #[test]
    fn strict_mode_rejects_ephemeral_material() {
        let result = TokenHashKeyRing::try_new(vec![(id("ephemeral"), vec![1; 32])], false, true);

        assert!(matches!(result, Err(TokenHashKeyConfigError::Invalid(_))));
    }
}
