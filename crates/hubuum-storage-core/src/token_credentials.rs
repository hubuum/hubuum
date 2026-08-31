use std::fmt;

use crate::StorageValidationError;

/// Maximum number of token-hash keys accepted in one rotation ring.
pub const MAX_TOKEN_HASH_KEYS: usize = 8;

/// Stable, non-secret identifier for one token-hashing key.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StorageTokenHashKeyId(String);

impl StorageTokenHashKeyId {
    pub fn try_new(value: impl Into<String>) -> Result<Self, StorageValidationError> {
        let value = value.into();
        let bytes = value.as_bytes();
        if bytes.is_empty() || bytes.len() > 32 {
            return Err(StorageValidationError::invalid(
                "token hash key IDs must contain 1-32 characters",
            ));
        }
        let valid_edge = |byte: u8| byte.is_ascii_lowercase() || byte.is_ascii_digit();
        if !valid_edge(bytes[0])
            || !valid_edge(bytes[bytes.len() - 1])
            || !bytes.iter().all(|byte| valid_edge(*byte) || *byte == b'-')
        {
            return Err(StorageValidationError::invalid(
                "token hash key IDs must use lowercase ASCII letters, numbers, or interior hyphens",
            ));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for StorageTokenHashKeyId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("StorageTokenHashKeyId")
            .field(&self.0)
            .finish()
    }
}

impl fmt::Display for StorageTokenHashKeyId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Bearer-token syntax family represented by a persisted digest.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageTokenFormat {
    Legacy,
    Version1,
}

impl StorageTokenFormat {
    #[must_use]
    pub const fn persistence_value(self) -> i16 {
        match self {
            Self::Legacy => 0,
            Self::Version1 => 1,
        }
    }

    pub fn from_persistence(value: i16) -> Result<Self, StorageValidationError> {
        match value {
            0 => Ok(Self::Legacy),
            1 => Ok(Self::Version1),
            _ => Err(StorageValidationError::invalid(
                "unsupported persisted token format",
            )),
        }
    }
}

/// One-way algorithm used for a persisted bearer-token digest.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageTokenHashAlgorithm {
    HmacSha256V1,
}

impl StorageTokenHashAlgorithm {
    #[must_use]
    pub const fn persistence_value(self) -> i16 {
        1
    }

    pub fn from_persistence(value: i16) -> Result<Self, StorageValidationError> {
        match value {
            1 => Ok(Self::HmacSha256V1),
            _ => Err(StorageValidationError::invalid(
                "unsupported persisted token hash algorithm",
            )),
        }
    }
}

/// Redacted digest and the non-secret metadata needed to interpret it.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageTokenDigest {
    lookup_value: String,
    format: StorageTokenFormat,
    algorithm: StorageTokenHashAlgorithm,
    key_id: Option<StorageTokenHashKeyId>,
}

impl StorageTokenDigest {
    pub fn try_new(
        lookup_value: impl Into<String>,
        format: StorageTokenFormat,
        algorithm: StorageTokenHashAlgorithm,
        key_id: Option<StorageTokenHashKeyId>,
    ) -> Result<Self, StorageValidationError> {
        let lookup_value = lookup_value.into();
        if lookup_value.is_empty() {
            return Err(StorageValidationError::invalid(
                "token digest must not be empty",
            ));
        }
        if format == StorageTokenFormat::Version1 && key_id.is_none() {
            return Err(StorageValidationError::invalid(
                "versioned token digests require a hash key ID",
            ));
        }
        Ok(Self {
            lookup_value,
            format,
            algorithm,
            key_id,
        })
    }

    /// Compatibility representation for a row created before key metadata existed.
    #[must_use]
    pub fn legacy_unidentified(lookup_value: impl Into<String>) -> Self {
        Self {
            lookup_value: lookup_value.into(),
            format: StorageTokenFormat::Legacy,
            algorithm: StorageTokenHashAlgorithm::HmacSha256V1,
            key_id: None,
        }
    }

    #[must_use]
    pub fn lookup_value(&self) -> &str {
        &self.lookup_value
    }

    /// Compares a persisted lookup value without data-dependent byte exits.
    #[must_use]
    pub fn matches_lookup_value(&self, persisted: &str) -> bool {
        let mut difference = self.lookup_value.len() ^ persisted.len();
        for (expected, actual) in self.lookup_value.bytes().zip(persisted.bytes()) {
            difference |= usize::from(expected ^ actual);
        }
        difference == 0
    }

    #[must_use]
    pub const fn format(&self) -> StorageTokenFormat {
        self.format
    }

    #[must_use]
    pub const fn algorithm(&self) -> StorageTokenHashAlgorithm {
        self.algorithm
    }

    #[must_use]
    pub const fn key_id(&self) -> Option<&StorageTokenHashKeyId> {
        self.key_id.as_ref()
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        String,
        StorageTokenFormat,
        StorageTokenHashAlgorithm,
        Option<StorageTokenHashKeyId>,
    ) {
        (self.lookup_value, self.format, self.algorithm, self.key_id)
    }
}

impl fmt::Debug for StorageTokenDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageTokenDigest")
            .field("lookup_value", &"<redacted>")
            .field("format", &self.format)
            .field("algorithm", &self.algorithm)
            .field("key_id", &self.key_id)
            .finish()
    }
}

/// One possible persisted representation of a presented bearer value.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageAuthenticationCredential {
    digest: StorageTokenDigest,
}

impl StorageAuthenticationCredential {
    /// Compatibility constructor for one unidentified legacy digest.
    #[must_use]
    pub fn new(lookup_value: impl Into<String>) -> Self {
        Self {
            digest: StorageTokenDigest::legacy_unidentified(lookup_value),
        }
    }

    #[must_use]
    pub const fn from_digest(digest: StorageTokenDigest) -> Self {
        Self { digest }
    }

    #[must_use]
    pub fn lookup_value(&self) -> &str {
        self.digest.lookup_value()
    }

    #[must_use]
    pub const fn digest(&self) -> &StorageTokenDigest {
        &self.digest
    }
}

impl fmt::Debug for StorageAuthenticationCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageAuthenticationCredential")
            .field("digest", &self.digest)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_ids_reject_ambiguous_environment_aliases() {
        for invalid in ["", "UPPER", "under_score", "-edge", "edge-", "a..b"] {
            assert!(
                StorageTokenHashKeyId::try_new(invalid).is_err(),
                "{invalid}"
            );
        }
    }

    #[test]
    fn digest_debug_output_redacts_lookup_material() {
        let digest = StorageTokenDigest::legacy_unidentified("sensitive-token-digest");
        let debug = format!("{digest:?}");

        assert!(!debug.contains("sensitive-token-digest"));
    }

    #[test]
    fn digest_lookup_comparison_checks_content_and_length() {
        let digest = StorageTokenDigest::legacy_unidentified("abcdef");

        assert!(digest.matches_lookup_value("abcdef"));
        assert!(!digest.matches_lookup_value("abcdeg"));
        assert!(!digest.matches_lookup_value("abcdef0"));
        assert!(!digest.matches_lookup_value("abcde"));
    }
}
