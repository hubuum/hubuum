use std::collections::{BTreeMap, hash_map::RandomState};
use std::fmt;
use std::hash::BuildHasher;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;

use crate::{
    DEFAULT_MAX_SECRET_BYTES, SecretError, SecretErrorKind, SecretName, SecretProviderKind,
    SecretRef, SecretValue, SecretVersion, SecretVersionSelector,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderHealthState {
    Unknown,
    Healthy,
    Degraded,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderHealth {
    pub(crate) kind: SecretProviderKind,
    pub(crate) state: ProviderHealthState,
    pub(crate) last_success: Option<SystemTime>,
    pub(crate) last_error_kind: Option<SecretErrorKind>,
}

impl ProviderHealth {
    pub fn kind(&self) -> &SecretProviderKind {
        &self.kind
    }

    pub fn state(&self) -> ProviderHealthState {
        self.state
    }

    pub fn last_success(&self) -> Option<SystemTime> {
        self.last_success
    }

    pub fn last_error_kind(&self) -> Option<SecretErrorKind> {
        self.last_error_kind
    }
}

pub struct ProviderSecret {
    pub(crate) value: Arc<SecretValue>,
    pub(crate) version: SecretVersion,
}

impl fmt::Debug for ProviderSecret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderSecret")
            .field("value", &"<redacted>")
            .field("version", &self.version)
            .finish()
    }
}

impl ProviderSecret {
    pub fn new(value: SecretValue, version: SecretVersion) -> Self {
        Self {
            value: Arc::new(value),
            version,
        }
    }

    pub fn value(&self) -> &Arc<SecretValue> {
        &self.value
    }

    pub fn version(&self) -> &SecretVersion {
        &self.version
    }
}

#[async_trait]
pub trait SecretProvider: Send + Sync + fmt::Debug {
    fn kind(&self) -> &SecretProviderKind;

    async fn resolve(&self, reference: &SecretRef) -> Result<ProviderSecret, SecretError>;

    async fn resolve_group(
        &self,
        references: &[SecretRef],
    ) -> Result<Vec<ProviderSecret>, SecretError>;
}

pub struct EnvironmentProvider {
    kind: SecretProviderKind,
    prefix: String,
    mappings: BTreeMap<SecretName, String>,
    max_bytes: usize,
    version_hasher: RandomState,
}

impl EnvironmentProvider {
    pub fn new(prefix: impl Into<String>) -> Result<Self, SecretError> {
        Self::with_kind(SecretProviderKind::environment(), prefix)
    }

    pub fn with_kind(
        kind: SecretProviderKind,
        prefix: impl Into<String>,
    ) -> Result<Self, SecretError> {
        let prefix = prefix.into();
        if prefix.len() > 128
            || !prefix
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return Err(SecretError::new(
                SecretErrorKind::InvalidProviderConfiguration,
                "environment secret prefixes must contain at most 128 uppercase ASCII letters, numbers, or underscores",
            ));
        }
        Ok(Self {
            kind,
            prefix,
            mappings: BTreeMap::new(),
            max_bytes: DEFAULT_MAX_SECRET_BYTES,
            version_hasher: RandomState::new(),
        })
    }

    pub fn max_bytes(mut self, max_bytes: usize) -> Result<Self, SecretError> {
        if max_bytes == 0 {
            return Err(SecretError::new(
                SecretErrorKind::InvalidProviderConfiguration,
                "environment secret size limit must be positive",
            ));
        }
        self.max_bytes = max_bytes;
        Ok(self)
    }

    pub fn mapping(
        mut self,
        name: SecretName,
        environment_name: impl Into<String>,
    ) -> Result<Self, SecretError> {
        let environment_name = environment_name.into();
        if environment_name.is_empty()
            || environment_name.len() > 256
            || !environment_name
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return Err(SecretError::new(
                SecretErrorKind::InvalidProviderConfiguration,
                "mapped environment names must contain 1-256 uppercase ASCII letters, numbers, or underscores",
            ));
        }
        self.mappings.insert(name, environment_name);
        Ok(self)
    }

    fn key(&self, name: &SecretName) -> String {
        if let Some(mapped) = self.mappings.get(name) {
            return mapped.clone();
        }
        format!("{}{}", self.prefix, name.as_str().to_ascii_uppercase()).replace('-', "_")
    }

    fn read(&self, reference: &SecretRef) -> Result<ProviderSecret, SecretError> {
        ensure_provider(reference, &self.kind)?;
        let key = self.key(reference.name());
        let value = std::env::var_os(&key).ok_or_else(|| {
            SecretError::new(
                SecretErrorKind::NotFound,
                "environment secret is not configured",
            )
        })?;
        #[cfg(unix)]
        let bytes = {
            use std::os::unix::ffi::OsStringExt;
            value.into_vec()
        };
        #[cfg(not(unix))]
        let bytes = value.into_string().map(String::into_bytes).map_err(|_| {
            SecretError::new(
                SecretErrorKind::InvalidValue,
                "environment secret is not valid Unicode on this platform",
            )
        })?;
        if bytes.len() > self.max_bytes {
            return Err(SecretError::new(
                SecretErrorKind::TooLarge,
                "environment secret exceeds the configured size limit",
            ));
        }
        let version = opaque_version(&self.version_hasher, &bytes)?;
        ensure_version(reference.version(), &version)?;
        Ok(ProviderSecret {
            value: Arc::new(SecretValue::new(bytes)?),
            version,
        })
    }
}

impl fmt::Debug for EnvironmentProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EnvironmentProvider")
            .field("kind", &self.kind)
            .field("prefix", &"<redacted>")
            .field("mapped_secret_count", &self.mappings.len())
            .field("max_bytes", &self.max_bytes)
            .finish()
    }
}

#[async_trait]
impl SecretProvider for EnvironmentProvider {
    fn kind(&self) -> &SecretProviderKind {
        &self.kind
    }

    async fn resolve(&self, reference: &SecretRef) -> Result<ProviderSecret, SecretError> {
        self.read(reference)
    }

    async fn resolve_group(
        &self,
        references: &[SecretRef],
    ) -> Result<Vec<ProviderSecret>, SecretError> {
        for attempt in 0..3 {
            let first = references
                .iter()
                .map(|reference| self.read(reference))
                .collect::<Result<Vec<_>, _>>()?;
            let versions = references
                .iter()
                .map(|reference| self.read(reference).map(|value| value.version))
                .collect::<Result<Vec<_>, _>>()?;
            if first.iter().map(|value| &value.version).eq(versions.iter()) {
                return Ok(first);
            }
            if attempt == 2 {
                return Err(SecretError::new(
                    SecretErrorKind::ChangedDuringRead,
                    "environment secret group changed while it was being resolved",
                ));
            }
        }
        unreachable!("the bounded environment group-read loop always returns")
    }
}

pub(crate) fn ensure_provider(
    reference: &SecretRef,
    expected: &SecretProviderKind,
) -> Result<(), SecretError> {
    if reference.provider() != expected {
        return Err(SecretError::new(
            SecretErrorKind::InvalidReference,
            "secret reference was sent to the wrong provider",
        ));
    }
    Ok(())
}

pub(crate) fn opaque_version(
    state: &RandomState,
    value: &[u8],
) -> Result<SecretVersion, SecretError> {
    SecretVersion::new(format!("v{:016x}", state.hash_one(value)))
}

pub(crate) fn ensure_version(
    selector: &SecretVersionSelector,
    actual: &SecretVersion,
) -> Result<(), SecretError> {
    if let SecretVersionSelector::Exact(expected) = selector
        && expected != actual
    {
        return Err(SecretError::new(
            SecretErrorKind::UnsupportedVersion,
            "the requested secret version is not available",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    static NEXT_ENVIRONMENT_ID: AtomicU64 = AtomicU64::new(1);

    struct TestEnvironment {
        key: String,
        name: SecretName,
    }

    impl TestEnvironment {
        fn new() -> Self {
            Self {
                key: format!(
                    "SECRET_PROVIDER_TEST_{}_{}",
                    std::process::id(),
                    NEXT_ENVIRONMENT_ID.fetch_add(1, Ordering::Relaxed)
                ),
                name: SecretName::new("value").unwrap(),
            }
        }

        fn provider(&self) -> EnvironmentProvider {
            EnvironmentProvider::new("")
                .unwrap()
                .mapping(self.name.clone(), &self.key)
                .unwrap()
        }

        fn reference(&self) -> SecretRef {
            SecretRef::new(SecretProviderKind::environment(), self.name.clone())
        }

        fn set(&self, value: &OsStr) {
            unsafe { std::env::set_var(&self.key, value) };
        }
    }

    impl Drop for TestEnvironment {
        fn drop(&mut self) {
            unsafe { std::env::remove_var(&self.key) };
        }
    }

    #[tokio::test]
    async fn environment_provider_distinguishes_missing_from_empty_values() {
        let environment = TestEnvironment::new();
        let provider = environment.provider();

        assert_eq!(
            provider
                .resolve(&environment.reference())
                .await
                .unwrap_err()
                .kind(),
            SecretErrorKind::NotFound
        );
        environment.set(OsStr::new(""));
        assert_eq!(
            provider
                .resolve(&environment.reference())
                .await
                .unwrap_err()
                .kind(),
            SecretErrorKind::InvalidValue
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn environment_provider_preserves_binary_values() {
        use std::os::unix::ffi::OsStringExt;

        let environment = TestEnvironment::new();
        let provider = environment.provider();
        let value = std::ffi::OsString::from_vec(vec![0xff, 0x01]);
        environment.set(&value);

        assert_eq!(
            provider
                .resolve(&environment.reference())
                .await
                .unwrap()
                .value()
                .expose(),
            [0xff, 0x01]
        );
    }

    #[tokio::test]
    async fn environment_provider_enforces_its_configured_size_limit() {
        let environment = TestEnvironment::new();
        let provider = environment.provider().max_bytes(4).unwrap();
        environment.set(OsStr::new("1234"));
        assert_eq!(
            provider
                .resolve(&environment.reference())
                .await
                .unwrap()
                .value()
                .len(),
            4
        );
        environment.set(OsStr::new("12345"));

        assert_eq!(
            provider
                .resolve(&environment.reference())
                .await
                .unwrap_err()
                .kind(),
            SecretErrorKind::TooLarge
        );
    }
}
