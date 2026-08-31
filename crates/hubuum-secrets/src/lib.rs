//! Typed, non-printing secret values and bounded environment/file providers.

pub const DEFAULT_MAX_SECRET_BYTES: usize = 1024 * 1024;

mod error;
mod file;
mod provider;
mod reference;
mod resolver;
mod value;

pub use error::{SecretError, SecretErrorKind};
pub use file::{FileProvider, FileProviderBuilder, FileSymlinkPolicy};
pub use provider::{
    EnvironmentProvider, ProviderHealth, ProviderHealthState, ProviderSecret, SecretProvider,
};
pub use reference::{SecretName, SecretProviderKind, SecretRef, SecretVersionSelector};
pub use resolver::{
    CachePolicy, ResolvedSecret, ResolvedSecretGroup, SecretResolver, SecretResolverBuilder,
    SecretResolverDiagnostics, StalePolicy,
};
pub use value::{SecretValue, SecretVersion};
