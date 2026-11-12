use hubuum_domain::IdentityScopeName;
use uuid::Uuid;

use super::{
    StorageAuthenticationProvider, StorageAuthenticationProviderKind,
    StorageBrowserProviderDefinition,
};
use crate::StorageValidationError;

/// Installation-local provider namespace retained even after a definition is removed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageProviderIdentityBinding {
    name: IdentityScopeName,
    id: Uuid,
    kind: StorageAuthenticationProviderKind,
    namespace: String,
}

impl StorageProviderIdentityBinding {
    pub fn from_provider(
        provider: &StorageAuthenticationProvider,
    ) -> Result<Self, StorageValidationError> {
        let id = provider
            .id()
            .ok_or_else(|| StorageValidationError::invalid("Provider ID required"))?;
        let namespace = match provider.browser_definition() {
            Some(StorageBrowserProviderDefinition::Oidc(definition)) => {
                definition.issuer().as_str().to_owned()
            }
            Some(StorageBrowserProviderDefinition::Oauth(definition)) => {
                definition.identity_namespace()
            }
            None if provider.kind() == StorageAuthenticationProviderKind::Ldap => {
                provider.name().to_owned()
            }
            None => {
                return Err(StorageValidationError::invalid(
                    "Validated browser definition required",
                ));
            }
        };
        Self::try_new(provider.name().to_owned(), id, provider.kind(), namespace)
    }

    pub fn try_new(
        name: String,
        id: Uuid,
        kind: StorageAuthenticationProviderKind,
        namespace: String,
    ) -> Result<Self, StorageValidationError> {
        let name = IdentityScopeName::new(name)
            .map_err(|error| StorageValidationError::invalid(error.to_string()))?;
        if name.as_str() == "local" || id.is_nil() || namespace.is_empty() || namespace.len() > 4096
        {
            return Err(StorageValidationError::invalid(
                "Invalid provider identity binding",
            ));
        }
        Ok(Self {
            name,
            id,
            kind,
            namespace,
        })
    }
    pub fn name(&self) -> &str {
        self.name.as_str()
    }
    pub const fn id(&self) -> Uuid {
        self.id
    }
    pub const fn kind(&self) -> StorageAuthenticationProviderKind {
        self.kind
    }
    pub fn namespace(&self) -> &str {
        &self.namespace
    }
}
