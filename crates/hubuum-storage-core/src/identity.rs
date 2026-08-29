use async_trait::async_trait;
use chrono::{DateTime, Utc};
use hubuum_domain::{
    ClassId, CollectionId, IdentityScopeId, ObjectId, PrincipalId, PrincipalKind, ResourceRevision,
    TokenId, UserId,
};

use crate::{StorageAuthorizationPermission, StorageError, StorageValidationError};

/// Opaque, redacted lookup material for one presented bearer credential.
///
/// The application authentication service derives this value from the raw
/// bearer token. Adapters may compare it with their persisted credential
/// representation, but neither the raw token nor a backend row crosses the
/// storage boundary.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageAuthenticationCredential {
    lookup_value: String,
}

impl std::fmt::Debug for StorageAuthenticationCredential {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StorageAuthenticationCredential")
            .field("lookup_value", &"<redacted>")
            .finish()
    }
}

impl StorageAuthenticationCredential {
    #[must_use]
    pub fn new(lookup_value: impl Into<String>) -> Self {
        Self {
            lookup_value: lookup_value.into(),
        }
    }

    #[must_use]
    pub fn lookup_value(&self) -> &str {
        &self.lookup_value
    }
}

/// Deterministic inputs for one bearer-credential validation.
///
/// The application owns time and token-lifetime configuration. Passing the
/// resulting window explicitly keeps storage adapters independent of global
/// configuration and makes compatibility tests deterministic.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageAuthenticationAttempt {
    credential: StorageAuthenticationCredential,
    observed_at: DateTime<Utc>,
    legacy_valid_after: DateTime<Utc>,
}

impl std::fmt::Debug for StorageAuthenticationAttempt {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StorageAuthenticationAttempt")
            .field("credential", &self.credential)
            .field("observed_at", &"<redacted>")
            .field("legacy_valid_after", &"<redacted>")
            .finish()
    }
}

impl StorageAuthenticationAttempt {
    pub fn try_new(
        credential: StorageAuthenticationCredential,
        observed_at: DateTime<Utc>,
        legacy_valid_after: DateTime<Utc>,
    ) -> Result<Self, StorageValidationError> {
        if legacy_valid_after > observed_at {
            return Err(StorageValidationError::invalid(
                "legacy token validity cutoff cannot be after the observation time",
            ));
        }
        Ok(Self {
            credential,
            observed_at,
            legacy_valid_after,
        })
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        StorageAuthenticationCredential,
        DateTime<Utc>,
        DateTime<Utc>,
    ) {
        (self.credential, self.observed_at, self.legacy_valid_after)
    }
}

/// Hash-free successful bearer-token authentication result.
///
/// The persisted credential representation and backend-only lifecycle columns
/// remain private to the adapter. Request handling receives the identity,
/// public descriptive metadata, revision, and scope flags needed by the
/// authenticated-principal and current-token APIs.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageAuthenticatedToken {
    id: TokenId,
    principal_id: PrincipalId,
    name: Option<String>,
    description: Option<String>,
    issued: DateTime<Utc>,
    expires_at: Option<DateTime<Utc>>,
    last_used_at: Option<DateTime<Utc>>,
    permission_scoped: bool,
    resource_scoped: bool,
    revision: ResourceRevision,
}

impl std::fmt::Debug for StorageAuthenticatedToken {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StorageAuthenticatedToken")
            .field("id", &"<redacted>")
            .field("principal_id", &"<redacted>")
            .field("name", &self.name.as_ref().map(|_| "<redacted>"))
            .field(
                "description",
                &self.description.as_ref().map(|_| "<redacted>"),
            )
            .field("issued", &"<redacted>")
            .field(
                "expires_at",
                &self.expires_at.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "last_used_at",
                &self.last_used_at.as_ref().map(|_| "<redacted>"),
            )
            .field("permission_scoped", &self.permission_scoped)
            .field("resource_scoped", &self.resource_scoped)
            .field("revision", &"<redacted>")
            .finish()
    }
}

impl StorageAuthenticatedToken {
    #[must_use]
    pub const fn builder(
        id: TokenId,
        principal_id: PrincipalId,
        issued: DateTime<Utc>,
        revision: ResourceRevision,
    ) -> StorageAuthenticatedTokenBuilder {
        StorageAuthenticatedTokenBuilder {
            id,
            principal_id,
            name: None,
            description: None,
            issued,
            expires_at: None,
            last_used_at: None,
            permission_scoped: false,
            resource_scoped: false,
            revision,
        }
    }

    #[must_use]
    pub const fn id(&self) -> TokenId {
        self.id
    }

    #[must_use]
    pub const fn principal_id(&self) -> PrincipalId {
        self.principal_id
    }

    #[must_use]
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    #[must_use]
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    #[must_use]
    pub const fn issued(&self) -> DateTime<Utc> {
        self.issued
    }

    #[must_use]
    pub const fn expires_at(&self) -> Option<DateTime<Utc>> {
        self.expires_at
    }

    #[must_use]
    pub const fn last_used_at(&self) -> Option<DateTime<Utc>> {
        self.last_used_at
    }

    #[must_use]
    pub const fn is_permission_scoped(&self) -> bool {
        self.permission_scoped
    }

    #[must_use]
    pub const fn is_resource_scoped(&self) -> bool {
        self.resource_scoped
    }

    #[must_use]
    pub const fn is_scoped(&self) -> bool {
        self.permission_scoped || self.resource_scoped
    }

    #[must_use]
    pub const fn revision(&self) -> ResourceRevision {
        self.revision
    }
}

/// Builder for the hash-free token projection returned after successful
/// authentication.
pub struct StorageAuthenticatedTokenBuilder {
    id: TokenId,
    principal_id: PrincipalId,
    name: Option<String>,
    description: Option<String>,
    issued: DateTime<Utc>,
    expires_at: Option<DateTime<Utc>>,
    last_used_at: Option<DateTime<Utc>>,
    permission_scoped: bool,
    resource_scoped: bool,
    revision: ResourceRevision,
}

impl StorageAuthenticatedTokenBuilder {
    #[must_use]
    pub fn name(mut self, name: Option<String>) -> Self {
        self.name = name;
        self
    }

    #[must_use]
    pub fn description(mut self, description: Option<String>) -> Self {
        self.description = description;
        self
    }

    #[must_use]
    pub const fn expires_at(mut self, expires_at: Option<DateTime<Utc>>) -> Self {
        self.expires_at = expires_at;
        self
    }

    #[must_use]
    pub const fn last_used_at(mut self, last_used_at: Option<DateTime<Utc>>) -> Self {
        self.last_used_at = last_used_at;
        self
    }

    #[must_use]
    pub const fn permission_scoped(mut self, permission_scoped: bool) -> Self {
        self.permission_scoped = permission_scoped;
        self
    }

    #[must_use]
    pub const fn resource_scoped(mut self, resource_scoped: bool) -> Self {
        self.resource_scoped = resource_scoped;
        self
    }

    pub fn try_build(self) -> Result<StorageAuthenticatedToken, StorageValidationError> {
        if self.expires_at.is_some_and(|value| value < self.issued) {
            return Err(StorageValidationError::invalid(
                "authenticated token expiry must not precede issuance",
            ));
        }
        if self.last_used_at.is_some_and(|value| value < self.issued) {
            return Err(StorageValidationError::invalid(
                "authenticated token last-use timestamp must not precede issuance",
            ));
        }
        Ok(StorageAuthenticatedToken {
            id: self.id,
            principal_id: self.principal_id,
            name: self.name,
            description: self.description,
            issued: self.issued,
            expires_at: self.expires_at,
            last_used_at: self.last_used_at,
            permission_scoped: self.permission_scoped,
            resource_scoped: self.resource_scoped,
            revision: self.revision,
        })
    }
}

/// Minimal principal data required after bearer-token validation.
///
/// Persistence metadata, settings documents, revisions, and provider sync
/// state deliberately stay behind the storage boundary.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageAuthenticationPrincipal {
    id: PrincipalId,
    kind: PrincipalKind,
    name: String,
    identity_scope_id: IdentityScopeId,
}

impl std::fmt::Debug for StorageAuthenticationPrincipal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StorageAuthenticationPrincipal")
            .field("id", &"<redacted>")
            .field("kind", &self.kind)
            .field("name", &"<redacted>")
            .field("identity_scope_id", &"<redacted>")
            .finish()
    }
}

impl StorageAuthenticationPrincipal {
    #[must_use]
    pub fn new(
        id: PrincipalId,
        kind: PrincipalKind,
        name: impl Into<String>,
        identity_scope_id: IdentityScopeId,
    ) -> Self {
        Self {
            id,
            kind,
            name: name.into(),
            identity_scope_id,
        }
    }

    #[must_use]
    pub const fn id(&self) -> PrincipalId {
        self.id
    }

    #[must_use]
    pub const fn kind(&self) -> PrincipalKind {
        self.kind
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub const fn identity_scope_id(&self) -> IdentityScopeId {
        self.identity_scope_id
    }

    #[must_use]
    pub const fn is_human(&self) -> bool {
        self.kind.is_human()
    }

    #[must_use]
    pub const fn is_service_account(&self) -> bool {
        self.kind.is_service_account()
    }
}

/// Password-free human projection used by human-only request extractors.
///
/// Credential hashes are not selected by an adapter implementing this
/// contract and therefore cannot accidentally cross into request handling or
/// diagnostics.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageAuthenticationHuman {
    id: UserId,
    proper_name: Option<String>,
    email: Option<String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    anonymized_at: Option<DateTime<Utc>>,
}

impl std::fmt::Debug for StorageAuthenticationHuman {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StorageAuthenticationHuman")
            .field("id", &"<redacted>")
            .field(
                "proper_name",
                &self.proper_name.as_ref().map(|_| "<redacted>"),
            )
            .field("email", &self.email.as_ref().map(|_| "<redacted>"))
            .field("created_at", &"<redacted>")
            .field("updated_at", &"<redacted>")
            .field(
                "anonymized_at",
                &self.anonymized_at.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

impl StorageAuthenticationHuman {
    pub fn try_new(
        id: UserId,
        proper_name: Option<String>,
        email: Option<String>,
        created_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
        anonymized_at: Option<DateTime<Utc>>,
    ) -> Result<Self, StorageValidationError> {
        if updated_at < created_at {
            return Err(StorageValidationError::invalid(
                "authentication human updated_at must not precede created_at",
            ));
        }
        if anonymized_at.is_some_and(|value| value < created_at || value > updated_at) {
            return Err(StorageValidationError::invalid(
                "authentication human anonymized_at must be within its creation and update timestamps",
            ));
        }
        Ok(Self {
            id,
            proper_name,
            email,
            created_at,
            updated_at,
            anonymized_at,
        })
    }

    #[must_use]
    pub const fn id(&self) -> UserId {
        self.id
    }

    #[must_use]
    pub fn proper_name(&self) -> Option<&str> {
        self.proper_name.as_deref()
    }

    #[must_use]
    pub fn email(&self) -> Option<&str> {
        self.email.as_deref()
    }

    #[must_use]
    pub const fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    #[must_use]
    pub const fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }

    #[must_use]
    pub const fn anonymized_at(&self) -> Option<DateTime<Utc>> {
        self.anonymized_at
    }
}

/// One consistent authentication read of a principal and its optional human
/// subtype.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageAuthenticationIdentity {
    principal: StorageAuthenticationPrincipal,
    human: Option<StorageAuthenticationHuman>,
}

impl StorageAuthenticationIdentity {
    pub fn try_new(
        principal: StorageAuthenticationPrincipal,
        human: Option<StorageAuthenticationHuman>,
    ) -> Result<Self, StorageValidationError> {
        if principal.is_human() != human.is_some()
            || human
                .as_ref()
                .is_some_and(|human| human.id().id() != principal.id().id())
        {
            return Err(StorageValidationError::invalid(
                "authentication principal kind, id, and human projection are inconsistent",
            ));
        }
        Ok(Self { principal, human })
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        StorageAuthenticationPrincipal,
        Option<StorageAuthenticationHuman>,
    ) {
        (self.principal, self.human)
    }
}

/// Complete information needed to load the narrowing dimensions of one token.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct StorageAuthenticationTokenScopeQuery {
    token_id: TokenId,
    permission_scoped: bool,
    resource_scoped: bool,
}

impl std::fmt::Debug for StorageAuthenticationTokenScopeQuery {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StorageAuthenticationTokenScopeQuery")
            .field("token_id", &"<redacted>")
            .field("permission_scoped", &self.permission_scoped)
            .field("resource_scoped", &self.resource_scoped)
            .finish()
    }
}

impl StorageAuthenticationTokenScopeQuery {
    #[must_use]
    pub const fn new(token_id: TokenId, permission_scoped: bool, resource_scoped: bool) -> Self {
        Self {
            token_id,
            permission_scoped,
            resource_scoped,
        }
    }

    #[must_use]
    pub const fn token_id(self) -> TokenId {
        self.token_id
    }

    #[must_use]
    pub const fn is_permission_scoped(self) -> bool {
        self.permission_scoped
    }

    #[must_use]
    pub const fn is_resource_scoped(self) -> bool {
        self.resource_scoped
    }

    #[must_use]
    pub const fn is_scoped(self) -> bool {
        self.permission_scoped || self.resource_scoped
    }
}

/// Resource ids in one token's enabled resource-scope dimension.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct StorageAuthenticationResourceScope {
    collection_ids: Vec<CollectionId>,
    class_ids: Vec<ClassId>,
    object_ids: Vec<ObjectId>,
}

impl std::fmt::Debug for StorageAuthenticationResourceScope {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StorageAuthenticationResourceScope")
            .field("collection_count", &self.collection_ids.len())
            .field("class_count", &self.class_ids.len())
            .field("object_count", &self.object_ids.len())
            .finish()
    }
}

impl StorageAuthenticationResourceScope {
    #[must_use]
    pub fn new(
        mut collection_ids: Vec<CollectionId>,
        mut class_ids: Vec<ClassId>,
        mut object_ids: Vec<ObjectId>,
    ) -> Self {
        collection_ids.sort_unstable();
        collection_ids.dedup();
        class_ids.sort_unstable();
        class_ids.dedup();
        object_ids.sort_unstable();
        object_ids.dedup();
        Self {
            collection_ids,
            class_ids,
            object_ids,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (Vec<CollectionId>, Vec<ClassId>, Vec<ObjectId>) {
        (self.collection_ids, self.class_ids, self.object_ids)
    }
}

/// Backend-neutral snapshot of a token's enabled narrowing dimensions.
///
/// `None` means that a dimension is disabled. `Some(empty)` means that it is
/// enabled but grants nothing; adapters must preserve that distinction.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageAuthenticationTokenScope {
    permissions: Option<Vec<StorageAuthorizationPermission>>,
    resources: Option<StorageAuthenticationResourceScope>,
}

impl std::fmt::Debug for StorageAuthenticationTokenScope {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StorageAuthenticationTokenScope")
            .field("permission_count", &self.permissions.as_ref().map(Vec::len))
            .field("resources", &self.resources)
            .finish()
    }
}

impl StorageAuthenticationTokenScope {
    #[must_use]
    pub fn new(
        permissions: Option<Vec<StorageAuthorizationPermission>>,
        resources: Option<StorageAuthenticationResourceScope>,
    ) -> Self {
        let permissions = permissions.map(|mut permissions| {
            permissions.sort_unstable();
            permissions.dedup();
            permissions
        });
        Self {
            permissions,
            resources,
        }
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        Option<Vec<StorageAuthorizationPermission>>,
        Option<StorageAuthenticationResourceScope>,
    ) {
        (self.permissions, self.resources)
    }
}

/// Authentication data every selectable storage backend must provide.
///
/// Implementations own persistence joins and row decoding. Consumers receive
/// only the projections required to build application authentication state.
#[async_trait]
pub trait AuthenticationStorage: Send + Sync {
    /// Validate one presented bearer credential and return its minimal
    /// authentication projection.
    ///
    /// Implementations must reject revoked and expired credentials, reject
    /// credentials owned by disabled principals, and may update non-security
    /// usage telemetry without failing an otherwise successful validation.
    async fn authenticate_bearer_token(
        &self,
        attempt: StorageAuthenticationAttempt,
    ) -> Result<StorageAuthenticatedToken, StorageError>;

    async fn get_authentication_identity(
        &self,
        principal_id: PrincipalId,
    ) -> Result<StorageAuthenticationIdentity, StorageError>;

    async fn get_authentication_token_scope(
        &self,
        query: StorageAuthenticationTokenScopeQuery,
    ) -> Result<Option<StorageAuthenticationTokenScope>, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enabled_empty_scope_dimensions_remain_present() {
        let scope = StorageAuthenticationTokenScope::new(
            Some(Vec::new()),
            Some(StorageAuthenticationResourceScope::default()),
        );

        let (permissions, resources) = scope.into_parts();
        assert_eq!(permissions, Some(Vec::new()));
        assert_eq!(
            resources.map(StorageAuthenticationResourceScope::into_parts),
            Some((Vec::new(), Vec::new(), Vec::new()))
        );
    }

    #[test]
    fn unscoped_query_has_no_enabled_dimensions() {
        let query =
            StorageAuthenticationTokenScopeQuery::new(TokenId::new(7).unwrap(), false, false);

        assert!(!query.is_scoped());
        assert!(!query.is_permission_scoped());
        assert!(!query.is_resource_scoped());
    }

    #[test]
    fn authentication_attempt_rejects_an_inverted_validity_window() {
        let observed_at = DateTime::<Utc>::default();
        let valid_after = observed_at + chrono::Duration::seconds(1);

        assert!(
            StorageAuthenticationAttempt::try_new(
                StorageAuthenticationCredential::new("lookup"),
                observed_at,
                valid_after,
            )
            .is_err()
        );
    }

    #[test]
    fn authentication_dto_debug_output_redacts_identity_values() {
        let principal = StorageAuthenticationPrincipal::new(
            PrincipalId::new(42).unwrap(),
            PrincipalKind::Human,
            "sensitive-name",
            IdentityScopeId::new(17).unwrap(),
        );

        let debug = format!("{principal:?}");
        assert!(!debug.contains("sensitive-name"));
        assert!(!debug.contains("42"));
        assert!(!debug.contains("17"));
    }

    #[test]
    fn authentication_credentials_and_results_redact_identifiers() {
        let credential = StorageAuthenticationCredential::new("sensitive-lookup-value");
        let token = StorageAuthenticatedToken::builder(
            TokenId::new(42).unwrap(),
            PrincipalId::new(17).unwrap(),
            DateTime::<Utc>::default(),
            ResourceRevision::new(3).unwrap(),
        )
        .name(Some("sensitive-name".to_string()))
        .permission_scoped(true)
        .try_build()
        .unwrap();

        let credential_debug = format!("{credential:?}");
        assert!(!credential_debug.contains("sensitive-lookup-value"));
        let token_debug = format!("{token:?}");
        assert!(!token_debug.contains("42"));
        assert!(!token_debug.contains("17"));
        assert!(!token_debug.contains("sensitive-name"));
        assert!(token.is_scoped());
    }
}
