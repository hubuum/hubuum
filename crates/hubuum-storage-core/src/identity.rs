use async_trait::async_trait;
use chrono::NaiveDateTime;

use crate::StorageError;

/// Principal kinds understood by authentication and authorization storage.
///
/// This enum belongs to the backend-neutral contract. Adapters must reject an
/// unknown persisted kind instead of passing a storage-specific string to the
/// application.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthenticationPrincipalKind {
    Human,
    ServiceAccount,
}

impl AuthenticationPrincipalKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Human => "human",
            Self::ServiceAccount => "service_account",
        }
    }

    #[must_use]
    pub const fn is_human(self) -> bool {
        matches!(self, Self::Human)
    }

    #[must_use]
    pub const fn is_service_account(self) -> bool {
        matches!(self, Self::ServiceAccount)
    }
}

/// Minimal principal data required after bearer-token validation.
///
/// Persistence metadata, settings documents, revisions, and provider sync
/// state deliberately stay behind the storage boundary.
#[derive(Clone, PartialEq, Eq)]
pub struct AuthenticationPrincipal {
    id: i32,
    kind: AuthenticationPrincipalKind,
    name: String,
    identity_scope_id: i32,
}

impl std::fmt::Debug for AuthenticationPrincipal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuthenticationPrincipal")
            .field("id", &"<redacted>")
            .field("kind", &self.kind)
            .field("name", &"<redacted>")
            .field("identity_scope_id", &"<redacted>")
            .finish()
    }
}

impl AuthenticationPrincipal {
    #[must_use]
    pub fn new(
        id: i32,
        kind: AuthenticationPrincipalKind,
        name: impl Into<String>,
        identity_scope_id: i32,
    ) -> Self {
        Self {
            id,
            kind,
            name: name.into(),
            identity_scope_id,
        }
    }

    #[must_use]
    pub const fn id(&self) -> i32 {
        self.id
    }

    #[must_use]
    pub const fn kind(&self) -> AuthenticationPrincipalKind {
        self.kind
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub const fn identity_scope_id(&self) -> i32 {
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
pub struct AuthenticationHuman {
    id: i32,
    proper_name: Option<String>,
    email: Option<String>,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    anonymized_at: Option<NaiveDateTime>,
}

impl std::fmt::Debug for AuthenticationHuman {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuthenticationHuman")
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

impl AuthenticationHuman {
    #[must_use]
    pub const fn new(
        id: i32,
        proper_name: Option<String>,
        email: Option<String>,
        created_at: NaiveDateTime,
        updated_at: NaiveDateTime,
        anonymized_at: Option<NaiveDateTime>,
    ) -> Self {
        Self {
            id,
            proper_name,
            email,
            created_at,
            updated_at,
            anonymized_at,
        }
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        i32,
        Option<String>,
        Option<String>,
        NaiveDateTime,
        NaiveDateTime,
        Option<NaiveDateTime>,
    ) {
        (
            self.id,
            self.proper_name,
            self.email,
            self.created_at,
            self.updated_at,
            self.anonymized_at,
        )
    }
}

/// One consistent authentication read of a principal and its optional human
/// subtype.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthenticationIdentity {
    principal: AuthenticationPrincipal,
    human: Option<AuthenticationHuman>,
}

impl AuthenticationIdentity {
    #[must_use]
    pub const fn new(
        principal: AuthenticationPrincipal,
        human: Option<AuthenticationHuman>,
    ) -> Self {
        Self { principal, human }
    }

    #[must_use]
    pub fn into_parts(self) -> (AuthenticationPrincipal, Option<AuthenticationHuman>) {
        (self.principal, self.human)
    }
}

/// Complete information needed to load the narrowing dimensions of one token.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct AuthenticationTokenScopeQuery {
    token_id: i32,
    permission_scoped: bool,
    resource_scoped: bool,
}

impl std::fmt::Debug for AuthenticationTokenScopeQuery {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuthenticationTokenScopeQuery")
            .field("token_id", &"<redacted>")
            .field("permission_scoped", &self.permission_scoped)
            .field("resource_scoped", &self.resource_scoped)
            .finish()
    }
}

impl AuthenticationTokenScopeQuery {
    #[must_use]
    pub const fn new(token_id: i32, permission_scoped: bool, resource_scoped: bool) -> Self {
        Self {
            token_id,
            permission_scoped,
            resource_scoped,
        }
    }

    #[must_use]
    pub const fn token_id(self) -> i32 {
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
pub struct AuthenticationResourceScope {
    collection_ids: Vec<i32>,
    class_ids: Vec<i32>,
    object_ids: Vec<i32>,
}

impl std::fmt::Debug for AuthenticationResourceScope {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuthenticationResourceScope")
            .field("collection_count", &self.collection_ids.len())
            .field("class_count", &self.class_ids.len())
            .field("object_count", &self.object_ids.len())
            .finish()
    }
}

impl AuthenticationResourceScope {
    #[must_use]
    pub const fn new(collection_ids: Vec<i32>, class_ids: Vec<i32>, object_ids: Vec<i32>) -> Self {
        Self {
            collection_ids,
            class_ids,
            object_ids,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (Vec<i32>, Vec<i32>, Vec<i32>) {
        (self.collection_ids, self.class_ids, self.object_ids)
    }
}

/// Backend-neutral snapshot of a token's enabled narrowing dimensions.
///
/// `None` means that a dimension is disabled. `Some(empty)` means that it is
/// enabled but grants nothing; adapters must preserve that distinction.
#[derive(Clone, PartialEq, Eq)]
pub struct AuthenticationTokenScope {
    permissions: Option<Vec<String>>,
    resources: Option<AuthenticationResourceScope>,
}

impl std::fmt::Debug for AuthenticationTokenScope {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuthenticationTokenScope")
            .field("permission_count", &self.permissions.as_ref().map(Vec::len))
            .field("resources", &self.resources)
            .finish()
    }
}

impl AuthenticationTokenScope {
    #[must_use]
    pub const fn new(
        permissions: Option<Vec<String>>,
        resources: Option<AuthenticationResourceScope>,
    ) -> Self {
        Self {
            permissions,
            resources,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (Option<Vec<String>>, Option<AuthenticationResourceScope>) {
        (self.permissions, self.resources)
    }
}

/// Authentication data every selectable storage backend must provide.
///
/// Implementations own persistence joins and row decoding. Consumers receive
/// only the projections required to build application authentication state.
#[async_trait]
pub trait AuthenticationStorage: Send + Sync {
    async fn load_authentication_identity(
        &self,
        principal_id: i32,
    ) -> Result<AuthenticationIdentity, StorageError>;

    async fn load_authentication_token_scope(
        &self,
        query: AuthenticationTokenScopeQuery,
    ) -> Result<Option<AuthenticationTokenScope>, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enabled_empty_scope_dimensions_remain_present() {
        let scope = AuthenticationTokenScope::new(
            Some(Vec::new()),
            Some(AuthenticationResourceScope::default()),
        );

        let (permissions, resources) = scope.into_parts();
        assert_eq!(permissions, Some(Vec::new()));
        assert_eq!(
            resources.map(AuthenticationResourceScope::into_parts),
            Some((Vec::new(), Vec::new(), Vec::new()))
        );
    }

    #[test]
    fn unscoped_query_has_no_enabled_dimensions() {
        let query = AuthenticationTokenScopeQuery::new(7, false, false);

        assert!(!query.is_scoped());
        assert!(!query.is_permission_scoped());
        assert!(!query.is_resource_scoped());
    }

    #[test]
    fn authentication_dto_debug_output_redacts_identity_values() {
        let principal = AuthenticationPrincipal::new(
            42,
            AuthenticationPrincipalKind::Human,
            "sensitive-name",
            17,
        );

        let debug = format!("{principal:?}");
        assert!(!debug.contains("sensitive-name"));
        assert!(!debug.contains("42"));
        assert!(!debug.contains("17"));
    }
}
