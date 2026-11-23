use std::fmt;

use chrono::{DateTime, Utc};
use hubuum_domain::{IdentityScopeId, PrincipalKind, ResourceRevision, UserId};
use hubuum_events_core::{EventContext, EventSubject};
use hubuum_query::QueryOptions;

use crate::validation::validate_sync_timestamps;
use crate::{StorageSyncedHuman, StorageValidationError};

/// Authentication evidence that must still match the human row when the token
/// commits. Adapters check this while holding the password-reset principal lock.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageLogin {
    subject: EventSubject,
    user: StorageUser,
    authentication_revision: Option<hubuum_domain::ResourceRevision>,
}

impl StorageLogin {
    /// A credential/change fence suitable for short-lived interaction storage.
    /// This retains neither the password nor its stored password hash.
    pub fn interaction_fingerprint(&self) -> hubuum_domain::InteractionDigest {
        Self::user_interaction_fingerprint(&self.user)
    }

    pub(crate) fn user_interaction_fingerprint(
        user: &StorageUser,
    ) -> hubuum_domain::InteractionDigest {
        use sha2::{Digest, Sha256};
        let parts = user.clone().into_parts();
        let bytes = serde_json::to_vec(&(parts.id(), parts.updated_at(), parts.password_hash()))
            .expect("credential fence is JSON-compatible");
        hubuum_domain::InteractionDigest::new(
            Sha256::digest(bytes)
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
        )
        .expect("SHA-256 is a lowercase hexadecimal digest")
    }

    /// Local login does not depend on an external provider revision.
    pub fn try_new(
        subject: EventSubject,
        user: StorageUser,
    ) -> Result<Self, StorageValidationError> {
        if subject.identity_scope() != "local" || subject.authentication_provider_kind() != "local"
        {
            return Err(StorageValidationError::invalid(
                "External login requires an authentication revision",
            ));
        }
        Self::validated(subject, user, None)
    }

    /// External evidence must carry the revision used for provider authentication.
    pub fn external(
        subject: EventSubject,
        user: StorageUser,
        revision: hubuum_domain::ResourceRevision,
    ) -> Result<Self, StorageValidationError> {
        if subject.identity_scope() == "local" || subject.authentication_provider_kind() == "local"
        {
            return Err(StorageValidationError::invalid(
                "External login requires an external identity scope",
            ));
        }
        Self::validated(subject, user, Some(revision))
    }

    fn validated(
        subject: EventSubject,
        user: StorageUser,
        authentication_revision: Option<hubuum_domain::ResourceRevision>,
    ) -> Result<Self, StorageValidationError> {
        if subject.principal_id().id() != user.id.id()
            || subject.kind() != PrincipalKind::Human
            || user.anonymized_at.is_some()
        {
            return Err(StorageValidationError::invalid(
                "invalid authenticated login subject",
            ));
        }
        Ok(Self {
            subject,
            user,
            authentication_revision,
        })
    }

    pub const fn authentication_revision(&self) -> Option<hubuum_domain::ResourceRevision> {
        self.authentication_revision
    }

    pub fn subject(&self) -> &EventSubject {
        &self.subject
    }

    pub(crate) fn with_administrator_status(mut self, is_admin: bool) -> Self {
        self.subject = self.subject.with_administrator_status(is_admin);
        self
    }

    pub fn matches_user(&self, current: &StorageUser) -> bool {
        current.id == self.user.id
            && current.password_hash == self.user.password_hash
            && current.updated_at == self.user.updated_at
            && current.anonymized_at.is_none()
    }
}

/// Named fields returned when a storage user crosses an API boundary.
pub struct StorageUserParts {
    id: UserId,
    password_hash: Option<String>,
    proper_name: Option<String>,
    email: Option<String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    anonymized_at: Option<DateTime<Utc>>,
}

impl StorageUserParts {
    #[must_use]
    pub const fn id(&self) -> UserId {
        self.id
    }
    #[must_use]
    pub fn password_hash(&self) -> Option<&str> {
        self.password_hash.as_deref()
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

/// Named fields returned when a user-list projection is consumed.
pub struct StorageUserListItemParts {
    user: StorageUser,
    identity_scope: String,
    provider_kind: String,
    name: String,
    provider_managed: bool,
    last_sync_attempted_at: Option<DateTime<Utc>>,
    last_sync_success_at: Option<DateTime<Utc>>,
    revision: ResourceRevision,
}

impl StorageUserListItemParts {
    #[must_use]
    pub const fn user(&self) -> &StorageUser {
        &self.user
    }
    #[must_use]
    pub fn identity_scope(&self) -> &str {
        &self.identity_scope
    }
    #[must_use]
    pub fn provider_kind(&self) -> &str {
        &self.provider_kind
    }
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    #[must_use]
    pub const fn provider_managed(&self) -> bool {
        self.provider_managed
    }
    #[must_use]
    pub const fn last_sync_attempted_at(&self) -> Option<DateTime<Utc>> {
        self.last_sync_attempted_at
    }
    #[must_use]
    pub const fn last_sync_success_at(&self) -> Option<DateTime<Utc>> {
        self.last_sync_success_at
    }
    #[must_use]
    pub const fn revision(&self) -> ResourceRevision {
        self.revision
    }
}

/// Named fields returned when a point projection is consumed.
pub struct StorageUserDetailsParts {
    id: UserId,
    proper_name: Option<String>,
    email: Option<String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    identity_scope_id: IdentityScopeId,
    provider_managed: bool,
    name: String,
    revision: ResourceRevision,
}

impl StorageUserDetailsParts {
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
    pub const fn identity_scope_id(&self) -> IdentityScopeId {
        self.identity_scope_id
    }
    #[must_use]
    pub const fn provider_managed(&self) -> bool {
        self.provider_managed
    }
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    #[must_use]
    pub const fn revision(&self) -> ResourceRevision {
        self.revision
    }
}

/// Backend-neutral human principal row.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageUser {
    id: UserId,
    password_hash: Option<String>,
    proper_name: Option<String>,
    email: Option<String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    anonymized_at: Option<DateTime<Utc>>,
}

/// Preserve the validated timestamps and identity of a synchronized external user.
impl From<StorageSyncedHuman> for StorageUser {
    fn from(human: StorageSyncedHuman) -> Self {
        Self {
            id: human.id(),
            password_hash: None,
            proper_name: human.proper_name().map(str::to_owned),
            email: human.email().map(str::to_owned),
            created_at: human.created_at(),
            updated_at: human.updated_at(),
            anonymized_at: human.anonymized_at(),
        }
    }
}

impl StorageUser {
    pub fn try_new(
        id: UserId,
        password_hash: Option<String>,
        proper_name: Option<String>,
        email: Option<String>,
        created_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
        anonymized_at: Option<DateTime<Utc>>,
    ) -> Result<Self, StorageValidationError> {
        if updated_at < created_at {
            return Err(StorageValidationError::invalid(
                "user updated_at must not precede created_at",
            ));
        }
        if anonymized_at.is_some_and(|value| value < created_at || value > updated_at) {
            return Err(StorageValidationError::invalid(
                "user anonymized_at must be within its creation and update timestamps",
            ));
        }
        Ok(Self {
            id,
            password_hash,
            proper_name,
            email,
            created_at,
            updated_at,
            anonymized_at,
        })
    }

    #[must_use]
    pub fn into_parts(self) -> StorageUserParts {
        StorageUserParts {
            id: self.id,
            password_hash: self.password_hash,
            proper_name: self.proper_name,
            email: self.email,
            created_at: self.created_at,
            updated_at: self.updated_at,
            anonymized_at: self.anonymized_at,
        }
    }
}

impl fmt::Debug for StorageUser {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageUser")
            .field("id", &"<redacted>")
            .field("has_password_hash", &self.password_hash.is_some())
            .field("has_proper_name", &self.proper_name.is_some())
            .field("has_email", &self.email.is_some())
            .field("is_anonymized", &self.anonymized_at.is_some())
            .finish()
    }
}

/// One user together with principal-owned identity metadata.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageUserListItem {
    user: StorageUser,
    identity_scope: String,
    provider_kind: String,
    name: String,
    provider_managed: bool,
    last_sync_attempted_at: Option<DateTime<Utc>>,
    last_sync_success_at: Option<DateTime<Utc>>,
    revision: ResourceRevision,
}

impl StorageUserListItem {
    #[must_use]
    pub fn builder(
        user: StorageUser,
        identity_scope: impl Into<String>,
        provider_kind: impl Into<String>,
        name: impl Into<String>,
        revision: ResourceRevision,
    ) -> StorageUserListItemBuilder {
        StorageUserListItemBuilder {
            user,
            identity_scope: identity_scope.into(),
            provider_kind: provider_kind.into(),
            name: name.into(),
            provider_managed: false,
            last_sync_attempted_at: None,
            last_sync_success_at: None,
            revision,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> StorageUserListItemParts {
        StorageUserListItemParts {
            user: self.user,
            identity_scope: self.identity_scope,
            provider_kind: self.provider_kind,
            name: self.name,
            provider_managed: self.provider_managed,
            last_sync_attempted_at: self.last_sync_attempted_at,
            last_sync_success_at: self.last_sync_success_at,
            revision: self.revision,
        }
    }
}

/// Builder for a user-list projection and its optional synchronization state.
pub struct StorageUserListItemBuilder {
    user: StorageUser,
    identity_scope: String,
    provider_kind: String,
    name: String,
    provider_managed: bool,
    last_sync_attempted_at: Option<DateTime<Utc>>,
    last_sync_success_at: Option<DateTime<Utc>>,
    revision: ResourceRevision,
}

impl StorageUserListItemBuilder {
    #[must_use]
    pub const fn provider_managed(mut self, value: bool) -> Self {
        self.provider_managed = value;
        self
    }

    #[must_use]
    pub const fn last_sync_attempted_at(mut self, value: Option<DateTime<Utc>>) -> Self {
        self.last_sync_attempted_at = value;
        self
    }

    #[must_use]
    pub const fn last_sync_success_at(mut self, value: Option<DateTime<Utc>>) -> Self {
        self.last_sync_success_at = value;
        self
    }

    pub fn try_build(self) -> Result<StorageUserListItem, StorageValidationError> {
        validate_sync_timestamps(self.last_sync_attempted_at, self.last_sync_success_at)?;
        Ok(StorageUserListItem {
            user: self.user,
            identity_scope: self.identity_scope,
            provider_kind: self.provider_kind,
            name: self.name,
            provider_managed: self.provider_managed,
            last_sync_attempted_at: self.last_sync_attempted_at,
            last_sync_success_at: self.last_sync_success_at,
            revision: self.revision,
        })
    }
}

impl fmt::Debug for StorageUserListItem {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageUserListItem")
            .field("user", &self.user)
            .field("identity_scope", &"<redacted>")
            .field("provider_kind", &self.provider_kind)
            .field("name", &"<redacted>")
            .field("provider_managed", &self.provider_managed)
            .finish()
    }
}

/// Strong point projection governed by the principal revision.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageUserDetails {
    id: UserId,
    proper_name: Option<String>,
    email: Option<String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    identity_scope_id: IdentityScopeId,
    provider_managed: bool,
    name: String,
    revision: ResourceRevision,
}

impl StorageUserDetails {
    #[must_use]
    pub fn builder(
        id: UserId,
        created_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
        identity_scope_id: IdentityScopeId,
        name: impl Into<String>,
        revision: ResourceRevision,
    ) -> StorageUserDetailsBuilder {
        StorageUserDetailsBuilder {
            id,
            proper_name: None,
            email: None,
            created_at,
            updated_at,
            identity_scope_id,
            provider_managed: false,
            name: name.into(),
            revision,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> StorageUserDetailsParts {
        StorageUserDetailsParts {
            id: self.id,
            proper_name: self.proper_name,
            email: self.email,
            created_at: self.created_at,
            updated_at: self.updated_at,
            identity_scope_id: self.identity_scope_id,
            provider_managed: self.provider_managed,
            name: self.name,
            revision: self.revision,
        }
    }
}

/// Builder for the strongly versioned user point projection.
pub struct StorageUserDetailsBuilder {
    id: UserId,
    proper_name: Option<String>,
    email: Option<String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    identity_scope_id: IdentityScopeId,
    provider_managed: bool,
    name: String,
    revision: ResourceRevision,
}

impl StorageUserDetailsBuilder {
    #[must_use]
    pub fn proper_name(mut self, value: Option<String>) -> Self {
        self.proper_name = value;
        self
    }

    #[must_use]
    pub fn email(mut self, value: Option<String>) -> Self {
        self.email = value;
        self
    }

    #[must_use]
    pub const fn provider_managed(mut self, value: bool) -> Self {
        self.provider_managed = value;
        self
    }

    pub fn try_build(self) -> Result<StorageUserDetails, StorageValidationError> {
        if self.updated_at < self.created_at {
            return Err(StorageValidationError::invalid(
                "user details updated_at must not precede created_at",
            ));
        }
        Ok(StorageUserDetails {
            id: self.id,
            proper_name: self.proper_name,
            email: self.email,
            created_at: self.created_at,
            updated_at: self.updated_at,
            identity_scope_id: self.identity_scope_id,
            provider_managed: self.provider_managed,
            name: self.name,
            revision: self.revision,
        })
    }
}

impl fmt::Debug for StorageUserDetails {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageUserDetails")
            .field("id", &"<redacted>")
            .field("has_proper_name", &self.proper_name.is_some())
            .field("has_email", &self.email.is_some())
            .field("identity_scope_id", &"<redacted>")
            .field("provider_managed", &self.provider_managed)
            .field("name", &"<redacted>")
            .field("revision", &self.revision)
            .finish()
    }
}

/// Stable user list/search request.
#[derive(Clone, PartialEq)]
pub struct StorageUserListQuery {
    options: QueryOptions,
}

impl fmt::Debug for StorageUserListQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageUserListQuery")
            .field("filter_count", &self.options.filters().len())
            .field("sort_count", &self.options.sort().len())
            .field("limit", &self.options.limit())
            .field("has_cursor", &self.options.has_cursor())
            .field("include_total", &self.options.include_total())
            .finish()
    }
}

impl StorageUserListQuery {
    #[must_use]
    pub const fn new(options: QueryOptions) -> Self {
        Self { options }
    }

    #[must_use]
    pub fn into_options(self) -> QueryOptions {
        self.options
    }
}

/// Local-human creation after application-owned password hashing.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageUserCreate {
    identity_scope: Option<String>,
    name: String,
    password_hash: String,
    proper_name: Option<String>,
    email: Option<String>,
    event_context: EventContext,
    credential_claim: Option<crate::StorageCredentialClaim>,
}

impl StorageUserCreate {
    /// Attach a fresh-authentication claim; adapters consume it in the mutation transaction.
    #[must_use]
    pub fn with_credential_claim(mut self, claim: crate::StorageCredentialClaim) -> Self {
        self.credential_claim = Some(claim);
        self
    }
    pub fn take_credential_claim(&mut self) -> Option<crate::StorageCredentialClaim> {
        self.credential_claim.take()
    }

    #[must_use]
    pub fn new(
        identity_scope: Option<String>,
        name: impl Into<String>,
        password_hash: impl Into<String>,
        proper_name: Option<String>,
        email: Option<String>,
        event_context: EventContext,
    ) -> Self {
        Self {
            identity_scope,
            name: name.into(),
            password_hash: password_hash.into(),
            proper_name,
            email,
            event_context,
            credential_claim: None,
        }
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        Option<String>,
        String,
        String,
        Option<String>,
        Option<String>,
        EventContext,
    ) {
        (
            self.identity_scope,
            self.name,
            self.password_hash,
            self.proper_name,
            self.email,
            self.event_context,
        )
    }
}

impl fmt::Debug for StorageUserCreate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageUserCreate")
            .field("has_identity_scope", &self.identity_scope.is_some())
            .field("name", &"<redacted>")
            .field("password_hash", &"<redacted>")
            .field("has_proper_name", &self.proper_name.is_some())
            .field("has_email", &self.email.is_some())
            .field("event_context", &"<redacted>")
            .finish()
    }
}

/// Human profile and credential patch after application-owned password hashing.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageUserUpdate {
    id: UserId,
    password_hash: Option<String>,
    proper_name: Option<String>,
    email: Option<String>,
    event_context: EventContext,
    credential_claim: Option<crate::StorageCredentialClaim>,
}

impl StorageUserUpdate {
    /// Attach a fresh-authentication claim; adapters consume it in the mutation transaction.
    #[must_use]
    pub fn with_credential_claim(mut self, claim: crate::StorageCredentialClaim) -> Self {
        self.credential_claim = Some(claim);
        self
    }
    pub fn take_credential_claim(&mut self) -> Option<crate::StorageCredentialClaim> {
        self.credential_claim.take()
    }

    #[must_use]
    pub const fn new(
        id: UserId,
        password_hash: Option<String>,
        proper_name: Option<String>,
        email: Option<String>,
        event_context: EventContext,
    ) -> Self {
        Self {
            id,
            password_hash,
            proper_name,
            email,
            event_context,
            credential_claim: None,
        }
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        UserId,
        Option<String>,
        Option<String>,
        Option<String>,
        EventContext,
    ) {
        (
            self.id,
            self.password_hash,
            self.proper_name,
            self.email,
            self.event_context,
        )
    }
}

impl fmt::Debug for StorageUserUpdate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageUserUpdate")
            .field("id", &"<redacted>")
            .field("has_password_hash", &self.password_hash.is_some())
            .field("has_proper_name", &self.proper_name.is_some())
            .field("has_email", &self.email.is_some())
            .field("event_context", &"<redacted>")
            .finish()
    }
}

/// Point user deletion with mandatory lifecycle event attribution.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageUserDelete {
    id: UserId,
    event_context: EventContext,
}

impl StorageUserDelete {
    #[must_use]
    pub const fn new(id: UserId, event_context: EventContext) -> Self {
        Self { id, event_context }
    }

    #[must_use]
    pub fn into_parts(self) -> (UserId, EventContext) {
        (self.id, self.event_context)
    }
}

impl fmt::Debug for StorageUserDelete {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageUserDelete")
            .field("id", &"<redacted>")
            .field("event_context", &"<redacted>")
            .finish()
    }
}

/// Pre-hashed local password replacement.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageUserPasswordUpdate {
    id: UserId,
    password_hash: String,
    event_context: EventContext,
}

impl StorageUserPasswordUpdate {
    #[must_use]
    pub fn new(id: UserId, password_hash: impl Into<String>, event_context: EventContext) -> Self {
        Self {
            id,
            password_hash: password_hash.into(),
            event_context,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (UserId, String, EventContext) {
        (self.id, self.password_hash, self.event_context)
    }
}

impl fmt::Debug for StorageUserPasswordUpdate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageUserPasswordUpdate")
            .field("id", &"<redacted>")
            .field("password_hash", &"<redacted>")
            .field("event_context", &"<redacted>")
            .finish()
    }
}

/// User anonymization with mandatory audit attribution.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageUserAnonymize {
    id: UserId,
    event_context: EventContext,
}

impl StorageUserAnonymize {
    #[must_use]
    pub const fn new(id: UserId, event_context: EventContext) -> Self {
        Self { id, event_context }
    }

    #[must_use]
    pub fn into_parts(self) -> (UserId, EventContext) {
        (self.id, self.event_context)
    }
}

impl fmt::Debug for StorageUserAnonymize {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageUserAnonymize")
            .field("id", &"<redacted>")
            .field("event_context", &"<redacted>")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_list_debug_output_redacts_credentials_and_cursors() {
        let create = StorageUserCreate::new(
            Some("sensitive-scope".to_string()),
            "sensitive-name",
            "sensitive-password-hash",
            None,
            None,
            EventContext::system(),
        );
        let debug = format!("{create:?}");
        assert!(!debug.contains("sensitive-scope"));
        assert!(!debug.contains("sensitive-name"));
        assert!(!debug.contains("sensitive-password-hash"));

        let query = StorageUserListQuery::new(
            QueryOptions::new(
                Vec::new(),
                Vec::new(),
                Some(20),
                Some("sensitive-cursor".to_string()),
                true,
            )
            .unwrap(),
        );
        let debug = format!("{query:?}");
        assert!(!debug.contains("sensitive-cursor"));
        assert!(debug.contains("has_cursor: true"));
    }
}
