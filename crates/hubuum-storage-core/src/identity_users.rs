use std::fmt;

use async_trait::async_trait;
use chrono::NaiveDateTime;
use hubuum_events_core::EventContext;
use hubuum_query::QueryOptions;

use crate::{StorageError, StorageIdentityPage};

/// Owned fields returned when a storage user crosses into an adapter or
/// application model.
pub type StorageUserParts = (
    i32,
    Option<String>,
    Option<String>,
    Option<String>,
    NaiveDateTime,
    NaiveDateTime,
    Option<NaiveDateTime>,
);

/// Owned fields returned when a user-list projection is consumed.
pub type StorageUserListItemParts = (
    StorageUser,
    String,
    String,
    String,
    bool,
    Option<NaiveDateTime>,
    Option<NaiveDateTime>,
    i64,
);

/// Owned fields returned when a point projection is consumed.
pub type StorageUserPointParts = (
    i32,
    Option<String>,
    Option<String>,
    NaiveDateTime,
    NaiveDateTime,
    i32,
    bool,
    String,
    i64,
);

/// Backend-neutral human principal row.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageUser {
    id: i32,
    password_hash: Option<String>,
    proper_name: Option<String>,
    email: Option<String>,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    anonymized_at: Option<NaiveDateTime>,
}

impl StorageUser {
    #[must_use]
    pub const fn new(
        id: i32,
        password_hash: Option<String>,
        proper_name: Option<String>,
        email: Option<String>,
        created_at: NaiveDateTime,
        updated_at: NaiveDateTime,
        anonymized_at: Option<NaiveDateTime>,
    ) -> Self {
        Self {
            id,
            password_hash,
            proper_name,
            email,
            created_at,
            updated_at,
            anonymized_at,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> StorageUserParts {
        (
            self.id,
            self.password_hash,
            self.proper_name,
            self.email,
            self.created_at,
            self.updated_at,
            self.anonymized_at,
        )
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
    last_sync_attempted_at: Option<NaiveDateTime>,
    last_sync_success_at: Option<NaiveDateTime>,
    revision: i64,
}

impl StorageUserListItem {
    #[must_use]
    pub fn builder(
        user: StorageUser,
        identity_scope: impl Into<String>,
        provider_kind: impl Into<String>,
        name: impl Into<String>,
        revision: i64,
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
        (
            self.user,
            self.identity_scope,
            self.provider_kind,
            self.name,
            self.provider_managed,
            self.last_sync_attempted_at,
            self.last_sync_success_at,
            self.revision,
        )
    }
}

/// Builder for a user-list projection and its optional synchronization state.
pub struct StorageUserListItemBuilder {
    user: StorageUser,
    identity_scope: String,
    provider_kind: String,
    name: String,
    provider_managed: bool,
    last_sync_attempted_at: Option<NaiveDateTime>,
    last_sync_success_at: Option<NaiveDateTime>,
    revision: i64,
}

impl StorageUserListItemBuilder {
    #[must_use]
    pub const fn provider_managed(mut self, value: bool) -> Self {
        self.provider_managed = value;
        self
    }

    #[must_use]
    pub const fn last_sync_attempted_at(mut self, value: Option<NaiveDateTime>) -> Self {
        self.last_sync_attempted_at = value;
        self
    }

    #[must_use]
    pub const fn last_sync_success_at(mut self, value: Option<NaiveDateTime>) -> Self {
        self.last_sync_success_at = value;
        self
    }

    #[must_use]
    pub fn build(self) -> StorageUserListItem {
        StorageUserListItem {
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
pub struct StorageUserPoint {
    id: i32,
    proper_name: Option<String>,
    email: Option<String>,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    identity_scope_id: i32,
    provider_managed: bool,
    name: String,
    revision: i64,
}

impl StorageUserPoint {
    #[must_use]
    pub fn builder(
        id: i32,
        created_at: NaiveDateTime,
        updated_at: NaiveDateTime,
        identity_scope_id: i32,
        name: impl Into<String>,
        revision: i64,
    ) -> StorageUserPointBuilder {
        StorageUserPointBuilder {
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
    pub fn into_parts(self) -> StorageUserPointParts {
        (
            self.id,
            self.proper_name,
            self.email,
            self.created_at,
            self.updated_at,
            self.identity_scope_id,
            self.provider_managed,
            self.name,
            self.revision,
        )
    }
}

/// Builder for the strongly versioned user point projection.
pub struct StorageUserPointBuilder {
    id: i32,
    proper_name: Option<String>,
    email: Option<String>,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    identity_scope_id: i32,
    provider_managed: bool,
    name: String,
    revision: i64,
}

impl StorageUserPointBuilder {
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

    #[must_use]
    pub fn build(self) -> StorageUserPoint {
        StorageUserPoint {
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

impl fmt::Debug for StorageUserPoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageUserPoint")
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
            .field("has_cursor", &self.options.cursor().is_some())
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
}

impl StorageUserCreate {
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
    id: i32,
    password_hash: Option<String>,
    proper_name: Option<String>,
    email: Option<String>,
    event_context: EventContext,
}

impl StorageUserUpdate {
    #[must_use]
    pub const fn new(
        id: i32,
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
        }
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        i32,
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
    id: i32,
    event_context: EventContext,
}

impl StorageUserDelete {
    #[must_use]
    pub const fn new(id: i32, event_context: EventContext) -> Self {
        Self { id, event_context }
    }

    #[must_use]
    pub fn into_parts(self) -> (i32, EventContext) {
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
    id: i32,
    password_hash: String,
    event_context: EventContext,
}

impl StorageUserPasswordUpdate {
    #[must_use]
    pub fn new(id: i32, password_hash: impl Into<String>, event_context: EventContext) -> Self {
        Self {
            id,
            password_hash: password_hash.into(),
            event_context,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (i32, String, EventContext) {
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
    id: i32,
    event_context: EventContext,
}

impl StorageUserAnonymize {
    #[must_use]
    pub const fn new(id: i32, event_context: EventContext) -> Self {
        Self { id, event_context }
    }

    #[must_use]
    pub fn into_parts(self) -> (i32, EventContext) {
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

/// Complete human-user persistence required of every selectable backend.
#[async_trait]
pub trait UserStorage: Send + Sync {
    async fn load_user(&self, id: i32) -> Result<StorageUser, StorageError>;

    async fn load_user_by_name(
        &self,
        identity_scope: String,
        name: String,
    ) -> Result<StorageUser, StorageError>;

    async fn load_user_point(&self, id: i32) -> Result<StorageUserPoint, StorageError>;

    async fn list_users(
        &self,
        query: StorageUserListQuery,
    ) -> Result<StorageIdentityPage<StorageUserListItem>, StorageError>;

    async fn create_user(&self, request: StorageUserCreate) -> Result<StorageUser, StorageError>;

    async fn update_user(&self, request: StorageUserUpdate) -> Result<StorageUser, StorageError>;

    /// Replace a local password and revoke active credentials atomically.
    async fn set_user_password(
        &self,
        request: StorageUserPasswordUpdate,
    ) -> Result<usize, StorageError>;

    async fn delete_user(&self, request: StorageUserDelete) -> Result<usize, StorageError>;

    async fn anonymize_user(&self, request: StorageUserAnonymize) -> Result<(), StorageError>;
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
            EventContext::system(),
            None,
            None,
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
