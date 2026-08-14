use std::fmt;

use async_trait::async_trait;
use chrono::NaiveDateTime;
use hubuum_events_core::EventContext;
use hubuum_query::QueryOptions;
use serde_json::Value;

use crate::{StorageError, StorageIdentityGroup, StoragePrincipalGroup, StorageRecordMetadata};

/// Portable principal record returned by identity-resource operations.
#[derive(Clone, PartialEq)]
pub struct StoragePrincipal {
    id: i32,
    kind: String,
    name: String,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    identity_scope_id: i32,
    provider_managed: bool,
    settings: Value,
    external_subject: Option<String>,
    last_sync_attempted_at: Option<NaiveDateTime>,
    last_sync_success_at: Option<NaiveDateTime>,
    revision: i64,
}

impl StoragePrincipal {
    #[must_use]
    pub fn builder(
        metadata: StorageRecordMetadata,
        kind: impl Into<String>,
        name: impl Into<String>,
        identity_scope_id: i32,
    ) -> StoragePrincipalBuilder {
        StoragePrincipalBuilder {
            metadata,
            kind: kind.into(),
            name: name.into(),
            identity_scope_id,
            provider_managed: false,
            settings: Value::Object(Default::default()),
            external_subject: None,
            last_sync_attempted_at: None,
            last_sync_success_at: None,
        }
    }

    #[must_use]
    pub const fn id(&self) -> i32 {
        self.id
    }

    #[must_use]
    pub fn into_parts(self) -> StoragePrincipalParts {
        StoragePrincipalParts {
            id: self.id,
            kind: self.kind,
            name: self.name,
            created_at: self.created_at,
            updated_at: self.updated_at,
            identity_scope_id: self.identity_scope_id,
            provider_managed: self.provider_managed,
            settings: self.settings,
            external_subject: self.external_subject,
            last_sync_attempted_at: self.last_sync_attempted_at,
            last_sync_success_at: self.last_sync_success_at,
            revision: self.revision,
        }
    }
}

impl fmt::Debug for StoragePrincipal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoragePrincipal")
            .field("id", &"<redacted>")
            .field("kind", &self.kind)
            .field("provider_managed", &self.provider_managed)
            .field("has_external_subject", &self.external_subject.is_some())
            .finish()
    }
}

/// Named fields of a portable principal record.
pub struct StoragePrincipalParts {
    pub id: i32,
    pub kind: String,
    pub name: String,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
    pub identity_scope_id: i32,
    pub provider_managed: bool,
    pub settings: Value,
    pub external_subject: Option<String>,
    pub last_sync_attempted_at: Option<NaiveDateTime>,
    pub last_sync_success_at: Option<NaiveDateTime>,
    pub revision: i64,
}

/// Builder for optional principal record fields.
pub struct StoragePrincipalBuilder {
    metadata: StorageRecordMetadata,
    kind: String,
    name: String,
    identity_scope_id: i32,
    provider_managed: bool,
    settings: Value,
    external_subject: Option<String>,
    last_sync_attempted_at: Option<NaiveDateTime>,
    last_sync_success_at: Option<NaiveDateTime>,
}

impl StoragePrincipalBuilder {
    #[must_use]
    pub const fn provider_managed(mut self, value: bool) -> Self {
        self.provider_managed = value;
        self
    }

    #[must_use]
    pub fn settings(mut self, value: Value) -> Self {
        self.settings = value;
        self
    }

    #[must_use]
    pub fn external_subject(mut self, value: Option<String>) -> Self {
        self.external_subject = value;
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
    pub fn build(self) -> StoragePrincipal {
        StoragePrincipal {
            id: self.metadata.id(),
            kind: self.kind,
            name: self.name,
            created_at: self.metadata.created_at(),
            updated_at: self.metadata.updated_at(),
            identity_scope_id: self.identity_scope_id,
            provider_managed: self.provider_managed,
            settings: self.settings,
            external_subject: self.external_subject,
            last_sync_attempted_at: self.last_sync_attempted_at,
            last_sync_success_at: self.last_sync_success_at,
            revision: self.metadata.revision(),
        }
    }
}

/// Validated application request to create one local group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageGroupCreate {
    identity_scope: Option<String>,
    name: String,
    description: Option<String>,
}

impl StorageGroupCreate {
    #[must_use]
    pub fn new(
        identity_scope: Option<String>,
        name: impl Into<String>,
        description: Option<String>,
    ) -> Self {
        Self {
            identity_scope,
            name: name.into(),
            description,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (Option<String>, String, Option<String>) {
        (self.identity_scope, self.name, self.description)
    }
}

/// Validated application request to update one local group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageGroupUpdate {
    name: Option<String>,
}

impl StorageGroupUpdate {
    #[must_use]
    pub const fn new(name: Option<String>) -> Self {
        Self { name }
    }

    #[must_use]
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    #[must_use]
    pub fn into_name(self) -> Option<String> {
        self.name
    }
}

/// One principal-settings document and the revision that owns it.
#[derive(Clone, Debug, PartialEq)]
pub struct StoragePrincipalSettings {
    principal_id: i32,
    revision: i64,
    document: Value,
}

impl StoragePrincipalSettings {
    #[must_use]
    pub const fn new(principal_id: i32, revision: i64, document: Value) -> Self {
        Self {
            principal_id,
            revision,
            document,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (i32, i64, Value) {
        (self.principal_id, self.revision, self.document)
    }

    #[must_use]
    pub const fn document(&self) -> &Value {
        &self.document
    }
}

/// Atomic mutation applied to a principal-settings document.
#[derive(Clone, Debug, PartialEq)]
pub enum StoragePrincipalSettingsMutation {
    Replace(Value),
    MergePatch(Value),
    JsonPatch(Value),
    Reset,
}

/// Complete group lifecycle and membership behavior required from a backend.
#[async_trait]
pub trait GroupStorage: Send + Sync {
    async fn load_group(&self, group_id: i32) -> Result<StorageIdentityGroup, StorageError>;

    async fn group_identity_scope_name(&self, group_id: i32) -> Result<String, StorageError>;

    async fn create_group(
        &self,
        command: StorageGroupCreate,
        context: Option<&EventContext>,
    ) -> Result<StorageIdentityGroup, StorageError>;

    async fn update_group(
        &self,
        group_id: i32,
        update: StorageGroupUpdate,
        context: Option<&EventContext>,
    ) -> Result<StorageIdentityGroup, StorageError>;

    async fn delete_group(
        &self,
        group_id: i32,
        context: Option<&EventContext>,
    ) -> Result<usize, StorageError>;

    async fn group_members(&self, group_id: i32) -> Result<Vec<StoragePrincipal>, StorageError>;

    async fn group_members_page(
        &self,
        group_id: i32,
        query_options: QueryOptions,
    ) -> Result<Vec<(StoragePrincipalGroup, StoragePrincipal)>, StorageError>;

    async fn count_group_members(
        &self,
        group_id: i32,
        query_options: QueryOptions,
    ) -> Result<i64, StorageError>;

    async fn group_member_principal(
        &self,
        principal_id: i32,
    ) -> Result<StoragePrincipal, StorageError>;

    async fn add_group_member(
        &self,
        principal_id: i32,
        group_id: i32,
        context: Option<&EventContext>,
    ) -> Result<StoragePrincipalGroup, StorageError>;

    async fn remove_group_member(
        &self,
        principal_id: i32,
        group_id: i32,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError>;
}

/// Principal point and settings behavior required from every backend.
#[async_trait]
pub trait PrincipalStorage: Send + Sync {
    async fn load_principal(&self, principal_id: i32) -> Result<StoragePrincipal, StorageError>;

    async fn load_principal_settings(
        &self,
        principal_id: i32,
    ) -> Result<StoragePrincipalSettings, StorageError>;

    async fn mutate_principal_settings(
        &self,
        principal_id: i32,
        mutation: StoragePrincipalSettingsMutation,
        context: &EventContext,
    ) -> Result<StoragePrincipalSettings, StorageError>;
}
