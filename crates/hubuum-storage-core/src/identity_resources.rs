use std::fmt;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use hubuum_domain::{GroupId, IdentityScopeId, PrincipalId, PrincipalKind, ResourceRevision};
use hubuum_events_core::EventContext;
use serde_json::Value;

use crate::validation::validate_sync_timestamps;
use crate::{
    StorageError, StorageGroupListQuery, StorageIdentityGroup, StoragePage, StoragePrincipalGroup,
    StorageRecordMetadata, StorageValidationError,
};

/// Portable principal record returned by identity-resource operations.
#[derive(Clone, PartialEq)]
pub struct StoragePrincipal {
    id: PrincipalId,
    kind: PrincipalKind,
    name: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    identity_scope_id: IdentityScopeId,
    provider_managed: bool,
    settings: Value,
    external_subject: Option<String>,
    last_sync_attempted_at: Option<DateTime<Utc>>,
    last_sync_success_at: Option<DateTime<Utc>>,
    revision: ResourceRevision,
}

impl StoragePrincipal {
    #[must_use]
    pub fn builder(
        metadata: StorageRecordMetadata,
        kind: PrincipalKind,
        name: impl Into<String>,
        identity_scope_id: IdentityScopeId,
    ) -> StoragePrincipalBuilder {
        StoragePrincipalBuilder {
            metadata,
            kind,
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
    pub const fn settings(&self) -> &Value {
        &self.settings
    }

    #[must_use]
    pub fn external_subject(&self) -> Option<&str> {
        self.external_subject.as_deref()
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

/// Builder for optional principal record fields.
pub struct StoragePrincipalBuilder {
    metadata: StorageRecordMetadata,
    kind: PrincipalKind,
    name: String,
    identity_scope_id: IdentityScopeId,
    provider_managed: bool,
    settings: Value,
    external_subject: Option<String>,
    last_sync_attempted_at: Option<DateTime<Utc>>,
    last_sync_success_at: Option<DateTime<Utc>>,
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
    pub const fn last_sync_attempted_at(mut self, value: Option<DateTime<Utc>>) -> Self {
        self.last_sync_attempted_at = value;
        self
    }

    #[must_use]
    pub const fn last_sync_success_at(mut self, value: Option<DateTime<Utc>>) -> Self {
        self.last_sync_success_at = value;
        self
    }

    pub fn try_build(self) -> Result<StoragePrincipal, StorageValidationError> {
        validate_sync_timestamps(self.last_sync_attempted_at, self.last_sync_success_at)?;
        if !self.settings.is_object() {
            return Err(StorageValidationError::invalid(
                "principal settings must be a JSON object",
            ));
        }
        Ok(StoragePrincipal {
            id: PrincipalId::from(self.metadata.id()),
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
        })
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
    principal_id: PrincipalId,
    revision: ResourceRevision,
    document: Value,
}

impl StoragePrincipalSettings {
    pub fn try_new(
        principal_id: PrincipalId,
        revision: ResourceRevision,
        document: Value,
    ) -> Result<Self, StorageValidationError> {
        if !document.is_object() {
            return Err(StorageValidationError::invalid(
                "principal settings must be a JSON object",
            ));
        }
        Ok(Self {
            principal_id,
            revision,
            document,
        })
    }

    #[must_use]
    pub fn into_parts(self) -> (PrincipalId, ResourceRevision, Value) {
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

/// One group-membership row paired with its principal projection.
#[derive(Clone, PartialEq)]
pub struct StorageGroupMember {
    membership: StoragePrincipalGroup,
    principal: StoragePrincipal,
}

impl StorageGroupMember {
    pub fn try_new(
        membership: StoragePrincipalGroup,
        principal: StoragePrincipal,
    ) -> Result<Self, StorageValidationError> {
        if membership.principal_id() != principal.id() {
            return Err(StorageValidationError::invalid(
                "group membership and principal ids must match",
            ));
        }
        Ok(Self {
            membership,
            principal,
        })
    }

    #[must_use]
    pub const fn membership(&self) -> &StoragePrincipalGroup {
        &self.membership
    }

    #[must_use]
    pub const fn principal(&self) -> &StoragePrincipal {
        &self.principal
    }

    #[must_use]
    pub fn into_parts(self) -> (StoragePrincipalGroup, StoragePrincipal) {
        (self.membership, self.principal)
    }
}

/// Complete group lifecycle behavior required from a backend.
#[async_trait]
pub trait GroupStorage: Send + Sync {
    /// List groups with stable filtering, cursor pagination, and an optional
    /// exact total.
    async fn list_groups(
        &self,
        query: StorageGroupListQuery,
    ) -> Result<StoragePage<StorageIdentityGroup>, StorageError>;

    async fn get_group(&self, group_id: GroupId) -> Result<StorageIdentityGroup, StorageError>;

    async fn resolve_group_identity_scope_name(
        &self,
        group_id: GroupId,
    ) -> Result<String, StorageError>;

    async fn create_group(
        &self,
        command: StorageGroupCreate,
        context: &EventContext,
    ) -> Result<crate::StorageMutationOutcome<StorageIdentityGroup>, StorageError>;

    async fn update_group(
        &self,
        group_id: GroupId,
        update: StorageGroupUpdate,
        context: &EventContext,
    ) -> Result<crate::StorageMutationOutcome<StorageIdentityGroup>, StorageError>;

    async fn delete_group(
        &self,
        group_id: GroupId,
        context: &EventContext,
    ) -> Result<crate::StorageMutationOutcome<usize>, StorageError>;
}

/// Principal point and settings behavior required from every backend.
#[async_trait]
pub trait PrincipalStorage: Send + Sync {
    async fn get_principal(
        &self,
        principal_id: PrincipalId,
    ) -> Result<StoragePrincipal, StorageError>;

    async fn get_principal_settings(
        &self,
        principal_id: PrincipalId,
    ) -> Result<StoragePrincipalSettings, StorageError>;

    async fn update_principal_settings(
        &self,
        principal_id: PrincipalId,
        mutation: StoragePrincipalSettingsMutation,
        context: &EventContext,
    ) -> Result<crate::StorageMutationOutcome<StoragePrincipalSettings>, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use hubuum_domain::{ResourceId, ResourceRevision};

    #[test]
    fn principal_projection_rejects_success_without_an_attempt() {
        let now = Utc::now();
        let metadata = StorageRecordMetadata::try_new(
            ResourceId::new(1).unwrap(),
            now,
            now,
            ResourceRevision::INITIAL,
        )
        .unwrap();
        let error = StoragePrincipal::builder(
            metadata,
            PrincipalKind::Human,
            "principal",
            IdentityScopeId::new(1).unwrap(),
        )
        .last_sync_success_at(Some(now))
        .try_build()
        .unwrap_err();

        assert_eq!(
            error.kind(),
            crate::StorageValidationErrorKind::InvalidValue
        );
    }
}
