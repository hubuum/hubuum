use async_trait::async_trait;
use chrono::{DateTime, Utc};
use hubuum_domain::{
    ClassId, CollectionId, HistoryRecordId, ObjectId, PrincipalId, ResourceId, ResourceRevision,
    TaskId,
};
use hubuum_query::QueryOptions;

use crate::{
    StorageClassRecord, StorageCollection, StorageError, StorageExportTemplate, StorageObject,
    StoragePage, StorageRemoteTarget,
};

/// Collection visibility applied by the adapter before counting or paging.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HistoryCollectionScope {
    All,
    Visible(Vec<CollectionId>),
}

/// Backend-neutral request for one resource's temporal history page.
#[derive(Clone)]
pub struct HistoryListQuery {
    entity_id: ResourceId,
    query_options: QueryOptions,
    collection_scope: HistoryCollectionScope,
}

impl HistoryListQuery {
    #[must_use]
    pub const fn new(
        entity_id: ResourceId,
        query_options: QueryOptions,
        collection_scope: HistoryCollectionScope,
    ) -> Self {
        Self {
            entity_id,
            query_options,
            collection_scope,
        }
    }

    #[must_use]
    pub const fn entity_id(&self) -> ResourceId {
        self.entity_id
    }

    #[must_use]
    pub const fn query_options(&self) -> &QueryOptions {
        &self.query_options
    }

    #[must_use]
    pub const fn collection_scope(&self) -> &HistoryCollectionScope {
        &self.collection_scope
    }

    #[must_use]
    pub fn into_parts(self) -> (ResourceId, QueryOptions, HistoryCollectionScope) {
        (self.entity_id, self.query_options, self.collection_scope)
    }
}

/// Backend-neutral request for one object's temporal history page.
#[derive(Clone)]
pub struct ObjectHistoryListQuery {
    object_id: ObjectId,
    class_id: ClassId,
    query_options: QueryOptions,
    collection_scope: HistoryCollectionScope,
}

impl ObjectHistoryListQuery {
    #[must_use]
    pub const fn new(
        object_id: ObjectId,
        class_id: ClassId,
        query_options: QueryOptions,
        collection_scope: HistoryCollectionScope,
    ) -> Self {
        Self {
            object_id,
            class_id,
            query_options,
            collection_scope,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (ObjectId, ClassId, QueryOptions, HistoryCollectionScope) {
        (
            self.object_id,
            self.class_id,
            self.query_options,
            self.collection_scope,
        )
    }
}

/// Point-in-time lookup shared by non-object history resources.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HistoryAsOfQuery {
    entity_id: ResourceId,
    at: DateTime<Utc>,
}

impl HistoryAsOfQuery {
    #[must_use]
    pub const fn new(entity_id: ResourceId, at: DateTime<Utc>) -> Self {
        Self { entity_id, at }
    }

    #[must_use]
    pub const fn into_parts(self) -> (ResourceId, DateTime<Utc>) {
        (self.entity_id, self.at)
    }
}

/// Point-in-time lookup for an object constrained by its class route.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObjectHistoryAsOfQuery {
    object_id: ObjectId,
    class_id: ClassId,
    at: DateTime<Utc>,
}

impl ObjectHistoryAsOfQuery {
    #[must_use]
    pub const fn new(object_id: ObjectId, class_id: ClassId, at: DateTime<Utc>) -> Self {
        Self {
            object_id,
            class_id,
            at,
        }
    }

    #[must_use]
    pub const fn into_parts(self) -> (ObjectId, ClassId, DateTime<Utc>) {
        (self.object_id, self.class_id, self.at)
    }
}

/// Backend-neutral kind of change represented by a temporal history entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageHistoryOperation {
    Create,
    Update,
    Delete,
}

impl StorageHistoryOperation {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Update => "update",
            Self::Delete => "delete",
        }
    }
}

/// Common temporal and provenance values for one history entry.
#[derive(Clone, PartialEq, Eq)]
pub struct HistoryMetadata {
    operation: StorageHistoryOperation,
    valid_from: DateTime<Utc>,
    valid_to: Option<DateTime<Utc>>,
    actor_id: Option<PrincipalId>,
    history_entry_id: HistoryRecordId,
    actor_kind: Option<String>,
    initiator_principal_id: Option<PrincipalId>,
    task_id: Option<TaskId>,
    revision: ResourceRevision,
}

/// Named temporal and provenance values used by application mappers.
pub struct HistoryMetadataParts {
    operation: StorageHistoryOperation,
    valid_from: DateTime<Utc>,
    valid_to: Option<DateTime<Utc>>,
    actor_id: Option<PrincipalId>,
    history_entry_id: HistoryRecordId,
    actor_kind: Option<String>,
    initiator_principal_id: Option<PrincipalId>,
    task_id: Option<TaskId>,
    revision: ResourceRevision,
}

impl HistoryMetadataParts {
    #[must_use]
    pub const fn operation(&self) -> StorageHistoryOperation {
        self.operation
    }

    #[must_use]
    pub const fn valid_from(&self) -> DateTime<Utc> {
        self.valid_from
    }

    #[must_use]
    pub const fn valid_to(&self) -> Option<DateTime<Utc>> {
        self.valid_to
    }

    #[must_use]
    pub const fn actor_id(&self) -> Option<PrincipalId> {
        self.actor_id
    }

    #[must_use]
    pub const fn history_entry_id(&self) -> HistoryRecordId {
        self.history_entry_id
    }

    #[must_use]
    pub fn actor_kind(&self) -> Option<&str> {
        self.actor_kind.as_deref()
    }

    #[must_use]
    pub const fn initiator_principal_id(&self) -> Option<PrincipalId> {
        self.initiator_principal_id
    }

    #[must_use]
    pub const fn task_id(&self) -> Option<TaskId> {
        self.task_id
    }

    #[must_use]
    pub const fn revision(&self) -> ResourceRevision {
        self.revision
    }
}

impl HistoryMetadata {
    pub fn try_new(
        operation: StorageHistoryOperation,
        valid_from: DateTime<Utc>,
        valid_to: Option<DateTime<Utc>>,
        history_entry_id: HistoryRecordId,
        revision: ResourceRevision,
    ) -> Result<Self, StorageError> {
        if valid_to.is_some_and(|timestamp| timestamp < valid_from) {
            return Err(StorageError::internal(
                "Persisted history valid_to must not be earlier than valid_from",
            ));
        }
        Ok(Self {
            operation,
            valid_from,
            valid_to,
            actor_id: None,
            history_entry_id,
            actor_kind: None,
            initiator_principal_id: None,
            task_id: None,
            revision,
        })
    }

    #[must_use]
    pub fn actor(mut self, actor_id: Option<PrincipalId>, actor_kind: Option<String>) -> Self {
        self.actor_id = actor_id;
        self.actor_kind = actor_kind;
        self
    }

    #[must_use]
    pub const fn initiator_principal_id(
        mut self,
        initiator_principal_id: Option<PrincipalId>,
    ) -> Self {
        self.initiator_principal_id = initiator_principal_id;
        self
    }

    #[must_use]
    pub const fn task_id(mut self, task_id: Option<TaskId>) -> Self {
        self.task_id = task_id;
        self
    }

    #[must_use]
    pub fn into_parts(self) -> HistoryMetadataParts {
        HistoryMetadataParts {
            operation: self.operation,
            valid_from: self.valid_from,
            valid_to: self.valid_to,
            actor_id: self.actor_id,
            history_entry_id: self.history_entry_id,
            actor_kind: self.actor_kind,
            initiator_principal_id: self.initiator_principal_id,
            task_id: self.task_id,
            revision: self.revision,
        }
    }
}

/// One resolved principal name used to enrich history provenance.
#[derive(Clone, PartialEq, Eq)]
pub struct HistoryPrincipalName {
    principal_id: PrincipalId,
    name: String,
}

impl HistoryPrincipalName {
    #[must_use]
    pub fn new(principal_id: PrincipalId, name: impl Into<String>) -> Self {
        Self {
            principal_id,
            name: name.into(),
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (PrincipalId, String) {
        (self.principal_id, self.name)
    }
}

/// One collection revision returned by [`HistoryStorage`].
#[derive(Clone, PartialEq, Eq)]
pub struct CollectionHistoryRecord {
    record: StorageCollection,
    metadata: HistoryMetadata,
}

impl CollectionHistoryRecord {
    #[must_use]
    pub const fn new(record: StorageCollection, metadata: HistoryMetadata) -> Self {
        Self { record, metadata }
    }

    #[must_use]
    pub fn into_parts(self) -> (StorageCollection, HistoryMetadata) {
        (self.record, self.metadata)
    }
}

/// One class revision returned by [`HistoryStorage`].
#[derive(Clone, PartialEq)]
pub struct ClassHistoryRecord {
    record: StorageClassRecord,
    metadata: HistoryMetadata,
}

impl ClassHistoryRecord {
    #[must_use]
    pub const fn new(record: StorageClassRecord, metadata: HistoryMetadata) -> Self {
        Self { record, metadata }
    }

    #[must_use]
    pub fn into_parts(self) -> (StorageClassRecord, HistoryMetadata) {
        (self.record, self.metadata)
    }
}

/// One object revision returned by [`HistoryStorage`].
#[derive(Clone, PartialEq)]
pub struct ObjectHistoryRecord {
    record: StorageObject,
    metadata: HistoryMetadata,
}

impl ObjectHistoryRecord {
    #[must_use]
    pub const fn new(record: StorageObject, metadata: HistoryMetadata) -> Self {
        Self { record, metadata }
    }

    #[must_use]
    pub fn into_parts(self) -> (StorageObject, HistoryMetadata) {
        (self.record, self.metadata)
    }
}

/// One export-template revision returned by [`HistoryStorage`].
#[derive(Clone, PartialEq)]
pub struct ExportTemplateHistoryRecord {
    record: StorageExportTemplate,
    metadata: HistoryMetadata,
}

impl ExportTemplateHistoryRecord {
    #[must_use]
    pub const fn new(record: StorageExportTemplate, metadata: HistoryMetadata) -> Self {
        Self { record, metadata }
    }

    #[must_use]
    pub fn into_parts(self) -> (StorageExportTemplate, HistoryMetadata) {
        (self.record, self.metadata)
    }
}

/// One remote-target revision returned by [`HistoryStorage`].
///
/// Deliberately does not implement `Debug`: transport templates and
/// authentication configuration may contain secrets.
#[derive(Clone, PartialEq)]
pub struct RemoteTargetHistoryRecord {
    record: StorageRemoteTarget,
    metadata: HistoryMetadata,
}

impl RemoteTargetHistoryRecord {
    #[must_use]
    pub const fn new(record: StorageRemoteTarget, metadata: HistoryMetadata) -> Self {
        Self { record, metadata }
    }

    #[must_use]
    pub fn into_parts(self) -> (StorageRemoteTarget, HistoryMetadata) {
        (self.record, self.metadata)
    }
}

/// Complete temporal-history capability required of every selectable backend.
#[async_trait]
pub trait HistoryStorage: Send + Sync {
    async fn resolve_history_principal_names(
        &self,
        principal_ids: Vec<PrincipalId>,
    ) -> Result<Vec<HistoryPrincipalName>, StorageError>;

    async fn list_collection_history(
        &self,
        query: HistoryListQuery,
    ) -> Result<StoragePage<CollectionHistoryRecord>, StorageError>;

    async fn get_collection_history_as_of(
        &self,
        query: HistoryAsOfQuery,
    ) -> Result<Option<CollectionHistoryRecord>, StorageError>;

    async fn list_class_history(
        &self,
        query: HistoryListQuery,
    ) -> Result<StoragePage<ClassHistoryRecord>, StorageError>;

    async fn get_class_history_as_of(
        &self,
        query: HistoryAsOfQuery,
    ) -> Result<Option<ClassHistoryRecord>, StorageError>;

    async fn list_object_history(
        &self,
        query: ObjectHistoryListQuery,
    ) -> Result<StoragePage<ObjectHistoryRecord>, StorageError>;

    async fn get_object_history_as_of(
        &self,
        query: ObjectHistoryAsOfQuery,
    ) -> Result<Option<ObjectHistoryRecord>, StorageError>;

    async fn list_export_template_history(
        &self,
        query: HistoryListQuery,
    ) -> Result<StoragePage<ExportTemplateHistoryRecord>, StorageError>;

    async fn get_export_template_history_as_of(
        &self,
        query: HistoryAsOfQuery,
    ) -> Result<Option<ExportTemplateHistoryRecord>, StorageError>;

    async fn list_remote_target_history(
        &self,
        query: HistoryListQuery,
    ) -> Result<StoragePage<RemoteTargetHistoryRecord>, StorageError>;

    async fn get_remote_target_history_as_of(
        &self,
        query: HistoryAsOfQuery,
    ) -> Result<Option<RemoteTargetHistoryRecord>, StorageError>;
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[test]
    fn history_queries_own_their_visibility_boundary() {
        let query = HistoryListQuery::new(
            ResourceId::new(17).unwrap(),
            QueryOptions::new(Vec::new(), Vec::new(), Some(25), None, true).unwrap(),
            HistoryCollectionScope::Visible(vec![
                CollectionId::new(3).unwrap(),
                CollectionId::new(5).unwrap(),
            ]),
        );

        let (entity_id, options, scope) = query.into_parts();
        assert_eq!(entity_id, ResourceId::new(17).unwrap());
        assert_eq!(options.limit(), Some(25));
        assert_eq!(
            scope,
            HistoryCollectionScope::Visible(vec![
                CollectionId::new(3).unwrap(),
                CollectionId::new(5).unwrap(),
            ])
        );
    }

    #[test]
    fn history_metadata_preserves_complete_provenance() {
        let valid_from = Utc.with_ymd_and_hms(2026, 8, 10, 12, 0, 0).unwrap();
        let revision = ResourceRevision::new(7).unwrap();
        let history_entry_id = HistoryRecordId::new(41).unwrap();
        let metadata = HistoryMetadata::try_new(
            StorageHistoryOperation::Update,
            valid_from,
            None,
            history_entry_id,
            revision,
        )
        .unwrap()
        .actor(PrincipalId::new(11).ok(), Some("human".to_string()))
        .initiator_principal_id(PrincipalId::new(13).ok())
        .task_id(TaskId::new(17).ok());
        let parts = metadata.into_parts();

        assert_eq!(parts.operation, StorageHistoryOperation::Update);
        assert_eq!(parts.valid_from, valid_from);
        assert_eq!(parts.valid_to, None);
        assert_eq!(parts.actor_id, PrincipalId::new(11).ok());
        assert_eq!(parts.history_entry_id, history_entry_id);
        assert_eq!(parts.actor_kind, Some("human".to_string()));
        assert_eq!(parts.initiator_principal_id, PrincipalId::new(13).ok());
        assert_eq!(parts.task_id, TaskId::new(17).ok());
        assert_eq!(parts.revision, revision);
    }

    #[test]
    fn history_metadata_rejects_reversed_validity_windows() {
        let valid_from = Utc.with_ymd_and_hms(2026, 8, 10, 12, 0, 0).unwrap();

        assert!(
            HistoryMetadata::try_new(
                StorageHistoryOperation::Update,
                valid_from,
                Some(valid_from - chrono::Duration::seconds(1)),
                HistoryRecordId::new(41).unwrap(),
                ResourceRevision::new(7).unwrap(),
            )
            .is_err()
        );
    }
}
