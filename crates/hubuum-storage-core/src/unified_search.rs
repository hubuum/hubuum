use std::fmt;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use hubuum_domain::{ClassId, CollectionId, ObjectId, PrincipalId, ResourceId, ResourceRevision};
use serde_json::Value;

use crate::{
    StorageAuthorizationPermission, StorageCandidatePage, StorageCandidatePageLimit,
    StorageClassSchemaPolicy, StorageError, StorageRecordMetadata, StorageValidationError,
};

/// Normalized collection, class, and object boundary for a scoped search.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageResourceScope {
    collection_ids: Vec<CollectionId>,
    class_ids: Vec<ClassId>,
    object_ids: Vec<ObjectId>,
}

impl StorageResourceScope {
    #[must_use]
    pub fn new(
        collection_ids: impl IntoIterator<Item = CollectionId>,
        class_ids: impl IntoIterator<Item = ClassId>,
        object_ids: impl IntoIterator<Item = ObjectId>,
    ) -> Self {
        Self {
            collection_ids: normalized_ids(collection_ids),
            class_ids: normalized_ids(class_ids),
            object_ids: normalized_ids(object_ids),
        }
    }

    #[must_use]
    pub fn collection_ids(&self) -> &[CollectionId] {
        &self.collection_ids
    }

    #[must_use]
    pub fn class_ids(&self) -> &[ClassId] {
        &self.class_ids
    }

    #[must_use]
    pub fn object_ids(&self) -> &[ObjectId] {
        &self.object_ids
    }
}

impl fmt::Debug for StorageResourceScope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageResourceScope")
            .field("collection_count", &self.collection_ids.len())
            .field("class_count", &self.class_ids.len())
            .field("object_count", &self.object_ids.len())
            .finish()
    }
}

fn normalized_ids<T: Ord>(ids: impl IntoIterator<Item = T>) -> Vec<T> {
    let mut ids = ids.into_iter().collect::<Vec<_>>();
    ids.sort_unstable();
    ids.dedup();
    ids
}

/// Principal and token boundaries which the adapter must apply before paging.
///
/// An absent permission or resource dimension is unrestricted. A present but
/// empty permission dimension denies every permission and therefore fails
/// closed for all search kinds.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageVisibility {
    principal_id: PrincipalId,
    is_admin: bool,
    permissions: Option<Vec<StorageAuthorizationPermission>>,
    resources: Option<StorageResourceScope>,
}

impl StorageVisibility {
    #[must_use]
    pub fn new(
        principal_id: PrincipalId,
        is_admin: bool,
        permissions: Option<impl IntoIterator<Item = StorageAuthorizationPermission>>,
        resources: Option<StorageResourceScope>,
    ) -> Self {
        let permissions = permissions.map(|permissions| {
            let mut permissions = permissions.into_iter().collect::<Vec<_>>();
            permissions.sort_unstable();
            permissions.dedup();
            permissions
        });
        Self {
            principal_id,
            is_admin,
            permissions,
            resources,
        }
    }

    #[must_use]
    pub const fn principal_id(&self) -> PrincipalId {
        self.principal_id
    }

    #[must_use]
    pub const fn is_admin(&self) -> bool {
        self.is_admin
    }

    #[must_use]
    pub fn permissions(&self) -> Option<&[StorageAuthorizationPermission]> {
        self.permissions.as_deref()
    }

    #[must_use]
    pub const fn resources(&self) -> Option<&StorageResourceScope> {
        self.resources.as_ref()
    }

    #[must_use]
    pub fn allows_permissions(&self, required: &[StorageAuthorizationPermission]) -> bool {
        self.permissions.as_ref().is_none_or(|allowed| {
            required
                .iter()
                .all(|permission| allowed.contains(permission))
        })
    }
}

impl fmt::Debug for StorageVisibility {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageVisibility")
            .field("principal_id", &"[redacted]")
            .field("is_admin", &self.is_admin)
            .field("permission_count", &self.permissions.as_ref().map(Vec::len))
            .field("resources", &self.resources)
            .finish()
    }
}

/// Backend-neutral decoded cursor for one ranked search kind.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageUnifiedSearchCursor {
    rank: i32,
    normalized_name: String,
    id: ResourceId,
}

impl StorageUnifiedSearchCursor {
    #[must_use]
    pub fn new(rank: i32, normalized_name: impl Into<String>, id: ResourceId) -> Self {
        Self {
            rank,
            normalized_name: normalized_name.into(),
            id,
        }
    }

    #[must_use]
    pub const fn rank(&self) -> i32 {
        self.rank
    }

    #[must_use]
    pub fn normalized_name(&self) -> &str {
        &self.normalized_name
    }

    #[must_use]
    pub const fn id(&self) -> ResourceId {
        self.id
    }
}

impl fmt::Debug for StorageUnifiedSearchCursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageUnifiedSearchCursor")
            .field("rank", &self.rank)
            .field("normalized_name", &"[redacted]")
            .field("id", &"[redacted]")
            .finish()
    }
}

/// One operation-shaped ranked search request.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageUnifiedSearchQuery {
    search_term: String,
    page_limit: StorageCandidatePageLimit,
    search_extended_document: bool,
    cursor: Option<StorageUnifiedSearchCursor>,
    visibility: StorageVisibility,
}

/// One unified-search row paired with the adapter-owned stable cursor that
/// follows the database's exact ranking and normalization semantics.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageUnifiedSearchCandidate<T> {
    item: T,
    cursor: StorageUnifiedSearchCursor,
}

impl<T> StorageUnifiedSearchCandidate<T> {
    #[must_use]
    pub const fn new(item: T, cursor: StorageUnifiedSearchCursor) -> Self {
        Self { item, cursor }
    }

    #[must_use]
    pub const fn item(&self) -> &T {
        &self.item
    }

    #[must_use]
    pub const fn cursor(&self) -> &StorageUnifiedSearchCursor {
        &self.cursor
    }

    #[must_use]
    pub fn into_parts(self) -> (T, StorageUnifiedSearchCursor) {
        (self.item, self.cursor)
    }
}

impl StorageUnifiedSearchQuery {
    #[must_use]
    pub fn new(
        search_term: impl Into<String>,
        page_limit: StorageCandidatePageLimit,
        visibility: StorageVisibility,
    ) -> Self {
        Self {
            search_term: search_term.into(),
            page_limit,
            search_extended_document: false,
            cursor: None,
            visibility,
        }
    }

    #[must_use]
    pub const fn search_extended_document(mut self, enabled: bool) -> Self {
        self.search_extended_document = enabled;
        self
    }

    #[must_use]
    pub fn cursor(mut self, cursor: Option<StorageUnifiedSearchCursor>) -> Self {
        self.cursor = cursor;
        self
    }

    #[must_use]
    pub fn search_term(&self) -> &str {
        &self.search_term
    }

    #[must_use]
    pub const fn page_limit(&self) -> StorageCandidatePageLimit {
        self.page_limit
    }

    #[must_use]
    pub const fn searches_extended_document(&self) -> bool {
        self.search_extended_document
    }

    #[must_use]
    pub const fn search_cursor(&self) -> Option<&StorageUnifiedSearchCursor> {
        self.cursor.as_ref()
    }

    #[must_use]
    pub const fn visibility(&self) -> &StorageVisibility {
        &self.visibility
    }
}

impl fmt::Debug for StorageUnifiedSearchQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageUnifiedSearchQuery")
            .field("search_term", &"[redacted]")
            .field("page_limit", &self.page_limit)
            .field("search_extended_document", &self.search_extended_document)
            .field("cursor", &self.cursor)
            .field("visibility", &self.visibility)
            .finish()
    }
}

/// Canonical flat collection projection shared by storage capabilities.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageCollection {
    id: CollectionId,
    name: String,
    description: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    parent_collection_id: Option<CollectionId>,
    revision: ResourceRevision,
}

impl fmt::Debug for StorageCollection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageCollection")
            .field("id", &self.id)
            .field("name", &"[redacted]")
            .field("description", &"[redacted]")
            .field("created_at", &self.created_at)
            .field("updated_at", &self.updated_at)
            .field("parent_collection_id", &self.parent_collection_id)
            .field("revision", &self.revision)
            .finish()
    }
}

impl StorageCollection {
    pub fn try_new(
        metadata: StorageRecordMetadata,
        name: impl Into<String>,
        description: impl Into<String>,
        parent_collection_id: Option<CollectionId>,
    ) -> Result<Self, StorageValidationError> {
        let id = CollectionId::from(metadata.id());
        if parent_collection_id == Some(id) {
            return Err(StorageValidationError::invalid(
                "collection must not be its own parent",
            ));
        }
        Ok(Self {
            id,
            name: name.into(),
            description: description.into(),
            created_at: metadata.created_at(),
            updated_at: metadata.updated_at(),
            parent_collection_id,
            revision: metadata.revision(),
        })
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        CollectionId,
        String,
        String,
        DateTime<Utc>,
        DateTime<Utc>,
        Option<CollectionId>,
        ResourceRevision,
    ) {
        (
            self.id,
            self.name,
            self.description,
            self.created_at,
            self.updated_at,
            self.parent_collection_id,
            self.revision,
        )
    }

    #[must_use]
    pub const fn id(&self) -> CollectionId {
        self.id
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn description(&self) -> &str {
        &self.description
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
    pub const fn revision(&self) -> ResourceRevision {
        self.revision
    }

    #[must_use]
    pub const fn parent_collection_id(&self) -> Option<CollectionId> {
        self.parent_collection_id
    }

    /// Return the canonical collection snapshot stored in audit documents.
    ///
    /// Adapters should construct collection events from this projection rather
    /// than serializing native persistence rows. Timestamps intentionally use
    /// the existing UTC-naive wire representation of durable audit snapshots.
    #[must_use]
    pub fn audit_snapshot(&self) -> Value {
        serde_json::json!({
            "id": self.id.id(),
            "name": self.name,
            "description": self.description,
            "created_at": self.created_at.naive_utc(),
            "updated_at": self.updated_at.naive_utc(),
            "parent_collection_id": self.parent_collection_id.map(CollectionId::id),
            "revision": self.revision.get(),
        })
    }
}

/// Expanded class projection used by catalog and unified-search reads.
#[derive(Clone, PartialEq)]
pub struct StorageClassWithCollection {
    id: ClassId,
    name: String,
    collection: StorageCollection,
    schema_policy: StorageClassSchemaPolicy,
    description: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    revision: ResourceRevision,
}

impl fmt::Debug for StorageClassWithCollection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageClassWithCollection")
            .field("id", &self.id)
            .field("name", &"[redacted]")
            .field("collection", &self.collection)
            .field("json_schema", &"[redacted]")
            .field("validate_schema", &self.schema_policy.validates_schema())
            .field("description", &"[redacted]")
            .field("created_at", &self.created_at)
            .field("updated_at", &self.updated_at)
            .field("revision", &self.revision)
            .finish()
    }
}

impl StorageClassWithCollection {
    #[must_use]
    pub fn builder(
        metadata: StorageRecordMetadata,
        name: impl Into<String>,
        collection: StorageCollection,
        description: impl Into<String>,
    ) -> StorageClassWithCollectionBuilder {
        StorageClassWithCollectionBuilder {
            metadata,
            name: name.into(),
            collection,
            description: description.into(),
            schema_policy: StorageClassSchemaPolicy::Absent,
        }
    }

    #[must_use]
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        ClassId,
        String,
        StorageCollection,
        Option<Value>,
        bool,
        String,
        DateTime<Utc>,
        DateTime<Utc>,
        ResourceRevision,
    ) {
        let (json_schema, validate_schema) = self.schema_policy.into_parts();
        (
            self.id,
            self.name,
            self.collection,
            json_schema,
            validate_schema,
            self.description,
            self.created_at,
            self.updated_at,
            self.revision,
        )
    }

    #[must_use]
    pub const fn id(&self) -> ClassId {
        self.id
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub const fn collection(&self) -> &StorageCollection {
        &self.collection
    }
}

pub struct StorageClassWithCollectionBuilder {
    metadata: StorageRecordMetadata,
    name: String,
    collection: StorageCollection,
    description: String,
    schema_policy: StorageClassSchemaPolicy,
}

impl StorageClassWithCollectionBuilder {
    #[must_use]
    pub fn schema_policy(mut self, schema_policy: StorageClassSchemaPolicy) -> Self {
        self.schema_policy = schema_policy;
        self
    }

    #[must_use]
    pub fn build(self) -> StorageClassWithCollection {
        StorageClassWithCollection {
            id: ClassId::from(self.metadata.id()),
            name: self.name,
            collection: self.collection,
            schema_policy: self.schema_policy,
            description: self.description,
            created_at: self.metadata.created_at(),
            updated_at: self.metadata.updated_at(),
            revision: self.metadata.revision(),
        }
    }
}

/// Canonical flat object projection shared by storage capabilities.
#[derive(Clone, PartialEq)]
pub struct StorageObject {
    id: ObjectId,
    name: String,
    collection_id: CollectionId,
    class_id: ClassId,
    data: Value,
    description: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    revision: ResourceRevision,
}

impl fmt::Debug for StorageObject {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageObject")
            .field("id", &self.id)
            .field("name", &"[redacted]")
            .field("collection_id", &self.collection_id)
            .field("class_id", &self.class_id)
            .field("data", &"[redacted]")
            .field("description", &"[redacted]")
            .field("created_at", &self.created_at)
            .field("updated_at", &self.updated_at)
            .field("revision", &self.revision)
            .finish()
    }
}

impl StorageObject {
    #[must_use]
    pub fn new(
        metadata: StorageRecordMetadata,
        name: impl Into<String>,
        collection_id: CollectionId,
        class_id: ClassId,
        data: Value,
        description: impl Into<String>,
    ) -> Self {
        Self {
            id: ObjectId::from(metadata.id()),
            name: name.into(),
            collection_id,
            class_id,
            data,
            description: description.into(),
            created_at: metadata.created_at(),
            updated_at: metadata.updated_at(),
            revision: metadata.revision(),
        }
    }

    #[must_use]
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        ObjectId,
        String,
        CollectionId,
        ClassId,
        Value,
        String,
        DateTime<Utc>,
        DateTime<Utc>,
        ResourceRevision,
    ) {
        (
            self.id,
            self.name,
            self.collection_id,
            self.class_id,
            self.data,
            self.description,
            self.created_at,
            self.updated_at,
            self.revision,
        )
    }

    #[must_use]
    pub const fn id(&self) -> ObjectId {
        self.id
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub const fn collection_id(&self) -> CollectionId {
        self.collection_id
    }

    #[must_use]
    pub const fn class_id(&self) -> ClassId {
        self.class_id
    }

    #[must_use]
    pub const fn data(&self) -> &Value {
        &self.data
    }

    #[must_use]
    pub fn description(&self) -> &str {
        &self.description
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
    pub const fn revision(&self) -> ResourceRevision {
        self.revision
    }

    /// Return the canonical object snapshot stored in audit documents.
    #[must_use]
    pub fn audit_snapshot(&self) -> Value {
        serde_json::json!({
            "id": self.id.id(),
            "name": self.name,
            "collection_id": self.collection_id.id(),
            "hubuum_class_id": self.class_id.id(),
            "data": self.data,
            "description": self.description,
            "created_at": self.created_at.naive_utc(),
            "updated_at": self.updated_at.naive_utc(),
            "revision": self.revision.get(),
        })
    }
}

/// Mandatory backend contract for the three ranked unified-search projections.
#[async_trait]
pub trait UnifiedSearchStorage: Send + Sync {
    async fn search_collections(
        &self,
        query: StorageUnifiedSearchQuery,
    ) -> Result<StorageCandidatePage<StorageUnifiedSearchCandidate<StorageCollection>>, StorageError>;

    async fn search_classes(
        &self,
        query: StorageUnifiedSearchQuery,
    ) -> Result<
        StorageCandidatePage<StorageUnifiedSearchCandidate<StorageClassWithCollection>>,
        StorageError,
    >;

    async fn search_objects(
        &self,
        query: StorageUnifiedSearchQuery,
    ) -> Result<StorageCandidatePage<StorageUnifiedSearchCandidate<StorageObject>>, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scoped_permissions_fail_closed() {
        let visibility = StorageVisibility::new(
            PrincipalId::new(42).unwrap(),
            true,
            Some([StorageAuthorizationPermission::ReadCollection]),
            None,
        );

        assert!(visibility.allows_permissions(&[StorageAuthorizationPermission::ReadCollection]));
        assert!(!visibility.allows_permissions(&[
            StorageAuthorizationPermission::ReadCollection,
            StorageAuthorizationPermission::ReadObject,
        ]));
    }

    #[test]
    fn resource_scope_normalizes_identifiers() {
        let scope = StorageResourceScope::new(
            [3, 1, 3].map(|id| CollectionId::new(id).unwrap()),
            [8, 4, 8].map(|id| ClassId::new(id).unwrap()),
            [7, 2, 7].map(|id| ObjectId::new(id).unwrap()),
        );

        assert_eq!(
            scope.collection_ids(),
            &[CollectionId::new(1).unwrap(), CollectionId::new(3).unwrap()]
        );
        assert_eq!(
            scope.class_ids(),
            &[ClassId::new(4).unwrap(), ClassId::new(8).unwrap()]
        );
        assert_eq!(
            scope.object_ids(),
            &[ObjectId::new(2).unwrap(), ObjectId::new(7).unwrap()]
        );
    }

    #[test]
    fn collection_audit_snapshot_has_backend_independent_shape() {
        let created_at = "2026-08-30T12:00:00Z".parse().unwrap();
        let updated_at = "2026-08-30T12:01:00Z".parse().unwrap();
        let metadata = StorageRecordMetadata::try_new(
            ResourceId::new(7).unwrap(),
            created_at,
            updated_at,
            ResourceRevision::new(3).unwrap(),
        )
        .unwrap();
        let collection = StorageCollection::try_new(
            metadata,
            "portable",
            "canonical",
            Some(CollectionId::new(2).unwrap()),
        )
        .unwrap();

        assert_eq!(
            collection.audit_snapshot(),
            serde_json::json!({
                "id": 7,
                "name": "portable",
                "description": "canonical",
                "created_at": "2026-08-30T12:00:00",
                "updated_at": "2026-08-30T12:01:00",
                "parent_collection_id": 2,
                "revision": 3,
            })
        );
    }

    #[test]
    fn debug_output_redacts_search_and_principal_values() {
        let visibility = StorageVisibility::new(
            PrincipalId::new(42).unwrap(),
            false,
            None::<[StorageAuthorizationPermission; 0]>,
            None,
        );
        let query = StorageUnifiedSearchQuery::new(
            "secret asset",
            StorageCandidatePageLimit::try_new(10).unwrap(),
            visibility,
        )
        .cursor(Some(StorageUnifiedSearchCursor::new(
            2,
            "secret asset",
            ResourceId::new(99).unwrap(),
        )));

        let debug = format!("{query:?}");
        assert!(!debug.contains("secret asset"));
        assert!(!debug.contains("42"));
        assert!(!debug.contains("99"));
    }
}
