//! Lightweight facts for batching authorization enrichment, without policy decisions.
use hubuum_domain::{
    ClassId, ClassRelationId, CollectionId, ExportTemplateId, ObjectId, ObjectRelationId,
    RemoteTargetId,
};
use std::fmt;

/// Class facts needed to construct an authorization resource.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageAuthorizationClassResource {
    id: ClassId,
    collection_id: CollectionId,
}

impl StorageAuthorizationClassResource {
    #[must_use]
    pub const fn new(id: ClassId, collection_id: CollectionId) -> Self {
        Self { id, collection_id }
    }

    #[must_use]
    pub const fn id(&self) -> ClassId {
        self.id
    }

    #[must_use]
    pub const fn collection_id(&self) -> CollectionId {
        self.collection_id
    }
}

impl fmt::Debug for StorageAuthorizationClassResource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageAuthorizationClassResource")
            .field("id", &"[redacted]")
            .field("collection_id", &"[redacted]")
            .finish()
    }
}

/// Object facts needed to construct an authorization resource.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageAuthorizationObjectResource {
    id: ObjectId,
    collection_id: CollectionId,
    class_id: ClassId,
    name: String,
}

impl StorageAuthorizationObjectResource {
    #[must_use]
    pub fn new(
        id: ObjectId,
        collection_id: CollectionId,
        class_id: ClassId,
        name: impl Into<String>,
    ) -> Self {
        Self {
            id,
            collection_id,
            class_id,
            name: name.into(),
        }
    }

    #[must_use]
    pub const fn id(&self) -> ObjectId {
        self.id
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
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl fmt::Debug for StorageAuthorizationObjectResource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageAuthorizationObjectResource")
            .field("id", &"[redacted]")
            .field("collection_id", &"[redacted]")
            .field("class_id", &"[redacted]")
            .field("name", &"[redacted]")
            .finish()
    }
}

/// A resource identity whose current authorization facts are requested.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StorageAuthorizationResourceKey {
    Class(ClassId),
    Object(ObjectId),
    Collection(CollectionId),
    ClassRelation(ClassRelationId),
    ObjectRelation(ObjectRelationId),
    ExportTemplate(ExportTemplateId),
    RemoteTarget(RemoteTargetId),
}

/// Deduplicated identities for one authorization enrichment batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageAuthorizationResourcesQuery {
    keys: Vec<StorageAuthorizationResourceKey>,
}
impl StorageAuthorizationResourcesQuery {
    #[must_use]
    pub fn new(keys: impl IntoIterator<Item = StorageAuthorizationResourceKey>) -> Self {
        let mut keys: Vec<_> = keys.into_iter().collect();
        keys.sort_unstable();
        keys.dedup();
        Self { keys }
    }
    #[must_use]
    pub fn keys(&self) -> &[StorageAuthorizationResourceKey] {
        &self.keys
    }
}

/// Current names and ownership/endpoints needed by local and delegated policies.
/// Missing resources (including missing relation endpoints) are omitted by adapters.
/// No request payloads, template contents, remote transport settings or credentials
/// are loaded by this projection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StorageAuthorizationResource {
    Class {
        resource: StorageAuthorizationClassResource,
        name: String,
    },
    Object(StorageAuthorizationObjectResource),
    Collection {
        id: CollectionId,
        name: String,
    },
    ClassRelation {
        id: ClassRelationId,
        from: StorageAuthorizationClassResource,
        to: StorageAuthorizationClassResource,
    },
    ObjectRelation {
        id: ObjectRelationId,
        from: StorageAuthorizationObjectResource,
        to: StorageAuthorizationObjectResource,
        class_relation_id: ClassRelationId,
    },
    ExportTemplate {
        id: ExportTemplateId,
        collection_id: CollectionId,
        name: String,
    },
    RemoteTarget {
        id: RemoteTargetId,
        collection_id: CollectionId,
        name: String,
    },
}
impl StorageAuthorizationResource {
    #[must_use]
    pub fn key(&self) -> StorageAuthorizationResourceKey {
        use StorageAuthorizationResourceKey as K;
        match self {
            Self::Class { resource, .. } => K::Class(resource.id()),
            Self::Object(resource) => K::Object(resource.id()),
            Self::Collection { id, .. } => K::Collection(*id),
            Self::ClassRelation { id, .. } => K::ClassRelation(*id),
            Self::ObjectRelation { id, .. } => K::ObjectRelation(*id),
            Self::ExportTemplate { id, .. } => K::ExportTemplate(*id),
            Self::RemoteTarget { id, .. } => K::RemoteTarget(*id),
        }
    }
}
