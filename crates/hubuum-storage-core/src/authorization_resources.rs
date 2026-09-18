//! Lightweight facts for batching authorization enrichment, without policy decisions.
use crate::{StorageAuthorizationClassResource, StorageAuthorizationObjectResource};
use hubuum_domain::{
    ClassId, ClassRelationId, CollectionId, ExportTemplateId, ObjectId, ObjectRelationId,
    RemoteTargetId,
};

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
