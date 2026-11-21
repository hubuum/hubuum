//! Same-image UUID correspondence, never inferred from a compatibility integer.
use hubuum_domain::{ClassId, CollectionId, ObjectId};
use sha2::{Digest, Sha256};

use super::*;
use crate::StorageIdentityEvidence;

#[derive(Clone, Debug)]
enum Coordinates {
    Collection(CollectionId),
    Class(ClassId, CollectionId),
    Object(ObjectId, CollectionId, ClassId),
}

/// Adapter-captured coordinates and identity from the same productive snapshot.
/// Raw representations stay inside the adapter; this value is not authorization.
#[derive(Clone)]
pub struct HistoryAuthorizationResource {
    coordinates: Coordinates,
    evidence: StorageIdentityEvidence,
}
impl HistoryAuthorizationResource {
    pub fn collection(id: CollectionId, evidence: StorageIdentityEvidence) -> Self {
        Self {
            coordinates: Coordinates::Collection(id),
            evidence,
        }
    }
    pub fn class(id: ClassId, collection: CollectionId, evidence: StorageIdentityEvidence) -> Self {
        Self {
            coordinates: Coordinates::Class(id, collection),
            evidence,
        }
    }
    pub fn object(
        id: ObjectId,
        collection: CollectionId,
        class: ClassId,
        evidence: StorageIdentityEvidence,
    ) -> Self {
        Self {
            coordinates: Coordinates::Object(id, collection, class),
            evidence,
        }
    }
    fn require_identity(&self, expected: ResourceIdentity) -> Result<(), StorageValidationError> {
        let encoded = serde_json::to_vec(&self.evidence)
            .map_err(|_| invalid("Cannot measure historical correspondence"))?;
        HistoryAuthorizationBytes::measure(encoded.len())?;
        if self.evidence.has_unknown_restore_source()
            || self
                .evidence
                .subject()
                .and_then(|s| s.record_identity())
                .is_none_or(|r| r.resource() != expected)
        {
            return Err(invalid(
                "Historical authorization correspondence or provenance is unproved",
            ));
        }
        Ok(())
    }
    fn require_reference(
        &self,
        path: &str,
        kind: StorageIdentityResourceKind,
        expected: ResourceIdentity,
    ) -> Result<(), StorageValidationError> {
        if !matches!(self.evidence.references().get(path), Some(StorageCapturedIdentity::Resource { family, reference }) if *family == kind && reference.known_identity() == Some(expected))
        {
            return Err(invalid(
                "Historical authorization endpoint correspondence is unproved",
            ));
        }
        Ok(())
    }
}
impl fmt::Debug for HistoryAuthorizationResource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HistoryAuthorizationResource")
            .finish_non_exhaustive()
    }
}

/// Validated original UUID tuple mapped to coordinates in one application image.
/// Private, non-serializable and name-free; original retained facts never change.
#[derive(Clone, Eq, PartialEq)]
pub struct HistoryObjectAuthorizationRemapping {
    object: ObjectId,
    collection: CollectionId,
    class: ClassId,
    facts_digest: [u8; 32],
    epoch: HistoryReadEpoch,
}
impl HistoryObjectAuthorizationRemapping {
    pub fn new(
        facts: &HistoryObjectAuthorizationFacts,
        epoch: HistoryReadEpoch,
        object: HistoryAuthorizationResource,
        collection: HistoryAuthorizationResource,
        class: HistoryAuthorizationResource,
    ) -> Result<Self, StorageValidationError> {
        let expected_class = facts
            .attribution()
            .class()
            .ok_or_else(|| invalid("Original class correspondence is missing"))?;
        object.require_identity(facts.target().resource())?;
        collection.require_identity(facts.attribution().collection())?;
        class.require_identity(expected_class)?;
        object.require_reference(
            "/collection_id",
            StorageIdentityResourceKind::Collection,
            facts.attribution().collection(),
        )?;
        object.require_reference(
            "/class_id",
            StorageIdentityResourceKind::Class,
            expected_class,
        )?;
        class.require_reference(
            "/collection_id",
            StorageIdentityResourceKind::Collection,
            facts.attribution().collection(),
        )?;
        let (
            Coordinates::Object(object_id, object_collection, object_class),
            Coordinates::Collection(collection_id),
            Coordinates::Class(class_id, class_collection),
        ) = (
            object.coordinates,
            collection.coordinates,
            class.coordinates,
        )
        else {
            return Err(invalid("Historical authorization resource families differ"));
        };
        if object_collection != collection_id
            || object_class != class_id
            || class_collection != collection_id
        {
            return Err(invalid(
                "Historical authorization coordinate relationships differ",
            ));
        }
        Ok(Self {
            object: object_id,
            collection: collection_id,
            class: class_id,
            facts_digest: Self::digest(facts)?,
            epoch,
        })
    }
    fn digest(facts: &HistoryObjectAuthorizationFacts) -> Result<[u8; 32], StorageValidationError> {
        let encoded = serde_json::to_vec(facts)
            .map_err(|_| invalid("Cannot bind historical authorization facts"))?;
        HistoryAuthorizationBytes::measure(encoded.len())?;
        Ok(Sha256::digest(encoded).into())
    }
    pub fn matches(
        &self,
        facts: &HistoryObjectAuthorizationFacts,
        epoch: &HistoryReadEpoch,
    ) -> bool {
        epoch == &self.epoch && Self::digest(facts).is_ok_and(|digest| digest == self.facts_digest)
    }
    pub const fn object_id(&self) -> ObjectId {
        self.object
    }
    pub const fn collection_id(&self) -> CollectionId {
        self.collection
    }
    pub const fn class_id(&self) -> ClassId {
        self.class
    }
}
impl fmt::Debug for HistoryObjectAuthorizationRemapping {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HistoryObjectAuthorizationRemapping")
            .finish_non_exhaustive()
    }
}
