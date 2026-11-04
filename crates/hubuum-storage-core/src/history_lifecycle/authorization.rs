//! Necessary stored policy inputs for an exact expired object version.
//! These facts do not authorize a caller and contain no object data/description.

use std::fmt;

mod remapping;
pub use remapping::*;

use hubuum_domain::{ClassId, CollectionId, ObjectId, ResourceRevision, VersionIdentity};

use super::*;
use crate::{
    StorageCapturedIdentity, StorageHistoryOperation, StorageIdentityResourceKind,
    StorageObjectHistoryRecord, StorageRecordIdentity, StorageRestoreVersionAncestry,
};

/// Retained encoded facts are separately bounded from the native payload input.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HistoryAuthorizationBytes(u32);
impl HistoryAuthorizationBytes {
    pub const MAX_BYTES: usize = 64 * 1024;
    fn measure(bytes: usize) -> Result<Self, StorageValidationError> {
        if bytes == 0 || bytes > Self::MAX_BYTES {
            return Err(invalid(
                "Retained object authorization exceeds its 64 KiB bound",
            ));
        }
        Ok(Self(bytes as u32))
    }
    pub const fn value(self) -> u32 {
        self.0
    }
}

#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryObjectAuthorizationKind {
    ClosedValue,
    Tombstone,
}

/// Original permission-relevant coordinates, extracted from a typed exact row.
/// Names belong only to protected retained facts and subsequent configured
/// historical authorization. Debug output never includes names or coordinates.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "ObjectAuthorizationInput")]
pub struct HistoryObjectAuthorizationFacts {
    target: StorageRecordIdentity,
    #[cfg_attr(feature = "openapi", schema(value_type = i32))]
    object_id: ObjectId,
    name: String,
    #[cfg_attr(feature = "openapi", schema(value_type = i32))]
    collection_id: CollectionId,
    #[cfg_attr(feature = "openapi", schema(value_type = i32))]
    class_id: ClassId,
    attribution: HistoryScopeAttribution,
    #[cfg_attr(feature = "openapi", schema(value_type = i64))]
    revision: ResourceRevision,
    read_span: HistoryExpiryReadSpan,
    kind: HistoryObjectAuthorizationKind,
    #[cfg_attr(feature = "openapi", schema(value_type = Vec<String>))]
    source_versions: Vec<VersionIdentity>,
    #[serde(skip)]
    encoded_bytes: HistoryAuthorizationBytes,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ObjectAuthorizationInput {
    target: StorageRecordIdentity,
    object_id: ObjectId,
    name: String,
    collection_id: CollectionId,
    class_id: ClassId,
    attribution: HistoryScopeAttribution,
    revision: ResourceRevision,
    read_span: HistoryExpiryReadSpan,
    kind: HistoryObjectAuthorizationKind,
    source_versions: Vec<VersionIdentity>,
}

impl TryFrom<ObjectAuthorizationInput> for HistoryObjectAuthorizationFacts {
    type Error = StorageValidationError;
    fn try_from(mut value: ObjectAuthorizationInput) -> Result<Self, Self::Error> {
        if value.name.len() > HistoryAuthorizationBytes::MAX_BYTES || value.name.contains('\0') {
            return Err(invalid("Stored object authorization name is unsupported"));
        }
        if value.attribution.class().is_none()
            || value.source_versions.len() > StorageRestoreVersionAncestry::MAX_VERSIONS
        {
            return Err(invalid(
                "Stored object authorization scope or ancestry is incomplete",
            ));
        }
        value.source_versions.sort_unstable();
        if value
            .source_versions
            .windows(2)
            .any(|pair| pair[0] == pair[1])
            || value.source_versions.contains(&value.target.version())
            || (value.target.version().as_bytes()[6] >> 4 == 8 && value.source_versions.is_empty())
        {
            return Err(invalid(
                "Stored object authorization has contradictory or unknown ancestry",
            ));
        }
        let expected_span = match value.kind {
            HistoryObjectAuthorizationKind::ClosedValue => HistoryExpiryReadSpan::closed(
                value.read_span.valid_from(),
                value.read_span.valid_to().ok_or_else(|| {
                    invalid("Object value authorization requires its original closure")
                })?,
            )?,
            HistoryObjectAuthorizationKind::Tombstone => {
                if value.read_span.valid_to().is_some() {
                    return Err(invalid(
                        "Bounded tombstone authorization requires explicit interval support",
                    ));
                }
                HistoryExpiryReadSpan::deleted(value.read_span.valid_from())
            }
        };
        if value.read_span != expected_span {
            return Err(invalid(
                "Stored object authorization differs from its original version state",
            ));
        }
        let encoded_bytes = HistoryAuthorizationBytes::measure(
            serde_json::to_vec(&value)
                .map_err(|_| invalid("Cannot encode retained object authorization"))?
                .len(),
        )?;
        Ok(Self {
            target: value.target,
            object_id: value.object_id,
            name: value.name,
            collection_id: value.collection_id,
            class_id: value.class_id,
            attribution: value.attribution,
            revision: value.revision,
            read_span: value.read_span,
            kind: value.kind,
            source_versions: value.source_versions,
            encoded_bytes,
        })
    }
}

impl fmt::Debug for HistoryObjectAuthorizationFacts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HistoryObjectAuthorizationFacts")
            .field("kind", &self.kind)
            .field("encoded_bytes", &self.encoded_bytes)
            .field("restore_source_count", &self.source_versions.len())
            .finish_non_exhaustive()
    }
}

impl HistoryObjectAuthorizationFacts {
    /// Capture the original stored snapshot once. The adapter must bind this
    /// exact row to its claimed native version; these facts are not permission.
    pub fn capture(row: StorageObjectHistoryRecord) -> Result<Self, StorageValidationError> {
        let (record, metadata) = row.into_parts();
        let evidence = metadata
            .logical_identity()
            .ok_or_else(|| invalid("Object authorization requires original identity evidence"))?;
        if evidence.has_unknown_restore_source() {
            return Err(invalid(
                "Object authorization requires proven restore ancestry",
            ));
        }
        let target = evidence
            .subject()
            .and_then(|subject| subject.record_identity())
            .ok_or_else(|| {
                invalid("Object authorization requires its original resource and version")
            })?;
        let reference =
            |path: &str, expected| -> Result<ResourceIdentity, StorageValidationError> {
                match evidence.references().get(path) {
                    Some(StorageCapturedIdentity::Resource { family, reference })
                        if *family == expected =>
                    {
                        reference
                            .known_identity()
                            .ok_or_else(|| invalid("Original object scope is unresolved"))
                    }
                    _ => Err(invalid("Original object scope is missing or contradictory")),
                }
            };
        let attribution = HistoryScopeAttribution::new(
            reference("/collection_id", StorageIdentityResourceKind::Collection)?,
            Some(reference("/class_id", StorageIdentityResourceKind::Class)?),
        );
        let (kind, read_span) = match metadata.operation() {
            StorageHistoryOperation::Create | StorageHistoryOperation::Update => (
                HistoryObjectAuthorizationKind::ClosedValue,
                HistoryExpiryReadSpan::closed(
                    metadata.valid_from(),
                    metadata.valid_to().ok_or_else(|| {
                        invalid("Open object versions cannot acquire expiry authorization")
                    })?,
                )?,
            ),
            StorageHistoryOperation::Delete => {
                if metadata.valid_to().is_some() {
                    return Err(invalid(
                        "Bounded tombstone authorization requires explicit interval support",
                    ));
                }
                (
                    HistoryObjectAuthorizationKind::Tombstone,
                    HistoryExpiryReadSpan::deleted(metadata.valid_from()),
                )
            }
        };
        if record.name().len() > HistoryAuthorizationBytes::MAX_BYTES {
            return Err(invalid(
                "Retained object authorization exceeds its 64 KiB bound",
            ));
        }
        ObjectAuthorizationInput {
            target,
            object_id: record.id(),
            name: record.name().to_owned(),
            collection_id: record.collection_id(),
            class_id: record.class_id(),
            attribution,
            revision: record.revision(),
            read_span,
            kind,
            source_versions: evidence.restore_source_versions().iter().collect(),
        }
        .try_into()
    }

    pub const fn target(&self) -> StorageRecordIdentity {
        self.target
    }
    pub const fn object_id(&self) -> ObjectId {
        self.object_id
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub const fn collection_id(&self) -> CollectionId {
        self.collection_id
    }
    pub const fn class_id(&self) -> ClassId {
        self.class_id
    }
    pub const fn attribution(&self) -> HistoryScopeAttribution {
        self.attribution
    }
    pub const fn revision(&self) -> ResourceRevision {
        self.revision
    }
    pub const fn read_span(&self) -> &HistoryExpiryReadSpan {
        &self.read_span
    }
    pub const fn kind(&self) -> HistoryObjectAuthorizationKind {
        self.kind
    }
    pub fn source_versions(&self) -> &[VersionIdentity] {
        &self.source_versions
    }
    pub const fn encoded_bytes(&self) -> HistoryAuthorizationBytes {
        self.encoded_bytes
    }
}

/// One retained population gap, bound to the original productive read epoch.
/// This is neither caller authorization nor permission to expose absence. The
/// adapter must establish exact retained membership under history authority;
/// the service must validate original correspondence and configured policy.
#[derive(Clone, Eq, PartialEq)]
pub struct HistoryObjectPopulationGap {
    intent: uuid::Uuid,
    facts: HistoryObjectAuthorizationFacts,
    class: ResourceIdentity,
    at: DateTime<Utc>,
    epoch: HistoryReadEpoch,
    selected_class: ClassId,
    remapping: Option<HistoryObjectAuthorizationRemapping>,
}

impl HistoryObjectPopulationGap {
    pub const MAX_CANDIDATES: usize = 1000;
    pub const MAX_FACT_BYTES: usize = 1024 * 1024;

    /// Bounded retained authority candidates, before current visibility and
    /// configured authorization. Missing old facts cannot establish absence.
    pub fn for_population(
        journal: &HistoryExpiryJournal,
        query: &crate::StorageClassObjectsAsOfQuery,
        epoch: HistoryReadEpoch,
    ) -> Result<Vec<Self>, StorageValidationError> {
        let class = query.expected_resource().ok_or_else(|| {
            invalid("Population gap requires the original logical class identity")
        })?;
        let mut gaps = Vec::new();
        let mut bytes = 0;
        for intent in journal.intents() {
            for member in intent.manifest().members() {
                if member.family() != StorageIdentityHistoryKind::Object
                    || !matches!(member.anchor(), HistoryRetentionAnchor::Closed(_))
                    || !member.read_span().contains(query.at())
                {
                    continue;
                }
                let facts = member.object_authorization().ok_or_else(|| {
                    invalid("Retained object interval lacks original population facts")
                })?;
                if facts.attribution().class() != Some(class) {
                    continue;
                }
                let next_bytes = bytes + facts.encoded_bytes().value() as usize;
                if gaps.len() == Self::MAX_CANDIDATES || next_bytes > Self::MAX_FACT_BYTES {
                    return Err(invalid(
                        "Retained population candidates exceed the read bound",
                    ));
                }
                gaps.push(Self::from_selection(
                    intent,
                    member.target(),
                    query,
                    epoch.clone(),
                    true,
                )?);
                bytes = next_bytes;
            }
        }
        Ok(gaps)
    }

    /// Adapter boundary after validating the original read context, retained
    /// authority and canonical class correspondence in the same snapshot.
    /// Unsupported filters fail closed rather than inventing matching facts.
    pub fn from_retained_member(
        intent: &HistoryExpiryIntent,
        target: StorageRecordIdentity,
        query: &crate::StorageClassObjectsAsOfQuery,
        epoch: HistoryReadEpoch,
    ) -> Result<Self, StorageValidationError> {
        Self::from_selection(intent, target, query, epoch, false)
    }

    fn from_selection(
        intent: &HistoryExpiryIntent,
        target: StorageRecordIdentity,
        query: &crate::StorageClassObjectsAsOfQuery,
        epoch: HistoryReadEpoch,
        resolve_coordinates: bool,
    ) -> Result<Self, StorageValidationError> {
        let class = query.expected_resource().ok_or_else(|| {
            invalid("Population gap requires the original logical class identity")
        })?;
        if !query.options().filters().is_empty() {
            return Err(invalid(
                "Retained population gap does not prove filtered selection",
            ));
        }
        let member = intent
            .manifest()
            .members()
            .iter()
            .find(|member| {
                member.family() == StorageIdentityHistoryKind::Object && member.target() == target
            })
            .ok_or_else(|| invalid("Population gap lacks exact retained membership"))?;
        let facts = member.object_authorization().ok_or_else(|| {
            invalid("Population gap lacks original historical authorization facts")
        })?;
        if facts.kind() != HistoryObjectAuthorizationKind::ClosedValue
            || facts.attribution().class() != Some(class)
            || !facts.read_span().contains(query.at())
            || (!resolve_coordinates
                && (facts.class_id() != query.class_id()
                    || matches!(query.collection_scope(), crate::StorageHistoryCollectionScope::Visible(ids)
                    if !ids.contains(&facts.collection_id()))))
        {
            return Err(invalid(
                "Retained population gap differs from the original selection",
            ));
        }
        Ok(Self {
            intent: intent.identity(),
            facts: facts.clone(),
            class,
            at: query.at(),
            epoch,
            selected_class: query.class_id(),
            remapping: None,
        })
    }

    pub fn matches_query(&self, query: &crate::StorageClassObjectsAsOfQuery) -> bool {
        query.expected_resource() == Some(self.class)
            && query.class_id() == self.selected_class
            && self.remapping.as_ref().map_or_else(
                || self.selected_class == self.facts.class_id(),
                |proof| {
                    proof.class_id() == self.selected_class
                        && proof.matches(&self.facts, &self.epoch)
                },
            )
            && query.at() == self.at
            && query.options().filters().is_empty()
            && !matches!(query.collection_scope(), crate::StorageHistoryCollectionScope::Visible(ids)
                if !ids.contains(&self.remapping.as_ref().map_or(self.facts.collection_id(), |proof| proof.collection_id())))
    }

    /// Attach only adapter-validated correspondence from the same productive snapshot.
    pub fn with_remapping(
        mut self,
        proof: HistoryObjectAuthorizationRemapping,
    ) -> Result<Self, StorageValidationError> {
        if !proof.matches(&self.facts, &self.epoch) || proof.class_id() != self.selected_class {
            return Err(invalid(
                "Population remapping differs from the original selection",
            ));
        }
        self.remapping = Some(proof);
        Ok(self)
    }
    pub const fn remapping(&self) -> Option<&HistoryObjectAuthorizationRemapping> {
        self.remapping.as_ref()
    }

    pub const fn intent(&self) -> uuid::Uuid {
        self.intent
    }
    pub const fn facts(&self) -> &HistoryObjectAuthorizationFacts {
        &self.facts
    }
    pub const fn epoch(&self) -> &HistoryReadEpoch {
        &self.epoch
    }
}

impl fmt::Debug for HistoryObjectPopulationGap {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HistoryObjectPopulationGap")
            .field("encoded_fact_bytes", &self.facts.encoded_bytes())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests;
