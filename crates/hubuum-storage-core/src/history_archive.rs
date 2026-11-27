//! Collection-only archive query snapshots. These catalog values are not lifecycle
//! authorization proofs. Public task/API activation and source retirement are off.
use std::collections::BTreeSet;
use std::ops::Range;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use hubuum_budget::{ByteLimit, LimitedWriter};
use hubuum_domain::{
    CollectionId, PrincipalIdentity, ResourceId, ResourceIdentity, ResourceRevision,
    VersionIdentity,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::artifacts::ArtifactReference;
use crate::{
    ApplicationFence, StorageBackupHistorySection, StorageCollection, StorageError,
    StorageExecutionPermit, StorageIdentityEvidence, StorageRecordIdentity, StorageRecordMetadata,
};
use execution::HistoryArchiveExecutionBinding;

/// The combined adapter supports original publication/producer ownership for
/// both protocols. Production must still qualify every producer before enabling
/// online expiry; this compatibility fact does not authorize physical deletion.
pub const HISTORY_ARCHIVE_REQUIRES_LEGACY_RUNTIME: bool = false;
pub const HISTORY_ARCHIVE_ROW_BYTES: usize = 4 * 1024 * 1024;
pub const HISTORY_ARCHIVE_CATALOG_BYTES: usize = 4 * 1024 * 1024;
pub const HISTORY_ARCHIVE_MAX_VERSIONS: usize = 10_000;
pub const HISTORY_ARCHIVE_BATCH_ROWS: usize = 1_000;
pub const HISTORY_ARCHIVE_BATCH_BYTES: usize = 16 * 1024 * 1024;
pub const HISTORY_ARCHIVE_CAPTURE_BYTES: usize = 5 * 1024 * 1024;
pub const HISTORY_ARCHIVE_MAX_CAPTURES: usize = 1_024;
pub const HISTORY_ARCHIVE_STAGING_BYTES: usize = 1024 * 1024 * 1024;
pub const HISTORY_ARCHIVE_READ_BYTES: u64 = 64 * 1024 * 1024;

fn invalid(message: &str) -> StorageError {
    StorageError::invalid_input(message)
}

pub fn history_archive_encoded_bytes(
    value: &impl Serialize,
    limit: usize,
) -> Result<Vec<u8>, StorageError> {
    let mut writer = LimitedWriter::new(Vec::new(), ByteLimit::new(limit));
    let encoded = serde_json::to_writer(&mut writer, value);
    if writer.budget().exceeded() {
        return Err(StorageError::input_too_large(
            "History archive encoded input exceeds budget",
        ));
    }
    encoded.map_err(|_| invalid("History archive encoding failed"))?;
    Ok(writer.into_inner())
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "i64", into = "i64")]
pub struct HistoryArchiveCatalogGeneration(i64);
impl TryFrom<i64> for HistoryArchiveCatalogGeneration {
    type Error = StorageError;
    fn try_from(value: i64) -> Result<Self, Self::Error> {
        if value < 0 {
            return Err(invalid("Catalog generation must not be negative"));
        }
        Ok(Self(value))
    }
}
impl From<HistoryArchiveCatalogGeneration> for i64 {
    fn from(value: HistoryArchiveCatalogGeneration) -> Self {
        value.0
    }
}
impl HistoryArchiveCatalogGeneration {
    pub const fn value(self) -> i64 {
        self.0
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum HistoryArchivePoint {
    Exact { version: VersionIdentity },
    AsOf { at: DateTime<Utc> },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryArchiveSelection {
    family: StorageBackupHistorySection,
    resource: ResourceIdentity,
    point: HistoryArchivePoint,
}
impl HistoryArchiveSelection {
    pub const fn new(
        family: StorageBackupHistorySection,
        resource: ResourceIdentity,
        point: HistoryArchivePoint,
    ) -> Self {
        Self {
            family,
            resource,
            point,
        }
    }
    pub const fn family(&self) -> StorageBackupHistorySection {
        self.family
    }
    pub const fn resource(&self) -> ResourceIdentity {
        self.resource
    }
    pub const fn point(&self) -> &HistoryArchivePoint {
        &self.point
    }
    fn covers_descriptor(&self, entry: &HistoryArchiveDescriptor) -> bool {
        self.matches_interval(entry.identity, entry.valid_from, Some(entry.valid_to))
    }

    pub fn matches(&self, row: &HistoryArchiveRow) -> bool {
        self.matches_interval(row.identity, row.valid_from, row.valid_to)
    }

    fn matches_interval(
        &self,
        identity: StorageRecordIdentity,
        from: DateTime<Utc>,
        to: Option<DateTime<Utc>>,
    ) -> bool {
        self.resource == identity.resource()
            && match self.point {
                HistoryArchivePoint::Exact { version } => identity.version() == version,
                HistoryArchivePoint::AsOf { at } => from <= at && to.is_none_or(|end| at < end),
            }
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "RowInput")]
pub struct HistoryArchiveRow {
    identity: StorageRecordIdentity,
    valid_from: DateTime<Utc>,
    valid_to: Option<DateTime<Utc>>,
    payload: Value,
    #[serde(skip)]
    collection: StorageCollection,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RowInput {
    identity: StorageRecordIdentity,
    valid_from: DateTime<Utc>,
    valid_to: Option<DateTime<Utc>>,
    payload: Value,
}
impl TryFrom<RowInput> for HistoryArchiveRow {
    type Error = StorageError;
    fn try_from(value: RowInput) -> Result<Self, Self::Error> {
        Self::new(
            value.identity,
            value.valid_from,
            value.valid_to,
            value.payload,
        )
    }
}
impl HistoryArchiveRow {
    /// Adapters supply the canonical collection snapshot, including identity evidence.
    pub fn new(
        identity: StorageRecordIdentity,
        valid_from: DateTime<Utc>,
        valid_to: Option<DateTime<Utc>>,
        mut payload: Value,
    ) -> Result<Self, StorageError> {
        let id: CollectionId = serde_json::from_value(payload["id"].clone())
            .map_err(|_| invalid("Invalid captured collection id"))?;
        let created_at: DateTime<Utc> = serde_json::from_value(payload["created_at"].clone())
            .map_err(|_| invalid("Invalid captured creation clock"))?;
        let updated_at: DateTime<Utc> = serde_json::from_value(payload["updated_at"].clone())
            .map_err(|_| invalid("Invalid captured update clock"))?;
        let revision: ResourceRevision = serde_json::from_value(payload["revision"].clone())
            .map_err(|_| invalid("Invalid captured revision"))?;
        let parent: Option<CollectionId> =
            serde_json::from_value(payload["parent_collection_id"].clone())
                .map_err(|_| invalid("Invalid captured parent collection"))?;
        let metadata =
            StorageRecordMetadata::try_new(ResourceId::from(id), created_at, updated_at, revision)
                .map_err(|error| invalid(&error.to_string()))?;
        let collection = StorageCollection::try_new(
            metadata,
            payload["name"]
                .as_str()
                .ok_or_else(|| invalid("Captured collection name missing"))?,
            payload["description"]
                .as_str()
                .ok_or_else(|| invalid("Captured collection description missing"))?,
            parent,
        )
        .map_err(|error| invalid(&error.to_string()))?;
        payload["created_at"] =
            serde_json::to_value(created_at).map_err(|_| invalid("Invalid captured clock"))?;
        payload["updated_at"] =
            serde_json::to_value(updated_at).map_err(|_| invalid("Invalid captured clock"))?;
        if valid_to.is_some_and(|end| end <= valid_from) {
            return Err(invalid("Invalid captured history interval"));
        }
        let evidence: StorageIdentityEvidence =
            serde_json::from_value(payload["logical_identity"].clone())
                .map_err(|_| invalid("Captured history lacks identity evidence"))?;
        if evidence
            .subject()
            .and_then(|subject| subject.record_identity())
            != Some(identity)
        {
            return Err(invalid(
                "Captured history identity differs from its evidence",
            ));
        }
        let row = Self {
            identity,
            valid_from,
            valid_to,
            payload,
            collection,
        };
        history_archive_encoded_bytes(&row, HISTORY_ARCHIVE_ROW_BYTES)?;
        Ok(row)
    }
    pub const fn collection(&self) -> &StorageCollection {
        &self.collection
    }
    pub const fn identity(&self) -> StorageRecordIdentity {
        self.identity
    }
    pub const fn valid_from(&self) -> DateTime<Utc> {
        self.valid_from
    }
    pub const fn valid_to(&self) -> Option<DateTime<Utc>> {
        self.valid_to
    }
    pub const fn payload(&self) -> &Value {
        &self.payload
    }
}

impl std::fmt::Debug for HistoryArchiveRow {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HistoryArchiveRow")
            .field("identity", &self.identity)
            .field("valid_from", &self.valid_from)
            .field("valid_to", &self.valid_to)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "DescriptorInput")]
pub struct HistoryArchiveDescriptor {
    identity: StorageRecordIdentity,
    valid_from: DateTime<Utc>,
    valid_to: DateTime<Utc>,
    payload_digest: [u8; 32],
    #[serde(skip_serializing_if = "Option::is_none")]
    primary_commitment_v1: Option<[u8; 32]>,
    dependencies: Vec<VersionIdentity>,
    format_version: u16,
    location: ArtifactReference,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DescriptorInput {
    identity: StorageRecordIdentity,
    valid_from: DateTime<Utc>,
    valid_to: DateTime<Utc>,
    payload_digest: [u8; 32],
    primary_commitment_v1: Option<[u8; 32]>,
    dependencies: Vec<VersionIdentity>,
    format_version: u16,
    location: ArtifactReference,
}
impl TryFrom<DescriptorInput> for HistoryArchiveDescriptor {
    type Error = StorageError;
    fn try_from(value: DescriptorInput) -> Result<Self, Self::Error> {
        let descriptor = Self {
            identity: value.identity,
            valid_from: value.valid_from,
            valid_to: value.valid_to,
            payload_digest: value.payload_digest,
            primary_commitment_v1: value.primary_commitment_v1,
            dependencies: value.dependencies,
            format_version: value.format_version,
            location: value.location,
        };
        descriptor.validate()?;
        Ok(descriptor)
    }
}
impl HistoryArchiveDescriptor {
    pub fn new(
        row: &HistoryArchiveRow,
        dependencies: Vec<VersionIdentity>,
        format_version: u16,
        location: ArtifactReference,
    ) -> Result<Self, StorageError> {
        let descriptor = Self {
            identity: row.identity,
            valid_from: row.valid_from,
            valid_to: row
                .valid_to
                .ok_or_else(|| invalid("Only closed versions can be catalogued"))?,
            primary_commitment_v1: Self::primary_commitment(row)?,
            payload_digest: Sha256::digest(history_archive_encoded_bytes(
                &row.payload,
                HISTORY_ARCHIVE_ROW_BYTES,
            )?)
            .into(),
            dependencies,
            format_version,
            location,
        };
        descriptor.validate()?;
        Ok(descriptor)
    }
    fn validate(&self) -> Result<(), StorageError> {
        if self.valid_to <= self.valid_from
            || self.dependencies.len() > 128
            || self.dependencies.contains(&self.identity.version())
            || self.dependencies.iter().collect::<BTreeSet<_>>().len() != self.dependencies.len()
            || self.format_version == 0
        {
            return Err(invalid("Invalid archive version descriptor"));
        }
        Ok(())
    }
    pub fn matches_row(&self, row: &HistoryArchiveRow) -> Result<bool, StorageError> {
        Ok(self.identity == row.identity
            && self.valid_from == row.valid_from
            && Some(self.valid_to) == row.valid_to
            && self.matches_payload(&row.payload)?)
    }

    fn matches_payload(&self, payload: &Value) -> Result<bool, StorageError> {
        Ok(self.matches_primary_commitment(payload)?
            && self.payload_digest
                == <[u8; 32]>::from(Sha256::digest(history_archive_encoded_bytes(
                    payload,
                    HISTORY_ARCHIVE_ROW_BYTES,
                )?)))
    }
    pub const fn identity(&self) -> StorageRecordIdentity {
        self.identity
    }
    pub const fn valid_from(&self) -> DateTime<Utc> {
        self.valid_from
    }
    pub const fn valid_to(&self) -> DateTime<Utc> {
        self.valid_to
    }
    pub const fn location(&self) -> &ArtifactReference {
        &self.location
    }
    const fn supported_format(&self) -> bool {
        matches!(self.format_version, 1..=3)
    }
    pub const fn format_version(&self) -> u16 {
        self.format_version
    }
    pub fn dependencies(&self) -> &[VersionIdentity] {
        &self.dependencies
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryArchiveCoverageState {
    Complete,
    Unavailable,
    Expired,
    Corrupt,
    DependencyIncomplete,
    BudgetLimited,
    Unsupported,
}

/// Catalog coverage is deliberately explicit: this slice requires retained hot
/// primary copies. A catalog miss alone never proves a complete empty query.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum HistoryArchiveCatalogCoverage {
    PrimaryCopiesRequired,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(try_from = "CatalogInput")]
pub struct HistoryArchiveCatalog {
    resource: ResourceIdentity,
    generation: HistoryArchiveCatalogGeneration,
    coverage: HistoryArchiveCatalogCoverage,
    versions: Vec<HistoryArchiveDescriptor>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CatalogInput {
    resource: ResourceIdentity,
    generation: HistoryArchiveCatalogGeneration,
    coverage: HistoryArchiveCatalogCoverage,
    versions: Vec<HistoryArchiveDescriptor>,
}
impl TryFrom<CatalogInput> for HistoryArchiveCatalog {
    type Error = StorageError;
    fn try_from(raw: CatalogInput) -> Result<Self, Self::Error> {
        let catalog = Self {
            resource: raw.resource,
            generation: raw.generation,
            coverage: raw.coverage,
            versions: raw.versions,
        };
        catalog.validate()?;
        Ok(catalog)
    }
}
impl HistoryArchiveCatalog {
    pub const fn empty(resource: ResourceIdentity) -> Self {
        Self {
            resource,
            generation: HistoryArchiveCatalogGeneration(0),
            coverage: HistoryArchiveCatalogCoverage::PrimaryCopiesRequired,
            versions: Vec::new(),
        }
    }
    pub const fn resource(&self) -> ResourceIdentity {
        self.resource
    }
    pub const fn generation(&self) -> HistoryArchiveCatalogGeneration {
        self.generation
    }
    pub fn versions(&self) -> &[HistoryArchiveDescriptor] {
        &self.versions
    }
    pub fn validate(&self) -> Result<(), StorageError> {
        if self.versions.len() > HISTORY_ARCHIVE_MAX_VERSIONS {
            return Err(StorageError::input_too_large(
                "History catalog candidate budget exceeded",
            ));
        }
        let mut versions = BTreeSet::new();
        for descriptor in &self.versions {
            if descriptor.identity.resource() != self.resource
                || !versions.insert(descriptor.identity.version())
            {
                return Err(invalid("Catalog identities are ambiguous"));
            }
        }
        history_archive_encoded_bytes(self, HISTORY_ARCHIVE_CATALOG_BYTES)?;
        Ok(())
    }
    pub fn publish(
        &self,
        expected: HistoryArchiveCatalogGeneration,
        additions: Vec<HistoryArchiveDescriptor>,
    ) -> Result<Self, StorageError> {
        self.validate()?;
        if self.generation != expected {
            return Err(StorageError::conflict(
                "History catalog generation changed; replan",
            ));
        }
        if additions.is_empty() || additions.len() > HISTORY_ARCHIVE_BATCH_ROWS {
            return Err(invalid("Archive publication requires 1..1000 versions"));
        }
        history_archive_encoded_bytes(&additions, HISTORY_ARCHIVE_BATCH_BYTES)?;
        let mut next = self.clone();
        for addition in additions {
            if let Some(old) = next
                .versions
                .iter_mut()
                .find(|v| v.identity == addition.identity)
            {
                if old.identity != addition.identity
                    || old.valid_from != addition.valid_from
                    || old.valid_to != addition.valid_to
                    || old.payload_digest != addition.payload_digest
                    || old.dependencies != addition.dependencies
                {
                    return Err(invalid(
                        "Catalog replacement changed logical version content",
                    ));
                }
                *old = addition;
            } else {
                next.versions.push(addition);
            }
        }
        next.generation = HistoryArchiveCatalogGeneration(
            self.generation
                .0
                .checked_add(1)
                .ok_or_else(|| invalid("Catalog generation exhausted"))?,
        );
        next.validate()?;
        Ok(next)
    }
}

/// Bounded historical facts usable for configured authorization even when the
/// payload cannot fit capture. This is not an authorization decision.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "AuthorizationInput")]
pub struct HistoryArchiveAuthorization {
    collection: CollectionId,
    identity: StorageRecordIdentity,
    name: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthorizationInput {
    collection: CollectionId,
    identity: StorageRecordIdentity,
    name: String,
}
impl TryFrom<AuthorizationInput> for HistoryArchiveAuthorization {
    type Error = StorageError;
    fn try_from(value: AuthorizationInput) -> Result<Self, Self::Error> {
        Self::new(value.collection, value.identity, value.name)
    }
}
impl HistoryArchiveAuthorization {
    pub fn new(
        collection: CollectionId,
        identity: StorageRecordIdentity,
        name: impl Into<String>,
    ) -> Result<Self, StorageError> {
        let facts = Self {
            collection,
            identity,
            name: name.into(),
        };
        history_archive_encoded_bytes(&facts, 4096)?;
        Ok(facts)
    }
    pub const fn collection(&self) -> CollectionId {
        self.collection
    }
    pub const fn identity(&self) -> StorageRecordIdentity {
        self.identity
    }
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Correlated source observations; an overflow retains only bounded auth facts.
pub enum HistoryArchiveInput {
    ObservedPrimary(HistoryArchiveObservedPrimary),
    CorruptPrimary(HistoryArchiveAuthorization),
    Available(HistoryArchiveRow),
    CompleteEmpty,
    Unavailable,
    BudgetLimited(HistoryArchiveAuthorization),
}

#[derive(Clone, Debug)]
pub struct HistoryArchiveCaptureRequest {
    principal: PrincipalIdentity,
    selection: HistoryArchiveSelection,
    generation: HistoryArchiveCatalogGeneration,
    available_until: DateTime<Utc>,
    retained_read: bool,
}
impl HistoryArchiveCaptureRequest {
    pub const fn new(
        principal: PrincipalIdentity,
        selection: HistoryArchiveSelection,
        generation: HistoryArchiveCatalogGeneration,
        available_until: DateTime<Utc>,
    ) -> Self {
        Self {
            principal,
            selection,
            generation,
            available_until,
            retained_read: false,
        }
    }
    /// A retained query can reconstruct an already verified closed primary from
    /// its pinned segment. Publication and other callers still retain full rows.
    #[must_use]
    pub const fn for_retained_read(mut self) -> Self {
        self.retained_read = true;
        self
    }

    /// A native adapter can project a large description only for the single
    /// supported descriptor selected by this opted-in read. Legacy catalogs,
    /// publication and ambiguous matches take the full source path.
    pub fn retained_primary_descriptor<'a>(
        &self,
        catalog: &'a HistoryArchiveCatalog,
    ) -> Option<&'a HistoryArchiveDescriptor> {
        if !self.retained_read
            || self.generation != catalog.generation
            || self.selection.family != StorageBackupHistorySection::CollectionHistory
        {
            return None;
        }
        let mut matches = catalog
            .versions
            .iter()
            .filter(|entry| self.selection.covers_descriptor(entry));
        let entry = matches.next()?;
        (matches.next().is_none()
            && entry.primary_commitment_v1.is_some()
            && entry.supported_format()
            && entry.dependencies.is_empty())
        .then_some(entry)
    }

    pub const fn principal(&self) -> PrincipalIdentity {
        self.principal
    }
    pub const fn selection(&self) -> &HistoryArchiveSelection {
        &self.selection
    }
    pub const fn generation(&self) -> HistoryArchiveCatalogGeneration {
        self.generation
    }
}

/// Bounded source facts retained after preparation when capture persistence
/// exhausts capacity. This is neither authorization nor a persisted query pin.
/// Construction is private to a validated capture; it has no wire format.
#[derive(Clone, Eq, PartialEq)]
pub struct HistoryArchiveBudgetEvidence {
    principal: PrincipalIdentity,
    selection: HistoryArchiveSelection,
    generation: HistoryArchiveCatalogGeneration,
    fence: ApplicationFence,
    runtime: Uuid,
    captured_at: DateTime<Utc>,
    available_until: DateTime<Utc>,
    authorization: Option<HistoryArchiveAuthorization>,
    execution: Option<HistoryArchiveExecutionBinding>,
}
impl std::fmt::Debug for HistoryArchiveBudgetEvidence {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("HistoryArchiveBudgetEvidence([redacted])")
    }
}
impl HistoryArchiveBudgetEvidence {
    pub fn validate_execution(
        &self,
        permit: &HistoryArchiveExecutionPermit,
    ) -> Result<(), StorageError> {
        if self.execution.as_ref() != Some(&permit.binding()) {
            return Err(StorageError::unavailable(
                "Archive budget facts belong to a different original producer",
            ));
        }
        Ok(())
    }
    pub fn authorization(&self) -> Option<&HistoryArchiveAuthorization> {
        self.authorization.as_ref()
    }
    /// Recheck immutable request bindings and the original lifetime. The caller
    /// must separately recheck live authority and configured authorization.
    pub fn validate_request(
        &self,
        request: &HistoryArchiveCaptureRequest,
        permit: &StorageExecutionPermit,
        now: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        if self.principal != request.principal
            || self.selection != request.selection
            || self.generation != request.generation
            || self.available_until != request.available_until
            || self.fence != permit.fence()
            || permit
                .owner()
                .is_none_or(|owner| owner.id() != self.runtime)
            || now < self.captured_at
            || now >= self.available_until
        {
            return Err(StorageError::unavailable(
                "Archive budget evidence unavailable",
            ));
        }
        Ok(())
    }
}

/// A full primary remains the legacy wire shape. Only an exact, observed match
/// between a closed primary and its pinned descriptor can produce a read proof.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(untagged, deny_unknown_fields)]
enum CapturedPrimary {
    Full(HistoryArchiveRow),
    Verified {
        #[serde(rename = "verified_primary_v1")]
        proof: VerifiedPrimary,
    },
}

impl CapturedPrimary {
    fn interval(&self) -> (DateTime<Utc>, Option<DateTime<Utc>>) {
        match self {
            Self::Full(row) => (row.valid_from, row.valid_to),
            Self::Verified { proof } => (proof.valid_from, Some(proof.valid_to)),
        }
    }

    fn identity(&self) -> StorageRecordIdentity {
        match self {
            Self::Full(row) => row.identity(),
            Self::Verified { proof } => proof.authorization.identity(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct VerifiedPrimary {
    authorization: HistoryArchiveAuthorization,
    valid_from: DateTime<Utc>,
    valid_to: DateTime<Utc>,
    payload_digest: [u8; 32],
}

impl VerifiedPrimary {
    fn from_observation(
        row: &HistoryArchiveRow,
        descriptor: &HistoryArchiveDescriptor,
        authorization: &HistoryArchiveAuthorization,
    ) -> Option<Self> {
        descriptor.matches_row(row).ok()?.then(|| Self {
            authorization: authorization.clone(),
            valid_from: row.valid_from,
            valid_to: descriptor.valid_to,
            payload_digest: descriptor.payload_digest,
        })
    }

    fn matches(
        &self,
        descriptor: &HistoryArchiveDescriptor,
        selection: &HistoryArchiveSelection,
    ) -> bool {
        self.authorization.identity() == descriptor.identity
            && self.valid_from == descriptor.valid_from
            && self.valid_to == descriptor.valid_to
            && self.payload_digest == descriptor.payload_digest
            && selection.matches_interval(descriptor.identity, self.valid_from, Some(self.valid_to))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(try_from = "CaptureInput")]
pub struct HistoryArchiveCapture {
    id: Uuid,
    principal: PrincipalIdentity,
    selection: HistoryArchiveSelection,
    generation: HistoryArchiveCatalogGeneration,
    fence: ApplicationFence,
    runtime: Uuid,
    captured_at: DateTime<Utc>,
    available_until: DateTime<Utc>,
    hot: Option<CapturedPrimary>,
    authorization: Option<HistoryArchiveAuthorization>,
    archive: Option<HistoryArchiveDescriptor>,
    coverage: HistoryArchiveCoverageState,
    reserved_read_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    execution: Option<HistoryArchiveExecutionBinding>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CaptureInput {
    id: Uuid,
    principal: PrincipalIdentity,
    selection: HistoryArchiveSelection,
    generation: HistoryArchiveCatalogGeneration,
    fence: ApplicationFence,
    runtime: Uuid,
    captured_at: DateTime<Utc>,
    available_until: DateTime<Utc>,
    hot: Option<CapturedPrimary>,
    authorization: Option<HistoryArchiveAuthorization>,
    archive: Option<HistoryArchiveDescriptor>,
    coverage: HistoryArchiveCoverageState,
    reserved_read_bytes: u64,
    #[serde(default)]
    execution: Option<HistoryArchiveExecutionBinding>,
}
impl TryFrom<CaptureInput> for HistoryArchiveCapture {
    type Error = StorageError;
    fn try_from(raw: CaptureInput) -> Result<Self, Self::Error> {
        let capture = Self {
            id: raw.id,
            principal: raw.principal,
            selection: raw.selection,
            generation: raw.generation,
            fence: raw.fence,
            runtime: raw.runtime,
            captured_at: raw.captured_at,
            available_until: raw.available_until,
            hot: raw.hot,
            authorization: raw.authorization,
            archive: raw.archive,
            coverage: raw.coverage,
            reserved_read_bytes: raw.reserved_read_bytes,
            execution: raw.execution,
        };
        capture.validate()?;
        Ok(capture)
    }
}
impl HistoryArchiveCapture {
    /// Adapter boundary after the single authority/snapshot/catalog lock order.
    pub fn capture(
        request: HistoryArchiveCaptureRequest,
        catalog: &HistoryArchiveCatalog,
        permit: &StorageExecutionPermit,
        input: HistoryArchiveInput,
        now: DateTime<Utc>,
    ) -> Result<Self, StorageError> {
        let owner = permit.owner().ok_or_else(|| {
            StorageError::unavailable("Archive reads require their original registered runtime")
        })?;
        catalog.validate()?;
        if request.generation != catalog.generation
            || request.selection.resource != catalog.resource
        {
            return Err(StorageError::conflict(
                "History catalog changed before capture",
            ));
        }
        if request.available_until <= now || request.available_until > now + Duration::hours(48) {
            return Err(invalid(
                "Archive result lifetime must be positive and at most 48 hours",
            ));
        }
        let (hot, authorization, source_coverage) = match input {
            HistoryArchiveInput::Available(row) => {
                let facts = HistoryArchiveAuthorization::new(
                    row.collection().id(),
                    row.identity(),
                    row.collection().name(),
                )?;
                (
                    Some(CapturedPrimary::Full(row)),
                    Some(facts),
                    HistoryArchiveCoverageState::Complete,
                )
            }
            HistoryArchiveInput::ObservedPrimary(observed) => {
                let proof = observed.proof;
                if request
                    .retained_primary_descriptor(catalog)
                    .is_none_or(|descriptor| {
                        descriptor.primary_commitment_v1 != Some(observed.commitment)
                            || !proof.matches(descriptor, &request.selection)
                    })
                {
                    return Err(invalid(
                        "Primary observation differs from the selected descriptor",
                    ));
                }
                let facts = proof.authorization.clone();
                (
                    Some(CapturedPrimary::Verified { proof }),
                    Some(facts),
                    HistoryArchiveCoverageState::Complete,
                )
            }
            HistoryArchiveInput::CorruptPrimary(facts) => {
                (None, Some(facts), HistoryArchiveCoverageState::Corrupt)
            }
            HistoryArchiveInput::BudgetLimited(facts) => (
                None,
                Some(facts),
                HistoryArchiveCoverageState::BudgetLimited,
            ),
            HistoryArchiveInput::CompleteEmpty => {
                (None, None, HistoryArchiveCoverageState::Complete)
            }
            HistoryArchiveInput::Unavailable => {
                (None, None, HistoryArchiveCoverageState::Unavailable)
            }
        };
        if hot.as_ref().is_some_and(|primary| {
            let (start, end) = primary.interval();
            !request
                .selection
                .matches_interval(primary.identity(), start, end)
        }) {
            return Err(invalid("Captured input differs from selection"));
        }
        // Resolve retained coverage from the immutable request, never from a
        // possibly older hot version supplied by a logical restore.
        let mut candidates = catalog
            .versions
            .iter()
            .filter(|entry| request.selection.covers_descriptor(entry));
        let candidate = candidates.next();
        let ambiguous = candidates.next().is_some();
        let missing_primary = candidate.is_some_and(|entry| {
            hot.as_ref()
                .is_none_or(|primary| primary.identity() != entry.identity)
        });
        // A restored open primary can outlive its own retained closure even
        // when no descriptor covers the requested instant. That gap cannot
        // be filled by the older primary's payload.
        let (hot_descriptor, retained_overlap) = hot.as_ref().map_or((None, false), |primary| {
            let (start, end) = primary.interval();
            let mut own = None;
            let mut overlap = false;
            // A second bounded catalog pass checks the source's whole original
            // interval, including newer retained intervals that already ended.
            for entry in &catalog.versions {
                if entry.identity == primary.identity() {
                    own = Some(entry);
                } else if start < entry.valid_to && end.is_none_or(|end| entry.valid_from < end) {
                    overlap = true;
                }
            }
            (own, overlap)
        });
        let retained_closure_gap = hot_descriptor.is_some_and(|entry| {
            matches!(request.selection.point, HistoryArchivePoint::AsOf { at }
                if at < entry.valid_from || at >= entry.valid_to)
        });
        let verified = hot_descriptor
            .zip(hot.as_ref())
            .zip(authorization.as_ref())
            .and_then(|((descriptor, primary), facts)| match primary {
                CapturedPrimary::Full(row) => {
                    VerifiedPrimary::from_observation(row, descriptor, facts)
                }
                CapturedPrimary::Verified { proof }
                    if proof.matches(descriptor, &request.selection) =>
                {
                    Some(proof.clone())
                }
                CapturedPrimary::Verified { .. } => None,
            });
        let hot_content_changed = hot_descriptor.is_some() && verified.is_none();
        let archive = candidate
            .filter(|_| {
                !ambiguous && !missing_primary && !retained_closure_gap && !retained_overlap
            })
            .cloned();
        let coverage = if request.selection.family != StorageBackupHistorySection::CollectionHistory
        {
            HistoryArchiveCoverageState::Unsupported
        } else if source_coverage != HistoryArchiveCoverageState::Complete {
            source_coverage
        } else if ambiguous {
            HistoryArchiveCoverageState::Corrupt
        } else if missing_primary || retained_closure_gap || retained_overlap {
            HistoryArchiveCoverageState::Unavailable
        } else if hot_content_changed {
            HistoryArchiveCoverageState::Corrupt
        } else if archive
            .as_ref()
            .is_some_and(|entry| !entry.supported_format())
        {
            HistoryArchiveCoverageState::Unsupported
        } else if archive
            .as_ref()
            .is_some_and(|entry| !entry.dependencies.is_empty())
        {
            HistoryArchiveCoverageState::DependencyIncomplete
        } else {
            HistoryArchiveCoverageState::Complete
        };
        let (hot, archive) = match (request.retained_read, coverage, archive, verified) {
            (true, HistoryArchiveCoverageState::Complete, Some(entry), Some(proof)) => {
                (Some(CapturedPrimary::Verified { proof }), Some(entry))
            }
            _ if matches!(hot, Some(CapturedPrimary::Verified { .. })) => (None, None),
            (_, _, entry, _) => (hot, entry),
        };
        let capture = Self {
            id: Uuid::new_v4(),
            principal: request.principal,
            selection: request.selection,
            generation: catalog.generation,
            fence: permit.fence(),
            runtime: owner.id(),
            captured_at: now,
            available_until: request.available_until,
            hot,
            authorization,
            archive,
            coverage,
            reserved_read_bytes: 0,
            execution: None,
        };
        capture.validate()?;
        Ok(capture)
    }
    /// Retain only bounded authorization facts, never the hot payload or location.
    pub fn staging_budget_error(&self) -> StorageError {
        StorageError::history_archive_capacity(HistoryArchiveBudgetEvidence {
            principal: self.principal,
            selection: self.selection.clone(),
            generation: self.generation,
            fence: self.fence,
            runtime: self.runtime,
            captured_at: self.captured_at,
            available_until: self.available_until,
            authorization: self.authorization.clone(),
            execution: self.execution.clone(),
        })
    }
    pub const fn id(&self) -> Uuid {
        self.id
    }
    pub const fn principal(&self) -> PrincipalIdentity {
        self.principal
    }
    pub const fn selection(&self) -> &HistoryArchiveSelection {
        &self.selection
    }
    pub const fn generation(&self) -> HistoryArchiveCatalogGeneration {
        self.generation
    }
    pub const fn hot(&self) -> Option<&HistoryArchiveRow> {
        match &self.hot {
            Some(CapturedPrimary::Full(row)) => Some(row),
            _ => None,
        }
    }

    /// Resolve a verified segment frame against the captured primary. Compare the
    /// raw payload before the row constructor normalizes timestamp spellings.
    pub fn resolve_retained_row(
        &self,
        identity: StorageRecordIdentity,
        interval: Range<DateTime<Utc>>,
        dependencies: &[VersionIdentity],
        payload: Value,
    ) -> Result<HistoryArchiveRow, StorageError> {
        let descriptor = self
            .archive
            .as_ref()
            .ok_or_else(|| invalid("Missing pinned archive descriptor"))?;
        if self.coverage != HistoryArchiveCoverageState::Complete
            || identity != descriptor.identity
            || interval.start != descriptor.valid_from
            || interval.end != descriptor.valid_to
            || dependencies != descriptor.dependencies()
            || !descriptor.matches_payload(&payload)?
        {
            return Err(invalid("Retained record differs from the pinned version"));
        }
        match &self.hot {
            Some(CapturedPrimary::Full(row))
                if row.identity == descriptor.identity
                    && row.valid_from == descriptor.valid_from
                    && row.valid_to == Some(descriptor.valid_to)
                    && payload == row.payload =>
            {
                Ok(row.clone())
            }
            Some(CapturedPrimary::Verified { proof }) => {
                let row = HistoryArchiveRow::new(
                    descriptor.identity,
                    descriptor.valid_from,
                    Some(descriptor.valid_to),
                    payload,
                )?;
                if proof.authorization.collection() != row.collection().id()
                    || proof.authorization.name() != row.collection().name()
                {
                    return Err(invalid(
                        "Retained authorization differs from the captured primary",
                    ));
                }
                Ok(row)
            }
            _ => Err(invalid("Pinned primary capture missing or inconsistent")),
        }
    }
    pub const fn authorization(&self) -> Option<&HistoryArchiveAuthorization> {
        self.authorization.as_ref()
    }
    pub const fn archive(&self) -> Option<&HistoryArchiveDescriptor> {
        self.archive.as_ref()
    }
    pub const fn available_until(&self) -> DateTime<Utc> {
        self.available_until
    }
    pub const fn reserved_read_bytes(&self) -> u64 {
        self.reserved_read_bytes
    }
    pub const fn captured_at(&self) -> DateTime<Utc> {
        self.captured_at
    }
    /// Adapter-owned clock observation may shorten exposure, never renew it.
    pub fn observe_expiry(mut self, now: DateTime<Utc>) -> Self {
        if now >= self.available_until {
            self.coverage = HistoryArchiveCoverageState::Expired;
        }
        self
    }
    pub fn coverage(&self, now: DateTime<Utc>) -> HistoryArchiveCoverageState {
        if now >= self.available_until {
            HistoryArchiveCoverageState::Expired
        } else {
            self.coverage
        }
    }
    fn validate(&self) -> Result<(), StorageError> {
        if self
            .execution
            .as_ref()
            .is_some_and(|binding| !binding.valid_for(self.fence))
        {
            return Err(invalid("Invalid persisted archive execution binding"));
        }
        if self
            .authorization
            .as_ref()
            .is_some_and(|facts| facts.identity().resource() != self.selection.resource())
            || self.hot().is_some_and(|row| {
                self.authorization.as_ref().is_none_or(|facts| {
                    facts.identity() != row.identity()
                        || facts.collection() != row.collection().id()
                        || facts.name() != row.collection().name()
                })
            })
        {
            return Err(invalid(
                "Captured authorization facts differ from source identity",
            ));
        }
        if let Some(CapturedPrimary::Verified { proof }) = &self.hot
            && (self.authorization.as_ref() != Some(&proof.authorization)
                || self.selection.family() != StorageBackupHistorySection::CollectionHistory
                || !matches!(
                    self.coverage,
                    HistoryArchiveCoverageState::Complete | HistoryArchiveCoverageState::Expired
                )
                || self.archive.as_ref().is_none_or(|entry| {
                    !entry.supported_format()
                        || !entry.dependencies().is_empty()
                        || !proof.matches(entry, &self.selection)
                }))
        {
            return Err(invalid(
                "Verified primary differs from the pinned archive capture",
            ));
        }
        if self.id.is_nil()
            || self.runtime.is_nil()
            || self.available_until <= self.captured_at
            || self.available_until > self.captured_at + Duration::hours(48)
            || self.reserved_read_bytes > HISTORY_ARCHIVE_READ_BYTES
            || self.hot().is_some_and(|row| !self.selection.matches(row))
            || self.archive.as_ref().is_some_and(|entry| {
                self.hot
                    .as_ref()
                    .is_none_or(|primary| primary.identity() != entry.identity())
            })
        {
            return Err(invalid("Invalid persisted archive capture"));
        }
        if self.coverage == HistoryArchiveCoverageState::Complete
            && (self.selection.family() != StorageBackupHistorySection::CollectionHistory
                || self.archive.as_ref().is_some_and(|entry| {
                    !entry.supported_format() || !entry.dependencies().is_empty()
                })
                || (self.hot.is_none()
                    && !matches!(self.selection.point(), HistoryArchivePoint::AsOf { .. })))
        {
            return Err(invalid(
                "Unsupported capture cannot claim complete coverage",
            ));
        }
        history_archive_encoded_bytes(self, HISTORY_ARCHIVE_CAPTURE_BYTES)?;
        Ok(())
    }
    /// Trusted adapter initial preparation, before persistence. Reload never
    /// calls this to replace an old capture's binding with a fresh producer.
    pub fn bind_execution(
        mut self,
        permit: &HistoryArchiveExecutionPermit,
    ) -> Result<Self, StorageError> {
        self.validate_access(self.principal, permit.application())?;
        let binding = permit.binding();
        if self.execution.as_ref().is_some_and(|old| old != &binding) {
            return Err(StorageError::unavailable(
                "Archive capture cannot change execution ownership",
            ));
        }
        self.execution = Some(binding);
        if let Err(error) = self.validate() {
            return Err(if error.kind() == crate::StorageErrorKind::InputTooLarge {
                self.staging_budget_error()
            } else {
                error
            });
        }
        Ok(self)
    }
    pub fn validate_execution(
        &self,
        permit: &HistoryArchiveExecutionPermit,
    ) -> Result<(), StorageError> {
        self.validate_access(self.principal, permit.application())?;
        if self.execution.as_ref() != Some(&permit.binding()) {
            return Err(StorageError::unavailable(
                "Archive capture belongs to a different original producer",
            ));
        }
        Ok(())
    }
    pub fn validate_access(
        &self,
        principal: PrincipalIdentity,
        permit: &StorageExecutionPermit,
    ) -> Result<(), StorageError> {
        self.validate()?;
        if self.principal != principal
            || self.fence != permit.fence()
            || permit
                .owner()
                .is_none_or(|owner| owner.id() != self.runtime)
        {
            return Err(StorageError::unavailable(
                "Archive capture owner or original application fence is unavailable",
            ));
        }
        Ok(())
    }
    pub fn reserve(&mut self, bytes: u64, now: DateTime<Utc>) -> Result<(), StorageError> {
        if self.coverage == HistoryArchiveCoverageState::Expired || now >= self.available_until {
            return Err(StorageError::unavailable("Archive result expired"));
        }
        let total = self
            .reserved_read_bytes
            .checked_add(bytes)
            .filter(|&sum| sum <= HISTORY_ARCHIVE_READ_BYTES)
            .ok_or_else(|| {
                StorageError::input_too_large("Archive cumulative read work exhausted")
            })?;
        self.reserved_read_bytes = total;
        Ok(())
    }
}

#[async_trait]
pub trait HistoryArchiveStorage: Send + Sync {
    /// Offline control transaction under exclusive process isolation.
    async fn load_history_archive_cleanup(
        &self,
        request: &HistoryArchiveCleanupRequest,
    ) -> Result<HistoryArchiveCleanupClaim, StorageError>;
    /// Re-read the exact pinned obligation under the original recovery fence and
    /// atomically release its claim, child, parent and capture pin.
    async fn settle_history_archive_cleanup(
        &self,
        settlement: HistoryArchiveCleanupSettlement,
    ) -> Result<(), StorageError>;
    async fn claim_history_archive_publication(
        &self,
        request: HistoryArchivePublicationRequest,
        permit: &HistoryArchiveExecutionPermit,
    ) -> Result<HistoryArchivePublicationClaim, StorageError>;

    async fn history_archive_catalog(
        &self,
        resource: ResourceIdentity,
        permit: &HistoryArchiveExecutionPermit,
    ) -> Result<HistoryArchiveCatalog, StorageError>;
    async fn publish_history_archive(
        &self,
        expected: HistoryArchiveCatalogGeneration,
        descriptors: Vec<HistoryArchiveDescriptor>,
        permit: &HistoryArchiveExecutionPermit,
    ) -> Result<HistoryArchiveCatalog, StorageError>;
    async fn capture_history_archive(
        &self,
        request: HistoryArchiveCaptureRequest,
        permit: &HistoryArchiveExecutionPermit,
    ) -> Result<HistoryArchiveCapture, StorageError>;
    async fn history_archive_capture(
        &self,
        id: Uuid,
        principal: PrincipalIdentity,
        permit: &HistoryArchiveExecutionPermit,
    ) -> Result<Option<HistoryArchiveCapture>, StorageError>;
    async fn reserve_history_archive_read(
        &self,
        id: Uuid,
        principal: PrincipalIdentity,
        bytes: u64,
        permit: &HistoryArchiveExecutionPermit,
    ) -> Result<HistoryArchiveCapture, StorageError>;
}

#[cfg(test)]
mod tests;

mod primary_observation;
pub use primary_observation::HistoryArchiveObservedPrimary;

mod execution;
pub use execution::{
    HistoryArchiveExecutionOwner, HistoryArchiveExecutionPermit, HistoryArchiveExecutionSettlement,
};

mod publication;
pub use publication::{
    HISTORY_ARCHIVE_PUBLICATION_BYTES, HISTORY_ARCHIVE_PUBLICATION_MANIFEST_BYTES,
    HistoryArchivePublicationClaim, HistoryArchivePublicationManifest,
};

mod artifact_intent;
pub use artifact_intent::{HistoryArchiveEncodedArtifactIntent, HistoryArchivePublicationRequest};

mod cleanup;
pub use cleanup::*;
