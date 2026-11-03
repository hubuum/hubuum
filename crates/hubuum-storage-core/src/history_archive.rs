//! Collection-only archive query snapshots. These catalog values are not lifecycle
//! authorization proofs. Public task/API activation and source retirement are off.
use std::collections::BTreeSet;

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

/// A combined runtime must not advertise online history expiry while this is true.
pub const HISTORY_ARCHIVE_REQUIRES_LEGACY_RUNTIME: bool = true;
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
    pub fn matches(&self, row: &HistoryArchiveRow) -> bool {
        self.resource == row.identity.resource()
            && match self.point {
                HistoryArchivePoint::Exact { version } => row.identity.version() == version,
                HistoryArchivePoint::AsOf { at } => {
                    row.valid_from <= at && row.valid_to.is_none_or(|end| at < end)
                }
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
            && self.payload_digest
                == <[u8; 32]>::from(Sha256::digest(history_archive_encoded_bytes(
                    &row.payload,
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
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
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
        }
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
    hot: Option<HistoryArchiveRow>,
    authorization: Option<HistoryArchiveAuthorization>,
    archive: Option<HistoryArchiveDescriptor>,
    coverage: HistoryArchiveCoverageState,
    reserved_read_bytes: u64,
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
    hot: Option<HistoryArchiveRow>,
    authorization: Option<HistoryArchiveAuthorization>,
    archive: Option<HistoryArchiveDescriptor>,
    coverage: HistoryArchiveCoverageState,
    reserved_read_bytes: u64,
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
            StorageError::unavailable("Archive reads require a registered Legacy runtime")
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
                    Some(row),
                    Some(facts),
                    HistoryArchiveCoverageState::Complete,
                )
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
        if hot
            .as_ref()
            .is_some_and(|row| !request.selection.matches(row))
        {
            return Err(invalid("Captured input differs from selection"));
        }
        // Resolve retained coverage from the immutable request, never from a
        // possibly older hot version supplied by a logical restore.
        let mut candidates =
            catalog
                .versions
                .iter()
                .filter(|entry| match request.selection.point {
                    HistoryArchivePoint::Exact { version } => entry.identity.version() == version,
                    HistoryArchivePoint::AsOf { at } => {
                        entry.valid_from <= at && at < entry.valid_to
                    }
                });
        let candidate = candidates.next();
        let ambiguous = candidates.next().is_some();
        let missing_primary = candidate.is_some_and(|entry| {
            hot.as_ref()
                .is_none_or(|row| row.identity != entry.identity)
        });
        // A restored open primary can outlive its own retained closure even
        // when no descriptor covers the requested instant. That gap cannot
        // be filled by the older primary's payload.
        let hot_descriptor = hot.as_ref().and_then(|row| {
            catalog
                .versions
                .iter()
                .find(|entry| entry.identity == row.identity)
        });
        let retained_closure_gap = hot_descriptor.is_some_and(|entry| {
            matches!(request.selection.point, HistoryArchivePoint::AsOf { at }
                if at < entry.valid_from || at >= entry.valid_to)
        });
        let hot_content_changed = hot_descriptor.is_some_and(|entry| {
            hot.as_ref()
                .is_some_and(|row| !entry.matches_row(row).unwrap_or(false))
        });
        let archive = candidate
            .filter(|_| !ambiguous && !missing_primary && !retained_closure_gap)
            .cloned();
        let coverage = if request.selection.family != StorageBackupHistorySection::CollectionHistory
        {
            HistoryArchiveCoverageState::Unsupported
        } else if source_coverage != HistoryArchiveCoverageState::Complete {
            source_coverage
        } else if ambiguous {
            HistoryArchiveCoverageState::Corrupt
        } else if missing_primary || retained_closure_gap {
            HistoryArchiveCoverageState::Unavailable
        } else if hot_content_changed {
            HistoryArchiveCoverageState::Corrupt
        } else if archive
            .as_ref()
            .is_some_and(|entry| entry.format_version != 1)
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
        };
        capture.validate()?;
        Ok(capture)
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
        self.hot.as_ref()
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
            .authorization
            .as_ref()
            .is_some_and(|facts| facts.identity().resource() != self.selection.resource())
            || self.hot.as_ref().is_some_and(|row| {
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
        if self.id.is_nil()
            || self.runtime.is_nil()
            || self.available_until <= self.captured_at
            || self.available_until > self.captured_at + Duration::hours(48)
            || self.reserved_read_bytes > HISTORY_ARCHIVE_READ_BYTES
            || self
                .hot
                .as_ref()
                .is_some_and(|row| !self.selection.matches(row))
            || self.archive.as_ref().is_some_and(|entry| {
                self.hot
                    .as_ref()
                    .is_none_or(|row| row.identity() != entry.identity())
            })
        {
            return Err(invalid("Invalid persisted archive capture"));
        }
        if self.coverage == HistoryArchiveCoverageState::Complete
            && (self.selection.family() != StorageBackupHistorySection::CollectionHistory
                || self.archive.as_ref().is_some_and(|entry| {
                    entry.format_version() != 1 || !entry.dependencies().is_empty()
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
    async fn history_archive_catalog(
        &self,
        resource: ResourceIdentity,
        permit: &StorageExecutionPermit,
    ) -> Result<HistoryArchiveCatalog, StorageError>;
    async fn publish_history_archive(
        &self,
        expected: HistoryArchiveCatalogGeneration,
        descriptors: Vec<HistoryArchiveDescriptor>,
        permit: &StorageExecutionPermit,
    ) -> Result<HistoryArchiveCatalog, StorageError>;
    async fn capture_history_archive(
        &self,
        request: HistoryArchiveCaptureRequest,
        permit: &StorageExecutionPermit,
    ) -> Result<HistoryArchiveCapture, StorageError>;
    async fn history_archive_capture(
        &self,
        id: Uuid,
        principal: PrincipalIdentity,
        permit: &StorageExecutionPermit,
    ) -> Result<Option<HistoryArchiveCapture>, StorageError>;
    async fn reserve_history_archive_read(
        &self,
        id: Uuid,
        principal: PrincipalIdentity,
        bytes: u64,
        permit: &StorageExecutionPermit,
    ) -> Result<HistoryArchiveCapture, StorageError>;
}

#[cfg(test)]
mod tests;
