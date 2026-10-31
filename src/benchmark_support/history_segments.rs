//! Offline #514 prototype, deliberately outside the storage SDK and HTTP API.
//!
//! A segment contains one history family and collection, length-prefixed JSON
//! records, and a bounded index. Publication uses the existing artifact protocol.
//! These readers prove record inclusion, not authorization, dependency availability,
//! query completeness, or permission to retire a source. Production tasks/catalogs
//! must supply those guarantees before this format can be activated.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, Write};

use chrono::{DateTime, Utc};
use hubuum_artifacts::{
    ArtifactError, ArtifactIdentity, ArtifactLimits, ArtifactManifest, ArtifactResult,
    ArtifactStore, VerifiedArtifact, stage_producer,
};
use hubuum_budget::{BudgetExceeded, ByteBudget, ByteLimit, LimitedWriter};
use hubuum_domain::{ResourceIdentity, VersionIdentity};
use hubuum_storage_core::StorageBackupHistorySection;
use serde::{Deserialize, Serialize};
use serde_json::{Value, from_slice, to_writer};

const MAGIC: &[u8; 5] = b"HUBH\x01";
const FOOTER: &[u8; 5] = b"HUBI\x01";
// Retained format v1 must not follow future artifact write-policy defaults.
const MAX_BLOCK_BYTES: u32 = 1024 * 1024;
// Two fetched blocks must fit P2's 1 MiB allowance for small exact results.
const BLOCK_BYTES: u32 = MAX_BLOCK_BYTES / 2;
pub const SEGMENT_BYTES: usize = 64 * 1024 * 1024;
pub const ROW_BYTES: usize = 4 * 1024 * 1024;
pub const INDEX_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_RECORDS: usize = 10_000;
const MAX_DEPENDENCIES: usize = 128;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SegmentScope {
    family: StorageBackupHistorySection,
    collection: ResourceIdentity,
}

impl SegmentScope {
    pub fn new(family: StorageBackupHistorySection, collection: ResourceIdentity) -> Self {
        Self { family, collection }
    }
}

/// Only closed, nonempty validity intervals can enter this prototype.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "RawInterval")]
pub struct VersionInterval {
    resource: ResourceIdentity,
    version: VersionIdentity,
    valid_from: DateTime<Utc>,
    valid_to: DateTime<Utc>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawInterval {
    resource: ResourceIdentity,
    version: VersionIdentity,
    valid_from: DateTime<Utc>,
    valid_to: DateTime<Utc>,
}

impl TryFrom<RawInterval> for VersionInterval {
    type Error = ArtifactError;
    fn try_from(raw: RawInterval) -> ArtifactResult<Self> {
        Self::new(raw.resource, raw.version, raw.valid_from, raw.valid_to)
    }
}

impl VersionInterval {
    pub fn new(
        resource: ResourceIdentity,
        version: VersionIdentity,
        valid_from: DateTime<Utc>,
        valid_to: DateTime<Utc>,
    ) -> ArtifactResult<Self> {
        if valid_from >= valid_to {
            return Err(ArtifactError::Invalid(
                "History interval must be nonempty and closed".into(),
            ));
        }
        Ok(Self {
            resource,
            version,
            valid_from,
            valid_to,
        })
    }
    pub fn resource(&self) -> ResourceIdentity {
        self.resource
    }
    pub fn version(&self) -> VersionIdentity {
        self.version
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "RawRecord")]
pub struct SegmentRecord {
    scope: SegmentScope,
    interval: VersionInterval,
    dependencies: Vec<VersionIdentity>,
    payload: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRecord {
    scope: SegmentScope,
    interval: VersionInterval,
    dependencies: Vec<VersionIdentity>,
    payload: Value,
}

impl TryFrom<RawRecord> for SegmentRecord {
    type Error = ArtifactError;
    fn try_from(raw: RawRecord) -> ArtifactResult<Self> {
        Self::new(raw.scope, raw.interval, raw.payload).with_dependencies(raw.dependencies)
    }
}

impl SegmentRecord {
    pub fn new(scope: SegmentScope, interval: VersionInterval, payload: Value) -> Self {
        Self {
            scope,
            interval,
            dependencies: Vec::new(),
            payload,
        }
    }
    pub fn with_dependencies(mut self, dependencies: Vec<VersionIdentity>) -> ArtifactResult<Self> {
        if dependencies.len() > MAX_DEPENDENCIES
            || dependencies.contains(&self.interval.version)
            || dependencies.iter().copied().collect::<BTreeSet<_>>().len() != dependencies.len()
        {
            return Err(ArtifactError::Invalid(
                "History dependencies must be bounded, distinct, and non-self-referential".into(),
            ));
        }
        self.dependencies = dependencies;
        Ok(self)
    }
    pub fn interval(&self) -> &VersionInterval {
        &self.interval
    }
    pub fn dependencies(&self) -> &[VersionIdentity] {
        &self.dependencies
    }
    pub fn payload(&self) -> &Value {
        &self.payload
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    interval: VersionInterval,
    offset: u64,
    length: u32,
}

/// Private fields retain the connection between verified bytes and their index.
/// There is no constructor accepting an unverified external index.
pub struct HistorySegment {
    proof: VerifiedArtifact,
    scope: SegmentScope,
    entries: Vec<Entry>,
    versions: BTreeMap<VersionIdentity, usize>,
    resources: BTreeMap<ResourceIdentity, Vec<usize>>,
    index_bytes: usize,
}

impl HistorySegment {
    /// Stage and read back the entire segment without publishing it. The owner
    /// still owes catalog/claim/generation fencing before publication.
    pub async fn stage<S, I>(
        store: &S,
        identity: ArtifactIdentity,
        scope: SegmentScope,
        records: I,
    ) -> ArtifactResult<Self>
    where
        S: ArtifactStore + ?Sized,
        I: IntoIterator<Item = SegmentRecord> + Send + 'static,
    {
        let limits = ArtifactLimits::builder()
            .block_bytes(BLOCK_BYTES)
            .artifact_bytes(SEGMENT_BYTES as u64)
            .build()?;
        let output_scope = scope;
        let (proof, ()) = stage_producer(store, identity, limits, move |output| {
            let mut output = LimitedWriter::new(output, ByteLimit::new(SEGMENT_BYTES));
            output.write_all(MAGIC).map_err(io_error)?;
            let header = encode(&output_scope, 512)?;
            write_frame(&mut output, &header)?;
            let mut entries = Vec::new();
            for record in records {
                if record.scope != output_scope {
                    return Err(ArtifactError::Invalid(
                        "History segment contains incompatible scopes".into(),
                    ));
                }
                if entries.len() == MAX_RECORDS {
                    return Err(ArtifactError::Limit(
                        "History segment exceeds record budget".into(),
                    ));
                }
                // Include the four-byte frame prefix in the encoded-row ceiling.
                let bytes = encode(&record, ROW_BYTES - 4)?;
                let offset = output.budget().used() as u64;
                write_frame(&mut output, &bytes)?;
                entries.push(Entry {
                    interval: record.interval,
                    offset,
                    length: (bytes.len() + 4) as u32,
                });
            }
            validate_entries(&entries)?;
            let index = encode(&entries, INDEX_BYTES)?;
            output.write_all(&index).map_err(io_error)?;
            output
                .write_all(&(index.len() as u32).to_le_bytes())
                .map_err(io_error)?;
            output.write_all(FOOTER).map_err(io_error)?;
            Ok(())
        })
        .await?;
        let identity = proof.manifest().identity().clone();
        match Self::open_verified(store, proof, &mut ByteLimit::new(SEGMENT_BYTES).start()).await {
            Ok(segment) => Ok(segment),
            Err(error) => {
                store.abort(&identity).await.map_err(|cleanup| ArtifactError::Backend(format!(
                    "History index read failed ({error}); abort failed ({cleanup}); staging cleanup remains required"
                )))?;
                Err(error)
            }
        }
    }

    /// Reconstruct the index from retained bytes under a pinned verification
    /// proof. A changed write policy cannot change the version-1 reader limits.
    /// This does not grant application access to the staged artifact.
    pub async fn open_verified<S: ArtifactStore + ?Sized>(
        store: &S,
        proof: VerifiedArtifact,
        budget: &mut ByteBudget,
    ) -> ArtifactResult<Self> {
        let manifest = proof.manifest();
        if manifest.byte_size() > SEGMENT_BYTES as u64
            || manifest.byte_size() < 18
            || manifest.block_bytes() > MAX_BLOCK_BYTES
        {
            return Err(ArtifactError::Corrupt(
                "Invalid history segment size".into(),
            ));
        }
        let header = read_range(store, manifest, 0, 9, budget).await?;
        if &header[..5] != MAGIC {
            return Err(ArtifactError::Corrupt(
                "Unsupported history segment format".into(),
            ));
        }
        let header_length =
            u32::from_le_bytes(header[5..].try_into().expect("fixed header")) as usize;
        if header_length > 512 {
            return Err(ArtifactError::Limit(
                "History scope exceeds header budget".into(),
            ));
        }
        let scope = from_slice(&read_range(store, manifest, 9, header_length, budget).await?)
            .map_err(|_| ArtifactError::Corrupt("Invalid history scope".into()))?;
        let footer_offset = manifest.byte_size() - 9;
        let footer = read_range(store, manifest, footer_offset, 9, budget).await?;
        if &footer[4..] != FOOTER {
            return Err(ArtifactError::Corrupt(
                "Unsupported history index format".into(),
            ));
        }
        let index_bytes =
            u32::from_le_bytes(footer[..4].try_into().expect("fixed footer")) as usize;
        if index_bytes > INDEX_BYTES {
            return Err(ArtifactError::Limit(
                "History index exceeds byte budget".into(),
            ));
        }
        let index_offset = footer_offset
            .checked_sub(index_bytes as u64)
            .ok_or_else(|| ArtifactError::Corrupt("History index lies outside segment".into()))?;
        let index = read_range(store, manifest, index_offset, index_bytes, budget).await?;
        let entries: Vec<Entry> = from_slice(&index)
            .map_err(|_| ArtifactError::Corrupt("Invalid history index".into()))?;
        if entries.len() > MAX_RECORDS {
            return Err(ArtifactError::Limit(
                "History index exceeds record budget".into(),
            ));
        }
        let mut offset = 9 + header_length as u64;
        for entry in &entries {
            if entry.offset != offset || !(4..=ROW_BYTES as u32).contains(&entry.length) {
                return Err(ArtifactError::Corrupt(
                    "Invalid history index frame layout".into(),
                ));
            }
            offset = offset
                .checked_add(u64::from(entry.length))
                .filter(|&end| end <= index_offset)
                .ok_or_else(|| ArtifactError::Corrupt("History frame overlaps index".into()))?;
        }
        if offset != index_offset {
            return Err(ArtifactError::Corrupt(
                "History index omits record bytes".into(),
            ));
        }
        validate_entries(&entries)?;
        let mut segment = Self {
            proof,
            scope,
            entries,
            versions: BTreeMap::new(),
            resources: BTreeMap::new(),
            index_bytes,
        };
        for (position, entry) in segment.entries.iter().enumerate() {
            segment.versions.insert(entry.interval.version, position);
            segment
                .resources
                .entry(entry.interval.resource)
                .or_default()
                .push(position);
        }
        for positions in segment.resources.values_mut() {
            positions
                .sort_unstable_by_key(|&position| segment.entries[position].interval.valid_from);
        }
        Ok(segment)
    }

    pub fn proof(&self) -> &VerifiedArtifact {
        &self.proof
    }
    pub fn scope(&self) -> &SegmentScope {
        &self.scope
    }
    pub fn record_count(&self) -> usize {
        self.entries.len()
    }
    pub fn index_bytes(&self) -> usize {
        self.index_bytes
    }

    /// Read only the verified blocks intersecting a record. The supplied budget
    /// counts full physical blocks (including retries), not just selected bytes.
    pub async fn exact<S: ArtifactStore + ?Sized>(
        &self,
        store: &S,
        resource: ResourceIdentity,
        version: VersionIdentity,
        budget: &mut ByteBudget,
    ) -> ArtifactResult<Option<SegmentRecord>> {
        let Some(&position) = self.versions.get(&version) else {
            return Ok(None);
        };
        let entry = &self.entries[position];
        if entry.interval.resource != resource {
            return Ok(None);
        }
        self.read(store, entry, budget).await.map(Some)
    }

    /// Half-open validity intervals: valid_from <= instant < valid_to.
    pub async fn as_of<S: ArtifactStore + ?Sized>(
        &self,
        store: &S,
        resource: ResourceIdentity,
        instant: DateTime<Utc>,
        budget: &mut ByteBudget,
    ) -> ArtifactResult<Option<SegmentRecord>> {
        let Some(positions) = self.resources.get(&resource) else {
            return Ok(None);
        };
        let end = positions
            .partition_point(|&position| self.entries[position].interval.valid_from <= instant);
        let Some(&position) = end
            .checked_sub(1)
            .and_then(|position| positions.get(position))
        else {
            return Ok(None);
        };
        let entry = &self.entries[position];
        if instant >= entry.interval.valid_to {
            return Ok(None);
        }
        self.read(store, entry, budget).await.map(Some)
    }

    async fn read<S: ArtifactStore + ?Sized>(
        &self,
        store: &S,
        entry: &Entry,
        budget: &mut ByteBudget,
    ) -> ArtifactResult<SegmentRecord> {
        let manifest = self.proof.manifest();
        let bytes =
            read_range(store, manifest, entry.offset, entry.length as usize, budget).await?;
        if bytes.len() < 4
            || u32::from_le_bytes(bytes[..4].try_into().expect("checked length")) as usize
                != bytes.len() - 4
        {
            return Err(ArtifactError::Corrupt(
                "History frame length mismatch".into(),
            ));
        }
        let record: SegmentRecord = from_slice(&bytes[4..])
            .map_err(|_| ArtifactError::Corrupt("Invalid history frame".into()))?;
        if record.interval != entry.interval || record.scope != self.scope {
            return Err(ArtifactError::Corrupt(
                "History index and record identities/interval disagree".into(),
            ));
        }
        Ok(record)
    }
}

fn validate_entries(entries: &[Entry]) -> ArtifactResult<()> {
    let mut versions = BTreeSet::new();
    let mut intervals: Vec<_> = entries.iter().map(|entry| &entry.interval).collect();
    for interval in &intervals {
        if !versions.insert(interval.version) {
            return Err(ArtifactError::Invalid(
                "Duplicate history version identity".into(),
            ));
        }
    }
    intervals.sort_unstable_by_key(|interval| (interval.resource, interval.valid_from));
    if intervals
        .windows(2)
        .any(|pair| pair[0].resource == pair[1].resource && pair[0].valid_to > pair[1].valid_from)
    {
        return Err(ArtifactError::Invalid(
            "Overlapping history validity intervals".into(),
        ));
    }
    Ok(())
}

fn encode(value: &impl Serialize, limit: usize) -> ArtifactResult<Vec<u8>> {
    let mut output = LimitedWriter::new(Vec::new(), ByteLimit::new(limit));
    let result = to_writer(&mut output, value);
    if output.budget().exceeded() {
        return Err(ArtifactError::Limit(
            "History frame/index exceeds encoded byte budget".into(),
        ));
    }
    result.map_err(|_| ArtifactError::Invalid("History value cannot be encoded".into()))?;
    Ok(output.into_inner())
}

fn write_frame(output: &mut impl Write, bytes: &[u8]) -> ArtifactResult<()> {
    output
        .write_all(&(bytes.len() as u32).to_le_bytes())
        .map_err(io_error)?;
    output.write_all(bytes).map_err(io_error)
}

fn io_error(error: io::Error) -> ArtifactError {
    if error
        .get_ref()
        .is_some_and(|source| source.is::<BudgetExceeded>())
    {
        ArtifactError::Limit("History segment exceeds byte budget".into())
    } else {
        ArtifactError::Backend(format!("History segment output failed: {error}"))
    }
}

async fn read_range<S: ArtifactStore + ?Sized>(
    store: &S,
    manifest: &ArtifactManifest,
    offset: u64,
    length: usize,
    budget: &mut ByteBudget,
) -> ArtifactResult<Vec<u8>> {
    let end = offset
        .checked_add(length as u64)
        .filter(|&end| end <= manifest.byte_size())
        .ok_or_else(|| ArtifactError::Corrupt("History frame lies outside segment".into()))?;
    let block_bytes = u64::from(manifest.block_bytes());
    let mut output = Vec::with_capacity(length);
    for ordinal in offset / block_bytes..end.div_ceil(block_bytes) {
        let start = ordinal * block_bytes;
        let expected = (manifest.byte_size() - start).min(block_bytes) as usize;
        budget.consume(expected).map_err(|_| {
            ArtifactError::Limit("History read exceeds physical byte budget".into())
        })?;
        let bytes = store
            .block(manifest.identity(), ordinal as u32)
            .await?
            .ok_or_else(|| ArtifactError::Corrupt("Missing history artifact block".into()))?;
        manifest.verify_block(ordinal as u32, &bytes)?;
        let from = offset.saturating_sub(start) as usize;
        let to = (end - start).min(block_bytes) as usize;
        output.extend_from_slice(&bytes[from..to]);
    }
    Ok(output)
}

#[cfg(test)]
mod tests;
