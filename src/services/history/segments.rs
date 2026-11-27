//! Retained history segment format and bounded block readers.
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
    ArtifactError, ArtifactIdentity, ArtifactLimits, ArtifactManifest, ArtifactReference,
    ArtifactResult, ArtifactStore, VerifiedArtifact, stage_producer,
};
use hubuum_budget::{BudgetExceeded, ByteBudget, ByteLimit, LimitedWriter};
use hubuum_domain::{ResourceIdentity, VersionIdentity};
use hubuum_storage_core::StorageBackupHistorySection;
use serde::{Deserialize, Serialize};
use serde_json::{Value, from_slice, to_writer};

mod index;
mod rewrite;
use index::{IndexedEntry, KeyWidth, SegmentFormat, VersionKey};
pub use rewrite::{FullySelectedHistorySegment, SegmentRewriteSelection, VerifiedSegmentRewrite};

const MARKER_BYTES: usize = 5 + size_of::<u32>();
// Retained format v1 must not follow future artifact write-policy defaults.
const MAX_BLOCK_BYTES: u32 = 1024 * 1024;
// Leave room for the compact index, manifest, and block rounding on cold reads.
const BLOCK_BYTES: u32 = 64 * 1024;
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
    pub const fn valid_from(&self) -> DateTime<Utc> {
        self.valid_from
    }
    pub const fn valid_to(&self) -> DateTime<Utc> {
        self.valid_to
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
    offset: u32,
    length: u32,
}

/// Private fields retain the connection between verified bytes and their index.
/// There is no constructor accepting an unverified external index.
pub struct HistorySegment {
    proof: VerifiedArtifact,
    reader: RetainedHistorySegment,
}

/// Index reconstructed from a durably published artifact and trusted catalog digest.
/// This proves segment-local inclusion only, never authorization or query coverage.
pub struct RetainedHistorySegment {
    format: SegmentFormat,
    manifest: ArtifactManifest,
    scope: SegmentScope,
    entries: Vec<IndexedEntry>,
    key_width: KeyWidth,
    versions: BTreeMap<VersionKey, usize>,
    resources: BTreeMap<ResourceIdentity, Vec<usize>>,
    index_bytes: usize,
}

impl HistorySegment {
    /// Producer limits used for new segments and for an owned rewrite intent.
    pub fn write_limits() -> ArtifactResult<ArtifactLimits> {
        ArtifactLimits::builder()
            .block_bytes(BLOCK_BYTES)
            .artifact_bytes(SEGMENT_BYTES as u64)
            .build()
    }

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
        let limits = Self::write_limits()?;
        let output_scope = scope;
        let (proof, ()) = stage_producer(store, identity, limits, move |output| {
            let mut output = LimitedWriter::new(output, ByteLimit::new(SEGMENT_BYTES));
            output
                .write_all(&segment_header(&output_scope)?)
                .map_err(io_error)?;
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
                let offset = output.budget().used() as u32; // Bounded by SEGMENT_BYTES.
                write_frame(&mut output, &bytes)?;
                entries.push(Entry {
                    interval: record.interval,
                    offset,
                    length: (bytes.len() + 4) as u32,
                });
            }
            validate_intervals(entries.iter().map(|entry| {
                let interval = &entry.interval;
                (
                    interval.version,
                    interval.resource,
                    interval.valid_from,
                    interval.valid_to,
                )
            }))?;
            output
                .write_all(&segment_index(&entries)?)
                .map_err(io_error)?;
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
    /// proof. A changed write policy cannot change the retained reader limits.
    /// This does not grant application access to the staged artifact.
    pub async fn open_verified<S: ArtifactStore + ?Sized>(
        store: &S,
        proof: VerifiedArtifact,
        budget: &mut ByteBudget,
    ) -> ArtifactResult<Self> {
        let reader =
            RetainedHistorySegment::open_manifest(store, proof.manifest().clone(), budget).await?;
        Ok(Self { proof, reader })
    }

    pub fn format_version(&self) -> u16 {
        self.reader.format_version()
    }
    pub fn proof(&self) -> &VerifiedArtifact {
        &self.proof
    }
    pub fn scope(&self) -> &SegmentScope {
        self.reader.scope()
    }
    pub fn record_count(&self) -> usize {
        self.reader.record_count()
    }
    pub fn index_bytes(&self) -> usize {
        self.reader.index_bytes()
    }

    pub async fn exact<S: ArtifactStore + ?Sized>(
        &self,
        store: &S,
        resource: ResourceIdentity,
        version: VersionIdentity,
        budget: &mut ByteBudget,
    ) -> ArtifactResult<Option<SegmentRecord>> {
        self.reader.exact(store, resource, version, budget).await
    }

    pub async fn as_of<S: ArtifactStore + ?Sized>(
        &self,
        store: &S,
        resource: ResourceIdentity,
        instant: DateTime<Utc>,
        budget: &mut ByteBudget,
    ) -> ArtifactResult<Option<SegmentRecord>> {
        self.reader.as_of(store, resource, instant, budget).await
    }
}

impl RetainedHistorySegment {
    pub fn format_version(&self) -> u16 {
        self.format as u16
    }

    /// Resolve a trusted persisted reference. Publication and manifest digest are
    /// checked before any index bytes are interpreted. Manifest traffic and every
    /// fetched block count against the same cumulative caller budget.
    pub async fn open<S: ArtifactStore + ?Sized>(
        store: &S,
        reference: &ArtifactReference,
        budget: &mut ByteBudget,
    ) -> ArtifactResult<Self> {
        // The adapter contract bounds one manifest call. Require its complete
        // envelope to fit before I/O, then account successful bytes exactly.
        let ceiling = ArtifactLimits::format_ceiling().manifest_bytes() as usize;
        if budget.remaining() < ceiling {
            return Err(ArtifactError::Limit(
                "History manifest exceeds read budget".into(),
            ));
        }
        let bytes = match store.published_manifest(reference).await {
            Ok(bytes) => bytes,
            Err(error) => {
                budget
                    .consume(ceiling)
                    .map_err(|_| ArtifactError::Limit("History read budget exhausted".into()))?;
                return Err(error);
            }
        };
        budget
            .consume(bytes.len())
            .map_err(|_| ArtifactError::Limit("History manifest exceeds read budget".into()))?;
        let manifest = ArtifactManifest::decode(
            &bytes,
            reference.identity(),
            ArtifactLimits::builder()
                .block_bytes(MAX_BLOCK_BYTES)
                .artifact_bytes(SEGMENT_BYTES as u64)
                .build()?,
        )?;
        if &manifest.manifest_digest() != reference.manifest_digest() {
            return Err(ArtifactError::Corrupt(
                "History catalog manifest digest mismatch".into(),
            ));
        }
        Self::open_manifest(store, manifest, budget).await
    }

    async fn open_manifest<S: ArtifactStore + ?Sized>(
        store: &S,
        manifest: ArtifactManifest,
        budget: &mut ByteBudget,
    ) -> ArtifactResult<Self> {
        if manifest.byte_size() > SEGMENT_BYTES as u64
            || manifest.byte_size() < (2 * MARKER_BYTES) as u64
            || manifest.block_bytes() > MAX_BLOCK_BYTES
        {
            return Err(ArtifactError::Corrupt(
                "Invalid history segment size".into(),
            ));
        }
        let mut blocks = SegmentBlockReader::new(store, &manifest, budget);
        let header = blocks.range(0, MARKER_BYTES).await?;
        let format = SegmentFormat::from_header(&header[..5])?;
        let header_length =
            u32::from_le_bytes(header[5..].try_into().expect("fixed header")) as usize;
        if header_length > 512 {
            return Err(ArtifactError::Limit(
                "History scope exceeds header budget".into(),
            ));
        }
        let scope = from_slice(&blocks.range(MARKER_BYTES as u64, header_length).await?)
            .map_err(|_| ArtifactError::Corrupt("Invalid history scope".into()))?;
        let footer_offset = manifest.byte_size() - MARKER_BYTES as u64;
        let footer = blocks.range(footer_offset, MARKER_BYTES).await?;
        if &footer[4..] != format.footer() {
            return Err(ArtifactError::Corrupt(
                "Unsupported history index format".into(),
            ));
        }
        let index_bytes =
            u32::from_le_bytes(footer[..4].try_into().expect("fixed footer")) as usize;
        if index_bytes > format.max_index_bytes() {
            return Err(ArtifactError::Limit(
                "History index exceeds byte budget".into(),
            ));
        }
        let index_offset = footer_offset
            .checked_sub(index_bytes as u64)
            .ok_or_else(|| ArtifactError::Corrupt("History index lies outside segment".into()))?;
        let index = blocks.range(index_offset, index_bytes).await?;
        drop(blocks);
        let (key_width, entries) = format.decode_index(&index)?;
        if entries.len() > MAX_RECORDS {
            return Err(ArtifactError::Limit(
                "History index exceeds record budget".into(),
            ));
        }
        let mut offset = (MARKER_BYTES + header_length) as u64;
        for entry in &entries {
            if u64::from(entry.offset) != offset || !(4..=ROW_BYTES as u32).contains(&entry.length)
            {
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
        validate_intervals(
            entries
                .iter()
                .map(|entry| (entry.version, entry.resource, entry.from, entry.to)),
        )
        .map_err(|error| match error {
            ArtifactError::Invalid(message) => ArtifactError::Corrupt(message),
            other => other,
        })?;
        let mut segment = Self {
            format,
            manifest,
            scope,
            entries,
            key_width,
            versions: BTreeMap::new(),
            resources: BTreeMap::new(),
            index_bytes,
        };
        for (position, entry) in segment.entries.iter().enumerate() {
            segment.versions.insert(entry.version, position);
            segment
                .resources
                .entry(entry.resource)
                .or_default()
                .push(position);
        }
        for positions in segment.resources.values_mut() {
            positions.sort_unstable_by_key(|&position| segment.entries[position].from);
        }
        Ok(segment)
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
        let Some(&position) = self.versions.get(&self.key_width.key(version)) else {
            return Ok(None);
        };
        let entry = &self.entries[position];
        if entry.resource != resource {
            return Ok(None);
        }
        // A shortened key may also match a UUID absent from this segment. The
        // authenticated record supplies the full identity for the exact query.
        let record = self.read(store, entry, budget).await?;
        Ok((record.interval.version == version).then_some(record))
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
        let end = positions.partition_point(|&position| self.entries[position].from <= instant);
        let Some(&position) = end
            .checked_sub(1)
            .and_then(|position| positions.get(position))
        else {
            return Ok(None);
        };
        let entry = &self.entries[position];
        if instant >= entry.to {
            return Ok(None);
        }
        self.read(store, entry, budget).await.map(Some)
    }

    async fn read<S: ArtifactStore + ?Sized>(
        &self,
        store: &S,
        entry: &IndexedEntry,
        budget: &mut ByteBudget,
    ) -> ArtifactResult<SegmentRecord> {
        let bytes = SegmentBlockReader::new(store, &self.manifest, budget)
            .range(u64::from(entry.offset), entry.length as usize)
            .await?;
        self.decode_frame(entry, &bytes)
    }

    fn decode_frame(&self, entry: &IndexedEntry, bytes: &[u8]) -> ArtifactResult<SegmentRecord> {
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
        if !entry.matches(&record.interval, self.key_width) || record.scope != self.scope {
            return Err(ArtifactError::Corrupt(
                "History index and record identities/interval disagree".into(),
            ));
        }
        Ok(record)
    }
}

fn validate_intervals<K: Ord>(
    entries: impl Iterator<Item = (K, ResourceIdentity, DateTime<Utc>, DateTime<Utc>)>,
) -> ArtifactResult<()> {
    let mut versions = BTreeSet::new();
    let mut intervals = Vec::with_capacity(entries.size_hint().0);
    for (version, resource, from, to) in entries {
        if !versions.insert(version) {
            return Err(ArtifactError::Invalid(
                "Duplicate history version identity".into(),
            ));
        }
        intervals.push((resource, from, to));
    }
    intervals.sort_unstable();
    if intervals.windows(2).any(|pair| {
        let (resource, _, end) = pair[0];
        let (next_resource, start, _) = pair[1];
        resource == next_resource && end > start
    }) {
        return Err(ArtifactError::Invalid(
            "Overlapping history validity intervals".into(),
        ));
    }
    Ok(())
}

fn segment_header(scope: &SegmentScope) -> ArtifactResult<Vec<u8>> {
    let mut header = SegmentFormat::CompressedIndex.header().to_vec();
    write_frame(&mut header, &encode(scope, 512)?)?;
    Ok(header)
}

fn segment_index(entries: &[Entry]) -> ArtifactResult<Vec<u8>> {
    let mut index = index::encode_compressed(entries)?;
    index.extend_from_slice(&(index.len() as u32).to_le_bytes());
    index.extend_from_slice(SegmentFormat::CompressedIndex.footer());
    Ok(index)
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

/// Index discovery caches header/footer blocks. A sequential frame scan instead
/// retains just the last intersecting block for the following frame.
struct SegmentBlockReader<'a, S: ?Sized> {
    store: &'a S,
    manifest: &'a ArtifactManifest,
    budget: &'a mut ByteBudget,
    cached: BTreeMap<u32, Vec<u8>>,
    sequential: bool,
}

impl<'a, S: ArtifactStore + ?Sized> SegmentBlockReader<'a, S> {
    fn new(store: &'a S, manifest: &'a ArtifactManifest, budget: &'a mut ByteBudget) -> Self {
        Self {
            store,
            manifest,
            budget,
            cached: BTreeMap::new(),
            sequential: false,
        }
    }

    async fn range(&mut self, offset: u64, length: usize) -> ArtifactResult<Vec<u8>> {
        let end = offset
            .checked_add(length as u64)
            .filter(|&end| end <= self.manifest.byte_size())
            .ok_or_else(|| ArtifactError::Corrupt("History frame lies outside segment".into()))?;
        let is_marker = length == MARKER_BYTES && (offset == 0 || end == self.manifest.byte_size());
        let block_bytes = u64::from(self.manifest.block_bytes());
        let mut output = Vec::with_capacity(length);
        for ordinal in offset / block_bytes..end.div_ceil(block_bytes) {
            let start = ordinal * block_bytes;
            let ordinal = ordinal as u32;
            let from = offset.saturating_sub(start) as usize;
            let to = (end - start).min(block_bytes) as usize;
            if let Some(bytes) = self.cached.get(&ordinal) {
                output.extend_from_slice(&bytes[from..to]);
                continue;
            }
            let expected = (self.manifest.byte_size() - start).min(block_bytes) as usize;
            self.budget.consume(expected).map_err(|_| {
                ArtifactError::Limit("History read exceeds physical byte budget".into())
            })?;
            let bytes = self
                .store
                .block(self.manifest.identity(), ordinal)
                .await?
                .ok_or_else(|| ArtifactError::Corrupt("Missing history artifact block".into()))?;
            self.manifest.verify_block(ordinal, &bytes)?;
            output.extend_from_slice(&bytes[from..to]);
            if self.sequential && u64::from(ordinal) == (end - 1) / block_bytes {
                self.cached.clear();
                self.cached.insert(ordinal, bytes);
            } else if is_marker {
                self.cached.insert(ordinal, bytes);
            }
        }
        Ok(output)
    }
}

#[cfg(test)]
mod tests;
