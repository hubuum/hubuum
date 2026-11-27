//! Stage a replacement for a mixed segment. The published source stays intact.
use std::collections::BTreeMap;

use hubuum_artifacts::{ArtifactIdentity, ArtifactLimits, ArtifactWriter, DestinationId, verify};
use hubuum_storage_core::{HistoryExpiryInputBytes, HistoryExpiryManifest, StorageRecordIdentity};

use super::{
    ArtifactError, ArtifactResult, ArtifactStore, BLOCK_BYTES, ByteBudget, ByteLimit, Entry,
    HistorySegment, ResourceIdentity, RetainedHistorySegment, SEGMENT_BYTES, SegmentBlockReader,
    VersionIdentity, segment_header, segment_index,
};

/// Exact identities for one source segment. This carries no deletion authority.
pub struct SegmentRewriteSelection(BTreeMap<VersionIdentity, ResourceIdentity>);

impl SegmentRewriteSelection {
    pub fn new(members: impl IntoIterator<Item = StorageRecordIdentity>) -> ArtifactResult<Self> {
        let mut selected = BTreeMap::new();
        for member in members {
            if selected.len() >= HistoryExpiryManifest::MAX_MEMBERS {
                return Err(ArtifactError::Limit(
                    "Too many history rewrite members".into(),
                ));
            }
            if selected
                .insert(member.version(), member.resource())
                .is_some()
            {
                return Err(ArtifactError::Invalid(
                    "Duplicate history rewrite version".into(),
                ));
            }
        }
        if selected.is_empty() {
            return Err(ArtifactError::Invalid(
                "Empty history rewrite selection".into(),
            ));
        }
        Ok(Self(selected))
    }
}

/// Read-back evidence for the staged replacement. Publication still requires a
/// catalog generation and an independent check of erasure and copy obligations.
pub struct StagedSegmentRewrite {
    segment: HistorySegment,
    verification_bytes: u64,
}

impl StagedSegmentRewrite {
    pub fn segment(&self) -> &HistorySegment {
        &self.segment
    }

    /// Sealed manifest, full block read-back, and reopening the destination index.
    pub fn verification_bytes(&self) -> u64 {
        self.verification_bytes
    }
}

impl RetainedHistorySegment {
    /// Verify all source frames and independently read back a fresh, unpublished
    /// replacement. Surviving frames are copied byte-for-byte, including their
    /// logical identities, dependency lists and original time precision.
    pub async fn stage_subset_rewrite<S: ArtifactStore + ?Sized>(
        &self,
        store: &S,
        destination: DestinationId,
        selection: &SegmentRewriteSelection,
        source_budget: &mut ByteBudget,
    ) -> ArtifactResult<StagedSegmentRewrite> {
        if selection.0.len() >= self.entries.len() {
            return Err(ArtifactError::Invalid(
                "A subset rewrite needs surviving records".into(),
            ));
        }
        let mut removed_bytes =
            ByteLimit::new(HistoryExpiryInputBytes::MAX_BATCH_BYTES as usize).start();
        for (&version, &resource) in &selection.0 {
            let entry = self
                .versions
                .get(&self.key_width.key(version))
                .map(|&position| &self.entries[position])
                .filter(|entry| entry.resource == resource)
                .ok_or_else(|| {
                    ArtifactError::Conflict("History rewrite selection is stale".into())
                })?;
            removed_bytes.consume(entry.length as usize).map_err(|_| {
                ArtifactError::Limit(
                    "Selected history frames exceed the rewrite input budget".into(),
                )
            })?;
        }
        let identity = ArtifactIdentity::new(destination);
        let limits = ArtifactLimits::builder()
            .block_bytes(BLOCK_BYTES)
            .artifact_bytes(SEGMENT_BYTES as u64)
            .build()?;
        let mut writer = ArtifactWriter::new(store, identity.clone(), limits).await?;
        let result = async {
            let header = segment_header(&self.scope)?;
            writer.write(&header).await?;
            let mut offset = header.len() as u32;
            let mut entries = Vec::with_capacity(self.entries.len() - selection.0.len());
            let mut removed = 0;
            let mut reader = SegmentBlockReader::new(store, &self.manifest, source_budget);
            reader.sequential = true;
            for entry in &self.entries {
                let frame = reader
                    .range(u64::from(entry.offset), entry.length as usize)
                    .await?;
                let record = self.decode_frame(entry, &frame)?;
                if selection.0.get(&record.interval.version) == Some(&record.interval.resource) {
                    removed += 1;
                    continue;
                }
                if record
                    .dependencies
                    .iter()
                    .any(|version| selection.0.contains_key(version))
                {
                    return Err(ArtifactError::Conflict(
                        "Surviving history depends on a selected version".into(),
                    ));
                }
                writer.write(&frame).await?;
                entries.push(Entry {
                    interval: record.interval,
                    offset,
                    length: entry.length,
                });
                offset += entry.length; // Input and output are each bounded by SEGMENT_BYTES.
            }
            // A shortened key can match a selected UUID absent from this segment.
            if removed != selection.0.len() {
                return Err(ArtifactError::Conflict(
                    "History rewrite selection is stale".into(),
                ));
            }
            writer.write(&segment_index(&entries)?).await?;
            let manifest = writer.seal().await?;
            let proof = verify(store, &manifest).await?;
            let mut reopen = ByteLimit::new(SEGMENT_BYTES).start();
            let segment = HistorySegment::open_verified(store, proof, &mut reopen).await?;
            Ok(StagedSegmentRewrite {
                segment,
                verification_bytes: manifest.encoded_len() as u64
                    + manifest.byte_size()
                    + reopen.used() as u64,
            })
        }
        .await;
        match result {
            Ok(segment) => Ok(segment),
            Err(primary) => match store.abort(&identity).await {
                Ok(()) => Err(primary),
                Err(abort) => Err(ArtifactError::AbortFailed {
                    primary: Box::new(primary),
                    abort: Box::new(abort),
                }),
            },
        }
    }
}
