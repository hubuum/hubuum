//! Stage a replacement for a mixed segment. The published source stays intact.
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use hubuum_artifacts::{
    ArtifactIdentity, ArtifactManifest, ArtifactReference, ArtifactWriter, OwnedArtifactPhase,
    OwnedArtifactReceipt, OwnedArtifactResolution, OwnedArtifactSession, OwnedArtifactStore,
    verify,
};
use hubuum_storage_core::{HistoryExpiryInputBytes, HistoryExpiryManifest, StorageRecordIdentity};
use sha2::{Digest, Sha256};

use super::{
    ArtifactError, ArtifactResult, ArtifactStore, ByteBudget, ByteLimit, Entry, HistorySegment,
    ResourceIdentity, RetainedHistorySegment, SEGMENT_BYTES, SegmentBlockReader, SegmentRecord,
    VersionIdentity, segment_header, segment_index,
};

/// Bounded exact identities for a rewrite batch and its expected members in one source.
pub struct SegmentRewriteSelection {
    members: Arc<BTreeMap<VersionIdentity, ResourceIdentity>>,
    source_versions: BTreeSet<VersionIdentity>,
}

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
        Ok(Self {
            source_versions: selected.keys().copied().collect(),
            members: Arc::new(selected),
        })
    }

    /// Project the batch onto the source being rewritten. Every surviving frame
    /// is still checked against dependency targets anywhere in the original batch.
    pub fn for_source(
        &self,
        members: impl IntoIterator<Item = StorageRecordIdentity>,
    ) -> ArtifactResult<Self> {
        let mut source_versions = BTreeSet::new();
        for member in members {
            if self.members.get(&member.version()) != Some(&member.resource())
                || !source_versions.insert(member.version())
            {
                return Err(ArtifactError::Invalid(
                    "Source membership differs from the selected rewrite batch".into(),
                ));
            }
        }
        if source_versions.is_empty() {
            return Err(ArtifactError::Invalid(
                "A rewrite needs a selected source member".into(),
            ));
        }
        Ok(Self {
            members: Arc::clone(&self.members),
            source_versions,
        })
    }

    fn survivor_count(&self, source: &RetainedHistorySegment) -> ArtifactResult<usize> {
        if self.source_versions.len() >= source.entries.len() {
            return Err(ArtifactError::Invalid(
                "A subset rewrite needs surviving records".into(),
            ));
        }
        self.check_source_entries(source)?;
        Ok(source.entries.len() - self.source_versions.len())
    }

    fn check_source_entries(&self, source: &RetainedHistorySegment) -> ArtifactResult<()> {
        let mut removed_bytes =
            ByteLimit::new(HistoryExpiryInputBytes::MAX_BATCH_BYTES as usize).start();
        for &version in &self.source_versions {
            let resource = self.members[&version];
            let entry = source
                .versions
                .get(&source.key_width.key(version))
                .map(|&position| &source.entries[position])
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
        Ok(())
    }
}

/// Exact membership evidence for a source with no surviving frames. A catalog
/// handoff must independently check the current generation and deletion authority.
pub struct FullySelectedHistorySegment {
    source: ArtifactReference,
    selection_commitment: [u8; 32],
}

impl FullySelectedHistorySegment {
    pub fn source(&self) -> &ArtifactReference {
        &self.source
    }

    pub fn selection_commitment(&self) -> &[u8; 32] {
        &self.selection_commitment
    }
}

/// Read-back evidence for a replacement. Provider publication and a catalog
/// handoff are separate facts; this value cannot authorize source retirement.
pub struct VerifiedSegmentRewrite {
    segment: HistorySegment,
    verification_bytes: u64,
    published: Option<ArtifactReference>,
}

impl VerifiedSegmentRewrite {
    pub fn segment(&self) -> &HistorySegment {
        &self.segment
    }

    /// Provider reference observed under the original owner after verification.
    pub fn published_reference(&self) -> Option<&ArtifactReference> {
        self.published.as_ref()
    }

    /// Destination bytes fetched for verification, including recovery reads.
    pub fn verification_bytes(&self) -> u64 {
        self.verification_bytes
    }
}

impl RetainedHistorySegment {
    /// Bind an output intent to the exact published source and selected identities.
    pub fn rewrite_commitment(&self, selection: &SegmentRewriteSelection) -> [u8; 32] {
        let mut hash = Sha256::new();
        let single_source = selection.source_versions.len() == selection.members.len();
        if single_source {
            // Preserve existing single-source intents across a worker upgrade.
            hash.update(b"hubuum/history/subset-rewrite/v1\0");
        } else {
            hash.update(b"hubuum/history/subset-rewrite/v2\0");
        }
        hash.update(self.manifest.manifest_digest());
        for (version, resource) in selection.members.iter() {
            if !single_source {
                hash.update([u8::from(selection.source_versions.contains(version))]);
            }
            hash.update(resource.as_bytes());
            hash.update(version.as_bytes());
        }
        hash.finalize().into()
    }

    /// Scan a published segment expected to stay intact. A matching target or
    /// a surviving dependency on the selected batch invalidates that expectation.
    pub async fn verify_unaffected_by_rewrite<S: ArtifactStore + ?Sized>(
        &self,
        source: &S,
        selection: &SegmentRewriteSelection,
        budget: &mut ByteBudget,
    ) -> ArtifactResult<()> {
        let unaffected = SegmentRewriteSelection {
            members: Arc::clone(&selection.members),
            source_versions: BTreeSet::new(),
        };
        self.visit_survivors(source, &unaffected, budget, async |_, _| Ok(()))
            .await
    }

    /// Verify a whole-source selection before planning a handoff with no
    /// replacement. Decode every authenticated frame to check its full UUID.
    pub async fn verify_fully_selected<S: ArtifactStore + ?Sized>(
        &self,
        source: &S,
        selection: &SegmentRewriteSelection,
        budget: &mut ByteBudget,
    ) -> ArtifactResult<FullySelectedHistorySegment> {
        if selection.source_versions.len() != self.entries.len() {
            return Err(ArtifactError::Conflict(
                "The selected versions do not cover the entire history segment".into(),
            ));
        }
        selection.check_source_entries(self)?;
        self.visit_survivors(source, selection, budget, async |_, _| {
            Err(ArtifactError::Conflict(
                "The history segment contains an unselected record".into(),
            ))
        })
        .await?;
        Ok(FullySelectedHistorySegment {
            source: ArtifactReference::from_catalog(
                self.manifest.identity().clone(),
                self.manifest.manifest_digest(),
            ),
            selection_commitment: self.rewrite_commitment(selection),
        })
    }

    /// Stage or recover the original owned attempt. On recovery, compare every
    /// surviving source frame with the sealed destination before reporting the
    /// provider phase. A caller still needs a fenced catalog handoff.
    pub async fn prepare_owned_subset_rewrite<
        S: ArtifactStore + ?Sized,
        D: OwnedArtifactStore + ?Sized,
    >(
        &self,
        source: &S,
        destination: &D,
        receipt: &D::Receipt,
        selection: &SegmentRewriteSelection,
        source_budget: &mut ByteBudget,
    ) -> ArtifactResult<VerifiedSegmentRewrite> {
        let intent = receipt.intent();
        if intent.identity() == self.manifest.identity()
            || intent.request_commitment() != &self.rewrite_commitment(selection)
            || intent.limits() != HistorySegment::write_limits()?
        {
            return Err(ArtifactError::Conflict(
                "History rewrite intent differs from its source, selection or limits".into(),
            ));
        }
        let session = OwnedArtifactSession::new(destination, receipt);
        let mut output = match owned_phase(destination, receipt).await? {
            OwnedArtifactPhase::Staging => {
                self.rewrite_into(
                    source,
                    &session,
                    intent.identity().clone(),
                    selection,
                    source_budget,
                )
                .await?
            }
            OwnedArtifactPhase::Sealed | OwnedArtifactPhase::Published(_) => {
                let bytes = session.manifest(intent.identity()).await?;
                let manifest =
                    ArtifactManifest::decode(&bytes, intent.identity(), intent.limits())?;
                let mut output = VerifiedSegmentRewrite::read_back(&session, &manifest).await?;
                output.verification_bytes += bytes.len() as u64;
                output.verification_bytes += self
                    .compare_survivors(source, &session, selection, source_budget, &output.segment)
                    .await?;
                output
            }
            OwnedArtifactPhase::Fenced => return Err(inactive_attempt()),
        };
        output.published = match owned_phase(destination, receipt).await? {
            OwnedArtifactPhase::Sealed => None,
            OwnedArtifactPhase::Published(reference)
                if reference.identity() == intent.identity()
                    && reference.manifest_digest()
                        == &output.segment.proof().manifest().manifest_digest() =>
            {
                Some(reference)
            }
            _ => return Err(inactive_attempt()),
        };
        Ok(output)
    }

    pub(super) async fn rewrite_into<S: ArtifactStore + ?Sized, D: ArtifactStore + ?Sized>(
        &self,
        source: &S,
        destination: &D,
        identity: ArtifactIdentity,
        selection: &SegmentRewriteSelection,
        source_budget: &mut ByteBudget,
    ) -> ArtifactResult<VerifiedSegmentRewrite> {
        let survivor_count = selection.survivor_count(self)?;
        let mut writer =
            ArtifactWriter::new(destination, identity, HistorySegment::write_limits()?).await?;
        let header = segment_header(&self.scope)?;
        writer.write(&header).await?;
        let mut offset = header.len() as u32;
        let mut entries = Vec::with_capacity(survivor_count);
        self.visit_survivors(source, selection, source_budget, async |record, frame| {
            writer.write(&frame).await?;
            entries.push(Entry {
                interval: record.interval,
                offset,
                length: frame.len() as u32,
            });
            offset += frame.len() as u32; // Input and output are each bounded by SEGMENT_BYTES.
            Ok(())
        })
        .await?;
        writer.write(&segment_index(&entries)?).await?;
        let manifest = writer.seal().await?;
        VerifiedSegmentRewrite::read_back(destination, &manifest).await
    }

    async fn compare_survivors<S: ArtifactStore + ?Sized, D: ArtifactStore + ?Sized>(
        &self,
        source: &S,
        destination: &D,
        selection: &SegmentRewriteSelection,
        source_budget: &mut ByteBudget,
        output: &HistorySegment,
    ) -> ArtifactResult<u64> {
        let mismatch = || {
            ArtifactError::Conflict("Sealed replacement differs from the surviving history".into())
        };
        if output.scope() != self.scope()
            || output.record_count() != selection.survivor_count(self)?
        {
            return Err(mismatch());
        }
        let entries = &output.reader.entries;
        let mut candidate = entries.iter();
        let manifest = output.proof().manifest();
        let mut budget = ByteLimit::new(SEGMENT_BYTES).start();
        let mut reader = SegmentBlockReader::new(destination, manifest, &mut budget);
        reader.sequential = true;
        // Keep the actual header and compressed index so a codec upgrade can
        // recover older output. The verified index bounds a contiguous frame
        // stream. Reconstruct that stream from the source without a second
        // full read of the destination.
        let mut offset = entries.first().ok_or_else(mismatch)?.offset as usize;
        let mut expected = Sha256::new();
        expected.update(reader.range(0, offset).await?);
        self.visit_survivors(source, selection, source_budget, async |record, frame| {
            let entry = candidate.next().ok_or_else(mismatch)?;
            if !entry.matches(&record.interval, output.reader.key_width)
                || entry.length as usize != frame.len()
            {
                return Err(mismatch());
            }
            offset += frame.len();
            expected.update(frame);
            Ok(())
        })
        .await?;
        expected.update(
            reader
                .range(offset as u64, manifest.byte_size() as usize - offset)
                .await?,
        );
        if &<[u8; 32]>::from(expected.finalize()) != manifest.stream_digest() {
            return Err(mismatch());
        }
        Ok(budget.used() as u64)
    }

    async fn visit_survivors<S: ArtifactStore + ?Sized>(
        &self,
        source: &S,
        selection: &SegmentRewriteSelection,
        source_budget: &mut ByteBudget,
        mut visit: impl AsyncFnMut(SegmentRecord, Vec<u8>) -> ArtifactResult<()>,
    ) -> ArtifactResult<()> {
        let mut removed = 0;
        let mut reader = SegmentBlockReader::new(source, &self.manifest, source_budget);
        reader.sequential = true;
        for entry in &self.entries {
            let frame = reader
                .range(u64::from(entry.offset), entry.length as usize)
                .await?;
            let record = self.decode_frame(entry, &frame)?;
            if let Some(resource) = selection.members.get(&record.interval.version) {
                if *resource != record.interval.resource
                    || !selection.source_versions.contains(&record.interval.version)
                {
                    return Err(ArtifactError::Conflict(
                        "Selected history appeared outside its expected source membership".into(),
                    ));
                }
                removed += 1;
                continue;
            }
            if record
                .dependencies
                .iter()
                .any(|version| selection.members.contains_key(version))
            {
                return Err(ArtifactError::Conflict(
                    "Surviving history depends on a selected version".into(),
                ));
            }
            visit(record, frame).await?;
        }
        // A shortened key can match a selected UUID absent from this segment.
        if removed != selection.source_versions.len() {
            return Err(ArtifactError::Conflict(
                "History rewrite selection is stale".into(),
            ));
        }
        Ok(())
    }
}

async fn owned_phase<D: OwnedArtifactStore + ?Sized>(
    store: &D,
    receipt: &D::Receipt,
) -> ArtifactResult<OwnedArtifactPhase> {
    match store.resolve_owned(receipt.intent()).await? {
        OwnedArtifactResolution::Established { artifact, phase }
            if artifact.same_binding(receipt) && artifact.intent() == receipt.intent() =>
        {
            Ok(phase)
        }
        _ => Err(inactive_attempt()),
    }
}

fn inactive_attempt() -> ArtifactError {
    ArtifactError::Conflict(
        "History rewrite attempt is unavailable or changed owner or phase".into(),
    )
}

impl VerifiedSegmentRewrite {
    async fn read_back<D: ArtifactStore + ?Sized>(
        destination: &D,
        manifest: &ArtifactManifest,
    ) -> ArtifactResult<Self> {
        let proof = verify(destination, manifest).await?;
        let mut reopen = ByteLimit::new(SEGMENT_BYTES).start();
        let segment = HistorySegment::open_verified(destination, proof, &mut reopen).await?;
        Ok(Self {
            segment,
            verification_bytes: manifest.stored_bytes() + reopen.used() as u64,
            published: None,
        })
    }
}
