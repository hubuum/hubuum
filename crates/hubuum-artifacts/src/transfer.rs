use crate::manifest::ManifestVerifier;
use crate::{
    ArtifactError, ArtifactIdentity, ArtifactLimits, ArtifactManifest, ArtifactReference,
    ArtifactResult, ArtifactStore, ManifestBuilder, VerifiedArtifact,
};

/// Backpressure is one awaited block write at a time. The caller owns producer
/// bounds; passing an already materialized artifact does not establish those.
pub struct ArtifactWriter<'a, S: ArtifactStore + ?Sized> {
    store: &'a S,
    identity: ArtifactIdentity,
    limits: ArtifactLimits,
    builder: Option<ManifestBuilder>,
    buffer: Vec<u8>,
    accepted: u64,
}

impl<'a, S: ArtifactStore + ?Sized> ArtifactWriter<'a, S> {
    pub async fn new(
        store: &'a S,
        identity: ArtifactIdentity,
        limits: ArtifactLimits,
    ) -> ArtifactResult<Self> {
        store.stage(&identity, limits).await?;
        Ok(Self {
            store,
            builder: Some(ManifestBuilder::new(identity.clone(), limits)),
            identity,
            limits,
            buffer: Vec::with_capacity(limits.block_bytes() as usize),
            accepted: 0,
        })
    }

    pub async fn write(&mut self, mut bytes: &[u8]) -> ArtifactResult<()> {
        let Some(mut builder) = self.builder.take() else {
            return Err(ArtifactError::Conflict(
                "Artifact writer previously failed".into(),
            ));
        };
        let next_size = self
            .accepted
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| ArtifactError::Limit("Artifact byte count overflow".into()))?;
        if next_size > self.limits.artifact_bytes() {
            return Err(ArtifactError::Limit("Artifact exceeds byte budget".into()));
        }
        while !bytes.is_empty() {
            let count = bytes
                .len()
                .min(self.limits.block_bytes() as usize - self.buffer.len());
            self.buffer.extend_from_slice(&bytes[..count]);
            bytes = &bytes[count..];
            if self.buffer.len() == self.limits.block_bytes() as usize {
                let ordinal = builder.block_count();
                builder.push(&self.buffer)?;
                self.store
                    .append(&self.identity, ordinal, &self.buffer)
                    .await?;
                self.buffer.clear();
            }
        }
        self.accepted = next_size;
        self.builder = Some(builder);
        Ok(())
    }

    pub async fn finish(self) -> ArtifactResult<ArtifactReference> {
        let store = self.store;
        let manifest = self.seal().await?;
        verify_and_publish(store, &manifest).await
    }

    /// Freeze output without publishing it, allowing an application use case
    /// to verify first and carry that proof into its own publication transaction.
    pub async fn seal(mut self) -> ArtifactResult<ArtifactManifest> {
        let mut builder = self
            .builder
            .take()
            .ok_or_else(|| ArtifactError::Conflict("Artifact writer previously failed".into()))?;
        if !self.buffer.is_empty() {
            let ordinal = builder.block_count();
            builder.push(&self.buffer)?;
            self.store
                .append(&self.identity, ordinal, &self.buffer)
                .await?;
        }
        let manifest = builder.finish()?;
        self.store.seal(&manifest).await?;
        Ok(manifest)
    }
}

/// Resume a sealed attempt using its independently durable expected manifest.
/// No worker-local file or producer side-effect replay is needed.
pub async fn verify_and_publish<S: ArtifactStore + ?Sized>(
    store: &S,
    manifest: &ArtifactManifest,
) -> ArtifactResult<ArtifactReference> {
    store.publish(&verify(store, manifest).await?).await
}

/// Independently verify a sealed stream. Keep this proof intact when crossing
/// into an application-owned publication transaction; durable fences still apply.
pub async fn verify<S: ArtifactStore + ?Sized>(
    store: &S,
    manifest: &ArtifactManifest,
) -> ArtifactResult<VerifiedArtifact> {
    if store.manifest(manifest.identity()).await? != manifest.encode() {
        return Err(ArtifactError::Corrupt(
            "Sealed artifact manifest differs from the expected upload".into(),
        ));
    }
    let mut verifier = manifest.verifier();
    for ordinal in 0..manifest.block_count() {
        let block = store
            .block(manifest.identity(), ordinal)
            .await?
            .ok_or_else(|| ArtifactError::Corrupt("Missing artifact block".into()))?;
        verifier.push(&block)?;
    }
    if store
        .block(manifest.identity(), manifest.block_count())
        .await?
        .is_some()
    {
        return Err(ArtifactError::Corrupt(
            "Unexpected trailing artifact block".into(),
        ));
    }
    verifier.finish()
}

/// A verified-block reader. `next_block` returns `None` only after the full
/// digest and exact EOF are checked. A failure permanently poisons the reader.
/// The last block is withheld until whole-stream verification succeeds.
pub struct ArtifactReader<'a, S: ArtifactStore + ?Sized> {
    store: &'a S,
    manifest: ArtifactManifest,
    verifier: Option<ManifestVerifier>,
    ordinal: u32,
    complete: bool,
}

impl<'a, S: ArtifactStore + ?Sized> ArtifactReader<'a, S> {
    pub async fn open(store: &'a S, reference: &ArtifactReference) -> ArtifactResult<Self> {
        let bytes = store.published_manifest(reference).await?;
        let manifest =
            ArtifactManifest::decode(&bytes, reference.identity(), ArtifactLimits::default())?;
        if &manifest.manifest_digest() != reference.manifest_digest() {
            return Err(ArtifactError::Corrupt(
                "Published manifest digest mismatch".into(),
            ));
        }
        Ok(Self {
            store,
            verifier: Some(manifest.verifier()),
            manifest,
            ordinal: 0,
            complete: false,
        })
    }
    pub fn manifest(&self) -> &ArtifactManifest {
        &self.manifest
    }
    pub async fn next_block(&mut self) -> ArtifactResult<Option<Vec<u8>>> {
        if self.complete {
            return Ok(None);
        }
        let mut verifier = self
            .verifier
            .take()
            .ok_or_else(|| ArtifactError::Corrupt("Artifact reader previously failed".into()))?;
        let block = self
            .store
            .block(self.manifest.identity(), self.ordinal)
            .await?;
        if self.ordinal == self.manifest.block_count() {
            if block.is_some() {
                return Err(ArtifactError::Corrupt(
                    "Unexpected trailing artifact block".into(),
                ));
            }
            verifier.finish()?;
            self.complete = true;
            return Ok(None);
        }
        let block = block.ok_or_else(|| ArtifactError::Corrupt("Truncated artifact".into()))?;
        verifier.push(&block)?;
        self.ordinal += 1;
        if self.ordinal == self.manifest.block_count() {
            if self
                .store
                .block(self.manifest.identity(), self.ordinal)
                .await?
                .is_some()
            {
                return Err(ArtifactError::Corrupt(
                    "Unexpected trailing artifact block".into(),
                ));
            }
            verifier.finish()?;
            self.complete = true;
        } else {
            self.verifier = Some(verifier);
        }
        Ok(Some(block))
    }
}
