use sha2::{Digest, Sha256};

use super::{
    ArtifactError, ArtifactId, ArtifactIdentity, ArtifactResult, AttemptId, DestinationId,
    ObjectVersion,
};

const MAGIC: &[u8; 5] = b"HUBA\x01";
const HEADER_BYTES: usize = 117;
const ENTRY_BYTES: usize = 36;
pub const MAX_BLOCK_BYTES: u32 = 1024 * 1024;
pub const MAX_ARTIFACT_BYTES: u64 = 64 * 1024 * 1024 * 1024;
pub const MAX_BLOCKS: u32 = 65_536;
pub const MAX_MANIFEST_BYTES: u32 = 4 * 1024 * 1024;

/// Per-attempt resource limits. Defaults match the #504 acceptance profile;
/// deployments can tune them within the finite version-1 reader envelope.
/// Readers use the retained format envelope, never the current write policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ArtifactLimits {
    block_bytes: u32,
    artifact_bytes: u64,
    blocks: u32,
    manifest_bytes: u32,
}

impl Default for ArtifactLimits {
    fn default() -> Self {
        Self {
            block_bytes: MAX_BLOCK_BYTES,
            artifact_bytes: 1024 * 1024 * 1024,
            blocks: 1024,
            manifest_bytes: 256 * 1024,
        }
    }
}

impl ArtifactLimits {
    /// Stable, finite decoding envelope for retained version-1 artifacts.
    /// Lowering a deployment's write limits must not invalidate old outputs.
    pub const fn format_ceiling() -> Self {
        Self {
            block_bytes: MAX_BLOCK_BYTES,
            artifact_bytes: MAX_ARTIFACT_BYTES,
            blocks: MAX_BLOCKS,
            manifest_bytes: MAX_MANIFEST_BYTES,
        }
    }
    pub fn builder() -> ArtifactLimitsBuilder {
        ArtifactLimitsBuilder(Self::default())
    }
    pub fn block_bytes(self) -> u32 {
        self.block_bytes
    }
    pub const fn artifact_bytes(self) -> u64 {
        self.artifact_bytes
    }
    pub fn blocks(self) -> u32 {
        self.blocks
    }
    pub fn manifest_bytes(self) -> u32 {
        self.manifest_bytes
    }
}

pub struct ArtifactLimitsBuilder(ArtifactLimits);
impl ArtifactLimitsBuilder {
    pub fn block_bytes(mut self, value: u32) -> Self {
        self.0.block_bytes = value;
        self
    }
    pub fn artifact_bytes(mut self, value: u64) -> Self {
        self.0.artifact_bytes = value;
        self
    }
    pub fn blocks(mut self, value: u32) -> Self {
        self.0.blocks = value;
        self
    }
    pub fn manifest_bytes(mut self, value: u32) -> Self {
        self.0.manifest_bytes = value;
        self
    }
    pub fn build(self) -> ArtifactResult<ArtifactLimits> {
        let limits = self.0;
        if limits.block_bytes == 0
            || limits.block_bytes > MAX_BLOCK_BYTES
            || limits.artifact_bytes > MAX_ARTIFACT_BYTES
            || limits.blocks > MAX_BLOCKS
            || limits.manifest_bytes < HEADER_BYTES as u32
            || limits.manifest_bytes > MAX_MANIFEST_BYTES
        {
            return Err(ArtifactError::Invalid(
                "Artifact limits exceed the version 1 resource ceilings".into(),
            ));
        }
        if limits
            .artifact_bytes
            .div_ceil(u64::from(limits.block_bytes))
            > u64::from(limits.blocks)
        {
            return Err(ArtifactError::Invalid(
                "Artifact byte limit requires more blocks than allowed".into(),
            ));
        }
        Ok(limits)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Block {
    length: u32,
    digest: [u8; 32],
}

/// Canonical bounded wire format. Entry order defines ordinal and offset; the
/// header binds all four identities, format, block size, total length and digest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactManifest {
    identity: ArtifactIdentity,
    block_bytes: u32,
    byte_size: u64,
    digest: [u8; 32],
    blocks: Vec<Block>,
}

impl ArtifactManifest {
    pub fn identity(&self) -> &ArtifactIdentity {
        &self.identity
    }
    pub fn block_bytes(&self) -> u32 {
        self.block_bytes
    }
    pub fn byte_size(&self) -> u64 {
        self.byte_size
    }
    pub fn block_count(&self) -> u32 {
        self.blocks.len() as u32
    }
    pub fn stream_digest(&self) -> &[u8; 32] {
        &self.digest
    }
    pub fn encoded_len(&self) -> usize {
        HEADER_BYTES + self.blocks.len() * ENTRY_BYTES
    }
    pub fn manifest_digest(&self) -> [u8; 32] {
        Sha256::digest(self.encode()).into()
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.encoded_len());
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(self.identity.artifact().as_bytes());
        bytes.extend_from_slice(self.identity.attempt().as_bytes());
        bytes.extend_from_slice(self.identity.destination().as_bytes());
        bytes.extend_from_slice(self.identity.version().as_bytes());
        bytes.extend_from_slice(&self.block_bytes.to_le_bytes());
        bytes.extend_from_slice(&self.byte_size.to_le_bytes());
        bytes.extend_from_slice(&self.digest);
        bytes.extend_from_slice(&self.block_count().to_le_bytes());
        for block in &self.blocks {
            bytes.extend_from_slice(&block.length.to_le_bytes());
            bytes.extend_from_slice(&block.digest);
        }
        bytes
    }

    pub fn decode(
        bytes: &[u8],
        expected: &ArtifactIdentity,
        limits: ArtifactLimits,
    ) -> ArtifactResult<Self> {
        if bytes.len() > limits.manifest_bytes as usize {
            return Err(ArtifactError::Limit(
                "Artifact manifest exceeds byte limit".into(),
            ));
        }
        if bytes.len() < HEADER_BYTES || &bytes[..5] != MAGIC {
            return Err(corrupt("Invalid artifact manifest header or format"));
        }
        let mut input = &bytes[5..];
        let identity = ArtifactIdentity::from_parts(
            ArtifactId::from_bytes(take(&mut input)?)?,
            AttemptId::from_bytes(take(&mut input)?)?,
            DestinationId::from_bytes(take(&mut input)?)?,
            ObjectVersion::from_bytes(take(&mut input)?)?,
        );
        if &identity != expected {
            return Err(corrupt("Artifact manifest identity mismatch"));
        }
        let block_bytes = u32::from_le_bytes(take(&mut input)?);
        let byte_size = u64::from_le_bytes(take(&mut input)?);
        let digest = take(&mut input)?;
        let count = u32::from_le_bytes(take(&mut input)?);
        if block_bytes == 0
            || block_bytes > limits.block_bytes
            || byte_size > limits.artifact_bytes
            || count > limits.blocks
        {
            return Err(ArtifactError::Limit(
                "Artifact manifest exceeds data/block limits".into(),
            ));
        }
        // Check count and exact encoded size before allocating the hash vector.
        if u64::from(count) != byte_size.div_ceil(u64::from(block_bytes))
            || input.len() != count as usize * ENTRY_BYTES
        {
            return Err(corrupt("Artifact manifest block count or length mismatch"));
        }
        let mut blocks = Vec::with_capacity(count as usize);
        for ordinal in 0..count {
            let length = u32::from_le_bytes(take(&mut input)?);
            let hash = take(&mut input)?;
            let expected_length = (byte_size - u64::from(ordinal) * u64::from(block_bytes))
                .min(u64::from(block_bytes));
            if u64::from(length) != expected_length {
                return Err(corrupt("Artifact manifest block length mismatch"));
            }
            blocks.push(Block {
                length,
                digest: hash,
            });
        }
        if count == 0 && digest != <[u8; 32]>::from(Sha256::digest([])) {
            return Err(corrupt("Empty artifact digest mismatch"));
        }
        Ok(Self {
            identity,
            block_bytes,
            byte_size,
            digest,
            blocks,
        })
    }

    pub fn verify_block(&self, ordinal: u32, bytes: &[u8]) -> ArtifactResult<()> {
        let block = self
            .blocks
            .get(ordinal as usize)
            .ok_or_else(|| corrupt("Unexpected artifact block"))?;
        if bytes.len() != block.length as usize
            || <[u8; 32]>::from(Sha256::digest(bytes)) != block.digest
        {
            return Err(corrupt("Artifact block length or digest mismatch"));
        }
        Ok(())
    }

    pub fn verifier(&self) -> ManifestVerifier {
        ManifestVerifier {
            manifest: self.clone(),
            next: 0,
            hash: Sha256::new(),
        }
    }
}

fn take<const N: usize>(input: &mut &[u8]) -> ArtifactResult<[u8; N]> {
    let value = input
        .get(..N)
        .ok_or_else(|| corrupt("Truncated artifact manifest"))?;
    let value = value.try_into().expect("checked fixed length");
    *input = &input[N..];
    Ok(value)
}

fn corrupt(message: &str) -> ArtifactError {
    ArtifactError::Corrupt(message.into())
}

/// Streaming manifest construction retains only the bounded hashes. Errors
/// poison the builder so ignored failures cannot produce a shorter success.
pub struct ManifestBuilder {
    manifest: ArtifactManifest,
    limits: ArtifactLimits,
    hash: Sha256,
    failed: bool,
}

impl ManifestBuilder {
    pub fn new(identity: ArtifactIdentity, limits: ArtifactLimits) -> Self {
        Self {
            manifest: ArtifactManifest {
                identity,
                block_bytes: limits.block_bytes,
                byte_size: 0,
                digest: [0; 32],
                blocks: Vec::new(),
            },
            limits,
            hash: Sha256::new(),
            failed: false,
        }
    }
    pub fn block_count(&self) -> u32 {
        self.manifest.block_count()
    }
    pub fn push(&mut self, bytes: &[u8]) -> ArtifactResult<()> {
        if self.failed {
            return Err(corrupt("Artifact builder previously failed"));
        }
        self.failed = true;
        if bytes.is_empty()
            || bytes.len() > self.limits.block_bytes as usize
            || self
                .manifest
                .blocks
                .last()
                .is_some_and(|last| last.length != self.limits.block_bytes)
        {
            return Err(ArtifactError::Invalid(
                "Only the final artifact block may be shorter than the block size".into(),
            ));
        }
        let total = self
            .manifest
            .byte_size
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| ArtifactError::Limit("Artifact length overflow".into()))?;
        if total > self.limits.artifact_bytes
            || self.manifest.block_count() >= self.limits.blocks
            || self.manifest.encoded_len() + ENTRY_BYTES > self.limits.manifest_bytes as usize
        {
            return Err(ArtifactError::Limit(
                "Artifact exceeds byte, block or manifest budget".into(),
            ));
        }
        self.hash.update(bytes);
        self.manifest.blocks.push(Block {
            length: bytes.len() as u32,
            digest: Sha256::digest(bytes).into(),
        });
        self.manifest.byte_size = total;
        self.failed = false;
        Ok(())
    }
    pub fn finish(mut self) -> ArtifactResult<ArtifactManifest> {
        if self.failed {
            return Err(corrupt("Artifact builder previously failed"));
        }
        self.manifest.digest = self.hash.finalize().into();
        Ok(self.manifest)
    }
}

pub struct ManifestVerifier {
    manifest: ArtifactManifest,
    next: u32,
    hash: Sha256,
}
impl ManifestVerifier {
    pub fn push(&mut self, bytes: &[u8]) -> ArtifactResult<()> {
        self.manifest.verify_block(self.next, bytes)?;
        self.hash.update(bytes);
        self.next += 1;
        Ok(())
    }
    pub fn finish(self) -> ArtifactResult<VerifiedArtifact> {
        if self.next != self.manifest.block_count()
            || <[u8; 32]>::from(self.hash.finalize()) != self.manifest.digest
        {
            return Err(corrupt(
                "Artifact stream is incomplete or has a mismatched digest",
            ));
        }
        Ok(VerifiedArtifact {
            manifest: self.manifest,
        })
    }
}

/// Proof obtainable only by reading and hashing the entire destination stream.
/// Persistence adapters must still check sealed ownership and publication fences.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedArtifact {
    manifest: ArtifactManifest,
}
impl VerifiedArtifact {
    pub fn reference(&self) -> super::ArtifactReference {
        super::ArtifactReference::from_catalog(
            self.manifest.identity().clone(),
            self.manifest.manifest_digest(),
        )
    }

    pub fn manifest(&self) -> &ArtifactManifest {
        &self.manifest
    }
}
