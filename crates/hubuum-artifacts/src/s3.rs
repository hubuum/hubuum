//! Private, directly readable S3 artifact destinations. This is a byte-store
//! capability, not task routing or a cold-provider implementation.

mod config;
pub use config::{S3Config, S3ConfigBuilder};

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use aws_sdk_s3::Client;
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{BucketVersioningStatus, CompletedMultipartUpload, CompletedPart};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{
    ArtifactError, ArtifactIdentity, ArtifactLimits, ArtifactManifest, ArtifactReference,
    ArtifactResult, ArtifactStore, Destination, VerifiedArtifact,
};

const CONTROL_BYTES: usize = 8 * 1024 * 1024;
const MAX_BLOCKS: u32 = 1024;
const ROOT: &str = "hubuum-artifacts-v1";

/// Immutable configuration and client are shared; attempt state is always read
/// from S3. The local cursor only bounds maintenance scan work, not correctness.
#[derive(Clone)]
pub struct S3ArtifactStore {
    client: Client,
    config: Arc<S3Config>,
    cursor: Arc<Mutex<Option<String>>>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Block {
    key: String,
    version: String,
    bytes: u32,
    sha256: [u8; 32],
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
enum Phase {
    Staging,
    Sealed,
    Published,
    Deleting,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct State {
    identity: ArtifactIdentity,
    limits: [u64; 4],
    expires: u64,
    phase: Phase,
    blocks: Vec<Block>,
    manifest: Option<Vec<u8>>,
    cleanup_after: Option<String>,
}

impl State {
    fn decode(bytes: &[u8]) -> ArtifactResult<Self> {
        let state: Self = serde_json::from_slice(bytes)
            .map_err(|_| ArtifactError::Corrupt("Invalid S3 artifact control record".into()))?;
        let invalid =
            || ArtifactError::Corrupt("S3 control record exceeds supported bounds".into());
        let limits = ArtifactLimits::builder()
            .block_bytes(state.limits[0].try_into().map_err(|_| invalid())?)
            .artifact_bytes(state.limits[1])
            .blocks(state.limits[2].try_into().map_err(|_| invalid())?)
            .manifest_bytes(state.limits[3].try_into().map_err(|_| invalid())?)
            .build()?;
        if limits.blocks() > MAX_BLOCKS
            || limits.manifest_bytes() > ArtifactLimits::default().manifest_bytes()
            || state.blocks.len() > limits.blocks() as usize
            || state.blocks.iter().any(|block| {
                block.bytes == 0
                    || block.bytes > limits.block_bytes()
                    || block.key.len() > 1024
                    || bounded_version(Some(&block.version)).is_err()
            })
            || state
                .blocks
                .iter()
                .map(|block| block.bytes as u64)
                .sum::<u64>()
                > limits.artifact_bytes()
            || state
                .manifest
                .as_ref()
                .is_some_and(|manifest| manifest.len() > limits.manifest_bytes() as usize)
            || state
                .cleanup_after
                .as_ref()
                .is_some_and(|key| key.len() > 1024)
        {
            return Err(invalid());
        }
        Ok(state)
    }
    fn writable(&self) -> ArtifactResult<()> {
        if self.phase != Phase::Staging || self.expires <= now()? {
            return Err(conflict());
        }
        Ok(())
    }
    fn readable(&self) -> ArtifactResult<()> {
        if self.phase == Phase::Published || (self.phase == Phase::Sealed && self.expires > now()?)
        {
            Ok(())
        } else {
            Err(conflict())
        }
    }
}

fn now() -> ArtifactResult<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_secs())
        .map_err(|_| ArtifactError::Backend("System clock precedes Unix epoch".into()))
}

fn conflict() -> ArtifactError {
    ArtifactError::Conflict(
        "S3 artifact identity, ownership or state changed; reload before retry".into(),
    )
}

// Never include SDK Display/Debug: those can include signed requests, endpoint
// query parameters or provider-controlled response bodies.
fn boundary<E: ProvideErrorMetadata, R>(error: SdkError<E, R>) -> ArtifactError {
    match error.as_service_error().and_then(|error| error.code()) {
        Some("NoSuchKey" | "NoSuchVersion" | "NotFound") => ArtifactError::NotFound,
        Some("PreconditionFailed" | "ConditionalRequestConflict") => conflict(),
        Some("AccessDenied" | "InvalidAccessKeyId" | "SignatureDoesNotMatch") => {
            ArtifactError::Backend("S3 access denied; physical obligations remain pending".into())
        }
        _ => ArtifactError::Backend("S3 operation failed; retry or reconcile durable state".into()),
    }
}

fn bounded_version(version: Option<&str>) -> ArtifactResult<String> {
    version
        .filter(|value| !value.is_empty() && *value != "null" && value.len() <= 1024)
        .map(str::to_owned)
        .ok_or_else(|| {
            ArtifactError::Backend("S3 requires non-null bounded object versions".into())
        })
}

impl S3ArtifactStore {
    /// Validate bucket versioning and reject public ACLs/policies. The bucket
    /// must already exist; this capability never creates or changes its policy.
    pub async fn connect(config: S3Config) -> ArtifactResult<Self> {
        let client = config.client()?;
        let versioning = client
            .get_bucket_versioning()
            .bucket(&config.bucket)
            .send()
            .await
            .map_err(boundary)?;
        if versioning.status() != Some(&BucketVersioningStatus::Enabled) {
            return Err(ArtifactError::Invalid(
                "S3 artifact bucket must enable versioning".into(),
            ));
        }
        let acl = client
            .get_bucket_acl()
            .bucket(&config.bucket)
            .send()
            .await
            .map_err(boundary)?;
        if acl
            .grants()
            .iter()
            .any(|grant| grant.grantee().and_then(|grantee| grantee.uri()).is_some())
        {
            return Err(ArtifactError::Invalid(
                "S3 artifact bucket must have a private ACL".into(),
            ));
        }
        let policy = client
            .get_bucket_policy_status()
            .bucket(&config.bucket)
            .send()
            .await;
        let public_policy = match policy {
            Ok(policy) => policy
                .policy_status()
                .and_then(|status| status.is_public())
                .unwrap_or(true),
            Err(error)
                if error.as_service_error().and_then(|error| error.code())
                    == Some("NoSuchBucketPolicy") =>
            {
                false
            }
            Err(error)
                if error.raw_response().is_some_and(|response| {
                    matches!(response.status().as_u16(), 200 | 404 | 501)
                }) =>
            {
                // Some compatible servers omit policy-status or encode an
                // invalid XML boolean (the pinned MinIO emits uppercase FALSE).
                // Require confirmed policy absence instead of inferring privacy.
                match client.get_bucket_policy().bucket(&config.bucket).send().await {
                    Err(error) if error.as_service_error().and_then(|error| error.code()) == Some("NoSuchBucketPolicy") => false,
                    Err(error) => return Err(boundary(error)),
                    Ok(_) => return Err(ArtifactError::Invalid("S3 provider must support private policy inspection or have no bucket policy".into())),
                }
            }
            Err(error) => return Err(boundary(error)),
        };
        if public_policy {
            return Err(ArtifactError::Invalid(
                "S3 artifact bucket must have a private policy".into(),
            ));
        }
        let store = Self {
            client,
            config: Arc::new(config),
            cursor: Arc::new(Mutex::new(None)),
        };
        store
            .register_destination(&store.config.destination)
            .await?;
        Ok(store)
    }

    pub fn destination(&self) -> &Destination {
        &self.config.destination
    }

    fn root(&self) -> String {
        format!("{ROOT}/{}/", String::from(self.destination().id()))
    }
    fn state_key(&self, identity: &ArtifactIdentity) -> String {
        format!(
            "{}states/{}",
            self.root(),
            String::from(identity.artifact())
        )
    }
    fn data_prefix(&self, identity: &ArtifactIdentity) -> String {
        format!(
            "{}data/{}/{}/{}/",
            self.root(),
            String::from(identity.artifact()),
            String::from(identity.attempt()),
            String::from(identity.version())
        )
    }

    fn intent_prefix(&self, identity: &ArtifactIdentity) -> String {
        format!(
            "{}intents/{}/{}/{}/",
            self.root(),
            String::from(identity.artifact()),
            String::from(identity.attempt()),
            String::from(identity.version())
        )
    }

    async fn get_bytes(
        &self,
        key: &str,
        version: Option<&str>,
        max: usize,
    ) -> ArtifactResult<(Vec<u8>, String)> {
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            self.get_bytes_inner(key, version, max),
        )
        .await
        .map_err(|_| ArtifactError::Backend("S3 bounded read deadline exceeded".into()))?
    }

    async fn get_bytes_inner(
        &self,
        key: &str,
        version: Option<&str>,
        max: usize,
    ) -> ArtifactResult<(Vec<u8>, String)> {
        let output = self
            .client
            .get_object()
            .bucket(&self.config.bucket)
            .key(key)
            .set_version_id(version.map(str::to_owned))
            .send()
            .await
            .map_err(boundary)?;
        if output.delete_marker() == Some(true) {
            return Err(ArtifactError::NotFound);
        }
        if output
            .content_length()
            .is_none_or(|length| length < 0 || length as u64 > max as u64)
        {
            return Err(ArtifactError::Limit(
                "S3 object exceeds the bounded read".into(),
            ));
        }
        let etag = output.e_tag().ok_or_else(conflict)?.to_owned();
        let mut body = output.body;
        let mut bytes = Vec::new();
        while let Some(chunk) = body.next().await {
            let chunk =
                chunk.map_err(|_| ArtifactError::Backend("S3 body transfer failed".into()))?;
            if chunk.len() > max - bytes.len() {
                return Err(ArtifactError::Limit(
                    "S3 body exceeds the bounded read".into(),
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok((bytes, etag))
    }

    async fn load(&self, identity: &ArtifactIdentity) -> ArtifactResult<(State, String)> {
        if identity.destination() != self.destination().id() {
            return Err(conflict());
        }
        let (bytes, etag) = self
            .get_bytes(&self.state_key(identity), None, CONTROL_BYTES)
            .await?;
        let state = State::decode(&bytes)?;
        if state.identity != *identity || state.blocks.len() > MAX_BLOCKS as usize {
            return Err(conflict());
        }
        Ok((state, etag))
    }

    async fn save(&self, state: &State, etag: Option<&str>) -> ArtifactResult<()> {
        let bytes = serde_json::to_vec(state)
            .map_err(|_| ArtifactError::Invalid("Cannot encode artifact state".into()))?;
        if bytes.len() > CONTROL_BYTES {
            return Err(ArtifactError::Limit(
                "S3 artifact control record exceeds its bound".into(),
            ));
        }
        self.put_control(&self.state_key(&state.identity), bytes, etag)
            .await
    }

    async fn put_control(
        &self,
        key: &str,
        bytes: Vec<u8>,
        etag: Option<&str>,
    ) -> ArtifactResult<()> {
        let output = self
            .client
            .put_object()
            .bucket(&self.config.bucket)
            .key(key)
            .set_if_match(etag.map(str::to_owned))
            .set_if_none_match(etag.is_none().then(|| "*".into()))
            .body(ByteStream::from(bytes))
            .send()
            .await
            .map_err(boundary)?;
        bounded_version(output.version_id())?;
        Ok(())
    }

    async fn claim(&self, key: &str, value: Vec<u8>) -> ArtifactResult<()> {
        match self.put_control(key, value.clone(), None).await {
            Ok(()) => Ok(()),
            Err(ArtifactError::Conflict(_)) => {
                if self.get_bytes(key, None, CONTROL_BYTES).await?.0 == value {
                    Ok(())
                } else {
                    Err(conflict())
                }
            }
            Err(error) => Err(error),
        }
    }

    async fn upload(&self, identity: &ArtifactIdentity, bytes: &[u8]) -> ArtifactResult<Block> {
        let upload_identity = Uuid::new_v4().to_string();
        let key = format!("{}{}", self.data_prefix(identity), upload_identity);
        let intent = format!("{}{}", self.intent_prefix(identity), upload_identity);
        // Intent precedes provider I/O, including a possibly lost create reply.
        // Exact-key listing also works on MinIO's restricted multipart API.
        self.put_control(&intent, Vec::new(), None).await?;
        let started = self
            .client
            .create_multipart_upload()
            .bucket(&self.config.bucket)
            .key(&key)
            .send()
            .await
            .map_err(boundary)?;
        let upload = started.upload_id().ok_or_else(conflict)?;
        let result = async {
            if upload.is_empty() || upload.len() > 4096 {
                return Err(ArtifactError::Backend(
                    "S3 upload identity exceeds supported bounds".into(),
                ));
            }
            let (_, etag) = self.get_bytes(&intent, None, 4096).await?;
            self.put_control(&intent, upload.as_bytes().to_vec(), Some(&etag))
                .await?;
            // Each object is one protocol block. A single final multipart part
            // may be smaller than S3's 5 MiB non-final minimum.
            let part = self
                .client
                .upload_part()
                .bucket(&self.config.bucket)
                .key(&key)
                .upload_id(upload)
                .part_number(1)
                .body(ByteStream::from(bytes.to_vec()))
                .send()
                .await
                .map_err(boundary)?;
            let completed = CompletedMultipartUpload::builder()
                .parts(
                    CompletedPart::builder()
                        .part_number(1)
                        .e_tag(part.e_tag().ok_or_else(conflict)?)
                        .build(),
                )
                .build();
            let output = self
                .client
                .complete_multipart_upload()
                .bucket(&self.config.bucket)
                .key(&key)
                .upload_id(upload)
                .if_none_match("*")
                .multipart_upload(completed)
                .send()
                .await
                .map_err(boundary)?;
            Ok(Block {
                key: key.clone(),
                version: bounded_version(output.version_id())?,
                bytes: bytes.len() as u32,
                sha256: Sha256::digest(bytes).into(),
            })
        }
        .await;
        if result.is_err() {
            // Best effort only. Drop, process loss, or an ambiguous response are
            // reconciled by listing this attempt's private prefix later.
            let _ = self
                .client
                .abort_multipart_upload()
                .bucket(&self.config.bucket)
                .key(&key)
                .upload_id(upload)
                .send()
                .await;
        }
        result
    }

    async fn delete_version(&self, key: &str, version: &str) -> ArtifactResult<()> {
        self.client
            .delete_object()
            .bucket(&self.config.bucket)
            .key(key)
            .version_id(version)
            .send()
            .await
            .map_err(boundary)?;
        // DELETE success is insufficient evidence: verify the exact version is
        // absent. A current delete marker cannot hide a retained locked version.
        match self
            .client
            .head_object()
            .bucket(&self.config.bucket)
            .key(key)
            .version_id(version)
            .send()
            .await
        {
            Err(error)
                if error
                    .raw_response()
                    .is_some_and(|response| response.status().as_u16() == 404) =>
            {
                Ok(())
            }
            Err(error) => Err(boundary(error)),
            Ok(_) => Err(ArtifactError::Backend(
                "S3 version remains after deletion; obligation retained".into(),
            )),
        }
    }

    async fn abort_upload(&self, key: &str, upload: &str) -> ArtifactResult<bool> {
        let removed = match self
            .client
            .abort_multipart_upload()
            .bucket(&self.config.bucket)
            .key(key)
            .upload_id(upload)
            .send()
            .await
        {
            Ok(_) => true,
            Err(error)
                if error.as_service_error().and_then(|error| error.code())
                    == Some("NoSuchUpload") =>
            {
                false
            }
            Err(error) => return Err(boundary(error)),
        };
        match self
            .client
            .list_parts()
            .bucket(&self.config.bucket)
            .key(key)
            .upload_id(upload)
            .max_parts(1)
            .send()
            .await
        {
            Ok(parts) if parts.parts().is_empty() && parts.is_truncated() != Some(true) => {
                Ok(removed)
            }
            Err(error)
                if error.as_service_error().and_then(|error| error.code())
                    == Some("NoSuchUpload") =>
            {
                Ok(removed)
            }
            Err(error) => Err(boundary(error)),
            Ok(_) => Err(ArtifactError::Backend(
                "S3 multipart parts remain after abort; obligation retained".into(),
            )),
        }
    }

    async fn reconcile(&self, state: &mut State, etag: &str) -> ArtifactResult<u32> {
        if matches!(state.phase, Phase::Staging | Phase::Sealed) {
            return Ok(0);
        }
        let prefix = self.data_prefix(&state.identity);
        let deleting = state.phase == Phase::Deleting;
        let mut removed = 0;
        let intent_prefix = self.intent_prefix(&state.identity);
        if state
            .cleanup_after
            .as_ref()
            .is_some_and(|key| !key.starts_with(&intent_prefix))
        {
            return Err(conflict());
        }
        let intents = self
            .client
            .list_objects_v2()
            .bucket(&self.config.bucket)
            .prefix(&intent_prefix)
            .set_start_after(state.cleanup_after.clone())
            .max_keys(1)
            .send()
            .await
            .map_err(boundary)?;
        // Neither published nor deleting attempts can accept a new append.
        // Any surviving upload belongs to a losing/interrupted writer.
        if let Some(intent) = intents.contents().first().and_then(|entry| entry.key()) {
            let suffix = intent.strip_prefix(&intent_prefix).ok_or_else(conflict)?;
            Uuid::parse_str(suffix).map_err(|_| conflict())?;
            let expected_key = format!("{prefix}{suffix}");
            let (known_upload, _) = self.get_bytes(intent, None, 4096).await?;
            let known_upload = String::from_utf8(known_upload).map_err(|_| conflict())?;
            let mut ids = Vec::new();
            if !known_upload.is_empty() {
                ids.push(known_upload);
            }
            let uploads = self
                .client
                .list_multipart_uploads()
                .bucket(&self.config.bucket)
                .prefix(&expected_key)
                .max_uploads(16 - ids.len() as i32)
                .send()
                .await
                .map_err(boundary)?;
            for upload in uploads.uploads() {
                upload
                    .key()
                    .filter(|key| *key == expected_key)
                    .ok_or_else(conflict)?;
                let id = upload.upload_id().ok_or_else(conflict)?;
                if id.is_empty() || id.len() > 4096 {
                    return Err(conflict());
                }
                if !ids.iter().any(|known| known == id) {
                    ids.push(id.to_owned());
                }
            }
            for id in ids {
                removed += u32::from(self.abort_upload(&expected_key, &id).await?);
            }
            if uploads.is_truncated() != Some(true) {
                state.cleanup_after = Some(intent.to_owned());
            }
        } else {
            state.cleanup_after = None;
        }
        self.save(state, Some(etag)).await?;
        if removed == 16 {
            return Ok(removed);
        }
        // ponytail: at most 1,024 retained blocks per attempt; scan pagination
        // uses a local cursor below rather than materializing a bucket listing.
        let mut key_marker = None;
        let mut version_marker = None;
        loop {
            let versions = self
                .client
                .list_object_versions()
                .bucket(&self.config.bucket)
                .prefix(&prefix)
                .set_key_marker(key_marker)
                .set_version_id_marker(version_marker)
                .max_keys(16 - removed as i32)
                .send()
                .await
                .map_err(boundary)?;
            for (key, version) in versions
                .versions()
                .iter()
                .map(|entry| (entry.key(), entry.version_id()))
                .chain(
                    versions
                        .delete_markers()
                        .iter()
                        .map(|entry| (entry.key(), entry.version_id())),
                )
            {
                let key = key
                    .filter(|key| key.starts_with(&prefix))
                    .ok_or_else(conflict)?;
                let version = bounded_version(version)?;
                let retained = state
                    .blocks
                    .iter()
                    .any(|block| block.key == key && block.version == version);
                if deleting || !retained {
                    self.delete_version(key, &version).await?;
                    removed += 1;
                }
            }
            if removed == 16 || versions.is_truncated() != Some(true) {
                break;
            }
            key_marker = versions.next_key_marker().map(str::to_owned);
            version_marker = versions.next_version_id_marker().map(str::to_owned);
            if key_marker.is_none() {
                return Err(conflict());
            }
        }
        Ok(removed)
    }
}

#[async_trait]
impl ArtifactStore for S3ArtifactStore {
    async fn register_destination(&self, destination: &Destination) -> ArtifactResult<()> {
        if destination != self.destination() {
            return Err(conflict());
        }
        let binding = serde_json::to_vec(&(
            destination.name(),
            &self.config.bucket,
            &self.config.region,
            &self.config.endpoint,
        ))
        .map_err(|_| conflict())?;
        self.claim(&format!("{}destination", self.root()), binding)
            .await?;
        self.claim(
            &format!("{ROOT}/names/{}", destination.name()),
            String::from(destination.id()).into_bytes(),
        )
        .await
    }

    async fn stage(
        &self,
        identity: &ArtifactIdentity,
        limits: ArtifactLimits,
    ) -> ArtifactResult<()> {
        if identity.destination() != self.destination().id() {
            return Err(conflict());
        }
        if limits.blocks() > MAX_BLOCKS
            || limits.manifest_bytes() > ArtifactLimits::default().manifest_bytes()
        {
            return Err(ArtifactError::Limit(
                "S3 v1 supports at most 1,024 blocks and a 256 KiB manifest".into(),
            ));
        }
        let ownership = serde_json::to_vec(identity).map_err(|_| conflict())?;
        for (kind, id) in [
            ("attempts", String::from(identity.attempt())),
            ("versions", String::from(identity.version())),
        ] {
            self.claim(&format!("{}{kind}/{id}", self.root()), ownership.clone())
                .await?;
        }
        let limits = [
            limits.block_bytes() as u64,
            limits.artifact_bytes(),
            limits.blocks() as u64,
            limits.manifest_bytes() as u64,
        ];
        let state = State {
            identity: identity.clone(),
            limits,
            expires: now()? + 3600,
            phase: Phase::Staging,
            blocks: Vec::new(),
            manifest: None,
            cleanup_after: None,
        };
        match self.save(&state, None).await {
            Ok(()) => Ok(()),
            Err(ArtifactError::Conflict(_)) => {
                let (old, _) = self.load(identity).await?;
                old.writable()?;
                if old.limits == limits {
                    Ok(())
                } else {
                    Err(conflict())
                }
            }
            Err(error) => Err(error),
        }
    }

    async fn append(
        &self,
        identity: &ArtifactIdentity,
        ordinal: u32,
        bytes: &[u8],
    ) -> ArtifactResult<()> {
        let (mut state, etag) = self.load(identity).await?;
        state.writable()?;
        if let Some(old) = state.blocks.get(ordinal as usize) {
            return if old.bytes as usize == bytes.len()
                && old.sha256 == <[u8; 32]>::from(Sha256::digest(bytes))
            {
                Ok(())
            } else {
                Err(conflict())
            };
        }
        let size: u64 = state.blocks.iter().map(|block| block.bytes as u64).sum();
        if ordinal as usize != state.blocks.len()
            || bytes.is_empty()
            || bytes.len() as u64 > state.limits[0]
            || size + bytes.len() as u64 > state.limits[1]
            || ordinal as u64 >= state.limits[2]
            || 117 + (ordinal as u64 + 1) * 36 > state.limits[3]
            || state
                .blocks
                .last()
                .is_some_and(|block| block.bytes as u64 != state.limits[0])
        {
            return Err(ArtifactError::Limit(
                "S3 artifact block is out of order or exceeds attempt limits".into(),
            ));
        }
        state.blocks.push(self.upload(identity, bytes).await?);
        // Recheck expiry after transfer. The CAS also fences concurrent abort.
        state.writable()?;
        self.save(&state, Some(&etag)).await
    }

    async fn seal(&self, manifest: &ArtifactManifest) -> ArtifactResult<()> {
        let (mut state, etag) = self.load(manifest.identity()).await?;
        if matches!(state.phase, Phase::Sealed | Phase::Published) {
            state.readable()?;
            return if state.manifest.as_deref() == Some(manifest.encode().as_slice()) {
                Ok(())
            } else {
                Err(conflict())
            };
        }
        state.writable()?;
        let bytes = manifest.encode();
        let limits = ArtifactLimits::builder()
            .block_bytes(state.limits[0] as u32)
            .artifact_bytes(state.limits[1])
            .blocks(state.limits[2] as u32)
            .manifest_bytes(state.limits[3] as u32)
            .build()?;
        ArtifactManifest::decode(&bytes, manifest.identity(), limits)?;
        if manifest.block_bytes() as u64 != state.limits[0]
            || manifest.block_count() as usize != state.blocks.len()
            || manifest.byte_size()
                != state
                    .blocks
                    .iter()
                    .map(|block| block.bytes as u64)
                    .sum::<u64>()
        {
            return Err(conflict());
        }
        state.phase = Phase::Sealed;
        state.manifest = Some(bytes);
        self.save(&state, Some(&etag)).await
    }

    async fn manifest(&self, identity: &ArtifactIdentity) -> ArtifactResult<Vec<u8>> {
        let (state, _) = self.load(identity).await?;
        state.readable()?;
        state.manifest.ok_or_else(conflict)
    }

    async fn published_manifest(&self, reference: &ArtifactReference) -> ArtifactResult<Vec<u8>> {
        let (state, _) = self.load(reference.identity()).await?;
        if state.phase != Phase::Published {
            return Err(conflict());
        }
        let bytes = state.manifest.ok_or_else(conflict)?;
        if ArtifactManifest::decode(
            &bytes,
            reference.identity(),
            ArtifactLimits::format_ceiling(),
        )?
        .manifest_digest()
            != *reference.manifest_digest()
        {
            return Err(conflict());
        }
        Ok(bytes)
    }

    async fn block(
        &self,
        identity: &ArtifactIdentity,
        ordinal: u32,
    ) -> ArtifactResult<Option<Vec<u8>>> {
        if ordinal > MAX_BLOCKS {
            return Err(ArtifactError::Limit(
                "S3 block ordinal exceeds its bound".into(),
            ));
        }
        let (state, _) = self.load(identity).await?;
        state.readable()?;
        let Some(block) = state.blocks.get(ordinal as usize) else {
            return Ok(None);
        };
        if !block.key.starts_with(&self.data_prefix(identity))
            || block.bytes as u64 > state.limits[0]
        {
            return Err(conflict());
        }
        let (bytes, _) = self
            .get_bytes(&block.key, Some(&block.version), block.bytes as usize)
            .await?;
        Ok(Some(bytes))
    }

    async fn publish(&self, verified: &VerifiedArtifact) -> ArtifactResult<ArtifactReference> {
        let (mut state, etag) = self.load(verified.manifest().identity()).await?;
        state.readable()?;
        if state.manifest.as_deref() != Some(verified.manifest().encode().as_slice()) {
            return Err(conflict());
        }
        if state.phase != Phase::Published {
            state.phase = Phase::Published;
            self.save(&state, Some(&etag)).await?;
        }
        Ok(verified.reference())
    }

    async fn abort(&self, identity: &ArtifactIdentity) -> ArtifactResult<()> {
        let (mut state, etag) = self.load(identity).await?;
        if state.phase == Phase::Published {
            return Err(conflict());
        }
        if state.phase != Phase::Deleting {
            state.phase = Phase::Deleting;
            self.save(&state, Some(&etag)).await?;
        }
        Ok(())
    }

    async fn cleanup(&self) -> ArtifactResult<u32> {
        let mut cursor = self.cursor.lock().await;
        let page = self
            .client
            .list_objects_v2()
            .bucket(&self.config.bucket)
            .prefix(format!("{}states/", self.root()))
            .set_start_after(cursor.clone())
            .max_keys(1)
            .send()
            .await
            .map_err(boundary)?;
        let Some(key) = page.contents().first().and_then(|entry| entry.key()) else {
            *cursor = None;
            return Ok(0);
        };
        *cursor = Some(key.to_owned());
        let (bytes, mut etag) = self.get_bytes(key, None, CONTROL_BYTES).await?;
        let mut state = State::decode(&bytes)?;
        if state.identity.destination() != self.destination().id()
            || self.state_key(&state.identity) != key
        {
            return Err(conflict());
        }
        if state.phase != Phase::Published && state.expires <= now()? {
            self.abort(&state.identity).await?;
            (state, etag) = self.load(&state.identity).await?;
        }
        self.reconcile(&mut state, &etag).await
    }
}

#[cfg(test)]
mod tests;
