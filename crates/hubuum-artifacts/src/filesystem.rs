//! Private, single-owner Unix filesystem byte store. Task ownership and atomic
//! primary-database completion are deliberately outside this adapter.
use std::fs::File;
use std::io::{self, Read, Write};
use std::os::unix::fs::MetadataExt;
use std::path::{Component, Path};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use rustix::fs::{self, AtFlags, Dir, FlockOperation, Mode, OFlags};
use serde::{Deserialize, Serialize};

use crate::owned::{StoreScope, authorize};
use crate::{
    ArtifactCreationIntent, ArtifactError, ArtifactIdentity, ArtifactLimits, ArtifactManifest,
    ArtifactReference, ArtifactResult, ArtifactStore, Destination, DestinationId, OwnedArtifact,
    OwnedArtifactAdmission, OwnedArtifactPhase, OwnedArtifactResolution, OwnedArtifactResult,
    OwnedArtifactStore, VerifiedArtifact,
};

type Result<T> = std::result::Result<T, FilesystemError>;
const STATE_LIMIT: usize = 4096;
const DESTINATION_LIMIT: usize = 1024;
const STAGING_SECONDS: u64 = 3600;
const ROOT_FORMAT: &[u8] = b"HUBF\x01";
const DIRECTORIES: [&str; 3] = ["artifacts", "attempts", "versions"];

/// Adapter-owned failures. Paths and operating-system messages are excluded
/// from the protocol error projection.
#[derive(Debug)]
pub enum FilesystemError {
    Io(io::Error),
    InvalidLayout,
    Protocol(ArtifactError),
    Worker,
}
impl std::fmt::Display for FilesystemError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(f, "Filesystem artifact I/O failed ({:?})", error.kind()),
            Self::InvalidLayout => f.write_str("Invalid private filesystem artifact layout"),
            Self::Protocol(error) => error.fmt(f),
            Self::Worker => f.write_str("Filesystem artifact worker failed"),
        }
    }
}
impl std::error::Error for FilesystemError {}
impl From<io::Error> for FilesystemError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}
impl From<rustix::io::Errno> for FilesystemError {
    fn from(error: rustix::io::Errno) -> Self {
        Self::Io(error.into())
    }
}
impl From<ArtifactError> for FilesystemError {
    fn from(error: ArtifactError) -> Self {
        Self::Protocol(error)
    }
}
impl From<FilesystemError> for ArtifactError {
    fn from(error: FilesystemError) -> Self {
        match error {
            FilesystemError::Protocol(error) => error,
            FilesystemError::Io(error) if error.kind() == io::ErrorKind::NotFound => Self::NotFound,
            error => Self::Backend(error.to_string()),
        }
    }
}
fn conflict() -> FilesystemError {
    ArtifactError::Conflict("Filesystem artifact identity, state or content conflicts".into())
        .into()
}
fn invalid() -> FilesystemError {
    FilesystemError::InvalidLayout
}
fn now() -> Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|time| time.as_secs())
        .map_err(|_| invalid())
}
fn key(bytes: &[u8; 16]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
fn block_key(ordinal: u32) -> String {
    format!("{ordinal:08x}.block")
}

/// A configured, pre-created, owner-only root on a local Unix filesystem.
/// Opening rejects symlinks in every root component. All later paths are fixed
/// names or validated UUIDs, resolved relative to pinned directory descriptors.
/// Requires Tokio. Clones and independent processes coordinate with flock.
#[derive(Clone)]
pub struct FilesystemArtifactStore {
    root: Arc<File>,
    #[cfg(test)]
    fail_sync: Arc<std::sync::atomic::AtomicIsize>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct State {
    format: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    owner: Option<ArtifactCreationIntent>,
    identity: ArtifactIdentity,
    limits: Limits,
    expires: u64,
    phase: Phase,
    blocks: u32,
    bytes: u64,
    cleanup_next: u32,
}
#[derive(Clone, Copy, PartialEq, Serialize, Deserialize)]
enum Phase {
    Preparing,
    Staging,
    Sealed,
    Published,
    Deleting,
    Discarded,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Limits {
    block: u32,
    bytes: u64,
    blocks: u32,
    manifest: u32,
}
impl From<ArtifactLimits> for Limits {
    fn from(limits: ArtifactLimits) -> Self {
        Self {
            block: limits.block_bytes(),
            bytes: limits.artifact_bytes(),
            blocks: limits.blocks(),
            manifest: limits.manifest_bytes(),
        }
    }
}
impl Limits {
    fn validated(&self) -> Result<ArtifactLimits> {
        Ok(ArtifactLimits::builder()
            .block_bytes(self.block)
            .artifact_bytes(self.bytes)
            .blocks(self.blocks)
            .manifest_bytes(self.manifest)
            .build()?)
    }
}
impl State {
    fn decode(bytes: &[u8]) -> Result<Self> {
        let state: Self = decode(bytes)?;
        state.limits.validated()?;
        if !matches!(
            (state.format, state.owner.is_some()),
            (1, false) | (2, true)
        ) || (state.owner.is_none() && state.phase == Phase::Preparing)
            || state.owner.as_ref().is_some_and(|intent| {
                intent.identity() != &state.identity
                    || intent.limits() != state.limits.validated().expect("validated above")
            })
            || state.blocks > state.limits.blocks
            || state.bytes > state.limits.bytes
            || state.bytes.div_ceil(u64::from(state.limits.block)) != u64::from(state.blocks)
            || state.cleanup_next > state.blocks
        {
            return Err(invalid());
        }
        Ok(state)
    }
    fn live(&self) -> Result<()> {
        if self.owner.is_none() && self.expires <= now()? {
            return Err(conflict());
        }
        Ok(())
    }
    fn readable(&self) -> Result<()> {
        match self.phase {
            Phase::Published => Ok(()),
            Phase::Sealed => self.live(),
            _ => Err(conflict()),
        }
    }
    fn writable(&self) -> Result<()> {
        self.live()?;
        if self.phase != Phase::Staging {
            return Err(conflict());
        }
        Ok(())
    }
}

impl FilesystemArtifactStore {
    pub async fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_owned();
        tokio::task::spawn_blocking(move || {
            if !root.is_absolute() {
                return Err(invalid());
            }
            let mut directory = File::open("/")?;
            for component in root.components() {
                match component {
                    Component::RootDir => {}
                    Component::Normal(name) => directory = open_dir(&directory, name)?,
                    _ => return Err(invalid()),
                }
            }
            private(&directory, true)?;
            let store = Self {
                root: Arc::new(directory),
                #[cfg(test)]
                fail_sync: Arc::new(std::sync::atomic::AtomicIsize::new(-1)),
            };
            store.locked(|store| store.initialize_layout())?;
            Ok(store)
        })
        .await
        .map_err(|_| FilesystemError::Worker)?
    }
    async fn run<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&Self) -> Result<T> + Send + 'static,
    ) -> ArtifactResult<T> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.locked(operation))
            .await
            .map_err(|_| ArtifactError::from(FilesystemError::Worker))?
            .map_err(ArtifactError::from)
    }
    fn locked<T>(&self, operation: impl FnOnce(&Self) -> Result<T>) -> Result<T> {
        // Open a distinct file description per operation: flock on a duplicated
        // descriptor would not exclude other operations in this process.
        let lock = open_file(&self.root, "lock", OFlags::RDWR | OFlags::CREATE)?;
        fs::flock(&lock, FlockOperation::LockExclusive)?;
        operation(self)
    }
    fn sync(&self, file: &File) -> Result<()> {
        #[cfg(test)]
        {
            use std::sync::atomic::Ordering;
            if self.fail_sync.fetch_sub(1, Ordering::SeqCst) == 0 {
                return Err(io::Error::other("injected sync failure").into());
            }
        }
        file.sync_all()?;
        #[cfg(target_os = "macos")]
        if file.metadata()?.is_file() {
            fs::fcntl_fullfsync(file)?;
        }
        Ok(())
    }
    fn directory(&self, name: &str) -> Result<File> {
        match fs::mkdirat(&*self.root, name, Mode::RWXU) {
            Ok(()) => self.sync(&self.root)?,
            Err(rustix::io::Errno::EXIST) => {}
            Err(error) => return Err(error.into()),
        }
        let directory = open_dir(&self.root, name)?;
        private(&directory, true)?;
        Ok(directory)
    }
    fn initialize_layout(&self) -> Result<()> {
        match read(&self.root, "format", ROOT_FORMAT.len()) {
            Ok(bytes) => {
                if bytes != ROOT_FORMAT {
                    return Err(invalid());
                }
                for name in DIRECTORIES {
                    private(&open_dir(&self.root, name)?, true)?;
                }
                return self.sync(&self.root);
            }
            Err(FilesystemError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        // Only empty roots and interrupted initialization are adoptable. Never
        // reinterpret existing data after losing the layout/ownership metadata.
        for entry in Dir::read_from(&*self.root)? {
            let entry = entry?;
            let name = entry.file_name().to_bytes();
            if matches!(name, b"." | b".." | b"lock" | b"pending") {
                continue;
            }
            if !DIRECTORIES
                .iter()
                .any(|directory| directory.as_bytes() == name)
            {
                return Err(invalid());
            }
            let directory = open_dir(&self.root, entry.file_name())?;
            private(&directory, true)?;
            for child in Dir::read_from(&directory)? {
                if !matches!(child?.file_name().to_bytes(), b"." | b"..") {
                    return Err(invalid());
                }
            }
        }
        for name in DIRECTORIES {
            self.directory(name)?;
        }
        self.atomic(&self.root, "format", ROOT_FORMAT)
    }
    fn check_identities(&self, identity: &ArtifactIdentity) -> Result<()> {
        let claims = [
            ("attempts", key(identity.attempt().as_bytes())),
            ("versions", key(identity.version().as_bytes())),
        ];
        // Preflight both keys before writing either. A crash between claims
        // may conservatively reserve an ID, but can never transfer ownership.
        for (directory, name) in &claims {
            let directory = open_dir(&self.root, *directory)?;
            private(&directory, true)?;
            match read(&directory, name, STATE_LIMIT) {
                Ok(bytes) => {
                    let owner: ArtifactIdentity = decode(&bytes)?;
                    if &owner != identity {
                        return Err(conflict());
                    }
                }
                Err(FilesystemError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
    fn claim_identities(&self, identity: &ArtifactIdentity) -> Result<()> {
        self.check_identities(identity)?;
        let claims = [
            ("attempts", key(identity.attempt().as_bytes())),
            ("versions", key(identity.version().as_bytes())),
        ];
        for (directory, name) in claims {
            let directory = open_dir(&self.root, directory)?;
            match read(&directory, &name, STATE_LIMIT) {
                Ok(_) => self.sync(&directory)?,
                Err(FilesystemError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                    self.atomic(&directory, &name, &encode(identity)?)?;
                }
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
    fn atomic(&self, directory: &File, name: &str, bytes: &[u8]) -> Result<()> {
        // One temporary name per directory is safe under the root lock. A crash
        // leaves it uncommitted; retries truncate and replace it without following links.
        let mut pending = open_file(directory, "pending", OFlags::WRONLY | OFlags::CREATE)?;
        pending.set_len(0)?;
        pending.write_all(bytes)?;
        self.sync(&pending)?;
        fs::renameat(directory, "pending", directory, name)?;
        self.sync(directory)
    }
    fn state(&self, identity: &ArtifactIdentity) -> Result<(File, State)> {
        let artifacts = open_dir(&self.root, "artifacts")?;
        private(&artifacts, true)?;
        let directory = open_dir(&artifacts, key(identity.artifact().as_bytes()))?;
        private(&directory, true)?;
        let state = State::decode(&read(&directory, "state", STATE_LIMIT)?)?;
        if &state.identity != identity {
            return Err(conflict());
        }
        Ok((directory, state))
    }
    fn save(&self, directory: &File, state: &State) -> Result<()> {
        self.atomic(directory, "state", &encode(state)?)
    }
    fn manifest_on(&self, directory: &File, state: &State) -> Result<Vec<u8>> {
        state.readable()?;
        let bytes = read(
            directory,
            "manifest",
            ArtifactLimits::format_ceiling().manifest_bytes() as usize,
        )?;
        ArtifactManifest::decode(&bytes, &state.identity, ArtifactLimits::format_ceiling())?;
        Ok(bytes)
    }
    fn destinations(&self) -> Result<Vec<(DestinationId, String)>> {
        match read(&self.root, "destinations", DESTINATION_LIMIT * 256) {
            Ok(bytes) => decode(&bytes),
            Err(FilesystemError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                Ok(Vec::new())
            }
            Err(error) => Err(error),
        }
    }
}

fn open_dir(directory: &File, name: impl rustix::path::Arg) -> Result<File> {
    Ok(fs::openat(
        directory,
        name,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )?
    .into())
}
fn open_file(directory: &File, name: &str, flags: OFlags) -> Result<File> {
    let file: File = fs::openat(
        directory,
        name,
        flags | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::RUSR | Mode::WUSR,
    )?
    .into();
    private(&file, false)?;
    Ok(file)
}
fn private(file: &File, directory: bool) -> Result<()> {
    let metadata = file.metadata()?;
    if metadata.uid() != rustix::process::geteuid().as_raw()
        || metadata.mode() & 0o077 != 0
        || metadata.is_dir() != directory
        || (!directory && (!metadata.is_file() || metadata.nlink() != 1))
    {
        return Err(invalid());
    }
    Ok(())
}
fn read(directory: &File, name: &str, limit: usize) -> Result<Vec<u8>> {
    let file = open_file(directory, name, OFlags::RDONLY)?;
    if file.metadata()?.len() > limit as u64 {
        return Err(invalid());
    }
    let mut bytes = Vec::new();
    file.take(limit as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > limit {
        return Err(invalid());
    }
    Ok(bytes)
}
fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    serde_json::from_slice(bytes).map_err(|_| invalid())
}
fn encode(value: &impl Serialize) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|_| invalid())
}
fn remove(directory: &File, name: &str) -> Result<bool> {
    match fs::unlinkat(directory, name, AtFlags::empty()) {
        Ok(()) => Ok(true),
        Err(rustix::io::Errno::NOENT) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

#[async_trait]
impl ArtifactStore for FilesystemArtifactStore {
    async fn register_destination(&self, destination: &Destination) -> ArtifactResult<()> {
        let destination = destination.clone();
        self.run(move |store| {
            let mut destinations = store.destinations()?;
            for (id, name) in &destinations {
                if *id == destination.id() || name == destination.name() {
                    return if *id == destination.id() && name == destination.name() {
                        store.sync(&store.root)
                    } else {
                        Err(conflict())
                    };
                }
            }
            if destinations.len() >= DESTINATION_LIMIT {
                return Err(invalid());
            }
            destinations.push((destination.id(), destination.name().to_owned()));
            store.atomic(&store.root, "destinations", &encode(&destinations)?)
        })
        .await
    }
    async fn stage(
        &self,
        identity: &ArtifactIdentity,
        limits: ArtifactLimits,
    ) -> ArtifactResult<()> {
        let identity = identity.clone();
        self.run(move |store| {
            if !store
                .destinations()?
                .iter()
                .any(|(id, _)| *id == identity.destination())
            {
                return Err(conflict());
            }
            let artifacts = open_dir(&store.root, "artifacts")?;
            private(&artifacts, true)?;
            let name = key(identity.artifact().as_bytes());
            match fs::mkdirat(&artifacts, &name, Mode::RWXU) {
                Ok(()) => {}
                Err(rustix::io::Errno::EXIST) => {}
                Err(error) => return Err(error.into()),
            }
            store.sync(&artifacts)?;
            let directory = open_dir(&artifacts, &name)?;
            private(&directory, true)?;
            match read(&directory, "state", STATE_LIMIT) {
                Ok(bytes) => {
                    let state = State::decode(&bytes)?;
                    if state.owner.is_some()
                        || state.identity != identity
                        || state.limits.validated()? != limits
                    {
                        return Err(conflict());
                    }
                    state.writable()?;
                    store.claim_identities(&identity)?;
                    store.sync(&directory)
                }
                Err(FilesystemError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                    store.claim_identities(&identity)?;
                    store.save(
                        &directory,
                        &State {
                            format: 1,
                            owner: None,
                            identity,
                            limits: limits.into(),
                            expires: now()? + STAGING_SECONDS,
                            phase: Phase::Staging,
                            blocks: 0,
                            bytes: 0,
                            cleanup_next: 0,
                        },
                    )
                }
                Err(error) => Err(error),
            }
        })
        .await
    }
    async fn append(
        &self,
        identity: &ArtifactIdentity,
        ordinal: u32,
        bytes: &[u8],
    ) -> ArtifactResult<()> {
        self.append_checked(identity, ordinal, bytes, None).await
    }
    async fn seal(&self, manifest: &ArtifactManifest) -> ArtifactResult<()> {
        self.seal_checked(manifest, None).await
    }
    async fn manifest(&self, identity: &ArtifactIdentity) -> ArtifactResult<Vec<u8>> {
        let identity = identity.clone();
        self.run(move |store| {
            let (directory, state) = store.state(&identity)?;
            store.manifest_on(&directory, &state)
        })
        .await
    }
    async fn published_manifest(&self, reference: &ArtifactReference) -> ArtifactResult<Vec<u8>> {
        let reference = reference.clone();
        self.run(move |store| {
            let (directory, state) = store.state(reference.identity())?;
            if state.phase != Phase::Published {
                return Err(conflict());
            }
            let bytes = store.manifest_on(&directory, &state)?;
            let manifest = ArtifactManifest::decode(
                &bytes,
                reference.identity(),
                ArtifactLimits::format_ceiling(),
            )?;
            if &manifest.manifest_digest() != reference.manifest_digest() {
                return Err(conflict());
            }
            Ok(bytes)
        })
        .await
    }
    async fn block(
        &self,
        identity: &ArtifactIdentity,
        ordinal: u32,
    ) -> ArtifactResult<Option<Vec<u8>>> {
        if ordinal > ArtifactLimits::format_ceiling().blocks() {
            return Err(ArtifactError::Limit(
                "Artifact ordinal exceeds format bounds".into(),
            ));
        }
        let identity = identity.clone();
        self.run(move |store| {
            let (directory, state) = store.state(&identity)?;
            state.readable()?;
            match read(
                &directory,
                &block_key(ordinal),
                ArtifactLimits::format_ceiling().block_bytes() as usize,
            ) {
                Ok(bytes) => Ok(Some(bytes)),
                Err(FilesystemError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                    Ok(None)
                }
                Err(error) => Err(error),
            }
        })
        .await
    }
    async fn publish(&self, verified: &VerifiedArtifact) -> ArtifactResult<ArtifactReference> {
        self.publish_checked(verified, None).await
    }
    async fn abort(&self, identity: &ArtifactIdentity) -> ArtifactResult<()> {
        self.abort_checked(identity, None).await
    }
    async fn cleanup(&self) -> ArtifactResult<u32> {
        self.run(move |store| {
            let artifacts = open_dir(&store.root, "artifacts")?;
            private(&artifacts, true)?;
            // Streaming enumeration keeps memory bounded. Tombstones remain to
            // fence delayed retries; large catalogs should be partitioned by root.
            for entry in Dir::read_from(&artifacts)? {
                let entry = entry?;
                let name = entry.file_name().to_bytes();
                if name == b"." || name == b".." {
                    continue;
                }
                if name.len() != 32 || !name.iter().all(u8::is_ascii_hexdigit) {
                    return Err(invalid());
                }
                let directory = open_dir(&artifacts, entry.file_name())?;
                private(&directory, true)?;
                let mut state: State = match read(&directory, "state", STATE_LIMIT) {
                    Ok(bytes) => State::decode(&bytes)?,
                    // Interrupted initial stage has no acknowledged attempt or blocks.
                    Err(FilesystemError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                        remove(&directory, "pending")?;
                        store.sync(&directory)?;
                        fs::unlinkat(&artifacts, entry.file_name(), AtFlags::REMOVEDIR)?;
                        store.sync(&artifacts)?;
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                // Bind the validated record to its directory before cleanup.
                if key(state.identity.artifact().as_bytes()).as_bytes() != name {
                    return Err(invalid());
                }
                if state.phase == Phase::Discarded || state.phase == Phase::Published {
                    continue;
                }
                if state.phase != Phase::Deleting
                    && (state.owner.is_some() || state.expires > now()?)
                {
                    continue;
                }
                state.phase = Phase::Deleting;
                store.save(&directory, &state)?;
                let mut removed = 0;
                let end = state.cleanup_next.saturating_add(16).min(state.blocks);
                for ordinal in state.cleanup_next..end {
                    removed += u32::from(remove(&directory, &block_key(ordinal))?);
                }
                store.sync(&directory)?;
                state.cleanup_next = end;
                if end == state.blocks {
                    // Also collect a block renamed just before a crashed append
                    // recorded its count. Never delete more than 16 physical blocks.
                    if removed < 16 {
                        removed += u32::from(remove(&directory, &block_key(state.blocks))?);
                        remove(&directory, "manifest")?;
                        remove(&directory, "pending")?;
                        store.sync(&directory)?;
                        state.phase = Phase::Discarded;
                    }
                }
                store.save(&directory, &state)?;
                return Ok(removed);
            }
            Ok(0)
        })
        .await
    }
}

#[cfg(test)]
mod tests;

impl FilesystemArtifactStore {
    async fn append_checked(
        &self,
        identity: &ArtifactIdentity,
        ordinal: u32,
        bytes: &[u8],
        artifact: Option<OwnedArtifact>,
    ) -> ArtifactResult<()> {
        if bytes.is_empty() || bytes.len() > ArtifactLimits::format_ceiling().block_bytes() as usize
        {
            return Err(ArtifactError::Limit(
                "Artifact block exceeds format bounds".into(),
            ));
        }
        let identity = identity.clone();
        let bytes = bytes.to_vec();
        self.run(move |store| {
            let (directory, mut state) = store.state(&identity)?;
            store.authorize(&state, artifact.as_ref())?;
            state.writable()?;
            let name = block_key(ordinal);
            if ordinal < state.blocks {
                if read(&directory, &name, state.limits.block as usize)? != bytes {
                    return Err(conflict());
                }
                return store.sync(&directory);
            }
            if ordinal != state.blocks
                || ordinal >= state.limits.blocks
                || bytes.len() > state.limits.block as usize
                || state.bytes != u64::from(state.blocks) * u64::from(state.limits.block)
                || state.bytes + bytes.len() as u64 > state.limits.bytes
                || 117 + (ordinal + 1) * 36 > state.limits.manifest
            {
                return Err(conflict());
            }
            // A crash may have renamed the next block before updating state.
            match read(&directory, &name, state.limits.block as usize) {
                Ok(old) if old != bytes => return Err(conflict()),
                Ok(_) => store.sync(&directory)?,
                Err(FilesystemError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                    store.atomic(&directory, &name, &bytes)?
                }
                Err(error) => return Err(error),
            }
            state.blocks += 1;
            state.bytes += bytes.len() as u64;
            store.save(&directory, &state)
        })
        .await
    }
    async fn seal_checked(
        &self,
        manifest: &ArtifactManifest,
        artifact: Option<OwnedArtifact>,
    ) -> ArtifactResult<()> {
        let manifest = manifest.clone();
        self.run(move |store| {
            let (directory, mut state) = store.state(manifest.identity())?;
            store.authorize(&state, artifact.as_ref())?;
            if matches!(state.phase, Phase::Sealed | Phase::Published) {
                if store.manifest_on(&directory, &state)? != manifest.encode() {
                    return Err(conflict());
                }
                return store.sync(&directory);
            }
            state.writable()?;
            let bytes = manifest.encode();
            ArtifactManifest::decode(&bytes, manifest.identity(), state.limits.validated()?)?;
            if manifest.block_bytes() != state.limits.block
                || manifest.block_count() != state.blocks
                || manifest.byte_size() != state.bytes
            {
                return Err(conflict());
            }
            for ordinal in 0..state.blocks {
                let block = open_file(&directory, &block_key(ordinal), OFlags::RDONLY)?;
                let expected = (state.bytes - u64::from(ordinal) * u64::from(state.limits.block))
                    .min(u64::from(state.limits.block));
                if block.metadata()?.len() != expected {
                    return Err(conflict());
                }
            }
            match open_file(&directory, &block_key(state.blocks), OFlags::RDONLY) {
                Ok(_) => return Err(conflict()),
                Err(FilesystemError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
            store.atomic(&directory, "manifest", &bytes)?;
            state.phase = Phase::Sealed;
            store.save(&directory, &state)
        })
        .await
    }
    async fn publish_checked(
        &self,
        verified: &VerifiedArtifact,
        artifact: Option<OwnedArtifact>,
    ) -> ArtifactResult<ArtifactReference> {
        let manifest = verified.manifest().clone();
        let reference = verified.reference();
        self.run(move |store| {
            let (directory, mut state) = store.state(manifest.identity())?;
            store.authorize(&state, artifact.as_ref())?;
            if store.manifest_on(&directory, &state)? != manifest.encode() {
                return Err(conflict());
            }
            state.phase = Phase::Published;
            store.save(&directory, &state)?;
            Ok(reference)
        })
        .await
    }
    async fn abort_checked(
        &self,
        identity: &ArtifactIdentity,
        artifact: Option<OwnedArtifact>,
    ) -> ArtifactResult<()> {
        let identity = identity.clone();
        self.run(move |store| {
            let (directory, mut state) = store.state(&identity)?;
            store.authorize(&state, artifact.as_ref())?;
            if state.phase == Phase::Published {
                return Err(conflict());
            }
            if state.phase != Phase::Discarded {
                state.phase = Phase::Deleting;
            }
            store.save(&directory, &state)
        })
        .await
    }
    fn scope(&self) -> Result<StoreScope> {
        let metadata = self.root.metadata()?;
        Ok(StoreScope::Filesystem {
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
    fn authorize(&self, state: &State, artifact: Option<&OwnedArtifact>) -> Result<()> {
        authorize(state.owner.as_ref(), &self.scope()?, artifact)?;
        Ok(())
    }
    fn owned_resolution(&self, state: &State) -> Result<OwnedArtifactResolution> {
        let Some(intent) = &state.owner else {
            return Err(conflict());
        };
        if state.phase == Phase::Preparing {
            return Ok(OwnedArtifactResolution::Pending);
        }
        // A previous state rename may have succeeded before its directory sync
        // failed. Re-establish durability before issuing a recovered receipt.
        let (directory, _) = self.state(&state.identity)?;
        self.sync(&directory)?;
        let phase = match state.phase {
            Phase::Preparing => unreachable!(),
            Phase::Staging => OwnedArtifactPhase::Staging,
            Phase::Sealed => OwnedArtifactPhase::Sealed,
            Phase::Published => {
                let (directory, _) = self.state(&state.identity)?;
                let bytes = self.manifest_on(&directory, state)?;
                let manifest = ArtifactManifest::decode(&bytes, &state.identity, intent.limits())?;
                OwnedArtifactPhase::Published(ArtifactReference::from_catalog(
                    state.identity.clone(),
                    manifest.manifest_digest(),
                ))
            }
            Phase::Deleting | Phase::Discarded => OwnedArtifactPhase::Fenced,
        };
        Ok(OwnedArtifactResolution::Established {
            artifact: OwnedArtifact::checked(intent.clone(), self.scope()?),
            phase,
        })
    }
}

#[async_trait]
impl OwnedArtifactStore for FilesystemArtifactStore {
    type Receipt = OwnedArtifact;
    async fn create_owned(
        &self,
        intent: &ArtifactCreationIntent,
    ) -> OwnedArtifactResult<OwnedArtifactAdmission> {
        let intent = intent.clone();
        self.run(move |store| {
            let identity = intent.identity();
            if !store
                .destinations()?
                .iter()
                .any(|(id, _)| *id == identity.destination())
            {
                return Err(conflict());
            }
            let artifacts = open_dir(&store.root, "artifacts")?;
            private(&artifacts, true)?;
            let name = key(identity.artifact().as_bytes());
            // Check all retained uniqueness claims before allocating a new attempt.
            store.check_identities(identity)?;
            match fs::mkdirat(&artifacts, &name, Mode::RWXU) {
                Ok(()) | Err(rustix::io::Errno::EXIST) => {}
                Err(error) => return Err(error.into()),
            }
            store.sync(&artifacts)?;
            let directory = open_dir(&artifacts, &name)?;
            private(&directory, true)?;
            let (mut state, created) = match read(&directory, "state", STATE_LIMIT) {
                Ok(bytes) => {
                    let state = State::decode(&bytes)?;
                    if state.owner.as_ref() != Some(&intent) {
                        return Err(conflict());
                    }
                    (state, false)
                }
                Err(FilesystemError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                    let state = State {
                        format: 2,
                        owner: Some(intent.clone()),
                        identity: identity.clone(),
                        limits: intent.limits().into(),
                        expires: now()?,
                        phase: Phase::Preparing,
                        blocks: 0,
                        bytes: 0,
                        cleanup_next: 0,
                    };
                    // Commit ownership before further identity claims or data I/O.
                    store.save(&directory, &state)?;
                    (state, true)
                }
                Err(error) => return Err(error),
            };
            if state.phase == Phase::Preparing {
                store.claim_identities(identity)?;
                state.phase = Phase::Staging;
                store.save(&directory, &state)?;
            }
            match store.owned_resolution(&state)? {
                OwnedArtifactResolution::Established { artifact, phase } => {
                    if created {
                        Ok(OwnedArtifactAdmission::Created(artifact))
                    } else {
                        Ok(OwnedArtifactAdmission::ResumedSameOwner { artifact, phase })
                    }
                }
                OwnedArtifactResolution::Pending => Err(conflict()),
            }
        })
        .await
        .map_err(Into::into)
    }
    async fn resolve_owned(
        &self,
        intent: &ArtifactCreationIntent,
    ) -> OwnedArtifactResult<OwnedArtifactResolution> {
        let intent = intent.clone();
        self.run(move |store| {
            let (_, state) = match store.state(intent.identity()) {
                Ok(value) => value,
                Err(FilesystemError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                    return Ok(OwnedArtifactResolution::Pending);
                }
                Err(error) => return Err(error),
            };
            if state.owner.as_ref() != Some(&intent) {
                return Err(conflict());
            }
            store.owned_resolution(&state)
        })
        .await
        .map_err(Into::into)
    }
    async fn append_owned(
        &self,
        artifact: &OwnedArtifact,
        ordinal: u32,
        bytes: &[u8],
    ) -> OwnedArtifactResult<()> {
        self.append_checked(
            artifact.intent().identity(),
            ordinal,
            bytes,
            Some(artifact.clone()),
        )
        .await
        .map_err(Into::into)
    }
    async fn seal_owned(
        &self,
        artifact: &OwnedArtifact,
        manifest: &ArtifactManifest,
    ) -> OwnedArtifactResult<()> {
        self.seal_checked(manifest, Some(artifact.clone()))
            .await
            .map_err(Into::into)
    }
    async fn publish_owned(
        &self,
        artifact: &OwnedArtifact,
        verified: &VerifiedArtifact,
    ) -> OwnedArtifactResult<ArtifactReference> {
        self.publish_checked(verified, Some(artifact.clone()))
            .await
            .map_err(Into::into)
    }
    async fn abort_owned(&self, artifact: &OwnedArtifact) -> OwnedArtifactResult<()> {
        self.abort_checked(artifact.intent().identity(), Some(artifact.clone()))
            .await
            .map_err(Into::into)
    }
}
