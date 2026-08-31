use std::collections::BTreeMap;
use std::collections::hash_map::RandomState;
use std::fmt;
use std::fs::{self, File, Metadata};
use std::io::{Read, Take};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;

use crate::provider::{ProviderSecret, ensure_provider, ensure_version, opaque_version};
use crate::{
    DEFAULT_MAX_SECRET_BYTES, SecretError, SecretErrorKind, SecretName, SecretProvider,
    SecretProviderKind, SecretRef, SecretValue,
};

const STABLE_READ_ATTEMPTS: usize = 3;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FileSymlinkPolicy {
    #[default]
    Reject,
    AllowWithinRoot,
}

pub struct FileProviderBuilder {
    kind: SecretProviderKind,
    root: PathBuf,
    paths: BTreeMap<SecretName, PathBuf>,
    path_prefix: PathBuf,
    max_bytes: usize,
    symlink_policy: FileSymlinkPolicy,
}

impl FileProviderBuilder {
    pub fn kind(mut self, kind: SecretProviderKind) -> Self {
        self.kind = kind;
        self
    }

    pub fn path(mut self, name: SecretName, path: impl Into<PathBuf>) -> Self {
        self.paths.insert(name, path.into());
        self
    }

    pub fn path_prefix(mut self, path_prefix: impl Into<PathBuf>) -> Self {
        self.path_prefix = path_prefix.into();
        self
    }

    pub fn max_bytes(mut self, max_bytes: usize) -> Self {
        self.max_bytes = max_bytes;
        self
    }

    pub fn symlink_policy(mut self, symlink_policy: FileSymlinkPolicy) -> Self {
        self.symlink_policy = symlink_policy;
        self
    }

    pub fn build(self) -> Result<FileProvider, SecretError> {
        if self.max_bytes == 0 {
            return Err(SecretError::new(
                SecretErrorKind::InvalidProviderConfiguration,
                "file secret size limit must be positive",
            ));
        }
        let root = fs::canonicalize(&self.root)
            .map_err(|error| map_io_error(error, "file secret root is unavailable"))?;
        if !fs::metadata(&root)
            .map_err(|error| map_io_error(error, "file secret root is unavailable"))?
            .is_dir()
        {
            return Err(SecretError::new(
                SecretErrorKind::UnsafePath,
                "file secret root must be a directory",
            ));
        }
        for path in self.paths.values() {
            validate_relative_path(path)?;
        }
        if !self.path_prefix.as_os_str().is_empty() {
            validate_relative_path(&self.path_prefix)?;
        }
        Ok(FileProvider {
            kind: self.kind,
            root,
            paths: self.paths,
            path_prefix: self.path_prefix,
            max_bytes: self.max_bytes,
            symlink_policy: self.symlink_policy,
            version_hasher: RandomState::new(),
        })
    }
}

pub struct FileProvider {
    kind: SecretProviderKind,
    root: PathBuf,
    paths: BTreeMap<SecretName, PathBuf>,
    path_prefix: PathBuf,
    max_bytes: usize,
    symlink_policy: FileSymlinkPolicy,
    version_hasher: RandomState,
}

impl FileProvider {
    pub fn builder(root: impl Into<PathBuf>) -> FileProviderBuilder {
        FileProviderBuilder {
            kind: SecretProviderKind::file(),
            root: root.into(),
            paths: BTreeMap::new(),
            path_prefix: PathBuf::new(),
            max_bytes: DEFAULT_MAX_SECRET_BYTES,
            symlink_policy: FileSymlinkPolicy::Reject,
        }
    }

    fn relative_path(&self, name: &SecretName) -> PathBuf {
        self.paths
            .get(name)
            .cloned()
            .unwrap_or_else(|| self.path_prefix.join(name.as_str()))
    }

    fn read(&self, reference: &SecretRef) -> Result<ProviderSecret, SecretError> {
        ensure_provider(reference, &self.kind)?;
        let relative = self.relative_path(reference.name());
        validate_relative_path(&relative)?;
        let candidate = self.root.join(&relative);

        for attempt in 0..STABLE_READ_ATTEMPTS {
            match self.read_once(&candidate, reference) {
                Err(error)
                    if error.kind() == SecretErrorKind::ChangedDuringRead
                        && attempt + 1 < STABLE_READ_ATTEMPTS => {}
                result => return result,
            }
        }
        unreachable!("the bounded stable-read loop always returns on its final attempt")
    }

    fn read_once(
        &self,
        candidate: &Path,
        reference: &SecretRef,
    ) -> Result<ProviderSecret, SecretError> {
        if self.symlink_policy == FileSymlinkPolicy::Reject {
            reject_symlink_components(&self.root, candidate)?;
        }

        let resolved = fs::canonicalize(candidate)
            .map_err(|error| map_io_error(error, "file secret is unavailable"))?;
        ensure_within_root(&self.root, &resolved)?;
        let path_metadata = fs::metadata(&resolved)
            .map_err(|error| map_io_error(error, "file secret is unavailable"))?;
        validate_regular_file(&path_metadata, self.max_bytes)?;

        let file = File::open(&resolved)
            .map_err(|error| map_io_error(error, "file secret is unavailable"))?;
        ensure_open_file_within_root(&self.root, &file)?;
        let before = file
            .metadata()
            .map_err(|error| map_io_error(error, "file secret is unavailable"))?;
        validate_regular_file(&before, self.max_bytes)?;

        let bytes = read_bounded_descriptor(file, &before, self.max_bytes)?;
        let current = fs::canonicalize(candidate).map_err(|_| changed_during_read())?;
        if current != resolved {
            return Err(changed_during_read());
        }

        let version = opaque_version(&self.version_hasher, &bytes)?;
        ensure_version(reference.version(), &version)?;
        Ok(ProviderSecret {
            value: Arc::new(SecretValue::new(bytes)?),
            version,
        })
    }
}

impl fmt::Debug for FileProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FileProvider")
            .field("kind", &self.kind)
            .field("root", &"<redacted>")
            .field("mapped_secret_count", &self.paths.len())
            .field("max_bytes", &self.max_bytes)
            .field("symlink_policy", &self.symlink_policy)
            .finish()
    }
}

#[async_trait]
impl SecretProvider for FileProvider {
    fn kind(&self) -> &SecretProviderKind {
        &self.kind
    }

    async fn resolve(&self, reference: &SecretRef) -> Result<ProviderSecret, SecretError> {
        self.read(reference)
    }

    async fn resolve_group(
        &self,
        references: &[SecretRef],
    ) -> Result<Vec<ProviderSecret>, SecretError> {
        for attempt in 0..STABLE_READ_ATTEMPTS {
            let first = references
                .iter()
                .map(|reference| self.read(reference))
                .collect::<Result<Vec<_>, _>>()?;
            let versions = references
                .iter()
                .map(|reference| self.read(reference).map(|value| value.version))
                .collect::<Result<Vec<_>, _>>()?;
            if first.iter().map(|value| &value.version).eq(versions.iter()) {
                return Ok(first);
            }
            if attempt + 1 == STABLE_READ_ATTEMPTS {
                return Err(changed_during_read());
            }
        }
        unreachable!("the bounded stable group-read loop always returns")
    }
}

fn validate_relative_path(path: &Path) -> Result<(), SecretError> {
    if path.as_os_str().is_empty()
        || !path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
    {
        return Err(SecretError::new(
            SecretErrorKind::UnsafePath,
            "file secret paths must be non-empty provider-relative paths without traversal",
        ));
    }
    Ok(())
}

fn reject_symlink_components(root: &Path, candidate: &Path) -> Result<(), SecretError> {
    let relative = candidate.strip_prefix(root).map_err(|_| {
        SecretError::new(
            SecretErrorKind::UnsafePath,
            "file secret path escapes the configured root",
        )
    })?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        current.push(component);
        let metadata = fs::symlink_metadata(&current)
            .map_err(|error| map_io_error(error, "file secret is unavailable"))?;
        if metadata.file_type().is_symlink() {
            return Err(SecretError::new(
                SecretErrorKind::UnsafePath,
                "file secret symlinks are disabled",
            ));
        }
    }
    Ok(())
}

fn ensure_within_root(root: &Path, resolved: &Path) -> Result<(), SecretError> {
    if !resolved.starts_with(root) || resolved == root {
        return Err(SecretError::new(
            SecretErrorKind::UnsafePath,
            "file secret path escapes the configured root",
        ));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn ensure_open_file_within_root(root: &Path, file: &File) -> Result<(), SecretError> {
    use std::os::fd::AsRawFd;

    let descriptor_path = PathBuf::from(format!("/proc/self/fd/{}", file.as_raw_fd()));
    let resolved = fs::canonicalize(descriptor_path).map_err(|_| changed_during_read())?;
    ensure_within_root(root, &resolved)
}

#[cfg(not(target_os = "linux"))]
fn ensure_open_file_within_root(_root: &Path, _file: &File) -> Result<(), SecretError> {
    Ok(())
}

fn validate_regular_file(metadata: &Metadata, max_bytes: usize) -> Result<(), SecretError> {
    if !metadata.is_file() {
        return Err(SecretError::new(
            SecretErrorKind::UnsafePath,
            "file secret must be an ordinary file",
        ));
    }
    if metadata.len() > max_bytes as u64 {
        return Err(SecretError::new(
            SecretErrorKind::TooLarge,
            "file secret exceeds the configured size limit",
        ));
    }
    Ok(())
}

fn read_bounded_descriptor(
    file: File,
    before: &Metadata,
    max_bytes: usize,
) -> Result<Vec<u8>, SecretError> {
    let mut bytes = Vec::with_capacity(before.len() as usize);
    let mut bounded: Take<File> = file.take((max_bytes as u64).saturating_add(1));
    bounded
        .read_to_end(&mut bytes)
        .map_err(|error| map_io_error(error, "file secret could not be read"))?;
    if bytes.len() > max_bytes {
        return Err(SecretError::new(
            SecretErrorKind::TooLarge,
            "file secret exceeds the configured size limit",
        ));
    }
    let file = bounded.into_inner();
    let after = file
        .metadata()
        .map_err(|error| map_io_error(error, "file secret is unavailable"))?;
    if metadata_changed(before, &after) {
        return Err(changed_during_read());
    }
    Ok(bytes)
}

fn metadata_changed(before: &Metadata, after: &Metadata) -> bool {
    if before.len() != after.len() || before.modified().ok() != after.modified().ok() {
        return true;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        before.dev() != after.dev() || before.ino() != after.ino()
    }
    #[cfg(not(unix))]
    {
        false
    }
}

fn changed_during_read() -> SecretError {
    SecretError::new(
        SecretErrorKind::ChangedDuringRead,
        "file secret changed while it was being read",
    )
}

fn map_io_error(error: std::io::Error, message: &'static str) -> SecretError {
    let kind = match error.kind() {
        std::io::ErrorKind::NotFound => SecretErrorKind::NotFound,
        std::io::ErrorKind::PermissionDenied => SecretErrorKind::PermissionDenied,
        std::io::ErrorKind::TimedOut => SecretErrorKind::Timeout,
        _ => SecretErrorKind::Unavailable,
    };
    SecretError::new(kind, message)
}

#[cfg(test)]
mod tests {
    use std::fs::{self, OpenOptions};
    use std::io::Write;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(1);

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "hubuum-secrets-{}-{}",
                std::process::id(),
                NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&path).unwrap();
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn reference(name: &str) -> SecretRef {
        SecretRef::new(SecretProviderKind::file(), SecretName::new(name).unwrap())
    }

    #[tokio::test]
    async fn reads_binary_secret_at_the_limit() {
        let directory = TestDirectory::new();
        fs::write(directory.path().join("binary"), [0, 1, 0xff, 2]).unwrap();
        let provider = FileProvider::builder(directory.path())
            .max_bytes(4)
            .build()
            .unwrap();

        let value = provider.resolve(&reference("binary")).await.unwrap();

        assert_eq!(value.value.expose(), [0, 1, 0xff, 2]);
    }

    #[test]
    fn descriptor_read_detects_growth_after_initial_metadata() {
        let directory = TestDirectory::new();
        let path = directory.path().join("growing");
        fs::write(&path, b"12").unwrap();
        let file = File::open(&path).unwrap();
        let before = file.metadata().unwrap();
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"3")
            .unwrap();

        assert_eq!(
            read_bounded_descriptor(file, &before, 4)
                .unwrap_err()
                .kind(),
            SecretErrorKind::ChangedDuringRead
        );
    }

    #[tokio::test]
    async fn rejects_oversized_and_non_regular_files() {
        let directory = TestDirectory::new();
        fs::write(directory.path().join("large"), b"12345").unwrap();
        fs::create_dir(directory.path().join("subdir")).unwrap();
        let provider = FileProvider::builder(directory.path())
            .max_bytes(4)
            .build()
            .unwrap();

        assert_eq!(
            provider
                .resolve(&reference("large"))
                .await
                .unwrap_err()
                .kind(),
            SecretErrorKind::TooLarge
        );
        assert_eq!(
            provider
                .resolve(&reference("subdir"))
                .await
                .unwrap_err()
                .kind(),
            SecretErrorKind::UnsafePath
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rejects_devices_fifos_and_sockets_without_opening_them() {
        use std::os::unix::net::UnixListener;

        let directory = TestDirectory::new();
        let fifo = directory.path().join("fifo");
        let fifo_path = std::ffi::CString::new(fifo.as_os_str().as_encoded_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(fifo_path.as_ptr(), 0o600) }, 0);
        let socket_path = directory.path().join("socket");
        let listener = UnixListener::bind(&socket_path).ok();
        let provider = FileProvider::builder(directory.path()).build().unwrap();

        assert_eq!(
            provider
                .resolve(&reference("fifo"))
                .await
                .unwrap_err()
                .kind(),
            SecretErrorKind::UnsafePath
        );
        if listener.is_some() {
            assert_eq!(
                provider
                    .resolve(&reference("socket"))
                    .await
                    .unwrap_err()
                    .kind(),
                SecretErrorKind::UnsafePath
            );
        }

        let device_provider = FileProvider::builder("/dev").build().unwrap();
        assert_eq!(
            device_provider
                .resolve(&reference("null"))
                .await
                .unwrap_err()
                .kind(),
            SecretErrorKind::UnsafePath
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn projected_secret_symlinks_are_explicit_and_confined() {
        use std::os::unix::fs::symlink;

        let directory = TestDirectory::new();
        let generation = directory.path().join("..2026_08_31");
        fs::create_dir(&generation).unwrap();
        fs::write(generation.join("password"), b"rotated-secret").unwrap();
        symlink("..2026_08_31", directory.path().join("..data")).unwrap();
        symlink("..data/password", directory.path().join("password")).unwrap();

        let strict = FileProvider::builder(directory.path()).build().unwrap();
        assert_eq!(
            strict
                .resolve(&reference("password"))
                .await
                .unwrap_err()
                .kind(),
            SecretErrorKind::UnsafePath
        );

        let projected = FileProvider::builder(directory.path())
            .symlink_policy(FileSymlinkPolicy::AllowWithinRoot)
            .build()
            .unwrap();
        assert_eq!(
            projected
                .resolve(&reference("password"))
                .await
                .unwrap()
                .value
                .expose(),
            b"rotated-secret"
        );

        let next_generation = directory.path().join("..2026_09_01");
        fs::create_dir(&next_generation).unwrap();
        fs::write(next_generation.join("password"), b"next-secret").unwrap();
        symlink("..2026_09_01", directory.path().join("..data-next")).unwrap();
        fs::rename(
            directory.path().join("..data-next"),
            directory.path().join("..data"),
        )
        .unwrap();
        assert_eq!(
            projected
                .resolve(&reference("password"))
                .await
                .unwrap()
                .value
                .expose(),
            b"next-secret"
        );

        fs::remove_file(directory.path().join("password")).unwrap();
        symlink("/etc/passwd", directory.path().join("password")).unwrap();
        assert_eq!(
            projected
                .resolve(&reference("password"))
                .await
                .unwrap_err()
                .kind(),
            SecretErrorKind::UnsafePath
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn projected_secret_groups_use_one_complete_generation() {
        use std::os::unix::fs::symlink;

        let directory = TestDirectory::new();
        let first_generation = directory.path().join("..2026_08_31");
        fs::create_dir(&first_generation).unwrap();
        fs::write(first_generation.join("username"), b"first-user").unwrap();
        fs::write(first_generation.join("password"), b"first-password").unwrap();
        symlink("..2026_08_31", directory.path().join("..data")).unwrap();
        symlink("..data/username", directory.path().join("username")).unwrap();
        symlink("..data/password", directory.path().join("password")).unwrap();
        let provider = FileProvider::builder(directory.path())
            .symlink_policy(FileSymlinkPolicy::AllowWithinRoot)
            .build()
            .unwrap();
        let references = [reference("username"), reference("password")];

        let first = provider.resolve_group(&references).await.unwrap();
        assert_eq!(first[0].value().expose(), b"first-user");
        assert_eq!(first[1].value().expose(), b"first-password");

        let second_generation = directory.path().join("..2026_09_01");
        fs::create_dir(&second_generation).unwrap();
        fs::write(second_generation.join("username"), b"second-user").unwrap();
        fs::write(second_generation.join("password"), b"second-password").unwrap();
        symlink("..2026_09_01", directory.path().join("..data-next")).unwrap();
        fs::rename(
            directory.path().join("..data-next"),
            directory.path().join("..data"),
        )
        .unwrap();

        let second = provider.resolve_group(&references).await.unwrap();
        assert_eq!(second[0].value().expose(), b"second-user");
        assert_eq!(second[1].value().expose(), b"second-password");
    }
}
