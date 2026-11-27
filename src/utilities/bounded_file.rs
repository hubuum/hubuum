use std::fs::{self, File};
use std::io::{self, Read};
use std::path::Path;

pub(crate) const MAX_CERTIFICATE_BUNDLE_BYTES: usize = 4 * 1024 * 1024;
pub(crate) const MAX_PRIVATE_KEY_BYTES: usize = 1024 * 1024;

pub(crate) fn read_bounded_regular_file(
    path: &Path,
    description: &str,
    max_bytes: usize,
) -> io::Result<Vec<u8>> {
    let max_bytes_u64 = u64::try_from(max_bytes).unwrap_or(u64::MAX);

    let (file, file_metadata) = open_bounded_file(path, description, max_bytes, max_bytes_u64)
        .map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("Failed to open {description} {path:?}: {error}"),
            )
        })?;
    let mut bytes = Vec::with_capacity(file_metadata.len() as usize);
    file.take(max_bytes_u64.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("Failed to read {description} {path:?}: {error}"),
            )
        })?;
    if bytes.len() > max_bytes {
        return Err(size_error(path, description, max_bytes));
    }
    Ok(bytes)
}

#[cfg(unix)]
fn open_bounded_file(
    path: &Path,
    description: &str,
    max_bytes: usize,
    max_bytes_u64: u64,
) -> io::Result<(File, fs::Metadata)> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

    // Resolve configured symlinks once, then compare the inspected identity to
    // the open descriptor. Nonblocking open also handles replacement of a
    // regular file by a special file without stalling the caller.
    let resolved = fs::canonicalize(path)?;
    let expected = fs::metadata(&resolved)?;
    validate_metadata(path, description, max_bytes, max_bytes_u64, &expected)?;
    let file = File::options()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(resolved)?;
    let opened = file.metadata()?;
    if (expected.dev(), expected.ino()) != (opened.dev(), opened.ino()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{description} {path:?} changed while it was opened"),
        ));
    }
    validate_metadata(path, description, max_bytes, max_bytes_u64, &opened)?;
    Ok((file, opened))
}

#[cfg(not(unix))]
fn open_bounded_file(
    path: &Path,
    description: &str,
    max_bytes: usize,
    max_bytes_u64: u64,
) -> io::Result<(File, fs::Metadata)> {
    // Let the OS follow any configured link in a single open. Inspect and
    // later read that same handle, so that replacing the pathname cannot
    // change the file between validation and consumption.
    let file = File::open(path)?;
    let opened = file.metadata()?;
    validate_metadata(path, description, max_bytes, max_bytes_u64, &opened)?;
    Ok((file, opened))
}

fn validate_metadata(
    path: &Path,
    description: &str,
    max_bytes: usize,
    max_bytes_u64: u64,
    metadata: &fs::Metadata,
) -> io::Result<()> {
    if !metadata.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{description} {path:?} must be a regular file"),
        ));
    }
    if metadata.len() > max_bytes_u64 {
        return Err(size_error(path, description, max_bytes));
    }
    Ok(())
}

fn size_error(path: &Path, description: &str, max_bytes: usize) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("{description} {path:?} exceeds the {max_bytes}-byte limit"),
    )
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use uuid::Uuid;

    use super::*;

    struct TestFile(PathBuf);

    impl TestFile {
        fn new(contents: &[u8]) -> Self {
            let path = std::env::temp_dir().join(format!("hubuum-bounded-file-{}", Uuid::new_v4()));
            fs::write(&path, contents).unwrap();
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TestFile {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
        }
    }

    #[test]
    fn reads_a_regular_file_at_the_limit() {
        let file = TestFile::new(b"1234");

        assert_eq!(
            read_bounded_regular_file(file.path(), "test material", 4).unwrap(),
            b"1234"
        );
    }

    #[test]
    fn rejects_a_regular_file_over_the_limit() {
        let file = TestFile::new(b"12345");

        let error = read_bounded_regular_file(file.path(), "test material", 4).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("exceeds the 4-byte limit"));
    }

    #[cfg(unix)]
    #[test]
    fn rejects_non_regular_files_before_opening_them() {
        let error =
            read_bounded_regular_file(Path::new("/dev/null"), "test material", 4).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("must be a regular file"));
    }

    #[cfg(unix)]
    #[test]
    fn follows_a_configured_symlink_to_a_regular_file() {
        let target = TestFile::new(b"secret");
        let link = std::env::temp_dir().join(format!("hubuum-bounded-link-{}", Uuid::new_v4()));
        std::os::unix::fs::symlink(target.path(), &link).unwrap();

        let result = read_bounded_regular_file(&link, "test material", 16);

        let _ = fs::remove_file(&link);
        assert_eq!(result.unwrap(), b"secret");
    }

    #[cfg(unix)]
    #[test]
    fn rejects_a_symlink_that_is_not_a_regular_file() {
        let link = std::env::temp_dir().join(format!("hubuum-bounded-link-{}", Uuid::new_v4()));
        std::os::unix::fs::symlink("/dev/null", &link).unwrap();

        let error = read_bounded_regular_file(&link, "test material", 4).unwrap_err();

        let _ = fs::remove_file(&link);
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }
}
