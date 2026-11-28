//! Kernel-owned temporary files: abrupt process exit must not leave named spools.
use std::fs::File;
#[cfg(windows)]
use std::fs::OpenOptions;
use std::io;
use std::path::PathBuf;

#[derive(Debug)]
pub(crate) struct SpoolFile {
    file: File,
    #[cfg(windows)]
    path: tempfile::TempPath,
}

impl SpoolFile {
    pub(crate) fn new() -> io::Result<Self> {
        #[cfg(unix)]
        {
            Ok(Self {
                file: tempfile::tempfile()?,
            })
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            // Unlike NamedTempFile's destructor, DELETE_ON_CLOSE also runs on
            // process termination. Default sharing permits the child to reopen.
            const FILE_FLAG_DELETE_ON_CLOSE: u32 = 0x04000000;
            let named = tempfile::Builder::new().make(|path| {
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .custom_flags(FILE_FLAG_DELETE_ON_CLOSE)
                    .open(path)
            })?;
            let (file, path) = named.into_parts();
            Ok(Self { file, path })
        }
    }
    pub(crate) fn as_file(&self) -> &File {
        &self.file
    }
    pub(crate) fn as_file_mut(&mut self) -> &mut File {
        &mut self.file
    }
    pub(crate) fn path(&self) -> PathBuf {
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            PathBuf::from(format!("/dev/fd/{}", self.file.as_raw_fd()))
        }
        #[cfg(windows)]
        {
            self.path.to_path_buf()
        }
    }
    #[cfg(unix)]
    pub(crate) fn raw_fd(&self) -> std::os::fd::RawFd {
        use std::os::fd::AsRawFd;
        self.file.as_raw_fd()
    }
}

/// Positional reads avoid shared offsets on macOS's inherited /dev/fd handles.
pub(crate) fn read_exact_at(file: &File, mut bytes: &mut [u8], mut offset: u64) -> io::Result<()> {
    while !bytes.is_empty() {
        #[cfg(unix)]
        let count = {
            use std::os::unix::fs::FileExt;
            file.read_at(bytes, offset)
        };
        #[cfg(windows)]
        let count = {
            use std::os::windows::fs::FileExt;
            file.seek_read(bytes, offset)
        };
        match count {
            Ok(0) => return Err(io::Error::from(io::ErrorKind::UnexpectedEof)),
            Ok(count) => {
                offset += count as u64;
                bytes = &mut bytes[count..];
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::process::Command;

    #[test]
    fn process_exit_reclaims_spools_without_running_destructors() {
        let directory = tempfile::tempdir().unwrap();
        let status = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "spool::file::tests::owner_exits_without_destructors",
                "--ignored",
            ])
            .env("TMPDIR", directory.path())
            .env("TMP", directory.path())
            .env("TEMP", directory.path())
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(73));
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }

    #[test]
    #[ignore = "subprocess fixture exits without Rust destructors"]
    fn owner_exits_without_destructors() {
        let mut file = SpoolFile::new().unwrap();
        file.as_file_mut().write_all(&[42; 1024 * 1024]).unwrap();
        assert_eq!(file.as_file().metadata().unwrap().len(), 1024 * 1024);
        std::process::exit(73);
    }
}
