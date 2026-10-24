//! Immutable exact artifact bytes, independently replayable without a heap copy.
use crate::{StorageError, StorageRestoreArtifactSummary};
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};

fn io_error(error: impl std::fmt::Display) -> StorageError {
    StorageError::internal(format!("Restore artifact I/O failed: {error}"))
}

pub struct StorageRestoreArtifactBuilder {
    writer: BufWriter<File>,
    hash: Sha256,
    size: usize,
    limit: usize,
}
impl StorageRestoreArtifactBuilder {
    pub fn new(limit: usize) -> Result<Self, StorageError> {
        Ok(Self {
            writer: BufWriter::new(tempfile::tempfile().map_err(io_error)?),
            hash: Sha256::new(),
            size: 0,
            limit,
        })
    }
    pub fn push(&mut self, bytes: &[u8]) -> Result<(), StorageError> {
        if bytes.len() > self.limit.saturating_sub(self.size) {
            return Err(StorageError::input_too_large(format!(
                "Restore artifact exceeds {} bytes",
                self.limit
            )));
        }
        self.writer.write_all(bytes).map_err(io_error)?;
        self.hash.update(bytes);
        self.size += bytes.len();
        Ok(())
    }
    pub fn finish(mut self) -> Result<StorageRestoreArtifact, StorageError> {
        let size = i64::try_from(self.size)
            .map_err(|_| StorageError::input_too_large("Restore artifact size overflow"))?;
        let hash = self
            .hash
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let summary = StorageRestoreArtifactSummary::try_new(size, hash)
            .map_err(|e| e.into_request_error())?;
        self.writer.flush().map_err(io_error)?;
        Ok(StorageRestoreArtifact {
            file: Arc::new(Mutex::new(self.writer.into_inner().map_err(io_error)?)),
            summary,
        })
    }
}
#[derive(Clone, Debug)]
pub struct StorageRestoreArtifact {
    file: Arc<Mutex<File>>,
    summary: StorageRestoreArtifactSummary,
}
impl StorageRestoreArtifact {
    pub fn from_reader(mut reader: impl Read, limit: usize) -> Result<Self, StorageError> {
        let mut builder = StorageRestoreArtifactBuilder::new(limit)?;
        let mut bytes = vec![0; 64 * 1024];
        loop {
            let count = reader.read(&mut bytes).map_err(io_error)?;
            if count == 0 {
                break;
            }
            builder.push(&bytes[..count])?;
        }
        builder.finish()
    }
    pub fn summary(&self) -> &StorageRestoreArtifactSummary {
        &self.summary
    }
    pub fn reader(&self) -> impl Read + Send + 'static {
        BufReader::new(ArtifactReader {
            file: self.file.clone(),
            position: 0,
            size: self.summary.byte_size() as u64,
        })
    }
}
struct ArtifactReader {
    file: Arc<Mutex<File>>,
    position: u64,
    size: u64,
}
impl Read for ArtifactReader {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let count = bytes.len().min((self.size - self.position) as usize);
        if count == 0 {
            return Ok(0);
        }
        let mut file = self
            .file
            .lock()
            .map_err(|_| io::Error::other("Restore artifact lock poisoned"))?;
        file.seek(SeekFrom::Start(self.position))?;
        let count = file.read(&mut bytes[..count])?;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Restore artifact truncated",
            ));
        }
        self.position += count as u64;
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn artifact_replays_exact_bytes_from_independent_cursors() {
        let bytes = b"  {\"unicode\":\"\\u96ea\"}\n";
        let artifact = StorageRestoreArtifact::from_reader(bytes.as_slice(), bytes.len()).unwrap();
        let mut first = artifact.reader();
        let mut prefix = [0; 3];
        first.read_exact(&mut prefix).unwrap();
        let mut actual = Vec::new();
        artifact.reader().read_to_end(&mut actual).unwrap();
        assert_eq!(actual, bytes);
        let mut suffix = Vec::new();
        first.read_to_end(&mut suffix).unwrap();
        assert_eq!(suffix, bytes[3..]);
    }
    #[test]
    fn artifact_builder_stops_before_exceeding_its_limit() {
        let mut builder = StorageRestoreArtifactBuilder::new(3).unwrap();
        builder.push(b"abc").unwrap();
        assert_eq!(
            builder.push(b"d").unwrap_err().kind(),
            crate::StorageErrorKind::InputTooLarge
        );
    }
}
