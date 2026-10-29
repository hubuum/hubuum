//! Disposable, bounded replay input for isolated template execution.
//! Backup capture continues to use StorageBackupReplayBuilder; template rows
//! have a different representation and need indexed access for Jinja sequences.
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use hubuum_budget::{ByteLimit, LimitedWriter};
use minijinja::value::{Enumerator, Object, ObjectRepr, Value};
use serde::{Deserialize, Serialize};
mod file;
pub(crate) use file::SpoolFile;
use file::read_exact_at;

use crate::{TemplateError, json_value_to_template_value};

pub const MAX_SPOOL_ROW_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_SPOOL_BYTES: u64 = 64 * 1024 * 1024 * 1024;

fn failure(error: impl std::fmt::Display) -> TemplateError {
    TemplateError::boundary(&format!("Template spool failed: {error}"))
}

/// A private temporary row file and disk index. Rows are encoded directly into
/// the file through a limit; neither the full dataset nor its index lives in RAM.
/// A failed append poisons the spool so a partial prefix cannot be rendered.
#[derive(Debug)]
pub struct TemplateRows {
    data: SpoolFile,
    index: SpoolFile,
    bytes: u64,
    rows: u64,
    limit: u64,
    failed: bool,
}
impl TemplateRows {
    pub fn new(limit: u64) -> Result<Self, TemplateError> {
        if limit == 0 || limit > MAX_SPOOL_BYTES {
            return Err(failure("invalid byte limit"));
        }
        Ok(Self {
            data: SpoolFile::new().map_err(failure)?,
            index: SpoolFile::new().map_err(failure)?,
            bytes: 0,
            rows: 0,
            limit,
            failed: false,
        })
    }

    pub fn push(&mut self, row: &serde_json::Value) -> Result<(), TemplateError> {
        if self.failed {
            return Err(failure("previous append failed"));
        }
        self.failed = true;
        // The fixed-size index is charged before writing a row.
        let available = self
            .limit
            .checked_sub(self.resident_bytes())
            .and_then(|value| value.checked_sub(16))
            .ok_or_else(|| failure("byte limit exceeded"))?;
        let mut writer = LimitedWriter::new(
            self.data.as_file_mut(),
            ByteLimit::new(available.min(MAX_SPOOL_ROW_BYTES as u64) as usize),
        );
        serde_json::to_writer(&mut writer, row).map_err(failure)?;
        writer.flush().map_err(failure)?;
        let size = writer.budget().used() as u64;
        self.index
            .as_file_mut()
            .write_all(&self.bytes.to_le_bytes())
            .map_err(failure)?;
        self.index
            .as_file_mut()
            .write_all(&size.to_le_bytes())
            .map_err(failure)?;
        self.index.as_file_mut().flush().map_err(failure)?;
        self.bytes += size;
        self.rows += 1;
        self.failed = false;
        Ok(())
    }

    #[cfg(unix)]
    pub(crate) fn raw_fds(&self) -> [std::os::fd::RawFd; 2] {
        [self.data.raw_fd(), self.index.raw_fd()]
    }

    pub fn len(&self) -> u64 {
        self.rows
    }
    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }
    pub fn resident_bytes(&self) -> u64 {
        self.bytes + self.rows * 16
    }

    pub(crate) fn descriptor(&self) -> Result<RowFiles, TemplateError> {
        if self.failed {
            return Err(failure("previous append failed"));
        }
        Ok(RowFiles {
            data: self.data.path().to_owned(),
            index: self.index.path().to_owned(),
            bytes: self.bytes,
            rows: self.rows,
        })
    }

    /// Read one validated row. Used for bounded JSON output as well as templates.
    pub fn row(&self, index: u64) -> Result<serde_json::Value, TemplateError> {
        self.descriptor()?.open()?.read(index).map_err(failure)
    }
}

/// Successful isolated rendering retained as an owned, disposable file.
/// Publication must still independently verify the destination after copying.
pub struct RenderedSpool {
    pub(crate) file: Arc<SpoolFile>,
    pub(crate) bytes: u64,
    pub(crate) missing: Vec<crate::MissingValue>,
    pub(crate) peak_heap_bytes: usize,
}
impl RenderedSpool {
    pub fn byte_size(&self) -> u64 {
        self.bytes
    }
    pub fn peak_heap_bytes(&self) -> usize {
        self.peak_heap_bytes
    }
    pub fn missing_values(&self) -> &[crate::MissingValue] {
        &self.missing
    }
    pub fn copy_to(&self, writer: &mut dyn Write) -> Result<(), TemplateError> {
        let file = self.file.as_file();
        if file.metadata().map_err(failure)?.len() != self.bytes {
            return Err(failure("rendered output length changed"));
        }
        let mut offset = 0;
        let mut buffer = [0; 64 * 1024];
        while offset < self.bytes {
            let count = (self.bytes - offset).min(buffer.len() as u64) as usize;
            read_exact_at(file, &mut buffer[..count], offset).map_err(failure)?;
            writer.write_all(&buffer[..count]).map_err(failure)?;
            offset += count as u64;
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct RowFiles {
    data: PathBuf,
    index: PathBuf,
    bytes: u64,
    rows: u64,
}
impl RowFiles {
    pub(crate) fn open(&self) -> Result<Arc<ReplayRows>, TemplateError> {
        let index_bytes = self
            .rows
            .checked_mul(16)
            .ok_or_else(|| failure("index overflow"))?;
        if self
            .bytes
            .checked_add(index_bytes)
            .is_none_or(|size| size > MAX_SPOOL_BYTES)
        {
            return Err(failure("input exceeds byte limit"));
        }
        let data = OpenOptions::new()
            .read(true)
            .open(&self.data)
            .map_err(failure)?;
        let index = OpenOptions::new()
            .read(true)
            .open(&self.index)
            .map_err(failure)?;
        if data.metadata().map_err(failure)?.len() != self.bytes
            || index.metadata().map_err(failure)?.len() != index_bytes
        {
            return Err(failure("input or index length changed"));
        }
        Ok(Arc::new(ReplayRows {
            files: (data, index),
            rows: self.rows,
            bytes: self.bytes,
            failed: AtomicBool::new(false),
        }))
    }
}

#[derive(Debug)]
pub(crate) struct ReplayRows {
    files: (File, File),
    rows: u64,
    bytes: u64,
    failed: AtomicBool,
}
impl ReplayRows {
    fn read(&self, ordinal: u64) -> io::Result<serde_json::Value> {
        if ordinal >= self.rows {
            return Err(io::Error::other("row outside spool"));
        }
        let (data, index) = &self.files;
        let mut entry = [0; 16];
        read_exact_at(index, &mut entry, ordinal * 16)?;
        let offset = u64::from_le_bytes(entry[..8].try_into().unwrap());
        let length = u64::from_le_bytes(entry[8..].try_into().unwrap());
        if length > MAX_SPOOL_ROW_BYTES as u64
            || offset
                .checked_add(length)
                .is_none_or(|end| end > self.bytes)
        {
            return Err(io::Error::other("invalid spool row extent"));
        }
        let mut bytes = vec![0; length as usize];
        read_exact_at(data, &mut bytes, offset)?;
        serde_json::from_slice(&bytes).map_err(io::Error::other)
    }
    pub(crate) fn failed(&self) -> bool {
        self.failed.load(Ordering::Relaxed)
    }
}
impl Object for ReplayRows {
    fn repr(self: &Arc<Self>) -> ObjectRepr {
        ObjectRepr::Seq
    }
    fn enumerate(self: &Arc<Self>) -> Enumerator {
        Enumerator::Seq(self.rows as usize)
    }
    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        let index = key.as_usize()? as u64;
        if index >= self.rows {
            return None;
        }
        match self.read(index) {
            Ok(value) => Some(json_value_to_template_value(&value)),
            Err(_) => {
                self.failed.store(true, Ordering::Relaxed);
                None
            }
        }
    }
}

impl Serialize for TemplateRows {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::{Error, SerializeSeq};
        let replay = self
            .descriptor()
            .and_then(|files| files.open())
            .map_err(S::Error::custom)?;
        let mut sequence = serializer.serialize_seq(Some(self.rows as usize))?;
        for index in 0..self.rows {
            sequence.serialize_element(&replay.read(index).map_err(S::Error::custom)?)?;
        }
        sequence.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use serde_json::json;

    #[test]
    fn replay_preserves_order_without_retaining_rows() {
        let mut rows = TemplateRows::new(1024).unwrap();
        rows.push(&json!({"id": 2})).unwrap();
        rows.push(&json!({"id": 1})).unwrap();
        assert_eq!(
            serde_json::to_value(&rows).unwrap(),
            json!([{"id": 2}, {"id": 1}])
        );
    }

    #[rstest]
    #[case::exact(24, true)]
    #[case::one_short(23, false)]
    fn row_and_index_share_the_resident_limit(#[case] limit: u64, #[case] accepted: bool) {
        let mut rows = TemplateRows::new(limit).unwrap();
        assert_eq!(rows.push(&json!({"id": 1})).is_ok(), accepted);
    }

    #[rstest]
    #[case::exact(MAX_SPOOL_ROW_BYTES - 2, true)]
    #[case::one_over(MAX_SPOOL_ROW_BYTES - 1, false)]
    fn encoded_row_limit_includes_json_framing(#[case] text_bytes: usize, #[case] accepted: bool) {
        let mut rows = TemplateRows::new((MAX_SPOOL_ROW_BYTES + 32) as u64).unwrap();
        assert_eq!(rows.push(&json!("x".repeat(text_bytes))).is_ok(), accepted);
    }

    #[test]
    fn failed_append_cannot_render_a_shorter_success() {
        let mut rows = TemplateRows::new(24).unwrap();
        rows.push(&json!({"id": 1})).unwrap();
        assert!(rows.push(&json!({"id": 2})).is_err());
        assert!(serde_json::to_value(&rows).is_err());
    }

    #[test]
    fn truncated_local_spool_fails_closed() {
        let mut rows = TemplateRows::new(1024).unwrap();
        rows.push(&json!(1)).unwrap();
        rows.data.as_file().set_len(0).unwrap();
        assert!(rows.row(0).is_err());
    }
}
