use std::io::{self, Write};

use serde::Serialize;

use crate::{StorageBackupRow, StorageError, StorageValidationError};

/// Validated limits carried into every backup capture. Bytes bound compact
/// logical rows and the final artifact; rows bound enumeration work separately.
#[derive(Clone, Copy, Debug)]
pub struct StorageBackupBudget {
    max_bytes: usize,
    max_rows: usize,
}

impl StorageBackupBudget {
    pub fn new(max_bytes: usize, max_rows: usize) -> Result<Self, StorageValidationError> {
        if max_bytes == 0 || max_rows == 0 {
            return Err(StorageValidationError::invalid(
                "Backup byte and row limits must be greater than zero",
            ));
        }
        Ok(Self {
            max_bytes,
            max_rows,
        })
    }

    #[must_use]
    pub const fn max_bytes(self) -> usize {
        self.max_bytes
    }

    #[must_use]
    pub const fn max_rows(self) -> usize {
        self.max_rows
    }

    /// Serialize through a hard byte limit, including all document framing.
    /// No write can grow the artifact contents beyond the limit.
    pub fn serialize<T: Serialize>(self, value: &T) -> Result<Vec<u8>, StorageError> {
        let mut writer = LimitedWriter::new(Vec::new(), self.max_bytes);
        serde_json::to_writer(&mut writer, value).map_err(|error| {
            if writer.exceeded {
                StorageError::input_too_large(format!(
                    "Backup output exceeds the configured {} byte limit",
                    self.max_bytes
                ))
            } else {
                StorageError::internal(format!("Backup serialization failed: {error}"))
            }
        })?;
        Ok(writer.inner)
    }
}

/// Per-capture accounting. Adapters charge each enumerated row before retaining
/// it, and abort on the first error. Counters contain no resource contents.
#[derive(Debug)]
pub struct StorageBackupCaptureProgress {
    budget: StorageBackupBudget,
    scanned_rows: usize,
    retained_rows: usize,
    retained_bytes: usize,
}

impl StorageBackupCaptureProgress {
    #[must_use]
    pub const fn new(budget: StorageBackupBudget) -> Self {
        Self {
            budget,
            scanned_rows: 0,
            retained_rows: 0,
            retained_bytes: 0,
        }
    }

    pub fn scan_row(&mut self) -> Result<(), StorageError> {
        if self.scanned_rows == self.budget.max_rows {
            return Err(self.limit_error("row work"));
        }
        self.scanned_rows += 1;
        Ok(())
    }

    pub fn retain_row(&mut self, row: &StorageBackupRow) -> Result<(), StorageError> {
        let mut writer = LimitedWriter::new(io::sink(), self.remaining_bytes());
        serde_json::to_writer(&mut writer, row).map_err(|error| {
            if writer.exceeded {
                self.limit_error("logical byte")
            } else {
                StorageError::internal(format!("Backup row serialization failed: {error}"))
            }
        })?;
        self.retained_bytes += writer.written;
        self.retained_rows += 1;
        Ok(())
    }

    #[must_use]
    pub const fn remaining_bytes(&self) -> usize {
        self.budget.max_bytes - self.retained_bytes
    }

    #[must_use]
    pub const fn max_bytes(&self) -> usize {
        self.budget.max_bytes
    }

    #[must_use]
    pub const fn scanned_rows(&self) -> usize {
        self.scanned_rows
    }

    #[must_use]
    pub const fn retained_rows(&self) -> usize {
        self.retained_rows
    }

    #[must_use]
    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    #[must_use]
    pub fn limit_error(&self, resource: &str) -> StorageError {
        StorageError::input_too_large(format!(
            "Backup exceeds the configured {resource} limit ({} bytes, {} rows); scanned_rows={}, retained_rows={}, retained_bytes={}",
            self.budget.max_bytes,
            self.budget.max_rows,
            self.scanned_rows,
            self.retained_rows,
            self.retained_bytes
        ))
    }
}

struct LimitedWriter<W> {
    inner: W,
    remaining: usize,
    written: usize,
    exceeded: bool,
}

impl<W> LimitedWriter<W> {
    fn new(inner: W, remaining: usize) -> Self {
        Self {
            inner,
            remaining,
            written: 0,
            exceeded: false,
        }
    }
}

impl<W: Write> Write for LimitedWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.remaining {
            self.exceeded = true;
            return Err(io::Error::other("backup byte limit exceeded"));
        }
        let count = self.inner.write(bytes)?;
        self.remaining -= count;
        self.written += count;
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StorageErrorKind;
    use rstest::rstest;
    use serde_json::json;

    #[rstest]
    #[case::bytes(0, 1)]
    #[case::rows(1, 0)]
    fn budgets_reject_zero_limits(#[case] bytes: usize, #[case] rows: usize) {
        assert!(StorageBackupBudget::new(bytes, rows).is_err());
    }

    #[rstest]
    #[case::exact(10, true)]
    #[case::too_small(9, false)]
    fn artifact_limit_counts_escaping_and_framing(#[case] bytes: usize, #[case] accepted: bool) {
        let result = StorageBackupBudget::new(bytes, 1)
            .unwrap()
            .serialize(&json!({"x": "\n"}));
        assert_eq!(result.is_ok(), accepted);
        if let Ok(bytes) = result {
            assert_eq!(bytes, br#"{"x":"\n"}"#);
        }
    }

    #[rstest]
    #[case::bytes(14, 100)]
    #[case::rows(100, 2)]
    fn capture_stops_before_retaining_an_excess_row(#[case] bytes: usize, #[case] rows: usize) {
        let mut progress =
            StorageBackupCaptureProgress::new(StorageBackupBudget::new(bytes, rows).unwrap());
        let row = StorageBackupRow::try_from_value(json!({"x": 1})).unwrap();
        let mut projected = 0;
        let result: Result<Vec<_>, StorageError> = (0..100)
            .map(|_| {
                progress.scan_row()?;
                projected += 1;
                progress.retain_row(&row)?;
                Ok(row.clone())
            })
            .collect();
        assert_eq!(result.unwrap_err().kind(), StorageErrorKind::InputTooLarge);
        assert!(projected <= 3);
        assert_eq!(progress.retained_rows(), 2);
        assert_eq!(progress.retained_bytes(), 14);
    }
}
