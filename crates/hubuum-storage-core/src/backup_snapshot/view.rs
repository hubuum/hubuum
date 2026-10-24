//! Shared validation reads for materialized and replayable snapshots.

use std::borrow::Cow;
use std::fmt;

use hubuum_domain::JsonSchemaLimits;

use super::{
    StorageBackupHistorySection as History, StorageBackupRow, StorageBackupSnapshot,
    StorageBackupStateSection as State,
};
use crate::{StorageError, StorageValidationError};

#[derive(Debug)]
pub(super) enum ReadError {
    Validation(StorageValidationError),
    Read(StorageError),
}

impl ReadError {
    pub(super) fn invalid(message: impl Into<String>) -> Self {
        Self::Validation(StorageValidationError::invalid(message))
    }

    pub(super) fn into_validation(self) -> StorageValidationError {
        match self {
            Self::Validation(error) => error,
            // Materialized snapshots cannot produce read failures. Keep this
            // conversion fallible rather than turning an unexpected failure into a panic.
            Self::Read(error) => StorageValidationError::invalid(error.to_string()),
        }
    }

    pub(super) fn into_storage(self) -> StorageError {
        match self {
            Self::Validation(error) => error.into_request_error(),
            Self::Read(error) => error,
        }
    }
}

impl From<StorageValidationError> for ReadError {
    fn from(error: StorageValidationError) -> Self {
        Self::Validation(error)
    }
}

impl From<StorageError> for ReadError {
    fn from(error: StorageError) -> Self {
        Self::Read(error)
    }
}

impl fmt::Display for ReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Validation(error) => error.fmt(formatter),
            Self::Read(error) => error.fmt(formatter),
        }
    }
}

pub(super) type RowIter<'a> =
    Box<dyn Iterator<Item = Result<Cow<'a, StorageBackupRow>, ReadError>> + 'a>;

pub(super) struct Rows<'a> {
    count: usize,
    replay: Box<dyn Fn() -> RowIter<'a> + 'a>,
}

impl<'a> Rows<'a> {
    pub(super) fn new(count: usize, replay: impl Fn() -> RowIter<'a> + 'a) -> Self {
        Self {
            count,
            replay: Box::new(replay),
        }
    }

    pub(super) fn borrowed(rows: &'a [StorageBackupRow]) -> Self {
        Self::new(rows.len(), move || {
            Box::new(rows.iter().map(|row| Ok(Cow::Borrowed(row))))
        })
    }

    pub(super) fn len(&self) -> usize {
        self.count
    }

    pub(super) fn iter(&self) -> RowIter<'a> {
        (self.replay)()
    }
}

impl<'a> IntoIterator for Rows<'a> {
    type Item = Result<Cow<'a, StorageBackupRow>, ReadError>;
    type IntoIter = RowIter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

pub(super) trait BackupView {
    fn state_rows(&self, section: State) -> Result<Rows<'_>, ReadError>;
    fn history_rows(&self, section: History) -> Result<Rows<'_>, ReadError>;
    fn has_state(&self, section: State) -> bool;
    fn includes_history(&self) -> bool;
    fn schema_limits(&self) -> JsonSchemaLimits;
}

impl BackupView for StorageBackupSnapshot {
    fn state_rows(&self, section: State) -> Result<Rows<'_>, ReadError> {
        self.state_sections
            .get(&section)
            .map(|rows| Rows::borrowed(rows))
            .ok_or_else(|| {
                ReadError::invalid(format!(
                    "Backup snapshot is missing required state section '{section}'"
                ))
            })
    }

    fn history_rows(&self, section: History) -> Result<Rows<'_>, ReadError> {
        match &self.history_sections {
            None => Ok(Rows::borrowed(&[])),
            Some(history) => history
                .get(&section)
                .map(|rows| Rows::borrowed(rows))
                .ok_or_else(|| {
                    ReadError::invalid(format!(
                        "Backup snapshot is missing required history section '{section}'"
                    ))
                }),
        }
    }

    fn has_state(&self, section: State) -> bool {
        self.state_sections.contains_key(&section)
    }
    fn includes_history(&self) -> bool {
        self.history_sections.is_some()
    }
    fn schema_limits(&self) -> JsonSchemaLimits {
        self.schema_limits
    }
}
