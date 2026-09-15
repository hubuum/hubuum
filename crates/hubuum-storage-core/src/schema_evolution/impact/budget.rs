use std::io::{self, Write};

use serde::Serialize;

use crate::{StorageError, StorageValidationError};

/// A finite budget for serialized report data, charged before retaining each row.
/// Accounting includes checkpoint metadata and grouping overhead. Exceeding the
/// budget fails the entire read; it never returns a silently truncated report.
#[derive(Debug)]
pub struct StorageSchemaReportBudget {
    max_bytes: usize,
    remaining_bytes: usize,
}

impl StorageSchemaReportBudget {
    pub const MAX_BYTES: usize = 16 * 1024 * 1024;

    pub fn new(max_bytes: usize) -> Result<Self, StorageValidationError> {
        if max_bytes == 0 || max_bytes > Self::MAX_BYTES {
            return Err(StorageValidationError::invalid(
                "Schema report budget must be between 1 byte and 16 MiB",
            ));
        }
        Ok(Self {
            max_bytes,
            remaining_bytes: max_bytes,
        })
    }

    /// Count compact JSON without allocating it. Call before cloning or retaining
    /// a value, and stop enumeration immediately if this returns an error.
    pub fn charge<T: Serialize>(&mut self, value: &T) -> Result<(), StorageError> {
        let mut writer = BudgetWriter {
            remaining: self.remaining_bytes,
            exceeded: false,
        };
        serde_json::to_writer(&mut writer, value).map_err(|error| {
            if writer.exceeded {
                StorageError::input_too_large(format!(
                    "Schema report data exceeds the {} byte assembly budget; no partial report was produced",
                    self.max_bytes
                ))
            } else {
                StorageError::internal(format!("Schema report serialization failed: {error}"))
            }
        })?;
        self.remaining_bytes = writer.remaining;
        Ok(())
    }
}

impl Default for StorageSchemaReportBudget {
    fn default() -> Self {
        Self {
            max_bytes: Self::MAX_BYTES,
            remaining_bytes: Self::MAX_BYTES,
        }
    }
}

struct BudgetWriter {
    remaining: usize,
    exceeded: bool,
}

impl Write for BudgetWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.remaining {
            self.exceeded = true;
            return Err(io::Error::other("schema report assembly budget exceeded"));
        }
        self.remaining -= bytes.len();
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StorageErrorKind;
    use rstest::rstest;

    #[rstest]
    #[case::zero(0)]
    #[case::above_ceiling(StorageSchemaReportBudget::MAX_BYTES + 1)]
    fn report_budgets_require_a_finite_positive_limit(#[case] bytes: usize) {
        assert!(StorageSchemaReportBudget::new(bytes).is_err());
    }

    #[rstest]
    #[case::ascii("plain")]
    #[case::unicode_and_escaping("\"å\\\n")]
    fn report_budget_counts_encoded_bytes_across_values(#[case] value: &str) {
        let bytes = serde_json::to_vec(value).unwrap().len();
        let mut budget = StorageSchemaReportBudget::new(bytes * 2).unwrap();
        budget.charge(&value).unwrap();
        budget.charge(&value).unwrap();
        assert_eq!(
            budget.charge(&value).unwrap_err().kind(),
            StorageErrorKind::InputTooLarge
        );
    }
}
