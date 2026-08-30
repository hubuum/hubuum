use std::num::NonZeroUsize;

use crate::StorageValidationError;

/// Hard ceiling for one backend-neutral candidate page.
///
/// Application code may choose a smaller page to control row size or policy
/// request width. Keeping the ceiling in the storage contract prevents an
/// adapter from turning a bounded enumeration request back into an unbounded
/// materialization.
pub const MAX_STORAGE_CANDIDATE_PAGE_SIZE: usize = 512;

/// Validated maximum number of rows in one candidate page.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageCandidatePageLimit(NonZeroUsize);

impl StorageCandidatePageLimit {
    pub fn try_new(value: usize) -> Result<Self, StorageValidationError> {
        let value = NonZeroUsize::new(value).ok_or_else(|| {
            StorageValidationError::invalid("A storage candidate page limit must be positive")
        })?;
        if value.get() > MAX_STORAGE_CANDIDATE_PAGE_SIZE {
            return Err(StorageValidationError::invalid(format!(
                "A storage candidate page limit must not exceed {MAX_STORAGE_CANDIDATE_PAGE_SIZE}"
            )));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub const fn get(self) -> usize {
        self.0.get()
    }
}

/// One bounded page from a larger candidate enumeration.
///
/// The cursor remains operation-specific: callers advance from the last row
/// using that operation's stable ordering contract when `has_more` is true.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageCandidatePage<T> {
    rows: Vec<T>,
    has_more: bool,
}

impl<T> StorageCandidatePage<T> {
    pub fn try_new(
        rows: Vec<T>,
        has_more: bool,
        limit: StorageCandidatePageLimit,
    ) -> Result<Self, StorageValidationError> {
        if rows.len() > limit.get() {
            return Err(StorageValidationError::invalid(
                "A storage candidate page exceeds its requested limit",
            ));
        }
        if rows.is_empty() && has_more {
            return Err(StorageValidationError::invalid(
                "An empty storage candidate page cannot report more rows",
            ));
        }
        Ok(Self { rows, has_more })
    }

    #[must_use]
    pub fn rows(&self) -> &[T] {
        &self.rows
    }

    #[must_use]
    pub const fn has_more(&self) -> bool {
        self.has_more
    }

    #[must_use]
    pub fn into_parts(self) -> (Vec<T>, bool) {
        (self.rows, self.has_more)
    }
}

pub(crate) fn validate_page_total(
    row_count: usize,
    total: Option<i64>,
) -> Result<(), StorageValidationError> {
    let row_count = i64::try_from(row_count)
        .map_err(|_| StorageValidationError::invalid("A storage page contains too many rows"))?;
    if total.is_some_and(|value| value < row_count) {
        return Err(StorageValidationError::invalid(
            "A storage page total must be at least the number of returned rows",
        ));
    }
    Ok(())
}

/// Named components of a backend-selected page.
pub struct StoragePageParts<T> {
    rows: Vec<T>,
    total: Option<i64>,
}

impl<T> StoragePageParts<T> {
    #[must_use]
    pub fn rows(&self) -> &[T] {
        &self.rows
    }

    #[must_use]
    pub const fn total(&self) -> Option<i64> {
        self.total
    }

    #[must_use]
    pub fn into_rows(self) -> Vec<T> {
        self.rows
    }
}

/// One backend-selected page and an optional exact total computed from the
/// same filtered, authorized result set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoragePage<T> {
    rows: Vec<T>,
    total: Option<i64>,
}

impl<T> StoragePage<T> {
    pub fn try_new(rows: Vec<T>, total: Option<i64>) -> Result<Self, StorageValidationError> {
        validate_page_total(rows.len(), total)?;
        Ok(Self { rows, total })
    }

    #[must_use]
    pub fn rows(&self) -> &[T] {
        &self.rows
    }

    #[must_use]
    pub const fn total(&self) -> Option<i64> {
        self.total
    }

    #[must_use]
    pub fn into_parts(self) -> (Vec<T>, Option<i64>) {
        (self.rows, self.total)
    }

    #[must_use]
    pub fn into_named_parts(self) -> StoragePageParts<T> {
        StoragePageParts {
            rows: self.rows,
            total: self.total,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn candidate_page_limits_are_positive_and_bounded() {
        assert!(StorageCandidatePageLimit::try_new(0).is_err());
        assert_eq!(
            StorageCandidatePageLimit::try_new(MAX_STORAGE_CANDIDATE_PAGE_SIZE)
                .unwrap()
                .get(),
            MAX_STORAGE_CANDIDATE_PAGE_SIZE
        );
        assert!(StorageCandidatePageLimit::try_new(MAX_STORAGE_CANDIDATE_PAGE_SIZE + 1).is_err());
    }

    #[test]
    fn candidate_pages_enforce_the_requested_bound() {
        let limit = StorageCandidatePageLimit::try_new(2).unwrap();
        let page = StorageCandidatePage::try_new(vec![1, 2], true, limit).unwrap();

        assert_eq!(page.rows(), &[1, 2]);
        assert!(page.has_more());
        assert_eq!(page.into_parts(), (vec![1, 2], true));
        assert!(StorageCandidatePage::try_new(vec![1, 2, 3], false, limit).is_err());
        assert!(StorageCandidatePage::<()>::try_new(Vec::new(), true, limit).is_err());
    }

    #[test]
    fn page_preserves_rows_and_optional_total() {
        let page = StoragePage::try_new(vec![1, 2], Some(7)).unwrap();

        assert_eq!(page.rows(), &[1, 2]);
        assert_eq!(page.total(), Some(7));
        assert_eq!(page.clone().into_parts(), (vec![1, 2], Some(7)));
        let parts = page.into_named_parts();
        assert_eq!(parts.rows(), &[1, 2]);
        assert_eq!(parts.total(), Some(7));
        assert_eq!(parts.into_rows(), vec![1, 2]);
    }

    #[test]
    fn pages_reject_negative_totals() {
        assert!(StoragePage::<()>::try_new(Vec::new(), Some(-1)).is_err());
    }

    #[test]
    fn pages_reject_totals_smaller_than_the_returned_page() {
        assert!(StoragePage::try_new(vec![1, 2], Some(1)).is_err());
    }
}
