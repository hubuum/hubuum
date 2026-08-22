use crate::StorageError;

pub(crate) fn validate_page_total(
    row_count: usize,
    total: Option<i64>,
) -> Result<(), StorageError> {
    let row_count = i64::try_from(row_count)
        .map_err(|_| StorageError::internal("A storage page contains too many rows"))?;
    if total.is_some_and(|value| value < row_count) {
        return Err(StorageError::internal(
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
    pub fn try_new(rows: Vec<T>, total: Option<i64>) -> Result<Self, StorageError> {
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
