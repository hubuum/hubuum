/// One backend-selected page and an optional exact total computed from the
/// same filtered, authorized result set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoragePage<T> {
    rows: Vec<T>,
    total: Option<i64>,
}

impl<T> StoragePage<T> {
    #[must_use]
    pub const fn new(rows: Vec<T>, total: Option<i64>) -> Self {
        Self { rows, total }
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
}

/// One backend-selected page with a mandatory exact total.
///
/// Use this only when the application contract always requires the total.
/// Query-driven pages that may skip a count use StoragePage instead.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageCountedPage<T> {
    rows: Vec<T>,
    total: i64,
}

impl<T> StorageCountedPage<T> {
    #[must_use]
    pub const fn new(rows: Vec<T>, total: i64) -> Self {
        Self { rows, total }
    }

    #[must_use]
    pub fn rows(&self) -> &[T] {
        &self.rows
    }

    #[must_use]
    pub const fn total(&self) -> i64 {
        self.total
    }

    #[must_use]
    pub fn into_parts(self) -> (Vec<T>, i64) {
        (self.rows, self.total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_preserves_rows_and_optional_total() {
        let page = StoragePage::new(vec![1, 2], Some(7));

        assert_eq!(page.rows(), &[1, 2]);
        assert_eq!(page.total(), Some(7));
        assert_eq!(page.into_parts(), (vec![1, 2], Some(7)));
    }

    #[test]
    fn counted_page_preserves_rows_and_mandatory_total() {
        let page = StorageCountedPage::new(vec![1, 2], 7);

        assert_eq!(page.rows(), &[1, 2]);
        assert_eq!(page.total(), 7);
        assert_eq!(page.into_parts(), (vec![1, 2], 7));
    }
}
