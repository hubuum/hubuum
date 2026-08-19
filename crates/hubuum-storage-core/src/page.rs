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
}
