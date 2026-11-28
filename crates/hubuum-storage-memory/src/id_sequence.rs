use crate::StorageError;

/// Highest allocated or replayed identifier, including records retained only in history.
#[derive(Clone, Copy, Default)]
pub(super) struct IdSequence<T = i32> {
    last: T,
}

impl<T: Default + Ord + Copy + Into<i64> + TryFrom<i64>> IdSequence<T> {
    pub(super) fn with_existing(id: T) -> Self {
        let mut ids = Self::default();
        ids.observe(id);
        ids
    }

    pub(super) fn observe(&mut self, id: T) {
        self.last = self.last.max(id);
    }

    pub(super) fn last(&self) -> T {
        self.last
    }

    pub(super) fn next(&self) -> Result<T, StorageError> {
        self.after(1)
    }

    pub(super) fn after(&self, count: usize) -> Result<T, StorageError> {
        i64::try_from(count)
            .ok()
            .and_then(|count| self.last.into().checked_add(count))
            .and_then(|id| T::try_from(id).ok())
            .ok_or_else(|| StorageError::internal("Resource identifiers exhausted"))
    }

    pub(super) fn allocate(&mut self) -> Result<T, StorageError> {
        self.last = self.next()?;
        Ok(self.last)
    }
}
