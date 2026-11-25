use crate::StorageError;

/// Highest allocated or replayed identifier, including records retained only in history.
#[derive(Clone, Copy, Default)]
pub(super) struct IdSequence {
    last: i32,
}

impl IdSequence {
    pub(super) fn with_existing(id: i32) -> Self {
        let mut ids = Self::default();
        ids.observe(id);
        ids
    }

    pub(super) fn observe(&mut self, id: i32) {
        self.last = self.last.max(id);
    }

    pub(super) fn allocate(&mut self) -> Result<i32, StorageError> {
        self.last = self
            .last
            .checked_add(1)
            .ok_or_else(|| StorageError::internal("Resource identifiers exhausted"))?;
        Ok(self.last)
    }
}
