use crate::QueryError;

pub const MAX_TRAVERSAL_DEPTH: i32 = 512;
pub const MAX_TRAVERSAL_WORK_ROWS: i32 = 50_000;

/// Server-owned limits on recursive depth and generated work, applied before
/// result deduplication, sorting, counting, and pagination.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TraversalBudget {
    max_depth: i32,
    max_work_rows: i32,
}

impl TraversalBudget {
    pub fn new(max_depth: i32, max_work_rows: i32) -> Result<Self, QueryError> {
        if !(1..=MAX_TRAVERSAL_DEPTH).contains(&max_depth) {
            return Err(QueryError::BadRequest(format!(
                "traversal depth must be between 1 and {MAX_TRAVERSAL_DEPTH}"
            )));
        }
        if !(1..=MAX_TRAVERSAL_WORK_ROWS).contains(&max_work_rows) {
            return Err(QueryError::BadRequest(format!(
                "traversal work rows must be between 1 and {MAX_TRAVERSAL_WORK_ROWS}"
            )));
        }
        Ok(Self {
            max_depth,
            max_work_rows,
        })
    }

    #[must_use]
    pub const fn max_depth(self) -> i32 {
        self.max_depth
    }
    #[must_use]
    pub const fn max_work_rows(self) -> i32 {
        self.max_work_rows
    }

    pub fn for_requested_depth(self, requested: i32) -> Result<Self, QueryError> {
        if requested > self.max_depth {
            return Err(QueryError::BadRequest(format!(
                "requested traversal depth exceeds the server maximum of {}",
                self.max_depth
            )));
        }
        Self::new(requested, self.max_work_rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case(0, 1)]
    #[case(513, 1)]
    #[case(1, 0)]
    #[case(1, 50_001)]
    fn invalid_work_budgets_are_rejected(#[case] depth: i32, #[case] work: i32) {
        assert!(TraversalBudget::new(depth, work).is_err());
    }

    #[test]
    fn request_cannot_expand_server_budget() {
        assert!(
            TraversalBudget::new(2, 100)
                .unwrap()
                .for_requested_depth(3)
                .is_err()
        );
    }
}
