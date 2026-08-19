/// A non-zero per-operation budget for storage reads performed by an export.
///
/// The application describes the limit without selecting a database mechanism.
/// Each adapter owns how the budget is enforced for its native read operations.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageQueryBudget {
    milliseconds: u64,
}

impl StorageQueryBudget {
    #[must_use]
    pub const fn from_millis(milliseconds: u64) -> Option<Self> {
        if milliseconds == 0 {
            None
        } else {
            Some(Self { milliseconds })
        }
    }

    #[must_use]
    pub const fn as_millis(self) -> u64 {
        self.milliseconds
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_disables_the_budget_and_positive_values_round_trip() {
        assert_eq!(StorageQueryBudget::from_millis(0), None);
        assert_eq!(
            StorageQueryBudget::from_millis(250).map(StorageQueryBudget::as_millis),
            Some(250)
        );
    }
}
