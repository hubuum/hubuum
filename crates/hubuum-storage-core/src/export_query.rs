use std::future::Future;
use std::pin::Pin;

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

/// Mandatory backend scope for bounded export reads.
///
/// The supplied future is evaluated exactly once. Every storage read it makes
/// through the same configured backend must honor `budget`; `None` explicitly
/// disables the export-specific limit. Adapters may use native cancellation,
/// per-operation deadlines, or an equivalent backend mechanism.
pub trait ExportQueryStorage: Send + Sync {
    fn run_export_queries<'a, F, R>(
        &'a self,
        budget: Option<StorageQueryBudget>,
        future: F,
    ) -> Pin<Box<dyn Future<Output = R> + 'a>>
    where
        F: Future<Output = R> + 'a,
        R: 'a;
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
