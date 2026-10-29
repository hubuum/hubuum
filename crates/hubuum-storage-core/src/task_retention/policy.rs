use std::time::Duration;

use super::*;

/// Per-transaction physical work bound, including owned children and outputs.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "BudgetInput")]
pub struct TaskPurgeBudget {
    max_rows: u32,
    max_bytes: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BudgetInput {
    max_rows: u32,
    max_bytes: u64,
}

impl TaskPurgeBudget {
    pub const MAX_ROWS: u32 = 1000;
    pub const MAX_BYTES: u64 = 16 * 1024 * 1024;
    pub fn new(max_rows: u32, max_bytes: u64) -> Result<Self, StorageValidationError> {
        if max_rows == 0
            || max_rows > Self::MAX_ROWS
            || max_bytes == 0
            || max_bytes > Self::MAX_BYTES
        {
            return Err(invalid(
                "Task purge budget exceeds its nonzero row or byte bounds",
            ));
        }
        Ok(Self {
            max_rows,
            max_bytes,
        })
    }
    pub const fn max_rows(self) -> u32 {
        self.max_rows
    }
    pub const fn max_bytes(self) -> u64 {
        self.max_bytes
    }
}
impl Default for TaskPurgeBudget {
    fn default() -> Self {
        Self {
            max_rows: Self::MAX_ROWS,
            max_bytes: Self::MAX_BYTES,
        }
    }
}
impl TryFrom<BudgetInput> for TaskPurgeBudget {
    type Error = StorageValidationError;
    fn try_from(value: BudgetInput) -> Result<Self, Self::Error> {
        Self::new(value.max_rows, value.max_bytes)
    }
}

/// Immutable policy. Durations use whole seconds in canonical snapshots.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "PolicyInput")]
pub struct TaskRetentionPolicy {
    revision: TaskRetentionPolicyRevision,
    deduplication_after_terminal_secs: u64,
    details_after_terminal_secs: Option<u64>,
    pending_protection_timeout_secs: u64,
    max_receipts: u32,
    max_receipt_bytes: u64,
    cleanup_budget: TaskPurgeBudget,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyInput {
    revision: TaskRetentionPolicyRevision,
    deduplication_after_terminal_secs: u64,
    details_after_terminal_secs: Option<u64>,
    pending_protection_timeout_secs: u64,
    max_receipts: u32,
    max_receipt_bytes: u64,
    cleanup_budget: TaskPurgeBudget,
}

pub(super) fn duration_seconds(value: Duration) -> Result<u64, StorageValidationError> {
    if value.subsec_nanos() != 0 || value.as_secs() > 100 * 366 * 24 * 60 * 60 {
        return Err(invalid(
            "Task retention durations require whole seconds within 100 years",
        ));
    }
    Ok(value.as_secs())
}

impl TaskRetentionPolicy {
    /// Compatibility policy before protected retention is explicitly enabled.
    pub fn disabled_default() -> Self {
        Self {
            revision: TaskRetentionPolicyRevision::new(0).expect("zero revision"),
            deduplication_after_terminal_secs: 7 * 24 * 60 * 60,
            details_after_terminal_secs: None,
            pending_protection_timeout_secs: 300,
            max_receipts: TASK_CONTROL_MAX_RECEIPTS as u32,
            max_receipt_bytes: TASK_CONTROL_MAX_RECEIPTS_BYTES as u64,
            cleanup_budget: TaskPurgeBudget::default(),
        }
    }
    pub fn new(
        revision: TaskRetentionPolicyRevision,
        deduplication_after_terminal: Duration,
        details_after_terminal: Option<Duration>,
    ) -> Result<Self, StorageValidationError> {
        let deduplication_after_terminal_secs = duration_seconds(deduplication_after_terminal)?;
        if deduplication_after_terminal_secs == 0 {
            return Err(invalid("Task deduplication window must be positive"));
        }
        Ok(Self {
            revision,
            deduplication_after_terminal_secs,
            details_after_terminal_secs: details_after_terminal
                .map(duration_seconds)
                .transpose()?,
            pending_protection_timeout_secs: 300,
            max_receipts: TASK_CONTROL_MAX_RECEIPTS as u32,
            max_receipt_bytes: TASK_CONTROL_MAX_RECEIPTS_BYTES as u64,
            cleanup_budget: TaskPurgeBudget::default(),
        })
    }
    pub fn with_pending_protection_timeout(
        mut self,
        value: Duration,
    ) -> Result<Self, StorageValidationError> {
        let seconds = duration_seconds(value)?;
        if seconds == 0 {
            return Err(invalid("Admission protection timeout must be positive"));
        }
        self.pending_protection_timeout_secs = seconds;
        Ok(self)
    }
    pub fn with_receipt_limits(
        mut self,
        count: u32,
        bytes: u64,
    ) -> Result<Self, StorageValidationError> {
        if count == 0
            || count as usize > TASK_CONTROL_MAX_RECEIPTS
            || bytes == 0
            || bytes > TASK_CONTROL_MAX_RECEIPTS_BYTES as u64
        {
            return Err(invalid("Task receipt limits exceed supported bounds"));
        }
        self.max_receipts = count;
        self.max_receipt_bytes = bytes;
        Ok(self)
    }
    pub const fn with_cleanup_budget(mut self, budget: TaskPurgeBudget) -> Self {
        self.cleanup_budget = budget;
        self
    }
    pub const fn revision(&self) -> TaskRetentionPolicyRevision {
        self.revision
    }
    pub const fn deduplication_after_terminal(&self) -> Duration {
        Duration::from_secs(self.deduplication_after_terminal_secs)
    }
    pub fn details_after_terminal(&self) -> Option<Duration> {
        self.details_after_terminal_secs.map(Duration::from_secs)
    }
    pub const fn pending_protection_timeout(&self) -> Duration {
        Duration::from_secs(self.pending_protection_timeout_secs)
    }
    pub const fn max_receipts(&self) -> u32 {
        self.max_receipts
    }
    pub const fn max_receipt_bytes(&self) -> u64 {
        self.max_receipt_bytes
    }
    pub const fn cleanup_budget(&self) -> TaskPurgeBudget {
        self.cleanup_budget
    }
}

impl TryFrom<PolicyInput> for TaskRetentionPolicy {
    type Error = StorageValidationError;
    fn try_from(v: PolicyInput) -> Result<Self, Self::Error> {
        Self::new(
            v.revision,
            Duration::from_secs(v.deduplication_after_terminal_secs),
            v.details_after_terminal_secs.map(Duration::from_secs),
        )?
        .with_pending_protection_timeout(Duration::from_secs(v.pending_protection_timeout_secs))?
        .with_receipt_limits(v.max_receipts, v.max_receipt_bytes)
        .map(|policy| policy.with_cleanup_budget(v.cleanup_budget))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum TaskReceiptActivation {
    Disabled,
    Backfilling {
        key_fingerprint: ControlKeyFingerprint,
    },
    Protected {
        key_fingerprint: ControlKeyFingerprint,
    },
}

impl TaskReceiptActivation {
    pub const fn key_fingerprint(&self) -> Option<&ControlKeyFingerprint> {
        match self {
            Self::Disabled => None,
            Self::Backfilling { key_fingerprint } | Self::Protected { key_fingerprint } => {
                Some(key_fingerprint)
            }
        }
    }
    pub const fn is_protected(&self) -> bool {
        matches!(self, Self::Protected { .. })
    }
}
