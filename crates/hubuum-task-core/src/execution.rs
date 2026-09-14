//! Validated execution policy and runtime-independent cooperative stop signals.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{Duration, Instant};

/// A bounded, single-line explanation supplied by the cancellation actor.
/// Debug output deliberately never includes its contents.
#[derive(Clone, PartialEq, Eq)]
pub struct TaskCancellationReason(String);

impl TaskCancellationReason {
    pub const MAX_BYTES: usize = 512;

    pub fn new(value: impl Into<String>) -> Result<Self, TaskControlError> {
        let value = value.into();
        if value.trim().is_empty()
            || value.len() > Self::MAX_BYTES
            || value.chars().any(char::is_control)
        {
            return Err(TaskControlError::InvalidReason);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for TaskCancellationReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("TaskCancellationReason([REDACTED])")
    }
}

/// A finite server-owned maximum execution duration, in whole milliseconds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TaskExecutionLimit(Duration);

impl TaskExecutionLimit {
    /// The largest supported duration is thirty days. This also bounds clock
    /// arithmetic and prevents configuration from silently disabling deadlines.
    pub const MAX_MILLISECONDS: u64 = 30 * 24 * 60 * 60 * 1_000;

    pub fn from_milliseconds(value: u64) -> Result<Self, TaskControlError> {
        if value == 0 || value > Self::MAX_MILLISECONDS {
            return Err(TaskControlError::InvalidDuration);
        }
        Ok(Self(Duration::from_millis(value)))
    }

    pub const fn duration(self) -> Duration {
        self.0
    }

    pub fn milliseconds(self) -> u64 {
        // Construction limits the value to thirty days.
        self.0.as_millis() as u64
    }
}

/// Stable, content-free reason for stopping an execution.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskStopReason {
    Cancelled,
    DeadlineExceeded,
}

impl TaskStopReason {
    pub fn from_persisted(value: &str) -> Option<Self> {
        match value {
            "cancel_requested" => Some(Self::Cancelled),
            "deadline_exceeded" => Some(Self::DeadlineExceeded),
            _ => None,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Cancelled => "cancel_requested",
            Self::DeadlineExceeded => "deadline_exceeded",
        }
    }

    const fn code(self) -> u8 {
        match self {
            Self::Cancelled => 1,
            Self::DeadlineExceeded => 2,
        }
    }

    fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::Cancelled),
            2 => Some(Self::DeadlineExceeded),
            _ => None,
        }
    }
}

impl fmt::Display for TaskStopReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::error::Error for TaskStopReason {}

/// Read-only execution context carried into cooperative services and adapters.
/// Clones share a stop signal and the original monotonic deadline.
#[derive(Clone, Debug)]
pub struct TaskExecutionContext {
    signal: Arc<AtomicU8>,
    deadline: Instant,
}

impl PartialEq for TaskExecutionContext {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.signal, &other.signal) && self.deadline == other.deadline
    }
}

impl Eq for TaskExecutionContext {}

/// The monitor's capability to publish a durable stop decision to an executor.
#[derive(Clone, Debug)]
pub struct TaskStopHandle {
    signal: Arc<AtomicU8>,
}

impl TaskExecutionContext {
    /// Convert the remaining duration of a persisted deadline once at admission.
    /// A zero duration is valid for a deadline that has already expired.
    pub fn new(remaining: Duration) -> Result<(Self, TaskStopHandle), TaskControlError> {
        let deadline = Instant::now()
            .checked_add(remaining)
            .ok_or(TaskControlError::InvalidDuration)?;
        let signal = Arc::new(AtomicU8::new(0));
        Ok((
            Self {
                signal: signal.clone(),
                deadline,
            },
            TaskStopHandle { signal },
        ))
    }

    /// Check at an actual work boundary. Dropping a future alone does not prove
    /// that a database transaction, subprocess, or external effect has stopped.
    pub fn check(&self) -> Result<(), TaskStopReason> {
        if let Some(reason) = TaskStopReason::from_code(self.signal.load(Ordering::Acquire)) {
            return Err(reason);
        }
        if Instant::now() >= self.deadline {
            return Err(TaskStopReason::DeadlineExceeded);
        }
        Ok(())
    }

    pub fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }
}

impl TaskStopHandle {
    /// Publish the first observed durable stop cause; repeated observations are
    /// idempotent and cannot turn a stopped executor back into running work.
    pub fn request_stop(&self, reason: TaskStopReason) {
        let _ = self
            .signal
            .compare_exchange(0, reason.code(), Ordering::AcqRel, Ordering::Acquire);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskControlError {
    InvalidReason,
    InvalidDuration,
}

impl fmt::Display for TaskControlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidReason => {
                "Cancellation reason must be a nonempty single line of at most 512 UTF-8 bytes"
            }
            Self::InvalidDuration => {
                "Task execution duration must be between one millisecond and thirty days"
            }
        })
    }
}

impl std::error::Error for TaskControlError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancellation_reason_debug_does_not_disclose_content() {
        let reason = TaskCancellationReason::new("sensitive operator explanation").unwrap();
        assert_eq!(format!("{reason:?}"), "TaskCancellationReason([REDACTED])");
    }

    #[test]
    fn cancellation_reason_limit_counts_utf8_bytes() {
        assert!(TaskCancellationReason::new("é".repeat(257)).is_err());
    }

    #[test]
    fn cancellation_reason_accepts_exact_byte_limit() {
        assert!(TaskCancellationReason::new("é".repeat(256)).is_ok());
    }

    #[test]
    fn cancellation_reason_rejects_log_line_injection() {
        assert!(TaskCancellationReason::new("reason\nforged entry").is_err());
    }

    #[test]
    fn cancelled_context_clones_cannot_continue() {
        let (context, stop) = TaskExecutionContext::new(Duration::from_secs(60)).unwrap();
        let downstream = context.clone();
        stop.request_stop(TaskStopReason::Cancelled);
        assert_eq!(downstream.check(), Err(TaskStopReason::Cancelled));
    }

    #[test]
    fn repeated_signals_preserve_first_stop_cause() {
        let (context, stop) = TaskExecutionContext::new(Duration::from_secs(60)).unwrap();
        stop.request_stop(TaskStopReason::Cancelled);
        stop.request_stop(TaskStopReason::DeadlineExceeded);
        assert_eq!(context.check(), Err(TaskStopReason::Cancelled));
    }

    #[test]
    fn already_expired_deadline_stops_before_first_work() {
        let (context, _) = TaskExecutionContext::new(Duration::ZERO).unwrap();
        assert_eq!(context.check(), Err(TaskStopReason::DeadlineExceeded));
    }

    #[test]
    fn execution_limit_rejects_zero() {
        assert!(TaskExecutionLimit::from_milliseconds(0).is_err());
    }

    #[test]
    fn execution_limit_rejects_overflow_input() {
        assert!(TaskExecutionLimit::from_milliseconds(u64::MAX).is_err());
    }
}
