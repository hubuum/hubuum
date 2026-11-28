use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use opentelemetry::KeyValue;
use serde::Serialize;

use super::current;

#[derive(Clone, Copy)]
pub(crate) enum CheckpointOperation {
    Read,
    Write,
    EncodeSign,
    Verify,
    FsyncFile,
    FsyncDirectory,
    Publish,
    Gc,
    CoalescingWait,
}

impl CheckpointOperation {
    const ALL: [Self; 9] = [
        Self::Read,
        Self::Write,
        Self::EncodeSign,
        Self::Verify,
        Self::FsyncFile,
        Self::FsyncDirectory,
        Self::Publish,
        Self::Gc,
        Self::CoalescingWait,
    ];

    const fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::EncodeSign => "encode_sign",
            Self::Verify => "verify",
            Self::FsyncFile => "fsync_file",
            Self::FsyncDirectory => "fsync_directory",
            Self::Publish => "publish",
            Self::Gc => "gc",
            Self::CoalescingWait => "coalescing_wait",
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum CheckpointLockOutcome {
    Acquired,
    Contended,
    Failure,
}

impl CheckpointLockOutcome {
    const ALL: [Self; 3] = [Self::Acquired, Self::Contended, Self::Failure];
    const fn as_str(self) -> &'static str {
        match self {
            Self::Acquired => "acquired",
            Self::Contended => "contended",
            Self::Failure => "failure",
        }
    }
}

struct Totals {
    read_bytes: AtomicU64,
    write_bytes: AtomicU64,
    gc_files: AtomicU64,
    gc_bytes: AtomicU64,
    calls: [AtomicU64; 9],
    failures: [AtomicU64; 9],
    duration_ns: [AtomicU64; 9],
    lock_calls: [AtomicU64; 3],
    lock_duration_ns: [AtomicU64; 3],
}

static TOTALS: Totals = Totals {
    read_bytes: AtomicU64::new(0),
    write_bytes: AtomicU64::new(0),
    gc_files: AtomicU64::new(0),
    gc_bytes: AtomicU64::new(0),
    calls: [const { AtomicU64::new(0) }; 9],
    failures: [const { AtomicU64::new(0) }; 9],
    duration_ns: [const { AtomicU64::new(0) }; 9],
    lock_calls: [const { AtomicU64::new(0) }; 3],
    lock_duration_ns: [const { AtomicU64::new(0) }; 3],
};

/// Process-cumulative publisher observations. File bytes count successful read
/// and write calls, including partial work before an error, never net growth.
#[derive(Clone, Debug, Serialize)]
pub(crate) struct CheckpointMeasurement {
    read_bytes: u64,
    write_bytes: u64,
    gc_files: u64,
    gc_bytes: u64,
    operations: BTreeMap<&'static str, OperationMeasurement>,
    locks: BTreeMap<&'static str, LockMeasurement>,
}

#[derive(Clone, Debug, Serialize)]
struct OperationMeasurement {
    calls: u64,
    failures: u64,
    duration_ns: u64,
}
#[derive(Clone, Debug, Serialize)]
struct LockMeasurement {
    calls: u64,
    duration_ns: u64,
}

pub(crate) fn checkpoint_measurement() -> Option<CheckpointMeasurement> {
    if std::env::var("HUBUUM_CONTROL_CHECKPOINT_MEASURE").as_deref() != Ok("true") {
        return None;
    }
    let read = |value: &AtomicU64| value.load(Ordering::Relaxed);
    Some(CheckpointMeasurement {
        read_bytes: read(&TOTALS.read_bytes),
        write_bytes: read(&TOTALS.write_bytes),
        gc_files: read(&TOTALS.gc_files),
        gc_bytes: read(&TOTALS.gc_bytes),
        operations: CheckpointOperation::ALL
            .into_iter()
            .map(|operation| {
                let index = operation as usize;
                (
                    operation.as_str(),
                    OperationMeasurement {
                        calls: read(&TOTALS.calls[index]),
                        failures: read(&TOTALS.failures[index]),
                        duration_ns: read(&TOTALS.duration_ns[index]),
                    },
                )
            })
            .collect(),
        locks: CheckpointLockOutcome::ALL
            .into_iter()
            .map(|outcome| {
                let index = outcome as usize;
                (
                    outcome.as_str(),
                    LockMeasurement {
                        calls: read(&TOTALS.lock_calls[index]),
                        duration_ns: read(&TOTALS.lock_duration_ns[index]),
                    },
                )
            })
            .collect(),
    })
}

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn operation_finished(operation: CheckpointOperation, success: bool, duration: Duration) {
    let index = operation as usize;
    TOTALS.calls[index].fetch_add(1, Ordering::Relaxed);
    TOTALS.failures[index].fetch_add(u64::from(!success), Ordering::Relaxed);
    TOTALS.duration_ns[index].fetch_add(nanos(duration), Ordering::Relaxed);
    if let Some(metrics) = current() {
        let attributes = [
            KeyValue::new("operation", operation.as_str()),
            KeyValue::new("outcome", if success { "success" } else { "failure" }),
        ];
        metrics.checkpoint_operations.add(1, &attributes);
        metrics
            .checkpoint_duration
            .record(duration.as_secs_f64(), &attributes);
    }
}

pub(crate) fn checkpoint_read_bytes(bytes: usize) {
    TOTALS.read_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    if let Some(metrics) = current() {
        metrics
            .checkpoint_io_bytes
            .add(bytes as u64, &[KeyValue::new("operation", "read")]);
    }
}

pub(crate) fn checkpoint_write_bytes(bytes: usize) {
    TOTALS
        .write_bytes
        .fetch_add(bytes as u64, Ordering::Relaxed);
    if let Some(metrics) = current() {
        metrics
            .checkpoint_io_bytes
            .add(bytes as u64, &[KeyValue::new("operation", "write")]);
    }
}

pub(crate) fn checkpoint_gc_removed(bytes: u64) {
    TOTALS.gc_files.fetch_add(1, Ordering::Relaxed);
    TOTALS.gc_bytes.fetch_add(bytes, Ordering::Relaxed);
    if let Some(metrics) = current() {
        metrics.checkpoint_gc_files.add(1, &[]);
        metrics.checkpoint_gc_bytes.add(bytes, &[]);
    }
}

pub(crate) fn checkpoint_lock_finished(outcome: CheckpointLockOutcome, duration: Duration) {
    let index = outcome as usize;
    TOTALS.lock_calls[index].fetch_add(1, Ordering::Relaxed);
    TOTALS.lock_duration_ns[index].fetch_add(nanos(duration), Ordering::Relaxed);
    if let Some(metrics) = current() {
        let attributes = [KeyValue::new("outcome", outcome.as_str())];
        metrics.checkpoint_lock_attempts.add(1, &attributes);
        metrics
            .checkpoint_lock_duration
            .record(duration.as_secs_f64(), &attributes);
    }
}

pub(crate) struct CheckpointTimer {
    operation: CheckpointOperation,
    started: Instant,
    finished: bool,
}
impl CheckpointTimer {
    pub(crate) fn start(operation: CheckpointOperation) -> Self {
        Self {
            operation,
            started: Instant::now(),
            finished: false,
        }
    }
    pub(crate) fn finish(mut self, success: bool) {
        self.finished = true;
        operation_finished(self.operation, success, self.started.elapsed());
    }
}
impl Drop for CheckpointTimer {
    fn drop(&mut self) {
        if !self.finished {
            operation_finished(self.operation, false, self.started.elapsed());
        }
    }
}
