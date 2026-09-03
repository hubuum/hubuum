use std::sync::Mutex;

use prometheus::{Counter, IntGauge, Registry, core::Collector};
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System, get_current_pid};

use crate::errors::ApiError;
use crate::operational_contracts::{MetricKind, metric_definition};

const PROCESS_CPU_SECONDS: &str = "process_cpu_seconds_total";
const PROCESS_OPEN_FDS: &str = "process_open_fds";
const PROCESS_MAX_FDS: &str = "process_max_fds";
const PROCESS_RESIDENT_MEMORY: &str = "process_resident_memory_bytes";
const PROCESS_VIRTUAL_MEMORY: &str = "process_virtual_memory_bytes";
const PROCESS_START_TIME: &str = "process_start_time_seconds";

struct ProcessState {
    system: System,
    pid: Pid,
    last_cpu_millis: u64,
}

struct ProcessSnapshot {
    cpu_millis: u64,
    open_files: Option<usize>,
    open_files_limit: Option<usize>,
    resident_memory_bytes: u64,
    virtual_memory_bytes: u64,
    start_time_seconds: u64,
}

pub(super) struct ProcessMetrics {
    state: Mutex<ProcessState>,
    cpu_seconds: Counter,
    open_fds: IntGauge,
    max_fds: IntGauge,
    resident_memory_bytes: IntGauge,
    virtual_memory_bytes: IntGauge,
    start_time_seconds: IntGauge,
}

impl ProcessMetrics {
    pub(super) fn new(registry: &Registry) -> Result<Self, ApiError> {
        let cpu_seconds = create_and_register_counter(registry, PROCESS_CPU_SECONDS)?;
        let open_fds = create_and_register_gauge(registry, PROCESS_OPEN_FDS)?;
        let max_fds = create_and_register_gauge(registry, PROCESS_MAX_FDS)?;
        let resident_memory_bytes = create_and_register_gauge(registry, PROCESS_RESIDENT_MEMORY)?;
        let virtual_memory_bytes = create_and_register_gauge(registry, PROCESS_VIRTUAL_MEMORY)?;
        let start_time_seconds = create_and_register_gauge(registry, PROCESS_START_TIME)?;

        let pid = get_current_pid().map_err(|error| {
            ApiError::InternalServerError(format!(
                "Failed to identify the current process for metrics: {error}"
            ))
        })?;
        let metrics = Self {
            state: Mutex::new(ProcessState {
                system: System::new(),
                pid,
                last_cpu_millis: 0,
            }),
            cpu_seconds,
            open_fds,
            max_fds,
            resident_memory_bytes,
            virtual_memory_bytes,
            start_time_seconds,
        };
        metrics.refresh_required()?;

        Ok(metrics)
    }

    pub(super) fn refresh(&self) -> bool {
        self.state
            .lock()
            .is_ok_and(|mut state| self.refresh_state(&mut state))
    }

    fn refresh_required(&self) -> Result<(), ApiError> {
        let mut state = self.state.lock().map_err(|_| {
            ApiError::InternalServerError("Failed to initialize process metrics state".to_string())
        })?;
        if self.refresh_state(&mut state) {
            Ok(())
        } else {
            Err(ApiError::InternalServerError(
                "Failed to read current process metrics".to_string(),
            ))
        }
    }

    fn refresh_state(&self, state: &mut ProcessState) -> bool {
        let pids = [state.pid];
        state.system.refresh_processes_specifics(
            ProcessesToUpdate::Some(&pids),
            false,
            ProcessRefreshKind::nothing()
                .without_tasks()
                .with_cpu()
                .with_memory(),
        );
        let Some(snapshot) = state.system.process(state.pid).map(ProcessSnapshot::from) else {
            return false;
        };

        if snapshot.cpu_millis >= state.last_cpu_millis {
            self.cpu_seconds
                .inc_by(snapshot.cpu_millis.saturating_sub(state.last_cpu_millis) as f64 / 1000.0);
            state.last_cpu_millis = snapshot.cpu_millis;
        }
        self.open_fds
            .set(saturating_i64(snapshot.open_files.unwrap_or_default()));
        self.max_fds.set(saturating_i64(
            snapshot.open_files_limit.unwrap_or_default(),
        ));
        self.resident_memory_bytes
            .set(saturating_i64(snapshot.resident_memory_bytes));
        self.virtual_memory_bytes
            .set(saturating_i64(snapshot.virtual_memory_bytes));
        self.start_time_seconds
            .set(saturating_i64(snapshot.start_time_seconds));

        true
    }
}

impl From<&sysinfo::Process> for ProcessSnapshot {
    fn from(process: &sysinfo::Process) -> Self {
        Self {
            cpu_millis: process.accumulated_cpu_time(),
            open_files: process.open_files(),
            open_files_limit: process_open_files_limit(process),
            resident_memory_bytes: process.memory(),
            virtual_memory_bytes: process.virtual_memory(),
            start_time_seconds: process.start_time(),
        }
    }
}

#[cfg(target_os = "macos")]
fn process_open_files_limit(_process: &sysinfo::Process) -> Option<usize> {
    let mut limits = std::mem::MaybeUninit::<libc::rlimit>::uninit();
    // SAFETY: `limits` points to writable storage for the `rlimit` value that
    // `getrlimit` initializes on success.
    let result = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, limits.as_mut_ptr()) };
    if result != 0 {
        return None;
    }

    // SAFETY: A successful `getrlimit` call initialized `limits`.
    let soft_limit = unsafe { limits.assume_init() }.rlim_cur;
    soft_limit.try_into().ok()
}

#[cfg(target_os = "windows")]
fn process_open_files_limit(_process: &sysinfo::Process) -> Option<usize> {
    // `open_files` is the process's kernel handle count on Windows. Windows has
    // no fixed per-process handle limit comparable to that count.
    None
}

#[cfg(not(any(target_os = "macos", target_os = "windows")))]
fn process_open_files_limit(process: &sysinfo::Process) -> Option<usize> {
    process.open_files_limit()
}

fn create_and_register_counter(registry: &Registry, name: &str) -> Result<Counter, ApiError> {
    let definition = metric_definition(name);
    assert!(matches!(definition.kind, MetricKind::Counter));
    let metric = Counter::new(definition.runtime_name(), definition.description)
        .map_err(|error| metric_creation_error(name, error))?;
    register_metric(registry, metric.clone(), name)?;
    Ok(metric)
}

fn create_and_register_gauge(registry: &Registry, name: &str) -> Result<IntGauge, ApiError> {
    let definition = metric_definition(name);
    assert!(matches!(definition.kind, MetricKind::Gauge));
    let metric = IntGauge::new(definition.runtime_name(), definition.description)
        .map_err(|error| metric_creation_error(name, error))?;
    register_metric(registry, metric.clone(), name)?;
    Ok(metric)
}

fn register_metric<C>(registry: &Registry, metric: C, name: &str) -> Result<(), ApiError>
where
    C: Collector + 'static,
{
    registry.register(Box::new(metric)).map_err(|error| {
        ApiError::InternalServerError(format!("Failed to register {name} metric: {error}"))
    })
}

fn metric_creation_error(name: &str, error: prometheus::Error) -> ApiError {
    ApiError::InternalServerError(format!("Failed to create {name} metric: {error}"))
}

fn saturating_i64<T>(value: T) -> i64
where
    T: TryInto<i64>,
{
    value.try_into().unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use prometheus::{Encoder, TextEncoder};

    use super::*;

    #[test]
    fn supported_platform_exports_process_metrics() {
        let registry = Registry::new();
        let metrics = ProcessMetrics::new(&registry).unwrap();
        assert!(metrics.refresh());

        let mut encoded = Vec::new();
        TextEncoder::new()
            .encode(&registry.gather(), &mut encoded)
            .unwrap();
        let body = String::from_utf8(encoded).unwrap();

        for metric_name in [
            PROCESS_CPU_SECONDS,
            PROCESS_MAX_FDS,
            PROCESS_OPEN_FDS,
            PROCESS_RESIDENT_MEMORY,
            PROCESS_START_TIME,
            PROCESS_VIRTUAL_MEMORY,
        ] {
            assert!(
                body.contains(metric_name),
                "missing process metric: {metric_name}"
            );
        }
        assert!(metrics.open_fds.get() > 0);
        #[cfg(target_os = "windows")]
        assert_eq!(metrics.max_fds.get(), 0);
        #[cfg(not(target_os = "windows"))]
        assert!(metrics.max_fds.get() > 0);
        assert!(metrics.resident_memory_bytes.get() > 0);
        assert!(metrics.virtual_memory_bytes.get() > 0);
        assert!(metrics.start_time_seconds.get() > 0);
    }

    #[test]
    fn large_unsigned_values_saturate_at_signed_metric_limit() {
        assert_eq!(saturating_i64(u64::MAX), i64::MAX);
    }
}
