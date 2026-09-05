//! Process boundary for untrusted template compilation and execution.
//!
//! The worker owns a capped allocator. A failed allocation terminates only that
//! worker; the parent owns the wall-clock deadline, protocol and admission cap.
use std::borrow::Cow;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::runtime::Builder;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};
use tokio::task::JoinSet;
use tokio::time::{Instant, timeout_at};
use tracing::instrument::WithSubscriber;
use tracing::{Instrument, debug, info_span, warn};

use serde::{Deserialize, Serialize};

use crate::{MissingValue, SizeLimitedWriter, TemplateAutoEscape, TemplateError, TemplateLimits};

pub const MAX_WORKER_HEAP_BYTES: usize = 128 * 1024 * 1024;
const MAX_REQUEST_BYTES: usize = 16 * 1024 * 1024;
const MAX_OUTPUT_BYTES: usize = 32 * 1024 * 1024;
const DEADLINE: Duration = Duration::from_secs(5);
const MAX_CONCURRENT_WORKERS: usize = 4;
const MAX_WAITING_WORKERS: usize = 16;
static RUNTIME: OnceLock<Result<WorkerRuntime, String>> = OnceLock::new();
static SHUTTING_DOWN: AtomicBool = AtomicBool::new(false);
static NEXT_OPERATION: AtomicU64 = AtomicU64::new(1);
static EVENT_HANDLER: OnceLock<fn(WorkerEvent)> = OnceLock::new();

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum MissingDataPolicy {
    Lenient,
    Strict,
    Omit,
    Null,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum Operation {
    Syntax,
    Render,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct WorkerRequest<'a> {
    pub operation: Operation,
    pub name: Cow<'a, str>,
    pub source: Cow<'a, str>,
    pub sources: Cow<'a, [(String, String)]>,
    pub context: Cow<'a, serde_json::Value>,
    pub limits: TemplateLimits,
    pub auto_escape: TemplateAutoEscape,
    pub missing_data: MissingDataPolicy,
    pub max_output_bytes: usize,
}

#[derive(Serialize, Deserialize)]
pub struct RenderedTemplate {
    output: String,
    missing: Vec<MissingValue>,
    peak_heap_bytes: usize,
}
impl RenderedTemplate {
    pub fn peak_heap_bytes(&self) -> usize {
        self.peak_heap_bytes
    }
    pub(crate) fn set_peak_heap_bytes(&mut self, bytes: usize) {
        self.peak_heap_bytes = bytes;
    }
    pub fn into_parts(self) -> (String, Vec<MissingValue>) {
        (self.output, self.missing)
    }
    pub(crate) fn new(output: String, missing: Vec<MissingValue>) -> Self {
        Self {
            output,
            missing,
            peak_heap_bytes: 0,
        }
    }
}

/// A complete request for composed rendering. Configuration stays explicit and
/// template source never supplies the executable or the execution budgets.
pub struct TemplateExecution<'a> {
    name: &'a str,
    source: &'a str,
    sources: &'a [(String, String)],
    limits: TemplateLimits,
    auto_escape: TemplateAutoEscape,
    missing_data: MissingDataPolicy,
    max_output_bytes: usize,
}
impl<'a> TemplateExecution<'a> {
    pub fn new(name: &'a str, source: &'a str, limits: TemplateLimits) -> Self {
        Self {
            name,
            source,
            sources: &[],
            limits,
            auto_escape: TemplateAutoEscape::None,
            missing_data: MissingDataPolicy::Strict,
            max_output_bytes: limits.max_output_bytes().min(MAX_OUTPUT_BYTES),
        }
    }
    pub fn sources(mut self, sources: &'a [(String, String)]) -> Self {
        self.sources = sources;
        self
    }
    pub fn auto_escape(mut self, value: TemplateAutoEscape) -> Self {
        self.auto_escape = value;
        self
    }
    pub fn missing_data(mut self, value: MissingDataPolicy) -> Self {
        self.missing_data = value;
        self
    }
    pub fn max_output_bytes(mut self, value: usize) -> Self {
        self.max_output_bytes = value.min(MAX_OUTPUT_BYTES);
        self
    }
    pub async fn render(
        self,
        context: &serde_json::Value,
    ) -> Result<RenderedTemplate, TemplateError> {
        // Check before cloning attacker-controlled inputs into the protocol.
        let source_bytes = self
            .sources
            .iter()
            .try_fold(self.source.len(), |size, (name, body)| {
                size.checked_add(name.len())
                    .and_then(|size| size.checked_add(body.len()))
            });
        if source_bytes.is_none_or(|size| size > MAX_REQUEST_BYTES) {
            return Err(TemplateError::boundary(
                "template input or output budget exceeded",
            ));
        }
        execute(WorkerRequest {
            operation: Operation::Render,
            name: self.name.into(),
            source: self.source.into(),
            sources: Cow::Borrowed(self.sources),
            context: Cow::Borrowed(context),
            limits: self.limits,
            auto_escape: self.auto_escape,
            missing_data: self.missing_data,
            max_output_bytes: self.max_output_bytes,
        })
        .await
    }
}

pub(crate) async fn validate_syntax(
    source: &str,
    limits: TemplateLimits,
) -> Result<(), TemplateError> {
    if source.len() > MAX_REQUEST_BYTES {
        return Err(TemplateError::boundary("template input budget exceeded"));
    }
    execute(WorkerRequest {
        operation: Operation::Syntax,
        name: "template".into(),
        source: source.into(),
        sources: Cow::Borrowed(&[]),
        context: Cow::Owned(serde_json::Value::Null),
        limits,
        auto_escape: TemplateAutoEscape::None,
        missing_data: MissingDataPolicy::Strict,
        max_output_bytes: 0,
    })
    .await
    .map(|_| ())
}

fn worker_executable() -> Result<PathBuf, TemplateError> {
    let executable = std::env::current_exe()
        .map_err(|_| TemplateError::boundary("cannot locate template worker"))?;
    let mut directory = executable
        .parent()
        .ok_or_else(|| TemplateError::boundary("cannot locate template worker directory"))?;
    if directory.file_name().is_some_and(|name| name == "deps") {
        directory = directory
            .parent()
            .ok_or_else(|| TemplateError::boundary("cannot locate template worker directory"))?;
    }
    Ok(directory.join(format!(
        "hubuum-template-worker{}",
        std::env::consts::EXE_SUFFIX
    )))
}

/// Bounded-cardinality operational events. No template names, sources, context,
/// rendered output, or worker-supplied error strings cross this interface.
#[derive(Debug, Clone, Copy)]
pub struct WorkerEvent {
    kind: &'static str,
    elapsed: Duration,
}
impl WorkerEvent {
    pub fn kind(self) -> &'static str {
        self.kind
    }
    pub fn elapsed(self) -> Duration {
        self.elapsed
    }
}

/// Install application telemetry without coupling this crate to its registry.
pub fn set_worker_event_handler(handler: fn(WorkerEvent)) {
    let _ = EVENT_HANDLER.set(handler);
}
fn event(kind: &'static str, started: Instant) {
    let elapsed = started.elapsed();
    if matches!(
        kind,
        "completed" | "admitted" | "started" | "cancelled" | "shutdown"
    ) {
        debug!(
            event = kind,
            elapsed_ms = elapsed.as_millis() as u64,
            "Template worker lifecycle"
        );
    } else {
        warn!(
            event = kind,
            elapsed_ms = elapsed.as_millis() as u64,
            "Template worker lifecycle"
        );
    }
    if let Some(handler) = EVENT_HANDLER.get() {
        handler(WorkerEvent { kind, elapsed });
    }
}

struct WorkerRuntime {
    jobs: mpsc::Sender<Job>,
    capacity: Arc<Semaphore>,
    stopping: watch::Sender<bool>,
    stopped: watch::Receiver<bool>,
}
struct Job {
    encoded: String,
    max_response_bytes: usize,
    started: Instant,
    deadline: Instant,
    response: oneshot::Sender<Result<RenderedTemplate, TemplateError>>,
    capacity: OwnedSemaphorePermit,
    span: tracing::Span,
    dispatcher: tracing::Dispatch,
    command: Command,
}
impl WorkerRuntime {
    fn admit(&self, started: Instant) -> Result<OwnedSemaphorePermit, TemplateError> {
        self.capacity.clone().try_acquire_owned().map_err(|_| {
            event("overloaded", started);
            TemplateError::boundary("template worker capacity exhausted or supervisor stopped")
        })
    }
    fn start() -> Result<Self, String> {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| "cannot initialize template supervisor")?;
        let capacity = Arc::new(Semaphore::new(MAX_CONCURRENT_WORKERS + MAX_WAITING_WORKERS));
        let (jobs, mut incoming) =
            mpsc::channel::<Job>(MAX_CONCURRENT_WORKERS + MAX_WAITING_WORKERS);
        let (stopping, mut shutdown) = watch::channel(false);
        let (finished, stopped) = watch::channel(false);
        std::thread::Builder::new().name("template-supervisor".into()).spawn(move || {
            runtime.block_on(async move {
                let workers = Arc::new(Semaphore::new(MAX_CONCURRENT_WORKERS));
                let mut running = JoinSet::new();
                loop {
                    tokio::select! {
                        biased;
                        _ = shutdown.changed() => break,
                        // Completed jobs have already returned their admission
                        // slots. Drain their tracking entries before admitting
                        // more jobs, including under a continuously ready queue.
                        Some(result) = running.join_next() => {
                            if result.is_err() { warn!("Template supervisor task failed"); }
                        }
                        Some(job) = incoming.recv() => {
                            let span = job.span.clone();
                            let dispatcher = job.dispatcher.clone();
                            running.spawn(supervise(job, workers.clone(), shutdown.clone()).instrument(span).with_subscriber(dispatcher));
                            debug_assert!(running.len() <= MAX_CONCURRENT_WORKERS + MAX_WAITING_WORKERS);
                        }
                        else => break,
                    }
                }
                incoming.close();
                while let Some(job) = incoming.recv().await {
                    event("shutdown", job.started);
                    let _ = job.response.send(Err(TemplateError::boundary("template supervisor is shutting down")));
                }
                while running.join_next().await.is_some() {}
            });
            // Acknowledge only after every child has been killed and reaped and
            // every supervisor task has returned, including blocking decoding.
            drop(runtime);
            let _ = finished.send(true);
        }).map_err(|_| "cannot start template supervisor")?;
        Ok(Self {
            jobs,
            capacity,
            stopping,
            stopped,
        })
    }

    async fn shutdown(&self) {
        self.capacity.close();
        let _ = self.stopping.send(true);
        let mut stopped = self.stopped.clone();
        let _ = stopped.wait_for(|value| *value).await;
    }
}

/// Stop admission and drain children before the application's telemetry/runtime
/// shuts down. The supervisor is independent of individual HTTP runtimes.
pub async fn shutdown_template_workers() {
    SHUTTING_DOWN.store(true, Ordering::SeqCst);
    if let Some(Ok(runtime)) = RUNTIME.get() {
        runtime.shutdown().await;
    }
}

async fn execute(request: WorkerRequest<'_>) -> Result<RenderedTemplate, TemplateError> {
    let started = Instant::now();
    if SHUTTING_DOWN.load(Ordering::SeqCst) {
        return Err(TemplateError::boundary(
            "template supervisor is shutting down",
        ));
    }
    let runtime = RUNTIME
        .get_or_init(WorkerRuntime::start)
        .as_ref()
        .map_err(|message| TemplateError::boundary(message))?;
    // Shutdown may have raced the first lazy initialization. Never submit work
    // to a supervisor that was not yet visible to the shutdown caller.
    if SHUTTING_DOWN.load(Ordering::SeqCst) {
        runtime.shutdown().await;
        return Err(TemplateError::boundary(
            "template supervisor is shutting down",
        ));
    }
    let capacity = runtime.admit(started)?;
    let max_response_bytes = request
        .max_output_bytes
        .saturating_mul(6)
        .saturating_add(1024 * 1024);
    // Borrowed input is encoded into a size-bounded buffer before transferring
    // ownership. No unbounded clone of the caller's context is made.
    let mut encoded = SizeLimitedWriter::new(MAX_REQUEST_BYTES);
    serde_json::to_writer(&mut encoded, &request).map_err(|_| {
        event("input_limit", started);
        TemplateError::boundary("template input budget exceeded")
    })?;
    let encoded = encoded
        .into_string()
        .map_err(|_| TemplateError::boundary("invalid template protocol input"))?;
    let mut command = Command::new(worker_executable()?);
    command.env_clear();
    let (response, result) = oneshot::channel();
    let span = info_span!("template_execution", operation_id = NEXT_OPERATION.fetch_add(1, Ordering::Relaxed), operation = ?request.operation, pid = tracing::field::Empty);
    let job = Job {
        encoded,
        max_response_bytes,
        started,
        deadline: started + DEADLINE,
        response,
        capacity,
        span,
        dispatcher: tracing::dispatcher::get_default(Clone::clone),
        command,
    };
    runtime
        .jobs
        .try_send(job)
        .map_err(|_| TemplateError::boundary("template supervisor unavailable"))?;
    // Dropping this receiver cancels queued or executing work. The independent
    // supervisor retains its permits until pipe teardown and child reaping.
    result
        .await
        .map_err(|_| TemplateError::boundary("template supervisor unavailable"))?
}

async fn supervise(mut job: Job, workers: Arc<Semaphore>, mut shutdown: watch::Receiver<bool>) {
    let _capacity = job.capacity;
    event("admitted", job.started);
    let worker = tokio::select! {
        biased;
        _ = job.response.closed() => { event("cancelled", job.started); return; }
        _ = shutdown.wait_for(|stopping| *stopping) => {
            event("shutdown", job.started);
            let _ = job.response.send(Err(TemplateError::boundary("template supervisor is shutting down")));
            return;
        }
        permit = timeout_at(job.deadline, workers.acquire_owned()) => match permit {
            Ok(Ok(permit)) => permit,
            _ => {
                event("admission_timeout", job.started);
                let _ = job.response.send(Err(TemplateError::boundary("template execution admission deadline exceeded")));
                return;
            }
        }
    };
    let _worker = worker;
    if Instant::now() >= job.deadline {
        event("admission_timeout", job.started);
        let _ = job.response.send(Err(TemplateError::boundary(
            "template execution admission deadline exceeded",
        )));
        return;
    }
    let child = job
        .command
        .kill_on_drop(true)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn();
    let mut child = match child {
        Ok(child) => child,
        Err(_) => {
            event("spawn_failed", job.started);
            let _ = job.response.send(Err(TemplateError::boundary(
                "cannot start hubuum-template-worker; install it beside the application binary",
            )));
            return;
        }
    };
    if let Some(pid) = child.id() {
        tracing::Span::current().record("pid", pid);
    }
    debug!(pid = child.id(), "Template worker started");
    event("started", job.started);
    let result = async {
        let mut input = child
            .stdin
            .take()
            .ok_or_else(|| TemplateError::boundary("missing template worker input"))?;
        let output = child
            .stdout
            .take()
            .ok_or_else(|| TemplateError::boundary("missing template worker output"))?;
        let send = async {
            input
                .write_all(job.encoded.as_bytes())
                .await
                .map_err(|_| TemplateError::boundary("template input transport failed"))?;
            drop(input);
            Ok::<_, TemplateError>(())
        };
        let receive = async {
            let mut bytes = Vec::new();
            output
                .take(job.max_response_bytes as u64 + 1)
                .read_to_end(&mut bytes)
                .await
                .map_err(|_| TemplateError::boundary("template output transport failed"))?;
            if bytes.len() > job.max_response_bytes {
                return Err(TemplateError::boundary("template response budget exceeded"));
            }
            Ok(bytes)
        };
        let wait = async {
            let status = child
                .wait()
                .await
                .map_err(|_| TemplateError::boundary("template worker status unavailable"))?;
            if !status.success() {
                return Err(TemplateError::boundary(
                    "template worker exceeded its heap budget or terminated during execution",
                ));
            }
            Ok(())
        };
        let (_, bytes, ()) = tokio::try_join!(send, receive, wait)?;
        Ok::<_, TemplateError>(bytes)
    };
    let (kind, result) = tokio::select! {
        biased;
        _ = job.response.closed() => ("cancelled", Err(TemplateError::boundary("template execution cancelled"))),
        _ = shutdown.wait_for(|stopping| *stopping) => ("shutdown", Err(TemplateError::boundary("template supervisor is shutting down"))),
        result = timeout_at(job.deadline, result) => match result {
            Ok(Ok(bytes)) => ("completed", Ok(bytes)),
            Ok(Err(error)) => ("worker_failed", Err(error)),
            Err(_) => ("execution_timeout", Err(TemplateError::boundary("template execution deadline exceeded"))),
        }
    };
    if result.is_err() {
        let _ = child.start_kill();
    }
    // Always reap before releasing either admission permit, even when the
    // request's future or its entire HTTP runtime has already disappeared.
    let reaped = child.wait().await;
    debug!(status = ?reaped, "Template worker reaped");
    let result = match result {
        Ok(bytes) => {
            // JSON decoding can be substantial at the bounded output ceiling.
            // It must not stall process supervision or request executor threads.
            tokio::task::spawn_blocking(move || {
                let response: Result<RenderedTemplate, String> = serde_json::from_slice(&bytes)
                    .map_err(|_| TemplateError::boundary("invalid template worker response"))?;
                response.map_err(|message| TemplateError::boundary(&message))
            })
            .await
            .unwrap_or_else(|_| Err(TemplateError::boundary("template response decoding failed")))
        }
        Err(error) => Err(error),
    };
    let (kind, result) = if kind != "completed" {
        (kind, result)
    } else if job.response.is_closed() {
        (
            "cancelled",
            Err(TemplateError::boundary("template execution cancelled")),
        )
    } else if *shutdown.borrow() {
        (
            "shutdown",
            Err(TemplateError::boundary(
                "template supervisor is shutting down",
            )),
        )
    } else if Instant::now() >= job.deadline {
        (
            "execution_timeout",
            Err(TemplateError::boundary(
                "template execution deadline exceeded",
            )),
        )
    } else if result.is_err() {
        ("render_failed", result)
    } else {
        ("completed", result)
    };
    event(kind, job.started);
    let _ = job.response.send(result);
}

#[cfg(test)]
mod tests;
