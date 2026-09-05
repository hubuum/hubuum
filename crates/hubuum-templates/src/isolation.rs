//! Process boundary for untrusted template compilation and execution.
//!
//! The worker owns a capped allocator. A failed allocation terminates only that
//! worker; the parent owns the wall-clock deadline, protocol and admission cap.
use std::borrow::Cow;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::{MissingValue, SizeLimitedWriter, TemplateAutoEscape, TemplateError, TemplateLimits};

pub const MAX_WORKER_HEAP_BYTES: usize = 128 * 1024 * 1024;
const MAX_REQUEST_BYTES: usize = 16 * 1024 * 1024;
const MAX_OUTPUT_BYTES: usize = 32 * 1024 * 1024;
const DEADLINE: Duration = Duration::from_secs(5);
const MAX_CONCURRENT_WORKERS: usize = 4;
static ADMISSION: (Mutex<usize>, Condvar) = (Mutex::new(0), Condvar::new());

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum MissingDataPolicy {
    Lenient,
    Strict,
    Omit,
    Null,
}

#[derive(Serialize, Deserialize)]
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
    pub fn render(self, context: &serde_json::Value) -> Result<RenderedTemplate, TemplateError> {
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
    }
}

pub(crate) fn validate_syntax(source: &str, limits: TemplateLimits) -> Result<(), TemplateError> {
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
    .map(|_| ())
}

struct Permit;
impl Permit {
    fn acquire(deadline: Instant) -> Result<Self, TemplateError> {
        let (lock, ready) = &ADMISSION;
        let mut active = lock.lock().unwrap_or_else(|error| error.into_inner());
        while *active >= MAX_CONCURRENT_WORKERS {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(TemplateError::boundary(
                    "template execution admission deadline exceeded",
                ));
            }
            let (guard, timeout) = ready
                .wait_timeout(active, remaining)
                .unwrap_or_else(|error| error.into_inner());
            active = guard;
            if timeout.timed_out() {
                return Err(TemplateError::boundary(
                    "template execution admission deadline exceeded",
                ));
            }
        }
        *active += 1;
        Ok(Self)
    }
}
impl Drop for Permit {
    fn drop(&mut self) {
        let (lock, ready) = &ADMISSION;
        *lock.lock().unwrap_or_else(|error| error.into_inner()) -= 1;
        ready.notify_one();
    }
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

fn execute(request: WorkerRequest<'_>) -> Result<RenderedTemplate, TemplateError> {
    let deadline = Instant::now() + DEADLINE;
    let max_response_bytes = request
        .max_output_bytes
        .saturating_mul(6)
        .saturating_add(1024 * 1024);
    let _permit = Permit::acquire(deadline)?;
    let mut encoded = SizeLimitedWriter::new(MAX_REQUEST_BYTES);
    serde_json::to_writer(&mut encoded, &request)
        .map_err(|_| TemplateError::boundary("template input budget exceeded"))?;
    let encoded = encoded
        .into_string()
        .map_err(|_| TemplateError::boundary("invalid template protocol input"))?;
    let mut child = Command::new(worker_executable()?)
        .env_clear()
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|_| {
            TemplateError::boundary(
                "cannot start hubuum-template-worker; install it beside the application binary",
            )
        })?;
    let mut input = child
        .stdin
        .take()
        .ok_or_else(|| TemplateError::boundary("missing template worker input"))?;
    let output = child
        .stdout
        .take()
        .ok_or_else(|| TemplateError::boundary("missing template worker output"))?;
    std::thread::scope(|scope| {
        let send = scope.spawn(move || input.write_all(encoded.as_bytes()));
        let receive = scope.spawn(move || {
            let mut bytes = Vec::new();
            output
                .take(max_response_bytes as u64)
                .read_to_end(&mut bytes)
                .map(|_| bytes)
        });
        let result = loop {
            match child.try_wait() {
                Ok(Some(status)) => {
                    break if status.success() {
                        Ok(())
                    } else {
                        Err(TemplateError::boundary(
                            "template worker exceeded its heap budget or terminated during execution",
                        ))
                    };
                }
                Ok(None) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(5))
                }
                Ok(None) => {
                    break Err(TemplateError::boundary(
                        "template execution deadline exceeded",
                    ));
                }
                Err(_) => {
                    break Err(TemplateError::boundary(
                        "template worker status unavailable",
                    ));
                }
            }
        };
        // Reap on every path, including timeouts and protocol failures, before
        // joining pipe threads. Killing also unblocks a stalled input writer.
        if result.is_err() {
            let _ = child.kill();
        }
        let _ = child.wait();
        let sent = send
            .join()
            .map_err(|_| TemplateError::boundary("template input transport failed"))?;
        let bytes = receive
            .join()
            .map_err(|_| TemplateError::boundary("template output transport failed"))?
            .map_err(|_| TemplateError::boundary("template output transport failed"))?;
        result?;
        sent.map_err(|_| TemplateError::boundary("template input transport failed"))?;
        let response: Result<RenderedTemplate, String> = serde_json::from_slice(&bytes)
            .map_err(|_| TemplateError::boundary("invalid template worker response"))?;
        response.map_err(|message| TemplateError::boundary(&message))
    })
}
