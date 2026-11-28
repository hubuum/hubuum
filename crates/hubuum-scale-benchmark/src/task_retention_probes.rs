//! Real remote effects and retry recognition across purge and logical/physical recovery.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use clap::{Args, ValueEnum};
use hubuum_domain::{TaskId, TaskIdentity};
use hubuum_storage_postgres::scale_benchmark::PostgresScaleBackend;
use reqwest::{Client, StatusCode, Url};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::Result;

const MAX_BYTES: usize = 1024 * 1024;
const POLL: Duration = Duration::from_millis(100);
const QUIET: Duration = Duration::from_secs(2);

#[derive(Clone, Copy, Debug, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum ProbePhase {
    Seed,
    /// The fixture accepts the effect but holds its HTTP response open.
    Dispatched,
    /// Check that unrelated cleanup did not stop the blocked remote task.
    StillDispatched,
    /// Retry after producers were killed before terminal persistence.
    RecoveredDispatched,
    Purged,
    Restored,
}

#[derive(Debug, Args)]
pub struct ProbeArgs {
    #[arg(long, value_enum)]
    phase: ProbePhase,
    #[arg(long)]
    origin: String,
    /// A second production replica for concurrent duplicate admission.
    #[arg(long)]
    other_origin: Option<String>,
    /// Owner-only JSON token array; only its first token is used.
    #[arg(long)]
    token_file: PathBuf,
    /// Synthetic endpoint, key, request variants and isolated fixture counter URL.
    #[arg(long)]
    request_file: PathBuf,
    /// Seed writes this owner-only identity proof; later phases read it.
    #[arg(long)]
    state_file: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    database_url: String,
    #[arg(long, default_value_t = 60)]
    timeout_seconds: u64,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Request {
    version: u32,
    endpoint: String,
    key: String,
    body: Value,
    conflict_body: Value,
    counter_url: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ProbeState {
    request_sha256: String,
    task_id: i32,
    task_identity: TaskIdentity,
}

#[derive(Serialize)]
struct Report {
    phase: ProbePhase,
    task_id: i32,
    task_identity: TaskIdentity,
    effect_count_before: u64,
    effect_count_after: u64,
    unavailable_attempts: u32,
    admission_ms: Option<u128>,
    terminal_ms: Option<u128>,
    physical_observation: Option<Value>,
    recovered_status: Option<String>,
    quiet_ms: u128,
    elapsed_ms: u128,
}

pub(super) fn input<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if fs::metadata(path)?.permissions().mode() & 0o077 != 0 {
            return Err("Probe inputs must be owner-only files".into());
        }
    }
    let mut bytes = Vec::new();
    File::open(path)?
        .take((MAX_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_BYTES {
        return Err("Probe input exceeds 1 MiB".into());
    }
    Ok(serde_json::from_slice(&bytes)?)
}

pub(super) fn output(path: &Path, value: &impl Serialize) -> Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(())
}

pub(super) fn api_origin(value: &str) -> Result<String> {
    let url = Url::parse(value)?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err("Probe API address must be an HTTP(S) origin without credentials".into());
    }
    Ok(value.trim_end_matches('/').to_owned())
}

async fn json(mut response: reqwest::Response) -> Result<Value> {
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if bytes.len() + chunk.len() > MAX_BYTES {
            return Err("Probe response exceeds 1 MiB".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(serde_json::from_slice(&bytes)?)
}

async fn effects(client: &Client, request: &Request) -> Result<u64> {
    let response = client.get(&request.counter_url).send().await?;
    if response.status() != StatusCode::OK {
        return Err("Isolated remote effect counter is unavailable".into());
    }
    json(response)
        .await?
        .get("count")
        .and_then(Value::as_u64)
        .ok_or_else(|| "Remote fixture returned no effect count".into())
}

async fn submit(
    client: &Client,
    endpoint: &str,
    token: &str,
    request: &Request,
    changed: bool,
) -> Result<(StatusCode, Value, u32)> {
    let mut unavailable = 0;
    loop {
        let response = client
            .post(endpoint)
            .bearer_auth(token)
            .header("Idempotency-Key", &request.key)
            .json(if changed {
                &request.conflict_body
            } else {
                &request.body
            })
            .send()
            .await?;
        let status = response.status();
        let body = json(response).await?;
        if status != StatusCode::SERVICE_UNAVAILABLE {
            return Ok((status, body, unavailable));
        }
        // Nonblocking cross-replica publication can reject a contending helper.
        // Keep the same key and report every transient response, never hide it.
        unavailable += 1;
        tokio::time::sleep(POLL).await;
    }
}

fn accepted(status: StatusCode, response: &Value) -> Result<i32> {
    if status != StatusCode::ACCEPTED {
        return Err(format!("Remote admission returned HTTP {status}, expected 202").into());
    }
    response
        .get("id")
        .and_then(Value::as_i64)
        .and_then(|id| i32::try_from(id).ok())
        .filter(|id| *id > 0)
        .ok_or_else(|| "Remote admission returned no positive task ID".into())
}

async fn quiet(client: &Client, request: &Request) -> Result<Duration> {
    let started = Instant::now();
    loop {
        if effects(client, request).await? != 1 {
            return Err("Idempotent remote retry changed the single observed effect".into());
        }
        if started.elapsed() >= QUIET {
            return Ok(started.elapsed());
        }
        tokio::time::sleep(POLL).await;
    }
}

pub(super) async fn terminal(client: &Client, origin: &str, token: &str, task: i32) -> Result<()> {
    loop {
        let response = client
            .get(format!("{origin}/api/v1/tasks/{task}"))
            .bearer_auth(token)
            .send()
            .await?;
        if response.status() != StatusCode::OK {
            return Err(format!("Probe task polling returned HTTP {}", response.status()).into());
        }
        match json(response).await?.get("status").and_then(Value::as_str) {
            Some("succeeded") => return Ok(()),
            Some("queued" | "validating" | "running") => tokio::time::sleep(POLL).await,
            _ => return Err("Remote effect probe did not succeed".into()),
        }
    }
}

async fn require_running(client: &Client, origin: &str, token: &str, task: i32) -> Result<()> {
    let response = client
        .get(format!("{origin}/api/v1/tasks/{task}"))
        .bearer_auth(token)
        .send()
        .await?;
    if response.status() != StatusCode::OK
        || json(response).await?.get("status").and_then(Value::as_str) != Some("running")
    {
        return Err("The dispatched remote task stopped before its controlled interruption".into());
    }
    Ok(())
}

async fn dispatched(
    client: &Client,
    origin: &str,
    token: &str,
    request: &Request,
    task: i32,
) -> Result<()> {
    loop {
        match effects(client, request).await? {
            0 => tokio::time::sleep(POLL).await,
            1 => return require_running(client, origin, token, task).await,
            _ => return Err("Remote dispatch produced more than one effect".into()),
        }
    }
}

async fn probe(options: &ProbeArgs, request: &Request, token: &str) -> Result<Report> {
    let started = Instant::now();
    let origin = api_origin(&options.origin)?;
    let other = options
        .other_origin
        .as_deref()
        .map(api_origin)
        .transpose()?;
    let primary = format!("{origin}{}", request.endpoint);
    let secondary = format!(
        "{}{}",
        other.as_deref().unwrap_or(&origin),
        request.endpoint
    );
    let client = Client::builder().timeout(Duration::from_secs(10)).build()?;
    let before = effects(&client, request).await?;
    let request_sha256 = Sha256::digest(serde_json::to_vec(request)?)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let mut unavailable_attempts = 0;
    let mut admission_ms = None;
    let mut terminal_ms = None;
    let mut physical_observation = None;
    let mut recovered_status = None;
    let state = match options.phase {
        ProbePhase::Seed | ProbePhase::Dispatched => {
            if before != 0 || options.state_file.exists() {
                return Err("Seed requires an unused effect counter and absent state file".into());
            }
            let admitted = Instant::now();
            let (first, duplicate) = tokio::join!(
                submit(&client, &primary, token, request, false),
                submit(&client, &secondary, token, request, false),
            );
            let (status, response, attempts) = first?;
            unavailable_attempts += attempts;
            let task_id = accepted(status, &response)?;
            let (status, response, attempts) = duplicate?;
            unavailable_attempts += attempts;
            if accepted(status, &response)? != task_id {
                return Err("Concurrent duplicate created a different task".into());
            }
            admission_ms = Some(admitted.elapsed().as_millis());
            let (status, _, attempts) = submit(&client, &secondary, token, request, true).await?;
            unavailable_attempts += attempts;
            if status != StatusCode::CONFLICT {
                return Err("Changed pending remote request did not conflict".into());
            }
            let backend = PostgresScaleBackend::connect(&options.database_url, 1)?;
            let native = TaskId::new(task_id)?;
            let identities = backend.resolve_task_retention_cohort(&[native]).await?;
            let task_identity = identities
                .first()
                .filter(|(id, _)| *id == native)
                .map(|(_, identity)| *identity)
                .ok_or("Remote probe task UUID is absent")?;
            if matches!(options.phase, ProbePhase::Dispatched) {
                dispatched(&client, &origin, token, request, task_id).await?;
            } else {
                let completing = Instant::now();
                terminal(&client, &origin, token, task_id).await?;
                terminal_ms = Some(completing.elapsed().as_millis());
            }
            let (status, response, attempts) =
                submit(&client, &secondary, token, request, false).await?;
            unavailable_attempts += attempts;
            if accepted(status, &response)? != task_id {
                return Err("Terminal duplicate changed task identity".into());
            }
            let (status, _, attempts) = submit(&client, &primary, token, request, true).await?;
            unavailable_attempts += attempts;
            if status != StatusCode::CONFLICT {
                return Err("Changed terminal remote request did not conflict".into());
            }
            ProbeState {
                request_sha256,
                task_id,
                task_identity,
            }
        }
        ProbePhase::StillDispatched | ProbePhase::RecoveredDispatched => {
            let state: ProbeState = input(&options.state_file)?;
            if state.request_sha256 != request_sha256 || before != 1 {
                return Err("Dispatch retry does not match its seeded request and effect".into());
            }
            for endpoint in [&primary, &secondary] {
                let (status, response, attempts) =
                    submit(&client, endpoint, token, request, false).await?;
                unavailable_attempts += attempts;
                if matches!(options.phase, ProbePhase::StillDispatched) {
                    if accepted(status, &response)? != state.task_id {
                        return Err("Running duplicate changed the original task".into());
                    }
                } else if status == StatusCode::ACCEPTED {
                    if accepted(status, &response)? != state.task_id
                        || response.get("status").and_then(Value::as_str) != Some("failed")
                    {
                        return Err("Recovered dispatch was not settled without replay".into());
                    }
                    recovered_status = Some("failed".to_owned());
                } else {
                    let identity = response
                        .get("task_identity")
                        .cloned()
                        .map(serde_json::from_value)
                        .transpose()?;
                    if status != StatusCode::GONE || identity != Some(state.task_identity) {
                        return Err("Recovered dispatch lost the original receipt identity".into());
                    }
                    recovered_status = Some("details_expired".to_owned());
                }
                let (status, _, attempts) = submit(&client, endpoint, token, request, true).await?;
                unavailable_attempts += attempts;
                if status != StatusCode::CONFLICT {
                    return Err("Changed dispatched request did not conflict".into());
                }
            }
            let backend = PostgresScaleBackend::connect(&options.database_url, 1)?;
            let observations = backend
                .observe_task_retention(&[state.task_identity])
                .await?;
            let observation = observations
                .first()
                .filter(|observed| {
                    observations.len() == 1
                        && observed.identity() == state.task_identity
                        && observed.receipt_present()
                })
                .ok_or("Dispatch retry lost its protected stable task receipt")?;
            if matches!(options.phase, ProbePhase::StillDispatched) {
                if observation.task_id().map(TaskId::id) != Some(state.task_id)
                    || observation.terminal_at_ms().is_some()
                    || observation.receipt_expires_at_ms().is_some()
                {
                    return Err(
                        "Running dispatch unexpectedly settled or acquired an expiry".into(),
                    );
                }
            } else if observation.terminal_at_ms().is_none()
                || observation.receipt_expires_at_ms().is_none()
            {
                return Err("Recovered dispatch lacks a fixed terminal receipt anchor".into());
            }
            physical_observation = Some(serde_json::to_value(observation)?);
            state
        }
        ProbePhase::Purged | ProbePhase::Restored => {
            let state: ProbeState = input(&options.state_file)?;
            if state.request_sha256 != request_sha256 || before != 1 {
                return Err("Retry phase does not match its seeded request and effect".into());
            }
            for endpoint in [&primary, &secondary] {
                let (status, response, attempts) =
                    submit(&client, endpoint, token, request, false).await?;
                unavailable_attempts += attempts;
                let identity = response
                    .get("task_identity")
                    .cloned()
                    .map(serde_json::from_value)
                    .transpose()?;
                if status != StatusCode::GONE || identity != Some(state.task_identity) {
                    return Err("Purged or restored retry did not recognize the original task UUID with HTTP 410".into());
                }
                let (status, _, attempts) = submit(&client, endpoint, token, request, true).await?;
                unavailable_attempts += attempts;
                if status != StatusCode::CONFLICT {
                    return Err("Changed purged or restored request did not conflict".into());
                }
            }
            let backend = PostgresScaleBackend::connect(&options.database_url, 1)?;
            let observations = backend
                .observe_task_retention(&[state.task_identity])
                .await?;
            let observation = observations
                .first()
                .filter(|observed| {
                    observations.len() == 1
                        && observed.identity() == state.task_identity
                        && observed.task_id().is_none()
                        && observed.child_rows() == 0
                        && observed.output_rows() == 0
                        && observed.details_removed() == Some(true)
                        && observed.receipt_present()
                })
                .ok_or("Recognized retry lacks physical removal and retained receipt evidence")?;
            physical_observation = Some(serde_json::to_value(observation)?);
            state
        }
    };
    let quiet_duration = quiet(&client, request).await?;
    if matches!(
        options.phase,
        ProbePhase::Dispatched | ProbePhase::StillDispatched
    ) {
        require_running(&client, &origin, token, state.task_id).await?;
    }
    let after = effects(&client, request).await?;
    if matches!(options.phase, ProbePhase::Seed | ProbePhase::Dispatched) {
        output(&options.state_file, &state)?;
    }
    Ok(Report {
        phase: options.phase,
        task_id: state.task_id,
        task_identity: state.task_identity,
        effect_count_before: before,
        effect_count_after: after,
        unavailable_attempts,
        admission_ms,
        terminal_ms,
        physical_observation,
        recovered_status,
        quiet_ms: quiet_duration.as_millis(),
        elapsed_ms: started.elapsed().as_millis(),
    })
}

pub async fn run(options: ProbeArgs) -> Result<()> {
    let request: Request = input(&options.request_file)?;
    let tokens: Vec<String> = input(&options.token_file)?;
    let target = request
        .endpoint
        .strip_prefix("/api/v1/remote-targets/")
        .and_then(|path| path.strip_suffix("/invoke"))
        .and_then(|id| id.parse::<i32>().ok());
    let counter = Url::parse(&request.counter_url)?;
    if !(3..=600).contains(&options.timeout_seconds)
        || request.version != 1
        || target.is_none_or(|id| id <= 0)
        || request.key.is_empty()
        || request.key.len() > 128
        || !request
            .key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        || !request.body.is_object()
        || !request.conflict_body.is_object()
        || request.body == request.conflict_body
        || tokens.is_empty()
        || tokens.len() > 32
        || tokens[0].is_empty()
        || !matches!(counter.scheme(), "http" | "https")
        || counter.host_str().is_none()
        || !counter.username().is_empty()
        || counter.password().is_some()
        || counter.fragment().is_some()
        || options.output.exists()
    {
        return Err("Invalid bounded remote retention probe settings".into());
    }
    let report = tokio::time::timeout(
        Duration::from_secs(options.timeout_seconds),
        probe(&options, &request, &tokens[0]),
    )
    .await
    .map_err(|_| "Remote retention probe exceeded its deadline")??;
    output(&options.output, &report)
}
