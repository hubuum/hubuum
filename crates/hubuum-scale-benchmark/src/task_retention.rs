//! Actual HTTP admission/protection workload. No clocks or task rows are synthesized.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use clap::Args;
use futures_util::{StreamExt, stream};
use hubuum_domain::{TaskId, TaskIdentity};
use hubuum_storage_postgres::scale_benchmark::PostgresScaleBackend;
use reqwest::{Client, StatusCode, Url};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{Result, write_json};

const MAX_INPUT_BYTES: u64 = 4 * 1024 * 1024;
const MAX_RESPONSE_BYTES: usize = 1024 * 1024;

#[derive(Debug, Args)]
pub struct TaskRetentionArgs {
    /// Running production API origins; the driver owns their lifecycle and resource limits
    #[arg(long, required = true, num_args = 1..)]
    origins: Vec<String>,
    /// Owner-only JSON array of bearer tokens; never copied to measurement output
    #[arg(long)]
    token_file: PathBuf,
    /// Versioned synthetic request recipe and exact cohort/concurrency settings
    #[arg(
        long,
        default_value = "scale-benchmarks/lifecycle/task-retention-v1.json"
    )]
    workload_spec: PathBuf,
    /// Unique safe ASCII prefix for this fresh cohort's idempotency keys
    #[arg(long)]
    cohort: String,
    #[arg(long)]
    output: PathBuf,
    /// Disposable database connection used only to pin exact admitted task identities
    #[arg(long)]
    database_url: String,
    /// Optional existing artifact directory for before/after production metric scrapes
    #[arg(long)]
    metric_directory: Option<PathBuf>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Workload {
    version: u32,
    count: usize,
    concurrency: usize,
    timeout_seconds: u64,
    duplicate_every: usize,
    conflict_every: usize,
    endpoint: String,
    body: Value,
    conflict_body: Value,
}

#[derive(Serialize)]
struct Admission {
    sequence: usize,
    principal_index: usize,
    origin_index: usize,
    task_id: i64,
    task_identity: Option<TaskIdentity>,
    admission_ms: u128,
    duplicate_ms: Option<u128>,
    conflict_ms: Option<u128>,
    unavailable_attempts: u64,
}

fn read_input(path: &Path) -> Result<Vec<u8>> {
    if fs::metadata(path)?.len() > MAX_INPUT_BYTES {
        return Err("Task-retention workload input exceeds 4 MiB".into());
    }
    Ok(fs::read(path)?)
}

fn instantiate(value: &Value, sequence: usize, cohort: &str) -> Value {
    match value {
        Value::String(value) => Value::String(
            value
                .replace("${sequence}", &sequence.to_string())
                .replace("${cohort}", cohort),
        ),
        Value::Array(values) => Value::Array(
            values
                .iter()
                .map(|value| instantiate(value, sequence, cohort))
                .collect(),
        ),
        Value::Object(values) => Value::Object(
            values
                .iter()
                .map(|(key, value)| (key.clone(), instantiate(value, sequence, cohort)))
                .collect(),
        ),
        value => value.clone(),
    }
}

async fn response_json(mut response: reqwest::Response) -> Result<Value> {
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if bytes.len() + chunk.len() > MAX_RESPONSE_BYTES {
            return Err("Task admission response exceeds the measurement bound".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(serde_json::from_slice(&bytes)?)
}

async fn submit(
    client: &Client,
    url: &str,
    token: &str,
    key: &str,
    body: &Value,
) -> Result<(StatusCode, Value)> {
    let response = client
        .post(url)
        .bearer_auth(token)
        .header("Idempotency-Key", key)
        .json(body)
        .send()
        .await?;
    let status = response.status();
    Ok((status, response_json(response).await?))
}

async fn submit_until(
    client: &Client,
    url: &str,
    token: &str,
    key: &str,
    body: &Value,
    deadline: Instant,
) -> Result<(StatusCode, Value, u64)> {
    let mut unavailable = 0;
    loop {
        let (status, body) = tokio::time::timeout(
            deadline.saturating_duration_since(Instant::now()),
            submit(client, url, token, key, body),
        )
        .await??;
        if status != StatusCode::SERVICE_UNAVAILABLE {
            return Ok((status, body, unavailable));
        }
        unavailable += 1;
        if Instant::now() >= deadline {
            return Err(
                format!("Admission unavailable after {unavailable} actual attempts").into(),
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn one(
    client: &Client,
    workload: &Workload,
    origins: &[String],
    tokens: &[String],
    cohort: &str,
    sequence: usize,
    backend: &PostgresScaleBackend,
) -> Result<Admission> {
    let principal_index = if tokens.len() == 1 || sequence % 10 < 8 {
        0
    } else {
        1 + sequence % (tokens.len() - 1)
    };
    let origin_index = sequence % origins.len();
    let url = format!("{}{}", origins[origin_index], workload.endpoint);
    let key = format!("{cohort}-{sequence}");
    let body = instantiate(&workload.body, sequence, cohort);
    let started = Instant::now();
    let (status, response, mut unavailable_attempts) = submit_until(
        client,
        &url,
        &tokens[principal_index],
        &key,
        &body,
        started + Duration::from_secs(workload.timeout_seconds),
    )
    .await?;
    if status != StatusCode::ACCEPTED {
        return Err(format!("Fresh admission returned HTTP {status}").into());
    }
    let task_id = response
        .get("id")
        .and_then(Value::as_i64)
        .filter(|id| *id > 0)
        .ok_or("Admission response has no positive task ID")?;
    let admission_ms = started.elapsed().as_millis();
    let native_id = TaskId::new(i32::try_from(task_id)?)?;
    let resolved = backend.resolve_task_retention_cohort(&[native_id]).await?;
    let task_identity = resolved
        .first()
        .filter(|(id, _)| *id == native_id)
        .map(|(_, identity)| *identity)
        .ok_or("Admitted task identity is missing")?;
    let retry_url = format!(
        "{}{}",
        origins[(origin_index + 1) % origins.len()],
        workload.endpoint
    );
    let duplicate_ms = if sequence.is_multiple_of(workload.duplicate_every) {
        let started = Instant::now();
        let (status, duplicate, unavailable) = submit_until(
            client,
            &retry_url,
            &tokens[principal_index],
            &key,
            &body,
            started + Duration::from_secs(workload.timeout_seconds),
        )
        .await?;
        unavailable_attempts += unavailable;
        if status != StatusCode::ACCEPTED
            || duplicate.get("id").and_then(Value::as_i64) != Some(task_id)
        {
            return Err(
                "Duplicate admission did not recognize the original task generation".into(),
            );
        }
        Some(started.elapsed().as_millis())
    } else {
        None
    };
    let conflict_ms = if sequence.is_multiple_of(workload.conflict_every) {
        let changed = instantiate(&workload.conflict_body, sequence, cohort);
        let started = Instant::now();
        let (status, _, unavailable) = submit_until(
            client,
            &retry_url,
            &tokens[principal_index],
            &key,
            &changed,
            started + Duration::from_secs(workload.timeout_seconds),
        )
        .await?;
        unavailable_attempts += unavailable;
        if status != StatusCode::CONFLICT {
            return Err(
                format!("Changed request returned HTTP {status}, expected conflict").into(),
            );
        }
        Some(started.elapsed().as_millis())
    } else {
        None
    };
    Ok(Admission {
        sequence,
        principal_index,
        origin_index,
        task_id,
        task_identity: Some(task_identity),
        admission_ms,
        duplicate_ms,
        conflict_ms,
        unavailable_attempts,
    })
}

async fn metrics(
    client: &Client,
    origins: &[String],
    directory: Option<&Path>,
    phase: &str,
) -> Result<()> {
    let Some(directory) = directory else {
        return Ok(());
    };
    for (index, origin) in origins.iter().enumerate() {
        let mut response = client.get(format!("{origin}/metrics")).send().await?;
        if !response.status().is_success() {
            return Err("Production metrics endpoint is unavailable".into());
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            if bytes.len() + chunk.len() > MAX_INPUT_BYTES as usize {
                return Err("Production metrics scrape exceeds 4 MiB".into());
            }
            bytes.extend_from_slice(&chunk);
        }
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(directory.join(format!("{phase}-api-{index}.prom")))?;
        file.write_all(&bytes)?;
    }
    Ok(())
}

pub async fn run(options: TaskRetentionArgs) -> Result<()> {
    let workload: Workload = serde_json::from_slice(&read_input(&options.workload_spec)?)?;
    let tokens: Vec<String> = serde_json::from_slice(&read_input(&options.token_file)?)?;
    if workload.version != 1
        || !(1..=10000).contains(&workload.count)
        || !(1..=128).contains(&workload.concurrency)
        || workload.timeout_seconds == 0
        || workload.timeout_seconds > 600
        || workload.duplicate_every == 0
        || workload.conflict_every == 0
        || !workload.endpoint.starts_with("/api/v1/")
        || !workload.body.is_object()
        || workload.body == workload.conflict_body
        || tokens.is_empty()
        || tokens.len() > 32
        || tokens.iter().any(String::is_empty)
        || options.cohort.is_empty()
        || options.cohort.len() > 64
        || !options
            .cohort
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        return Err("Invalid bounded task-retention workload settings".into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if fs::metadata(&options.token_file)?.permissions().mode() & 0o077 != 0 {
            return Err("Bearer token file must be owner-only".into());
        }
    }
    if options.output.exists() {
        return Err("Refusing an existing measurement output path".into());
    }
    let origins = options
        .origins
        .iter()
        .map(|origin| {
            let url = Url::parse(origin)?;
            if !matches!(url.scheme(), "http" | "https")
                || url.host_str().is_none()
                || url.path() != "/"
                || url.query().is_some()
                || !url.username().is_empty()
                || url.password().is_some()
            {
                return Err(
                    "Measurement origins must be HTTP(S) origins without credentials or paths"
                        .into(),
                );
            }
            Ok(origin.trim_end_matches('/').to_owned())
        })
        .collect::<Result<Vec<_>>>()?;
    let client = Client::builder()
        .timeout(Duration::from_secs(workload.timeout_seconds))
        .build()?;
    metrics(
        &client,
        &origins,
        options.metric_directory.as_deref(),
        "before-admission",
    )
    .await?;
    let started = Instant::now();
    let backend = PostgresScaleBackend::connect(&options.database_url, 8)?;
    let results = stream::iter(0..workload.count)
        .map(|sequence| {
            one(
                &client,
                &workload,
                &origins,
                &tokens,
                &options.cohort,
                sequence,
                &backend,
            )
        })
        .buffer_unordered(workload.concurrency)
        .collect::<Vec<_>>()
        .await;
    let mut admissions = Vec::new();
    let mut failures = Vec::new();
    for result in results {
        match result {
            Ok(value) => admissions.push(value),
            Err(error) => failures.push(error.to_string()),
        }
    }
    admissions.sort_unstable_by_key(|value| value.sequence);
    metrics(
        &client,
        &origins,
        options.metric_directory.as_deref(),
        "after-admission",
    )
    .await?;
    write_json(
        &options.output,
        &json!({
            "format_version":1, "phase":"protected_admission", "workload_file":options.workload_spec,
            "cohort":options.cohort, "requested":workload.count, "completed":admissions.len(), "failures":failures,
            "concurrency":workload.concurrency, "principals":tokens.len(), "origins":origins.len(),
            "elapsed_ms":started.elapsed().as_millis(), "admissions":admissions,
            "expiry_acceptance_measured":false,
        }),
    )?;
    if !failures.is_empty() {
        return Err("Protected admission workload failed; retained all results in report".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn workload_substitution_preserves_json_types_and_only_changes_declared_strings() {
        let value = json!({"description":"${cohort}-${sequence}", "count":2, "nested":["${sequence}",true]});
        assert_eq!(
            instantiate(&value, 7, "run"),
            json!({"description":"run-7", "count":2, "nested":["7",true]})
        );
    }
}
