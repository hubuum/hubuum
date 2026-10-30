//! Real held tasks, retained repair reports and bounded large-child cleanup.

use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::{Args, ValueEnum};
use hubuum_domain::{TaskId, TaskIdentity};
use hubuum_storage_core::TaskPurgeScope;
use hubuum_storage_postgres::scale_benchmark::{
    PostgresScaleBackend, PostgresTaskRetentionObservation,
};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::Result;
use crate::lifecycle_process::{self, ProcessLimits};
use crate::task_retention_observe::protected_state;
use crate::task_retention_probes::{api_origin, input, output, terminal};

#[derive(Clone, Copy, Debug, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum SafetyPhase {
    Capture,
    Held,
    Released,
    Recovered,
}

#[derive(Debug, Args)]
pub struct SafetyArgs {
    #[arg(long, value_enum)]
    phase: SafetyPhase,
    #[arg(long)]
    spec_file: PathBuf,
    #[arg(long)]
    state_file: PathBuf,
    #[arg(long)]
    remote_state: PathBuf,
    #[arg(long)]
    origin: String,
    #[arg(long)]
    token_file: PathBuf,
    #[arg(long)]
    database_url: String,
    #[arg(long)]
    admin_binary: PathBuf,
    #[arg(long)]
    repository: PathBuf,
    #[arg(long)]
    key_file: PathBuf,
    #[arg(long)]
    artifact_directory: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long, default_value_t = 300)]
    timeout_seconds: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Spec {
    large_task: i32,
    held_task: i32,
    dependent_task: i32,
    dependent_class: i32,
}

#[derive(Deserialize, Serialize)]
struct Proof {
    task: TaskId,
    identity: TaskIdentity,
    deadline_ms: i64,
    children: u64,
    outputs: u64,
}

#[derive(Deserialize, Serialize)]
struct State {
    large: Proof,
    held: Proof,
    dependency: Proof,
    report_sha256: String,
}

fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn now_ms() -> Result<i64> {
    Ok(i64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
    )?)
}

async fn report(
    client: &Client,
    origin: &str,
    token: &str,
    spec: &Spec,
    create: bool,
) -> Result<Vec<u8>> {
    let url = format!(
        "{origin}/api/v1/classes/{}/schema/tasks/{}/report",
        spec.dependent_class, spec.dependent_task
    );
    let request = if create {
        client
            .post(url)
            .json(&json!({"object_url_template":"https://example.invalid/objects/{object_id}"}))
    } else {
        client.get(url)
    };
    let mut response = request.bearer_auth(token).send().await?;
    if response.status() != StatusCode::OK {
        return Err("Retained repair-report dependency is unavailable".into());
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if bytes.len() + chunk.len() > 1024 * 1024 {
            return Err("Synthetic one-object repair report exceeds its measurement bound".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    if bytes.is_empty() {
        return Err("Retained repair report is empty".into());
    }
    Ok(bytes)
}

async fn capture(
    options: &SafetyArgs,
    spec: &Spec,
    token: &str,
    origin: &str,
    client: &Client,
    backend: &PostgresScaleBackend,
) -> Result<State> {
    let ids = [spec.large_task, spec.held_task, spec.dependent_task];
    let mut tasks = Vec::new();
    for id in ids {
        let task = TaskId::new(id)?;
        terminal(client, origin, token, id).await?;
        tasks.push(task);
    }
    let rendered = report(client, origin, token, spec, true).await?;
    let identities = backend.resolve_task_retention_cohort(&tasks).await?;
    let mut proofs = Vec::new();
    for task in tasks {
        let identity = identities
            .iter()
            .find(|(id, _)| *id == task)
            .ok_or("Missing original task UUID")?
            .1;
        let rows = backend.observe_task_retention(&[identity]).await?;
        let observed = rows
            .first()
            .filter(|row| rows.len() == 1 && row.task_id() == Some(task))
            .ok_or("Ambiguous task observation")?;
        proofs.push(Proof {
            task,
            identity,
            deadline_ms: observed
                .details_expires_at_ms()
                .ok_or("Missing original detail deadline")?,
            children: observed.child_rows(),
            outputs: observed.output_rows(),
        });
    }
    let mut proofs = proofs.into_iter();
    let state = State {
        large: proofs.next().ok_or("Missing large task")?,
        held: proofs.next().ok_or("Missing held task")?,
        dependency: proofs.next().ok_or("Missing dependent task")?,
        report_sha256: digest(&rendered),
    };
    if state.large.children < 1201 || state.held.children < 1201 || state.dependency.outputs != 1 {
        return Err("Real fixture did not create 1201 children and one retained report".into());
    }
    output(&options.state_file, &state)?;
    Ok(state)
}

fn retained(observed: &PostgresTaskRetentionObservation, proof: &Proof) -> Result<()> {
    if observed.task_id() != Some(proof.task)
        || observed.child_rows() != proof.children
        || observed.output_rows() != proof.outputs
        || observed.details_expires_at_ms() != Some(proof.deadline_ms)
        || observed.details_removed() == Some(true)
    {
        return Err("Cleanup changed a held or dependent task or its original deadline".into());
    }
    Ok(())
}

async fn execute(options: &SafetyArgs) -> Result<Value> {
    let spec: Spec = input(&options.spec_file)?;
    let tokens: Vec<String> = input(&options.token_file)?;
    let token = tokens
        .first()
        .filter(|token| !token.is_empty())
        .ok_or("Missing fixture bearer")?;
    if spec.dependent_class <= 0
        || [spec.large_task, spec.held_task, spec.dependent_task]
            .iter()
            .any(|id| *id <= 0)
    {
        return Err("Invalid safety fixture identities".into());
    }
    let origin = api_origin(&options.origin)?;
    let client = Client::builder().timeout(Duration::from_secs(30)).build()?;
    let backend = PostgresScaleBackend::connect(&options.database_url, 2)?;
    let started = Instant::now();
    if matches!(options.phase, SafetyPhase::Capture) {
        let state = capture(options, &spec, token, &origin, &client, &backend).await?;
        return Ok(
            json!({"phase":options.phase,"state":state,"elapsed_ms":started.elapsed().as_millis()}),
        );
    }
    let state: State = input(&options.state_file)?;
    if state.large.task.id() != spec.large_task
        || state.held.task.id() != spec.held_task
        || state.dependency.task.id() != spec.dependent_task
    {
        return Err("Safety observation does not match its original task snapshot".into());
    }
    let target = match options.phase {
        SafetyPhase::Held => state.large.identity,
        SafetyPhase::Released => state.held.identity,
        SafetyPhase::Recovered => {
            let remote: Value = input(&options.remote_state)?;
            serde_json::from_value(
                remote
                    .get("task_identity")
                    .cloned()
                    .ok_or("Missing dispatched task UUID")?,
            )?
        }
        SafetyPhase::Capture => unreachable!(),
    };
    fs::create_dir(&options.artifact_directory)?;
    let limits = ProcessLimits::new(512 * 1024 * 1024, 120)?;
    let mut passes = Vec::new();
    let mut original_deadline = match options.phase {
        SafetyPhase::Held => Some(state.large.deadline_ms),
        SafetyPhase::Released => Some(state.held.deadline_ms),
        _ => None,
    };
    loop {
        let rows = backend
            .observe_task_retention(&[target, state.dependency.identity])
            .await?;
        let observed = &rows[0];
        retained(&rows[1], &state.dependency)?;
        if matches!(options.phase, SafetyPhase::Held) {
            let held = backend
                .observe_task_retention(&[state.held.identity])
                .await?;
            retained(&held[0], &state.held)?;
        }
        if let Some(deadline) = observed.details_expires_at_ms() {
            if original_deadline.is_some_and(|original| original != deadline) {
                return Err("Cleanup renewed the original task detail deadline".into());
            }
            original_deadline = Some(deadline);
        }
        let deadline =
            original_deadline.ok_or("Recovered dispatch lacks a detail expiry anchor")?;
        if now_ms()? < deadline.max(state.dependency.deadline_ms) {
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        }
        let pass = passes.len();
        let mut command = Command::new(&options.admin_binary);
        command
            .env("HUBUUM_DATABASE_URL", &options.database_url)
            .env("HUBUUM_MIGRATION_DATABASE_URL", &options.database_url)
            .env("HUBUUM_DATABASE_ROLE_MODE", "single")
            .env("HUBUUM_CONTROL_CHECKPOINT_MEASURE", "true")
            .args(["--task-retention", "run", "--control-recovery-directory"])
            .arg(&options.repository)
            .arg("--control-recovery-key")
            .arg(&options.key_file)
            .args(["--log-level", "error"]);
        let (result, resources) = lifecycle_process::run(
            &mut command,
            &options.artifact_directory.join(format!("pass-{pass}")),
            &limits,
        );
        output(
            &options.artifact_directory.join(format!("pass-{pass}.json")),
            &json!({"process":resources,"result":result.as_ref().map(|result| json!({"status":result.status.to_string(),"stdout":String::from_utf8_lossy(&result.stdout),"stderr":String::from_utf8_lossy(&result.stderr)})).map_err(ToString::to_string)}),
        )?;
        let result = result?;
        if !result.status.success() {
            if lifecycle_process::control_repository_busy(&result.stderr) {
                passes.push(json!({"at_ms":now_ms()?,"artifact":format!("pass-{pass}.json"),"outcome":"control_repository_busy"}));
                if passes.len() >= 300 {
                    return Err("Safety cleanup exceeded its bounded pass count".into());
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            return Err("Production safety cleanup pass failed".into());
        }
        let admin: Value = serde_json::from_slice(&result.stdout)?;
        let control = protected_state(&options.repository, &admin)?;
        let budget = control.policy().cleanup_budget();
        let removed_rows = admin["removed_rows"]
            .as_u64()
            .ok_or("Missing actual removed rows")?;
        let removed_bytes = admin["removed_bytes"]
            .as_u64()
            .ok_or("Missing actual removed bytes")?;
        if removed_rows > u64::from(budget.max_rows()) || removed_bytes > budget.max_bytes() {
            return Err("Actual cleanup pass exceeded its shared physical budget".into());
        }
        passes.push(json!({"at_ms":now_ms()?,"rows":removed_rows,"bytes":removed_bytes,"budget_rows":budget.max_rows(),"budget_bytes":budget.max_bytes()}));
        let removed = control.purge_batches().values().any(|batch| {
            batch.removed().is_some()
                && batch.ticket().manifest().scope() == TaskPurgeScope::TaskDetails
                && batch.ticket().manifest().tasks().contains(&target)
        });
        let rows = backend
            .observe_task_retention(&[target, state.dependency.identity])
            .await?;
        retained(&rows[1], &state.dependency)?;
        if matches!(options.phase, SafetyPhase::Held) {
            let held = backend
                .observe_task_retention(&[state.held.identity])
                .await?;
            retained(&held[0], &state.held)?;
        }
        let observed = &rows[0];
        if removed
            && observed.task_id().is_none()
            && observed.child_rows() == 0
            && observed.output_rows() == 0
            && observed.details_removed() == Some(true)
        {
            if digest(&report(&client, &origin, token, &spec, false).await?) != state.report_sha256
            {
                return Err("Retained repair report changed across unrelated cleanup".into());
            }
            return Ok(
                json!({"phase":options.phase,"target":target,"removed_at_ms":now_ms()?,"original_deadline_ms":deadline,"passes":passes,"physical":observed,"dependency":rows[1],"held_preserved":matches!(options.phase,SafetyPhase::Held),"elapsed_ms":started.elapsed().as_millis()}),
            );
        }
        if passes.len() >= 300 {
            return Err("Safety cleanup exceeded its bounded pass count".into());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

pub async fn run(options: SafetyArgs) -> Result<()> {
    if options.output.exists() || !(1..=600).contains(&options.timeout_seconds) {
        return Err("Invalid safety probe output or timeout".into());
    }
    let report = tokio::time::timeout(
        Duration::from_secs(options.timeout_seconds),
        execute(&options),
    )
    .await
    .map_err(|_| "Safety probe exceeded its deadline")??;
    output(&options.output, &report)
}
