//! Observe real database clocks, physical cleanup, and verified publication.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::Args;
use hubuum_domain::TaskIdentity;
use hubuum_scale_core::ScaleBenchmarkBackend;
use hubuum_scale_core::lifecycle::LifecycleBenchmarkBackend;
use hubuum_storage_core::{TaskControlState, TaskPurgeScope};
use hubuum_storage_postgres::scale_benchmark::PostgresScaleBackend;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::lifecycle_process::{self, ProcessLimits};
use crate::{Result, write_json};

#[derive(Debug, Args)]
pub struct ObserveArgs {
    #[arg(long)]
    database_url: String,
    #[arg(long)]
    admission_report: PathBuf,
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
    #[arg(long, default_value_t = 1200)]
    timeout_seconds: u64,
    /// Keep the initial receipt occupancy replenished for this real duration
    #[arg(long, default_value_t = 0)]
    steady_seconds: u64,
    #[arg(long, num_args = 1..)]
    refill_origins: Vec<String>,
    #[arg(long)]
    refill_token_file: Option<PathBuf>,
    #[arg(long)]
    refill_workload: Option<PathBuf>,
    #[arg(long, default_value_t = 256)]
    refill_max_batch: usize,
    /// Measure only real output expiry, leaving task detail retention disabled
    #[arg(long)]
    output_only: bool,
}

#[derive(Default, Serialize)]
struct Lifetime {
    terminal_at_ms: Option<i64>,
    receipt_expires_at_ms: Option<i64>,
    details_expires_at_ms: Option<i64>,
    receipt_retirement_observed_ms: Option<i64>,
    detail_removal_observed_ms: Option<i64>,
    output_expires_at_ms: Option<i64>,
    output_removal_observed_ms: Option<i64>,
}

#[derive(Deserialize)]
struct SignedControl {
    payload: Payload,
}
#[derive(Deserialize)]
struct Payload {
    task_control: TaskControlState,
}

fn now_ms() -> Result<i64> {
    Ok(i64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
    )?)
}

fn stable_anchor(saved: &mut Option<i64>, observed: Option<i64>) -> Result<()> {
    if let Some(observed) = observed {
        if saved.is_some_and(|saved| saved != observed) {
            return Err("Original lifecycle anchor changed during observation".into());
        }
        *saved = Some(observed);
    }
    Ok(())
}

pub(super) fn protected_state(repository: &Path, report: &Value) -> Result<TaskControlState> {
    let identity = report
        .get("checkpoint")
        .and_then(Value::as_str)
        .ok_or("Administrator report lacks verified checkpoint")?;
    if identity.len() != 64
        || !identity
            .bytes()
            .all(|value| value.is_ascii_digit() || (b'a'..=b'f').contains(&value))
    {
        return Err("Invalid checkpoint identity in administrator report".into());
    }
    let path = repository.join(format!("{identity}.json"));
    if fs::metadata(&path)?.len() > 20 * 1024 * 1024 + 128 * 1024 {
        return Err("Composite checkpoint exceeds declared encoded bound".into());
    }
    let bytes = fs::read(path)?;
    let digest = Sha256::digest(&bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    if digest != identity {
        return Err("Verified checkpoint file identity changed".into());
    }
    Ok(serde_json::from_slice::<SignedControl>(&bytes)?
        .payload
        .task_control)
}

fn distribution(values: &mut [i64]) -> Value {
    values.sort_unstable();
    let percentile = |percent: usize| {
        values
            .get((values.len() * percent).div_ceil(100).saturating_sub(1))
            .copied()
    };
    json!({"count":values.len(),"p50_ms":percentile(50),"p95_ms":percentile(95),"p99_ms":percentile(99),"max_ms":values.last()})
}

fn eligible_lags(
    samples: &BTreeMap<TaskIdentity, Lifetime>,
    receipts: bool,
    from_ms: i64,
    until_ms: i64,
    observed_ms: i64,
) -> Value {
    let mut lags = Vec::new();
    let mut eligible = 0;
    let mut oldest_pending_ms = 0;
    for sample in samples.values() {
        let (due, removed) = if receipts {
            (
                sample.receipt_expires_at_ms,
                sample.receipt_retirement_observed_ms,
            )
        } else {
            (
                sample.details_expires_at_ms,
                sample.detail_removal_observed_ms,
            )
        };
        if let Some(due) = due.filter(|due| *due >= from_ms && *due <= until_ms) {
            eligible += 1;
            if let Some(removed) = removed {
                lags.push((removed - due).max(0));
            } else {
                oldest_pending_ms = oldest_pending_ms.max((observed_ms - due).max(0));
            }
        }
    }
    let distribution = distribution(&mut lags);
    let complete = eligible > 0 && eligible == lags.len();
    json!({"eligible":eligible,"completed":lags.len(),"censored":eligible-lags.len(),
        "oldest_pending_ms":oldest_pending_ms,"distribution":distribution,
        "p95_within_60_seconds":complete && distribution["p95_ms"].as_i64().is_some_and(|lag|lag <= 60000)})
}

pub async fn run(options: ObserveArgs) -> Result<()> {
    if options.timeout_seconds == 0
        || options.timeout_seconds > 86400
        || options.output.exists()
        || options.steady_seconds >= options.timeout_seconds
        || options.output_only && options.steady_seconds != 0
        || !(1..=1000).contains(&options.refill_max_batch)
        || options.steady_seconds > 0
            && (options.refill_origins.is_empty()
                || options.refill_token_file.is_none()
                || options.refill_workload.is_none())
    {
        return Err("Invalid observer timeout or existing output".into());
    }
    fs::create_dir(&options.artifact_directory)?;
    let input: Value = serde_json::from_slice(&fs::read(&options.admission_report)?)?;
    let cohort = input
        .get("admissions")
        .and_then(Value::as_array)
        .ok_or("Missing admitted cohort")?;
    if cohort.is_empty() || cohort.len() > 10000 {
        return Err("Observer requires 1..=10000 real admitted identities".into());
    }
    let mut identities = cohort
        .iter()
        .map(|row| {
            serde_json::from_value::<TaskIdentity>(
                row.get("task_identity")
                    .cloned()
                    .ok_or("Missing original task identity")?,
            )
            .map_err(Into::into)
        })
        .collect::<Result<Vec<_>>>()?;
    if identities.iter().collect::<BTreeSet<_>>().len() != identities.len() {
        return Err("Original task identities must be distinct".into());
    }
    let backend = PostgresScaleBackend::connect(&options.database_url, 8)?;
    let database_environment = backend.prepare_measurement().await?.identity;
    let database_round_trip_nanos = backend.task_retention_round_trip_nanos().await?;
    let resource_baseline = backend.begin_resource_measurement().await?;
    let allocation_before = backend.lifecycle_inventory().await?;
    let target_receipts = identities.len();
    let mut samples = identities
        .iter()
        .copied()
        .map(|identity| (identity, Lifetime::default()))
        .collect::<BTreeMap<_, _>>();
    let limits = ProcessLimits::new(512 * 1024 * 1024, 120)?;
    let started = Instant::now();
    let steady_started_ms = now_ms()?;
    let steady_until_ms = steady_started_ms + i64::try_from(options.steady_seconds * 1000)?;
    let mut passes = Vec::new();
    let mut occupancy = Vec::new();
    let mut completed = false;
    let mut pass = 0;
    loop {
        // Observe original clocks before cleanup can retire the receipt that carries them.
        for batch in identities.chunks(1000) {
            for observed in backend.observe_task_retention(batch).await? {
                let sample = samples
                    .get_mut(&observed.identity())
                    .ok_or("Unexpected observed task identity")?;
                stable_anchor(&mut sample.terminal_at_ms, observed.terminal_at_ms())?;
                stable_anchor(
                    &mut sample.receipt_expires_at_ms,
                    observed.receipt_expires_at_ms(),
                )?;
                stable_anchor(
                    &mut sample.details_expires_at_ms,
                    observed.details_expires_at_ms(),
                )?;
                stable_anchor(
                    &mut sample.output_expires_at_ms,
                    observed.output_expires_at_ms(),
                )?;
                if options.output_only
                    && sample.terminal_at_ms.is_some()
                    && sample.output_expires_at_ms.is_none()
                {
                    return Err(
                        "Terminal output probe has no observed original output expiry".into(),
                    );
                }
            }
        }
        // Retain the last observed original clocks even if the next real
        // administrator invocation fails or is stopped by its resource limit.
        write_json(
            &options.artifact_directory.join("latest-lifetimes.json"),
            &samples,
        )?;
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
        let process_report = format!("pass-{pass}.json");
        write_json(
            &options.artifact_directory.join(&process_report),
            &json!({"process": resources, "result": result.as_ref().map(|output| json!({
                "status": output.status.to_string(),
                "stdout": String::from_utf8_lossy(&output.stdout),
                "stderr": String::from_utf8_lossy(&output.stderr),
            })).map_err(ToString::to_string)}),
        )?;
        let result = result?;
        if !result.status.success() {
            return Err(format!(
                "Production retention pass {pass} failed with {}",
                result.status
            )
            .into());
        }
        let report: Value = serde_json::from_slice(&result.stdout)?;
        let protected = protected_state(&options.repository, &report)?;
        occupancy.push(json!({"at_ms":now_ms()?,"receipts":protected.receipts().len(),"target":target_receipts}));
        let removed = protected
            .purge_batches()
            .values()
            .filter(|batch| {
                batch.removed().is_some()
                    && batch.ticket().manifest().scope()
                        == if options.output_only {
                            TaskPurgeScope::ExpiredOutputs
                        } else {
                            TaskPurgeScope::TaskDetails
                        }
            })
            .flat_map(|batch| batch.ticket().manifest().tasks().iter().copied())
            .collect::<BTreeSet<_>>();
        let observed_at = now_ms()?;
        for batch in identities.chunks(1000) {
            for observed in backend.observe_task_retention(batch).await? {
                let sample = samples
                    .get_mut(&observed.identity())
                    .ok_or("Unexpected observed task identity")?;
                if options.output_only
                    && observed.output_rows() == 0
                    && observed.outputs_removed() == Some(true)
                    && removed.contains(&observed.identity())
                {
                    if observed.task_id().is_none() {
                        return Err("Output-only expiry unexpectedly removed task details".into());
                    }
                    sample.output_removal_observed_ms.get_or_insert(observed_at);
                }
                if !options.output_only
                    && observed.task_id().is_none()
                    && observed.child_rows() == 0
                    && observed.output_rows() == 0
                    && observed.details_removed() == Some(true)
                    && removed.contains(&observed.identity())
                {
                    sample.detail_removal_observed_ms.get_or_insert(observed_at);
                }
                if !observed.receipt_present()
                    && protected.receipt_for_task(observed.identity()).is_none()
                    && sample.receipt_expires_at_ms.is_some_and(|expiry| {
                        expiry <= protected.receipt_expiry_floor().timestamp_millis()
                    })
                {
                    sample
                        .receipt_retirement_observed_ms
                        .get_or_insert(observed_at);
                }
            }
        }
        write_json(
            &options.artifact_directory.join("latest-lifetimes.json"),
            &samples,
        )?;
        passes.push(json!({"observed_at_ms":observed_at,"artifact":process_report}));
        if started.elapsed() < Duration::from_secs(options.steady_seconds) {
            let refill = target_receipts
                .saturating_sub(protected.receipts().len())
                .min(options.refill_max_batch);
            if refill > 0 {
                if identities.len() + refill > 60000 {
                    return Err("Steady workload exhausted its declared 60k admission bound".into());
                }
                let mut recipe: Value = serde_json::from_slice(&fs::read(
                    options
                        .refill_workload
                        .as_ref()
                        .ok_or("Missing refill recipe")?,
                )?)?;
                recipe["count"] = json!(refill);
                let recipe_path = options
                    .artifact_directory
                    .join(format!("refill-{pass}-recipe.json"));
                let refill_path = options
                    .artifact_directory
                    .join(format!("refill-{pass}.json"));
                write_json(&recipe_path, &recipe)?;
                let mut command = Command::new(std::env::current_exe()?);
                let metric_directory = options
                    .artifact_directory
                    .join(format!("refill-{pass}-metrics"));
                fs::create_dir(&metric_directory)?;
                command
                    .args(["task-retention", "--origins"])
                    .args(&options.refill_origins)
                    .arg("--database-url")
                    .arg(&options.database_url)
                    .arg("--token-file")
                    .arg(
                        options
                            .refill_token_file
                            .as_ref()
                            .ok_or("Missing refill credentials")?,
                    )
                    .arg("--workload-spec")
                    .arg(&recipe_path)
                    .arg("--cohort")
                    .arg(format!("refill-{}-{pass}", std::process::id()))
                    .arg("--output")
                    .arg(&refill_path)
                    .arg("--metric-directory")
                    .arg(metric_directory);
                let (result, resources) = lifecycle_process::run(
                    &mut command,
                    &options
                        .artifact_directory
                        .join(format!("refill-{pass}-process")),
                    &limits,
                );
                write_json(
                    &options
                        .artifact_directory
                        .join(format!("refill-{pass}-resources.json")),
                    &json!({"process":resources,"result":result.as_ref().map(|output|json!({
                        "status":output.status.to_string(),"stderr":String::from_utf8_lossy(&output.stderr)
                    })).map_err(ToString::to_string)}),
                )?;
                if !result?.status.success() {
                    return Err("Production steady refill failed; inspect refill artifacts".into());
                }
                let refill_report: Value = serde_json::from_slice(&fs::read(refill_path)?)?;
                let rows = refill_report
                    .get("admissions")
                    .and_then(Value::as_array)
                    .ok_or("Missing refill admissions")?;
                if rows.len() != refill {
                    return Err("Refill admission count differs from requested bounded work".into());
                }
                for row in rows {
                    let identity = serde_json::from_value::<TaskIdentity>(
                        row.get("task_identity")
                            .cloned()
                            .ok_or("Missing refill task UUID")?,
                    )?;
                    if samples.insert(identity, Lifetime::default()).is_some() {
                        return Err("Refill unexpectedly reused an earlier task UUID".into());
                    }
                    identities.push(identity);
                }
            }
        }
        pass += 1;
        if started.elapsed() >= Duration::from_secs(options.steady_seconds)
            && samples.values().all(|sample| {
                if options.output_only {
                    sample.output_removal_observed_ms.is_some()
                        && sample.output_expires_at_ms.is_some()
                } else {
                    sample.detail_removal_observed_ms.is_some()
                        && sample.receipt_retirement_observed_ms.is_some()
                }
            })
        {
            completed = true;
            break;
        }
        if started.elapsed() >= Duration::from_secs(options.timeout_seconds) {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let mut receipt_lags = samples
        .values()
        .filter_map(|sample| {
            sample
                .receipt_retirement_observed_ms
                .zip(sample.receipt_expires_at_ms)
                .map(|(at, due)| (at - due).max(0))
        })
        .collect::<Vec<_>>();
    let mut detail_lags = samples
        .values()
        .filter_map(|sample| {
            sample
                .detail_removal_observed_ms
                .zip(sample.details_expires_at_ms)
                .map(|(at, due)| (at - due).max(0))
        })
        .collect::<Vec<_>>();
    let receipt_distribution = distribution(&mut receipt_lags);
    let detail_distribution = distribution(&mut detail_lags);
    let mut output_lags = samples
        .values()
        .filter_map(|sample| {
            sample
                .output_removal_observed_ms
                .zip(sample.output_expires_at_ms)
                .map(|(at, due)| (at - due).max(0))
        })
        .collect::<Vec<_>>();
    let output_distribution = distribution(&mut output_lags);
    let steady_receipt_lags = eligible_lags(
        &samples,
        true,
        steady_started_ms,
        steady_until_ms,
        now_ms()?,
    );
    let steady_detail_lags = eligible_lags(
        &samples,
        false,
        steady_started_ms,
        steady_until_ms,
        now_ms()?,
    );
    let mut database_resources = backend
        .finish_resource_measurement(resource_baseline)
        .await?;
    // The generic backend sampler sees host-wide PostgreSQL processes. This
    // deployment records the owned container's process tree and cgroup instead.
    database_resources.cpu_seconds = None;
    database_resources.peak_resident_bytes = None;
    let allocation_after = backend.lifecycle_inventory().await?;
    write_json(
        &options.output,
        &json!({"format_version":1,"phase":if options.output_only {"real_output_expiry"} else if options.steady_seconds > 0 {"steady_refill_and_drain"} else {"saturation_cleanup"},"cohort":input.get("cohort"),"count":identities.len(),"completed":completed,
        "receipt_expiry_lag":receipt_distribution,"detail_expiry_lag":detail_distribution,
        "output_expiry_lag":output_distribution,
        "raw_lifetimes":samples,"passes":passes,"elapsed_ms":started.elapsed().as_millis(),
        "steady_seconds":options.steady_seconds,"steady_started_ms":steady_started_ms,"steady_until_ms":steady_until_ms,"occupancy":occupancy,
        "steady_receipt_expiry":steady_receipt_lags,"steady_detail_expiry":steady_detail_lags,
        "database_environment":database_environment,"database_round_trip_nanos":database_round_trip_nanos,
        "database_resources":database_resources,"allocation_before":allocation_before,"allocation_after":allocation_after,
        "steady_state_measured":options.steady_seconds > 0,"p6_acceptance":false,
        "limitations":["held/output/dependency cohorts and recovery faults require separate runs","completion observation is an upper bound and includes administrator publication delay","recorded occupancy and all eligible denominators require evaluation before acceptance"]}),
    )?;
    if !completed {
        return Err(
            "Timed out with censored eligible tasks; observer report retains all samples".into(),
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unfinished_eligible_work_cannot_pass_a_percentile_gate() {
        let samples = BTreeMap::from([
            (
                TaskIdentity::generate(),
                Lifetime {
                    receipt_expires_at_ms: Some(1000),
                    receipt_retirement_observed_ms: Some(1100),
                    ..Lifetime::default()
                },
            ),
            (
                TaskIdentity::generate(),
                Lifetime {
                    receipt_expires_at_ms: Some(1000),
                    ..Lifetime::default()
                },
            ),
            (
                TaskIdentity::generate(),
                Lifetime {
                    receipt_expires_at_ms: Some(100000),
                    ..Lifetime::default()
                },
            ),
        ]);
        let report = eligible_lags(&samples, true, 0, 2000, 62000);
        assert_eq!(report["eligible"], 2);
        assert_eq!(report["censored"], 1);
        assert_eq!(report["oldest_pending_ms"], 61000);
        assert_eq!(report["p95_within_60_seconds"], false);
    }
}
