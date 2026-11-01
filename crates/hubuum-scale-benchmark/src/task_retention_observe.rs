//! Observe real database clocks, physical cleanup, and verified publication.
//! Production workers perform cleanup; the observer only requests status.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::Args;
use hubuum_domain::TaskIdentity;
use hubuum_scale_core::ScaleBenchmarkBackend;
use hubuum_scale_core::lifecycle::LifecycleBenchmarkBackend;
use hubuum_storage_core::{TaskControlState, TaskPurgeScope, TaskReceiptOutcome};
use hubuum_storage_postgres::scale_benchmark::{
    PostgresScaleBackend, PostgresTaskRetentionObservation,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::lifecycle_process::{self, ProcessLimits};
use crate::{Result, write_json};

mod refill;

use refill::PendingRefill;

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
    // Query-start lower bounds, never the later time the result was received.
    last_observed_ms: Option<i64>,
    last_observed_nonterminal_ms: Option<i64>,
}

#[derive(Clone, Copy)]
struct ObservationTimes {
    started_ms: i64,
    completed_ms: i64,
}

impl ObservationTimes {
    /// Both arguments are truncated epoch milliseconds from `now_ms`. Keep the
    /// query start as a lower bound and add one millisecond to completion so its
    /// fractional part cannot understate the removal observation's upper bound.
    fn new(started_ms: i64, completed_ms: i64) -> Result<Self> {
        if completed_ms < started_ms {
            return Err("Observation clock moved backwards during a database query".into());
        }
        Ok(Self {
            started_ms,
            completed_ms: completed_ms
                .checked_add(1)
                .ok_or("Observation completion upper bound overflowed")?,
        })
    }

    fn record_removal(self, observed: &mut Option<i64>) {
        observed.get_or_insert(self.completed_ms);
    }
}

impl Lifetime {
    fn observe(
        &mut self,
        row: &PostgresTaskRetentionObservation,
        times: ObservationTimes,
    ) -> Result<()> {
        stable_anchor(&mut self.terminal_at_ms, row.terminal_at_ms())?;
        stable_anchor(&mut self.receipt_expires_at_ms, row.receipt_expires_at_ms())?;
        stable_anchor(&mut self.details_expires_at_ms, row.details_expires_at_ms())?;
        stable_anchor(&mut self.output_expires_at_ms, row.output_expires_at_ms())?;
        self.record_observation_time(
            times,
            row.is_nonterminal()
                && row.task_id().is_some()
                && row.receipt_present()
                && row.terminal_at_ms().is_none(),
        );
        Ok(())
    }

    fn record_observation_time(&mut self, times: ObservationTimes, nonterminal: bool) {
        self.last_observed_ms = Some(times.started_ms);
        self.last_observed_nonterminal_ms = nonterminal.then_some(times.started_ms);
    }

    fn nonterminal_through(&self, at: i64) -> bool {
        self.terminal_at_ms.is_none()
            && self
                .last_observed_nonterminal_ms
                .is_some_and(|observed| observed >= at && Some(observed) == self.last_observed_ms)
    }
}

fn receipt_occupancy(control: &TaskControlState, target: usize, at: i64) -> Value {
    let mut may_execute = 0;
    let mut terminal = 0;
    let mut recovery_unknown = 0;
    let mut eligible = 0;
    let mut oldest_eligible_ms = 0;
    for receipt in control.receipts() {
        match receipt.outcome() {
            TaskReceiptOutcome::MayExecute => may_execute += 1,
            TaskReceiptOutcome::Terminal { .. } => terminal += 1,
            TaskReceiptOutcome::RecoveryUnknown { .. } => recovery_unknown += 1,
        }
        if let Some(due) = receipt.expires_at().map(|at| at.timestamp_millis())
            && due <= at
        {
            eligible += 1;
            oldest_eligible_ms = oldest_eligible_ms.max(at - due);
        }
    }
    json!({"at_ms":at,"receipts":control.receipts().len(),"target":target,
        "may_execute":may_execute,"terminal":terminal,"recovery_unknown":recovery_unknown,
        "terminal_unexpired":terminal+recovery_unknown-eligible,"eligible_receipts":eligible,
        "oldest_eligible_ms":oldest_eligible_ms,
        "scope":"verified checkpoint receipt outcomes; MayExecute does not distinguish queued/running or terminal catch-up"})
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
    let mut nonterminal = 0;
    let mut unknown_deadlines = 0;
    let mut lower_bounds = Vec::new();
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
        if due.is_none() {
            if sample.nonterminal_through(until_ms) {
                nonterminal += 1;
            } else {
                unknown_deadlines += 1;
            }
        }
        if let Some(due) = due.filter(|due| *due >= from_ms && *due <= until_ms) {
            eligible += 1;
            if let Some(removed) = removed {
                lags.push((removed - due).max(0));
                lower_bounds.push((removed - due).max(0));
            } else {
                let lower_bound = (observed_ms - due).max(0);
                oldest_pending_ms = oldest_pending_ms.max(lower_bound);
                lower_bounds.push(lower_bound);
            }
        }
    }
    let lower_bound_distribution = distribution(&mut lower_bounds);
    let distribution = distribution(&mut lags);
    let complete = eligible > 0 && eligible == lags.len() && unknown_deadlines == 0;
    let p95_status = if unknown_deadlines > 0 {
        "incomplete"
    } else if eligible == 0 {
        "no_eligible_work"
    } else if lower_bound_distribution["p95_ms"]
        .as_i64()
        .is_some_and(|lag| lag > 60000)
    {
        "breach"
    } else if complete {
        "pass"
    } else {
        "incomplete"
    };
    json!({"eligible":eligible,"completed":lags.len(),"censored":eligible-lags.len(),
        "oldest_pending_ms":oldest_pending_ms,"distribution":distribution,
        "nonterminal_not_eligible":nonterminal,"unknown_deadlines":unknown_deadlines,
        "censored_lower_bound_distribution":lower_bound_distribution,"p95_status":p95_status,
        "p95_within_60_seconds":complete && distribution["p95_ms"].as_i64().is_some_and(|lag|lag <= 60000)})
}

fn require_steady_expiry_targets(details: &Value, receipts: &Value) -> Result<()> {
    if details["p95_within_60_seconds"] != true || receipts["p95_within_60_seconds"] != true {
        return Err(format!(
            "Steady expiry target missed or incomplete (details: {}, receipts: {}); raw report retained",
            details["p95_status"], receipts["p95_status"],
        ).into());
    }
    Ok(())
}

fn register_refill(
    admitted: Vec<TaskIdentity>,
    identities: &mut Vec<TaskIdentity>,
    samples: &mut BTreeMap<TaskIdentity, Lifetime>,
) -> Result<()> {
    for identity in admitted {
        if samples.contains_key(&identity) {
            return Err("Refill unexpectedly reused an earlier task UUID".into());
        }
        samples.insert(identity, Lifetime::default());
        identities.push(identity);
    }
    Ok(())
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
    let mut refill_decisions = Vec::new();
    let mut completed = false;
    let mut pass = 0;
    let mut pending_refill: Option<PendingRefill> = None;
    loop {
        if pending_refill
            .as_ref()
            .is_some_and(PendingRefill::is_finished)
        {
            register_refill(
                pending_refill
                    .take()
                    .expect("finished refill exists")
                    .finish()?,
                &mut identities,
                &mut samples,
            )?;
        }
        // Observe original clocks before cleanup can retire the receipt that carries them.
        let mut observed_receipts = 0;
        for batch in identities.chunks(1000) {
            let query_started_ms = now_ms()?;
            let observations = backend.observe_task_retention(batch).await?;
            let times = ObservationTimes::new(query_started_ms, now_ms()?)?;
            for observed in observations {
                observed_receipts += usize::from(observed.receipt_present());
                let sample = samples
                    .get_mut(&observed.identity())
                    .ok_or("Unexpected observed task identity")?;
                sample.observe(&observed, times)?;
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
        if pending_refill.is_none()
            && started.elapsed() < Duration::from_secs(options.steady_seconds)
        {
            // With no producer running, every admitted identity is registered
            // and receipt presence can only decrease during this scan. Its sum
            // therefore conservatively bounds occupancy at refill selection.
            // Status-lock contention must not stop the workload producer; these
            // counts schedule arrivals, never prove protected cleanup success.
            let refill = target_receipts
                .saturating_sub(observed_receipts)
                .min(options.refill_max_batch);
            if refill > 0 {
                if identities.len() + refill > 60000 {
                    return Err("Steady workload exhausted its declared 60k admission bound".into());
                }
                refill_decisions.push(json!({"pass":pass,"at_ms":now_ms()?,
                    "registered_tasks":identities.len(),"observed_receipts":observed_receipts,
                    "requested":refill,"target":target_receipts}));
                pending_refill = Some(PendingRefill::start(&options, pass, refill)?);
            }
        }
        let mut command = Command::new(&options.admin_binary);
        command
            .env("HUBUUM_DATABASE_URL", &options.database_url)
            .env("HUBUUM_MIGRATION_DATABASE_URL", &options.database_url)
            .env("HUBUUM_DATABASE_ROLE_MODE", "single")
            .env("HUBUUM_CONTROL_CHECKPOINT_MEASURE", "true")
            .args(["--task-retention", "status", "--control-recovery-directory"])
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
            if lifecycle_process::control_repository_busy(&result.stderr) {
                passes.push(json!({"observed_at_ms":now_ms()?,"artifact":process_report,"outcome":"control_repository_busy"}));
                pass += 1;
                if started.elapsed() >= Duration::from_secs(options.timeout_seconds) {
                    break;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
            return Err(format!(
                "Production retention observation {pass} failed with {}",
                result.status
            )
            .into());
        }
        let report: Value = serde_json::from_slice(&result.stdout)?;
        let protected = protected_state(&options.repository, &report)?;
        occupancy.push(receipt_occupancy(&protected, target_receipts, now_ms()?));
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
        for batch in identities.chunks(1000) {
            let query_started_ms = now_ms()?;
            let observations = backend.observe_task_retention(batch).await?;
            let times = ObservationTimes::new(query_started_ms, now_ms()?)?;
            for observed in observations {
                let sample = samples
                    .get_mut(&observed.identity())
                    .ok_or("Unexpected observed task identity")?;
                sample.observe(&observed, times)?;
                if options.output_only
                    && observed.output_rows() == 0
                    && observed.outputs_removed() == Some(true)
                    && removed.contains(&observed.identity())
                {
                    if observed.task_id().is_none() {
                        return Err("Output-only expiry unexpectedly removed task details".into());
                    }
                    times.record_removal(&mut sample.output_removal_observed_ms);
                }
                if !options.output_only
                    && observed.task_id().is_none()
                    && observed.child_rows() == 0
                    && observed.output_rows() == 0
                    && observed.details_removed() == Some(true)
                    && removed.contains(&observed.identity())
                {
                    times.record_removal(&mut sample.detail_removal_observed_ms);
                }
                if !observed.receipt_present()
                    && protected.receipt_for_task(observed.identity()).is_none()
                    && sample.receipt_expires_at_ms.is_some_and(|expiry| {
                        expiry <= protected.receipt_expiry_floor().timestamp_millis()
                    })
                {
                    times.record_removal(&mut sample.receipt_retirement_observed_ms);
                }
            }
        }
        write_json(
            &options.artifact_directory.join("latest-lifetimes.json"),
            &samples,
        )?;
        passes.push(json!({"observed_at_ms":now_ms()?,"artifact":process_report}));
        pass += 1;
        if started.elapsed() >= Duration::from_secs(options.steady_seconds)
            && pending_refill.is_none()
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
    // A timed-out observer still accounts for the admitted identities and joins
    // its bounded producer before reporting incomplete evidence.
    if let Some(refill) = pending_refill {
        register_refill(refill.finish()?, &mut identities, &mut samples)?;
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
    let observation_finished_ms = now_ms()?;
    let steady_receipt_lags = eligible_lags(
        &samples,
        true,
        steady_started_ms,
        steady_until_ms,
        observation_finished_ms,
    );
    let steady_detail_lags = eligible_lags(
        &samples,
        false,
        steady_started_ms,
        steady_until_ms,
        observation_finished_ms,
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
        "observation_finished_ms":observation_finished_ms,
        "full_drain_outcome":if completed {"complete"} else {"incomplete_timeout"},
        "full_drain_note":"A drain timeout is not itself an eligible-expiry percentile breach; inspect each denominator and p95_status",
        "observation_time_bounds":"source clocks and query starts round down to milliseconds; removal observations add a checked one-millisecond margin to query completion for an upper bound",
        "refill_execution":"one bounded producer concurrent with expiry observations",
        "refill_decisions":refill_decisions,
        "refill_source":"physical receipt presence across the complete registered cohort with no producer running; independent of administrator status-lock availability",
        "cleanup_execution":"production workers; observer invokes read-only administrator status",
        "steady_seconds":options.steady_seconds,"steady_started_ms":steady_started_ms,"steady_until_ms":steady_until_ms,"occupancy":occupancy,
        "steady_receipt_expiry":steady_receipt_lags,"steady_detail_expiry":steady_detail_lags,
        "database_environment":database_environment,"database_round_trip_nanos":database_round_trip_nanos,
        "database_resources":database_resources,"allocation_before":allocation_before,"allocation_after":allocation_after,
        "steady_state_measured":options.steady_seconds > 0,"p6_acceptance":false,
        "limitations":["held/output/dependency cohorts and recovery faults require separate runs","completion observation is an upper bound and includes administrator publication delay","recorded occupancy and all eligible denominators require evaluation before acceptance"]}),
    )?;
    if !completed {
        return Err(
            "Timed out before full cohort drain; report retains separate eligible-expiry evidence"
                .into(),
        );
    }
    if options.steady_seconds > 0 {
        require_steady_expiry_targets(&steady_detail_lags, &steady_receipt_lags)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case::earlier_cohort(true)]
    #[case::within_refill(false)]
    fn concurrent_refill_cannot_reuse_an_observed_identity(#[case] previously_tracked: bool) {
        let identity = TaskIdentity::generate();
        let mut identities = Vec::new();
        let mut samples = BTreeMap::new();
        let admitted = if previously_tracked {
            identities.push(identity);
            samples.insert(identity, Lifetime::default());
            vec![identity]
        } else {
            vec![identity, identity]
        };
        assert!(register_refill(admitted, &mut identities, &mut samples).is_err());
    }

    #[rstest]
    #[case::within_target(59999, 59999, true)]
    #[case::at_target(60000, 60000, true)]
    #[case::details_breach(60001, 1000, false)]
    #[case::receipts_breach(1000, 60001, false)]
    fn steady_expiry_failures_stop_later_qualification_phases(
        #[case] detail_lag: i64,
        #[case] receipt_lag: i64,
        #[case] accepted: bool,
    ) {
        let samples = BTreeMap::from([(
            TaskIdentity::generate(),
            Lifetime {
                details_expires_at_ms: Some(1000),
                receipt_expires_at_ms: Some(1000),
                detail_removal_observed_ms: Some(1000 + detail_lag),
                receipt_retirement_observed_ms: Some(1000 + receipt_lag),
                ..Lifetime::default()
            },
        )]);
        let details = eligible_lags(&samples, false, 0, 2000, 70000);
        let receipts = eligible_lags(&samples, true, 0, 2000, 70000);
        assert_eq!(
            require_steady_expiry_targets(&details, &receipts).is_ok(),
            accepted
        );
    }

    #[test]
    fn empty_steady_evidence_cannot_pass_the_expiry_gate() {
        let report = eligible_lags(&BTreeMap::new(), false, 0, 2000, 70000);
        assert!(require_steady_expiry_targets(&report, &report).is_err());
    }

    #[rstest]
    #[case::busy(b"Error: ServiceUnavailable(\"Control repository is busy; retry without bypassing protection\")\n", true)]
    #[case::engine_warning(b"engine warning\nError: ServiceUnavailable(\"Control repository is busy; retry without bypassing protection\")\n", true)]
    #[case::recovery_required(b"Error: ServiceUnavailable(\"Recover independently acknowledged control before task administration\")\n", false)]
    #[case::invalid_diagnostic(b"\xffError: ServiceUnavailable(\"Control repository is busy; retry without bypassing protection\")\n", false)]
    fn only_repository_lock_refusal_is_retryable(#[case] stderr: &[u8], #[case] expected: bool) {
        assert_eq!(lifecycle_process::control_repository_busy(stderr), expected);
    }

    #[rstest]
    #[case::crossed_cutoff(1999, 3000, true, false)]
    #[case::at_cutoff(2000, 3000, true, true)]
    #[case::missing_or_terminal_row(2000, 3000, false, false)]
    fn nonterminal_proof_cannot_extend_to_query_completion(
        #[case] started_ms: i64,
        #[case] completed_ms: i64,
        #[case] nonterminal: bool,
        #[case] expected: bool,
    ) {
        let mut sample = Lifetime::default();
        sample.record_observation_time(
            ObservationTimes::new(started_ms, completed_ms).unwrap(),
            nonterminal,
        );
        assert_eq!(sample.nonterminal_through(2000), expected);
    }

    #[test]
    fn query_duration_cannot_understate_a_removal_lag_breach() {
        let mut sample = Lifetime {
            receipt_expires_at_ms: Some(1000),
            ..Lifetime::default()
        };
        ObservationTimes::new(60000, 62001)
            .unwrap()
            .record_removal(&mut sample.receipt_retirement_observed_ms);
        let report = eligible_lags(
            &BTreeMap::from([(TaskIdentity::generate(), sample)]),
            true,
            0,
            2000,
            62002,
        );
        assert_eq!(report["distribution"]["p95_ms"], 61002);
        assert_eq!(report["p95_status"], "breach");
    }

    #[rstest]
    #[case::backwards(2000, 1999)]
    #[case::upper_bound_overflow(i64::MAX, i64::MAX)]
    fn invalid_observation_clock_cannot_produce_evidence(
        #[case] started_ms: i64,
        #[case] completed_ms: i64,
    ) {
        assert!(ObservationTimes::new(started_ms, completed_ms).is_err());
    }

    #[rstest]
    #[case::still_incomplete(3000, 2000, "incomplete")]
    #[case::proven_breach(62000, 61000, "breach")]
    fn unfinished_eligible_work_cannot_pass_a_percentile_gate(
        #[case] observed_at: i64,
        #[case] oldest: i64,
        #[case] status: &str,
    ) {
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
        let report = eligible_lags(&samples, true, 0, 2000, observed_at);
        assert_eq!(report["eligible"], 2);
        assert_eq!(report["censored"], 1);
        assert_eq!(report["oldest_pending_ms"], oldest);
        assert_eq!(report["p95_status"], status);
        assert_eq!(report["p95_within_60_seconds"], false);
    }

    #[rstest]
    #[case::observed_nonterminal(Some(62000), 1, 0, "pass")]
    #[case::missing_evidence(None, 0, 1, "incomplete")]
    #[case::stale_nonterminal(Some(1000), 0, 1, "incomplete")]
    fn unexpired_active_work_is_distinct_from_an_unknown_deadline(
        #[case] nonterminal_at: Option<i64>,
        #[case] nonterminal_count: i64,
        #[case] unknown_count: i64,
        #[case] status: &str,
    ) {
        let samples = BTreeMap::from([
            (
                TaskIdentity::generate(),
                Lifetime {
                    terminal_at_ms: Some(0),
                    receipt_expires_at_ms: Some(1000),
                    receipt_retirement_observed_ms: Some(1100),
                    ..Lifetime::default()
                },
            ),
            (
                TaskIdentity::generate(),
                Lifetime {
                    last_observed_ms: Some(62000),
                    last_observed_nonterminal_ms: nonterminal_at,
                    ..Lifetime::default()
                },
            ),
        ]);
        let report = eligible_lags(&samples, true, 0, 2000, 62000);
        assert_eq!(report["eligible"], 1);
        assert_eq!(report["censored"], 0);
        assert_eq!(report["nonterminal_not_eligible"], nonterminal_count);
        assert_eq!(report["unknown_deadlines"], unknown_count);
        assert_eq!(report["p95_status"], status);
    }
}
