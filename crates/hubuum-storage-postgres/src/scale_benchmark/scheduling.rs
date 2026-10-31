//! Real fenced claims on a disposable lifecycle workload. Fixture SQL stays in
//! the adapter; timed work uses the same execution capability as production.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use chrono::{NaiveDateTime, TimeDelta};
use diesel::QueryableByName;
use diesel::sql_types::{BigInt, Bool, Integer, Jsonb, Nullable, Text, Timestamp};
use diesel_async::{RunQueryDsl, SimpleAsyncConnection};
use futures_util::future::try_join_all;
use hubuum_scale_core::lifecycle::{
    LifecycleBenchmarkBackend, LifecycleFixture, LifecycleWorkload,
};
use hubuum_storage_core::{
    StorageTaskClaimRequest, StorageTaskEventInput, StorageTaskFailure, StorageTaskKind,
    StorageTaskLeaseDuration, StorageTaskPolicyUpdate, TaskCapacityLimit, TaskCapacityStorage,
    TaskExecutionStorage, TaskKindWeight, TaskScopeCapacity, TaskWorkerId,
};
use serde::Serialize;
use serde_json::{Value, json};

use super::{PostgresScaleBackend, benchmark_pool, ensure_fresh_database};
use crate::{
    PostgresPool, PostgresStorage, PostgresStorageError, capture_queries, with_connection,
    with_transaction,
};

type Result<T> = std::result::Result<T, PostgresStorageError>;

#[derive(Clone, Copy, Serialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
enum SchedulingPhase {
    Complete,
    Prepare,
    Workers { slots: u16 },
}

#[derive(Clone, Serialize)]
pub struct SchedulingBenchmarkOptions {
    workload: LifecycleWorkload,
    phase: SchedulingPhase,
    fixture: Option<LifecycleFixture>,
    slots: u16,
    samples: u32,
    timed_seconds: Option<u64>,
    overdue: bool,
}

impl SchedulingBenchmarkOptions {
    pub fn new(workload: LifecycleWorkload) -> Self {
        Self {
            workload,
            phase: SchedulingPhase::Complete,
            fixture: None,
            slots: 1,
            samples: 10_000,
            timed_seconds: None,
            overdue: false,
        }
    }

    pub fn prepare_only(mut self) -> Self {
        self.phase = SchedulingPhase::Prepare;
        self
    }

    pub fn with_worker_process_slots(mut self, slots: u16) -> Result<Self> {
        if slots == 0 || slots > self.slots {
            return Err(PostgresStorageError::invalid_input(
                "Worker process slots must be positive and fit the prepared pool",
            ));
        }
        self.phase = SchedulingPhase::Workers { slots };
        Ok(self)
    }

    fn fixture_key(&self) -> Value {
        json!({"workload": self.workload, "fixture": self.fixture, "slots": self.slots,
            "overdue": self.overdue, "principal_cap": self.timed_seconds.is_some()})
    }

    pub fn with_fixture(mut self, fixture: LifecycleFixture) -> Result<Self> {
        if fixture.profile().totals.principals < u64::from(self.workload.principal_count()) {
            return Err(PostgresStorageError::invalid_input(
                "The lifecycle dataset must contain every scheduling principal",
            ));
        }
        self.fixture = Some(fixture);
        Ok(self)
    }

    pub fn with_slots(mut self, slots: u16) -> Result<Self> {
        if ![1, 4].contains(&slots) {
            return Err(PostgresStorageError::invalid_input(
                "Scheduling profiles use one or four slots",
            ));
        }
        if matches!(self.phase, SchedulingPhase::Workers { slots: workers } if workers > slots) {
            return Err(PostgresStorageError::invalid_input(
                "Worker process slots exceed the prepared pool",
            ));
        }
        self.slots = slots;
        Ok(self)
    }

    pub fn with_samples(mut self, samples: u32) -> Result<Self> {
        if !(1..=100_000).contains(&samples) {
            return Err(PostgresStorageError::invalid_input(
                "Scheduling samples must be in 1..=100000",
            ));
        }
        self.samples = samples;
        Ok(self)
    }

    pub fn with_timed_window(mut self, seconds: Option<u64>) -> Result<Self> {
        if seconds.is_some_and(|value| !(30..=3600).contains(&value)) {
            return Err(PostgresStorageError::invalid_input(
                "Timed windows must be 30..=3600 seconds",
            ));
        }
        self.timed_seconds = seconds;
        Ok(self)
    }

    pub fn with_overdue(mut self, overdue: bool) -> Self {
        self.overdue = overdue;
        self
    }
}

#[derive(Serialize)]
struct ClaimSample {
    task: i32,
    principal: i32,
    kind: String,
    promoted: bool,
    sequence: i64,
    claim_us: u64,
    lock_wait_us: i64,
    round_trips: usize,
    elapsed_us: u64,
    busy_us: u64,
    window_busy_us: u64,
}

#[derive(Serialize)]
pub struct SchedulingBenchmarkReport {
    options: SchedulingBenchmarkOptions,
    fixture_inventory: Option<Value>,
    warmup_claims: usize,
    principal_cap_blocked_us: BTreeMap<i32, u64>,
    measurement_window: Option<MeasurementWindow>,
    postgres: String,
    elapsed_us: u64,
    claim_p95_us: u64,
    claim_p99_us: u64,
    lock_p95_us: u64,
    max_round_trips: usize,
    ordinary_starts: BTreeMap<String, u64>,
    principal_starts: BTreeMap<i32, u64>,
    promotions: u64,
    empty_polls: Vec<u64>,
    failed_polls: Vec<String>,
    occupied_slot_fraction: f64,
    samples: Vec<ClaimSample>,
}

#[derive(Serialize)]
struct MeasurementWindow {
    start: NaiveDateTime,
    end: NaiveDateTime,
}

#[derive(QueryableByName)]
struct MeasurementClock {
    #[diesel(sql_type = Timestamp)]
    now: NaiveDateTime,
}

#[derive(QueryableByName)]
struct PreparedFixture {
    #[diesel(sql_type = Jsonb)]
    settings: Value,
    #[diesel(sql_type = Nullable<Jsonb>)]
    inventory: Option<Value>,
}

#[derive(QueryableByName)]
struct ClaimDiagnostics {
    #[diesel(sql_type = BigInt)]
    last_lock_wait_us: i64,
    #[diesel(sql_type = BigInt)]
    last_claim_sequence: i64,
    #[diesel(sql_type = Bool)]
    last_claim_promoted: bool,
}

fn micros(duration: Duration) -> u64 {
    duration.as_micros().min(u128::from(u64::MAX)) as u64
}
fn percentile(mut values: Vec<u64>, percentile: usize) -> u64 {
    values.sort_unstable();
    values
        .get((values.len() * percentile).div_ceil(100).saturating_sub(1))
        .copied()
        .unwrap_or(0)
}

impl PostgresScaleBackend {
    async fn prepare_scheduling_fixture(
        &self,
        options: &SchedulingBenchmarkOptions,
    ) -> Result<Option<Value>> {
        let fixture_inventory = if let Some(fixture) = &options.fixture {
            self.load_lifecycle_fixture(fixture)
                .await
                .map_err(|error| PostgresStorageError::invalid_input(error.to_string()))?;
            Some(
                self.lifecycle_inventory()
                    .await
                    .map_err(|error| PostgresStorageError::internal(error.to_string()))?,
            )
        } else {
            ensure_fresh_database(&self.pool)
                .await
                .map_err(|error| PostgresStorageError::invalid_input(error.to_string()))?;
            None
        };
        // A fresh database is mandatory: do not reset a caller's existing tasks.
        with_transaction(&self.pool, async |connection| {
            let count = diesel::sql_query("SELECT count(*)::bigint AS value FROM public.tasks WHERE status IN ('queued','validating','running')")
                .get_result::<super::CountRow>(connection).await?.value;
            if count != 0 { return Err(PostgresStorageError::invalid_input("Scheduling benchmark requires an empty disposable task table")); }
            if options.fixture.is_none() {
                diesel::sql_query("INSERT INTO principals(kind,name) SELECT 'service_account','scheduler-fixture-' || value FROM generate_series(1,$1) value")
                    .bind::<Integer,_>(options.workload.principal_count() as i32).execute(connection).await?;
            }
            connection.batch_execute("SET LOCAL hubuum.reconcile_task_capacity='on'").await?;
            for batch in options.workload.tasks().collect::<Vec<_>>().chunks(128) {
                let rows: Vec<_> = batch.iter().map(|task| json!({"ordinal":task.ordinal(), "principal":task.principal_ordinal(), "duration_ms": task.duration().as_millis() as u64})).collect();
                diesel::sql_query("INSERT INTO tasks(kind,status,submitted_by,request_payload,created_at) \
                    SELECT CASE WHEN r.ordinal%2=0 THEN 'import' ELSE 'export' END,'queued',p.id, \
                    jsonb_build_object('duration_ms',r.duration_ms), (clock_timestamp() AT TIME ZONE 'UTC') - $2*interval '1 second' \
                    FROM jsonb_to_recordset($1) AS r(ordinal int,principal int,duration_ms bigint) \
                    JOIN (SELECT id,row_number() OVER (ORDER BY id) AS ordinal FROM principals) p ON p.ordinal=r.principal")
                    .bind::<Jsonb,_>(json!(rows)).bind::<Integer,_>(if options.overdue {3600} else {0}).execute(connection).await?;
            }
            connection.batch_execute("ANALYZE public.tasks; ANALYZE hubuum_control.task_accounting; ANALYZE hubuum_control.task_principal_service").await?;
            Ok(())
        }).await?;
        let storage = PostgresStorage::unobserved(self.pool.clone());
        let current = storage.task_policy().await?;
        let mut policy = current.policy().clone().with_scheduling(
            current
                .policy()
                .scheduling()
                .clone()
                .with_capacity(options.slots, 1)
                .expect("validated profile slots")
                .with_weight(
                    TaskKindWeight::new(StorageTaskKind::Import, 3).expect("fixed weight"),
                ),
        );
        if options.timed_seconds.is_some() {
            let principal = policy.principal();
            policy = policy.with_principal(TaskScopeCapacity::new(
                principal.outstanding(),
                TaskCapacityLimit::new(1).expect("fixed cap"),
                principal.resident_bytes(),
            ));
        }
        storage
            .replace_task_policy(StorageTaskPolicyUpdate::new(current.revision(), policy))
            .await?;
        let inventory = fixture_inventory
            .map(serde_json::to_value)
            .transpose()
            .map_err(|error| PostgresStorageError::internal(error.to_string()))?;
        with_transaction(&self.pool, async |connection| {
            connection.batch_execute("CREATE TABLE hubuum_control.scheduling_benchmark_fixture (
                singleton BOOLEAN PRIMARY KEY CHECK(singleton),settings JSONB NOT NULL,inventory JSONB)").await?;
            diesel::sql_query("INSERT INTO hubuum_control.scheduling_benchmark_fixture VALUES(TRUE,$1,$2)")
                .bind::<Jsonb,_>(options.fixture_key()).bind::<Nullable<Jsonb>,_>(&inventory)
                .execute(connection).await?;
            Ok::<_,PostgresStorageError>(())
        }).await?;
        Ok(inventory)
    }

    pub async fn benchmark_task_scheduling(
        &self,
        options: SchedulingBenchmarkOptions,
    ) -> Result<SchedulingBenchmarkReport> {
        let fixture_inventory = match options.phase {
            SchedulingPhase::Complete | SchedulingPhase::Prepare => {
                self.prepare_scheduling_fixture(&options).await?
            }
            SchedulingPhase::Workers { .. } => {
                // A separate process can only consume a fixture prepared by this
                // tool with matching workload, policy and dataset settings.
                let fixture = with_connection(&self.pool, async |connection| {
                    diesel::sql_query("SELECT settings,inventory FROM hubuum_control.scheduling_benchmark_fixture WHERE singleton")
                        .get_result::<PreparedFixture>(connection).await.map_err(PostgresStorageError::from)
                }).await?;
                if fixture.settings != options.fixture_key() {
                    return Err(PostgresStorageError::invalid_input(
                        "Worker settings do not match the prepared benchmark fixture",
                    ));
                }
                fixture.inventory
            }
        };
        let measurement_window = if let Some(seconds) = options.timed_seconds {
            let start = with_connection(&self.pool, async |connection| {
                diesel::sql_query("SELECT clock_timestamp() AT TIME ZONE 'UTC' AS now")
                    .get_result::<MeasurementClock>(connection)
                    .await
                    .map(|row| row.now)
                    .map_err(PostgresStorageError::from)
            })
            .await?;
            Some(MeasurementWindow {
                start,
                end: start + TimeDelta::seconds(seconds as i64),
            })
        } else {
            None
        };
        let started = Instant::now();
        let next = Arc::new(AtomicU32::new(0));
        // Prepared worker processes share one eight-connection pool each. The
        // single-process diagnostic mode can still model two independent pools.
        let replicas = if matches!(options.phase, SchedulingPhase::Complete) && options.slots == 4 {
            vec![
                self.pool.clone(),
                benchmark_pool(&self.database_url, 8)
                    .map_err(|error| PostgresStorageError::internal(error.to_string()))?,
            ]
        } else {
            vec![self.pool.clone()]
        };
        let process_slots = match options.phase {
            SchedulingPhase::Complete => options.slots,
            SchedulingPhase::Prepare => 0,
            SchedulingPhase::Workers { slots } => slots,
        };
        let workers: Vec<_> = (0..process_slots)
            .map(|slot| {
                (
                    replicas[usize::from(slot) % replicas.len()].clone(),
                    StorageTaskClaimRequest::new(
                        TaskWorkerId::generate(),
                        StorageTaskLeaseDuration::from_milliseconds(120_000).expect("fixed lease"),
                    ),
                )
            })
            .collect();
        // Registration may claim because the queue is populated. Measure every
        // resulting claim instead of silently consuming warm-up turns.
        let results = try_join_all(workers.into_iter().map(|(pool, request)| {
            run_worker(pool, request, options.clone(), next.clone(), started)
        }))
        .await?;
        let mut samples = Vec::new();
        let mut empty_polls = Vec::new();
        let mut failed_polls = Vec::new();
        for (mut claims, mut empty, mut failed) in results {
            samples.append(&mut claims);
            empty_polls.append(&mut empty);
            failed_polls.append(&mut failed);
        }
        samples.sort_by_key(|claim| claim.sequence);
        let warmup_claims = if options.timed_seconds.is_none() {
            100.min(samples.len())
        } else {
            0
        };
        let measured = &samples[warmup_claims..];
        let principal_cap_blocked_us = if let Some(window) = &measurement_window {
            with_connection(&self.pool, async |connection| {
                diesel::sql_query(
                    "SELECT submitted_by AS principal, \
                    sum(extract(epoch FROM least(finished_at,$2)-greatest(started_at,$1))*1000000)::bigint AS occupied_us \
                    FROM tasks WHERE status='failed' AND submitted_by IS NOT NULL \
                    AND finished_at>$1 AND started_at<$2 GROUP BY submitted_by",
                )
                .bind::<Timestamp,_>(window.start)
                .bind::<Timestamp,_>(window.end)
                .load::<PrincipalOccupancy>(connection)
                .await
                .map(|rows| {
                    rows.into_iter()
                        .map(|row| (row.principal, row.occupied_us.max(0) as u64))
                        .collect()
                })
                .map_err(PostgresStorageError::from)
            })
            .await?
        } else {
            BTreeMap::new()
        };
        let mut ordinary_starts = BTreeMap::new();
        let mut principal_starts = BTreeMap::new();
        for sample in measured {
            if !sample.promoted {
                *ordinary_starts.entry(sample.kind.clone()).or_default() += 1;
            }
            *principal_starts.entry(sample.principal).or_default() += 1;
        }
        let postgres = with_connection(&self.pool, async |connection| {
            diesel::sql_query("SELECT version() AS value")
                .get_result::<super::TextRow>(connection)
                .await
                .map(|row| row.value)
                .map_err(PostgresStorageError::from)
        })
        .await?;
        let elapsed_us = micros(started.elapsed());
        Ok(SchedulingBenchmarkReport {
            claim_p95_us: percentile(measured.iter().map(|s| s.claim_us).collect(), 95),
            claim_p99_us: percentile(measured.iter().map(|s| s.claim_us).collect(), 99),
            lock_p95_us: percentile(
                measured
                    .iter()
                    .map(|s| s.lock_wait_us.max(0) as u64)
                    .collect(),
                95,
            ),
            max_round_trips: samples.iter().map(|s| s.round_trips).max().unwrap_or(0),
            promotions: measured.iter().filter(|s| s.promoted).count() as u64,
            ordinary_starts,
            principal_starts,
            occupied_slot_fraction: samples.iter().map(|s| s.window_busy_us as f64).sum::<f64>()
                / (options
                    .timed_seconds
                    .map_or(elapsed_us, |seconds| seconds * 1_000_000) as f64
                    * f64::from(if options.slots == 1 {
                        1
                    } else {
                        options.slots - 1
                    })),
            options,
            fixture_inventory,
            warmup_claims,
            principal_cap_blocked_us,
            measurement_window,
            postgres,
            elapsed_us,
            empty_polls,
            failed_polls,
            samples,
        })
    }
}

async fn run_worker(
    pool: PostgresPool,
    request: StorageTaskClaimRequest,
    options: SchedulingBenchmarkOptions,
    next: Arc<AtomicU32>,
    started: Instant,
) -> Result<(Vec<ClaimSample>, Vec<u64>, Vec<String>)> {
    let storage = PostgresStorage::unobserved(pool.clone());
    let mut samples = Vec::new();
    let mut empty = Vec::new();
    let mut failed = Vec::new();
    loop {
        if options
            .timed_seconds
            .is_some_and(|seconds| started.elapsed() >= Duration::from_secs(seconds))
        {
            break;
        }
        let target = options.samples
            + if options.timed_seconds.is_none() {
                100
            } else {
                0
            };
        #[allow(
            deprecated,
            reason = "try_update requires Rust 1.95; this crate supports Rust 1.88"
        )]
        let Ok(ticket) = next.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            (value < target).then_some(value + 1)
        }) else {
            break;
        };
        let claim_started = Instant::now();
        let (claimed, queries) = capture_queries(storage.claim_next_task(request)).await;
        let claim_us = micros(claim_started.elapsed());
        let claim = match claimed {
            Ok(Some(claim)) => claim,
            Ok(None) => {
                empty.push(claim_us);
                next.fetch_sub(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(5)).await;
                continue;
            }
            Err(error) => {
                failed.push(error.to_string());
                break;
            }
        };
        let diagnostics = with_connection(&pool, async |connection| {
                        diesel::sql_query("SELECT last_lock_wait_us,last_claim_sequence,last_claim_promoted FROM hubuum_control.task_workers WHERE worker_identity=$1::uuid")
                            .bind::<Text,_>(request.worker().as_string()).get_result::<ClaimDiagnostics>(connection).await.map_err(PostgresStorageError::from)
                    }).await?;
        let task = claim.task();
        let id = task.id().id();
        let kind = task.kind().as_str().to_string();
        let principal = task.submitted_by().expect("fixture principal").id();
        let busy_started = Instant::now();
        let busy_offset = started.elapsed();
        if options.timed_seconds.is_some() {
            let duration = task
                .request_payload()
                .and_then(|value| value.get("duration_ms"))
                .and_then(serde_json::Value::as_u64)
                .expect("fixture duration");
            tokio::time::sleep(Duration::from_millis(duration)).await;
        }
        let busy_us = micros(busy_started.elapsed());
        let window_busy_us = options.timed_seconds.map_or(0, |seconds| {
            micros(Duration::from_secs(seconds).saturating_sub(busy_offset)).min(busy_us)
        });
        storage
            .fail_task(StorageTaskFailure::new(
                claim.lease().clone(),
                "benchmark completed",
                StorageTaskEventInput::new("failed", "benchmark completed"),
            ))
            .await?;
        // Keep every compared class continuously queued. Refill is
        // excluded from claim latency, but included in utilization.
        with_transaction(&pool, async |connection| {
                        connection.batch_execute("SET LOCAL hubuum.reconcile_task_capacity='on'").await?;
                        diesel::sql_query("INSERT INTO tasks(kind,status,submitted_by,request_payload,created_at) \
                            SELECT kind,'queued',submitted_by,jsonb_build_object('duration_ms',$2),created_at FROM tasks WHERE id=$1")
                            .bind::<Integer,_>(id).bind::<BigInt,_>(options.workload.tasks().nth(ticket as usize % options.workload.task_count() as usize).expect("fixture recipe").duration().as_millis() as i64)
                            .execute(connection).await?;
                        Ok::<_, PostgresStorageError>(())
                    }).await?;
        samples.push(ClaimSample {
            task: id,
            principal,
            kind,
            promoted: diagnostics.last_claim_promoted,
            sequence: diagnostics.last_claim_sequence,
            claim_us,
            lock_wait_us: diagnostics.last_lock_wait_us,
            round_trips: queries.total_queries(),
            elapsed_us: micros(started.elapsed()),
            busy_us,
            window_busy_us,
        });
    }
    Ok::<_, PostgresStorageError>((samples, empty, failed))
}

#[derive(QueryableByName)]
struct PrincipalOccupancy {
    #[diesel(sql_type = Integer)]
    principal: i32,
    #[diesel(sql_type = BigInt)]
    occupied_us: i64,
}
