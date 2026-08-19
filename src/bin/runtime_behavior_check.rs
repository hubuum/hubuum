use std::ffi::OsString;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::{Child, Command as ProcessCommand, Stdio};
use std::time::{Duration, Instant};

use clap::{Args, Parser, Subcommand};
use diesel::QueryableByName;
use diesel_async::RunQueryDsl;
use hubuum::observability::runtime_behavior::{
    MetricSnapshot, ProcessIdleReport, ReadinessReport, RuntimeBehaviorAssessment,
    RuntimeBehaviorBudgets, RuntimeBehaviorReport, TaskNotificationReport,
};
use hubuum_storage_postgres::{
    PostgresPool, PostgresPoolSettings, build_postgres_pool, with_connection, with_transaction,
};
use reqwest::{Client, StatusCode};
use tokio::time::sleep;

const TASK_IDLE_COUNTER: &str = "hubuum_task_worker_iterations_total";
const TASK_CLAIM_COUNTER: &str = "hubuum_task_claims_total";
const DB_ACQUISITION_COUNTER: &str = "hubuum_db_connection_acquire_duration_seconds_count";
const TASK_QUEUE_CHANNEL: &str = "hubuum_task_queue";
const TERMINAL_TASK_STATUSES: [&str; 4] =
    ["succeeded", "failed", "partially_succeeded", "cancelled"];

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;

fn runtime_check_pool(database_url: &str) -> Result<PostgresPool> {
    let settings = PostgresPoolSettings::builder(database_url)
        .max_size(2)
        .statement_timeout_ms(0)
        .acquire_timeout_ms(30_000)
        .build()?;
    Ok(build_postgres_pool(&settings)?)
}

#[derive(Debug, Parser)]
#[command(about = "Measure and assess Hubuum runtime background behavior")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Start primary and standby processes and write a JSON measurement.
    Measure(MeasureArgs),
    /// Enforce budgets and optionally compare base and head measurements.
    Assess(AssessArgs),
}

#[derive(Debug, Args)]
struct MeasureArgs {
    #[arg(long)]
    server_binary: PathBuf,
    #[arg(long)]
    database_url: String,
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    label: String,
    #[arg(long, default_value_t = 60)]
    sample_seconds: u64,
    #[arg(long, default_value_t = 18_080)]
    primary_port: u16,
    #[arg(long, default_value_t = 18_081)]
    standby_port: u16,
    #[arg(long, default_value_t = 10)]
    readiness_requests: u32,
    #[arg(long, default_value_t = 60)]
    startup_timeout_seconds: u64,
    #[arg(long, default_value_t = 7_000)]
    notification_timeout_ms: u64,
    #[arg(long, default_value_t = 1_000)]
    task_claim_timeout_ms: u64,
}

#[derive(Debug, Args)]
struct AssessArgs {
    #[arg(long)]
    head: PathBuf,
    #[arg(long)]
    base: Option<PathBuf>,
    #[arg(long)]
    markdown_output: Option<PathBuf>,
    #[arg(long, default_value_t = 1.9)]
    max_primary_background_db_per_second: f64,
    #[arg(long, default_value_t = 1.2)]
    max_standby_background_db_per_second: f64,
    #[arg(long, default_value_t = 3.0)]
    max_aggregate_background_db_per_second: f64,
    #[arg(long, default_value_t = 0.25)]
    max_task_idle_per_second: f64,
    #[arg(long, default_value_t = 0.25)]
    max_fanout_poll_per_second: f64,
    #[arg(long, default_value_t = 1.1)]
    max_restore_db_per_second: f64,
    #[arg(long, default_value_t = 2.25)]
    max_task_worker_db_per_idle_iteration: f64,
    #[arg(long, default_value_t = 1.1)]
    max_fanout_db_per_poll_wakeup: f64,
    #[arg(long, default_value_t = 1_000.0)]
    max_notification_latency_ms: f64,
    #[arg(long, default_value_t = 1_000.0)]
    max_task_claim_latency_ms: f64,
    #[arg(long, default_value_t = 25.0)]
    max_relative_regression_percent: f64,
}

struct ManagedServer {
    child: Child,
    log_path: PathBuf,
}

impl ManagedServer {
    fn spawn(
        binary: &Path,
        database_url: &str,
        port: u16,
        role: &str,
        log_path: PathBuf,
    ) -> Result<Self> {
        let log = File::create(&log_path)?;
        let mut command = ProcessCommand::new(binary);
        command
            .env_clear()
            .env("HUBUUM_DATABASE_URL", database_url)
            .env("HUBUUM_BIND_IP", "127.0.0.1")
            .env("HUBUUM_BIND_PORT", port.to_string())
            .env("HUBUUM_RUNTIME_ROLE", role)
            .env("HUBUUM_LOG_LEVEL", "warn")
            .env("HUBUUM_CLIENT_ALLOWLIST", "*")
            .env("HUBUUM_METRICS_ENABLED", "true")
            .env("HUBUUM_ACTIX_WORKERS", "1")
            .env("HUBUUM_DB_POOL_SIZE", "10")
            .env("HUBUUM_TASK_WORKERS", "1")
            .env("HUBUUM_TASK_POLL_INTERVAL_MS", "5000")
            .env("HUBUUM_EVENT_FANOUT_WORKERS", "1")
            .env("HUBUUM_EVENT_FANOUT_POLL_INTERVAL_MS", "5000")
            .env("HUBUUM_EVENT_DELIVERY_WORKERS", "0")
            .env("HUBUUM_EVENT_RETENTION_PURGE_ENABLED", "false")
            .env("HUBUUM_TOKEN_RETENTION_PURGE_ENABLED", "false")
            .env("HUBUUM_LOGIN_RATE_LIMIT_BACKEND", "memory")
            .env(
                "HUBUUM_TOKEN_HASH_KEY",
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            )
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log));
        let child = command.spawn()?;
        Ok(Self { child, log_path })
    }

    fn ensure_running(&mut self) -> Result<()> {
        if let Some(status) = self.child.try_wait()? {
            return Err(benchmark_error(format!(
                "server exited with {status}; log tail:\n{}",
                read_log_tail(&self.log_path)?
            )));
        }
        Ok(())
    }
}

impl Drop for ManagedServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Measure(args) => measure(args).await,
        Command::Assess(args) => assess(args),
    }
}

async fn measure(args: MeasureArgs) -> Result<()> {
    validate_measure_args(&args)?;
    if let Some(parent) = args.output.parent() {
        fs::create_dir_all(parent)?;
    }
    let client = Client::builder()
        .connect_timeout(Duration::from_secs(1))
        .timeout(Duration::from_secs(2))
        .build()?;
    let primary_url = format!("http://127.0.0.1:{}", args.primary_port);
    let standby_url = format!("http://127.0.0.1:{}", args.standby_port);
    let mut primary = ManagedServer::spawn(
        &args.server_binary,
        &args.database_url,
        args.primary_port,
        "all",
        log_path(&args.output, "primary"),
    )?;
    wait_until_ready(
        &client,
        &primary_url,
        &mut primary,
        Duration::from_secs(args.startup_timeout_seconds),
    )
    .await?;
    let mut standby = ManagedServer::spawn(
        &args.server_binary,
        &args.database_url,
        args.standby_port,
        "api",
        log_path(&args.output, "standby"),
    )?;
    wait_until_ready(
        &client,
        &standby_url,
        &mut standby,
        Duration::from_secs(args.startup_timeout_seconds),
    )
    .await?;

    let (primary_before, primary_before_at) = fetch_snapshot(&client, &primary_url).await?;
    let (standby_before, standby_before_at) = fetch_snapshot(&client, &standby_url).await?;
    sleep(Duration::from_secs(args.sample_seconds)).await;
    primary.ensure_running()?;
    standby.ensure_running()?;
    let (primary_after, primary_after_at) = fetch_snapshot(&client, &primary_url).await?;
    let (standby_after, standby_after_at) = fetch_snapshot(&client, &standby_url).await?;

    let primary_idle = ProcessIdleReport::from_snapshots(
        "all",
        primary_after_at
            .saturating_duration_since(primary_before_at)
            .as_secs_f64(),
        &primary_before,
        &primary_after,
    )?;
    let standby_idle = ProcessIdleReport::from_snapshots(
        "api",
        standby_after_at
            .saturating_duration_since(standby_before_at)
            .as_secs_f64(),
        &standby_before,
        &standby_after,
    )?;
    validate_roles(&primary_after, &standby_after)?;

    let (primary_readiness, primary_after_readiness) = measure_readiness(
        &client,
        &primary_url,
        &primary_after,
        args.readiness_requests,
    )
    .await?;
    let (standby_readiness, _) = measure_readiness(
        &client,
        &standby_url,
        &standby_after,
        args.readiness_requests,
    )
    .await?;

    let task_notification = measure_task_notification(
        &client,
        &primary_url,
        &args.database_url,
        &primary_after_readiness,
        Duration::from_millis(args.notification_timeout_ms),
        Duration::from_millis(args.task_claim_timeout_ms),
    )
    .await?;
    primary.ensure_running()?;
    standby.ensure_running()?;

    let report = RuntimeBehaviorReport::new(
        args.label,
        primary_idle,
        standby_idle,
        primary_readiness,
        standby_readiness,
        task_notification,
    );
    report.write(&args.output)?;
    println!(
        "Wrote runtime behavior report to {} (primary {:.3}, standby {:.3} background DB acquisitions/s)",
        args.output.display(),
        report.primary().background_db_acquisitions_per_second(),
        report.standby().background_db_acquisitions_per_second()
    );
    Ok(())
}

fn assess(args: AssessArgs) -> Result<()> {
    let head = RuntimeBehaviorReport::read(&args.head)?;
    let base = args
        .base
        .as_deref()
        .map(RuntimeBehaviorReport::read)
        .transpose()?;
    let budgets = RuntimeBehaviorBudgets {
        max_primary_background_db_per_second: args.max_primary_background_db_per_second,
        max_standby_background_db_per_second: args.max_standby_background_db_per_second,
        max_aggregate_background_db_per_second: args.max_aggregate_background_db_per_second,
        max_task_idle_per_second: args.max_task_idle_per_second,
        max_fanout_poll_per_second: args.max_fanout_poll_per_second,
        max_restore_db_per_second: args.max_restore_db_per_second,
        max_task_worker_db_per_idle_iteration: args.max_task_worker_db_per_idle_iteration,
        max_fanout_db_per_poll_wakeup: args.max_fanout_db_per_poll_wakeup,
        max_notification_latency_ms: args.max_notification_latency_ms,
        max_task_claim_latency_ms: args.max_task_claim_latency_ms,
        max_relative_regression_percent: args.max_relative_regression_percent,
    };
    let assessment = RuntimeBehaviorAssessment::assess(&head, base.as_ref(), budgets);
    print!("{}", assessment.markdown());
    if let Some(path) = &args.markdown_output {
        assessment.append_markdown(path)?;
    }
    assessment.ensure_passed()
}

async fn wait_until_ready(
    client: &Client,
    url: &str,
    server: &mut ManagedServer,
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        server.ensure_running()?;
        if let Ok(response) = client.get(format!("{url}/readyz")).send().await
            && response.status() == StatusCode::OK
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(benchmark_error(format!(
                "server at {url} did not become ready; log tail:\n{}",
                read_log_tail(&server.log_path)?
            )));
        }
        sleep(Duration::from_millis(100)).await;
    }
}

async fn fetch_snapshot(client: &Client, url: &str) -> Result<(MetricSnapshot, Instant)> {
    let response = client.get(format!("{url}/metrics")).send().await?;
    if response.status() != StatusCode::OK {
        return Err(benchmark_error(format!(
            "metrics request to {url} returned {}",
            response.status()
        )));
    }
    let body = response.text().await?;
    let received_at = Instant::now();
    Ok((MetricSnapshot::parse(&body)?, received_at))
}

async fn measure_readiness(
    client: &Client,
    url: &str,
    before: &MetricSnapshot,
    request_count: u32,
) -> Result<(ReadinessReport, MetricSnapshot)> {
    let mut successful = 0;
    let mut latencies = Vec::with_capacity(request_count as usize);
    for _ in 0..request_count {
        let started_at = Instant::now();
        let response = client.get(format!("{url}/readyz")).send().await?;
        latencies.push(started_at.elapsed().as_secs_f64() * 1000.0);
        if response.status() == StatusCode::OK {
            successful += 1;
        }
    }
    let (after, _) = fetch_snapshot(client, url).await?;
    let acquisitions =
        before.counter_delta(&after, DB_ACQUISITION_COUNTER, &[("caller", "readiness")])?;
    Ok((
        ReadinessReport::new(request_count, successful, acquisitions, &latencies)?,
        after,
    ))
}

async fn measure_task_notification(
    http_client: &Client,
    primary_url: &str,
    database_url: &str,
    initial_snapshot: &MetricSnapshot,
    notification_timeout: Duration,
    claim_timeout: Duration,
) -> Result<TaskNotificationReport> {
    let pool = runtime_check_pool(database_url)?;
    warm_database_pool(&pool).await?;

    let idle_before = initial_snapshot.value(TASK_IDLE_COUNTER, &[("outcome", "idle")]);
    let warmup_started_at = Instant::now();
    notify_task_worker(&pool, "warmup").await?;
    let aligned_snapshot = wait_for_counter_increment(
        http_client,
        primary_url,
        TASK_IDLE_COUNTER,
        &[("outcome", "idle")],
        idle_before,
        notification_timeout,
    )
    .await?;
    let warmup_wakeup_latency_ms = warmup_started_at.elapsed().as_secs_f64() * 1000.0;

    let claims_before = aligned_snapshot.value(TASK_CLAIM_COUNTER, &[("kind", "export")]);
    let claim_started_at = Instant::now();
    let task_id = insert_synthetic_task(&pool).await?;
    wait_for_counter_increment(
        http_client,
        primary_url,
        TASK_CLAIM_COUNTER,
        &[("kind", "export")],
        claims_before,
        claim_timeout,
    )
    .await?;
    let claim_latency_ms = claim_started_at.elapsed().as_secs_f64() * 1000.0;
    let terminal_status = wait_for_terminal_task(&pool, task_id, Duration::from_secs(5)).await?;
    Ok(TaskNotificationReport::new(
        warmup_wakeup_latency_ms,
        claim_latency_ms,
        terminal_status,
    ))
}

async fn wait_for_counter_increment(
    client: &Client,
    url: &str,
    name: &str,
    labels: &[(&str, &str)],
    before: f64,
    timeout: Duration,
) -> Result<MetricSnapshot> {
    let deadline = Instant::now() + timeout;
    loop {
        let (snapshot, _) = fetch_snapshot(client, url).await?;
        if snapshot.value(name, labels) > before {
            return Ok(snapshot);
        }
        if Instant::now() >= deadline {
            return Err(benchmark_error(format!(
                "counter '{name}' did not increase within {} ms",
                timeout.as_millis()
            )));
        }
        sleep(Duration::from_millis(20)).await;
    }
}

#[derive(QueryableByName)]
struct SyntheticTaskId {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    id: i32,
}

#[derive(QueryableByName)]
struct SyntheticTaskStatus {
    #[diesel(sql_type = diesel::sql_types::Text)]
    status: String,
}

async fn warm_database_pool(pool: &PostgresPool) -> Result<()> {
    with_connection(
        pool,
        async |conn| -> std::result::Result<_, diesel::result::Error> {
            diesel::sql_query("SELECT 1").execute(conn).await?;
            Ok(())
        },
    )
    .await
    .map_err(|error| benchmark_error(error.to_string()))?;
    Ok(())
}

async fn notify_task_worker(pool: &PostgresPool, payload: &str) -> Result<()> {
    let payload = payload.to_string();
    with_connection(
        pool,
        async move |conn| -> std::result::Result<_, diesel::result::Error> {
            diesel::sql_query("SELECT pg_notify($1, $2)")
                .bind::<diesel::sql_types::Text, _>(TASK_QUEUE_CHANNEL)
                .bind::<diesel::sql_types::Text, _>(payload)
                .execute(conn)
                .await?;
            Ok(())
        },
    )
    .await
    .map_err(|error| benchmark_error(error.to_string()))?;
    Ok(())
}

async fn insert_synthetic_task(pool: &PostgresPool) -> Result<i32> {
    let task_id = with_transaction(
        pool,
        async |conn| -> std::result::Result<_, diesel::result::Error> {
            let task = diesel::sql_query(
                "INSERT INTO tasks (kind, status, request_payload) \
                 VALUES ('export', 'queued', '{}'::jsonb) RETURNING id",
            )
            .get_result::<SyntheticTaskId>(conn)
            .await?;
            diesel::sql_query("SELECT pg_notify($1, $2)")
                .bind::<diesel::sql_types::Text, _>(TASK_QUEUE_CHANNEL)
                .bind::<diesel::sql_types::Text, _>(task.id.to_string())
                .execute(conn)
                .await?;
            Ok(task.id)
        },
    )
    .await
    .map_err(|error| benchmark_error(error.to_string()))?;
    Ok(task_id)
}

async fn wait_for_terminal_task(
    pool: &PostgresPool,
    task_id: i32,
    timeout: Duration,
) -> Result<String> {
    let deadline = Instant::now() + timeout;
    loop {
        let status = with_connection(
            pool,
            async |conn| -> std::result::Result<_, diesel::result::Error> {
                diesel::sql_query("SELECT status FROM tasks WHERE id = $1")
                    .bind::<diesel::sql_types::Integer, _>(task_id)
                    .get_result::<SyntheticTaskStatus>(conn)
                    .await
                    .map(|row| row.status)
            },
        )
        .await
        .map_err(|error| benchmark_error(error.to_string()))?;
        if TERMINAL_TASK_STATUSES.contains(&status.as_str()) {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            return Err(benchmark_error(format!(
                "synthetic task {task_id} did not finish within {} ms",
                timeout.as_millis()
            )));
        }
        sleep(Duration::from_millis(20)).await;
    }
}

fn validate_roles(primary: &MetricSnapshot, standby: &MetricSnapshot) -> Result<()> {
    if primary.value("hubuum_runtime_info", &[("role", "all")]) != 1.0 {
        return Err(benchmark_error("primary did not report runtime role 'all'"));
    }
    if standby.value("hubuum_runtime_info", &[("role", "api")]) != 1.0 {
        return Err(benchmark_error("standby did not report runtime role 'api'"));
    }
    if primary.value("hubuum_task_workers_configured", &[]) != 1.0 {
        return Err(benchmark_error(
            "primary did not report exactly one task worker",
        ));
    }
    if standby.value("hubuum_task_workers_configured", &[]) != 0.0 {
        return Err(benchmark_error("standby unexpectedly enabled task workers"));
    }
    Ok(())
}

fn validate_measure_args(args: &MeasureArgs) -> Result<()> {
    if args.sample_seconds < 10 {
        return Err(benchmark_error(
            "sample duration must be at least 10 seconds",
        ));
    }
    if args.primary_port == args.standby_port {
        return Err(benchmark_error("primary and standby ports must differ"));
    }
    if args.readiness_requests == 0 {
        return Err(benchmark_error(
            "readiness request count must be greater than zero",
        ));
    }
    if !args.server_binary.is_file() {
        return Err(benchmark_error(format!(
            "server binary '{}' does not exist",
            args.server_binary.display()
        )));
    }
    Ok(())
}

fn log_path(output: &Path, role: &str) -> PathBuf {
    let stem = output
        .file_stem()
        .map_or_else(|| OsString::from("runtime-behavior"), OsString::from);
    let mut file_name = stem;
    file_name.push(format!(".{role}.log"));
    output.with_file_name(file_name)
}

fn read_log_tail(path: &Path) -> Result<String> {
    let content = fs::read_to_string(path)?;
    Ok(content
        .lines()
        .rev()
        .take(40)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<Vec<_>>()
        .join("\n"))
}

fn benchmark_error(message: impl Into<String>) -> Error {
    std::io::Error::other(message.into()).into()
}
