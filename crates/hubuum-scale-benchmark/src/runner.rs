use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use futures_util::{StreamExt, stream};
use hubuum_scale_core::{
    BackendResourceReport, BenchmarkPrincipal, CorrectnessReport, DatasetManifest,
    LatencyDistribution, LifecycleReport, LimitMode, ResourceReport, Result, RuntimeIdentity,
    ScaleAxis, ScaleBenchmarkBackend, ScaleBenchmarkReport, ScaleProfile, ScenarioReport,
    WorkloadScenario, WorkloadSpec, invalid_data,
};
use reqwest::{Client, StatusCode, Url, header};
use serde_json::{Value, json};
use sysinfo::System;
use tokio::time::sleep;

use crate::metrics::MetricSnapshot;

const TOKEN_HASH_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

#[derive(Clone, Debug)]
pub struct MeasureOptions {
    pub server_binary: PathBuf,
    pub admin_binary: Option<PathBuf>,
    pub restore_test_database_url: Option<String>,
    pub artifact_directory: PathBuf,
    pub label: String,
    pub port: u16,
    pub limit_mode: LimitMode,
    pub scale_axis: Option<ScaleAxis>,
    pub run_lifecycle: bool,
    pub startup_timeout: Duration,
}

struct ManagedServer {
    child: Child,
    log_path: PathBuf,
}

impl ManagedServer {
    fn spawn(
        options: &MeasureOptions,
        profile: &ScaleProfile,
        backend_environment: &BTreeMap<String, String>,
    ) -> Result<Self> {
        fs::create_dir_all(&options.artifact_directory)?;
        let log_path = options.artifact_directory.join("server.log");
        let log = File::create(&log_path)?;
        let limits = options.limit_mode.settings();
        let mut command = Command::new(&options.server_binary);
        command
            .env_clear()
            .env("HUBUUM_BIND_IP", "127.0.0.1")
            .env("HUBUUM_BIND_PORT", options.port.to_string())
            .env("HUBUUM_RUNTIME_ROLE", "all")
            .env("HUBUUM_LOG_LEVEL", "warn")
            .env("HUBUUM_CLIENT_ALLOWLIST", "*")
            .env("HUBUUM_METRICS_ENABLED", "true")
            .env("HUBUUM_ACTIX_WORKERS", "4")
            .env(
                "HUBUUM_DB_POOL_SIZE",
                profile.provisioning.db_pool_size.to_string(),
            )
            .env(
                "HUBUUM_DB_STATEMENT_TIMEOUT_MS",
                profile.provisioning.db_statement_timeout_ms.to_string(),
            )
            .env(
                "HUBUUM_DEFAULT_PAGE_LIMIT",
                limits.default_page_limit.to_string(),
            )
            .env(
                "HUBUUM_MAX_PAGE_LIMIT",
                limits.maximum_page_limit.to_string(),
            )
            .env(
                "HUBUUM_MAX_TRANSITIVE_DEPTH",
                limits.maximum_graph_depth.to_string(),
            )
            .env(
                "HUBUUM_EXPORT_MAX_OUTPUT_BYTES",
                limits.maximum_export_output_bytes.to_string(),
            )
            .env(
                "HUBUUM_BACKUP_MAX_OUTPUT_BYTES",
                profile.provisioning.backup_max_output_bytes.to_string(),
            )
            .env(
                "HUBUUM_RESTORE_MAX_UPLOAD_BYTES",
                profile.provisioning.restore_max_upload_bytes.to_string(),
            )
            .env("HUBUUM_TASK_WORKERS", "2")
            .env("HUBUUM_EVENT_FANOUT_WORKERS", "1")
            .env("HUBUUM_EVENT_DELIVERY_WORKERS", "0")
            .env("HUBUUM_EVENT_RETENTION_PURGE_ENABLED", "false")
            .env("HUBUUM_TOKEN_RETENTION_PURGE_ENABLED", "false")
            .env("HUBUUM_LOGIN_RATE_LIMIT_BACKEND", "memory")
            .env("HUBUUM_LOGIN_RATE_LIMIT_ENABLED", "false")
            .env("HUBUUM_TOKEN_HASH_KEY", TOKEN_HASH_KEY)
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log));
        command.envs(backend_environment);
        Ok(Self {
            child: command.spawn()?,
            log_path,
        })
    }

    fn ensure_running(&mut self) -> Result<()> {
        if let Some(status) = self.child.try_wait()? {
            return Err(invalid_data(format!(
                "scale benchmark server exited with {status}; log tail:\n{}",
                log_tail(&self.log_path)?
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

#[derive(Clone)]
struct RequestContext {
    client: Client,
    base_url: String,
    tokens: BTreeMap<String, String>,
    sparse_collection_ids: BTreeSet<i64>,
    sparse_candidate_count: u64,
}

#[derive(Default)]
struct RequestOutcome {
    latency_ms: f64,
    success: bool,
    timed_out: bool,
    status: Option<u16>,
    bytes: u64,
    items: u64,
    graph_nodes: Option<u64>,
    graph_edges: Option<u64>,
    pages: u64,
    traversal_ms: Option<f64>,
    page_latencies_ms: Vec<f64>,
    page_statuses: Vec<u16>,
    successful_pages: u64,
    page_timeouts: u64,
    ids: Vec<i64>,
    total: Option<u64>,
    unauthorized_rows: u64,
    authorization_candidates: Option<u64>,
    authorized_rows: Option<u64>,
}

pub async fn measure_scale_benchmark<B: ScaleBenchmarkBackend>(
    backend: &B,
    options: MeasureOptions,
    profile: ScaleProfile,
    manifest: DatasetManifest,
    workload: WorkloadSpec,
    generation_ms: u64,
    loading_ms: u64,
) -> Result<ScaleBenchmarkReport> {
    profile.validate()?;
    manifest.validate(&profile)?;
    workload.validate()?;
    fs::create_dir_all(&options.artifact_directory)?;
    backend.verify_dataset(&profile, &manifest).await?;
    let preparation = backend.prepare_measurement().await?;
    let backend_resources_before = backend.begin_resource_measurement().await?;

    let client = Client::builder()
        .connect_timeout(Duration::from_secs(2))
        .timeout(Duration::from_secs(workload.request_timeout_seconds))
        .build()?;
    let base_url = format!("http://127.0.0.1:{}", options.port);
    let backend_environment = backend.server_environment();
    let mut server = ManagedServer::spawn(&options, &profile, &backend_environment)?;
    wait_until_ready(&client, &base_url, &mut server, options.startup_timeout).await?;
    let before_metrics = fetch_metrics(&client, &base_url).await?;
    let benchmark_principals = backend.benchmark_principals();
    let tokens = login_principals(&client, &base_url, &benchmark_principals).await?;
    let context = RequestContext {
        client: client.clone(),
        base_url: base_url.clone(),
        tokens,
        sparse_collection_ids: preparation.sparse_collection_ids.clone(),
        sparse_candidate_count: manifest.totals.classes,
    };

    let mut scenarios = Vec::new();
    let mut correctness = CorrectnessReport {
        request_failures: 0,
        traversal_duplicates: 0,
        traversal_missing: 0,
        unauthorized_rows: 0,
        manifest_mismatches: 0,
        lifecycle_failures: 0,
    };

    for scenario in workload
        .scenarios
        .iter()
        .filter(|scenario| scenario.applies_to(options.scale_axis))
    {
        let path = workload.render_path(scenario, &manifest, options.limit_mode)?;
        let outcome = execute_request(&context, scenario, &path).await;
        merge_correctness(&mut correctness, &outcome);
        scenarios.push(report_outcomes(
            scenario,
            "first_touch",
            1,
            &[outcome],
            Duration::ZERO,
        ));
    }

    backend.mark_computed_ready().await?;
    for scenario in workload
        .scenarios
        .iter()
        .filter(|scenario| scenario.applies_to(options.scale_axis))
    {
        let path = workload.render_path(scenario, &manifest, options.limit_mode)?;
        for _ in 0..scenario.warmup_requests.unwrap_or(workload.warmup_requests) {
            let _ = execute_request(&context, scenario, &path).await;
        }
    }

    for scenario in workload
        .scenarios
        .iter()
        .filter(|scenario| scenario.applies_to(options.scale_axis))
    {
        let path = workload.render_path(scenario, &manifest, options.limit_mode)?;
        let sample_count = scenario
            .single_client_samples
            .unwrap_or(workload.single_client_samples);
        let started = Instant::now();
        let mut outcomes = Vec::with_capacity(sample_count);
        for _ in 0..sample_count {
            outcomes.push(execute_request(&context, scenario, &path).await);
        }
        for outcome in &outcomes {
            merge_correctness(&mut correctness, outcome);
        }
        scenarios.push(report_outcomes(
            scenario,
            "warm_single_client",
            1,
            &outcomes,
            started.elapsed(),
        ));
        if scenario.traverse {
            let started = Instant::now();
            let traversal = execute_traversal(&context, scenario, &path).await;
            merge_correctness(&mut correctness, &traversal);
            scenarios.push(report_outcomes(
                scenario,
                "complete_cursor_traversal",
                1,
                &[traversal],
                started.elapsed(),
            ));
        }
    }

    for (phase, concurrency) in [
        ("moderate_concurrency", workload.moderate_concurrency),
        ("higher_concurrency", workload.higher_concurrency),
    ] {
        let (outcomes, elapsed) = run_mixed(
            &context,
            &manifest,
            &workload,
            options.limit_mode,
            workload.concurrent_samples,
            concurrency,
            options.scale_axis,
        )
        .await?;
        for outcome in &outcomes {
            merge_correctness(&mut correctness, outcome);
        }
        scenarios.push(report_named_outcomes(
            "mixed-interactive",
            phase,
            "mixed",
            concurrency,
            &outcomes,
            elapsed,
        ));
    }
    let (mixed, elapsed) = run_mixed(
        &context,
        &manifest,
        &workload,
        options.limit_mode,
        workload.mixed_samples,
        1,
        options.scale_axis,
    )
    .await?;
    for outcome in &mixed {
        merge_correctness(&mut correctness, outcome);
    }
    scenarios.push(report_named_outcomes(
        "weighted-mixed-interactive",
        "warm_deterministic_mix",
        "mixed",
        1,
        &mixed,
        elapsed,
    ));

    let mutation_outcomes = run_mutation_sequence(&context, &manifest).await;
    for outcome in &mutation_outcomes {
        merge_correctness(&mut correctness, outcome);
    }
    scenarios.push(report_named_outcomes(
        "object-and-relation-mutations",
        "mutations_last",
        "admin",
        1,
        &mutation_outcomes,
        Duration::from_secs_f64(
            mutation_outcomes
                .iter()
                .map(|outcome| outcome.latency_ms)
                .sum::<f64>()
                / 1_000.0,
        ),
    ));

    server.ensure_running()?;
    let after_metrics = fetch_metrics(&client, &base_url).await?;
    let backend_resources = backend
        .finish_resource_measurement(backend_resources_before)
        .await?;
    let resources = resource_report(&before_metrics, &after_metrics, backend_resources)?;
    drop(server);

    let lifecycle = if options.run_lifecycle {
        run_lifecycle(
            &options,
            &profile,
            &backend_environment,
            generation_ms,
            loading_ms,
        )
        .await
    } else {
        LifecycleReport {
            dataset_generation_ms: generation_ms,
            dataset_loading_ms: loading_ms,
            backup_generation_ms: None,
            backup_artifact_bytes: None,
            backup_logical_rows: None,
            backup_section_counts: BTreeMap::new(),
            offline_verification_ms: None,
            restore_ms: None,
            semantic_verification_ms: None,
            computed_rebuild_ms: None,
            outcome: "not_requested".to_string(),
            supported_ceiling_bytes: profile
                .provisioning
                .backup_max_output_bytes
                .min(profile.provisioning.restore_max_upload_bytes),
        }
    };
    if matches!(
        lifecycle.outcome.as_str(),
        "admin_binary_not_supplied"
            | "backup_failed"
            | "offline_verification_failed"
            | "isolated_restore_failed"
    ) {
        correctness.lifecycle_failures += 1;
    }
    let runtime = RuntimeIdentity {
        runner: runner_identity(),
        backend: preparation.identity,
        process_fresh: true,
        database_fresh: preparation.database_fresh,
        deliberate_warmup_requests: workload.warmup_requests,
    };
    ScaleBenchmarkReport::new(
        options.label,
        manifest,
        &workload,
        options.limit_mode,
        options.scale_axis,
        runtime,
        scenarios,
        correctness,
        resources,
        lifecycle,
    )
}

async fn wait_until_ready(
    client: &Client,
    base_url: &str,
    server: &mut ManagedServer,
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        server.ensure_running()?;
        if let Ok(response) = client.get(format!("{base_url}/readyz")).send().await
            && response.status() == StatusCode::OK
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(invalid_data(format!(
                "server did not become ready within {} seconds; log tail:\n{}",
                timeout.as_secs(),
                log_tail(&server.log_path)?
            )));
        }
        sleep(Duration::from_millis(100)).await;
    }
}

async fn login_principals(
    client: &Client,
    base_url: &str,
    principals: &[BenchmarkPrincipal],
) -> Result<BTreeMap<String, String>> {
    let mut tokens = BTreeMap::new();
    for principal in principals {
        let response = client
            .post(format!("{base_url}/api/v0/auth/login"))
            .json(&json!({
                "name": principal.username(),
                "password": principal.password()
            }))
            .send()
            .await?;
        if response.status() != StatusCode::OK {
            return Err(invalid_data(format!(
                "benchmark principal '{}' could not authenticate ({})",
                principal.role(),
                response.status()
            )));
        }
        let body = response.json::<Value>().await?;
        let token = body
            .get("token")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid_data("login response did not contain a token"))?;
        tokens.insert(principal.role().to_string(), token.to_string());
    }
    Ok(tokens)
}

async fn execute_request(
    context: &RequestContext,
    scenario: &WorkloadScenario,
    path: &str,
) -> RequestOutcome {
    let started = Instant::now();
    let Some(token) = context.tokens.get(&scenario.principal) else {
        return RequestOutcome {
            latency_ms: started.elapsed().as_secs_f64() * 1_000.0,
            success: false,
            ..RequestOutcome::default()
        };
    };
    let response = context
        .client
        .get(format!("{}{}", context.base_url, path))
        .bearer_auth(token)
        .send()
        .await;
    let allowed_sparse_collections = scenario
        .verify_sparse_visibility
        .then_some(&context.sparse_collection_ids);
    let mut outcome = response_outcome(response, started, allowed_sparse_collections).await;
    if scenario.verify_sparse_visibility {
        outcome.authorization_candidates = Some(context.sparse_candidate_count);
        outcome.authorized_rows = outcome.total;
    }
    outcome
}

async fn response_outcome(
    response: std::result::Result<reqwest::Response, reqwest::Error>,
    started: Instant,
    allowed_sparse_collections: Option<&BTreeSet<i64>>,
) -> RequestOutcome {
    let response = match response {
        Ok(response) => response,
        Err(error) => {
            return RequestOutcome {
                latency_ms: started.elapsed().as_secs_f64() * 1_000.0,
                success: false,
                timed_out: error.is_timeout(),
                ..RequestOutcome::default()
            };
        }
    };
    let status = response.status();
    let success = status.is_success();
    let total = response
        .headers()
        .get("X-Total-Count")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok());
    let body = match response.bytes().await {
        Ok(body) => body,
        Err(error) => {
            return RequestOutcome {
                latency_ms: started.elapsed().as_secs_f64() * 1_000.0,
                success: false,
                timed_out: error.is_timeout(),
                ..RequestOutcome::default()
            };
        }
    };
    let value = serde_json::from_slice::<Value>(&body).unwrap_or(Value::Null);
    let (items, ids, unauthorized_rows, graph_nodes, graph_edges) =
        inspect_response(&value, allowed_sparse_collections);
    let latency_ms = started.elapsed().as_secs_f64() * 1_000.0;
    RequestOutcome {
        latency_ms,
        success,
        timed_out: false,
        status: Some(status.as_u16()),
        bytes: body.len() as u64,
        items,
        graph_nodes,
        graph_edges,
        pages: 1,
        ids,
        total,
        unauthorized_rows,
        traversal_ms: None,
        ..RequestOutcome::default()
    }
}

fn inspect_response(
    value: &Value,
    allowed_sparse_collections: Option<&BTreeSet<i64>>,
) -> (u64, Vec<i64>, u64, Option<u64>, Option<u64>) {
    let values = match value {
        Value::Array(values) => values.as_slice(),
        Value::Object(_) => std::slice::from_ref(value),
        _ => &[],
    };
    let ids = values
        .iter()
        .filter_map(|value| value.get("id").and_then(Value::as_i64))
        .collect::<Vec<_>>();
    let unauthorized = if let Some(allowed) = allowed_sparse_collections {
        values
            .iter()
            .filter_map(|value| value.get("collection_id").and_then(Value::as_i64))
            .filter(|collection_id| !allowed.contains(collection_id))
            .count() as u64
    } else {
        0
    };
    let graph_nodes = value
        .get("objects")
        .or_else(|| value.get("classes"))
        .and_then(Value::as_array)
        .map(|nodes| nodes.len() as u64);
    let graph_edges = value
        .get("relations")
        .and_then(Value::as_array)
        .map(|edges| edges.len() as u64);
    (
        values.len() as u64,
        ids,
        unauthorized,
        graph_nodes,
        graph_edges,
    )
}

async fn execute_traversal(
    context: &RequestContext,
    scenario: &WorkloadScenario,
    path: &str,
) -> RequestOutcome {
    let started = Instant::now();
    let Some(token) = context.tokens.get(&scenario.principal) else {
        return RequestOutcome {
            success: false,
            ..RequestOutcome::default()
        };
    };
    let base_url = match Url::parse(&format!("{}{}", context.base_url, path)) {
        Ok(url) => url,
        Err(_) => {
            return RequestOutcome {
                success: false,
                ..RequestOutcome::default()
            };
        }
    };
    let mut outcome = RequestOutcome {
        success: true,
        ..RequestOutcome::default()
    };
    let mut next_cursor: Option<String> = None;
    for _ in 0..100_000 {
        let mut url = base_url.clone();
        if let Some(cursor) = next_cursor.as_deref() {
            url.query_pairs_mut().append_pair("cursor", cursor);
        }
        let request_started = Instant::now();
        let response = context.client.get(url).bearer_auth(token).send().await;
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                outcome.success = false;
                outcome.timed_out |= error.is_timeout();
                outcome
                    .page_latencies_ms
                    .push(request_started.elapsed().as_secs_f64() * 1_000.0);
                outcome.page_timeouts += u64::from(error.is_timeout());
                break;
            }
        };
        outcome.pages += 1;
        outcome.success &= response.status().is_success();
        outcome.status = Some(response.status().as_u16());
        outcome.page_statuses.push(response.status().as_u16());
        outcome.successful_pages += u64::from(response.status().is_success());
        if outcome.total.is_none() {
            outcome.total = response
                .headers()
                .get("X-Total-Count")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse().ok());
        }
        next_cursor = response
            .headers()
            .get("X-Next-Cursor")
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let body = match response.bytes().await {
            Ok(body) => body,
            Err(error) => {
                outcome.success = false;
                outcome.timed_out |= error.is_timeout();
                outcome.page_timeouts += u64::from(error.is_timeout());
                let page_latency = request_started.elapsed().as_secs_f64() * 1_000.0;
                outcome.latency_ms += page_latency;
                outcome.page_latencies_ms.push(page_latency);
                break;
            }
        };
        let page_latency = request_started.elapsed().as_secs_f64() * 1_000.0;
        outcome.latency_ms += page_latency;
        outcome.page_latencies_ms.push(page_latency);
        outcome.bytes += body.len() as u64;
        if let Ok(value) = serde_json::from_slice::<Value>(&body) {
            let allowed_sparse_collections = scenario
                .verify_sparse_visibility
                .then_some(&context.sparse_collection_ids);
            let (items, ids, unauthorized, graph_nodes, graph_edges) =
                inspect_response(&value, allowed_sparse_collections);
            outcome.items += items;
            outcome.ids.extend(ids);
            outcome.unauthorized_rows += unauthorized;
            outcome.graph_nodes = graph_nodes.or(outcome.graph_nodes);
            outcome.graph_edges = graph_edges.or(outcome.graph_edges);
        }
        if next_cursor.is_none() {
            break;
        }
    }
    if scenario.verify_sparse_visibility {
        outcome.authorization_candidates = Some(context.sparse_candidate_count);
        outcome.authorized_rows =
            Some(outcome.ids.iter().copied().collect::<BTreeSet<_>>().len() as u64);
    }
    outcome.traversal_ms = Some(started.elapsed().as_secs_f64() * 1_000.0);
    outcome
}

async fn run_mixed(
    context: &RequestContext,
    manifest: &DatasetManifest,
    workload: &WorkloadSpec,
    limit_mode: LimitMode,
    samples: usize,
    concurrency: usize,
    scale_axis: Option<ScaleAxis>,
) -> Result<(Vec<RequestOutcome>, Duration)> {
    let total_weight = workload
        .scenarios
        .iter()
        .filter(|scenario| scenario.include_in_mixed && scenario.applies_to(scale_axis))
        .map(|scenario| scenario.weight)
        .sum::<u64>();
    let mut state = workload.seed;
    let mut selected = Vec::with_capacity(samples);
    for _ in 0..samples {
        state = mix(state);
        let mut choice = state % total_weight;
        let scenario = workload
            .scenarios
            .iter()
            .filter(|scenario| scenario.include_in_mixed && scenario.applies_to(scale_axis))
            .find(|scenario| {
                if choice < scenario.weight {
                    true
                } else {
                    choice -= scenario.weight;
                    false
                }
            })
            .expect("weighted workload selection has a scenario");
        let path = workload.render_path(scenario, manifest, limit_mode)?;
        selected.push((scenario.clone(), path));
    }
    let started = Instant::now();
    let outcomes = stream::iter(selected)
        .map(|(scenario, path)| {
            let context = context.clone();
            async move { execute_request(&context, &scenario, &path).await }
        })
        .buffer_unordered(concurrency)
        .collect::<Vec<_>>()
        .await;
    Ok((outcomes, started.elapsed()))
}

async fn run_mutation_sequence(
    context: &RequestContext,
    manifest: &DatasetManifest,
) -> Vec<RequestOutcome> {
    let Some(token) = context.tokens.get("admin") else {
        return vec![RequestOutcome {
            success: false,
            ..RequestOutcome::default()
        }];
    };
    let class_id = manifest.anchors["hot_class_id"];
    let name = format!("scale-mutation-{}", std::process::id());
    let started = Instant::now();
    let create = context
        .client
        .post(format!("{}/api/v1/classes/{class_id}/", context.base_url))
        .bearer_auth(token)
        .json(&json!({"name": name, "data": {"mutation": true}, "description": "scale create"}))
        .send()
        .await;
    let mut create_outcome = RequestOutcome::default();
    let create = match create {
        Ok(response) => response,
        Err(error) => {
            create_outcome.latency_ms = started.elapsed().as_secs_f64() * 1_000.0;
            create_outcome.timed_out = error.is_timeout();
            return vec![create_outcome];
        }
    };
    create_outcome.success = create.status().is_success();
    create_outcome.status = Some(create.status().as_u16());
    let etag = create
        .headers()
        .get(header::ETAG)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let body = create.bytes().await.unwrap_or_default();
    create_outcome.latency_ms = started.elapsed().as_secs_f64() * 1_000.0;
    create_outcome.bytes = body.len() as u64;
    create_outcome.items = 1;
    let object_id = serde_json::from_slice::<Value>(&body)
        .ok()
        .and_then(|value| value.get("id").and_then(Value::as_i64));
    let (Some(object_id), Some(etag)) = (object_id, etag) else {
        create_outcome.success = false;
        return vec![create_outcome];
    };

    let started = Instant::now();
    let patch = context
        .client
        .patch(format!(
            "{}/api/v1/classes/{class_id}/{object_id}",
            context.base_url
        ))
        .bearer_auth(token)
        .header(header::IF_MATCH, etag)
        .json(&json!({"description": "scale update"}))
        .send()
        .await;
    let mut patch_outcome = RequestOutcome::default();
    let patch = match patch {
        Ok(response) => response,
        Err(error) => {
            patch_outcome.latency_ms = started.elapsed().as_secs_f64() * 1_000.0;
            patch_outcome.timed_out = error.is_timeout();
            return vec![create_outcome, patch_outcome];
        }
    };
    patch_outcome.success = patch.status().is_success();
    patch_outcome.status = Some(patch.status().as_u16());
    let etag = patch
        .headers()
        .get(header::ETAG)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let body = patch.bytes().await.unwrap_or_default();
    patch_outcome.latency_ms = started.elapsed().as_secs_f64() * 1_000.0;
    patch_outcome.bytes = body.len() as u64;
    patch_outcome.items = 1;
    let Some(object_etag) = etag else {
        patch_outcome.success = false;
        return vec![create_outcome, patch_outcome];
    };

    let secondary_class_id = manifest.anchors["secondary_hot_class_id"];
    let secondary_object_id = manifest.anchors["secondary_object_id"];
    let relation_path = format!(
        "{}/api/v1/classes/{class_id}/{object_id}/relations/{secondary_class_id}/{secondary_object_id}",
        context.base_url
    );
    let started = Instant::now();
    let relation_create = context
        .client
        .post(&relation_path)
        .bearer_auth(token)
        .send()
        .await;
    let mut relation_create_outcome = RequestOutcome::default();
    let relation_create = match relation_create {
        Ok(response) => response,
        Err(error) => {
            relation_create_outcome.latency_ms = started.elapsed().as_secs_f64() * 1_000.0;
            relation_create_outcome.timed_out = error.is_timeout();
            return vec![create_outcome, patch_outcome, relation_create_outcome];
        }
    };
    relation_create_outcome.success = relation_create.status().is_success();
    relation_create_outcome.status = Some(relation_create.status().as_u16());
    let relation_etag = relation_create
        .headers()
        .get(header::ETAG)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let body = relation_create.bytes().await.unwrap_or_default();
    relation_create_outcome.latency_ms = started.elapsed().as_secs_f64() * 1_000.0;
    relation_create_outcome.bytes = body.len() as u64;
    relation_create_outcome.items = 1;
    let Some(relation_etag) = relation_etag else {
        relation_create_outcome.success = false;
        return vec![create_outcome, patch_outcome, relation_create_outcome];
    };

    let started = Instant::now();
    let relation_delete = context
        .client
        .delete(&relation_path)
        .bearer_auth(token)
        .header(header::IF_MATCH, relation_etag)
        .send()
        .await;
    let mut relation_delete_outcome = RequestOutcome {
        latency_ms: started.elapsed().as_secs_f64() * 1_000.0,
        ..RequestOutcome::default()
    };
    match relation_delete {
        Ok(response) => {
            relation_delete_outcome.success = response.status().is_success();
            relation_delete_outcome.status = Some(response.status().as_u16());
            relation_delete_outcome.bytes = response.content_length().unwrap_or_default();
        }
        Err(error) => relation_delete_outcome.timed_out = error.is_timeout(),
    }

    let started = Instant::now();
    let delete = context
        .client
        .delete(format!(
            "{}/api/v1/classes/{class_id}/{object_id}",
            context.base_url
        ))
        .bearer_auth(token)
        .header(header::IF_MATCH, object_etag)
        .send()
        .await;
    let mut delete_outcome = RequestOutcome::default();
    match delete {
        Ok(response) => {
            delete_outcome.success = response.status().is_success();
            delete_outcome.status = Some(response.status().as_u16());
            delete_outcome.bytes = response.content_length().unwrap_or_default();
        }
        Err(error) => delete_outcome.timed_out = error.is_timeout(),
    }
    delete_outcome.latency_ms = started.elapsed().as_secs_f64() * 1_000.0;
    vec![
        create_outcome,
        patch_outcome,
        relation_create_outcome,
        relation_delete_outcome,
        delete_outcome,
    ]
}

fn report_outcomes(
    scenario: &WorkloadScenario,
    phase: &str,
    concurrency: usize,
    outcomes: &[RequestOutcome],
    elapsed: Duration,
) -> ScenarioReport {
    report_named_outcomes(
        &scenario.name,
        phase,
        &scenario.principal,
        concurrency,
        outcomes,
        elapsed,
    )
}

fn report_named_outcomes(
    name: &str,
    phase: &str,
    principal: &str,
    concurrency: usize,
    outcomes: &[RequestOutcome],
    elapsed: Duration,
) -> ScenarioReport {
    let latencies = outcomes
        .iter()
        .flat_map(|outcome| {
            if outcome.page_latencies_ms.is_empty() {
                vec![outcome.latency_ms]
            } else {
                outcome.page_latencies_ms.clone()
            }
        })
        .collect::<Vec<_>>();
    let requests = latencies.len() as u64;
    let successful_requests = outcomes
        .iter()
        .map(|outcome| {
            if outcome.page_latencies_ms.is_empty() {
                u64::from(outcome.success)
            } else {
                outcome.successful_pages
            }
        })
        .sum::<u64>();
    let timeouts = outcomes
        .iter()
        .map(|outcome| {
            if outcome.page_latencies_ms.is_empty() {
                u64::from(outcome.timed_out)
            } else {
                outcome.page_timeouts
            }
        })
        .sum::<u64>();
    let failures = requests.saturating_sub(successful_requests + timeouts);
    let mut status_counts = BTreeMap::new();
    for outcome in outcomes {
        if outcome.page_statuses.is_empty() {
            if let Some(status) = outcome.status {
                *status_counts.entry(status).or_insert(0) += 1;
            }
        } else {
            for status in &outcome.page_statuses {
                *status_counts.entry(*status).or_insert(0) += 1;
            }
        }
    }
    let duplicate_rows = outcomes
        .iter()
        .filter(|outcome| outcome.traversal_ms.is_some())
        .map(|outcome| {
            let unique = outcome.ids.iter().copied().collect::<BTreeSet<_>>();
            outcome.ids.len().saturating_sub(unique.len()) as u64
        })
        .sum();
    let missing_rows = outcomes
        .iter()
        .filter(|outcome| outcome.traversal_ms.is_some())
        .map(|outcome| {
            let unique = outcome.ids.iter().copied().collect::<BTreeSet<_>>();
            outcome
                .total
                .map(|total| total.saturating_sub(unique.len() as u64))
                .unwrap_or_default()
        })
        .sum();
    let elapsed_seconds = if elapsed.is_zero() {
        latencies.iter().sum::<f64>() / 1_000.0
    } else {
        elapsed.as_secs_f64()
    };
    ScenarioReport {
        name: name.to_string(),
        phase: phase.to_string(),
        principal: principal.to_string(),
        concurrency,
        requests,
        successful_requests,
        failures,
        timeouts,
        status_counts,
        requests_per_second: if elapsed_seconds > 0.0 {
            requests as f64 / elapsed_seconds
        } else {
            0.0
        },
        latency: LatencyDistribution::from_samples(&latencies),
        response_bytes: outcomes.iter().map(|outcome| outcome.bytes).sum(),
        response_items: outcomes.iter().map(|outcome| outcome.items).sum(),
        graph_nodes: outcomes
            .iter()
            .filter_map(|outcome| outcome.graph_nodes)
            .max(),
        graph_edges: outcomes
            .iter()
            .filter_map(|outcome| outcome.graph_edges)
            .max(),
        pages: outcomes.iter().map(|outcome| outcome.pages).sum(),
        traversal_ms: outcomes.iter().find_map(|outcome| outcome.traversal_ms),
        traversal_first_page_ms: outcomes
            .iter()
            .find_map(|outcome| outcome.page_latencies_ms.as_slice().first().copied()),
        traversal_middle_page_ms: outcomes.iter().find_map(|outcome| {
            (!outcome.page_latencies_ms.is_empty())
                .then(|| outcome.page_latencies_ms[outcome.page_latencies_ms.len() / 2])
        }),
        traversal_final_page_ms: outcomes
            .iter()
            .find_map(|outcome| outcome.page_latencies_ms.as_slice().last().copied()),
        duplicate_rows,
        missing_rows,
        unauthorized_rows: outcomes
            .iter()
            .map(|outcome| outcome.unauthorized_rows)
            .sum(),
        authorization_candidates: outcomes
            .iter()
            .filter_map(|outcome| outcome.authorization_candidates)
            .max(),
        authorized_rows: outcomes
            .iter()
            .filter_map(|outcome| outcome.authorized_rows)
            .max(),
    }
}

fn merge_correctness(report: &mut CorrectnessReport, outcome: &RequestOutcome) {
    if !outcome.success {
        report.request_failures += 1;
    }
    if outcome.traversal_ms.is_some() {
        let unique = outcome.ids.iter().copied().collect::<BTreeSet<_>>();
        report.traversal_duplicates += outcome.ids.len().saturating_sub(unique.len()) as u64;
        report.traversal_missing += outcome
            .total
            .map(|total| total.saturating_sub(unique.len() as u64))
            .unwrap_or_default();
    }
    report.unauthorized_rows += outcome.unauthorized_rows;
}

async fn fetch_metrics(client: &Client, base_url: &str) -> Result<MetricSnapshot> {
    let response = client.get(format!("{base_url}/metrics")).send().await?;
    if response.status() != StatusCode::OK {
        return Err(invalid_data(format!(
            "metrics endpoint returned {}",
            response.status()
        )));
    }
    MetricSnapshot::parse(&response.text().await?)
}

fn resource_report(
    before: &MetricSnapshot,
    after: &MetricSnapshot,
    backend: BackendResourceReport,
) -> Result<ResourceReport> {
    let mut storage_metric_deltas = BTreeMap::new();
    for metric in [
        "hubuum_storage_operation_duration_seconds_count",
        "hubuum_storage_operation_errors_total",
        "hubuum_db_operation_duration_seconds_count",
        "hubuum_db_operation_errors_total",
    ] {
        storage_metric_deltas.insert(
            metric.to_string(),
            before.total_counter_delta(after, metric)?,
        );
    }
    let mut pool_metric_deltas = BTreeMap::new();
    for metric in [
        "hubuum_db_connection_acquire_duration_seconds_count",
        "hubuum_db_connection_acquire_failures_total",
    ] {
        pool_metric_deltas.insert(
            metric.to_string(),
            before.total_counter_delta(after, metric)?,
        );
    }
    Ok(ResourceReport {
        application_cpu_seconds: after.value("process_cpu_seconds_total", &[])
            - before.value("process_cpu_seconds_total", &[]),
        peak_application_resident_bytes: before
            .value("process_resident_memory_bytes", &[])
            .max(after.value("process_resident_memory_bytes", &[]))
            .max(0.0) as u64,
        backend,
        storage_metric_deltas,
        pool_metric_deltas,
    })
}

async fn run_lifecycle(
    options: &MeasureOptions,
    profile: &ScaleProfile,
    backend_environment: &BTreeMap<String, String>,
    generation_ms: u64,
    loading_ms: u64,
) -> LifecycleReport {
    let supported_ceiling_bytes = profile
        .provisioning
        .backup_max_output_bytes
        .min(profile.provisioning.restore_max_upload_bytes);
    let mut report = LifecycleReport {
        dataset_generation_ms: generation_ms,
        dataset_loading_ms: loading_ms,
        backup_generation_ms: None,
        backup_artifact_bytes: None,
        backup_logical_rows: None,
        backup_section_counts: BTreeMap::new(),
        offline_verification_ms: None,
        restore_ms: None,
        semantic_verification_ms: None,
        computed_rebuild_ms: None,
        outcome: "not_run".to_string(),
        supported_ceiling_bytes,
    };
    let Some(admin_binary) = options.admin_binary.as_deref() else {
        report.outcome = "admin_binary_not_supplied".to_string();
        return report;
    };
    let backup_path = options.artifact_directory.join("dataset-backup.json");
    let started = Instant::now();
    let backup = admin_command(admin_binary, profile, backend_environment)
        .arg("--backup")
        .arg(&backup_path)
        .output();
    report.backup_generation_ms = Some(elapsed_ms(started));
    let backup = match backup {
        Ok(output) if output.status.success() => output,
        _ => {
            report.outcome = "backup_failed".to_string();
            return report;
        }
    };
    let _ = backup;
    report.backup_artifact_bytes = fs::metadata(&backup_path).ok().map(|meta| meta.len());
    if report
        .backup_artifact_bytes
        .is_some_and(|bytes| bytes > report.supported_ceiling_bytes)
    {
        report.outcome = "backup_exceeds_supported_ceiling".to_string();
        let _ = fs::remove_file(&backup_path);
        return report;
    }

    let started = Instant::now();
    let offline = admin_command(admin_binary, profile, backend_environment)
        .arg("--json")
        .arg("--verify-backup")
        .arg(&backup_path)
        .output();
    report.offline_verification_ms = Some(elapsed_ms(started));
    let offline = match offline {
        Ok(output) if output.status.success() => output,
        _ => {
            report.outcome = "offline_verification_failed".to_string();
            return report;
        }
    };
    if let Ok(value) = serde_json::from_slice::<Value>(&offline.stdout) {
        report.backup_logical_rows = find_u64(&value, "total_items");
        report.backup_section_counts = find_u64_map(&value, "section_counts");
    }
    let Some(restore_database_url) = &options.restore_test_database_url else {
        report.outcome = "backup_verified_offline".to_string();
        return report;
    };

    let started = Instant::now();
    let restore = admin_command(admin_binary, profile, backend_environment)
        .arg("--json")
        .arg("--verify-backup")
        .arg(&backup_path)
        .arg("--restore-test-database-url")
        .arg(restore_database_url)
        .output();
    let restore_and_verification_ms = elapsed_ms(started);
    let restore = match restore {
        Ok(output) if output.status.success() => output,
        _ => {
            report.outcome = "isolated_restore_failed".to_string();
            return report;
        }
    };
    if let Ok(value) = serde_json::from_slice::<Value>(&restore.stdout) {
        report.restore_ms = find_u64(&value, "restore_duration_ms");
    }
    report.semantic_verification_ms =
        Some(restore_and_verification_ms.saturating_sub(report.restore_ms.unwrap_or_default()));
    // The isolated verifier checks restored state against the source backup.
    // A successful result therefore has no pending computed-data rebuild.
    report.computed_rebuild_ms = Some(0);
    report.outcome = "backup_verified_and_restored".to_string();
    report
}

fn admin_command(
    binary: &Path,
    profile: &ScaleProfile,
    backend_environment: &BTreeMap<String, String>,
) -> Command {
    let mut command = Command::new(binary);
    command
        .env_clear()
        .env("HUBUUM_TOKEN_HASH_KEY", TOKEN_HASH_KEY)
        .env(
            "HUBUUM_BACKUP_MAX_OUTPUT_BYTES",
            profile.provisioning.backup_max_output_bytes.to_string(),
        )
        .env(
            "HUBUUM_RESTORE_MAX_UPLOAD_BYTES",
            profile.provisioning.restore_max_upload_bytes.to_string(),
        );
    command.envs(backend_environment);
    command
}

fn find_u64(value: &Value, key: &str) -> Option<u64> {
    match value {
        Value::Object(map) => map
            .get(key)
            .and_then(Value::as_u64)
            .or_else(|| map.values().find_map(|value| find_u64(value, key))),
        Value::Array(values) => values.iter().find_map(|value| find_u64(value, key)),
        _ => None,
    }
}

fn find_u64_map(value: &Value, key: &str) -> BTreeMap<String, u64> {
    match value {
        Value::Object(map) => map
            .get(key)
            .and_then(Value::as_object)
            .map(|values| {
                values
                    .iter()
                    .filter_map(|(name, value)| value.as_u64().map(|value| (name.clone(), value)))
                    .collect()
            })
            .or_else(|| {
                map.values()
                    .map(|value| find_u64_map(value, key))
                    .find(|values| !values.is_empty())
            })
            .unwrap_or_default(),
        Value::Array(values) => values
            .iter()
            .map(|value| find_u64_map(value, key))
            .find(|values| !values.is_empty())
            .unwrap_or_default(),
        _ => BTreeMap::new(),
    }
}

fn runner_identity() -> String {
    let os = std::env::consts::OS;
    let arch = std::env::consts::ARCH;
    let host = System::host_name().unwrap_or_else(|| "unknown-host".to_string());
    format!("{host}/{os}/{arch}")
}

fn log_tail(path: &Path) -> Result<String> {
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

fn mix(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().try_into().unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn graph_response_inspection_reports_returned_work_shape() {
        let value = json!({
            "objects": [{"id": 1}, {"id": 2}, {"id": 3}],
            "relations": [{"id": 10}, {"id": 11}],
        });

        let (items, ids, unauthorized, nodes, edges) = inspect_response(&value, None);

        assert_eq!(items, 1);
        assert!(ids.is_empty());
        assert_eq!(unauthorized, 0);
        assert_eq!(nodes, Some(3));
        assert_eq!(edges, Some(2));
    }
}
