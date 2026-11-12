//! Two independently limited processes share the real queue and signed repository.
use super::*;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Deserialize, Serialize)]
struct Plan {
    tasks: Vec<TaskId>,
    collection: i32,
    token: String,
}

pub(super) fn runtime_directory(report: &Path) -> PathBuf {
    report.with_extension("runtime")
}

fn report_path(report: &Path, index: usize) -> PathBuf {
    report.with_extension(format!("replica-{index}.json"))
}

async fn await_file(path: &Path) {
    tokio::time::timeout(std::time::Duration::from_secs(1800), async {
        while !path.is_file() {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "Replica did not finish its bounded phase: {}",
            path.display()
        )
    });
}

pub(super) async fn run_replicas(
    destination: &Path,
    root: &Path,
    tasks: &[TaskId],
    collection: i32,
    token: String,
    replicas: usize,
) -> Vec<Value> {
    let plan = root.join("plan.json");
    std::fs::write(
        &plan,
        serde_json::to_vec(&Plan {
            tasks: tasks.to_vec(),
            collection,
            token,
        })
        .unwrap(),
    )
    .unwrap();
    std::fs::set_permissions(&plan, std::fs::Permissions::from_mode(0o600)).unwrap();
    // The launcher starts replicas only after the ready marker is durable.
    std::fs::write(root.join("prepared"), []).unwrap();
    let mut reports = Vec::new();
    for index in 0..replicas {
        let path = report_path(destination, index);
        await_file(&path).await;
        reports.push(serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap());
    }
    reports
}

#[derive(Default)]
struct TaskObservations {
    statuses: Vec<StorageTaskStatus>,
    oversized_source_rows: u64,
    max_scanned_source_rows: u64,
}

async fn task_observations(
    storage: &(impl HistoryLifecycleStorage + ExecutionStorage + TaskQueueStorage),
    scope: &StorageExecutionScope,
    tasks: &[TaskId],
) -> TaskObservations {
    storage
        .run_in_scope(scope.clone(), async {
            let owner = storage
                .admit_history_publication(HistoryPublicationKind::TaskOutput)
                .await
                .unwrap();
            let result = storage
                .run_in_scope(scope.clone().with_history_read(owner.context()), async {
                    let mut observations = TaskObservations::default();
                    for id in tasks {
                        let task = storage.get_task_access(*id).await.unwrap().into_parts().0;
                        if task.status().is_terminal()
                            && task.status() != StorageTaskStatus::Succeeded
                        {
                            eprintln!(
                                "Shared task {id:?} ended {:?}: {:?}",
                                task.status(),
                                task.summary()
                            );
                        }
                        observations.statuses.push(task.status());
                        let events = storage
                            .list_task_events(StorageTaskChildListQuery::new(
                                *id,
                                QueryOptions::new(vec![], vec![], Some(100), None, false).unwrap(),
                            ))
                            .await
                            .unwrap()
                            .into_parts()
                            .0;
                        observations.oversized_source_rows += events
                            .iter()
                            .filter_map(|event| {
                                event.data()?.get("oversized_source_rows")?.as_u64()
                            })
                            .sum::<u64>();
                        observations.max_scanned_source_rows =
                            observations.max_scanned_source_rows.max(
                                events
                                    .iter()
                                    .filter_map(|event| event.data()?.get("scanned")?.as_u64())
                                    .max()
                                    .unwrap_or(0),
                            );
                    }
                    observations
                })
                .await;
            storage
                .settle_history_publication(
                    owner
                        .after_drained(std::future::ready(Ok(())), std::future::ready(Ok(())))
                        .await
                        .unwrap(),
                )
                .await
                .unwrap();
            result
        })
        .await
}

#[actix_web::test]
#[ignore = "explicit Linux shared-process resource and slow-consumer qualification"]
async fn qualified_history_expiry_replica() {
    assert_eq!(std::env::consts::OS, "linux");
    let destination =
        PathBuf::from(std::env::var("HUBUUM_TEST_HISTORY_QUALIFICATION_REPORT").unwrap());
    let root = runtime_directory(&destination);
    let index: usize = std::env::var("HUBUUM_TEST_HISTORY_QUALIFICATION_REPLICA")
        .unwrap()
        .parse()
        .unwrap();
    assert!(index < 2);
    let slots: usize = std::env::var("HUBUUM_TEST_HISTORY_QUALIFICATION_SLOTS")
        .unwrap()
        .parse()
        .unwrap();
    assert!(matches!(slots, 1 | 2));
    let plan: Plan =
        serde_json::from_slice(&std::fs::read(root.join("plan.json")).unwrap()).unwrap();
    // Combined pool capacity is eight per process, including registration/control.
    let control_pool = postgres_test_pool_with_timeout(&database_url(), 2, 0);
    let control = hubuum::test_support::history_expiry_qualification_storage(control_pool.clone());
    let registration = control.register_history_runtime(None).await.unwrap();
    let runtime = hubuum::test_support::history_expiry_qualification_storage(
        postgres_test_pool_with_timeout(&runtime_database_url(), 6, 0),
    );
    let options = serde_json::from_value(
        json!({"directory":root.join("repository"), "key":root.join("key")}),
    )
    .unwrap();
    let (storage, context) = hubuum::test_support::history_expiry_qualification_context(
        runtime,
        registration.owner(),
        options,
    );
    let application = storage
        .capture_application_execution(Some(registration.owner()))
        .await
        .unwrap();
    let scope = StorageExecutionScope::default().with_application_access(application);
    let (server, address) = hubuum::test_support::start_history_expiry_qualification_api(&context)
        .await
        .unwrap();
    let handle = server.handle();
    let server = actix_web::rt::spawn(server);
    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.set_recv_buffer_size(1024).unwrap();
    let mut slow = socket.connect(address).await.unwrap();
    slow.write_all(
        format!(
            "GET /api/v1/collections/{} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {}\r\nConnection: close\r\n\r\n",
            plan.collection, plan.token
        )
        .as_bytes(),
    )
    .await
    .unwrap();
    let headers = tokio::time::timeout(std::time::Duration::from_secs(60), async {
        let mut headers = Vec::new();
        while !headers.ends_with(b"\r\n\r\n") {
            assert!(headers.len() < 8192);
            headers.push(slow.read_u8().await.unwrap());
        }
        headers
    })
    .await
    .expect("The real HTTP endpoint must start its large response");
    let headers = String::from_utf8(headers).unwrap();
    assert!(headers.starts_with("HTTP/1.1 200"), "{headers}");
    assert!(
        headers.lines().any(|line| line
            .to_ascii_lowercase()
            .strip_prefix("content-length:")
            .and_then(|length| length.trim().parse::<usize>().ok())
            .is_some_and(|length| length >= 4 * 1024 * 1024 - 8192)),
        "Slow-consumer fixture must reach the real large response: {headers}"
    );
    std::fs::write(root.join(format!("ready-{index}")), []).unwrap();
    await_file(&root.join("start")).await;
    let started = Instant::now();
    let mut slow = Some(slow);
    let mut slow_consumer_blocked = false;
    let mut claimed_iterations = 0;
    // Hold each process at its declared slots before another process can take
    // more queue turns. Every dispatch still uses the production claim path.
    for result in futures_util::future::join_all(
        (0..slots).map(|_| hubuum::test_support::qualify_history_expiry_iteration(&context)),
    )
    .await
    {
        claimed_iterations += u64::from(result.unwrap());
    }
    assert_eq!(claimed_iterations, slots as u64);
    std::fs::write(root.join(format!("claimed-{index}")), []).unwrap();
    let replicas: usize = std::env::var("HUBUUM_TEST_HISTORY_QUALIFICATION_REPLICAS")
        .unwrap()
        .parse()
        .unwrap();
    for replica in 0..replicas {
        await_file(&root.join(format!("claimed-{replica}"))).await;
    }
    let mut database_round_trip_us = Vec::new();
    let mut max_scanned_source_rows = 0;
    let oversized_reported_rows = loop {
        for result in futures_util::future::join_all(
            (0..slots).map(|_| hubuum::test_support::qualify_history_expiry_iteration(&context)),
        )
        .await
        {
            claimed_iterations += u64::from(result.unwrap());
        }
        if slow.is_some() && started.elapsed() >= std::time::Duration::from_secs(3) {
            let snapshot = control.capture_history_lifecycle().await.unwrap();
            let intents = snapshot.control().authority().expiry().intents();
            assert!(
                !intents.is_empty(),
                "No native admission during slow response"
            );
            assert!(
                intents.iter().all(|intent| intent.removals().is_empty()),
                "A slow pre-exclusion HTTP response must prevent physical removal"
            );
            slow_consumer_blocked = true;
            drop(slow.take());
        }
        let observations = task_observations(&storage, &scope, &plan.tasks).await;
        max_scanned_source_rows = max_scanned_source_rows.max(observations.max_scanned_source_rows);
        let statuses = observations.statuses;
        assert!(
            statuses
                .iter()
                .all(|status| !status.is_terminal() || *status == StorageTaskStatus::Succeeded),
            "A shared worker failed: {statuses:?}"
        );
        let probe = Instant::now();
        with_connection(&control_pool, async |connection| {
            diesel::sql_query("SELECT 1").execute(connection).await
        })
        .await
        .unwrap();
        database_round_trip_us.push(u64::try_from(probe.elapsed().as_micros()).unwrap());
        let retained = control.capture_history_lifecycle().await.unwrap();
        let all_receipted = retained
            .control()
            .authority()
            .expiry()
            .intents()
            .iter()
            .all(|intent| intent.removals().len() == intent.manifest().members().len());
        if statuses.iter().all(|status| status.is_terminal()) && all_receipted {
            assert!(
                statuses
                    .iter()
                    .all(|status| *status == StorageTaskStatus::Succeeded),
                "Shared worker outcomes: {statuses:?}"
            );
            break observations.oversized_source_rows;
        }
        assert!(
            started.elapsed() < std::time::Duration::from_secs(1800),
            "Shared workers did not finish: {statuses:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    };
    assert!(slow_consumer_blocked);
    assert!(
        claimed_iterations > 0,
        "Both replicas must execute real queued work"
    );
    handle.stop(true).await;
    server.await.unwrap().unwrap();
    storage.unregister_runtime(registration).await.unwrap();
    let report = json!({"replica":index, "slots":slots, "initial_claimed_slots":slots, "http_workers":2,
        "claimed_iterations":claimed_iterations, "slow_consumer_blocked":slow_consumer_blocked,
        "slow_consumer_retired":true, "oversized_reported_rows":oversized_reported_rows,
        "max_scanned_source_rows":max_scanned_source_rows,
        "worker_seconds":started.elapsed().as_secs_f64(),
        "database_round_trip_us":database_round_trip_us,
        "kernel_peak_rss_bytes":high_water(libc::RUSAGE_SELF),
        "kernel_child_peak_rss_bytes":high_water(libc::RUSAGE_CHILDREN)});
    let path = report_path(&destination, index);
    let pending = path.with_extension("pending");
    std::fs::write(&pending, serde_json::to_vec(&report).unwrap()).unwrap();
    std::fs::rename(pending, path).unwrap();
}
