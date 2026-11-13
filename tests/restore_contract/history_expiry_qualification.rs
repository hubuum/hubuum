//! Explicit process-level H1/H4 driver. Ordinary suites never run this fixture.
use super::*;
use chrono::Utc;
use diesel::QueryableByName;
use diesel::sql_types::{BigInt, Text};
use hubuum_domain::PrincipalIdentity;
use hubuum_storage_postgres::{PostgresPool, with_connection};
use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
use std::time::Instant;

mod replica;

#[derive(QueryableByName)]
struct NativeInput {
    #[diesel(sql_type = Text)]
    version: String,
    #[diesel(sql_type = BigInt)]
    input_bytes: i64,
}

async fn native_input(pool: &PostgresPool, resource: ResourceIdentity) -> BTreeMap<String, u64> {
    with_connection(pool, async |connection| {
        diesel::sql_query("SELECT logical_identity #>> '{subject,version}' AS version,octet_length(to_jsonb(h.*)::text)::bigint AS input_bytes FROM public.collections_history h WHERE logical_identity #>> '{subject,identity,identity}'=$1 AND valid_to IS NOT NULL")
            .bind::<Text,_>(resource.to_string()).load::<NativeInput>(connection).await
    }).await.unwrap().into_iter().map(|row| (row.version, u64::try_from(row.input_bytes).unwrap())).collect()
}

fn high_water(who: libc::c_int) -> u64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes the valid out-pointer on success. Linux
    // reports ru_maxrss in KiB. Only SELF/CHILDREN constants are used below.
    assert_eq!(unsafe { libc::getrusage(who, usage.as_mut_ptr()) }, 0);
    u64::try_from(unsafe { usage.assume_init() }.ru_maxrss).unwrap() * 1024
}

#[tokio::test]
#[ignore = "explicit Linux process qualification: 512 MiB, two CPUs, fresh isolated PostgreSQL"]
async fn qualified_history_expiry_worker() {
    assert_eq!(
        std::env::consts::OS,
        "linux",
        "Resource qualification requires Linux kernel high-water evidence"
    );
    measure().await;
}

async fn measure() {
    let _guard = RESTORE_TEST_LOCK.lock().await;
    let preparing = Instant::now();
    let shape = std::env::var("HUBUUM_TEST_HISTORY_QUALIFICATION_SHAPE").unwrap();
    let slots: usize = std::env::var("HUBUUM_TEST_HISTORY_QUALIFICATION_SLOTS")
        .unwrap()
        .parse()
        .unwrap();
    assert!(matches!(slots, 1 | 2));
    let replicas: usize = std::env::var("HUBUUM_TEST_HISTORY_QUALIFICATION_REPLICAS")
        .unwrap_or_else(|_| "1".into())
        .parse()
        .unwrap();
    assert!(matches!(replicas, 1 | 2));
    let (updates, payload_bytes) = match shape.as_str() {
        "rows" => (1001, 128),
        // Reach a full byte-limited batch plus a continuation without making
        // four scopes exceed the supported 256 MiB full-history backup budget.
        "bytes" => (513, 32 * 1024),
        "large_row" => (4, 4 * 1024 * 1024 - 8192),
        "oversized" => (2, 17 * 1024 * 1024),
        _ => panic!("Unknown qualification shape"),
    };
    let pool = postgres_test_pool_with_timeout(&database_url(), 2, 0);
    let fixture = RestoreContractFixture::new(StorageBackendKind::Postgres, pool.clone())
        .await
        .unwrap();
    let storage = fixture.storage();
    let destination = std::path::PathBuf::from(
        std::env::var("HUBUUM_TEST_HISTORY_QUALIFICATION_REPORT").unwrap(),
    );
    let root = replica::runtime_directory(&destination);
    let repository = root.join("repository");
    std::fs::create_dir_all(&repository).unwrap();
    std::fs::set_permissions(&repository, std::fs::Permissions::from_mode(0o700)).unwrap();
    let signing_key = root.join("key");
    std::fs::write(&signing_key, [91; 32]).unwrap();
    std::fs::set_permissions(&signing_key, std::fs::Permissions::from_mode(0o600)).unwrap();
    fixture
        .initialize_recovery(&repository, &signing_key)
        .await
        .unwrap();
    let policy = storage.task_policy().await.unwrap();
    let capacity = policy
        .policy()
        .for_kind(StorageTaskKind::HistoryExpiry)
        .capacity();
    storage
        .replace_task_policy(StorageTaskPolicyUpdate::new(
            policy.revision(),
            policy.policy().clone().with_kind(TaskKindCapacity::new(
                StorageTaskKind::HistoryExpiry,
                TaskScopeCapacity::new(
                    capacity.outstanding(),
                    TaskCapacityLimit::new((slots * replicas) as u64).unwrap(),
                    capacity.resident_bytes(),
                ),
                TaskCapacityLimit::new(8).unwrap(),
            )),
        ))
        .await
        .unwrap();
    let mut sources = Vec::new();
    for slot in 0..slots * replicas {
        let collection = fixture
            .create_collection(
                &format!("expiry_qualification_{}_{}", Uuid::new_v4(), slot),
                "initial",
            )
            .await
            .unwrap();
        let resource = storage
            .resolve_logical_identity(StorageIdentityTarget::Resource {
                kind: StorageIdentityResourceKind::Collection,
                id: collection.id().into(),
            })
            .await
            .unwrap()
            .subject()
            .and_then(StorageIdentitySubject::record_identity)
            .unwrap()
            .resource();
        let key = HistoryPolicyKey::new(
            StorageIdentityHistoryKind::Collection,
            HistoryPolicyScope::Collection { identity: resource },
        )
        .unwrap();
        let snapshot = storage.capture_history_lifecycle().await.unwrap();
        let proposal = HistoryPolicyChange::new(
            key,
            snapshot
                .control()
                .authority()
                .policies()
                .current_revision(key),
            HistoryPolicyBinding::Expire {
                trigger: HistoryExpiryTrigger::Age {
                    age_seconds: HistoryRetentionSeconds::new(0).unwrap(),
                    deleted_age_seconds: None,
                },
                minimum_age_seconds: HistoryRetentionSeconds::new(0).unwrap(),
            },
            HistoryPolicyApplication::Prospective,
        )
        .unwrap();
        storage
            .commit_history_lifecycle(
                snapshot
                    .control()
                    .apply(
                        HistoryPolicyApplyRequest::new(proposal, None),
                        PrincipalIdentity::generate(),
                        snapshot.fence(),
                        snapshot.observed_at(),
                    )
                    .unwrap(),
            )
            .await
            .unwrap();
        let payload = "x".repeat(payload_bytes);
        for update in 0..updates {
            fixture
                .update_collection_description(
                    collection.id(),
                    if (shape == "oversized" && update == 1)
                        || (shape == "large_row" && update % 2 == 1)
                    {
                        "closed oversized source".into()
                    } else {
                        format!("{update}:{payload}")
                    },
                )
                .await
                .unwrap();
        }
        sources.push((key, native_input(&pool, resource).await));
    }
    let slow_consumer = {
        let admin = hubuum::tests::create_test_admin(&pool).await;
        let token = admin.create_token(&pool).await.unwrap().get_token();
        let collection = fixture
            .create_collection(
                &format!("slow_consumer_{}", Uuid::new_v4()),
                &"s".repeat(4 * 1024 * 1024 - 8192),
            )
            .await
            .unwrap();
        (collection.id().id(), token)
    };
    let backup = if shape == "oversized" {
        assert!(
            matches!(
                fixture.capture(true).await,
                Err(hubuum::errors::ApiError::PayloadTooLarge(_))
            ),
            "An unsupported source row must fail backup before publication"
        );
        None
    } else {
        Some(fixture.capture(true).await.unwrap())
    };
    let registration = storage.register_history_runtime(None).await.unwrap();
    let runtime = fixture.runtime_read_storage(postgres_test_pool_with_timeout(
        &runtime_database_url(),
        6,
        0,
    ));
    let options =
        serde_json::from_value(json!({"directory":repository,"key":signing_key})).unwrap();
    let (bound, _) = hubuum::test_support::history_expiry_qualification_context(
        runtime,
        registration.owner(),
        options,
    );
    let application = bound
        .capture_application_execution(Some(registration.owner()))
        .await
        .unwrap();
    let fence = application.fence();
    let scope = StorageExecutionScope::default().with_application_access(application);
    let mut tasks = Vec::new();
    bound
        .run_in_scope(scope.clone(), async {
            for (key, _) in &sources {
                let snapshot = bound.capture_history_lifecycle().await.unwrap();
                let policy = snapshot
                    .control()
                    .authority()
                    .policies()
                    .records()
                    .iter()
                    .rev()
                    .find(|record| record.proposal().key() == *key)
                    .unwrap();
                let submission = HistoryExpiryTaskSubmission::periodic(
                    HistoryLifecycleScan::new(*key, 1000).unwrap(),
                    fence,
                    policy,
                )
                .unwrap();
                tasks.push(
                    bound
                        .submit_history_expiry_task(submission)
                        .await
                        .unwrap()
                        .task()
                        .id(),
                );
            }
        })
        .await;
    bound.unregister_runtime(registration).await.unwrap();
    let fixture_seconds = preparing.elapsed().as_secs_f64();
    eprintln!("Qualification stage: shape={shape} fixture_seconds={fixture_seconds}");
    let started = Instant::now();
    let replica_reports = replica::run_replicas(
        &destination,
        &root,
        &tasks,
        slow_consumer.0,
        slow_consumer.1,
        replicas,
        storage,
    )
    .await;
    let worker_seconds = started.elapsed().as_secs_f64();
    eprintln!("Qualification stage: shape={shape} worker_seconds={worker_seconds}");
    let oversized_reported_rows = replica_reports[0]["oversized_reported_rows"]
        .as_u64()
        .unwrap();
    let completed = storage.capture_history_lifecycle().await.unwrap();
    let mut batches = Vec::new();
    let mut oversized_deferred_rows = 0;
    let mut oversized_preserved_rows = 0;
    for (key, input) in &sources {
        let intents = completed
            .control()
            .authority()
            .expiry()
            .intents()
            .iter()
            .filter(|intent| intent.manifest().key() == *key);
        // Periodic production may admit another finite batch while a different
        // scope waits. Account for every admitted batch, independent of UUID order.
        for intent in intents {
            let native_input_bytes: u64 = intent
                .manifest()
                .members()
                .iter()
                .map(|member| input[&member.target().version().to_string()])
                .sum();
            let largest_claimed_row_bytes = intent
                .manifest()
                .members()
                .iter()
                .map(|member| input[&member.target().version().to_string()])
                .max()
                .unwrap();
            batches.push(json!({"rows":intent.manifest().members().len(), "input_bytes":intent.manifest().input_bytes(),
                "largest_claimed_row_bytes":largest_claimed_row_bytes,
                "native_input_bytes":native_input_bytes, "removed_rows":intent.removals().len()}));
        }
        oversized_deferred_rows += input
            .values()
            .filter(|bytes| **bytes > 16 * 1024 * 1024)
            .count();
        if shape == "oversized" {
            let HistoryPolicyScope::Collection { identity } = key.scope() else {
                unreachable!("fixture collection scope")
            };
            let remaining = native_input(&pool, identity).await;
            for (version, bytes) in input.iter().filter(|(_, bytes)| **bytes > 16 * 1024 * 1024) {
                assert_eq!(
                    remaining.get(version),
                    Some(bytes),
                    "Oversized rows must survive the worker unchanged"
                );
                oversized_preserved_rows += 1;
            }
        }
    }
    let receipts = completed.control().authority().expiry().clone();
    let replay_started = Instant::now();
    let (replay_preserved_receipts, quarantine_released) = if let Some(backup) = backup {
        fixture
            .restore_with_recovery(backup, &repository, &signing_key)
            .await
            .unwrap();
        let replayed = storage.capture_history_lifecycle().await.unwrap();
        (
            Some(replayed.control().authority().expiry() == &receipts),
            Some(
                storage
                    .read_application_control()
                    .await
                    .unwrap()
                    .is_released_for(&storage.read_deletion_control().await.unwrap()),
            ),
        )
    } else {
        (None, None)
    };
    let replay_seconds = replay_started.elapsed().as_secs_f64();
    eprintln!("Qualification stage: shape={shape} replay_seconds={replay_seconds}");
    let report = json!({"shape":shape, "slots":slots, "replicas":replicas, "measured_at":Utc::now(), "batches":batches,
        "fixture_seconds":fixture_seconds, "worker_seconds":worker_seconds, "replay_seconds":replay_seconds,
        "oversized_deferred_rows":oversized_deferred_rows,
        "oversized_preserved_rows":oversized_preserved_rows, "backup_rejected_oversized":shape == "oversized",
        "oversized_reported_rows":oversized_reported_rows,
        "max_scanned_source_rows":replica_reports.iter().map(|report| report["max_scanned_source_rows"].as_u64().unwrap()).max().unwrap(),
        "kernel_peak_rss_bytes":replica_reports.iter().map(|report| report["kernel_peak_rss_bytes"].as_u64().unwrap()).max().unwrap(),
        "kernel_child_peak_rss_bytes":high_water(libc::RUSAGE_CHILDREN), "replica_reports":replica_reports,
        "replay_preserved_receipts":replay_preserved_receipts, "quarantine_released":quarantine_released});
    std::fs::write(destination, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
