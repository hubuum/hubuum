use super::*;
use futures_util::TryStreamExt;
use hubuum_domain::PrincipalIdentity;

#[derive(QueryableByName)]
struct AccountingRow {
    #[diesel(sql_type = Timestamptz)]
    valid_from: DateTime<Utc>,
    #[diesel(sql_type = Nullable<Timestamptz>)]
    valid_to: Option<DateTime<Utc>>,
    #[diesel(sql_type = Nullable<Jsonb>)]
    logical_identity: Option<Value>,
    #[diesel(sql_type = Bool)]
    deleted: bool,
    #[diesel(sql_type = BigInt)]
    logical_bytes: i64,
    #[diesel(sql_type = BigInt)]
    metadata_bytes: i64,
}

pub(super) async fn observe(
    storage: &PostgresStorage,
    policy: HistoryPolicyReference,
    actor: PrincipalIdentity,
) -> Result<HistorySizeObservation, PostgresStorageError> {
    let before = storage.capture_history_lifecycle().await?;
    let fallback = StorageExecutionPermit::new(
        None,
        ApplicationFence::new(
            before.fence().incarnation(),
            before.fence().deletion_revision().value() as i64,
        )?,
    );
    storage
        .runtime()
        .with_history_control_transaction(fallback, async move |connection| {
            observe_on(connection, policy, actor).await
        })
        .await
}

// Caller owns the exclusive history control gate in production lock order.
async fn observe_on(
    connection: &mut PostgresConnection,
    policy: HistoryPolicyReference,
    actor: PrincipalIdentity,
) -> Result<HistorySizeObservation, PostgresStorageError> {
    // The exclusive history control lock serializes refreshes and prevents
    // ordinary history writes/physical expiry while the census is folded.
    let snapshot = capture_on(connection).await?;
    let mut census = HistorySizeCensus::new(snapshot.clone(), &policy)?;
    let family = policy.key().family();
    let (table, position) = expiry::source(family);
    let (from, until) = if family == StorageIdentityHistoryKind::ClassSchema {
        ("occurred_at", "NULL::timestamptz")
    } else {
        ("valid_from", "valid_to")
    };
    let deleted = if matches!(
        family,
        StorageIdentityHistoryKind::ClassSchema | StorageIdentityHistoryKind::ComputedDefinitions
    ) {
        "FALSE"
    } else {
        "op='D'"
    };
    sql_query("SELECT set_config('statement_timeout', CASE WHEN current_setting('statement_timeout')::INTERVAL=INTERVAL '0' THEN '5000' ELSE GREATEST(1,FLOOR(LEAST(EXTRACT(EPOCH FROM current_setting('statement_timeout')::INTERVAL)*1000,5000)))::BIGINT::TEXT END,true)")
            .execute(connection).await?;
    // Only identity metadata crosses the adapter boundary. All retained rows
    // count, including open, held, dependency-protected and excluded copies.
    let query = format!(
        "SELECT {from} AS valid_from,{until} AS valid_to,CASE WHEN octet_length(logical_identity::text)<=1048576 THEN logical_identity END AS logical_identity,{deleted} AS deleted,public.hubuum_history_logical_bytes_v1(to_jsonb(h.*)) AS logical_bytes,coalesce(octet_length(logical_identity::text),0)::bigint AS metadata_bytes FROM public.{table} h ORDER BY {position} LIMIT 100001"
    );
    {
        let mut rows = sql_query(query)
            .load_stream::<AccountingRow>(connection)
            .await?;
        while let Some(row) = rows.try_next().await? {
            let raw = row.logical_identity.ok_or_else(|| {
                PostgresStorageError::unavailable(
                    "History accounting has unresolved or oversized identity",
                )
            })?;
            let evidence: StorageIdentityEvidence = serde_json::from_value(raw).map_err(|_| {
                PostgresStorageError::unavailable("History accounting has invalid identity")
            })?;
            let anchor = if family == StorageIdentityHistoryKind::ClassSchema {
                Some(HistoryRetentionAnchor::SchemaEvent(row.valid_from))
            } else if row.deleted {
                Some(HistoryRetentionAnchor::Deleted(row.valid_from))
            } else {
                row.valid_to.map(HistoryRetentionAnchor::Closed)
            };
            let candidate =
                HistoryExpiryCandidate::from_evidence(family, &evidence, anchor, false)?;
            let bytes =
                HistoryLogicalBytes::new(u64::try_from(row.logical_bytes).map_err(|_| {
                    PostgresStorageError::unavailable("Invalid logical history bytes")
                })?)?;
            census
                .observe(candidate.as_ref(), bytes, row.metadata_bytes as u64)
                .map_err(|error| PostgresStorageError::unavailable(error.to_string()))?;
        }
    }
    let mut accounting = census.finish()?;
    let change = snapshot
        .control()
        .observe_size(&mut accounting, actor)
        .map_err(|error| PostgresStorageError::unavailable(error.to_string()))?;
    let applied = commit_on(connection, change).await?;
    Ok::<_, PostgresStorageError>(HistorySizeObservation::new(accounting, applied))
}

#[cfg(all(test, feature = "integration-test-support"))]
mod tests {
    use super::*;
    use crate::test_support::integration_test_pool;

    use diesel_async::SimpleAsyncConnection;

    #[tokio::test]
    async fn native_refresh_retains_one_pressure_transition_and_noop_is_exact() {
        let backend = PostgresStorage::unobserved(integration_test_pool(1));
        let before = backend.capture_history_lifecycle().await.unwrap();
        let fallback = StorageExecutionPermit::new(
            None,
            ApplicationFence::new(
                before.fence().incarnation(),
                before.fence().deletion_revision().value() as i64,
            )
            .unwrap(),
        );
        backend.runtime().with_history_control_transaction(fallback, async |connection| {
            connection.batch_execute("SAVEPOINT size_accounting_fixture").await?;
            let snapshot = capture_on(connection).await?;
            let actor = PrincipalIdentity::generate();
            let proposal = HistoryPolicyChange::new(HistoryPolicyKey::new(StorageIdentityHistoryKind::ClassSchema, HistoryPolicyScope::Deployment)?, HistoryPolicyRevision::new(0)?, HistoryPolicyBinding::Expire {
                trigger: HistoryExpiryTrigger::LogicalSize { upper_logical_bytes: HistoryLogicalBytes::new(1)?, lower_logical_bytes: HistoryLogicalBytes::new(0)? }, minimum_age_seconds: HistoryRetentionSeconds::new(0)?,
            }, HistoryPolicyApplication::Prospective)?;
            let applied = commit_on(connection, snapshot.control().apply(HistoryPolicyApplyRequest::new(proposal, None), actor, snapshot.fence(), snapshot.observed_at() - chrono::Duration::seconds(1)).map_err(|_| PostgresStorageError::invalid_input("Invalid size accounting test proposal"))?).await?;
            let policy = HistoryPolicyReference::from(&applied.control().authority().policies().records()[0]);
            sql_query("INSERT INTO public.hubuumclass(name,description,collection_id) SELECT $1,'',id FROM public.collections WHERE parent_collection_id IS NULL")
                .bind::<Text, _>(format!("size_accounting_{}", Uuid::new_v4().simple())).execute(connection).await?;
            let (first, started) = observe_on(connection, policy.clone(), actor).await?.into_parts();
            assert!(first.pressure().reclaiming());
            assert!(first.transition().is_some());
            let (repeated, after) = observe_on(connection, policy, actor).await?.into_parts();
            assert!(repeated.transition().is_none());
            assert_eq!(after.control(), started.control());
            assert_eq!(after.control().authority().size_transitions().len(), 1);
            connection.batch_execute("ROLLBACK TO SAVEPOINT size_accounting_fixture").await?;
            Ok::<_, PostgresStorageError>(())
        }).await.unwrap();
    }
    #[derive(QueryableByName)]
    struct Measured {
        #[diesel(sql_type = BigInt)]
        bytes: i64,
    }

    #[rstest::rstest]
    #[case("{\"x\": [1.0000, 1e3, 1e-7, -0.000, -12.3400, null, false]}")]
    #[case("{\"🦀\": \"\\u0001\\b\\t\\n\\f\\r\\\"\\\\/æøå\"}")]
    #[case("[[],{},true,\"\"]")]
    #[tokio::test]
    async fn native_logical_measure_matches_the_shared_v1_format(#[case] raw: &str) {
        let value: Value = serde_json::from_str(raw).unwrap();
        let expected = HistoryLogicalBytes::measure_json_v1(&value)
            .unwrap()
            .value();
        let pool = integration_test_pool(1);
        let actual = crate::with_connection(&pool, async |connection| {
            sql_query("SELECT public.hubuum_history_logical_bytes_v1($1) AS bytes")
                .bind::<Jsonb, _>(value)
                .get_result::<Measured>(connection)
                .await
                .map_err(PostgresStorageError::from)
        })
        .await
        .unwrap();
        assert_eq!(actual.bytes as u64, expected);
    }
}

#[cfg(all(test, feature = "integration-test-support"))]
mod concurrency_tests {
    use super::*;
    use crate::test_support::integration_test_pool;
    use tokio::sync::oneshot;
    use tokio::time::{Duration, Instant, sleep};

    #[derive(QueryableByName)]
    struct BackendPid {
        #[diesel(sql_type = Integer)]
        pid: i32,
    }

    #[tokio::test]
    async fn control_commit_and_waiting_registration_preserve_lock_order() {
        let pool = integration_test_pool(3);
        let backend = PostgresStorage::unobserved(pool.clone());
        let before = backend.capture_history_lifecycle().await.unwrap();
        let fallback = StorageExecutionPermit::new(
            None,
            ApplicationFence::new(
                before.fence().incarnation(),
                before.fence().deletion_revision().value() as i64,
            )
            .unwrap(),
        );
        let mut registrar = pool.get().await.unwrap();
        let pid = sql_query("SELECT pg_backend_pid() AS pid")
            .get_result::<BackendPid>(&mut registrar)
            .await
            .unwrap()
            .pid;
        let registration = RuntimeRegistration::generate();
        let (held, entered) = oneshot::channel();
        let (waiting, recheck) = oneshot::channel();
        let writer =
            backend
                .runtime()
                .with_history_control_transaction(fallback, async |connection| {
                    held.send(()).unwrap();
                    recheck.await.unwrap();
                    let snapshot = capture_on(connection).await?;
                    let material = serde_json::to_value(snapshot.control()).unwrap();
                    let committed = sql_query(
                        "SELECT public.hubuum_history_lifecycle_commit($1,$1,$2,$3) AS committed",
                    )
                    .bind::<Jsonb, _>(material)
                    .bind::<SqlUuid, _>(snapshot.fence().incarnation())
                    .bind::<BigInt, _>(snapshot.fence().deletion_revision().value() as i64)
                    .get_result::<CommittedRow>(connection)
                    .await?
                    .committed;
                    assert!(committed);
                    Ok::<_, PostgresStorageError>(())
                });
        let newcomer = async {
            entered.await.unwrap();
            sql_query("SELECT public.hubuum_register_runtime_isolated($1,$2,NULL) AS committed")
                .bind::<SqlUuid, _>(registration.id())
                .bind::<SqlUuid, _>(registration.ownership_token())
                .get_result::<CommittedRow>(&mut registrar)
                .await
        };
        let observer = async {
            let mut connection = pool.get().await.unwrap();
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                let blocked = sql_query("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock') AS committed")
                    .bind::<Integer, _>(pid).get_result::<CommittedRow>(&mut connection).await.unwrap().committed;
                if blocked {
                    waiting.send(()).unwrap();
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "registration did not reach the authority lock"
                );
                sleep(Duration::from_millis(5)).await;
            }
        };
        let (written, registered, ()) = tokio::join!(writer, newcomer, observer);
        written.unwrap();
        assert!(registered.unwrap().committed);
        drop(registrar);
        backend.unregister_runtime(registration).await.unwrap();
    }
}
