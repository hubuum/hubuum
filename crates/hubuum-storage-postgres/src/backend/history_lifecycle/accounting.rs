use super::*;
use diesel::result::Error as DieselError;
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
    fold_on(connection, policy.key().family(), &mut census).await?;
    let mut accounting = census.finish()?;
    let change = snapshot
        .control()
        .observe_size(&mut accounting, actor)
        .map_err(|error| PostgresStorageError::unavailable(error.to_string()))?;
    let applied = commit_on(connection, change).await?;
    Ok::<_, PostgresStorageError>(HistorySizeObservation::new(accounting, applied))
}

pub(super) async fn fold_on(
    connection: &mut PostgresConnection,
    family: StorageIdentityHistoryKind,
    census: &mut HistorySizeCensus,
) -> Result<(), PostgresStorageError> {
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
            .execute(connection).await.map_err(unavailable_query)?;
    // Only identity metadata crosses the adapter boundary. All retained rows
    // count, including open, held, dependency-protected and excluded copies.
    let query = format!(
        "SELECT {from} AS valid_from,{until} AS valid_to,CASE WHEN octet_length(logical_identity::text)<=1048576 THEN logical_identity END AS logical_identity,{deleted} AS deleted,public.hubuum_history_logical_bytes_v1(to_jsonb(h.*)) AS logical_bytes,coalesce(octet_length(logical_identity::text),0)::bigint AS metadata_bytes FROM public.{table} h ORDER BY {position} LIMIT 100001"
    );
    {
        let mut rows = sql_query(query)
            .load_stream::<AccountingRow>(connection)
            .await
            .map_err(unavailable_query)?;
        while let Some(row) = rows.try_next().await.map_err(unavailable_query)? {
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
    Ok(())
}

fn unavailable_query(error: DieselError) -> PostgresStorageError {
    // Preserve adapter diagnostics without exposing raw database errors.
    let error = PostgresStorageError::from(error);
    PostgresStorageError::unavailable(format!(
        "Complete history accounting is unavailable: {error}"
    ))
}

#[cfg(all(test, feature = "integration-test-support"))]
mod qualification;

#[cfg(all(test, feature = "integration-test-support"))]
mod tests {
    use super::*;
    use crate::test_support::{
        database_role_tests_enabled, integration_test_database_roles,
        integration_test_migration_pool, integration_test_pool,
    };

    use diesel_async::SimpleAsyncConnection;
    use std::time::{Duration, Instant};

    #[derive(QueryableByName)]
    struct DependencyIndex {
        #[diesel(sql_type = Text)]
        predicate: String,
        #[diesel(sql_type = Text)]
        identity: String,
    }

    #[rstest::rstest]
    #[case(StorageIdentityHistoryKind::Collection)]
    #[case(StorageIdentityHistoryKind::Class)]
    #[case(StorageIdentityHistoryKind::Object)]
    #[case(StorageIdentityHistoryKind::ClassRelation)]
    #[case(StorageIdentityHistoryKind::ObjectRelation)]
    #[case(StorageIdentityHistoryKind::ExportTemplate)]
    #[case(StorageIdentityHistoryKind::RemoteTarget)]
    #[case(StorageIdentityHistoryKind::ComputedDefinitions)]
    #[tokio::test]
    async fn native_interval_families_index_open_copy_dependencies(
        #[case] family: StorageIdentityHistoryKind,
    ) {
        let pool = integration_test_pool(1);
        let (table, _) = expiry::source(family);
        let index = crate::with_connection(&pool, async |connection| {
            sql_query("SELECT pg_get_expr(i.indpred,i.indrelid) AS predicate,pg_get_indexdef(i.indexrelid,1,true) AS identity FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid WHERE i.indrelid=to_regclass($1) AND c.relname=$2")
                .bind::<Text, _>(format!("public.{table}"))
                .bind::<Text, _>(format!("{table}_open_dependency"))
                .get_result::<DependencyIndex>(connection).await.map_err(PostgresStorageError::from)
        }).await.unwrap();
        assert_eq!(index.predicate, "(valid_to IS NULL)");
        assert!(index.identity.contains("subject,identity,identity"));
    }

    #[tokio::test]
    async fn schema_dependency_index_serves_unknown_and_matching_class_references() {
        #[derive(QueryableByName)]
        struct Plan {
            #[diesel(sql_type = Text, column_name = "QUERY PLAN")]
            line: String,
        }
        let pool = integration_test_pool(1);
        let plan = crate::with_transaction(&pool, async |connection| {
            connection.batch_execute("SET LOCAL enable_seqscan = off").await?;
            // Run the predicate on the migrated relation, with both branches needed for safe expiry.
            sql_query("EXPLAIN (COSTS OFF) SELECT 1 FROM public.class_schema_revisions WHERE logical_identity #>> '{references,/class_id,reference,identity}' IS NULL OR logical_identity #>> '{references,/class_id,reference,identity}'=$1")
                .bind::<Text, _>(Uuid::new_v4().to_string())
                .load::<Plan>(connection).await.map_err(PostgresStorageError::from)
        }).await.unwrap();
        assert!(
            plan.iter()
                .any(|row| row.line.contains("class_schema_revisions_class_dependency"))
        );
    }

    #[tokio::test]
    async fn native_census_preserves_an_inherited_timeout_and_reports_unavailability() {
        let pool = integration_test_pool(2);
        let backend = PostgresStorage::unobserved(pool.clone());
        let before = backend.capture_history_lifecycle().await.unwrap();
        let permit = StorageExecutionPermit::new(
            None,
            ApplicationFence::new(
                before.fence().incarnation(),
                before.fence().deletion_revision().value() as i64,
            )
            .unwrap(),
        );
        let owner_pool = integration_test_migration_pool(1);
        let result = backend.runtime().with_history_control_transaction(permit, async |connection| {
            connection.batch_execute("SAVEPOINT census_timeout_fixture").await?;
            let snapshot = capture_on(connection).await?;
            let key = HistoryPolicyKey::new(StorageIdentityHistoryKind::Collection, HistoryPolicyScope::Deployment)?;
            let proposal = HistoryPolicyChange::new(key, snapshot.control().authority().policies().current_revision(key), HistoryPolicyBinding::Expire {
                trigger: HistoryExpiryTrigger::LogicalSize { upper_logical_bytes: HistoryLogicalBytes::new(1)?, lower_logical_bytes: HistoryLogicalBytes::new(0)? },
                minimum_age_seconds: HistoryRetentionSeconds::new(0)?,
            }, HistoryPolicyApplication::Prospective)?;
            let applied = commit_on(connection, snapshot.control().apply(HistoryPolicyApplyRequest::new(proposal, None), PrincipalIdentity::generate(), snapshot.fence(), snapshot.observed_at()).map_err(|_| PostgresStorageError::invalid_input("Invalid census timeout fixture policy"))?).await?;
            let policy = HistoryPolicyReference::from(applied.control().authority().policies().records().last().unwrap());
            let mut census = HistorySizeCensus::new(applied.clone(), &policy)?;
            let mut blocker = owner_pool.get().await.unwrap();
            blocker.batch_execute("BEGIN").await.unwrap();
            if database_role_tests_enabled() {
                sql_query("SELECT set_config('role',$1,true)")
                    .bind::<Text, _>(integration_test_database_roles().owner().as_str())
                    .execute(&mut blocker)
                    .await
                    .unwrap();
            }
            blocker
                .batch_execute("LOCK TABLE public.collections_history IN ACCESS EXCLUSIVE MODE")
                .await
                .unwrap();
            connection.batch_execute("SAVEPOINT census_attempt; SET LOCAL statement_timeout='5ms'").await?;
            let started = Instant::now();
            let error = fold_on(connection, key.family(), &mut census).await.unwrap_err();
            let elapsed = started.elapsed();
            blocker.batch_execute("ROLLBACK").await?;
            connection.batch_execute("ROLLBACK TO SAVEPOINT census_attempt; RELEASE SAVEPOINT census_attempt").await?;
            assert_eq!(error.kind(), StorageErrorKind::Unavailable);
            assert!(elapsed < Duration::from_secs(1), "An inherited five-millisecond timeout must not become five seconds: {elapsed:?}");
            assert_eq!(capture_on(connection).await?.control(), applied.control());
            connection.batch_execute("ROLLBACK TO SAVEPOINT census_timeout_fixture").await?;
            Ok::<_, PostgresStorageError>(())
        }).await;
        result.unwrap();
    }

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
