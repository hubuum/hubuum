//! Native census envelope probe, run explicitly in a disposable resource fixture.
use super::*;
use crate::test_support::{
    database_role_tests_enabled, integration_test_database_roles, integration_test_migration_pool,
};
use diesel_async::SimpleAsyncConnection;
use hubuum_domain::ResourceIdentity;
use std::time::{Duration, Instant};

#[derive(QueryableByName)]
struct SourceSize {
    #[diesel(sql_type = BigInt)]
    rows: i64,
    #[diesel(sql_type = BigInt)]
    metadata_bytes: i64,
    #[diesel(sql_type = BigInt)]
    largest_identity_bytes: i64,
}

#[derive(QueryableByName)]
struct LogicalTotals {
    #[diesel(sql_type = BigInt)]
    total: i64,
    #[diesel(sql_type = BigInt)]
    held: i64,
    #[diesel(sql_type = BigInt)]
    held_rows: i64,
    #[diesel(sql_type = BigInt)]
    held_closed: i64,
}

#[derive(QueryableByName)]
struct ScopedCollection {
    #[diesel(sql_type = Integer)]
    id: i32,
    #[diesel(sql_type = Jsonb)]
    logical_identity: Value,
}

fn admission(
    snapshot: &HistoryLifecycleSnapshot,
    key: HistoryPolicyKey,
) -> HistoryExpiryAdmissionRequest {
    HistoryExpiryAdmissionRequest::new(
        Uuid::new_v4(),
        Uuid::new_v4(),
        HistoryExpiryOrigin::Manual {
            principal: PrincipalIdentity::generate(),
        },
        HistoryLifecycleScan::new(key, 1000).unwrap(),
        snapshot.fence(),
    )
    .unwrap()
}

async fn retain_mixed_state(
    connection: &mut PostgresConnection,
    mut snapshot: HistoryLifecycleSnapshot,
    key: HistoryPolicyKey,
) -> Result<
    (
        HistoryLifecycleSnapshot,
        ResourceIdentity,
        HistoryExpiryIntent,
    ),
    PostgresStorageError,
> {
    let collection = sql_query("INSERT INTO public.collections(name,description,parent_collection_id) SELECT $1,'before',id FROM public.collections WHERE parent_collection_id IS NULL RETURNING id,logical_identity")
        .bind::<Text, _>(format!("census_held_{}", Uuid::new_v4().simple()))
        .get_result::<ScopedCollection>(connection).await?;
    let evidence: StorageIdentityEvidence =
        serde_json::from_value(collection.logical_identity).unwrap();
    let held = evidence
        .subject()
        .unwrap()
        .record_identity()
        .unwrap()
        .resource();
    sql_query("UPDATE public.collections SET description='after' WHERE id=$1")
        .bind::<Integer, _>(collection.id)
        .execute(connection)
        .await?;
    let scoped = HistoryPolicyKey::new(
        key.family(),
        HistoryPolicyScope::Collection { identity: held },
    )?;
    let change = HistoryPolicyChange::new(
        scoped,
        snapshot
            .control()
            .authority()
            .policies()
            .current_revision(scoped),
        HistoryPolicyBinding::Expire {
            trigger: HistoryExpiryTrigger::LogicalSize {
                upper_logical_bytes: HistoryLogicalBytes::new(1)?,
                lower_logical_bytes: HistoryLogicalBytes::new(0)?,
            },
            minimum_age_seconds: HistoryRetentionSeconds::new(0)?,
        },
        HistoryPolicyApplication::Prospective,
    )?;
    snapshot = commit_on(
        connection,
        snapshot
            .control()
            .apply(
                HistoryPolicyApplyRequest::new(change, None),
                PrincipalIdentity::generate(),
                snapshot.fence(),
                snapshot.observed_at() - chrono::Duration::days(1),
            )
            .map_err(|_| {
                PostgresStorageError::invalid_input("Invalid overlapping census fixture policy")
            })?,
    )
    .await?;
    let hold = HistoryRetentionHold::new(
        Uuid::new_v4(),
        scoped,
        snapshot.observed_at() - chrono::Duration::days(1),
        None,
        PrincipalIdentity::generate(),
    )?;
    snapshot = commit_on(
        connection,
        snapshot.control().create_hold(hold, snapshot.fence())?,
    )
    .await?;
    connection
        .batch_execute("SET LOCAL hubuum.restore_identities='on'")
        .await?;
    sql_query(
        r#"
        WITH source AS (SELECT * FROM public.collections_history ORDER BY history_id LIMIT 1)
        INSERT INTO public.collections_history
            (history_id,id,name,description,created_at,updated_at,parent_collection_id,
             op,valid_from,valid_to,revision,logical_identity)
        SELECT (SELECT max(history_id) FROM public.collections_history)+1,
            id,name,description,created_at,updated_at,parent_collection_id,
            'U',now()-interval '2 hours',now()-interval '1 hour',1000001,
            jsonb_set(logical_identity,'{subject,version}',to_jsonb(gen_random_uuid()))
        FROM source
    "#,
    )
    .execute(connection)
    .await?;
    connection
        .batch_execute("SET LOCAL hubuum.restore_identities='off'")
        .await?;
    let page = super::super::expiry::admit_on(connection, admission(&snapshot, key)).await?;
    let manifest = page
        .manifest()
        .expect("The unheld original copy must receive size membership");
    assert_eq!(manifest.members().len(), 1);
    assert_ne!(manifest.members()[0].target().resource(), held);
    assert!(!manifest.members()[0].size_authorizations().is_empty());
    snapshot = capture_on(connection).await?;
    let pressures = snapshot.control().authority().size_transitions();
    assert_eq!(pressures.len(), 2);
    assert!(pressures.iter().all(|event| event.pressure().reclaiming()));
    let totals = sql_query("SELECT sum(public.hubuum_history_logical_bytes_v1(to_jsonb(h.*)))::bigint AS total,sum(public.hubuum_history_logical_bytes_v1(to_jsonb(h.*))) FILTER (WHERE logical_identity #>> '{subject,identity,identity}'=$1)::bigint AS held,count(*) FILTER (WHERE logical_identity #>> '{subject,identity,identity}'=$1) AS held_rows,count(*) FILTER (WHERE logical_identity #>> '{subject,identity,identity}'=$1 AND valid_to IS NOT NULL) AS held_closed FROM public.collections_history h")
        .bind::<Text, _>(held.to_string()).get_result::<LogicalTotals>(connection).await?;
    assert_eq!((totals.held_rows, totals.held_closed), (2, 1));
    for event in pressures {
        let expected = if event.pressure().policy().key() == scoped {
            totals.held
        } else {
            totals.total - totals.held
        };
        assert_eq!(
            event.pressure().measured_bytes().value(),
            u64::try_from(expected).unwrap(),
            "Held versions count under their specific source without double charging the broader source"
        );
    }
    let pending = snapshot
        .control()
        .authority()
        .expiry()
        .intent(manifest.identity())
        .unwrap()
        .clone();
    Ok((snapshot, held, pending))
}

#[tokio::test]
#[ignore = "explicit maximum-census resource qualification"]
async fn maximum_native_census() {
    let shape = std::env::var("HUBUUM_TEST_HISTORY_QUALIFICATION_SHAPE").expect("census shape");
    let (rows, metadata, must_complete, must_fail) = match shape.as_str() {
        "supported" => (1000, 0, true, false),
        "rows" => (100_000, 0, false, false),
        "rows_over" => (100_001, 0, false, true),
        "identity" => (1, 1024 * 1024, true, false),
        "identity_over" => (1, 1024 * 1024 + 1, false, true),
        "metadata" => (64, 1024 * 1024, false, false),
        "metadata_over" => (65, 1024 * 1024, false, true),
        _ => panic!("Unknown census shape"),
    };
    let backend = PostgresStorage::unobserved(integration_test_migration_pool(1));
    let before = backend.capture_history_lifecycle().await.unwrap();
    let permit = StorageExecutionPermit::new(
        None,
        ApplicationFence::new(
            before.fence().incarnation(),
            before.fence().deletion_revision().value() as i64,
        )
        .unwrap(),
    );
    let mut report = backend.runtime().with_history_control_transaction(permit, async |connection| {
        connection.batch_execute("SAVEPOINT census_fixture").await?;
        if database_role_tests_enabled() {
            sql_query("SELECT set_config('role',$1,true)")
                .bind::<Text, _>(integration_test_database_roles().owner().as_str())
                .execute(connection).await?;
        }
        let snapshot = capture_on(connection).await?;
        let key = HistoryPolicyKey::new(StorageIdentityHistoryKind::Collection, HistoryPolicyScope::Deployment)?;
        let change = HistoryPolicyChange::new(key, snapshot.control().authority().policies().current_revision(key), HistoryPolicyBinding::Expire {
            trigger: HistoryExpiryTrigger::LogicalSize { upper_logical_bytes: HistoryLogicalBytes::new(1)?, lower_logical_bytes: HistoryLogicalBytes::new(0)? },
            minimum_age_seconds: HistoryRetentionSeconds::new(0)?,
        }, HistoryPolicyApplication::Prospective)?;
        let mut applied = commit_on(connection, snapshot.control().apply(HistoryPolicyApplyRequest::new(change, None), PrincipalIdentity::generate(), snapshot.fence(), snapshot.observed_at() - chrono::Duration::days(1)).map_err(|_| PostgresStorageError::invalid_input("Invalid census qualification policy"))?).await?;
        let policy = HistoryPolicyReference::from(applied.control().authority().policies().records().last().unwrap());
        let retained = if shape == "rows_over" {
            let (snapshot, held, pending) = retain_mixed_state(connection, applied, key).await?;
            applied = snapshot;
            Some((held, pending))
        } else { None };
        // This is an owner-only, rolled-back restore fixture. Keep the real
        // table, indexes, native logical-byte function and runtime census query.
        connection.batch_execute("SET LOCAL statement_timeout='0'; SET LOCAL hubuum.restore_identities='on'").await?;
        let base = sql_query("SELECT count(*)::bigint AS rows,coalesce(sum(octet_length(logical_identity::text)),0)::bigint AS metadata_bytes,coalesce(max(octet_length(logical_identity::text)),0)::bigint AS largest_identity_bytes FROM public.collections_history")
            .get_result::<SourceSize>(connection).await?;
        let added = if metadata == 0 { rows - base.rows } else { rows };
        assert!(added > 0, "Qualification requires a fresh disposable database");
        sql_query(r#"
            WITH source AS (
                SELECT * FROM public.collections_history ORDER BY history_id LIMIT 1
            ), copies AS (
                SELECT h.*, g,
                    jsonb_set(h.logical_identity, '{subject,version}', to_jsonb(gen_random_uuid())) AS evidence
                FROM source h CROSS JOIN generate_series(1, $1) g
            ), sized AS (
                SELECT *, CASE WHEN $2=0 THEN evidence ELSE
                    jsonb_set(evidence, '{references}', (evidence->'references') ||
                        jsonb_build_object('/census', jsonb_build_object(
                            'kind', 'resource', 'family', 'collection',
                            'reference', evidence #> '{subject,identity}')))
                    END AS sized_evidence
                FROM copies
            ), padded AS (
                SELECT *, CASE WHEN $2=0 THEN sized_evidence ELSE
                    jsonb_set(evidence, '{references}', (evidence->'references') ||
                        jsonb_build_object('/census' || repeat('x', GREATEST(0,
                            $2 - CASE WHEN $3 AND g=$1 THEN $4 ELSE 0 END
                               - octet_length(sized_evidence::text))::integer),
                            jsonb_build_object('kind', 'resource', 'family', 'collection',
                                'reference', evidence #> '{subject,identity}')))
                    END AS identity
                FROM sized
            )
            INSERT INTO public.collections_history
                (history_id, id, name, description, created_at, updated_at, parent_collection_id,
                 op, valid_from, valid_to, revision, logical_identity)
            SELECT (SELECT coalesce(max(history_id),0) FROM public.collections_history)+g,
                id, name, description, created_at, updated_at, parent_collection_id,
                'U', now()-interval '2 hours', now()-interval '1 hour', g+1000000, identity
            FROM padded
        "#)
            .bind::<BigInt, _>(added)
            .bind::<BigInt, _>(metadata)
            .bind::<Bool, _>(shape == "metadata")
            .bind::<BigInt, _>(base.metadata_bytes)
            .execute(connection).await?;
        let seeded = sql_query("SELECT count(*)::bigint AS rows,coalesce(sum(octet_length(logical_identity::text)),0)::bigint AS metadata_bytes,coalesce(max(octet_length(logical_identity::text)),0)::bigint AS largest_identity_bytes FROM public.collections_history")
            .get_result::<SourceSize>(connection).await?;
        if shape == "identity" || shape == "identity_over" {
            assert_eq!(seeded.largest_identity_bytes, metadata);
        }
        if shape == "metadata" {
            assert_eq!(seeded.metadata_bytes, HistorySizeCensus::MAX_METADATA_BYTES as i64);
        }
        connection.batch_execute("SET LOCAL hubuum.restore_identities='off'").await?;
        // Measure only after provisioning, with the production runtime grants.
        if database_role_tests_enabled() {
            sql_query("SELECT set_config('role',$1,true)")
                .bind::<Text, _>(integration_test_database_roles().runtime().as_str())
                .execute(connection).await?;
        }
        connection.batch_execute("SAVEPOINT census_attempt").await?;
        let started = Instant::now();
        let result = observe_on(connection, policy, PrincipalIdentity::generate()).await;
        let elapsed = started.elapsed();
        connection.batch_execute("ROLLBACK TO SAVEPOINT census_attempt; RELEASE SAVEPOINT census_attempt").await?;
        assert_eq!(capture_on(connection).await?.control(), applied.control(), "The observation/savepoint must not leave partial pressure, audit or authority");
        let outcome = match result {
            Ok(observation) => {
                assert!(!must_fail, "An over-budget census cannot produce a gauge");
                let (accounting, _) = observation.into_parts();
                let material = serde_json::to_value(&accounting).unwrap();
                assert_eq!(material["scanned_rows"], seeded.rows.to_string());
                assert!(accounting.pressure().measured_bytes().value() > 0);
                serde_json::json!({"status":"complete", "accounting":material})
            }
            Err(error) => {
                assert!(!must_complete, "Supported census failed: {error}");
                assert_eq!(error.kind(), StorageErrorKind::Unavailable);
                serde_json::json!({"status":"unavailable", "reason":error.to_string()})
            }
        };
        assert!(elapsed < Duration::from_secs(10), "Census failed to terminate within its bounded envelope: {elapsed:?}");
        let fallback = if shape == "rows_over" {
            let started = Instant::now();
            let pure = super::super::expiry::admit_on(connection, admission(&applied, key)).await?;
            let pure_us = started.elapsed().as_micros();
            assert!(pure.accounting_unavailable());
            assert!(pure.manifest().is_none(), "Incomplete accounting cannot authorize size-only membership");
            assert_eq!(capture_on(connection).await?.control(), applied.control());
            let change = HistoryPolicyChange::new(key, applied.control().authority().policies().current_revision(key), HistoryPolicyBinding::Expire {
                trigger: HistoryExpiryTrigger::AgeOrSize {
                    age: HistoryAgeRule::new(HistoryRetentionSeconds::new(0)?, None),
                    size: HistorySizeRule::new(HistoryLogicalBytes::new(1)?, HistoryLogicalBytes::new(0)?)?,
                }, minimum_age_seconds: HistoryRetentionSeconds::new(0)?,
            }, HistoryPolicyApplication::Prospective)?;
            let age = commit_on(connection, applied.control().apply(HistoryPolicyApplyRequest::new(change, None), PrincipalIdentity::generate(), applied.fence(), applied.observed_at() - chrono::Duration::hours(23)).map_err(|_| PostgresStorageError::invalid_input("Invalid age fallback fixture policy"))?).await?;
            let started = Instant::now();
            let page = super::super::expiry::admit_on(connection, admission(&age, key)).await?;
            let age_us = started.elapsed().as_micros();
            assert!(page.accounting_unavailable());
            let manifest = page.manifest().expect("Independent age eligibility must remain productive");
            assert!(!manifest.members().is_empty());
            assert!(manifest.members().iter().all(|member| member.size_authorizations().is_empty()));
            let after = capture_on(connection).await?;
            assert_eq!(after.control().authority().size_transitions(), age.control().authority().size_transitions());
            let (held, pending) = retained.as_ref().unwrap();
            assert_eq!(after.control().authority().expiry().intent(pending.manifest().identity()), Some(pending));
            assert!(manifest.members().iter().all(|member| member.target().resource() != *held && member.target() != pending.manifest().members()[0].target()));
            assert_eq!(after.control().authority().current_holds().collect::<Vec<_>>(), age.control().authority().current_holds().collect::<Vec<_>>());
            assert!(pure_us < 10_000_000 && age_us < 10_000_000);
            serde_json::json!({"size_only_members":0,"age_members":manifest.members().len(),"age_size_witnesses":0,"pressure_unchanged":true,"size_only_us":pure_us,"age_or_size_us":age_us,"overlapping_size_sources":2,"held_copies":2,"held_closed_copies":1,"pending_size_members":1,"governing_partition_verified":true,"pending_preserved":true,"held_and_pending_unselected":true,"holds_preserved":true})
        } else {
            Value::Null
        };
        connection.batch_execute("ROLLBACK TO SAVEPOINT census_fixture").await?;
        Ok::<_, PostgresStorageError>(serde_json::json!({"shape":shape,"source_rows":seeded.rows,"metadata_bytes":seeded.metadata_bytes,"largest_identity_bytes":seeded.largest_identity_bytes,"elapsed_us":elapsed.as_micros(),"control_unchanged":true,"outcome":outcome,"fallback":fallback}))
    }).await.unwrap();
    assert_eq!(
        backend.capture_history_lifecycle().await.unwrap().control(),
        before.control(),
        "The completed probe must release its gates and roll back fixture authority"
    );
    report["gate_released"] = serde_json::json!(true);
    if cfg!(target_os = "linux") {
        let status = std::fs::read_to_string("/proc/self/status").unwrap();
        let peak_kib: u64 = status
            .lines()
            .find_map(|line| line.strip_prefix("VmHWM:"))
            .unwrap()
            .split_whitespace()
            .next()
            .unwrap()
            .parse()
            .unwrap();
        report["kernel_peak_rss_bytes"] = serde_json::json!(peak_kib * 1024);
        // Keep short probes observable after their peak. This terminal grace
        // period is outside the operation timer and cannot improve its result.
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    std::fs::write(
        std::env::var("HUBUUM_TEST_HISTORY_QUALIFICATION_REPORT").expect("census report"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
}
