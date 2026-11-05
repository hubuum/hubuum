use super::*;
use crate::test_support::{
    IsolatedRestoreTarget, database_role_tests_enabled, integration_test_database_roles,
    integration_test_database_url, integration_test_migration_pool, integration_test_pool,
    integration_test_restore_database_url, integration_test_restore_migration_database_url,
};
use crate::{PostgresPoolSettings, build_postgres_pool};
use diesel::sql_types::Integer;
use diesel_async::SimpleAsyncConnection;
use rstest::rstest;

#[rstest]
#[case::retained_collection("collection", true, false)]
#[case::other_collection("collection", false, true)]
#[case::other_family("object", true, true)]
#[tokio::test]
async fn archive_capture_resource_gate_respects_retained_membership(
    #[case] family: &str,
    #[case] same_resource: bool,
    #[case] expected: bool,
) {
    let selected = Uuid::new_v4();
    let retained = if same_resource {
        selected
    } else {
        Uuid::new_v4()
    };
    let admin = if database_role_tests_enabled() {
        integration_test_migration_pool(1)
    } else {
        integration_test_pool(1)
    };
    let outcome = crate::with_connection(&admin, async |connection| {
        connection.batch_execute("BEGIN").await?;
        let result = async {
            if database_role_tests_enabled() {
                connection.batch_execute(&format!("SET LOCAL ROLE {}", integration_test_database_roles().owner().as_str())).await?;
            }
            sql_query("INSERT INTO hubuum_control.history_expiry_members(family,resource_uuid,version_uuid,intent_uuid,valid_from,valid_to) VALUES($1,$2,gen_random_uuid(),gen_random_uuid(),clock_timestamp()-interval '2 hours',clock_timestamp()-interval '1 hour')")
                .bind::<Text,_>(family).bind::<SqlUuid,_>(retained).execute(connection).await?;
            sql_query("SELECT public.hubuum_history_archive_resource_available($1) AS allowed")
                .bind::<SqlUuid,_>(selected).get_result::<Allowed>(connection).await
                .map(|row| row.allowed).map_err(PostgresStorageError::from)
        }.await;
        connection.batch_execute("ROLLBACK").await?;
        result
    }).await.unwrap();
    assert_eq!(outcome, expected);
}

#[rstest]
#[case::legacy(0, true)]
#[case::online_v1(1, true)]
#[case::unknown(7, false)]
#[tokio::test]
async fn native_archive_entry_accepts_only_supported_protocols(
    #[case] protocol: i32,
    #[case] expected: bool,
) {
    let backend = PostgresStorage::unobserved(integration_test_pool(2));
    let registration = backend.register_runtime().await.unwrap();
    let permit = backend
        .capture_application_execution(Some(registration.owner()))
        .await
        .unwrap();
    let admin = if database_role_tests_enabled() {
        integration_test_migration_pool(1)
    } else {
        integration_test_pool(1)
    };
    let outcome=crate::with_connection(&admin,async |connection| {
        connection.batch_execute("BEGIN").await?;
        let result=async {
            if database_role_tests_enabled() {
                connection.batch_execute(&format!("SET LOCAL ROLE {}",integration_test_database_roles().owner().as_str())).await?;
            }
            // Authority first, before the temporary registration-table lock.
            sql_query("SELECT public.hubuum_deletion_execution_allowed() AS allowed").get_result::<Allowed>(connection).await?;
            connection.batch_execute("ALTER TABLE hubuum_control.runtime_registrations ADD COLUMN IF NOT EXISTS history_protocol integer NOT NULL DEFAULT 0").await?;
            sql_query("UPDATE hubuum_control.runtime_registrations SET history_protocol=$1 WHERE id=$2")
                .bind::<Integer,_>(protocol).bind::<SqlUuid,_>(permit.owner().unwrap().id()).execute(connection).await?;
            let allowed=fenced!(sql_query("SELECT public.hubuum_archive_history_entry($1,$2,$3,$4) AS allowed"),&permit)
                .get_result::<Allowed>(connection).await?.allowed;
            Ok::<_,PostgresStorageError>(allowed)
        }.await;
        connection.batch_execute("ROLLBACK").await?;
        result
    }).await;
    backend.unregister_runtime(registration).await.unwrap();
    if protocol == 7 {
        assert!(
            outcome.is_err(),
            "unknown protocol must violate the registry constraint"
        );
    } else {
        assert_eq!(outcome.unwrap(), expected);
    }
}

#[tokio::test]
async fn rejected_capture_preserves_expired_cleanup_accounting() {
    #[derive(Debug, Eq, PartialEq, QueryableByName)]
    struct Counter {
        #[diesel(sql_type = Integer)]
        captures: i32,
        #[diesel(sql_type = BigInt)]
        bytes: i64,
        #[diesel(sql_type = BigInt)]
        actual_captures: i64,
        #[diesel(sql_type = BigInt)]
        actual_bytes: i64,
        #[diesel(sql_type = BigInt)]
        live_bytes: i64,
    }
    const COUNTER: &str = "SELECT captures,bytes,(SELECT count(*) FROM hubuum_control.history_archive_captures) AS actual_captures,(SELECT COALESCE(sum(bytes),0) FROM hubuum_control.history_archive_captures) AS actual_bytes,(SELECT COALESCE(sum(bytes),0) FROM hubuum_control.history_archive_captures WHERE id=$1) AS live_bytes FROM hubuum_control.history_archive_capture_capacity WHERE id=1";
    let backend = PostgresStorage::unobserved(integration_test_pool(2));
    let registration = backend.register_runtime().await.unwrap();
    let permit = backend
        .capture_application_execution(Some(registration.owner()))
        .await
        .unwrap();
    let resource = ResourceIdentity::generate();
    let catalog = HistoryArchiveCatalog::empty(resource);
    let make_capture = |now| {
        HistoryArchiveCapture::capture(
            HistoryArchiveCaptureRequest::new(
                PrincipalIdentity::generate(),
                HistoryArchiveSelection::new(
                    StorageBackupHistorySection::CollectionHistory,
                    resource,
                    HistoryArchivePoint::AsOf { at: now },
                ),
                catalog.generation(),
                now + chrono::Duration::hours(1),
            ),
            &catalog,
            &permit,
            HistoryArchiveInput::Unavailable,
            now,
        )
        .unwrap()
    };
    let live = make_capture(Utc::now());
    let expired = make_capture(Utc::now() - chrono::Duration::hours(2));
    let mut oversized = serde_json::to_value(make_capture(Utc::now())).unwrap();
    oversized["padding"] = serde_json::json!("x".repeat(HISTORY_ARCHIVE_CAPTURE_BYTES));
    let admin = if database_role_tests_enabled() {
        integration_test_migration_pool(1)
    } else {
        integration_test_pool(1)
    };
    let outcome = crate::with_connection(&admin, async |connection| {
        connection.batch_execute("BEGIN").await?;
        let result = async {
            if database_role_tests_enabled() {
                sql_query("SELECT set_config('role', $1, true)")
                    .bind::<Text, _>(integration_test_database_roles().owner().as_str()).execute(connection).await?;
            }
            let authority = fenced!(sql_query("SELECT public.hubuum_archive_history_entry($1,$2,$3,$4) AS allowed"), &permit)
                .get_result::<Allowed>(connection).await?.allowed;
            if !authority { return Err(PostgresStorageError::unavailable("Archive fixture owner unavailable")); }
            // Keep the capacity lock across both observations and the rollback
            // check. A second transaction would admit unrelated captures before
            // we compare counters, making this global invariant test race.
            sql_query("SELECT id FROM hubuum_control.history_archive_capture_capacity WHERE id=1 FOR UPDATE")
                .execute(connection).await?;
            let before = sql_query(COUNTER)
                .bind::<SqlUuid,_>(live.id()).get_result::<Counter>(connection).await?;
            connection.batch_execute("SAVEPOINT archive_capture_probe").await?;
            assert!(save_on(connection, &live, None, &permit).await?);
            // Simulate an elapsed stored metadata pin, not read coverage or
            // runtime/publication-owner expiry. The registration remains live.
            assert!(save_on(connection, &expired, None, &permit).await?);
            let mut observations = Vec::new();
            for _ in 0..2 {
                let allowed = fenced!(sql_query("SELECT public.hubuum_history_archive_capture_save($1,$2,$3,$4,$5,$6) AS allowed")
                    .bind::<Jsonb,_>(oversized.clone()).bind::<Nullable<BigInt>,_>(None::<i64>), &permit)
                    .get_result::<Allowed>(connection).await?.allowed;
                let counter = sql_query(COUNTER).bind::<SqlUuid,_>(live.id())
                    .get_result::<Counter>(connection).await?;
                observations.push((allowed,counter));
            }
            connection.batch_execute("ROLLBACK TO SAVEPOINT archive_capture_probe").await?;
            let restored = sql_query(COUNTER).bind::<SqlUuid,_>(live.id())
                .get_result::<Counter>(connection).await?;
            Ok::<_,PostgresStorageError>((before,observations,restored))
        }.await;
        connection.batch_execute("ROLLBACK").await?;
        result
    }).await;
    let archive = backend
        .run_in_scope(
            StorageExecutionScope::default().with_application_access(permit.clone()),
            backend.admit_history_archive_execution(),
        )
        .await
        .unwrap();
    let proof = archive.permit().clone();
    let owner_still_live = backend
        .run_in_scope(
            proof.execution_scope().clone(),
            backend.history_archive_catalog(resource, &proof),
        )
        .await;
    let settled = archive
        .after_drained(std::future::ready(Ok(())), std::future::ready(Ok(())))
        .await
        .unwrap();
    backend
        .settle_history_producer(settled.producer())
        .await
        .unwrap();
    backend
        .settle_history_publication(settled.into_publication())
        .await
        .unwrap();
    backend.unregister_runtime(registration).await.unwrap();
    let (before, observations, restored) = outcome.unwrap();
    assert!(owner_still_live.is_ok());
    for (allowed, counter) in &observations {
        assert!(!allowed);
        assert_eq!(i64::from(counter.captures), counter.actual_captures);
        assert_eq!(counter.bytes, counter.actual_bytes);
        assert!(counter.live_bytes > 0);
    }
    assert_eq!(observations[0].1, observations[1].1);
    assert_eq!(restored, before);
}

#[tokio::test]
async fn rejected_admission_commits_cleanup_before_the_next_attempt() {
    // Only this adapter unit test uses the runner's separate restore database.
    // Other test binaries run sequentially and receive it back without our pins.
    let migration_url = integration_test_restore_migration_database_url();
    IsolatedRestoreTarget::new(&migration_url, &integration_test_database_url()).unwrap();
    let pool = |database_url| {
        let settings = PostgresPoolSettings::builder(database_url)
            .max_size(2)
            .statement_timeout_ms(10_000)
            .acquire_timeout_ms(5_000)
            .build()
            .unwrap();
        build_postgres_pool(&settings).unwrap()
    };
    let admin = pool(migration_url);
    let backend = PostgresStorage::unobserved(pool(integration_test_restore_database_url()));
    let registration = backend.register_runtime().await.unwrap();
    let permit = backend
        .capture_application_execution(Some(registration.owner()))
        .await
        .unwrap();
    let name = format!("archive_cleanup_{}", Uuid::new_v4());
    let identity = crate::with_transaction(&admin, async |connection| {
        if database_role_tests_enabled() {
            sql_query("SELECT set_config('role', $1, true)")
                .bind::<Text, _>(integration_test_database_roles().owner().as_str())
                .execute(connection).await?;
        }
        let material = sql_query("INSERT INTO public.collections(name,description,parent_collection_id) SELECT $1,$2,id FROM public.collections WHERE parent_collection_id IS NULL RETURNING logical_identity::text AS material")
            .bind::<Text, _>(&name).bind::<Text, _>("x".repeat(24 * 1024))
            .get_result::<Material>(connection).await?;
        let evidence: StorageIdentityEvidence = decode(material.material.as_deref().unwrap())?;
        let identity = evidence.subject().and_then(StorageIdentitySubject::record_identity).unwrap();
        // Exercise the real 1 GiB limit using charged metadata fixtures, without
        // allocating a GiB of irrelevant payloads. Table/counter charges agree.
        let empty = sql_query("SELECT captures=0 AND bytes=0 AS allowed FROM hubuum_control.history_archive_capture_capacity WHERE id=1 FOR UPDATE")
            .get_result::<Allowed>(connection).await?;
        assert!(empty.allowed, "The isolated capacity fixture must start empty");
        sql_query("INSERT INTO hubuum_control.history_archive_captures(id,principal,expires_at,material,bytes) SELECT gen_random_uuid(),$1,clock_timestamp()+interval '24 hours',jsonb_build_object('runtime',$2::text),CASE WHEN n=256 THEN 4194304-32768 ELSE 4194304 END FROM generate_series(1,256) n")
            .bind::<SqlUuid, _>(Uuid::new_v4()).bind::<Text, _>(registration.owner().id().to_string())
            .execute(connection).await?;
        sql_query("INSERT INTO hubuum_control.history_archive_captures(id,principal,expires_at,material,bytes) SELECT gen_random_uuid(),$1,clock_timestamp()-interval '1 hour',jsonb_build_object('runtime',$2::text),1024 FROM generate_series(1,32)")
            .bind::<SqlUuid, _>(Uuid::new_v4()).bind::<Text, _>(registration.owner().id().to_string())
            .execute(connection).await?;
        sql_query("UPDATE hubuum_control.history_archive_capture_capacity SET captures=(SELECT count(*) FROM hubuum_control.history_archive_captures),bytes=(SELECT sum(bytes) FROM hubuum_control.history_archive_captures) WHERE id=1")
            .execute(connection).await?;
        Ok::<_, PostgresStorageError>(identity)
    }).await.unwrap();
    let request = HistoryArchiveCaptureRequest::new(
        PrincipalIdentity::generate(),
        HistoryArchiveSelection::new(
            StorageBackupHistorySection::CollectionHistory,
            identity.resource(),
            HistoryArchivePoint::Exact {
                version: identity.version(),
            },
        ),
        HistoryArchiveCatalogGeneration::default(),
        Utc::now() + chrono::Duration::hours(1),
    );
    let archive = backend
        .run_in_scope(
            StorageExecutionScope::default().with_application_access(permit.clone()),
            backend.admit_history_archive_execution(),
        )
        .await
        .unwrap();
    let proof = archive.permit().clone();
    let rejected = backend
        .run_in_scope(
            proof.execution_scope().clone(),
            backend.capture_history_archive(request.clone(), &proof),
        )
        .await;
    let admitted = backend
        .run_in_scope(
            proof.execution_scope().clone(),
            backend.capture_history_archive(request.clone(), &proof),
        )
        .await;
    // Cleanup before asserting the outcomes, including when the old rollback
    // behavior rejects both attempts.
    crate::with_transaction(&admin, async |connection| {
        if database_role_tests_enabled() {
            sql_query("SELECT set_config('role', $1, true)")
                .bind::<Text, _>(integration_test_database_roles().owner().as_str())
                .execute(connection).await?;
        }
        sql_query("WITH removed AS (DELETE FROM hubuum_control.history_archive_captures WHERE material->>'runtime'=$1 RETURNING bytes) UPDATE hubuum_control.history_archive_capture_capacity SET captures=captures-(SELECT count(*)::integer FROM removed),bytes=bytes-(SELECT COALESCE(sum(bytes),0) FROM removed) WHERE id=1")
            .bind::<Text, _>(registration.owner().id().to_string()).execute(connection).await?;
        sql_query("DELETE FROM public.collections WHERE name=$1").bind::<Text, _>(&name).execute(connection).await?;
        sql_query("DELETE FROM public.collections_history WHERE name=$1").bind::<Text, _>(&name).execute(connection).await?;
        Ok::<_, PostgresStorageError>(())
    }).await.unwrap();
    let settled = archive
        .after_drained(std::future::ready(Ok(())), std::future::ready(Ok(())))
        .await
        .unwrap();
    backend
        .settle_history_producer(settled.producer())
        .await
        .unwrap();
    backend
        .settle_history_publication(settled.into_publication())
        .await
        .unwrap();
    backend.unregister_runtime(registration).await.unwrap();
    rejected
        .unwrap_err()
        .history_archive_budget_evidence()
        .expect("Bounded rejection evidence")
        .validate_request(&request, &permit, Utc::now())
        .unwrap();
    assert_eq!(
        admitted.unwrap().coverage(Utc::now()),
        HistoryArchiveCoverageState::Complete
    );
}

// Real retained row and runtime-role productive entry. Only the transaction
// metadata helper is exercised; no claim/provider receipt or work is issued.
// Rollback leaves no physical obligation requiring a fixture reset.
#[derive(Clone, Copy, Debug)]
enum PublicationClaimCase {
    UnboundedRollback,
    BoundedRollback,
    ExpiredControl,
}

#[rstest::rstest]
#[case::unbounded(PublicationClaimCase::UnboundedRollback)]
#[case::bounded(PublicationClaimCase::BoundedRollback)]
#[case::expired_control(PublicationClaimCase::ExpiredControl)]
#[tokio::test]
async fn native_publication_claim_rollback_keeps_child_pin_and_record_atomic(
    #[case] case: PublicationClaimCase,
) {
    use hubuum_events_core::EventContext;
    use hubuum_storage_core::artifacts::{ArtifactIdentity, ArtifactLimits, DestinationId};
    use hubuum_storage_core::{
        StorageCollectionCreate, StorageCollectionUpdate, StorageGroupCreate,
    };
    let backend = PostgresStorage::unobserved(integration_test_pool(2));
    let name = format!("claim-rollback-{}", Uuid::new_v4());
    let mut group = None;
    let mut collection = None;
    let mut registration = None;
    let mut owner = None;
    let outcome = async {
        let created = backend.create_group(StorageGroupCreate::new(None, name.clone(), None), &EventContext::system()).await?.into_value();
        let group_id = created.id();
        group = Some(created);
        let created = backend.create_collection(StorageCollectionCreate::new(name, "original fixed version", group_id, None), &EventContext::system()).await?.into_value();
        let collection_id = created.id();
        collection = Some(created);
        let identity = backend.resolve_logical_identity(StorageIdentityTarget::Resource { kind: StorageIdentityResourceKind::Collection, id: collection_id.into() }).await?.subject().and_then(StorageIdentitySubject::record_identity)
            .ok_or_else(|| StorageError::unavailable("Fixture original collection identity is unavailable"))?;
        backend.update_collection(collection_id, StorageCollectionUpdate::new(None, Some("later version".into())), &EventContext::system()).await?.into_value();
        let registered = backend.register_runtime().await?;
        let application = backend.capture_application_execution(Some(registered.owner())).await;
        registration = Some(registered);
        let application = application?;
        let execution = if matches!(case, PublicationClaimCase::UnboundedRollback) {
            None
        } else {
            Some(hubuum_task_core::TaskExecutionContext::new(std::time::Duration::from_secs(2)).unwrap().0)
        };
        let mut scope = StorageExecutionScope::default().with_application_access(application);
        if let Some(execution) = &execution { scope = scope.with_task_execution(Some(execution.clone())); }
        owner = Some(backend.run_in_scope(scope, backend.admit_history_archive_execution()).await?);
        let permit = owner.as_ref().expect("admitted case owner").permit().clone();
        let capture = backend.run_in_scope(permit.execution_scope().clone(), backend.capture_history_archive(HistoryArchiveCaptureRequest::new(PrincipalIdentity::generate(), HistoryArchiveSelection::new(StorageBackupHistorySection::CollectionHistory, identity.resource(), HistoryArchivePoint::Exact { version: identity.version() }), HistoryArchiveCatalogGeneration::default(), Utc::now() + chrono::Duration::hours(1)), &permit)).await?;
        let manifest = HistoryArchivePublicationManifest::from_capture(&capture, &permit, ArtifactIdentity::new(DestinationId::generate()), Utc::now())?;
        let intent = HistoryArchiveEncodedArtifactIntent::try_new(manifest.location().clone(), ArtifactLimits::builder().artifact_bytes(HISTORY_ARCHIVE_PUBLICATION_BYTES).build().unwrap(), b"neutral rollback claim metadata only".to_vec(), manifest.request_commitment()?)?;
        let request = HistoryArchivePublicationRequest::try_new(manifest, intent)?;
        let mut cutoff_rejected = true;
        let mut cutoff_diagnostic = None;
        let failure = if matches!(case, PublicationClaimCase::ExpiredControl) {
            let execution = execution.as_ref().expect("bounded case");
            tokio::time::sleep(execution.remaining()).await;
            let stopped = execution.check() == Err(hubuum_task_core::TaskStopReason::DeadlineExceeded);
            let productive = backend.run_in_scope(permit.execution_scope().clone(), backend.history_archive_catalog(capture.selection().resource(), &permit)).await;
            let productive_rejected = productive.is_err_and(|e| e.kind() == hubuum_storage_core::StorageErrorKind::TaskDeadlineExceeded);
            // Control is API/task-exempt, but cannot bypass the native fixed
            // cutoff. Use the real runtime login and ALWAYS roll back, including
            // if a regression mistakenly accepts this expired claim.
            let mut raw_rejected = false;
            let mut clock_expired = false;
            let failed = backend.runtime().with_control_transaction(async |connection| {
                let clock = sql_query("SELECT clock_timestamp() AS recorded_at").get_result::<Clock>(connection).await?;
                clock_expired = clock.recorded_at >= request.manifest().execution_deadline();
                if database_role_tests_enabled() {
                    let runtime = sql_query("SELECT current_user::TEXT=$1 AS allowed")
                        .bind::<Text,_>(integration_test_database_roles().runtime().as_str())
                        .get_result::<Allowed>(connection).await?;
                    if !runtime.allowed { return Err(PostgresStorageError::conflict("Native cutoff probe requires runtime role")); }
                }
                let producer = permit.producer();
                let runtime = permit.application().owner().expect("actual registered runtime");
                let saved = sql_query("SELECT public.hubuum_history_archive_publication_claim($1,$2,$3,$4,$5,$6,$7,$8,$9) AS allowed")
                    .bind::<SqlUuid,_>(producer.parent().id()).bind::<SqlUuid,_>(producer.parent().ownership_token())
                    .bind::<SqlUuid,_>(producer.id()).bind::<SqlUuid,_>(producer.ownership_token())
                    .bind::<SqlUuid,_>(runtime.id()).bind::<SqlUuid,_>(runtime.ownership_token())
                    .bind::<Binary,_>(history_archive_encoded_bytes(request.manifest(), HISTORY_ARCHIVE_PUBLICATION_MANIFEST_BYTES)?)
                    .bind::<Binary,_>(request.intent().encoded())
                    .bind::<Jsonb,_>(json(&capture,HISTORY_ARCHIVE_CAPTURE_BYTES)?)
                    .get_result::<Allowed>(connection).await?;
                raw_rejected = !saved.allowed;
                Err::<HistoryArchivePublicationRequest,_>(PostgresStorageError::conflict("injected rollback after protected claim writes"))
            }).await;
            cutoff_rejected = stopped && productive_rejected && clock_expired && raw_rejected;
            if !cutoff_rejected {
                cutoff_diagnostic = Some(format!("Native metadata cutoff mismatch: original_scope_stopped={stopped}, productive_rejected={productive_rejected}, database_clock_expired={clock_expired}, raw_control_rejected={raw_rejected}"));
            }
            failed
        } else {
            backend.run_in_scope(permit.execution_scope().clone(), backend.runtime().with_history_archive_claim_transaction(&permit, async |connection| {
                publication_claim_on(connection, request, &permit).await?;
                Err::<HistoryArchivePublicationRequest, _>(PostgresStorageError::conflict("injected rollback after protected claim writes"))
            })).await
        };
        let rollback_reached = match failure {
            Err(error) if error.to_string() == "injected rollback after protected claim writes" => true,
            Err(error) => return Err(StorageError::from(error)),
            Ok(_) => false,
        };
        let admin = if database_role_tests_enabled() { integration_test_migration_pool(1) } else { integration_test_pool(1) };
        let observed = crate::with_transaction(&admin, async |connection| {
            if database_role_tests_enabled() {
                connection.batch_execute(&format!("SET LOCAL ROLE {}", integration_test_database_roles().owner().as_str())).await?;
            }
            // Owner access inspects protected rollback state ONLY. Productive
            // claim validation above used runtime role and original full scope.
            sql_query("SELECT NOT EXISTS(SELECT 1 FROM hubuum_control.history_archive_publication_claims WHERE publication_id=$1) AND EXISTS(SELECT 1 FROM hubuum_control.history_publication_producers WHERE publication_id=$1 AND NOT retired AND NOT publication_claimed) AND EXISTS(SELECT 1 FROM hubuum_control.history_publication_owners WHERE id=$1 AND producer_count=1) AND EXISTS(SELECT 1 FROM hubuum_control.history_archive_captures WHERE id=$2 AND NOT publication_claimed) AS allowed")
                .bind::<SqlUuid,_>(permit.producer().parent().id()).bind::<SqlUuid,_>(capture.id())
                .get_result::<Allowed>(connection).await.map_err(PostgresStorageError::from)
        }).await.map_err(StorageError::from)?;
        if let Some(diagnostic) = cutoff_diagnostic { return Err(StorageError::conflict(diagnostic)); }
        Ok::<_, StorageError>((rollback_reached, observed.allowed && cutoff_rejected))
    }.await;
    // Direct awaited DB-only fixture work; no artifact producer/transport was
    // ever issued. Cleanup never resets unknown authority or a committed claim.
    let cleanup = async {
        if let Some(owner) = owner.take() {
            let finished = owner
                .after_drained(std::future::ready(Ok(())), std::future::ready(Ok(())))
                .await?;
            backend.settle_history_producer(finished.producer()).await?;
            backend
                .settle_history_publication(finished.into_publication())
                .await?;
        }
        if let Some(registration) = registration.take() {
            backend.unregister_runtime(registration).await?;
        }
        if let Some(collection) = collection.take() {
            backend
                .delete_collection(collection.id(), &EventContext::system())
                .await?
                .into_value();
        }
        if let Some(group) = group.take() {
            backend
                .delete_group(group.id(), &EventContext::system())
                .await?
                .into_value();
        }
        Ok::<_, StorageError>(())
    }
    .await;
    assert!(
        cleanup.is_ok(),
        "fixture cleanup={cleanup:?}; original case={outcome:?}"
    );
    assert_eq!(outcome.unwrap(), (true, true));
}
