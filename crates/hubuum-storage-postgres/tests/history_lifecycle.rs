#![cfg(feature = "integration-test-support")]

use chrono::{Duration, Utc};
use diesel::sql_types::{Array, Jsonb, Text, Uuid as SqlUuid};
use diesel::{QueryableByName, sql_query};
use diesel_async::{RunQueryDsl, SimpleAsyncConnection};
use hubuum_domain::PrincipalIdentity;
use hubuum_storage_core::*;
use hubuum_storage_postgres::test_support::{
    database_role_tests_enabled, integration_test_database_roles, integration_test_migration_pool,
    integration_test_pool,
};
use hubuum_storage_postgres::{
    PostgresConnection, PostgresPool, PostgresStorage, PostgresStorageError, with_transaction,
};
use serde_json::Value;
use uuid::Uuid;

static CONTROL_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test]
async fn publication_registry_round_trip_rejects_retired_scope() {
    let _guard = CONTROL_LOCK.lock().await;
    let storage = PostgresStorage::unobserved(integration_test_pool(2));
    let registration = storage.register_history_runtime(None).await.unwrap();
    let storage = storage.with_runtime_owner(registration.owner());
    let permit = storage
        .capture_application_execution(Some(registration.owner()))
        .await
        .unwrap();
    let scope = StorageExecutionScope::default().with_application_access(permit);
    let owner = storage
        .run_in_scope(
            scope.clone(),
            storage.admit_history_publication(HistoryPublicationKind::TaskOutput),
        )
        .await
        .unwrap();
    let productive = scope.with_history_read(owner.context());
    let root = hubuum_domain::CollectionId::new(1).unwrap();
    storage
        .run_in_scope(productive.clone(), storage.get_collection(root))
        .await
        .unwrap();
    // Registry fixture only: the SQL read above has finished, and no external
    // publisher exists. Real HTTP driver completion is qualified separately.
    let settlement = owner
        .after_drained(std::future::ready(Ok(())), std::future::ready(Ok(())))
        .await
        .unwrap();
    storage
        .settle_history_publication(settlement)
        .await
        .unwrap();
    let retired = storage
        .run_in_scope(productive, storage.get_collection(root))
        .await;
    storage.unregister_runtime(registration).await.unwrap();
    assert!(
        retired.is_err(),
        "A settled publication context cannot enter productive SQL"
    );
}

#[derive(QueryableByName)]
struct Material {
    #[diesel(sql_type = Jsonb)]
    material: Value,
}
async fn owner(connection: &mut PostgresConnection) -> Result<(), PostgresStorageError> {
    if database_role_tests_enabled() {
        sql_query("SELECT set_config('role',$1,true)")
            .bind::<Text, _>(integration_test_database_roles().owner().as_str())
            .execute(connection)
            .await?;
    }
    Ok(())
}
struct Fixture {
    storage: PostgresStorage,
    owner_pool: PostgresPool,
    before: Value,
}
impl Fixture {
    async fn new() -> Self {
        let owner_pool = integration_test_migration_pool(2);
        let before = with_transaction(&owner_pool, async |connection| {
            owner(connection).await?;
            Ok::<_, PostgresStorageError>(
                sql_query(
                    "SELECT material FROM hubuum_control.history_lifecycle_control WHERE id=1",
                )
                .get_result::<Material>(connection)
                .await?
                .material,
            )
        })
        .await
        .unwrap();
        Self {
            storage: PostgresStorage::unobserved(integration_test_pool(2)),
            owner_pool,
            before,
        }
    }
    async fn restore(self) {
        let previous: HistoryLifecycleControl =
            serde_json::from_value(self.before.clone()).unwrap();
        let current = self.storage.capture_history_lifecycle().await.unwrap();
        let event_ids = current
            .control()
            .audit_events_since(&previous)
            .unwrap()
            .into_iter()
            .map(|event| event.event_id().as_uuid())
            .collect::<Vec<_>>();
        with_transaction(&self.owner_pool, async |connection| {
            owner(connection).await?;
            // Owner-only cleanup opts into the existing purge guard and removes
            // only this fixture's exact canonical event identities.
            connection
                .batch_execute("SET LOCAL events.allow_purge='on'")
                .await?;
            sql_query("DELETE FROM public.events WHERE event_id=ANY($1)")
                .bind::<Array<SqlUuid>, _>(event_ids)
                .execute(connection)
                .await?;
            sql_query("UPDATE hubuum_control.history_lifecycle_control SET material=$1 WHERE id=1")
                .bind::<Jsonb, _>(self.before)
                .execute(connection)
                .await?;
            Ok::<_, PostgresStorageError>(())
        })
        .await
        .unwrap();
    }
}
fn proposal(snapshot: &HistoryLifecycleSnapshot) -> HistoryPolicyChange {
    let key = HistoryPolicyKey::new(
        StorageIdentityHistoryKind::Object,
        HistoryPolicyScope::Deployment,
    )
    .unwrap();
    HistoryPolicyChange::new(
        key,
        snapshot
            .control()
            .authority()
            .policies()
            .current_revision(key),
        HistoryPolicyBinding::Indefinite,
        HistoryPolicyApplication::Retrospective {
            closed_through: snapshot.observed_at() - Duration::seconds(1),
        },
    )
    .unwrap()
}

#[tokio::test]
async fn runtime_applies_only_an_issued_revision_bound_review() {
    let _guard = CONTROL_LOCK.lock().await;
    let fixture = Fixture::new().await;
    let snapshot = fixture.storage.capture_history_lifecycle().await.unwrap();
    let principal = PrincipalIdentity::generate();
    let preview = HistoryPolicyPreview::issue(
        principal,
        proposal(&snapshot),
        snapshot.fence(),
        snapshot.observed_at(),
        HistoryPreviewImpact::new(HistoryPreviewCoverage::Bounded, 0, 0, Vec::new()).unwrap(),
    )
    .unwrap();
    let issued = fixture
        .storage
        .commit_history_lifecycle(
            snapshot
                .control()
                .issue_preview(preview.clone(), snapshot.fence())
                .unwrap(),
        )
        .await
        .unwrap();
    let change = issued
        .control()
        .apply(
            HistoryPolicyApplyRequest::new(
                preview.proposal().clone(),
                Some(preview.reference().clone()),
            ),
            principal,
            issued.fence(),
            Utc::now(),
        )
        .unwrap();
    let result = fixture
        .storage
        .commit_history_lifecycle(change.clone())
        .await;
    let stale = fixture.storage.commit_history_lifecycle(change).await;
    fixture.restore().await;
    assert!(result.is_ok());
    assert!(matches!(stale, Err(error) if error.kind() == StorageErrorKind::Conflict));
}

#[tokio::test]
async fn required_audit_failure_rolls_back_policy_authority() {
    let _guard = CONTROL_LOCK.lock().await;
    let fixture = Fixture::new().await;
    let snapshot = fixture.storage.capture_history_lifecycle().await.unwrap();
    let hold = HistoryRetentionHold::new(
        Uuid::new_v4(),
        HistoryPolicyKey::new(
            StorageIdentityHistoryKind::Object,
            HistoryPolicyScope::Deployment,
        )
        .unwrap(),
        Utc::now(),
        None,
        PrincipalIdentity::generate(),
    )
    .unwrap();
    let change = snapshot
        .control()
        .create_hold(hold, snapshot.fence())
        .unwrap();
    let target = change.next().audit_events_since(change.expected()).unwrap()[0]
        .event_id()
        .as_uuid();
    // Inject a transaction-local canonical append failure using the real event
    // insert path. The trigger is removed before any assertion or fixture reset.
    with_transaction(&fixture.owner_pool, async |connection| {
        owner(connection).await?;
        let setup = format!("CREATE FUNCTION public.history_lifecycle_audit_fault() RETURNS trigger LANGUAGE plpgsql AS $fault$ BEGIN IF NEW.event_id='{target}'::uuid THEN RAISE EXCEPTION 'required audit fault'; END IF; RETURN NEW; END $fault$; CREATE TRIGGER history_lifecycle_audit_fault BEFORE INSERT ON public.events FOR EACH ROW EXECUTE FUNCTION public.history_lifecycle_audit_fault();");
        connection.batch_execute(&setup).await?;
        Ok::<_,PostgresStorageError>(())
    }).await.unwrap();
    let result = fixture.storage.commit_history_lifecycle(change).await;
    with_transaction(&fixture.owner_pool, async |connection| {
        owner(connection).await?;
        connection.batch_execute("DROP TRIGGER history_lifecycle_audit_fault ON public.events; DROP FUNCTION public.history_lifecycle_audit_fault();").await?;
        Ok::<_,PostgresStorageError>(())
    }).await.unwrap();
    let after = fixture.storage.capture_history_lifecycle().await.unwrap();
    fixture.restore().await;
    assert!(result.is_err());
    assert_eq!(after.control(), snapshot.control());
}

#[tokio::test]
async fn competing_hold_changes_preserve_the_winner() {
    let _guard = CONTROL_LOCK.lock().await;
    let fixture = Fixture::new().await;
    let snapshot = fixture.storage.capture_history_lifecycle().await.unwrap();
    let principal = PrincipalIdentity::generate();
    let key = HistoryPolicyKey::new(
        StorageIdentityHistoryKind::Object,
        HistoryPolicyScope::Deployment,
    )
    .unwrap();
    let first =
        HistoryRetentionHold::new(Uuid::new_v4(), key, Utc::now(), None, principal).unwrap();
    let second =
        HistoryRetentionHold::new(Uuid::new_v4(), key, Utc::now(), None, principal).unwrap();
    let first = snapshot
        .control()
        .create_hold(first, snapshot.fence())
        .unwrap();
    let second = snapshot
        .control()
        .create_hold(second, snapshot.fence())
        .unwrap();
    let (left, right) = tokio::join!(
        fixture.storage.commit_history_lifecycle(first),
        fixture.storage.commit_history_lifecycle(second)
    );
    fixture.restore().await;
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
}

// Owner installation creates a validated retained control fixture only. The
// transition under test uses the real runtime capability and required audit
// sink; this does not enable or qualify native intent/removal admission.
#[rstest::rstest]
#[case::hold("hold", true)]
#[case::policy("policy", true)]
#[case::journal_mutation("cancel", false)]
#[tokio::test]
async fn protective_revision_after_retained_expiry(
    #[case] operation: &str,
    #[case] accepted: bool,
) {
    let _guard = CONTROL_LOCK.lock().await;
    let fixture = Fixture::new().await;
    let registration = fixture
        .storage
        .register_history_runtime(None)
        .await
        .unwrap();
    let permit = fixture
        .storage
        .capture_application_execution(Some(registration.owner()))
        .await
        .unwrap();
    let scope = StorageExecutionScope::default().with_application_access(permit);
    let original = fixture.storage.capture_history_lifecycle().await.unwrap();
    let now = original.observed_at();
    let principal = PrincipalIdentity::generate();
    let key = HistoryPolicyKey::new(
        StorageIdentityHistoryKind::Object,
        HistoryPolicyScope::Deployment,
    )
    .unwrap();
    let policy = HistoryPolicyChange::new(
        key,
        HistoryPolicyRevision::new(0).unwrap(),
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
    let policy = original
        .control()
        .apply(
            HistoryPolicyApplyRequest::new(policy, None),
            principal,
            original.fence(),
            now - Duration::seconds(3),
        )
        .unwrap();
    let selected = HistoryLifecycleSnapshot::new(
        policy.next().clone(),
        original.fence().incarnation(),
        original.fence().deletion_revision(),
        now,
    )
    .unwrap();
    let candidate = HistoryExpiryCandidate::new(
        StorageIdentityHistoryKind::Object,
        StorageRecordIdentity::new(
            hubuum_domain::ResourceIdentity::generate(),
            hubuum_domain::VersionIdentity::generate(),
        ),
        Some(HistoryRetentionAnchor::Closed(now - Duration::seconds(1))),
        vec![HistoryScopeAttribution::new(
            hubuum_domain::ResourceIdentity::generate(),
            Some(hubuum_domain::ResourceIdentity::generate()),
        )],
        false,
    )
    .unwrap()
    .with_native_interval(now - Duration::seconds(2), Some(now - Duration::seconds(1)))
    .unwrap();
    let intent_id = Uuid::new_v4();
    let mut selection = HistoryExpirySelection::new(
        intent_id,
        HistoryExpiryOrigin::Manual { principal },
        HistoryLifecycleScan::new(key, 1).unwrap(),
        &selected,
    )
    .unwrap();
    assert!(matches!(
        selection
            .consider(candidate, HistoryExpiryInputBytes::new(1).unwrap(), &[])
            .unwrap(),
        HistoryExpirySelectionOutcome::Selected,
    ));
    let retained = selected
        .control()
        .admit_expiry(selection.finish().unwrap(), selected.fence())
        .unwrap();
    with_transaction(&fixture.owner_pool, async |connection| {
        owner(connection).await?;
        sql_query("UPDATE hubuum_control.history_lifecycle_control SET material=$1 WHERE id=1")
            .bind::<Jsonb, _>(serde_json::to_value(retained.next()).unwrap())
            .execute(connection)
            .await?;
        Ok::<_, PostgresStorageError>(())
    })
    .await
    .unwrap();
    let before = fixture.storage.capture_history_lifecycle().await.unwrap();
    let intent_id = before.control().authority().expiry().intents()[0].identity();
    let change = match operation {
        "hold" => before
            .control()
            .create_hold(
                HistoryRetentionHold::new(Uuid::new_v4(), key, now, None, principal).unwrap(),
                before.fence(),
            )
            .unwrap(),
        "policy" => before
            .control()
            .apply(
                HistoryPolicyApplyRequest::new(
                    HistoryPolicyChange::new(
                        key,
                        before
                            .control()
                            .authority()
                            .policies()
                            .current_revision(key),
                        HistoryPolicyBinding::Indefinite,
                        HistoryPolicyApplication::Prospective,
                    )
                    .unwrap(),
                    None,
                ),
                principal,
                before.fence(),
                now,
            )
            .unwrap(),
        "cancel" => before
            .control()
            .cancel_expiry(intent_id, principal, before.fence(), now)
            .unwrap(),
        _ => unreachable!(),
    };
    let expected = change.next().clone();
    let result = fixture
        .storage
        .run_in_scope(scope, fixture.storage.commit_history_lifecycle(change))
        .await;
    let after = fixture.storage.capture_history_lifecycle().await.unwrap();
    fixture
        .storage
        .unregister_runtime(registration)
        .await
        .unwrap();
    fixture.restore().await;
    if accepted {
        result.unwrap();
        assert_eq!(after.control(), &expected);
        assert_eq!(
            after.control().authority().expiry(),
            before.control().authority().expiry()
        );
        assert_eq!(
            after.control().authority().revision().value(),
            before.control().authority().revision().value() + 1
        );
    } else {
        assert!(matches!(result, Err(error) if error.kind()==StorageErrorKind::Conflict));
        assert_eq!(after.control(), before.control());
    }
}

// Real trigger-created object history, followed by owner-only missing-copy
// fixtures. This qualifies selection/absence, not protected expiry execution.
#[rstest::rstest]
#[case::expired_removed(true, false, Some(StorageErrorKind::NotFound))]
#[case::unproved_gap(false, false, Some(StorageErrorKind::Unavailable))]
#[case::before_creation(true, true, None)]
#[tokio::test]
async fn original_object_binding_rejects_missing_payload_as_ordinary_absence(
    #[case] retained: bool,
    #[case] before_creation: bool,
    #[case] expected_error: Option<StorageErrorKind>,
) {
    use diesel::sql_types::{BigInt, Integer, Timestamptz};
    use hubuum_domain::{ClassId, ObjectId};
    #[derive(QueryableByName)]
    struct Closed {
        #[diesel(sql_type = Integer)]
        id: i32,
        #[diesel(sql_type = Integer)]
        class_id: i32,
        #[diesel(sql_type = BigInt)]
        history_id: i64,
        #[diesel(sql_type = Jsonb)]
        logical_identity: Value,
        #[diesel(sql_type = Timestamptz)]
        valid_from: chrono::DateTime<Utc>,
        #[diesel(sql_type = Timestamptz)]
        valid_to: chrono::DateTime<Utc>,
        #[diesel(sql_type = Timestamptz)]
        created_at: chrono::DateTime<Utc>,
    }
    let _guard = CONTROL_LOCK.lock().await;
    let fixture = Fixture::new().await;
    let name = format!("history_object_absence_{}", Uuid::new_v4().simple());
    let intent = Uuid::new_v4();
    let closed = with_transaction(&fixture.owner_pool, async |connection| {
        owner(connection).await?;
        sql_query("INSERT INTO public.hubuumclass(name,description,collection_id) SELECT $1,'',id FROM public.collections WHERE parent_collection_id IS NULL")
            .bind::<Text,_>(&name).execute(connection).await?;
        sql_query("INSERT INTO public.hubuumobject(name,description,collection_id,hubuum_class_id) SELECT $1,'before',collection_id,id FROM public.hubuumclass WHERE name=$1")
            .bind::<Text,_>(&name).execute(connection).await?;
        sql_query("UPDATE public.hubuumobject SET description='after' WHERE name=$1")
            .bind::<Text,_>(&name).execute(connection).await?;
        let closed = sql_query("SELECT id,hubuum_class_id AS class_id,history_id,logical_identity,valid_from,valid_to,created_at AT TIME ZONE 'UTC' AS created_at FROM public.hubuumobject_history WHERE name=$1 AND op='I'")
            .bind::<Text,_>(&name).get_result::<Closed>(connection).await?;
        let original: StorageIdentityEvidence = serde_json::from_value(closed.logical_identity.clone()).unwrap();
        let target = original.subject().unwrap().record_identity().unwrap();
        if retained {
            sql_query("INSERT INTO hubuum_control.history_expiry_members(family,resource_uuid,version_uuid,intent_uuid,valid_from,valid_to) VALUES('object',$1,$2,$3,$4,$5)")
                .bind::<SqlUuid,_>(Uuid::from_bytes(*target.resource().as_bytes()))
                .bind::<SqlUuid,_>(Uuid::from_bytes(*target.version().as_bytes()))
                .bind::<SqlUuid,_>(intent).bind::<Timestamptz,_>(closed.valid_from)
                .bind::<Timestamptz,_>(closed.valid_to).execute(connection).await?;
        }
        sql_query("DELETE FROM public.hubuumobject_history WHERE history_id=$1")
            .bind::<BigInt,_>(closed.history_id).execute(connection).await?;
        Ok::<_,PostgresStorageError>(closed)
    }).await.unwrap();
    let original: StorageIdentityEvidence =
        serde_json::from_value(closed.logical_identity).unwrap();
    let resource = original
        .subject()
        .unwrap()
        .record_identity()
        .unwrap()
        .resource();
    let at = if before_creation {
        closed.created_at - Duration::microseconds(1)
    } else {
        closed.valid_from
    };
    let result = fixture
        .storage
        .get_object_history_as_of(
            StorageObjectHistoryAsOfQuery::new(
                ObjectId::new(closed.id).unwrap(),
                ClassId::new(closed.class_id).unwrap(),
                at,
            )
            .for_resource(resource),
        )
        .await;
    with_transaction(&fixture.owner_pool, async |connection| {
        owner(connection).await?;
        sql_query("DELETE FROM hubuum_control.history_expiry_members WHERE intent_uuid=$1")
            .bind::<SqlUuid, _>(intent)
            .execute(connection)
            .await?;
        sql_query("DELETE FROM public.hubuumobject WHERE id=$1")
            .bind::<Integer, _>(closed.id)
            .execute(connection)
            .await?;
        sql_query("DELETE FROM public.hubuumclass WHERE id=$1")
            .bind::<Integer, _>(closed.class_id)
            .execute(connection)
            .await?;
        sql_query("DELETE FROM public.hubuumobject_history WHERE id=$1")
            .bind::<Integer, _>(closed.id)
            .execute(connection)
            .await?;
        sql_query("DELETE FROM public.class_schema_history WHERE class_id=$1")
            .bind::<Integer, _>(closed.class_id)
            .execute(connection)
            .await?;
        sql_query("DELETE FROM public.hubuumclass_history WHERE id=$1")
            .bind::<Integer, _>(closed.class_id)
            .execute(connection)
            .await?;
        Ok::<_, PostgresStorageError>(())
    })
    .await
    .unwrap();
    fixture.restore().await;
    match expected_error {
        Some(expected) => assert!(matches!(result, Err(error) if error.kind()==expected)),
        None => assert!(matches!(result, Ok(None))),
    }
}

// Protocol admission only. These tests deliberately manufacture payload-free
// exclusions as owner fixtures; they do not enable or qualify native expiry CAS.
#[rstest::rstest]
#[case::issued_review(false)]
#[case::publication_generation(true)]
#[tokio::test]
async fn publication_registry_rollback_preserves_revision_zero_obligations(
    #[case] generation_only: bool,
) {
    let _guard = CONTROL_LOCK.lock().await;
    let storage = PostgresStorage::unobserved(integration_test_pool(1));
    let snapshot = storage.capture_history_lifecycle().await.unwrap();
    let preview = HistoryPolicyPreview::issue(
        PrincipalIdentity::generate(),
        proposal(&snapshot),
        snapshot.fence(),
        snapshot.observed_at(),
        HistoryPreviewImpact::new(HistoryPreviewCoverage::Bounded, 0, 0, Vec::new()).unwrap(),
    )
    .unwrap();
    let issued = snapshot
        .control()
        .issue_preview(preview, snapshot.fence())
        .unwrap();
    let pool = integration_test_migration_pool(1);
    with_transaction(&pool, async |connection| {
        owner(connection).await?;
        connection.batch_execute("SAVEPOINT history_rollback_fixture").await?;
        if generation_only {
            sql_query("UPDATE hubuum_control.history_publication_control SET generation=generation+1 WHERE id=1")
                .execute(connection).await?;
        } else {
            sql_query("UPDATE hubuum_control.history_lifecycle_control SET material=$1 WHERE id=1")
                .bind::<Jsonb, _>(serde_json::to_value(issued.next()).unwrap())
                .execute(connection).await?;
        }
        connection.batch_execute("SAVEPOINT rejected_history_rollback").await?;
        let rejected = connection.batch_execute(include_str!("../migrations/2026-11-03-000003_history_lifecycle/down.sql")).await;
        assert!(rejected.unwrap_err().to_string().contains("Retained history lifecycle authority prevents rollback"));
        connection.batch_execute("ROLLBACK TO SAVEPOINT history_rollback_fixture").await?;
        Ok::<_, PostgresStorageError>(())
    }).await.unwrap();
}

mod runtime_protocol {
    use super::*;
    use diesel::result::{DatabaseErrorKind, Error as DieselError};
    use diesel::sql_types::{Bool, Integer};
    use rstest::rstest;
    use tokio::time::{Duration as WaitDuration, sleep, timeout};

    #[derive(QueryableByName)]
    struct Gate {
        #[diesel(sql_type = Bool)]
        allowed: bool,
    }
    #[derive(QueryableByName)]
    struct Backend {
        #[diesel(sql_type = Integer)]
        pid: i32,
    }
    async fn register(connection: &mut PostgresConnection, id: Uuid) -> Result<bool, DieselError> {
        Ok(
            sql_query("SELECT public.hubuum_register_runtime($1,$2) AS allowed")
                .bind::<SqlUuid, _>(id)
                .bind::<SqlUuid, _>(id)
                .get_result::<Gate>(connection)
                .await?
                .allowed,
        )
    }
    async fn fenced(connection: &mut PostgresConnection) -> Result<bool, DieselError> {
        Ok(sql_query("SELECT public.hubuum_history_producers_fenced((SELECT incarnation FROM hubuum_control.application_control WHERE id=1),(SELECT (material->>'revision')::BIGINT FROM hubuum_control.deletion_authority WHERE id=1)) AS allowed")
            .get_result::<Gate>(connection).await?.allowed)
    }
    #[tokio::test]
    async fn publication_registry_legacy_archive_hook_refuses_online_runtime_advertisement() {
        let _guard = CONTROL_LOCK.lock().await;
        let pool = integration_test_migration_pool(1);
        with_transaction(&pool, async |connection| {
            owner(connection).await?;
            let installed = sql_query("SELECT to_regprocedure('public.hubuum_archive_history_requires_legacy_runtime()') IS NOT NULL AS allowed")
                .get_result::<Gate>(connection).await?.allowed;
            if !installed {
                // Transaction-local fixture; never replace a landed archive hook.
                connection.batch_execute("CREATE FUNCTION public.hubuum_archive_history_requires_legacy_runtime() RETURNS BOOLEAN LANGUAGE SQL IMMUTABLE AS 'SELECT TRUE'").await?;
            }
            let identity = Uuid::new_v4();
            let allowed = sql_query("SELECT public.hubuum_register_history_runtime($1,$2,NULL::JSONB) AS allowed")
                .bind::<SqlUuid, _>(identity).bind::<SqlUuid, _>(Uuid::new_v4())
                .get_result::<Gate>(connection).await?.allowed;
            assert!(!allowed, "An installed legacy-only archive reader cannot advertise OnlineV1");
            if !installed {
                connection.batch_execute("DROP FUNCTION public.hubuum_archive_history_requires_legacy_runtime()").await?;
            }
            Ok::<_, PostgresStorageError>(())
        }).await.unwrap();
    }
    async fn exclude(connection: &mut PostgresConnection, id: Uuid) {
        // The authority tuple must change with every admitted intent. A lock
        // alone does not invalidate an already established RR snapshot.
        sql_query(
            "UPDATE hubuum_control.history_lifecycle_control SET material=material WHERE id=1",
        )
        .execute(connection)
        .await
        .unwrap();
        sql_query("INSERT INTO hubuum_control.history_expiry_members(family,resource_uuid,version_uuid,intent_uuid,valid_from) VALUES('object',$1,$1,$1,clock_timestamp())")
            .bind::<SqlUuid, _>(id).execute(connection).await.unwrap();
    }
    async fn blocked(connection: &mut PostgresConnection, pid: i32) {
        timeout(WaitDuration::from_secs(5), async {
            loop {
                let waiting = sql_query("SELECT cardinality(pg_blocking_pids($1))>0 AS allowed")
                    .bind::<Integer, _>(pid)
                    .get_result::<Gate>(connection)
                    .await
                    .unwrap();
                if waiting.allowed {
                    break;
                }
                sleep(WaitDuration::from_millis(10)).await;
            }
        })
        .await
        .expect("reader must actually wait on the writer's authority lock");
    }

    #[tokio::test]
    async fn mixed_fleet_blocks_fencing_and_retained_exclusion_refuses_legacy_startup() {
        let _guard = CONTROL_LOCK.lock().await;
        let fixture = Fixture::new().await;
        let aware = fixture
            .storage
            .register_history_runtime(None)
            .await
            .unwrap();
        let legacy = fixture.storage.register_runtime().await.unwrap();
        let mixed = with_transaction(&fixture.owner_pool, async |connection| {
            owner(connection).await?;
            Ok::<_, PostgresStorageError>(fenced(connection).await?)
        })
        .await
        .unwrap();
        fixture.storage.unregister_runtime(legacy).await.unwrap();
        let upgraded = with_transaction(&fixture.owner_pool, async |connection| {
            owner(connection).await?;
            Ok::<_, PostgresStorageError>(fenced(connection).await?)
        })
        .await
        .unwrap();
        let exclusion = Uuid::new_v4();
        with_transaction(&fixture.owner_pool, async |connection| {
            owner(connection).await?;
            exclude(connection, exclusion).await;
            Ok::<_, PostgresStorageError>(())
        })
        .await
        .unwrap();
        let refused = fixture.storage.register_runtime().await;
        let allowed = fixture.storage.register_history_runtime(None).await;
        fixture.storage.unregister_runtime(aware).await.unwrap();
        let allowed_startup = allowed.is_ok();
        if let Ok(registration) = allowed {
            fixture
                .storage
                .unregister_runtime(registration)
                .await
                .unwrap();
        }
        with_transaction(&fixture.owner_pool, async |connection| {
            owner(connection).await?;
            sql_query("DELETE FROM hubuum_control.history_expiry_members WHERE intent_uuid=$1")
                .bind::<SqlUuid, _>(exclusion)
                .execute(connection)
                .await?;
            Ok::<_, PostgresStorageError>(())
        })
        .await
        .unwrap();
        fixture.restore().await;
        assert!(!mixed);
        assert!(upgraded);
        assert!(matches!(refused, Err(error) if error.kind()==StorageErrorKind::Unavailable));
        assert!(allowed_startup);
    }

    #[rstest]
    #[case::rc_initial_intent_first(false, false, true)]
    #[case::rr_initial_intent_first(true, false, true)]
    #[case::rc_retained_intent_first(false, true, true)]
    #[case::rr_retained_intent_first(true, true, true)]
    #[case::rc_initial_registration_first(false, false, false)]
    #[case::rr_initial_registration_first(true, false, false)]
    #[case::rc_retained_registration_first(false, true, false)]
    #[case::rr_retained_registration_first(true, true, false)]
    #[tokio::test]
    async fn old_snapshot_cannot_admit_a_legacy_producer_across_protocol_transition(
        #[case] repeatable_read: bool,
        #[case] retained: bool,
        #[case] intent_first: bool,
    ) {
        let _guard = CONTROL_LOCK.lock().await;
        let fixture = Fixture::new().await;
        let pool = integration_test_migration_pool(3);
        let mut writer = pool.get().await.unwrap();
        let mut reader = pool.get().await.unwrap();
        let mut observer = pool.get().await.unwrap();
        let identity = Uuid::new_v4();
        let prior = Uuid::new_v4();
        if retained {
            writer.batch_execute("BEGIN").await.unwrap();
            owner(&mut writer).await.unwrap();
            exclude(&mut writer, prior).await;
            writer.batch_execute("COMMIT").await.unwrap();
        }
        reader
            .batch_execute(if repeatable_read {
                "BEGIN ISOLATION LEVEL REPEATABLE READ"
            } else {
                "BEGIN ISOLATION LEVEL READ COMMITTED"
            })
            .await
            .unwrap();
        owner(&mut reader).await.unwrap();
        // Establish MVCC without taking an authority lock before the writer.
        sql_query("SELECT public.hubuum_history_lifecycle_capture() AS material")
            .get_result::<Material>(&mut reader)
            .await
            .unwrap();
        let pid = sql_query("SELECT pg_backend_pid() AS pid")
            .get_result::<Backend>(&mut reader)
            .await
            .unwrap()
            .pid;
        writer.batch_execute("BEGIN").await.unwrap();
        owner(&mut writer).await.unwrap();
        if intent_first {
            assert!(fenced(&mut writer).await.unwrap());
            exclude(&mut writer, identity).await;
        } else {
            assert_eq!(register(&mut writer, identity).await.unwrap(), !retained);
        }
        let result = {
            let pending = async {
                if intent_first {
                    register(&mut reader, identity).await
                } else {
                    fenced(&mut reader).await
                }
            };
            tokio::pin!(pending);
            tokio::select! {
                result = &mut pending => panic!("gate completed before writer released: {result:?}"),
                () = blocked(&mut observer, pid) => {}
            }
            writer.batch_execute("COMMIT").await.unwrap();
            timeout(WaitDuration::from_secs(5), &mut pending)
                .await
                .expect("gate must settle after writer commits")
        };
        reader.batch_execute("ROLLBACK").await.unwrap();
        writer.batch_execute("BEGIN").await.unwrap();
        owner(&mut writer).await.unwrap();
        sql_query(
            "DELETE FROM hubuum_control.runtime_registrations WHERE id=$1 AND ownership_token=$1",
        )
        .bind::<SqlUuid, _>(identity)
        .execute(&mut writer)
        .await
        .unwrap();
        sql_query("DELETE FROM hubuum_control.history_expiry_members WHERE intent_uuid IN ($1,$2)")
            .bind::<SqlUuid, _>(identity)
            .bind::<SqlUuid, _>(prior)
            .execute(&mut writer)
            .await
            .unwrap();
        writer.batch_execute("COMMIT").await.unwrap();
        fixture.restore().await;
        if repeatable_read {
            assert!(matches!(
                result,
                Err(DieselError::DatabaseError(
                    DatabaseErrorKind::SerializationFailure,
                    _
                ))
            ));
        } else {
            assert_eq!(result.unwrap(), !intent_first && retained);
        }
    }
}

mod read_predicates {
    use super::*;
    use diesel::sql_types::{Bool, Nullable};
    use hubuum_domain::{IdentityReference, ResourceIdentity, VersionIdentity};
    use hubuum_storage_postgres::with_connection;
    use rstest::rstest;

    #[derive(QueryableByName)]
    struct Visibility {
        #[diesel(sql_type = Bool)]
        visible: bool,
    }
    #[rstest]
    #[case::before("1999-12-31T23:59:59Z", false, false)]
    #[case::inclusive_start("2000-01-01T00:00:00Z", false, true)]
    #[case::exclusive_end("2000-01-02T00:00:00Z", false, false)]
    #[case::other_resource("2000-01-01T00:00:00Z", true, false)]
    #[tokio::test]
    async fn absent_copy_predicate_uses_original_resource_binding(
        #[case] at: &str,
        #[case] unrelated: bool,
        #[case] expired: bool,
    ) {
        let _guard = CONTROL_LOCK.lock().await;
        let fixture = Fixture::new().await;
        let resource = Uuid::new_v4();
        let intent = Uuid::new_v4();
        with_transaction(&fixture.owner_pool, async |connection| {
            owner(connection).await?;
            // Predicate fixture only; this neither admits an intent nor deletes
            // payloads, and is not native executor qualification.
            sql_query("INSERT INTO hubuum_control.history_expiry_members(family,resource_uuid,version_uuid,intent_uuid,valid_from,valid_to) VALUES('object',$1,$2,$2,'2000-01-01','2000-01-02')")
                .bind::<SqlUuid, _>(resource).bind::<SqlUuid, _>(intent)
                .execute(connection).await?;
            Ok::<_, PostgresStorageError>(())
        }).await.unwrap();
        let requested = if unrelated { Uuid::new_v4() } else { resource };
        let result = with_connection(&integration_test_pool(1), async |connection| {
            Ok::<_, PostgresStorageError>(sql_query("SELECT public.hubuum_history_requested_as_of_visible('object',$1,$2::TIMESTAMPTZ) AS visible")
                .bind::<SqlUuid, _>(requested).bind::<Text, _>(at)
                .get_result::<Visibility>(connection).await?.visible)
        }).await;
        with_transaction(&fixture.owner_pool, async |connection| {
            owner(connection).await?;
            sql_query("DELETE FROM hubuum_control.history_expiry_members WHERE intent_uuid=$1")
                .bind::<SqlUuid, _>(intent)
                .execute(connection)
                .await?;
            Ok::<_, PostgresStorageError>(())
        })
        .await
        .unwrap();
        assert_eq!(result.unwrap(), !expired);
    }

    fn derived_version() -> VersionIdentity {
        let mut bytes = *VersionIdentity::generate().as_bytes();
        bytes[6] = (bytes[6] & 0x0f) | 0x80;
        VersionIdentity::from_bytes(bytes).unwrap()
    }
    #[rstest]
    #[case::original("original", false)]
    #[case::known_alias("alias", false)]
    #[case::new_authored_version("authored", true)]
    #[case::unrelated_source("other_source", true)]
    #[case::unknown_source("unknown", false)]
    #[case::inherited_unknown("inherited_unknown", false)]
    #[case::other_resource("other_resource", true)]
    #[case::other_family("other_family", true)]
    #[case::missing_evidence("missing", false)]
    #[case::missing_other_family("missing_other_family", true)]
    #[tokio::test]
    async fn runtime_predicate_preserves_retained_source_version_exclusions(
        #[case] mode: &str,
        #[case] expected_visible: bool,
    ) {
        let _guard = CONTROL_LOCK.lock().await;
        let fixture = Fixture::new().await;
        let resource = ResourceIdentity::generate();
        let version = VersionIdentity::generate();
        let intent = Uuid::new_v4();
        let copy_resource = if mode == "other_resource" {
            ResourceIdentity::generate()
        } else {
            resource
        };
        let copy_version = match mode {
            "authored" | "other_source" => VersionIdentity::generate(),
            "unknown" | "inherited_unknown" => derived_version(),
            _ => version,
        };
        let evidence = StorageIdentityEvidence::try_new(
            Some(StorageIdentitySubject::Resource {
                identity: IdentityReference::Known {
                    identity: copy_resource,
                },
                version: copy_version,
            }),
            Default::default(),
        )
        .unwrap();
        let evidence = match mode {
            "alias" | "other_source" | "other_resource" | "inherited_unknown" => {
                evidence.at_restore_baseline(derived_version()).unwrap()
            }
            _ => evidence,
        };
        with_transaction(&fixture.owner_pool, async |connection| {
            owner(connection).await?;
            // Predicate fixture only: generic journal admission remains disabled.
            sql_query("INSERT INTO hubuum_control.history_expiry_members(family,resource_uuid,version_uuid,intent_uuid,valid_from,valid_to) VALUES('object',$1,$2,$3,'2000-01-01','2000-01-02')")
                .bind::<SqlUuid, _>(Uuid::from_bytes(*resource.as_bytes()))
                .bind::<SqlUuid, _>(Uuid::from_bytes(*version.as_bytes()))
                .bind::<SqlUuid, _>(intent).execute(connection).await?;
            Ok::<_, PostgresStorageError>(())
        }).await.unwrap();
        let runtime = integration_test_pool(1);
        let family = if matches!(mode, "other_family" | "missing_other_family") {
            "class"
        } else {
            "object"
        };
        let evidence = (!matches!(mode, "missing" | "missing_other_family"))
            .then(|| serde_json::to_value(evidence).unwrap());
        let result = with_connection(&runtime, async |connection| {
            Ok::<_, PostgresStorageError>(
                sql_query("SELECT public.hubuum_history_version_visible($1,$2) AS visible")
                    .bind::<Text, _>(family)
                    .bind::<Nullable<Jsonb>, _>(evidence)
                    .get_result::<Visibility>(connection)
                    .await?
                    .visible,
            )
        })
        .await;
        with_transaction(&fixture.owner_pool, async |connection| {
            owner(connection).await?;
            sql_query("DELETE FROM hubuum_control.history_expiry_members WHERE intent_uuid=$1")
                .bind::<SqlUuid, _>(intent)
                .execute(connection)
                .await?;
            Ok::<_, PostgresStorageError>(())
        })
        .await
        .unwrap();
        fixture.restore().await;
        assert_eq!(result.unwrap(), expected_visible);
    }
}
