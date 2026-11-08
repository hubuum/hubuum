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
async fn combined_archive_allows_online_registration() {
    let _guard = CONTROL_LOCK.lock().await;
    let storage = PostgresStorage::unobserved(integration_test_pool(2));
    let legacy = storage.register_runtime().await.unwrap();
    let permit = storage
        .capture_application_execution(Some(legacy.owner()))
        .await
        .unwrap();
    let archive = storage
        .run_in_scope(
            StorageExecutionScope::default().with_application_access(permit),
            storage.admit_history_archive_execution(),
        )
        .await
        .unwrap();
    let permit = archive.permit().clone();
    let catalog = storage
        .run_in_scope(
            permit.execution_scope().clone(),
            storage.history_archive_catalog(hubuum_domain::ResourceIdentity::generate(), &permit),
        )
        .await
        .unwrap();
    let pool = integration_test_migration_pool(1);
    let before = with_transaction(&pool, async |connection| {
        owner(connection).await?;
        sql_query("SELECT jsonb_build_object('application',public.hubuum_application_control_read(),'owners',(SELECT count(*) FROM hubuum_control.runtime_registrations)) AS material")
            .get_result::<Material>(connection).await.map_err(PostgresStorageError::from)
    }).await.unwrap().material;
    let attempt = storage.register_history_runtime(None).await;
    let rejection = match attempt {
        Ok(unexpected) => {
            storage.unregister_runtime(unexpected).await.unwrap();
            None
        }
        Err(error) => Some(error.kind()),
    };
    let after = with_transaction(&pool, async |connection| {
        owner(connection).await?;
        sql_query("SELECT jsonb_build_object('application',public.hubuum_application_control_read(),'owners',(SELECT count(*) FROM hubuum_control.runtime_registrations)) AS material")
            .get_result::<Material>(connection).await.map_err(PostgresStorageError::from)
    }).await.unwrap().material;
    let settled = archive
        .after_drained(std::future::ready(Ok(())), std::future::ready(Ok(())))
        .await
        .unwrap();
    storage
        .settle_history_producer(settled.producer())
        .await
        .unwrap();
    storage
        .settle_history_publication(settled.into_publication())
        .await
        .unwrap();
    storage.unregister_runtime(legacy).await.unwrap();
    assert!(catalog.versions().is_empty());
    assert_eq!(rejection, None);
    assert_eq!(before, after);
}

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
        let rejected = connection.batch_execute(include_str!("../migrations/2026-11-03-000005_history_lifecycle/down.sql")).await;
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
            // Simulate an older archive reader within this transaction only.
            connection.batch_execute("CREATE OR REPLACE FUNCTION public.hubuum_archive_history_requires_legacy_runtime() RETURNS BOOLEAN LANGUAGE SQL IMMUTABLE SECURITY DEFINER SET search_path=pg_catalog AS 'SELECT TRUE'").await?;
            let identity = Uuid::new_v4();
            let allowed = sql_query("SELECT public.hubuum_register_history_runtime($1,$2,NULL::JSONB) AS allowed")
                .bind::<SqlUuid, _>(identity).bind::<SqlUuid, _>(Uuid::new_v4())
                .get_result::<Gate>(connection).await?.allowed;
            assert!(!allowed, "An installed legacy-only archive reader cannot advertise OnlineV1");
            connection.batch_execute("CREATE OR REPLACE FUNCTION public.hubuum_archive_history_requires_legacy_runtime() RETURNS BOOLEAN LANGUAGE SQL IMMUTABLE SECURITY DEFINER SET search_path=pg_catalog AS 'SELECT FALSE'").await?;
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
    #[rstest]
    #[case::rc_intent_first(false, true)]
    #[case::rr_intent_first(true, true)]
    #[case::rc_registration_first(false, false)]
    #[case::rr_registration_first(true, false)]
    #[tokio::test]
    async fn real_first_intent_and_legacy_startup_have_one_winner(
        #[case] repeatable_read: bool,
        #[case] intent_first: bool,
    ) {
        let _guard = CONTROL_LOCK.lock().await;
        let (fixture, key, collection) =
            expiry_fixture(StorageIdentityHistoryKind::Object, 32).await;
        let runtime = fixture
            .storage
            .register_history_runtime(None)
            .await
            .unwrap();
        let application = fixture
            .storage
            .capture_application_execution(Some(runtime.owner()))
            .await
            .unwrap();
        let before = fixture.storage.capture_history_lifecycle().await.unwrap();
        let batch = Uuid::new_v4();
        let request = HistoryExpiryAdmissionRequest::new(
            batch,
            Uuid::new_v4(),
            HistoryExpiryOrigin::Scheduled { initiator: None },
            HistoryLifecycleScan::new(key, 1).unwrap(),
            before.fence(),
        )
        .unwrap();
        fixture
            .storage
            .run_in_scope(
                StorageExecutionScope::default().with_application_access(application),
                fixture.storage.admit_history_expiry(request),
            )
            .await
            .unwrap()
            .unwrap();
        let admitted = fixture.storage.capture_history_lifecycle().await.unwrap();
        let event_ids = admitted
            .control()
            .audit_events_since(before.control())
            .unwrap()
            .into_iter()
            .map(|event| event.event_id().as_uuid())
            .collect::<Vec<_>>();
        // Reuse the actual adapter-selected native proof. Owner-only reset is
        // confined to this fixture; the race below executes real admission DDL.
        with_transaction(&fixture.owner_pool, async |connection| {
            owner(connection).await?;
            connection
                .batch_execute("SET LOCAL events.allow_purge='on'")
                .await?;
            sql_query("DELETE FROM public.events WHERE event_id=ANY($1)")
                .bind::<Array<SqlUuid>, _>(event_ids)
                .execute(connection)
                .await?;
            sql_query("DELETE FROM hubuum_control.history_expiry_members WHERE intent_uuid=$1")
                .bind::<SqlUuid, _>(batch)
                .execute(connection)
                .await?;
            sql_query("UPDATE hubuum_control.history_lifecycle_control SET material=$1 WHERE id=1")
                .bind::<Jsonb, _>(serde_json::to_value(before.control()).unwrap())
                .execute(connection)
                .await?;
            Ok::<_, PostgresStorageError>(())
        })
        .await
        .unwrap();
        let expected = serde_json::to_value(before.control()).unwrap();
        let replacement = serde_json::to_value(admitted.control()).unwrap();
        let invoke = async |connection: &mut PostgresConnection| -> Result<bool, DieselError> {
            Ok(
                sql_query("SELECT public.hubuum_history_expiry_admit($1,$2,$3,$4) AS allowed")
                    .bind::<Jsonb, _>(&expected)
                    .bind::<Jsonb, _>(&replacement)
                    .bind::<SqlUuid, _>(before.fence().incarnation())
                    .bind::<diesel::sql_types::BigInt, _>(
                        before.fence().deletion_revision().value() as i64,
                    )
                    .get_result::<Gate>(connection)
                    .await?
                    .allowed,
            )
        };
        let pool = integration_test_migration_pool(3);
        let mut writer = pool.get().await.unwrap();
        let mut reader = pool.get().await.unwrap();
        let mut observer = pool.get().await.unwrap();
        reader
            .batch_execute(if repeatable_read {
                "BEGIN ISOLATION LEVEL REPEATABLE READ"
            } else {
                "BEGIN ISOLATION LEVEL READ COMMITTED"
            })
            .await
            .unwrap();
        owner(&mut reader).await.unwrap();
        sql_query("SELECT public.hubuum_history_lifecycle_capture() AS material")
            .get_result::<Material>(&mut reader)
            .await
            .unwrap();
        let pid = sql_query("SELECT pg_backend_pid() AS pid")
            .get_result::<Backend>(&mut reader)
            .await
            .unwrap()
            .pid;
        let legacy = Uuid::new_v4();
        writer.batch_execute("BEGIN").await.unwrap();
        owner(&mut writer).await.unwrap();
        if intent_first {
            assert!(invoke(&mut writer).await.unwrap());
        } else {
            assert!(register(&mut writer, legacy).await.unwrap());
        }
        let result = {
            let pending = async {
                if intent_first {
                    register(&mut reader, legacy).await
                } else {
                    invoke(&mut reader).await
                }
            };
            tokio::pin!(pending);
            tokio::select! {result=&mut pending=>panic!("authority gate did not wait: {result:?}"),()=blocked(&mut observer,pid)=>{}}
            writer.batch_execute("COMMIT").await.unwrap();
            timeout(WaitDuration::from_secs(5), &mut pending)
                .await
                .expect("gate must finish after commit")
        };
        reader.batch_execute("ROLLBACK").await.unwrap();
        writer.batch_execute("BEGIN").await.unwrap();
        owner(&mut writer).await.unwrap();
        sql_query(
            "DELETE FROM hubuum_control.runtime_registrations WHERE id=$1 AND ownership_token=$1",
        )
        .bind::<SqlUuid, _>(legacy)
        .execute(&mut writer)
        .await
        .unwrap();
        writer.batch_execute("COMMIT").await.unwrap();
        fixture.storage.unregister_runtime(runtime).await.unwrap();
        cleanup_expiry_fixture(fixture, collection, batch).await;
        if repeatable_read {
            assert!(matches!(
                result,
                Err(DieselError::DatabaseError(
                    DatabaseErrorKind::SerializationFailure,
                    _
                ))
            ));
        } else {
            assert!(!result.unwrap());
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

// Read-gate fixture only. The owner installs retained authority and removes its
// exact closed setup row; no protected executor or first-removal proof is made.
#[rstest::rstest]
#[case::absent_copy("absent")]
#[case::missing_original_facts("missing_facts")]
#[case::stale_original_class("stale_class")]
#[tokio::test]
async fn native_population_gap_preserves_original_facts_after_copy_absence(
    #[case] condition: &str,
) {
    use diesel::sql_types::{BigInt, Integer};
    use hubuum_domain::{ClassId, ObjectId, ResourceIdentity};
    use hubuum_query::QueryOptions;

    #[derive(QueryableByName)]
    struct Id {
        #[diesel(sql_type = Integer)]
        id: i32,
    }
    #[derive(QueryableByName)]
    struct Measured {
        #[diesel(sql_type = BigInt)]
        bytes: i64,
    }
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
    let key = HistoryPolicyKey::new(
        StorageIdentityHistoryKind::Object,
        HistoryPolicyScope::Deployment,
    )
    .unwrap();
    let principal = PrincipalIdentity::generate();
    let change = original
        .control()
        .apply(
            HistoryPolicyApplyRequest::new(
                HistoryPolicyChange::new(
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
                .unwrap(),
                None,
            ),
            principal,
            original.fence(),
            original.observed_at(),
        )
        .unwrap();
    fixture
        .storage
        .run_in_scope(
            scope.clone(),
            fixture.storage.commit_history_lifecycle(change),
        )
        .await
        .unwrap();
    let (class, object) = with_transaction(&fixture.owner_pool, async |connection| {
        owner(connection).await?;
        let class = sql_query("INSERT INTO public.hubuumclass(name,description,collection_id) VALUES($1,'population gap fixture',1) RETURNING id")
            .bind::<Text,_>(format!("population_gap_{}",Uuid::new_v4().simple()))
            .get_result::<Id>(connection).await?.id;
        let object = sql_query("INSERT INTO public.hubuumobject(name,description,collection_id,hubuum_class_id,data) VALUES('original_member','gap fixture',1,$1,'{}') RETURNING id")
            .bind::<Integer,_>(class).get_result::<Id>(connection).await?.id;
        Ok::<_,PostgresStorageError>((class,object))
    }).await.unwrap();
    // A separate committed mutation creates a real nonempty closed interval.
    with_transaction(&fixture.owner_pool, async |connection| {
        owner(connection).await?;
        sql_query("UPDATE public.hubuumobject SET name='current_member' WHERE id=$1")
            .bind::<Integer, _>(object)
            .execute(connection)
            .await?;
        Ok::<_, PostgresStorageError>(())
    })
    .await
    .unwrap();
    // Unbound setup read only; the query under test below uses a live V1 owner.
    let row = fixture
        .storage
        .list_object_history(StorageObjectHistoryListQuery::new(
            ObjectId::new(object).unwrap(),
            ClassId::new(class).unwrap(),
            QueryOptions::new(vec![], vec![], Some(10), None, false).unwrap(),
            StorageHistoryCollectionScope::All,
        ))
        .await
        .unwrap()
        .into_parts()
        .0
        .into_iter()
        .find(|row| row.clone().into_parts().1.valid_to().is_some())
        .unwrap();
    let metadata = row.clone().into_parts().1;
    let history_id = metadata.history_entry_id();
    let at = metadata.valid_from();
    let facts = HistoryObjectAuthorizationFacts::capture(row).unwrap();
    let target = facts.target();
    let selected = fixture.storage.capture_history_lifecycle().await.unwrap();
    let candidate = HistoryExpiryCandidate::new(
        StorageIdentityHistoryKind::Object,
        target,
        Some(HistoryRetentionAnchor::Closed(metadata.valid_to().unwrap())),
        vec![facts.attribution()],
        false,
    )
    .unwrap()
    .with_read_span(facts.read_span().clone())
    .unwrap();
    let candidate = if condition == "missing_facts" {
        candidate
    } else {
        candidate.with_object_authorization(facts.clone()).unwrap()
    };
    let input = with_transaction(&fixture.owner_pool, async |connection| {
        owner(connection).await?;
        Ok::<_,PostgresStorageError>(sql_query("SELECT octet_length(to_jsonb(h)::TEXT)::BIGINT AS bytes FROM public.hubuumobject_history h WHERE history_id=$1")
            .bind::<BigInt,_>(history_id.id()).get_result::<Measured>(connection).await?.bytes)
    }).await.unwrap();
    let mut selection = HistoryExpirySelection::new(
        Uuid::new_v4(),
        HistoryExpiryOrigin::Manual { principal },
        HistoryLifecycleScan::new(key, 1).unwrap(),
        &selected,
    )
    .unwrap();
    assert!(matches!(
        selection
            .consider(
                candidate,
                HistoryExpiryInputBytes::new(input as u64).unwrap(),
                &[]
            )
            .unwrap(),
        HistoryExpirySelectionOutcome::Selected
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
        sql_query("DELETE FROM public.hubuumobject_history WHERE history_id=$1")
            .bind::<BigInt, _>(history_id.id())
            .execute(connection)
            .await?;
        Ok::<_, PostgresStorageError>(())
    })
    .await
    .unwrap();
    let publication = fixture
        .storage
        .run_in_scope(
            scope.clone(),
            fixture
                .storage
                .admit_history_publication(HistoryPublicationKind::TaskOutput),
        )
        .await
        .unwrap();
    let original_class = facts.attribution().class().unwrap();
    let query = StorageClassObjectsAsOfQuery::try_new(
        ClassId::new(class).unwrap(),
        at,
        QueryOptions::new(vec![], vec![], Some(2), None, false).unwrap(),
    )
    .unwrap()
    .for_resource(if condition == "stale_class" {
        ResourceIdentity::generate()
    } else {
        original_class
    });
    let result = fixture
        .storage
        .run_in_scope(
            scope.with_history_read(publication.context()),
            fixture.storage.list_class_objects_as_of(query.clone()),
        )
        .await;
    let after = fixture.storage.capture_history_lifecycle().await.unwrap();
    // Registry-only fixture: all issued SQL has returned and no publisher exists.
    let settled = publication
        .after_drained(std::future::ready(Ok(())), std::future::ready(Ok(())))
        .await
        .unwrap();
    fixture
        .storage
        .settle_history_publication(settled)
        .await
        .unwrap();
    fixture
        .storage
        .unregister_runtime(registration)
        .await
        .unwrap();
    with_transaction(&fixture.owner_pool, async |connection| {
        owner(connection).await?;
        sql_query("DELETE FROM public.hubuumobject WHERE id=$1")
            .bind::<Integer, _>(object)
            .execute(connection)
            .await?;
        sql_query("DELETE FROM public.hubuumclass WHERE id=$1")
            .bind::<Integer, _>(class)
            .execute(connection)
            .await?;
        Ok::<_, PostgresStorageError>(())
    })
    .await
    .unwrap();
    fixture.restore().await;
    let error = result
        .err()
        .expect("Incomplete population must not return success");
    assert_eq!(error.kind(), StorageErrorKind::Unavailable);
    if condition == "absent" {
        let gap = error
            .history_population_gap_evidence()
            .expect("Exact original facts survive copy absence");
        assert_eq!(gap.facts(), &facts);
        assert!(gap.matches_query(&query));
        assert!(!format!("{error:?}").contains("original_member"));
    } else {
        assert!(error.history_population_gap_evidence().is_none());
    }
    let intents = after.control().authority().expiry().intents();
    assert_eq!(intents.len(), 1);
    assert!(
        intents[0].removals().is_empty(),
        "Fixture absence cannot create a first-removal receipt or clock"
    );
    assert_eq!(intents[0].manifest().members()[0].target(), target);
}

mod archive_producer_slots {
    use super::*;
    use diesel::result::{DatabaseErrorKind, Error as DieselError};
    use diesel::sql_types::{BigInt, Bool, Integer};
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
    async fn reserve(
        connection: &mut PostgresConnection,
        producer: &HistoryProducerOwner,
    ) -> Result<bool, DieselError> {
        let child = producer.context();
        reserve_parts(
            connection,
            child.parent(),
            child.id(),
            child.ownership_token(),
        )
        .await
    }
    async fn reserve_parts(
        connection: &mut PostgresConnection,
        parent: &HistoryReadContext,
        identity: Uuid,
        ownership: Uuid,
    ) -> Result<bool, DieselError> {
        let epoch = parent.epoch();
        let runtime = epoch.application().owner().unwrap();
        Ok(sql_query("SELECT public.hubuum_history_producer_admit($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) AS allowed")
            .bind::<SqlUuid,_>(identity).bind::<SqlUuid,_>(ownership)
            .bind::<SqlUuid,_>(parent.id()).bind::<SqlUuid,_>(parent.ownership_token())
            .bind::<SqlUuid,_>(runtime.id()).bind::<SqlUuid,_>(runtime.ownership_token())
            .bind::<SqlUuid,_>(epoch.history().incarnation())
            .bind::<BigInt,_>(epoch.history().deletion_revision().value() as i64)
            .bind::<BigInt,_>(epoch.history().authority_revision().value() as i64)
            .bind::<BigInt,_>(epoch.generation().value() as i64)
            .get_result::<Gate>(connection).await?.allowed)
    }
    async fn settle_raw(
        connection: &mut PostgresConnection,
        parent: &HistoryReadContext,
        identity: Uuid,
        ownership: Uuid,
    ) -> Result<bool, DieselError> {
        Ok(
            sql_query("SELECT public.hubuum_history_producer_settle($1,$2,$3) AS allowed")
                .bind::<SqlUuid, _>(parent.id())
                .bind::<SqlUuid, _>(identity)
                .bind::<SqlUuid, _>(ownership)
                .get_result::<Gate>(connection)
                .await?
                .allowed,
        )
    }
    async fn retire(
        connection: &mut PostgresConnection,
        parent: &HistoryReadContext,
    ) -> Result<bool, DieselError> {
        Ok(
            sql_query("SELECT public.hubuum_history_publication_settle($1,$2) AS allowed")
                .bind::<SqlUuid, _>(parent.id())
                .bind::<SqlUuid, _>(parent.ownership_token())
                .get_result::<Gate>(connection)
                .await?
                .allowed,
        )
    }
    async fn blocked(connection: &mut PostgresConnection, pid: i32) {
        timeout(WaitDuration::from_secs(5), async {
            loop {
                if sql_query("SELECT cardinality(pg_blocking_pids($1))>0 AS allowed")
                    .bind::<Integer, _>(pid)
                    .get_result::<Gate>(connection)
                    .await
                    .unwrap()
                    .allowed
                {
                    break;
                }
                sleep(WaitDuration::from_millis(10)).await;
            }
        })
        .await
        .expect("parent tuple must actually block the racing operation");
    }

    async fn archive_census(pool: &PostgresPool, runtime: Uuid) -> Value {
        with_transaction(pool, async |connection| {
            owner(connection).await?;
            Ok::<_, PostgresStorageError>(sql_query("SELECT jsonb_build_object('parents',(SELECT count(*) FROM hubuum_control.history_publication_owners WHERE runtime_id=$1),'children',(SELECT count(*) FROM hubuum_control.history_publication_producers c JOIN hubuum_control.history_publication_owners p ON p.id=c.publication_id WHERE p.runtime_id=$1),'admission',(SELECT admission_revision FROM hubuum_control.history_publication_control WHERE id=1)) AS material")
                .bind::<SqlUuid,_>(runtime).get_result::<Material>(connection).await?.material)
        }).await.unwrap()
    }

    #[rstest::rstest]
    #[case::commit(false)]
    #[case::connection_loss_rollback(true)]
    #[tokio::test]
    async fn archive_producer_atomic_permit_is_returned_only_after_commit(#[case] rollback: bool) {
        use hubuum_storage_postgres::test_support::terminate_backend;
        use hubuum_storage_postgres::{PostgresFaultController, PostgresFaultPoint};
        let _guard = CONTROL_LOCK.lock().await;
        let pool = integration_test_pool(3);
        let storage = PostgresStorage::unobserved(pool.clone());
        let registration = storage.register_runtime().await.unwrap();
        let application = storage
            .capture_application_execution(Some(registration.owner()))
            .await
            .unwrap();
        let scope = StorageExecutionScope::default().with_application_access(application);
        let inspect = integration_test_migration_pool(1);
        let before = archive_census(&inspect, registration.id()).await;
        let fault = PostgresFaultController::pausing(PostgresFaultPoint::TransactionBeforeCommit);
        let task_storage = storage.clone();
        let task_fault = fault.clone();
        let pending = tokio::spawn(async move {
            task_storage
                .run_in_scope_send(
                    scope,
                    task_fault.run(task_storage.admit_history_archive_execution()),
                )
                .await
        });
        let reached = timeout(WaitDuration::from_secs(5), fault.wait_until_reached())
            .await
            .expect("atomic admission reaches precommit seam");
        let not_returned = !pending.is_finished();
        let invisible = archive_census(&inspect, registration.id()).await;
        if rollback {
            terminate_backend(&pool, reached.backend_pid().unwrap())
                .await
                .unwrap();
        }
        fault.resume();
        let result = timeout(WaitDuration::from_secs(5), pending)
            .await
            .unwrap()
            .unwrap();
        let returned = result.is_ok();
        let committed = archive_census(&inspect, registration.id()).await;
        if let Ok(owner) = result {
            owner
                .permit()
                .validate_scope(owner.permit().execution_scope())
                .unwrap();
            let drained = owner
                .after_drained(std::future::ready(Ok(())), std::future::ready(Ok(())))
                .await
                .unwrap();
            storage
                .settle_history_producer(drained.producer())
                .await
                .unwrap();
            storage
                .settle_history_publication(drained.into_publication())
                .await
                .unwrap();
        }
        let after = archive_census(&inspect, registration.id()).await;
        storage.unregister_runtime(registration).await.unwrap();
        assert!(not_returned);
        assert_eq!(
            invisible, before,
            "uncommitted control slots must not escape"
        );
        assert_eq!(returned, !rollback);
        assert_eq!(committed["parents"], if rollback { 0 } else { 1 });
        assert_eq!(committed["children"], if rollback { 0 } else { 1 });
        if rollback {
            assert_eq!(
                committed, before,
                "rollback cannot leak a counter/slot update"
            );
        }
        assert_eq!(after["parents"], 0);
        assert_eq!(after["children"], 0);
    }

    #[tokio::test]
    async fn archive_producer_rejects_a_different_api_owner_and_cleans_up_after_settlement() {
        let _guard = CONTROL_LOCK.lock().await;
        let storage = PostgresStorage::unobserved(integration_test_pool(2));
        let user = storage
            .create_user(StorageUserCreate::new(
                None,
                format!("archive_scope_{}", Uuid::new_v4().simple()),
                "test-hash",
                None,
                None,
                hubuum_events_core::EventContext::system(),
            ))
            .await
            .unwrap()
            .into_value()
            .into_parts();
        let principal = hubuum_domain::PrincipalId::new(user.id().id()).unwrap();
        let registration = storage.register_runtime().await.unwrap();
        let application = storage
            .capture_application_execution(Some(registration.owner()))
            .await
            .unwrap();
        let first = storage
            .admit_api_request(principal, ApiRequestClass::History, 0)
            .await
            .unwrap();
        let second = storage
            .admit_api_request(principal, ApiRequestClass::History, 0)
            .await
            .unwrap();
        let scope = StorageExecutionScope::default()
            .with_application_access(application.clone())
            .with_api_request(first.clone());
        let publication = storage
            .run_in_scope(
                scope.clone(),
                storage.admit_history_publication(HistoryPublicationKind::ArchiveRead),
            )
            .await
            .unwrap();
        let parent = publication.context();
        let wrong = StorageExecutionScope::default()
            .with_application_access(application)
            .with_api_request(second.clone())
            .with_history_read(parent.clone());
        let refused = storage
            .run_in_scope(wrong, storage.admit_history_producer(parent.clone()))
            .await;
        let producer = storage
            .run_in_scope(
                scope.with_history_read(parent.clone()),
                storage.admit_history_producer(parent),
            )
            .await
            .unwrap();
        // Occupancy settlement is independent: completion must not reacquire it.
        storage.settle_api_request(first).await.unwrap();
        let child = producer
            .after_completed(std::future::ready(Ok(())))
            .await
            .unwrap();
        storage.settle_history_producer(&child).await.unwrap();
        let drained = publication
            .after_drained(std::future::ready(Ok(())), std::future::ready(Ok(())))
            .await
            .unwrap();
        storage.settle_history_publication(drained).await.unwrap();
        storage.settle_api_request(second).await.unwrap();
        storage.unregister_runtime(registration).await.unwrap();
        storage
            .delete_user(StorageUserDelete::new(
                user.id(),
                hubuum_events_core::EventContext::system(),
            ))
            .await
            .unwrap()
            .into_value();
        assert!(refused.is_err());
    }

    #[tokio::test]
    async fn archive_producer_child_identity_collision_does_not_lock_another_parent() {
        let _guard = CONTROL_LOCK.lock().await;
        let storage = PostgresStorage::unobserved(integration_test_pool(2));
        let registration = storage.register_runtime().await.unwrap();
        let permit = storage
            .capture_application_execution(Some(registration.owner()))
            .await
            .unwrap();
        let scope = StorageExecutionScope::default().with_application_access(permit);
        let first = storage
            .run_in_scope(
                scope.clone(),
                storage.admit_history_publication(HistoryPublicationKind::ArchiveRead),
            )
            .await
            .unwrap();
        let second = storage
            .run_in_scope(
                scope,
                storage.admit_history_publication(HistoryPublicationKind::ArchiveRead),
            )
            .await
            .unwrap();
        let a = first.context();
        let b = second.context();
        let child = Uuid::new_v4();
        let token_a = Uuid::new_v4();
        let token_b = Uuid::new_v4();
        let pool = integration_test_pool(2);
        let mut writer = pool.get().await.unwrap();
        let mut reader = pool.get().await.unwrap();
        writer.batch_execute("BEGIN").await.unwrap();
        assert!(
            reserve_parts(&mut writer, &a, child, token_a)
                .await
                .unwrap()
        );
        reader.batch_execute("BEGIN").await.unwrap();
        let independent = timeout(
            WaitDuration::from_secs(5),
            reserve_parts(&mut reader, &b, child, token_b),
        )
        .await
        .expect("same child nonce under a distinct parent must not wait on the held unique index")
        .unwrap();
        reader.batch_execute("COMMIT").await.unwrap();
        writer.batch_execute("COMMIT").await.unwrap();
        let wrong_parent = settle_raw(&mut reader, &a, child, token_b).await.unwrap();
        assert!(settle_raw(&mut writer, &a, child, token_a).await.unwrap());
        assert!(settle_raw(&mut reader, &b, child, token_b).await.unwrap());
        // Inspect the REAL migrated constraint/trigger surface, not DDL prose.
        for publication in [first, second] {
            let drained = publication
                .after_drained(std::future::ready(Ok(())), std::future::ready(Ok(())))
                .await
                .unwrap();
            storage.settle_history_publication(drained).await.unwrap();
        }
        storage.unregister_runtime(registration).await.unwrap();
        // Inspection is owner-only diagnostics, AFTER runtime work and exact
        // fixture settlement. It grants nothing to productive admission.
        let schema = with_transaction(&integration_test_migration_pool(1), async |connection| {
            owner(connection).await?;
            Ok::<_, PostgresStorageError>(sql_query("SELECT jsonb_build_object('unique',(SELECT jsonb_agg(pg_get_indexdef(indexrelid)) FROM pg_index WHERE indrelid='hubuum_control.history_publication_producers'::regclass AND indisunique),'fks',(SELECT count(*) FROM pg_constraint WHERE conrelid='hubuum_control.history_publication_producers'::regclass AND contype='f'),'triggers',(SELECT count(*) FROM pg_trigger WHERE tgrelid='hubuum_control.history_publication_producers'::regclass)) AS material").get_result::<Material>(connection).await.unwrap().material)
        }).await.unwrap();
        assert!(independent && !wrong_parent);
        let indexes = schema["unique"].as_array().unwrap();
        assert_eq!(indexes.len(), 1);
        assert!(indexes[0].as_str().unwrap().ends_with("(publication_id)"));
        assert_eq!(schema["fks"], 0);
        assert_eq!(schema["triggers"], 0);
    }

    #[rstest::rstest]
    #[case::rc(false)]
    #[case::rr(true)]
    #[tokio::test]
    async fn archive_producer_completed_slot_cannot_be_reassigned_by_an_old_snapshot(
        #[case] repeatable_read: bool,
    ) {
        let _guard = CONTROL_LOCK.lock().await;
        let storage = PostgresStorage::unobserved(integration_test_pool(2));
        let registration = storage.register_runtime().await.unwrap();
        let permit = storage
            .capture_application_execution(Some(registration.owner()))
            .await
            .unwrap();
        let scope = StorageExecutionScope::default().with_application_access(permit);
        let publication = storage
            .run_in_scope(
                scope,
                storage.admit_history_publication(HistoryPublicationKind::ArchiveRead),
            )
            .await
            .unwrap();
        let parent = publication.context();
        let producer = HistoryProducerOwner::generate(parent.clone());
        let replacement = HistoryProducerOwner::generate(parent.clone());
        let pool = integration_test_pool(2);
        let mut writer = pool.get().await.unwrap();
        let mut reader = pool.get().await.unwrap();
        reader
            .batch_execute(if repeatable_read {
                "BEGIN ISOLATION LEVEL REPEATABLE READ"
            } else {
                "BEGIN ISOLATION LEVEL READ COMMITTED"
            })
            .await
            .unwrap();
        sql_query("SELECT public.hubuum_history_publication_generation()>=0 AS allowed")
            .get_result::<Gate>(&mut reader)
            .await
            .unwrap();
        writer.batch_execute("BEGIN").await.unwrap();
        assert!(reserve(&mut writer, &producer).await.unwrap());
        let child = producer.context();
        assert!(
            settle_raw(&mut writer, &parent, child.id(), child.ownership_token())
                .await
                .unwrap()
        );
        writer.batch_execute("COMMIT").await.unwrap();
        let result = reserve(&mut reader, &replacement).await;
        reader.batch_execute("ROLLBACK").await.unwrap();
        let drained = publication
            .after_drained(std::future::ready(Ok(())), std::future::ready(Ok(())))
            .await
            .unwrap();
        storage.settle_history_publication(drained).await.unwrap();
        storage.unregister_runtime(registration).await.unwrap();
        if repeatable_read {
            assert!(
                matches!(
                    result,
                    Err(DieselError::DatabaseError(
                        DatabaseErrorKind::SerializationFailure,
                        _
                    ))
                ),
                "retained snapshot must not reset single assignment: {result:?}"
            );
        } else {
            assert!(!result.unwrap());
        }
    }

    #[tokio::test]
    async fn archive_producer_rejects_a_different_live_application_owner() {
        let _guard = CONTROL_LOCK.lock().await;
        let storage = PostgresStorage::unobserved(integration_test_pool(2));
        let original = storage.register_runtime().await.unwrap();
        let other = storage.register_runtime().await.unwrap();
        let permit = storage
            .capture_application_execution(Some(original.owner()))
            .await
            .unwrap();
        let replacement = storage
            .capture_application_execution(Some(other.owner()))
            .await
            .unwrap();
        let original_scope = StorageExecutionScope::default().with_application_access(permit);
        let publication = storage
            .run_in_scope(
                original_scope.clone(),
                storage.admit_history_publication(HistoryPublicationKind::ArchiveRead),
            )
            .await
            .unwrap();
        let context = publication.context();
        let mismatched = StorageExecutionScope::default()
            .with_application_access(replacement)
            .with_history_read(context.clone());
        let refused = storage
            .run_in_scope(mismatched, storage.admit_history_producer(context.clone()))
            .await;
        // A rejected mismatched scope must not consume the original slot.
        let producer = storage
            .run_in_scope(
                original_scope.with_history_read(context.clone()),
                storage.admit_history_producer(context),
            )
            .await
            .unwrap();
        let completed = producer
            .after_completed(std::future::ready(Ok(())))
            .await
            .unwrap();
        storage.settle_history_producer(&completed).await.unwrap();
        let drained = publication
            .after_drained(std::future::ready(Ok(())), std::future::ready(Ok(())))
            .await
            .unwrap();
        storage.settle_history_publication(drained).await.unwrap();
        storage.unregister_runtime(original).await.unwrap();
        storage.unregister_runtime(other).await.unwrap();
        assert!(refused.is_err());
    }

    #[rstest::rstest]
    #[case::rc_retirement_first(false, true)]
    #[case::rr_retirement_first(true, true)]
    #[case::rc_reservation_first(false, false)]
    #[case::rr_reservation_first(true, false)]
    #[tokio::test]
    async fn archive_producer_reservation_and_parent_retirement_serialize(
        #[case] repeatable_read: bool,
        #[case] retirement_first: bool,
    ) {
        let _guard = CONTROL_LOCK.lock().await;
        let storage = PostgresStorage::unobserved(integration_test_pool(2));
        let registration = storage.register_runtime().await.unwrap();
        let permit = storage
            .capture_application_execution(Some(registration.owner()))
            .await
            .unwrap();
        let scope = StorageExecutionScope::default().with_application_access(permit);
        let publication = storage
            .run_in_scope(
                scope,
                storage.admit_history_publication(HistoryPublicationKind::ArchiveRead),
            )
            .await
            .unwrap();
        let parent = publication.context();
        // Trusted adapter fixture: this is the exact candidate whose native
        // admission is being raced, not a fabricated persisted owner.
        let producer = HistoryProducerOwner::generate(parent.clone());
        let pool = integration_test_pool(3);
        let mut writer = pool.get().await.unwrap();
        let mut reader = pool.get().await.unwrap();
        let mut observer = pool.get().await.unwrap();
        reader
            .batch_execute(if repeatable_read {
                "BEGIN ISOLATION LEVEL REPEATABLE READ"
            } else {
                "BEGIN ISOLATION LEVEL READ COMMITTED"
            })
            .await
            .unwrap();
        sql_query("SELECT public.hubuum_history_publication_generation()>=0 AS allowed")
            .get_result::<Gate>(&mut reader)
            .await
            .unwrap();
        let pid = sql_query("SELECT pg_backend_pid() AS pid")
            .get_result::<Backend>(&mut reader)
            .await
            .unwrap()
            .pid;
        writer.batch_execute("BEGIN").await.unwrap();
        let first = if retirement_first {
            retire(&mut writer, &parent).await
        } else {
            reserve(&mut writer, &producer).await
        };
        assert!(first.unwrap());
        let result = {
            let pending = async {
                if retirement_first {
                    reserve(&mut reader, &producer).await
                } else {
                    retire(&mut reader, &parent).await
                }
            };
            tokio::pin!(pending);
            tokio::select! {
                result = &mut pending => panic!("operation escaped the held parent tuple: {result:?}"),
                () = blocked(&mut observer, pid) => {}
            }
            writer.batch_execute("COMMIT").await.unwrap();
            timeout(WaitDuration::from_secs(5), &mut pending)
                .await
                .expect("operation must finish after writer commit")
        };
        reader.batch_execute("ROLLBACK").await.unwrap();
        let stale = storage
            .run_in_scope(
                StorageExecutionScope::default()
                    .with_application_access(parent.epoch().application().clone())
                    .with_history_read(parent.clone()),
                storage.admit_history_producer(parent.clone()),
            )
            .await;
        if !retirement_first {
            // No external work was issued by this registry race fixture.
            let completed = producer
                .after_completed(std::future::ready(Ok(())))
                .await
                .unwrap();
            storage.settle_history_producer(&completed).await.unwrap();
            storage.settle_history_producer(&completed).await.unwrap();
            let drained = publication
                .after_drained(std::future::ready(Ok(())), std::future::ready(Ok(())))
                .await
                .unwrap();
            storage.settle_history_publication(drained).await.unwrap();
        }
        storage.unregister_runtime(registration).await.unwrap();
        assert!(
            stale.is_err(),
            "retired or already assigned parent cannot reserve again"
        );
        if repeatable_read {
            assert!(
                matches!(
                    result,
                    Err(DieselError::DatabaseError(
                        DatabaseErrorKind::SerializationFailure,
                        _
                    ))
                ),
                "retained RR snapshot must not become successful admission: {result:?}"
            );
        } else {
            assert!(!result.unwrap());
        }
    }

    #[tokio::test]
    async fn archive_producer_rollback_and_repeated_settlement_preserve_counter() {
        let _guard = CONTROL_LOCK.lock().await;
        let storage = PostgresStorage::unobserved(integration_test_pool(2));
        let registration = storage.register_runtime().await.unwrap();
        let permit = storage
            .capture_application_execution(Some(registration.owner()))
            .await
            .unwrap();
        let scope = StorageExecutionScope::default().with_application_access(permit);
        let publication = storage
            .run_in_scope(
                scope.clone(),
                storage.admit_history_publication(HistoryPublicationKind::ArchiveRead),
            )
            .await
            .unwrap();
        let parent = publication.context();
        let producer = HistoryProducerOwner::generate(parent.clone());
        let pool = integration_test_pool(1);
        let mut connection = pool.get().await.unwrap();
        connection.batch_execute("BEGIN").await.unwrap();
        assert!(reserve(&mut connection, &producer).await.unwrap());
        connection.batch_execute("ROLLBACK").await.unwrap();
        // The actual productive adapter must still be able to assign its slot.
        let producer = storage
            .run_in_scope(
                scope.with_history_read(parent.clone()),
                storage.admit_history_producer(parent.clone()),
            )
            .await
            .unwrap();
        let occupied = !retire(&mut connection, &parent).await.unwrap();
        let completed = producer
            .after_completed(std::future::ready(Ok(())))
            .await
            .unwrap();
        connection.batch_execute("BEGIN").await.unwrap();
        let child = completed.context();
        let rolled_back =
            sql_query("SELECT public.hubuum_history_producer_settle($1,$2,$3) AS allowed")
                .bind::<SqlUuid, _>(child.parent().id())
                .bind::<SqlUuid, _>(child.id())
                .bind::<SqlUuid, _>(child.ownership_token())
                .get_result::<Gate>(&mut connection)
                .await
                .unwrap()
                .allowed;
        connection.batch_execute("ROLLBACK").await.unwrap();
        let still_occupied = !retire(&mut connection, &parent).await.unwrap();
        storage.settle_history_producer(&completed).await.unwrap();
        storage.settle_history_producer(&completed).await.unwrap();
        let drained = publication
            .after_drained(std::future::ready(Ok(())), std::future::ready(Ok(())))
            .await
            .unwrap();
        storage.settle_history_publication(drained).await.unwrap();
        storage.unregister_runtime(registration).await.unwrap();
        assert!(occupied && rolled_back && still_occupied);
    }
}

async fn expiry_fixture(
    family: StorageIdentityHistoryKind,
    width: i32,
) -> (Fixture, HistoryPolicyKey, i32) {
    use diesel::sql_types::Integer;
    #[derive(QueryableByName)]
    struct Created {
        #[diesel(sql_type=Integer)]
        id: i32,
        #[diesel(sql_type=Jsonb)]
        logical_identity: Value,
    }
    let fixture = Fixture::new().await;
    let name = format!("expiry_native_{}", Uuid::new_v4().simple());
    let created=with_transaction(&fixture.owner_pool,async |connection| {
        owner(connection).await?;
        let created=sql_query("INSERT INTO public.collections(name,description,parent_collection_id) SELECT $1,'before',id FROM public.collections WHERE parent_collection_id IS NULL RETURNING id,logical_identity")
            .bind::<Text,_>(&name).get_result::<Created>(connection).await?;
        for suffix in ["a","b"] {
            sql_query("INSERT INTO public.hubuumclass(name,description,collection_id) VALUES($1,'before',$2)")
                .bind::<Text,_>(format!("{name}_{suffix}")).bind::<Integer,_>(created.id).execute(connection).await?;
        }
        sql_query("INSERT INTO public.hubuumobject(name,description,collection_id,hubuum_class_id) SELECT name,repeat('x',$2),collection_id,id FROM public.hubuumclass WHERE collection_id=$1")
            .bind::<Integer,_>(created.id).bind::<Integer,_>(width).execute(connection).await?;
        sql_query("INSERT INTO public.hubuumclass_relation(from_hubuum_class_id,to_hubuum_class_id) SELECT a.id,b.id FROM public.hubuumclass a CROSS JOIN public.hubuumclass b WHERE a.name=$1||'_a' AND b.name=$1||'_b'")
            .bind::<Text,_>(&name).execute(connection).await?;
        sql_query("INSERT INTO public.hubuumobject_relation(from_hubuum_object_id,to_hubuum_object_id,class_relation_id) SELECT a.id,b.id,r.id FROM public.hubuumobject a CROSS JOIN public.hubuumobject b JOIN public.hubuumclass_relation r ON r.to_hubuum_class_id=b.hubuum_class_id WHERE a.name=$1||'_a' AND b.name=$1||'_b' AND r.from_hubuum_class_id=a.hubuum_class_id")
            .bind::<Text,_>(&name).execute(connection).await?;
        sql_query("INSERT INTO public.export_templates(collection_id,name,description,content_type,template,kind) VALUES($1,'expiry','before','text/plain','hello','fragment')")
            .bind::<Integer,_>(created.id).execute(connection).await?;
        sql_query("INSERT INTO public.remote_targets(collection_id,name,description,method,url_template,allowed_subject_types) VALUES($1,'expiry','before','get','https://example.invalid','[\"collection\"]')")
            .bind::<Integer,_>(created.id).execute(connection).await?;
        for table in ["collections","hubuumclass","hubuumobject","export_templates","remote_targets"] {
            let column=if table=="collections" {"id"} else {"collection_id"};
            sql_query(format!("UPDATE public.{table} SET description='after' WHERE {column}=$1"))
                .bind::<Integer,_>(created.id).execute(connection).await?;
        }
        // Deleting the second class closes computed history and both relation
        // histories through real triggers. Its schema events remain historical.
        sql_query("DELETE FROM public.hubuumclass WHERE name=$1||'_b'").bind::<Text,_>(&name).execute(connection).await?;
        Ok::<_,PostgresStorageError>(created)
    }).await.unwrap();
    // Schema revisions are retained independently after class deletion. The
    // positive schema-event fixture deliberately has no retained revision;
    // ordinary live revision dependencies are checked separately below.
    if family == StorageIdentityHistoryKind::ClassSchema {
        with_transaction(&fixture.owner_pool,async |connection| {
            owner(connection).await?;
            sql_query("DELETE FROM public.class_schema_revisions WHERE class_id IN (SELECT id FROM public.hubuumclass_history WHERE name=$1||'_b')").bind::<Text,_>(&name).execute(connection).await?;
            Ok::<_,PostgresStorageError>(())
        }).await.unwrap();
    }
    let evidence: StorageIdentityEvidence =
        serde_json::from_value(created.logical_identity).unwrap();
    let collection = evidence
        .subject()
        .unwrap()
        .record_identity()
        .unwrap()
        .resource();
    let scope = if family == StorageIdentityHistoryKind::ClassSchema {
        let identity=with_transaction(&fixture.owner_pool,async |connection| {
            owner(connection).await?;
            sql_query("SELECT logical_identity AS material FROM public.hubuumclass_history WHERE name=$1||'_b' AND op='I'").bind::<Text,_>(&name).get_result::<Material>(connection).await.map_err(PostgresStorageError::from)
        }).await.unwrap();
        let evidence: StorageIdentityEvidence = serde_json::from_value(identity.material).unwrap();
        HistoryPolicyScope::Class {
            identity: evidence
                .subject()
                .unwrap()
                .record_identity()
                .unwrap()
                .resource(),
        }
    } else {
        HistoryPolicyScope::Collection {
            identity: collection,
        }
    };
    let key = HistoryPolicyKey::new(family, scope).unwrap();
    let before = fixture.storage.capture_history_lifecycle().await.unwrap();
    let proposal = HistoryPolicyChange::new(
        key,
        HistoryPolicyRevision::new(0).unwrap(),
        HistoryPolicyBinding::Expire {
            trigger: HistoryExpiryTrigger::Age {
                age_seconds: HistoryRetentionSeconds::new(0).unwrap(),
                deleted_age_seconds: Some(HistoryRetentionSeconds::new(0).unwrap()),
            },
            minimum_age_seconds: HistoryRetentionSeconds::new(0).unwrap(),
        },
        HistoryPolicyApplication::Prospective,
    )
    .unwrap();
    let change = before
        .control()
        .apply(
            HistoryPolicyApplyRequest::new(proposal, None),
            PrincipalIdentity::generate(),
            before.fence(),
            before.observed_at() - Duration::seconds(30),
        )
        .unwrap();
    with_transaction(&fixture.owner_pool, async |connection| {
        owner(connection).await?;
        sql_query("UPDATE hubuum_control.history_lifecycle_control SET material=$1 WHERE id=1")
            .bind::<Jsonb, _>(serde_json::to_value(change.next()).unwrap())
            .execute(connection)
            .await?;
        Ok::<_, PostgresStorageError>(())
    })
    .await
    .unwrap();
    (fixture, key, created.id)
}

async fn cleanup_expiry_fixture(fixture: Fixture, collection: i32, batch: Uuid) {
    use diesel::sql_types::Integer;
    with_transaction(&fixture.owner_pool,async |connection| {
        owner(connection).await?;
        sql_query("DELETE FROM hubuum_control.history_expiry_members WHERE intent_uuid=$1").bind::<SqlUuid,_>(batch).execute(connection).await?;
        sql_query("DELETE FROM public.collections WHERE id=$1").bind::<Integer,_>(collection).execute(connection).await?;
        for (table,filter) in [
            ("class_schema_revisions","class_id IN (SELECT id FROM public.hubuumclass_history WHERE collection_id=$1)"),
            ("class_schema_history","class_id IN (SELECT id FROM public.hubuumclass_history WHERE collection_id=$1)"),
            ("computed_field_history","class_id IN (SELECT id FROM public.hubuumclass_history WHERE collection_id=$1)"),
            ("hubuumobject_relation_history","from_hubuum_object_id IN (SELECT id FROM public.hubuumobject_history WHERE collection_id=$1)"),
            ("hubuumclass_relation_history","from_hubuum_class_id IN (SELECT id FROM public.hubuumclass_history WHERE collection_id=$1)"),
            ("hubuumobject_history","collection_id=$1"), ("hubuumclass_history","collection_id=$1"),
            ("export_templates_history","collection_id=$1"),("remote_targets_history","collection_id=$1"),("collections_history","id=$1"),
        ] {sql_query(format!("DELETE FROM public.{table} WHERE {filter}")).bind::<Integer,_>(collection).execute(connection).await?;}
        Ok::<_,PostgresStorageError>(())
    }).await.unwrap();
    fixture.restore().await;
}

#[rstest::rstest]
#[case::collection(StorageIdentityHistoryKind::Collection)]
#[case::class(StorageIdentityHistoryKind::Class)]
#[case::object(StorageIdentityHistoryKind::Object)]
#[case::class_relation(StorageIdentityHistoryKind::ClassRelation)]
#[case::object_relation(StorageIdentityHistoryKind::ObjectRelation)]
#[case::template(StorageIdentityHistoryKind::ExportTemplate)]
#[case::remote(StorageIdentityHistoryKind::RemoteTarget)]
#[case::computed(StorageIdentityHistoryKind::ComputedDefinitions)]
#[case::schema(StorageIdentityHistoryKind::ClassSchema)]
#[tokio::test]
async fn native_expiry_admits_exact_trigger_created_membership(
    #[case] family: StorageIdentityHistoryKind,
) {
    let _guard = CONTROL_LOCK.lock().await;
    let (fixture, key, collection) = expiry_fixture(family, 32).await;
    let runtime = fixture
        .storage
        .register_history_runtime(None)
        .await
        .unwrap();
    let application = fixture
        .storage
        .capture_application_execution(Some(runtime.owner()))
        .await
        .unwrap();
    let before = fixture.storage.capture_history_lifecycle().await.unwrap();
    let batch = Uuid::new_v4();
    let request = HistoryExpiryAdmissionRequest::new(
        batch,
        Uuid::new_v4(),
        HistoryExpiryOrigin::Manual {
            principal: PrincipalIdentity::generate(),
        },
        HistoryLifecycleScan::new(key, 1).unwrap(),
        before.fence(),
    )
    .unwrap();
    let result = fixture
        .storage
        .run_in_scope(
            StorageExecutionScope::default().with_application_access(application),
            fixture.storage.admit_history_expiry(request),
        )
        .await;
    let after = fixture.storage.capture_history_lifecycle().await.unwrap();
    let protected_schema = if family == StorageIdentityHistoryKind::ClassSchema {
        Some(with_transaction(&fixture.owner_pool,async |connection| {
            owner(connection).await?;
            sql_query("SELECT to_jsonb(public.hubuum_history_expiry_dependency('class_schema',h.logical_identity)) AS material FROM public.class_schema_history h JOIN public.hubuumclass c ON c.id=h.class_id WHERE c.collection_id=$1 ORDER BY h.id LIMIT 1").bind::<diesel::sql_types::Integer,_>(collection).get_result::<Material>(connection).await.map_err(PostgresStorageError::from)
        }).await.unwrap().material)
    } else {
        None
    };
    let projection=with_transaction(&fixture.owner_pool,async |connection| {
        owner(connection).await?;
        sql_query("SELECT coalesce(jsonb_agg(to_jsonb(m)),'[]'::JSONB) AS material FROM hubuum_control.history_expiry_members m WHERE intent_uuid=$1").bind::<SqlUuid,_>(batch).get_result::<Material>(connection).await.map_err(PostgresStorageError::from)
    }).await.unwrap().material;
    fixture.storage.unregister_runtime(runtime).await.unwrap();
    cleanup_expiry_fixture(fixture, collection, batch).await;
    if let Some(protected) = protected_schema {
        assert_eq!(protected, Value::Bool(true));
    }
    let manifest = result.unwrap().expect("eligible native batch");
    assert_eq!(manifest.members().len(), 1);
    assert_eq!(
        after
            .control()
            .authority()
            .expiry()
            .intent(batch)
            .unwrap()
            .manifest(),
        &manifest
    );
    assert_eq!(projection.as_array().unwrap().len(), 1);
    assert!(manifest.members()[0].input_bytes().value() > 0);
    if family == StorageIdentityHistoryKind::Object {
        assert!(manifest.members()[0].object_authorization().is_some());
    }
}

#[rstest::rstest]
#[case::held("held")]
#[case::stale_hold("stale_hold")]
#[case::legacy("legacy")]
#[case::oversize("oversize")]
#[case::audit("audit")]
#[tokio::test]
async fn native_expiry_refuses_unsafe_or_uncommitted_selection(#[case] mode: &str) {
    let _guard = CONTROL_LOCK.lock().await;
    let (fixture, key, collection) = expiry_fixture(
        StorageIdentityHistoryKind::Object,
        if mode == "oversize" {
            17 * 1024 * 1024
        } else {
            32
        },
    )
    .await;
    let runtime = fixture
        .storage
        .register_history_runtime(None)
        .await
        .unwrap();
    let application = fixture
        .storage
        .capture_application_execution(Some(runtime.owner()))
        .await
        .unwrap();
    let principal = PrincipalIdentity::generate();
    let original = fixture.storage.capture_history_lifecycle().await.unwrap();
    if matches!(mode, "held" | "stale_hold") {
        fixture
            .storage
            .commit_history_lifecycle(
                original
                    .control()
                    .create_hold(
                        HistoryRetentionHold::new(
                            Uuid::new_v4(),
                            key,
                            original.observed_at(),
                            None,
                            principal,
                        )
                        .unwrap(),
                        original.fence(),
                    )
                    .unwrap(),
            )
            .await
            .unwrap();
    }
    let before = fixture.storage.capture_history_lifecycle().await.unwrap();
    let legacy = if mode == "legacy" {
        Some(fixture.storage.register_runtime().await.unwrap())
    } else {
        None
    };
    if mode == "audit" {
        with_transaction(&fixture.owner_pool,async |connection| {
            owner(connection).await?;
            connection.batch_execute("CREATE FUNCTION public.history_expiry_audit_fault() RETURNS trigger LANGUAGE plpgsql AS $fault$ BEGIN IF NEW.metadata #>> '{lifecycle,name}'='lifecycle.erasure.requested' THEN RAISE EXCEPTION 'required expiry audit fault'; END IF; RETURN NEW; END $fault$; CREATE TRIGGER history_expiry_audit_fault BEFORE INSERT ON public.events FOR EACH ROW EXECUTE FUNCTION public.history_expiry_audit_fault();").await?;
            Ok::<_,PostgresStorageError>(())
        }).await.unwrap();
    }
    let batch = Uuid::new_v4();
    let request = HistoryExpiryAdmissionRequest::new(
        batch,
        Uuid::new_v4(),
        HistoryExpiryOrigin::Manual { principal },
        HistoryLifecycleScan::new(key, 1).unwrap(),
        if mode == "stale_hold" {
            original.fence()
        } else {
            before.fence()
        },
    )
    .unwrap();
    let result = fixture
        .storage
        .run_in_scope(
            StorageExecutionScope::default().with_application_access(application),
            fixture.storage.admit_history_expiry(request),
        )
        .await;
    if mode == "audit" {
        with_transaction(&fixture.owner_pool,async |connection| {owner(connection).await?;connection.batch_execute("DROP TRIGGER history_expiry_audit_fault ON public.events; DROP FUNCTION public.history_expiry_audit_fault();").await?;Ok::<_,PostgresStorageError>(())}).await.unwrap();
    }
    let after = fixture.storage.capture_history_lifecycle().await.unwrap();
    if let Some(legacy) = legacy {
        fixture.storage.unregister_runtime(legacy).await.unwrap();
    }
    fixture.storage.unregister_runtime(runtime).await.unwrap();
    cleanup_expiry_fixture(fixture, collection, batch).await;
    if matches!(mode, "held" | "oversize") {
        assert!(result.unwrap().is_none());
    } else {
        assert!(result.is_err(), "{mode} unexpectedly admitted a batch");
    }
    assert_eq!(after.control(), before.control());
}

#[tokio::test]
async fn admitted_expiry_fences_old_publications_without_retiring_them() {
    use diesel::sql_types::Integer;
    #[derive(QueryableByName)]
    struct Count {
        #[diesel(sql_type=diesel::sql_types::BigInt)]
        count: i64,
    }
    let _guard = CONTROL_LOCK.lock().await;
    let (fixture, key, collection) = expiry_fixture(StorageIdentityHistoryKind::Object, 32).await;
    let runtime = fixture
        .storage
        .register_history_runtime(None)
        .await
        .unwrap();
    let application = fixture
        .storage
        .capture_application_execution(Some(runtime.owner()))
        .await
        .unwrap();
    let scope = StorageExecutionScope::default().with_application_access(application);
    let publication = fixture
        .storage
        .run_in_scope(
            scope.clone(),
            fixture
                .storage
                .admit_history_publication(HistoryPublicationKind::Export),
        )
        .await
        .unwrap();
    let before = fixture.storage.capture_history_lifecycle().await.unwrap();
    let batch = Uuid::new_v4();
    let request = HistoryExpiryAdmissionRequest::new(
        batch,
        Uuid::new_v4(),
        HistoryExpiryOrigin::Manual {
            principal: PrincipalIdentity::generate(),
        },
        HistoryLifecycleScan::new(key, 1).unwrap(),
        before.fence(),
    )
    .unwrap();
    let admitted = fixture
        .storage
        .run_in_scope(scope.clone(), fixture.storage.admit_history_expiry(request))
        .await;
    let stale = fixture
        .storage
        .run_in_scope(
            scope.with_history_read(publication.context()),
            fixture
                .storage
                .get_collection(hubuum_domain::CollectionId::new(collection).unwrap()),
        )
        .await;
    let charged = with_transaction(&fixture.owner_pool, async |connection| {
        owner(connection).await?;
        sql_query("SELECT count(*) FROM hubuum_control.history_publication_owners WHERE id=$1")
            .bind::<SqlUuid, _>(publication.context().id())
            .get_result::<Count>(connection)
            .await
            .map_err(PostgresStorageError::from)
    })
    .await
    .unwrap()
    .count;
    // No external work is issued by this registry fixture. Exact completion
    // remains necessary after the new intent fences productive entry.
    fixture
        .storage
        .settle_history_publication(
            publication
                .after_drained(std::future::ready(Ok(())), std::future::ready(Ok(())))
                .await
                .unwrap(),
        )
        .await
        .unwrap();
    let capture = fixture.storage.capture_control_checkpoint().await.unwrap();
    let preserved=with_transaction(&fixture.owner_pool,async |connection| {
        owner(connection).await?;
        sql_query("SELECT count(*) FROM public.hubuumobject_history WHERE collection_id=$1 AND valid_to IS NOT NULL").bind::<Integer,_>(collection).get_result::<Count>(connection).await.map_err(PostgresStorageError::from)
    }).await.unwrap().count;
    fixture.storage.unregister_runtime(runtime).await.unwrap();
    cleanup_expiry_fixture(fixture, collection, batch).await;
    let manifest = admitted.unwrap().unwrap();
    assert!(stale.is_err());
    assert_eq!(charged, 1);
    assert!(preserved > 0);
    assert_eq!(
        capture
            .history_lifecycle()
            .unwrap()
            .expiry()
            .intent(batch)
            .unwrap()
            .manifest(),
        &manifest
    );
}
