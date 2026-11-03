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

// Protocol admission only. These tests deliberately manufacture payload-free
// exclusions as owner fixtures; they do not enable or qualify native expiry CAS.
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
