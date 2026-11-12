//! Supported logical restore, protected replay and configured authorization.
//! Retained authority is a signed logical fixture, not H4 or transport qualification.
use futures_util::FutureExt;
use std::os::unix::fs::PermissionsExt;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use diesel::sql_types::{BigInt, Jsonb, Text, Uuid as SqlUuid};
use hubuum::errors::ApiError;
use hubuum::models::Permissions;
use hubuum::permissions::test_support::mock_treetop::{MockAllowRule, MockTreetopBackend};
use hubuum::permissions::{ResourceFields, ResourceKind};
use reqwest::Url;

use hubuum::test_support::{
    create_history_remapping_object, history_application_context, history_population_as_of,
    prepare_history_read_fixture,
};
use hubuum_domain::{ClassId, PrincipalIdentity};
use hubuum_events_core::EventContext;
use hubuum_storage_postgres::test_support::{
    database_role_tests_enabled, integration_test_database_roles,
};
use hubuum_storage_postgres::{
    PostgresPool, PostgresStorageError, with_connection, with_transaction,
};

use super::*;

fn fixture_error(error: impl std::fmt::Display) -> ApiError {
    ApiError::InternalServerError(error.to_string())
}

#[derive(Clone, Copy)]
struct Remap {
    collection: (i32, i32),
    class: (i32, i32),
    object: (i32, i32),
}
impl Remap {
    fn row(&self, row: &StorageBackupRow, fields: &[(&str, (i32, i32))]) -> StorageBackupRow {
        let mut value = row.clone().into_value();
        for &(field, (old, new)) in fields {
            if let Some(value) = value.pointer_mut(&format!("/{field}"))
                && value.as_i64() == Some(i64::from(old))
            {
                *value = json!(new);
            }
        }
        StorageBackupRow::try_from_value(value).unwrap()
    }

    fn snapshot(
        self,
        snapshot: StorageBackupSnapshot,
    ) -> Result<StorageBackupSnapshot, StorageValidationError> {
        use StorageBackupHistorySection as H;
        use StorageBackupStateSection as S;
        let (mut state, mut history) = snapshot.into_parts();
        for (section, rows) in &mut state {
            let fields: Vec<_> = match section {
                S::Collections => vec![
                    ("id", self.collection),
                    ("parent_collection_id", self.collection),
                ],
                S::Classes => vec![("id", self.class), ("collection_id", self.collection)],
                S::Objects => vec![
                    ("id", self.object),
                    ("class_id", self.class),
                    ("collection_id", self.collection),
                ],
                S::CollectionHierarchy => vec![
                    ("ancestor_collection_id", self.collection),
                    ("descendant_collection_id", self.collection),
                ],
                S::CollectionAuthorization
                | S::CollectionPermissionGrants
                | S::EventSinkCollectionGrants => vec![("collection_id", self.collection)],
                S::ClassSchemaState | S::ClassSchemaRevisions | S::ComputedFieldDefinitions => {
                    vec![("class_id", self.class)]
                }
                S::ObjectSchemaEvidence => {
                    vec![("object_id", self.object), ("class_id", self.class)]
                }
                _ => vec![],
            };
            for row in rows {
                *row = self.row(row, &fields);
            }
        }
        if let Some(history) = &mut history {
            for (section, rows) in history {
                let fields: Vec<_> = match section {
                    H::CollectionHistory => vec![
                        ("id", self.collection),
                        ("parent_collection_id", self.collection),
                    ],
                    H::ClassHistory => vec![("id", self.class), ("collection_id", self.collection)],
                    H::ObjectHistory => vec![
                        ("id", self.object),
                        ("class_id", self.class),
                        ("collection_id", self.collection),
                    ],
                    H::ClassSchemaHistory => {
                        vec![("class_id", self.class), ("snapshot/class_id", self.class)]
                    }
                    H::ComputedFieldHistory => vec![("class_id", self.class)],
                    _ => vec![],
                };
                for row in rows {
                    *row = self.row(row, &fields);
                }
            }
        }
        StorageBackupSnapshot::try_new(state, history)
    }
}

fn replace_identity(
    snapshot: StorageBackupSnapshot,
    original: ResourceIdentity,
) -> StorageBackupSnapshot {
    let replacement = ResourceIdentity::generate();
    let replace = |row: &mut StorageBackupRow| {
        let Some(raw) = row.get("logical_identity") else {
            return;
        };
        let evidence: StorageIdentityEvidence = serde_json::from_value(raw.clone()).unwrap();
        let subject = evidence.subject().map(|subject| match subject {
            StorageIdentitySubject::Resource {
                identity: hubuum_domain::IdentityReference::Known { identity },
                version,
            } if identity == original => StorageIdentitySubject::Resource {
                identity: hubuum_domain::IdentityReference::Known {
                    identity: replacement,
                },
                version,
            },
            other => other,
        });
        let mut references = evidence.references().clone();
        for reference in references.values_mut() {
            if let StorageCapturedIdentity::Resource {
                reference: hubuum_domain::IdentityReference::Known { identity },
                ..
            } = reference
                && *identity == original
            {
                *identity = replacement;
            }
        }
        let evidence = StorageIdentityEvidence::try_new(subject, references).unwrap();
        let mut fields = row.fields().clone();
        fields.insert(
            "logical_identity".into(),
            serde_json::to_value(evidence).unwrap(),
        );
        *row = StorageBackupRow::try_from_value(Value::Object(fields)).unwrap();
    };
    let (mut state, mut history) = snapshot.into_parts();
    for rows in state.values_mut() {
        for row in rows {
            replace(row);
        }
    }
    if let Some(history) = &mut history {
        for rows in history.values_mut() {
            for row in rows {
                replace(row);
            }
        }
    }
    StorageBackupSnapshot::try_new(state, history).unwrap()
}

fn restore_target(runtime: &str, migration: &str, primary: &[&str]) -> Result<String, ApiError> {
    let invalid = || ApiError::Conflict("Unproved isolated restore target".into());
    let parse = |raw: &str| {
        let parsed = Url::parse(raw).map_err(|_| invalid())?;
        if !matches!(parsed.scheme(), "postgres" | "postgresql") || parsed.host_str().is_none() {
            return Err(invalid());
        }
        let name = parsed.path().strip_prefix('/').ok_or_else(invalid)?;
        if name.is_empty()
            || name.len() > 63
            || !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        {
            return Err(invalid());
        }
        Ok(parsed)
    };
    let runtime = parse(runtime)?;
    let migration = parse(migration)?;
    if runtime.host_str() != migration.host_str()
        || runtime.port() != migration.port()
        || runtime.path() != migration.path()
    {
        return Err(invalid());
    }
    for primary in primary {
        if parse(primary)?.path() == runtime.path() {
            return Err(invalid());
        }
    }
    Ok(runtime.path()[1..].to_owned())
}

#[rstest]
#[case::local("hubuum_test_db_123_456_789_restore", "primary", true)]
#[case::hosted("hubuum_restore_0123456789abcdef", "primary", true)]
#[case::primary("primary", "primary", false)]
#[case::encoded("%70rimary", "primary", false)]
fn restore_reset_requires_distinct_configured_target(
    #[case] target: &str,
    #[case] primary: &str,
    #[case] allowed: bool,
) {
    let runtime = format!("postgres://runtime@localhost:5432/{target}");
    let migration = format!("postgres://migration@localhost:5432/{target}");
    let primary = format!("postgres://runtime@localhost:5432/{primary}");
    assert_eq!(
        restore_target(&runtime, &migration, &[&primary]).is_ok(),
        allowed
    );
    assert!(
        restore_target(
            &runtime,
            "postgres://migration@localhost:5432/unrelated",
            &[&primary]
        )
        .is_err()
    );
}

fn configured_restore_target() -> Result<String, ApiError> {
    restore_target(
        &runtime_database_url(),
        &database_url(),
        &[
            &std::env::var("HUBUUM_DATABASE_URL")
                .map_err(|_| fixture_error("Missing primary target"))?,
            &std::env::var("HUBUUM_MIGRATION_DATABASE_URL")
                .map_err(|_| fixture_error("Missing primary migration target"))?,
        ],
    )
}

async fn require_fixture_target(pool: &PostgresPool) -> Result<(), ApiError> {
    let expected = configured_restore_target()?;
    let actual = with_connection(pool, async |connection| {
        diesel::select(diesel::dsl::sql::<Text>("current_database()"))
            .get_result::<String>(connection)
            .await
            .map_err(PostgresStorageError::from)
    })
    .await
    .map_err(StorageError::from)?;
    if actual != expected {
        return Err(ApiError::Conflict(
            "Connected target is not the configured isolated restore database".into(),
        ));
    }
    Ok(())
}

#[tokio::test]
async fn restore_fixture_refuses_primary_connection_before_any_write() {
    let _guard = RESTORE_TEST_LOCK.lock().await;
    let primary = std::env::var("HUBUUM_MIGRATION_DATABASE_URL").expect("runner primary target");
    let pool = postgres_test_pool_with_timeout(&primary, 1, DEFAULT_DB_STATEMENT_TIMEOUT_MS);
    assert!(matches!(
        require_fixture_target(&pool).await,
        Err(ApiError::Conflict(_))
    ));
}

fn require_fixture_deletion_transition(
    previous: &DeletionControlState,
    expected: &DeletionControlState,
) -> Result<(), ApiError> {
    if previous.revision() != 0
        || !previous.intents().is_empty()
        || !expected.covers(previous)
        || previous.intents() != expected.intents()
    {
        return Err(ApiError::Conflict(
            "Unproved fixture deletion transition".into(),
        ));
    }
    Ok(())
}

#[rstest]
#[case::matching(false, false, true)]
#[case::lineage(true, false, false)]
#[case::epoch(false, true, false)]
fn restore_reset_rejects_unrelated_empty_authority(
    #[case] lineage: bool,
    #[case] epoch: bool,
    #[case] allowed: bool,
) {
    let previous = DeletionControlState::empty(Uuid::new_v4(), Uuid::new_v4()).unwrap();
    let expected = DeletionControlState::empty(
        if lineage {
            Uuid::new_v4()
        } else {
            previous.lineage()
        },
        if epoch {
            Uuid::new_v4()
        } else {
            previous.epoch()
        },
    )
    .unwrap();
    assert_eq!(
        require_fixture_deletion_transition(&previous, &expected).is_ok(),
        allowed
    );
}

// Test-only teardown after acceptance, signed restore_original and owner
// settlement, exclusively in the runner's destructive restore DB under its lock.
pub(super) async fn restore_fixture_authority(
    pool: &PostgresPool,
    previous: &HistoryLifecycleControl,
    previous_deletion: &DeletionControlState,
    expected: &HistoryLifecycleControl,
    expected_deletion: &DeletionControlState,
    intent: Option<Uuid>,
    expected_application: &ApplicationControlState,
) -> Result<ApplicationControlState, ApiError> {
    require_fixture_target(pool).await?;
    require_fixture_deletion_transition(previous_deletion, expected_deletion)?;
    let remaining: Vec<_> = expected
        .authority()
        .expiry()
        .intents()
        .iter()
        .filter(|entry| Some(entry.manifest().identity()) != intent)
        .collect();
    let original: Vec<_> = previous.authority().expiry().intents().iter().collect();
    if remaining != original {
        return Err(ApiError::Conflict(
            "Unrelated fixture obligations changed".into(),
        ));
    }
    let material = serde_json::to_value(previous).unwrap();
    let expected = serde_json::to_value(expected).unwrap();
    let deletion = serde_json::to_value(previous_deletion).unwrap();
    let expected_deletion = serde_json::to_value(expected_deletion).unwrap();
    let restored_application = ApplicationControlState::initial(previous_deletion);
    let database = configured_restore_target()?;
    let expected_application = json!({
        "incarnation":expected_application.fence().incarnation(),
        "revision":expected_application.fence().revision(),
        "reapplication_revision":expected_application.reapplication().map(ApplicationReapplication::revision),
        "released_revision":expected_application.released_revision(),
        "checkpoint":expected_application.checkpoint(),
    });
    with_transaction(pool, async |connection| {
        if database_role_tests_enabled() {
            diesel::sql_query("SELECT set_config('role',$1,true)")
                .bind::<Text, _>(integration_test_database_roles().owner().as_str())
                .execute(connection).await?;
        }
        let changed = diesel::sql_query(
            "UPDATE hubuum_control.deletion_authority SET material=$1 WHERE id=1 AND material=$2
             AND current_database()=$3
             AND public.hubuum_application_control_read()=$4
             AND NOT EXISTS(SELECT 1 FROM hubuum_control.runtime_registrations)
             AND NOT EXISTS(SELECT 1 FROM hubuum_control.history_publication_owners)
             AND NOT EXISTS(SELECT 1 FROM hubuum_control.api_request_owners)",
        ).bind::<Jsonb, _>(deletion).bind::<Jsonb, _>(expected_deletion)
            .bind::<Text, _>(database).bind::<Jsonb, _>(&expected_application).execute(connection).await?;
        if changed != 1 {
            return Err(PostgresStorageError::conflict("Fixture authority changed or productive owners remain"));
        }
        let changed = diesel::sql_query(
            "UPDATE hubuum_control.history_lifecycle_control SET material=$1 WHERE id=1 AND material=$2",
        ).bind::<Jsonb, _>(material).bind::<Jsonb, _>(expected).execute(connection).await?;
        if changed != 1 {
            return Err(PostgresStorageError::conflict("Fixture history authority changed"));
        }
        let changed = diesel::sql_query(
            "UPDATE hubuum_control.application_control SET incarnation=$1,reapplication_revision=NULL,
             released_revision=$2,checkpoint=NULL,pending_restore=FALSE WHERE id=1 AND (public.hubuum_application_control_read()-'revision')=($3::jsonb-'revision') AND pending_restore=FALSE",
        ).bind::<SqlUuid, _>(restored_application.fence().incarnation())
            .bind::<BigInt, _>(restored_application.fence().revision())
            .bind::<Jsonb, _>(&expected_application).execute(connection).await?;
        if changed != 1 {
            return Err(PostgresStorageError::conflict("Fixture application image changed or restore remains pending"));
        }
        if let Some(intent) = intent {
            diesel::sql_query("DELETE FROM hubuum_control.history_expiry_members WHERE intent_uuid=$1")
                .bind::<SqlUuid, _>(intent).execute(connection).await?;
        }
        Ok::<_, PostgresStorageError>(())
    }).await.map_err(fixture_error)?;
    Ok(restored_application)
}

#[rstest]
#[case::allowed("allowed")]
#[case::historical_denied("historical_denied")]
#[case::current_denied("current_denied")]
#[case::old_coordinates("old_coordinates")]
#[case::revoked("revoked")]
#[case::stale_scope("stale_scope")]
#[case::object_uuid("object_uuid")]
#[case::collection_uuid("collection_uuid")]
#[case::class_uuid("class_uuid")]
#[tokio::test]
async fn remapped_original_history_requires_configured_authorization(
    #[values(StorageBackendKind::Postgres, StorageBackendKind::Memory)] kind: StorageBackendKind,
    #[case] condition: &str,
) {
    let _guard = RESTORE_TEST_LOCK.lock().await;
    let pool = postgres_test_pool_with_timeout(&database_url(), 2, DEFAULT_DB_STATEMENT_TIMEOUT_MS);
    require_fixture_target(&pool)
        .await
        .expect("pre-write isolated fixture target proof");
    let fixture = RestoreContractFixture::new(kind, pool.clone())
        .await
        .unwrap();
    let storage = fixture.storage();
    let before = storage.capture_history_lifecycle().await.unwrap();
    let before_deletion = storage.read_deletion_control().await.unwrap();
    let operation = Uuid::new_v4();
    let read = fixture.runtime_read_storage(postgres_test_pool_with_timeout(
        &runtime_database_url(),
        2,
        DEFAULT_DB_STATEMENT_TIMEOUT_MS,
    ));
    let mut pending_runtime = None;
    let mut pending_publication = None;
    let mut recovery_paths = None;
    let mut fixture_intent = None;
    let mut retired_permit = None;
    let mut repair_snapshot = None;
    let result = AssertUnwindSafe(async {
        let directory =
            std::env::temp_dir().join(format!("hubuum-513-read-recovery-{}", Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
        let repository = directory.join("authority_repository");
        std::fs::create_dir(&repository).unwrap();
        std::fs::set_permissions(&repository, std::fs::Permissions::from_mode(0o700)).unwrap();
        let signing_key = directory.join("key");
        std::fs::write(&signing_key, [73; 32]).unwrap();
        std::fs::set_permissions(&signing_key, std::fs::Permissions::from_mode(0o600)).unwrap();
        let restore_repository = directory.join("restore_repository");
        std::fs::create_dir(&restore_repository).unwrap();
        std::fs::set_permissions(&restore_repository, std::fs::Permissions::from_mode(0o700))
            .unwrap();
        recovery_paths = Some((restore_repository.clone(), signing_key.clone()));
        fixture
            .initialize_recovery(&restore_repository, &signing_key)
            .await?;
        fixture
            .initialize_recovery(&repository, &signing_key)
            .await?;
        let actor = PrincipalIdentity::generate();
        let key = HistoryPolicyKey::new(
            StorageIdentityHistoryKind::Object,
            HistoryPolicyScope::Deployment,
        )
        .map_err(fixture_error)?;
        storage
            .commit_history_lifecycle(
                before
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
                                HistoryPolicyBinding::Expire {
                                    trigger: HistoryExpiryTrigger::Age {
                                        age_seconds: HistoryRetentionSeconds::new(0)
                                            .map_err(fixture_error)?,
                                        deleted_age_seconds: None,
                                    },
                                    minimum_age_seconds: HistoryRetentionSeconds::new(0)
                                        .map_err(fixture_error)?,
                                },
                                HistoryPolicyApplication::Prospective,
                            )
                            .map_err(fixture_error)?,
                            None,
                        ),
                        actor,
                        before.fence(),
                        before.observed_at(),
                    )
                    .map_err(|error| fixture_error(format!("{error:?}")))?,
            )
            .await?;
        let event = EventContext::system();
        let scope = TestScope::new();
        let user = storage
            .create_user(StorageUserCreate::new(
                None,
                scope.scoped_name("remap_user"),
                "fixture-hash",
                None,
                None,
                event.clone(),
            ))
            .await?
            .into_value()
            .into_parts();
        let principal_id: hubuum_domain::PrincipalId = user.id().into();
        let group = storage
            .create_group(
                StorageGroupCreate::new(None, scope.scoped_name("remap_group"), None),
                &event,
            )
            .await?
            .into_value();
        storage
            .add_group_member(principal_id, group.id(), &event)
            .await?
            .into_value();
        let object =
            create_history_remapping_object(storage, group.id(), &scope.scoped_name("remap"))
                .await?;
        let rows = storage
            .list_object_history(StorageObjectHistoryListQuery::new(
                object.id(),
                object.class_id(),
                QueryOptions::new(vec![], vec![], Some(10), None, false).map_err(fixture_error)?,
                StorageHistoryCollectionScope::All,
            ))
            .await?
            .into_parts()
            .0;
        let row = rows
            .into_iter()
            .find(|row| row.clone().into_parts().1.valid_to().is_some())
            .unwrap();
        let metadata = row.clone().into_parts().1;
        let facts = HistoryObjectAuthorizationFacts::capture(row).map_err(fixture_error)?;
        let original_facts = serde_json::to_value(&facts).unwrap();
        let class_uuid = facts.attribution().class().unwrap();
        let at = metadata.valid_from();
        let snapshot = storage.capture_history_lifecycle().await?;
        let mut selection = HistoryExpirySelection::new(
            operation,
            HistoryExpiryOrigin::Manual { principal: actor },
            HistoryLifecycleScan::new(key, 1).map_err(fixture_error)?,
            &snapshot,
        )
        .map_err(fixture_error)?;
        let candidate = HistoryExpiryCandidate::new(
            StorageIdentityHistoryKind::Object,
            facts.target(),
            Some(HistoryRetentionAnchor::Closed(metadata.valid_to().unwrap())),
            vec![facts.attribution()],
            false,
        )
        .map_err(fixture_error)?
        .with_read_span(facts.read_span().clone())
        .map_err(fixture_error)?
        .with_object_authorization(facts.clone())
        .map_err(fixture_error)?;
        // Logical read-gate fixture budget, not a measured expiry/executor input.
        assert!(matches!(
            selection
                .consider(
                    candidate,
                    HistoryExpiryInputBytes::new(1).map_err(fixture_error)?,
                    &[]
                )
                .map_err(fixture_error)?,
            HistoryExpirySelectionOutcome::Selected
        ));
        let manifest = selection.finish().map_err(fixture_error)?;
        let manifest_identity = manifest.identity();
        fixture_intent = Some(manifest_identity);
        let authority = snapshot
            .control()
            .admit_expiry(manifest, snapshot.fence())
            .map_err(fixture_error)?
            .next()
            .authority()
            .clone();
        let mapping = Remap {
            collection: (object.collection_id().id(), 1_000_001),
            class: (object.class_id().id(), 2_000_001),
            object: (object.id().id(), 3_000_001),
        };
        let mut backup = mapping
            .snapshot(fixture.capture(true).await?)
            .map_err(fixture_error)?;
        repair_snapshot = Some(backup.clone());
        let mismatch = match condition {
            "object_uuid" => Some(facts.target().resource()),
            "collection_uuid" => Some(facts.attribution().collection()),
            "class_uuid" => Some(class_uuid),
            _ => None,
        };
        if let Some(identity) = mismatch {
            backup = replace_identity(backup, identity);
        }
        let original_permit = storage.capture_application_execution(None).await?;
        retired_permit = Some(original_permit.clone());
        if let Err(error) = fixture
            .restore_with_recovery(backup, &restore_repository, &signing_key)
            .await
        {
            if mismatch.is_some() {
                assert!(matches!(
                    condition,
                    "object_uuid" | "collection_uuid" | "class_uuid"
                ));
                return Ok::<_, ApiError>(("restore_rejected", Some(error)));
            }
            return Err(error);
        }
        if mismatch.is_none() {
            for (kind, old, mapped, expected) in [
                (
                    StorageIdentityResourceKind::Object,
                    mapping.object.0,
                    mapping.object.1,
                    facts.target().resource(),
                ),
                (
                    StorageIdentityResourceKind::Collection,
                    mapping.collection.0,
                    mapping.collection.1,
                    facts.attribution().collection(),
                ),
                (
                    StorageIdentityResourceKind::Class,
                    mapping.class.0,
                    mapping.class.1,
                    class_uuid,
                ),
            ] {
                let evidence = storage
                    .resolve_logical_identity(StorageIdentityTarget::Resource {
                        kind,
                        id: ResourceId::new(mapped).map_err(fixture_error)?,
                    })
                    .await?;
                if old == mapped
                    || evidence.has_unknown_restore_source()
                    || evidence
                        .subject()
                        .and_then(|s| s.record_identity())
                        .is_none_or(|record| record.resource() != expected)
                {
                    return Err(ApiError::ServiceUnavailable(
                        "Restore did not establish the original UUID at changed coordinates".into(),
                    ));
                }
            }
        }
        if let Err(error) =
            prepare_history_read_fixture(storage, authority, &repository, &signing_key).await
        {
            if mismatch.is_some() {
                let expected = if condition == "object_uuid" {
                    "first receipt"
                } else {
                    "differs from retained membership"
                };
                assert!(
                    error.to_string().contains(expected),
                    "Unexpected correspondence rejection: {error}"
                );
                let application = storage.read_application_control().await?;
                assert!(!application.is_released_for(&storage.read_deletion_control().await?));
                let retained = storage.capture_history_lifecycle().await?;
                assert!(
                    retained
                        .control()
                        .authority()
                        .expiry()
                        .intent(manifest_identity)
                        .unwrap()
                        .removals()
                        .is_empty()
                );
                return Ok(("reapplication_rejected", Some(error)));
            }
            return Err(error);
        }
        let runtime = read.register_history_runtime(None).await?;
        let runtime_owner = runtime.owner();
        pending_runtime = Some(runtime);
        let permit = read
            .capture_application_execution(Some(runtime_owner))
            .await?;
        let scope = StorageExecutionScope::default().with_application_access(permit);
        let publication = read
            .run_in_scope(
                scope.clone(),
                read.admit_history_publication(HistoryPublicationKind::TaskOutput),
            )
            .await?;
        let publication_context = publication.context();
        pending_publication = Some(publication);
        let scope = scope.with_history_read(publication_context.clone());
        let policy = Arc::new(MockTreetopBackend::new());
        let (object_id, collection_id, class_id) = if condition == "old_coordinates" {
            (mapping.object.0, mapping.collection.0, mapping.class.0)
        } else {
            (mapping.object.1, mapping.collection.1, mapping.class.1)
        };
        policy.add_rule(MockAllowRule {
            group_id: group.id().id(),
            action: Permissions::ReadClass,
            resource_kind: ResourceKind::Class,
            resource_id: Some(class_id),
            attrs: ResourceFields::default(),
        });
        for name in ["original_member", "current_member"] {
            if (condition == "historical_denied" && name == "original_member")
                || (condition == "current_denied" && name == "current_member")
            {
                continue;
            }
            policy.add_rule(MockAllowRule {
                group_id: group.id().id(),
                action: Permissions::ReadObject,
                resource_kind: ResourceKind::Object,
                resource_id: Some(object_id),
                attrs: ResourceFields {
                    collection_id: Some(collection_id),
                    class_id: Some(class_id),
                    name: Some(name.into()),
                    ..Default::default()
                },
            });
        }
        let context = history_application_context(read.clone(), policy);
        let principal = read
            .run_in_scope(
                scope.clone(),
                read.get_authentication_identity(principal_id),
            )
            .await?
            .into_parts()
            .0;
        if condition == "revoked" {
            storage
                .run_in_scope(
                    scope.clone(),
                    storage.remove_group_member(principal_id, group.id(), &event),
                )
                .await?
                .into_value();
        }
        let selected = StorageClassObjectsAsOfQuery::try_new(
            ClassId::new(mapping.class.1).map_err(fixture_error)?,
            at,
            QueryOptions::new(vec![], vec![], Some(2), None, false).map_err(fixture_error)?,
        )
        .map_err(fixture_error)?
        .for_resource(class_uuid);
        let productive = if condition == "stale_scope" {
            StorageExecutionScope::default()
                .with_application_access(original_permit)
                .with_history_read(publication_context)
        } else {
            scope.clone()
        };
        let observed = read
            .run_in_scope(
                productive,
                history_population_as_of(&context, &principal, selected),
            )
            .await;
        eprintln!(
            "remapping {condition} {} read_outcome={}",
            kind.as_str(),
            match &observed {
                Ok(_) => "success",
                Err(ApiError::NotFound(_)) => "not_found",
                Err(ApiError::Forbidden(_)) => "forbidden",
                Err(ApiError::ServiceUnavailable(_)) => "unavailable",
                Err(_) => "other_error",
            }
        );
        let retained = storage.capture_history_lifecycle().await?;
        let retained_facts = retained
            .control()
            .authority()
            .expiry()
            .intent(manifest_identity)
            .expect("fixture manifest must remain in retained authority")
            .manifest()
            .members()[0]
            .object_authorization()
            .unwrap();
        assert_eq!(
            serde_json::to_value(retained_facts).unwrap(),
            original_facts
        );
        let stage = "read";
        let error = observed.err();
        match condition {
            "allowed" => assert!(matches!(
                error.as_ref(),
                Some(ApiError::NotFound(message))
                    if message == "Historical object population contains an expired version"
            )),
            "historical_denied" | "current_denied" | "old_coordinates" | "revoked" => {
                assert!(matches!(error, Some(ApiError::Forbidden(_))))
            }
            "stale_scope" => assert!(matches!(error, Some(ApiError::ServiceUnavailable(_)))),
            _ => assert!(
                stage == "restore_rejected"
                    || matches!(error, Some(ApiError::ServiceUnavailable(_)))
            ),
        }
        Ok::<_, ApiError>((stage, error))
    })
    .catch_unwind()
    .await;
    // Every fixture-issued future has now returned or unwound. There is no
    // transport or detached producer. Exact settlement waits native SHARE work.
    let mut cleanup_errors = Vec::new();
    if let Some(publication) = pending_publication {
        match publication
            .after_drained(std::future::ready(Ok(())), std::future::ready(Ok(())))
            .await
        {
            Ok(settled) => {
                if let Err(error) = read.settle_history_publication(settled).await {
                    cleanup_errors.push(error.to_string());
                }
            }
            Err(error) => cleanup_errors.push(error.to_string()),
        }
    }
    if let Some(runtime) = pending_runtime
        && let Err(error) = read.unregister_runtime(runtime).await
    {
        cleanup_errors.push(error.to_string());
    }
    if kind == StorageBackendKind::Postgres {
        if let Some((directory, key)) = recovery_paths {
            // Reintroduce the known correct source before restoring the original
            // fixture image. A mismatched UUID cannot fabricate a first receipt;
            // teardown must let real protected replay establish that outcome.
            if let Some(snapshot) = repair_snapshot {
                if let Err(error) = fixture
                    .restore_with_recovery(snapshot, &directory, &key)
                    .await
                {
                    cleanup_errors.push(format!("correct source recovery: {error}"));
                }
            } else if let Err(error) = fixture.resume_restore_with_recovery(&directory, &key).await
            {
                cleanup_errors.push(format!("committed restore recovery: {error}"));
            }
            if cleanup_errors.is_empty()
                && let Err(error) = fixture
                    .restore_original_with_recovery(&directory, &key)
                    .await
            {
                cleanup_errors.push(format!("protected original restore: {error}"));
            }
        }
        if cleanup_errors.is_empty() {
            match (
                storage.capture_history_lifecycle().await,
                storage.read_deletion_control().await,
                storage.read_application_control().await,
            ) {
                (Ok(expected), Ok(expected_deletion), Ok(expected_application)) => {
                    let reset = async {
                        // Same fence, deliberately absent reapplication/checkpoint: a
                        // fence-only CAS would wrongly accept this observed state.
                        let altered = ApplicationControlState::new(
                            expected_application.fence(),
                            None,
                            expected_application.released_revision(),
                            None,
                        )
                        .map_err(fixture_error)?;
                        if altered == expected_application {
                            return Err(fixture_error(
                                "Signed fixture reapplication proof missing",
                            ));
                        }
                        if restore_fixture_authority(
                            &pool,
                            before.control(),
                            &before_deletion,
                            expected.control(),
                            &expected_deletion,
                            fixture_intent,
                            &altered,
                        )
                        .await
                        .is_ok()
                        {
                            return Err(fixture_error("Incomplete application CAS accepted"));
                        }
                        if storage.read_application_control().await? != expected_application
                            || storage.read_deletion_control().await? != expected_deletion
                            || storage.capture_history_lifecycle().await?.control()
                                != expected.control()
                        {
                            return Err(fixture_error(
                                "Rejected fixture CAS changed protected control",
                            ));
                        }
                        restore_fixture_authority(
                            &pool,
                            before.control(),
                            &before_deletion,
                            expected.control(),
                            &expected_deletion,
                            fixture_intent,
                            &expected_application,
                        )
                        .await
                    }
                    .await;
                    if let Err(error) = &reset {
                        cleanup_errors.push(error.to_string());
                    } else if let Ok(initial_application) = reset {
                        match (
                            storage.capture_history_lifecycle().await,
                            storage.read_deletion_control().await,
                            storage.read_application_control().await,
                        ) {
                            (Ok(actual), Ok(deletion), Ok(application))
                                if actual.control() == before.control()
                                    && deletion == before_deletion
                                    && application == initial_application
                                    && application.fence().incarnation()
                                        != expected_application.fence().incarnation() => {}
                            (Err(error), _, _) | (_, Err(error), _) | (_, _, Err(error)) => {
                                cleanup_errors.push(error.to_string())
                            }
                            _ => cleanup_errors
                                .push("Fixture protected authority differs after teardown".into()),
                        }
                    }
                }
                (Err(error), _, _) | (_, Err(error), _) | (_, _, Err(error)) => {
                    cleanup_errors.push(error.to_string())
                }
            }
        }
    }
    if kind == StorageBackendKind::Postgres
        && cleanup_errors.is_empty()
        && let Some(permit) = retired_permit
        && read
            .run_in_scope(
                StorageExecutionScope::default().with_application_access(permit),
                read.get_authentication_identity(PrincipalId::new(1).unwrap()),
            )
            .await
            .err()
            .is_none_or(|error| error.kind() != StorageErrorKind::Unavailable)
    {
        cleanup_errors.push("Retired fixture permit became productive after teardown".into());
    }
    let (stage, _error) = match result {
        Err(primary) => {
            if !cleanup_errors.is_empty() {
                eprintln!("secondary fixture cleanup errors: {cleanup_errors:?}");
            }
            std::panic::resume_unwind(primary)
        }
        Ok(Err(primary)) => panic!(
            "{} {condition} primary setup failure: {primary}; secondary cleanup errors: {cleanup_errors:?}",
            kind.as_str()
        ),
        Ok(Ok(outcome)) => {
            assert!(
                cleanup_errors.is_empty(),
                "fixture cleanup failed: {cleanup_errors:?}"
            );
            outcome
        }
    };
    eprintln!(
        "remapping {condition} {} outcome_stage={stage}",
        kind.as_str()
    );
}
