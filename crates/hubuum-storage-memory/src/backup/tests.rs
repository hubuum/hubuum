use super::*;

fn run(future: impl Future<Output = ()>) {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(future);
}

fn restored(snapshot: StorageBackupSnapshot) -> MemoryStorage {
    MemoryStorage {
        state: Arc::new(RwLock::new(restore(snapshot).unwrap())),
    }
}

fn edit_history(
    snapshot: StorageBackupSnapshot,
    section: StorageBackupHistorySection,
    edit: impl FnOnce(&mut Vec<StorageBackupRow>),
) -> StorageBackupSnapshot {
    let (state, history) = snapshot.into_parts();
    let mut history = history.unwrap();
    edit(history.get_mut(&section).unwrap());
    StorageBackupSnapshot::try_new(state, Some(history)).unwrap()
}

#[test]
fn schema_history_allocation_survives_reordered_backup_rows() {
    run(async {
        let storage = MemoryStorage::new();
        for name in ["first schema", "second schema"] {
            storage
                .create_class(
                    StorageClassCreate::builder(name, CollectionId::new(1).unwrap(), "history")
                        .build(),
                    &EventContext::system(),
                )
                .await
                .unwrap()
                .into_value();
        }
        let snapshot = edit_history(
            storage.capture_backup_snapshot(true).await.unwrap(),
            StorageBackupHistorySection::ClassSchemaHistory,
            |rows| rows.reverse(),
        );
        let storage = restored(snapshot);
        storage
            .create_class(
                StorageClassCreate::builder(
                    "third schema",
                    CollectionId::new(1).unwrap(),
                    "history",
                )
                .build(),
                &EventContext::system(),
            )
            .await
            .unwrap()
            .into_value();
        let (_, history) = storage
            .capture_backup_snapshot(true)
            .await
            .unwrap()
            .into_parts();
        assert_eq!(
            history.unwrap()[&StorageBackupHistorySection::ClassSchemaHistory].len(),
            3
        );
    });
}

fn set_field(row: &mut StorageBackupRow, field: &str, value: Value) {
    let mut fields = row.fields().clone();
    fields.insert(field.to_string(), value);
    *row = StorageBackupRow::try_from_value(Value::Object(fields)).unwrap();
}

async fn queued_task(storage: &MemoryStorage) -> TaskId {
    storage
        .create_task(
            StorageTaskCreateRequest::builder(
                StorageTaskKind::Export,
                PrincipalId::new(1).unwrap(),
                json!({}),
                0,
            )
            .try_build(10)
            .unwrap(),
        )
        .await
        .unwrap()
        .id()
}

async fn completed_task(storage: &MemoryStorage) -> TaskId {
    let id = queued_task(storage).await;
    let mut state = storage.state.write().await;
    let task = state.tasks.get_mut(&id.id()).unwrap();
    task.status = StorageTaskStatus::Succeeded;
    task.finished_at = Some(Utc::now());
    task.updated_at = task.finished_at.unwrap();
    state
        .append_task_event_record(
            id,
            StorageTaskEventInput::new("succeeded", "Export finished")
                .with_data(Some(json!({"rows": 3}))),
        )
        .unwrap();
    id
}

async fn task_events(storage: &MemoryStorage, task_id: TaskId) -> Vec<StorageTaskEvent> {
    storage
        .list_task_events(StorageTaskChildListQuery::new(
            task_id,
            QueryOptions::empty(),
        ))
        .await
        .unwrap()
        .into_parts()
        .0
}

#[test]
fn external_memberships_use_directory_revocation_provenance() {
    run(async {
        let storage = MemoryStorage::new();
        let key = "cn=operators,ou=groups,dc=example,dc=com";
        let _ = storage
            .sync_external_user(
                StorageExternalUserSync::builder("directory", LDAP_PROVIDER_KIND, "alice", "alice")
                    .groups(vec![StorageExternalGroup::new(key, "operators", None)])
                    .build(),
            )
            .await
            .unwrap();
        let (sections, _) = storage
            .capture_backup_snapshot(false)
            .await
            .unwrap()
            .into_parts();
        let group = sections[&StorageBackupStateSection::Groups]
            .iter()
            .find(|row| row.get("external_key") == Some(&json!(key)))
            .unwrap();
        let source = sections[&StorageBackupStateSection::GroupMembershipSources]
            .iter()
            .find(|row| row.get("group_id") == group.get("id"))
            .unwrap();
        assert_eq!(
            (
                source.get("source"),
                source.get("source_scope_id"),
                source.get("source_key")
            ),
            (
                Some(&json!(EXTERNAL_MEMBERSHIP_SOURCE)),
                group.get("identity_scope_id"),
                Some(&json!(key))
            )
        );
    });
}

#[test]
fn completed_task_logs_survive_repeated_backup_restores() {
    run(async {
        let storage = MemoryStorage::new();
        // Interleave another audit event to exercise the shared event sequence.
        let _ = storage
            .update_collection(
                CollectionId::new(1).unwrap(),
                StorageCollectionUpdate::new(None, Some("Updated root".to_string())),
                &EventContext::system(),
            )
            .await
            .unwrap();
        let id = completed_task(&storage).await;
        let expected = task_events(&storage, id).await;
        assert_eq!(expected.len(), 2);
        let snapshot = storage.capture_backup_snapshot(true).await.unwrap();
        let (_, history) = snapshot.clone().into_parts();
        let audit_ids = history.unwrap()[&StorageBackupHistorySection::AuditEvents]
            .iter()
            .map(|row| Row(row).number("id").unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            audit_ids.iter().collect::<BTreeSet<_>>().len(),
            audit_ids.len()
        );
        let first = restored(snapshot);
        let second = restored(first.capture_backup_snapshot(true).await.unwrap());
        assert!(task_events(&second, id).await == expected);
    });
}

#[test]
fn backups_exclude_active_task_logs() {
    run(async {
        let storage = MemoryStorage::new();
        let id = queued_task(&storage).await;
        assert_eq!(task_events(&storage, id).await.len(), 1);
        let (_, history) = storage
            .capture_backup_snapshot(true)
            .await
            .unwrap()
            .into_parts();
        assert!(history.unwrap()[&StorageBackupHistorySection::AuditEvents].is_empty());
    });
}

#[test]
fn postgres_task_audit_rows_restore_the_task_event_read_model() {
    run(async {
        let storage = MemoryStorage::new();
        let id = completed_task(&storage).await;
        let now = Utc::now();
        let snapshot = edit_history(
            storage.capture_backup_snapshot(true).await.unwrap(),
            StorageBackupHistorySection::AuditEvents,
            |rows| {
                *rows = vec![row(json!({
                    "id": 200, "event_id": Uuid::new_v4(), "occurred_at": now,
                    "entity_type": "task", "entity_id": id.id(), "entity_name": null,
                    "collection_id": null, "action": "succeeded", "actor_principal_id": 1,
                    "actor_kind": "user", "initiator_principal_id": 1, "task_id": id.id(),
                    "request_id": null, "correlation_id": null, "summary": "Imported completion",
                    "before": null, "after": null, "metadata": {"data": {"rows": 7}},
                    "schema_version": 1, "dispatched_at": now,
                    "before_revision": null, "after_revision": null
                })).unwrap()];
            },
        );
        let storage = restored(snapshot);
        let expected = StorageTaskEvent::builder(
            EventSequence::new(200).unwrap(),
            id,
            "succeeded",
            "Imported completion",
            now,
            "user",
        )
        .data(Some(json!({"rows": 7})))
        .actor_principal_id(Some(PrincipalId::new(1).unwrap()))
        .provenance(Some(PrincipalId::new(1).unwrap()), Some(id))
        .build();
        assert!(task_events(&storage, id).await == vec![expected]);
    });
}

#[test]
fn task_event_sequences_continue_after_imported_history() {
    run(async {
        let storage = MemoryStorage::new();
        let id = completed_task(&storage).await;
        let snapshot = edit_history(
            storage.capture_backup_snapshot(true).await.unwrap(),
            StorageBackupHistorySection::AuditEvents,
            |rows| {
                set_field(&mut rows[0], "id", json!(200));
                set_field(&mut rows[1], "id", json!(201));
                rows.reverse();
            },
        );
        let storage = restored(snapshot);
        storage
            .state
            .write()
            .await
            .append_task_event_record(
                id,
                StorageTaskEventInput::new("cleanup", "Retained output cleaned up"),
            )
            .unwrap();
        let ids = task_events(&storage, id)
            .await
            .iter()
            .map(|event| event.id().get())
            .collect::<Vec<_>>();
        assert_eq!(ids, vec![200, 201, 202]);
    });
}

async fn updated_collection_snapshot() -> StorageBackupSnapshot {
    let storage = MemoryStorage::new();
    let _ = storage
        .update_collection(
            CollectionId::new(1).unwrap(),
            StorageCollectionUpdate::new(None, Some("Updated root".to_string())),
            &EventContext::system(),
        )
        .await
        .unwrap();
    storage.capture_backup_snapshot(true).await.unwrap()
}

async fn collection_as_of(storage: &MemoryStorage, at: DateTime<Utc>) -> StorageCollection {
    storage
        .get_collection_history_as_of(StorageHistoryAsOfQuery::new(
            ResourceId::new(1).unwrap(),
            at,
        ))
        .await
        .unwrap()
        .unwrap()
        .into_parts()
        .0
}

#[test]
fn imported_temporal_history_uses_timestamps_before_entry_ids() {
    run(async {
        let at = Utc::now() + Duration::seconds(1);
        let snapshot = edit_history(
            updated_collection_snapshot().await,
            StorageBackupHistorySection::CollectionHistory,
            |rows| {
                set_field(&mut rows[0], "history_entry_id", json!(30));
                set_field(&mut rows[0], "valid_from", json!(at - Duration::seconds(1)));
                set_field(&mut rows[0], "valid_to", json!(at));
                set_field(&mut rows[1], "history_entry_id", json!(20));
                set_field(&mut rows[1], "valid_from", json!(at));
                rows.reverse();
            },
        );
        let storage = restored(snapshot);
        assert_eq!(collection_as_of(&storage, at).await.revision().get(), 2);
    });
}

#[test]
fn imported_temporal_history_breaks_timestamp_ties_by_entry_id() {
    run(async {
        let at = Utc::now() + Duration::seconds(1);
        let snapshot = edit_history(
            updated_collection_snapshot().await,
            StorageBackupHistorySection::CollectionHistory,
            |rows| {
                set_field(&mut rows[0], "valid_from", json!(at));
                set_field(&mut rows[0], "valid_to", json!(at));
                set_field(&mut rows[1], "valid_from", json!(at));
                rows.reverse();
            },
        );
        let storage = restored(snapshot);
        assert_eq!(collection_as_of(&storage, at).await.revision().get(), 2);
    });
}

async fn deleted_task_snapshot(
    storage: &MemoryStorage,
) -> (StorageBackupSnapshot, TaskId, DateTime<Utc>) {
    let id = completed_task(storage).await;
    let deleted_at = Utc::now();
    let snapshot = edit_history(
        storage.capture_backup_snapshot(true).await.unwrap(),
        StorageBackupHistorySection::TerminalTasks,
        |rows| {
            let row = rows
                .iter_mut()
                .find(|row| row.get("id") == Some(&json!(id.id())))
                .unwrap();
            set_field(row, "deleted_at", json!(deleted_at));
            set_field(row, "deleted_by", json!(1));
            set_field(row, "updated_at", json!(deleted_at));
        },
    );
    (snapshot, id, deleted_at)
}

#[test]
fn restored_task_access_preserves_soft_deletion_metadata() {
    run(async {
        let (snapshot, id, deleted_at) = deleted_task_snapshot(&MemoryStorage::new()).await;
        let task = restored(snapshot)
            .get_task_access(id)
            .await
            .unwrap()
            .into_parts()
            .0;
        assert_eq!(
            (task.deleted_at(), task.deleted_by()),
            (Some(deleted_at), Some(PrincipalId::new(1).unwrap()))
        );
    });
}

#[test]
fn restored_task_list_excludes_soft_deleted_tasks() {
    run(async {
        let storage = MemoryStorage::new();
        let visible = completed_task(&storage).await;
        let (snapshot, _, _) = deleted_task_snapshot(&storage).await;
        let storage = restored(snapshot);
        let tasks = storage
            .list_tasks(StorageTaskListQuery::new(
                None,
                None,
                None,
                QueryOptions::empty(),
            ))
            .await
            .unwrap()
            .into_parts()
            .0;
        assert_eq!(
            tasks.iter().map(StorageTask::id).collect::<Vec<_>>(),
            vec![visible]
        );
    });
}

fn snapshot_with_correlation(correlation: Value) -> StorageBackupSnapshot {
    let mut state = MemoryState::new();
    state
        .append_simple_event(
            EntityType::Restore,
            1,
            None,
            Action::Succeeded,
            &EventContext::system(),
            "Restore completed",
        )
        .unwrap();
    edit_history(
        capture(&state, true).unwrap(),
        StorageBackupHistorySection::AuditEvents,
        |rows| {
            set_field(&mut rows[0], "correlation_id", correlation);
        },
    )
}

#[test]
fn restore_normalizes_supported_legacy_correlation_strings() {
    let state = restore(snapshot_with_correlation(json!(
        "legacy invalid correlation"
    )))
    .unwrap();
    assert!(
        state.events[0]
            .clone()
            .into_parts()
            .0
            .correlation_id()
            .is_none()
    );
}

#[test]
fn restore_preserves_valid_correlation_ids() {
    let correlation = "backup-restore-123";
    let state = restore(snapshot_with_correlation(json!(correlation))).unwrap();
    assert_eq!(
        state.events[0].clone().into_parts().0.correlation_id(),
        Some(&CorrelationId::new(correlation).unwrap())
    );
}

#[test]
fn restore_rejects_malformed_correlation_field_types() {
    assert!(restore(snapshot_with_correlation(json!(123))).is_err());
}
