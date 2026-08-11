//! Compile-time-adjacent guards for the application/storage boundary.

#[cfg(test)]
use std::fs;
#[cfg(test)]
use std::path::{Path, PathBuf};

#[cfg(test)]
fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

#[cfg(test)]
fn rust_files(directory: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let mut pending = vec![directory.to_path_buf()];

    while let Some(current) = pending.pop() {
        for entry in fs::read_dir(&current)
            .unwrap_or_else(|error| panic!("could not read {}: {error}", current.display()))
        {
            let path = entry.expect("directory entry should be readable").path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                files.push(path);
            }
        }
    }

    files.sort();
    files
}

#[cfg(test)]
fn is_storage_adapter(root: &Path, path: &Path) -> bool {
    ["postgres", "memory"].into_iter().any(|adapter| {
        path == root.join(format!("src/storage/{adapter}.rs"))
            || path.starts_with(root.join(format!("src/storage/{adapter}")))
    })
}

#[test]
fn app_context_exposes_only_an_opaque_backend_handle() {
    let root = repository_root();
    let context_path = root.join("src/permissions/context.rs");
    let context_source = fs::read_to_string(&context_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", context_path.display()));
    let trait_path = root.join("src/storage/context.rs");
    let trait_source = fs::read_to_string(&trait_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", trait_path.display()));
    let application_path = root.join("src/application.rs");
    let application_source = fs::read_to_string(&application_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", application_path.display()));

    assert!(
        context_source.contains("backend: StorageHandle"),
        "AppContext must own the opaque backend handle"
    );
    for forbidden in [
        "pub db_pool:",
        "pub fn postgres_pool",
        "pub fn clone_postgres_pool",
        "impl Deref for AppContext",
        "crate::storage::postgres",
        "PostgresPool",
        "fn postgres",
    ] {
        assert!(
            !context_source.contains(forbidden),
            "AppContext exposes forbidden backend detail: {forbidden}"
        );
    }
    let backend_context_body = trait_source
        .split_once("pub trait StorageContext")
        .and_then(|(_, remainder)| remainder.split_once("\n}"))
        .map(|(body, _)| body)
        .expect("StorageContext should have a readable trait body");
    assert!(
        !backend_context_body.contains("fn db_pool"),
        "StorageContext must not expose a database-pool accessor"
    );
    assert!(
        trait_source.contains("pub(in crate::storage) fn postgres_pool"),
        "only storage adapter modules may recover the PostgreSQL pool"
    );
    assert!(
        !application_source.contains(".app_data(Data::new(app_pool"),
        "the production HTTP server must not register a raw database pool"
    );
    assert!(
        !application_source.contains("pool: db::PostgresPool"),
        "operational HTTP services must receive AppContext rather than PostgresPool"
    );
}

#[test]
fn application_consumers_do_not_import_database_implementation_details() {
    let root = repository_root();
    let mut paths = Vec::new();
    for directory in [
        "src/api",
        "src/services",
        "src/extractors",
        "src/middlewares",
        "src/observability/metrics",
        "src/permissions",
    ] {
        paths.extend(rust_files(&root.join(directory)));
    }
    for file in [
        "src/auth.rs",
        "src/backups/mod.rs",
        "src/events/delivery.rs",
        "src/events/fanout.rs",
        "src/events/retention.rs",
        "src/exports/mod.rs",
        "src/restores/mod.rs",
        "src/tasks/helpers.rs",
        "src/tasks/execution.rs",
        "src/tasks/planning.rs",
        "src/tasks/preload.rs",
        "src/tasks/resolution.rs",
        "src/tasks/remote_call.rs",
        "src/token_retention.rs",
        "src/traits/mod.rs",
        "src/traits/authz.rs",
        "src/traits/permissions.rs",
    ] {
        paths.push(root.join(file));
    }

    let mut violations = Vec::new();
    for path in paths {
        let source = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("could not read {}: {error}", path.display()));
        let source = if path.starts_with(root.join("src/services"))
            || path == root.join("src/exports/mod.rs")
        {
            source.split("#[cfg(test)]").next().unwrap_or(&source)
        } else {
            &source
        };
        for forbidden in [
            "crate::storage::postgres",
            "PostgresPool",
            "postgres_pool",
            "diesel::",
            "diesel_async",
            "pool: AppContext",
            "pool: &AppContext",
        ] {
            if source.contains(forbidden) {
                violations.push(format!("{} imports or uses {forbidden}", path.display()));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "application consumers crossed the opaque backend boundary:\n{}",
        violations.join("\n")
    );
}

#[test]
fn backend_neutral_layers_do_not_import_database_implementation_details() {
    let root = repository_root();
    let mut violations = Vec::new();

    for directory in ["src/services", "src/storage"] {
        for path in rust_files(&root.join(directory)) {
            if is_storage_adapter(&root, &path)
                || path == root.join("src/storage/context.rs")
                || path == root.join("src/storage/capabilities.rs")
                || path == root.join("src/storage/factory.rs")
            {
                continue;
            }

            let source = fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("could not read {}: {error}", path.display()));
            let source = if path.starts_with(root.join("src/services")) {
                source.split("#[cfg(test)]").next().unwrap_or(&source)
            } else {
                &source
            };
            for forbidden in [
                "crate::storage::postgres::operations",
                "crate::storage::postgres::with_connection",
                "crate::storage::postgres::with_transaction",
                "diesel::",
                "diesel_async",
                "postgres_pool",
            ] {
                if source.contains(forbidden) {
                    violations.push(format!("{} imports or uses {forbidden}", path.display()));
                }
            }
            if path.starts_with(root.join("src/storage")) && source.contains("ApiError") {
                violations.push(format!(
                    "{} couples backend-neutral storage to ApiError",
                    path.display()
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "backend-neutral service/storage code crossed into the PostgreSQL compatibility layer:\n{}",
        violations.join("\n")
    );
}

#[test]
fn object_domain_types_are_free_of_persistence_implementation_details() {
    let root = repository_root();
    let mut violations = Vec::new();

    for relative_path in ["src/models/object.rs", "src/models/traits/object.rs"] {
        let path = root.join(relative_path);
        let source = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("could not read {}: {error}", path.display()));
        for forbidden in [
            "diesel::",
            "diesel(",
            "crate::schema",
            "storage::postgres",
            "CursorSqlMapping",
            "CursorSqlField",
            "CursorSqlType",
        ] {
            if source.contains(forbidden) {
                violations.push(format!("{} contains {forbidden}", path.display()));
            }
        }
    }

    let adapter_path = root.join("src/storage/postgres/operations/object.rs");
    let adapter_source = fs::read_to_string(&adapter_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", adapter_path.display()));
    for required in [
        "struct HubuumObjectRow",
        "struct NewHubuumObjectRow",
        "struct UpdateHubuumObjectRow",
        "impl From<HubuumObjectRow> for HubuumObject",
        "impl CursorSqlMapping for HubuumObjectRow",
    ] {
        assert!(
            adapter_source.contains(required),
            "PostgreSQL object adapter is missing {required}"
        );
    }

    assert!(
        violations.is_empty(),
        "object domain types crossed into persistence details:\n{}",
        violations.join("\n")
    );
}

#[test]
fn relation_domain_types_are_free_of_persistence_implementation_details() {
    let root = repository_root();
    let mut violations = Vec::new();

    for relative_path in [
        "src/models/relation.rs",
        "src/models/traits/class_relation.rs",
        "src/models/traits/object_relation.rs",
    ] {
        let path = root.join(relative_path);
        let source = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("could not read {}: {error}", path.display()));
        let production_source = source.split("#[cfg(test)]").next().unwrap_or(&source);
        for forbidden in [
            "diesel::",
            "diesel(",
            "crate::schema",
            "storage::postgres",
            "CursorSqlMapping",
            "CursorSqlField",
            "CursorSqlType",
        ] {
            if production_source.contains(forbidden) {
                violations.push(format!("{} contains {forbidden}", path.display()));
            }
        }
    }

    let adapter_path = root.join("src/storage/postgres/operations/relation_rows.rs");
    let adapter_source = fs::read_to_string(&adapter_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", adapter_path.display()));
    for required in [
        "struct HubuumClassRelationRow",
        "struct NewHubuumClassRelationRow",
        "struct HubuumObjectRelationRow",
        "struct NewHubuumObjectRelationRow",
        "struct HubuumClassRelationTransitiveRow",
        "struct ClassGraphQueryRow",
        "struct RelatedObjectGraphQueryRow",
        "impl CursorSqlMapping for HubuumClassRelationRow",
        "impl CursorSqlMapping for HubuumObjectRelationRow",
        "impl CursorSqlMapping for HubuumClassRelationTransitiveRow",
        "impl CursorSqlMapping for ClassGraphQueryRow",
        "impl CursorSqlMapping for RelatedObjectGraphQueryRow",
    ] {
        assert!(
            adapter_source.contains(required),
            "PostgreSQL relation adapter is missing {required}"
        );
    }

    assert!(
        violations.is_empty(),
        "relation domain types crossed into persistence details:\n{}",
        violations.join("\n")
    );
}

#[test]
fn selectable_storage_backends_are_complete_and_test_models_are_not_selectable() {
    let root = repository_root();
    let contract_path = root.join("src/storage/contract.rs");
    let contract_source = fs::read_to_string(&contract_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", contract_path.display()));
    let context_path = root.join("src/storage/context.rs");
    let context_source = fs::read_to_string(&context_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", context_path.display()));

    let contract_body = contract_source
        .split_once("pub(crate) trait StorageBackend:")
        .and_then(|(_, remainder)| remainder.split_once("\n{"))
        .map(|(body, _)| body)
        .expect("StorageBackend should have a readable aggregate trait declaration");
    for required in [
        "LifecycleStorage",
        "AuthenticationStorage",
        "IdentityStorage",
        "AuthorizationStorage",
        "CatalogStorage",
        "ComputedFieldLifecycleStorage",
        "ComputedObjectStorage",
        "ObjectAggregateStorage",
        "RelationQueryStorage",
        "AuditEventStorage",
        "EventSubscriptionStorage",
        "EventDeliveryAdministrationStorage",
        "EventDeliveryStorage",
        "EventFanoutStorage",
        "EventHealthStorage",
        "EventRetentionStorage",
        "MetricsStorage",
        "OperationalStateStorage",
        "TokenRetentionStorage",
        "HistoryStorage",
        "InventoryStorage",
        "UnifiedSearchStorage",
        "ObjectRecordStorage",
        "RemoteTargetStorage",
        "TaskQueueStorage",
        "TaskExecutionStorage",
        "BackupSnapshotStorage",
        "RestoreStorage",
        "ImportStorage",
        "ExportQueryStorage",
        "ExportTemplateStorage",
        "StorageExecution",
        "sealed::CertifiedStorageBackend",
    ] {
        assert!(
            contract_body.contains(required),
            "complete storage contract is missing {required}"
        );
    }
    assert!(
        contract_source.contains("impl sealed::CertifiedStorageBackend for PostgresStorage"),
        "PostgreSQL must be explicitly certified in the central storage contract"
    );
    assert!(
        !contract_source.contains("CertifiedStorageBackend for MemoryStorageModel"),
        "the focused memory contract model must not be selectable as a full backend"
    );
    for forbidden_marker in ["WorkflowStorage", "OperationalStorage"] {
        assert!(
            !contract_source.contains(forbidden_marker),
            "complete storage certification must not rely on marker {forbidden_marker}"
        );
    }
    assert!(
        context_source.contains("assert_complete_storage_backend(&backend)"),
        "application composition must enforce the complete storage contract"
    );
    for operation in ["collections", "classes", "objects"] {
        assert!(
            context_source.contains(&format!("\"catalog\", \"{operation}\"")),
            "catalog operation {operation} must use the common storage observer"
        );
    }
    for operation in ["list", "enrich"] {
        assert!(
            context_source.contains(&format!("\"computed_objects\", \"{operation}\"")),
            "computed-object operation {operation} must use the common storage observer"
        );
    }
    let compact_context = context_source.split_whitespace().collect::<String>();
    for operation in [
        "state",
        "list_shared",
        "list_personal",
        "get",
        "create_shared",
        "update_shared",
        "delete_shared",
        "create_personal",
        "update_personal",
        "delete_personal",
        "request_rebuild",
        "execute_rebuild",
    ] {
        assert!(
            compact_context.contains(&format!("\"computed_fields\",\"{operation}\"")),
            "computed-field lifecycle operation {operation} must use the common storage observer"
        );
    }
    assert!(
        compact_context.contains("\"backup_snapshots\",\"snapshot\""),
        "backup snapshot creation must use the common storage observer"
    );
    for operation in [
        "stage",
        "get_job",
        "get_status",
        "expire",
        "start_draining",
        "apply",
        "fail_and_resume",
        "coordinator_snapshot",
        "resume_without_job",
        "resume_terminal",
        "tick",
        "drain_state",
        "remove_instance",
    ] {
        assert!(
            compact_context.contains(&format!("\"restores\",\"{operation}\"")),
            "restore operation {operation} must use the common storage observer"
        );
    }
    for operation in [
        "root_collection",
        "collection_by_id",
        "collection_by_key",
        "collections_by_name",
        "collection_child_by_name",
        "class_by_name",
        "classes_by_names",
        "object_by_name",
        "objects_by_names",
        "class_relation_exists",
        "object_relation_exists",
        "group_exists",
        "preflight",
        "apply_strict",
        "apply_best_effort",
        "record_results",
    ] {
        assert!(
            compact_context.contains(&format!("\"imports\",\"{operation}\"")),
            "import operation {operation} must use the common storage observer"
        );
    }
    for operation in [
        "get",
        "list",
        "create",
        "update",
        "delete",
        "record_invocation",
    ] {
        assert!(
            compact_context.contains(&format!("\"remote_targets\",\"{operation}\"")),
            "remote-target operation {operation} must use the common storage observer"
        );
    }
    for operation in [
        "get",
        "list",
        "list_in_collection",
        "class_collection",
        "create",
        "replace",
        "delete",
    ] {
        assert!(
            compact_context.contains(&format!("\"export_templates\",\"{operation}\"")),
            "export-template lifecycle operation {operation} must use the common storage observer"
        );
    }
    for operation in [
        "claim",
        "renew_lease",
        "recover_leases",
        "append_event",
        "update_state",
        "complete",
        "fail",
        "purge_export_outputs",
        "purge_backup_outputs",
    ] {
        assert!(
            compact_context.contains(&format!("\"task_execution\",\"{operation}\"")),
            "task execution operation {operation} must use the common storage observer"
        );
    }
    assert!(
        compact_context.contains("\"object_aggregates\",\"aggregate\""),
        "object aggregation must use the common storage observer"
    );
    assert!(
        compact_context.contains("\"inventory\",\"counts\""),
        "inventory counts must use the common storage observer"
    );
    for operation in [
        "validate",
        "validate_new",
        "validate_update",
        "save",
        "create",
        "update",
        "delete",
        "load",
        "collection",
        "class",
    ] {
        assert!(
            compact_context.contains(&format!("\"object_records\",\"{operation}\"")),
            "object record operation {operation} must use the common storage observer"
        );
    }
    for operation in [
        "create",
        "get_access",
        "list",
        "list_events",
        "list_import_results",
        "list_export_outputs",
        "list_backup_outputs",
        "get_export_summary",
        "get_backup_summary",
        "get_export_output",
        "get_backup_output",
    ] {
        assert!(
            compact_context.contains(&format!("\"tasks\",\"{operation}\"")),
            "task queue operation {operation} must use the common storage observer"
        );
    }
    for operation in [
        "authenticate_bearer_token",
        "load_identity",
        "load_token_scope",
    ] {
        assert!(
            compact_context.contains(&format!("\"authentication\",\"{operation}\"")),
            "authentication operation {operation} must use the common storage observer"
        );
    }
    for operation in [
        "default_admin_bootstrap_required",
        "bootstrap_default_admin",
        "reset_local_password",
        "ensure_scope",
        "load_scope_name",
        "load_scope_names",
        "load_membership",
        "list_tokens",
        "human_owner_group_member",
        "principal_is_disabled",
        "load_service_account",
        "load_service_account_point",
        "list_service_accounts",
        "create_service_account",
        "update_service_account",
        "disable_service_account",
        "delete_service_account",
        "load_external_state",
        "mark_external_sync_attempted",
        "sync_external_user",
    ] {
        assert!(
            compact_context.contains(&format!("\"identity\",\"{operation}\"")),
            "identity operation {operation} must use the common storage observer"
        );
    }
    for operation in [
        "load_principal",
        "principal_is_group_member",
        "load_classes",
        "load_objects",
        "authorize_local_collection",
        "authorize_local_collections",
        "local_authorized_collections",
        "list_collection_candidates",
        "list_group_candidates",
        "policy_snapshot",
        "list_local_collection_grants",
        "get_local_collection_grant",
        "load_local_collection_permission_set",
        "apply_local_collection_grant",
        "revoke_local_collection_grant",
        "revoke_all_local_collection_grants",
    ] {
        assert!(
            compact_context.contains(&format!("\"authorization\",\"{operation}\"")),
            "authorization operation {operation} must use the common storage observer"
        );
    }
    for operation in [
        "readiness_snapshot",
        "maintenance_state",
        "storage_snapshot",
        "task_queue_snapshot",
        "export_template_health",
        "export_templates_for_audit",
    ] {
        assert!(
            compact_context.contains(&format!("\"operational_state\",\"{operation}\"")),
            "operational-state operation {operation} must use the common storage observer"
        );
    }
    for operation in [
        "list_classes",
        "list_objects",
        "classes_touching",
        "objects_touching",
        "classes_touching_ids",
        "classes_between_ids",
        "objects_touching_ids",
        "objects_between_ids",
        "related_classes",
        "related_objects",
        "related_objects_for_roots",
        "bidirectional_objects_for_roots",
    ] {
        assert!(
            compact_context.contains(&format!("\"relations\",\"{operation}\"")),
            "relation operation {operation} must use the common storage observer"
        );
    }
    assert!(
        compact_context.contains("\"audit_events\",\"list\""),
        "audit event listing must use the common storage observer"
    );
    for operation in [
        "count_enabled_sinks",
        "list_sinks",
        "load_sink",
        "create_sink",
        "update_sink",
        "delete_sink",
        "list_subscriptions",
        "load_subscription",
        "create_subscription",
        "update_subscription",
        "delete_subscription",
    ] {
        assert!(
            compact_context.contains(&format!("\"event_subscriptions\",\"{operation}\"")),
            "event-subscription operation {operation} must use the common storage observer"
        );
    }
    for operation in ["list", "load", "release_for_retry", "mark_dead"] {
        assert!(
            compact_context.contains(&format!("\"event_delivery\",\"{operation}\"")),
            "event-delivery administration operation {operation} must use the common storage observer"
        );
    }
}

#[test]
fn process_entry_points_compose_only_through_backend_neutral_storage() {
    let root = repository_root();
    let mut violations = Vec::new();

    for file in [
        "src/application.rs",
        "src/administration.rs",
        "src/utilities/init.rs",
    ] {
        let path = root.join(file);
        let source = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("could not read {}: {error}", path.display()));
        let production = source
            .split("\n#[cfg(test)]\nmod tests")
            .next()
            .unwrap_or(&source);
        for forbidden in [
            "crate::storage::postgres",
            "hubuum_storage_postgres",
            "PostgresPool",
            "PgConnection",
            "diesel::",
            "diesel_async",
            "crate::schema",
            "StorageBackendKind::Postgresql",
        ] {
            if production.contains(forbidden) {
                violations.push(format!("{} imports or uses {forbidden}", path.display()));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "process entry points selected a storage implementation directly:\n{}",
        violations.join("\n")
    );
}

#[test]
fn event_administration_consumers_use_the_backend_neutral_application_service() {
    let root = repository_root();
    for file in [
        "src/models/event_subscription.rs",
        "src/application.rs",
        "src/api/v1/handlers/events.rs",
        "src/api/v1/handlers/event_sinks.rs",
        "src/api/v1/handlers/event_subscriptions.rs",
        "src/api/v1/handlers/event_deliveries.rs",
        "tests/api_platform_suite/event_subscriptions.rs",
    ] {
        let path = root.join(file);
        let source = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("could not read {}: {error}", path.display()));
        for forbidden in [
            "storage::capabilities::events",
            "storage::capabilities::event_subscription",
            "storage::capabilities::event_delivery",
            "list_events_with_total_count",
            "save_event_sink_record",
            "update_event_sink_record",
            "delete_event_sink_record",
            "save_event_subscription_record",
            "update_event_subscription_record",
            "delete_event_subscription_record",
            "EventSink::list_with_total_count",
            "EventSubscription::list_with_total_count",
        ] {
            assert!(
                !source.contains(forbidden),
                "{} still uses event adapter detail {forbidden}",
                path.display()
            );
        }
    }

    for file in [
        "src/events/model.rs",
        "src/models/event_delivery.rs",
        "src/models/event_subscription.rs",
        "tests/api_platform_suite/events.rs",
        "tests/api_platform_suite/event_deliveries.rs",
        "tests/api_platform_suite/event_subscriptions.rs",
    ] {
        let path = root.join(file);
        let source = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("could not read {}: {error}", path.display()));
        for forbidden in [
            "storage::postgres",
            "diesel::",
            "diesel_async",
            "crate::schema",
            "CursorSql",
            "EventSinkRow",
            "NewEventSinkRow",
            "UpdateEventSinkRow",
            "EventSubscriptionRow",
            "NewEventSubscriptionRow",
            "UpdateEventSubscriptionRow",
            "EventDeliveryRow",
            "EventRow",
            "NewEventRow",
        ] {
            assert!(
                !source.contains(forbidden),
                "{} still uses event persistence detail {forbidden}",
                path.display()
            );
        }
    }

    for file in [
        "tests/api_core_data_suite/object_data_patch/mod.rs",
        "tests/api_core_data_suite/object_data_patch/atomicity.rs",
        "tests/api_identity_suite/principal_settings.rs",
        "tests/api_identity_suite/principal_settings_json_patch.rs",
        "tests/api_identity_suite/service_accounts.rs",
        "tests/api_platform_suite/events.rs",
    ] {
        let path = root.join(file);
        let source = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("could not read {}: {error}", path.display()));
        for forbidden in [
            "schema::events",
            "events::table",
            "events::dsl",
            "emit_event(",
            "operations::event_record",
            "Event::as_select",
            "EventRow",
            "NewEventRow",
        ] {
            assert!(
                !source.contains(forbidden),
                "{} still uses event persistence detail {forbidden}",
                path.display()
            );
        }
    }
}

#[test]
fn remaining_administration_consumers_do_not_use_postgres_facades() {
    let root = repository_root();
    for file in [
        "src/api/handlers/meta.rs",
        "src/api/v1/handlers/collections.rs",
        "src/traits/permissions.rs",
    ] {
        let path = root.join(file);
        let source = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("could not read {}: {error}", path.display()));
        for forbidden in [
            "storage::capabilities",
            "PermissionControllerBackend",
            "collection_permission_set_from_backend",
            "load_database_state",
            "load_task_queue_state",
            "apply_permissions_from_backend",
            "revoke_permissions_from_backend",
            "revoke_all_from_backend",
        ] {
            assert!(
                !source.contains(forbidden),
                "{} still uses backend facade detail {forbidden}",
                path.display()
            );
        }
    }
}

#[test]
fn task_execution_consumers_do_not_import_postgres_task_state_helpers() {
    let root = repository_root();
    for file in [
        "src/backups/mod.rs",
        "src/exports/mod.rs",
        "src/tasks/execution.rs",
        "src/tasks/remote_call.rs",
        "src/tasks/worker.rs",
    ] {
        let path = root.join(file);
        let source = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("could not read {}: {error}", path.display()));
        for forbidden in [
            "TaskBackend",
            "TaskStateUpdate",
            "claim_next_queued_task",
            "renew_task_lease(&pool",
            "storage::capabilities::task",
            "storage::postgres::operations::task::{",
            "storage::postgres::operations::task::purge_expired_",
            "snapshot_backup_db",
            "insert_remote_call_result",
            "execute_computed_reindex_task",
        ] {
            assert!(
                !source.contains(forbidden),
                "{} still imports PostgreSQL task state helper {forbidden}",
                path.display()
            );
        }
    }
}

#[test]
fn export_consumers_use_only_backend_neutral_query_and_authorization_contracts() {
    let root = repository_root();
    for file in [
        "src/exports/mod.rs",
        "src/services/authorization_resources.rs",
    ] {
        let path = root.join(file);
        let source = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("could not read {}: {error}", path.display()));
        let production = source.split("#[cfg(test)]").next().unwrap_or(&source);
        for forbidden in [
            "storage::capabilities",
            "storage::postgres",
            "diesel::",
            "diesel_async",
            "statement_timeout",
        ] {
            assert!(
                !production.contains(forbidden),
                "{} still selects backend implementation detail {forbidden}",
                path.display()
            );
        }
    }
}

#[test]
fn export_template_consumers_use_only_the_backend_neutral_lifecycle_contract() {
    let root = repository_root();
    for file in [
        "src/models/export_template.rs",
        "src/api/v1/handlers/export_templates.rs",
        "src/exports/mod.rs",
    ] {
        let path = root.join(file);
        let source = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("could not read {}: {error}", path.display()));
        let production = source.split("#[cfg(test)]").next().unwrap_or(&source);
        for forbidden in [
            "storage::postgres",
            "diesel::",
            "diesel_async",
            "crate::schema",
            "CursorSql",
            "impl_history_pagination!",
            "ExportTemplateRow",
            "NewExportTemplateRow",
            "UpdateExportTemplateRow",
            "load_export_template_record",
            "save_export_template_record",
            "update_export_template_record",
            "delete_export_template_record",
        ] {
            assert!(
                !production.contains(forbidden),
                "{} still selects export-template adapter detail {forbidden}",
                path.display()
            );
        }
    }
}

#[test]
fn remote_target_consumers_use_the_backend_neutral_application_service() {
    let root = repository_root();
    for file in [
        "src/models/remote_target.rs",
        "src/api/v1/handlers/remote_targets.rs",
        "src/tasks/remote_call.rs",
        "tests/api_jobs_suite/remote_targets.rs",
    ] {
        let path = root.join(file);
        let source = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("could not read {}: {error}", path.display()));
        for forbidden in [
            "storage::capabilities::remote_target",
            "save_remote_target_record",
            "update_remote_target_record",
            "delete_remote_target_record",
            "emit_remote_target_invoked_event",
            ".instance(&context)",
            ".instance(backend)",
            "RemoteTarget::list_with_total_count",
            "storage::postgres",
            "diesel::",
            "diesel_async",
            "crate::schema",
            "CursorSql",
            "impl_history_pagination!",
            "RemoteTargetRow",
            "NewRemoteTargetRow",
            "UpdateRemoteTargetRow",
            "NewRemoteCallResult",
        ] {
            assert!(
                !source.contains(forbidden),
                "{} still uses PostgreSQL remote-target helper {forbidden}",
                path.display()
            );
        }
    }
}

#[test]
fn restore_consumers_use_only_the_mandatory_storage_contract() {
    let root = repository_root();
    let path = root.join("src/restores/mod.rs");
    let source = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", path.display()));

    for forbidden in [
        "storage::capabilities::restore",
        "apply_restore_db",
        "delete_server_instance_db",
        "expire_restore_stage_db",
        "fail_restore_and_resume_db",
        "insert_restore_job_db",
        "load_restore_coordinator_snapshot_db",
        "load_restore_job_db",
        "load_restore_status_job_db",
        "maintenance_generation_and_instances_db",
        "restore_coordinator_tick_db",
        "resume_maintenance_without_job_db",
        "resume_terminal_restore_db",
        "start_restore_draining_db",
        "RestoreJobRow",
        "RestoreJobStatusRecord",
    ] {
        assert!(
            !source.contains(forbidden),
            "restore application code still uses backend detail {forbidden}"
        );
    }
    assert!(
        source.contains("RestoreStorage"),
        "restore application code must depend on the mandatory storage contract"
    );
}

#[test]
fn storage_error_translation_has_one_way_dependency_direction() {
    let root = repository_root();
    let errors_path = root.join("src/errors.rs");
    let errors_source = fs::read_to_string(&errors_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", errors_path.display()));
    let postgres_error_path = root.join("src/storage/postgres/error.rs");
    let postgres_error_source = fs::read_to_string(&postgres_error_path).unwrap_or_else(|error| {
        panic!("could not read {}: {error}", postgres_error_path.display())
    });

    assert!(
        !errors_source.contains("impl From<ApiError> for StorageError"),
        "application errors must not provide a reverse conversion into storage"
    );
    assert!(
        errors_source.contains("impl From<StorageError> for ApiError"),
        "the application layer must own storage-to-API error translation"
    );
    assert!(
        postgres_error_source.contains("struct PostgresStorageError"),
        "the PostgreSQL adapter must own its backend-specific error"
    );
    assert!(
        postgres_error_source.contains("impl From<PostgresStorageError> for StorageError"),
        "the PostgreSQL adapter error must translate at the storage boundary"
    );
}

#[test]
fn persistence_facades_do_not_reexport_internal_layers_wholesale() {
    let root = repository_root();
    let backend_path = root.join("src/storage/capabilities.rs");
    let backend_source = fs::read_to_string(&backend_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", backend_path.display()));
    let library_path = root.join("src/lib.rs");
    let library_source = fs::read_to_string(&library_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", library_path.display()));

    assert!(
        !backend_source
            .contains("pub(crate) use crate::storage::postgres::operations as capabilities"),
        "the application capability facade must explicitly whitelist operations"
    );
    for forbidden in [
        "operations::Status",
        "mod active_tokens",
        "mod external_identity",
        "mod identity",
        "mod service_account",
        "with_storage_call_site",
        "with_mutation_provenance_scope",
        "with_revision_precondition_scope",
    ] {
        assert!(
            !backend_source.contains(forbidden),
            "authentication and execution context must not cross the PostgreSQL capability facade: {forbidden}"
        );
    }
    assert!(
        library_source.contains("#[doc(hidden)]\npub mod storage;"),
        "the internal root storage module must remain hidden from generated API documentation"
    );
}
