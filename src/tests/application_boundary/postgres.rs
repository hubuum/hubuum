use super::*;

#[test]
fn import_execution_is_owned_by_the_postgres_adapter() {
    let root = repository_root();
    let adapter_path =
        root.join("crates/hubuum-storage-postgres/src/operations/import_execution.rs");
    let adapter = read_source(&adapter_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", adapter_path.display()));
    for forbidden in [
        "crate::errors",
        "crate::models",
        "hubuum_storage_postgres",
        "ApiError",
        "get_config",
    ] {
        assert!(
            !adapter.contains(forbidden),
            "the PostgreSQL import executor depends on application detail {forbidden}"
        );
    }
    for required in [
        "pub async fn preflight_import",
        "pub async fn apply_import_strict",
        "pub async fn apply_import_best_effort",
        "execute_operation",
    ] {
        assert!(
            adapter.contains(required),
            "the PostgreSQL import executor is missing {required}"
        );
    }

    let facade_path = root.join("crates/hubuum-storage-postgres/src/backend/imports.rs");
    let facade = read_source(&facade_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", facade_path.display()));
    let implementation = item_body(&facade, "impl", "ImportStorage for PostgresStorage");
    for method in [
        "preflight_import",
        "apply_import_strict",
        "apply_import_best_effort",
    ] {
        let method_body = item_body(implementation, "fn", method);
        assert!(
            method_body.contains(&format!("import_workflow::{method}")),
            "the {method} import implementation must delegate into the adapter crate"
        );
        assert!(
            !method_body.contains("self.pool"),
            "the {method} import implementation leaks the PostgreSQL pool"
        );
    }

    let legacy_path = root.join("src/storage/postgres/operations/task_import.rs");
    assert!(
        !legacy_path.exists(),
        "the application-owned PostgreSQL import executor still exists"
    );
}

#[test]
fn collection_lifecycle_is_owned_by_the_postgres_adapter() {
    let root = repository_root();
    let adapter_path = root.join("crates/hubuum-storage-postgres/src/operations/collection.rs");
    let adapter = read_source(&adapter_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", adapter_path.display()));
    for forbidden in [
        "crate::errors",
        "crate::models",
        "hubuum_storage_postgres",
        "ApiError",
    ] {
        assert!(
            !adapter.contains(forbidden),
            "{} depends on application path {forbidden}",
            adapter_path.display()
        );
    }

    let capability_path =
        root.join("crates/hubuum-storage-postgres/src/backend/capabilities/resources.rs");
    let capability = read_source(&capability_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", capability_path.display()));
    let implementation = item_body(&capability, "impl", "CollectionStorage for PostgresStorage");
    assert!(
        implementation.contains("crate::operations::collection"),
        "the collection trait implementation must delegate into the adapter crate"
    );
    for forbidden in [
        "CollectionID::new",
        "NewCollection",
        "save_collection_record",
        "update_collection_record",
        "delete_collection_record",
        "move_collection_record",
    ] {
        assert!(
            !implementation.contains(forbidden),
            "the collection trait implementation retains application detail {forbidden}"
        );
    }
}

#[test]
fn catalog_queries_are_owned_by_the_postgres_adapter() {
    let root = repository_root();
    let adapter_path = root.join("crates/hubuum-storage-postgres/src/operations/catalog.rs");
    let adapter = read_source(&adapter_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", adapter_path.display()));
    for forbidden in [
        "crate::errors",
        "crate::models",
        "hubuum_storage_postgres",
        "ApiError",
    ] {
        assert!(
            !adapter.contains(forbidden),
            "{} depends on application path {forbidden}",
            adapter_path.display()
        );
    }

    let capability_path =
        root.join("crates/hubuum-storage-postgres/src/backend/capabilities/queries.rs");
    let capability = read_source(&capability_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", capability_path.display()));
    let implementation = item_body(&capability, "impl", "CatalogStorage for PostgresStorage");
    for method in ["list_collections", "list_classes", "list_objects"] {
        let method_body = item_body(implementation, "fn", method);
        assert!(
            method_body.contains("crate::operations::catalog"),
            "the {method} catalog implementation must delegate into the adapter crate"
        );
        for forbidden in ["&self.pool", "UserSearchBackend", "_to_storage"] {
            assert!(
                !method_body.contains(forbidden),
                "the {method} catalog implementation retains application detail {forbidden}"
            );
        }
    }

    let legacy_path = root.join("src/storage/postgres/operations/catalog.rs");
    assert!(
        !legacy_path.exists(),
        "the application-owned PostgreSQL catalog facade still exists"
    );
}

#[test]
fn computed_object_queries_are_owned_by_the_postgres_adapter() {
    let root = repository_root();
    let adapter_path =
        root.join("crates/hubuum-storage-postgres/src/operations/computed_objects.rs");
    let adapter = read_source(&adapter_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", adapter_path.display()));
    for forbidden in [
        "crate::errors",
        "crate::models",
        "hubuum_storage_postgres",
        "ApiError",
    ] {
        assert!(
            !adapter.contains(forbidden),
            "{} depends on application path {forbidden}",
            adapter_path.display()
        );
    }
    let snapshot_start = adapter
        .find("with_read_only_snapshot")
        .expect("computed-object queries must run in a read-only snapshot");
    let resolution = adapter
        .find("resolve_computed_query_fields")
        .expect("computed-object queries must resolve fields in the adapter");
    let enrichment = adapter
        .find("enrich_with_query_snapshot")
        .expect("computed-object queries must enrich from the resolved snapshot");
    assert!(
        snapshot_start < resolution && resolution < enrichment,
        "computed resolution, selection, and enrichment must share the adapter snapshot"
    );

    let capability_path =
        root.join("crates/hubuum-storage-postgres/src/backend/capabilities/queries.rs");
    let capability = read_source(&capability_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", capability_path.display()));
    let implementation = item_body(
        &capability,
        "impl",
        "ComputedObjectStorage for PostgresStorage",
    );
    for method in ["list_computed_objects", "enrich_objects_with_computed"] {
        let method_body = item_body(implementation, "fn", method);
        assert!(
            method_body.contains("crate::operations::computed_objects"),
            "the {method} computed-object implementation must delegate into the adapter crate"
        );
        assert!(
            !method_body.contains("&self.pool"),
            "the {method} computed-object implementation leaks the PostgreSQL pool"
        );
    }

    let legacy_path = root.join("src/storage/postgres/operations/computed_objects.rs");
    assert!(
        !legacy_path.exists(),
        "the application-owned PostgreSQL computed-object facade still exists"
    );
}

#[test]
fn computed_fields_are_owned_by_the_postgres_adapter() {
    let root = repository_root();
    let adapter_path =
        root.join("crates/hubuum-storage-postgres/src/operations/computed_fields.rs");
    let adapter = read_source(&adapter_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", adapter_path.display()));
    for forbidden in [
        "crate::config",
        "crate::errors",
        "crate::models",
        "hubuum_storage_postgres",
        "ApiError",
    ] {
        assert!(
            !adapter.contains(forbidden),
            "{} depends on application path {forbidden}",
            adapter_path.display()
        );
    }
    for required in [
        "pub async fn get_computed_field_state",
        "pub async fn list_shared_computed_fields",
        "pub async fn list_personal_computed_fields",
        "pub async fn create_shared_computed_field",
        "pub async fn update_shared_computed_field",
        "pub async fn delete_shared_computed_field",
        "pub async fn create_personal_computed_field",
        "pub async fn update_personal_computed_field",
        "pub async fn delete_personal_computed_field",
        "pub async fn request_computed_field_rebuild",
        "pub async fn execute_computed_field_rebuild",
    ] {
        assert!(
            adapter.contains(required),
            "PostgreSQL computed-field lifecycle is missing {required}"
        );
    }

    let facade_path = root.join("crates/hubuum-storage-postgres/src/backend/computed_fields.rs");
    let facade = read_source(&facade_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", facade_path.display()));
    let implementation = item_body(&facade, "impl", "ComputedFieldStorage for PostgresStorage");
    for method in [
        "get_computed_field_state",
        "list_shared_computed_fields",
        "list_personal_computed_fields",
        "get_computed_field",
        "create_shared_computed_field",
        "update_shared_computed_field",
        "delete_shared_computed_field",
        "create_personal_computed_field",
        "update_personal_computed_field",
        "delete_personal_computed_field",
        "request_computed_field_rebuild",
        "execute_computed_field_rebuild",
    ] {
        let method_body = item_body(implementation, "fn", method);
        assert!(
            method_body.contains("postgres_computed_fields::"),
            "the {method} computed-field implementation must delegate into the adapter crate"
        );
        for forbidden in ["self.pool", "operations::computed_field", "_to_storage"] {
            assert!(
                !method_body.contains(forbidden),
                "the {method} computed-field implementation retains application detail {forbidden}"
            );
        }
    }
}

#[test]
fn object_aggregate_queries_are_owned_by_the_postgres_adapter() {
    let root = repository_root();
    let adapter_path = root.join("crates/hubuum-storage-postgres/src/operations/object_aggregate");
    let adapter = read_rust_module_tree(&adapter_path);
    let entrypoint_path =
        root.join("crates/hubuum-storage-postgres/src/operations/object_aggregate.rs");
    let entrypoint = read_source(&entrypoint_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", entrypoint_path.display()));
    for forbidden in [
        "crate::errors",
        "crate::models",
        "hubuum_storage_postgres",
        "ApiError",
        "get_config",
    ] {
        assert!(
            !adapter.contains(forbidden) && !entrypoint.contains(forbidden),
            "the PostgreSQL aggregate adapter depends on application detail {forbidden}"
        );
    }

    let capability_path =
        root.join("crates/hubuum-storage-postgres/src/backend/capabilities/queries.rs");
    let capability = read_source(&capability_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", capability_path.display()));
    let implementation = item_body(
        &capability,
        "impl",
        "ObjectAggregateStorage for PostgresStorage",
    );
    let method = item_body(implementation, "fn", "aggregate_objects");
    assert!(
        method.contains("crate::operations::object_aggregate"),
        "object aggregation must delegate into the PostgreSQL adapter crate"
    );
    assert!(
        !method.contains("&self.pool"),
        "object aggregation must not expose the PostgreSQL pool"
    );

    let legacy_path = root.join("src/storage/postgres/operations/user/object_aggregate");
    assert!(
        !legacy_path.exists(),
        "the application-owned PostgreSQL aggregate implementation still exists"
    );
}

#[test]
fn worker_notification_io_is_owned_by_the_postgres_adapter() {
    let root = repository_root();
    let adapter_path = root.join("crates/hubuum-storage-postgres/src/worker_notifications.rs");
    let adapter = read_source(&adapter_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", adapter_path.display()));
    for required in ["LISTEN", "UNLISTEN", "pg_notify", "notifications_stream"] {
        assert!(
            adapter.contains(required),
            "the PostgreSQL notification adapter is missing native behavior {required}"
        );
    }
    for forbidden in [
        "crate::errors",
        "crate::lifecycle",
        "hubuum_storage_postgres",
        "ApiError",
        "actix_rt",
        "get_config",
    ] {
        assert!(
            !adapter.contains(forbidden),
            "the PostgreSQL notification adapter depends on application detail {forbidden}"
        );
    }

    let backend_path = root.join("crates/hubuum-storage-postgres/src/backend/notifications.rs");
    let backend = read_source(&backend_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", backend_path.display()));
    assert!(
        backend.contains("crate::worker_notifications::listen"),
        "the PostgreSQL backend contract must delegate native listening within the adapter crate"
    );
    for forbidden in ["LISTEN", "UNLISTEN", "pg_notify", "notifications_stream"] {
        assert!(
            !backend.contains(forbidden),
            "the backend contract implementation owns native notification detail {forbidden}"
        );
    }

    let composition_path = root.join("src/storage/notifications.rs");
    let composition = read_source(&composition_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", composition_path.display()));
    for forbidden in ["hubuum_storage_postgres", "PostgresStorage", "PostgresPool"] {
        assert!(
            !composition.contains(forbidden),
            "application notification lifecycle depends on PostgreSQL detail {forbidden}"
        );
    }
}

#[test]
fn collection_authorization_queries_are_owned_by_the_postgres_adapter() {
    let root = repository_root();
    let adapter_path =
        root.join("crates/hubuum-storage-postgres/src/operations/authorization/queries.rs");
    let adapter = read_source(&adapter_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", adapter_path.display()));
    for forbidden in [
        "crate::errors",
        "crate::models",
        "hubuum_storage_postgres",
        "ApiError",
        "get_config",
    ] {
        assert!(
            !adapter.contains(forbidden),
            "{} depends on application detail {forbidden}",
            adapter_path.display()
        );
    }

    let capability_path =
        root.join("crates/hubuum-storage-postgres/src/backend/capabilities/identity_queries.rs");
    let capability = read_source(&capability_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", capability_path.display()));
    let implementation = item_body(
        &capability,
        "impl",
        "CollectionAuthorizationQueryStorage for PostgresStorage",
    );
    for method in [
        "load_principal_collection_permissions",
        "list_all_principal_collection_permissions",
        "list_principal_collection_permissions",
        "list_effective_principal_collection_permissions",
        "list_visible_collections",
        "has_group_collection_permission",
        "list_effective_group_collection_permissions",
        "load_groups_with_collection_permission",
        "list_groups_with_collection_permission",
    ] {
        let method_body = item_body(implementation, "fn", method);
        assert!(
            method_body.contains("crate::operations::authorization"),
            "the {method} implementation must delegate into the adapter crate"
        );
        assert!(
            !method_body.contains("&self.pool"),
            "the {method} implementation must not expose the PostgreSQL pool"
        );
    }
}

#[test]
fn class_lifecycle_is_owned_by_the_postgres_adapter() {
    let root = repository_root();
    let adapter_path = root.join("crates/hubuum-storage-postgres/src/operations/class.rs");
    let adapter = read_source(&adapter_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", adapter_path.display()));
    for forbidden in [
        "crate::errors",
        "crate::models",
        "hubuum_storage_postgres",
        "ApiError",
    ] {
        assert!(
            !adapter.contains(forbidden),
            "{} depends on application path {forbidden}",
            adapter_path.display()
        );
    }

    let capability_path =
        root.join("crates/hubuum-storage-postgres/src/backend/capabilities/resources.rs");
    let capability = read_source(&capability_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", capability_path.display()));
    let implementation = item_body(&capability, "impl", "ClassStorage for PostgresStorage");
    assert!(
        implementation.contains("crate::operations::class"),
        "the class trait implementation must delegate into the adapter crate"
    );
    for forbidden in [
        "ClassSelector::",
        "class_selector_from_storage",
        "NewHubuumClass",
        "UpdateHubuumClass",
        "create_class_record",
        "update_class_record",
        "delete_class_record",
        "load_class_names",
    ] {
        assert!(
            !implementation.contains(forbidden),
            "the class trait implementation retains application detail {forbidden}"
        );
    }
}

#[test]
fn object_lifecycle_is_owned_by_the_postgres_adapter() {
    let root = repository_root();
    let adapter_path = root.join("crates/hubuum-storage-postgres/src/operations/object.rs");
    let adapter = read_source(&adapter_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", adapter_path.display()));
    for forbidden in [
        "crate::errors",
        "crate::models",
        "hubuum_storage_postgres",
        "ApiError",
    ] {
        assert!(
            !adapter.contains(forbidden),
            "{} depends on application path {forbidden}",
            adapter_path.display()
        );
    }

    let capability_path =
        root.join("crates/hubuum-storage-postgres/src/backend/capabilities/resources.rs");
    let capability = read_source(&capability_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", capability_path.display()));
    let implementation = item_body(&capability, "impl", "ObjectStorage for PostgresStorage");
    assert!(
        implementation.contains("crate::operations::object"),
        "the object trait implementation must delegate into the adapter crate"
    );
    for forbidden in [
        "ObjectSelector::",
        "object_selector_from_storage",
        "NewHubuumObject",
        "UpdateHubuumObject",
        "ObjectDataPatchDocument",
        "save_object_record",
        "update_object_record",
        "delete_object_record",
    ] {
        assert!(
            !implementation.contains(forbidden),
            "the object trait implementation retains application detail {forbidden}"
        );
    }

    assert!(!root.join("src/storage/postgres").exists());
}

#[test]
fn relation_lifecycles_are_owned_by_the_postgres_adapter() {
    let root = repository_root();
    let adapter_path = root.join("crates/hubuum-storage-postgres/src/operations/relation.rs");
    let adapter = read_source(&adapter_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", adapter_path.display()));
    for forbidden in [
        "crate::errors",
        "crate::models",
        "hubuum_storage_postgres",
        "ApiError",
    ] {
        assert!(
            !adapter.contains(forbidden),
            "{} depends on application path {forbidden}",
            adapter_path.display()
        );
    }

    let capability_path =
        root.join("crates/hubuum-storage-postgres/src/backend/capabilities/resources.rs");
    let capability = read_source(&capability_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", capability_path.display()));
    for contract in [
        "ClassRelationStorage for PostgresStorage",
        "ObjectRelationStorage for PostgresStorage",
    ] {
        let implementation = item_body(&capability, "impl", contract);
        assert!(
            implementation.contains("crate::operations::relation"),
            "the {contract} implementation must delegate into the adapter crate"
        );
        for forbidden in [
            "HubuumClassRelationID::new",
            "HubuumObjectRelationID::new",
            "prepare_class_relation_record",
            "prepare_object_relation_record",
            "save_class_relation_record",
            "save_object_relation_record",
        ] {
            assert!(
                !implementation.contains(forbidden),
                "the {contract} implementation retains application detail {forbidden}"
            );
        }
    }

    assert!(!root.join("src/storage/postgres").exists());
}

#[test]
fn relation_queries_are_owned_by_the_postgres_adapter() {
    let root = repository_root();
    let adapter_path = root.join("crates/hubuum-storage-postgres/src/operations/relation_query.rs");
    let adapter = read_source(&adapter_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", adapter_path.display()));
    for forbidden in [
        "crate::errors",
        "crate::models",
        "hubuum_storage_postgres",
        "ApiError",
    ] {
        assert!(
            !adapter.contains(forbidden),
            "{} depends on application path {forbidden}",
            adapter_path.display()
        );
    }

    let capability_path =
        root.join("crates/hubuum-storage-postgres/src/backend/capabilities/queries.rs");
    let capability = read_source(&capability_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", capability_path.display()));
    let implementation = item_body(
        &capability,
        "impl",
        "RelationQueryStorage for PostgresStorage",
    );
    for method in [
        "list_class_relations",
        "list_object_relations",
        "list_class_relations_touching",
        "list_object_relations_touching",
        "list_class_relations_touching_ids",
        "list_class_relations_between_ids",
        "list_object_relations_touching_ids",
        "list_object_relations_between_ids",
        "list_related_classes",
        "list_related_objects",
        "list_related_objects_for_roots",
        "list_bidirectionally_related_objects_for_roots",
    ] {
        let method_body = item_body(implementation, "fn", method);
        assert!(
            method_body.contains("crate::operations::relation_query"),
            "the {method} implementation must delegate into the adapter crate"
        );
        assert!(
            !method_body.contains("&self.pool"),
            "the {method} implementation must not expose the PostgreSQL pool"
        );
    }

    let legacy_facade = root.join("src/storage/postgres/operations/relation_query.rs");
    assert!(
        !legacy_facade.exists(),
        "relation query SQL must not regain an application-owned facade"
    );
}

#[test]
fn principal_state_queries_are_owned_by_the_postgres_adapter() {
    let root = repository_root();
    let adapter_path =
        root.join("crates/hubuum-storage-postgres/src/operations/identity_principals.rs");
    let adapter = read_source(&adapter_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", adapter_path.display()));
    for forbidden in [
        "crate::errors",
        "crate::models",
        "hubuum_storage_postgres",
        "ApiError",
    ] {
        assert!(
            !adapter.contains(forbidden),
            "{} depends on application path {forbidden}",
            adapter_path.display()
        );
    }

    let capability_path =
        root.join("crates/hubuum-storage-postgres/src/backend/capabilities/identity.rs");
    let capability = read_source(&capability_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", capability_path.display()));
    let membership = item_body(
        &capability,
        "impl",
        "GroupMembershipStorage for PostgresStorage",
    );
    for method in ["get_principal_group", "is_human_owner_group_member"] {
        let method_body = item_body(membership, "fn", method);
        assert!(
            method_body.contains("crate::operations::identity_principals"),
            "the {method} implementation must delegate into the adapter crate"
        );
        assert!(!method_body.contains("&self.pool"));
    }
    let service_accounts = item_body(
        &capability,
        "impl",
        "ServiceAccountStorage for PostgresStorage",
    );
    let method = "is_service_account_disabled";
    let method_body = item_body(service_accounts, "fn", method);
    assert!(
        method_body.contains("crate::operations::identity_principals"),
        "the {method} implementation must delegate into the adapter crate"
    );
    assert!(
        !method_body.contains("&self.pool"),
        "the {method} implementation must not expose the PostgreSQL pool"
    );
}

#[test]
fn service_account_resources_are_owned_by_the_postgres_adapter() {
    let root = repository_root();
    let adapter_path =
        root.join("crates/hubuum-storage-postgres/src/operations/service_account.rs");
    let adapter = read_source(&adapter_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", adapter_path.display()));
    for forbidden in [
        "crate::errors",
        "crate::models",
        "hubuum_storage_postgres",
        "ApiError",
    ] {
        assert!(
            !adapter.contains(forbidden),
            "{} depends on application path {forbidden}",
            adapter_path.display()
        );
    }

    let capability_path =
        root.join("crates/hubuum-storage-postgres/src/backend/capabilities/identity.rs");
    let capability = read_source(&capability_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", capability_path.display()));
    let implementation = item_body(
        &capability,
        "impl",
        "ServiceAccountStorage for PostgresStorage",
    );
    for method in [
        "get_service_account",
        "get_service_account_details",
        "list_manageable_service_accounts",
        "create_service_account",
        "update_service_account",
        "disable_service_account",
        "delete_service_account",
    ] {
        let method_body = item_body(implementation, "fn", method);
        assert!(
            method_body.contains("crate::operations::service_account"),
            "the {method} implementation must delegate into the adapter crate"
        );
        assert!(
            !method_body.contains("&self.pool"),
            "the {method} implementation must not expose the PostgreSQL pool"
        );
    }
}

#[test]
fn external_identity_sync_is_owned_by_the_postgres_adapter() {
    let root = repository_root();
    let adapter_path =
        root.join("crates/hubuum-storage-postgres/src/operations/external_identity.rs");
    let adapter = read_source(&adapter_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", adapter_path.display()));
    for forbidden in [
        "crate::errors",
        "crate::models",
        "hubuum_storage_postgres",
        "ApiError",
        "hubuum_auth_core",
    ] {
        assert!(
            !adapter.contains(forbidden),
            "{} depends on application path {forbidden}",
            adapter_path.display()
        );
    }

    let capability_path =
        root.join("crates/hubuum-storage-postgres/src/backend/capabilities/identity.rs");
    let capability = read_source(&capability_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", capability_path.display()));
    let implementation = item_body(
        &capability,
        "impl",
        "ExternalIdentityStorage for PostgresStorage",
    );
    for method in [
        "get_external_principal_state",
        "record_external_sync_failure",
        "sync_external_user",
    ] {
        let method_body = item_body(implementation, "fn", method);
        assert!(
            method_body.contains("crate::operations::external_identity"),
            "the {method} implementation must delegate into the adapter crate"
        );
        assert!(
            !method_body.contains("&self.pool"),
            "the {method} implementation must not expose the PostgreSQL pool"
        );
    }

    assert!(
        !root
            .join("src/storage/postgres/operations/external_identity.rs")
            .exists(),
        "application composition must not retain a duplicate external identity SQL module"
    );
}

#[test]
fn principal_resources_are_owned_by_the_postgres_adapter() {
    let root = repository_root();
    let adapter_path = root.join("crates/hubuum-storage-postgres/src/operations/principal.rs");
    let adapter = read_source(&adapter_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", adapter_path.display()));
    for forbidden in [
        "crate::errors",
        "crate::models",
        "hubuum_storage_postgres",
        "ApiError",
    ] {
        assert!(
            !adapter.contains(forbidden),
            "{} depends on application path {forbidden}",
            adapter_path.display()
        );
    }

    let capability_path =
        root.join("crates/hubuum-storage-postgres/src/backend/capabilities/identity_queries.rs");
    let capability = read_source(&capability_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", capability_path.display()));
    let implementation = item_body(&capability, "impl", "PrincipalStorage for PostgresStorage");
    for method in [
        "get_principal",
        "get_principal_settings",
        "update_principal_settings",
    ] {
        let method_body = item_body(implementation, "fn", method);
        assert!(
            method_body.contains("crate::operations::principal"),
            "the {method} implementation must delegate into the adapter crate"
        );
        assert!(
            !method_body.contains("&self.pool"),
            "the {method} implementation must not expose the PostgreSQL pool"
        );
    }

    assert!(!root.join("src/storage/postgres").exists());
}

#[test]
fn group_resources_are_owned_by_the_postgres_adapter() {
    let root = repository_root();
    let adapter_path = root.join("crates/hubuum-storage-postgres/src/operations/group.rs");
    let adapter = read_source(&adapter_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", adapter_path.display()));
    for forbidden in [
        "crate::errors",
        "crate::models",
        "hubuum_storage_postgres",
        "ApiError",
    ] {
        assert!(
            !adapter.contains(forbidden),
            "{} depends on application path {forbidden}",
            adapter_path.display()
        );
    }

    let capability_path =
        root.join("crates/hubuum-storage-postgres/src/backend/capabilities/identity_queries.rs");
    let capability = read_source(&capability_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", capability_path.display()));
    let implementation = item_body(&capability, "impl", "GroupStorage for PostgresStorage");
    for method in [
        "get_group",
        "list_groups",
        "resolve_group_identity_scope_name",
        "create_group",
        "update_group",
        "delete_group",
    ] {
        let method_body = item_body(implementation, "fn", method);
        assert!(
            method_body.contains("crate::operations::group"),
            "the {method} implementation must delegate into the adapter crate"
        );
        assert!(
            !method_body.contains("&self.pool"),
            "the {method} implementation must not expose the PostgreSQL pool"
        );
    }

    let identity_capability_path =
        root.join("crates/hubuum-storage-postgres/src/backend/capabilities/identity.rs");
    let identity_capability = read_source(&identity_capability_path).unwrap_or_else(|error| {
        panic!(
            "could not read {}: {error}",
            identity_capability_path.display()
        )
    });
    let identity_implementation = item_body(
        &identity_capability,
        "impl",
        "GroupMembershipStorage for PostgresStorage",
    );
    for method in [
        "list_principal_groups",
        "load_group_member_principals",
        "list_group_members",
        "add_group_member",
        "remove_group_member",
    ] {
        let method_body = item_body(identity_implementation, "fn", method);
        assert!(
            method_body.contains("crate::operations::group"),
            "the {method} implementation must delegate into the adapter crate"
        );
        assert!(
            !method_body.contains("&self.pool"),
            "the {method} implementation must not expose the PostgreSQL pool"
        );
    }

    assert!(!root.join("src/storage/postgres").exists());
}

#[test]
fn user_resources_are_owned_by_the_postgres_adapter() {
    let root = repository_root();
    let adapter_path = root.join("crates/hubuum-storage-postgres/src/operations/user.rs");
    let adapter = read_source(&adapter_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", adapter_path.display()));
    for forbidden in [
        "crate::errors",
        "crate::models",
        "hubuum_storage_postgres",
        "ApiError",
    ] {
        assert!(
            !adapter.contains(forbidden),
            "{} depends on application path {forbidden}",
            adapter_path.display()
        );
    }

    let capability_path =
        root.join("crates/hubuum-storage-postgres/src/backend/capabilities/identity.rs");
    let capability = read_source(&capability_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", capability_path.display()));
    let implementation = item_body(&capability, "impl", "UserStorage for PostgresStorage");
    for method in [
        "get_user",
        "get_user_by_name",
        "get_user_details",
        "list_users",
        "create_user",
        "update_user",
        "set_user_password",
        "delete_user",
        "anonymize_user",
    ] {
        let method_body = item_body(implementation, "fn", method);
        assert!(
            method_body.contains("crate::operations::user"),
            "the {method} implementation must delegate into the adapter crate"
        );
        assert!(
            !method_body.contains("&self.pool"),
            "the {method} implementation must not expose the PostgreSQL pool"
        );
    }

    assert!(!root.join("src/storage/postgres").exists());
}

#[test]
fn token_resources_are_owned_by_the_postgres_adapter() {
    let root = repository_root();
    let adapter_path = root.join("crates/hubuum-storage-postgres/src/operations/token.rs");
    let adapter = read_source(&adapter_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", adapter_path.display()));
    for forbidden in [
        "crate::errors",
        "crate::models",
        "hubuum_storage_postgres",
        "ApiError",
    ] {
        assert!(
            !adapter.contains(forbidden),
            "{} depends on application path {forbidden}",
            adapter_path.display()
        );
    }

    let capability_path =
        root.join("crates/hubuum-storage-postgres/src/backend/capabilities/identity.rs");
    let capability = read_source(&capability_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", capability_path.display()));
    let token_implementation = item_body(&capability, "impl", "TokenStorage for PostgresStorage");
    for method in [
        "create_token",
        "renew_token",
        "get_token_metadata",
        "load_token_metadata_by_ids",
        "revoke_token",
        "revoke_token_by_hash",
        "revoke_all_principal_tokens",
        "list_retained_tokens",
        "token_key_usage",
    ] {
        let method_body = item_body(token_implementation, "fn", method);
        assert!(
            method_body.contains("crate::operations::token"),
            "the {method} implementation must delegate into the adapter crate"
        );
        assert!(
            !method_body.contains("&self.pool"),
            "the {method} implementation must not expose the PostgreSQL pool"
        );
    }

    assert!(!root.join("src/storage/postgres").exists());
}

#[test]
fn postgres_operational_queries_are_owned_by_the_adapter_crate() {
    let root = repository_root();
    for operation in [
        "authentication",
        "authorization",
        "backup",
        "bootstrap",
        "event_audit",
        "event_delivery",
        "event_fanout",
        "event_observability",
        "event_record",
        "event_retention",
        "event_subscription",
        "history",
        "identity_credentials",
        "identity_scope",
        "inventory",
        "maintenance",
        "meta",
        "metrics",
        "probe",
        "remote_target",
        "token_retention",
        "unified_search",
    ] {
        let adapter_file = root.join(format!(
            "crates/hubuum-storage-postgres/src/operations/{operation}.rs"
        ));
        let adapter_directory = root.join(format!(
            "crates/hubuum-storage-postgres/src/operations/{operation}"
        ));
        let (adapter_path, source) = if adapter_file.exists() {
            let source = read_source(&adapter_file).unwrap_or_else(|error| {
                panic!("could not read {}: {error}", adapter_file.display())
            });
            (adapter_file, source)
        } else {
            let source = read_rust_module_tree(&adapter_directory);
            (adapter_directory, source)
        };
        for forbidden in ["crate::errors", "crate::models", "hubuum_storage_postgres"] {
            assert!(
                !source.contains(forbidden),
                "{} depends on application path {forbidden}",
                adapter_path.display()
            );
        }

        let old_path = root.join(format!("src/storage/postgres/operations/{operation}.rs"));
        assert!(
            !old_path.exists(),
            "{} must not retain an application-owned implementation",
            old_path.display()
        );
    }

    let capability_path =
        root.join("crates/hubuum-storage-postgres/src/backend/capabilities/operational.rs");
    let capability = read_source(&capability_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", capability_path.display()));
    let implementation = item_body(
        &capability,
        "impl",
        "TokenRetentionStorage for PostgresStorage",
    );
    assert!(
        implementation.contains("crate::operations::token_retention"),
        "the token-retention trait implementation must delegate into the adapter crate"
    );
    assert!(
        !implementation.contains("&self.pool"),
        "the token-retention trait implementation must not expose the PostgreSQL pool"
    );

    assert!(
        !root.join("src/storage/postgres").exists(),
        "the application crate must not retain a PostgreSQL compatibility tree"
    );
}
