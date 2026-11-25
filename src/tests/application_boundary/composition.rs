use super::*;

#[test]
fn authorization_context_is_stronger_than_storage_context() {
    let root = repository_root();
    let path = root.join("src/permissions/context.rs");
    let source = read_source(&path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", path.display()));

    assert!(
        source.contains("pub trait AuthorizationContext: StorageContext"),
        "permission-aware workflows need an explicit capability above storage"
    );
    assert!(
        source.contains("impl AuthorizationContext for AppContext"),
        "AppContext must provide configured authorization selection"
    );
    assert!(
        source.contains("fn authorization_mode(&self) -> AuthorizationMode<'_>;"),
        "authorization selection must be explicit rather than defaulting to local policy"
    );
    assert!(
        source.contains("impl AuthorizationContext for StorageHandle {"),
        "a bare storage handle must not bypass production authorization selection"
    );
}

#[test]
fn opaque_storage_entrypoints_have_unique_bounded_observation_labels() {
    use std::collections::HashSet;

    let root = repository_root();
    let context_source = read_rust_module_tree(&root.join("src/storage/context"));
    let observed_source = read_source(&root.join("src/storage/observed.rs"))
        .expect("resource observer should be readable");
    let mut labels = HashSet::new();

    for (source, marker) in [
        (&context_source, "observe_storage_call("),
        (&context_source, "observe_infallible_storage_call("),
        (&observed_source, "self.call("),
    ] {
        for (offset, _) in source.match_indices(marker) {
            let call = &source[offset + marker.len()..];
            let capability_marker = "StorageCapability::";
            let capability_start = call.find(capability_marker).unwrap_or_else(|| {
                panic!("storage observer must use a typed StorageCapability label")
            }) + capability_marker.len();
            let capability = call[capability_start..]
                .chars()
                .take_while(|character| character.is_ascii_alphanumeric())
                .collect::<String>();
            assert!(
                !capability.is_empty(),
                "storage capability variant is missing"
            );
            let operation = call[capability_start + capability.len()..]
                .split('"')
                .nth(1)
                .expect("storage observer must use a static operation label")
                .to_string();
            assert!(
                !operation.is_empty()
                    && operation
                        .bytes()
                        .all(|byte| byte.is_ascii_lowercase() || byte == b'_'),
                "storage operation label must be bounded snake_case: {operation}"
            );
            let pair = (capability, operation);
            assert!(
                labels.insert(pair.clone()),
                "duplicate storage observation label pair: {}/{}",
                pair.0,
                pair.1
            );
        }
    }

    let manifest = storage_semantic_manifest(&root);
    let traits = manifest
        .get("traits")
        .and_then(toml::Value::as_table)
        .expect("semantic coverage inventory should have a traits table");
    let unobserved_traits = ["ExecutionStorage"];
    let mut expected_observations = 0;

    for (trait_name, value) in traits {
        if unobserved_traits.contains(&trait_name.as_str()) {
            continue;
        }
        let entry = value
            .as_table()
            .unwrap_or_else(|| panic!("trait inventory entry {trait_name} must be a table"));
        let methods = toml_string_set(entry, "methods");

        let (implementation, observer) = match trait_name.as_str() {
            "CollectionStorage"
            | "ClassStorage"
            | "ObjectStorage"
            | "ClassRelationStorage"
            | "ObjectRelationStorage" => (
                item_body(
                    &observed_source,
                    "impl<S>",
                    &format!("{trait_name} for ObservedStorage<S>"),
                ),
                "self.call(",
            ),
            _ => (
                item_body(
                    &context_source,
                    "impl",
                    &format!("{trait_name} for StorageHandle"),
                ),
                "observe_storage_call(",
            ),
        };

        for method in methods {
            let body = item_body(implementation, "fn", &method);
            let observer_count = body.matches(observer).count()
                + body.matches("observe_infallible_storage_call(").count();
            expected_observations += 1;
            assert_eq!(
                observer_count, 1,
                "{trait_name}::{method} must cross exactly one common storage observer"
            );
            let capability_marker = "StorageCapability::";
            let capability_start = body.find(capability_marker).unwrap_or_else(|| {
                panic!("{trait_name}::{method} must use a typed storage capability")
            }) + capability_marker.len();
            let capability_len = body[capability_start..]
                .chars()
                .take_while(|character| character.is_ascii_alphanumeric())
                .count();
            let operation = body[capability_start + capability_len..]
                .split('"')
                .nth(1)
                .unwrap_or_else(|| {
                    panic!("{trait_name}::{method} must use a static operation label")
                });
            // Preserve the published telemetry label when this capability is renamed.
            let expected_operation = match (trait_name.as_str(), method.as_str()) {
                ("ExternalIdentityStorage", "record_external_sync_failure") => {
                    "mark_external_sync_attempted"
                }
                _ => method.as_str(),
            };
            assert_eq!(
                operation, expected_operation,
                "{trait_name}::{method} must use its established operation label"
            );
        }
    }

    // Optional analysis is observed at the same opaque handle boundary without
    // becoming a requirement of every selectable StorageBackend.
    let analysis = item_body(&context_source, "fn", "analyze_query_usage");
    assert_eq!(analysis.matches("observe_storage_call(").count(), 1);
    assert!(labels.contains(&("QueryUsage".to_string(), "analyze_query_usage".to_string())));
    expected_observations += 1;

    assert_eq!(
        labels.len(),
        expected_observations,
        "every observed operation must have one unique bounded label pair"
    );
}

#[test]
fn all_domain_models_are_free_of_database_implementation_details() {
    let root = repository_root();
    let mut violations = Vec::new();

    for path in rust_files(&root.join("src/models")) {
        let source = read_source(&path)
            .unwrap_or_else(|error| panic!("could not read {}: {error}", path.display()));
        let production_source = source.split("#[cfg(test)]").next().unwrap_or(&source);
        for forbidden in [
            "hubuum_storage_postgres",
            "diesel::",
            "diesel_async",
            "crate::schema",
        ] {
            if production_source.contains(forbidden) {
                violations.push(format!("{} contains {forbidden}", path.display()));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "domain models crossed into database implementation details:\n{}",
        violations.join("\n")
    );
}

#[test]
fn postgres_adapter_helpers_accept_only_postgres_owned_context() {
    let root = repository_root();
    let mut violations = Vec::new();

    for path in rust_files(&root.join("crates/hubuum-storage-postgres/src")) {
        let source = read_source(&path)
            .unwrap_or_else(|error| panic!("could not read {}: {error}", path.display()));
        if source.contains("StorageContext") {
            violations.push(format!(
                "{} uses the application storage context instead of PostgresPool",
                path.display()
            ));
        }
    }

    assert!(
        violations.is_empty(),
        "PostgreSQL internals must not masquerade as backend-neutral operations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn postgres_benchmark_composes_before_calling_domain_operations() {
    let root = repository_root();
    let path = root.join("benches/storage_postgres_criterion.rs");
    let source = read_source(&path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", path.display()));

    for required in [
        "benchmark_support::storage_for_postgres(pool)",
        "benchmark_support::services_for_storage(&storage)",
        "storage: BenchmarkStorageContext",
    ] {
        assert!(
            source.contains(required),
            "PostgreSQL benchmark fixture must compose through {required}"
        );
    }
    for forbidden in [
        "pool: PostgresPool",
        "save_without_events(&pool)",
        "delete_without_events(&self.pool)",
        "services_for_postgres",
    ] {
        assert!(
            !source.contains(forbidden),
            "PostgreSQL benchmark bypasses the opaque boundary through {forbidden}"
        );
    }
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
        "src/errors.rs",
        "src/services/exports/mod.rs",
        "src/restores/mod.rs",
        "src/tasks/helpers.rs",
        "src/tasks/execution.rs",
        "src/tasks/planning.rs",
        "src/tasks/preload.rs",
        "src/tasks/resolution.rs",
        "src/tasks/worker.rs",
        "src/tasks/remote_call.rs",
        "src/token_retention.rs",
        "src/traits/mod.rs",
        "src/traits/authz.rs",
        "src/traits/permissions.rs",
    ] {
        paths.push(root.join(file));
    }

    let mut violations = Vec::new();
    for path in paths.into_iter().filter(|path| !is_test_module(path)) {
        let source = read_source(&path)
            .unwrap_or_else(|error| panic!("could not read {}: {error}", path.display()));
        let source = if path.starts_with(root.join("src/services"))
            || path == root.join("src/services/exports/mod.rs")
        {
            source.split("#[cfg(test)]").next().unwrap_or(&source)
        } else {
            &source
        };
        for forbidden in [
            "hubuum_storage_postgres",
            "PostgresPool",
            "postgres_pool",
            "spawn_postgres_notification_listener",
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
fn resource_services_cannot_request_unrecorded_mutations() {
    let root = repository_root();
    let files_and_mutations = [
        (
            "src/services/collections.rs",
            &[
                ".create_collection(",
                ".update_collection(",
                ".delete_collection(",
                ".move_collection(",
            ][..],
        ),
        (
            "src/services/classes.rs",
            &[".create_class(", ".update_class(", ".delete_class("][..],
        ),
        (
            "src/services/objects.rs",
            &[".create_object(", ".update_object(", ".delete_object("][..],
        ),
        (
            "src/services/class_relations.rs",
            &[".create_class_relation(", ".delete_class_relation("][..],
        ),
        (
            "src/services/object_relations.rs",
            &[".create_object_relation(", ".delete_object_relation("][..],
        ),
    ];

    for (relative_path, mutations) in files_and_mutations {
        let path = root.join(relative_path);
        let source = read_source(&path)
            .unwrap_or_else(|error| panic!("could not read {}: {error}", path.display()));
        let production = source.split("#[cfg(test)]").next().unwrap_or(&source);
        for mutation in mutations {
            let mut remainder = production;
            while let Some(offset) = remainder.find(mutation) {
                let call = &remainder[offset..];
                let call = call
                    .split_once(".await")
                    .map_or(call, |(before_await, _)| before_await);
                assert!(
                    call.contains("context"),
                    "{} uses {mutation} without the required audit context",
                    path.display()
                );
                remainder = &remainder[offset + mutation.len()..];
            }
        }
        for forbidden in ["save_without_events", "delete_without_events"] {
            assert!(
                !production.contains(forbidden),
                "{} exposes an unrecorded mutation through {forbidden}",
                path.display()
            );
        }
    }
}

#[test]
fn legacy_root_postgres_tree_is_absent() {
    let root = repository_root();
    let path = root.join("src/storage/postgres/mod.rs");
    assert!(
        !path.exists(),
        "the legacy root PostgreSQL compatibility tree must be removed"
    );
}

#[test]
fn backend_neutral_layers_do_not_import_database_implementation_details() {
    let root = repository_root();
    let mut violations = Vec::new();

    for directory in ["src/services", "src/storage"] {
        for path in rust_files(&root.join(directory)) {
            if is_storage_adapter(&root, &path)
                || path.starts_with(root.join("src/storage/context"))
                || path == root.join("src/storage/factory.rs")
            {
                continue;
            }

            let source = read_source(&path)
                .unwrap_or_else(|error| panic!("could not read {}: {error}", path.display()));
            let source = if path.starts_with(root.join("src/services")) {
                source.split("#[cfg(test)]").next().unwrap_or(&source)
            } else {
                &source
            };
            for forbidden in [
                "hubuum_storage_postgres::operations",
                "hubuum_storage_postgres::with_connection",
                "hubuum_storage_postgres::with_transaction",
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
fn validated_identifiers_are_owned_by_the_publishable_domain_crate() {
    let root = repository_root();
    let identifier_source = read_source(&root.join("crates/hubuum-domain/src/identifier.rs"))
        .expect("domain identifier source should be readable");

    for (domain_name, model_path, application_name) in [
        ("CollectionId", "src/models/collection.rs", "CollectionID"),
        ("ClassId", "src/models/class.rs", "HubuumClassID"),
        ("ObjectId", "src/models/object.rs", "HubuumObjectID"),
        (
            "ClassRelationId",
            "src/models/relation.rs",
            "HubuumClassRelationID",
        ),
        (
            "ObjectRelationId",
            "src/models/relation.rs",
            "HubuumObjectRelationID",
        ),
        ("GroupId", "src/models/group.rs", "GroupID"),
        ("PrincipalId", "src/models/principal.rs", "PrincipalID"),
        ("UserId", "src/models/user.rs", "UserID"),
        ("TokenId", "src/models/token.rs", "TokenID"),
        ("TaskId", "src/models/task.rs", "TaskID"),
        (
            "EventSinkId",
            "src/models/event_subscription.rs",
            "EventSinkID",
        ),
        (
            "EventSubscriptionId",
            "src/models/event_subscription.rs",
            "EventSubscriptionID",
        ),
        (
            "EventDeliveryId",
            "src/models/event_delivery.rs",
            "EventDeliveryID",
        ),
        ("RestoreJobId", "src/models/backup.rs", "RestoreJobID"),
        (
            "ExportTemplateId",
            "src/models/export_template.rs",
            "ExportTemplateID",
        ),
        (
            "RemoteTargetId",
            "src/models/remote_target.rs",
            "RemoteTargetID",
        ),
        (
            "ServiceAccountId",
            "src/models/service_account.rs",
            "ServiceAccountID",
        ),
        (
            "ComputedFieldDefinitionId",
            "src/models/computed_field.rs",
            "ComputedFieldDefinitionID",
        ),
    ] {
        assert!(
            identifier_source.contains(domain_name),
            "hubuum-domain is missing {domain_name}"
        );
        let model_source = read_source(&root.join(model_path))
            .unwrap_or_else(|error| panic!("could not read {model_path}: {error}"));
        assert!(
            model_source.contains(&format!("{domain_name} as {application_name}")),
            "{model_path} must only alias {domain_name} as {application_name}"
        );
        assert!(
            !model_source.contains(&format!("struct {application_name}")),
            "{model_path} must not redefine {application_name}"
        );
    }

    assert!(
        !read_source(&root.join("src/macros.rs"))
            .expect("application macro source should be readable")
            .contains("macro_rules! int_id_newtype"),
        "the application crate must not own a second identifier generator"
    );
}

#[test]
fn resource_services_depend_on_their_exact_storage_families() {
    let root = repository_root();

    for (file, service, storage_trait) in [
        ("collections.rs", "CollectionService", "CollectionStorage"),
        ("classes.rs", "ClassService", "ClassStorage"),
        ("objects.rs", "ObjectService", "ObjectStorage"),
        (
            "class_relations.rs",
            "ClassRelationService",
            "ClassRelationStorage",
        ),
        (
            "object_relations.rs",
            "ObjectRelationService",
            "ObjectRelationStorage",
        ),
    ] {
        let path = root.join("src/services").join(file);
        let source = read_source(&path)
            .unwrap_or_else(|error| panic!("could not read {}: {error}", path.display()));
        let production = source.split("#[cfg(test)]").next().unwrap_or(&source);

        assert!(
            production.contains(&format!("storage: Arc<dyn {storage_trait}>")),
            "{service} must depend directly on {storage_trait}"
        );
        for forbidden in ["LifecycleStorage", "StorageBackend", "StorageHandle"] {
            assert!(
                !production.contains(forbidden),
                "{service} depends on overly broad storage type {forbidden}"
            );
        }
    }
}

#[test]
fn selectable_storage_backends_are_complete_and_test_models_are_not_selectable() {
    let root = repository_root();
    let contract_path = root.join("crates/hubuum-storage-core/src/backend.rs");
    let contract_source = read_source(&contract_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", contract_path.display()));
    let family_path = root.join("crates/hubuum-storage-core/src/families.rs");
    let family_source = read_source(&family_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", family_path.display()));
    let adapter_path = root.join("crates/hubuum-storage-postgres/src/backend/mod.rs");
    let adapter_source = read_source(&adapter_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", adapter_path.display()));
    let context_path = root.join("src/storage/context");
    let context_source = read_rust_module_tree(&context_path);
    let notification_adapter_path =
        root.join("crates/hubuum-storage-postgres/src/backend/notifications.rs");
    let notification_adapter_source =
        read_source(&notification_adapter_path).unwrap_or_else(|error| {
            panic!(
                "could not read {}: {error}",
                notification_adapter_path.display()
            )
        });

    let contract_families = trait_supertraits(&contract_source, "StorageBackend");
    for required in REQUIRED_STORAGE_BACKEND_FAMILIES {
        assert!(
            contract_families.contains(*required),
            "complete storage contract is missing family {required}"
        );
    }
    let family_traits = REQUIRED_STORAGE_BACKEND_FAMILIES
        .iter()
        .flat_map(|family| trait_supertraits(&family_source, family))
        .collect::<BTreeSet<_>>();
    for required in REQUIRED_STORAGE_BACKEND_TRAITS {
        assert!(
            family_traits.contains(*required),
            "complete storage families are missing {required}"
        );
    }
    assert!(
        adapter_source.contains("impl StorageBackend for PostgresStorage {}"),
        "PostgreSQL must explicitly opt into the complete storage contract"
    );
    assert!(
        !adapter_source.contains("StorageBackend for MemoryStorageModel"),
        "the focused memory contract model must not be selectable as a full backend"
    );
    let required = "S: RegisteredStorageBackend";
    assert!(
        context_source.contains(required),
        "application composition must enforce the complete contract through {required}"
    );
    let notification_adapter_production = notification_adapter_source
        .split("#[cfg(test)]")
        .next()
        .unwrap_or(&notification_adapter_source);
    assert!(
        !notification_adapter_production.contains("get_config"),
        "the PostgreSQL notification adapter must receive settings through composition"
    );
}

#[test]
fn ordinary_storage_mutations_have_no_optional_audit_context_escape_hatch() {
    let root = repository_root();
    for crate_path in [
        "crates/hubuum-storage-core/src",
        "crates/hubuum-storage-postgres/src",
    ] {
        let source = read_rust_module_tree(&root.join(crate_path));
        for forbidden in [
            "Option<EventContext>",
            "Option<&EventContext>",
            "event_context: Option",
        ] {
            assert!(
                !source.contains(forbidden),
                "{crate_path} exposes an optional audit context through {forbidden}"
            );
        }
    }

    let families = read_source(&root.join("crates/hubuum-storage-core/src/families.rs"))
        .expect("complete storage families should be readable");
    let workflows = trait_supertraits(&families, "WorkflowStorage");
    assert!(
        workflows.contains("ImportStorage") && workflows.contains("RestoreStorage"),
        "import and restore must remain explicit complete-backend capabilities"
    );
}

#[test]
fn audited_resource_mutations_return_durable_receipt_outcomes() {
    let root = repository_root();
    let mutation = read_source(&root.join("crates/hubuum-storage-core/src/mutation.rs"))
        .expect("mutation outcome contract should be readable");
    let mutation = mutation.split_whitespace().collect::<Vec<_>>().join(" ");
    for required in [
        "pub struct StorageAuditReceipt",
        "pub struct StorageAuditReceipts",
        "pub enum StorageMutationOutcome<T>",
        "Committed { value: T, audits: StorageAuditReceipts, }",
        "Unchanged(T)",
    ] {
        assert!(
            mutation.contains(required),
            "audited mutation contract is missing {required}"
        );
    }

    for file in [
        "resource_lifecycle.rs",
        "relation_lifecycle.rs",
        "identity_resources.rs",
        "transaction.rs",
    ] {
        let source = read_source(&root.join("crates/hubuum-storage-core/src").join(file))
            .unwrap_or_else(|error| panic!("could not read {file}: {error}"));
        assert!(
            source.contains("StorageMutationOutcome<"),
            "{file} must expose explicit audited mutation outcomes"
        );
    }
}

#[test]
fn selectable_backends_pass_a_sealed_behavioral_certification_gate() {
    let root = repository_root();
    let certification = read_source(&root.join("src/storage/contract.rs"))
        .expect("storage certification gate should be readable");
    for required in [
        "trait CertifiedStorageBackend: StorageBackend + private::Sealed",
        "impl CertifiedStorageBackend for hubuum_storage_postgres::PostgresStorage",
    ] {
        assert!(
            certification.contains(required),
            "storage certification gate is missing {required}"
        );
    }

    let composition = read_source(&root.join("src/storage/context/mod.rs"))
        .expect("storage composition gate should be readable");
    assert!(
        composition.contains(
            "trait RegisteredStorageBackend:\n    CertifiedStorageBackend + Clone + 'static"
        ) && composition.contains("S: RegisteredStorageBackend"),
        "runtime storage selection must require behavioral certification"
    );
}

#[test]
fn storage_observers_are_required_at_production_composition() {
    let root = repository_root();
    let runtime = read_source(&root.join("crates/hubuum-storage-postgres/src/runtime.rs"))
        .expect("PostgreSQL runtime should be readable");
    assert!(
        runtime.contains("pub fn new(pool: PostgresPool, observer: Arc<dyn PostgresObserver>)"),
        "PostgreSQL runtime construction must require an application-owned observer"
    );
    assert!(
        runtime.contains("pub fn unobserved(pool: PostgresPool)"),
        "tests and one-shot tools need an explicit observation opt-out"
    );

    let composition = read_source(&root.join("src/storage/factory.rs"))
        .expect("PostgreSQL application composition should be readable");
    assert!(
        composition.contains("PostgresStorage::new(pool, Arc::new(ApplicationPostgresObserver))"),
        "production composition must provide its native observer implementation"
    );
}

#[test]
fn every_registered_backend_runs_the_reusable_six_part_audit_contract() {
    let root = repository_root();
    let manifest = read_source(&root.join("crates/hubuum-storage-conformance/Cargo.toml"))
        .expect("storage conformance manifest should be readable");
    for required in [
        "publish = true",
        "rust-api = \"experimental-public\"",
        "policy-document = \"docs/rust_api/hubuum-storage-conformance.md\"",
        "release-train = \"storage-sdk\"",
    ] {
        assert!(
            manifest.contains(required),
            "the public conformance harness is missing {required}"
        );
    }

    let fixture = read_source(&root.join("src/tests/storage_contract/certification.rs"))
        .expect("registered backend contract fixture should be readable");
    for required in [
        "for environment in available_backend_environments()",
        "verify_backend_audit_contract(&fixture)",
        "assert_eq!(report.checks(), 6)",
    ] {
        assert!(
            fixture.contains(required),
            "registered backend certification is missing {required}"
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
        let source = read_source(&path)
            .unwrap_or_else(|error| panic!("could not read {}: {error}", path.display()));
        let production = source
            .split("\n#[cfg(test)]\nmod tests")
            .next()
            .unwrap_or(&source);
        for forbidden in [
            "hubuum_storage_postgres",
            "hubuum_storage_postgres",
            "PostgresPool",
            "PgConnection",
            "diesel::",
            "diesel_async",
            "crate::schema",
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
fn validated_facts_remain_typed_across_application_storage_boundaries() {
    let root = repository_root();

    let computed_service = read_source(&root.join("src/services/computed_fields.rs"))
        .expect("computed-field service should be readable");
    assert!(
        !computed_service.contains("_id_to_storage")
            && !computed_service.contains("expect(\"validated computed field"),
        "computed-field services must carry validated IDs instead of flattening and rebuilding them"
    );

    let computed_contract =
        read_source(&root.join("crates/hubuum-storage-core/src/computed_fields.rs"))
            .expect("computed-field storage contract should be readable");
    let input = item_body(
        &computed_contract,
        "struct",
        "StorageComputedFieldDefinitionInput",
    );
    assert!(
        input.contains("definition: Definition")
            && !input.contains("operation: Value")
            && !input.contains("result_type: String"),
        "computed definitions must cross storage as one validated proof value"
    );

    let lifecycle = read_source(&root.join("crates/hubuum-storage-core/src/resource_lifecycle.rs"))
        .expect("resource lifecycle contract should be readable");
    let class = item_body(&lifecycle, "struct", "StorageClass");
    assert!(
        class.contains("schema_policy: StorageClassSchemaPolicy")
            && !class.contains("validate_schema: bool"),
        "class schema and enforcement state must have one correlated representation"
    );

    let task_model =
        read_source(&root.join("src/models/task.rs")).expect("task model should be readable");
    assert_eq!(
        enum_variants(&task_model, "TaskDetails"),
        BTreeSet::from([
            "Backup".to_string(),
            "Export".to_string(),
            "Import".to_string(),
            "SchemaValidation".to_string(),
            "SchemaProfiling".to_string(),
            "Reindex".to_string(),
            "RemoteCall".to_string(),
        ]),
        "task details must use mutually exclusive variants"
    );
    let new_event = item_body(&task_model, "struct", "NewTaskEventRecord");
    assert!(
        !new_event.contains("task_id"),
        "a claimed task lease must remain the sole proof of event task identity"
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
        let source = read_source(&path)
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
        let source = read_source(&path)
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
        let source = read_source(&path)
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
        let source = read_source(&path)
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
        "src/services/exports/mod.rs",
        "src/tasks/execution.rs",
        "src/tasks/remote_call.rs",
        "src/tasks/worker.rs",
    ] {
        let path = root.join(file);
        let source = read_source(&path)
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
    let mut paths = rust_files(&root.join("src/services/exports"));
    paths.push(root.join("src/services/authorization_resources.rs"));
    for path in paths.into_iter().filter(|path| !is_test_module(path)) {
        let source = read_source(&path)
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
    let mut paths = rust_files(&root.join("src/services/exports"));
    paths.extend([
        root.join("src/models/export_template.rs"),
        root.join("src/api/v1/handlers/export_templates.rs"),
    ]);
    for path in paths.into_iter().filter(|path| !is_test_module(path)) {
        let source = read_source(&path)
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
fn export_template_lifecycle_is_owned_by_the_postgres_adapter() {
    let root = repository_root();
    let adapter_path =
        root.join("crates/hubuum-storage-postgres/src/operations/export_template.rs");
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
            "the PostgreSQL export-template adapter depends on application detail {forbidden}"
        );
    }
    for required in [
        "with_read_only_snapshot",
        "assert_locked_revision_precondition",
        "append_export_template_audit",
        "apply_query_options_with_fields",
    ] {
        assert!(
            adapter.contains(required),
            "the PostgreSQL export-template adapter is missing {required}"
        );
    }

    let capability_path =
        root.join("crates/hubuum-storage-postgres/src/backend/export_templates.rs");
    let capability = read_source(&capability_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", capability_path.display()));
    let implementation = item_body(
        &capability,
        "impl",
        "ExportTemplateStorage for PostgresStorage",
    );
    for method in [
        "get_export_template",
        "list_export_templates",
        "list_export_templates_in_collection",
        "create_export_template",
        "replace_export_template",
        "delete_export_template",
    ] {
        let method_body = item_body(implementation, "fn", method);
        assert!(
            method_body.contains("crate::operations::export_template"),
            "the {method} export-template implementation must delegate into the adapter crate"
        );
        assert!(
            !method_body.contains("self.pool"),
            "the {method} export-template implementation leaks the PostgreSQL pool"
        );
    }

    let legacy_path = root.join("src/storage/postgres/operations/export_template.rs");
    assert!(
        !legacy_path.exists(),
        "the application-owned PostgreSQL export-template implementation still exists"
    );
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
        let source = read_source(&path)
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
    let source = read_source(&path)
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

    let facade_path = root.join("crates/hubuum-storage-postgres/src/backend/restores.rs");
    let facade = read_source(&facade_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", facade_path.display()));
    let implementation = item_body(&facade, "impl", "RestoreStorage for PostgresStorage");
    let apply = item_body(implementation, "fn", "apply_restore_replay");
    assert!(
        apply.contains("restore_lifecycle::apply_restore_replay"),
        "the destructive restore implementation must delegate into the adapter crate"
    );
    for forbidden in ["self.pool", "apply_restore_db", "map_postgres_error"] {
        assert!(
            !apply.contains(forbidden),
            "the destructive restore facade retains application detail {forbidden}"
        );
    }

    let legacy_path = root.join("src/storage/postgres/operations/restore.rs");
    assert!(
        !legacy_path.exists(),
        "the application-owned destructive restore implementation still exists"
    );
}

#[test]
fn storage_error_translation_has_one_way_dependency_direction() {
    let root = repository_root();
    let errors_path = root.join("src/errors.rs");
    let errors_source = read_source(&errors_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", errors_path.display()));
    let postgres_error_path = root.join("crates/hubuum-storage-postgres/src/error.rs");
    let postgres_error_source = read_source(&postgres_error_path).unwrap_or_else(|error| {
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
    assert!(
        postgres_error_source.contains("impl From<DieselError> for PostgresStorageError"),
        "Diesel failures must be classified inside the PostgreSQL crate"
    );
    assert!(
        !postgres_error_source.contains("source: ApiError"),
        "the PostgreSQL adapter error must classify, not retain, an application error"
    );
    for required in [
        "kind: StorageErrorKind",
        "message: String",
        "current_revision: Option<ResourceRevision>",
    ] {
        assert!(
            postgres_error_source.contains(required),
            "the PostgreSQL adapter error is missing classified field {required}"
        );
    }
}
