//! Compile-time-adjacent guards for the application/storage boundary.

#[cfg(test)]
use std::collections::BTreeSet;
#[cfg(test)]
use std::fs;
#[cfg(test)]
use std::path::{Path, PathBuf};

#[cfg(test)]
fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

#[cfg(test)]
fn read_source(path: &Path) -> std::io::Result<String> {
    fs::read_to_string(path).map(|source| source.replace("\r\n", "\n"))
}

#[cfg(test)]
fn item_body<'a>(source: &'a str, keyword: &str, name: &str) -> &'a str {
    let marker = format!("{keyword} {name}");
    let declaration = source
        .find(&marker)
        .unwrap_or_else(|| panic!("could not find {marker}"));
    let opening = source[declaration..]
        .find('{')
        .map(|offset| declaration + offset)
        .unwrap_or_else(|| panic!("could not find opening brace for {marker}"));
    let mut depth = 0_u32;
    for (offset, character) in source[opening..].char_indices() {
        match character {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return &source[opening + 1..opening + offset];
                }
            }
            _ => {}
        }
    }
    panic!("could not find closing brace for {marker}");
}

#[cfg(test)]
fn trait_methods(source: &str, trait_name: &str) -> BTreeSet<String> {
    item_body(source, "trait", trait_name)
        .lines()
        .filter_map(|line| {
            let line = line.trim_start();
            let method = line
                .strip_prefix("async fn ")
                .or_else(|| line.strip_prefix("fn "))?;
            Some(
                method
                    .chars()
                    .take_while(|character| character.is_ascii_alphanumeric() || *character == '_')
                    .collect(),
            )
        })
        .collect()
}

#[cfg(test)]
fn enum_variants(source: &str, enum_name: &str) -> BTreeSet<String> {
    let mut variants = BTreeSet::new();
    let mut nested = 0_i32;
    for line in item_body(source, "enum", enum_name).lines() {
        let line = line.trim();
        if nested == 0 && !line.is_empty() && !line.starts_with('#') && !line.starts_with("//") {
            let candidate = line
                .chars()
                .take_while(|character| character.is_ascii_alphanumeric() || *character == '_')
                .collect::<String>();
            if candidate
                .chars()
                .next()
                .is_some_and(|character| character.is_ascii_uppercase())
            {
                variants.insert(candidate);
            }
        }
        for character in line.chars() {
            match character {
                '(' | '[' | '{' => nested += 1,
                ')' | ']' | '}' => nested -= 1,
                _ => {}
            }
        }
    }
    variants
}

#[cfg(test)]
fn toml_string_set(table: &toml::Table, key: &str) -> BTreeSet<String> {
    table
        .get(key)
        .and_then(toml::Value::as_array)
        .unwrap_or_else(|| panic!("semantic coverage entry is missing array '{key}'"))
        .iter()
        .map(|value| {
            value
                .as_str()
                .unwrap_or_else(|| panic!("semantic coverage '{key}' entries must be strings"))
                .to_string()
        })
        .collect()
}

#[cfg(test)]
fn storage_semantic_manifest(root: &Path) -> toml::Value {
    let path = root.join("docs/storage_boundary/semantic-coverage.toml");
    let source = read_source(&path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", path.display()));
    toml::from_str(&source).expect("storage semantic coverage inventory should be valid TOML")
}

#[cfg(test)]
fn assert_scenario_exists(root: &Path, scenario: &str) {
    let (path, symbol) = scenario.split_once("::").unwrap_or_else(|| {
        panic!("semantic coverage scenario must use path::symbol syntax: {scenario}")
    });
    let path = root.join(path);
    let source = read_source(&path)
        .unwrap_or_else(|error| panic!("could not read scenario {}: {error}", path.display()));
    assert!(
        source.contains(&format!("fn {symbol}")),
        "semantic coverage scenario {scenario} does not name a function in its source"
    );
}

#[cfg(test)]
const REQUIRED_STORAGE_BACKEND_TRAITS: &[&str] = &[
    "StorageIdentity",
    "CollectionStore",
    "ClassStore",
    "ObjectStore",
    "ClassRelationStore",
    "ObjectRelationStore",
    "AuthenticationStorage",
    "IdentityStorage",
    "UserStorage",
    "TokenStorage",
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
    "HistoryStorage",
    "InventoryStorage",
    "MetricsStorage",
    "OperationalStateStorage",
    "TokenRetentionStorage",
    "UnifiedSearchStorage",
    "GroupStorage",
    "PrincipalStorage",
    "CollectionAuthorizationStorage",
    "RemoteTargetStorage",
    "TaskQueueStorage",
    "TaskExecutionStorage",
    "BackupSnapshotStorage",
    "RestoreStorage",
    "ImportStorage",
    "ExportQueryStorage",
    "ExportTemplateStorage",
    "WorkerNotificationStorage",
    "StorageExecution",
];

#[test]
fn storage_boundary_documentation_covers_the_complete_contract() {
    let root = repository_root();
    let overview_path = root.join("docs/storage_boundary.md");
    let overview = read_source(&overview_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", overview_path.display()));

    for document in [
        "capability-families.md",
        "backend-author-guide.md",
        "maintainer-guide.md",
        "testing.md",
    ] {
        let relative_link = format!("storage_boundary/{document}");
        assert!(
            overview.contains(&relative_link),
            "storage boundary overview must link to {relative_link}"
        );

        let path = root.join("docs/storage_boundary").join(document);
        assert!(
            path.is_file(),
            "storage boundary guide is missing {}",
            path.display()
        );
    }

    let family_path = root.join("docs/storage_boundary/capability-families.md");
    let families = read_source(&family_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", family_path.display()));
    for required_trait in REQUIRED_STORAGE_BACKEND_TRAITS {
        assert!(
            families.contains(required_trait),
            "required storage trait {required_trait} is not mapped to a capability family"
        );
    }
}

#[test]
fn storage_semantic_coverage_inventory_matches_traits_variants_and_evidence() {
    let root = repository_root();
    let manifest = storage_semantic_manifest(&root);
    let traits = manifest
        .get("traits")
        .and_then(toml::Value::as_table)
        .expect("semantic coverage inventory should have a traits table");
    let expected_traits = REQUIRED_STORAGE_BACKEND_TRAITS
        .iter()
        .map(|name| (*name).to_string())
        .collect::<BTreeSet<_>>();
    let inventoried_traits = traits.keys().cloned().collect::<BTreeSet<_>>();
    assert_eq!(
        inventoried_traits, expected_traits,
        "semantic coverage must inventory every complete-backend trait exactly"
    );

    let contract = read_source(&root.join("crates/hubuum-storage-core/src/backend.rs"))
        .expect("complete storage contract should be readable");
    let aggregate = contract
        .split_once("pub trait StorageBackend:")
        .and_then(|(_, remainder)| remainder.split_once("\n{"))
        .map(|(body, _)| body)
        .expect("StorageBackend should have a readable aggregate declaration")
        .split('+')
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_string)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        aggregate, expected_traits,
        "the complete backend aggregate and semantic inventory must change together"
    );

    for (trait_name, value) in traits {
        let entry = value
            .as_table()
            .unwrap_or_else(|| panic!("trait inventory entry {trait_name} must be a table"));
        let source_path = entry
            .get("source")
            .and_then(toml::Value::as_str)
            .unwrap_or_else(|| panic!("trait {trait_name} is missing its source"));
        let source = read_source(&root.join(source_path))
            .unwrap_or_else(|error| panic!("could not read {source_path}: {error}"));
        assert_eq!(
            toml_string_set(entry, "methods"),
            trait_methods(&source, trait_name),
            "trait method inventory drifted for {trait_name}"
        );

        let scenarios = ["shared_scenarios", "native_scenarios"]
            .into_iter()
            .filter_map(|key| entry.get(key).map(|_| toml_string_set(entry, key)))
            .flatten()
            .collect::<BTreeSet<_>>();
        assert!(
            !scenarios.is_empty(),
            "trait {trait_name} must name shared or adapter-native semantic evidence"
        );
        for scenario in scenarios {
            assert_scenario_exists(&root, &scenario);
        }
    }

    let enums = manifest
        .get("enums")
        .and_then(toml::Value::as_table)
        .expect("semantic coverage inventory should have an enums table");
    assert!(
        !enums.is_empty(),
        "input variant inventory must not be empty"
    );
    for (enum_name, value) in enums {
        let entry = value
            .as_table()
            .unwrap_or_else(|| panic!("enum inventory entry {enum_name} must be a table"));
        let source_path = entry
            .get("source")
            .and_then(toml::Value::as_str)
            .unwrap_or_else(|| panic!("enum {enum_name} is missing its source"));
        let source = read_source(&root.join(source_path))
            .unwrap_or_else(|error| panic!("could not read {source_path}: {error}"));
        assert_eq!(
            toml_string_set(entry, "variants"),
            enum_variants(&source, enum_name),
            "input variant inventory drifted for {enum_name}"
        );
        let scenarios = toml_string_set(entry, "scenarios");
        assert!(
            !scenarios.is_empty(),
            "enum {enum_name} must name semantic evidence"
        );
        for scenario in scenarios {
            assert_scenario_exists(&root, &scenario);
        }
    }
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
fn read_rust_module_tree(directory: &Path) -> String {
    rust_files(directory)
        .into_iter()
        .map(|path| {
            read_source(&path)
                .unwrap_or_else(|error| panic!("could not read {}: {error}", path.display()))
        })
        .collect::<Vec<_>>()
        .join("\n")
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
    let context_source = read_source(&context_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", context_path.display()));
    let trait_path = root.join("src/storage/context");
    let trait_source = read_rust_module_tree(&trait_path);
    let application_path = root.join("src/application.rs");
    let application_source = read_source(&application_path)
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
    for forbidden in ["PermissionBackend", "permission_backend"] {
        assert!(
            !backend_context_body.contains(forbidden),
            "storage-only contexts must not select authorization policy: {forbidden}"
        );
    }
    let backend_access_body = trait_source
        .split_once("pub trait BackendAccess")
        .and_then(|(_, remainder)| remainder.split_once("\n    }"))
        .map(|(body, _)| body)
        .expect("the sealed backend access trait should have a readable body");
    assert!(
        backend_access_body.contains("fn storage_handle(&self) -> StorageHandle"),
        "storage contexts must preserve the already configured opaque handle"
    );
    for forbidden in ["PostgresPool", "postgres_pool", "db_pool"] {
        assert!(
            !backend_access_body.contains(forbidden),
            "the sealed context contract exposes backend detail: {forbidden}"
        );
    }
    assert!(
        !application_source.contains(".app_data(Data::new(app_pool"),
        "the production HTTP server must not register a raw database pool"
    );
    assert!(
        !application_source.contains("pool: db::PostgresPool"),
        "operational HTTP services must receive AppContext rather than PostgresPool"
    );

    assert_eq!(
        trait_source
            .matches("match &$handle.inner.implementation")
            .count(),
        1,
        "backend selection must stay centralized in dispatch_backend"
    );
    assert!(
        trait_source.contains("macro_rules! dispatch_backend"),
        "the opaque handle must centralize exhaustive backend dispatch"
    );
    for test_only_compatibility in [
        "#[cfg(any(test, feature = \"integration-test-support\"))]\nimpl private::BackendAccess for PostgresPool",
        "#[cfg(any(test, feature = \"integration-test-support\"))]\nimpl StorageContext for PostgresPool",
    ] {
        assert!(
            trait_source.contains(test_only_compatibility),
            "concrete-pool context compatibility must remain test-only"
        );
    }
}

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
        source.contains(
            "#[cfg(any(test, feature = \"integration-test-support\"))]\nimpl AuthorizationContext for StorageHandle"
        ),
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
            let quoted = call
                .split('"')
                .skip(1)
                .step_by(2)
                .take(2)
                .collect::<Vec<_>>();
            assert_eq!(
                quoted.len(),
                2,
                "storage observer must have static capability and operation labels"
            );
            let pair = (quoted[0].to_string(), quoted[1].to_string());
            for label in [&pair.0, &pair.1] {
                assert!(
                    !label.is_empty()
                        && label
                            .bytes()
                            .all(|byte| byte.is_ascii_lowercase() || byte == b'_'),
                    "storage observation label must be bounded snake_case: {label}"
                );
            }
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
    let unobserved_traits = ["StorageIdentity", "ExportQueryStorage", "StorageExecution"];
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
            "CollectionStore"
            | "ClassStore"
            | "ObjectStore"
            | "ClassRelationStore"
            | "ObjectRelationStore" => (
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
            if trait_name == "MetricsStorage" && method == "metrics_pool_state" {
                assert_eq!(
                    observer_count, 0,
                    "pool-state collection must not recursively observe metric collection"
                );
                continue;
            }
            expected_observations += 1;
            assert_eq!(
                observer_count, 1,
                "{trait_name}::{method} must cross exactly one common storage observer"
            );
        }
    }

    assert_eq!(
        labels.len(),
        expected_observations,
        "every observed contract method must have one unique bounded label pair"
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
            "crate::storage::postgres",
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

    for path in rust_files(&root.join("src/storage/postgres")) {
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
    let path = root.join("benches/postgres/storage_postgres_criterion.rs");
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
        "src/exports/mod.rs",
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
    for path in paths {
        let source = read_source(&path)
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
fn group_domain_types_are_free_of_persistence_implementation_details() {
    let root = repository_root();
    let mut violations = Vec::new();

    for relative_path in ["src/models/group.rs", "src/models/principal_group.rs"] {
        let path = root.join(relative_path);
        let source = read_source(&path)
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

    let adapter_path = root.join("src/storage/postgres/operations/group.rs");
    let adapter_source = read_source(&adapter_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", adapter_path.display()));
    for required in [
        "struct GroupRow",
        "struct PrincipalGroupRow",
        "struct UpdateGroupRow",
        "impl From<GroupRow> for Group",
        "impl From<PrincipalGroupRow> for PrincipalGroup",
        "impl CursorSqlMapping for GroupRow",
    ] {
        assert!(
            adapter_source.contains(required),
            "PostgreSQL group adapter is missing {required}"
        );
    }
    assert!(
        violations.is_empty(),
        "group domain types crossed into persistence details:\n{}",
        violations.join("\n")
    );
}

#[test]
fn principal_domain_types_are_free_of_persistence_implementation_details() {
    let root = repository_root();
    let path = root.join("src/models/principal.rs");
    let source = read_source(&path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", path.display()));
    let production_source = source.split("#[cfg(test)]").next().unwrap_or(&source);
    let violations = [
        "diesel::",
        "diesel(",
        "crate::schema",
        "storage::postgres",
        "CursorSqlMapping",
        "CursorSqlField",
        "CursorSqlType",
    ]
    .into_iter()
    .filter(|forbidden| production_source.contains(forbidden))
    .collect::<Vec<_>>();

    let adapter_path = root.join("src/storage/postgres/operations/principal.rs");
    let adapter_source = read_source(&adapter_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", adapter_path.display()));
    for required in [
        "struct PrincipalRow",
        "struct PrincipalMemberQueryRow",
        "impl From<PrincipalRow> for Principal",
        "impl CursorSqlMapping for PrincipalRow",
        "impl CursorSqlMapping for PrincipalMemberQueryRow",
    ] {
        assert!(
            adapter_source.contains(required),
            "PostgreSQL principal adapter is missing {required}"
        );
    }

    assert!(
        violations.is_empty(),
        "principal domain types crossed into persistence details: {}",
        violations.join(", ")
    );
}

#[test]
fn identity_subtype_domain_types_are_free_of_persistence_implementation_details() {
    let root = repository_root();
    let mut violations = Vec::new();

    for relative_path in [
        "src/models/identity.rs",
        "src/models/user.rs",
        "src/models/service_account.rs",
        "src/models/token.rs",
        "src/models/traits/user.rs",
    ] {
        let path = root.join(relative_path);
        let source = read_source(&path)
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

    for (relative_path, required) in [
        (
            "crates/hubuum-storage-postgres/src/operations/identity_scope.rs",
            &["struct IdentityScopeRow"][..],
        ),
        (
            "src/storage/postgres/operations/user/mod.rs",
            &[
                "struct UserRow",
                "struct UpdateUserRow",
                "impl From<UserRow> for User",
                "CursorSqlMapping for UserWithNameQueryRow",
            ][..],
        ),
        (
            "src/storage/postgres/operations/service_account.rs",
            &[
                "struct ServiceAccountRow",
                "struct UpdateServiceAccountRow",
                "impl From<ServiceAccountRow> for ServiceAccount",
                "CursorSqlMapping for ServiceAccountWithNameQueryRow",
            ][..],
        ),
        (
            "src/storage/postgres/operations/token.rs",
            &[
                "struct PrincipalTokenRow",
                "impl From<PrincipalTokenRow> for PrincipalToken",
                "CursorSqlMapping for PrincipalTokenRow",
            ][..],
        ),
    ] {
        let path = root.join(relative_path);
        let source = read_source(&path)
            .unwrap_or_else(|error| panic!("could not read {}: {error}", path.display()));
        for item in required {
            assert!(
                source.contains(item),
                "PostgreSQL identity adapter is missing {item}"
            );
        }
    }

    assert!(
        violations.is_empty(),
        "identity subtype domain types crossed into persistence details:\n{}",
        violations.join("\n")
    );
}

#[test]
fn permission_domain_types_are_free_of_persistence_implementation_details() {
    let root = repository_root();
    let mut violations = Vec::new();

    for relative_path in [
        "src/models/permissions.rs",
        "src/models/output.rs",
        "src/models/traits/output.rs",
    ] {
        let path = root.join(relative_path);
        let source = read_source(&path)
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
            "PermissionFilter",
        ] {
            if production_source.contains(forbidden) {
                violations.push(format!("{} contains {forbidden}", path.display()));
            }
        }
    }

    let adapter_path = root.join("src/storage/postgres/operations/permissions.rs");
    let adapter_source = read_source(&adapter_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", adapter_path.display()));
    for required in [
        "struct PermissionRow",
        "struct NewPermission",
        "struct UpdatePermission",
        "impl From<PermissionRow> for Permission",
        "trait PermissionFilter",
    ] {
        assert!(
            adapter_source.contains(required),
            "PostgreSQL permission adapter is missing {required}"
        );
    }

    let query_path = root.join("src/storage/postgres/operations/collection/permissions.rs");
    let query_source = read_source(&query_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", query_path.display()));
    assert!(
        query_source.contains("impl CursorSqlMapping for GroupPermissionQueryRow"),
        "PostgreSQL adapter must own the group-permission SQL cursor mapping"
    );

    assert!(
        violations.is_empty(),
        "permission domain types crossed into persistence details:\n{}",
        violations.join("\n")
    );
}

#[test]
fn collection_domain_types_are_free_of_persistence_implementation_details() {
    let root = repository_root();
    let mut violations = Vec::new();

    for relative_path in [
        "src/models/collection.rs",
        "src/models/traits/collection.rs",
    ] {
        let path = root.join(relative_path);
        let source = read_source(&path)
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

    let adapter_path = root.join("src/storage/postgres/operations/collection/records.rs");
    let adapter_source = read_source(&adapter_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", adapter_path.display()));
    for required in [
        "struct CollectionRow",
        "struct NewCollectionRow",
        "struct UpdateCollectionRow",
        "impl From<CollectionRow> for Collection",
        "impl CursorSqlMapping for CollectionRow",
    ] {
        assert!(
            adapter_source.contains(required),
            "PostgreSQL collection adapter is missing {required}"
        );
    }

    let history_path = root.join("src/storage/postgres/operations/history.rs");
    let history_source = read_source(&history_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", history_path.display()));
    assert!(
        history_source.contains("struct CollectionHistoryRow"),
        "PostgreSQL history adapter must own the collection-history row"
    );

    assert!(
        violations.is_empty(),
        "collection domain types crossed into persistence details:\n{}",
        violations.join("\n")
    );
}

#[test]
fn class_domain_types_are_free_of_persistence_implementation_details() {
    let root = repository_root();
    let mut violations = Vec::new();

    for relative_path in ["src/models/class.rs", "src/models/traits/class.rs"] {
        let path = root.join(relative_path);
        let source = read_source(&path)
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

    let adapter_path = root.join("src/storage/postgres/operations/class.rs");
    let adapter_source = read_source(&adapter_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", adapter_path.display()));
    for required in [
        "struct HubuumClassRow",
        "struct NewHubuumClassRow",
        "struct UpdateHubuumClassRow",
        "impl From<HubuumClassRow> for HubuumClass",
        "impl CursorSqlMapping for HubuumClassRow",
    ] {
        assert!(
            adapter_source.contains(required),
            "PostgreSQL class adapter is missing {required}"
        );
    }

    let history_path = root.join("src/storage/postgres/operations/history.rs");
    let history_source = read_source(&history_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", history_path.display()));
    assert!(
        history_source.contains("struct HubuumClassHistoryRow"),
        "PostgreSQL history adapter must own the class-history row"
    );

    let output_path = root.join("src/models/traits/output.rs");
    let output_source = read_source(&output_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", output_path.display()));
    assert!(
        !output_source.contains("impl CursorSqlMapping for HubuumClassExpanded"),
        "expanded class output must not own PostgreSQL cursor mappings"
    );

    assert!(
        violations.is_empty(),
        "class domain types crossed into persistence details:\n{}",
        violations.join("\n")
    );
}

#[test]
fn object_domain_types_are_free_of_persistence_implementation_details() {
    let root = repository_root();
    let mut violations = Vec::new();

    for relative_path in ["src/models/object.rs", "src/models/traits/object.rs"] {
        let path = root.join(relative_path);
        let source = read_source(&path)
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
    let adapter_source = read_source(&adapter_path)
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
        let source = read_source(&path)
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
    let adapter_source = read_source(&adapter_path)
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
fn workflow_domain_types_are_free_of_persistence_implementation_details() {
    let root = repository_root();
    let mut violations = Vec::new();

    for relative_path in [
        "src/models/task.rs",
        "src/models/backup.rs",
        "src/models/computed_field.rs",
        "src/models/revision.rs",
        "src/models/search.rs",
    ] {
        let path = root.join(relative_path);
        let source = read_source(&path)
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
            "ParsedQueryParamSqlExt",
            "SQLComponent",
            "SQLValue",
        ] {
            if production_source.contains(forbidden) {
                violations.push(format!("{} contains {forbidden}", path.display()));
            }
        }
    }

    for (relative_path, required) in [
        (
            "src/storage/postgres/operations/task_rows.rs",
            &[
                "struct TaskRow",
                "struct NewTaskRow",
                "struct ImportTaskResultRow",
                "struct ExportTaskOutputRow",
                "struct BackupTaskOutputRow",
                "impl From<TaskRow> for crate::models::TaskRecord",
                "impl CursorSqlMapping for TaskRow",
            ][..],
        ),
        (
            "src/storage/postgres/operations/computed_field_rows.rs",
            &[
                "struct ComputedFieldDefinitionRow",
                "struct NewComputedFieldDefinitionRow",
                "struct ClassComputationStateRow",
                "struct ObjectComputedDataRow",
                "impl From<ComputedFieldDefinitionRow> for ComputedFieldDefinition",
                "impl CursorSqlMapping for ComputedFieldDefinitionRow",
            ][..],
        ),
        (
            "crates/hubuum-storage-postgres/src/revision.rs",
            &[
                "struct PostgresRevision",
                "ToSql<BigInt, Pg> for PostgresRevision",
                "FromSql<BigInt, DB> for PostgresRevision",
                "From<PostgresRevision> for ResourceRevision",
            ][..],
        ),
        (
            "src/storage/postgres/operations/search.rs",
            &[
                "struct SQLComponent",
                "enum SQLValue",
                "trait ParsedQueryParamSqlExt",
                "impl ParsedQueryParamSqlExt for ParsedQueryParam",
            ][..],
        ),
    ] {
        let path = root.join(relative_path);
        let source = read_source(&path)
            .unwrap_or_else(|error| panic!("could not read {}: {error}", path.display()));
        for item in required {
            assert!(
                source.contains(item),
                "PostgreSQL workflow adapter is missing {item}"
            );
        }
    }

    assert!(
        violations.is_empty(),
        "workflow domain types crossed into persistence details:\n{}",
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
        ("collections.rs", "CollectionService", "CollectionStore"),
        ("classes.rs", "ClassService", "ClassStore"),
        ("objects.rs", "ObjectService", "ObjectStore"),
        (
            "class_relations.rs",
            "ClassRelationService",
            "ClassRelationStore",
        ),
        (
            "object_relations.rs",
            "ObjectRelationService",
            "ObjectRelationStore",
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
    let composition_path = root.join("src/storage/contract.rs");
    let composition_source = read_source(&composition_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", composition_path.display()));
    let context_path = root.join("src/storage/context");
    let context_source = read_rust_module_tree(&context_path);
    let notification_adapter_path = root.join("src/storage/postgres/notifications.rs");
    let notification_adapter_source =
        read_source(&notification_adapter_path).unwrap_or_else(|error| {
            panic!(
                "could not read {}: {error}",
                notification_adapter_path.display()
            )
        });

    let contract_body = contract_source
        .split_once("pub trait StorageBackend:")
        .and_then(|(_, remainder)| remainder.split_once("\n{"))
        .map(|(body, _)| body)
        .expect("StorageBackend should have a readable aggregate trait declaration");
    for required in REQUIRED_STORAGE_BACKEND_TRAITS {
        assert!(
            contract_body.contains(required),
            "complete storage contract is missing {required}"
        );
    }
    assert!(
        composition_source.contains("impl StorageBackend for PostgresStorage {}"),
        "PostgreSQL must explicitly opt into the complete storage contract"
    );
    assert!(
        !composition_source.contains("StorageBackend for MemoryStorageModel"),
        "the focused memory contract model must not be selectable as a full backend"
    );
    for forbidden_marker in ["WorkflowStorage", "OperationalStorage"] {
        assert!(
            !contract_source.contains(forbidden_marker),
            "complete storage certification must not rely on marker {forbidden_marker}"
        );
    }
    assert!(
        context_source.contains("assert_complete_storage_backend(&backend, backend_kind)"),
        "application composition must enforce the complete contract and backend identity"
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
        "src/exports/mod.rs",
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
    for file in [
        "src/exports/mod.rs",
        "src/services/authorization_resources.rs",
    ] {
        let path = root.join(file);
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
    for file in [
        "src/models/export_template.rs",
        "src/api/v1/handlers/export_templates.rs",
        "src/exports/mod.rs",
    ] {
        let path = root.join(file);
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
        "current_etag: Option<String>",
    ] {
        assert!(
            postgres_error_source.contains(required),
            "the PostgreSQL adapter error is missing classified field {required}"
        );
    }
}

#[test]
fn postgres_operational_queries_are_owned_by_the_adapter_crate() {
    let root = repository_root();
    for operation in [
        "authentication",
        "backup",
        "bootstrap",
        "event_delivery",
        "event_fanout",
        "event_observability",
        "event_retention",
        "identity_credentials",
        "identity_scope",
        "inventory",
        "maintenance",
        "meta",
        "metrics",
        "probe",
    ] {
        let adapter_path = root.join(format!(
            "crates/hubuum-storage-postgres/src/operations/{operation}.rs"
        ));
        let source = read_source(&adapter_path)
            .unwrap_or_else(|error| panic!("could not read {}: {error}", adapter_path.display()));
        for forbidden in ["crate::errors", "crate::models", "crate::storage::postgres"] {
            assert!(
                !source.contains(forbidden),
                "{} depends on application path {forbidden}",
                adapter_path.display()
            );
        }

        let old_path = root.join(format!("src/storage/postgres/operations/{operation}.rs"));
        if operation == "event_delivery" {
            let administration = read_source(&old_path)
                .unwrap_or_else(|error| panic!("could not read {}: {error}", old_path.display()));
            for moved_worker_operation in [
                "pub(crate) async fn claim_event_delivery_batch(",
                "pub(crate) async fn mark_event_delivery_succeeded(",
                "pub(crate) async fn mark_event_delivery_failed(",
                "pub(crate) async fn list_event_deliveries_with_total_count(",
                "pub(crate) async fn load_event_delivery(",
                "pub(crate) async fn release_event_delivery_for_retry(",
                "pub(crate) async fn mark_event_delivery_dead(",
            ] {
                assert!(
                    !administration.contains(moved_worker_operation),
                    "{} retains worker operation {moved_worker_operation}",
                    old_path.display()
                );
            }
        } else if matches!(
            operation,
            "event_fanout" | "event_retention" | "maintenance"
        ) {
            let shim = read_source(&old_path)
                .unwrap_or_else(|error| panic!("could not read {}: {error}", old_path.display()));
            assert!(
                shim.contains(&format!("hubuum_storage_postgres::operations::{operation}")),
                "the temporary {operation} shim must delegate into the adapter crate"
            );
        } else {
            assert!(
                !old_path.exists(),
                "{} must not retain an application-owned implementation",
                old_path.display()
            );
        }
    }

    let identity_scope_shim = root.join("src/storage/postgres/operations/identity.rs");
    let shim = read_source(&identity_scope_shim).unwrap_or_else(|error| {
        panic!("could not read {}: {error}", identity_scope_shim.display())
    });
    assert!(
        shim.contains("hubuum_storage_postgres::operations::identity_scope"),
        "the temporary identity-scope shim must delegate into the adapter crate"
    );
    for forbidden in ["diesel::", "crate::schema"] {
        assert!(
            !shim.contains(forbidden),
            "{} retains query implementation detail {forbidden}",
            identity_scope_shim.display()
        );
    }
}

#[test]
fn persistence_facades_do_not_reexport_internal_layers_wholesale() {
    let root = repository_root();
    let storage_path = root.join("src/storage/mod.rs");
    let storage_source = read_source(&storage_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", storage_path.display()));
    let library_path = root.join("src/lib.rs");
    let library_source = read_source(&library_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", library_path.display()));

    assert!(
        !storage_source.contains("mod capabilities"),
        "backend-neutral consumers must use traits instead of a PostgreSQL capability facade"
    );
    assert!(
        library_source.contains("#[doc(hidden)]\npub mod storage;"),
        "the internal root storage module must remain hidden from generated API documentation"
    );
}
