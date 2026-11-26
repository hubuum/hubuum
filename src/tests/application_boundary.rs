//! Compile-time-adjacent guards for the application/storage boundary.

#[cfg(test)]
use std::collections::BTreeSet;
#[cfg(test)]
use std::fs;
#[cfg(test)]
use std::path::{Path, PathBuf};

#[test]
fn postgres_operation_modules_are_independent_of_the_application() {
    let operations = repository_root().join("crates/hubuum-storage-postgres/src/operations");
    for path in rust_files(&operations) {
        let source = read_source(&path).unwrap();
        for forbidden in [
            "crate::errors",
            "crate::models",
            "hubuum_storage_postgres",
            "ApiError",
            "get_config",
        ] {
            assert!(
                !source.contains(forbidden),
                "{} depends on application detail {forbidden}",
                path.display()
            );
        }
    }
}

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
        .match_indices(&marker)
        .find_map(|(index, _)| {
            let next = source[index + marker.len()..].chars().next();
            next.is_none_or(|character| !character.is_ascii_alphanumeric() && character != '_')
                .then_some(index)
        })
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
fn trait_supertraits(source: &str, trait_name: &str) -> BTreeSet<String> {
    source
        .split_once(&format!("pub trait {trait_name}:"))
        .and_then(|(_, remainder)| remainder.split_once("\n{"))
        .map(|(body, _)| body)
        .unwrap_or_else(|| panic!("{trait_name} should have a readable aggregate declaration"))
        .split('+')
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_string)
        .collect()
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
fn storage_method_contract_manifest(root: &Path) -> toml::Value {
    let path = root.join("docs/storage_boundary/method-contracts.toml");
    let source = read_source(&path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", path.display()));
    toml::from_str(&source).expect("storage method contracts should be valid TOML")
}

#[cfg(test)]
fn assert_scenario_exists(root: &Path, scenario: &str) {
    let _ = scenario_body(root, scenario);
}

#[cfg(test)]
fn scenario_body(root: &Path, scenario: &str) -> String {
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
    item_body(&source, "fn", symbol).to_string()
}

#[cfg(test)]
const STORAGE_METHOD_EFFECTS: &[&str] = &[
    "observation",
    "point_read",
    "page_read",
    "candidate_read",
    "batch_read",
    "complete_read",
    "validation",
    "audited_mutation",
    "workflow_mutation",
    "execution_scope",
    "transaction",
];

#[cfg(test)]
fn scenario_asserts_an_effect(body: &str) -> bool {
    [
        "assert!(",
        "assert_eq!(",
        "assert_ne!(",
        "matches!(",
        ".expect_err(",
    ]
    .iter()
    .any(|assertion| body.contains(assertion))
}

#[cfg(test)]
const REQUIRED_STORAGE_BACKEND_TRAITS: &[&str] = &[
    "CollectionStorage",
    "ClassStorage",
    "ObjectStorage",
    "ClassRelationStorage",
    "ObjectRelationStorage",
    "AuthenticationStorage",
    "LocalIdentityCredentialStorage",
    "IdentityScopeStorage",
    "GroupMembershipStorage",
    "ServiceAccountStorage",
    "ExternalIdentityStorage",
    "UserStorage",
    "TokenStorage",
    "AuthorizationDataStorage",
    "CatalogStorage",
    "ComputedFieldStorage",
    "SchemaEvolutionStorage",
    "QueryUsageStorage",
    "ComputedObjectStorage",
    "ObjectAggregateStorage",
    "RelationQueryStorage",
    "AuditEventStorage",
    "EventConfigurationStorage",
    "EventDeliveryAdministrationStorage",
    "EventDeliveryWorkerStorage",
    "EventFanoutStorage",
    "EventHealthStorage",
    "EventRetentionStorage",
    "HistoryStorage",
    "InventoryStorage",
    "MetricsStorage",
    "OperationalStateStorage",
    "RuntimeSettingsStorage",
    "TokenRetentionStorage",
    "UnifiedSearchStorage",
    "GroupStorage",
    "PrincipalStorage",
    "CollectionAuthorizationQueryStorage",
    "RemoteTargetStorage",
    "TaskQueueStorage",
    "TaskExecutionStorage",
    "BackupSnapshotStorage",
    "RestoreStorage",
    "ImportStorage",
    "ExportTemplateStorage",
    "ExecutionStorage",
    "TransactionStorage",
];

#[cfg(test)]
const REQUIRED_STORAGE_BACKEND_FAMILIES: &[&str] = &[
    "ResourceStorage",
    "IdentityStorage",
    "QueryStorage",
    "WorkflowStorage",
    "EventStorage",
    "OperationalStorage",
];

#[test]
fn storage_boundary_documentation_covers_the_complete_contract() {
    let root = repository_root();
    let overview_path = root.join("docs/storage_boundary.md");
    let overview = read_source(&overview_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", overview_path.display()));

    for document in [
        "contract.md",
        "capability-families.md",
        "backend-author-guide.md",
        "maintainer-guide.md",
        "testing.md",
        "transactions-and-events.md",
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

    let contract_path = root.join("docs/storage_boundary/contract.md");
    let contract = read_source(&contract_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", contract_path.display()));
    for obligation in [
        "Attribution Is Mandatory",
        "The Result Proves the Audit Write",
        "State and Durable Side Effects Are Atomic",
        "Observation Is Application-Owned and Mandatory",
        "Selection Requires Behavioral Certification",
    ] {
        assert!(
            contract.contains(obligation),
            "normative storage contract must document {obligation}"
        );
    }
    for contract_artifact in [
        "StorageMutationOutcome",
        "ImportStorage",
        "RestoreStorage",
        "StorageObserver",
        "hubuum-storage-conformance",
        "CertifiedStorageBackend",
        "crates/hubuum-storage-postgres/migrations",
    ] {
        assert!(
            contract.contains(contract_artifact),
            "normative storage contract must reference {contract_artifact}"
        );
    }

    let group_path = root.join("docs/storage_boundary/capability-families.md");
    let groups = read_source(&group_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", group_path.display()));
    for required_trait in REQUIRED_STORAGE_BACKEND_TRAITS {
        assert!(
            groups.contains(required_trait),
            "required storage trait {required_trait} is not mapped to a semantic capability group"
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
    let aggregate = trait_supertraits(&contract, "StorageBackend");
    let expected_families = REQUIRED_STORAGE_BACKEND_FAMILIES
        .iter()
        .map(|name| (*name).to_string())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        aggregate, expected_families,
        "the complete backend aggregate and family inventory must change together"
    );
    let family_source = read_source(&root.join("crates/hubuum-storage-core/src/families.rs"))
        .expect("storage family bounds should be readable");
    let family_traits = REQUIRED_STORAGE_BACKEND_FAMILIES
        .iter()
        .flat_map(|family| trait_supertraits(&family_source, family))
        .collect::<BTreeSet<_>>();
    assert_eq!(
        family_traits, expected_traits,
        "storage family bounds and semantic inventory must change together"
    );

    let mut missing_method_evidence = Vec::new();
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
        let methods = toml_string_set(entry, "methods");
        assert_eq!(
            methods,
            trait_methods(&source, trait_name),
            "trait method inventory drifted for {trait_name}"
        );

        let effects = entry
            .get("effects")
            .and_then(toml::Value::as_table)
            .unwrap_or_else(|| {
                panic!("trait {trait_name} is missing method effect classifications")
            });
        let mut classified_methods = BTreeSet::new();
        for (effect, values) in effects {
            assert!(
                STORAGE_METHOD_EFFECTS.contains(&effect.as_str()),
                "trait {trait_name} uses unknown method effect {effect}"
            );
            let values = values
                .as_array()
                .unwrap_or_else(|| panic!("trait {trait_name} effect {effect} must be an array"));
            for value in values {
                let method = value.as_str().unwrap_or_else(|| {
                    panic!("trait {trait_name} effect {effect} entries must be method names")
                });
                assert!(
                    methods.contains(method),
                    "trait {trait_name} effect {effect} names unknown method {method}"
                );
                assert!(
                    classified_methods.insert(method.to_string()),
                    "trait {trait_name} method {method} has more than one effect"
                );
            }
        }
        assert_eq!(
            classified_methods, methods,
            "trait {trait_name} must classify every method effect exactly once"
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
        let scenario_bodies = scenarios
            .iter()
            .map(|scenario| (scenario, scenario_body(&root, scenario)))
            .collect::<Vec<_>>();
        assert!(
            scenario_bodies
                .iter()
                .any(|(_, body)| scenario_asserts_an_effect(body)),
            "trait {trait_name} must name an effect-asserting semantic scenario"
        );
        for scenario in &scenarios {
            assert_scenario_exists(&root, scenario);
        }
        missing_method_evidence.extend(methods.iter().filter_map(|method| {
            let invocation = format!(".{method}(");
            (!scenario_bodies
                .iter()
                .any(|(_, body)| body.contains(&invocation)))
            .then(|| format!("{trait_name}::{method}"))
        }));
    }
    assert!(
        missing_method_evidence.is_empty(),
        "storage methods lack a named scenario that directly invokes the method and asserts effects: {missing_method_evidence:#?}"
    );

    let context_free_mutations = manifest
        .get("context_free_mutations")
        .and_then(toml::Value::as_table)
        .expect("semantic coverage must classify context-free mutations");
    let mutation_contract =
        read_source(&root.join("docs/storage_boundary/transactions-and-events.md"))
            .expect("context-free mutation contract should be readable");
    let mut classified = BTreeSet::new();
    for (classification, values) in context_free_mutations {
        let values = values.as_array().unwrap_or_else(|| {
            panic!("context-free mutation classification {classification} must be an array")
        });
        for value in values {
            let qualified = value.as_str().unwrap_or_else(|| {
                panic!("context-free mutation entries must be qualified method names")
            });
            assert!(
                classified.insert(qualified.to_string()),
                "context-free mutation {qualified} is classified more than once"
            );
            let (trait_name, method) = qualified.split_once("::").unwrap_or_else(|| {
                panic!("context-free mutation {qualified} must use Trait::method syntax")
            });
            let trait_entry = traits
                .get(trait_name)
                .and_then(toml::Value::as_table)
                .unwrap_or_else(|| {
                    panic!("context-free mutation {qualified} names an unknown trait")
                });
            assert!(
                toml_string_set(trait_entry, "methods").contains(method),
                "context-free mutation {qualified} names an unknown method"
            );
            assert!(
                mutation_contract.contains(qualified),
                "context-free mutation {qualified} is missing from the normative documentation"
            );
        }
    }

    let expected_context_free = traits
        .iter()
        .flat_map(|(trait_name, value)| {
            let effects = value
                .as_table()
                .and_then(|entry| entry.get("effects"))
                .and_then(toml::Value::as_table)
                .expect("every trait has checked effect classifications");
            ["observation", "workflow_mutation"]
                .into_iter()
                .filter_map(|effect| effects.get(effect))
                .flat_map(|values| {
                    values
                        .as_array()
                        .expect("checked effect classifications are arrays")
                })
                .map(|method| {
                    format!(
                        "{trait_name}::{}",
                        method
                            .as_str()
                            .expect("checked effect classifications contain strings")
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        classified, expected_context_free,
        "context-free writer inventory must exactly match observation and workflow-mutation effects"
    );

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

#[test]
fn storage_collection_method_contracts_match_effect_inventory() {
    const COLLECTION_EFFECTS: [&str; 4] =
        ["page_read", "candidate_read", "batch_read", "complete_read"];
    const QUERY_PROFILE_FIELDS: [&str; 10] = [
        "shape",
        "filters",
        "sorts",
        "cursor",
        "visibility",
        "count",
        "snapshot",
        "bounds",
        "invalid_error",
        "unsupported_error",
    ];
    const COLLECTION_PROFILE_FIELDS: [&str; 11] = [
        "shape",
        "ordering",
        "duplicate_inputs",
        "missing_inputs",
        "multiplicity",
        "completeness",
        "bounds",
        "visibility",
        "snapshot",
        "invalid_error",
        "unsupported_error",
    ];

    let root = repository_root();
    let semantic = storage_semantic_manifest(&root);
    let semantic_traits = semantic
        .get("traits")
        .and_then(toml::Value::as_table)
        .expect("semantic coverage should have a traits table");
    let mut expected = std::collections::BTreeMap::new();
    for (trait_name, value) in semantic_traits {
        let effects = value
            .get("effects")
            .and_then(toml::Value::as_table)
            .unwrap_or_else(|| panic!("trait {trait_name} should classify effects"));
        for effect in COLLECTION_EFFECTS {
            if let Some(methods) = effects.get(effect).and_then(toml::Value::as_array) {
                for method in methods {
                    let method = method.as_str().unwrap_or_else(|| {
                        panic!("trait {trait_name} effect {effect} should contain method names")
                    });
                    expected.insert(format!("{trait_name}::{method}"), effect.to_string());
                }
            }
        }
    }

    let contracts = storage_method_contract_manifest(&root);
    let methods = contracts
        .get("methods")
        .and_then(toml::Value::as_table)
        .expect("method contracts should have a methods table");
    assert_eq!(
        methods.keys().cloned().collect::<BTreeSet<_>>(),
        expected.keys().cloned().collect::<BTreeSet<_>>(),
        "method contracts must cover every collection-shaped storage method exactly"
    );
    let query_profiles = contracts
        .get("query_profiles")
        .and_then(toml::Value::as_table)
        .expect("method contracts should have query profiles");
    let collection_profiles = contracts
        .get("collection_profiles")
        .and_then(toml::Value::as_table)
        .expect("method contracts should have collection profiles");
    let mut used_query_profiles = BTreeSet::new();
    let mut used_collection_profiles = BTreeSet::new();

    for (qualified_method, value) in methods {
        let entry = value
            .as_table()
            .unwrap_or_else(|| panic!("method contract {qualified_method} should be a table"));
        let effect = entry
            .get("effect")
            .and_then(toml::Value::as_str)
            .unwrap_or_else(|| panic!("method contract {qualified_method} needs an effect"));
        assert_eq!(
            Some(effect),
            expected.get(qualified_method).map(String::as_str),
            "method contract effect drifted for {qualified_method}"
        );
        assert!(
            entry
                .get("carrier")
                .and_then(toml::Value::as_str)
                .is_some_and(|carrier| !carrier.trim().is_empty()),
            "method contract {qualified_method} needs its exact input carrier"
        );
        let profile_name = entry
            .get("profile")
            .and_then(toml::Value::as_str)
            .unwrap_or_else(|| panic!("method contract {qualified_method} needs a profile"));
        let (profiles, required_fields, used_profiles) = match effect {
            "page_read" | "candidate_read" => (
                query_profiles,
                QUERY_PROFILE_FIELDS.as_slice(),
                &mut used_query_profiles,
            ),
            "batch_read" | "complete_read" => (
                collection_profiles,
                COLLECTION_PROFILE_FIELDS.as_slice(),
                &mut used_collection_profiles,
            ),
            other => panic!("method contract {qualified_method} has unsupported effect {other}"),
        };
        let profile = profiles
            .get(profile_name)
            .and_then(toml::Value::as_table)
            .unwrap_or_else(|| {
                panic!("method contract {qualified_method} names missing profile {profile_name}")
            });
        used_profiles.insert(profile_name.to_string());
        for field in required_fields {
            assert!(
                profile.contains_key(*field),
                "profile {profile_name} used by {qualified_method} is missing {field}"
            );
        }
        for field in ["invalid_error", "unsupported_error"] {
            assert_eq!(
                profile.get(field).and_then(toml::Value::as_str),
                Some("InvalidInput"),
                "profile {profile_name} must map {field} to the portable InvalidInput error"
            );
        }
        let expected_shape = match effect {
            "page_read" => "page",
            "candidate_read" => "candidate",
            "batch_read" => "batch",
            "complete_read" => "complete",
            _ => unreachable!(),
        };
        assert_eq!(
            profile.get("shape").and_then(toml::Value::as_str),
            Some(expected_shape),
            "profile {profile_name} has the wrong result shape for {qualified_method}"
        );
    }

    assert_eq!(
        used_query_profiles,
        query_profiles.keys().cloned().collect(),
        "query profiles must all be used"
    );
    assert_eq!(
        used_collection_profiles,
        collection_profiles.keys().cloned().collect(),
        "collection profiles must all be used"
    );
}

#[test]
fn storage_contract_struct_representations_remain_private() {
    let source_dir = repository_root().join("crates/hubuum-storage-core/src");
    for path in rust_files(&source_dir) {
        let source = read_source(&path)
            .unwrap_or_else(|error| panic!("could not read {}: {error}", path.display()));
        for (index, line) in source.lines().enumerate() {
            let Some((field, _)) = line.trim_start().strip_prefix("pub ").and_then(|line| {
                line.split_once(':').filter(|(field, _)| {
                    field
                        .chars()
                        .all(|character| character.is_ascii_alphanumeric() || character == '_')
                })
            }) else {
                continue;
            };
            panic!(
                "storage contract field {field} must remain private at {}:{}",
                path.display(),
                index + 1
            );
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
fn is_test_module(path: &Path) -> bool {
    path.file_name().is_some_and(|name| name == "tests.rs")
        || path
            .components()
            .any(|component| component.as_os_str() == "tests")
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
        "hubuum_storage_postgres",
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
    let factory = read_source(&root.join("src/storage/factory.rs")).unwrap();
    for test_only_compatibility in [
        "#[cfg(any(test, feature = \"integration-test-support\"))]\nimpl private::BackendAccess for PostgresPool",
        "#[cfg(any(test, feature = \"integration-test-support\"))]\nimpl StorageContext for PostgresPool",
    ] {
        assert!(
            factory.contains(test_only_compatibility),
            "concrete-pool context compatibility must remain test-only"
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

#[test]
fn actix_http2_stays_disabled_until_its_h2_dependency_is_fixed() {
    let manifest_path = repository_root().join("Cargo.toml");
    let source = read_source(&manifest_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", manifest_path.display()));
    let manifest = toml::from_str::<toml::Value>(&source).expect("root manifest should be valid");
    let dependencies = manifest
        .get("dependencies")
        .and_then(toml::Value::as_table)
        .expect("root manifest should have dependencies");

    for dependency in ["actix-web", "actix-http"] {
        let settings = dependencies
            .get(dependency)
            .and_then(toml::Value::as_table)
            .unwrap_or_else(|| panic!("{dependency} should use explicit dependency settings"));
        assert_eq!(
            settings
                .get("default-features")
                .and_then(toml::Value::as_bool),
            Some(false),
            "{dependency} default features would re-enable vulnerable h2 0.3"
        );
        assert!(
            settings
                .get("features")
                .and_then(toml::Value::as_array)
                .is_none_or(|features| features
                    .iter()
                    .all(|feature| feature.as_str() != Some("http2"))),
            "{dependency} must not enable HTTP/2 while Actix depends on vulnerable h2 0.3"
        );
    }

    // TLS features previously enabled HTTP/2 indirectly. Check the resolved
    // dependency graph as well as the direct feature selection above.
    let lockfile_path = repository_root().join("Cargo.lock");
    let lockfile_source = read_source(&lockfile_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", lockfile_path.display()));
    let lockfile =
        toml::from_str::<toml::Value>(&lockfile_source).expect("lockfile should be valid");
    let packages = lockfile
        .get("package")
        .and_then(toml::Value::as_array)
        .expect("lockfile should have packages");
    assert!(
        packages
            .iter()
            .all(|package| package.get("name").and_then(toml::Value::as_str) != Some("h2")),
        "h2 must remain absent, including dependencies enabled indirectly by TLS features"
    );
}

#[cfg(test)]
mod composition;
#[cfg(test)]
mod postgres;
