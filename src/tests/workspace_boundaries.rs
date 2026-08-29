use std::fs;
use std::path::Path;
use std::path::PathBuf;

const FORBIDDEN_DEPENDENCY_PATTERNS: &[&str] = &["hubuum", "actix*", "diesel*"];
const DEPENDENCY_SECTIONS: &[&str] = &["dependencies", "build-dependencies"];

fn dependency_matches_pattern(dependency: &str, pattern: &str) -> bool {
    pattern
        .strip_suffix('*')
        .map_or(dependency == pattern, |prefix| {
            dependency.starts_with(prefix)
        })
}

fn dependency_tables(manifest: &toml::Value) -> Vec<&toml::Table> {
    let mut tables = DEPENDENCY_SECTIONS
        .iter()
        .filter_map(|section| manifest.get(section).and_then(toml::Value::as_table))
        .collect::<Vec<_>>();

    if let Some(targets) = manifest.get("target").and_then(toml::Value::as_table) {
        for target in targets.values() {
            tables.extend(
                DEPENDENCY_SECTIONS
                    .iter()
                    .filter_map(|section| target.get(section).and_then(toml::Value::as_table)),
            );
        }
    }

    tables
}

fn dependency_package_name(
    alias: &str,
    dependency: &toml::Value,
    workspace_dependencies: Option<&toml::Table>,
) -> String {
    let dependency = if dependency
        .get("workspace")
        .and_then(toml::Value::as_bool)
        .unwrap_or(false)
    {
        workspace_dependencies
            .and_then(|dependencies| dependencies.get(alias))
            .unwrap_or(dependency)
    } else {
        dependency
    };

    dependency
        .get("package")
        .and_then(toml::Value::as_str)
        .unwrap_or(alias)
        .to_string()
}

fn forbidden_dependencies(
    manifest: &toml::Value,
    workspace_dependencies: Option<&toml::Table>,
) -> Vec<(String, String)> {
    dependency_tables(manifest)
        .into_iter()
        .flat_map(toml::Table::iter)
        .filter_map(|(alias, dependency)| {
            let package = dependency_package_name(alias, dependency, workspace_dependencies);
            FORBIDDEN_DEPENDENCY_PATTERNS
                .iter()
                .any(|pattern| dependency_matches_pattern(&package, pattern))
                .then(|| (alias.clone(), package))
        })
        .collect()
}

fn rust_sources(directory: &Path) -> Vec<(PathBuf, String)> {
    let mut sources = Vec::new();
    let mut pending = vec![directory.to_path_buf()];

    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(&directory)
            .unwrap_or_else(|error| panic!("could not read {}: {error}", directory.display()))
        {
            let path = entry.expect("directory entry should be readable").path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                let source = fs::read_to_string(&path)
                    .unwrap_or_else(|error| panic!("could not read {}: {error}", path.display()));
                sources.push((path, source));
            }
        }
    }

    sources
}

#[test]
fn workspace_crate_manifests_stay_app_neutral() {
    let repository = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_manifest = fs::read_to_string(repository.join("Cargo.toml"))
        .expect("workspace Cargo.toml should be readable");
    let workspace_manifest = toml::from_str::<toml::Value>(&workspace_manifest)
        .expect("workspace Cargo.toml should be valid");
    let members = workspace_manifest
        .get("workspace")
        .and_then(|workspace| workspace.get("members"))
        .and_then(toml::Value::as_array)
        .expect("Cargo.toml should declare workspace.members");
    let workspace_dependencies = workspace_manifest
        .get("workspace")
        .and_then(|workspace| workspace.get("dependencies"))
        .and_then(toml::Value::as_table);

    for member in members {
        let member = member
            .as_str()
            .expect("workspace member paths should be strings");
        let manifest_path = repository.join(member).join("Cargo.toml");
        let manifest = fs::read_to_string(&manifest_path).unwrap_or_else(|error| {
            panic!("{} should be readable: {error}", manifest_path.display())
        });
        let manifest = toml::from_str::<toml::Value>(&manifest)
            .unwrap_or_else(|error| panic!("{} should be valid: {error}", manifest_path.display()));

        let mut forbidden = forbidden_dependencies(&manifest, workspace_dependencies);
        if member == "crates/hubuum-storage-postgres" {
            forbidden.retain(|(_, package)| !package.starts_with("diesel"));
        }

        if let Some((alias, package)) = forbidden.into_iter().next() {
            panic!(
                "workspace crate {member} must remain app-neutral and cannot depend on {package} (declared as {alias})"
            );
        }
    }
}

#[test]
fn domain_and_storage_contract_sources_stay_backend_and_transport_neutral() {
    let repository = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut violations = Vec::new();

    for member in ["hubuum-domain", "hubuum-storage-core"] {
        for (path, source) in rust_sources(&repository.join("crates").join(member).join("src")) {
            for forbidden in [
                "use actix",
                "actix_web::",
                "diesel::",
                "diesel_async::",
                "crate::config",
                "crate::errors::ApiError",
                "PostgresPool",
            ] {
                if source.contains(forbidden) {
                    violations.push(format!("{} contains {forbidden}", path.display()));
                }
            }
        }
    }

    assert!(
        violations.is_empty(),
        "workspace boundary source leaked backend or transport details:\n{}",
        violations.join("\n")
    );
}

#[test]
fn only_the_postgres_adapter_may_depend_on_diesel() {
    let postgres_manifest = toml::from_str::<toml::Value>(
        r#"
        [dependencies]
        diesel = "2"
        diesel-async = "0.9"
        "#,
    )
    .unwrap();
    let unrelated_manifest = postgres_manifest.clone();

    let mut postgres_forbidden = forbidden_dependencies(&postgres_manifest, None);
    postgres_forbidden.retain(|(_, package)| !package.starts_with("diesel"));

    assert!(postgres_forbidden.is_empty());
    assert_eq!(forbidden_dependencies(&unrelated_manifest, None).len(), 2);
}

#[test]
fn workspace_boundary_check_resolves_renamed_dependencies() {
    let manifest = toml::from_str::<toml::Value>(
        r#"
        [dependencies]
        database = { package = "diesel", version = "2" }
        "#,
    )
    .unwrap();

    assert_eq!(
        forbidden_dependencies(&manifest, None),
        vec![("database".to_string(), "diesel".to_string())]
    );
}

#[test]
fn workspace_boundary_check_includes_target_dependencies() {
    let manifest = toml::from_str::<toml::Value>(
        r#"
        [target.'cfg(unix)'.dependencies]
        web = { package = "actix-web", version = "4" }
        "#,
    )
    .unwrap();

    assert_eq!(
        forbidden_dependencies(&manifest, None),
        vec![("web".to_string(), "actix-web".to_string())]
    );
}
