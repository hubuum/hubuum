use std::fs;
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

        if let Some((alias, package)) = forbidden_dependencies(&manifest, workspace_dependencies)
            .into_iter()
            .next()
        {
            panic!(
                "workspace crate {member} must remain app-neutral and cannot depend on {package} (declared as {alias})"
            );
        }
    }
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
