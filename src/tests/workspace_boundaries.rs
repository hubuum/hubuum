use std::fs;
use std::path::PathBuf;

const FORBIDDEN_DEPENDENCY_PATTERNS: &[&str] = &["hubuum", "actix*", "diesel*"];

fn dependency_matches_pattern(dependency: &str, pattern: &str) -> bool {
    pattern
        .strip_suffix('*')
        .map_or(dependency == pattern, |prefix| {
            dependency.starts_with(prefix)
        })
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

        for section in ["dependencies", "build-dependencies"] {
            let Some(dependencies) = manifest.get(section).and_then(toml::Value::as_table) else {
                continue;
            };

            for dependency in dependencies.keys() {
                let is_app_dependency = FORBIDDEN_DEPENDENCY_PATTERNS
                    .iter()
                    .any(|pattern| dependency_matches_pattern(dependency, pattern));
                assert!(
                    !is_app_dependency,
                    "workspace crate {member} must remain app-neutral and cannot depend on {dependency}"
                );
            }
        }
    }
}
