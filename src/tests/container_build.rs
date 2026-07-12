use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;

#[test]
fn dockerfile_copies_every_workspace_manifest() {
    let repository = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let manifest = fs::read_to_string(repository.join("Cargo.toml"))
        .expect("repository Cargo.toml should be readable");
    let manifest = toml::from_str::<toml::Value>(&manifest)
        .expect("repository Cargo.toml should be valid TOML");
    let members = manifest
        .get("workspace")
        .and_then(|workspace| workspace.get("members"))
        .and_then(toml::Value::as_array)
        .expect("Cargo.toml should declare workspace.members");

    let expected = members
        .iter()
        .map(|member| {
            let member = member
                .as_str()
                .expect("workspace member paths should be strings");
            assert!(
                !member.contains(['*', '?', '[']),
                "Docker manifest parity requires explicit workspace member paths, got '{member}'"
            );
            format!("{member}/Cargo.toml")
        })
        .collect::<BTreeSet<_>>();

    let dockerfile = fs::read_to_string(repository.join("Dockerfile"))
        .expect("repository Dockerfile should be readable");
    let mut copied = BTreeSet::new();
    for line in dockerfile.lines().map(str::trim) {
        let Some(copy) = line.strip_prefix("COPY ") else {
            continue;
        };
        let fields = copy.split_whitespace().collect::<Vec<_>>();
        let Some(source) = fields.first().copied() else {
            continue;
        };
        if !source.starts_with("crates/") || !source.ends_with("/Cargo.toml") {
            continue;
        }
        assert_eq!(
            fields.len(),
            2,
            "workspace manifest COPY must have one source and one destination: {line}"
        );
        assert_eq!(
            fields[1],
            format!("./{source}"),
            "workspace manifest must be copied to its original relative path"
        );
        copied.insert(source.to_string());
    }

    assert_eq!(
        copied, expected,
        "Dockerfile dependency-cache manifest COPY entries must exactly match Cargo workspace members"
    );
}

#[test]
fn production_container_runs_as_non_root_with_a_healthcheck() {
    let repository = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let dockerfile = fs::read_to_string(repository.join("Dockerfile"))
        .expect("repository Dockerfile should be readable");

    assert!(
        dockerfile
            .lines()
            .any(|line| line.trim() == "USER hubuum:hubuum"),
        "production Dockerfile must select the dedicated hubuum user"
    );
    assert!(
        dockerfile.contains("HEALTHCHECK") && dockerfile.contains("/healthz"),
        "production Dockerfile must probe the unauthenticated liveness endpoint"
    );
}

#[test]
fn container_build_tool_downloads_are_pinned_and_verified() {
    let repository = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let dockerfile = fs::read_to_string(repository.join("Dockerfile"))
        .expect("repository Dockerfile should be readable");

    assert!(dockerfile.contains("CARGO_BINSTALL_VERSION=\"1.20.1\""));
    assert!(dockerfile.contains("sha256sum --check --strict"));
    assert!(dockerfile.contains("DIESEL_CLI_VERSION=\"2.3.11\""));
    assert!(dockerfile.contains("diesel_cli@${DIESEL_CLI_VERSION}"));
    assert!(
        !dockerfile.contains("cargo-bins/cargo-binstall/main/install-from-binstall-release.sh"),
        "Docker builds must not execute the mutable cargo-binstall bootstrap script"
    );
}

#[test]
fn development_compose_requires_a_local_password_and_loopback_database_port() {
    let repository = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let compose = fs::read_to_string(repository.join("docker-compose.yml"))
        .expect("repository docker-compose.yml should be readable");

    assert!(compose.contains("${POSTGRES_PASSWORD:?"));
    assert!(compose.contains("127.0.0.1:9998:5432"));
    assert!(!compose.contains("hubuum_password"));
}
