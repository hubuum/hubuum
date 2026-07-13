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
fn production_container_runs_as_non_root() {
    let repository = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let dockerfile = fs::read_to_string(repository.join("Dockerfile"))
        .expect("repository Dockerfile should be readable");

    assert!(
        dockerfile
            .lines()
            .any(|line| line.trim() == "USER hubuum:hubuum"),
        "production Dockerfile must select the dedicated hubuum user"
    );
}

#[test]
fn production_container_has_a_healthcheck() {
    let repository = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let dockerfile = fs::read_to_string(repository.join("Dockerfile"))
        .expect("repository Dockerfile should be readable");

    assert!(
        dockerfile.contains("HEALTHCHECK") && dockerfile.contains("/healthz"),
        "production Dockerfile must probe the unauthenticated liveness endpoint"
    );
}

#[test]
fn production_container_base_images_are_pinned() {
    let repository = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let dockerfile = fs::read_to_string(repository.join("Dockerfile"))
        .expect("repository Dockerfile should be readable");
    let postgres_dockerfile = fs::read_to_string(repository.join("docker/postgres/Dockerfile"))
        .expect("PostgreSQL Dockerfile should be readable");

    for line in dockerfile
        .lines()
        .chain(postgres_dockerfile.lines())
        .map(str::trim)
        .filter(|line| line.starts_with("FROM ") && !line.starts_with("FROM scratch"))
    {
        assert!(
            line.contains("@sha256:"),
            "container base image must be pinned by digest: {line}"
        );
    }
}

#[test]
fn container_dependency_images_are_pinned() {
    let repository = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workflow = fs::read_to_string(repository.join(".github/workflows/ci.yml"))
        .expect("CI workflow should be readable");
    let installer = fs::read_to_string(repository.join("scripts/install-single-host.sh"))
        .expect("single-host installer should be readable");

    assert!(workflow.contains("postgres:18.4@sha256:"));
    assert!(installer.contains("postgres:18.4-alpine3.24@sha256:"));
}

#[test]
fn development_compose_requires_a_local_password() {
    let repository = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let compose = fs::read_to_string(repository.join("docker-compose.yml"))
        .expect("repository docker-compose.yml should be readable");

    assert!(compose.contains("${POSTGRES_PASSWORD:?"));
    assert!(!compose.contains("hubuum_password"));
}

#[test]
fn development_compose_limits_container_privileges() {
    let repository = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let compose = fs::read_to_string(repository.join("docker-compose.yml"))
        .expect("repository docker-compose.yml should be readable");

    assert!(compose.contains("127.0.0.1:9998:5432"));
    assert!(compose.contains("read_only: true"));
    assert!(compose.contains("cap_drop:"));
    assert!(compose.contains("no-new-privileges:true"));
}

#[test]
fn single_host_installer_limits_api_container_privileges() {
    let repository = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let installer = fs::read_to_string(repository.join("scripts/install-single-host.sh"))
        .expect("single-host installer should be readable");

    assert!(installer.contains("read_only: true"));
    assert!(installer.contains("cap_drop:"));
    assert!(installer.contains("no-new-privileges:true"));
}
