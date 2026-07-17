use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;
#[cfg(unix)]
use std::process::{Command, Output};
#[cfg(unix)]
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(unix)]
use rstest::rstest;

#[cfg(unix)]
fn run_entrypoint(runtime_role: &str, arguments: &[&str]) -> Output {
    use std::os::unix::fs::PermissionsExt;

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let repository = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let commands = std::env::temp_dir().join(format!(
        "hubuum-entrypoint-test-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir(&commands).expect("fake command directory should be created");
    for (name, body) in [
        ("hubuum-admin", "#!/bin/sh\nprintf 'admin:%s\\n' \"$*\"\n"),
        ("hubuum-server", "#!/bin/sh\nprintf 'server:%s\\n' \"$*\"\n"),
        ("wget", "#!/bin/sh\nprintf 'wget:%s\\n' \"$*\"\n"),
    ] {
        let path = commands.join(name);
        fs::write(&path, body).expect("fake command should be written");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755))
            .expect("fake command should be executable");
    }

    let output = Command::new("/bin/sh")
        .arg(repository.join("entrypoint.sh"))
        .args(arguments)
        .env("PATH", &commands)
        .env("HUBUUM_RUNTIME_ROLE", runtime_role)
        .env_remove("HUBUUM_SKIP_MIGRATIONS")
        .output()
        .expect("entrypoint should run");
    fs::remove_dir_all(commands).expect("fake command directory should be removed");
    output
}

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
    let entrypoint = fs::read_to_string(repository.join("entrypoint.sh"))
        .expect("repository entrypoint should be readable");

    assert!(
        dockerfile.contains("HEALTHCHECK")
            && dockerfile.contains("--container-healthcheck")
            && dockerfile.contains("/proc/1/cmdline"),
        "production Dockerfile must health-check the effective runtime role"
    );
    assert!(
        entrypoint.contains("kill -0 1"),
        "worker health must follow the supervised server process"
    );
}

#[cfg(unix)]
#[rstest]
#[case("all", "worker", "admin:--database-ready")]
#[case("worker", "all", "admin:--migrate")]
fn entrypoint_cli_runtime_role_overrides_environment_for_migrations(
    #[case] environment_role: &str,
    #[case] cli_role: &str,
    #[case] expected_admin_command: &str,
) {
    let output = run_entrypoint(
        environment_role,
        &["--runtime-role", cli_role, "--log-level", "debug"],
    );
    let stdout = String::from_utf8(output.stdout).unwrap();

    assert!(output.status.success(), "entrypoint failed: {stdout}");
    assert!(stdout.contains(expected_admin_command));
    assert!(stdout.contains(&format!(
        "server:--runtime-role {cli_role} --log-level debug"
    )));
}

#[cfg(unix)]
#[rstest]
#[case("all", "worker", false)]
#[case("worker", "api", true)]
fn healthcheck_uses_cli_runtime_role_over_environment(
    #[case] environment_role: &str,
    #[case] cli_role: &str,
    #[case] expects_http_probe: bool,
) {
    let output = run_entrypoint(
        environment_role,
        &[
            "--container-healthcheck",
            "hubuum-server",
            &format!("--runtime-role={cli_role}"),
        ],
    );
    let stdout = String::from_utf8(output.stdout).unwrap();

    if expects_http_probe {
        assert!(output.status.success(), "healthcheck failed: {stdout}");
    }
    assert_eq!(stdout.contains("wget:"), expects_http_probe);
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

#[test]
fn single_host_installer_generates_redundant_http_upstreams() {
    let repository = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let installer = fs::read_to_string(repository.join("scripts/install-single-host.sh"))
        .expect("single-host installer should be readable");

    assert!(installer.contains("hubuum-api-standby:"));
    assert!(installer.contains("hubuum-web-standby:"));
    assert!(installer.contains("command: [\"--runtime-role\", \"api\"]"));
    assert!(installer.contains("health_uri /readyz"));
    assert!(installer.contains("BACKEND_BASE_URL=http://caddy:8081"));
    assert!(
        installer.contains("HUBUUM_LOGIN_RATE_LIMIT_BACKEND: ${HUBUUM_LOGIN_RATE_LIMIT_BACKEND}")
    );
}

#[test]
fn single_host_updater_never_tears_down_the_stack() {
    let repository = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let updater = fs::read_to_string(repository.join("scripts/update-single-host.sh"))
        .expect("single-host updater should be readable");
    let rollout = fs::read_to_string(repository.join("scripts/single-host-rollout.sh"))
        .expect("single-host rollout helper should be readable");

    assert!(updater.contains("hubuum_rollout false"));
    assert!(!updater.contains("systemctl restart"));
    assert!(!updater.contains("down --remove-orphans"));
    assert!(!rollout.contains("down --remove-orphans"));
}
