use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
#[cfg(unix)]
use std::process::{Command, Output};
#[cfg(unix)]
use std::sync::atomic::{AtomicU64, Ordering};

use rstest::rstest;

fn read_repository_text(relative_path: &str) -> String {
    let repository = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    fs::read_to_string(repository.join(relative_path))
        .unwrap_or_else(|error| panic!("{relative_path} should be readable: {error}"))
        .replace("\r\n", "\n")
}

fn lines_between<'a>(text: &'a str, start: &str, end: &str) -> Option<Vec<&'a str>> {
    let mut lines = text.lines();
    lines.find(|line| *line == start)?;

    let mut section = Vec::new();
    for line in lines {
        if line == end {
            return Some(section);
        }
        section.push(line);
    }
    None
}

#[rstest]
#[case("\n")]
#[case("\r\n")]
fn lines_between_handles_platform_line_endings(#[case] line_ending: &str) {
    let workflow = [
        "jobs:",
        "  benchmarks:",
        "    with:",
        "      auto_discover: true",
        "  runtime-behavior:",
    ]
    .join(line_ending);

    assert_eq!(
        lines_between(&workflow, "  benchmarks:", "  runtime-behavior:"),
        Some(vec!["    with:", "      auto_discover: true"])
    );
}

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
    assert!(
        dockerfile
            .lines()
            .any(|line| line.trim() == "RUN chmod 0755 /entrypoint.sh"),
        "production entrypoint must be readable by the dedicated hubuum user"
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
#[case("all", "worker")]
#[case("worker", "all")]
fn entrypoint_uses_only_the_readiness_probe_before_starting_the_server(
    #[case] environment_role: &str,
    #[case] cli_role: &str,
) {
    let output = run_entrypoint(
        environment_role,
        &["--runtime-role", cli_role, "--log-level", "debug"],
    );
    let stdout = String::from_utf8(output.stdout).unwrap();

    assert!(output.status.success(), "entrypoint failed: {stdout}");
    assert!(stdout.contains("admin:--database-ready"));
    assert!(!stdout.contains("admin:--migrate"));
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
fn production_container_refreshes_runtime_packages() {
    let dockerfile = read_repository_text("Dockerfile");
    let (_, runtime_stage) = dockerfile
        .rsplit_once("\nFROM ")
        .expect("production Dockerfile should contain a runtime stage");

    assert!(
        runtime_stage.contains("\nRUN apk upgrade --no-cache && \\\n"),
        "production runtime stage must upgrade packages from the pinned Alpine repositories"
    );
}

#[test]
fn container_dependency_images_are_pinned() {
    let repository = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workflow = fs::read_to_string(repository.join(".github/workflows/ci.yml"))
        .expect("CI workflow should be readable");
    let benchmark_workflow =
        fs::read_to_string(repository.join(".github/workflows/benchmarks.yml"))
            .expect("benchmark workflow should be readable");
    let installer = read_repository_text("scripts/install-single-host.sh");

    assert!(workflow.contains("postgres:18.4@sha256:"));
    assert!(benchmark_workflow.contains("postgres:18.4-alpine3.24@sha256:"));
    assert!(installer.contains("postgres:18.4-alpine3.24@sha256:"));
}

#[test]
fn benchmark_action_autodiscovers_every_cargo_benchmark() {
    let repository = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workflow = fs::read_to_string(repository.join(".github/workflows/benchmarks.yml"))
        .expect("benchmark workflow should be readable");
    let benchmark_job = lines_between(&workflow, "  benchmarks:", "  runtime-behavior:")
        .expect("benchmark workflow should contain a bounded benchmark job");
    assert!(
        benchmark_job
            .iter()
            .any(|line| line.contains("auto_discover: true"))
    );
    assert!(
        !benchmark_job
            .iter()
            .any(|line| line.contains("benchmarks_json:"))
    );
    assert!(
        !benchmark_job
            .iter()
            .any(|line| line.contains("\"features\":\"postgres-bench\"")),
        "root-only benchmark features must not be applied to workspace members"
    );

    let manifest = read_repository_text("Cargo.toml");
    let manifest: toml::Value =
        toml::from_str(&manifest).expect("root Cargo manifest should be valid TOML");
    let benchmarks = manifest
        .get("bench")
        .and_then(toml::Value::as_array)
        .expect("root Cargo manifest should declare benchmarks");
    assert!(!benchmarks.is_empty());

    let postgres_benchmark = benchmarks
        .iter()
        .find(|benchmark| {
            benchmark.get("name").and_then(toml::Value::as_str)
                == Some("storage_postgres_criterion")
        })
        .expect("root Cargo manifest should declare the PostgreSQL benchmark");
    let required_features = postgres_benchmark
        .get("required-features")
        .and_then(toml::Value::as_array)
        .expect("PostgreSQL benchmark should declare required features");
    assert!(
        required_features
            .iter()
            .any(|feature| feature.as_str() == Some("postgres-bench")),
        "PostgreSQL benchmark should own its feature requirement"
    );

    for benchmark in benchmarks {
        let name = benchmark
            .get("name")
            .and_then(toml::Value::as_str)
            .expect("benchmark should have a name");
        let path = benchmark
            .get("path")
            .and_then(toml::Value::as_str)
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(format!("benches/{name}.rs")));
        let path = Path::new(&path);
        assert!(
            path.parent() == Some(Path::new("benches")),
            "benchmark '{name}' must be a direct child of benches/ for action auto-discovery"
        );
        assert!(
            repository.join(path).is_file(),
            "benchmark '{name}' path '{}' should exist",
            path.display()
        );
    }
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
fn compose_database_roles_default_to_single_with_split_opt_in() {
    let compose = read_repository_text("docker-compose.yml");
    let initializer = read_repository_text("docker/postgres/init-database-roles.sh");

    assert!(compose.contains("HUBUUM_DATABASE_ROLE_MODE: ${HUBUUM_DATABASE_ROLE_MODE:-single}"));
    assert!(compose.contains("HUBUUM_MIGRATION_DATABASE_URL: ${HUBUUM_MIGRATION_DATABASE_URL:-}"));
    assert!(!compose.contains(
        "HUBUUM_MIGRATION_DATABASE_URL: ${HUBUUM_MIGRATION_DATABASE_URL:-${HUBUUM_DATABASE_URL"
    ));
    assert!(
        compose
            .contains("HUBUUM_DATABASE_PRIVILEGE_MODE: ${HUBUUM_DATABASE_PRIVILEGE_MODE:-strict}")
    );
    assert!(compose.contains("POSTGRES_MIGRATOR_PASSWORD: ${POSTGRES_MIGRATOR_PASSWORD:-}"));
    assert!(compose.contains("POSTGRES_RUNTIME_PASSWORD: ${POSTGRES_RUNTIME_PASSWORD:-}"));
    assert!(initializer.contains("role_mode=\"${HUBUUM_DATABASE_ROLE_MODE:-single}\""));
    assert!(initializer.contains("split)"));
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
fn compose_deployments_forward_page_limit_configuration() {
    let repository = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let compose = fs::read_to_string(repository.join("docker-compose.yml"))
        .expect("repository docker-compose.yml should be readable");
    let installer = read_repository_text("scripts/install-single-host.sh");

    for variable in ["HUBUUM_DEFAULT_PAGE_LIMIT", "HUBUUM_MAX_PAGE_LIMIT"] {
        assert!(compose.contains(variable));
        assert!(installer.contains(&format!("{variable}: ${{{variable}}}")));
    }
}

#[test]
fn single_host_installer_limits_api_container_privileges() {
    let installer = read_repository_text("scripts/install-single-host.sh");

    assert!(installer.contains("read_only: true"));
    assert!(installer.contains("cap_drop:"));
    assert!(installer.contains("no-new-privileges:true"));
}

#[test]
fn single_host_installer_generates_redundant_http_upstreams() {
    let installer = read_repository_text("scripts/install-single-host.sh");

    assert!(installer.contains("hubuum-api-standby:"));
    assert!(installer.contains("hubuum-web-standby:"));
    assert!(!installer.contains("    depends_on:\n      - hubuum-api\n      - hubuum-api-standby"));
    assert!(installer.contains("command: [\"--runtime-role\", \"api\"]"));
    assert!(installer.contains("reverse_proxy hubuum-api:${API_PORT}"));
    assert!(installer.contains("redir * /hubuum-api/"));
    assert!(!installer.contains("{$HUBUUM_BIND_PORT}"));
    assert!(installer.contains("BACKEND_BASE_URL=http://caddy:8081"));
    assert!(
        installer.contains("HUBUUM_LOGIN_RATE_LIMIT_BACKEND: ${HUBUUM_LOGIN_RATE_LIMIT_BACKEND}")
    );
    assert!(installer.contains("export PODMAN_COMPOSE_WARNING_LOGS=false"));
    assert!(installer.contains("Environment=PODMAN_COMPOSE_WARNING_LOGS=false"));
}

#[test]
fn single_host_api_health_checks_use_five_second_interval() {
    let installer = read_repository_text("scripts/install-single-host.sh");
    let api_proxy = installer
        .split_once("(api_proxy) {")
        .and_then(|(_, remainder)| remainder.split_once("(metrics_primary_proxy)"))
        .map(|(api_proxy, _)| api_proxy)
        .expect("installer should contain a bounded API proxy block");

    assert!(api_proxy.contains("health_uri /readyz"));
    assert!(api_proxy.contains("health_interval 5s"));
}

#[test]
fn single_host_direct_routing_uses_one_health_checked_public_api_proxy() {
    let installer = read_repository_text("scripts/install-single-host.sh");
    let direct_start = installer
        .find(r#"elif [[ "$MODE" == "all" && "$SHARED_HOST_ROUTING" == "direct" ]]; then"#)
        .expect("installer should generate direct shared-host routing");
    let direct_end = installer[direct_start..]
        .find(r#"elif [[ "$MODE" == "all" && "$SHARED_HOST_ROUTING" == "prefixed" ]]; then"#)
        .map(|offset| direct_start + offset)
        .expect("direct shared-host routing should end before prefixed routing");
    let direct_template = &installer[direct_start..direct_end];

    assert!(direct_template.contains(
        "@backend path /api/v0* /api/v1* /api-doc* /swagger-ui*\n\
         \thandle @backend {\n\
         \t\timport api_proxy\n\
         \t}"
    ));
    assert_eq!(direct_template.matches("import api_proxy").count(), 1);
}

#[test]
fn single_host_metrics_routes_target_each_backend_process() {
    let installer = read_repository_text("scripts/install-single-host.sh");

    assert!(installer.contains(
        "(metrics_primary_proxy) {\n\
                                \treverse_proxy hubuum-api:${API_PORT}\n\
                                }"
    ));
    assert!(installer.contains(
        "(metrics_standby_proxy) {\n\
                                \treverse_proxy hubuum-api-standby:${API_PORT}\n\
                                }"
    ));
    assert!(installer.contains(
        "handle /metrics/standby {\n\
                                \t\trewrite * /metrics\n\
                                \t\timport metrics_standby_proxy\n\
                                \t}"
    ));
    assert!(!installer.contains("handle /metrics {\n\t\timport api_proxy\n\t}"));
}

#[test]
fn single_host_installer_emits_canonical_caddyfile_indentation() {
    let installer = read_repository_text("scripts/install-single-host.sh");
    let template_start = installer
        .find("CADDYFILE_TEMP=")
        .expect("installer should generate a Caddyfile");
    let template_end = installer[template_start..]
        .find("# Caddy bind-mounts")
        .map(|offset| template_start + offset)
        .expect("Caddyfile generation should have a known end marker");
    let template = &installer[template_start..template_end];

    assert!(template.lines().any(|line| line.starts_with('\t')));
    assert!(!template.lines().any(|line| line.starts_with("    ")));
}

#[test]
fn single_host_installer_preserves_the_bind_mounted_caddyfile_inode() {
    let installer = read_repository_text("scripts/install-single-host.sh");

    assert!(installer.contains("cp \"$CADDYFILE_TEMP\" \"$INSTALL_DIR/Caddyfile\""));
    assert!(!installer.contains("mv \"$CADDYFILE_TEMP\" \"$INSTALL_DIR/Caddyfile\""));
}

#[test]
fn single_host_updater_never_tears_down_the_stack() {
    let repository = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let updater = read_repository_text("scripts/update-single-host.sh");
    let rollout_helper = fs::read_to_string(repository.join("scripts/single-host-rollout.sh"))
        .expect("single-host rollout helper should be readable");
    let refresh = updater
        .find("refresh_deployment_files\n")
        .expect("single-host updater should refresh generated deployment files");
    let pull = updater[refresh..]
        .find("\"${COMPOSE_CMD[@]}\" pull")
        .map(|offset| refresh + offset)
        .expect("single-host updater should pull images after refreshing configuration");
    let rollout = updater[pull..]
        .find("hubuum_rollout")
        .map(|offset| pull + offset)
        .expect("single-host updater should roll services after refreshing configuration");

    assert!(refresh < pull);
    assert!(pull < rollout);
    assert!(updater.contains("--refresh-config"));
    assert!(updater.contains("source \"$INSTALL_DIR/single-host-rollout.sh\""));
    assert!(updater.contains("export PODMAN_COMPOSE_WARNING_LOGS=false"));
    assert!(!updater.contains("systemctl restart"));
    assert!(!updater.contains("down --remove-orphans"));
    assert!(!rollout_helper.contains("down --remove-orphans"));
}

#[test]
fn container_ci_exercises_live_single_host_http_continuity() {
    let repository = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workflow = fs::read_to_string(repository.join(".github/workflows/ci.yml"))
        .expect("CI workflow should be readable");
    let live_test = repository.join("scripts/test-single-host-zero-downtime.sh");

    assert!(live_test.is_file());
    assert!(workflow.contains("bash scripts/test-single-host-zero-downtime.sh"));
}

#[test]
fn tagged_release_validation_requires_each_direct_prerequisite() {
    let workflow = read_repository_text(".github/workflows/ci.yml");
    let validation_start = workflow
        .find("\n  validate-tag-release:")
        .expect("CI should define tagged release validation");
    let validation_end = workflow[validation_start + 1..]
        .find("\n  build-tag-linux-artifacts:")
        .map(|offset| validation_start + 1 + offset)
        .expect("tagged release validation should have a known end marker");
    let validation = &workflow[validation_start..validation_end];

    assert!(validation.contains("always() &&"));
    for prerequisite in [
        "verify-tag-main-ci-success",
        "dependency-policy",
        "openapi-contract",
        "operational-contract",
        "container-build",
    ] {
        assert!(validation.contains(&format!("needs.{prerequisite}.result == 'success'")));
    }
    assert!(!validation.contains("needs.*.result"));
}

#[test]
fn tagged_release_jobs_explicitly_require_successful_dependencies() {
    let workflow = read_repository_text(".github/workflows/ci.yml");
    let jobs: [(&str, Option<&str>, &[&str]); 5] = [
        (
            "build-tag-linux-artifacts",
            Some("build-tag-native-artifacts"),
            &["validate-tag-release"],
        ),
        (
            "build-tag-native-artifacts",
            Some("publish-github-release"),
            &["validate-tag-release"],
        ),
        (
            "publish-github-release",
            Some("build-main-linux-artifacts"),
            &[
                "build-tag-linux-artifacts",
                "build-tag-native-artifacts",
                "publish-tag-container-manifests",
            ],
        ),
        (
            "publish-tag-container-images",
            Some("publish-tag-container-manifests"),
            &["validate-tag-release"],
        ),
        (
            "publish-tag-container-manifests",
            None,
            &["publish-tag-container-images"],
        ),
    ];

    for (job, next_job, dependencies) in jobs {
        let job_marker = format!("\n  {job}:");
        let job_start = workflow
            .find(&job_marker)
            .unwrap_or_else(|| panic!("CI should define {job}"));
        let job_end = next_job
            .map(|next_job| {
                let next_marker = format!("\n  {next_job}:");
                workflow[job_start + 1..]
                    .find(&next_marker)
                    .map(|offset| job_start + 1 + offset)
                    .unwrap_or_else(|| panic!("{job} should precede {next_job}"))
            })
            .unwrap_or(workflow.len());
        let job_definition = &workflow[job_start..job_end];

        assert!(job_definition.contains("always() &&"));
        assert!(job_definition.contains("startsWith(github.ref, 'refs/tags/v')"));
        for dependency in dependencies {
            assert!(
                job_definition.contains(&format!("needs.{dependency}.result == 'success'")),
                "{job} should require {dependency} to succeed"
            );
        }
    }
}

#[test]
fn tagged_release_attestors_can_persist_artifact_metadata() {
    let workflow = read_repository_text(".github/workflows/ci.yml");

    for (job, next_job) in [
        ("publish-github-release", Some("build-main-linux-artifacts")),
        (
            "publish-tag-container-images",
            Some("publish-tag-container-manifests"),
        ),
        ("publish-tag-container-manifests", None),
    ] {
        let job_marker = format!("\n  {job}:");
        let job_start = workflow
            .find(&job_marker)
            .unwrap_or_else(|| panic!("CI should define {job}"));
        let job_end = next_job
            .map(|next_job| {
                let next_marker = format!("\n  {next_job}:");
                workflow[job_start + 1..]
                    .find(&next_marker)
                    .map(|offset| job_start + 1 + offset)
                    .unwrap_or_else(|| panic!("{job} should precede {next_job}"))
            })
            .unwrap_or(workflow.len());
        let job_definition = &workflow[job_start..job_end];

        assert!(job_definition.contains("actions/attest@"));
        assert!(
            job_definition.contains("artifact-metadata: write"),
            "{job} should be able to persist artifact metadata storage records"
        );
    }
}

#[test]
fn main_builds_remain_run_artifacts_without_a_rolling_github_release() {
    let workflow = read_repository_text(".github/workflows/ci.yml");

    assert!(workflow.contains("\n  build-main-linux-artifacts:"));
    assert!(workflow.contains("\n  build-main-native-artifacts:"));
    assert!(
        workflow.contains("name: main-${{ matrix.platform.os_name }}-${{ matrix.feature.ext }}")
    );
    assert!(!workflow.contains("\n  publish-main-release:"));
    assert!(!workflow.contains("main-latest"));
}

#[test]
fn main_publication_runs_after_inapplicable_ci_jobs_are_skipped() {
    let workflow = read_repository_text(".github/workflows/ci.yml");

    for (job, next_job) in [
        ("build-main-linux-artifacts", "build-main-native-artifacts"),
        (
            "build-main-native-artifacts",
            "publish-main-container-images",
        ),
        (
            "publish-main-container-images",
            "publish-main-container-manifests",
        ),
    ] {
        let job_definition =
            lines_between(&workflow, &format!("  {job}:"), &format!("  {next_job}:"))
                .unwrap_or_else(|| panic!("CI should define {job} before {next_job}"))
                .join("\n");

        assert!(job_definition.contains("    if: >-\n      always() &&"));
        assert!(job_definition.contains("needs.changes.result == 'success'"));
        assert!(job_definition.contains("needs.ci-gate.result == 'success'"));
        assert!(job_definition.contains("needs.changes.outputs.artifacts == 'true'"));
    }

    let manifest_job = lines_between(
        &workflow,
        "  publish-main-container-manifests:",
        "  publish-tag-container-images:",
    )
    .expect("CI should define main manifest publication before tagged publication")
    .join("\n");
    assert!(manifest_job.contains("    if: >-\n      always() &&"));
    assert!(manifest_job.contains("needs.publish-main-container-images.result == 'success'"));
}

#[test]
fn main_container_manifest_is_sha_addressed_before_channel_promotion() {
    let workflow = read_repository_text(".github/workflows/ci.yml");
    let job_start = workflow
        .find("\n  publish-main-container-images:")
        .expect("CI should define main container publication");
    let job_end = workflow[job_start + 1..]
        .find("\n  publish-tag-container-images:")
        .map(|offset| job_start + 1 + offset)
        .expect("main container publication should precede tagged publication");
    let jobs = &workflow[job_start..job_end];

    assert!(jobs.contains(
        "tags: ghcr.io/${{ github.repository_owner }}/hubuum-server:sha-${{ github.sha }}-${{ matrix.platform.arch }}"
    ));
    assert!(jobs.contains("org.opencontainers.image.version=sha-${{ github.sha }}"));
    assert!(jobs.contains("sha_tag=\"${image}:sha-${GITHUB_SHA}\""));
    for tag in ["$sha_tag", "${image}:main", "${image}:main-full"] {
        assert!(
            jobs.contains(&format!("--tag \"{tag}\"")),
            "main manifest publication should create {tag}"
        );
    }
    for platform in ["amd64", "arm64"] {
        assert!(
            jobs.contains(&format!("\"${{sha_tag}}-{platform}\"")),
            "main manifest should use the SHA-addressed {platform} image"
        );
    }
}
