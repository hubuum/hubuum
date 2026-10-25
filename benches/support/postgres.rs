use diesel::{Connection, PgConnection};
use hubuum_storage_postgres::{PostgresPool, PostgresPoolSettings, build_postgres_pool};
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Duration;
use tokio::runtime::{Builder, Runtime};

static NEXT_NAME_ID: AtomicU64 = AtomicU64::new(1);

const POSTGRES_DATABASE: &str = "hubuum_bench";
const POSTGRES_IMAGE: &str = "docker.io/library/postgres:18.6-alpine3.24@sha256:77f585114c32fbca283dc835b0596f4e52b51b4c6662d7810b2f4084f60a1873";

pub fn benchmark_pool(database_url: &str) -> PostgresPool {
    let settings = PostgresPoolSettings::builder(database_url)
        .max_size(4)
        .statement_timeout_ms(0)
        .acquire_timeout_ms(30_000)
        .build()
        .expect("benchmark pool settings must be valid");
    build_postgres_pool(&settings).expect("benchmark pool must be constructible")
}

pub fn unique_name(prefix: &str) -> String {
    let id = NEXT_NAME_ID.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{}-{id}", std::process::id())
}

fn command_diagnostics(output: &Output) -> String {
    format!(
        "status: {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    )
}

pub struct PostgresBenchmarkDatabase {
    container_name: String,
    database_url: String,
}

impl PostgresBenchmarkDatabase {
    pub fn start() -> Self {
        let container_name = unique_name("hubuum-storage-benchmark");
        let output = Command::new("docker")
            .args(["run", "--detach", "--rm", "--name"])
            .arg(&container_name)
            .args([
                "--env",
                "POSTGRES_PASSWORD=postgres",
                "--env",
                "POSTGRES_DB=hubuum_bench",
                "--publish",
                "127.0.0.1::5432",
                POSTGRES_IMAGE,
                "postgres",
                "-c",
                "autovacuum=off",
                "-c",
                "checkpoint_timeout=30min",
            ])
            .output()
            .expect("Docker must be installed to run the PostgreSQL benchmark");
        assert!(
            output.status.success(),
            "PostgreSQL benchmark container should start:\n{}",
            command_diagnostics(&output),
        );

        let mut database = Self {
            container_name,
            database_url: String::new(),
        };
        let port = database.wait_for_port();
        database.database_url =
            format!("postgres://postgres:postgres@127.0.0.1:{port}/{POSTGRES_DATABASE}");
        database.wait_until_ready();
        hubuum_storage_postgres::run_embedded_migrations(&database.database_url)
            .expect("benchmark database migrations should succeed");
        database
    }

    fn wait_for_port(&self) -> u16 {
        for _ in 0..80 {
            let output = Command::new("docker")
                .args(["port", &self.container_name, "5432/tcp"])
                .output()
                .expect("Docker should inspect the PostgreSQL benchmark port");
            if output.status.success()
                && let Some(port) = String::from_utf8_lossy(&output.stdout)
                    .lines()
                    .find_map(|line| line.rsplit_once(':')?.1.trim().parse().ok())
            {
                return port;
            }
            thread::sleep(Duration::from_millis(250));
        }
        panic!("Docker did not publish the PostgreSQL benchmark port");
    }

    fn wait_until_container_ready(&self) {
        for _ in 0..120 {
            let status = Command::new("docker")
                .args([
                    "exec",
                    &self.container_name,
                    "pg_isready",
                    "--username",
                    "postgres",
                    "--dbname",
                    POSTGRES_DATABASE,
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .expect("Docker should check PostgreSQL benchmark readiness");
            if status.success() {
                return;
            }
            thread::sleep(Duration::from_millis(250));
        }

        let logs = Command::new("docker")
            .args(["logs", &self.container_name])
            .output()
            .expect("Docker should read PostgreSQL benchmark logs");
        panic!(
            "PostgreSQL benchmark container did not become ready:\n{}",
            command_diagnostics(&logs),
        );
    }

    fn wait_until_ready(&self) {
        self.wait_until_container_ready();

        let mut last_error = None;
        for _ in 0..120 {
            match PgConnection::establish(&self.database_url) {
                Ok(_) => return,
                Err(error) => last_error = Some(error),
            }
            thread::sleep(Duration::from_millis(250));
        }

        let logs = Command::new("docker")
            .args(["logs", &self.container_name])
            .output()
            .expect("Docker should read PostgreSQL benchmark logs");
        panic!(
            "PostgreSQL benchmark container was not reachable from the host: {}\n{}",
            last_error.expect("at least one host connection attempt should fail"),
            command_diagnostics(&logs),
        );
    }

    pub fn url(&self) -> &str {
        &self.database_url
    }
}

impl Drop for PostgresBenchmarkDatabase {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "--force", &self.container_name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

pub fn runtime() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("benchmark runtime should build")
}
