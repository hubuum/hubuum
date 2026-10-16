#![no_main]

use std::time::Duration;

use hubuum_regex::{Compiler, MAX_PATTERN_BYTES, Pattern};
use hubuum_storage_memory::regex_compiler::MemoryRegexCompiler;
use hubuum_storage_postgres::regex_compiler::PostgresRegexCompiler;
use libfuzzer_sys::fuzz_target;
use tokio::runtime::{Builder, Runtime};
use tokio_postgres::config::Host;
use tokio_postgres::error::SqlState;
use tokio_postgres::{Client, Config, NoTls, Statement};

thread_local! {
    static DATABASE: Oracle = Oracle::new();
}

struct Oracle {
    runtime: Runtime,
    client: Client,
    statement: Statement,
}

impl Oracle {
    fn new() -> Self {
        // Never inherit application database configuration from .env.
        let url = std::env::var("HUBUUM_REGEX_FUZZ_DATABASE_URL")
            .expect("set HUBUUM_REGEX_FUZZ_DATABASE_URL for a disposable database");
        let mut config: Config = url.parse().expect("invalid fuzz database configuration");
        assert_eq!(config.get_dbname(), Some("hubuum_regex_fuzz"));
        assert!(!config.get_hosts().is_empty());
        assert!(config.get_hosts().iter().all(|host| {
            matches!(host, Host::Tcp(name) if matches!(name.as_str(), "localhost" | "127.0.0.1" | "::1"))
        }), "the fuzz database must use a loopback host");
        config
            .application_name("hubuum-regex-fuzz")
            .connect_timeout(Duration::from_secs(5));
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let (client, connection) = runtime
            .block_on(config.connect(NoTls))
            .expect("connect to the disposable fuzz database");
        runtime.spawn(async move {
            // Connection failures are reported by the next query below.
            let _ = connection.await;
        });
        let statement = runtime.block_on(async {
            client
                .batch_execute(
                    "SET statement_timeout = '1s'; SET default_transaction_read_only = on",
                )
                .await
                .expect("configure native execution limits");
            let encoding: String = client
                .query_one("SHOW server_encoding", &[])
                .await
                .unwrap()
                .get(0);
            assert_eq!(encoding, "UTF8");
            client
                .prepare("SELECT $1::text COLLATE \"C\" ~ $2")
                .await
                .expect("prepare the native regex oracle")
        });
        Self {
            runtime,
            client,
            statement,
        }
    }

    fn matches(&self, pattern: &str, text: &str) -> Option<bool> {
        self.runtime.block_on(async {
            match self
                .client
                .query_one(&self.statement, &[&text, &pattern])
                .await
            {
                Ok(row) => Some(row.get(0)),
                Err(error) if error.code() == Some(&SqlState::QUERY_CANCELED) => {
                    // Execution-policy exhaustion is permitted by the language.
                    // Prove the statement ended and the connection is reusable.
                    self.client
                        .simple_query("SELECT 1")
                        .await
                        .expect("the timed-out native statement must release its connection");
                    None
                }
                Err(error) => panic!("native matching of an accepted pattern failed: {error}"),
            }
        })
    }
}

fuzz_target!(|data: &[u8]| {
    let Ok(input) = std::str::from_utf8(data) else {
        return;
    };
    let Some((source, text)) = input.split_once('\0') else {
        return;
    };
    if source.len() > MAX_PATTERN_BYTES || text.len() > 1024 || text.contains('\0') {
        return;
    }
    let Ok(pattern) = Pattern::new(source) else {
        return;
    };
    let memory = MemoryRegexCompiler
        .compile(&pattern)
        .expect("memory must compile every accepted portable pattern");
    let postgres = PostgresRegexCompiler.compile(&pattern).unwrap();
    let actual = DATABASE.with(|oracle| oracle.matches(postgres.as_str(), text));
    if let Some(actual) = actual {
        assert_eq!(
            actual,
            memory.is_match(text),
            "pattern {source:?}, text {text:?}"
        );
    }
});
