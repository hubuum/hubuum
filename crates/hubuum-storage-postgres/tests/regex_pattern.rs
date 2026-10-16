#![cfg(feature = "integration-test-support")]

use diesel::QueryableByName;
use diesel::sql_types::{Bool, Text};
use diesel_async::RunQueryDsl;
use hubuum_regex::{Compiler, Pattern, conformance};
use hubuum_storage_memory::regex_compiler::MemoryRegexCompiler;
use hubuum_storage_postgres::regex_compiler::PostgresRegexCompiler;
use hubuum_storage_postgres::test_support::integration_test_pool;
use hubuum_storage_postgres::with_connection;

#[derive(Debug, QueryableByName)]
struct MatchRow {
    #[diesel(sql_type = Bool)]
    matched: bool,
}

#[tokio::test]
async fn portable_regex_conformance() {
    let pool = integration_test_pool(1);
    for case in conformance::cases() {
        let pattern = Pattern::new(case.pattern()).unwrap();
        let native = PostgresRegexCompiler.compile(&pattern).unwrap();
        let row = with_connection(&pool, async |connection| {
            diesel::sql_query("SELECT $1::text COLLATE \"C\" ~ $2 AS matched")
                .bind::<Text, _>(case.text())
                .bind::<Text, _>(native.as_str())
                .get_result::<MatchRow>(connection)
                .await
        })
        .await
        .unwrap_or_else(|error| panic!("{case:?}: {error}; emitted {}", native.as_str()));
        assert_eq!(row.matched, case.matches(), "{case:?}");
    }
}

#[derive(Debug, QueryableByName)]
struct TextMatchRow {
    #[diesel(sql_type = Text)]
    text: String,
    #[diesel(sql_type = Bool)]
    matched: bool,
}

#[tokio::test]
async fn generated_portable_patterns_match_memory() {
    use diesel::sql_types::Array;

    let pool = integration_test_pool(1);
    let atoms = [
        "a", "β", ".", "[^a]", "[a-z]", r"\d", "(?i:k)", "(?m:^)", "(?m:$)",
    ];
    let alphabet = ["a", "b", "β", "\n", "\r", "K", "١", "💡"];
    let mut texts = vec![String::new()];
    for first in alphabet {
        texts.push(first.to_owned());
        for second in alphabet {
            texts.push(format!("{first}{second}"));
        }
    }
    let mut sources = Vec::new();
    for first in atoms {
        for second in atoms {
            sources.push(format!("(?:{first}|{second})"));
            sources.push(format!("(?:{first}){second}"));
        }
        for quantifier in ["?", "*", "+", "{0}", "{2,3}", "{2,}"] {
            sources.push(format!("(?:{first}){quantifier}"));
        }
    }
    for source in sources {
        let pattern = Pattern::new(&source).unwrap();
        let native = PostgresRegexCompiler.compile(&pattern).unwrap();
        let memory = MemoryRegexCompiler.compile(&pattern).unwrap();
        let rows = with_connection(&pool, async |connection| {
            diesel::sql_query(
                "SELECT text, text COLLATE \"C\" ~ $1 AS matched FROM unnest($2::text[]) AS inputs(text)",
            )
            .bind::<Text, _>(native.as_str())
            .bind::<Array<Text>, _>(&texts)
            .load::<TextMatchRow>(connection)
            .await
        })
        .await
        .unwrap_or_else(|error| panic!("{source}: {error}; emitted {}", native.as_str()));
        for row in rows {
            assert_eq!(
                row.matched,
                memory.is_match(&row.text),
                "{source} against {:?}",
                row.text
            );
        }
    }
}
