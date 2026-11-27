#![cfg(feature = "integration-test-support")]

use diesel::QueryableByName;
use diesel::sql_types::{Integer, Nullable};
use diesel_async::{RunQueryDsl, SimpleAsyncConnection};
use hubuum_storage_postgres::test_support::{
    database_role_tests_enabled, integration_test_migration_pool, integration_test_pool,
};
use hubuum_storage_postgres::{PostgresStorageError, with_transaction};
use rstest::rstest;

#[derive(QueryableByName)]
struct Revocation {
    #[diesel(sql_type = Nullable<Integer>)]
    seconds_after_issuance: Option<i32>,
}

#[rstest]
#[case::active(None, None)]
#[case::inverted(Some(-1), Some(0))]
#[case::valid(Some(1), Some(1))]
#[tokio::test]
async fn migration_repairs_only_inverted_revocations(
    #[case] original: Option<i32>,
    #[case] expected: Option<i32>,
) {
    let pool = if database_role_tests_enabled() {
        integration_test_migration_pool(1)
    } else {
        integration_test_pool(1)
    };
    let actual = with_transaction(&pool, async |connection| {
        connection
            .batch_execute(
                "CREATE TEMP TABLE tokens (issued timestamp NOT NULL, revoked_at timestamp)
                 ON COMMIT DROP; SET LOCAL search_path TO pg_temp;",
            )
            .await?;
        diesel::sql_query(
            "INSERT INTO tokens VALUES (timestamp '2026-01-01',
             timestamp '2026-01-01' + $1 * interval '1 second')",
        )
        .bind::<Nullable<Integer>, _>(original)
        .execute(connection)
        .await?;
        connection
            .batch_execute(include_str!(
                "../migrations/2026-11-27-000001_token_revocation_order/up.sql"
            ))
            .await?;
        let row = diesel::sql_query(
            "SELECT extract(epoch FROM (revoked_at - issued))::integer AS seconds_after_issuance
             FROM tokens",
        )
        .get_result::<Revocation>(connection)
        .await?;
        Ok::<_, PostgresStorageError>(row.seconds_after_issuance)
    })
    .await
    .unwrap();
    assert_eq!(actual, expected);
}
