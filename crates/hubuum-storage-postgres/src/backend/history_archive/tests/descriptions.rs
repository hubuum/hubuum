use crate::schema::{collection_description_digest_history as observations, collections_history};
use diesel::result::Error;
use diesel::{ExpressionMethods, OptionalExtension, QueryDsl, delete, update};
use diesel_async::AsyncConnection;
use sha2::{Digest, Sha256};

use super::*;

#[rstest]
#[case::new_large(None, Some(("δ\n", 400_000)))]
#[case::small_to_large(Some(8), Some(("\"\\", 524_288)))]
#[case::large_to_large(Some(1_048_576), Some(("β", 650_000)))]
#[case::large_to_small(Some(1_048_576), Some(("z", 128)))]
#[case::large_to_oversize(Some(1_048_576), Some(("z", 4 * 1024 * 1024 + 1)))]
#[case::source_deleted(Some(1_048_576), None)]
#[tokio::test]
async fn derived_description_matches_current_history(
    #[case] initial_bytes: Option<usize>,
    #[case] final_value: Option<(&str, usize)>,
) {
    let pool = if database_role_tests_enabled() {
        integration_test_migration_pool(1)
    } else {
        integration_test_pool(1)
    };
    let name = format!("archive-description-{}", Uuid::new_v4());
    let final_description = final_value.map(|(fragment, count)| fragment.repeat(count));
    let initial_description = initial_bytes.map(|n| "a".repeat(n));
    let inserted = initial_description
        .as_ref()
        .or(final_description.as_ref())
        .unwrap();
    let mut conn = pool.get().await.unwrap();
    let row = conn
        .test_transaction::<_, Error, _>(async |conn| {
            if database_role_tests_enabled() {
                sql_query("SELECT set_config('role', $1, true)")
                    .bind::<Text, _>(integration_test_database_roles().owner().as_str())
                    .execute(conn)
                    .await?;
            }
            sql_query(
                "INSERT INTO public.collections(name,description,parent_collection_id)
             SELECT $1,$2,id FROM public.collections WHERE parent_collection_id IS NULL",
            )
            .bind::<Text, _>(&name)
            .bind::<Text, _>(inserted)
            .execute(conn)
            .await?;
            let id = collections_history::table
                .filter(collections_history::name.eq(&name))
                .select(collections_history::history_id)
                .get_result::<i64>(conn)
                .await?;
            if initial_bytes.is_some() {
                let source = collections_history::table.find(id);
                if let Some(description) = &final_description {
                    update(source)
                        .set(collections_history::description.eq(description))
                        .execute(conn)
                        .await?;
                } else {
                    delete(source).execute(conn).await?;
                }
            }
            observations::table
                .find(id)
                .select((observations::digest, observations::encoded_bytes))
                .first::<(Vec<u8>, i32)>(conn)
                .await
                .optional()
        })
        .await;
    let expected = final_description
        .filter(|s| (1_048_576..=4_194_304).contains(&s.len()))
        .map(|s| {
            (
                Sha256::digest(s.as_bytes()).to_vec(),
                serde_json::to_vec(&s).unwrap().len() - 2,
            )
        });
    assert_eq!(
        row.map(|(digest, bytes)| (digest, usize::try_from(bytes).unwrap())),
        expected
    );
}

#[tokio::test]
async fn runtime_can_only_read_derived_descriptions() {
    if !database_role_tests_enabled() {
        return;
    }
    let pool = integration_test_pool(1);
    let mut conn = pool.get().await.unwrap();
    let row = sql_query("SELECT has_table_privilege(current_user, oid, 'SELECT')
                         AND NOT has_table_privilege(current_user, oid, 'INSERT, UPDATE, DELETE, TRUNCATE, TRIGGER') AS allowed
                         FROM pg_class WHERE oid='public.collection_description_digest_history'::regclass")
        .get_result::<Allowed>(&mut conn).await.unwrap();
    assert!(row.allowed);
}
