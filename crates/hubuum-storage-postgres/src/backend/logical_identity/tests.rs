use diesel::sql_types::{Integer, Text};
use diesel_async::SimpleAsyncConnection;
use hubuum_domain::{IdentityReference, ResourceIdentity};
use rstest::rstest;
use uuid::Uuid;

use super::*;
use crate::test_support::integration_test_pool;
use crate::{PostgresConnection, with_transaction};

#[derive(QueryableByName)]
struct Record {
    #[diesel(sql_type = Integer)]
    id: i32,
    #[diesel(sql_type = Jsonb)]
    logical_identity: serde_json::Value,
}

impl Record {
    fn evidence(&self) -> StorageIdentityEvidence {
        serde_json::from_value(self.logical_identity.clone()).unwrap()
    }

    fn identity(&self) -> StorageRecordIdentity {
        self.evidence()
            .subject()
            .unwrap()
            .record_identity()
            .unwrap()
    }
}

async fn collection(
    connection: &mut PostgresConnection,
    parent: Option<i32>,
) -> Result<Record, PostgresStorageError> {
    let name = format!("logical_identity_{}", Uuid::new_v4().simple());
    Ok(diesel::sql_query(
        "INSERT INTO public.collections(name, description, parent_collection_id)
         VALUES ($1, 'logical identity test', COALESCE($2, (SELECT id FROM public.collections WHERE parent_collection_id IS NULL)))
         RETURNING id, logical_identity",
    )
    .bind::<Text, _>(name)
    .bind::<Nullable<Integer>, _>(parent)
    .get_result(connection)
    .await?)
}

#[rstest]
#[case::update(false)]
#[case::delete(true)]
#[tokio::test]
async fn closing_history_preserves_version_identity(#[case] delete: bool) {
    let pool = integration_test_pool(1);
    with_transaction(&pool, async |connection| {
        connection.batch_execute("SAVEPOINT identity_fixture").await?;
        let created = collection(connection, None).await?;
        let sql = if delete {
            "DELETE FROM public.collections WHERE id = $1"
        } else {
            "UPDATE public.collections SET description = 'changed' WHERE id = $1"
        };
        diesel::sql_query(sql).bind::<Integer, _>(created.id).execute(connection).await?;
        let closed = diesel::sql_query(
            "SELECT id, logical_identity FROM public.collections_history WHERE id = $1 AND op = 'I' AND valid_to IS NOT NULL",
        ).bind::<Integer, _>(created.id).get_result::<Record>(connection).await?;
        assert_eq!(closed.identity(), created.identity());
        connection.batch_execute("ROLLBACK TO SAVEPOINT identity_fixture").await?;
        Ok::<_, PostgresStorageError>(())
    }).await.unwrap();
}

#[tokio::test]
async fn versions_share_resource_identity_but_have_distinct_version_identities() {
    let pool = integration_test_pool(1);
    with_transaction(&pool, async |connection| {
        connection.batch_execute("SAVEPOINT identity_fixture").await?;
        let created = collection(connection, None).await?;
        diesel::sql_query("UPDATE public.collections SET description = 'changed' WHERE id = $1")
            .bind::<Integer, _>(created.id).execute(connection).await?;
        diesel::sql_query("DELETE FROM public.collections WHERE id = $1")
            .bind::<Integer, _>(created.id).execute(connection).await?;
        let versions = diesel::sql_query("SELECT id, logical_identity FROM public.collections_history WHERE id = $1 ORDER BY history_id")
            .bind::<Integer, _>(created.id).load::<Record>(connection).await?;
        let identities = versions.iter().map(Record::identity).collect::<Vec<_>>();
        let unique_versions = identities.iter().map(|identity| identity.version()).collect::<std::collections::BTreeSet<_>>();
        assert_eq!((identities.len(), unique_versions.len(), identities.iter().all(|identity| identity.resource() == created.identity().resource())), (3, 3, true));
        connection.batch_execute("ROLLBACK TO SAVEPOINT identity_fixture").await?;
        Ok::<_, PostgresStorageError>(())
    }).await.unwrap();
}

#[tokio::test]
async fn moving_collection_keeps_the_historical_parent_identity() {
    let pool = integration_test_pool(1);
    with_transaction(&pool, async |connection| {
        connection.batch_execute("SAVEPOINT identity_fixture").await?;
        let original = collection(connection, None).await?;
        let destination = collection(connection, None).await?;
        let child = collection(connection, Some(original.id)).await?;
        let moved = diesel::sql_query("UPDATE public.collections SET parent_collection_id = $2 WHERE id = $1 RETURNING id, logical_identity")
            .bind::<Integer, _>(child.id).bind::<Integer, _>(destination.id).get_result::<Record>(connection).await?;
        let history = diesel::sql_query("SELECT id, logical_identity FROM public.collections_history WHERE id = $1 AND op = 'I'")
            .bind::<Integer, _>(child.id).get_result::<Record>(connection).await?;
        let parent = |row: &Record| -> ResourceIdentity {
            match row.evidence().references()["/parent_collection_id"] {
                StorageCapturedIdentity::Resource { reference: IdentityReference::Known { identity }, .. } => identity,
                _ => panic!("native parent identity must be captured"),
            }
        };
        assert_eq!((parent(&history), parent(&moved)), (original.identity().resource(), destination.identity().resource()));
        connection.batch_execute("ROLLBACK TO SAVEPOINT identity_fixture").await?;
        Ok::<_, PostgresStorageError>(())
    }).await.unwrap();
}

#[tokio::test]
async fn ordinary_updates_cannot_replace_logical_identity() {
    let pool = integration_test_pool(1);
    with_transaction(&pool, async |connection| {
        connection
            .batch_execute("SAVEPOINT identity_fixture")
            .await?;
        let created = collection(connection, None).await?;
        let error = diesel::sql_query(
            "UPDATE public.collections SET logical_identity = '{}' WHERE id = $1",
        )
        .bind::<Integer, _>(created.id)
        .execute(connection)
        .await
        .unwrap_err();
        connection
            .batch_execute("ROLLBACK TO SAVEPOINT identity_fixture")
            .await?;
        assert!(
            error
                .to_string()
                .contains("Logical identity evidence cannot be supplied")
        );
        Ok::<_, PostgresStorageError>(())
    })
    .await
    .unwrap();
}
