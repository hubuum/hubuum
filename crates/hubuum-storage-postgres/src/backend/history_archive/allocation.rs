//! Session-local accounting of the native archive catalog, including TOAST and indexes.
use diesel::QueryableByName;
use diesel::sql_query;
use diesel::sql_types::{BigInt, Jsonb, Text, Uuid as SqlUuid};
use diesel_async::{RunQueryDsl, SimpleAsyncConnection};
use hubuum_storage_core::{HISTORY_ARCHIVE_CATALOG_BYTES, HistoryArchiveCatalog};
use serde::Serialize;
use uuid::Uuid;

use super::json;
use crate::test_support::{
    database_role_tests_enabled, integration_test_database_roles, integration_test_migration_pool,
    integration_test_pool,
};
use crate::{PostgresStorageError, with_transaction};

#[derive(QueryableByName, Serialize)]
pub struct ArchiveCatalogAllocation {
    #[diesel(sql_type = BigInt)]
    resources: i64,
    #[diesel(sql_type = BigInt)]
    versions: i64,
    #[diesel(sql_type = BigInt)]
    table_bytes_including_toast: i64,
    #[diesel(sql_type = BigInt)]
    index_bytes: i64,
}

impl ArchiveCatalogAllocation {
    pub fn total_bytes(&self) -> u64 {
        (self.table_bytes_including_toast + self.index_bytes) as u64
    }
}

/// Measure the production encoding and physical table definition in a disposable table.
pub async fn measure_archive_catalog_allocation(
    catalogs: &[HistoryArchiveCatalog],
) -> Result<ArchiveCatalogAllocation, PostgresStorageError> {
    let split_roles = database_role_tests_enabled();
    let pool = if split_roles {
        integration_test_migration_pool(1)
    } else {
        integration_test_pool(1)
    };
    let measured = with_transaction(&pool, async |conn| {
        if split_roles {
            sql_query("SELECT set_config('role', $1, true)")
                .bind::<Text, _>(integration_test_database_roles().owner().as_str())
                .execute(conn).await?;
        }
        conn.batch_execute(
            "CREATE TEMP TABLE catalog_allocation
             (LIKE hubuum_control.history_archive_catalogs INCLUDING ALL) ON COMMIT DROP",
        ).await?;
        for catalog in catalogs {
            let material = json(catalog, HISTORY_ARCHIVE_CATALOG_BYTES)?;
            sql_query("INSERT INTO pg_temp.catalog_allocation (resource, generation, material) VALUES ($1,$2,$3)")
                .bind::<SqlUuid, _>(Uuid::from_bytes(*catalog.resource().as_bytes()))
                .bind::<BigInt, _>(catalog.generation().value())
                .bind::<Jsonb, _>(material)
                .execute(conn).await?;
        }
        conn.batch_execute("ANALYZE pg_temp.catalog_allocation").await?;
        Ok::<_, PostgresStorageError>(sql_query(
            "SELECT count(*)::bigint AS resources, coalesce(sum(jsonb_array_length(material->'versions')),0)::bigint AS versions,
             pg_table_size('pg_temp.catalog_allocation') AS table_bytes_including_toast,
             pg_indexes_size('pg_temp.catalog_allocation') AS index_bytes FROM pg_temp.catalog_allocation"
        ).get_result::<ArchiveCatalogAllocation>(conn).await?)
    }).await?;
    let versions: usize = catalogs
        .iter()
        .map(|catalog| catalog.versions().len())
        .sum();
    if measured.resources != catalogs.len() as i64 || measured.versions != versions as i64 {
        return Err(PostgresStorageError::database(
            "Archive catalog allocation fixture count mismatch",
        ));
    }
    Ok(measured)
}
