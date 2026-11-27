//! Session-local accounting of the native archive catalog, including TOAST and indexes.
use diesel::QueryableByName;
use diesel::sql_query;
use diesel::sql_types::{BigInt, Binary, Integer, Jsonb, Text, Uuid as SqlUuid};
use diesel_async::{RunQueryDsl, SimpleAsyncConnection};
use futures_util::{Stream, StreamExt};
use hubuum_storage_core::artifacts::ArtifactManifest;
use hubuum_storage_core::{HISTORY_ARCHIVE_CATALOG_BYTES, HistoryArchiveCatalog};
use serde::Serialize;
use uuid::Uuid;

use super::{json, packed};
use crate::test_support::{
    database_role_tests_enabled, integration_test_database_roles, integration_test_migration_pool,
    integration_test_pool,
};
use crate::{PostgresConnection, PostgresStorageError, with_transaction};

#[derive(QueryableByName, Serialize)]
pub struct ArchiveProviderAllocation {
    #[diesel(sql_type = BigInt)]
    destination_bytes: i64,
    #[diesel(sql_type = BigInt)]
    attempt_bytes_including_manifests_and_indexes: i64,
    #[diesel(sql_type = BigInt)]
    chunk_relation_bytes_including_toast_and_indexes: i64,
    #[diesel(sql_type = BigInt)]
    chunk_column_bytes: i64,
    #[diesel(sql_type = BigInt)]
    encoded_chunk_bytes: i64,
}

impl ArchiveProviderAllocation {
    pub fn metadata_bytes(&self) -> u64 {
        (self.destination_bytes
            + self.attempt_bytes_including_manifests_and_indexes
            + self.chunk_relation_bytes_including_toast_and_indexes
            - self.chunk_column_bytes) as u64
    }
}

async fn insert_publication(
    conn: &mut PostgresConnection,
    manifest: &ArtifactManifest,
    blocks: Vec<Vec<u8>>,
) -> Result<(), PostgresStorageError> {
    let id = manifest.identity();
    let artifact = Uuid::from_bytes(*id.artifact().as_bytes());
    let destination = Uuid::from_bytes(*id.destination().as_bytes());
    sql_query("INSERT INTO pg_temp.archive_destinations(id,name) VALUES ($1,'catalog-profile') ON CONFLICT (id) DO NOTHING")
        .bind::<SqlUuid,_>(destination).execute(conn).await?;
    sql_query("INSERT INTO pg_temp.archive_attempts
        (artifact_id,attempt_id,destination_id,object_version,state,block_bytes,max_bytes,max_blocks,max_manifest_bytes,byte_size,block_count,manifest,manifest_sha256)
        VALUES ($1,$2,$3,$4,'published',$5,67108864,1024,262144,$6,$7,$8,$9)")
        .bind::<SqlUuid,_>(artifact).bind::<SqlUuid,_>(Uuid::from_bytes(*id.attempt().as_bytes()))
        .bind::<SqlUuid,_>(destination).bind::<SqlUuid,_>(Uuid::from_bytes(*id.version().as_bytes()))
        .bind::<Integer,_>(manifest.block_bytes() as i32).bind::<BigInt,_>(manifest.byte_size() as i64)
        .bind::<Integer,_>(manifest.block_count() as i32).bind::<Binary,_>(manifest.encode())
        .bind::<Binary,_>(manifest.manifest_digest().as_slice()).execute(conn).await?;
    if blocks.len() != manifest.block_count() as usize {
        return Err(PostgresStorageError::database(
            "Archive allocation fixture block count mismatch",
        ));
    }
    for (ordinal, bytes) in blocks.into_iter().enumerate() {
        manifest.verify_block(ordinal as u32, &bytes).map_err(|_| {
            PostgresStorageError::database("Archive allocation fixture block mismatch")
        })?;
        sql_query(
            "INSERT INTO pg_temp.archive_chunks(artifact_id,ordinal,content) VALUES ($1,$2,$3)",
        )
        .bind::<SqlUuid, _>(artifact)
        .bind::<Integer, _>(ordinal as i32)
        .bind::<Binary, _>(bytes)
        .execute(conn)
        .await?;
    }
    Ok(())
}

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
    publications: impl Stream<Item = (HistoryArchiveCatalog, ArtifactManifest, Vec<Vec<u8>>)> + Send,
    expected_versions: usize,
) -> Result<
    (
        ArchiveCatalogAllocation,
        ArchiveCatalogAllocation,
        ArchiveProviderAllocation,
    ),
    PostgresStorageError,
> {
    let split_roles = database_role_tests_enabled();
    let pool = if split_roles {
        integration_test_migration_pool(1)
    } else {
        integration_test_pool(1)
    };
    with_transaction(&pool, async move |conn| {
        if split_roles {
            sql_query("SELECT set_config('role', $1, true)")
                .bind::<Text, _>(integration_test_database_roles().owner().as_str())
                .execute(conn).await?;
        }
        conn.batch_execute(
            "CREATE TEMP TABLE catalog_allocation
             (LIKE hubuum_control.history_archive_catalogs INCLUDING ALL) ON COMMIT DROP;
             CREATE TEMP TABLE catalog_packed
             (LIKE hubuum_control.history_archive_catalogs INCLUDING ALL) ON COMMIT DROP;
             CREATE TEMP TABLE archive_destinations (LIKE public.artifact_destinations INCLUDING ALL) ON COMMIT DROP;
             CREATE TEMP TABLE archive_attempts (LIKE public.artifact_attempts INCLUDING ALL) ON COMMIT DROP;
             CREATE TEMP TABLE archive_chunks (LIKE public.artifact_chunks INCLUDING ALL) ON COMMIT DROP",
        ).await?;
        let mut resources = 0;
        let mut expected_segment_bytes = 0;
        let mut publications = std::pin::pin!(publications);
        while let Some((catalog, manifest, blocks)) = publications.next().await {
            resources += 1;
            expected_segment_bytes += manifest.byte_size() as i64;
            insert_publication(conn, &manifest, blocks).await?;
            let material = json(&catalog, HISTORY_ARCHIVE_CATALOG_BYTES)?;
            sql_query("INSERT INTO pg_temp.catalog_allocation (resource, generation, material) VALUES ($1,$2,$3)")
                .bind::<SqlUuid, _>(Uuid::from_bytes(*catalog.resource().as_bytes()))
                .bind::<BigInt, _>(catalog.generation().value())
                .bind::<Jsonb, _>(material)
                .execute(conn).await?;
            let bytes = packed::encode(&catalog)?;
            if packed::decode(&bytes)? != catalog {
                return Err(PostgresStorageError::database("Packed history catalog roundtrip mismatch"));
            }
            sql_query("INSERT INTO pg_temp.catalog_packed (resource, generation, packed, version_count) VALUES ($1,$2,$3,$4)")
                .bind::<SqlUuid, _>(Uuid::from_bytes(*catalog.resource().as_bytes()))
                .bind::<BigInt, _>(catalog.generation().value())
                .bind::<Binary, _>(bytes)
                .bind::<Integer, _>(catalog.versions().len() as i32)
                .execute(conn).await?;
        }
        conn.batch_execute("ANALYZE pg_temp.catalog_allocation; ANALYZE pg_temp.catalog_packed;
            ANALYZE pg_temp.archive_destinations; ANALYZE pg_temp.archive_attempts; ANALYZE pg_temp.archive_chunks").await?;
        let legacy = sql_query(
            "SELECT count(*)::bigint AS resources, coalesce(sum(jsonb_array_length(material->'versions')),0)::bigint AS versions,
             pg_table_size('pg_temp.catalog_allocation') AS table_bytes_including_toast,
             pg_indexes_size('pg_temp.catalog_allocation') AS index_bytes FROM pg_temp.catalog_allocation"
        ).get_result::<ArchiveCatalogAllocation>(conn).await?;
        let packed = sql_query(
            "SELECT count(*)::bigint AS resources, coalesce(sum(version_count),0)::bigint AS versions,
             pg_table_size('pg_temp.catalog_packed') AS table_bytes_including_toast,
             pg_indexes_size('pg_temp.catalog_packed') AS index_bytes FROM pg_temp.catalog_packed"
        ).get_result::<ArchiveCatalogAllocation>(conn).await?;
        let provider = sql_query(
            "SELECT pg_total_relation_size('pg_temp.archive_destinations') AS destination_bytes,
             pg_total_relation_size('pg_temp.archive_attempts') AS attempt_bytes_including_manifests_and_indexes,
             pg_total_relation_size('pg_temp.archive_chunks') AS chunk_relation_bytes_including_toast_and_indexes,
             coalesce(sum(pg_column_size(content)),0)::bigint AS chunk_column_bytes,
             coalesce(sum(octet_length(content)),0)::bigint AS encoded_chunk_bytes FROM pg_temp.archive_chunks"
        ).get_result::<ArchiveProviderAllocation>(conn).await?;
        if provider.encoded_chunk_bytes != expected_segment_bytes || [&legacy, &packed].into_iter().any(|row|
            row.resources != resources || row.versions != expected_versions as i64) {
            return Err(PostgresStorageError::database("Archive allocation fixture count mismatch"));
        }
        Ok::<_, PostgresStorageError>((legacy, packed, provider))
    }).await
}
