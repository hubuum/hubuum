//! Native catalog persistence. Old rows retain their logical JSON representation.
use diesel::sql_types::{BigInt, Binary, Bool, Integer, Jsonb, Nullable, Text, Uuid as SqlUuid};
use diesel::{OptionalExtension, QueryableByName, sql_query};
use diesel_async::RunQueryDsl;
use hubuum_domain::ResourceIdentity;
use hubuum_storage_core::{
    HISTORY_ARCHIVE_CATALOG_BYTES, HistoryArchiveCatalog, HistoryArchiveCatalogGeneration,
    StorageExecutionPermit,
};
use uuid::Uuid;

use super::{Allowed, decode, json, packed};
use crate::{PostgresConnection, PostgresStorageError};

#[derive(QueryableByName)]
struct CatalogRow {
    #[diesel(sql_type = Nullable<Text>)]
    material: Option<String>,
    #[diesel(sql_type = Nullable<Binary>)]
    packed: Option<Vec<u8>>,
}

pub(super) async fn load(
    connection: &mut PostgresConnection,
    resource: ResourceIdentity,
    locked: bool,
    permit: &StorageExecutionPermit,
) -> Result<HistoryArchiveCatalog, PostgresStorageError> {
    let row = fenced!(
        sql_query("SELECT material::text, packed FROM public.hubuum_history_archive_catalog_fetch($1,$2,$3,$4,$5,$6)")
            .bind::<SqlUuid,_>(Uuid::from_bytes(*resource.as_bytes()))
            .bind::<Bool,_>(locked),
        permit
    ).get_result::<CatalogRow>(connection).await.optional()?;
    let Some(row) = row else {
        return Ok(HistoryArchiveCatalog::empty(resource));
    };
    let catalog: HistoryArchiveCatalog = match (row.material, row.packed) {
        (Some(raw), None) => decode(&raw)?,
        (None, Some(bytes)) => packed::decode(&bytes)?,
        _ => {
            return Err(PostgresStorageError::database(
                "Invalid history catalog representation",
            ));
        }
    };
    if catalog.resource() != resource {
        return Err(PostgresStorageError::database("Catalog resource mismatch"));
    }
    Ok(catalog)
}

pub(super) async fn store(
    connection: &mut PostgresConnection,
    catalog: &HistoryArchiveCatalog,
    expected: HistoryArchiveCatalogGeneration,
    permit: &StorageExecutionPermit,
) -> Result<(), PostgresStorageError> {
    let encoded = packed::encode(catalog)?;
    // Preserve acceptance of a valid legacy catalog if its packed representation
    // exceeds the independent physical row ceiling.
    let (raw, bytes, count) = if encoded.len() > HISTORY_ARCHIVE_CATALOG_BYTES {
        (
            Some(json(catalog, HISTORY_ARCHIVE_CATALOG_BYTES)?),
            None,
            None,
        )
    } else {
        (None, Some(encoded), Some(catalog.versions().len() as i32))
    };
    let saved = fenced!(
        sql_query("SELECT public.hubuum_history_archive_catalog_store($1,$2,$3,$4,$5,$6,$7,$8,$9) AS allowed")
            .bind::<SqlUuid,_>(Uuid::from_bytes(*catalog.resource().as_bytes()))
            .bind::<BigInt,_>(expected.value())
            .bind::<Nullable<Jsonb>,_>(raw)
            .bind::<Nullable<Binary>,_>(bytes)
            .bind::<Nullable<Integer>,_>(count),
        permit
    ).get_result::<Allowed>(connection).await?;
    if !saved.allowed {
        return Err(PostgresStorageError::conflict("Catalog generation changed"));
    }
    Ok(())
}

#[cfg(all(test, feature = "integration-test-support"))]
mod tests;
