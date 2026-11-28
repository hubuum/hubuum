use super::*;
use diesel::OptionalExtension;
use diesel::sql_types::Integer;
use hubuum_storage_core::artifacts::ArtifactReference;

#[derive(QueryableByName)]
struct Pins {
    #[diesel(sql_type=Jsonb)]
    locations: Value,
}

pub(super) async fn read_on(
    connection: &mut PostgresConnection,
    target: StorageRecordIdentity,
    fence: ApplicationFence,
) -> Result<HistoryArchivePinnedCopies, PostgresStorageError> {
    // One MVCC statement observes the fence and every matching capture. Direct
    // access to the protected table requires the operator database role.
    let row = sql_query(
        "WITH image AS MATERIALIZED (SELECT public.hubuum_application_control_read() AS control),
              pins AS (SELECT CASE WHEN octet_length((material #> '{archive,location}')::text) <= 1024
                                   THEN material #> '{archive,location}' END AS location
                       FROM hubuum_control.history_archive_captures
                       WHERE material #>> '{archive,identity,resource}' = $1
                         AND material #>> '{archive,identity,version}' = $2
                       LIMIT $5)
         SELECT COALESCE((SELECT jsonb_agg(location) FROM pins),'[]'::jsonb) AS locations
         FROM image WHERE control->>'incarnation'=$3 AND (control->>'revision')::bigint=$4",
    )
    .bind::<Text, _>(Uuid::from_bytes(*target.resource().as_bytes()).to_string())
    .bind::<Text, _>(Uuid::from_bytes(*target.version().as_bytes()).to_string())
    .bind::<Text, _>(fence.incarnation().to_string())
    .bind::<BigInt, _>(fence.revision())
    .bind::<Integer, _>((HISTORY_ARCHIVE_MAX_CAPTURES + 1) as i32)
    .get_result::<Pins>(connection)
    .await.optional()?
    .ok_or_else(|| PostgresStorageError::conflict("Archive pin observation fence changed"))?;
    let locations: Vec<ArtifactReference> =
        serde_json::from_value(row.locations).map_err(|error| {
            PostgresStorageError::invalid_persisted_value("archive pin location", error)
        })?;
    Ok(HistoryArchivePinnedCopies::from_locations(
        target, fence, locations,
    )?)
}
