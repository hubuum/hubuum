use super::*;
use diesel::OptionalExtension;
use diesel::sql_types::Integer;
use hubuum_storage_core::artifacts::ArtifactReference;

#[derive(QueryableByName)]
struct Pins {
    #[diesel(sql_type=Jsonb)]
    locations: Value,
    #[diesel(sql_type=Jsonb)]
    reclaimed: Value,
}

pub(super) async fn read_on(
    connection: &mut PostgresConnection,
    target: StorageRecordIdentity,
    fence: ApplicationFence,
) -> Result<HistoryArchiveCopyHistory, PostgresStorageError> {
    // One MVCC statement observes the fence and every matching capture. Direct
    // access to the protected table requires the operator database role.
    let row = sql_query(
        "WITH image AS MATERIALIZED (SELECT public.hubuum_application_control_read() AS control),
              pins AS (SELECT CASE WHEN octet_length((material #> '{archive,location}')::text) <= 1024
                                   THEN material #> '{archive,location}' END AS location
                       FROM hubuum_control.history_archive_captures
                       WHERE material #>> '{archive,identity,resource}' = $1
                         AND material #>> '{archive,identity,version}' = $2
                       LIMIT $5),
              reclaimed AS (
                  SELECT artifact, attempt, destination, object_version, manifest_digest
                  FROM hubuum_control.history_archive_reclaimed_pins
                  WHERE resource=CAST($1::text AS uuid) AND version=CAST($2::text AS uuid) LIMIT $5
              )
         SELECT COALESCE((SELECT jsonb_agg(location) FROM pins),'[]'::jsonb) AS locations,
                COALESCE((SELECT jsonb_agg(jsonb_build_object(
                    'identity', jsonb_build_object('artifact',artifact,'attempt',attempt,
                        'destination',destination,'version',object_version),
                    'manifest_digest', ARRAY(SELECT get_byte(manifest_digest,i) FROM generate_series(0,31) i)
                )) FROM reclaimed),'[]'::jsonb) AS reclaimed
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
    let locations = |value| {
        serde_json::from_value::<Vec<ArtifactReference>>(value).map_err(|error| {
            PostgresStorageError::invalid_persisted_value("archive pin location", error)
        })
    };
    Ok(HistoryArchiveCopyHistory::from_sources(
        target,
        fence,
        locations(row.locations)?,
        locations(row.reclaimed)?,
    )?)
}

pub(super) async fn record_displacements(
    connection: &mut PostgresConnection,
    publication: &HistoryArchiveCatalogUpdate,
    permit: &StorageExecutionPermit,
) -> Result<(), PostgresStorageError> {
    let catalog = publication.catalog();
    let recorded = fenced!(
        sql_query("SELECT public.hubuum_history_archive_record_displacements($1,$2,$3,$4,$5,$6,$7) AS allowed")
            .bind::<SqlUuid,_>(Uuid::from_bytes(*catalog.resource().as_bytes()))
            .bind::<BigInt,_>(catalog.generation().value())
            .bind::<Jsonb,_>(json(&publication.displaced(), HISTORY_ARCHIVE_BATCH_BYTES)?),
        permit
    ).get_result::<Allowed>(connection).await?;
    if !recorded.allowed {
        return Err(PostgresStorageError::conflict(
            "Archive catalog displacement generation changed",
        ));
    }
    Ok(())
}
