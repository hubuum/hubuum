use super::*;
use diesel::OptionalExtension;

#[derive(QueryableByName)]
struct Retained {
    #[diesel(sql_type=Binary)]
    manifest: Vec<u8>,
    #[diesel(sql_type=Nullable<Binary>)]
    verified_manifest_digest: Option<Vec<u8>>,
    #[diesel(sql_type=Binary)]
    encoded_intent: Vec<u8>,
    #[diesel(sql_type=Jsonb)]
    capture: Value,
    #[diesel(sql_type=Jsonb)]
    isolation: Value,
    #[diesel(sql_type=SqlUuid)]
    parent_token: Uuid,
    #[diesel(sql_type=SqlUuid)]
    child_token: Uuid,
}

pub(super) async fn claim_on(
    connection: &mut PostgresConnection,
    request: &HistoryArchiveCleanupRequest,
) -> Result<HistoryArchiveCleanupClaim, PostgresStorageError> {
    sql_query("SELECT id FROM hubuum_control.deletion_authority WHERE id=1 FOR SHARE")
        .execute(connection)
        .await?;
    sql_query("SELECT id FROM hubuum_control.application_control WHERE id=1 FOR UPDATE")
        .execute(connection)
        .await?;
    let application = crate::backend::deletion_control::read_application_on(connection).await?;
    if application.fence() != request.fence() || application.released_revision().is_some() {
        return Err(PostgresStorageError::unavailable(
            "Offline archive cleanup requires the same quarantined image",
        ));
    }

    // Productive admission takes the application gate first. Exact retirement
    // also takes parent/child locks. Fail promptly on a contending settlement.
    let row = sql_query(
        "SELECT a.material AS manifest, a.verified_manifest_digest, a.encoded_intent, h.material AS capture, p.isolation,
            p.ownership_token AS parent_token, c.ownership_token AS child_token
         FROM hubuum_control.history_archive_publication_claims a
         JOIN hubuum_control.history_publication_owners p
           ON p.id=a.publication_id AND p.ownership_token=a.publication_ownership
         JOIN hubuum_control.history_publication_producers c
           ON c.publication_id=p.id AND c.publication_ownership=p.ownership_token
           AND c.id=a.producer_id AND c.ownership_token=a.producer_ownership
         JOIN hubuum_control.history_archive_captures h ON h.id=a.capture_id
         WHERE p.id=$1 AND p.kind='archive_read' AND p.producer_assigned AND p.producer_count=1
           AND NOT c.retired AND c.publication_claimed AND h.publication_claimed
           AND p.isolation IS NOT NULL AND p.incarnation<>$2
           AND p.runtime_id=(h.material->>'runtime')::uuid
           AND p.incarnation=(h.material #>> '{fence,incarnation}')::uuid
           AND p.deletion_revision=(h.material #>> '{fence,revision}')::bigint
           AND c.id=(h.material #>> '{execution,child}')::uuid
           AND p.id=(h.material #>> '{execution,parent}')::uuid
           AND p.history_revision=(h.material #>> '{execution,history,authority_revision}')::bigint
           AND p.generation=(h.material #>> '{execution,generation}')::bigint
           AND NOT EXISTS (SELECT 1 FROM hubuum_control.runtime_registrations)
           AND NOT EXISTS (SELECT 1 FROM hubuum_control.history_archive_publication_claims other
                           WHERE other.capture_id=h.id AND other.publication_id<>p.id)
         FOR UPDATE OF p,c,a,h NOWAIT",
    )
    .bind::<SqlUuid, _>(request.publication())
    .bind::<SqlUuid, _>(request.fence().incarnation())
    .get_result::<Retained>(connection)
    .await
    .optional()?
    .ok_or_else(|| {
        PostgresStorageError::unavailable(
            "Original isolated archive claim or capture pin is unavailable",
        )
    })?;
    let invalid =
        || PostgresStorageError::unavailable("Original archive recovery metadata is invalid");
    let domain: RuntimeIsolationDomain =
        serde_json::from_value(row.isolation).map_err(|_| invalid())?;
    if !domain.same_lock(request.isolation()) {
        return Err(invalid());
    }
    let manifest: HistoryArchivePublicationManifest =
        serde_json::from_slice(&row.manifest).map_err(|_| invalid())?;
    if history_archive_encoded_bytes(&manifest, HISTORY_ARCHIVE_PUBLICATION_MANIFEST_BYTES)?
        != row.manifest
    {
        return Err(invalid());
    }
    let capture = serde_json::from_value(row.capture).map_err(|_| invalid())?;
    Ok(HistoryArchiveCleanupClaim::from_retained(
        request,
        manifest,
        &capture,
        row.encoded_intent,
        [row.parent_token, row.child_token],
        row.verified_manifest_digest
            .map(|bytes| bytes.try_into().map_err(|_| invalid()))
            .transpose()?,
    )?)
}

pub(super) async fn settle_on(
    connection: &mut PostgresConnection,
    settlement: HistoryArchiveCleanupSettlement,
) -> Result<(), PostgresStorageError> {
    let expected = settlement.claim();
    if claim_on(connection, expected.request()).await? != *expected {
        return Err(PostgresStorageError::conflict(
            "Retained archive claim changed before cleanup settlement",
        ));
    }
    let id = expected.request().publication();
    for (query, key) in [
        (
            "DELETE FROM hubuum_control.history_archive_publication_claims WHERE publication_id=$1",
            id,
        ),
        (
            "DELETE FROM hubuum_control.history_publication_producers WHERE publication_id=$1",
            id,
        ),
        (
            "DELETE FROM hubuum_control.history_publication_owners WHERE id=$1",
            id,
        ),
        (
            "UPDATE hubuum_control.history_archive_captures SET publication_claimed=FALSE WHERE id=$1 AND publication_claimed",
            expected.manifest().capture(),
        ),
    ] {
        if sql_query(query)
            .bind::<SqlUuid, _>(key)
            .execute(connection)
            .await?
            != 1
        {
            return Err(PostgresStorageError::conflict(
                "Archive cleanup settlement lost original ownership",
            ));
        }
    }
    Ok(())
}
