//! Portable artifact replay uses fresh attempts and independent destination reads.
use crate::{PostgresConnection, PostgresStorageError};
use chrono::{DateTime, Utc};
use diesel::sql_types::{BigInt, Binary, Bool, Integer, Jsonb, Timestamptz, Uuid as SqlUuid};
use diesel::{OptionalExtension, QueryableByName};
use diesel_async::RunQueryDsl;
use hubuum_storage_core::artifacts::{
    ArtifactBackupFrame, ArtifactIdentity, ArtifactLimits, ArtifactManifest, ArtifactReference,
    BackupArtifactDecoder, BackupArtifactHeader, BackupArtifactPart, DestinationId,
    ManifestBuilder,
};
use hubuum_storage_core::{
    StorageBackupCaptureProgress, StorageBackupHistorySection, StorageBackupReplay,
    StorageBackupReplayBuilder,
};
use serde_json::Value;
use uuid::Uuid;

fn invalid(error: impl std::fmt::Display) -> PostgresStorageError {
    PostgresStorageError::invalid_input(format!("Backup artifact: {error}"))
}
#[derive(QueryableByName)]
struct HeaderRow {
    #[diesel(sql_type = Integer)]
    task_id: i32,
    #[diesel(sql_type = Timestamptz)]
    expires_at: DateTime<Utc>,
    #[diesel(sql_type = Jsonb)]
    reference: Value,
    #[diesel(sql_type = Binary)]
    manifest: Vec<u8>,
}
#[derive(QueryableByName)]
struct BlockRow {
    #[diesel(sql_type = Binary)]
    content: Vec<u8>,
}
#[derive(QueryableByName)]
struct CapacityAvailable {
    #[diesel(sql_type = Bool)]
    available: bool,
}
fn retain(
    frame: ArtifactBackupFrame,
    builder: &mut StorageBackupReplayBuilder,
    progress: &mut StorageBackupCaptureProgress,
) -> Result<(), PostgresStorageError> {
    progress.scan_row()?;
    let row = frame.into_row()?;
    progress.retain_row(&row)?;
    builder.push(row)?;
    Ok(())
}
pub(super) async fn capture(
    conn: &mut PostgresConnection,
    builder: &mut StorageBackupReplayBuilder,
    progress: &mut StorageBackupCaptureProgress,
) -> Result<(), PostgresStorageError> {
    diesel::sql_query("DECLARE hubuum_backup_artifacts NO SCROLL CURSOR WITHOUT HOLD FOR SELECT o.task_id,o.output_expires_at AT TIME ZONE 'UTC' AS expires_at,o.artifact_reference AS reference,a.manifest FROM export_task_outputs o JOIN tasks t ON t.id=o.task_id JOIN artifact_attempts a ON a.artifact_id=(o.artifact_reference #>> '{identity,artifact}')::UUID WHERE t.status IN ('succeeded','partially_succeeded','failed','cancelled') AND a.state='published' ORDER BY o.id")
        .execute(conn).await?;
    loop {
        crate::runtime::task_execution_checkpoint()?;
        let Some(row) = diesel::sql_query("FETCH FORWARD 1 FROM hubuum_backup_artifacts")
            .get_result::<HeaderRow>(conn)
            .await
            .optional()?
        else {
            break;
        };
        let reference: ArtifactReference =
            serde_json::from_value(row.reference).map_err(invalid)?;
        let manifest = ArtifactManifest::decode(
            &row.manifest,
            reference.identity(),
            ArtifactLimits::format_ceiling(),
        )
        .map_err(invalid)?;
        if &manifest.manifest_digest() != reference.manifest_digest() {
            return Err(invalid("published manifest digest differs"));
        }
        let mut verifier = manifest.verifier();
        retain(
            ArtifactBackupFrame::header(
                hubuum_domain::TaskId::new(row.task_id).map_err(invalid)?,
                row.expires_at,
                reference.clone(),
                &manifest,
            ),
            builder,
            progress,
        )?;
        let artifact = Uuid::from_bytes(*reference.identity().artifact().as_bytes());
        for ordinal in 0..manifest.block_count() {
            crate::runtime::task_execution_checkpoint()?;
            let block = diesel::sql_query(
                "SELECT content FROM artifact_chunks WHERE artifact_id=$1 AND ordinal=$2",
            )
            .bind::<SqlUuid, _>(artifact)
            .bind::<Integer, _>(ordinal as i32)
            .get_result::<BlockRow>(conn)
            .await?;
            verifier.push(&block.content).map_err(invalid)?;
            retain(
                ArtifactBackupFrame::block(
                    reference.identity().artifact(),
                    ordinal,
                    &block.content,
                ),
                builder,
                progress,
            )?;
        }
        verifier.finish().map_err(invalid)?;
    }
    diesel::sql_query("CLOSE hubuum_backup_artifacts")
        .execute(conn)
        .await?;
    Ok(())
}

struct Restoring {
    header: BackupArtifactHeader,
    identity: ArtifactIdentity,
    builder: ManifestBuilder,
}
impl Restoring {
    async fn start(
        conn: &mut PostgresConnection,
        header: BackupArtifactHeader,
    ) -> Result<Self, PostgresStorageError> {
        // Restore creates a second physical copy. Reserve it before writing any
        // blocks, retaining the old copy's charge until independent cleanup.
        diesel::sql_query("SELECT public.hubuum_task_capacity_lock()")
            .execute(conn)
            .await?;
        let capacity = diesel::sql_query("SELECT COALESCE((SELECT sum(reserved_bytes::NUMERIC) FROM artifact_task_owners),0) + COALESCE((SELECT sum(resident_input_bytes::NUMERIC+resident_output_bytes) FROM hubuum_control.task_accounting WHERE NOT internal),0) + $1 <= (policy #>> '{deployment,resident_bytes}')::NUMERIC AS available FROM hubuum_control.task_policy WHERE singleton")
            .bind::<BigInt, _>(header.manifest().byte_size() as i64)
            .get_result::<CapacityAvailable>(conn).await?;
        if !capacity.available {
            return Err(PostgresStorageError::conflict(
                "Restored artifact exceeds deployment resident capacity; finish cleanup or raise the shared limit",
            ));
        }
        let destination = DestinationId::from_bytes([0x50; 16]).map_err(invalid)?;
        let identity = ArtifactIdentity::new(destination);
        let id = Uuid::from_bytes(*identity.artifact().as_bytes());
        let limits = header.limits();
        diesel::sql_query("INSERT INTO artifact_destinations(id,name) VALUES($1,'primary-artifacts') ON CONFLICT(id) DO NOTHING")
            .bind::<SqlUuid,_>(Uuid::from_bytes(*destination.as_bytes())).execute(conn).await?;
        diesel::sql_query("INSERT INTO artifact_attempts(artifact_id,attempt_id,destination_id,object_version,block_bytes,max_bytes,max_blocks,max_manifest_bytes) VALUES($1,$2,$3,$4,$5,$6,$7,$8)")
            .bind::<SqlUuid,_>(id).bind::<SqlUuid,_>(Uuid::from_bytes(*identity.attempt().as_bytes()))
            .bind::<SqlUuid,_>(Uuid::from_bytes(*destination.as_bytes())).bind::<SqlUuid,_>(Uuid::from_bytes(*identity.version().as_bytes()))
            .bind::<Integer,_>(limits.block_bytes() as i32).bind::<BigInt,_>(limits.artifact_bytes() as i64)
            .bind::<Integer,_>(limits.blocks() as i32).bind::<Integer,_>(limits.manifest_bytes() as i32).execute(conn).await?;
        diesel::sql_query("INSERT INTO artifact_task_owners(artifact_id,task_identity,task_id,lease_token,worker_identity,spool_bytes,reserved_bytes,output_expires_at) SELECT $1,(logical_identity #>> '{subject,identity,identity}')::UUID,id,$3,$4,0,$5,$6 FROM tasks WHERE id=$2")
            .bind::<SqlUuid,_>(id).bind::<Integer,_>(header.task_id().id()).bind::<SqlUuid,_>(Uuid::new_v4()).bind::<SqlUuid,_>(Uuid::new_v4())
            .bind::<BigInt,_>(limits.artifact_bytes() as i64).bind::<Timestamptz,_>(header.expires_at()).execute(conn).await?;
        let builder = ManifestBuilder::new(identity.clone(), limits);
        Ok(Self {
            header,
            identity,
            builder,
        })
    }
    async fn push(
        &mut self,
        conn: &mut PostgresConnection,
        ordinal: u32,
        bytes: &[u8],
    ) -> Result<(), PostgresStorageError> {
        self.builder.push(bytes).map_err(invalid)?;
        diesel::sql_query(
            "INSERT INTO artifact_chunks(artifact_id,ordinal,content) VALUES($1,$2,$3)",
        )
        .bind::<SqlUuid, _>(Uuid::from_bytes(*self.identity.artifact().as_bytes()))
        .bind::<Integer, _>(ordinal as i32)
        .bind::<Binary, _>(bytes)
        .execute(conn)
        .await?;
        Ok(())
    }
    async fn finish(self, conn: &mut PostgresConnection) -> Result<(), PostgresStorageError> {
        let manifest = self.builder.finish().map_err(invalid)?;
        let id = Uuid::from_bytes(*self.identity.artifact().as_bytes());
        diesel::sql_query("UPDATE artifact_attempts SET state='sealed',manifest=$2 WHERE artifact_id=$1 AND byte_size=$3 AND block_count=$4")
            .bind::<SqlUuid,_>(id).bind::<Binary,_>(manifest.encode()).bind::<BigInt,_>(manifest.byte_size() as i64).bind::<Integer,_>(manifest.block_count() as i32).execute(conn).await?;
        // Verify persisted blocks independently of the upload/source decoder.
        let mut verifier = manifest.verifier();
        for ordinal in 0..manifest.block_count() {
            let stored = diesel::sql_query(
                "SELECT content FROM artifact_chunks WHERE artifact_id=$1 AND ordinal=$2",
            )
            .bind::<SqlUuid, _>(id)
            .bind::<Integer, _>(ordinal as i32)
            .get_result::<BlockRow>(conn)
            .await?;
            verifier.push(&stored.content).map_err(invalid)?;
        }
        let proof = verifier.finish().map_err(invalid)?;
        let reference = crate::artifacts::publish_on(conn, &proof).await?;
        diesel::sql_query(
            "INSERT INTO hubuum_restored_artifacts(source_reference,new_reference) VALUES($1,$2)",
        )
        .bind::<Jsonb, _>(serde_json::to_value(self.header.reference()).map_err(invalid)?)
        .bind::<Jsonb, _>(serde_json::to_value(reference).map_err(invalid)?)
        .execute(conn)
        .await?;
        Ok(())
    }
}
pub(super) async fn restore(
    conn: &mut PostgresConnection,
    snapshot: &StorageBackupReplay,
) -> Result<(), PostgresStorageError> {
    diesel::sql_query("CREATE TEMP TABLE hubuum_restored_artifacts(source_reference JSONB PRIMARY KEY,new_reference JSONB NOT NULL) ON COMMIT DROP").execute(conn).await?;
    let mut decoder = BackupArtifactDecoder::default();
    let mut current: Option<Restoring> = None;
    for row in snapshot.history_rows(StorageBackupHistorySection::ExportArtifacts) {
        crate::runtime::task_execution_checkpoint()?;
        match decoder.push(row?)? {
            BackupArtifactPart::Header(header) => {
                if let Some(previous) = current.take() {
                    previous.finish(conn).await?;
                }
                current = Some(Restoring::start(conn, *header).await?);
            }
            BackupArtifactPart::Block { ordinal, bytes } => {
                current
                    .as_mut()
                    .ok_or_else(|| invalid("missing restore header"))?
                    .push(conn, ordinal, &bytes)
                    .await?;
            }
        }
    }
    decoder.finish()?;
    if let Some(previous) = current {
        previous.finish(conn).await?;
    }
    Ok(())
}
#[derive(QueryableByName)]
struct ReferenceRow {
    #[diesel(sql_type = Jsonb)]
    reference: Value,
}
pub(super) async fn remap_output(
    conn: &mut PostgresConnection,
    mut row: Value,
) -> Result<Value, PostgresStorageError> {
    if let Some(reference) = row
        .get("artifact_reference")
        .filter(|reference| !reference.is_null())
    {
        let mapped = diesel::sql_query("SELECT new_reference AS reference FROM hubuum_restored_artifacts WHERE source_reference=$1")
            .bind::<Jsonb,_>(reference).get_result::<ReferenceRow>(conn).await?;
        row["artifact_reference"] = mapped.reference;
    }
    Ok(row)
}
