//! PostgreSQL byte-store capability of the complete storage backend.
//! Task-owned publication is fenced and committed with terminal task state.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use diesel::sql_types::{
    BigInt, Binary, Bool, Integer, Nullable, Text, Timestamptz, Uuid as SqlUuid,
};
use diesel::{OptionalExtension, QueryableByName};
use diesel_async::RunQueryDsl;
use hubuum_storage_core::StorageErrorKind;
use hubuum_storage_core::artifacts::{
    ArtifactError, ArtifactIdentity, ArtifactLimits, ArtifactManifest, ArtifactReference,
    ArtifactReservation, ArtifactResult, ArtifactStore, Destination, LOCAL_SPOOL_CLEANUP_GRACE,
    TaskArtifactStore, VerifiedArtifact,
};
use uuid::Uuid;

use crate::{PostgresConnection, PostgresRuntime, PostgresStorageError};

/// Constructed only by PostgreSQL composition. Pools do not cross the immutable
/// byte-store contract, and all queries retain the configured runtime observer.
#[derive(Clone)]
pub struct PostgresArtifactStore {
    runtime: PostgresRuntime,
}

impl PostgresArtifactStore {
    pub(crate) fn new(runtime: PostgresRuntime) -> Self {
        Self { runtime }
    }
}

fn boundary(error: PostgresStorageError) -> ArtifactError {
    match error.kind() {
        StorageErrorKind::NotFound => ArtifactError::NotFound,
        StorageErrorKind::Conflict => ArtifactError::Conflict(error.to_string()),
        StorageErrorKind::InvalidInput => ArtifactError::Invalid(error.to_string()),
        StorageErrorKind::InputTooLarge => ArtifactError::Limit(error.to_string()),
        _ => ArtifactError::Backend(error.to_string()),
    }
}

fn invalid(error: impl std::fmt::Display) -> PostgresStorageError {
    PostgresStorageError::invalid_input(error.to_string())
}
fn conflict() -> PostgresStorageError {
    PostgresStorageError::conflict("Artifact attempt identity, state or ownership changed")
}
fn artifact_id(identity: &ArtifactIdentity) -> Uuid {
    Uuid::from_bytes(*identity.artifact().as_bytes())
}

#[derive(QueryableByName)]
struct Attempt {
    #[diesel(sql_type = SqlUuid)]
    attempt_id: Uuid,
    #[diesel(sql_type = SqlUuid)]
    destination_id: Uuid,
    #[diesel(sql_type = SqlUuid)]
    object_version: Uuid,
    #[diesel(sql_type = Text)]
    state: String,
    #[diesel(sql_type = Bool)]
    live: bool,
    #[diesel(sql_type = Integer)]
    block_bytes: i32,
    #[diesel(sql_type = BigInt)]
    max_bytes: i64,
    #[diesel(sql_type = Integer)]
    max_blocks: i32,
    #[diesel(sql_type = Integer)]
    max_manifest_bytes: i32,
    #[diesel(sql_type = BigInt)]
    byte_size: i64,
    #[diesel(sql_type = Integer)]
    block_count: i32,
    #[diesel(sql_type = Nullable<Binary>)]
    manifest: Option<Vec<u8>>,
    #[diesel(sql_type = Nullable<Binary>)]
    manifest_sha256: Option<Vec<u8>>,
}

impl Attempt {
    fn check(&self, identity: &ArtifactIdentity) -> Result<(), PostgresStorageError> {
        if self.attempt_id.as_bytes() != identity.attempt().as_bytes()
            || self.destination_id.as_bytes() != identity.destination().as_bytes()
            || self.object_version.as_bytes() != identity.version().as_bytes()
        {
            return Err(conflict());
        }
        Ok(())
    }
    fn writable(&self) -> Result<(), PostgresStorageError> {
        if self.state != "staging" || !self.live {
            return Err(conflict());
        }
        Ok(())
    }
    fn readable(&self) -> Result<(), PostgresStorageError> {
        if self.state != "published" && !(self.state == "sealed" && self.live) {
            return Err(conflict());
        }
        Ok(())
    }
    fn limits(&self) -> Result<ArtifactLimits, PostgresStorageError> {
        ArtifactLimits::builder()
            .block_bytes(self.block_bytes as u32)
            .artifact_bytes(self.max_bytes as u64)
            .blocks(self.max_blocks as u32)
            .manifest_bytes(self.max_manifest_bytes as u32)
            .build()
            .map_err(invalid)
    }
}

async fn load(
    connection: &mut PostgresConnection,
    identity: &ArtifactIdentity,
    lock: bool,
) -> Result<Attempt, PostgresStorageError> {
    let sql = format!(
        "SELECT attempt_id, destination_id, object_version, state, expires_at > clock_timestamp() AS live, block_bytes, max_bytes, max_blocks, max_manifest_bytes, byte_size, block_count, manifest, manifest_sha256 FROM artifact_attempts WHERE artifact_id = $1 {}",
        if lock { "FOR UPDATE" } else { "" }
    );
    let attempt = diesel::sql_query(sql)
        .bind::<SqlUuid, _>(artifact_id(identity))
        .get_result::<Attempt>(connection)
        .await?;
    attempt.check(identity)?;
    Ok(attempt)
}

#[derive(QueryableByName)]
struct Content {
    #[diesel(sql_type = Binary)]
    content: Vec<u8>,
}

#[async_trait]
impl ArtifactStore for PostgresArtifactStore {
    async fn register_destination(&self, destination: &Destination) -> ArtifactResult<()> {
        self.runtime.with_transaction(async |connection| {
            let id = Uuid::from_bytes(*destination.id().as_bytes());
            diesel::sql_query("INSERT INTO artifact_destinations(id, name) VALUES ($1, $2) ON CONFLICT DO NOTHING")
                .bind::<SqlUuid, _>(id).bind::<Text, _>(destination.name()).execute(connection).await?;
            #[derive(QueryableByName)]
            struct Name { #[diesel(sql_type = Text)] name: String }
            let name = diesel::sql_query("SELECT name FROM artifact_destinations WHERE id = $1")
                .bind::<SqlUuid, _>(id).get_result::<Name>(connection).await.optional()?;
            if name.is_none_or(|name| name.name != destination.name()) { return Err(conflict()); }
            Ok(())
        }).await.map_err(boundary)
    }

    async fn stage(
        &self,
        identity: &ArtifactIdentity,
        limits: ArtifactLimits,
    ) -> ArtifactResult<()> {
        self.runtime.with_transaction(async |connection| {
            diesel::sql_query("INSERT INTO artifact_attempts(artifact_id, attempt_id, destination_id, object_version, block_bytes, max_bytes, max_blocks, max_manifest_bytes) VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT DO NOTHING")
                .bind::<SqlUuid, _>(artifact_id(identity)).bind::<SqlUuid, _>(Uuid::from_bytes(*identity.attempt().as_bytes()))
                .bind::<SqlUuid, _>(Uuid::from_bytes(*identity.destination().as_bytes())).bind::<SqlUuid, _>(Uuid::from_bytes(*identity.version().as_bytes()))
                .bind::<Integer, _>(limits.block_bytes() as i32).bind::<BigInt, _>(limits.artifact_bytes() as i64)
                .bind::<Integer, _>(limits.blocks() as i32).bind::<Integer, _>(limits.manifest_bytes() as i32).execute(connection).await?;
            let attempt = load(connection, identity, true).await?;
            attempt.writable()?;
            if attempt.limits()? != limits { return Err(conflict()); }
            Ok(())
        }).await.map_err(boundary)
    }

    async fn append(
        &self,
        identity: &ArtifactIdentity,
        ordinal: u32,
        bytes: &[u8],
    ) -> ArtifactResult<()> {
        if bytes.is_empty()
            || bytes.len() > 1024 * 1024
            || ordinal >= ArtifactLimits::format_ceiling().blocks()
        {
            return Err(ArtifactError::Limit(
                "Artifact block exceeds version 1 bounds".into(),
            ));
        }
        self.runtime
            .with_transaction(async |connection| {
                let attempt = load(connection, identity, true).await?;
                attempt.writable()?;
                if ordinal < attempt.block_count as u32 {
                    let old = diesel::sql_query(
                        "SELECT content FROM artifact_chunks WHERE artifact_id=$1 AND ordinal=$2",
                    )
                    .bind::<SqlUuid, _>(artifact_id(identity))
                    .bind::<Integer, _>(ordinal as i32)
                    .get_result::<Content>(connection)
                    .await?;
                    return if old.content == bytes {
                        Ok(())
                    } else {
                        Err(conflict())
                    };
                }
                if ordinal != attempt.block_count as u32
                    || bytes.len() > attempt.block_bytes as usize
                    || attempt.byte_size + bytes.len() as i64 > attempt.max_bytes
                    || attempt.block_count >= attempt.max_blocks
                    || 117 + (ordinal + 1) * 36 > attempt.max_manifest_bytes as u32
                {
                    return Err(invalid(
                        "Artifact block exceeds attempt limits or is out of order",
                    ));
                }
                diesel::sql_query(
                    "INSERT INTO artifact_chunks(artifact_id, ordinal, content) VALUES ($1,$2,$3)",
                )
                .bind::<SqlUuid, _>(artifact_id(identity))
                .bind::<Integer, _>(ordinal as i32)
                .bind::<Binary, _>(bytes)
                .execute(connection)
                .await?;
                Ok(())
            })
            .await
            .map_err(boundary)
    }

    async fn seal(&self, manifest: &ArtifactManifest) -> ArtifactResult<()> {
        self.runtime
            .with_transaction(async |connection| {
                let attempt = load(connection, manifest.identity(), true).await?;
                if matches!(attempt.state.as_str(), "sealed" | "published") {
                    attempt.readable()?;
                    return if attempt.manifest.as_deref() == Some(manifest.encode().as_slice()) {
                        Ok(())
                    } else {
                        Err(conflict())
                    };
                }
                attempt.writable()?;
                let encoded = manifest.encode();
                ArtifactManifest::decode(&encoded, manifest.identity(), attempt.limits()?)
                    .map_err(invalid)?;
                if manifest.block_bytes() != attempt.block_bytes as u32
                    || manifest.byte_size() != attempt.byte_size as u64
                    || manifest.block_count() != attempt.block_count as u32
                {
                    return Err(conflict());
                }
                diesel::sql_query(
                    "UPDATE artifact_attempts SET state='sealed', manifest=$2 WHERE artifact_id=$1",
                )
                .bind::<SqlUuid, _>(artifact_id(manifest.identity()))
                .bind::<Binary, _>(encoded)
                .execute(connection)
                .await?;
                Ok(())
            })
            .await
            .map_err(boundary)
    }

    async fn manifest(&self, identity: &ArtifactIdentity) -> ArtifactResult<Vec<u8>> {
        self.runtime
            .with_connection(async |connection| {
                let attempt = load(connection, identity, false).await?;
                attempt.readable()?;
                attempt.manifest.ok_or_else(conflict)
            })
            .await
            .map_err(boundary)
    }

    async fn block(
        &self,
        identity: &ArtifactIdentity,
        ordinal: u32,
    ) -> ArtifactResult<Option<Vec<u8>>> {
        if ordinal > 1024 {
            return Err(ArtifactError::Limit(
                "Artifact block ordinal exceeds version 1 bounds".into(),
            ));
        }
        self.runtime.with_connection(async |connection| {
            // One statement prevents a cleanup race between state and byte reads.
            let row = diesel::sql_query("SELECT c.content FROM artifact_attempts a JOIN artifact_chunks c USING (artifact_id) WHERE a.artifact_id=$1 AND a.attempt_id=$2 AND a.destination_id=$3 AND a.object_version=$4 AND (a.state='published' OR (a.state='sealed' AND a.expires_at > clock_timestamp())) AND c.ordinal=$5")
                .bind::<SqlUuid, _>(artifact_id(identity)).bind::<SqlUuid, _>(Uuid::from_bytes(*identity.attempt().as_bytes()))
                .bind::<SqlUuid, _>(Uuid::from_bytes(*identity.destination().as_bytes())).bind::<SqlUuid, _>(Uuid::from_bytes(*identity.version().as_bytes()))
                .bind::<Integer, _>(ordinal as i32).get_result::<Content>(connection).await.optional()?;
            Ok::<_, PostgresStorageError>(row.map(|row| row.content))
        }).await.map_err(boundary)
    }

    async fn published_manifest(&self, reference: &ArtifactReference) -> ArtifactResult<Vec<u8>> {
        self.runtime
            .with_connection(async |connection| {
                let attempt = load(connection, reference.identity(), false).await?;
                if attempt.state != "published"
                    || attempt.manifest_sha256.as_deref()
                        != Some(reference.manifest_digest().as_slice())
                {
                    return Err(conflict());
                }
                attempt.manifest.ok_or_else(conflict)
            })
            .await
            .map_err(boundary)
    }

    async fn publish(&self, verified: &VerifiedArtifact) -> ArtifactResult<ArtifactReference> {
        self.runtime.with_transaction(async |connection| {
            #[derive(QueryableByName)]
            struct Bound { #[diesel(sql_type = Bool)] bound: bool }
            let bound = diesel::sql_query("SELECT EXISTS(SELECT 1 FROM artifact_task_owners WHERE artifact_id=$1) AS bound")
                .bind::<SqlUuid, _>(artifact_id(verified.manifest().identity())).get_result::<Bound>(connection).await?;
            if bound.bound { return Err(PostgresStorageError::conflict("Task artifacts require atomic task completion")); }
            publish_on(connection, verified).await
        }).await.map_err(boundary)
    }

    async fn abort(&self, identity: &ArtifactIdentity) -> ArtifactResult<()> {
        self.runtime
            .with_transaction(async |connection| {
                let attempt = load(connection, identity, true).await?;
                if attempt.state == "published" {
                    return Err(conflict());
                }
                if attempt.state == "discarded" {
                    return Ok(());
                }
                diesel::sql_query(
                    "UPDATE artifact_attempts SET state='deleting' WHERE artifact_id=$1",
                )
                .bind::<SqlUuid, _>(artifact_id(identity))
                .execute(connection)
                .await?;
                Ok(())
            })
            .await
            .map_err(boundary)
    }

    async fn cleanup(&self) -> ArtifactResult<u32> {
        self.runtime.with_transaction(async |connection| {
            #[derive(QueryableByName)]
            struct Candidate {
                #[diesel(sql_type = SqlUuid)] artifact_id: Uuid,
                #[diesel(sql_type = Nullable<BigInt>)] spool_bytes: Option<i64>,
                #[diesel(sql_type = Nullable<Timestamptz>)] cleanup_after: Option<DateTime<Utc>>,
            }
            let candidate = diesel::sql_query("SELECT a.artifact_id, o.spool_bytes, o.cleanup_after FROM artifact_attempts a LEFT JOIN artifact_task_owners o USING(artifact_id)
                WHERE (o.cleanup_after IS NULL OR o.cleanup_after<=clock_timestamp()) AND a.state <> 'discarded' AND (a.state='deleting'
                OR (a.state<>'published' AND a.expires_at<=clock_timestamp())
                OR (o.artifact_id IS NOT NULL AND (o.output_expires_at<=clock_timestamp()
                    OR NOT EXISTS (SELECT 1 FROM tasks t WHERE t.id=o.task_id AND t.deleted_at IS NULL
                        AND (t.logical_identity #>> '{subject,identity,identity}')::UUID=o.task_identity)
                    OR (a.state='published' AND NOT EXISTS (SELECT 1 FROM export_task_outputs e WHERE (e.artifact_reference #>> '{identity,artifact}')::UUID=a.artifact_id) AND NOT EXISTS (SELECT 1 FROM backup_task_outputs b WHERE (b.artifact_reference #>> '{identity,artifact}')::UUID=a.artifact_id))
                    OR (a.state<>'published' AND NOT EXISTS (SELECT 1 FROM tasks t WHERE t.id=o.task_id
                        AND t.lease_token=o.lease_token AND t.lease_expires_at>clock_timestamp()
                        AND t.status IN ('running','validating'))))))
                ORDER BY a.expires_at,a.artifact_id FOR UPDATE OF a SKIP LOCKED LIMIT 1")
                .get_result::<Candidate>(connection).await.optional()?;
            let Some(candidate) = candidate else { return Ok(0); };
            diesel::sql_query("UPDATE artifact_attempts SET state='deleting' WHERE artifact_id=$1")
                .bind::<SqlUuid, _>(candidate.artifact_id).execute(connection).await?;
            if candidate.spool_bytes.is_some_and(|bytes| bytes > 0) && candidate.cleanup_after.is_none() {
                diesel::sql_query("UPDATE artifact_task_owners SET cleanup_after=clock_timestamp()+$2*INTERVAL '1 millisecond' WHERE artifact_id=$1")
                    .bind::<SqlUuid, _>(candidate.artifact_id)
                    .bind::<BigInt, _>(LOCAL_SPOOL_CLEANUP_GRACE.as_millis() as i64)
                    .execute(connection).await?;
                return Ok(0);
            }
            let deleted = diesel::sql_query("DELETE FROM artifact_chunks WHERE artifact_id=$1 AND ordinal IN (SELECT ordinal FROM artifact_chunks WHERE artifact_id=$1 ORDER BY ordinal LIMIT 16)")
                .bind::<SqlUuid, _>(candidate.artifact_id).execute(connection).await?;
            diesel::sql_query("UPDATE artifact_attempts SET state='discarded', manifest=NULL WHERE artifact_id=$1 AND NOT EXISTS (SELECT 1 FROM artifact_chunks WHERE artifact_id=$1)")
                .bind::<SqlUuid, _>(candidate.artifact_id).execute(connection).await?;
            diesel::sql_query("DELETE FROM artifact_task_owners WHERE artifact_id=$1 AND EXISTS (SELECT 1 FROM artifact_attempts WHERE artifact_id=$1 AND state='discarded')")
                .bind::<SqlUuid, _>(candidate.artifact_id).execute(connection).await?;
            Ok::<_, PostgresStorageError>(deleted as u32)
        }).await.map_err(boundary)
    }
}

pub(crate) async fn publish_on(
    connection: &mut PostgresConnection,
    verified: &VerifiedArtifact,
) -> Result<ArtifactReference, PostgresStorageError> {
    let manifest = verified.manifest();
    let digest = manifest.manifest_digest();
    let attempt = load(connection, manifest.identity(), true).await?;
    attempt.readable()?;
    if attempt.manifest.as_deref() != Some(manifest.encode().as_slice()) {
        return Err(conflict());
    }
    if attempt.state == "published" {
        if attempt.manifest_sha256.as_deref() != Some(digest.as_slice()) {
            return Err(conflict());
        }
    } else {
        diesel::sql_query("UPDATE artifact_attempts SET state='published', manifest_sha256=$2 WHERE artifact_id=$1")
            .bind::<SqlUuid, _>(artifact_id(manifest.identity())).bind::<Binary, _>(digest.as_slice()).execute(connection).await?;
    }
    Ok(verified.reference())
}

pub(crate) async fn publish_for_task(
    connection: &mut PostgresConnection,
    task_id: i32,
    lease: &Uuid,
    verified: &VerifiedArtifact,
) -> Result<(), PostgresStorageError> {
    #[derive(QueryableByName)]
    struct Owned {
        #[diesel(sql_type = SqlUuid)]
        artifact_id: Uuid,
    }
    load(connection, verified.manifest().identity(), true).await?;
    let owner = diesel::sql_query("SELECT o.artifact_id FROM artifact_task_owners o JOIN tasks t ON t.id=$2 AND o.task_identity=(t.logical_identity #>> '{subject,identity,identity}')::UUID WHERE o.artifact_id=$1 AND o.lease_token=$3 AND o.output_expires_at>clock_timestamp() FOR UPDATE OF o")
        .bind::<SqlUuid, _>(artifact_id(verified.manifest().identity())).bind::<Integer, _>(task_id).bind::<SqlUuid, _>(*lease)
        .get_result::<Owned>(connection).await.optional()?.ok_or_else(conflict)?;
    publish_on(connection, verified).await?;
    diesel::sql_query(
        "UPDATE artifact_task_owners SET spool_bytes=0, reserved_bytes=$2 WHERE artifact_id=$1",
    )
    .bind::<SqlUuid, _>(owner.artifact_id)
    .bind::<BigInt, _>(verified.manifest().byte_size() as i64)
    .execute(connection)
    .await?;
    Ok(())
}

#[async_trait]
impl TaskArtifactStore for PostgresArtifactStore {
    async fn bind_task(
        &self,
        identity: &ArtifactIdentity,
        lease: &hubuum_storage_core::StorageTaskLease,
        reservation: ArtifactReservation,
        output_expires_at: chrono::DateTime<chrono::Utc>,
    ) -> ArtifactResult<()> {
        self.runtime
            .with_transaction(async |connection| {
                let token = Uuid::parse_str(lease.token().adapter_value()).map_err(invalid)?;
                diesel::sql_query(
                    "SELECT public.hubuum_bind_task_artifact($1,$2,$3,$4,$5,$6,$7,$8)",
                )
                .bind::<SqlUuid, _>(artifact_id(identity))
                .bind::<SqlUuid, _>(Uuid::from_bytes(*identity.attempt().as_bytes()))
                .bind::<Integer, _>(lease.task_id().id())
                .bind::<SqlUuid, _>(token)
                .bind::<SqlUuid, _>(Uuid::from_bytes(*reservation.worker()))
                .bind::<BigInt, _>(reservation.spool_bytes() as i64)
                .bind::<BigInt, _>(reservation.worker_bytes() as i64)
                .bind::<diesel::sql_types::Timestamptz, _>(output_expires_at)
                .execute(connection)
                .await?;
                load(connection, identity, false).await?;
                Ok::<_, PostgresStorageError>(())
            })
            .await
            .map_err(boundary)
    }
}
