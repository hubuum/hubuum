//! Retained approval evidence and transactional single-use enforcement.
use super::event_record::append_event;
use crate::{PostgresConnection, PostgresRuntime, PostgresStorageError};
use chrono::{NaiveDateTime, Utc};
use diesel::sql_types::{BigInt, Integer, Nullable, Text, Timestamp};
use diesel::{OptionalExtension, QueryableByName, sql_query};
use diesel_async::RunQueryDsl;
use hubuum_domain::{PrincipalId, RestoreJobId, TokenId};
use hubuum_events_core::{
    Action, AuditDocument, EntityType, EventContext, EventEntityId, NewEvent,
};
use hubuum_storage_core::{
    CREDENTIAL_APPROVAL_LIFETIME_SECONDS, StorageAuditReceipt, StorageAuditReceipts,
    StorageCredentialApprovalCreate, StorageCredentialApprovalMetadata, StorageCredentialClaim,
    StorageCredentialFingerprint, StorageCredentialOperation, StorageMutationOutcome,
};
use serde_json::json;

#[derive(QueryableByName)]
struct ApprovalRow {
    #[diesel(sql_type = Integer)]
    id: i32,
    #[diesel(sql_type = Integer)]
    actor_id: i32,
    #[diesel(sql_type = Integer)]
    token_id: i32,
    #[diesel(sql_type = Text)]
    operation: String,
    #[diesel(sql_type = Nullable<Integer>)]
    target_id: Option<i32>,
    #[diesel(sql_type = Text)]
    secret_digest: String,
    #[diesel(sql_type = Nullable<BigInt>)]
    restore_job_id: Option<i64>,
    #[diesel(sql_type = Nullable<Timestamp>)]
    invalidated_at: Option<NaiveDateTime>,
    #[diesel(sql_type = Text)]
    request_digest: String,
    #[diesel(sql_type = Timestamp)]
    authenticated_at: NaiveDateTime,
    #[diesel(sql_type = Timestamp)]
    expires_at: NaiveDateTime,
    #[diesel(sql_type = Nullable<Timestamp>)]
    consumed_at: Option<NaiveDateTime>,
}
impl ApprovalRow {
    fn metadata(self) -> Result<StorageCredentialApprovalMetadata, PostgresStorageError> {
        let operation = serde_json::from_value(json!(self.operation))
            .map_err(|_| PostgresStorageError::database("Invalid credential approval operation"))?;
        let mut claim = StorageCredentialClaim::new(
            PrincipalId::new(self.actor_id)?,
            TokenId::new(self.token_id)?,
            operation,
            StorageCredentialFingerprint::new(self.secret_digest)
                .map_err(|_| PostgresStorageError::database("Invalid approval digest"))?,
            StorageCredentialFingerprint::new(self.request_digest)
                .map_err(|_| PostgresStorageError::database("Invalid approval binding"))?,
            self.authenticated_at.and_utc(),
        )
        .target(self.target_id.map(PrincipalId::new).transpose()?);
        if let Some(id) = self.restore_job_id {
            claim = claim.restore_job(RestoreJobId::new(id)?);
        }
        let mut metadata = StorageCredentialApprovalMetadata::new(
            self.id,
            &claim,
            self.authenticated_at.and_utc(),
            self.expires_at.and_utc(),
        )
        .map_err(|_| PostgresStorageError::database("Invalid approval metadata"))?;
        if let Some(at) = self.consumed_at {
            metadata
                .consume(at.and_utc())
                .map_err(|_| PostgresStorageError::database("Invalid approval consumption"))?;
        }
        if let Some(at) = self.invalidated_at {
            metadata.invalidate(at.and_utc());
        }
        Ok(metadata)
    }
}
#[derive(QueryableByName)]
struct ActiveToken {
    #[diesel(sql_type = Nullable<Timestamp>)]
    expires_at: Option<NaiveDateTime>,
}
fn denied() -> PostgresStorageError {
    PostgresStorageError::reauthentication_required()
}

async fn lock_actor(
    connection: &mut PostgresConnection,
    claim: &StorageCredentialClaim,
) -> Result<Option<NaiveDateTime>, PostgresStorageError> {
    // Principal locks precede token locks, matching password updates. Sort the
    // actor and target so two administrators acting on one another cannot invert them.
    sql_query("SELECT id FROM principals WHERE id = $1 OR id = $2 ORDER BY id FOR UPDATE")
        .bind::<Integer, _>(claim.actor_id().id())
        .bind::<Nullable<Integer>, _>(claim.target_id().map(PrincipalId::id))
        .execute(connection)
        .await?;
    let token = sql_query("SELECT t.expires_at FROM tokens t JOIN users u ON u.id=t.principal_id \
        WHERE t.id=$1 AND t.principal_id=$2 AND t.revoked_at IS NULL \
        AND NOT t.permission_scoped AND NOT t.resource_scoped AND u.anonymized_at IS NULL \
        AND (t.expires_at > timezone('UTC', clock_timestamp()) OR (t.expires_at IS NULL AND t.issued > $3)) FOR UPDATE OF t")
        .bind::<Integer,_>(claim.token_id().id()).bind::<Integer,_>(claim.actor_id().id())
        .bind::<Timestamp,_>(claim.legacy_valid_after().naive_utc())
        .get_result::<ActiveToken>(connection).await.optional()?.ok_or_else(denied)?;
    Ok(token.expires_at)
}

async fn audit(
    connection: &mut PostgresConnection,
    metadata: &StorageCredentialApprovalMetadata,
    action: Action,
    context: &EventContext,
) -> Result<StorageAuditReceipt, PostgresStorageError> {
    let document = AuditDocument::try_new(
        format!("Credential approval {} {}", metadata.id(), action.as_str()),
        None,
        Some(json!(metadata)),
        json!({"approval_id": metadata.id(), "operation": metadata.operation().as_str(), "target_id": metadata.target_id()}),
    )?;
    let event = NewEvent::from_document(
        EntityType::CredentialApproval,
        action,
        context.actor_kind(),
        document,
    )
    .map_err(|_| PostgresStorageError::internal("Invalid credential approval event"))?
    .with_context(context)
    .with_entity_id(EventEntityId::new(metadata.id())?);
    Ok(append_event(connection, &event).await?.into_audit_receipt())
}

pub async fn create(
    runtime: &PostgresRuntime,
    request: StorageCredentialApprovalCreate,
) -> Result<StorageMutationOutcome<StorageCredentialApprovalMetadata>, PostgresStorageError> {
    let (claim, authenticated_at, context) = request.into_parts();
    runtime.with_transaction(async move |connection| {
        if context.actor_user_id() != Some(claim.actor_id()) { return Err(denied()); }
        let token_expiry = lock_actor(connection, &claim).await?;
        let mut expiry = authenticated_at + chrono::Duration::seconds(CREDENTIAL_APPROVAL_LIFETIME_SECONDS);
        if let Some(token_expiry) = token_expiry { expiry = expiry.min(token_expiry.and_utc()); }
        if authenticated_at > Utc::now() || expiry <= Utc::now() { return Err(denied()); }
        let row = sql_query("INSERT INTO credential_approvals (actor_id,token_id,operation,target_id,secret_digest,request_digest,authenticated_at,expires_at,restore_job_id) \
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING *")
            .bind::<Integer,_>(claim.actor_id().id()).bind::<Integer,_>(claim.token_id().id())
            .bind::<Text,_>(claim.operation().as_str()).bind::<Nullable<Integer>,_>(claim.target_id().map(PrincipalId::id))
            .bind::<Text,_>(claim.secret_digest().as_str()).bind::<Text,_>(claim.request_digest().as_str())
            .bind::<Timestamp,_>(authenticated_at.naive_utc()).bind::<Timestamp,_>(expiry.naive_utc())
            .bind::<Nullable<BigInt>,_>(claim.restore_job_id().map(RestoreJobId::id))
            .get_result::<ApprovalRow>(connection).await?;
        let metadata = row.metadata()?;
        let receipt = audit(connection, &metadata, Action::Created, &context).await?;
        Ok(StorageMutationOutcome::committed(metadata, receipt))
    }).await
}

pub async fn get(
    runtime: &PostgresRuntime,
    id: i32,
) -> Result<StorageCredentialApprovalMetadata, PostgresStorageError> {
    runtime
        .with_connection(async move |connection| {
            sql_query("SELECT * FROM credential_approvals WHERE id=$1")
                .bind::<Integer, _>(id)
                .get_result::<ApprovalRow>(connection)
                .await?
                .metadata()
        })
        .await
}

pub(crate) async fn consume(
    connection: &mut PostgresConnection,
    claim: Option<&StorageCredentialClaim>,
    expected_operation: StorageCredentialOperation,
    target: Option<i32>,
    context: &EventContext,
) -> Result<Option<StorageAuditReceipt>, PostgresStorageError> {
    let Some(claim) = claim else {
        return Ok(None);
    }; // Explicit internal/bootstrap/login issuance.
    if claim.operation() != expected_operation
        || claim.target_id().map(PrincipalId::id) != target
        || context.actor_user_id() != Some(claim.actor_id())
    {
        return Err(denied());
    }
    lock_actor(connection, claim).await?;
    let row = sql_query("WITH approval_clock AS MATERIALIZED (SELECT timezone('UTC', clock_timestamp()) AS observed_at) \
        UPDATE credential_approvals SET consumed_at=approval_clock.observed_at FROM approval_clock \
        WHERE secret_digest=$1 AND request_digest=$2 AND actor_id=$3 AND token_id=$4 AND operation=$5 \
        AND target_id IS NOT DISTINCT FROM $6 AND restore_job_id IS NOT DISTINCT FROM $7 AND consumed_at IS NULL AND invalidated_at IS NULL \
        AND authenticated_at <= approval_clock.observed_at AND expires_at > approval_clock.observed_at RETURNING credential_approvals.*")
        .bind::<Text,_>(claim.secret_digest().as_str()).bind::<Text,_>(claim.request_digest().as_str())
        .bind::<Integer,_>(claim.actor_id().id()).bind::<Integer,_>(claim.token_id().id())
        .bind::<Text,_>(claim.operation().as_str()).bind::<Nullable<Integer>,_>(target)
        .bind::<Nullable<BigInt>,_>(claim.restore_job_id().map(RestoreJobId::id))
        .get_result::<ApprovalRow>(connection).await.optional()?.ok_or_else(denied)?;
    Ok(Some(
        audit(connection, &row.metadata()?, Action::Succeeded, context).await?,
    ))
}

pub(crate) fn outcome<T>(
    value: T,
    audit: StorageAuditReceipt,
    approval: Option<StorageAuditReceipt>,
) -> StorageMutationOutcome<T> {
    StorageMutationOutcome::committed_with_audits(
        value,
        StorageAuditReceipts::new(audit, approval.into_iter().collect()),
    )
}

pub(crate) async fn restore_evidence(
    connection: &mut PostgresConnection,
    job_id: i64,
) -> Result<Option<StorageCredentialApprovalMetadata>, PostgresStorageError> {
    sql_query("SELECT * FROM credential_approvals WHERE restore_job_id=$1 AND consumed_at IS NOT NULL ORDER BY consumed_at DESC, id DESC LIMIT 1")
        .bind::<BigInt,_>(job_id).get_result::<ApprovalRow>(connection).await.optional()?.map(ApprovalRow::metadata).transpose()
}
