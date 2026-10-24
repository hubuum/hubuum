//! Session mutations lock principal, then family, then credentials on every replica.
use super::{event_record::append_event, user::UserRow};
use crate::{PostgresConnection, PostgresRuntime, PostgresStorageError};
use chrono::{Duration, NaiveDateTime};
use diesel::{
    prelude::*,
    sql_types::{BigInt, Integer, Nullable, Text, Timestamp, Uuid as SqlUuid},
};
use diesel_async::RunQueryDsl;
use hubuum_domain::{PrincipalId, TokenId};
use hubuum_events_core::{
    Action, AuditDocument, EntityType, EventContext, EventEntityId, NewEvent,
};
use hubuum_storage_core::{
    AuthSessionId, RefreshCredential, SessionCredentials, SessionLifetimes, SessionRevocation,
    StorageAuditReceipt, StorageAuditReceipts, StorageMutationOutcome, StorageSessionCreate,
    StorageSessionIssued, StorageSessionRotate, StorageSessionRotation,
};
use uuid::Uuid;

#[derive(QueryableByName)]
struct Family {
    #[diesel(sql_type = SqlUuid)]
    id: Uuid,
    #[diesel(sql_type = Integer)]
    principal_id: i32,
    #[diesel(sql_type = Text)]
    client_id: String,
    #[diesel(sql_type = BigInt)]
    access_seconds: i64,
    #[diesel(sql_type = BigInt)]
    idle_seconds: i64,
    #[diesel(sql_type = Timestamp)]
    absolute_expires_at: NaiveDateTime,
    #[diesel(sql_type = Timestamp)]
    idle_expires_at: NaiveDateTime,
    #[diesel(sql_type = Nullable<Timestamp>)]
    revoked_at: Option<NaiveDateTime>,
}
#[derive(QueryableByName)]
struct RefreshRow {
    #[diesel(sql_type = SqlUuid)]
    id: Uuid,
    #[diesel(sql_type = SqlUuid)]
    session_id: Uuid,
    #[diesel(sql_type = Text)]
    hash_key_id: String,
    #[diesel(sql_type = Timestamp)]
    expires_at: NaiveDateTime,
    #[diesel(sql_type = Nullable<Timestamp>)]
    consumed_at: Option<NaiveDateTime>,
}
#[derive(QueryableByName)]
struct Owner {
    #[diesel(sql_type = Integer)]
    principal_id: i32,
}
#[derive(QueryableByName)]
struct Id {
    #[diesel(sql_type = Integer)]
    id: i32,
}

fn denied() -> PostgresStorageError {
    PostgresStorageError::authentication_required("Authentication failed")
}
async fn now(connection: &mut PostgresConnection) -> Result<NaiveDateTime, PostgresStorageError> {
    Ok(diesel::select(diesel::dsl::sql::<Timestamp>(
        "clock_timestamp() AT TIME ZONE 'UTC'",
    ))
    .get_result(connection)
    .await?)
}
async fn lock_principal(
    connection: &mut PostgresConnection,
    id: i32,
) -> Result<bool, PostgresStorageError> {
    use crate::schema::principals as p;
    let Some((managed, synced)) = p::table
        .find(id)
        .for_update()
        .select((p::provider_managed, p::last_sync_success_at))
        .first::<(bool, Option<NaiveDateTime>)>(connection)
        .await
        .optional()?
    else {
        return Ok(false);
    };
    let anonymized = crate::schema::users::table
        .find(id)
        .select(crate::schema::users::anonymized_at)
        .first::<Option<NaiveDateTime>>(connection)
        .await
        .optional()?;
    Ok(anonymized.is_some_and(|value| value.is_none()) && (!managed || synced.is_some()))
}
async fn event(
    connection: &mut PostgresConnection,
    family: &Family,
    action: Action,
    reason: &str,
    context: &EventContext,
) -> Result<StorageAuditReceipt, PostgresStorageError> {
    let document = AuditDocument::builder(format!("Interactive session {reason}"))
        .metadata(serde_json::json!({"session_id": family.id, "principal_id": family.principal_id, "reason": reason}))
        .try_build().map_err(|e| PostgresStorageError::invalid_input(e.to_string()))?;
    let event = NewEvent::from_document(
        EntityType::Authentication,
        action,
        context.actor_kind(),
        document,
    )
    .map_err(|e| PostgresStorageError::invalid_input(e.to_string()))?
    .with_context(context)
    .with_entity_id(EventEntityId::new(family.principal_id)?);
    Ok(append_event(connection, &event).await?.into_audit_receipt())
}
async fn issue(
    connection: &mut PostgresConnection,
    family: &Family,
    credentials: &SessionCredentials,
    issued: NaiveDateTime,
) -> Result<StorageSessionIssued, PostgresStorageError> {
    let deadlines = SessionLifetimes::try_new(family.access_seconds, family.idle_seconds)
        .map_err(|e| PostgresStorageError::database(e.to_string()))?
        .deadlines(issued.and_utc(), family.absolute_expires_at.and_utc())
        .map_err(|e| PostgresStorageError::database(e.to_string()))?;
    let access_expiry = deadlines.access_expires_at().naive_utc();
    let refresh_expiry = deadlines.refresh_expires_at().naive_utc();
    let access = credentials.access();
    let key_id = access.key_id().ok_or_else(denied)?.as_str();
    let id = diesel::sql_query("INSERT INTO tokens (token, principal_id, issued, expires_at, token_format, token_hash_algorithm, token_hash_key_id, auth_session_id, credential_kind) VALUES ($1,$2,$3,$4,1,1,$5,$6,'session_access') RETURNING id")
        .bind::<Text, _>(access.lookup_value())
        .bind::<Integer, _>(family.principal_id)
        .bind::<Timestamp, _>(issued)
        .bind::<Timestamp, _>(access_expiry)
        .bind::<Text, _>(key_id)
        .bind::<SqlUuid, _>(family.id)
        .get_result::<Id>(connection)
        .await?
        .id;
    let refresh = credentials.refresh();
    diesel::sql_query("INSERT INTO refresh_tokens (id, session_id, digest, hash_key_id, issued_at, expires_at) VALUES ($1,$2,$3,$4,$5,$6)")
        .bind::<SqlUuid, _>(Uuid::new_v4())
        .bind::<SqlUuid, _>(family.id)
        .bind::<Text, _>(refresh.lookup_value())
        .bind::<Text, _>(refresh.key_id().ok_or_else(denied)?.as_str())
        .bind::<Timestamp, _>(issued)
        .bind::<Timestamp, _>(refresh_expiry)
        .execute(connection)
        .await?;
    diesel::sql_query(
        "UPDATE auth_sessions SET last_refreshed_at=$2, idle_expires_at=$3 WHERE id=$1",
    )
    .bind::<SqlUuid, _>(family.id)
    .bind::<Timestamp, _>(issued)
    .bind::<Timestamp, _>(refresh_expiry)
    .execute(connection)
    .await?;
    Ok(StorageSessionIssued::new(
        AuthSessionId::new(family.id),
        PrincipalId::new(family.principal_id)?,
        TokenId::new(id)?,
        deadlines,
    ))
}

pub async fn create(
    runtime: &PostgresRuntime,
    request: StorageSessionCreate,
) -> Result<StorageMutationOutcome<StorageSessionIssued>, PostgresStorageError> {
    runtime
        .with_transaction(async move |connection| -> Result<_, PostgresStorageError> {
            let subject = request.login().subject();
            let principal_id = subject.principal_id().id();
            if !lock_principal(connection, principal_id).await? {
                return Err(denied());
            }
            let user = crate::schema::users::table
                .find(principal_id)
                .select(UserRow::as_select())
                .first::<UserRow>(connection)
                .await?
                .into_storage()?;
            if !request.login().matches_user(&user) {
                return Err(denied());
            }
            let issued = now(connection).await?;
            let policy = request.policy();
            let family = Family {
                id: Uuid::new_v4(),
                principal_id,
                client_id: request.client().as_str().to_owned(),
                access_seconds: policy.access_seconds(),
                idle_seconds: policy.idle_seconds(),
                absolute_expires_at: issued + Duration::seconds(policy.absolute_seconds()),
                idle_expires_at: issued + Duration::seconds(policy.idle_seconds()),
                revoked_at: None,
            };
            diesel::sql_query("INSERT INTO auth_sessions (id,principal_id,client_id,authenticated_at,last_refreshed_at,access_seconds,idle_seconds,absolute_expires_at,idle_expires_at,retain_until) VALUES ($1,$2,$3,$4,$4,$5,$6,$7,$8,$9)")
                .bind::<SqlUuid, _>(family.id)
                .bind::<Integer, _>(principal_id)
                .bind::<Text, _>(&family.client_id)
                .bind::<Timestamp, _>(issued)
                .bind::<BigInt, _>(family.access_seconds)
                .bind::<BigInt, _>(family.idle_seconds)
                .bind::<Timestamp, _>(family.absolute_expires_at)
                .bind::<Timestamp, _>(family.idle_expires_at)
                .bind::<Timestamp, _>(
                    family.absolute_expires_at + Duration::seconds(policy.retention_seconds()),
                )
                .execute(connection)
                .await?;
            let response = issue(connection, &family, request.credentials(), issued).await?;
            let token_event = NewEvent::from_document(
                EntityType::Token,
                Action::Created,
                request.context().actor_kind(),
                AuditDocument::builder("Interactive access token created")
                    .metadata(serde_json::json!({"session_id": family.id}))
                    .try_build()?,
            )
            .map_err(|e| PostgresStorageError::invalid_input(e.to_string()))?
            .with_context(request.context())
            .with_entity_id(EventEntityId::new(response.access_id().id())?);
            let audit = append_event(connection, &token_event)
                .await?
                .into_audit_receipt();
            let login_event = NewEvent::from_document(
                EntityType::Authentication,
                Action::Succeeded,
                request.context().actor_kind(),
                subject.successful_login_document(),
            )
            .map_err(|e| PostgresStorageError::invalid_input(e.to_string()))?
            .with_context(request.context())
            .with_entity_id(EventEntityId::new(principal_id)?)
            .with_entity_name(subject.name());
            let login_audit = append_event(connection, &login_event)
                .await?
                .into_audit_receipt();
            Ok(StorageMutationOutcome::committed_with_audits(
                response,
                StorageAuditReceipts::new(audit, vec![login_audit]),
            ))
        })
        .await
}

pub async fn refresh_principal(
    runtime: &PostgresRuntime,
    credential: RefreshCredential,
) -> Result<PrincipalId, PostgresStorageError> {
    runtime
        .with_connection(async move |connection| -> Result<_, PostgresStorageError> {
            let owner = diesel::sql_query("SELECT s.principal_id FROM auth_sessions s JOIN refresh_tokens r ON r.session_id=s.id WHERE r.digest=$1 AND r.hash_key_id=$2")
                .bind::<Text, _>(credential.digest().lookup_value())
                .bind::<Text, _>(credential.digest().key_id().ok_or_else(denied)?.as_str())
                .get_result::<Owner>(connection)
                .await
                .optional()?
                .ok_or_else(denied)?;
            Ok(PrincipalId::new(owner.principal_id)?)
        })
        .await
}

pub async fn revoke_family(
    connection: &mut PostgresConnection,
    id: Uuid,
    reason: &str,
) -> Result<usize, PostgresStorageError> {
    let count = diesel::sql_query("UPDATE auth_sessions SET revoked_at=clock_timestamp() AT TIME ZONE 'UTC', revocation_reason=$2 WHERE id=$1 AND revoked_at IS NULL")
        .bind::<SqlUuid, _>(id)
        .bind::<Text, _>(reason)
        .execute(connection)
        .await?;
    diesel::sql_query("UPDATE tokens SET revoked_at=clock_timestamp() AT TIME ZONE 'UTC' WHERE auth_session_id=$1 AND revoked_at IS NULL")
        .bind::<SqlUuid, _>(id)
        .execute(connection)
        .await?;
    Ok(count)
}

pub async fn revoke_principal(
    connection: &mut PostgresConnection,
    principal_id: i32,
) -> Result<usize, PostgresStorageError> {
    let families = diesel::sql_query("SELECT * FROM auth_sessions WHERE principal_id=$1 AND revoked_at IS NULL ORDER BY id FOR UPDATE")
        .bind::<Integer, _>(principal_id)
        .load::<Family>(connection)
        .await?;
    for family in &families {
        // Callers revoke the principal's access tokens in the same transaction.
        // Preserve their before snapshots and mutation counts until that write.
        diesel::sql_query("UPDATE auth_sessions SET revoked_at=clock_timestamp() AT TIME ZONE 'UTC', revocation_reason='principal_security_event' WHERE id=$1")
            .bind::<SqlUuid, _>(family.id)
            .execute(connection)
            .await?;
    }
    Ok(families.len())
}

pub async fn rotate(
    runtime: &PostgresRuntime,
    request: StorageSessionRotate,
) -> Result<StorageMutationOutcome<StorageSessionRotation>, PostgresStorageError> {
    runtime
        .with_transaction(async move |connection| -> Result<_, PostgresStorageError> {
            let digest = request.presented().digest();
            let refresh = diesel::sql_query("SELECT * FROM refresh_tokens WHERE digest=$1")
                .bind::<Text, _>(digest.lookup_value())
                .get_result::<RefreshRow>(connection)
                .await
                .optional()?;
            let Some(refresh) = refresh else {
                return Ok(StorageMutationOutcome::unchanged(
                    StorageSessionRotation::Rejected,
                ));
            };
            if digest
                .key_id()
                .is_none_or(|key| key.as_str() != refresh.hash_key_id)
            {
                return Ok(StorageMutationOutcome::unchanged(
                    StorageSessionRotation::Rejected,
                ));
            }
            let owner = diesel::sql_query("SELECT principal_id FROM auth_sessions WHERE id=$1")
                .bind::<SqlUuid, _>(refresh.session_id)
                .get_result::<Owner>(connection)
                .await
                .optional()?;
            let Some(owner) = owner else {
                return Ok(StorageMutationOutcome::unchanged(
                    StorageSessionRotation::Rejected,
                ));
            };
            if !lock_principal(connection, owner.principal_id).await? {
                return Ok(StorageMutationOutcome::unchanged(
                    StorageSessionRotation::Rejected,
                ));
            }
            let family = diesel::sql_query("SELECT * FROM auth_sessions WHERE id=$1 FOR UPDATE")
                .bind::<SqlUuid, _>(refresh.session_id)
                .get_result::<Family>(connection)
                .await
                .optional()?;
            let Some(family) = family else {
                return Ok(StorageMutationOutcome::unchanged(
                    StorageSessionRotation::Rejected,
                ));
            };
            // Re-read only after the family lock: concurrent callers must observe consumption.
            let refresh = diesel::sql_query("SELECT * FROM refresh_tokens WHERE id=$1")
                .bind::<SqlUuid, _>(refresh.id)
                .get_result::<RefreshRow>(connection)
                .await?;
            if family.client_id != request.client().as_str() {
                return Ok(StorageMutationOutcome::unchanged(
                    StorageSessionRotation::Rejected,
                ));
            }
            if refresh.consumed_at.is_some() {
                revoke_family(connection, family.id, "refresh_replay").await?;
                diesel::sql_query("UPDATE refresh_tokens SET reuse_detected_at=COALESCE(reuse_detected_at,clock_timestamp() AT TIME ZONE 'UTC') WHERE id=$1")
                    .bind::<SqlUuid, _>(refresh.id)
                    .execute(connection)
                    .await?;
                let audit = event(
                    connection,
                    &family,
                    Action::Revoked,
                    "refresh_replay",
                    request.context(),
                )
                .await?;
                return Ok(StorageMutationOutcome::committed(
                    StorageSessionRotation::Replayed,
                    audit,
                ));
            }
            let observed = now(connection).await?;
            if family.revoked_at.is_some()
                || observed >= family.idle_expires_at
                || observed >= family.absolute_expires_at
                || observed >= refresh.expires_at
            {
                return Ok(StorageMutationOutcome::unchanged(
                    StorageSessionRotation::Rejected,
                ));
            }
            diesel::sql_query("UPDATE refresh_tokens SET consumed_at=$2 WHERE id=$1")
                .bind::<SqlUuid, _>(refresh.id)
                .bind::<Timestamp, _>(observed)
                .execute(connection)
                .await?;
            let issued = issue(connection, &family, request.credentials(), observed).await?;
            let audit = event(
                connection,
                &family,
                Action::Updated,
                "refreshed",
                request.context(),
            )
            .await?;
            Ok(StorageMutationOutcome::committed(
                StorageSessionRotation::Issued(issued),
                audit,
            ))
        })
        .await
}

pub async fn revoke(
    runtime: &PostgresRuntime,
    target: SessionRevocation,
    context: EventContext,
) -> Result<StorageMutationOutcome<usize>, PostgresStorageError> {
    runtime
        .with_transaction(async move |connection| -> Result<_, PostgresStorageError> {
            let (owner, token_id) = match target {
                SessionRevocation::All(id) => (id.id(), None),
                SessionRevocation::Current(id) => {
                    let owner = diesel::sql_query("SELECT principal_id FROM tokens WHERE id=$1 AND auth_session_id IS NOT NULL")
                        .bind::<Integer, _>(id.id())
                        .get_result::<Owner>(connection)
                        .await
                        .optional()?
                        .ok_or_else(denied)?;
                    (owner.principal_id, Some(id.id()))
                }
            };
            lock_principal(connection, owner).await?;
            let families = diesel::sql_query("SELECT * FROM auth_sessions WHERE principal_id=$1 AND revoked_at IS NULL AND ($2 IS NULL OR id=(SELECT auth_session_id FROM tokens WHERE id=$2)) ORDER BY id FOR UPDATE")
                .bind::<Integer, _>(owner)
                .bind::<Nullable<Integer>, _>(token_id)
                .load::<Family>(connection)
                .await?;
            let mut audits = Vec::new();
            for family in &families {
                revoke_family(connection, family.id, "logout").await?;
                audits.push(event(connection, family, Action::Revoked, "logout", &context).await?);
            }
            if audits.is_empty() {
                return Ok(StorageMutationOutcome::unchanged(0));
            }
            Ok(StorageMutationOutcome::committed_with_audits(
                families.len(),
                StorageAuditReceipts::try_from_vec(audits)
                    .map_err(|e| PostgresStorageError::invalid_input(e.to_string()))?,
            ))
        })
        .await
}

pub async fn purge(runtime: &PostgresRuntime, limit: usize) -> Result<usize, PostgresStorageError> {
    let limit = i64::try_from(limit)
        .map_err(|_| PostgresStorageError::invalid_input("session purge limit out of range"))?;
    runtime
        .with_transaction(async move |connection| -> Result<_, PostgresStorageError> {
            Ok(diesel::sql_query("DELETE FROM auth_sessions WHERE id IN (SELECT id FROM auth_sessions WHERE retain_until < clock_timestamp() AT TIME ZONE 'UTC' AND NOT EXISTS (SELECT 1 FROM tokens WHERE auth_session_id=auth_sessions.id) ORDER BY retain_until,id LIMIT $1 FOR UPDATE SKIP LOCKED)")
                .bind::<BigInt, _>(limit)
                .execute(connection)
                .await?)
        })
        .await
}

pub(super) async fn revoke_access(
    connection: &mut PostgresConnection,
    token_id: i32,
    principal_id: i32,
    context: &EventContext,
) -> Result<Option<StorageAuditReceipt>, PostgresStorageError> {
    let session = crate::schema::tokens::table
        .find(token_id)
        .filter(crate::schema::tokens::principal_id.eq(principal_id))
        .select(crate::schema::tokens::auth_session_id)
        .first::<Option<Uuid>>(connection)
        .await
        .optional()?
        .flatten();
    let Some(session) = session else {
        return Ok(None);
    };
    let family = diesel::sql_query("SELECT * FROM auth_sessions WHERE id=$1 FOR UPDATE")
        .bind::<SqlUuid, _>(session)
        .get_result::<Family>(connection)
        .await?;
    let revision = crate::schema::tokens::table
        .find(token_id)
        .for_update()
        .select(crate::schema::tokens::revision)
        .first::<crate::PostgresRevision>(connection)
        .await?;
    crate::runtime::assert_locked_revision_precondition(
        connection,
        &crate::revision::RevisionOwner::Token.key(token_id),
        revision,
    )
    .await?;
    let already_revoked = crate::schema::tokens::table
        .find(token_id)
        .select(crate::schema::tokens::revoked_at)
        .first::<Option<NaiveDateTime>>(connection)
        .await?
        .is_some();
    if already_revoked {
        return Ok(None);
    }
    revoke_family(connection, family.id, "credential_revoked").await?;
    Ok(Some(
        event(
            connection,
            &family,
            Action::Revoked,
            "credential_revoked",
            context,
        )
        .await?,
    ))
}
