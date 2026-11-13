//! Lock order: authentication revision, identity (renewal), principal, session,
//! encrypted credential. No database transaction remains open during OIDC I/O.
use crate::{PostgresConnection, PostgresRuntime, PostgresStorageError};
use chrono::Utc;
use diesel::sql_types::{BigInt, Integer, Jsonb, Nullable, Uuid as SqlUuid};
use diesel::{OptionalExtension, QueryableByName, sql_query};
use diesel_async::RunQueryDsl;
use hubuum_domain::PrincipalId;
use hubuum_storage_core::{
    SessionRevocation, StorageExternalInteractionAuthentication, StorageUpstreamClaimOutcome,
    StorageUpstreamRenewal, StorageUpstreamSession,
};
use serde_json::Value;
use uuid::Uuid;

#[derive(QueryableByName)]
struct Row {
    #[diesel(sql_type=Jsonb)]
    document: Value,
}
#[derive(QueryableByName)]
struct Revision {
    #[diesel(sql_type=BigInt)]
    revision: i64,
}

fn invalid() -> PostgresStorageError {
    PostgresStorageError::authentication_required("Upstream session changed")
}
fn decode(row: Row) -> Result<StorageUpstreamSession, PostgresStorageError> {
    serde_json::from_value(row.document)
        .map_err(|_| PostgresStorageError::database("Invalid upstream session record"))
}
async fn revision(connection: &mut PostgresConnection) -> Result<i64, PostgresStorageError> {
    Ok(sql_query(
        "SELECT revision FROM runtime_settings WHERE setting_group='authentication' FOR SHARE",
    )
    .get_result::<Revision>(connection)
    .await?
    .revision)
}
async fn lock_principal(
    connection: &mut PostgresConnection,
    principal: PrincipalId,
) -> Result<(), PostgresStorageError> {
    sql_query("SELECT id FROM principals WHERE id=$1 FOR UPDATE")
        .bind::<Integer, _>(principal.id())
        .execute(connection)
        .await?;
    Ok(())
}
pub(super) async fn insert(
    connection: &mut PostgresConnection,
    record: &StorageUpstreamSession,
) -> Result<(), PostgresStorageError> {
    sql_query("INSERT INTO auth_upstream_credentials(session_id,principal_id,provider_id,revision,document) VALUES($1,$2,$3,$4,$5)")
        .bind::<SqlUuid,_>(record.session().value()).bind::<Integer,_>(record.principal().id())
        .bind::<SqlUuid,_>(record.binding().provider().external_id().ok_or_else(invalid)?)
        .bind::<BigInt,_>(record.binding().revision().get())
        .bind::<Jsonb,_>(serde_json::to_value(record).map_err(|_| invalid())?).execute(connection).await?;
    Ok(())
}
async fn save(
    connection: &mut PostgresConnection,
    record: &StorageUpstreamSession,
) -> Result<(), PostgresStorageError> {
    sql_query("UPDATE auth_upstream_credentials SET document=$2 WHERE session_id=$1")
        .bind::<SqlUuid, _>(record.session().value())
        .bind::<Jsonb, _>(serde_json::to_value(record).map_err(|_| invalid())?)
        .execute(connection)
        .await?;
    Ok(())
}
async fn eligible(
    connection: &mut PostgresConnection,
    principal: PrincipalId,
    revision: i64,
    session: Option<Uuid>,
) -> Result<Option<StorageUpstreamSession>, PostgresStorageError> {
    let row = sql_query("SELECT c.document FROM auth_upstream_credentials c JOIN auth_sessions s ON s.id=c.session_id AND s.principal_id=c.principal_id JOIN principals p ON p.id=c.principal_id JOIN users u ON u.id=p.id JOIN identity_scopes i ON i.id=p.identity_scope_id JOIN authentication_provider_bindings b ON b.provider_id=c.provider_id WHERE b.name=i.name AND b.kind=i.provider_kind AND b.kind IN ('oidc','oauth') AND b.namespace=c.document->>'namespace' AND i.name=c.document->>'scope' AND p.external_subject=c.document->>'subject' AND c.principal_id=$1 AND c.revision=$2 AND ($3 IS NULL OR c.session_id=$3) AND s.revoked_at IS NULL AND s.absolute_expires_at > clock_timestamp() AT TIME ZONE 'UTC' AND s.idle_expires_at > clock_timestamp() AT TIME ZONE 'UTC' AND u.anonymized_at IS NULL AND p.provider_managed AND p.last_sync_success_at IS NOT NULL ORDER BY s.absolute_expires_at DESC,c.session_id LIMIT 1 FOR UPDATE OF s,c")
        .bind::<Integer,_>(principal.id()).bind::<BigInt,_>(revision).bind::<Nullable<SqlUuid>,_>(session)
        .get_result::<Row>(connection).await.optional()?;
    row.map(decode).transpose()
}
pub async fn claim(
    runtime: &PostgresRuntime,
    principal: PrincipalId,
) -> Result<StorageUpstreamClaimOutcome, PostgresStorageError> {
    runtime
        .with_transaction(async move |connection| -> Result<_, PostgresStorageError> {
            let revision = revision(connection).await?;
            lock_principal(connection, principal).await?;
            let Some(mut record) = eligible(connection, principal, revision, None).await? else {
                return Ok(StorageUpstreamClaimOutcome::Unavailable);
            };
            let outcome = record.claim(Utc::now());
            save(connection, &record).await?;
            Ok(outcome)
        })
        .await
}
pub async fn finish(
    runtime: &PostgresRuntime,
    request: StorageUpstreamRenewal,
) -> Result<(), PostgresStorageError> {
    runtime
        .with_transaction(async move |connection| {
            let revision = revision(connection).await?;
            let authentication =
                StorageExternalInteractionAuthentication::Login(request.proof().clone());
            super::external_identity::lock_interaction_identity(connection, &authentication)
                .await?;
            let original = request.claim().session();
            lock_principal(connection, original.principal()).await?;
            let Some(mut record) = eligible(
                connection,
                original.principal(),
                revision,
                Some(original.session().value()),
            )
            .await?
            else {
                return Err(invalid());
            };
            record
                .renewed(
                    request.claim(),
                    request.proof(),
                    request.credential().cloned(),
                    Utc::now(),
                )
                .map_err(|_| invalid())?;
            let (_, user) =
                super::external_identity::synchronize_interaction(connection, &authentication)
                    .await?;
            if user.id().id() != original.principal().id() {
                return Err(invalid());
            }
            save(connection, &record).await
        })
        .await
}
pub async fn logout_credentials(
    runtime: &PostgresRuntime,
    target: hubuum_storage_core::StorageUpstreamRevocation,
) -> Result<Vec<StorageUpstreamSession>, PostgresStorageError> {
    use hubuum_storage_core::StorageUpstreamRevocation;
    let (principal, token, session, provider) = match target {
        StorageUpstreamRevocation::Sessions(SessionRevocation::All(id)) => {
            (Some(id.id()), None, None, None)
        }
        StorageUpstreamRevocation::Sessions(SessionRevocation::Current(id)) => {
            (None, Some(id.id()), None, None)
        }
        StorageUpstreamRevocation::Sessions(SessionRevocation::Session(id)) => {
            (None, None, Some(id.value()), None)
        }
        StorageUpstreamRevocation::Provider(binding) => (None, None, None, Some(binding.id())),
    };
    runtime.with_read_connection(async move |connection| {
        let rows = sql_query("SELECT c.document FROM auth_upstream_credentials c JOIN auth_sessions s ON s.id=c.session_id WHERE s.revoked_at IS NULL AND (($1 IS NOT NULL AND c.principal_id=$1) OR ($2 IS NOT NULL AND c.session_id=(SELECT auth_session_id FROM tokens WHERE id=$2)) OR ($3 IS NOT NULL AND c.session_id=$3) OR ($4 IS NOT NULL AND c.provider_id=$4)) ORDER BY c.session_id LIMIT 32")
            .bind::<Nullable<Integer>,_>(principal).bind::<Nullable<Integer>,_>(token).bind::<Nullable<SqlUuid>,_>(session).bind::<Nullable<SqlUuid>,_>(provider)
            .load::<Row>(connection).await?;
        rows.into_iter().map(decode).collect()
    }).await
}

pub async fn fail(
    runtime: &PostgresRuntime,
    claim: hubuum_storage_core::StorageUpstreamClaim,
    failure: hubuum_storage_core::StorageExternalSyncFailure,
) -> Result<(), PostgresStorageError> {
    runtime
        .with_transaction(async move |connection| {
            let revision = revision(connection).await?;
            let original = claim.session();
            lock_principal(connection, original.principal()).await?;
            let Some(record) = eligible(
                connection,
                original.principal(),
                revision,
                Some(original.session().value()),
            )
            .await?
            else {
                return Err(invalid());
            };
            if !record.accepts(&claim, Utc::now()) {
                return Err(invalid());
            }
            let latest = super::external_identity::sync_success_on_connection(
                connection,
                original.principal(),
            )
            .await?;
            if latest.is_some_and(|at| at > claim.started_at()) {
                return Err(invalid());
            }
            sql_query("DELETE FROM auth_upstream_credentials WHERE session_id=$1")
                .bind::<SqlUuid, _>(original.session().value())
                .execute(connection)
                .await?;
            super::external_identity::failure_on_connection(
                connection,
                original.principal().id(),
                failure,
            )
            .await
        })
        .await
}
