//! Lock order: authentication revision, identity (renewal), principal, session,
//! encrypted credential. No database transaction remains open during OIDC I/O.
use crate::{PostgresConnection, PostgresRuntime, PostgresStorageError};
use chrono::{DateTime, Utc};
use diesel::sql_types::{BigInt, Integer, Jsonb, Nullable, Text, Timestamptz, Uuid as SqlUuid};
use diesel::{OptionalExtension, QueryableByName, sql_query};
use diesel_async::RunQueryDsl;
use hubuum_domain::FreshExternalAuthentication;
use hubuum_domain::PrincipalId;
use hubuum_domain::ResourceRevision;
use hubuum_storage_core::{
    SessionRevocation, StorageError, StorageExternalInteractionAuthentication,
    StorageUpstreamApproval, StorageUpstreamClaim, StorageUpstreamClaimOutcome,
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
async fn reject_ineligible_claim(
    connection: &mut PostgresConnection,
    claim: &StorageUpstreamClaim,
) -> Result<(), PostgresStorageError> {
    use crate::schema::auth_sessions::dsl as sessions;
    use diesel::{ExpressionMethods, QueryDsl, dsl::exists, select};

    let original = claim.session();
    let revoked = select(exists(
        sessions::auth_sessions
            .filter(sessions::id.eq(original.session().value()))
            .filter(sessions::principal_id.eq(original.principal().id()))
            .filter(sessions::revoked_at.is_not_null()),
    ))
    .get_result::<bool>(connection)
    .await?;
    if revoked {
        let latest =
            super::external_identity::sync_success_on_connection(connection, original.principal())
                .await?;
        if latest.is_some_and(|at| at <= claim.started_at()) {
            return Err(StorageError::upstream_session_revoked(original.session()).into());
        }
    }
    Err(invalid())
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
enum SessionSelection {
    ClaimAt(DateTime<Utc>),
    Approval,
    Session(Uuid),
}

async fn eligible(
    connection: &mut PostgresConnection,
    principal: PrincipalId,
    revision: i64,
    selection: SessionSelection,
) -> Result<Option<StorageUpstreamSession>, PostgresStorageError> {
    let (session, claim_at) = match selection {
        SessionSelection::ClaimAt(now) => (None, Some(now)),
        SessionSelection::Approval => (None, None),
        SessionSelection::Session(id) => (Some(id), None),
    };
    let row = sql_query(
        "SELECT c.document FROM auth_upstream_credentials c
         JOIN auth_sessions s ON s.id=c.session_id AND s.principal_id=c.principal_id
         JOIN principals p ON p.id=c.principal_id JOIN users u ON u.id=p.id
         JOIN identity_scopes i ON i.id=p.identity_scope_id
         JOIN authentication_provider_bindings b ON b.provider_id=c.provider_id
         WHERE b.name=i.name AND b.kind=i.provider_kind AND b.kind IN ('oidc','oauth')
           AND b.namespace=c.document->>'namespace' AND i.name=c.document->>'scope'
           AND p.external_subject=c.document->>'subject' AND c.principal_id=$1 AND COALESCE((c.document->>'current_revision')::bigint,c.revision)=$2
           AND ($3 IS NULL OR c.session_id=$3)
           AND ($4 IS NULL OR c.document->>'credential' IS NOT NULL
                OR (c.document->'lease'->>1)::timestamptz > $4)
           AND s.revoked_at IS NULL AND s.absolute_expires_at > clock_timestamp() AT TIME ZONE 'UTC'
           AND s.idle_expires_at > clock_timestamp() AT TIME ZONE 'UTC' AND u.anonymized_at IS NULL
           AND p.provider_managed AND p.last_sync_success_at IS NOT NULL
         ORDER BY CASE WHEN $4 IS NULL THEN 0
                       WHEN (c.document->'lease'->>1)::timestamptz > $4 THEN 0
                       WHEN (c.document->>'retry_after')::timestamptz > $4 THEN 2
                       ELSE 1 END, s.absolute_expires_at DESC,c.session_id LIMIT 1 FOR UPDATE OF s,c",
    )
    .bind::<Integer, _>(principal.id())
    .bind::<BigInt, _>(revision)
    .bind::<Nullable<SqlUuid>, _>(session)
    .bind::<Nullable<Timestamptz>, _>(claim_at)
    .get_result::<Row>(connection)
    .await
    .optional()?;
    row.map(decode).transpose()
}
pub async fn claim(
    runtime: &PostgresRuntime,
    expected: ResourceRevision,
    principal: PrincipalId,
) -> Result<StorageUpstreamClaimOutcome, PostgresStorageError> {
    runtime
        .with_transaction(async move |connection| -> Result<_, PostgresStorageError> {
            let revision = revision(connection).await?;
            if revision != expected.get() {
                return Err(invalid());
            }
            lock_principal(connection, principal).await?;
            let now = Utc::now();
            let Some(mut record) = eligible(
                connection,
                principal,
                revision,
                SessionSelection::ClaimAt(now),
            )
            .await?
            else {
                return Ok(StorageUpstreamClaimOutcome::Unavailable);
            };
            let outcome = record.claim(now);
            save(connection, &record).await?;
            Ok(outcome)
        })
        .await
}
pub async fn approval_session(
    runtime: &PostgresRuntime,
    proof: FreshExternalAuthentication,
) -> Result<Option<StorageUpstreamApproval>, PostgresStorageError> {
    runtime
        .with_transaction(async move |connection| {
            let revision = revision(connection).await?;
            let principal = proof
                .authentication()
                .binding()
                .authority()
                .actor()
                .ok_or_else(invalid)?
                .principal_id();
            lock_principal(connection, principal).await?;
            eligible(connection, principal, revision, SessionSelection::Approval)
                .await?
                .map(|record| StorageUpstreamApproval::new(&record, proof).map_err(|_| invalid()))
                .transpose()
        })
        .await
}

pub(super) async fn retain_approval(
    connection: &mut PostgresConnection,
    authentication: &StorageExternalInteractionAuthentication,
    revision: i64,
) -> Result<(), PostgresStorageError> {
    let StorageExternalInteractionAuthentication::ApprovalWithCredential {
        approval,
        credential,
    } = authentication
    else {
        return Ok(());
    };
    let principal = approval
        .proof()
        .authentication()
        .binding()
        .authority()
        .actor()
        .ok_or_else(invalid)?
        .principal_id();
    let mut record = eligible(
        connection,
        principal,
        revision,
        SessionSelection::Session(approval.session().value()),
    )
    .await?
    .ok_or_else(invalid)?;
    record
        .reauthenticated(approval, credential.clone(), Utc::now())
        .map_err(|_| invalid())?;
    save(connection, &record).await
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
            super::external_identity::lock_interaction_identity(
                connection,
                request.proof().identity(),
                request.proof().binding(),
            )
            .await?;
            let original = request.claim().session();
            lock_principal(connection, original.principal()).await?;
            let Some(mut record) = eligible(
                connection,
                original.principal(),
                revision,
                SessionSelection::Session(original.session().value()),
            )
            .await?
            else {
                return reject_ineligible_claim(connection, request.claim()).await;
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
pub(super) async fn issued_session_credential(
    connection: &mut PostgresConnection,
    id: Uuid,
) -> Result<Option<StorageUpstreamSession>, PostgresStorageError> {
    // Serialize with renewals before reading the grant that revocation will delete.
    sql_query("SELECT id FROM principals WHERE id=(SELECT principal_id FROM auth_sessions WHERE id=$1) FOR UPDATE")
        .bind::<SqlUuid,_>(id).execute(connection).await?;
    let row = sql_query("SELECT document FROM auth_upstream_credentials WHERE session_id=$1")
        .bind::<SqlUuid, _>(id)
        .get_result::<Row>(connection)
        .await
        .optional()?;
    Ok(row.and_then(|row| {
        decode(row)
            .inspect_err(|_| {
                tracing::warn!("Unreadable upstream grant during local session cleanup")
            })
            .ok()
    }))
}

pub async fn logout_credentials(
    runtime: &PostgresRuntime,
    target: hubuum_storage_core::StorageUpstreamRevocation,
) -> Result<Vec<StorageUpstreamSession>, PostgresStorageError> {
    runtime
        .with_read_connection(async move |connection| {
            logout_on_connection(connection, target).await
        })
        .await
}

pub(super) async fn logout_on_connection(
    connection: &mut PostgresConnection,
    target: hubuum_storage_core::StorageUpstreamRevocation,
) -> Result<Vec<StorageUpstreamSession>, PostgresStorageError> {
    use futures_util::TryStreamExt;
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
        StorageUpstreamRevocation::Provider(binding) => (None, None, None, Some(binding)),
    };
    let mut rows = sql_query("SELECT c.document FROM auth_upstream_credentials c JOIN auth_sessions s ON s.id=c.session_id WHERE s.revoked_at IS NULL AND (($1 IS NOT NULL AND c.principal_id=$1) OR ($2 IS NOT NULL AND c.session_id=(SELECT auth_session_id FROM tokens WHERE id=$2)) OR ($3 IS NOT NULL AND c.session_id=$3) OR ($4 IS NOT NULL AND c.provider_id=$4 AND c.document->>'namespace'=$5)) ORDER BY c.session_id")
            .bind::<Nullable<Integer>,_>(principal).bind::<Nullable<Integer>,_>(token).bind::<Nullable<SqlUuid>,_>(session)
            .bind::<Nullable<SqlUuid>,_>(provider.as_ref().map(|binding| binding.id()))
            .bind::<Nullable<Text>,_>(provider.as_ref().map(|binding| binding.namespace()))
            .load_stream::<Row>(connection).await?;
    let mut grants = Vec::new();
    while let Some(row) = rows.try_next().await? {
        match decode(row) {
            Ok(grant) if grant.credential().is_some() => grants.push(grant),
            Ok(_) => {}
            Err(_) => tracing::warn!("Unreadable upstream grant during local logout"),
        }
    }
    Ok(grants)
}

pub async fn fail(
    runtime: &PostgresRuntime,
    claim: hubuum_storage_core::StorageUpstreamClaim,
    failure: hubuum_storage_core::StorageUpstreamFailure,
) -> Result<(), PostgresStorageError> {
    runtime
        .with_transaction(async move |connection| {
            let revision = revision(connection).await?;
            let original = claim.session();
            lock_principal(connection, original.principal()).await?;
            let Some(mut record) = eligible(
                connection,
                original.principal(),
                revision,
                SessionSelection::Session(original.session().value()),
            )
            .await?
            else {
                return reject_ineligible_claim(connection, &claim).await;
            };
            let now = Utc::now();
            if !record.accepts(&claim, now) {
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
            let provider =
                super::security_candidates::binding_on_connection(connection, original.scope())
                    .await?
                    .ok_or_else(invalid)?;
            if record.defer_credential_retry(&claim, &provider, failure, now) {
                save(connection, &record).await?;
            } else {
                sql_query("DELETE FROM auth_upstream_credentials WHERE session_id=$1")
                    .bind::<SqlUuid, _>(original.session().value())
                    .execute(connection)
                    .await?;
            }
            if let Some(failure) = failure.sync_failure() {
                super::external_identity::failure_on_connection(
                    connection,
                    original.principal().id(),
                    failure,
                )
                .await?;
            }
            Ok(())
        })
        .await
}
