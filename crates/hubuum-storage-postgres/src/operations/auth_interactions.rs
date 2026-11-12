//! Lock order: authentication settings, interaction, principals, credentials and
//! session/candidate rows. Rejected transitions that change state still commit.
use chrono::{DateTime, Utc};
use diesel::sql_types::{
    BigInt, Bool, Integer, Jsonb, Nullable, Text, Timestamp, Timestamptz, Uuid as SqlUuid,
};
use diesel::{OptionalExtension, QueryDsl, QueryableByName, SelectableHelper, sql_query};
use diesel_async::RunQueryDsl;
use hubuum_domain::{
    InteractionBrowserBinding, InteractionDigest, InteractionIssuedResult, InteractionState,
    InteractionTransitionError, ResourceRevision,
};
use hubuum_events_core::EventContext;
use hubuum_storage_core::{
    AuthSessionId, MAX_ACTIVE_AUTH_INTERACTIONS, MAX_ACTIVE_AUTH_INTERACTIONS_PER_ACTOR,
    MAX_ACTIVE_AUTH_INTERACTIONS_PER_CLIENT, MAX_AUTH_INTERACTIONS, SessionClientId,
    SessionRevocation, StorageCredentialApprovalCreate, StorageErrorKind, StorageInteractionAccess,
    StorageInteractionAction, StorageInteractionCommand, StorageInteractionComplete,
    StorageInteractionCompletion, StorageInteractionCreate, StorageInteractionEvidence,
    StorageInteractionIssuance, StorageInteractionLogin, StorageInteractionOutcome,
    StorageInteractionRecord, StorageInteractionView, StorageLogin, StorageMutationOutcome,
    StorageSessionCreate,
};
use serde_json::Value;
use uuid::Uuid;

use super::{auth_sessions, credential_approval, security_candidates, user::UserRow};
use crate::{PostgresConnection, PostgresRuntime, PostgresStorageError};

#[derive(QueryableByName)]
struct Row {
    #[diesel(sql_type=Jsonb)]
    document: Value,
}
impl Row {
    fn record(self) -> Result<StorageInteractionRecord, PostgresStorageError> {
        StorageInteractionRecord::from_document(self.document).map_err(|_| {
            PostgresStorageError::database("Invalid persisted authentication interaction")
        })
    }
}
#[derive(QueryableByName)]
struct Revision {
    #[diesel(sql_type=BigInt)]
    revision: i64,
}
#[derive(QueryableByName)]
struct Counts {
    #[diesel(sql_type=BigInt)]
    total: i64,
    #[diesel(sql_type=BigInt)]
    active: i64,
    #[diesel(sql_type=BigInt)]
    client: i64,
    #[diesel(sql_type=BigInt)]
    actor: i64,
}
#[derive(QueryableByName)]
struct Valid {
    #[diesel(sql_type=Bool)]
    valid: bool,
}
#[derive(QueryableByName)]
struct Identity {
    #[diesel(sql_type=Text)]
    scope: String,
    #[diesel(sql_type=Text)]
    kind: String,
    #[diesel(sql_type=Nullable<Text>)]
    subject: Option<String>,
    #[diesel(sql_type=Bool)]
    eligible: bool,
}

fn invalid() -> PostgresStorageError {
    PostgresStorageError::authentication_required("Invalid authentication interaction")
}
async fn now(connection: &mut PostgresConnection) -> Result<DateTime<Utc>, PostgresStorageError> {
    Ok(
        diesel::select(diesel::dsl::sql::<Timestamptz>("clock_timestamp()"))
            .get_result(connection)
            .await?,
    )
}
async fn settings(
    connection: &mut PostgresConnection,
) -> Result<ResourceRevision, PostgresStorageError> {
    let value = sql_query(
        "SELECT revision FROM runtime_settings WHERE setting_group='authentication' FOR SHARE",
    )
    .get_result::<Revision>(connection)
    .await?;
    ResourceRevision::new(value.revision)
        .map_err(|error| PostgresStorageError::database(error.to_string()))
}
async fn load(
    connection: &mut PostgresConnection,
    id: Uuid,
) -> Result<StorageInteractionRecord, PostgresStorageError> {
    sql_query("SELECT document FROM auth_interactions WHERE id=$1 FOR UPDATE")
        .bind::<SqlUuid, _>(id)
        .get_result::<Row>(connection)
        .await
        .optional()?
        .ok_or_else(invalid)?
        .record()
}
async fn save(
    connection: &mut PostgresConnection,
    record: &StorageInteractionRecord,
) -> Result<(), PostgresStorageError> {
    let document = record
        .to_document()
        .map_err(|error| PostgresStorageError::invalid_input(error.to_string()))?;
    sql_query("UPDATE auth_interactions SET document=$2,retain_until=$3 WHERE id=$1")
        .bind::<SqlUuid, _>(record.lifecycle().binding().id().value())
        .bind::<Jsonb, _>(document)
        .bind::<Timestamptz, _>(record.retain_until())
        .execute(connection)
        .await?;
    Ok(())
}
async fn actor_valid(
    connection: &mut PostgresConnection,
    record: &StorageInteractionRecord,
    at: DateTime<Utc>,
) -> Result<bool, PostgresStorageError> {
    let Some(actor) = record.lifecycle().binding().authority().actor() else {
        return Ok(true);
    };
    let Some(lease) = record.actor_lease() else {
        return Ok(false);
    };
    if at >= lease.valid_until() {
        return Ok(false);
    }
    sql_query("SELECT id FROM principals WHERE id=$1 FOR UPDATE")
        .bind::<Integer, _>(actor.principal_id().id())
        .execute(connection)
        .await?;
    let valid=sql_query("SELECT true AS valid FROM tokens t JOIN users u ON u.id=t.principal_id WHERE t.id=$1 AND t.principal_id=$2 AND t.revoked_at IS NULL AND NOT t.permission_scoped AND NOT t.resource_scoped AND u.anonymized_at IS NULL AND t.issued <= $3 AND (t.expires_at IS NULL OR t.expires_at >= $4) FOR UPDATE OF t")
        .bind::<Integer,_>(actor.token_id().id()).bind::<Integer,_>(actor.principal_id().id())
        .bind::<Timestamp,_>(record.lifecycle().created_at().naive_utc()).bind::<Timestamp,_>(lease.valid_until().naive_utc())
        .get_result::<Valid>(connection).await.optional()?.is_some_and(|row|row.valid);
    if !valid {
        return Ok(false);
    }
    if let Some(group) = lease.recovery_group() {
        // The helper returns a policy error, not a SQL error; the transaction
        // remains usable so invalidation can commit.
        match security_candidates::require_local_recovery_administrator(
            connection,
            actor.principal_id().id(),
            group,
        )
        .await
        {
            Ok(()) => {}
            Err(error) if error.kind() == StorageErrorKind::PermissionDenied => {
                return Ok(false);
            }
            Err(error) => return Err(error),
        }
    }
    Ok(true)
}
async fn fence(
    connection: &mut PostgresConnection,
    record: &mut StorageInteractionRecord,
    revision: ResourceRevision,
    at: DateTime<Utc>,
) -> Result<Option<InteractionTransitionError>, PostgresStorageError> {
    if !matches!(
        record.lifecycle().state(at),
        InteractionState::Pending | InteractionState::Ready
    ) {
        return Ok(None);
    }
    let error = if record.lifecycle().binding().revision() != revision {
        Some(InteractionTransitionError::ConfigurationChanged)
    } else if !actor_valid(connection, record, at).await? {
        Some(InteractionTransitionError::Invalid)
    } else {
        None
    };
    if error.is_some() {
        record.invalidate();
    }
    Ok(error)
}

pub async fn create(
    runtime: &PostgresRuntime,
    request: StorageInteractionCreate,
) -> Result<StorageInteractionView, PostgresStorageError> {
    runtime.with_transaction(async move |connection| -> Result<_, PostgresStorageError> {
        sql_query("SELECT pg_advisory_xact_lock(575,1)").execute(connection).await?;
        let revision=settings(connection).await?;
        purge_on_connection(connection,128).await?;
        let mut record=request.into_record();
        let at=now(connection).await?;
        if record.lifecycle().created_at()>at || record.lifecycle().expires_at()<=at || fence(connection,&mut record,revision,at).await?.is_some() { return Err(invalid()); }
        if let Some(provider)=record.provider() {
            let compatible=sql_query("SELECT NOT EXISTS(SELECT 1 FROM authentication_provider_bindings WHERE name=$1 AND (provider_id<>$2 OR kind<>$3 OR namespace<>$4)) AS valid")
                .bind::<Text,_>(provider.name()).bind::<SqlUuid,_>(provider.id()).bind::<Text,_>(provider.kind().as_str()).bind::<Text,_>(provider.namespace()).get_result::<Valid>(connection).await?;
            if !compatible.valid { return Err(invalid()); }
        }
        let binding=record.lifecycle().binding();
        let counts=sql_query("WITH interactions AS (SELECT *,created_at<=$3 AND expires_at>$3 AND (document#>>'{lifecycle,phase,state}' IN ('pending','ready','awaiting_approval') OR (document#>>'{lifecycle,phase,state}'='authenticating' AND (document#>>'{lifecycle,phase,deadline}')::timestamptz>$3)) AS active FROM auth_interactions) SELECT count(*) AS total,count(*) FILTER(WHERE active) AS active,count(*) FILTER(WHERE active AND client_id=$1) AS client,count(*) FILTER(WHERE active AND actor_id=$2) AS actor FROM interactions")
            .bind::<Text,_>(binding.client().as_str()).bind::<Nullable<Integer>,_>(binding.authority().actor().map(|actor|actor.principal_id().id())).bind::<Timestamptz,_>(at).get_result::<Counts>(connection).await?;
        if counts.total>=MAX_AUTH_INTERACTIONS as i64 || counts.active>=MAX_ACTIVE_AUTH_INTERACTIONS as i64 || counts.client>=MAX_ACTIVE_AUTH_INTERACTIONS_PER_CLIENT as i64 || counts.actor>=MAX_ACTIVE_AUTH_INTERACTIONS_PER_ACTOR as i64 { return Err(PostgresStorageError::conflict("Too many authentication interactions")); }
        let written=sql_query("INSERT INTO auth_interactions(id,client_id,actor_id,user_code_digest,created_at,expires_at,retain_until,document) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT DO NOTHING")
            .bind::<SqlUuid,_>(binding.id().value()).bind::<Text,_>(binding.client().as_str())
            .bind::<Nullable<Integer>,_>(binding.authority().actor().map(|actor|actor.principal_id().id()))
            .bind::<Nullable<Text>,_>(record.user_code().map(InteractionDigest::as_str))
            .bind::<Timestamptz,_>(record.lifecycle().created_at()).bind::<Timestamptz,_>(record.lifecycle().expires_at()).bind::<Timestamptz,_>(record.retain_until())
            .bind::<Jsonb,_>(record.to_document().map_err(|error|PostgresStorageError::invalid_input(error.to_string()))?).execute(connection).await?;
        if written!=1 { return Err(PostgresStorageError::conflict("Authentication interaction collision")); }
        Ok(record.view(at))
    }).await
}

pub async fn bind_browser(
    runtime: &PostgresRuntime,
    code: InteractionDigest,
    browser: InteractionBrowserBinding,
) -> Result<StorageInteractionOutcome, PostgresStorageError> {
    runtime
        .with_transaction(async move |connection| -> Result<_, PostgresStorageError> {
            let revision = settings(connection).await?;
            let mut record = sql_query(
                "SELECT document FROM auth_interactions WHERE user_code_digest=$1 FOR UPDATE",
            )
            .bind::<Text, _>(code.as_str())
            .get_result::<Row>(connection)
            .await
            .optional()?
            .ok_or_else(invalid)?
            .record()?;
            let at = now(connection).await?;
            let outcome = if let Some(error) = fence(connection, &mut record, revision, at).await? {
                StorageInteractionOutcome::Rejected(error)
            } else {
                record
                    .bind_browser(browser, at)
                    .map(StorageInteractionOutcome::View)
                    .unwrap_or_else(StorageInteractionOutcome::Rejected)
            };
            record.discard_inactive_evidence(at);
            save(connection, &record).await?;
            Ok(outcome)
        })
        .await
}

async fn login(
    connection: &mut PostgresConnection,
    record: &StorageInteractionRecord,
    evidence: &StorageInteractionLogin,
    subject: &str,
) -> Result<StorageLogin, PostgresStorageError> {
    let id = evidence.subject().principal_id().id();
    let identity=sql_query("SELECT s.name AS scope,s.provider_kind AS kind,p.external_subject AS subject,(NOT p.provider_managed OR p.last_sync_success_at IS NOT NULL) AS eligible FROM principals p JOIN identity_scopes s ON s.id=p.identity_scope_id WHERE p.id=$1 FOR UPDATE OF p")
        .bind::<Integer,_>(id).get_result::<Identity>(connection).await.optional()?.ok_or_else(invalid)?;
    if !identity.eligible
        || identity.scope != evidence.subject().identity_scope()
        || identity.kind != evidence.subject().authentication_provider_kind()
        || match record.provider() {
            Some(provider) => {
                provider.name() != identity.scope
                    || provider.kind().as_str() != identity.kind
                    || identity.subject.as_deref() != Some(subject)
            }
            None => identity.scope != "local" || subject != id.to_string(),
        }
    {
        return Err(invalid());
    }
    let user = crate::schema::users::table
        .find(id)
        .select(UserRow::as_select())
        .first::<UserRow>(connection)
        .await?
        .into_storage()?;
    evidence.restore_login(user).map_err(|_| invalid())
}

pub async fn advance(
    runtime: &PostgresRuntime,
    command: StorageInteractionCommand,
) -> Result<StorageInteractionOutcome, PostgresStorageError> {
    runtime
        .with_transaction(async move |connection| -> Result<_, PostgresStorageError> {
            let revision = settings(connection).await?;
            let mut record = load(connection, command.authorization().id().value()).await?;
            if !record.authorizes(&command) {
                return Err(invalid());
            }
            let at = now(connection).await?;
            let outcome = if let Some(error) = fence(connection, &mut record, revision, at).await? {
                StorageInteractionOutcome::Rejected(error)
            } else {
                if let StorageInteractionAction::Authenticate { authentication, .. } =
                    command.action()
                    && let StorageInteractionEvidence::Login(evidence) = authentication.evidence()
                {
                    login(
                        connection,
                        &record,
                        evidence,
                        authentication.proof().subject(),
                    )
                    .await?;
                }
                record.apply(command, at)
            };
            save(connection, &record).await?;
            Ok(outcome)
        })
        .await
}

pub async fn complete(
    runtime: &PostgresRuntime,
    request: StorageInteractionComplete,
) -> Result<StorageInteractionCompletion, PostgresStorageError> {
    runtime
        .with_transaction(async move |connection| -> Result<_, PostgresStorageError> {
            let revision = settings(connection).await?;
            let mut record = load(connection, request.access().id().value()).await?;
            if !record.allows(request.access()) {
                return Err(invalid());
            }
            let at = now(connection).await?;
            if let Some(error) = fence(connection, &mut record, revision, at).await? {
                save(connection, &record).await?;
                return Ok(StorageInteractionCompletion::Rejected(error));
            }
            let evidence = match record.ready(request.access(), at) {
                Ok(evidence) => evidence.clone(),
                Err(error) => {
                    save(connection, &record).await?;
                    return Ok(StorageInteractionCompletion::Rejected(error));
                }
            };
            if !record.accepts_issuance(request.issuance()) {
                return Ok(StorageInteractionCompletion::Rejected(
                    InteractionTransitionError::Invalid,
                ));
            }
            let (outcome, issued) = match (evidence, request.issuance()) {
                (
                    StorageInteractionEvidence::Login(evidence),
                    StorageInteractionIssuance::Session {
                        credentials,
                        policy,
                        context,
                    },
                ) => {
                    let proof = record
                        .lifecycle()
                        .authenticated_subject()
                        .ok_or_else(invalid)?;
                    let login = login(connection, &record, &evidence, proof.subject()).await?;
                    let session = auth_sessions::create_on_connection(
                        connection,
                        StorageSessionCreate::new(
                            login,
                            SessionClientId::try_new(
                                record.lifecycle().binding().client().as_str(),
                            )
                            .map_err(|_| invalid())?,
                            credentials.clone(),
                            *policy,
                            context.clone(),
                        ),
                    )
                    .await?;
                    let id = session.clone().into_value().session_id().value();
                    (
                        StorageInteractionCompletion::Session(session),
                        InteractionIssuedResult::Session(id),
                    )
                }
                (
                    StorageInteractionEvidence::Login(evidence),
                    StorageInteractionIssuance::Approval { claim, context },
                ) => {
                    let proof = record
                        .lifecycle()
                        .authenticated_subject()
                        .ok_or_else(invalid)?;
                    login(connection, &record, &evidence, proof.subject()).await?;
                    let approval = credential_approval::create_on_connection(
                        connection,
                        StorageCredentialApprovalCreate::new(
                            claim.clone(),
                            evidence.authenticated_at(),
                            context.clone(),
                        ),
                    )
                    .await?;
                    let id = approval.clone().into_value().id();
                    (
                        StorageInteractionCompletion::Approval(approval),
                        InteractionIssuedResult::Approval(id),
                    )
                }
                (
                    StorageInteractionEvidence::Verification(candidate),
                    StorageInteractionIssuance::ProviderVerification,
                ) => {
                    let id = candidate.id();
                    security_candidates::verify_on_connection(connection, *candidate).await?;
                    (
                        StorageInteractionCompletion::ProviderVerification(id),
                        InteractionIssuedResult::ProviderVerification(id),
                    )
                }
                _ => {
                    return Ok(StorageInteractionCompletion::Rejected(
                        InteractionTransitionError::Invalid,
                    ));
                }
            };
            let completed = now(connection).await?;
            if !actor_valid(connection, &record, completed).await? {
                return Err(invalid());
            }
            record.consume(issued, completed).map_err(|_| invalid())?;
            save(connection, &record).await?;
            Ok(outcome)
        })
        .await
}

pub async fn revoke(
    runtime: &PostgresRuntime,
    access: StorageInteractionAccess,
    context: EventContext,
) -> Result<StorageMutationOutcome<usize>, PostgresStorageError> {
    runtime
        .with_transaction(async move |connection| -> Result<_, PostgresStorageError> {
            // No revision/actor rejection: cleanup remains available after a
            // provider change or revocation of the original initiating credential.
            settings(connection).await?;
            let record = load(connection, access.id().value()).await?;
            if !record.allows(&access) || now(connection).await? >= record.retain_until() {
                return Err(invalid());
            }
            let Some(InteractionIssuedResult::Session(id)) = record.lifecycle().issued_result()
            else {
                return Err(PostgresStorageError::conflict(
                    "Interaction has not issued a session",
                ));
            };
            auth_sessions::revoke_on_connection(
                connection,
                SessionRevocation::Session(AuthSessionId::new(id)),
                context,
            )
            .await
        })
        .await
}

async fn purge_on_connection(
    connection: &mut PostgresConnection,
    limit: usize,
) -> Result<usize, PostgresStorageError> {
    if limit > 4096 {
        return Err(PostgresStorageError::invalid_input(
            "Interaction cleanup limit must be at most 4096",
        ));
    }
    Ok(sql_query("DELETE FROM auth_interactions WHERE id IN (SELECT id FROM auth_interactions WHERE retain_until<=clock_timestamp() ORDER BY retain_until,id LIMIT $1 FOR UPDATE SKIP LOCKED)")
        .bind::<BigInt,_>(limit as i64).execute(connection).await?)
}
pub async fn purge(runtime: &PostgresRuntime, limit: usize) -> Result<usize, PostgresStorageError> {
    runtime
        .with_transaction(async move |connection| purge_on_connection(connection, limit).await)
        .await
}
