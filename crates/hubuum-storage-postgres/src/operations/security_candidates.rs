//! Ephemeral verification and atomic activation over the shared settings authority.
use chrono::Utc;
use diesel::sql_types::{BigInt, Integer, Jsonb, Text, Timestamptz, Uuid as SqlUuid};
use diesel::{OptionalExtension, QueryableByName, sql_query};
use diesel_async::RunQueryDsl;
use hubuum_domain::IdentityScopeName;
use hubuum_storage_core::{
    MAX_SECURITY_CANDIDATES, MAX_SECURITY_CANDIDATES_PER_ACTOR, StorageAuthenticationProviderKind,
    StorageCredentialOperation, StorageProviderIdentityBinding,
    StorageRuntimeSettingsUpdateOutcome, StorageRuntimeSettingsValue, StorageSecurityCandidate,
    StorageSecurityCandidateActivation, StorageSecurityCandidateOwner,
};
use serde_json::Value;
use uuid::Uuid;

use crate::{PostgresConnection, PostgresRuntime, PostgresStorageError};

#[derive(QueryableByName)]
struct CandidateRow {
    #[diesel(sql_type = Jsonb)]
    document: Value,
}

impl CandidateRow {
    fn candidate(self) -> Result<StorageSecurityCandidate, PostgresStorageError> {
        StorageSecurityCandidate::from_document(self.document)
            .map_err(|_| PostgresStorageError::database("Invalid persisted security candidate"))
    }
}

#[derive(QueryableByName)]
struct CandidateCount {
    #[diesel(sql_type = BigInt)]
    total: i64,
    #[diesel(sql_type = BigInt)]
    actor: i64,
}

pub async fn create(
    runtime: &PostgresRuntime,
    candidate: StorageSecurityCandidate,
) -> Result<(), PostgresStorageError> {
    runtime.with_transaction(async |connection| -> Result<_, PostgresStorageError> {
        // Serialize bounded admission across actors and replicas. Activation never
        // takes this lock, and expired candidates carry no reusable authority.
        sql_query("SELECT pg_advisory_xact_lock(574, 1)").execute(connection).await?;
        sql_query("DELETE FROM security_candidates WHERE expires_at <= clock_timestamp()")
            .execute(connection).await?;
        let count = sql_query("SELECT count(*) AS total, count(*) FILTER (WHERE actor_id=$1) AS actor FROM security_candidates")
            .bind::<Integer,_>(candidate.owner().actor_id().id()).get_result::<CandidateCount>(connection).await?;
        if count.total >= MAX_SECURITY_CANDIDATES as i64 || count.actor >= MAX_SECURITY_CANDIDATES_PER_ACTOR as i64 {
            return Err(PostgresStorageError::conflict("Too many active security candidates"));
        }
        if candidate.expires_at() <= Utc::now() || candidate.created_at() > Utc::now() || candidate.checks().is_empty() {
            return Err(PostgresStorageError::invalid_input("Invalid security candidate lifetime or checks"));
        }
        let inserted = sql_query("INSERT INTO security_candidates (id,actor_id,token_id,expires_at,document) \
            SELECT $1,$2,$3,$4,$5 FROM runtime_settings WHERE setting_group=$6 AND revision=$7")
            .bind::<SqlUuid,_>(candidate.id()).bind::<Integer,_>(candidate.owner().actor_id().id())
            .bind::<Integer,_>(candidate.owner().token_id().id()).bind::<Timestamptz,_>(candidate.expires_at())
            .bind::<Jsonb,_>(candidate.to_document()).bind::<diesel::sql_types::Text,_>(candidate.assignment().group().as_str())
            .bind::<BigInt,_>(candidate.base_revision().get()).execute(connection).await?;
        if inserted != 1 {
            let actual=sql_query("SELECT revision FROM runtime_settings WHERE setting_group=$1").bind::<Text,_>(candidate.assignment().group().as_str()).get_result::<SettingsRevision>(connection).await?;
            return Err(PostgresStorageError::revision_conflict("Security settings changed", hubuum_domain::ResourceRevision::new(actual.revision).map_err(|error| PostgresStorageError::database(error.to_string()))?));
        }
        Ok(())
    }).await
}

pub async fn get(
    runtime: &PostgresRuntime,
    id: Uuid,
    owner: StorageSecurityCandidateOwner,
) -> Result<StorageSecurityCandidate, PostgresStorageError> {
    runtime.with_connection(async |connection| -> Result<_, PostgresStorageError> {
        sql_query("SELECT document FROM security_candidates WHERE id=$1 AND actor_id=$2 AND token_id=$3")
            .bind::<SqlUuid,_>(id).bind::<Integer,_>(owner.actor_id().id()).bind::<Integer,_>(owner.token_id().id())
            .get_result::<CandidateRow>(connection).await.optional()?
            .ok_or_else(|| PostgresStorageError::not_found("Security candidate not found"))?.candidate()
    }).await
}

async fn lock(
    connection: &mut PostgresConnection,
    id: Uuid,
) -> Result<StorageSecurityCandidate, PostgresStorageError> {
    sql_query("SELECT document FROM security_candidates WHERE id=$1 FOR UPDATE")
        .bind::<SqlUuid, _>(id)
        .get_result::<CandidateRow>(connection)
        .await
        .optional()?
        .ok_or_else(|| PostgresStorageError::not_found("Security candidate not found"))?
        .candidate()
}

async fn save(
    connection: &mut PostgresConnection,
    candidate: &StorageSecurityCandidate,
) -> Result<(), PostgresStorageError> {
    sql_query("UPDATE security_candidates SET document=$2 WHERE id=$1")
        .bind::<SqlUuid, _>(candidate.id())
        .bind::<Jsonb, _>(candidate.to_document())
        .execute(connection)
        .await?;
    Ok(())
}

pub async fn verify(
    runtime: &PostgresRuntime,
    candidate: StorageSecurityCandidate,
) -> Result<(), PostgresStorageError> {
    runtime
        .with_transaction(async |connection| -> Result<_, PostgresStorageError> {
            let mut stored = lock(connection, candidate.id()).await?;
            stored
                .accept_verification(candidate, database_now(connection).await?)
                .map_err(|error| PostgresStorageError::conflict(error.to_string()))?;
            save(connection, &stored).await
        })
        .await
}

pub async fn activate(
    runtime: &PostgresRuntime,
    request: StorageSecurityCandidateActivation,
) -> Result<StorageRuntimeSettingsUpdateOutcome, PostgresStorageError> {
    runtime.with_transaction(async |connection| -> Result<_, PostgresStorageError> {
        // Keep principal/token/approval locks ahead of candidate/settings locks.
        super::credential_approval::consume(connection, Some(request.claim()), StorageCredentialOperation::ActivateSecurityCandidate, None, request.update().context()).await?;
        require_local_recovery_administrator(connection, request.claim().actor_id().id(), request.recovery_group()).await?;
        let mut candidate = lock(connection, request.candidate_id()).await?;
        candidate.activate(&request, database_now(connection).await?).map_err(|error| PostgresStorageError::conflict(error.to_string()))?;
        let previous = sql_query("SELECT value AS document FROM runtime_settings WHERE setting_group=$1 FOR UPDATE")
            .bind::<Text,_>(request.update().group().as_str()).get_result::<SettingsDocument>(connection).await?;
        let previous = hubuum_storage_core::StorageRuntimeSettingsAssignment::from_document(request.update().group(), previous.document.as_ref())
            .map_err(|error| PostgresStorageError::database(error.to_string()))?;
        let outcome = super::runtime_settings::update_on_connection(connection, request.update()).await?;
        if request.update().group() == hubuum_storage_core::StorageRuntimeSettingsGroup::Authentication {
            let scopes = sql_query("SELECT name FROM identity_scopes WHERE name <> 'local'").load::<ScopeName>(connection).await?;
            let revoked: Vec<_> = scopes.into_iter().filter(|scope| !candidate.retains_authenticated_provider(&previous, &scope.name)).map(|scope| scope.name).collect();
            invalidate_provider_credentials(connection, &revoked).await?;
        }
        if let Some(StorageRuntimeSettingsValue::Authentication(providers)) = candidate.verification_assignment().value() {
            for provider in providers.providers() {
                let existing = sql_query("SELECT name, provider_kind AS kind FROM identity_scopes WHERE name=$1 FOR UPDATE")
                    .bind::<Text,_>(provider.name()).get_result::<ExistingScope>(connection).await.optional()?;
                if existing.is_some_and(|scope| scope.kind != provider.kind().as_str()) {
                    return Err(PostgresStorageError::conflict("Existing identity scope belongs to another provider kind"));
                }
                let binding = StorageProviderIdentityBinding::from_provider(provider).map_err(|error| PostgresStorageError::invalid_input(error.to_string()))?;
                let written = sql_query("INSERT INTO authentication_provider_bindings (name,provider_id,kind,namespace) VALUES ($1,$2,$3,$4) ON CONFLICT (name) DO UPDATE SET name=EXCLUDED.name WHERE authentication_provider_bindings.provider_id=EXCLUDED.provider_id AND authentication_provider_bindings.kind=EXCLUDED.kind AND authentication_provider_bindings.namespace=EXCLUDED.namespace")
                    .bind::<Text,_>(binding.name()).bind::<SqlUuid,_>(binding.id()).bind::<Text,_>(binding.kind().as_str()).bind::<Text,_>(binding.namespace()).execute(connection).await?;
                if written != 1 { return Err(PostgresStorageError::conflict("Provider identity namespace is immutable; use a new scope")); }
            }
        }
        save(connection, &candidate).await?;
        Ok(outcome)
    }).await
}

#[derive(QueryableByName)]
struct ProviderBindingRow {
    #[diesel(sql_type = Text)]
    name: String,
    #[diesel(sql_type = SqlUuid)]
    provider_id: Uuid,
    #[diesel(sql_type = Text)]
    kind: String,
    #[diesel(sql_type = Text)]
    namespace: String,
}

pub async fn provider_binding(
    runtime: &PostgresRuntime,
    name: IdentityScopeName,
) -> Result<Option<StorageProviderIdentityBinding>, PostgresStorageError> {
    runtime.with_connection(async |connection| -> Result<_, PostgresStorageError> {
        sql_query("SELECT name,provider_id,kind,namespace FROM authentication_provider_bindings WHERE name=$1")
            .bind::<Text,_>(name.as_str()).get_result::<ProviderBindingRow>(connection).await.optional()?
            .map(|row| {
                let kind = StorageAuthenticationProviderKind::parse(&row.kind).map_err(|error| PostgresStorageError::database(error.to_string()))?;
                StorageProviderIdentityBinding::try_new(row.name, row.provider_id, kind, row.namespace).map_err(|error| PostgresStorageError::database(error.to_string()))
            }).transpose()
    }).await
}

pub async fn purge(runtime: &PostgresRuntime) -> Result<usize, PostgresStorageError> {
    runtime
        .with_connection(async |connection| -> Result<_, PostgresStorageError> {
            Ok(
                sql_query("DELETE FROM security_candidates WHERE expires_at <= clock_timestamp()")
                    .execute(connection)
                    .await?,
            )
        })
        .await
}

#[derive(QueryableByName)]
struct ExistingScope {
    #[diesel(sql_type = Text)]
    kind: String,
}

#[derive(QueryableByName)]
struct RecoveryAdministrator {
    #[diesel(sql_type = Integer)]
    id: i32,
}

async fn require_local_recovery_administrator(
    connection: &mut PostgresConnection,
    actor: i32,
    group: &str,
) -> Result<(), PostgresStorageError> {
    // Principal is already locked by approval consumption. Protect the local
    // password and membership rows through the settings commit as well.
    let admin = sql_query(
        "SELECT p.id FROM principals p JOIN users u ON u.id=p.id \
        JOIN identity_scopes ps ON ps.id=p.identity_scope_id \
        JOIN group_memberships m ON m.principal_id=p.id JOIN groups g ON g.id=m.group_id \
        JOIN identity_scopes gs ON gs.id=g.identity_scope_id \
        WHERE p.id=$1 AND ps.name='local' AND NOT p.provider_managed \
        AND u.password IS NOT NULL AND u.anonymized_at IS NULL \
        AND g.groupname=$2 AND gs.name='local' AND g.managed_by='local' \
        FOR UPDATE OF u,m,g",
    )
    .bind::<Integer, _>(actor)
    .bind::<Text, _>(group)
    .get_result::<RecoveryAdministrator>(connection)
    .await
    .optional()?;
    if admin.is_none_or(|admin| admin.id != actor) {
        return Err(PostgresStorageError::permission_denied(
            "Local recovery administrator access changed",
        ));
    }
    Ok(())
}

/// Pin external login issuance to the configuration used before authentication.
/// This shared lock precedes principal locks; activation takes the exclusive
/// settings lock before revoking external principals' credentials.
pub(super) async fn fence_login(
    connection: &mut PostgresConnection,
    login: &hubuum_storage_core::StorageLogin,
) -> Result<(), PostgresStorageError> {
    if let Some(expected) = login.authentication_revision() {
        let actual = sql_query(
            "SELECT revision FROM runtime_settings WHERE setting_group='authentication' FOR SHARE",
        )
        .get_result::<SettingsRevision>(connection)
        .await?;
        if actual.revision != expected.get() {
            return Err(PostgresStorageError::authentication_required(
                "Authentication configuration changed; sign in again",
            ));
        }
    }
    Ok(())
}

#[derive(QueryableByName)]
struct SettingsRevision {
    #[diesel(sql_type = BigInt)]
    revision: i64,
}

#[derive(QueryableByName)]
struct SettingsDocument {
    #[diesel(sql_type = diesel::sql_types::Nullable<Jsonb>)]
    document: Option<Value>,
}

#[derive(QueryableByName)]
struct ScopeName {
    #[diesel(sql_type = Text)]
    name: String,
}

async fn invalidate_provider_credentials(
    connection: &mut PostgresConnection,
    scopes: &[String],
) -> Result<(), PostgresStorageError> {
    use diesel::sql_types::Array;
    if scopes.is_empty() {
        return Ok(());
    }
    // The authentication settings lock fences issuance before principal locks.
    sql_query("SELECT p.id FROM principals p JOIN identity_scopes s ON s.id=p.identity_scope_id WHERE s.name=ANY($1) ORDER BY p.id FOR UPDATE OF p")
        .bind::<Array<Text>,_>(scopes).execute(connection).await?;
    sql_query("UPDATE auth_sessions SET revoked_at=timezone('UTC',clock_timestamp()), revocation_reason='provider_configuration_changed' WHERE revoked_at IS NULL AND principal_id IN (SELECT p.id FROM principals p JOIN identity_scopes s ON s.id=p.identity_scope_id WHERE s.name=ANY($1))")
        .bind::<Array<Text>,_>(scopes).execute(connection).await?;
    sql_query("UPDATE tokens SET revoked_at=timezone('UTC',clock_timestamp()), revision=revision+1 WHERE revoked_at IS NULL AND principal_id IN (SELECT p.id FROM principals p JOIN identity_scopes s ON s.id=p.identity_scope_id WHERE s.name=ANY($1))")
        .bind::<Array<Text>,_>(scopes).execute(connection).await?;
    sql_query("UPDATE credential_approvals SET invalidated_at=timezone('UTC',clock_timestamp()) WHERE consumed_at IS NULL AND invalidated_at IS NULL AND actor_id IN (SELECT p.id FROM principals p JOIN identity_scopes s ON s.id=p.identity_scope_id WHERE s.name=ANY($1))")
        .bind::<Array<Text>,_>(scopes).execute(connection).await?;
    Ok(())
}

/// Runs without an application permission backend, using explicit privileged CLI authority.
pub async fn recover(
    runtime: &PostgresRuntime,
    request: hubuum_storage_core::StorageSecurityRecovery,
) -> Result<[StorageRuntimeSettingsUpdateOutcome; 2], PostgresStorageError> {
    use hubuum_events_core::{
        Action, AuditDocument, EntityType, EventContext, EventEntityId, NewEvent,
    };
    runtime.with_transaction(async |connection| -> Result<_, PostgresStorageError> {
        sql_query("SELECT id FROM principals WHERE id=$1 FOR UPDATE").bind::<Integer,_>(request.administrator().id()).execute(connection).await?;
        require_local_recovery_administrator(connection, request.administrator().id(), request.recovery_group()).await?;
        let authentication = super::runtime_settings::update_on_connection(connection, &request.updates()[0]).await?;
        let authorization = super::runtime_settings::update_on_connection(connection, &request.updates()[1]).await?;
        let scopes = sql_query("SELECT name FROM identity_scopes WHERE name <> 'local'").load::<ScopeName>(connection).await?;
        invalidate_provider_credentials(connection, &scopes.into_iter().map(|scope| scope.name).collect::<Vec<_>>()).await?;
        sql_query("DELETE FROM security_candidates").execute(connection).await?;
        sql_query("UPDATE credential_approvals SET invalidated_at=timezone('UTC',clock_timestamp()) WHERE consumed_at IS NULL AND invalidated_at IS NULL").execute(connection).await?;
        let context = EventContext::system();
        let document = AuditDocument::builder("Offline security recovery: local authorization and local-only authentication")
            .metadata(serde_json::json!({"recovery_administrator_id": request.administrator().id(), "operation": "offline_security_recovery"})).try_build()?;
        let event = NewEvent::from_document(EntityType::Authentication, Action::Updated, context.actor_kind(), document)
            .map_err(|error| PostgresStorageError::invalid_input(error.to_string()))?
            .with_context(&context).with_entity_id(EventEntityId::new(request.administrator().id())?);
        super::event_record::append_event(connection, &event).await?;
        Ok([authentication, authorization])
    }).await
}

async fn database_now(
    connection: &mut PostgresConnection,
) -> Result<chrono::DateTime<Utc>, PostgresStorageError> {
    Ok(
        diesel::select(diesel::dsl::sql::<Timestamptz>("clock_timestamp()"))
            .get_result(connection)
            .await?,
    )
}
