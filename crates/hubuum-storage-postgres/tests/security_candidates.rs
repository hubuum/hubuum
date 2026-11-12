#![cfg(feature = "integration-test-support")]

mod auth_interactions;

use chrono::{Duration, Utc};
use diesel_async::SimpleAsyncConnection;
use hubuum_domain::{PrincipalId, ResourceRevision, TokenId};
use hubuum_events_core::EventContext;
use hubuum_storage_core::*;
use hubuum_storage_postgres::test_support::{
    database_role_tests_enabled, integration_test_database_roles, integration_test_database_url,
    integration_test_migration_pool, integration_test_pool,
};
use hubuum_storage_postgres::{
    PostgresPool, PostgresPoolSettings, PostgresStorage, PostgresStorageError, build_postgres_pool,
    with_transaction,
};
use uuid::Uuid;

const LEGACY: &str =
    include_str!("../migrations/2026-10-19-000001_query_observation_control/up.sql");
const SETTINGS: &str = include_str!("../migrations/2026-11-06-000001_runtime_settings/up.sql");
const POLICIES: &str = include_str!("../migrations/2026-11-10-000001_runtime_policies/up.sql");
const APPROVALS: &str = include_str!("../migrations/2026-09-19-000001_credential_approvals/up.sql");
const INTERACTIONS: &str = include_str!("../migrations/2026-11-12-000002_auth_interactions/up.sql");
const CANDIDATES: &str = include_str!("../migrations/2026-11-12-000001_security_candidates/up.sql");

/// Independent security authority per test, using current production table shapes
/// and the actual settings/candidate migrations. Connections use the runtime role.
struct Fixture {
    schema: String,
    migration_pool: PostgresPool,
}
impl Fixture {
    async fn new() -> Self {
        let f = Self {
            schema: format!("security_candidates_{}", Uuid::new_v4().simple()),
            migration_pool: if database_role_tests_enabled() {
                integration_test_migration_pool(1)
            } else {
                integration_test_pool(1)
            },
        };
        let tables = [
            "principals",
            "users",
            "groups",
            "group_memberships",
            "identity_scopes",
            "tokens",
            "auth_sessions",
            "refresh_tokens",
            "events",
        ]
        .map(|table| format!("CREATE TABLE {table} (LIKE public.{table} INCLUDING ALL);"))
        .join("\n");
        f.owner(&format!("CREATE SCHEMA {schema}; SET LOCAL search_path TO {schema}; {tables} {APPROVALS} {LEGACY} {SETTINGS} {POLICIES} {CANDIDATES} {INTERACTIONS}
            -- LIKE INCLUDING ALL does not copy triggers. Reuse the production
            -- revision guard so credential revocation cannot bypass its contract.
            CREATE TRIGGER tokens_revision BEFORE INSERT OR UPDATE ON tokens
                FOR EACH ROW EXECUTE FUNCTION public.hubuum_manage_resource_revision('none', 'coalesce', 'last_used_at');
            CREATE SEQUENCE fixture_token_ids START 2;
            ALTER TABLE tokens ALTER COLUMN id SET DEFAULT nextval('fixture_token_ids');
            INSERT INTO identity_scopes(id,name,provider_kind) VALUES(1,'local','local');
            INSERT INTO principals(id,kind,name,identity_scope_id) VALUES(1,'human','recovery_admin',1);
            INSERT INTO users(id,kind,password) VALUES(1,'human','fixture-password-hash');
            INSERT INTO groups(id,groupname,description,identity_scope_id,managed_by) VALUES(1,'admin','recovery',1,'local');
            INSERT INTO group_memberships(principal_id,group_id) VALUES(1,1);
            INSERT INTO tokens(id,token,principal_id,expires_at) VALUES(1,'fixture-token',1,now()+interval '1 hour');
            {grants}", schema=f.schema, grants=f.grants())).await.unwrap();
        f
    }
    fn grants(&self) -> String {
        if !database_role_tests_enabled() {
            return String::new();
        }
        let role = integration_test_database_roles()
            .runtime()
            .as_str()
            .to_owned();
        format!(
            "GRANT USAGE ON SCHEMA {schema} TO \"{role}\"; GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA {schema} TO \"{role}\"; GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA {schema} TO \"{role}\";",
            schema = self.schema
        )
    }
    async fn owner(&self, sql: &str) -> Result<(), PostgresStorageError> {
        with_transaction(
            &self.migration_pool,
            async |connection| -> Result<(), PostgresStorageError> {
                if database_role_tests_enabled() {
                    connection
                        .batch_execute(&format!(
                            "SET LOCAL ROLE \"{}\"",
                            integration_test_database_roles().owner().as_str()
                        ))
                        .await?;
                }
                connection
                    .batch_execute(&format!("SET LOCAL search_path TO {}; {sql}", self.schema))
                    .await?;
                Ok(())
            },
        )
        .await
    }
    fn replica(&self) -> PostgresStorage {
        let url = integration_test_database_url();
        let separator = if url.contains('?') { '&' } else { '?' };
        let settings = PostgresPoolSettings::builder(format!(
            "{url}{separator}options=-csearch_path%3D{},pg_catalog",
            self.schema
        ))
        .max_size(2)
        .acquire_timeout_ms(5000)
        .build()
        .unwrap();
        PostgresStorage::unobserved(build_postgres_pool(&settings).unwrap())
    }
    async fn cleanup(self) {
        self.owner(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .await
            .unwrap();
    }
}
fn digest(c: char) -> StorageCredentialFingerprint {
    StorageCredentialFingerprint::new(c.to_string().repeat(64)).unwrap()
}
fn owner() -> StorageSecurityCandidateOwner {
    StorageSecurityCandidateOwner::new(PrincipalId::new(1).unwrap(), TokenId::new(1).unwrap())
}
fn context() -> EventContext {
    EventContext::user(owner().actor_id(), None, None)
}
fn update() -> StorageRuntimeSettingsUpdate {
    StorageRuntimeSettingsUpdate::new(
        StorageRuntimeSettingsAssignment::Managed(StorageRuntimeSettingsValue::Authentication(
            StorageAuthenticationProviders::try_new(Vec::new()).unwrap(),
        )),
        ResourceRevision::INITIAL,
        context(),
    )
}
fn candidate() -> StorageSecurityCandidate {
    let checks = [
        StorageSecurityCheckKind::Configuration,
        StorageSecurityCheckKind::RecoveryAdministrator,
    ]
    .into_iter()
    .map(|kind| {
        StorageSecurityCheck::try_new(kind, StorageSecurityCheckStatus::Passed, None, "verified")
            .unwrap()
    })
    .collect();
    StorageSecurityCandidate::try_new(
        Uuid::new_v4(),
        owner(),
        &update(),
        StorageSecurityCandidateBinding::new(digest('a'), digest('b'), digest('c')),
    )
    .unwrap()
    .with_checks(checks)
    .unwrap()
    .verify(digest('d'))
    .unwrap()
}
async fn approval(backend: &PostgresStorage, candidate_id: Uuid) -> (StorageCredentialClaim, i32) {
    let claim = StorageCredentialClaim::new(
        owner().actor_id(),
        owner().token_id(),
        StorageCredentialOperation::ActivateSecurityCandidate,
        digest('e'),
        digest('f'),
        Utc::now() - Duration::hours(24),
    )
    .security_candidate(candidate_id);
    let id = backend
        .create_credential_approval(StorageCredentialApprovalCreate::new(
            claim.clone(),
            Utc::now(),
            context(),
        ))
        .await
        .unwrap()
        .into_value()
        .id();
    (claim, id)
}
fn activation(
    candidate: &StorageSecurityCandidate,
    claim: StorageCredentialClaim,
) -> StorageSecurityCandidateActivation {
    StorageSecurityCandidateActivation::try_new(
        candidate,
        digest('d'),
        update(),
        claim,
        "admin".into(),
    )
    .unwrap()
}

#[tokio::test]
async fn competing_replicas_consume_candidate_and_approval_once() {
    let f = Fixture::new().await;
    let first = f.replica();
    let second = f.replica();
    let candidate = candidate();
    first
        .create_security_candidate(candidate.clone())
        .await
        .unwrap();
    let (claim, id) = approval(&first, candidate.id()).await;
    let (a, b) = tokio::join!(
        first.activate_security_candidate(activation(&candidate, claim.clone())),
        second.activate_security_candidate(activation(&candidate, claim))
    );
    assert_eq!(
        usize::from(a.is_ok()) + usize::from(b.is_ok()),
        1,
        "{a:?} {b:?}"
    );
    assert!(
        second
            .get_credential_approval(id)
            .await
            .unwrap()
            .consumed_at()
            .is_some()
    );
    assert_eq!(
        second
            .get_security_candidate(candidate.id(), owner())
            .await
            .unwrap()
            .state(Utc::now()),
        StorageSecurityCandidateState::Activated
    );
    f.cleanup().await;
}

#[tokio::test]
async fn failed_settings_audit_rolls_back_approval_and_receipt_consumption() {
    let f = Fixture::new().await;
    let backend = f.replica();
    let candidate = candidate();
    backend
        .create_security_candidate(candidate.clone())
        .await
        .unwrap();
    let (claim, id) = approval(&backend, candidate.id()).await;
    f.owner("ALTER TABLE runtime_setting_changes ADD CONSTRAINT reject_fixture_write CHECK (revision < 2)").await.unwrap();
    assert!(
        backend
            .activate_security_candidate(activation(&candidate, claim.clone()))
            .await
            .is_err()
    );
    assert!(
        backend
            .get_credential_approval(id)
            .await
            .unwrap()
            .consumed_at()
            .is_none()
    );
    assert_eq!(
        backend
            .get_security_candidate(candidate.id(), owner())
            .await
            .unwrap()
            .state(Utc::now()),
        StorageSecurityCandidateState::Verified
    );
    f.owner("ALTER TABLE runtime_setting_changes DROP CONSTRAINT reject_fixture_write")
        .await
        .unwrap();
    backend
        .activate_security_candidate(activation(&candidate, claim))
        .await
        .unwrap();
    f.cleanup().await;
}

#[tokio::test]
async fn lost_recovery_grant_prevents_activation_without_spending_approval() {
    let f = Fixture::new().await;
    let backend = f.replica();
    let candidate = candidate();
    backend
        .create_security_candidate(candidate.clone())
        .await
        .unwrap();
    let (claim, id) = approval(&backend, candidate.id()).await;
    f.owner("DELETE FROM group_memberships WHERE principal_id=1")
        .await
        .unwrap();
    let error = backend
        .activate_security_candidate(activation(&candidate, claim))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), StorageErrorKind::PermissionDenied);
    assert!(
        backend
            .get_credential_approval(id)
            .await
            .unwrap()
            .consumed_at()
            .is_none()
    );
    f.cleanup().await;
}

#[tokio::test]
async fn offline_recovery_discards_authority_and_persists_across_replica_restart() {
    let f = Fixture::new().await;
    let backend = f.replica();
    let candidate = candidate();
    backend
        .create_security_candidate(candidate.clone())
        .await
        .unwrap();
    let (_, approval) = approval(&backend, candidate.id()).await;
    let snapshot = backend.load_runtime_settings().await.unwrap();
    let request =
        StorageSecurityRecovery::try_new(owner().actor_id(), "admin".into(), &snapshot).unwrap();
    backend.recover_security_settings(request).await.unwrap();
    drop(backend);
    let restarted = f.replica();
    assert!(
        restarted
            .get_security_candidate(candidate.id(), owner())
            .await
            .is_err()
    );
    assert!(
        restarted
            .get_credential_approval(approval)
            .await
            .unwrap()
            .invalidated_at()
            .is_some()
    );
    let snapshot = restarted.load_runtime_settings().await.unwrap();
    assert!(snapshot.authentication().unwrap().providers().is_empty());
    assert_eq!(
        snapshot.authorization(),
        Some(StorageAuthorizationBackend::Local)
    );
    f.cleanup().await;
}

#[tokio::test]
async fn recovery_revision_conflict_rolls_back_both_groups() {
    let f = Fixture::new().await;
    let backend = f.replica();
    let snapshot = backend.load_runtime_settings().await.unwrap();
    let recovery =
        StorageSecurityRecovery::try_new(owner().actor_id(), "admin".into(), &snapshot).unwrap();
    backend
        .update_runtime_settings(StorageRuntimeSettingsUpdate::new(
            StorageRuntimeSettingsAssignment::Managed(StorageRuntimeSettingsValue::Authorization(
                StorageAuthorizationBackend::Local,
            )),
            ResourceRevision::INITIAL,
            context(),
        ))
        .await
        .unwrap();
    let error = backend
        .recover_security_settings(recovery)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), StorageErrorKind::RevisionConflict);
    assert_eq!(
        backend
            .load_runtime_settings()
            .await
            .unwrap()
            .record(StorageRuntimeSettingsGroup::Authentication)
            .revision(),
        ResourceRevision::INITIAL
    );
    f.cleanup().await;
}

#[tokio::test]
async fn removing_provider_revokes_its_credentials_and_preserves_manual_grants() {
    let f = Fixture::new().await;
    let backend = f.replica();
    f.owner("INSERT INTO identity_scopes(id,name,provider_kind) VALUES(2,'directory','ldap');
        INSERT INTO principals(id,kind,name,identity_scope_id,provider_managed,last_sync_success_at) VALUES(2,'human','external_user',2,true,now());
        INSERT INTO users(id,kind) VALUES(2,'human');
        INSERT INTO group_memberships(principal_id,group_id) VALUES(2,1);
        INSERT INTO auth_sessions(id,principal_id,client_id,authenticated_at,last_refreshed_at,access_seconds,idle_seconds,absolute_expires_at,idle_expires_at,retain_until)
        VALUES('00000000-0000-4000-8000-000000000002',2,'test',now(),now(),60,120,now()+interval '1 hour',now()+interval '2 minutes',now()+interval '2 hours');
        INSERT INTO tokens(id,token,principal_id,expires_at,auth_session_id,credential_kind) VALUES(2,'external-token',2,now()+interval '1 hour','00000000-0000-4000-8000-000000000002','session_access');").await.unwrap();
    let candidate = candidate();
    backend
        .create_security_candidate(candidate.clone())
        .await
        .unwrap();
    let (claim, _) = approval(&backend, candidate.id()).await;
    backend
        .activate_security_candidate(activation(&candidate, claim))
        .await
        .unwrap();
    f.owner("DO $$ BEGIN
        IF EXISTS(SELECT 1 FROM tokens WHERE id=2 AND revoked_at IS NULL) OR EXISTS(SELECT 1 FROM auth_sessions WHERE principal_id=2 AND revoked_at IS NULL) THEN RAISE EXCEPTION 'external credentials survived'; END IF;
        IF NOT EXISTS(SELECT 1 FROM tokens WHERE id=2 AND revision=2) THEN RAISE EXCEPTION 'revocation revision not advanced by database'; END IF;
        IF NOT EXISTS(SELECT 1 FROM group_memberships WHERE principal_id=2 AND group_id=1) THEN RAISE EXCEPTION 'manual grants removed'; END IF;
        IF EXISTS(SELECT 1 FROM tokens WHERE id=1 AND revoked_at IS NOT NULL) THEN RAISE EXCEPTION 'local recovery token revoked'; END IF;
        END $$;").await.unwrap();
    f.cleanup().await;
}

#[tokio::test]
async fn verification_updates_reject_a_stale_replica() {
    let f = Fixture::new().await;
    let backend = f.replica();
    let mut document = candidate().to_document();
    document["receipt"] = serde_json::Value::Null;
    let candidate = StorageSecurityCandidate::from_document(document).unwrap();
    backend
        .create_security_candidate(candidate.clone())
        .await
        .unwrap();
    let checked = candidate
        .clone()
        .with_checks(candidate.checks().to_vec())
        .unwrap()
        .verify(digest('d'))
        .unwrap();
    backend
        .verify_security_candidate(checked.clone())
        .await
        .unwrap();
    let error = f
        .replica()
        .verify_security_candidate(checked)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), StorageErrorKind::Conflict);
    assert_eq!(
        backend
            .get_security_candidate(candidate.id(), owner())
            .await
            .unwrap()
            .state(Utc::now()),
        StorageSecurityCandidateState::Verified
    );
    f.cleanup().await;
}

#[tokio::test]
async fn expiry_cleanup_discards_candidate_authority() {
    let f = Fixture::new().await;
    let backend = f.replica();
    let candidate = candidate();
    backend
        .create_security_candidate(candidate.clone())
        .await
        .unwrap();
    f.owner("UPDATE security_candidates SET expires_at=now()-interval '1 second', document=document || jsonb_build_object('created_at',now()-interval '301 seconds','expires_at',now()-interval '1 second')").await.unwrap();
    assert_eq!(backend.purge_security_candidates().await.unwrap(), 1);
    assert_eq!(
        backend
            .get_security_candidate(candidate.id(), owner())
            .await
            .unwrap_err()
            .kind(),
        StorageErrorKind::NotFound
    );
    f.cleanup().await;
}

#[tokio::test]
async fn provider_identity_bindings_survive_restart_and_reject_reassignment() {
    let f = Fixture::new().await;
    let id = Uuid::new_v4();
    f.owner(&format!("INSERT INTO authentication_provider_bindings(name,provider_id,kind,namespace) VALUES('directory','{id}','ldap','directory')")).await.unwrap();
    let bound = f
        .replica()
        .authentication_provider_binding(
            hubuum_domain::IdentityScopeName::new("directory").unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(bound.id(), id);
    assert!(
        f.owner("UPDATE authentication_provider_bindings SET namespace='another-directory'")
            .await
            .is_err()
    );
    assert_eq!(
        f.replica()
            .authentication_provider_binding(
                hubuum_domain::IdentityScopeName::new("directory").unwrap()
            )
            .await
            .unwrap(),
        Some(bound)
    );
    f.cleanup().await;
}
