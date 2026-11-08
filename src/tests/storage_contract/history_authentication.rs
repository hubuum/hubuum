//! Authentication precedes publication admission on every complete backend.

use std::future::ready;

use chrono::Utc;
use hubuum_domain::TokenId;
use hubuum_storage_core::{
    ApiRequestAdmissionStorage, ApiRequestClass, CredentialActivityPolicy, DeletionControlStorage,
    HistoryLifecycleStorage, HistoryPublicationKind, ObservedSource, RuntimeRegistration,
};
use hubuum_storage_postgres::diesel_async_prelude::*;
use hubuum_storage_postgres::with_connection;
use hubuum_task_core::TaskExecutionContext;
use rstest::rstest;

use super::*;

#[derive(Clone, Copy)]
enum BootstrapOperation {
    Authentication,
    CredentialObservation,
    ApiAdmission,
}

struct Fixture {
    unbound: StorageHandle,
    backend: StorageHandle,
    user: BackendUserFixture,
    token: TokenId,
    registration: Option<RuntimeRegistration>,
    scope: StorageExecutionScope,
}

impl Fixture {
    async fn new(unbound: StorageHandle) -> Self {
        let user = create_backend_user(&unbound, &prefix("history_authentication")).await;
        let token = unbound
            .authenticate_bearer_token(attempt(&user))
            .await
            .unwrap()
            .id();
        let registration = unbound.register_history_runtime(None).await.unwrap();
        let application = unbound
            .capture_application_execution(Some(registration.owner()))
            .await
            .unwrap();
        let backend = unbound.clone().with_runtime_owner(registration.owner());
        Self {
            unbound,
            backend,
            user,
            token,
            registration: Some(registration),
            scope: StorageExecutionScope::default().with_application_access(application),
        }
    }

    async fn run(
        &self,
        operation: BootstrapOperation,
        scope: StorageExecutionScope,
    ) -> Result<(), StorageError> {
        match operation {
            BootstrapOperation::Authentication => self
                .backend
                .run_in_scope(
                    scope,
                    self.backend.authenticate_bearer_token(attempt(&self.user)),
                )
                .await
                .map(|_| ()),
            BootstrapOperation::CredentialObservation => {
                self.backend
                    .run_in_scope(
                        scope,
                        self.backend.observe_credential_source(
                            self.token,
                            ObservedSource::ConnectionPeer("127.0.0.1".parse().unwrap()),
                            CredentialActivityPolicy::try_new(true, 7, 8).unwrap(),
                        ),
                    )
                    .await
            }
            BootstrapOperation::ApiAdmission => {
                let permit = self
                    .backend
                    .run_in_scope(
                        scope,
                        self.backend.admit_api_request(
                            self.user.principal_id,
                            ApiRequestClass::History,
                            0,
                        ),
                    )
                    .await?;
                self.backend.settle_api_request(permit).await
            }
        }
    }

    async fn retire_runtime(&mut self) {
        self.unbound
            .unregister_runtime(self.registration.take().unwrap())
            .await
            .unwrap();
    }

    async fn cleanup(mut self) {
        if self.registration.is_some() {
            self.retire_runtime().await;
        }
        // Source activity deliberately survives principal deletion in production.
        // Remove only this fixture's retained row before deleting its principal.
        if self.unbound.descriptor().kind() == StorageBackendKind::Postgres {
            use crate::schema::credential_activity;
            let token = self.token.id();
            with_connection(pool().get_ref(), async move |connection| {
                diesel::delete(
                    credential_activity::table.filter(credential_activity::token_id.eq(token)),
                )
                .execute(connection)
                .await
            })
            .await
            .unwrap();
        }
        delete_backend_user(&self.unbound, self.user).await;
    }
}

fn attempt(user: &BackendUserFixture) -> StorageAuthenticationAttempt {
    let now = Utc::now();
    StorageAuthenticationAttempt::try_new(
        StorageAuthenticationCredential::new(&user.token_hash),
        now,
        now - chrono::Duration::hours(2),
    )
    .unwrap()
}

fn supervised(scope: StorageExecutionScope) -> StorageExecutionScope {
    scope.with_task_execution(Some(
        TaskExecutionContext::new(Duration::from_secs(10))
            .unwrap()
            .0,
    ))
}

#[rstest]
#[case::matching(false)]
#[case::stale(true)]
#[actix_web::test]
async fn every_backend_binds_handler_local_revision_preconditions(#[case] stale: bool) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture = Fixture::new(backend).await;
        let scope = supervised(fixture.scope.clone());
        let owner = fixture
            .backend
            .run_in_scope(
                scope.clone(),
                fixture
                    .backend
                    .admit_history_publication(HistoryPublicationKind::HttpResponse),
            )
            .await
            .unwrap();
        let scope = scope.with_history_read(owner.context());
        let checked = fixture
            .backend
            .run_in_scope(scope, async {
                let revision = fixture
                    .backend
                    .get_user_details(fixture.user.id)
                    .await
                    .unwrap()
                    .into_parts()
                    .revision();
                let revision = if stale {
                    revision.checked_advance().unwrap()
                } else {
                    revision
                };
                crate::storage::with_revision_precondition(
                    &fixture.backend,
                    Some(StorageRevisionPrecondition::new(
                        StorageRevisionTarget::Principal(fixture.user.principal_id),
                        vec![revision],
                    )),
                    fixture.backend.update_user(StorageUserUpdate::new(
                        fixture.user.id,
                        None,
                        Some("Bound conditional update".into()),
                        None,
                        EventContext::system(),
                    )),
                )
                .await
            })
            .await;
        fixture
            .backend
            .settle_history_publication(
                owner
                    .after_drained(ready(Ok(())), ready(Ok(())))
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
        fixture.cleanup().await;
        if stale {
            assert_eq!(
                checked.unwrap_err().kind(),
                StorageErrorKind::RevisionConflict
            );
        } else {
            let _ = checked.unwrap();
        }
    }
}

#[rstest]
#[case::inherit(0, true)]
#[case::tighter_budget(1, true)]
#[case::clear_precondition(2, false)]
#[case::replace_precondition(3, false)]
#[case::clear_budget(4, false)]
#[case::longer_budget(5, false)]
#[case::replace_task(6, false)]
#[actix_web::test]
async fn every_backend_preserves_narrowed_publication_constraints(
    #[case] change: u8,
    #[case] allowed: bool,
) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture = Fixture::new(backend).await;
        let scope = supervised(fixture.scope.clone());
        let owner = fixture
            .backend
            .run_in_scope(
                scope.clone(),
                fixture
                    .backend
                    .admit_history_publication(HistoryPublicationKind::Export),
            )
            .await
            .unwrap();
        let scope = scope.with_history_read(owner.context());
        let checked = fixture
            .backend
            .run_in_scope(scope, async {
                let condition = StorageRevisionPrecondition::new(
                    StorageRevisionTarget::Principal(fixture.user.principal_id),
                    vec![],
                );
                let narrowed = StorageExecutionScope::default()
                    .with_revision_precondition(Some(condition))
                    .with_query_budget(StorageQueryBudget::from_millis(1000));
                // Capture the derived proof, then re-enter outside its local scope.
                // Clearing captured constraints must fail as well as nested clears.
                let captured = fixture
                    .backend
                    .run_in_scope(narrowed, async {
                        fixture.backend.current_execution_scope()
                    })
                    .await;
                let changed = match change {
                    0 => captured,
                    1 => captured.with_query_budget(StorageQueryBudget::from_millis(500)),
                    2 => captured.with_revision_precondition(None),
                    3 => {
                        captured.with_revision_precondition(Some(StorageRevisionPrecondition::new(
                            StorageRevisionTarget::Token(fixture.token),
                            vec![],
                        )))
                    }
                    4 => captured.with_query_budget(None),
                    5 => captured.with_query_budget(StorageQueryBudget::from_millis(2000)),
                    6 => supervised(captured),
                    _ => unreachable!(),
                };
                // Re-entering beneath the original scope must not replace the
                // captured, stricter proof with its less constrained parent.
                fixture
                    .backend
                    .run_in_scope(changed, fixture.backend.get_user(fixture.user.id))
                    .await
            })
            .await;
        fixture
            .backend
            .settle_history_publication(
                owner
                    .after_drained(ready(Ok(())), ready(Ok(())))
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
        fixture.cleanup().await;
        if allowed {
            checked.unwrap();
        } else {
            assert_eq!(checked.unwrap_err().kind(), StorageErrorKind::Unavailable);
        }
    }
}

#[rstest]
#[case::authentication(BootstrapOperation::Authentication)]
#[case::credential_source(BootstrapOperation::CredentialObservation)]
#[case::api_admission(BootstrapOperation::ApiAdmission)]
#[actix_web::test]
async fn every_backend_supports_prepublication_bootstrap(#[case] operation: BootstrapOperation) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture = Fixture::new(backend).await;
        let scope = match operation {
            BootstrapOperation::ApiAdmission => supervised(fixture.scope.clone()),
            _ => fixture.scope.clone(),
        };
        let result = fixture.run(operation, scope).await;
        fixture.cleanup().await;
        result.unwrap();
    }
}

#[rstest]
#[case::authentication(BootstrapOperation::Authentication)]
#[case::credential_source(BootstrapOperation::CredentialObservation)]
#[case::api_admission(BootstrapOperation::ApiAdmission)]
#[actix_web::test]
async fn every_backend_bootstrap_rejects_a_retired_runtime(#[case] operation: BootstrapOperation) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let mut fixture = Fixture::new(backend).await;
        fixture.retire_runtime().await;
        let result = fixture.run(operation, fixture.scope.clone()).await;
        fixture.cleanup().await;
        assert_eq!(result.unwrap_err().kind(), StorageErrorKind::Unavailable);
    }
}

#[rstest]
#[case::authentication(BootstrapOperation::Authentication)]
#[case::credential_source(BootstrapOperation::CredentialObservation)]
#[case::api_admission(BootstrapOperation::ApiAdmission)]
#[actix_web::test]
async fn every_backend_bootstrap_rejects_a_retired_publication(
    #[case] operation: BootstrapOperation,
) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture = Fixture::new(backend).await;
        let owner = fixture
            .backend
            .run_in_scope(
                fixture.scope.clone(),
                fixture
                    .backend
                    .admit_history_publication(HistoryPublicationKind::HttpResponse),
            )
            .await
            .unwrap();
        let scope = fixture.scope.clone().with_history_read(owner.context());
        let settlement = owner
            .after_drained(ready(Ok(())), ready(Ok(())))
            .await
            .unwrap();
        fixture
            .backend
            .settle_history_publication(settlement)
            .await
            .unwrap();
        let result = fixture.run(operation, scope).await;
        fixture.cleanup().await;
        assert_eq!(result.unwrap_err().kind(), StorageErrorKind::Unavailable);
    }
}

#[rstest]
#[case::api(true)]
#[case::task(false)]
#[actix_web::test]
async fn every_backend_authentication_cannot_bootstrap_admitted_work(#[case] api: bool) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture = Fixture::new(backend).await;
        let scope = supervised(fixture.scope.clone());
        let permit = if api {
            Some(
                fixture
                    .backend
                    .run_in_scope(
                        scope.clone(),
                        fixture.backend.admit_api_request(
                            fixture.user.principal_id,
                            ApiRequestClass::History,
                            0,
                        ),
                    )
                    .await
                    .unwrap(),
            )
        } else {
            None
        };
        let scope = match &permit {
            Some(permit) => scope.with_api_request(permit.clone()),
            None => scope,
        };
        let result = fixture.run(BootstrapOperation::Authentication, scope).await;
        if let Some(permit) = permit {
            fixture.backend.settle_api_request(permit).await.unwrap();
        }
        fixture.cleanup().await;
        assert_eq!(result.unwrap_err().kind(), StorageErrorKind::Unavailable);
    }
}

#[actix_web::test]
async fn every_backend_bootstrap_does_not_exempt_domain_reads() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture = Fixture::new(backend).await;
        let result = fixture
            .backend
            .run_in_scope(
                fixture.scope.clone(),
                fixture.backend.get_user(fixture.user.id),
            )
            .await;
        fixture.cleanup().await;
        assert_eq!(result.unwrap_err().kind(), StorageErrorKind::Unavailable);
    }
}
