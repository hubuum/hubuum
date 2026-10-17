use std::sync::atomic::{AtomicUsize, Ordering};

use hubuum_storage_postgres::diesel_async_prelude::*;
use hubuum_storage_postgres::with_transaction;
use rstest::rstest;

use super::*;
use crate::events::EventContext;
use crate::models::{NewUser, UpdateUser, UserID};
use crate::services::authentication::issue_login_token;
use crate::storage::{ExternalIdentityStorage, StorageExternalUserSync, storage_handle};
use crate::tests::{TestContext, app_context};

#[actix_web::test]
async fn verified_password_cannot_issue_a_token_after_reset() {
    let context = TestContext::new().await;
    let app = app_context(&context.pool);
    let name = context.normal_user.name(&context.pool).await.unwrap();
    let authenticated = login(
        app.get_ref(),
        LoginUser {
            identity_scope: None,
            name: name.clone(),
            password: "testpassword".into(),
        },
    )
    .await
    .unwrap();
    crate::services::identity::reset_local_password(
        app.get_ref(),
        &name,
        "replacement-password".into(),
    )
    .await
    .unwrap();
    let error = issue_login_token(app.get_ref(), &authenticated, &EventContext::system())
        .await
        .err()
        .unwrap();
    assert!(matches!(error, ApiError::Unauthorized(_)));
}

#[rstest]
#[case::create("create")]
#[case::update("update")]
#[case::reset("reset")]
#[actix_web::test]
async fn empty_password_changes_are_rejected_before_persistence(#[case] operation: &str) {
    let context = TestContext::new().await;
    let result = match operation {
        "create" => NewUser {
            identity_scope: None,
            name: context.scoped_name("empty_password"),
            password: String::new(),
            proper_name: None,
            email: None,
        }
        .save_without_events(&context.pool)
        .await
        .map(|_| ()),
        "update" => UpdateUser {
            password: Some(String::new()),
            proper_name: None,
            email: None,
        }
        .save_without_events(UserID::new(context.normal_user.id).unwrap(), &context.pool)
        .await
        .map(|_| ()),
        "reset" => {
            let name = context.normal_user.name(&context.pool).await.unwrap();
            crate::services::identity::reset_local_password(&context.pool, &name, String::new())
                .await
                .map(|_| ())
        }
        _ => unreachable!(),
    };
    assert!(matches!(result, Err(ApiError::BadRequest(_))));
}

#[derive(Clone, Copy)]
enum RefreshFailure {
    Unavailable,
    Rejected,
    Protocol,
    Config,
}

struct FailingDirectory {
    failure: RefreshFailure,
    calls: Arc<AtomicUsize>,
}

impl AuthProviderBackend for FailingDirectory {
    fn authenticate<'a>(
        &'a self,
        _storage: &'a crate::storage::StorageHandle,
        _login: LoginUser,
    ) -> AuthProviderFuture<'a, User> {
        Box::pin(async { Err(auth_failure()) })
    }

    fn refresh<'a>(
        &'a self,
        _storage: &'a crate::storage::StorageHandle,
        _state: &'a ExternalUserState,
    ) -> AuthProviderRefreshFuture<'a> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Err(AuthProviderRefreshError::Provider(match self.failure {
                RefreshFailure::Unavailable => AuthProviderError::Unavailable("offline".into()),
                RefreshFailure::Rejected => AuthProviderError::AuthenticationFailed,
                RefreshFailure::Protocol => {
                    AuthProviderError::Protocol("invalid directory response".into())
                }
                RefreshFailure::Config => {
                    AuthProviderError::Config("invalid service credentials".into())
                }
            }))
        })
    }
}

#[rstest]
#[case::outage(RefreshFailure::Unavailable, true)]
#[case::missing_or_disabled(RefreshFailure::Rejected, false)]
#[case::protocol_error(RefreshFailure::Protocol, false)]
#[case::configuration_error(RefreshFailure::Config, false)]
#[actix_web::test]
async fn only_temporary_outages_allow_cached_authority(
    #[case] failure: RefreshFailure,
    #[case] allowed: bool,
) {
    let context = TestContext::new().await;
    let storage = storage_handle(&context.pool);
    let scope = context.scoped_name("refresh_denial");
    let user = storage
        .sync_external_user(
            StorageExternalUserSync::builder(&scope, "ldap", "subject", "alice").build(),
        )
        .await
        .unwrap()
        .into_value();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry =
        AuthProviderRegistry::from_config(AuthProvidersConfig { ldap: vec![] }).unwrap();
    registry
        .register(RegisteredAuthProvider {
            name: scope.clone(),
            kind: "ldap".into(),
            display_order: 100,
            refresh_policy: Some(RefreshPolicy::new(300, 3600).unwrap()),
            backend: Box::new(FailingDirectory {
                failure,
                calls: calls.clone(),
            }),
        })
        .unwrap();
    with_transaction(&context.pool, async |connection| {
        diesel::sql_query("UPDATE principals SET last_sync_success_at = now() - interval '10 minutes', last_sync_attempted_at = now() - interval '10 minutes' WHERE id = $1")
            .bind::<diesel::sql_types::Integer, _>(user.id().id()).execute(connection).await?;
        Ok::<_, diesel::result::Error>(())
    }).await.unwrap();
    for _ in 0..2 {
        assert_eq!(
            refresh_principal_with_registry(
                &storage,
                user.id().id(),
                get_external_principal_state(&storage, user.id().id())
                    .await
                    .unwrap()
                    .unwrap(),
                &registry
            )
            .await
            .is_ok(),
            allowed
        );
    }
    assert_eq!(
        calls.as_ref().load(Ordering::Relaxed),
        1,
        "backoff must not turn a rejection into cached access"
    );
    with_transaction(&context.pool, async |connection| {
        diesel::sql_query("DELETE FROM principals WHERE id = $1")
            .bind::<diesel::sql_types::Integer, _>(user.id().id())
            .execute(connection)
            .await?;
        diesel::sql_query("DELETE FROM identity_scopes WHERE name = $1")
            .bind::<diesel::sql_types::Text, _>(&scope)
            .execute(connection)
            .await?;
        Ok::<_, diesel::result::Error>(())
    })
    .await
    .unwrap();
}
