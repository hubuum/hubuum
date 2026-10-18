use hubuum_auth_core::{AuthenticatedExternalUser, ExternalGroup, ExternalUserProfile};
use hubuum_domain::PrincipalId;
use hubuum_events_core::EventEntityId;
use hubuum_query::QueryOptions;
use hubuum_storage_core::{
    AuditEventStorage, StorageAuditEventFilters, StorageAuditEventListQuery,
};
use hubuum_storage_postgres::diesel_async_prelude::*;
use hubuum_storage_postgres::with_transaction;
use rstest::rstest;

use super::*;
use crate::events::{EntityType, EventContext};
use crate::models::LoginUser;
use crate::permissions::test_support::mock_treetop::MockTreetopBackend;
use crate::services::authentication::issue_login_token;
use crate::storage::{AuthorizationDataStorage, storage_handle};
use crate::tests::{TestContext, app_context_with_permission_backend};

/// Exercises configured-provider dispatch and the real identity synchronization
/// operation; LDAP transport itself has its pinned OpenLDAP contract suite.
struct DirectoryProvider {
    scope: String,
}

impl DirectoryProvider {
    fn identity(&self, name: &str, member: bool) -> AuthenticatedExternalUser {
        AuthenticatedExternalUser {
            profile: ExternalUserProfile {
                subject: format!("subject-{name}"),
                name: name.to_string(),
                proper_name: None,
                email: None,
            },
            groups: if member {
                vec![ExternalGroup {
                    key: "operators".into(),
                    name: "operators".into(),
                    description: None,
                }]
            } else {
                vec![]
            },
        }
    }
}

impl AuthProviderBackend for DirectoryProvider {
    fn authenticate<'a>(
        &'a self,
        storage: &'a crate::storage::StorageHandle,
        login: ValidatedLogin,
    ) -> AuthProviderFuture<'a, StorageUser> {
        Box::pin(async move {
            if login.password().as_str() != "directory-password" {
                return Err(ApiError::Unauthorized("Authentication failure".into()));
            }
            sync_external_user_from_backend(
                storage,
                &self.scope,
                LDAP_PROVIDER_KIND,
                self.identity(login.name(), true),
            )
            .await
        })
    }

    fn refresh<'a>(
        &'a self,
        storage: &'a crate::storage::StorageHandle,
        state: &'a ExternalUserState,
    ) -> AuthProviderRefreshFuture<'a> {
        Box::pin(async move {
            sync_external_user_from_backend(
                storage,
                &self.scope,
                LDAP_PROVIDER_KIND,
                self.identity(state.refresh_request.username(), false),
            )
            .await
            .map(|_| ())
            .map_err(AuthProviderRefreshError::Internal)
        })
    }
}

#[rstest]
#[case::admin(true)]
#[case::non_admin(false)]
#[actix_web::test]
async fn configured_external_login_snapshots_synced_identity_and_policy(#[case] admin: bool) {
    let context = TestContext::new().await;
    let scope = context.scoped_name("login_directory");
    let username = context.scoped_name("directory_user");
    let mut registry =
        AuthProviderRegistry::from_config(AuthProvidersConfig { ldap: vec![] }).unwrap();
    registry
        .register(RegisteredAuthProvider {
            name: scope.clone(),
            kind: LDAP_PROVIDER_KIND.into(),
            display_order: 100,
            refresh_policy: Some(RefreshPolicy::new(300, 3600).unwrap()),
            backend: Box::new(DirectoryProvider {
                scope: scope.clone(),
            }),
        })
        .unwrap();
    let authenticated = login_with_registry(
        &context.pool,
        LoginUser {
            identity_scope: Some(scope.clone()),
            name: username.clone(),
            password: "directory-password".into(),
        }
        .validate()
        .unwrap(),
        &registry,
    )
    .await
    .unwrap();
    let user = authenticated.user();
    let storage = storage_handle(&context.pool);
    let synced = storage
        .get_authorization_principal(PrincipalId::new(user.id).unwrap())
        .await
        .unwrap();
    let groups = synced.into_group_ids();
    assert_eq!(
        groups.len(),
        1,
        "provider memberships must be synchronized before policy evaluation"
    );
    let policy = MockTreetopBackend::new();
    if admin {
        policy.add_admin_rule(groups[0].id());
    }
    let app = app_context_with_permission_backend(context.pool.get_ref().clone(), Arc::new(policy));
    let token = issue_login_token(&app, &authenticated, &EventContext::system())
        .await
        .unwrap();

    // A provider refresh removes membership after login. It must neither emit
    // another login event nor alter the original subject snapshot.
    let state = get_external_principal_state(&storage, user.id)
        .await
        .unwrap()
        .unwrap();
    let state = ExternalUserState::new(state, RefreshPolicy::new(300, 3600).unwrap()).unwrap();
    assert!(
        registry
            .provider(&scope)
            .unwrap()
            .backend
            .refresh(&storage, &state)
            .await
            .is_ok()
    );
    let events = storage
        .list_audit_events(StorageAuditEventListQuery::new(
            vec![],
            true,
            StorageAuditEventFilters::new()
                .entity_type(Some(EntityType::Authentication))
                .entity_id(Some(EventEntityId::new(user.id).unwrap())),
            QueryOptions::empty(),
        ))
        .await
        .unwrap()
        .into_parts()
        .0;
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0].as_parts().0.metadata()["subject"],
        serde_json::json!({
            "principal_id": user.id, "name": username, "kind": "human", "identity_scope": scope,
            "authentication_provider_kind": "ldap", "is_admin": admin,
        })
    );
    let event_json = serde_json::to_string(events[0].as_parts().0).unwrap();
    assert!(!event_json.contains("directory-password"));
    assert!(!event_json.contains(&token.into_token().get_token()));
    // Provider-owned records intentionally cannot be deleted through the API.
    with_transaction(&context.pool, async |connection| {
        diesel::sql_query("DELETE FROM principals WHERE id = $1")
            .bind::<diesel::sql_types::Integer, _>(user.id)
            .execute(connection)
            .await?;
        diesel::sql_query("DELETE FROM groups WHERE id = $1")
            .bind::<diesel::sql_types::Integer, _>(groups[0].id())
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
