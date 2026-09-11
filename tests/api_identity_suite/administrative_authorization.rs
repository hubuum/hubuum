use std::sync::Arc;

use actix_web::{App, dev::ServiceResponse, http::StatusCode, test, web::Data};
use rstest::rstest;
use serde_json::json;

use crate::events::EventContext;
use crate::models::{Group, Permissions, PrincipalID, PrincipalTokenCreateRequest, User};
use crate::permissions::AppContext;
use crate::permissions::test_support::mock_treetop::MockTreetopBackend;
use crate::services::identity::delete_service_account;
use crate::tests::asserts::assert_response_status;
use crate::tests::{
    TestContext, app_context, app_context_with_permission_backend, create_groups_with_prefix,
    create_test_service_account, persisted_test_token, scoped_token, service_account_token,
};

const PRINCIPALS: &str = "/api/v1/iam/principals";
const SERVICE_ACCOUNTS: &str = "/api/v1/iam/service-accounts";

struct AdminFixture {
    context: TestContext,
    app_context: Data<AppContext>,
    policy_group: Group,
    user: User,
    token: String,
}

impl AdminFixture {
    async fn new(local_admin: bool, external_admin: Option<bool>) -> Self {
        let context = TestContext::new().await;
        let (user, token) = if local_admin {
            (context.admin_user.clone(), context.admin_token.clone())
        } else {
            (context.normal_user.clone(), context.normal_token.clone())
        };
        let policy_group = create_groups_with_prefix(
            &context.pool,
            &context.scoped_name("external_admin_policy"),
            1,
        )
        .await
        .remove(0);
        policy_group
            .add_member_without_events(&context.pool, &user)
            .await
            .unwrap();
        let selected_context = match external_admin {
            None => app_context(&context.pool),
            Some(allowed) => {
                let backend = MockTreetopBackend::new();
                if allowed {
                    backend.add_admin_rule(policy_group.id);
                }
                Data::new(app_context_with_permission_backend(
                    context.pool.get_ref().clone(),
                    Arc::new(backend),
                ))
            }
        };
        Self {
            context,
            app_context: selected_context,
            policy_group,
            user,
            token,
        }
    }

    fn other_human_id(&self) -> i32 {
        if self.user.id == self.context.admin_user.id {
            self.context.normal_user.id
        } else {
            self.context.admin_user.id
        }
    }

    async fn owner_group(&self) -> Group {
        create_groups_with_prefix(
            &self.context.pool,
            &self.context.scoped_name("service_account_owner"),
            1,
        )
        .await
        .remove(0)
    }

    async fn request(&self, request: test::TestRequest) -> ServiceResponse {
        let app = test::init_service(
            App::new()
                .app_data(self.app_context.clone())
                .configure(crate::api::config),
        )
        .await;
        request
            .insert_header(("Authorization", format!("Bearer {}", self.token)))
            .send_request(&app)
            .await
            .map_into_boxed_body()
    }

    async fn cleanup(self) {
        self.policy_group
            .delete_without_events(&self.context.pool)
            .await
            .unwrap();
        self.context
            .normal_user
            .delete_without_events(&self.context.pool)
            .await
            .unwrap();
        self.context
            .admin_user
            .delete_without_events(&self.context.pool)
            .await
            .unwrap();
    }
}

#[derive(Clone, Copy, Debug)]
enum PrincipalTarget {
    OtherHuman,
    SelfHuman,
    OwnedServiceAccount,
    UnrelatedServiceAccount,
}

#[rstest]
#[case::local_admin(true, None, true)]
#[case::local_non_admin(false, None, false)]
#[case::external_denies_local_admin(true, Some(false), false)]
#[case::external_grants_non_local_admin(false, Some(true), true)]
#[actix_web::test]
async fn principal_management_uses_selected_admin_policy(
    #[case] local_admin: bool,
    #[case] external_admin: Option<bool>,
    #[case] admin_allowed: bool,
    #[values(
        "create",
        "list",
        "inspect",
        "renew",
        "revoke",
        "groups",
        "permissions"
    )]
    operation: &str,
    #[values(
        PrincipalTarget::OtherHuman,
        PrincipalTarget::SelfHuman,
        PrincipalTarget::OwnedServiceAccount,
        PrincipalTarget::UnrelatedServiceAccount
    )]
    target: PrincipalTarget,
) {
    let fixture = AdminFixture::new(local_admin, external_admin).await;
    let owner_group = fixture.owner_group().await;
    let account = create_test_service_account(&fixture.context.pool, &owner_group, None).await;
    let principal_id = match target {
        PrincipalTarget::OtherHuman => fixture.other_human_id(),
        PrincipalTarget::SelfHuman => fixture.user.id,
        PrincipalTarget::OwnedServiceAccount => {
            owner_group
                .add_member_without_events(&fixture.context.pool, &fixture.user)
                .await
                .unwrap();
            account.id
        }
        PrincipalTarget::UnrelatedServiceAccount => account.id,
    };
    let raw = PrincipalTokenCreateRequest::new(PrincipalID::new(principal_id).unwrap())
        .create(&fixture.context.pool, &EventContext::system())
        .await
        .unwrap()
        .get_token();
    let token = persisted_test_token(&fixture.context.pool, &raw).await;
    let tokens_path = format!("{PRINCIPALS}/{principal_id}/tokens");
    let token_path = format!("{tokens_path}/{}", token.id);
    let (request, success) = match operation {
        "create" => (
            test::TestRequest::post()
                .uri(&tokens_path)
                .set_json(json!({})),
            StatusCode::CREATED,
        ),
        "list" => (test::TestRequest::get().uri(&tokens_path), StatusCode::OK),
        "inspect" => (test::TestRequest::get().uri(&token_path), StatusCode::OK),
        "renew" => (
            test::TestRequest::post()
                .uri(&format!("{token_path}/renew"))
                .set_json(json!({})),
            StatusCode::CREATED,
        ),
        "revoke" => (
            test::TestRequest::post().uri(&format!("{token_path}/revoke")),
            StatusCode::NO_CONTENT,
        ),
        "groups" | "permissions" => (
            test::TestRequest::get().uri(&format!("{PRINCIPALS}/{principal_id}/{operation}")),
            StatusCode::OK,
        ),
        _ => unreachable!(),
    };
    let allowed = admin_allowed
        || matches!(
            target,
            PrincipalTarget::SelfHuman | PrincipalTarget::OwnedServiceAccount
        );
    let response = fixture.request(request).await;
    assert_response_status(
        response,
        if allowed {
            success
        } else {
            StatusCode::NOT_FOUND
        },
    )
    .await;

    delete_service_account(&fixture.context.pool, account.id, &EventContext::system())
        .await
        .unwrap();
    owner_group
        .delete_without_events(&fixture.context.pool)
        .await
        .unwrap();
    fixture.cleanup().await;
}

fn settings_request(principal_id: i32, operation: &str) -> (test::TestRequest, StatusCode) {
    let path = format!("{PRINCIPALS}/{principal_id}/settings");
    match operation {
        "get" => (test::TestRequest::get().uri(&path), StatusCode::OK),
        "put" => (
            test::TestRequest::put()
                .uri(&path)
                .set_json(json!({"theme": "dark"})),
            StatusCode::OK,
        ),
        "patch" => (
            test::TestRequest::patch()
                .uri(&path)
                .set_json(json!({"theme": "dark"})),
            StatusCode::OK,
        ),
        "delete" => (
            test::TestRequest::delete().uri(&path),
            StatusCode::NO_CONTENT,
        ),
        _ => unreachable!(),
    }
}

#[rstest]
#[case::local_admin(true, None, true)]
#[case::local_non_admin(false, None, false)]
#[case::external_denies_local_admin(true, Some(false), false)]
#[case::external_grants_non_local_admin(false, Some(true), true)]
#[actix_web::test]
async fn cross_principal_settings_use_selected_admin_policy(
    #[case] local_admin: bool,
    #[case] external_admin: Option<bool>,
    #[case] allowed: bool,
    #[values("get", "put", "patch", "delete")] operation: &str,
    #[values(false, true)] service_account: bool,
) {
    let fixture = AdminFixture::new(local_admin, external_admin).await;
    let owner_group = fixture.owner_group().await;
    let account = create_test_service_account(&fixture.context.pool, &owner_group, None).await;
    let target = if service_account {
        account.id
    } else {
        fixture.other_human_id()
    };
    let (request, success) = settings_request(target, operation);
    let response = fixture.request(request).await;
    assert_response_status(
        response,
        if allowed {
            success
        } else {
            StatusCode::NOT_FOUND
        },
    )
    .await;

    delete_service_account(&fixture.context.pool, account.id, &EventContext::system())
        .await
        .unwrap();
    owner_group
        .delete_without_events(&fixture.context.pool)
        .await
        .unwrap();
    fixture.cleanup().await;
}

#[rstest]
#[case::local_admin(true, None, false, true)]
#[case::local_non_admin(false, None, false, false)]
#[case::external_denies_local_admin(true, Some(false), false, false)]
#[case::external_grants_non_local_admin(false, Some(true), false, true)]
#[case::external_denial_preserves_human_owner(false, Some(false), true, true)]
#[actix_web::test]
async fn service_account_management_uses_selected_admin_policy(
    #[case] local_admin: bool,
    #[case] external_admin: Option<bool>,
    #[case] owner_member: bool,
    #[case] allowed: bool,
    #[values("create", "list", "get", "patch", "disable", "delete")] operation: &str,
) {
    let fixture = AdminFixture::new(local_admin, external_admin).await;
    let owner_group = fixture.owner_group().await;
    if owner_member {
        owner_group
            .add_member_without_events(&fixture.context.pool, &fixture.user)
            .await
            .unwrap();
    }
    let account = create_test_service_account(&fixture.context.pool, &owner_group, None).await;
    let path = format!("{SERVICE_ACCOUNTS}/{}", account.id);
    let (request, success) = match operation {
        "create" => (
            test::TestRequest::post()
                .uri(SERVICE_ACCOUNTS)
                .set_json(json!({
                    "name": fixture.context.scoped_name("created_account"),
                    "description": "selected backend authorization regression",
                    "owner_group_id": owner_group.id,
                })),
            StatusCode::CREATED,
        ),
        "list" => (
            test::TestRequest::get().uri(&format!("{SERVICE_ACCOUNTS}?id={}", account.id)),
            StatusCode::OK,
        ),
        "get" => (test::TestRequest::get().uri(&path), StatusCode::OK),
        "patch" => (
            test::TestRequest::patch()
                .uri(&path)
                .set_json(json!({"description": "updated"})),
            StatusCode::OK,
        ),
        "disable" => (
            test::TestRequest::post().uri(&format!("{path}/disable")),
            StatusCode::OK,
        ),
        "delete" => (
            test::TestRequest::delete().uri(&path),
            StatusCode::NO_CONTENT,
        ),
        _ => unreachable!(),
    };
    let expected = if allowed || operation == "list" {
        success
    } else if operation == "create" {
        StatusCode::FORBIDDEN
    } else {
        StatusCode::NOT_FOUND
    };
    let response = assert_response_status(fixture.request(request).await, expected).await;
    if operation == "list" {
        let rows: Vec<serde_json::Value> = test::read_body_json(response).await;
        let ids: Vec<_> = rows.iter().map(|row| row["id"].as_i64().unwrap()).collect();
        assert_eq!(
            ids,
            if allowed {
                vec![i64::from(account.id)]
            } else {
                vec![]
            }
        );
    } else if operation == "create" && allowed {
        let created: serde_json::Value = test::read_body_json(response).await;
        let id = i32::try_from(created["id"].as_i64().unwrap()).unwrap();
        delete_service_account(&fixture.context.pool, id, &EventContext::system())
            .await
            .unwrap();
    }

    if operation != "delete" || !allowed {
        delete_service_account(&fixture.context.pool, account.id, &EventContext::system())
            .await
            .unwrap();
    }
    owner_group
        .delete_without_events(&fixture.context.pool)
        .await
        .unwrap();
    fixture.cleanup().await;
}

#[rstest]
#[case::local_admin(true, None, false, true)]
#[case::local_non_admin(false, None, false, false)]
#[case::external_denies_local_admin(true, Some(false), false, false)]
#[case::external_grants_non_local_admin(false, Some(true), false, true)]
#[case::external_denial_preserves_target_group_member(false, Some(false), true, true)]
#[actix_web::test]
async fn service_account_reassignment_requires_authority_over_target_group(
    #[case] local_admin: bool,
    #[case] external_admin: Option<bool>,
    #[case] target_member: bool,
    #[case] allowed: bool,
) {
    let fixture = AdminFixture::new(local_admin, external_admin).await;
    let owner_group = fixture.owner_group().await;
    let account = create_test_service_account(&fixture.context.pool, &owner_group, None).await;
    owner_group
        .add_member_without_events(&fixture.context.pool, &fixture.user)
        .await
        .unwrap();
    // Keep the target independent of both the current owner and admin-policy groups.
    let target_group = create_groups_with_prefix(
        &fixture.context.pool,
        &fixture.context.scoped_name("handoff_target"),
        1,
    )
    .await
    .remove(0);
    if target_member {
        target_group
            .add_member_without_events(&fixture.context.pool, &fixture.user)
            .await
            .unwrap();
    }
    let response = fixture
        .request(
            test::TestRequest::patch()
                .uri(&format!("{SERVICE_ACCOUNTS}/{}", account.id))
                .set_json(json!({"owner_group_id": target_group.id})),
        )
        .await;
    assert_response_status(
        response,
        if allowed {
            StatusCode::OK
        } else {
            StatusCode::FORBIDDEN
        },
    )
    .await;

    delete_service_account(&fixture.context.pool, account.id, &EventContext::system())
        .await
        .unwrap();
    owner_group
        .delete_without_events(&fixture.context.pool)
        .await
        .unwrap();
    target_group
        .delete_without_events(&fixture.context.pool)
        .await
        .unwrap();
    fixture.cleanup().await;
}

#[rstest]
#[actix_web::test]
async fn external_admin_denial_preserves_self_settings(
    #[values(false, true)] service_account: bool,
    #[values(false, true)] scoped: bool,
    #[values("get", "put", "patch", "delete")] operation: &str,
) {
    let mut fixture = AdminFixture::new(false, Some(false)).await;
    let owner_group = fixture.owner_group().await;
    let account = create_test_service_account(&fixture.context.pool, &owner_group, None).await;
    let principal_id = if service_account {
        fixture.token = service_account_token(&fixture.context.pool, &account, None, None).await;
        account.id
    } else {
        fixture.user.id
    };
    if scoped {
        fixture.token = scoped_token(
            &fixture.context.pool,
            principal_id,
            &[Permissions::ReadCollection],
        )
        .await;
    }
    let (request, expected) = settings_request(principal_id, operation);
    assert_response_status(fixture.request(request).await, expected).await;

    delete_service_account(&fixture.context.pool, account.id, &EventContext::system())
        .await
        .unwrap();
    owner_group
        .delete_without_events(&fixture.context.pool)
        .await
        .unwrap();
    fixture.cleanup().await;
}

#[rstest]
#[actix_web::test]
async fn external_admin_grant_preserves_human_unscoped_management_boundary(
    #[values(false, true)] service_account: bool,
    #[values("credentials", "settings", "service_accounts")] operation: &str,
) {
    let mut fixture = AdminFixture::new(false, Some(true)).await;
    let owner_group = fixture.owner_group().await;
    let account = create_test_service_account(&fixture.context.pool, &owner_group, None).await;
    if service_account {
        fixture
            .policy_group
            .add_member_without_events(&fixture.context.pool, &account)
            .await
            .unwrap();
        owner_group
            .add_member_without_events(&fixture.context.pool, &account)
            .await
            .unwrap();
        fixture.token = service_account_token(&fixture.context.pool, &account, None, None).await;
    } else {
        fixture.token = scoped_token(
            &fixture.context.pool,
            fixture.user.id,
            &[Permissions::ReadCollection],
        )
        .await;
    }
    let (request, expected) = match operation {
        "credentials" => (
            test::TestRequest::post()
                .uri(&format!("{PRINCIPALS}/{}/tokens", account.id))
                .set_json(json!({})),
            StatusCode::FORBIDDEN,
        ),
        "settings" => (
            settings_request(fixture.other_human_id(), "put").0,
            StatusCode::NOT_FOUND,
        ),
        "service_accounts" => (
            test::TestRequest::patch()
                .uri(&format!("{SERVICE_ACCOUNTS}/{}", account.id))
                .set_json(json!({"description": "forbidden"})),
            StatusCode::FORBIDDEN,
        ),
        _ => unreachable!(),
    };
    assert_response_status(fixture.request(request).await, expected).await;

    delete_service_account(&fixture.context.pool, account.id, &EventContext::system())
        .await
        .unwrap();
    owner_group
        .delete_without_events(&fixture.context.pool)
        .await
        .unwrap();
    fixture.cleanup().await;
}
