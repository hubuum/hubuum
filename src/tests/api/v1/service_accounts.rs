//! Service-account identity, token-lifecycle, authorization, and task tests.
//!
//! Batch 1 — identity: login exclusion, cross-kind name collisions, class-table
//! inheritance subtype invariants, principal-row delete cascade.
//! Batch 2 — token lifecycle / scopes / disabled SA.
//! Batch 3 — authorization: principal-centric perms, scope narrowing, the
//! `kind = 'human'` / unscoped gate on the human/IAM extractors.
//! Batch 4 — principal routes: token management authz, path-scoped revoke,
//! group membership, namespace effective permissions.
//! Batch 5 — task ownership: attribution, per-principal idempotency, task load
//! authorization, disabled-SA cancellation, scope-snapshot persistence/parse.

#[cfg(test)]
mod tests {
    use actix_web::{App, http::StatusCode, test, web};
    use diesel::prelude::*;
    use rstest::rstest;

    use crate::api;
    use crate::db::traits::Status;
    use crate::db::traits::authz::scope_allows;
    use crate::db::traits::task::scope_snapshot_json;
    use crate::db::with_connection;
    use crate::errors::ApiError;
    use crate::models::Namespace;
    use crate::models::namespace::user_can_on_any;
    use crate::models::principal::load_principal_by_id;
    use crate::models::service_account::cancel_pending_tasks_for_principal;
    use crate::models::token::{Token, create_principal_token};
    use crate::models::user::{LoginUser, NewUser};
    use crate::models::{
        NewServiceAccount, NewTaskRecord, Permissions, PrincipalID, PrincipalMemberResponse,
        ServiceAccount, ServiceAccountID, ServiceAccountResponse, TaskID, TaskKind, TaskRecord,
        TaskStatus,
    };
    use crate::tests::api_operations::{delete_request, get_request, patch_request, post_request};
    use crate::tests::{
        TestContext, create_test_group, create_test_service_account, create_test_user,
        ensure_admin_group, scoped_token, service_account_token,
    };
    use crate::traits::PermissionController;
    use crate::utilities::auth::generate_random_password;

    const LOGIN_ENDPOINT: &str = "/api/v0/auth/login";
    const PRINCIPALS_ENDPOINT: &str = "/api/v1/iam/principals";

    // ----- Batch 1: identity / subtype invariants -----

    /// #1: a service account has no `users` row / password, so it cannot
    /// password-login — the login endpoint returns a generic 401.
    #[actix_web::test]
    async fn test_service_account_cannot_password_login() {
        let context = TestContext::new().await;
        let pool = context.pool.clone();

        let group = create_test_group(&pool).await;
        let sa = create_test_service_account(&pool, &group, None).await;
        let sa_name = load_principal_by_id(&pool, sa.id).await.unwrap().name;

        let app = test::init_service(
            App::new()
                .app_data(context.pool.clone())
                .configure(api::config),
        )
        .await;

        let login = web::Form(LoginUser {
            name: sa_name,
            password: "irrelevant".to_string(),
        });
        let resp = test::TestRequest::post()
            .uri(LOGIN_ENDPOINT)
            .set_json(&login)
            .send_request(&app)
            .await;

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    /// #12: `principals.name` is the single, race-safe uniqueness authority across
    /// kinds — names cannot collide regardless of which kind is created first.
    #[rstest]
    #[case::user_then_sa(true)]
    #[case::sa_then_user(false)]
    #[actix_web::test]
    async fn test_cross_kind_name_collision_rejected(#[case] user_first: bool) {
        let context = TestContext::new().await;
        let pool = &context.pool;
        let group = create_test_group(pool).await;
        let name = format!("collide-{}", generate_random_password(8));

        let create_user = |name: String| async move {
            NewUser {
                name,
                password: "pw".to_string(),
                email: None,
            }
            .save(pool)
            .await
            .map(|_| ())
        };
        let create_sa = |name: String| async move {
            NewServiceAccount {
                name,
                description: None,
                owner_group_id: group.id,
            }
            .save(pool, None)
            .await
            .map(|_| ())
        };

        let collision = if user_first {
            create_user(name.clone()).await.unwrap();
            create_sa(name.clone()).await
        } else {
            create_sa(name.clone()).await.unwrap();
            create_user(name.clone()).await
        };

        assert!(
            matches!(collision, Err(ApiError::Conflict(_))),
            "cross-kind name collision must be rejected as Conflict, got {collision:?}"
        );
    }

    /// #15: the composite `(id, kind)` FK makes the subtype tables mutually
    /// exclusive — a `service_accounts` row for a human principal id is rejected.
    #[actix_web::test]
    async fn test_subtype_fk_rejects_service_account_row_for_human_id() {
        let context = TestContext::new().await;
        let pool = &context.pool;
        let user = create_test_user(pool).await;
        let group = create_test_group(pool).await;

        let result = with_connection(pool, |conn| {
            diesel::insert_into(crate::schema::service_accounts::table)
                .values((
                    crate::schema::service_accounts::id.eq(user.id),
                    crate::schema::service_accounts::description.eq(""),
                    crate::schema::service_accounts::owner_group_id.eq(group.id),
                ))
                .execute(conn)
        });

        assert!(result.is_err());
    }

    /// #15 (other subtype): a `users` row for a service-account principal id is
    /// likewise rejected.
    #[actix_web::test]
    async fn test_subtype_fk_rejects_user_row_for_service_account_id() {
        let context = TestContext::new().await;
        let pool = &context.pool;
        let group = create_test_group(pool).await;
        let sa: ServiceAccount = create_test_service_account(pool, &group, None).await;

        let result = with_connection(pool, |conn| {
            diesel::insert_into(crate::schema::users::table)
                .values((
                    crate::schema::users::id.eq(sa.id),
                    crate::schema::users::password.eq("x"),
                ))
                .execute(conn)
        });

        assert!(result.is_err());
    }

    /// #11: deleting a user removes its `principals` row and cascades to the
    /// `users` row, its tokens, and its group memberships — one facet per case.
    #[derive(Clone, Copy)]
    enum CascadeFacet {
        Principal,
        UsersRow,
        Tokens,
        Memberships,
    }

    #[rstest]
    #[case::principal(CascadeFacet::Principal)]
    #[case::users_row(CascadeFacet::UsersRow)]
    #[case::tokens(CascadeFacet::Tokens)]
    #[case::memberships(CascadeFacet::Memberships)]
    #[actix_web::test]
    async fn test_delete_user_cascades(#[case] facet: CascadeFacet) {
        let context = TestContext::new().await;
        let pool = &context.pool;

        let user = create_test_user(pool).await;
        let group = create_test_group(pool).await;
        group.add_member(pool, &user).await.unwrap();
        user.create_token(pool).await.unwrap();
        let pid = user.id;

        user.delete(pool).await.expect("user delete should succeed");

        let remaining: i64 = match facet {
            CascadeFacet::Principal => with_connection(pool, |conn| {
                crate::schema::principals::table
                    .filter(crate::schema::principals::id.eq(pid))
                    .count()
                    .get_result(conn)
            })
            .unwrap(),
            CascadeFacet::UsersRow => with_connection(pool, |conn| {
                crate::schema::users::table
                    .filter(crate::schema::users::id.eq(pid))
                    .count()
                    .get_result(conn)
            })
            .unwrap(),
            CascadeFacet::Tokens => with_connection(pool, |conn| {
                crate::schema::tokens::table
                    .filter(crate::schema::tokens::principal_id.eq(pid))
                    .count()
                    .get_result(conn)
            })
            .unwrap(),
            CascadeFacet::Memberships => with_connection(pool, |conn| {
                crate::schema::group_memberships::table
                    .filter(crate::schema::group_memberships::principal_id.eq(pid))
                    .count()
                    .get_result(conn)
            })
            .unwrap(),
        };

        assert_eq!(remaining, 0);
    }

    // ----- Batch 2: token lifecycle / scopes / disabled SA -----

    /// #3: a past `expires_at` rejects; a future `expires_at` validates; a NULL
    /// `expires_at` falls back to the global lifetime window. `offset_hours` =
    /// `Some(h)` mints with `now + h`; `None` mints a plain NULL-expiry token.
    #[rstest]
    #[case::expired(Some(-1), false)]
    #[case::future(Some(1), true)]
    #[case::null_uses_window(None, true)]
    #[actix_web::test]
    async fn test_token_expiry_validation(
        #[case] offset_hours: Option<i64>,
        #[case] expected_valid: bool,
    ) {
        let context = TestContext::new().await;
        let pool = &context.pool;
        let user = create_test_user(pool).await;

        let token = match offset_hours {
            Some(h) => {
                let expiry = chrono::Utc::now().naive_utc() + chrono::Duration::hours(h);
                create_principal_token(pool, user.id, None, None, Some(expiry), None)
                    .await
                    .unwrap()
            }
            None => user.create_token(pool).await.unwrap(),
        };

        assert_eq!(token.is_valid(pool).await.is_ok(), expected_valid);
    }

    /// #4: revocation is a soft delete — the token no longer validates...
    #[actix_web::test]
    async fn test_revoked_token_is_rejected() {
        let context = TestContext::new().await;
        let pool = &context.pool;
        let user = create_test_user(pool).await;

        let raw = user.create_token(pool).await.unwrap();
        raw.delete(pool).await.unwrap();

        assert!(raw.is_valid(pool).await.is_err());
    }

    /// #4 (cont.): ...but the row remains, with `revoked_at` set (not deleted).
    #[actix_web::test]
    async fn test_revoked_token_row_is_retained() {
        use crate::schema::tokens::dsl::{revoked_at, token as token_col, tokens};

        let context = TestContext::new().await;
        let pool = &context.pool;
        let user = create_test_user(pool).await;

        let raw = user.create_token(pool).await.unwrap();
        let hash = raw.storage_hash();
        raw.delete(pool).await.unwrap();

        let revoked_at_value: Option<chrono::NaiveDateTime> = with_connection(pool, |conn| {
            tokens
                .filter(token_col.eq(&hash))
                .select(revoked_at)
                .first::<Option<chrono::NaiveDateTime>>(conn)
        })
        .expect("token row should still exist after soft-revoke");

        assert!(revoked_at_value.is_some());
    }

    /// #5: a successful validation advances `last_used_at` (non-decreasing across
    /// uses; set after the first use).
    #[actix_web::test]
    async fn test_last_used_at_advances_on_use() {
        let context = TestContext::new().await;
        let pool = &context.pool;
        let user = create_test_user(pool).await;
        let raw = user.create_token(pool).await.unwrap();

        let first = raw.is_valid(pool).await.unwrap().last_used_at;
        let second = raw.is_valid(pool).await.unwrap().last_used_at;

        assert!(matches!((first, second), (Some(a), Some(b)) if b >= a));
    }

    /// #5 (regression): the `last_used_at` returned by validation must equal the
    /// value persisted in Postgres (microsecond resolution). Before the fix the
    /// returned value carried the raw nanosecond wall clock, so on platforms with
    /// sub-microsecond clocks (Linux CI) it was microscopically ahead of the
    /// stored value, breaking non-decreasing comparisons across uses.
    #[actix_web::test]
    async fn test_last_used_at_matches_persisted_value() {
        use crate::schema::tokens::dsl::{last_used_at, token as token_col, tokens};

        let context = TestContext::new().await;
        let pool = &context.pool;
        let user = create_test_user(pool).await;
        let raw = user.create_token(pool).await.unwrap();

        let returned = raw.is_valid(pool).await.unwrap().last_used_at;

        let hash = raw.storage_hash();
        let persisted: Option<chrono::NaiveDateTime> = with_connection(pool, |conn| {
            tokens
                .filter(token_col.eq(&hash))
                .select(last_used_at)
                .first::<Option<chrono::NaiveDateTime>>(conn)
        })
        .expect("token row should exist after validation");

        assert_eq!(returned, persisted);
    }

    /// #8: disabling a service account makes its existing tokens fail validation.
    #[actix_web::test]
    async fn test_disabled_sa_token_rejected_at_validation() {
        let context = TestContext::new().await;
        let pool = &context.pool;

        let group = create_test_group(pool).await;
        let sa = create_test_service_account(pool, &group, None).await;
        let token = Token(service_account_token(pool, &sa, None, None).await);
        assert!(
            token.is_valid(pool).await.is_ok(),
            "precondition: active SA token validates"
        );

        ServiceAccountID::new(sa.id)
            .unwrap()
            .disable(pool)
            .await
            .unwrap();

        assert!(token.is_valid(pool).await.is_err());
    }

    /// #24: a disabled service account cannot mint new tokens — 409.
    #[actix_web::test]
    async fn test_disabled_sa_token_mint_returns_409() {
        let context = TestContext::new().await;
        let pool = &context.pool;

        let group = create_test_group(pool).await;
        let sa = create_test_service_account(pool, &group, None).await;
        ServiceAccountID::new(sa.id)
            .unwrap()
            .disable(pool)
            .await
            .unwrap();

        let resp = post_request(
            pool,
            &context.admin_token,
            &format!("{PRINCIPALS_ENDPOINT}/{}/tokens", sa.id),
            &serde_json::json!({ "name": "should-fail" }),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::CONFLICT);
    }

    /// #13: an empty `scopes` array is a client bug and is rejected with 400.
    #[actix_web::test]
    async fn test_token_mint_empty_scopes_rejected() {
        let context = TestContext::new().await;
        let pool = &context.pool;
        let group = create_test_group(pool).await;
        let sa = create_test_service_account(pool, &group, None).await;

        let resp = post_request(
            pool,
            &context.admin_token,
            &format!("{PRINCIPALS_ENDPOINT}/{}/tokens", sa.id),
            &serde_json::json!({ "scopes": [] }),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    /// Duplicate scope entries are rejected at the request boundary with 400.
    #[actix_web::test]
    async fn test_token_mint_duplicate_scopes_rejected() {
        let context = TestContext::new().await;
        let pool = &context.pool;
        let group = create_test_group(pool).await;
        let sa = create_test_service_account(pool, &group, None).await;

        let resp = post_request(
            pool,
            &context.admin_token,
            &format!("{PRINCIPALS_ENDPOINT}/{}/tokens", sa.id),
            &serde_json::json!({
                "scopes": ["ReadCollection", "ReadCollection"]
            }),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    /// #13: omitting `scopes` mints an unscoped token (`scoped = false`).
    #[actix_web::test]
    async fn test_token_mint_omitted_scopes_is_unscoped() {
        use crate::schema::tokens::dsl::{principal_id, scoped, tokens};

        let context = TestContext::new().await;
        let pool = &context.pool;
        let group = create_test_group(pool).await;
        let sa = create_test_service_account(pool, &group, None).await;

        let resp = post_request(
            pool,
            &context.admin_token,
            &format!("{PRINCIPALS_ENDPOINT}/{}/tokens", sa.id),
            &serde_json::json!({ "name": "unscoped-tok" }),
        )
        .await;
        assert_eq!(
            resp.status(),
            StatusCode::CREATED,
            "precondition: token mint succeeds"
        );

        let scoped_flags: Vec<bool> = with_connection(pool, |conn| {
            tokens
                .filter(principal_id.eq(sa.id))
                .select(scoped)
                .load::<bool>(conn)
        })
        .unwrap();

        assert_eq!(scoped_flags, vec![false]);
    }

    /// #2 / #13 (mechanism): scope intersection is fail-closed. `None` (unscoped)
    /// allows; a present-but-empty scope set denies all; otherwise every requested
    /// permission must be in the scope set.
    #[rstest]
    #[case::unscoped_allows(None, vec![Permissions::ReadCollection], true)]
    #[case::empty_denies_all(Some(vec![]), vec![Permissions::ReadCollection], false)]
    #[case::match_allows(
        Some(vec![Permissions::ReadCollection]),
        vec![Permissions::ReadCollection],
        true
    )]
    #[case::mismatch_denies(
        Some(vec![Permissions::ReadCollection]),
        vec![Permissions::CreateClass],
        false
    )]
    #[actix_web::test]
    async fn test_scope_allows(
        #[case] scopes: Option<Vec<Permissions>>,
        #[case] requested: Vec<Permissions>,
        #[case] expected: bool,
    ) {
        assert_eq!(scope_allows(scopes.as_deref(), &requested), expected);
    }

    // ----- Batch 3: authz / scoped behavior -----

    const NAMESPACES_ENDPOINT: &str = "/api/v1/namespaces";
    const USERS_ENDPOINT: &str = "/api/v1/iam/users";

    /// #6 / #16: a service account gets a group's namespace permissions only via
    /// explicit membership — a fresh SA (owner group set, no membership) has none.
    #[rstest]
    #[case::member_allowed(true, StatusCode::OK)]
    #[case::nonmember_denied(false, StatusCode::FORBIDDEN)]
    #[actix_web::test]
    async fn test_service_account_namespace_access_requires_membership(
        #[case] in_group: bool,
        #[case] expected: StatusCode,
    ) {
        let context = TestContext::new().await;
        let pool = &context.pool;
        let fixture = context.with_namespace().await;
        let sa = create_test_service_account(pool, &fixture.owner_group, None).await;

        if in_group {
            fixture.owner_group.add_member(pool, &sa).await.unwrap();
        }

        let token = service_account_token(pool, &sa, None, None).await;
        let resp = get_request(
            pool,
            &token,
            &format!("{NAMESPACES_ENDPOINT}/{}", fixture.namespace.id),
        )
        .await;

        assert_eq!(resp.status(), expected);
    }

    /// #2: a scoped token allows an in-scope permission — read is in scope, so the
    /// namespace GET (ReadCollection) succeeds even though the token is scoped.
    #[actix_web::test]
    async fn test_scoped_token_allows_in_scope_permission() {
        let context = TestContext::new().await;
        let pool = &context.pool;
        let fixture = context.with_namespace().await;
        let sa = create_test_service_account(pool, &fixture.owner_group, None).await;
        fixture.owner_group.add_member(pool, &sa).await.unwrap();

        let token = scoped_token(pool, sa.id, &[Permissions::ReadCollection]).await;
        let resp = get_request(
            pool,
            &token,
            &format!("{NAMESPACES_ENDPOINT}/{}", fixture.namespace.id),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::OK);
    }

    /// #2: a scoped token denies an out-of-scope permission — the group grants
    /// UpdateCollection, but the token's scope is read-only, so the PATCH is denied.
    #[actix_web::test]
    async fn test_scoped_token_denies_out_of_scope_permission() {
        let context = TestContext::new().await;
        let pool = &context.pool;
        let fixture = context.with_namespace().await;
        let sa = create_test_service_account(pool, &fixture.owner_group, None).await;
        fixture.owner_group.add_member(pool, &sa).await.unwrap();

        let token = scoped_token(pool, sa.id, &[Permissions::ReadCollection]).await;
        let resp = patch_request(
            pool,
            &token,
            &format!("{NAMESPACES_ENDPOINT}/{}", fixture.namespace.id),
            &serde_json::json!({ "description": "scoped-write-attempt" }),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    /// #9 / #36: the human/admin extractors reject non-human/scoped callers. An
    /// unscoped human admin is allowed; a scoped human-admin token is rejected
    /// (scope gate, #9); a service account in the admin group with an unscoped
    /// token is rejected (kind gate, #36) — Forbidden, never a 500.
    #[derive(Clone, Copy)]
    enum AdminCaller {
        HumanAdminUnscoped,
        HumanAdminScoped,
        ServiceAccountInAdminGroup,
    }

    #[rstest]
    #[case::human_admin_unscoped(AdminCaller::HumanAdminUnscoped, StatusCode::OK)]
    #[case::human_admin_scoped(AdminCaller::HumanAdminScoped, StatusCode::FORBIDDEN)]
    #[case::sa_in_admin_group(AdminCaller::ServiceAccountInAdminGroup, StatusCode::FORBIDDEN)]
    #[actix_web::test]
    async fn test_admin_extractor_rejects_scoped_and_service_accounts(
        #[case] caller: AdminCaller,
        #[case] expected: StatusCode,
    ) {
        let context = TestContext::new().await;
        let pool = &context.pool;

        let token = match caller {
            AdminCaller::HumanAdminUnscoped => context.admin_token.clone(),
            AdminCaller::HumanAdminScoped => {
                scoped_token(pool, context.admin_user.id, &[Permissions::ReadCollection]).await
            }
            AdminCaller::ServiceAccountInAdminGroup => {
                let admin_group = ensure_admin_group(pool).await;
                let group = create_test_group(pool).await;
                let sa = create_test_service_account(pool, &group, None).await;
                admin_group.add_member(pool, &sa).await.unwrap();
                service_account_token(pool, &sa, None, None).await
            }
        };

        let resp = get_request(pool, &token, USERS_ENDPOINT).await;
        assert_eq!(resp.status(), expected);
    }

    // ----- Batch 4: principal routes / group membership / namespace perms -----

    const IAM_GROUPS_ENDPOINT: &str = "/api/v1/iam/groups";

    /// #7: minting an SA's tokens is allowed for an admin or a human member of the
    /// SA's owner group, and denied for a non-member non-admin.
    #[derive(Clone, Copy)]
    enum TokenManager {
        Admin,
        OwnerGroupMember,
        Outsider,
    }

    #[rstest]
    #[case::admin(TokenManager::Admin, StatusCode::CREATED)]
    #[case::owner_group_member(TokenManager::OwnerGroupMember, StatusCode::CREATED)]
    #[case::outsider(TokenManager::Outsider, StatusCode::NOT_FOUND)]
    #[actix_web::test]
    async fn test_sa_token_mint_authz(#[case] caller: TokenManager, #[case] expected: StatusCode) {
        let context = TestContext::new().await;
        let pool = &context.pool;
        let group = create_test_group(pool).await;
        let sa = create_test_service_account(pool, &group, None).await;

        let token = match caller {
            TokenManager::Admin => context.admin_token.clone(),
            TokenManager::OwnerGroupMember => {
                let member = create_test_user(pool).await;
                group.add_member(pool, &member).await.unwrap();
                member.create_token(pool).await.unwrap().get_token()
            }
            TokenManager::Outsider => {
                let outsider = create_test_user(pool).await;
                outsider.create_token(pool).await.unwrap().get_token()
            }
        };

        let resp = post_request(
            pool,
            &token,
            &format!("{PRINCIPALS_ENDPOINT}/{}/tokens", sa.id),
            &serde_json::json!({ "name": "minted" }),
        )
        .await;

        assert_eq!(resp.status(), expected);
    }

    /// #22: revoke is scoped by BOTH path ids — revoking a token id under the wrong
    /// principal path is a 404, while the owning principal path succeeds (204).
    #[rstest]
    #[case::owning_principal(true, StatusCode::NO_CONTENT)]
    #[case::wrong_principal(false, StatusCode::NOT_FOUND)]
    #[actix_web::test]
    async fn test_token_revoke_is_path_scoped(
        #[case] use_owning_principal: bool,
        #[case] expected: StatusCode,
    ) {
        let context = TestContext::new().await;
        let pool = &context.pool;
        let group = create_test_group(pool).await;
        let owner = create_test_service_account(pool, &group, None).await;
        let other = create_test_service_account(pool, &group, None).await;

        service_account_token(pool, &owner, None, None).await;
        let token_id: i32 = with_connection(pool, |conn| {
            use crate::schema::tokens::dsl::{id, principal_id, tokens};
            tokens
                .filter(principal_id.eq(owner.id))
                .select(id)
                .order(id.desc())
                .first::<i32>(conn)
        })
        .unwrap();

        let path_principal = if use_owning_principal {
            owner.id
        } else {
            other.id
        };
        let resp = post_request(
            pool,
            &context.admin_token,
            &format!("{PRINCIPALS_ENDPOINT}/{path_principal}/tokens/{token_id}/revoke"),
            &serde_json::json!({}),
        )
        .await;

        assert_eq!(resp.status(), expected);
    }

    /// #23 / #33: the management endpoints reject non-human and scoped callers. A
    /// service-account token (even for an SA in its own owner group) and a scoped
    /// human token are both Forbidden from minting tokens.
    #[derive(Clone, Copy)]
    enum NonManager {
        ServiceAccountInOwnGroup,
        ScopedHuman,
    }

    #[rstest]
    #[case::service_account(NonManager::ServiceAccountInOwnGroup)]
    #[case::scoped_human(NonManager::ScopedHuman)]
    #[actix_web::test]
    async fn test_management_endpoint_rejects_sa_and_scoped_callers(#[case] caller: NonManager) {
        let context = TestContext::new().await;
        let pool = &context.pool;
        let group = create_test_group(pool).await;
        let sa = create_test_service_account(pool, &group, None).await;

        let (token, target_principal) = match caller {
            NonManager::ServiceAccountInOwnGroup => {
                group.add_member(pool, &sa).await.unwrap();
                (service_account_token(pool, &sa, None, None).await, sa.id)
            }
            NonManager::ScopedHuman => {
                let token =
                    scoped_token(pool, context.admin_user.id, &[Permissions::ReadCollection]).await;
                (token, sa.id)
            }
        };

        let resp = post_request(
            pool,
            &token,
            &format!("{PRINCIPALS_ENDPOINT}/{target_principal}/tokens"),
            &serde_json::json!({ "name": "nope" }),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    /// #29: the principal-shaped group-listing route works for a service-account
    /// principal.
    #[actix_web::test]
    async fn test_principal_groups_route_works_for_service_account() {
        let context = TestContext::new().await;
        let pool = &context.pool;
        let group = create_test_group(pool).await;
        let sa = create_test_service_account(pool, &group, None).await;
        group.add_member(pool, &sa).await.unwrap();

        let resp = get_request(
            pool,
            &context.admin_token,
            &format!("{PRINCIPALS_ENDPOINT}/{}/groups", sa.id),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::OK);
    }

    /// #29: the principal-shaped namespace effective-permission route works for a
    /// service-account principal (it has perms via its owner group).
    #[actix_web::test]
    async fn test_namespace_principal_permissions_route_for_service_account() {
        let context = TestContext::new().await;
        let pool = &context.pool;
        let fixture = context.with_namespace().await;
        let sa = create_test_service_account(pool, &fixture.owner_group, None).await;
        fixture.owner_group.add_member(pool, &sa).await.unwrap();

        let resp = get_request(
            pool,
            &context.admin_token,
            &format!(
                "{NAMESPACES_ENDPOINT}/{}/permissions/principal/{}",
                fixture.namespace.id, sa.id
            ),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::OK);
    }

    /// #21: group-membership mutation is admin-only — a non-admin human cannot add
    /// a member.
    #[rstest]
    #[case::admin(true, StatusCode::NO_CONTENT)]
    #[case::non_admin(false, StatusCode::FORBIDDEN)]
    #[actix_web::test]
    async fn test_group_member_mutation_is_admin_only(
        #[case] as_admin: bool,
        #[case] expected: StatusCode,
    ) {
        let context = TestContext::new().await;
        let pool = &context.pool;
        let group = create_test_group(pool).await;
        let sa = create_test_service_account(pool, &group, None).await;

        let token = if as_admin {
            context.admin_token.clone()
        } else {
            context.normal_token.clone()
        };

        let resp = post_request(
            pool,
            &token,
            &format!("{IAM_GROUPS_ENDPOINT}/{}/members/{}", group.id, sa.id),
            &serde_json::json!({}),
        )
        .await;

        assert_eq!(resp.status(), expected);
    }

    /// A principal's effective-permissions report lists each namespace it can act
    /// on, broken down by the granting group (here, its owner group's grant).
    #[actix_web::test]
    async fn test_principal_permissions_report_groups_by_granting_group() {
        use crate::api::v1::handlers::principals::PrincipalNamespacePermissions;

        let context = TestContext::new().await;
        let pool = &context.pool;
        let fixture = context.with_namespace().await;
        let sa = create_test_service_account(pool, &fixture.owner_group, None).await;
        fixture.owner_group.add_member(pool, &sa).await.unwrap();

        let resp = get_request(
            pool,
            &context.admin_token,
            &format!("{PRINCIPALS_ENDPOINT}/{}/permissions", sa.id),
        )
        .await;
        let report: Vec<PrincipalNamespacePermissions> = test::read_body_json(resp).await;

        let namespace = report
            .iter()
            .find(|n| n.namespace_id == fixture.namespace.id)
            .expect("report should include the namespace the SA has access to");
        let grant_from_owner = namespace
            .grants
            .iter()
            .any(|g| g.group_id == fixture.owner_group.id && !g.permissions.is_empty());

        assert!(
            grant_from_owner,
            "report should attribute the namespace permissions to the owner group"
        );
    }

    /// #21: group member listing returns both human and service-account members,
    /// each tagged with its `kind`.
    #[actix_web::test]
    async fn test_group_member_listing_includes_both_kinds() {
        let context = TestContext::new().await;
        let pool = &context.pool;
        let group = create_test_group(pool).await;
        let human = create_test_user(pool).await;
        let sa = create_test_service_account(pool, &group, None).await;
        group.add_member(pool, &human).await.unwrap();
        group.add_member(pool, &sa).await.unwrap();

        let resp = get_request(
            pool,
            &context.admin_token,
            &format!("{IAM_GROUPS_ENDPOINT}/{}/members", group.id),
        )
        .await;
        let members: Vec<PrincipalMemberResponse> = test::read_body_json(resp).await;

        let kinds: std::collections::HashSet<&str> =
            members.iter().map(|m| m.kind.as_str()).collect();
        assert!(
            kinds.contains("human") && kinds.contains("service_account"),
            "member listing should include both kinds, got {kinds:?}"
        );
    }

    // ----- Batch 5: task ownership / scope snapshot -----

    /// Persist a synthetic task owned by `submitted_by`. `scopes = Some(..)` marks
    /// the task as submitted by a scoped token and stores the scope snapshot.
    async fn synthetic_task(
        pool: &crate::db::DbPool,
        submitted_by: i32,
        status: TaskStatus,
        idempotency_key: Option<String>,
        scopes: Option<&[Permissions]>,
    ) -> TaskRecord {
        NewTaskRecord {
            kind: TaskKind::Report.as_str().to_string(),
            status: status.as_str().to_string(),
            submitted_by: Some(submitted_by),
            submitted_token_id: None,
            submitted_token_scoped: scopes.is_some(),
            submitted_token_scopes: scope_snapshot_json(scopes),
            idempotency_key,
            request_hash: None,
            request_payload: None,
            summary: None,
            total_items: 0,
            processed_items: 0,
            success_items: 0,
            failed_items: 0,
            request_redacted_at: None,
            started_at: None,
            finished_at: None,
        }
        .create(pool)
        .await
        .unwrap()
    }

    /// #17: a service account can own a task (`submitted_by` = SA principal id).
    #[actix_web::test]
    async fn test_service_account_can_own_a_task() {
        let context = TestContext::new().await;
        let pool = &context.pool;
        let group = create_test_group(pool).await;
        let sa = create_test_service_account(pool, &group, None).await;

        let task = synthetic_task(pool, sa.id, TaskStatus::Queued, None, None).await;

        assert_eq!(task.submitted_by, Some(sa.id));
    }

    /// #17: idempotency is keyed by `(submitted_by, idempotency_key)` over
    /// principal ids — the same key under a different principal does not collide.
    #[rstest]
    #[case::same_principal(true, true)]
    #[case::different_principal(false, false)]
    #[actix_web::test]
    async fn test_task_idempotency_is_per_principal(
        #[case] same_principal: bool,
        #[case] expect_found: bool,
    ) {
        let context = TestContext::new().await;
        let pool = &context.pool;
        let group = create_test_group(pool).await;
        let sa = create_test_service_account(pool, &group, None).await;
        let other = create_test_service_account(pool, &group, None).await;

        let key = format!("idem-{}", generate_random_password(8));
        synthetic_task(pool, sa.id, TaskStatus::Queued, Some(key.clone()), None).await;

        let lookup = if same_principal { sa.id } else { other.id };
        let found = TaskRecord::find_by_idempotency(pool, lookup, &key)
            .await
            .unwrap();

        assert_eq!(found.is_some(), expect_found);
    }

    /// #19: an SA-submitted task is loadable by an admin, the SA itself, and a
    /// human member of the SA's owner group, but not by an unrelated non-admin.
    #[derive(Clone, Copy)]
    enum TaskViewer {
        Admin,
        ServiceAccountItself,
        OwnerGroupMember,
        Unrelated,
    }

    #[rstest]
    #[case::admin(TaskViewer::Admin, true)]
    #[case::sa_itself(TaskViewer::ServiceAccountItself, true)]
    #[case::owner_group_member(TaskViewer::OwnerGroupMember, true)]
    #[case::unrelated(TaskViewer::Unrelated, false)]
    #[actix_web::test]
    async fn test_sa_task_load_authorization(#[case] viewer: TaskViewer, #[case] allowed: bool) {
        let context = TestContext::new().await;
        let pool = &context.pool;
        let group = create_test_group(pool).await;
        let sa = create_test_service_account(pool, &group, None).await;
        let task = synthetic_task(pool, sa.id, TaskStatus::Queued, None, None).await;
        let task_id = TaskID::new(task.id).unwrap();

        let subject_id = match viewer {
            TaskViewer::Admin => context.admin_user.id,
            TaskViewer::ServiceAccountItself => sa.id,
            TaskViewer::OwnerGroupMember => {
                let member = create_test_user(pool).await;
                group.add_member(pool, &member).await.unwrap();
                member.id
            }
            TaskViewer::Unrelated => create_test_user(pool).await.id,
        };
        let subject = PrincipalID::new(subject_id).unwrap();

        let result = task_id.load_authorized(pool, &subject).await;
        assert_eq!(result.is_ok(), allowed);
    }

    /// #28: disabling an SA cancels its pending tasks (the disable flow calls
    /// `cancel_pending_tasks_for_principal`).
    #[actix_web::test]
    async fn test_disabled_sa_pending_task_is_cancelled() {
        let context = TestContext::new().await;
        let pool = &context.pool;
        let group = create_test_group(pool).await;
        let sa = create_test_service_account(pool, &group, None).await;
        let task = synthetic_task(pool, sa.id, TaskStatus::Queued, None, None).await;

        cancel_pending_tasks_for_principal(pool, sa.id)
            .await
            .unwrap();

        let status: String = with_connection(pool, |conn| {
            use crate::schema::tasks::dsl::{id, status, tasks};
            tasks
                .filter(id.eq(task.id))
                .select(status)
                .first::<String>(conn)
        })
        .unwrap();
        assert_eq!(status, TaskStatus::Cancelled.as_str());
    }

    /// #27: a task submitted by a scoped token persists its scope boundary
    /// (`submitted_token_scoped = true`) for the worker to reconstruct later.
    #[actix_web::test]
    async fn test_scoped_task_persists_scope_snapshot() {
        let context = TestContext::new().await;
        let pool = &context.pool;
        let group = create_test_group(pool).await;
        let sa = create_test_service_account(pool, &group, None).await;

        let task = synthetic_task(
            pool,
            sa.id,
            TaskStatus::Queued,
            None,
            Some(&[Permissions::ReadCollection]),
        )
        .await;

        assert!(task.submitted_token_scoped);
    }

    /// #35: snapshot reconstruction is fail-closed — the worker parses every scope
    /// string through `Permissions::from_string`, which rejects unknown values.
    #[rstest]
    #[case::known("ReadCollection", true)]
    #[case::unknown("NotARealPermission", false)]
    #[actix_web::test]
    async fn test_snapshot_permission_parse_is_fail_closed(
        #[case] raw: &str,
        #[case] expected_ok: bool,
    ) {
        assert_eq!(Permissions::from_string(raw).is_ok(), expected_ok);
    }

    // ----- Review fixes -----

    /// Review #1: `user_can_on_any` (used by the template/remote-target listings)
    /// intersects with token scopes fail-closed — a token scoped away from the
    /// requested permission sees nothing, even though the group grants it.
    #[rstest]
    #[case::unscoped(None, true)]
    #[case::in_scope(Some(vec![Permissions::ReadTemplate]), true)]
    #[case::out_of_scope(Some(vec![Permissions::ReadCollection]), false)]
    #[actix_web::test]
    async fn test_user_can_on_any_respects_scopes(
        #[case] scope: Option<Vec<Permissions>>,
        #[case] expect_visible: bool,
    ) {
        let context = TestContext::new().await;
        let pool = &context.pool;
        let fixture = context.with_namespace().await;
        let group = create_test_group(pool).await;
        fixture
            .namespace
            .grant_one(pool, group.id, Permissions::ReadTemplate)
            .await
            .unwrap();
        let sa = create_test_service_account(pool, &group, None).await;
        group.add_member(pool, &sa).await.unwrap();

        let visible = user_can_on_any(pool, &sa, Permissions::ReadTemplate, scope.as_deref())
            .await
            .unwrap()
            .iter()
            .any(|n| n.id == fixture.namespace.id);

        assert_eq!(visible, expect_visible);
    }

    /// Review #2: a service account can read a task it submitted; an unrelated
    /// service account gets a 404 (its token is accepted, then load_authorized
    /// denies). Before the fix, the SA token was rejected outright.
    #[rstest]
    #[case::submitting_sa(true, StatusCode::OK)]
    #[case::unrelated_sa(false, StatusCode::NOT_FOUND)]
    #[actix_web::test]
    async fn test_service_account_can_read_its_own_task(
        #[case] own: bool,
        #[case] expected: StatusCode,
    ) {
        let context = TestContext::new().await;
        let pool = &context.pool;
        let group = create_test_group(pool).await;
        let sa = create_test_service_account(pool, &group, None).await;
        let other = create_test_service_account(pool, &group, None).await;
        let task = synthetic_task(pool, sa.id, TaskStatus::Queued, None, None).await;

        let reader = if own { &sa } else { &other };
        let token = service_account_token(pool, reader, None, None).await;
        let resp = get_request(pool, &token, &format!("/api/v1/tasks/{}", task.id)).await;

        assert_eq!(resp.status(), expected);
    }

    /// Review #3: namespace search requires `ReadCollection`, not merely *some*
    /// permission row — a group holding only `CreateClass` does not make the
    /// namespace visible.
    #[rstest]
    #[case::read_collection_visible(Permissions::ReadCollection, true)]
    #[case::create_class_only_hidden(Permissions::CreateClass, false)]
    #[actix_web::test]
    async fn test_namespace_search_requires_read_collection(
        #[case] granted: Permissions,
        #[case] expect_visible: bool,
    ) {
        let context = TestContext::new().await;
        let pool = &context.pool;
        let fixture = context.with_namespace().await;
        let group = create_test_group(pool).await;
        fixture
            .namespace
            .grant_one(pool, group.id, granted)
            .await
            .unwrap();
        let sa = create_test_service_account(pool, &group, None).await;
        group.add_member(pool, &sa).await.unwrap();
        let token = service_account_token(pool, &sa, None, None).await;

        let resp = get_request(pool, &token, NAMESPACES_ENDPOINT).await;
        let namespaces: Vec<Namespace> = test::read_body_json(resp).await;
        let visible = namespaces.iter().any(|n| n.id == fixture.namespace.id);

        assert_eq!(visible, expect_visible);
    }

    /// Review #4: deleting a group that still owns service accounts returns a clear
    /// 409 Conflict; a group with no owned SAs deletes normally.
    #[rstest]
    #[case::owns_service_account(true, StatusCode::CONFLICT)]
    #[case::no_service_accounts(false, StatusCode::NO_CONTENT)]
    #[actix_web::test]
    async fn test_group_delete_conflicts_when_owning_service_accounts(
        #[case] owns_sa: bool,
        #[case] expected: StatusCode,
    ) {
        let context = TestContext::new().await;
        let pool = &context.pool;
        let group = create_test_group(pool).await;
        if owns_sa {
            create_test_service_account(pool, &group, None).await;
        }

        let resp = delete_request(
            pool,
            &context.admin_token,
            &format!("{IAM_GROUPS_ENDPOINT}/{}", group.id),
        )
        .await;

        assert_eq!(resp.status(), expected);
    }

    // ----- Review fixes (round 2) -----

    const SERVICE_ACCOUNTS_ENDPOINT: &str = "/api/v1/iam/service-accounts";

    /// Reassigning an SA's owner group requires authority over the TARGET group:
    /// an admin may move it anywhere; a non-admin may move it only to a group they
    /// belong to (managing the current group alone is not enough).
    #[derive(Clone, Copy)]
    enum Reassigner {
        Admin,
        MemberOfBothGroups,
        MemberOfCurrentGroupOnly,
    }

    #[rstest]
    #[case::admin(Reassigner::Admin, StatusCode::OK)]
    #[case::member_of_both(Reassigner::MemberOfBothGroups, StatusCode::OK)]
    #[case::member_of_current_only(Reassigner::MemberOfCurrentGroupOnly, StatusCode::FORBIDDEN)]
    #[actix_web::test]
    async fn test_owner_group_reassignment_authz(
        #[case] caller: Reassigner,
        #[case] expected: StatusCode,
    ) {
        let context = TestContext::new().await;
        let pool = &context.pool;
        let current_group = create_test_group(pool).await;
        let target_group = create_test_group(pool).await;
        let sa = create_test_service_account(pool, &current_group, None).await;

        let token = match caller {
            Reassigner::Admin => context.admin_token.clone(),
            Reassigner::MemberOfBothGroups => {
                let user = create_test_user(pool).await;
                current_group.add_member(pool, &user).await.unwrap();
                target_group.add_member(pool, &user).await.unwrap();
                user.create_token(pool).await.unwrap().get_token()
            }
            Reassigner::MemberOfCurrentGroupOnly => {
                let user = create_test_user(pool).await;
                current_group.add_member(pool, &user).await.unwrap();
                user.create_token(pool).await.unwrap().get_token()
            }
        };

        let resp = patch_request(
            pool,
            &token,
            &format!("{SERVICE_ACCOUNTS_ENDPOINT}/{}", sa.id),
            &serde_json::json!({ "owner_group_id": target_group.id }),
        )
        .await;

        assert_eq!(resp.status(), expected);
    }

    /// Cancelling pending work must NOT touch an already-claimed `running` task —
    /// it can't be stopped, and mislabelling it `cancelled` while it keeps
    /// performing side effects is the bug this guards against. (The `queued`
    /// positive is not asserted here: the global task worker may legitimately
    /// claim a queued task mid-test, which would race this check.)
    #[actix_web::test]
    async fn test_cancel_pending_does_not_cancel_running_tasks() {
        let context = TestContext::new().await;
        let pool = &context.pool;
        let group = create_test_group(pool).await;
        let sa = create_test_service_account(pool, &group, None).await;
        let task = synthetic_task(pool, sa.id, TaskStatus::Running, None, None).await;

        cancel_pending_tasks_for_principal(pool, sa.id)
            .await
            .unwrap();

        let status: String = with_connection(pool, |conn| {
            use crate::schema::tasks::dsl::{id, status, tasks};
            tasks
                .filter(id.eq(task.id))
                .select(status)
                .first::<String>(conn)
        })
        .unwrap();
        assert_eq!(status, TaskStatus::Running.as_str());
    }

    /// The listing applies the manageability filter in SQL: a non-admin sees only
    /// service accounts owned by groups they belong to.
    #[actix_web::test]
    async fn test_sa_listing_filters_by_owner_group_for_non_admin() {
        let context = TestContext::new().await;
        let pool = &context.pool;
        let owned_group = create_test_group(pool).await;
        let other_group = create_test_group(pool).await;
        let member = create_test_user(pool).await;
        owned_group.add_member(pool, &member).await.unwrap();

        let visible_sa = create_test_service_account(pool, &owned_group, None).await;
        let hidden_sa = create_test_service_account(pool, &other_group, None).await;
        let token = member.create_token(pool).await.unwrap().get_token();

        let resp = get_request(pool, &token, SERVICE_ACCOUNTS_ENDPOINT).await;
        let accounts: Vec<ServiceAccountResponse> = test::read_body_json(resp).await;
        let ids: std::collections::HashSet<i32> = accounts.iter().map(|a| a.id).collect();

        assert!(
            ids.contains(&visible_sa.id) && !ids.contains(&hidden_sa.id),
            "non-admin must see only owner-group SAs, got {ids:?}"
        );
    }

    /// The listing honours the cursor page `limit` (isolated from concurrent tests
    /// via a unique name prefix).
    #[actix_web::test]
    async fn test_sa_listing_honours_page_limit() {
        let context = TestContext::new().await;
        let pool = &context.pool;
        let group = create_test_group(pool).await;
        let prefix = format!("page-{}", generate_random_password(8));
        for index in 0..3 {
            NewServiceAccount {
                name: format!("{prefix}-{index}"),
                description: None,
                owner_group_id: group.id,
            }
            .save(pool, None)
            .await
            .unwrap();
        }

        let resp = get_request(
            pool,
            &context.admin_token,
            &format!("{SERVICE_ACCOUNTS_ENDPOINT}?name__contains={prefix}&limit=2&sort=id"),
        )
        .await;
        let page: Vec<ServiceAccountResponse> = test::read_body_json(resp).await;

        assert_eq!(page.len(), 2);
    }
}
