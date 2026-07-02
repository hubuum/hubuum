#[cfg(test)]
mod tests {
    use crate::db::traits::ActiveTokens;
    use crate::models::group::NewGroup;
    use crate::models::user::{LoginUser, NewUser, UpdateUser, User, UserID, UserResponse};
    use crate::models::{Group, PrincipalTokenMetadata};
    use crate::pagination::NEXT_CURSOR_HEADER;
    use actix_web::{http::StatusCode, test};
    use rstest::rstest;

    use crate::tests::api_operations::{delete_request, get_request, patch_request, post_request};
    use crate::tests::asserts::{
        assert_paginated_collection_total_count, assert_response_status, header_value,
    };
    use crate::tests::{TestContext, create_test_admin, create_test_user, test_context};

    const USERS_ENDPOINT: &str = "/api/v1/iam/users";
    const PRINCIPALS_ENDPOINT: &str = "/api/v1/iam/principals";

    async fn assert_user_response_matches(
        pool: &crate::db::DbPool,
        user: &User,
        response: &UserResponse,
    ) {
        assert_eq!(response.id, user.id);
        assert_eq!(response.name, user.name(pool).await.unwrap());
        assert_eq!(response.email, user.email);
        assert_eq!(response.created_at, user.created_at);
        assert_eq!(response.updated_at, user.updated_at);
    }

    async fn check_show_user(
        context: &TestContext,
        target: &User,
        requester: &User,
        expected_status: StatusCode,
    ) {
        let token = requester
            .create_token(&context.pool)
            .await
            .unwrap()
            .get_token();

        let resp = get_request(
            &context.pool,
            &token,
            &format!("{}/{}", USERS_ENDPOINT, &target.id),
        )
        .await;
        let resp = assert_response_status(resp, expected_status).await;

        if expected_status == StatusCode::OK {
            let body = test::read_body(resp).await;
            let returned_value: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert!(returned_value.get("password").is_none());
            let returned_user: UserResponse = serde_json::from_value(returned_value).unwrap();
            assert_user_response_matches(&context.pool, target, &returned_user).await;
        }
    }

    async fn check_show_user_tokens(
        context: &TestContext,
        target: &User,
        requester: &User,
        expected_status: StatusCode,
    ) {
        let token = requester
            .create_token(&context.pool)
            .await
            .unwrap()
            .get_token();

        let resp = get_request(
            &context.pool,
            &token,
            &format!("{}/{}/tokens", PRINCIPALS_ENDPOINT, &target.id),
        )
        .await;
        let _ = assert_response_status(resp, expected_status).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_show_user(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let test_user = create_test_user(&context.pool).await;
        let test_admin_user = create_test_admin(&context.pool).await;

        // The format here is (target, requester, expected_status).
        check_show_user(&context, &test_user, &test_user, StatusCode::OK).await;
        check_show_user(
            &context,
            &test_admin_user,
            &test_user,
            StatusCode::FORBIDDEN,
        )
        .await;
        check_show_user(&context, &test_user, &test_admin_user, StatusCode::OK).await;

        // Tokens are admin_or_self. Note that the format is (target, requester, expected_status).
        check_show_user_tokens(&context, &test_user, &test_user, StatusCode::OK).await;
        check_show_user_tokens(
            &context,
            &test_admin_user,
            &test_user,
            StatusCode::NOT_FOUND,
        )
        .await;
        check_show_user_tokens(&context, &test_user, &test_admin_user, StatusCode::OK).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_create_and_delete_user(#[future(awt)] test_context: TestContext) {
        let context = test_context;

        let new_user = NewUser {
            name: "test_create_user_endpoint".to_string(),
            password: "testpassword".to_string(),
            proper_name: Some("Test Create User".to_string()),
            email: None,
        };

        // Just checking that only admins can create users...
        let resp = post_request(
            &context.pool,
            &context.normal_token,
            USERS_ENDPOINT,
            &new_user,
        )
        .await;
        let _ = assert_response_status(resp, StatusCode::FORBIDDEN).await;

        let resp = post_request(
            &context.pool,
            &context.admin_token,
            USERS_ENDPOINT,
            &new_user,
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;

        let headers = resp.headers().clone();
        let created_user_url = headers.get("Location").unwrap().to_str().unwrap();
        let body = test::read_body(resp).await;
        let created_value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(created_value.get("password").is_none());
        let created_user_from_create: UserResponse = serde_json::from_value(created_value).unwrap();

        let resp = get_request(&context.pool, &context.admin_token, created_user_url).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let created_user_from_get: UserResponse = test::read_body_json(resp).await;

        assert_eq!(created_user_from_create, created_user_from_get);

        // Validate that the location is what we expect
        assert_eq!(
            created_user_url,
            &format!("{}/{}", USERS_ENDPOINT, created_user_from_get.id)
        );

        // And only admins can delete users...
        let resp = delete_request(&context.pool, &context.normal_token, created_user_url).await;
        let _ = assert_response_status(resp, StatusCode::FORBIDDEN).await;

        let resp = delete_request(&context.pool, &context.admin_token, created_user_url).await;
        let _ = assert_response_status(resp, StatusCode::NO_CONTENT).await;

        let resp = get_request(&context.pool, &context.admin_token, created_user_url).await;
        let _ = assert_response_status(resp, StatusCode::NOT_FOUND).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_patch_user(#[future(awt)] test_context: TestContext) {
        let context = test_context;

        // Test setting a new password
        let updated_user = UpdateUser {
            password: Some("newpassword".to_string()),
            proper_name: Some("Updated Proper Name".to_string()),
            email: None,
        };

        let test_user = create_test_user(&context.pool).await;
        let patch_url = format!("{}/{}", USERS_ENDPOINT, test_user.id);

        // Only admins can patch users...
        let resp = patch_request(
            &context.pool,
            &context.normal_token,
            &patch_url,
            &updated_user,
        )
        .await;
        let _ = assert_response_status(resp, StatusCode::FORBIDDEN).await;

        let resp = patch_request(
            &context.pool,
            &context.admin_token,
            &patch_url,
            &updated_user,
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let body = test::read_body(resp).await;
        let patched_value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(patched_value.get("password").is_none());
        let patched_user: UserResponse = serde_json::from_value(patched_value).unwrap();

        assert_eq!(
            patched_user.name,
            test_user.name(&context.pool).await.unwrap()
        );
        assert_eq!(patched_user.proper_name, updated_user.proper_name.clone());
        assert_eq!(patched_user.email, test_user.email);

        let stored_user = UserID::new(test_user.id)
            .unwrap()
            .user(&context.pool)
            .await
            .unwrap();
        assert_ne!(stored_user.password, test_user.password);
        LoginUser {
            name: test_user.name(&context.pool).await.unwrap(),
            password: "newpassword".to_string(),
        }
        .login(&context.pool)
        .await
        .unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn test_list_users_requires_admin(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let resp = get_request(&context.pool, &context.normal_token, USERS_ENDPOINT).await;
        let _ = assert_response_status(resp, StatusCode::FORBIDDEN).await;
    }

    #[rstest]
    #[case::id_asc("id.asc", &[0, 1, 2])]
    #[case::id_desc("id.desc", &[2, 1, 0])]
    #[case::name_asc("name.asc", &[0, 1, 2])]
    #[case::name_desc("name.desc", &[2, 1, 0])]
    #[case::proper_name_asc("proper_name.asc", &[0, 1, 2])]
    #[case::proper_name_desc("proper_name.desc", &[2, 1, 0])]
    #[actix_web::test]
    async fn test_list_users_sorted(
        #[case] sort_order: &str,
        #[case] expected_order: &[usize],
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let prefix = format!("test_list_users_sorted_{}", sort_order.replace('.', "_"));

        let mut created_users = Vec::new();
        for i in 0..3 {
            let user = NewUser {
                name: format!("{prefix}_{i}"),
                password: "testpassword".to_string(),
                proper_name: Some(format!("{prefix} Proper {i}")),
                email: Some(format!("{prefix}_{i}@example.com")),
            }
            .save(&context.pool)
            .await
            .unwrap();
            created_users.push(user);
        }

        let url = format!("{USERS_ENDPOINT}?name__contains={prefix}&sort={sort_order}");
        let resp = get_request(&context.pool, &context.admin_token, &url).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let users: Vec<UserResponse> = test::read_body_json(resp).await;

        assert_eq!(users.len(), created_users.len());
        assert_eq!(users[0].id, created_users[expected_order[0]].id);
        assert_eq!(users[1].id, created_users[expected_order[1]].id);
        assert_eq!(users[2].id, created_users[expected_order[2]].id);

        for user in created_users {
            user.delete(&context.pool).await.unwrap();
        }
    }

    #[rstest]
    #[actix_web::test]
    async fn test_list_users_filter_by_proper_name(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let prefix = "proper_name_filter";

        let matching_user = NewUser {
            name: format!("{prefix}_match_username"),
            password: "testpassword".to_string(),
            proper_name: Some(format!("{prefix}_match_display")),
            email: Some(format!("{prefix}_match@example.com")),
        }
        .save(&context.pool)
        .await
        .unwrap();

        let other_user = NewUser {
            name: format!("{prefix}_other_username"),
            password: "testpassword".to_string(),
            proper_name: Some(format!("{prefix}_other_display")),
            email: Some(format!("{prefix}_other@example.com")),
        }
        .save(&context.pool)
        .await
        .unwrap();

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{USERS_ENDPOINT}?proper_name__contains={prefix}_match&sort=id"),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let users: Vec<UserResponse> = test::read_body_json(resp).await;

        assert_eq!(users.len(), 1);
        assert_eq!(users[0].id, matching_user.id);
        assert_eq!(users[0].proper_name, matching_user.proper_name);

        matching_user.delete(&context.pool).await.unwrap();
        other_user.delete(&context.pool).await.unwrap();
    }

    #[rstest]
    #[case::limit_1(1)]
    #[case::limit_2(2)]
    #[case::limit_5(3)]
    #[actix_web::test]
    async fn test_list_users_limit(#[case] limit: usize, #[future(awt)] test_context: TestContext) {
        let context = test_context;
        let prefix = format!("test_list_users_limit_{limit}");

        let mut created_users = Vec::new();
        for i in 0..3 {
            let user = NewUser {
                name: format!("{prefix}_{i}"),
                password: "testpassword".to_string(),
                proper_name: Some(format!("{prefix} Proper {i}")),
                email: Some(format!("{prefix}_{i}@example.com")),
            }
            .save(&context.pool)
            .await
            .unwrap();
            created_users.push(user);
        }

        let url = format!("{USERS_ENDPOINT}?name__contains={prefix}&sort=id&limit={limit}");
        let resp = get_request(&context.pool, &context.admin_token, &url).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let users: Vec<UserResponse> = test::read_body_json(resp).await;

        assert_eq!(users.len(), limit);

        for user in created_users {
            user.delete(&context.pool).await.unwrap();
        }
    }

    #[rstest]
    #[actix_web::test]
    async fn test_list_users_cursor_pagination(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let prefix = "cursor_user";
        let mut created_users = Vec::new();

        for idx in 0..3 {
            created_users.push(
                NewUser {
                    name: format!("{prefix}_{idx}"),
                    password: "testpassword".to_string(),
                    proper_name: Some(format!("{prefix} Proper {idx}")),
                    email: Some(format!("{prefix}_{idx}@example.com")),
                }
                .save(&context.pool)
                .await
                .unwrap(),
            );
        }

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{USERS_ENDPOINT}?name__contains={prefix}&limit=2&sort=id"),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let next_cursor = header_value(&resp, NEXT_CURSOR_HEADER);
        let users: Vec<UserResponse> = test::read_body_json(resp).await;

        assert_eq!(users.len(), 2);
        assert!(next_cursor.is_some());
        assert!(users[0].id < users[1].id);

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "{USERS_ENDPOINT}?name__contains={prefix}&limit=2&sort=id&cursor={}",
                next_cursor.unwrap()
            ),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let users: Vec<UserResponse> = test::read_body_json(resp).await;
        assert!(!users.is_empty());

        for user in created_users {
            user.delete(&context.pool).await.unwrap();
        }
    }

    #[rstest]
    #[actix_web::test]
    async fn test_user_tokens_cursor_pagination(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let test_user = create_test_user(&context.pool).await;
        let token = test_user
            .create_token(&context.pool)
            .await
            .unwrap()
            .get_token();

        test_user.create_token(&context.pool).await.unwrap();
        test_user.create_token(&context.pool).await.unwrap();

        let resp = get_request(
            &context.pool,
            &token,
            &format!("{}/{}/tokens?limit=1", PRINCIPALS_ENDPOINT, test_user.id),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let next_cursor = header_value(&resp, NEXT_CURSOR_HEADER);
        let tokens: Vec<crate::models::PrincipalTokenMetadata> = test::read_body_json(resp).await;

        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].principal_id, test_user.id);
        assert!(next_cursor.is_some());

        let resp = get_request(
            &context.pool,
            &token,
            &format!(
                "{}/{}/tokens?limit=1&cursor={}",
                PRINCIPALS_ENDPOINT,
                test_user.id,
                next_cursor.unwrap()
            ),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let tokens: Vec<crate::models::PrincipalTokenMetadata> = test::read_body_json(resp).await;
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].principal_id, test_user.id);
    }

    #[rstest]
    #[actix_web::test]
    async fn test_user_tokens_total_count_matches_paginated_results(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let test_user = create_test_user(&context.pool).await;
        let auth_token = test_user
            .create_token(&context.pool)
            .await
            .unwrap()
            .get_token();

        test_user.create_token(&context.pool).await.unwrap();
        test_user.create_token(&context.pool).await.unwrap();

        let (tokens, total_count): (Vec<PrincipalTokenMetadata>, i64) =
            assert_paginated_collection_total_count(&context.pool, &auth_token, 10, |cursor| {
                match cursor {
                    Some(cursor) => format!(
                        "{}/{}/tokens?sort=issued_at.asc,name.asc&limit=1&cursor={cursor}",
                        PRINCIPALS_ENDPOINT, test_user.id
                    ),
                    None => format!(
                        "{}/{}/tokens?sort=issued_at.asc,name.asc&limit=1",
                        PRINCIPALS_ENDPOINT, test_user.id
                    ),
                }
            })
            .await;

        assert_eq!(total_count, tokens.len() as i64);
        assert!(
            tokens
                .iter()
                .all(|token| token.principal_id == test_user.id)
        );
    }

    #[rstest]
    #[actix_web::test]
    async fn test_user_groups_filtering(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let user = create_test_user(&context.pool).await;
        let matching_group = NewGroup {
            groupname: format!("filter-user-groups-{}", user.id),
            description: Some("matching group".to_string()),
        }
        .save(&context.pool)
        .await
        .unwrap();
        let other_group = NewGroup {
            groupname: format!("other-user-groups-{}", user.id),
            description: Some("non-matching group".to_string()),
        }
        .save(&context.pool)
        .await
        .unwrap();

        matching_group
            .add_member(&context.pool, &user)
            .await
            .unwrap();
        other_group.add_member(&context.pool, &user).await.unwrap();

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "{}/{}/groups?groupname__contains=filter-user-groups&sort=id",
                PRINCIPALS_ENDPOINT, user.id
            ),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let groups: Vec<crate::models::Group> = test::read_body_json(resp).await;

        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].id, matching_group.id);

        matching_group.delete(&context.pool).await.unwrap();
        other_group.delete(&context.pool).await.unwrap();
        user.delete(&context.pool).await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn test_user_groups_total_count_matches_paginated_results(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let user = create_test_user(&context.pool).await;
        let expected_groups = vec![
            NewGroup {
                groupname: format!("pagination-user-groups-a-{}", user.id),
                description: Some("first group".to_string()),
            }
            .save(&context.pool)
            .await
            .unwrap(),
            NewGroup {
                groupname: format!("pagination-user-groups-b-{}", user.id),
                description: Some("second group".to_string()),
            }
            .save(&context.pool)
            .await
            .unwrap(),
            NewGroup {
                groupname: format!("pagination-user-groups-c-{}", user.id),
                description: Some("third group".to_string()),
            }
            .save(&context.pool)
            .await
            .unwrap(),
        ];

        for group in &expected_groups {
            group.add_member(&context.pool, &user).await.unwrap();
        }

        let (groups, total_count): (Vec<Group>, i64) = assert_paginated_collection_total_count(
            &context.pool,
            &context.admin_token,
            10,
            |cursor| match cursor {
                Some(cursor) => format!(
                    "{}/{}/groups?groupname__contains=pagination-user-groups&sort=id&limit=2&cursor={cursor}",
                    PRINCIPALS_ENDPOINT, user.id
                ),
                None => format!(
                    "{}/{}/groups?groupname__contains=pagination-user-groups&sort=id&limit=2",
                    PRINCIPALS_ENDPOINT, user.id
                ),
            },
        )
        .await;

        assert_eq!(total_count, expected_groups.len() as i64);
        assert_eq!(
            groups.iter().map(|group| group.id).collect::<Vec<_>>(),
            expected_groups
                .iter()
                .map(|group| group.id)
                .collect::<Vec<_>>()
        );

        for group in expected_groups {
            group.delete(&context.pool).await.unwrap();
        }
        user.delete(&context.pool).await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn test_user_tokens_filtering(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let user = create_test_user(&context.pool).await;
        let auth_token = user.create_token(&context.pool).await.unwrap().get_token();

        // Token filtering is now by the token's `name` label (principal model),
        // not by the raw token value. Mint two named tokens via the principal
        // token endpoint and filter for the matching label.
        let matching_name = format!("matching-token-{}", user.id);
        let other_name = format!("other-token-{}", user.id);
        for token_name in [&matching_name, &other_name] {
            let resp = post_request(
                &context.pool,
                &auth_token,
                &format!("{}/{}/tokens", PRINCIPALS_ENDPOINT, user.id),
                &serde_json::json!({ "name": token_name }),
            )
            .await;
            let _ = assert_response_status(resp, StatusCode::CREATED).await;
        }

        let resp = get_request(
            &context.pool,
            &auth_token,
            &format!(
                "{}/{}/tokens?name={matching_name}&sort=name",
                PRINCIPALS_ENDPOINT, user.id
            ),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let tokens: Vec<crate::models::PrincipalTokenMetadata> = test::read_body_json(resp).await;
        let expected_issued = user
            .tokens(&context.pool)
            .await
            .unwrap()
            .into_iter()
            .find(|token| token.name.as_deref() == Some(matching_name.as_str()))
            .map(|token| token.issued)
            .expect("matching token should exist in the database");

        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].principal_id, user.id);
        assert_eq!(tokens[0].name.as_deref(), Some(matching_name.as_str()));
        assert_eq!(tokens[0].issued, expected_issued);
    }

    #[rstest]
    #[actix_web::test]
    async fn test_api_anonymize_user(#[future(awt)] test_context: TestContext) {
        use crate::db::with_connection;
        use diesel::prelude::*;

        let context = test_context;

        // Create a throwaway user to anonymize.
        let uname = format!("api_anon_{}", context.scope.scope_id);
        let new_user = crate::models::NewUser {
            name: uname.clone(),
            password: "secret".into(),
            proper_name: Some("API Anon".into()),
            email: Some("x@example.com".into()),
        }
        .save(&context.pool)
        .await
        .unwrap();

        let resp = post_request(
            &context.pool,
            &context.admin_token,
            &format!("/api/v1/iam/users/{}/anonymize", new_user.id),
            &serde_json::json!({}),
        )
        .await;
        assert_response_status(resp, StatusCode::NO_CONTENT).await;

        let principal_name: String = with_connection(&context.pool, |conn| {
            use crate::schema::principals::dsl as p;
            p::principals
                .filter(p::id.eq(new_user.id))
                .select(p::name)
                .first(conn)
        })
        .unwrap();
        assert_eq!(principal_name, format!("anonymized-{}", new_user.id));
    }

    #[rstest]
    #[actix_web::test]
    async fn test_api_anonymize_missing_user_returns_404(#[future(awt)] test_context: TestContext) {
        let context = test_context;

        let resp = post_request(
            &context.pool,
            &context.admin_token,
            "/api/v1/iam/users/2147483647/anonymize",
            &serde_json::json!({}),
        )
        .await;

        assert_response_status(resp, StatusCode::NOT_FOUND).await;
    }
}
