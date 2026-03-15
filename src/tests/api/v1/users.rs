#[cfg(test)]
mod tests {
    use crate::models::group::NewGroup;
    use crate::models::user::{NewUser, UpdateUser, User};
    use crate::pagination::NEXT_CURSOR_HEADER;
    use actix_web::{http::StatusCode, test};
    use rstest::rstest;

    use crate::tests::api_operations::{delete_request, get_request, patch_request, post_request};
    use crate::tests::asserts::{assert_response_status, header_value};
    use crate::tests::{TestContext, create_test_admin, create_test_user, test_context};

    const USERS_ENDPOINT: &str = "/api/v1/iam/users";

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

        if resp.status() == expected_status {
            let returned_user: User = test::read_body_json(resp).await;
            assert_eq!(target, &returned_user);
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
            &format!("{}/{}/tokens", USERS_ENDPOINT, &target.id),
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
        check_show_user(&context, &test_admin_user, &test_user, StatusCode::OK).await;
        check_show_user(&context, &test_user, &test_admin_user, StatusCode::OK).await;

        // Tokens are admin_or_self. Note that the format is (target, requester, expected_status).
        check_show_user_tokens(&context, &test_user, &test_user, StatusCode::OK).await;
        check_show_user_tokens(
            &context,
            &test_admin_user,
            &test_user,
            StatusCode::FORBIDDEN,
        )
        .await;
        check_show_user_tokens(&context, &test_user, &test_admin_user, StatusCode::OK).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_create_and_delete_user(#[future(awt)] test_context: TestContext) {
        let context = test_context;

        let new_user = NewUser {
            username: "test_create_user_endpoint".to_string(),
            password: "testpassword".to_string(),
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
        let created_user_from_create: User = test::read_body_json(resp).await;

        let resp = get_request(&context.pool, &context.admin_token, created_user_url).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let created_user_from_get: User = test::read_body_json(resp).await;

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
            username: None,
            password: Some("newpassword".to_string()),
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
        let patched_user: User = test::read_body_json(resp).await;

        assert_eq!(patched_user.username, test_user.username);
        assert_ne!(patched_user.password, test_user.password);
        assert_eq!(patched_user.email, test_user.email);
    }

    #[rstest]
    #[case::id_asc("id.asc", &[0, 1, 2])]
    #[case::id_desc("id.desc", &[2, 1, 0])]
    #[case::name_asc("name.asc", &[0, 1, 2])]
    #[case::name_desc("name.desc", &[2, 1, 0])]
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
                username: format!("{prefix}_{i}"),
                password: "testpassword".to_string(),
                email: Some(format!("{prefix}_{i}@example.com")),
            }
            .save(&context.pool)
            .await
            .unwrap();
            created_users.push(user);
        }

        let url = format!("{USERS_ENDPOINT}?username__contains={prefix}&sort={sort_order}");
        let resp = get_request(&context.pool, &context.admin_token, &url).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let users: Vec<User> = test::read_body_json(resp).await;

        assert_eq!(users.len(), created_users.len());
        assert_eq!(users[0].id, created_users[expected_order[0]].id);
        assert_eq!(users[1].id, created_users[expected_order[1]].id);
        assert_eq!(users[2].id, created_users[expected_order[2]].id);

        for user in created_users {
            user.delete(&context.pool).await.unwrap();
        }
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
                username: format!("{prefix}_{i}"),
                password: "testpassword".to_string(),
                email: Some(format!("{prefix}_{i}@example.com")),
            }
            .save(&context.pool)
            .await
            .unwrap();
            created_users.push(user);
        }

        let url = format!("{USERS_ENDPOINT}?username__contains={prefix}&sort=id&limit={limit}");
        let resp = get_request(&context.pool, &context.admin_token, &url).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let users: Vec<User> = test::read_body_json(resp).await;

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
                    username: format!("{prefix}_{idx}"),
                    password: "testpassword".to_string(),
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
            &format!("{USERS_ENDPOINT}?username__contains={prefix}&limit=2&sort=id"),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let next_cursor = header_value(&resp, NEXT_CURSOR_HEADER);
        let users: Vec<User> = test::read_body_json(resp).await;

        assert_eq!(users.len(), 2);
        assert!(next_cursor.is_some());
        assert!(users[0].id < users[1].id);

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "{USERS_ENDPOINT}?username__contains={prefix}&limit=2&sort=id&cursor={}",
                next_cursor.unwrap()
            ),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let users: Vec<User> = test::read_body_json(resp).await;
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
            &format!("{}/{}/tokens?limit=1", USERS_ENDPOINT, test_user.id),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let next_cursor = header_value(&resp, NEXT_CURSOR_HEADER);
        let tokens: Vec<crate::models::UserToken> = test::read_body_json(resp).await;

        assert_eq!(tokens.len(), 1);
        assert!(tokens[0].token.contains("..."));
        assert!(next_cursor.is_some());

        let resp = get_request(
            &context.pool,
            &token,
            &format!(
                "{}/{}/tokens?limit=1&cursor={}",
                USERS_ENDPOINT,
                test_user.id,
                next_cursor.unwrap()
            ),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let tokens: Vec<crate::models::UserToken> = test::read_body_json(resp).await;
        assert_eq!(tokens.len(), 1);
        assert!(tokens[0].token.contains("..."));
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
                USERS_ENDPOINT, user.id
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
    async fn test_user_tokens_filtering(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let user = create_test_user(&context.pool).await;
        let auth_token = user.create_token(&context.pool).await.unwrap().get_token();
        let matching_token = user.create_token(&context.pool).await.unwrap().get_token();
        user.create_token(&context.pool).await.unwrap();

        let resp = get_request(
            &context.pool,
            &auth_token,
            &format!(
                "{}/{}/tokens?name={matching_token}&sort=name",
                USERS_ENDPOINT, user.id
            ),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let tokens: Vec<crate::models::UserToken> = test::read_body_json(resp).await;

        assert_eq!(tokens.len(), 1);
        assert_ne!(tokens[0].token, matching_token);
        assert!(tokens[0].token.contains("..."));
    }
}
