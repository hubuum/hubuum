#[cfg(test)]
mod tests {
    use crate::models::group::NewGroup;
    use crate::models::pagination::NEXT_CURSOR_HEADER;
    use crate::models::user::{NewUser, UpdateUser, User};
    use actix_web::{http::StatusCode, test};
    use yare::parameterized;

    use crate::tests::api_operations::{delete_request, get_request, patch_request, post_request};
    use crate::tests::asserts::{assert_response_status, header_value};
    use crate::tests::{create_test_admin, create_test_user, setup_pool_and_tokens};

    const USERS_ENDPOINT: &str = "/api/v1/iam/users";

    async fn check_show_user(target: &User, requester: &User, expected_status: StatusCode) {
        let (pool, _, _) = setup_pool_and_tokens().await;
        let token = requester.create_token(&pool).await.unwrap().get_token();

        let resp = get_request(&pool, &token, &format!("{}/{}", USERS_ENDPOINT, &target.id)).await;
        let resp = assert_response_status(resp, expected_status).await;

        if resp.status() == expected_status {
            let returned_user: User = test::read_body_json(resp).await;
            assert_eq!(target, &returned_user);
        }
    }

    async fn check_show_user_tokens(target: &User, requester: &User, expected_status: StatusCode) {
        let (pool, _, _) = setup_pool_and_tokens().await;
        let token = requester.create_token(&pool).await.unwrap().get_token();

        let resp = get_request(
            &pool,
            &token,
            &format!("{}/{}/tokens", USERS_ENDPOINT, &target.id),
        )
        .await;
        let _ = assert_response_status(resp, expected_status).await;
    }

    #[actix_web::test]
    async fn test_show_user() {
        let (pool, _, _) = setup_pool_and_tokens().await;
        let test_user = create_test_user(&pool).await;
        let test_admin_user = create_test_admin(&pool).await;

        // The format here is (target, requester, expected_status).
        check_show_user(&test_user, &test_user, StatusCode::OK).await;
        check_show_user(&test_admin_user, &test_user, StatusCode::OK).await;
        check_show_user(&test_user, &test_admin_user, StatusCode::OK).await;

        // Tokens are admin_or_self. Note that the format is (target, requester, expected_status).
        check_show_user_tokens(&test_user, &test_user, StatusCode::OK).await;
        check_show_user_tokens(&test_admin_user, &test_user, StatusCode::FORBIDDEN).await;
        check_show_user_tokens(&test_user, &test_admin_user, StatusCode::OK).await;
    }

    #[actix_web::test]
    async fn test_create_and_delete_user() {
        let (pool, admin_token, normal_token) = setup_pool_and_tokens().await;

        let new_user = NewUser {
            username: "test_create_user_endpoint".to_string(),
            password: "testpassword".to_string(),
            email: None,
        };

        // Just checking that only admins can create users...
        let resp = post_request(&pool, &normal_token, USERS_ENDPOINT, &new_user).await;
        let _ = assert_response_status(resp, StatusCode::FORBIDDEN).await;

        let resp = post_request(&pool, &admin_token, USERS_ENDPOINT, &new_user).await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;

        let headers = resp.headers().clone();
        let created_user_url = headers.get("Location").unwrap().to_str().unwrap();
        let created_user_from_create: User = test::read_body_json(resp).await;

        let resp = get_request(&pool, &admin_token, created_user_url).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let created_user_from_get: User = test::read_body_json(resp).await;

        assert_eq!(created_user_from_create, created_user_from_get);

        // Validate that the location is what we expect
        assert_eq!(
            created_user_url,
            &format!("{}/{}", USERS_ENDPOINT, created_user_from_get.id)
        );

        // And only admins can delete users...
        let resp = delete_request(&pool, &normal_token, created_user_url).await;
        let _ = assert_response_status(resp, StatusCode::FORBIDDEN).await;

        let resp = delete_request(&pool, &admin_token, created_user_url).await;
        let _ = assert_response_status(resp, StatusCode::NO_CONTENT).await;

        let resp = get_request(&pool, &admin_token, created_user_url).await;
        let _ = assert_response_status(resp, StatusCode::NOT_FOUND).await;
    }

    #[actix_web::test]
    async fn test_patch_user() {
        let (pool, admin_token, user_token) = setup_pool_and_tokens().await;

        // Test setting a new password
        let updated_user = UpdateUser {
            username: None,
            password: Some("newpassword".to_string()),
            email: None,
        };

        let test_user = create_test_user(&pool).await;
        let patch_url = format!("{}/{}", USERS_ENDPOINT, test_user.id);

        // Only admins can patch users...
        let resp = patch_request(&pool, &user_token, &patch_url, &updated_user).await;
        let _ = assert_response_status(resp, StatusCode::FORBIDDEN).await;

        let resp = patch_request(&pool, &admin_token, &patch_url, &updated_user).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let patched_user: User = test::read_body_json(resp).await;

        assert_eq!(patched_user.username, test_user.username);
        assert_ne!(patched_user.password, test_user.password);
        assert_eq!(patched_user.email, test_user.email);
    }

    #[parameterized(
        id_asc = { "id.asc", &[0, 1, 2] },
        id_desc = { "id.desc", &[2, 1, 0] },
        name_asc = { "name.asc", &[0, 1, 2] },
        name_desc = { "name.desc", &[2, 1, 0] },
    )]
    #[test_macro(actix_web::test)]
    async fn test_list_users_sorted(sort_order: &str, expected_order: &[usize]) {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let prefix = format!("test_list_users_sorted_{}", sort_order.replace('.', "_"));

        let mut created_users = Vec::new();
        for i in 0..3 {
            let user = NewUser {
                username: format!("{prefix}_{i}"),
                password: "testpassword".to_string(),
                email: Some(format!("{prefix}_{i}@example.com")),
            }
            .save(&pool)
            .await
            .unwrap();
            created_users.push(user);
        }

        let url = format!("{USERS_ENDPOINT}?username__contains={prefix}&sort={sort_order}");
        let resp = get_request(&pool, &admin_token, &url).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let users: Vec<User> = test::read_body_json(resp).await;

        assert_eq!(users.len(), created_users.len());
        assert_eq!(users[0].id, created_users[expected_order[0]].id);
        assert_eq!(users[1].id, created_users[expected_order[1]].id);
        assert_eq!(users[2].id, created_users[expected_order[2]].id);

        for user in created_users {
            user.delete(&pool).await.unwrap();
        }
    }

    #[parameterized(
        limit_1 = { 1 },
        limit_2 = { 2 },
        limit_5 = { 3 },
    )]
    #[test_macro(actix_web::test)]
    async fn test_list_users_limit(limit: usize) {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let prefix = format!("test_list_users_limit_{limit}");

        let mut created_users = Vec::new();
        for i in 0..3 {
            let user = NewUser {
                username: format!("{prefix}_{i}"),
                password: "testpassword".to_string(),
                email: Some(format!("{prefix}_{i}@example.com")),
            }
            .save(&pool)
            .await
            .unwrap();
            created_users.push(user);
        }

        let url = format!("{USERS_ENDPOINT}?username__contains={prefix}&sort=id&limit={limit}");
        let resp = get_request(&pool, &admin_token, &url).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let users: Vec<User> = test::read_body_json(resp).await;

        assert_eq!(users.len(), limit);

        for user in created_users {
            user.delete(&pool).await.unwrap();
        }
    }

    #[actix_web::test]
    async fn test_list_users_cursor_pagination() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let prefix = "cursor_user";
        let mut created_users = Vec::new();

        for idx in 0..3 {
            created_users.push(
                NewUser {
                    username: format!("{prefix}_{idx}"),
                    password: "testpassword".to_string(),
                    email: Some(format!("{prefix}_{idx}@example.com")),
                }
                .save(&pool)
                .await
                .unwrap(),
            );
        }

        let resp = get_request(
            &pool,
            &admin_token,
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
            &pool,
            &admin_token,
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
            user.delete(&pool).await.unwrap();
        }
    }

    #[actix_web::test]
    async fn test_user_tokens_cursor_pagination() {
        let (pool, _, _) = setup_pool_and_tokens().await;
        let test_user = create_test_user(&pool).await;
        let token = test_user.create_token(&pool).await.unwrap().get_token();

        test_user.create_token(&pool).await.unwrap();
        test_user.create_token(&pool).await.unwrap();

        let resp = get_request(
            &pool,
            &token,
            &format!("{}/{}/tokens?limit=1", USERS_ENDPOINT, test_user.id),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let next_cursor = header_value(&resp, NEXT_CURSOR_HEADER);
        let tokens: Vec<crate::models::UserToken> = test::read_body_json(resp).await;

        assert_eq!(tokens.len(), 1);
        assert!(next_cursor.is_some());

        let resp = get_request(
            &pool,
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
    }

    #[actix_web::test]
    async fn test_user_groups_filtering() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let user = create_test_user(&pool).await;
        let matching_group = NewGroup {
            groupname: format!("filter-user-groups-{}", user.id),
            description: Some("matching group".to_string()),
        }
        .save(&pool)
        .await
        .unwrap();
        let other_group = NewGroup {
            groupname: format!("other-user-groups-{}", user.id),
            description: Some("non-matching group".to_string()),
        }
        .save(&pool)
        .await
        .unwrap();

        matching_group.add_member(&pool, &user).await.unwrap();
        other_group.add_member(&pool, &user).await.unwrap();

        let resp = get_request(
            &pool,
            &admin_token,
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

        matching_group.delete(&pool).await.unwrap();
        other_group.delete(&pool).await.unwrap();
        user.delete(&pool).await.unwrap();
    }

    #[actix_web::test]
    async fn test_user_tokens_filtering() {
        let (pool, _, _) = setup_pool_and_tokens().await;
        let user = create_test_user(&pool).await;
        let auth_token = user.create_token(&pool).await.unwrap().get_token();
        let matching_token = user.create_token(&pool).await.unwrap().get_token();
        user.create_token(&pool).await.unwrap();

        let resp = get_request(
            &pool,
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
        assert_eq!(tokens[0].token, matching_token);
    }
}
