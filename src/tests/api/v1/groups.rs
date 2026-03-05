#[cfg(test)]
mod tests {
    use actix_web::{http::StatusCode, test};
    use rstest::rstest;

    use crate::models::group::{Group, NewGroup, UpdateGroup};
    use crate::models::user::{NewUser, User};
    use crate::pagination::NEXT_CURSOR_HEADER;
    use crate::tests::api_operations::{delete_request, get_request, patch_request, post_request};
    use crate::tests::asserts::{assert_response_status, header_value};
    use crate::tests::{
        TestContext, create_test_admin, create_test_group, create_test_user, test_context,
    };

    const GROUPS_ENDPOINT: &str = "/api/v1/iam/groups";
    const USERS_ENDPOINT: &str = "/api/v1/iam/users";

    async fn check_show_group(
        context: &TestContext,
        target: &Group,
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
            &format!("{}/{}", GROUPS_ENDPOINT, &target.id),
        )
        .await;
        let resp = assert_response_status(resp, expected_status).await;

        if resp.status() == expected_status {
            let returned_group: Group = test::read_body_json(resp).await;
            assert_eq!(target, &returned_group);
        }
    }

    #[rstest]
    #[actix_web::test]
    async fn test_show_group(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let test_user = create_test_user(&context.pool).await;
        let test_admin = create_test_admin(&context.pool).await;

        let test_group = create_test_group(&context.pool).await;
        test_group
            .add_member(&context.pool, &test_user)
            .await
            .unwrap();

        let test_admin_group = create_test_group(&context.pool).await;

        // The format here is (target, requester, expected_status).
        // Check that anyone can see every group.
        check_show_group(&context, &test_group, &test_user, StatusCode::OK).await;
        check_show_group(&context, &test_admin_group, &test_user, StatusCode::OK).await;
        check_show_group(&context, &test_admin_group, &test_admin, StatusCode::OK).await;
        check_show_group(&context, &test_group, &test_admin, StatusCode::OK).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_create_and_delete_group(#[future(awt)] test_context: TestContext) {
        let context = test_context;

        let new_group = NewGroup {
            groupname: "test_create_group_endpoint".to_string(),
            description: Some("Test group".to_string()),
        };

        // Just checking that only admins can create groups...
        let resp = post_request(
            &context.pool,
            &context.normal_token,
            GROUPS_ENDPOINT,
            &new_group,
        )
        .await;
        let _ = assert_response_status(resp, StatusCode::FORBIDDEN).await;

        let resp = post_request(
            &context.pool,
            &context.admin_token,
            GROUPS_ENDPOINT,
            &new_group,
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;

        let headers = resp.headers().clone();
        let created_group_url = headers.get("Location").unwrap().to_str().unwrap();
        let created_group_from_create: Group = test::read_body_json(resp).await;
        let resp = get_request(&context.pool, &context.admin_token, created_group_url).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let created_group: Group = test::read_body_json(resp).await;

        // Validate that the location is what we expect
        assert_eq!(
            created_group_url,
            &format!("{}/{}", GROUPS_ENDPOINT, created_group.id)
        );

        assert_eq!(created_group, created_group_from_create);
        assert_eq!(new_group.groupname, created_group_from_create.groupname);
        assert_eq!(new_group.description, Some(created_group.description));

        // And only admins can delete groups...
        let resp = delete_request(&context.pool, &context.normal_token, created_group_url).await;
        let _ = assert_response_status(resp, StatusCode::FORBIDDEN).await;

        let resp = delete_request(&context.pool, &context.admin_token, created_group_url).await;
        let _ = assert_response_status(resp, StatusCode::NO_CONTENT).await;

        let resp = get_request(&context.pool, &context.admin_token, created_group_url).await;
        let _ = assert_response_status(resp, StatusCode::NOT_FOUND).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_patch_group(#[future(awt)] test_context: TestContext) {
        let context = test_context;

        // Test setting a new password
        let updated_group = UpdateGroup {
            groupname: Some("newgroupname".to_string()),
        };

        let test_group = create_test_group(&context.pool).await;
        let patch_url = format!("{}/{}", GROUPS_ENDPOINT, test_group.id);

        // Only admins can patch groups...
        let resp = patch_request(
            &context.pool,
            &context.normal_token,
            &patch_url,
            &updated_group,
        )
        .await;
        let _ = assert_response_status(resp, StatusCode::FORBIDDEN).await;

        let resp = patch_request(
            &context.pool,
            &context.admin_token,
            &patch_url,
            &updated_group,
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let patched_group: Group = test::read_body_json(resp).await;

        let resp = get_request(&context.pool, &context.admin_token, &patch_url).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let refetched_group: Group = test::read_body_json(resp).await;

        assert_eq!(patched_group.groupname, updated_group.groupname.unwrap());
        assert_eq!(patched_group, refetched_group);
    }

    #[rstest]
    #[case::filter_by_name("name")]
    #[case::filter_by_id("id")]
    #[case::filter_by_desc("description")]
    #[actix_web::test]
    async fn test_list_groups_filtered(
        #[case] filter_tpl: &str,
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let groupname = format!("test_list_groups_filtered_{filter_tpl}");
        let mygroup = NewGroup {
            groupname: groupname.clone(),
            description: Some(groupname.clone()),
        }
        .save(&context.pool)
        .await
        .unwrap();

        let arg = match filter_tpl {
            "name" => mygroup.groupname.clone(),
            "id" => mygroup.id.to_string(),
            "description" => mygroup.description.clone(),
            other => panic!("unexpected filter template: {other}"),
        };
        let url = format!("{GROUPS_ENDPOINT}?{filter_tpl}={arg}");

        let resp = get_request(&context.pool, &context.admin_token, &url).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let groups: Vec<Group> = test::read_body_json(resp).await;

        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].groupname, mygroup.groupname);

        mygroup.delete(&context.pool).await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn test_group_add_and_delete_member(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let group = create_test_group(&context.pool).await;
        let user = create_test_user(&context.pool).await;

        let resp = post_request(
            &context.pool,
            &context.admin_token,
            &format!("{}/{}/members/{}", GROUPS_ENDPOINT, group.id, user.id),
            &(),
        )
        .await;
        let _ = assert_response_status(resp, StatusCode::NO_CONTENT).await;

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{}/{}/members", GROUPS_ENDPOINT, group.id),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;

        let members: Vec<User> = test::read_body_json(resp).await;
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].id, user.id);

        let resp = delete_request(
            &context.pool,
            &context.admin_token,
            &format!("{}/{}/members/{}", GROUPS_ENDPOINT, group.id, user.id),
        )
        .await;
        let _ = assert_response_status(resp, StatusCode::NO_CONTENT).await;

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{}/{}/members", GROUPS_ENDPOINT, group.id),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;

        let members: Vec<User> = test::read_body_json(resp).await;
        assert_eq!(members.len(), 0);

        user.delete(&context.pool).await.unwrap();
        group.delete(&context.pool).await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn test_group_delete_member_only_targets_requested_group(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let first_group = create_test_group(&context.pool).await;
        let second_group = create_test_group(&context.pool).await;
        let user = create_test_user(&context.pool).await;

        first_group.add_member(&context.pool, &user).await.unwrap();
        second_group.add_member(&context.pool, &user).await.unwrap();

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{}/{}/groups?sort=id", USERS_ENDPOINT, user.id),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let user_groups_before: Vec<Group> = test::read_body_json(resp).await;
        assert_eq!(user_groups_before.len(), 2);
        let user_group_ids_before: Vec<i32> =
            user_groups_before.iter().map(|group| group.id).collect();
        assert!(user_group_ids_before.contains(&first_group.id));
        assert!(user_group_ids_before.contains(&second_group.id));

        let resp = delete_request(
            &context.pool,
            &context.admin_token,
            &format!("{}/{}/members/{}", GROUPS_ENDPOINT, first_group.id, user.id),
        )
        .await;
        let _ = assert_response_status(resp, StatusCode::NO_CONTENT).await;

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{}/{}/members", GROUPS_ENDPOINT, first_group.id),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let first_group_members: Vec<User> = test::read_body_json(resp).await;
        assert_eq!(first_group_members.len(), 0);

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{}/{}/members", GROUPS_ENDPOINT, second_group.id),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let second_group_members: Vec<User> = test::read_body_json(resp).await;
        assert_eq!(second_group_members.len(), 1);
        assert_eq!(second_group_members[0].id, user.id);

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{}/{}/groups?sort=id", USERS_ENDPOINT, user.id),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let user_groups_after: Vec<Group> = test::read_body_json(resp).await;
        assert_eq!(user_groups_after.len(), 1);
        assert_eq!(user_groups_after[0].id, second_group.id);

        user.delete(&context.pool).await.unwrap();
        first_group.delete(&context.pool).await.unwrap();
        second_group.delete(&context.pool).await.unwrap();
    }

    #[rstest]
    #[case::id_asc("id.asc", &[0, 1, 2])]
    #[case::id_desc("id.desc", &[2, 1, 0])]
    #[case::name_asc("name.asc", &[0, 1, 2])]
    #[case::name_desc("name.desc", &[2, 1, 0])]
    #[actix_web::test]
    async fn test_list_groups_sorted(
        #[case] sort_order: &str,
        #[case] expected_order: &[usize],
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let prefix = format!("test_list_groups_sorted_{}", sort_order.replace('.', "_"));

        let mut created_groups = Vec::new();
        for i in 0..3 {
            let group = NewGroup {
                groupname: format!("{prefix}_{i}"),
                description: Some(format!("{prefix}_description_{i}")),
            }
            .save(&context.pool)
            .await
            .unwrap();
            created_groups.push(group);
        }

        let url = format!("{GROUPS_ENDPOINT}?groupname__contains={prefix}&sort={sort_order}");
        let resp = get_request(&context.pool, &context.admin_token, &url).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let groups: Vec<Group> = test::read_body_json(resp).await;

        assert_eq!(groups.len(), created_groups.len());
        assert_eq!(groups[0].id, created_groups[expected_order[0]].id);
        assert_eq!(groups[1].id, created_groups[expected_order[1]].id);
        assert_eq!(groups[2].id, created_groups[expected_order[2]].id);

        for group in created_groups {
            group.delete(&context.pool).await.unwrap();
        }
    }

    #[rstest]
    #[case::limit_1(1)]
    #[case::limit_2(2)]
    #[case::limit_5(3)]
    #[actix_web::test]
    async fn test_list_groups_limit(
        #[case] limit: usize,
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let prefix = format!("test_list_groups_limit_{limit}");

        let mut created_groups = Vec::new();
        for i in 0..3 {
            let group = NewGroup {
                groupname: format!("{prefix}_{i}"),
                description: Some(format!("{prefix}_description_{i}")),
            }
            .save(&context.pool)
            .await
            .unwrap();
            created_groups.push(group);
        }

        let url = format!("{GROUPS_ENDPOINT}?groupname__contains={prefix}&sort=id&limit={limit}");
        let resp = get_request(&context.pool, &context.admin_token, &url).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let groups: Vec<Group> = test::read_body_json(resp).await;
        assert_eq!(groups.len(), limit);

        for group in created_groups {
            group.delete(&context.pool).await.unwrap();
        }
    }

    #[rstest]
    #[actix_web::test]
    async fn test_list_groups_cursor_pagination(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let prefix = "cursor-group";
        let mut created_groups = Vec::new();

        for idx in 0..3 {
            let group = NewGroup {
                groupname: format!("{prefix}-{idx}"),
                description: Some("cursor pagination".to_string()),
            }
            .save(&context.pool)
            .await
            .unwrap();
            created_groups.push(group);
        }

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{GROUPS_ENDPOINT}?groupname__contains={prefix}&limit=2&sort=id"),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let next_cursor = header_value(&resp, NEXT_CURSOR_HEADER);
        let groups: Vec<Group> = test::read_body_json(resp).await;

        assert_eq!(groups.len(), 2);
        assert!(next_cursor.is_some());

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "{GROUPS_ENDPOINT}?groupname__contains={prefix}&limit=2&sort=id&cursor={}",
                next_cursor.unwrap()
            ),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let groups: Vec<Group> = test::read_body_json(resp).await;
        assert!(!groups.is_empty());

        for group in created_groups {
            group.delete(&context.pool).await.unwrap();
        }
    }

    #[rstest]
    #[actix_web::test]
    async fn test_group_members_cursor_pagination(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let group = create_test_group(&context.pool).await;
        let user_one = create_test_user(&context.pool).await;
        let user_two = create_test_user(&context.pool).await;

        group.add_member(&context.pool, &user_one).await.unwrap();
        group.add_member(&context.pool, &user_two).await.unwrap();

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{}/{}/members?limit=1&sort=id", GROUPS_ENDPOINT, group.id),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let next_cursor = header_value(&resp, NEXT_CURSOR_HEADER);
        let members: Vec<User> = test::read_body_json(resp).await;

        assert_eq!(members.len(), 1);
        assert!(next_cursor.is_some());

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "{}/{}/members?limit=1&sort=id&cursor={}",
                GROUPS_ENDPOINT,
                group.id,
                next_cursor.unwrap()
            ),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let members: Vec<User> = test::read_body_json(resp).await;
        assert_eq!(members.len(), 1);
    }

    #[rstest]
    #[actix_web::test]
    async fn test_group_members_filtering(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let group = create_test_group(&context.pool).await;
        let matching_user = NewUser {
            username: format!("filter-group-member-match-{}", group.id),
            password: "testpassword".to_string(),
            email: Some(format!("match-{}@example.com", group.id)),
        }
        .save(&context.pool)
        .await
        .unwrap();
        let other_user = NewUser {
            username: format!("filter-group-member-other-{}", group.id),
            password: "testpassword".to_string(),
            email: Some(format!("other-{}@example.com", group.id)),
        }
        .save(&context.pool)
        .await
        .unwrap();

        group
            .add_member(&context.pool, &matching_user)
            .await
            .unwrap();
        group.add_member(&context.pool, &other_user).await.unwrap();

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "{}/{}/members?username__contains=filter-group-member-match&sort=id",
                GROUPS_ENDPOINT, group.id
            ),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let members: Vec<User> = test::read_body_json(resp).await;

        assert_eq!(members.len(), 1);
        assert_eq!(members[0].id, matching_user.id);
    }
}
