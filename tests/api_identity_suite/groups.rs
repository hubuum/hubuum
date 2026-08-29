#[cfg(test)]
mod tests {
    use crate::api::etag::{IfMatchCondition, RevisionedResource};
    use actix_web::{http::StatusCode, test};
    use chrono::SubsecRound;
    use hubuum_storage_postgres::diesel_async_prelude::*;
    use rstest::rstest;

    use crate::models::group::{Group, GroupID, GroupResponse, NewGroup, UpdateGroup};
    use crate::models::user::{NewUser, User};
    use crate::models::{
        LDAP_PROVIDER_KIND, MembershipPrincipalResponse, PrincipalID, PrincipalKind,
        PrincipalMemberResponse,
    };
    use crate::pagination::NEXT_CURSOR_HEADER;
    use crate::services::identity::ensure_identity_scope;
    use crate::storage::with_revision_precondition;
    use crate::tests::api_operations::{delete_request, get_request, patch_request, post_request};
    use crate::tests::asserts::{assert_response_status, header_value};
    use crate::tests::{
        TestContext, create_test_admin, create_test_group, create_test_user, test_context,
    };
    use crate::traits::{GroupIdApplicationExt, PrincipalIdApplicationExt};
    use hubuum_storage_postgres::with_connection;

    const GROUPS_ENDPOINT: &str = "/api/v1/iam/groups";
    const PRINCIPALS_ENDPOINT: &str = "/api/v1/iam/principals";

    #[rstest]
    #[actix_web::test]
    async fn legacy_identity_scope_upsert_returns_an_unchanged_row(
        #[future(awt)] test_context: TestContext,
    ) {
        use crate::schema::identity_scopes;

        let context = test_context;
        let scope_name = context.scoped_name("legacy_identity_scope_upsert");
        let created = ensure_identity_scope(&context.pool, &scope_name, LDAP_PROVIDER_KIND)
            .await
            .unwrap();

        let returned = with_connection(&context.pool, async |conn| {
            diesel::insert_into(identity_scopes::table)
                .values((
                    identity_scopes::name.eq(&scope_name),
                    identity_scopes::provider_kind.eq(LDAP_PROVIDER_KIND),
                ))
                .on_conflict(identity_scopes::name)
                .do_update()
                .set(identity_scopes::provider_kind.eq(crate::models::LDAP_PROVIDER_KIND))
                .returning((
                    identity_scopes::id,
                    identity_scopes::revision,
                    identity_scopes::updated_at,
                ))
                .get_result::<(i32, PostgresRevision, chrono::NaiveDateTime)>(conn)
                .await
        })
        .await
        .unwrap();

        assert_eq!(returned.0, created.id);
        assert_eq!(returned.1.into_domain(), created.revision);
        assert_eq!(returned.2, created.updated_at);
    }

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
            &format!("{}/{}", GROUPS_ENDPOINT, target.id),
        )
        .await;
        let resp = assert_response_status(resp, expected_status).await;

        if resp.status() == expected_status {
            let returned_group: GroupResponse = test::read_body_json(resp).await;
            assert_eq!(target.id, returned_group.id);
            assert_eq!(target.groupname, returned_group.groupname);
            assert_eq!(target.description, returned_group.description);
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
            .add_member_without_events(&context.pool, &test_user)
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
            identity_scope: None,
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
        let created_group_from_create: GroupResponse = test::read_body_json(resp).await;
        let resp = get_request(&context.pool, &context.admin_token, created_group_url).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let created_group: GroupResponse = test::read_body_json(resp).await;

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
    async fn test_directory_managed_group_is_read_only(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let identity_scope = ensure_identity_scope(
            &context.pool,
            &context.scoped_name("directory"),
            crate::models::LDAP_PROVIDER_KIND,
        )
        .await
        .unwrap();
        let groupname = context.scoped_name("external_group");
        let group_id = with_connection(&context.pool, async |conn| {
            use crate::schema::groups;

            diesel::insert_into(groups::table)
                .values((
                    groups::identity_scope_id.eq(identity_scope.id),
                    groups::groupname.eq(&groupname),
                    groups::description.eq("Directory managed group"),
                    groups::managed_by.eq(crate::models::LDAP_PROVIDER_KIND),
                    groups::external_key.eq(context.scoped_name("external_group_key")),
                ))
                .returning(groups::id)
                .get_result::<i32>(conn)
                .await
        })
        .await
        .unwrap();
        let group = GroupID::new(group_id)
            .unwrap()
            .group(&context.pool)
            .await
            .unwrap();
        let group_url = format!("{GROUPS_ENDPOINT}/{}", group.id);

        let resp = get_request(&context.pool, &context.normal_token, &group_url).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let returned_group: GroupResponse = test::read_body_json(resp).await;
        assert_eq!(returned_group.id, group.id);
        assert_eq!(returned_group.identity_scope, identity_scope.name);
        assert_eq!(returned_group.managed_by, crate::models::LDAP_PROVIDER_KIND);

        let update = UpdateGroup {
            groupname: Some(context.scoped_name("local_override")),
        };
        let resp = patch_request(&context.pool, &context.admin_token, &group_url, &update).await;
        assert_response_status(resp, StatusCode::FORBIDDEN).await;

        let resp = delete_request(&context.pool, &context.admin_token, &group_url).await;
        assert_response_status(resp, StatusCode::FORBIDDEN).await;

        let user = create_test_user(&context.pool).await;
        let member_url = format!("{GROUPS_ENDPOINT}/{}/members/{}", group.id, user.id);

        let resp = post_request(&context.pool, &context.admin_token, &member_url, &()).await;
        assert_response_status(resp, StatusCode::FORBIDDEN).await;

        let resp = delete_request(&context.pool, &context.admin_token, &member_url).await;
        assert_response_status(resp, StatusCode::FORBIDDEN).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn batch_group_responses_resolve_multiple_identity_scopes(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let local_group = create_test_group(&context.pool).await;
        let external_scope = ensure_identity_scope(
            &context.pool,
            &context.scoped_name("batch_group_scope"),
            crate::models::LDAP_PROVIDER_KIND,
        )
        .await
        .unwrap();
        let external_group_id = with_connection(&context.pool, async |conn| {
            use crate::schema::groups;

            diesel::insert_into(groups::table)
                .values((
                    groups::identity_scope_id.eq(external_scope.id),
                    groups::groupname.eq(context.scoped_name("batch_external_group")),
                    groups::description.eq("Directory managed group"),
                    groups::managed_by.eq(crate::models::LDAP_PROVIDER_KIND),
                    groups::external_key.eq(context.scoped_name("batch_external_group_key")),
                ))
                .returning(groups::id)
                .get_result::<i32>(conn)
                .await
        })
        .await
        .unwrap();
        let external_group = GroupID::new(external_group_id)
            .unwrap()
            .group(&context.pool)
            .await
            .unwrap();

        let responses =
            GroupResponse::from_groups(&context.pool, vec![local_group, external_group])
                .await
                .unwrap();

        assert_eq!(
            responses[0].identity_scope,
            crate::models::LOCAL_IDENTITY_SCOPE
        );
        assert_eq!(responses[1].identity_scope, external_scope.name);
    }

    #[rstest]
    #[actix_web::test]
    async fn batch_principal_responses_resolve_multiple_identity_scopes(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let local = PrincipalID::new(context.normal_user.id)
            .unwrap()
            .principal(&context.pool)
            .await
            .unwrap();
        let external_scope = ensure_identity_scope(
            &context.pool,
            &context.scoped_name("batch_principal_scope"),
            crate::models::LDAP_PROVIDER_KIND,
        )
        .await
        .unwrap();
        let external_id = with_connection(&context.pool, async |conn| {
            use crate::schema::principals;

            diesel::insert_into(principals::table)
                .values((
                    principals::identity_scope_id.eq(external_scope.id),
                    principals::kind.eq(PrincipalKind::Human.as_str()),
                    principals::name.eq(context.scoped_name("batch_external_principal")),
                    principals::provider_managed.eq(true),
                    principals::external_subject.eq(context.scoped_name("batch_subject")),
                ))
                .returning(principals::id)
                .get_result::<i32>(conn)
                .await
        })
        .await
        .unwrap();
        let external = PrincipalID::new(external_id)
            .unwrap()
            .principal(&context.pool)
            .await
            .unwrap();

        let responses =
            futures::future::try_join_all(vec![local, external].into_iter().map(|principal| {
                MembershipPrincipalResponse::from_principal(&context.pool, principal)
            }))
            .await
            .unwrap();

        assert_eq!(
            responses[0].identity_scope,
            crate::models::LOCAL_IDENTITY_SCOPE
        );
        assert_eq!(responses[1].identity_scope, external_scope.name);
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
        let patched_group: GroupResponse = test::read_body_json(resp).await;

        let resp = get_request(&context.pool, &context.admin_token, &patch_url).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let refetched_group: GroupResponse = test::read_body_json(resp).await;

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
            identity_scope: None,
            groupname: groupname.clone(),
            description: Some(groupname.clone()),
        }
        .save_without_events(&context.pool)
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
        let groups: Vec<GroupResponse> = test::read_body_json(resp).await;

        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].groupname, mygroup.groupname);

        mygroup.delete_without_events(&context.pool).await.unwrap();
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
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let membership: PrincipalMemberResponse = test::read_body_json(resp).await;
        assert_eq!(membership.principal_id, user.id);
        assert_eq!(membership.group_id, group.id);

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{}/{}/members", GROUPS_ENDPOINT, group.id),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;

        let members: Vec<PrincipalMemberResponse> = test::read_body_json(resp).await;
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].principal_id, user.id);

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

        let members: Vec<PrincipalMemberResponse> = test::read_body_json(resp).await;
        assert_eq!(members.len(), 0);

        user.delete_without_events(&context.pool).await.unwrap();
        group.delete_without_events(&context.pool).await.unwrap();
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

        first_group
            .add_member_without_events(&context.pool, &user)
            .await
            .unwrap();
        second_group
            .add_member_without_events(&context.pool, &user)
            .await
            .unwrap();

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{}/{}/groups?sort=id", PRINCIPALS_ENDPOINT, user.id),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let user_groups_before: Vec<GroupResponse> = test::read_body_json(resp).await;
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
        let first_group_members: Vec<PrincipalMemberResponse> = test::read_body_json(resp).await;
        assert_eq!(first_group_members.len(), 0);

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{}/{}/members", GROUPS_ENDPOINT, second_group.id),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let second_group_members: Vec<PrincipalMemberResponse> = test::read_body_json(resp).await;
        assert_eq!(second_group_members.len(), 1);
        assert_eq!(second_group_members[0].principal_id, user.id);

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{}/{}/groups?sort=id", PRINCIPALS_ENDPOINT, user.id),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let user_groups_after: Vec<GroupResponse> = test::read_body_json(resp).await;
        assert_eq!(user_groups_after.len(), 1);
        assert_eq!(user_groups_after[0].id, second_group.id);

        user.delete_without_events(&context.pool).await.unwrap();
        first_group
            .delete_without_events(&context.pool)
            .await
            .unwrap();
        second_group
            .delete_without_events(&context.pool)
            .await
            .unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn conditional_membership_add_does_not_recreate_a_deleted_membership(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let group = create_test_group(&context.pool).await;
        let user = create_test_user(&context.pool).await;
        group
            .add_member_without_events(&context.pool, &user)
            .await
            .unwrap();
        let membership =
            crate::services::identity::get_principal_group(&context.pool, user.id, group.id)
                .await
                .unwrap();
        let tag = membership.entity_tag().unwrap();
        let precondition = IfMatchCondition::Tags(vec![tag.clone()])
            .database_precondition(&tag)
            .unwrap();

        group
            .remove_member_without_events(&user, &context.pool)
            .await
            .unwrap();
        let error = with_revision_precondition(
            &context.pool,
            precondition,
            group.add_member_without_events(&context.pool, &user),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            crate::errors::ApiError::PreconditionFailed(_, _)
        ));
        assert!(matches!(
            crate::services::identity::get_principal_group(&context.pool, user.id, group.id).await,
            Err(crate::errors::ApiError::NotFound(_))
        ));
    }

    #[rstest]
    #[actix_web::test]
    async fn membership_add_returns_the_revision_after_adding_the_manual_source(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let group = create_test_group(&context.pool).await;
        let user = create_test_user(&context.pool).await;
        with_connection(&context.pool, async |conn| {
            use crate::schema::group_memberships;
            diesel::insert_into(group_memberships::table)
                .values((
                    group_memberships::principal_id.eq(user.id),
                    group_memberships::group_id.eq(group.id),
                ))
                .execute(conn)
                .await
        })
        .await
        .unwrap();
        let initial =
            crate::services::identity::get_principal_group(&context.pool, user.id, group.id)
                .await
                .unwrap();

        let response = post_request(
            &context.pool,
            &context.admin_token,
            &format!("{GROUPS_ENDPOINT}/{}/members/{}", group.id, user.id),
            &serde_json::json!({}),
        )
        .await;
        let response = assert_response_status(response, StatusCode::CREATED).await;
        let response_etag = response
            .headers()
            .get(actix_web::http::header::ETAG)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let returned: PrincipalMemberResponse = test::read_body_json(response).await;
        let persisted =
            crate::services::identity::get_principal_group(&context.pool, user.id, group.id)
                .await
                .unwrap();

        assert!(persisted.revision > initial.revision);
        assert_eq!(returned.revision, persisted.revision);
        assert_eq!(response_etag, returned.entity_tag().unwrap().to_string());
        assert!(returned.principal.is_none());
    }

    #[rstest]
    #[actix_web::test]
    async fn membership_delete_returns_the_surviving_membership_revision(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let group = create_test_group(&context.pool).await;
        let user = create_test_user(&context.pool).await;
        let endpoint = format!("{GROUPS_ENDPOINT}/{}/members/{}", group.id, user.id);

        let response = post_request(
            &context.pool,
            &context.admin_token,
            &endpoint,
            &serde_json::json!({}),
        )
        .await;
        assert_response_status(response, StatusCode::CREATED).await;

        let scope = ensure_identity_scope(
            &context.pool,
            crate::models::LOCAL_IDENTITY_SCOPE,
            crate::models::LOCAL_PROVIDER_KIND,
        )
        .await
        .unwrap();
        with_connection(&context.pool, async |conn| {
            use crate::schema::group_membership_sources;
            diesel::insert_into(group_membership_sources::table)
                .values((
                    group_membership_sources::principal_id.eq(user.id),
                    group_membership_sources::group_id.eq(group.id),
                    group_membership_sources::source.eq(crate::models::EXTERNAL_MEMBERSHIP_SOURCE),
                    group_membership_sources::source_scope_id.eq(scope.id),
                    group_membership_sources::source_key.eq("surviving-source"),
                ))
                .execute(conn)
                .await
        })
        .await
        .unwrap();
        let before =
            crate::services::identity::get_principal_group(&context.pool, user.id, group.id)
                .await
                .unwrap();

        let response = delete_request(&context.pool, &context.admin_token, &endpoint).await;
        let response = assert_response_status(response, StatusCode::NO_CONTENT).await;
        let response_etag = header_value(&response, actix_web::http::header::ETAG.as_str())
            .expect("surviving membership ETag");
        let surviving =
            crate::services::identity::get_principal_group(&context.pool, user.id, group.id)
                .await
                .unwrap();

        assert!(surviving.revision > before.revision);
        assert_eq!(response_etag, surviving.entity_tag().unwrap().to_string());
        assert_ne!(response_etag, before.entity_tag().unwrap().to_string());
    }

    #[rstest]
    #[actix_web::test]
    async fn stale_membership_delete_rejects_an_absent_manual_source(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let group = create_test_group(&context.pool).await;
        let user = create_test_user(&context.pool).await;
        group
            .add_member_without_events(&context.pool, &user)
            .await
            .unwrap();
        let scope = ensure_identity_scope(
            &context.pool,
            crate::models::LOCAL_IDENTITY_SCOPE,
            crate::models::LOCAL_PROVIDER_KIND,
        )
        .await
        .unwrap();
        with_connection(&context.pool, async |conn| {
            use crate::schema::group_membership_sources;
            diesel::insert_into(group_membership_sources::table)
                .values((
                    group_membership_sources::principal_id.eq(user.id),
                    group_membership_sources::group_id.eq(group.id),
                    group_membership_sources::source.eq(crate::models::EXTERNAL_MEMBERSHIP_SOURCE),
                    group_membership_sources::source_scope_id.eq(scope.id),
                    group_membership_sources::source_key.eq("surviving-stale-source"),
                ))
                .execute(conn)
                .await
        })
        .await
        .unwrap();
        let before =
            crate::services::identity::get_principal_group(&context.pool, user.id, group.id)
                .await
                .unwrap();
        let stale_tag = before.entity_tag().unwrap();
        let precondition = IfMatchCondition::Tags(vec![stale_tag.clone()])
            .database_precondition(&stale_tag)
            .unwrap();

        group
            .remove_member_without_events(&user, &context.pool)
            .await
            .unwrap();
        let error = with_revision_precondition(
            &context.pool,
            precondition,
            group.remove_member_without_events(&user, &context.pool),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            crate::errors::ApiError::RevisionConflict(_, revision) if revision.get() == 3
        ));
    }

    #[rstest]
    #[actix_web::test]
    async fn tagged_group_points_exclude_revision_exempt_sync_bookkeeping(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let group = create_test_group(&context.pool).await;
        let sync_time = chrono::Utc::now().naive_utc().trunc_subsecs(6);
        with_connection(&context.pool, async |conn| {
            use crate::schema::groups;
            diesel::update(groups::table.filter(groups::id.eq(group.id)))
                .set((
                    groups::last_sync_attempted_at.eq(Some(sync_time)),
                    groups::last_sync_success_at.eq(Some(sync_time)),
                ))
                .execute(conn)
                .await
        })
        .await
        .unwrap();

        let response = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{GROUPS_ENDPOINT}/{}", group.id),
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        assert!(
            response
                .headers()
                .contains_key(actix_web::http::header::ETAG)
        );
        let body: serde_json::Value = test::read_body_json(response).await;
        assert!(body.get("last_sync_attempted_at").is_none());
        assert!(body.get("last_sync_success_at").is_none());

        let response = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{GROUPS_ENDPOINT}?id={}", group.id),
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        let body: Vec<serde_json::Value> = test::read_body_json(response).await;
        assert_eq!(
            body[0]["last_sync_attempted_at"],
            serde_json::json!(sync_time)
        );
        assert_eq!(
            body[0]["last_sync_success_at"],
            serde_json::json!(sync_time)
        );
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
                identity_scope: None,
                groupname: format!("{prefix}_{i}"),
                description: Some(format!("{prefix}_description_{i}")),
            }
            .save_without_events(&context.pool)
            .await
            .unwrap();
            created_groups.push(group);
        }

        let url = format!("{GROUPS_ENDPOINT}?groupname__contains={prefix}&sort={sort_order}");
        let resp = get_request(&context.pool, &context.admin_token, &url).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let groups: Vec<GroupResponse> = test::read_body_json(resp).await;

        assert_eq!(groups.len(), created_groups.len());
        assert_eq!(groups[0].id, created_groups[expected_order[0]].id);
        assert_eq!(groups[1].id, created_groups[expected_order[1]].id);
        assert_eq!(groups[2].id, created_groups[expected_order[2]].id);

        for group in created_groups {
            group.delete_without_events(&context.pool).await.unwrap();
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
                identity_scope: None,
                groupname: format!("{prefix}_{i}"),
                description: Some(format!("{prefix}_description_{i}")),
            }
            .save_without_events(&context.pool)
            .await
            .unwrap();
            created_groups.push(group);
        }

        let url = format!("{GROUPS_ENDPOINT}?groupname__contains={prefix}&sort=id&limit={limit}");
        let resp = get_request(&context.pool, &context.admin_token, &url).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let groups: Vec<GroupResponse> = test::read_body_json(resp).await;
        assert_eq!(groups.len(), limit);

        for group in created_groups {
            group.delete_without_events(&context.pool).await.unwrap();
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
                identity_scope: None,
                groupname: format!("{prefix}-{idx}"),
                description: Some("cursor pagination".to_string()),
            }
            .save_without_events(&context.pool)
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
        let groups: Vec<GroupResponse> = test::read_body_json(resp).await;

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
        let groups: Vec<GroupResponse> = test::read_body_json(resp).await;
        assert!(!groups.is_empty());

        for group in created_groups {
            group.delete_without_events(&context.pool).await.unwrap();
        }
    }

    #[rstest]
    #[actix_web::test]
    async fn test_group_members_cursor_pagination(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let group = create_test_group(&context.pool).await;
        let user_one = create_test_user(&context.pool).await;
        let user_two = create_test_user(&context.pool).await;

        group
            .add_member_without_events(&context.pool, &user_one)
            .await
            .unwrap();
        group
            .add_member_without_events(&context.pool, &user_two)
            .await
            .unwrap();

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{}/{}/members?limit=1&sort=id", GROUPS_ENDPOINT, group.id),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let next_cursor = header_value(&resp, NEXT_CURSOR_HEADER);
        let members: Vec<PrincipalMemberResponse> = test::read_body_json(resp).await;

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
        let members: Vec<PrincipalMemberResponse> = test::read_body_json(resp).await;
        assert_eq!(members.len(), 1);
    }

    #[rstest]
    #[actix_web::test]
    async fn test_group_members_filtering(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let group = create_test_group(&context.pool).await;
        let matching_user = NewUser {
            identity_scope: None,
            name: format!("filter-group-member-match-{}", group.id),
            password: "testpassword".to_string(),
            proper_name: Some("Matching Member".to_string()),
            email: Some(format!("match-{}@example.com", group.id)),
        }
        .save_without_events(&context.pool)
        .await
        .unwrap();
        let other_user = NewUser {
            identity_scope: None,
            name: format!("filter-group-member-other-{}", group.id),
            password: "testpassword".to_string(),
            proper_name: Some("Other Member".to_string()),
            email: Some(format!("other-{}@example.com", group.id)),
        }
        .save_without_events(&context.pool)
        .await
        .unwrap();

        group
            .add_member_without_events(&context.pool, &matching_user)
            .await
            .unwrap();
        group
            .add_member_without_events(&context.pool, &other_user)
            .await
            .unwrap();

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "{}/{}/members?name__contains=filter-group-member-match&sort=id",
                GROUPS_ENDPOINT, group.id
            ),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let members: Vec<PrincipalMemberResponse> = test::read_body_json(resp).await;

        assert_eq!(members.len(), 1);
        assert_eq!(members[0].principal_id, matching_user.id);
    }
}
