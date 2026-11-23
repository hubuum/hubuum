use super::*;

#[actix_web::test]
async fn every_available_storage_backend_supplies_local_authorization_data() {
    let _permit = postgres_permit().await;

    for backend in available_backends() {
        let user = create_backend_user(&backend, &prefix("authorization_user")).await;
        let owner_group = backend
            .create_group(
                StorageGroupCreate::new(
                    None,
                    prefix("authorization_owner_group"),
                    Some("authorization compatibility owner".to_string()),
                ),
                &EventContext::system(),
            )
            .await
            .expect("backend-local authorization owner group should be created")
            .into_value();
        let group = backend
            .create_group(
                StorageGroupCreate::new(
                    None,
                    prefix("authorization_group"),
                    Some("authorization compatibility group".to_string()),
                ),
                &EventContext::system(),
            )
            .await
            .expect("backend-local authorization group should be created")
            .into_value();
        backend
            .add_group_member(user.principal_id, group.id(), &EventContext::system())
            .await
            .expect("backend-local authorization membership should be created")
            .into_value();
        let fixture = create_backend_object_fixture_for_owner(
            &backend,
            &prefix("authorization_resource"),
            vec![serde_json::json!({})],
            owner_group.id(),
        )
        .await;
        let collection_id = fixture.collection.id();
        let principal_id = user.principal_id;
        let group_id = group.id();

        let principal = backend
            .get_authorization_principal(principal_id)
            .await
            .expect("certified backend should supply authorization principal facts");
        assert!(principal.group_ids().contains(&group_id));

        let membership = StorageAuthorizationGroupMembershipQuery::new(
            principal_id,
            group.name(),
            LOCAL_IDENTITY_SCOPE,
        );
        assert!(
            backend
                .is_authorization_principal_group_member(membership)
                .await
                .expect("certified backend should query group membership")
        );

        let classes = backend
            .list_authorization_classes(StorageAuthorizationResourceIds::new([
                ResourceId::new(fixture.class.id().id()).unwrap(),
                ResourceId::new(fixture.class.id().id()).unwrap(),
            ]))
            .await
            .expect("certified backend should project authorization class facts");
        assert_eq!(classes.len(), 1);
        assert_eq!(classes[0].id(), fixture.class.id());
        assert_eq!(classes[0].collection_id(), collection_id);

        let objects = backend
            .list_authorization_objects(StorageAuthorizationResourceIds::new([
                ResourceId::new(fixture.objects[0].id().id()).unwrap(),
                ResourceId::new(fixture.objects[0].id().id()).unwrap(),
            ]))
            .await
            .expect("certified backend should project authorization object facts");
        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0].id(), fixture.objects[0].id());
        assert_eq!(objects[0].collection_id(), collection_id);
        assert_eq!(objects[0].class_id(), fixture.class.id());
        assert_eq!(objects[0].name(), fixture.objects[0].name());

        let access_query = || {
            StorageAuthorizationCollectionAccessQuery::new(
                principal_id,
                collection_id,
                [StorageAuthorizationPermission::ReadCollection],
            )
        };
        let batch_access_query = || {
            StorageAuthorizationCollectionsAccessQuery::new(
                principal_id,
                [collection_id, collection_id],
                [StorageAuthorizationPermission::ReadCollection],
            )
        };
        assert!(
            !backend
                .authorize_local_collection(access_query())
                .await
                .expect("missing local grant should deny")
        );
        assert!(
            !backend
                .authorize_local_collections(batch_access_query())
                .await
                .expect("missing local batch grant should deny")
        );

        let key = StorageAuthorizationGrantKey::new(collection_id, group_id);
        backend
            .apply_local_collection_grant(StorageAuthorizationGrantMutation::new(
                key,
                [StorageAuthorizationPermission::ReadCollection],
                false,
                EventContext::system(),
            ))
            .await
            .expect("certified backend should apply a local grant")
            .into_value();
        let grant = backend
            .get_local_collection_grant(key)
            .await
            .expect("certified backend should load a local grant")
            .expect("applied local grant should exist");
        assert!(
            grant
                .permissions()
                .contains(&StorageAuthorizationPermission::ReadCollection)
        );
        let (permission_collection_id, permission_revision, permission_grants) = backend
            .get_local_collection_permission_set(StorageAuthorizationPermissionSetQuery::new(
                collection_id,
                Some(group_id),
            ))
            .await
            .expect("certified backend should load revisioned permission sets")
            .into_parts();
        assert_eq!(permission_collection_id, collection_id);
        assert!(permission_revision.get() > 0);
        assert_eq!(permission_grants.len(), 1);
        assert_eq!(permission_grants[0].group_id(), group_id);
        assert!(
            backend
                .authorize_local_collection(access_query())
                .await
                .expect("applied local grant should authorize")
        );
        assert!(
            backend
                .authorize_local_collections(batch_access_query())
                .await
                .expect("applied local grant should authorize the batch")
        );

        let page_options = || {
            QueryOptions::new(Vec::new(), Vec::new(), None, None, true)
                .expect("contract query must be valid")
        };
        let principal_query =
            || StorageAuthorizationPrincipalCollectionQuery::new(principal_id, collection_id);

        let principal_permissions = backend
            .load_principal_collection_permissions(principal_query())
            .await
            .expect("certified backend should project principal collection grants");
        assert!(
            principal_permissions
                .iter()
                .cloned()
                .any(|row| row.into_parts().0.id() == group_id)
        );

        let all_permissions = backend
            .list_all_principal_collection_permissions(principal_id)
            .await
            .expect("certified backend should project all principal collection grants");
        assert!(all_permissions.iter().cloned().any(|row| {
            let (_, row_group, collection) = row.into_parts();
            collection.id() == collection_id && row_group.id() == group_id
        }));

        let (principal_page, principal_total) = backend
            .list_principal_collection_permissions(
                StorageAuthorizationPrincipalCollectionPageQuery::new(
                    principal_query(),
                    page_options(),
                ),
            )
            .await
            .expect("certified backend should page principal collection grants")
            .into_parts();
        assert!(principal_total.is_some_and(|total| total >= 1));
        assert!(!principal_page.is_empty());

        let effective_principal = backend
            .list_effective_principal_collection_permissions(principal_query())
            .await
            .expect("certified backend should project effective principal grants");
        assert!(
            effective_principal
                .iter()
                .cloned()
                .any(|row| row.into_parts().4.id() == group_id)
        );

        let visible = backend
            .list_visible_collections(StorageAuthorizationCollectionVisibilityQuery::new(
                principal_id,
                false,
                StorageAuthorizationPermission::ReadCollection,
                None,
            ))
            .await
            .expect("certified backend should project visible collections");
        assert!(
            visible
                .iter()
                .any(|collection| collection.id() == collection_id)
        );

        let group_query = StorageAuthorizationGroupCollectionQuery::new(
            collection_id,
            group_id,
            StorageAuthorizationPermission::ReadCollection,
        );
        assert!(
            backend
                .has_group_collection_permission(group_query)
                .await
                .expect("certified backend should test group collection grants")
        );

        let effective_group = backend
            .list_effective_group_collection_permissions(collection_id, group_id)
            .await
            .expect("certified backend should project effective group grants");
        assert!(!effective_group.is_empty());

        let groups_query = || {
            StorageAuthorizationCollectionGroupsQuery::new(
                collection_id,
                StorageAuthorizationPermission::ReadCollection,
            )
        };
        let groups = backend
            .load_groups_with_collection_permission(groups_query())
            .await
            .expect("certified backend should list groups with collection grants");
        assert!(groups.iter().any(|candidate| candidate.id() == group_id));

        let (groups_page, groups_total) = backend
            .list_groups_with_collection_permission(
                StorageAuthorizationCollectionGroupsPageQuery::new(groups_query(), page_options()),
            )
            .await
            .expect("certified backend should page groups with collection grants")
            .into_parts();
        assert!(groups_total.is_some_and(|total| total >= 1));
        assert!(!groups_page.is_empty());

        let collections = backend
            .list_local_authorized_collections(StorageAuthorizationCollectionsQuery::new(
                principal_id,
                [StorageAuthorizationPermission::ReadCollection],
            ))
            .await
            .expect("certified backend should run reverse authorization queries");
        assert!(
            collections
                .iter()
                .any(|collection| collection.id() == collection_id)
        );

        let page = backend
            .list_local_collection_grants(StorageAuthorizationCollectionGrantListQuery::new(
                collection_id,
                [StorageAuthorizationPermission::ReadCollection],
                QueryOptions::new(Vec::new(), Vec::new(), None, None, true)
                    .expect("contract query must be valid"),
            ))
            .await
            .expect("certified backend should list local grants");
        let (items, total_count) = page.into_parts();
        assert!(total_count.is_some_and(|total| total >= 1));
        assert!(!items.is_empty());

        let collection_candidates = backend
            .load_authorization_collection_candidates(
                StorageAuthorizationCollectionCandidateQuery::new(
                    None,
                    StorageCandidatePageLimit::try_new(512)
                        .expect("contract candidate page limit should be valid"),
                ),
            )
            .await
            .expect("certified backend should list authorization collection candidates")
            .into_parts()
            .0;
        assert!(
            collection_candidates
                .iter()
                .any(|collection| collection.id() == collection_id)
        );

        let group_candidates = backend
            .load_authorization_group_candidates(StorageAuthorizationGroupCandidateQuery::new(
                QueryOptions::new(
                    vec![
                        ParsedQueryParam::from_parts(
                            FilterField::Description,
                            SearchOperator::Equals { is_negated: false },
                            group.description().to_string(),
                        )
                        .unwrap(),
                    ],
                    Vec::new(),
                    None,
                    None,
                    false,
                )
                .expect("authorization candidate query should be valid"),
                StorageCandidatePageLimit::try_new(512)
                    .expect("contract candidate page limit should be valid"),
            ))
            .await
            .expect("certified backend should list authorization group candidates")
            .into_parts()
            .0;
        assert!(
            group_candidates
                .iter()
                .any(|candidate| candidate.id() == group_id)
        );

        let policy_snapshot = backend
            .get_authorization_policy_snapshot()
            .await
            .expect("certified backend should supply the local policy snapshot");
        assert!(policy_snapshot.into_iter().any(|row| {
            let (grant, snapshot_group, collection) = row.into_parts();
            grant.group_id() == group_id
                && snapshot_group.id() == group_id
                && collection.id() == collection_id
        }));

        backend
            .revoke_local_collection_grant(StorageAuthorizationGrantMutation::new(
                key,
                [StorageAuthorizationPermission::ReadCollection],
                false,
                EventContext::system(),
            ))
            .await
            .expect("certified backend should revoke selected local permissions")
            .into_value();
        assert!(
            !backend
                .authorize_local_collection(access_query())
                .await
                .expect("revoked local grant should deny")
        );
        backend
            .revoke_all_local_collection_grants(StorageAuthorizationGrantDelete::new(
                key,
                EventContext::system(),
            ))
            .await
            .expect("certified backend should remove the local grant row")
            .into_value();

        backend
            .remove_group_member(user.principal_id, group.id(), &EventContext::system())
            .await
            .expect("backend-local authorization membership should be removed")
            .into_value();
        backend
            .delete_group(group.id(), &EventContext::system())
            .await
            .expect("backend-local authorization group should be removed")
            .into_value();
        delete_backend_object_fixture(&backend, fixture).await;
        backend
            .delete_group(owner_group.id(), &EventContext::system())
            .await
            .expect("backend-local authorization owner group should be removed")
            .into_value();
        delete_backend_user(&backend, user).await;
    }
}
