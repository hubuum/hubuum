use super::*;
use crate::services::remote_targets::create_remote_target;
use hubuum_domain::RemoteTargetId;
use hubuum_storage_core::StorageErrorKind;
use serde_json::json;

#[actix_web::test]
async fn remote_target_updates_are_fenced_by_the_authorized_revision() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture =
            create_backend_object_fixture(&backend, &prefix("remote_revision"), vec![]).await;
        let target = create_remote_target(&backend, serde_json::from_value(serde_json::json!({
            "collection_id":fixture.collection.id().id(), "name":prefix("target"), "description":"test",
            "method":"post", "url_template":"https://approved.example/hook", "allowed_subject_types":["collection"]
        })).unwrap(), EventContext::system()).await.unwrap();
        let id = RemoteTargetId::new(target.id).unwrap();
        let request = |url: &str| {
            StorageRemoteTargetUpdate::new(
                id,
                target.revision,
                StorageRemoteTargetPatch::new().with_url_template(Some(url.to_owned())),
                EventContext::system(),
            )
        };
        let (first, second) = tokio::join!(
            backend.update_remote_target(request("https://first.example/hook")),
            backend.update_remote_target(request("https://second.example/hook")),
        );
        assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
        assert_eq!(
            first.err().or(second.err()).unwrap().kind(),
            StorageErrorKind::Conflict
        );
        delete_backend_object_fixture(&backend, fixture).await;
    }
}

#[rstest::rstest]
#[case::approved_path("https://approved.example/other", true)]
#[case::different_host("https://other.example/hook", false)]
#[case::different_port("https://approved.example:8443/hook", false)]
#[case::http("http://approved.example/hook", false)]
#[actix_web::test]
async fn authorized_invocation_preserves_the_credential_destination(
    #[case] rendered_url: &str,
    #[case] allowed: bool,
) {
    use crate::models::{RemoteInvocationSubject, StoredRemoteCallTaskPayload};
    use crate::services::remote_targets::invocation::AuthorizedRemoteInvocation;
    use hubuum_outbound_http::validate_outbound_url;
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture =
            create_backend_object_fixture(&backend, &prefix("remote_destination"), vec![]).await;
        let user = super::history_fixtures::owner_user(&backend, &fixture).await;
        backend
            .apply_local_collection_grant(StorageAuthorizationGrantMutation::new(
                StorageAuthorizationGrantKey::new(
                    fixture.collection.id(),
                    fixture.owned_group.as_ref().unwrap().id(),
                ),
                [
                    StorageAuthorizationPermission::ReadCollection,
                    StorageAuthorizationPermission::ExecuteRemoteTarget,
                ],
                false,
                EventContext::system(),
            ))
            .await
            .unwrap()
            .into_value();
        let target = create_remote_target(&backend, serde_json::from_value(serde_json::json!({
            "collection_id":fixture.collection.id().id(), "name":prefix("target"), "description":"test",
            "method":"post", "url_template":"https://approved.example/hook", "allowed_subject_types":["collection"],
            "auth_config":{"type":"bearer_secret","secret":"remote_test_token"}
        })).unwrap(), EventContext::system()).await.unwrap();
        let context = AppContext::new(
            backend.clone(),
            Arc::new(LocalPermissionBackend::new(backend.clone(), "admin".into())),
        );
        let principal = backend
            .get_authentication_identity(user.principal_id)
            .await
            .unwrap()
            .into_parts()
            .0;
        let request = StoredRemoteCallTaskPayload {
            target_id: RemoteTargetId::new(target.id).unwrap(),
            target_revision: Some(target.revision),
            subject: RemoteInvocationSubject::Collection {
                collection_id: fixture.collection.id(),
            },
            parameters: Default::default(),
            body_override: Default::default(),
        };
        let invocation =
            AuthorizedRemoteInvocation::from_stored(&context, &principal, None, &request)
                .await
                .unwrap();
        let approved = match validate_outbound_url(rendered_url) {
            Ok(parts) => invocation.approve_destination(parts).is_ok(),
            Err(_) => false,
        };
        assert_eq!(approved, allowed);
        delete_backend_user(&backend, user).await;
        delete_backend_object_fixture(&backend, fixture).await;
    }
}

#[rstest::rstest]
#[case::class_access_revoked(false)]
#[case::complete_context_read(true)]
#[actix_web::test]
async fn stored_invocation_rechecks_expanded_class_read(#[case] class_read: bool) {
    use crate::models::{RemoteInvocationSubject, StoredRemoteCallTaskPayload};
    use crate::services::remote_targets::invocation::AuthorizedRemoteInvocation;

    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture = create_backend_object_fixture(
            &backend,
            &prefix("remote_context_read"),
            vec![json!({"secret": "object-data"})],
        )
        .await;
        let user = super::history_fixtures::owner_user(&backend, &fixture).await;
        let mut permissions = vec![
            StorageAuthorizationPermission::ReadCollection,
            StorageAuthorizationPermission::ReadObject,
            StorageAuthorizationPermission::ExecuteRemoteTarget,
        ];
        if class_read {
            permissions.push(StorageAuthorizationPermission::ReadClass);
        }
        backend
            .apply_local_collection_grant(StorageAuthorizationGrantMutation::new(
                StorageAuthorizationGrantKey::new(
                    fixture.collection.id(),
                    fixture.owned_group.as_ref().unwrap().id(),
                ),
                permissions,
                true,
                EventContext::system(),
            ))
            .await
            .unwrap()
            .into_value();
        let target = create_remote_target(
            &backend,
            serde_json::from_value(json!({
                "collection_id": fixture.collection.id(),
                "class_id": fixture.class.id(),
                "name": prefix("target"),
                "description": "test",
                "method": "post",
                "url_template": "https://approved.example/hook",
                "allowed_subject_types": ["object"],
                "body_template": "{{ class | tojson }}"
            }))
            .unwrap(),
            EventContext::system(),
        )
        .await
        .unwrap();
        let context = AppContext::new(
            backend.clone(),
            Arc::new(LocalPermissionBackend::new(backend.clone(), "admin".into())),
        );
        let request = StoredRemoteCallTaskPayload {
            target_id: RemoteTargetId::new(target.id).unwrap(),
            target_revision: Some(target.revision),
            subject: RemoteInvocationSubject::Object {
                class_id: fixture.class.id(),
                object_id: fixture.objects[0].id(),
            },
            parameters: Default::default(),
            body_override: Default::default(),
        };
        let result =
            AuthorizedRemoteInvocation::from_stored(&context, &user.principal_id, None, &request)
                .await;
        if class_read {
            assert!(result.is_ok(), "the complete context should be readable");
        } else {
            assert!(matches!(result, Err(ApiError::Forbidden(_))));
        }
        delete_backend_user(&backend, user).await;
        delete_backend_object_fixture(&backend, fixture).await;
    }
}
