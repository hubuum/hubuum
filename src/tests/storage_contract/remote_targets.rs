use super::*;
use crate::services::remote_targets::create_remote_target;
use hubuum_domain::RemoteTargetId;
use hubuum_storage_core::StorageErrorKind;

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
