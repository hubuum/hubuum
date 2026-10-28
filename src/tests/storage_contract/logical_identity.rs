use super::*;
use hubuum_storage_conformance::verify_collection_version_identity;

#[actix_web::test]
async fn every_backend_preserves_identity_when_closing_history() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture =
            create_backend_object_fixture(&backend, &prefix("logical_identity"), vec![]).await;
        let result = verify_collection_version_identity(
            &backend,
            backend.collection_store().as_ref(),
            fixture.collection.id(),
        )
        .await;
        delete_backend_object_fixture(&backend, fixture).await;
        assert!(result.is_ok(), "{result:?}");
    }
}
