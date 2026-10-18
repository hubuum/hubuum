use super::*;
use crate::models::Permissions;
use crate::permissions::test_support::{MockAllowRule, MockTreetopBackend};
use crate::permissions::{ResourceFields, ResourceKind};
use serde_json::json;

pub(super) async fn create_named(
    backend: &StorageHandle,
    fixture: &BackendObjectFixture,
    name: &str,
) -> StorageObject {
    let class = backend
        .class_store()
        .resolve_class(StorageClassSelector::Id(fixture.class.id()))
        .await
        .unwrap();
    backend
        .object_store()
        .create_object(
            &class,
            StorageObjectCreate::new(
                name,
                fixture.collection.id(),
                fixture.class.id(),
                json!({"value": 1}),
                "history fixture",
            ),
            &EventContext::system(),
        )
        .await
        .unwrap()
        .into_value()
}

pub(super) async fn update(
    backend: &StorageHandle,
    id: ObjectId,
    changes: StorageObjectUpdate,
) -> StorageObject {
    let object = backend.object_store().get_object(id).await.unwrap();
    backend
        .object_store()
        .update_object(&object, changes, &EventContext::system())
        .await
        .unwrap()
        .into_value()
}

pub(super) async fn delete(backend: &StorageHandle, id: ObjectId) {
    let object = backend.object_store().get_object(id).await.unwrap();
    backend
        .object_store()
        .delete_object(&object, &EventContext::system())
        .await
        .unwrap()
        .into_value();
}

pub(super) async fn move_class(
    backend: &StorageHandle,
    class_id: ClassId,
    collection_id: CollectionId,
) {
    let class = backend
        .class_store()
        .resolve_class(StorageClassSelector::Id(class_id))
        .await
        .unwrap();
    backend
        .class_store()
        .update_class(
            &class,
            StorageClassUpdate::builder()
                .collection_id(Some(collection_id))
                .build(),
            &EventContext::system(),
        )
        .await
        .unwrap()
        .into_value();
}

pub(super) async fn relocate_object(
    environment: &BackendTestEnvironment,
    id: ObjectId,
    source: &BackendObjectFixture,
    target: &BackendObjectFixture,
) {
    match environment {
        BackendTestEnvironment::Postgres { pool } => {
            hubuum_storage_postgres::test_support::relocate_object_history_fixture(
                pool,
                source.class.id(),
                id,
                target.collection.id(),
            )
            .await
            .unwrap();
        }
        BackendTestEnvironment::Memory { .. } => {
            let backend = environment.storage();
            let object = backend.object_store().get_object(id).await.unwrap();
            move_class(&backend, source.class.id(), target.collection.id()).await;
            backend
                .object_store()
                .update_object(
                    &object,
                    StorageObjectUpdate::builder()
                        .collection_id(Some(target.collection.id()))
                        .build(),
                    &EventContext::system(),
                )
                .await
                .unwrap()
                .into_value();
        }
    }
}

pub(super) async fn grant_object_reads(backend: &StorageHandle, fixture: &BackendObjectFixture) {
    backend
        .apply_local_collection_grant(StorageAuthorizationGrantMutation::new(
            StorageAuthorizationGrantKey::new(
                fixture.collection.id(),
                fixture.owned_group.as_ref().unwrap().id(),
            ),
            [StorageAuthorizationPermission::ReadObject],
            false,
            EventContext::system(),
        ))
        .await
        .unwrap()
        .into_value();
}

pub(super) async fn owner_user(
    backend: &StorageHandle,
    fixture: &BackendObjectFixture,
) -> BackendUserFixture {
    grant_object_reads(backend, fixture).await;
    let user = create_backend_user(backend, &prefix("history_reader")).await;
    backend
        .add_group_member(
            user.principal_id,
            fixture.owned_group.as_ref().unwrap().id(),
            &EventContext::system(),
        )
        .await
        .unwrap()
        .into_value();
    user
}

pub(super) fn external_permissions(
    fixture: &BackendObjectFixture,
    name: Option<&str>,
    admin: bool,
) -> Arc<MockTreetopBackend> {
    let backend = Arc::new(MockTreetopBackend::new());
    let group_id = fixture.owned_group.as_ref().unwrap().id().id();
    backend.add_rule(MockAllowRule {
        group_id,
        action: Permissions::ReadObject,
        resource_kind: ResourceKind::Object,
        resource_id: None,
        attrs: ResourceFields {
            collection_id: Some(fixture.collection.id().id()),
            name: name.map(str::to_string),
            ..Default::default()
        },
    });
    if admin {
        backend.add_admin_rule(group_id);
    }
    backend
}

/// Manufacture retained class changes with each adapter's supported test fixture.
pub(super) async fn reclassify_object(
    environment: &BackendTestEnvironment,
    object_id: ObjectId,
    target: &BackendObjectFixture,
) {
    match environment {
        BackendTestEnvironment::Postgres { pool } => {
            hubuum_storage_postgres::test_support::reclassify_object_history_fixture(
                pool,
                object_id,
                target.class.id(),
            )
            .await
            .unwrap();
        }
        BackendTestEnvironment::Memory { .. } => {
            update(
                &environment.storage(),
                object_id,
                StorageObjectUpdate::builder()
                    .class_id(Some(target.class.id()))
                    .collection_id(Some(target.collection.id()))
                    .build(),
            )
            .await;
        }
    }
}
