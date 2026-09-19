use super::*;
use crate::models::{
    ExportContentType, ExportScopeKind, ExportTemplateKind, NewExportTemplate,
    NewHubuumClassRelation, NewHubuumObjectRelation,
};
use crate::permissions::{AuthzTarget, ResourceRef};
use crate::services::authorization_resources::task_authorization_resources;
use crate::traits::{CanDelete, CanSave};
use hubuum_domain::{ExportTemplateId, RemoteTargetId};
use hubuum_storage_core::StorageAuthorizationResourceKey as K;
use std::collections::HashMap;

#[actix_web::test]
async fn batched_authorization_facts_match_individual_resolution_on_every_backend() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let from = create_backend_object_fixture(
            &backend,
            &prefix("batch_from"),
            vec![serde_json::json!({})],
        )
        .await;
        let to = create_backend_object_fixture(
            &backend,
            &prefix("batch_to"),
            vec![serde_json::json!({})],
        )
        .await;
        let class_relation = NewHubuumClassRelation {
            from_hubuum_class_id: from.class.id().id(),
            to_hubuum_class_id: to.class.id().id(),
            forward_template_alias: None,
            reverse_template_alias: None,
            from_max_relations: None,
            to_max_relations: None,
        }
        .save_without_events(&backend)
        .await
        .unwrap();
        let object_relation = NewHubuumObjectRelation {
            from_hubuum_object_id: from.objects[0].id().id(),
            to_hubuum_object_id: to.objects[0].id().id(),
            class_relation_id: class_relation.id,
        }
        .save_without_events(&backend)
        .await
        .unwrap();
        let template = NewExportTemplate {
            collection_id: from.collection.id().id(),
            name: prefix("batch_template"),
            description: String::new(),
            content_type: ExportContentType::TextPlain,
            template: "{{ name }}".into(),
            kind: ExportTemplateKind::Export,
            scope_kind: Some(ExportScopeKind::ObjectsInClass),
            class_id: Some(from.class.id().id()),
            default_query: None,
            include: None,
            relation_context: None,
            default_missing_data_policy: None,
            default_limits: None,
        }
        .save_without_events(&backend)
        .await
        .unwrap();
        let target = crate::services::remote_targets::create_remote_target(&backend, serde_json::from_value(serde_json::json!({
            "collection_id":to.collection.id().id(), "name":prefix("batch_remote"), "description":"batch fixture", "method":"post",
            "url_template":"https://example.com/", "allowed_subject_types":["collection"],
        })).unwrap(), EventContext::system()).await.unwrap();
        let expected = HashMap::from([
            (
                K::Collection(from.collection.id()),
                from.collection
                    .id()
                    .to_resource_ref(&backend)
                    .await
                    .unwrap(),
            ),
            (
                K::Class(from.class.id()),
                from.class.id().to_resource_ref(&backend).await.unwrap(),
            ),
            (
                K::Object(from.objects[0].id()),
                from.objects[0]
                    .id()
                    .to_resource_ref(&backend)
                    .await
                    .unwrap(),
            ),
            (
                K::ClassRelation(ClassRelationId::new(class_relation.id).unwrap()),
                class_relation.to_resource_ref(&backend).await.unwrap(),
            ),
            (
                K::ObjectRelation(ObjectRelationId::new(object_relation.id).unwrap()),
                object_relation.to_resource_ref(&backend).await.unwrap(),
            ),
            (
                K::ExportTemplate(ExportTemplateId::new(template.id).unwrap()),
                template.to_resource_ref(&backend).await.unwrap(),
            ),
            (
                K::RemoteTarget(RemoteTargetId::new(target.id).unwrap()),
                ResourceRef::remote_target(target.id, target.collection_id, Some(target.name)),
            ),
        ]);
        // Duplicate and missing identities must neither duplicate output nor invent
        // associations. Identical integer IDs in different kinds stay distinct.
        let missing = [
            K::Collection(CollectionId::new(i32::MAX).unwrap()),
            K::Class(ClassId::new(i32::MAX).unwrap()),
            K::Object(ObjectId::new(i32::MAX).unwrap()),
            K::ClassRelation(ClassRelationId::new(i32::MAX).unwrap()),
            K::ObjectRelation(ObjectRelationId::new(i32::MAX).unwrap()),
            K::ExportTemplate(ExportTemplateId::new(i32::MAX).unwrap()),
            K::RemoteTarget(RemoteTargetId::new(i32::MAX).unwrap()),
        ];
        let keys = expected
            .keys()
            .copied()
            .chain(expected.keys().copied())
            .chain(missing)
            .collect::<Vec<_>>();
        let facts = backend
            .load_authorization_resources(
                hubuum_storage_core::StorageAuthorizationResourcesQuery::new(keys.clone()),
            )
            .await
            .unwrap();
        assert_eq!(
            facts.iter().map(|fact| fact.key()).collect::<HashSet<_>>(),
            expected.keys().copied().collect::<HashSet<_>>()
        );
        let actual = task_authorization_resources(&backend, keys).await.unwrap();
        assert_eq!(actual, expected);
        object_relation
            .delete_without_events(&backend)
            .await
            .unwrap();
        class_relation
            .delete_without_events(&backend)
            .await
            .unwrap();
        ExportTemplateId::new(template.id)
            .unwrap()
            .delete_without_events(&backend)
            .await
            .unwrap();
        crate::services::remote_targets::delete_remote_target(
            &backend,
            target.id,
            EventContext::system(),
        )
        .await
        .unwrap();
        delete_backend_object_fixture(&backend, from).await;
        delete_backend_object_fixture(&backend, to).await;
    }
}

#[actix_web::test]
async fn batched_collection_decisions_preserve_single_grant_conjunction_and_order() {
    use hubuum_storage_core::{
        StorageAuthorizationCollectionAccessQuery as Query, StorageAuthorizationGrantKey,
        StorageAuthorizationGrantMutation, StorageAuthorizationPermission as P,
    };
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let user = create_backend_user(&backend, &prefix("batch_grants_user")).await;
        let fixture =
            create_backend_object_fixture(&backend, &prefix("batch_grants"), Vec::new()).await;
        let mut groups = Vec::new();
        for permission in [P::ReadClass, P::ReadObject] {
            let group = backend
                .create_group(
                    StorageGroupCreate::new(None, prefix("batch_grant_group"), None),
                    &EventContext::system(),
                )
                .await
                .unwrap()
                .into_value();
            backend
                .add_group_member(user.principal_id, group.id(), &EventContext::system())
                .await
                .unwrap()
                .into_value();
            backend
                .apply_local_collection_grant(StorageAuthorizationGrantMutation::new(
                    StorageAuthorizationGrantKey::new(fixture.collection.id(), group.id()),
                    [permission],
                    false,
                    EventContext::system(),
                ))
                .await
                .unwrap()
                .into_value();
            groups.push(group);
        }
        let query = |permissions: Vec<P>| {
            Query::new(user.principal_id, fixture.collection.id(), permissions)
        };
        let queries = vec![
            query(vec![P::ReadObject]),
            query(vec![P::ReadObject, P::ReadClass]),
            query(vec![P::ReadClass]),
            query(vec![P::ReadRemoteTarget]),
            query(Vec::new()),
            query(vec![P::ReadObject]),
            Query::new(
                user.principal_id,
                CollectionId::new(i32::MAX).unwrap(),
                Vec::new(),
            ),
            Query::new(
                PrincipalId::new(i32::MAX).unwrap(),
                fixture.collection.id(),
                Vec::new(),
            ),
        ];
        let mut expected = Vec::new();
        for query in queries.iter().cloned() {
            expected.push(backend.authorize_local_collection(query).await.unwrap());
        }
        assert_eq!(
            expected,
            vec![true, false, true, false, true, true, false, false]
        );
        assert_eq!(
            backend
                .authorize_local_collection_batch(queries)
                .await
                .unwrap(),
            expected
        );
        delete_backend_object_fixture(&backend, fixture).await;
        for group in groups {
            backend
                .delete_group(group.id(), &EventContext::system())
                .await
                .unwrap()
                .into_value();
        }
        delete_backend_user(&backend, user).await;
    }
}
