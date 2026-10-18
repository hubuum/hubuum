use super::history_fixtures::{create_named, external_permissions, owner_user, reclassify_object};
use super::*;
use crate::models::search::parse_query_parameter;
use crate::models::{
    HistoryDirection, HistoryTimelineParameters, Permissions, TokenResourceScope, TokenScope,
};
use crate::permissions::test_support::{MockAllowRule, MockTreetopBackend};
use crate::permissions::{ResourceFields, ResourceKind};
use crate::services::history::navigation::{HistoryEntry, HistoryReader, HistoryRows};
use crate::services::history::{class_objects_as_of, object_history_by_name};
use crate::storage::{
    StorageClassObjectsAsOfQuery, StorageHistoryNavigationQuery, StorageHistoryResource,
    StorageObjectHistoryNameQuery,
};
use chrono::{DateTime, Utc};
use hubuum_domain::HistoryRecordId;
use rstest::rstest;
use serde_json::json;

#[rstest]
#[case::class_collection(false, "collection")]
#[case::class_scope(false, "class")]
#[case::object_collection(true, "collection")]
#[case::object_class(true, "class")]
#[case::object_scope(true, "object")]
#[actix_web::test]
async fn history_surfaces_preserve_resource_scopes(
    #[case] object: bool,
    #[case] scope_kind: &str,
    #[values(false, true)] external: bool,
    #[values("list", "as_of", "timeline", "entry")] surface: &str,
) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture =
            create_backend_object_fixture(&backend, &prefix("history_access"), vec![json!({})])
                .await;
        let user = owner_user(&backend, &fixture).await;
        backend
            .apply_local_collection_grant(StorageAuthorizationGrantMutation::new(
                StorageAuthorizationGrantKey::new(
                    fixture.collection.id(),
                    fixture.owned_group.as_ref().unwrap().id(),
                ),
                [
                    StorageAuthorizationPermission::ReadClass,
                    StorageAuthorizationPermission::ReadObject,
                ],
                false,
                EventContext::system(),
            ))
            .await
            .unwrap()
            .into_value();
        let permission = if object {
            Permissions::ReadObject
        } else {
            Permissions::ReadClass
        };
        let resource = if object {
            StorageHistoryResource::Object {
                class_id: fixture.class.id(),
                object_id: fixture.objects[0].id(),
            }
        } else {
            StorageHistoryResource::Class(fixture.class.id())
        };
        let policy: Arc<dyn crate::permissions::PermissionBackend> = if external {
            let policy = Arc::new(MockTreetopBackend::new());
            policy.add_rule(MockAllowRule {
                group_id: fixture.owned_group.as_ref().unwrap().id().id(),
                action: permission,
                resource_kind: if object {
                    ResourceKind::Object
                } else {
                    ResourceKind::Class
                },
                resource_id: None,
                attrs: ResourceFields {
                    collection_id: Some(fixture.collection.id().id()),
                    ..Default::default()
                },
            });
            policy
        } else {
            Arc::new(LocalPermissionBackend::new(backend.clone(), "admin".into()))
        };
        let context = AppContext::new(backend.clone(), policy);
        let principal = backend
            .get_authentication_identity(user.principal_id)
            .await
            .unwrap()
            .into_parts()
            .0;
        let scope = TokenScope::from_request_parts(
            Some(vec![permission]),
            Some(vec![match scope_kind {
                "collection" => TokenResourceScope::Collection(fixture.collection.id()),
                "class" => TokenResourceScope::Class(fixture.class.id()),
                "object" => TokenResourceScope::Object(fixture.objects[0].id()),
                _ => unreachable!(),
            }]),
        )
        .unwrap()
        .unwrap();
        let options = parse_query_parameter("limit=10&sort=history_id").unwrap();
        let (expected, at) = initial_history(&backend, resource).await;
        let reader = HistoryReader::authorize(&context, &principal, Some(&scope), resource)
            .await
            .unwrap();
        let ids = match surface {
            "list" => match reader.list(&options).await.unwrap().0 {
                HistoryRows::Class(rows) => rows
                    .into_iter()
                    .map(|row| row.history_id)
                    .collect::<Vec<_>>(),
                HistoryRows::Object(rows) => rows.into_iter().map(|row| row.history_id).collect(),
            },
            "timeline" => reader
                .timeline(
                    serde_json::from_value::<HistoryTimelineParameters>(json!({"limit": 10}))
                        .unwrap(),
                )
                .await
                .unwrap()
                .0
                .into_iter()
                .map(|row| row.history_id)
                .collect(),
            "as_of" | "entry" => {
                let entry = if surface == "as_of" {
                    reader.as_of(at).await.unwrap()
                } else {
                    reader.entry(expected).await.unwrap()
                };
                vec![match entry {
                    HistoryEntry::Class(row) => row.history_id,
                    HistoryEntry::Object(row) => row.history_id,
                }]
            }
            _ => unreachable!(),
        };
        assert_eq!(
            ids,
            vec![expected.id()],
            "{surface}, {scope_kind}, external={external}"
        );
        delete_backend_user(&backend, user).await;
        delete_backend_object_fixture(&backend, fixture).await;
    }
}

#[rstest]
#[case::denied_local(false, false)]
#[case::allowed_local(false, true)]
#[case::denied_external_administrator(true, false)]
#[case::allowed_external_administrator(true, true)]
#[actix_web::test]
async fn reclassified_objects_keep_live_authorization(
    #[case] external: bool,
    #[case] current_readable: bool,
    #[values(false, true)] population: bool,
) {
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let backend = environment.storage();
        let source =
            create_backend_object_fixture(&backend, &prefix("history_source"), vec![]).await;
        let mut target =
            create_backend_object_fixture(&backend, &prefix("history_target"), vec![]).await;
        let object = create_named(&backend, &source, "retained").await;
        let (_, at) = initial_history(
            &backend,
            StorageHistoryResource::Object {
                class_id: source.class.id(),
                object_id: object.id(),
            },
        )
        .await;
        reclassify_object(&environment, object.id(), &target).await;
        target.objects.push(object);
        let user = owner_user(&backend, &source).await;
        if current_readable {
            backend
                .apply_local_collection_grant(StorageAuthorizationGrantMutation::new(
                    StorageAuthorizationGrantKey::new(
                        target.collection.id(),
                        source.owned_group.as_ref().unwrap().id(),
                    ),
                    [StorageAuthorizationPermission::ReadObject],
                    false,
                    EventContext::system(),
                ))
                .await
                .unwrap()
                .into_value();
        }
        let policy: Arc<dyn crate::permissions::PermissionBackend> = if external {
            let policy = external_permissions(&source, None, true);
            if current_readable {
                policy.add_rule(MockAllowRule {
                    group_id: source.owned_group.as_ref().unwrap().id().id(),
                    action: Permissions::ReadObject,
                    resource_kind: ResourceKind::Object,
                    resource_id: None,
                    attrs: ResourceFields {
                        collection_id: Some(target.collection.id().id()),
                        ..Default::default()
                    },
                });
            }
            policy
        } else {
            Arc::new(LocalPermissionBackend::new(backend.clone(), "admin".into()))
        };
        let context = AppContext::new(backend.clone(), policy);
        let principal = backend
            .get_authentication_identity(user.principal_id)
            .await
            .unwrap()
            .into_parts()
            .0;
        let options = parse_query_parameter("limit=1").unwrap();
        let (rows, total) = if population {
            class_objects_as_of(
                &context,
                &principal,
                None,
                StorageClassObjectsAsOfQuery::try_new(source.class.id(), at, options).unwrap(),
            )
            .await
            .unwrap()
        } else {
            object_history_by_name(
                &context,
                &principal,
                None,
                StorageObjectHistoryNameQuery::try_new(
                    source.class.id(),
                    "retained".into(),
                    options,
                )
                .unwrap(),
            )
            .await
            .unwrap()
        };
        assert_eq!(
            (rows.len(), total),
            (usize::from(current_readable), i64::from(current_readable)),
            "population={population}, external={external}"
        );
        delete_backend_user(&backend, user).await;
        delete_backend_object_fixture(&backend, target).await;
        delete_backend_object_fixture(&backend, source).await;
    }
}

async fn initial_history(
    backend: &StorageHandle,
    resource: StorageHistoryResource,
) -> (HistoryRecordId, DateTime<Utc>) {
    let options = QueryOptions::new(
        vec![],
        HistoryDirection::Older.sorts(),
        Some(1),
        None,
        false,
    )
    .unwrap();
    let marker = backend
        .list_history_markers(
            StorageHistoryNavigationQuery::try_new(
                resource,
                options,
                StorageHistoryCollectionScope::All,
            )
            .unwrap(),
        )
        .await
        .unwrap()
        .into_parts()
        .0
        .remove(0);
    let metadata = marker.into_parts().3.into_parts();
    (metadata.history_entry_id(), metadata.valid_from())
}

#[actix_web::test]
async fn point_history_reports_missing_retention_after_the_authorization_gate() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture =
            create_backend_object_fixture(&backend, &prefix("history_missing"), vec![]).await;
        let user = owner_user(&backend, &fixture).await;
        let context = AppContext::new(backend.clone(), external_permissions(&fixture, None, true));
        let principal = backend
            .get_authentication_identity(user.principal_id)
            .await
            .unwrap()
            .into_parts()
            .0;
        let reader = HistoryReader::authorize(
            &context,
            &principal,
            None,
            StorageHistoryResource::Class(ClassId::new(i32::MAX).unwrap()),
        )
        .await
        .unwrap();
        assert!(matches!(
            reader.as_of(Utc::now()).await,
            Err(ApiError::NotFound(_))
        ));
        delete_backend_user(&backend, user).await;
        delete_backend_object_fixture(&backend, fixture).await;
    }
}
