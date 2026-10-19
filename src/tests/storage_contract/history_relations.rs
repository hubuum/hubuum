use super::history_fixtures::{move_class, update};
use super::*;
use crate::models::search::parse_query_parameter;
use crate::models::{HistoricalEndpointContext, Permissions, TokenResourceScope, TokenScope};
use crate::pagination::{NEXT_CURSOR_HEADER, TOTAL_COUNT_HEADER, finalize_page};
use crate::permissions::test_support::{MockAllowRule, MockTreetopBackend};
use crate::permissions::{ResourceFields, ResourceKind};
use crate::services::history::navigation::HistoryReader;
use crate::storage::{
    StorageHistoricalRelation, StorageHistoryResource, StorageRelationHistoryEntry,
    StorageRelationHistoryQuery, StorageRelationHistoryResource, StorageRelationHistorySelection,
};
use chrono::{DateTime, Duration as ChronoDuration, SecondsFormat, Utc};
use rstest::rstest;
use serde_json::{Value, json};

fn relation_permission(objects: bool) -> Permissions {
    if objects {
        Permissions::ReadObjectRelation
    } else {
        Permissions::ReadClassRelation
    }
}

#[rstest]
#[case::ordinary(false)]
#[case::administrator(true)]
#[actix_web::test]
async fn relationship_root_rejects_wrong_class_even_for_deleted_history_admin(
    #[case] admin: bool,
    #[values(false, true)] deleted: bool,
) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture =
            create_backend_relation_fixture(&backend, &prefix("relation_wrong_class")).await;
        let user = user(&backend, &fixture).await;
        if deleted {
            super::history_fixtures::delete(&backend, fixture.objects[0].id()).await;
        }
        let context = AppContext::new(backend.clone(), policy(&fixture, true, None, admin));
        let principal = backend
            .get_authentication_identity(user.principal_id)
            .await
            .unwrap()
            .into_parts()
            .0;
        let result = HistoryReader::authorize(
            &context,
            &principal,
            None,
            StorageHistoryResource::Object {
                class_id: fixture.classes[1].id(),
                object_id: fixture.objects[0].id(),
            },
        )
        .await;
        let result = match result {
            Ok(reader) => reader
                .relationships(
                    StorageRelationHistorySelection::Changes,
                    parse_query_parameter("limit=10").unwrap(),
                )
                .await
                .map(|_| ()),
            Err(error) => Err(error),
        };
        assert!(matches!(result, Err(ApiError::NotFound(_))));
        delete_backend_user(&backend, user).await;
        cleanup(&backend, fixture).await;
    }
}

fn root(fixture: &BackendRelationFixture, objects: bool) -> StorageHistoryResource {
    if objects {
        StorageHistoryResource::Object {
            class_id: fixture.classes[0].id(),
            object_id: fixture.objects[0].id(),
        }
    } else {
        StorageHistoryResource::Class(fixture.classes[0].id())
    }
}

fn query(
    resource: StorageHistoryResource,
    selection: StorageRelationHistorySelection,
    params: &str,
) -> StorageRelationHistoryQuery {
    let resource = match resource {
        StorageHistoryResource::Class(id) => StorageRelationHistoryResource::Class(id),
        StorageHistoryResource::Object { object_id, .. } => {
            StorageRelationHistoryResource::Object(object_id)
        }
    };
    StorageRelationHistoryQuery::try_new(
        resource,
        selection,
        parse_query_parameter(params).unwrap(),
    )
    .unwrap()
}

async fn history(
    backend: &StorageHandle,
    resource: StorageHistoryResource,
) -> Vec<StorageRelationHistoryEntry> {
    backend
        .list_relation_history(query(
            resource,
            StorageRelationHistorySelection::Changes,
            "limit=50&sort=history_id",
        ))
        .await
        .unwrap()
        .into_parts()
        .0
}

async fn delete_edge(backend: &StorageHandle, fixture: &BackendRelationFixture, objects: bool) {
    if objects {
        let row = backend
            .object_relation_store()
            .resolve_object_relation(StorageObjectRelationSelector::Id(
                fixture.object_relation_id,
            ))
            .await
            .unwrap();
        backend
            .object_relation_store()
            .delete_object_relation(&row, &EventContext::system())
            .await
            .unwrap()
            .into_value();
    } else {
        let row = backend
            .class_relation_store()
            .resolve_class_relation(fixture.class_relation_id)
            .await
            .unwrap();
        backend
            .class_relation_store()
            .delete_class_relation(&row, &EventContext::system())
            .await
            .unwrap()
            .into_value();
    }
}

async fn cleanup(backend: &StorageHandle, fixture: BackendRelationFixture) {
    // Classes own the cascades, including edges explicitly removed by a test.
    for class in fixture.classes {
        match backend
            .class_store()
            .resolve_class(StorageClassSelector::Id(class.id()))
            .await
        {
            Ok(class) => {
                backend
                    .class_store()
                    .delete_class(&class, &EventContext::system())
                    .await
                    .unwrap()
                    .into_value();
            }
            Err(error) => assert_eq!(error.kind(), StorageErrorKind::NotFound),
        }
    }
    backend
        .collection_store()
        .delete_collection(fixture.collection.id(), &EventContext::system())
        .await
        .unwrap()
        .into_value();
    backend
        .delete_group(fixture.owner_group.id(), &EventContext::system())
        .await
        .unwrap()
        .into_value();
}

fn edge_id(row: &StorageRelationHistoryEntry) -> i32 {
    match row.record().relation() {
        StorageHistoricalRelation::Class(row) => row.metadata().id().id(),
        StorageHistoricalRelation::Object(row) => row.metadata().id().id(),
    }
}

#[rstest]
#[case::before_create(0, -1, false)]
#[case::created(0, 0, true)]
#[case::after_create(0, 1, true)]
#[case::before_delete(1, -1, true)]
#[case::deleted(1, 0, false)]
#[case::after_delete(1, 1, false)]
#[actix_web::test]
async fn every_backend_relation_validity_is_half_open(
    #[values(false, true)] objects: bool,
    #[case] boundary: usize,
    #[case] nanos: i64,
    #[case] active: bool,
) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture = create_backend_relation_fixture(&backend, &prefix("relation_boundary")).await;
        let resource = root(&fixture, objects);
        delete_edge(&backend, &fixture, objects).await;
        let changes = history(&backend, resource).await;
        let at =
            changes[boundary].record().metadata().valid_from() + ChronoDuration::nanoseconds(nanos);
        let (rows, total) = backend
            .list_relation_history(query(
                resource,
                StorageRelationHistorySelection::At(at),
                "limit=2",
            ))
            .await
            .unwrap()
            .into_parts();
        assert_eq!(rows.len(), usize::from(active));
        assert_eq!(total, Some(i64::from(active)));
        if let Some(row) = rows.first() {
            assert_eq!(edge_id(row), edge_id(&changes[0]));
        }
        cleanup(&backend, fixture).await;
    }
}

#[rstest]
#[case::class(false)]
#[case::object(true)]
#[actix_web::test]
async fn every_backend_recreated_edge_keeps_distinct_identity(#[case] objects: bool) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture =
            create_backend_relation_fixture(&backend, &prefix("relation_recreated")).await;
        let resource = root(&fixture, objects);
        delete_edge(&backend, &fixture, objects).await;
        if objects {
            let prepared = backend
                .object_relation_store()
                .prepare_object_relation(StorageObjectRelationCreateSelector::Explicit(
                    StorageObjectRelationCreate::new(
                        fixture.objects[0].id(),
                        fixture.objects[1].id(),
                        fixture.class_relation_id,
                    ),
                ))
                .await
                .unwrap();
            backend
                .object_relation_store()
                .create_object_relation(&prepared, &EventContext::system())
                .await
                .unwrap()
                .into_value();
        } else {
            let prepared = backend
                .class_relation_store()
                .prepare_class_relation(
                    StorageClassRelationCreate::builder(
                        fixture.classes[0].id(),
                        fixture.classes[1].id(),
                    )
                    .build(),
                )
                .await
                .unwrap();
            backend
                .class_relation_store()
                .create_class_relation(&prepared, &EventContext::system())
                .await
                .unwrap()
                .into_value();
        }
        let changes = history(&backend, resource).await;
        assert_eq!(
            changes
                .iter()
                .map(|row| row.record().metadata().operation().as_str())
                .collect::<Vec<_>>(),
            ["create", "delete", "create"]
        );
        assert_eq!(edge_id(&changes[0]), edge_id(&changes[1]));
        assert_ne!(edge_id(&changes[0]), edge_id(&changes[2]));
        let rows = backend
            .list_relation_history(query(
                resource,
                StorageRelationHistorySelection::At(changes[2].context_at()),
                "limit=5",
            ))
            .await
            .unwrap()
            .into_parts()
            .0;
        assert_eq!(
            rows.iter().map(edge_id).collect::<Vec<_>>(),
            [edge_id(&changes[2])]
        );
        cleanup(&backend, fixture).await;
    }
}

async fn rename_endpoint(
    backend: &StorageHandle,
    fixture: &BackendRelationFixture,
    objects: bool,
    name: &str,
) {
    if objects {
        update(
            backend,
            fixture.objects[1].id(),
            StorageObjectUpdate::builder()
                .name(Some(name.into()))
                .build(),
        )
        .await;
    } else {
        let class = backend
            .class_store()
            .resolve_class(StorageClassSelector::Id(fixture.classes[1].id()))
            .await
            .unwrap();
        backend
            .class_store()
            .update_class(
                &class,
                StorageClassUpdate::builder()
                    .name(Some(name.into()))
                    .build(),
                &EventContext::system(),
            )
            .await
            .unwrap()
            .into_value();
    }
}

#[rstest]
#[case::old(false)]
#[case::new(true)]
#[actix_web::test]
async fn every_backend_hydrates_endpoint_at_selected_instant(
    #[values(false, true)] objects: bool,
    #[case] after_rename: bool,
) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture = create_backend_relation_fixture(&backend, &prefix("relation_context")).await;
        let resource = root(&fixture, objects);
        let old_name = if objects {
            fixture.objects[1].name()
        } else {
            fixture.classes[1].name()
        };
        let created = history(&backend, resource).await[0].context_at();
        let new_name = prefix("renamed_endpoint");
        rename_endpoint(&backend, &fixture, objects, &new_name).await;
        let at = if after_rename { Utc::now() } else { created };
        let row = backend
            .list_relation_history(query(
                resource,
                StorageRelationHistorySelection::At(at),
                "limit=1",
            ))
            .await
            .unwrap()
            .into_parts()
            .0
            .pop()
            .unwrap();
        assert_eq!(
            row.to().unwrap().name(),
            if after_rename {
                new_name.as_str()
            } else {
                old_name
            }
        );
        assert_eq!(row.context_at(), at);
        cleanup(&backend, fixture).await;
    }
}

#[rstest]
#[case::class(false)]
#[case::object(true)]
#[actix_web::test]
async fn every_backend_endpoint_cascade_closes_adjacency(#[case] objects: bool) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture = create_backend_relation_fixture(&backend, &prefix("relation_cascade")).await;
        let resource = root(&fixture, objects);
        let class = backend
            .class_store()
            .resolve_class(StorageClassSelector::Id(fixture.classes[1].id()))
            .await
            .unwrap();
        backend
            .class_store()
            .delete_class(&class, &EventContext::system())
            .await
            .unwrap()
            .into_value();
        let changes = history(&backend, resource).await;
        assert_eq!(
            changes
                .last()
                .unwrap()
                .record()
                .metadata()
                .operation()
                .as_str(),
            "delete"
        );
        let rows = backend
            .list_relation_history(query(
                resource,
                StorageRelationHistorySelection::At(Utc::now()),
                "limit=5",
            ))
            .await
            .unwrap()
            .into_parts()
            .0;
        assert!(rows.is_empty());
        cleanup(&backend, fixture).await;
    }
}

async fn user(backend: &StorageHandle, fixture: &BackendRelationFixture) -> BackendUserFixture {
    let user = create_backend_user(backend, &prefix("relation_reader")).await;
    backend
        .add_group_member(
            user.principal_id,
            fixture.owner_group.id(),
            &EventContext::system(),
        )
        .await
        .unwrap()
        .into_value();
    user
}

fn policy(
    fixture: &BackendRelationFixture,
    objects: bool,
    name: Option<&str>,
    admin: bool,
) -> Arc<MockTreetopBackend> {
    let policy = Arc::new(MockTreetopBackend::new());
    let (permission, kind, ids) = if objects {
        (
            Permissions::ReadObject,
            ResourceKind::Object,
            [fixture.objects[0].id().id(), fixture.objects[1].id().id()],
        )
    } else {
        (
            Permissions::ReadClass,
            ResourceKind::Class,
            [fixture.classes[0].id().id(), fixture.classes[1].id().id()],
        )
    };
    for (index, id) in ids.into_iter().enumerate() {
        policy.add_rule(MockAllowRule {
            group_id: fixture.owner_group.id().id(),
            action: permission,
            resource_kind: kind.clone(),
            resource_id: Some(id),
            attrs: ResourceFields {
                name: (index == 1).then_some(name).flatten().map(str::to_owned),
                ..Default::default()
            },
        });
    }
    policy.add_rule(MockAllowRule {
        group_id: fixture.owner_group.id().id(),
        action: relation_permission(objects),
        resource_kind: if objects {
            ResourceKind::ObjectRelation
        } else {
            ResourceKind::ClassRelation
        },
        resource_id: None,
        attrs: ResourceFields::default(),
    });
    if admin {
        policy.add_admin_rule(fixture.owner_group.id().id());
    }
    policy
}

#[rstest]
#[case::historical_only(false)]
#[case::current_only(true)]
#[actix_web::test]
async fn relationship_policy_requires_current_and_historical_endpoint_names(
    #[values(false, true)] objects: bool,
    #[case] current: bool,
) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture = create_backend_relation_fixture(&backend, &prefix("relation_policy")).await;
        let at = Utc::now();
        let old_name = if objects {
            fixture.objects[1].name()
        } else {
            fixture.classes[1].name()
        };
        let new_name = prefix("renamed_policy_endpoint");
        rename_endpoint(&backend, &fixture, objects, &new_name).await;
        let user = user(&backend, &fixture).await;
        let context = AppContext::new(
            backend.clone(),
            policy(
                &fixture,
                objects,
                Some(if current { new_name.as_str() } else { old_name }),
                false,
            ),
        );
        let principal = backend
            .get_authentication_identity(user.principal_id)
            .await
            .unwrap()
            .into_parts()
            .0;
        let reader = HistoryReader::authorize(&context, &principal, None, root(&fixture, objects))
            .await
            .unwrap();
        let (rows, total) = reader
            .relationships(
                StorageRelationHistorySelection::At(at),
                parse_query_parameter("limit=1").unwrap(),
            )
            .await
            .unwrap();
        assert!(rows.is_empty());
        assert_eq!(total, 0);
        delete_backend_user(&backend, user).await;
        cleanup(&backend, fixture).await;
    }
}

#[rstest]
#[case::ordinary(false, false, 0)]
#[case::administrator(true, false, 2)]
#[case::scoped_administrator(true, true, 0)]
#[actix_web::test]
async fn relationship_deleted_history_requires_unscoped_administrator(
    #[values(false, true)] objects: bool,
    #[case] admin: bool,
    #[case] scoped: bool,
    #[case] expected: usize,
) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture = create_backend_relation_fixture(&backend, &prefix("relation_deleted")).await;
        delete_edge(&backend, &fixture, objects).await;
        let user = user(&backend, &fixture).await;
        let context = AppContext::new(backend.clone(), policy(&fixture, objects, None, admin));
        let scope = scoped.then(|| {
            TokenScope::from_request_parts(
                Some(vec![
                    relation_permission(objects),
                    if objects {
                        Permissions::ReadObject
                    } else {
                        Permissions::ReadClass
                    },
                ]),
                Some(vec![TokenResourceScope::Collection(
                    fixture.collection.id(),
                )]),
            )
            .unwrap()
            .unwrap()
        });
        let principal = backend
            .get_authentication_identity(user.principal_id)
            .await
            .unwrap()
            .into_parts()
            .0;
        let reader = HistoryReader::authorize(
            &context,
            &principal,
            scope.as_ref(),
            root(&fixture, objects),
        )
        .await
        .unwrap();
        let (rows, total) = reader
            .relationships(
                StorageRelationHistorySelection::Changes,
                parse_query_parameter("limit=10").unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), expected);
        assert_eq!(total, expected as i64);
        delete_backend_user(&backend, user).await;
        cleanup(&backend, fixture).await;
    }
}

async fn add_edge(backend: &StorageHandle, fixture: &mut BackendRelationFixture, objects: bool) {
    if objects {
        let class = backend
            .class_store()
            .resolve_class(StorageClassSelector::Id(fixture.classes[1].id()))
            .await
            .unwrap();
        let object = backend
            .object_store()
            .create_object(
                &class,
                StorageObjectCreate::new(
                    prefix("extra_endpoint"),
                    fixture.collection.id(),
                    fixture.classes[1].id(),
                    json!({}),
                    "",
                ),
                &EventContext::system(),
            )
            .await
            .unwrap()
            .into_value();
        let prepared = backend
            .object_relation_store()
            .prepare_object_relation(StorageObjectRelationCreateSelector::Explicit(
                StorageObjectRelationCreate::new(
                    fixture.objects[0].id(),
                    object.id(),
                    fixture.class_relation_id,
                ),
            ))
            .await
            .unwrap();
        backend
            .object_relation_store()
            .create_object_relation(&prepared, &EventContext::system())
            .await
            .unwrap()
            .into_value();
        fixture.objects.push(object);
    } else {
        let class = backend
            .class_store()
            .create_class(
                StorageClassCreate::builder(prefix("extra_endpoint"), fixture.collection.id(), "")
                    .build(),
                &EventContext::system(),
            )
            .await
            .unwrap()
            .into_value();
        let prepared = backend
            .class_relation_store()
            .prepare_class_relation(
                StorageClassRelationCreate::builder(fixture.classes[0].id(), class.id()).build(),
            )
            .await
            .unwrap();
        backend
            .class_relation_store()
            .create_class_relation(&prepared, &EventContext::system())
            .await
            .unwrap()
            .into_value();
        fixture.classes.push(class);
    }
}

fn uri(
    fixture: &BackendRelationFixture,
    objects: bool,
    at: Option<DateTime<Utc>>,
    params: &str,
) -> String {
    let base = match root(fixture, objects) {
        StorageHistoryResource::Class(id) => format!("/api/v1/classes/{id}"),
        StorageHistoryResource::Object {
            class_id,
            object_id,
        } => format!("/api/v1/classes/{class_id}/{object_id}"),
    };
    match at {
        Some(at) => format!(
            "{base}/relations/history/as-of?at={}&{params}",
            at.to_rfc3339_opts(SecondsFormat::Nanos, true)
        ),
        None => format!("{base}/relations/history?{params}"),
    }
}

#[rstest]
#[case::counted(true)]
#[case::uncounted(false)]
#[actix_web::test]
async fn relationship_api_pages_past_hidden_endpoints_without_count_or_cursor_leaks(
    #[values(false, true)] objects: bool,
    #[case] include_total: bool,
) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let mut fixture =
            create_backend_relation_fixture(&backend, &prefix("relation_hidden")).await;
        add_edge(&backend, &mut fixture, objects).await;
        let user = user(&backend, &fixture).await;
        let app = test::init_service(
            App::new()
                .app_data(Data::new(AppContext::new(
                    backend.clone(),
                    policy(&fixture, objects, None, false),
                )))
                .configure(crate::api::config),
        )
        .await;
        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&uri(
                    &fixture,
                    objects,
                    Some(Utc::now()),
                    &format!("limit=1&include_total={include_total}"),
                ))
                .insert_header((
                    http::header::AUTHORIZATION,
                    format!("Bearer {}", user.raw_token),
                ))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), http::StatusCode::OK);
        assert!(response.headers().get(NEXT_CURSOR_HEADER).is_none());
        assert_eq!(
            response
                .headers()
                .get(TOTAL_COUNT_HEADER)
                .map(|value| value.to_str().unwrap()),
            include_total.then_some("1")
        );
        let body: Value = test::read_body_json(response).await;
        assert_eq!(body["relations"].as_array().unwrap().len(), 1);
        assert_eq!(body["coverage"], "retained_history");
        assert_eq!(body["relations"][0]["to"]["status"], "available");
        delete_backend_user(&backend, user).await;
        cleanup(&backend, fixture).await;
    }
}

#[rstest]
#[case::instant("instant")]
#[case::root("root")]
#[case::selection("selection")]
#[case::order("order")]
#[case::token("token")]
#[actix_web::test]
async fn relationship_api_rejects_rebound_cursors(
    #[values(false, true)] objects: bool,
    #[case] change: &str,
) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let mut fixture =
            create_backend_relation_fixture(&backend, &prefix("relation_cursor")).await;
        add_edge(&backend, &mut fixture, objects).await;
        let user = user(&backend, &fixture).await;
        let other = create_backend_user(&backend, &prefix("other_token")).await;
        let permissions = policy(&fixture, objects, None, false);
        permissions.add_rule(MockAllowRule {
            group_id: fixture.owner_group.id().id(),
            action: if objects {
                Permissions::ReadObject
            } else {
                Permissions::ReadClass
            },
            resource_kind: if objects {
                ResourceKind::Object
            } else {
                ResourceKind::Class
            },
            resource_id: None,
            attrs: ResourceFields::default(),
        });
        let app = test::init_service(
            App::new()
                .app_data(Data::new(AppContext::new(backend.clone(), permissions)))
                .configure(crate::api::config),
        )
        .await;
        let at = Utc::now();
        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&uri(
                    &fixture,
                    objects,
                    Some(at),
                    "limit=1&include_total=false",
                ))
                .insert_header((
                    http::header::AUTHORIZATION,
                    format!("Bearer {}", user.raw_token),
                ))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), http::StatusCode::OK);
        let cursor = response
            .headers()
            .get(NEXT_CURSOR_HEADER)
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        let params = format!(
            "limit=1&include_total=false&cursor={cursor}{}",
            if change == "order" {
                "&sort=history_id"
            } else {
                ""
            }
        );
        let selected = match change {
            "selection" => None,
            "instant" => Some(at + ChronoDuration::nanoseconds(1)),
            _ => Some(at),
        };
        let mut address = uri(&fixture, objects, selected, &params);
        if change == "root" {
            address = address.replacen(
                &format!("/classes/{}/", fixture.classes[0].id()),
                &format!("/classes/{}/", fixture.classes[1].id()),
                1,
            );
        }
        let token = if change == "token" {
            &other.raw_token
        } else {
            &user.raw_token
        };
        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&address)
                .insert_header((http::header::AUTHORIZATION, format!("Bearer {token}")))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), http::StatusCode::BAD_REQUEST);
        delete_backend_user(&backend, other).await;
        delete_backend_user(&backend, user).await;
        cleanup(&backend, fixture).await;
    }
}

#[rstest]
#[case::class(false)]
#[case::object(true)]
#[actix_web::test]
async fn relationship_local_policy_checks_moved_endpoint_collection(#[case] objects: bool) {
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let backend = environment.storage();
        let fixture = create_backend_relation_fixture(&backend, &prefix("relation_move")).await;
        let target =
            create_backend_object_fixture(&backend, &prefix("relation_private_target"), vec![])
                .await;
        backend
            .apply_local_collection_grant(StorageAuthorizationGrantMutation::new(
                StorageAuthorizationGrantKey::new(
                    fixture.collection.id(),
                    fixture.owner_group.id(),
                ),
                [
                    StorageAuthorizationPermission::ReadClass,
                    StorageAuthorizationPermission::ReadObject,
                    StorageAuthorizationPermission::ReadClassRelation,
                    StorageAuthorizationPermission::ReadObjectRelation,
                ],
                false,
                EventContext::system(),
            ))
            .await
            .unwrap()
            .into_value();
        let user = user(&backend, &fixture).await;
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
        let reader = HistoryReader::authorize(&context, &principal, None, root(&fixture, objects))
            .await
            .unwrap();
        let at = Utc::now();
        let (before, _) = reader
            .relationships(
                StorageRelationHistorySelection::At(at),
                parse_query_parameter("limit=1&include_total=false").unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            before.len(),
            1,
            "the edge must be visible before its endpoint moves"
        );
        if objects {
            match environment {
                BackendTestEnvironment::Postgres { ref pool } => {
                    hubuum_storage_postgres::test_support::relocate_object_history_fixture(
                        pool,
                        fixture.classes[1].id(),
                        fixture.objects[1].id(),
                        target.collection.id(),
                    )
                    .await
                    .unwrap()
                }
                BackendTestEnvironment::Memory { .. } => {
                    let object = backend
                        .object_store()
                        .get_object(fixture.objects[1].id())
                        .await
                        .unwrap();
                    move_class(&backend, fixture.classes[1].id(), target.collection.id()).await;
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
        } else {
            move_class(&backend, fixture.classes[1].id(), target.collection.id()).await;
        }
        let (rows, total) = reader
            .relationships(
                StorageRelationHistorySelection::At(at),
                parse_query_parameter("limit=1").unwrap(),
            )
            .await
            .unwrap();
        assert!(rows.is_empty());
        assert_eq!(total, 0);
        delete_backend_user(&backend, user).await;
        cleanup(&backend, fixture).await;
        delete_backend_object_fixture(&backend, target).await;
    }
}

#[rstest]
#[case::class(false)]
#[case::object(true)]
#[actix_web::test]
async fn relationship_token_must_cover_both_endpoints(#[case] objects: bool) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture = create_backend_relation_fixture(&backend, &prefix("relation_scope")).await;
        let user = user(&backend, &fixture).await;
        let context = AppContext::new(backend.clone(), policy(&fixture, objects, None, false));
        let scope = TokenScope::from_request_parts(
            None,
            Some(vec![TokenResourceScope::Class(fixture.classes[0].id())]),
        )
        .unwrap()
        .unwrap();
        let principal = backend
            .get_authentication_identity(user.principal_id)
            .await
            .unwrap()
            .into_parts()
            .0;
        let reader =
            HistoryReader::authorize(&context, &principal, Some(&scope), root(&fixture, objects))
                .await
                .unwrap();
        let (rows, total) = reader
            .relationships(
                StorageRelationHistorySelection::At(Utc::now()),
                parse_query_parameter("limit=1").unwrap(),
            )
            .await
            .unwrap();
        assert!(rows.is_empty());
        assert_eq!(total, 0);
        delete_backend_user(&backend, user).await;
        cleanup(&backend, fixture).await;
    }
}

#[rstest]
#[case::class(false)]
#[case::object(true)]
#[actix_web::test]
async fn relationship_pages_keep_authorized_order_across_many_edges(#[case] objects: bool) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let mut fixture =
            create_backend_relation_fixture(&backend, &prefix("relation_pages")).await;
        for _ in 0..19 {
            add_edge(&backend, &mut fixture, objects).await;
        }
        let user = user(&backend, &fixture).await;
        let permissions = policy(&fixture, objects, None, false);
        permissions.add_rule(MockAllowRule {
            group_id: fixture.owner_group.id().id(),
            action: if objects {
                Permissions::ReadObject
            } else {
                Permissions::ReadClass
            },
            resource_kind: if objects {
                ResourceKind::Object
            } else {
                ResourceKind::Class
            },
            resource_id: None,
            attrs: ResourceFields::default(),
        });
        let context = AppContext::new(backend.clone(), permissions);
        let principal = backend
            .get_authentication_identity(user.principal_id)
            .await
            .unwrap()
            .into_parts()
            .0;
        let reader = HistoryReader::authorize(&context, &principal, None, root(&fixture, objects))
            .await
            .unwrap();
        let mut options = parse_query_parameter("limit=3&include_total=false").unwrap();
        let selection = StorageRelationHistorySelection::At(Utc::now());
        let mut ids = Vec::new();
        loop {
            let (rows, total) = reader
                .relationships(selection, options.clone())
                .await
                .unwrap();
            assert_eq!(total, crate::pagination::SKIPPED_TOTAL_COUNT);
            let page = finalize_page(rows, &options).unwrap();
            assert!(page.items.len() <= 3);
            for row in page.items {
                assert!(matches!(
                    row.to,
                    HistoricalEndpointContext::Available { .. }
                ));
                ids.push(row.history_id);
            }
            let Some(cursor) = page.next_cursor else {
                break;
            };
            options.set_cursor(Some(cursor)).unwrap();
        }
        assert_eq!(ids.len(), 20);
        assert!(ids.windows(2).all(|pair| pair[0] > pair[1]));
        delete_backend_user(&backend, user).await;
        cleanup(&backend, fixture).await;
    }
}

#[rstest]
#[case::class(false)]
#[case::object(true)]
#[actix_web::test]
async fn postgres_relationship_hydration_has_constant_query_count(#[case] objects: bool) {
    use hubuum_storage_postgres::capture_queries;
    let _permit = postgres_permit().await;
    let backend = StorageHandle::postgres(pool().get_ref().clone());
    let mut fixture =
        create_backend_relation_fixture(&backend, &prefix("relation_query_budget")).await;
    for _ in 0..19 {
        add_edge(&backend, &mut fixture, objects).await;
    }
    let at = Utc::now();
    let run = |limit| {
        backend.list_relation_history(query(
            root(&fixture, objects),
            StorageRelationHistorySelection::At(at),
            &format!("limit={limit}&include_total=false"),
        ))
    };
    let (small, small_queries) = capture_queries(run(1)).await;
    let (large, large_queries) = capture_queries(run(20)).await;
    assert_eq!(small.unwrap().into_parts().0.len(), 1);
    assert_eq!(large.unwrap().into_parts().0.len(), 20);
    assert_eq!(small_queries.domain_queries(), 2);
    assert_eq!(large_queries.domain_queries(), 2);
    assert_eq!(large_queries.connection_checkouts(), 1);
    cleanup(&backend, fixture).await;
}
