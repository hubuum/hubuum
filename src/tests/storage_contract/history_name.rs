use super::*;
use crate::models::search::parse_query_parameter;
use crate::models::{HubuumObjectID, Permissions, TokenResourceScope, TokenScope};
use crate::pagination::{NEXT_CURSOR_HEADER, TOTAL_COUNT_HEADER};
use crate::permissions::test_support::{MockAllowRule, MockTreetopBackend};
use crate::permissions::{PermissionBackend, ResourceFields, ResourceKind};
use crate::services::history::object_history_by_name;
use crate::storage::{StorageHistoryOperation, StorageObjectHistoryNameQuery, StorageObjectUpdate};
use hubuum_query::{CursorValue, encode_cursor_values};
use rstest::rstest;
use serde_json::{Value, json};

async fn create_named(
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

async fn update(
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

async fn delete(backend: &StorageHandle, id: ObjectId) {
    let object = backend.object_store().get_object(id).await.unwrap();
    backend
        .object_store()
        .delete_object(&object, &EventContext::system())
        .await
        .unwrap()
        .into_value();
}

async fn move_class(backend: &StorageHandle, class_id: ClassId, collection_id: CollectionId) {
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

fn query(class: ClassId, name: &str, params: &str) -> StorageObjectHistoryNameQuery {
    StorageObjectHistoryNameQuery::try_new(
        class,
        name.to_string(),
        prepare_db_pagination::<HubuumObjectHistory>(&parse_query_parameter(params).unwrap())
            .unwrap(),
    )
    .unwrap()
}

#[rstest]
#[case("server01")]
#[case("123")]
#[case("Case, &/Ω")]
#[case("")]
#[actix_web::test]
async fn every_backend_name_history_spans_recreation_and_renames(#[case] name: &str) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let mut fixture =
            create_backend_object_fixture(&backend, &prefix("name_history"), vec![]).await;
        let mut expected = Vec::new();
        for _ in 0..2 {
            let object = create_named(&backend, &fixture, name).await;
            expected.extend([(object.id(), "create"), (object.id(), "delete")]);
            delete(&backend, object.id()).await;
        }
        let object = create_named(&backend, &fixture, name).await;
        update(
            &backend,
            object.id(),
            StorageObjectUpdate::builder()
                .name(Some("away".into()))
                .build(),
        )
        .await;
        let object = update(
            &backend,
            object.id(),
            StorageObjectUpdate::builder()
                .name(Some(name.into()))
                .build(),
        )
        .await;
        expected.extend([(object.id(), "create"), (object.id(), "update")]);
        fixture.objects.push(object);
        let other = create_backend_object_fixture(&backend, &prefix("other_class"), vec![]).await;
        let unrelated = create_named(&backend, &other, name).await;

        let (rows, total) = backend
            .list_object_history_by_name(
                query(fixture.class.id(), name, "sort=history_id&limit=20")
                    .visibility(StorageHistoryCollectionScope::All, true),
            )
            .await
            .unwrap()
            .into_parts();
        let actual = rows
            .into_iter()
            .map(|row| {
                let (record, metadata) = row.into_parts();
                let metadata = metadata.into_parts();
                assert_eq!(record.name(), name);
                assert_eq!(record.class_id(), fixture.class.id());
                if metadata.operation() == StorageHistoryOperation::Delete {
                    assert_eq!(metadata.valid_to(), Some(metadata.valid_from()));
                }
                (record.id(), metadata.operation().as_str())
            })
            .collect::<Vec<_>>();
        assert_eq!(total, Some(expected.len() as i64));
        assert_eq!(actual, expected);
        let identity_rows = backend
            .list_object_history(StorageObjectHistoryListQuery::new(
                fixture.objects[0].id(),
                fixture.class.id(),
                prepare_db_pagination::<HubuumObjectHistory>(
                    &parse_query_parameter("limit=20").unwrap(),
                )
                .unwrap(),
                StorageHistoryCollectionScope::All,
            ))
            .await
            .unwrap()
            .into_parts()
            .0;
        assert_eq!(identity_rows.len(), 3);
        assert!(
            identity_rows
                .into_iter()
                .any(|row| row.into_parts().0.name() == "away")
        );
        delete(&backend, unrelated.id()).await;
        delete_backend_object_fixture(&backend, other).await;
        delete_backend_object_fixture(&backend, fixture).await;
    }
}

#[rstest]
#[case("history_id", true)]
#[case("-history_id", false)]
#[case("revision", true)]
#[case("-revision", false)]
#[actix_web::test]
async fn every_backend_name_history_pages_across_identities(
    #[case] sort: &str,
    #[case] include_total: bool,
) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture =
            create_backend_object_fixture(&backend, &prefix("history_pages"), vec![]).await;
        for _ in 0..3 {
            let object = create_named(&backend, &fixture, "123").await;
            delete(&backend, object.id()).await;
        }
        let mut options = prepare_db_pagination::<HubuumObjectHistory>(
            &parse_query_parameter(&format!(
                "sort={sort}&limit=1&include_total={include_total}"
            ))
            .unwrap(),
        )
        .unwrap();
        // A storage page has an exact row limit; public look-ahead is tested below.
        options.set_limit(Some(2)).unwrap();
        let mut ids = Vec::new();
        for _ in 0..3 {
            let (rows, total) = backend
                .list_object_history_by_name(
                    StorageObjectHistoryNameQuery::try_new(
                        fixture.class.id(),
                        "123".into(),
                        options.clone(),
                    )
                    .unwrap()
                    .visibility(StorageHistoryCollectionScope::All, true),
                )
                .await
                .unwrap()
                .into_parts();
            assert_eq!(total, include_total.then_some(6));
            assert_eq!(rows.len(), 2);
            let parts = rows
                .into_iter()
                .map(|row| row.into_parts().1.into_parts())
                .collect::<Vec<_>>();
            ids.extend(
                parts
                    .iter()
                    .map(|metadata| metadata.history_entry_id().id()),
            );
            let last = parts.last().unwrap();
            let values = options
                .sort()
                .iter()
                .map(|sort| {
                    CursorValue::Integer(if sort.field == FilterField::Revision {
                        last.revision().get()
                    } else {
                        last.history_entry_id().id()
                    })
                })
                .collect();
            options
                .set_cursor(Some(encode_cursor_values(options.sort(), values).unwrap()))
                .unwrap();
        }
        assert_eq!(ids.iter().collect::<HashSet<_>>().len(), 6);
        assert!(ids.windows(2).all(|pair| if sort == "history_id" {
            pair[0] < pair[1]
        } else {
            pair[0] > pair[1]
        }));
        delete_backend_object_fixture(&backend, fixture).await;
    }
}

#[rstest]
#[case(false, 0)]
#[case(true, 2)]
#[actix_web::test]
async fn every_backend_name_history_handles_no_live_object(
    #[case] include_deleted: bool,
    #[case] expected: usize,
) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture =
            create_backend_object_fixture(&backend, &prefix("deleted_history"), vec![]).await;
        let object = create_named(&backend, &fixture, "server01").await;
        delete(&backend, object.id()).await;
        let (rows, total) = backend
            .list_object_history_by_name(
                query(fixture.class.id(), "server01", "limit=10")
                    .visibility(StorageHistoryCollectionScope::All, include_deleted),
            )
            .await
            .unwrap()
            .into_parts();
        assert_eq!(rows.len(), expected);
        assert_eq!(total, Some(expected as i64));
        delete_backend_object_fixture(&backend, fixture).await;
    }
}

async fn owner_user(backend: &StorageHandle, fixture: &BackendObjectFixture) -> BackendUserFixture {
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

#[rstest]
#[case::default("limit=10")]
#[case::revision_ties("sort=revision&limit=10")]
#[actix_web::test]
async fn every_backend_name_history_normalizes_storage_ordering(#[case] params: &str) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture =
            create_backend_object_fixture(&backend, &prefix("history_default_sort"), vec![]).await;
        for _ in 0..2 {
            let object = create_named(&backend, &fixture, "server01").await;
            delete(&backend, object.id()).await;
        }
        let query = StorageObjectHistoryNameQuery::try_new(
            fixture.class.id(),
            "server01".into(),
            parse_query_parameter(params).unwrap(),
        )
        .unwrap()
        .visibility(StorageHistoryCollectionScope::All, true);
        let ids = backend
            .list_object_history_by_name(query)
            .await
            .unwrap()
            .into_parts()
            .0
            .into_iter()
            .map(|row| row.into_parts().1.into_parts().history_entry_id().id())
            .collect::<Vec<_>>();
        assert_eq!(ids.len(), 4);
        assert!(ids.windows(2).all(|pair| pair[0] > pair[1]));
        delete_backend_object_fixture(&backend, fixture).await;
    }
}

fn external_permissions(
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

#[rstest]
#[case::historical_collection(false)]
#[case::current_collection(true)]
#[actix_web::test]
async fn every_backend_name_history_applies_both_collection_checks(#[case] deny_current: bool) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let mut fixture =
            create_backend_object_fixture(&backend, &prefix("history_collections"), vec![]).await;
        let target =
            create_backend_object_fixture(&backend, &prefix("history_target"), vec![]).await;
        let object = create_named(&backend, &fixture, "server01").await;
        move_class(&backend, fixture.class.id(), target.collection.id()).await;
        let object = update(
            &backend,
            object.id(),
            StorageObjectUpdate::builder()
                .collection_id(Some(target.collection.id()))
                .build(),
        )
        .await;
        fixture.objects.push(object);
        let visible = if deny_current {
            fixture.collection.id()
        } else {
            target.collection.id()
        };
        let (rows, total) = backend
            .list_object_history_by_name(
                query(fixture.class.id(), "server01", "limit=1")
                    .visibility(StorageHistoryCollectionScope::Visible(vec![visible]), false),
            )
            .await
            .unwrap()
            .into_parts();
        assert_eq!(total, Some(if deny_current { 0 } else { 1 }));
        assert_eq!(rows.len(), if deny_current { 0 } else { 1 });
        if let Some(row) = rows.into_iter().next() {
            assert_eq!(row.into_parts().0.collection_id(), target.collection.id());
        }
        delete_backend_object_fixture(&backend, fixture).await;
        delete_backend_object_fixture(&backend, target).await;
    }
}

#[rstest]
#[case::matching("revision=1", 1)]
#[case::range("revision__gt=1", 1)]
#[case::empty("revision=99", 0)]
#[actix_web::test]
async fn every_backend_name_history_filters_revisions_before_counting(
    #[case] filter: &str,
    #[case] expected: usize,
) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let mut fixture =
            create_backend_object_fixture(&backend, &prefix("history_revision"), vec![]).await;
        let old = create_named(&backend, &fixture, "server01").await;
        delete(&backend, old.id()).await;
        let current = create_named(&backend, &fixture, "server01").await;
        let current = update(
            &backend,
            current.id(),
            StorageObjectUpdate::builder()
                .description(Some("revision two".into()))
                .build(),
        )
        .await;
        fixture.objects.push(current);
        let (rows, total) = backend
            .list_object_history_by_name(
                query(
                    fixture.class.id(),
                    "server01",
                    &format!("{filter}&limit=10"),
                )
                .visibility(StorageHistoryCollectionScope::All, false),
            )
            .await
            .unwrap()
            .into_parts();
        // Only the current identity is visible: its create and update revisions.
        assert_eq!(rows.len(), expected);
        assert_eq!(total, Some(expected as i64));
        delete_backend_object_fixture(&backend, fixture).await;
    }
}

#[rstest]
#[case::class("class")]
#[case::name("name")]
#[case::sort("sort")]
#[case::filter("filter")]
#[case::token("token")]
#[actix_web::test]
async fn name_history_api_rejects_cursor_reuse_for_other_queries(#[case] change: &str) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let mut fixture =
            create_backend_object_fixture(&backend, &prefix("history_cursor_scope"), vec![]).await;
        let current = create_named(&backend, &fixture, "123").await;
        update(
            &backend,
            current.id(),
            StorageObjectUpdate::builder()
                .description(Some("changed".into()))
                .build(),
        )
        .await;
        fixture.objects.push(current);
        let user = owner_user(&backend, &fixture).await;
        let other = owner_user(&backend, &fixture).await;
        let app = test::init_service(
            App::new()
                .app_data(Data::new(AppContext::new(
                    backend.clone(),
                    external_permissions(&fixture, None, false),
                )))
                .configure(crate::api::config),
        )
        .await;
        let uri = format!(
            "/api/v1/classes/{}/objects/history?name=123&limit=1",
            fixture.class.id()
        );
        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&uri)
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
            .unwrap();
        let changed = match change {
            "class" => uri.replace(&format!("/{}/", fixture.class.id()), "/2147483647/"),
            "name" => uri.replace("name=123", "name=124"),
            "sort" => format!("{uri}&sort=history_id"),
            "filter" => format!("{uri}&revision=1"),
            _ => uri.clone(),
        };
        let token = if change == "token" {
            &other.raw_token
        } else {
            &user.raw_token
        };
        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&format!("{changed}&cursor={cursor}"))
                .insert_header((http::header::AUTHORIZATION, format!("Bearer {token}")))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), http::StatusCode::BAD_REQUEST);
        delete_backend_user(&backend, other).await;
        delete_backend_user(&backend, user).await;
        delete_backend_object_fixture(&backend, fixture).await;
    }
}

#[rstest]
#[case::local(false)]
#[case::external(true)]
#[actix_web::test]
async fn name_history_api_hides_deleted_predecessors_in_rows_totals_and_cursors(
    #[case] external: bool,
    #[values(false, true)] include_total: bool,
) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let mut fixture =
            create_backend_object_fixture(&backend, &prefix("history_api"), vec![]).await;
        let old = create_named(&backend, &fixture, "123").await;
        delete(&backend, old.id()).await;
        let current = create_named(&backend, &fixture, "123").await;
        update(
            &backend,
            current.id(),
            StorageObjectUpdate::builder()
                .description(Some("changed".into()))
                .build(),
        )
        .await;
        fixture.objects.push(current.clone());
        let user = owner_user(&backend, &fixture).await;
        let permissions: Arc<dyn PermissionBackend> = if external {
            external_permissions(&fixture, None, false)
        } else {
            Arc::new(LocalPermissionBackend::new(
                backend.clone(),
                crate::tests::integration_test_config()
                    .unwrap()
                    .admin_groupname,
            ))
        };
        let app = test::init_service(
            App::new()
                .app_data(Data::new(AppContext::new(backend.clone(), permissions)))
                .configure(crate::api::config),
        )
        .await;
        let uri = format!(
            "/api/v1/classes/{}/objects/history?name=123&limit=1&sort=history_id&include_total={include_total}",
            fixture.class.id()
        );
        let mut cursor = None;
        for index in 0..2 {
            let suffix = cursor
                .as_ref()
                .map(|value| format!("&cursor={value}"))
                .unwrap_or_default();
            let response = test::call_service(
                &app,
                test::TestRequest::get()
                    .uri(&format!("{uri}{suffix}"))
                    .insert_header((
                        http::header::AUTHORIZATION,
                        format!("Bearer {}", user.raw_token),
                    ))
                    .to_request(),
            )
            .await;
            if response.status() != http::StatusCode::OK {
                let status = response.status();
                let body = test::read_body(response).await;
                panic!(
                    "page {index} returned {status}: {}",
                    String::from_utf8_lossy(&body)
                );
            }
            assert_eq!(
                response
                    .headers()
                    .get(TOTAL_COUNT_HEADER)
                    .map(|value| value.to_str().unwrap()),
                include_total.then_some("2")
            );
            cursor = response
                .headers()
                .get(NEXT_CURSOR_HEADER)
                .map(|value| value.to_str().unwrap().to_string());
            assert_eq!(cursor.is_some(), index == 0);
            let rows: Vec<Value> = test::read_body_json(response).await;
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0]["id"], current.id().id());
            assert_eq!(rows[0]["revision"], index + 1);
            assert!(rows[0].get("data").is_some());
            assert!(rows[0].get("provenance").is_some());
        }
        delete_backend_user(&backend, user).await;
        delete_backend_object_fixture(&backend, fixture).await;
    }
}

#[rstest]
#[case::current_denied("old")]
#[case::snapshot_denied("new")]
#[actix_web::test]
async fn name_history_external_policy_checks_current_and_snapshot_names(
    #[case] allowed_name: &str,
) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let mut fixture =
            create_backend_object_fixture(&backend, &prefix("history_policy"), vec![]).await;
        let object = create_named(&backend, &fixture, "old").await;
        update(
            &backend,
            object.id(),
            StorageObjectUpdate::builder()
                .name(Some("new".into()))
                .build(),
        )
        .await;
        fixture.objects.push(object);
        let user = owner_user(&backend, &fixture).await;
        let principal = backend
            .get_authentication_identity(user.principal_id)
            .await
            .unwrap()
            .into_parts()
            .0;
        let context = AppContext::new(
            backend.clone(),
            external_permissions(&fixture, Some(allowed_name), false),
        );
        let (rows, total) = object_history_by_name(
            &context,
            &principal,
            None,
            query(fixture.class.id(), "old", "limit=1"),
        )
        .await
        .unwrap();
        assert!(rows.is_empty());
        assert_eq!(total, 0);
        delete_backend_user(&backend, user).await;
        delete_backend_object_fixture(&backend, fixture).await;
    }
}

#[rstest]
#[case::unscoped_admin(false, 3)]
#[case::object_scoped_admin(true, 1)]
#[actix_web::test]
async fn name_history_deleted_access_respects_external_admin_and_token_scope(
    #[case] scoped: bool,
    #[case] expected: usize,
) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let mut fixture =
            create_backend_object_fixture(&backend, &prefix("history_scoped"), vec![]).await;
        let old = create_named(&backend, &fixture, "server01").await;
        delete(&backend, old.id()).await;
        let current = create_named(&backend, &fixture, "server01").await;
        let scope = scoped.then(|| {
            TokenScope::from_request_parts(
                Some(vec![Permissions::ReadObject]),
                Some(vec![TokenResourceScope::Object(
                    HubuumObjectID::new(current.id().id()).unwrap(),
                )]),
            )
            .unwrap()
        });
        fixture.objects.push(current);
        let user = owner_user(&backend, &fixture).await;
        let principal = backend
            .get_authentication_identity(user.principal_id)
            .await
            .unwrap()
            .into_parts()
            .0;
        let context = AppContext::new(backend.clone(), external_permissions(&fixture, None, true));
        let (rows, total) = object_history_by_name(
            &context,
            &principal,
            scope.flatten().as_ref(),
            query(fixture.class.id(), "server01", "limit=10"),
        )
        .await
        .unwrap();
        assert_eq!(rows.len(), expected);
        assert_eq!(total, expected as i64);
        delete_backend_user(&backend, user).await;
        delete_backend_object_fixture(&backend, fixture).await;
    }
}

#[rstest]
#[case::missing("", 400)]
#[case::duplicate("name=a&name=b", 400)]
#[case::nul("name=%00", 400)]
#[case::non_exact("name__contains=a", 400)]
#[case::unsupported_filter("name=a&description=b", 400)]
#[case::unsupported_sort("name=a&sort=name", 400)]
#[case::malformed_cursor("name=a&cursor=invalid", 400)]
#[case::empty_result("name=absent&include_total=false", 200)]
#[case::empty_name("name=", 200)]
#[actix_web::test]
async fn name_history_api_validates_queries_even_without_matches(
    #[case] params: &str,
    #[case] expected_status: u16,
) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture =
            create_backend_object_fixture(&backend, &prefix("history_empty"), vec![]).await;
        let user = owner_user(&backend, &fixture).await;
        let app = test::init_service(
            App::new()
                .app_data(Data::new(AppContext::new(
                    backend.clone(),
                    external_permissions(&fixture, None, false),
                )))
                .configure(crate::api::config),
        )
        .await;
        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&format!(
                    "/api/v1/classes/2147483647/objects/history?{params}"
                ))
                .insert_header((
                    http::header::AUTHORIZATION,
                    format!("Bearer {}", user.raw_token),
                ))
                .to_request(),
        )
        .await;
        assert_eq!(response.status().as_u16(), expected_status);
        if expected_status == 200 {
            assert!(response.headers().get(NEXT_CURSOR_HEADER).is_none());
            if params.contains("include_total=false") {
                assert!(response.headers().get(TOTAL_COUNT_HEADER).is_none());
            }
            let rows: Vec<Value> = test::read_body_json(response).await;
            assert!(rows.is_empty());
        }
        delete_backend_user(&backend, user).await;
        delete_backend_object_fixture(&backend, fixture).await;
    }
}

#[rstest]
#[case::local(false)]
#[case::external(true)]
#[actix_web::test]
async fn name_history_token_collection_scope_filters_historical_placement(#[case] external: bool) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let mut fixture =
            create_backend_object_fixture(&backend, &prefix("history_scope_source"), vec![]).await;
        let target =
            create_backend_object_fixture(&backend, &prefix("history_scope_target"), vec![]).await;
        let object = create_named(&backend, &fixture, "server01").await;
        move_class(&backend, fixture.class.id(), target.collection.id()).await;
        fixture.objects.push(
            update(
                &backend,
                object.id(),
                StorageObjectUpdate::builder()
                    .collection_id(Some(target.collection.id()))
                    .build(),
            )
            .await,
        );
        let user = owner_user(&backend, &fixture).await;
        backend
            .add_group_member(
                user.principal_id,
                target.owned_group.as_ref().unwrap().id(),
                &EventContext::system(),
            )
            .await
            .unwrap()
            .into_value();
        let principal = backend
            .get_authentication_identity(user.principal_id)
            .await
            .unwrap()
            .into_parts()
            .0;
        let permissions: Arc<dyn PermissionBackend> = if external {
            let permissions = external_permissions(&fixture, None, false);
            permissions.add_rule(MockAllowRule {
                group_id: target.owned_group.as_ref().unwrap().id().id(),
                action: Permissions::ReadObject,
                resource_kind: ResourceKind::Object,
                resource_id: None,
                attrs: ResourceFields {
                    collection_id: Some(target.collection.id().id()),
                    ..Default::default()
                },
            });
            permissions
        } else {
            Arc::new(LocalPermissionBackend::new(
                backend.clone(),
                crate::tests::integration_test_config()
                    .unwrap()
                    .admin_groupname,
            ))
        };
        let context = AppContext::new(backend.clone(), permissions);
        let scope = TokenScope::from_request_parts(
            Some(vec![Permissions::ReadObject]),
            Some(vec![TokenResourceScope::Collection(
                CollectionID::new(target.collection.id().id()).unwrap(),
            )]),
        )
        .unwrap();
        let (rows, total) = object_history_by_name(
            &context,
            &principal,
            scope.as_ref(),
            query(fixture.class.id(), "server01", "limit=1"),
        )
        .await
        .unwrap();
        assert_eq!(total, 1);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].collection_id, target.collection.id().id());
        delete_backend_user(&backend, user).await;
        delete_backend_object_fixture(&backend, fixture).await;
        delete_backend_object_fixture(&backend, target).await;
    }
}
