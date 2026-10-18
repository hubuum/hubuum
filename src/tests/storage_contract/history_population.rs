use super::history_fixtures::*;
use super::*;
use crate::models::Permissions;
use crate::models::search::parse_query_parameter;
use crate::pagination::{NEXT_CURSOR_HEADER, TOTAL_COUNT_HEADER};
use crate::permissions::test_support::MockAllowRule;
use crate::permissions::{ResourceFields, ResourceKind};
use crate::services::history::class_objects_as_of;
use crate::storage::{StorageClassObjectsAsOfQuery, StorageObjectHistoryRecord};
use chrono::{DateTime, Duration as ChronoDuration, SecondsFormat, Utc};
use hubuum_query::{CursorValue, encode_cursor_values};
use rstest::rstest;
use serde_json::{Value, json};

fn query(class: ClassId, at: DateTime<Utc>, params: &str) -> StorageClassObjectsAsOfQuery {
    StorageClassObjectsAsOfQuery::try_new(class, at, parse_query_parameter(params).unwrap())
        .unwrap()
}

async fn versions(
    backend: &StorageHandle,
    class: ClassId,
    object: ObjectId,
) -> Vec<StorageObjectHistoryRecord> {
    let mut rows = backend
        .list_object_history(StorageObjectHistoryListQuery::new(
            object,
            class,
            prepare_db_pagination::<HubuumObjectHistory>(
                &parse_query_parameter("sort=revision&limit=20").unwrap(),
            )
            .unwrap(),
            StorageHistoryCollectionScope::All,
        ))
        .await
        .unwrap()
        .into_parts()
        .0;
    // Deletion tombstones may retain the preceding revision. History identities
    // order all transitions, independent of the older listing's default order.
    rows.sort_by_key(|row| row.clone().into_parts().1.into_parts().history_entry_id());
    rows
}

#[rstest]
#[case::before_create(0, -1, None)]
#[case::create(0, 0, Some(1))]
#[case::before_update(1, -1, Some(1))]
#[case::update(1, 0, Some(2))]
#[case::before_delete(2, -1, Some(2))]
#[case::delete(2, 0, None)]
#[actix_web::test]
async fn every_backend_population_uses_half_open_validity(
    #[case] boundary: usize,
    #[case] nanos: i64,
    #[case] expected: Option<i64>,
) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture =
            create_backend_object_fixture(&backend, &prefix("population_boundaries"), vec![]).await;
        let object = create_named(&backend, &fixture, "old").await;
        update(
            &backend,
            object.id(),
            StorageObjectUpdate::builder()
                .name(Some("new".into()))
                .data(Some(json!({"value": 2})))
                .build(),
        )
        .await;
        delete(&backend, object.id()).await;
        let history = versions(&backend, fixture.class.id(), object.id()).await;
        let at = history[boundary]
            .clone()
            .into_parts()
            .1
            .into_parts()
            .valid_from()
            + ChronoDuration::nanoseconds(nanos);
        let (rows, total) = backend
            .list_class_objects_as_of(
                query(fixture.class.id(), at, "limit=10")
                    .visibility(StorageHistoryCollectionScope::All, true),
            )
            .await
            .unwrap()
            .into_parts();
        assert_eq!(rows.len(), usize::from(expected.is_some()));
        assert_eq!(total, Some(rows.len() as i64));
        if let Some(revision) = expected {
            let (record, metadata) = rows.into_iter().next().unwrap().into_parts();
            assert_eq!(record.id(), object.id());
            assert_eq!(metadata.revision().get(), revision);
            assert_eq!(record.data(), &json!({"value": revision}));
        }
        delete_backend_object_fixture(&backend, fixture).await;
    }
}

#[rstest]
#[case::ordinary(false, 0)]
#[case::deleted_authority(true, 1)]
#[actix_web::test]
async fn every_backend_population_preserves_recreated_identity(
    #[case] include_deleted: bool,
    #[case] expected: usize,
) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let mut fixture =
            create_backend_object_fixture(&backend, &prefix("population_recreated"), vec![]).await;
        let old = create_named(&backend, &fixture, "same").await;
        delete(&backend, old.id()).await;
        let current = create_named(&backend, &fixture, "same").await;
        fixture.objects.push(current.clone());
        let at = versions(&backend, fixture.class.id(), old.id()).await[0]
            .clone()
            .into_parts()
            .1
            .into_parts()
            .valid_from();
        let rows = backend
            .list_class_objects_as_of(
                query(fixture.class.id(), at, "limit=10")
                    .visibility(StorageHistoryCollectionScope::All, include_deleted),
            )
            .await
            .unwrap()
            .into_parts()
            .0;
        assert_eq!(rows.len(), expected);
        if let Some(row) = rows.into_iter().next() {
            let record = row.into_parts().0;
            assert_eq!(record.id(), old.id());
            assert_ne!(record.id(), current.id());
        }
        delete_backend_object_fixture(&backend, fixture).await;
    }
}

#[rstest]
#[case::old_collection(true)]
#[case::new_collection(false)]
#[actix_web::test]
async fn every_backend_population_requires_current_and_historical_visibility(
    #[case] old_visible: bool,
) {
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let backend = environment.storage();
        let mut fixture =
            create_backend_object_fixture(&backend, &prefix("population_move"), vec![]).await;
        let target =
            create_backend_object_fixture(&backend, &prefix("population_target"), vec![]).await;
        let object = create_named(&backend, &fixture, "moving").await;
        let at = versions(&backend, fixture.class.id(), object.id()).await[0]
            .clone()
            .into_parts()
            .1
            .into_parts()
            .valid_from();
        relocate_object(&environment, object.id(), &fixture, &target).await;
        fixture.objects.push(object);
        let visible = if old_visible {
            fixture.collection.id()
        } else {
            target.collection.id()
        };
        let (rows, total) = backend
            .list_class_objects_as_of(
                query(fixture.class.id(), at, "limit=1")
                    .visibility(StorageHistoryCollectionScope::Visible(vec![visible]), false),
            )
            .await
            .unwrap()
            .into_parts();
        assert!(rows.is_empty());
        assert_eq!(total, Some(0));
        delete_backend_object_fixture(&backend, fixture).await;
        delete_backend_object_fixture(&backend, target).await;
    }
}

#[rstest]
#[case::old_revision("revision=1", 0)]
#[case::effective_revision("revision__gte=2", 1)]
#[actix_web::test]
async fn every_backend_population_filters_only_effective_versions(
    #[case] filter: &str,
    #[case] expected: usize,
) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture = create_backend_object_fixture(
            &backend,
            &prefix("population_revision"),
            vec![json!({})],
        )
        .await;
        update(
            &backend,
            fixture.objects[0].id(),
            StorageObjectUpdate::builder()
                .description(Some("revision 2".into()))
                .build(),
        )
        .await;
        let at = Utc::now();
        let (rows, total) = backend
            .list_class_objects_as_of(query(fixture.class.id(), at, &format!("limit=10&{filter}")))
            .await
            .unwrap()
            .into_parts();
        assert_eq!(rows.len(), expected);
        assert_eq!(total, Some(expected as i64));
        delete_backend_object_fixture(&backend, fixture).await;
    }
}

#[rstest]
#[case::ascending("history_id")]
#[case::descending("-history_id")]
#[case::revision_ties("revision")]
#[actix_web::test]
async fn every_backend_population_pages_effective_versions(
    #[case] sort: &str,
    #[values(false, true)] include_total: bool,
) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture = create_backend_object_fixture(
            &backend,
            &prefix("population_pages"),
            vec![json!({}); 5],
        )
        .await;
        let at = Utc::now();
        let mut query = query(
            fixture.class.id(),
            at,
            &format!("limit=2&sort={sort}&include_total={include_total}"),
        );
        let mut seen = HashSet::new();
        loop {
            let (rows, total) = backend
                .list_class_objects_as_of(query.clone())
                .await
                .unwrap()
                .into_parts();
            assert_eq!(total, include_total.then_some(5));
            if rows.is_empty() {
                break;
            }
            let values = rows.last().unwrap().clone().into_parts().1.into_parts();
            let cursor_values = query
                .options()
                .sort()
                .iter()
                .map(|sort| {
                    CursorValue::Integer(if sort.field == FilterField::Revision {
                        values.revision().get()
                    } else {
                        values.history_entry_id().id()
                    })
                })
                .collect();
            let cursor = encode_cursor_values(query.options().sort(), cursor_values).unwrap();
            for row in rows {
                assert!(seen.insert(row.into_parts().0.id()));
            }
            let mut options = query.options().clone();
            options.set_cursor(Some(cursor)).unwrap();
            query = query.with_options(options).unwrap();
        }
        assert_eq!(seen.len(), 5);
        delete_backend_object_fixture(&backend, fixture).await;
    }
}

fn uri(class: ClassId, at: DateTime<Utc>, params: &str) -> String {
    format!(
        "/api/v1/classes/{class}/objects/history/as-of?at={}&{params}",
        at.to_rfc3339_opts(SecondsFormat::Nanos, true)
    )
}

fn permissions(
    fixture: &BackendObjectFixture,
    name: Option<&str>,
) -> Arc<crate::permissions::test_support::MockTreetopBackend> {
    let permissions = external_permissions(fixture, name, false);
    permissions.add_rule(MockAllowRule {
        group_id: fixture.owned_group.as_ref().unwrap().id().id(),
        action: Permissions::ReadClass,
        resource_kind: ResourceKind::Class,
        resource_id: Some(fixture.class.id().id()),
        attrs: ResourceFields::default(),
    });
    permissions
}

#[rstest]
#[case::historical_name("old")]
#[case::current_name("new")]
#[actix_web::test]
async fn population_external_policy_checks_both_object_shapes(#[case] allowed_name: &str) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let mut fixture =
            create_backend_object_fixture(&backend, &prefix("population_policy"), vec![]).await;
        let object = create_named(&backend, &fixture, "old").await;
        let at = Utc::now();
        fixture.objects.push(
            update(
                &backend,
                object.id(),
                StorageObjectUpdate::builder()
                    .name(Some("new".into()))
                    .build(),
            )
            .await,
        );
        let user = owner_user(&backend, &fixture).await;
        let context = AppContext::new(backend.clone(), permissions(&fixture, Some(allowed_name)));
        let principal = backend
            .get_authentication_identity(user.principal_id)
            .await
            .unwrap()
            .into_parts()
            .0;
        let (rows, total) = class_objects_as_of(
            &context,
            &principal,
            None,
            query(fixture.class.id(), at, "limit=1"),
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
#[case::instant("at")]
#[case::class("class")]
#[case::order("sort")]
#[case::filter("filter")]
#[case::token("token")]
#[actix_web::test]
async fn population_api_binds_continuation_to_selection(#[case] change: &str) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture = create_backend_object_fixture(
            &backend,
            &prefix("population_cursor"),
            vec![json!({}); 3],
        )
        .await;
        let user = owner_user(&backend, &fixture).await;
        let other = owner_user(&backend, &fixture).await;
        let at = Utc::now();
        let app = test::init_service(
            App::new()
                .app_data(Data::new(AppContext::new(
                    backend.clone(),
                    permissions(&fixture, None),
                )))
                .configure(crate::api::config),
        )
        .await;
        let address = uri(fixture.class.id(), at, "limit=1&include_total=false");
        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&address)
                .insert_header((
                    http::header::AUTHORIZATION,
                    format!("Bearer {}", user.raw_token),
                ))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), http::StatusCode::OK);
        assert!(response.headers().get(TOTAL_COUNT_HEADER).is_none());
        let cursor = response
            .headers()
            .get(NEXT_CURSOR_HEADER)
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        let changed = match change {
            "at" => uri(
                fixture.class.id(),
                at - ChronoDuration::nanoseconds(1),
                "limit=1&include_total=false",
            ),
            "class" => uri(
                ClassId::new(i32::MAX).unwrap(),
                at,
                "limit=1&include_total=false",
            ),
            "sort" => format!("{address}&sort=history_id"),
            "filter" => format!("{address}&revision=1"),
            _ => address,
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
#[case::retained(false)]
#[case::unavailable(true)]
#[actix_web::test]
async fn population_api_returns_matching_class_context(#[case] before_history: bool) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture = create_backend_object_fixture(
            &backend,
            &prefix("population_context"),
            vec![json!({"old": true})],
        )
        .await;
        let at = if before_history {
            DateTime::from_timestamp(0, 0).unwrap()
        } else {
            Utc::now()
        };
        let class = backend
            .class_store()
            .resolve_class(StorageClassSelector::Id(fixture.class.id()))
            .await
            .unwrap();
        backend
            .class_store()
            .update_class(
                &class,
                StorageClassUpdate::builder()
                    .name(Some(prefix("renamed")))
                    .build(),
                &EventContext::system(),
            )
            .await
            .unwrap()
            .into_value();
        update(
            &backend,
            fixture.objects[0].id(),
            StorageObjectUpdate::builder()
                .data(Some(json!({"new": true})))
                .build(),
        )
        .await;
        let user = owner_user(&backend, &fixture).await;
        let app = test::init_service(
            App::new()
                .app_data(Data::new(AppContext::new(
                    backend.clone(),
                    permissions(&fixture, None),
                )))
                .configure(crate::api::config),
        )
        .await;
        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&uri(fixture.class.id(), at, "limit=10"))
                .insert_header((
                    http::header::AUTHORIZATION,
                    format!("Bearer {}", user.raw_token),
                ))
                .to_request(),
        )
        .await;
        if before_history {
            assert_eq!(response.status(), http::StatusCode::NOT_FOUND);
        } else {
            assert_eq!(response.status(), http::StatusCode::OK);
            let body: Value = test::read_body_json(response).await;
            assert_eq!(body["coverage"], "retained_history");
            assert_eq!(body["class"]["name"], fixture.class.name());
            assert_eq!(body["objects"][0]["data"], json!({"old": true}));
        }
        delete_backend_user(&backend, user).await;
        delete_backend_object_fixture(&backend, fixture).await;
    }
}

#[rstest]
#[case::ordinary(false, false, 0)]
#[case::administrator(true, false, 1)]
#[case::scoped_administrator(true, true, 0)]
#[actix_web::test]
async fn population_service_preserves_deleted_resource_policy(
    #[case] admin: bool,
    #[case] scoped: bool,
    #[case] expected: usize,
    #[values(false, true)] include_total: bool,
) {
    use crate::models::TokenScope;
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture =
            create_backend_object_fixture(&backend, &prefix("population_deleted_policy"), vec![])
                .await;
        let object = create_named(&backend, &fixture, "deleted").await;
        let at = Utc::now();
        delete(&backend, object.id()).await;
        let user = owner_user(&backend, &fixture).await;
        let principal = backend
            .get_authentication_identity(user.principal_id)
            .await
            .unwrap()
            .into_parts()
            .0;
        let scope = scoped.then(|| {
            TokenScope::from_request_parts(Some(vec![Permissions::ReadObject]), None)
                .unwrap()
                .unwrap()
        });
        let context = AppContext::new(backend.clone(), external_permissions(&fixture, None, admin));
        let (rows, total) = class_objects_as_of(
            &context,
            &principal,
            scope.as_ref(),
            query(
                fixture.class.id(),
                at,
                &format!("limit=1&include_total={include_total}"),
            ),
        )
        .await
        .unwrap();
        assert_eq!(rows.len(), expected);
        assert_eq!(
            total,
            if include_total {
                expected as i64
            } else {
                crate::pagination::SKIPPED_TOTAL_COUNT
            }
        );
        delete_backend_user(&backend, user).await;
        delete_backend_object_fixture(&backend, fixture).await;
    }
}

#[rstest]
#[case::missing("")]
#[case::invalid("at=bad")]
#[case::duplicate("at=2026-01-01T00:00:00Z&at=2026-01-01T00:00:00Z")]
#[case::live_filter("at=2026-01-01T00:00:00Z&name=live")]
#[case::live_sort("at=2026-01-01T00:00:00Z&sort=name")]
#[case::computed("at=2026-01-01T00:00:00Z&include=computed")]
#[actix_web::test]
async fn population_api_rejects_invalid_or_live_queries(#[case] params: &str) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture =
            create_backend_object_fixture(&backend, &prefix("population_invalid"), vec![]).await;
        let user = owner_user(&backend, &fixture).await;
        let app = test::init_service(
            App::new()
                .app_data(Data::new(AppContext::new(
                    backend.clone(),
                    permissions(&fixture, None),
                )))
                .configure(crate::api::config),
        )
        .await;
        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&format!(
                    "/api/v1/classes/{}/objects/history/as-of?{params}",
                    fixture.class.id()
                ))
                .insert_header((
                    http::header::AUTHORIZATION,
                    format!("Bearer {}", user.raw_token),
                ))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), http::StatusCode::BAD_REQUEST);
        delete_backend_user(&backend, user).await;
        delete_backend_object_fixture(&backend, fixture).await;
    }
}

#[rstest]
#[case::old_name("old")]
#[case::new_name("new")]
#[actix_web::test]
async fn population_api_requires_both_class_shapes(#[case] allowed_name: &str) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture =
            create_backend_object_fixture(&backend, &prefix("population_class_auth"), vec![]).await;
        let at = Utc::now();
        let class = backend
            .class_store()
            .resolve_class(StorageClassSelector::Id(fixture.class.id()))
            .await
            .unwrap();
        let new_name = prefix("renamed_class");
        backend
            .class_store()
            .update_class(
                &class,
                StorageClassUpdate::builder()
                    .name(Some(new_name.clone()))
                    .build(),
                &EventContext::system(),
            )
            .await
            .unwrap()
            .into_value();
        let user = owner_user(&backend, &fixture).await;
        let policy = external_permissions(&fixture, None, false);
        policy.add_rule(MockAllowRule {
            group_id: fixture.owned_group.as_ref().unwrap().id().id(),
            action: Permissions::ReadClass,
            resource_kind: ResourceKind::Class,
            resource_id: Some(fixture.class.id().id()),
            attrs: ResourceFields {
                name: Some(if allowed_name == "old" {
                    fixture.class.name().to_owned()
                } else {
                    new_name
                }),
                ..Default::default()
            },
        });
        let app = test::init_service(
            App::new()
                .app_data(Data::new(AppContext::new(backend.clone(), policy)))
                .configure(crate::api::config),
        )
        .await;
        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&uri(fixture.class.id(), at, "limit=1"))
                .insert_header((
                    http::header::AUTHORIZATION,
                    format!("Bearer {}", user.raw_token),
                ))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), http::StatusCode::FORBIDDEN);
        delete_backend_user(&backend, user).await;
        delete_backend_object_fixture(&backend, fixture).await;
    }
}

#[rstest]
#[case::source_before(false, -1, 1)]
#[case::source_at(false, 0, 0)]
#[case::target_before(true, -1, 0)]
#[case::target_at(true, 0, 1)]
#[actix_web::test]
async fn every_backend_population_uses_historical_class_membership(
    #[case] target_class: bool,
    #[case] nanos: i64,
    #[case] expected: usize,
) {
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let backend = environment.storage();
        let mut source =
            create_backend_object_fixture(&backend, &prefix("population_source_class"), vec![])
                .await;
        let target =
            create_backend_object_fixture(&backend, &prefix("population_target_class"), vec![])
                .await;
        let object = create_named(&backend, &source, "moving").await;
        super::history_fixtures::reclassify_object(&environment, object.id(), &target).await;
        let boundary = versions(&backend, target.class.id(), object.id()).await[0]
            .clone()
            .into_parts()
            .1
            .into_parts()
            .valid_from();
        source.objects.push(object.clone());
        let class_id = if target_class {
            target.class.id()
        } else {
            source.class.id()
        };
        let (rows, total) = backend
            .list_class_objects_as_of(query(
                class_id,
                boundary + ChronoDuration::nanoseconds(nanos),
                "limit=10",
            ))
            .await
            .unwrap()
            .into_parts();
        assert_eq!(rows.len(), expected);
        assert_eq!(total, Some(expected as i64));
        if let Some(row) = rows.into_iter().next() {
            let (record, _) = row.into_parts();
            assert_eq!(record.class_id(), class_id);
            assert_eq!(record.id(), object.id());
        }
        delete_backend_object_fixture(&backend, source).await;
        delete_backend_object_fixture(&backend, target).await;
    }
}
