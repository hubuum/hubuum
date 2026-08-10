use std::sync::{Arc, LazyLock};

use actix_web::web::Data;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::events::{EventFanoutSettings, EventRetentionSettings};
use crate::models::TokenRetentionSettings;
use crate::models::identity::LOCAL_IDENTITY_SCOPE;
use crate::models::search::QueryOptions;
use crate::models::{
    CollectionHistory, CollectionID, ExportTemplateHistory, HubuumClassHistory,
    HubuumObjectHistory, NewHubuumClass, NewHubuumObject, RemoteTargetHistory,
};
use crate::pagination::prepare_db_pagination;
use crate::services::Services;
use crate::storage::StorageHandle;
use crate::storage::postgres::PostgresPool;
use crate::storage::{
    AuthenticationStorage, AuthenticationTokenScopeQuery, AuthorizationCollectionAccessQuery,
    AuthorizationCollectionGrantListQuery, AuthorizationCollectionsQuery, AuthorizationGrantKey,
    AuthorizationGrantMutation, AuthorizationGroupMembershipQuery, AuthorizationPermission,
    AuthorizationStorage, EventArchive, EventDeliveryStorage, EventFanoutStorage,
    EventHealthStorage, EventRetentionStorage, HistoryAsOfQuery, HistoryCollectionScope,
    HistoryListQuery, HistoryStorage, MetricsStorage, ObjectHistoryAsOfQuery,
    ObjectHistoryListQuery, OperationalStateStorage, RetainedEvent, STORAGE_CONTRACT_VERSION,
    StorageBackendKind, StorageError, TokenRetentionStorage, UnifiedSearchQuery,
    UnifiedSearchStorage, UnifiedSearchVisibility,
};

#[derive(Clone, Copy, Debug)]
pub(crate) enum LifecycleContractImplementation {
    MemoryModel,
    PostgresAdapter,
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_metrics_snapshots() {
    let _permit = postgres_permit().await;

    for kind in StorageBackendKind::ALL {
        match kind {
            StorageBackendKind::Postgresql => {
                let backend = StorageHandle::postgres(pool().get_ref().clone());

                let pool_state = backend.metrics_pool_state();
                assert!(pool_state.max_connections > 0);
                backend
                    .metrics_inventory_snapshot()
                    .await
                    .expect("certified backend should supply inventory metrics");
                backend
                    .metrics_task_snapshot()
                    .await
                    .expect("certified backend should supply task metrics");
                backend
                    .metrics_event_snapshot()
                    .await
                    .expect("certified backend should supply event metrics");
            }
        }
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_authentication_projections() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let user = crate::tests::create_user_with_params(
        pool.get_ref(),
        &prefix("authentication_user"),
        "testpassword",
    )
    .await;

    for kind in StorageBackendKind::ALL {
        match kind {
            StorageBackendKind::Postgresql => {
                let backend = StorageHandle::postgres(pool.get_ref().clone());
                let identity = backend
                    .load_authentication_identity(user.id)
                    .await
                    .expect("certified backend should supply authentication identity data");
                let (principal, human) = identity.into_parts();

                assert_eq!(principal.id(), user.id);
                assert!(principal.is_human());
                assert!(human.is_some());

                let scope = backend
                    .load_authentication_token_scope(AuthenticationTokenScopeQuery::new(
                        i32::MAX,
                        true,
                        false,
                    ))
                    .await
                    .expect("certified backend should preserve empty scope dimensions")
                    .expect("an enabled scope dimension should produce a scope snapshot");
                let (permissions, resources) = scope.into_parts();
                assert_eq!(permissions, Some(Vec::new()));
                assert_eq!(resources, None);
            }
        }
    }

    user.delete_without_events(pool.get_ref())
        .await
        .expect("authentication compatibility fixture should be removed");
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_local_authorization_data() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let user = crate::tests::create_user_with_params(
        pool.get_ref(),
        &prefix("authorization_user"),
        "testpassword",
    )
    .await;
    let group = crate::tests::create_test_group(pool.get_ref()).await;
    group
        .add_member_without_events(pool.get_ref(), &user)
        .await
        .expect("authorization compatibility membership should be created");
    let fixture = crate::tests::create_collection_fixture(
        pool.get_ref(),
        &prefix("authorization_collection"),
    )
    .await;

    for kind in StorageBackendKind::ALL {
        match kind {
            StorageBackendKind::Postgresql => {
                let backend = StorageHandle::postgres(pool.get_ref().clone());
                let principal = backend
                    .load_authorization_principal(user.id)
                    .await
                    .expect("certified backend should supply authorization principal facts");
                assert!(principal.group_ids().contains(&group.id));

                let membership = AuthorizationGroupMembershipQuery::new(
                    user.id,
                    &group.groupname,
                    LOCAL_IDENTITY_SCOPE,
                );
                assert!(
                    backend
                        .authorization_principal_is_group_member(membership)
                        .await
                        .expect("certified backend should query group membership")
                );

                let access_query = || {
                    AuthorizationCollectionAccessQuery::new(
                        user.id,
                        fixture.collection.id,
                        [AuthorizationPermission::ReadCollection],
                    )
                };
                assert!(
                    !backend
                        .authorize_local_collection(access_query())
                        .await
                        .expect("missing local grant should deny")
                );

                let key = AuthorizationGrantKey::new(fixture.collection.id, group.id);
                backend
                    .apply_local_collection_grant(AuthorizationGrantMutation::new(
                        key,
                        [AuthorizationPermission::ReadCollection],
                        false,
                    ))
                    .await
                    .expect("certified backend should apply a local grant");
                let grant = backend
                    .get_local_collection_grant(key)
                    .await
                    .expect("certified backend should load a local grant")
                    .expect("applied local grant should exist");
                assert!(
                    grant
                        .permissions()
                        .contains(&AuthorizationPermission::ReadCollection)
                );
                assert!(
                    backend
                        .authorize_local_collection(access_query())
                        .await
                        .expect("applied local grant should authorize")
                );

                let collections = backend
                    .local_authorized_collections(AuthorizationCollectionsQuery::new(
                        user.id,
                        [AuthorizationPermission::ReadCollection],
                    ))
                    .await
                    .expect("certified backend should run reverse authorization queries");
                assert!(
                    collections
                        .iter()
                        .any(|collection| collection.id() == fixture.collection.id)
                );

                let page = backend
                    .list_local_collection_grants(AuthorizationCollectionGrantListQuery::new(
                        fixture.collection.id,
                        [AuthorizationPermission::ReadCollection],
                        QueryOptions {
                            filters: Vec::new(),
                            sort: Vec::new(),
                            limit: None,
                            cursor: None,
                            include_total: true,
                        },
                    ))
                    .await
                    .expect("certified backend should list local grants");
                let (items, total_count) = page.into_parts();
                assert!(total_count >= 1);
                assert!(!items.is_empty());

                let collection_candidates = backend
                    .list_authorization_collection_candidates()
                    .await
                    .expect("certified backend should list authorization collection candidates");
                assert!(
                    collection_candidates
                        .iter()
                        .any(|collection| collection.id() == fixture.collection.id)
                );

                let group_candidates = backend
                    .list_authorization_group_candidates(QueryOptions {
                        filters: Vec::new(),
                        sort: Vec::new(),
                        limit: None,
                        cursor: None,
                        include_total: false,
                    })
                    .await
                    .expect("certified backend should list authorization group candidates");
                assert!(
                    group_candidates
                        .iter()
                        .any(|candidate| candidate.id() == group.id)
                );

                let policy_snapshot = backend
                    .authorization_policy_snapshot()
                    .await
                    .expect("certified backend should supply the local policy snapshot");
                assert!(policy_snapshot.into_iter().any(|row| {
                    let (grant, snapshot_group, collection) = row.into_parts();
                    grant.group_id() == group.id
                        && snapshot_group.id() == group.id
                        && collection.id() == fixture.collection.id
                }));

                backend
                    .revoke_local_collection_grant(AuthorizationGrantMutation::new(
                        key,
                        [AuthorizationPermission::ReadCollection],
                        false,
                    ))
                    .await
                    .expect("certified backend should revoke selected local permissions");
                assert!(
                    !backend
                        .authorize_local_collection(access_query())
                        .await
                        .expect("revoked local grant should deny")
                );
                backend
                    .revoke_all_local_collection_grants(key)
                    .await
                    .expect("certified backend should remove the local grant row");
            }
        }
    }

    group
        .remove_member_without_events(&user, pool.get_ref())
        .await
        .expect("authorization compatibility membership should be removed");
    group
        .delete_without_events(pool.get_ref())
        .await
        .expect("authorization compatibility group should be removed");
    fixture
        .cleanup()
        .await
        .expect("authorization compatibility collection should be removed");
    user.delete_without_events(pool.get_ref())
        .await
        .expect("authorization compatibility user should be removed");
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_complete_temporal_history() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let fixture =
        crate::tests::create_collection_fixture(pool.get_ref(), &prefix("history_collection"))
            .await;
    let actor_name = prefix("history_actor");
    let actor =
        crate::tests::create_user_with_params(pool.get_ref(), &actor_name, "testpassword").await;
    let at = chrono::Utc::now();

    for kind in StorageBackendKind::ALL {
        match kind {
            StorageBackendKind::Postgresql => {
                let backend = StorageHandle::postgres(pool.get_ref().clone());
                let collection_options = prepare_db_pagination::<CollectionHistory>(
                    &crate::models::search::parse_query_parameter("limit=10")
                        .expect("history compatibility query should parse"),
                )
                .expect("collection history pagination should prepare");
                let collection_page = backend
                    .list_collection_history(HistoryListQuery::new(
                        fixture.collection.id,
                        collection_options,
                        HistoryCollectionScope::All,
                    ))
                    .await
                    .expect("certified backend should list collection history");
                let (collection_rows, total_count) = collection_page.into_parts();
                assert!(!collection_rows.is_empty());
                assert!(total_count >= 1);
                assert!(
                    backend
                        .collection_history_as_of(HistoryAsOfQuery::new(fixture.collection.id, at,))
                        .await
                        .expect("certified backend should load collection history as of a point")
                        .is_some()
                );

                let class_options = prepare_db_pagination::<HubuumClassHistory>(
                    &crate::models::search::parse_query_parameter("limit=10")
                        .expect("history compatibility query should parse"),
                )
                .expect("class history pagination should prepare");
                backend
                    .list_class_history(HistoryListQuery::new(
                        i32::MAX,
                        class_options,
                        HistoryCollectionScope::All,
                    ))
                    .await
                    .expect("certified backend should list class history");
                assert!(
                    backend
                        .class_history_as_of(HistoryAsOfQuery::new(i32::MAX, at))
                        .await
                        .expect("certified backend should query class history as of a point")
                        .is_none()
                );

                let object_options = prepare_db_pagination::<HubuumObjectHistory>(
                    &crate::models::search::parse_query_parameter("limit=10")
                        .expect("history compatibility query should parse"),
                )
                .expect("object history pagination should prepare");
                backend
                    .list_object_history(ObjectHistoryListQuery::new(
                        i32::MAX,
                        i32::MAX,
                        object_options,
                        HistoryCollectionScope::All,
                    ))
                    .await
                    .expect("certified backend should list object history");
                assert!(
                    backend
                        .object_history_as_of(ObjectHistoryAsOfQuery::new(i32::MAX, i32::MAX, at))
                        .await
                        .expect("certified backend should query object history as of a point")
                        .is_none()
                );

                let template_options = prepare_db_pagination::<ExportTemplateHistory>(
                    &crate::models::search::parse_query_parameter("limit=10")
                        .expect("history compatibility query should parse"),
                )
                .expect("template history pagination should prepare");
                backend
                    .list_export_template_history(HistoryListQuery::new(
                        i32::MAX,
                        template_options,
                        HistoryCollectionScope::All,
                    ))
                    .await
                    .expect("certified backend should list template history");
                assert!(
                    backend
                        .export_template_history_as_of(HistoryAsOfQuery::new(i32::MAX, at))
                        .await
                        .expect("certified backend should query template history as of a point")
                        .is_none()
                );

                let remote_target_options = prepare_db_pagination::<RemoteTargetHistory>(
                    &crate::models::search::parse_query_parameter("limit=10")
                        .expect("history compatibility query should parse"),
                )
                .expect("remote-target history pagination should prepare");
                backend
                    .list_remote_target_history(HistoryListQuery::new(
                        i32::MAX,
                        remote_target_options,
                        HistoryCollectionScope::All,
                    ))
                    .await
                    .expect("certified backend should list remote-target history");
                assert!(
                    backend
                        .remote_target_history_as_of(HistoryAsOfQuery::new(i32::MAX, at))
                        .await
                        .expect(
                            "certified backend should query remote-target history as of a point"
                        )
                        .is_none()
                );

                let names = backend
                    .resolve_history_principal_names(vec![actor.id])
                    .await
                    .expect("certified backend should resolve history principal names");
                assert!(names.into_iter().any(|row| {
                    let (principal_id, name) = row.into_parts();
                    principal_id == actor.id && name == actor_name
                }));
            }
        }
    }

    fixture
        .cleanup()
        .await
        .expect("history compatibility collection should be removed");
    actor
        .delete_without_events(pool.get_ref())
        .await
        .expect("history compatibility actor should be removed");
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_ranked_unified_search() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let needle = prefix("unified_search");
    let collection = crate::tests::create_collection_fixture(pool.get_ref(), &needle).await;
    let fixture = crate::tests::create_object_fixture(
        pool.get_ref(),
        collection,
        NewHubuumClass {
            name: format!("{needle}_class"),
            collection_id: 0,
            json_schema: Some(serde_json::json!({"title": needle})),
            validate_schema: Some(false),
            description: "unified search compatibility class".to_string(),
        },
        vec![NewHubuumObject {
            name: format!("{needle}_object"),
            collection_id: 0,
            hubuum_class_id: 0,
            data: serde_json::json!({"needle": needle}),
            description: "unified search compatibility object".to_string(),
        }],
    )
    .await
    .expect("unified search compatibility fixture should be created");

    for kind in StorageBackendKind::ALL {
        match kind {
            StorageBackendKind::Postgresql => {
                let backend = StorageHandle::postgres(pool.get_ref().clone());
                let request = || {
                    UnifiedSearchQuery::new(
                        needle.clone(),
                        10,
                        UnifiedSearchVisibility::new(
                            i32::MAX,
                            true,
                            None::<Vec<AuthorizationPermission>>,
                            None,
                        ),
                    )
                    .search_extended_document(true)
                };

                let collections = backend
                    .search_unified_collections(request())
                    .await
                    .expect("certified backend should search collections");
                assert!(collections.into_iter().any(|row| {
                    let (id, ..) = row.into_parts();
                    id == fixture.collection.collection.id
                }));

                let classes = backend
                    .search_unified_classes(request())
                    .await
                    .expect("certified backend should search classes");
                assert!(classes.into_iter().any(|row| {
                    let (id, ..) = row.into_parts();
                    id == fixture.class.id
                }));

                let objects = backend
                    .search_unified_objects(request())
                    .await
                    .expect("certified backend should search objects");
                assert!(objects.into_iter().any(|row| {
                    let (id, ..) = row.into_parts();
                    id == fixture.objects[0].id
                }));
            }
        }
    }

    fixture
        .cleanup()
        .await
        .expect("unified search compatibility fixture should be removed");
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_operational_state() {
    let _permit = postgres_permit().await;

    for kind in StorageBackendKind::ALL {
        match kind {
            StorageBackendKind::Postgresql => {
                let backend = StorageHandle::postgres(pool().get_ref().clone());
                let state = backend
                    .maintenance_state()
                    .await
                    .expect("certified backend should expose maintenance state");
                let readiness = backend
                    .readiness_snapshot()
                    .await
                    .expect("certified backend should expose readiness state");

                assert_eq!(readiness.maintenance_state(), state);
                assert!(readiness.schema_is_ready());
            }
        }
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_event_health() {
    let _permit = postgres_permit().await;

    for kind in StorageBackendKind::ALL {
        match kind {
            StorageBackendKind::Postgresql => {
                let backend = StorageHandle::postgres(pool().get_ref().clone());
                backend
                    .event_delivery_health()
                    .await
                    .expect("certified backend should expose event delivery health");
            }
        }
    }
}

#[actix_web::test]
async fn every_available_storage_backend_processes_event_fanout() {
    let _permit = postgres_permit().await;
    let settings = EventFanoutSettings::new(10, 30_000)
        .expect("compatibility fan-out settings should be valid");

    for kind in StorageBackendKind::ALL {
        match kind {
            StorageBackendKind::Postgresql => {
                let backend = StorageHandle::postgres(pool().get_ref().clone());
                backend
                    .process_event_fanout_batch(settings)
                    .await
                    .expect("certified backend should process event fan-out");
            }
        }
    }
}

#[actix_web::test]
async fn every_available_storage_backend_processes_event_retention() {
    struct DiscardArchive;

    impl EventArchive for DiscardArchive {
        fn archive(&self, _events: &[RetainedEvent]) -> Result<(), StorageError> {
            Ok(())
        }
    }

    let _permit = postgres_permit().await;
    let settings = EventRetentionSettings::new(10_000, 10_000, 10)
        .expect("compatibility event-retention settings should be valid");

    for kind in StorageBackendKind::ALL {
        match kind {
            StorageBackendKind::Postgresql => {
                let backend = StorageHandle::postgres(pool().get_ref().clone());
                let summary = backend
                    .process_event_retention_batch(settings, &DiscardArchive)
                    .await
                    .expect("certified backend should process event retention");

                assert!(!summary.did_work());
            }
        }
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_token_retention() {
    let _permit = postgres_permit().await;
    let settings = TokenRetentionSettings::builder()
        .retention_days(1_000_000)
        .token_lifetime_hours(24)
        .batch_size(10)
        .build()
        .expect("compatibility retention settings should be valid");

    for kind in StorageBackendKind::ALL {
        match kind {
            StorageBackendKind::Postgresql => {
                let backend = StorageHandle::postgres(pool().get_ref().clone());
                let purged = backend
                    .purge_expired_tokens(settings)
                    .await
                    .expect("certified backend should execute token retention");

                assert_eq!(purged, 0);
            }
        }
    }
}

#[actix_web::test]
async fn every_available_storage_backend_composes_through_the_complete_contract() {
    let _permit = postgres_permit().await;

    for kind in StorageBackendKind::ALL {
        match kind {
            StorageBackendKind::Postgresql => {
                let backend = StorageHandle::postgres(pool().get_ref().clone());
                fn accepts_event_delivery_contract(_backend: &impl EventDeliveryStorage) {}
                accepts_event_delivery_contract(&backend);
                let descriptor = backend.descriptor();
                assert_eq!(descriptor.kind(), kind);
                assert_eq!(descriptor.contract_version(), STORAGE_CONTRACT_VERSION);

                let services = Services::from_lifecycle_storage(backend.lifecycle_storage());
                let root = services
                    .collections()
                    .get(CollectionID::new(1).expect("valid root collection id"))
                    .await
                    .expect("certified backend should serve lifecycle operations");
                assert_eq!(root.id, 1);
            }
        }
    }
}

pub(crate) async fn postgres_permit() -> OwnedSemaphorePermit {
    static LIMITER: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(4)));
    LIMITER
        .clone()
        .acquire_owned()
        .await
        .expect("storage contract semaphore should remain open")
}

pub(crate) fn pool() -> Data<PostgresPool> {
    let config = crate::tests::integration_test_config()
        .expect("integration test config should be initialized");
    Data::new(crate::storage::postgres::init_postgres_pool(
        &config.database_url,
        2,
    ))
}

pub(crate) fn prefix(label: &str) -> String {
    let suffix = crate::utilities::auth::generate_random_password(12).to_ascii_lowercase();
    format!("storage_contract_{label}_{suffix}")
}
