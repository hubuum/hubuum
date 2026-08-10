use std::sync::{Arc, LazyLock};

use actix_web::web::Data;
use async_trait::async_trait;
use diesel::{ExpressionMethods, QueryDsl};
use diesel_async::RunQueryDsl;
use hubuum_task_core::IdempotencyKey;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::events::{EventContext, EventFanoutSettings, EventRetentionSettings};
use crate::models::TokenRetentionSettings;
use crate::models::identity::LOCAL_IDENTITY_SCOPE;
use crate::models::search::{
    FilterField, ParsedQueryParam, QueryOptions, SearchOperator,
    parse_query_parameter_with_computed_filters_and_passthrough,
};
use crate::models::{
    CollectionHistory, CollectionID, ExportTemplateHistory, HubuumClassHistory,
    HubuumObjectHistory, NewComputedFieldDefinition, NewHubuumClass, NewHubuumClassRelation,
    NewHubuumObject, NewHubuumObjectRelation, RemoteTargetHistory,
};
use crate::pagination::prepare_db_pagination;
use crate::services::Services;
use crate::storage::StorageHandle;
use crate::storage::postgres::PostgresPool;
use crate::storage::{
    AuthenticationStorage, AuthenticationTokenScopeQuery, AuthorizationCollectionAccessQuery,
    AuthorizationCollectionGrantListQuery, AuthorizationCollectionsQuery, AuthorizationGrantKey,
    AuthorizationGrantMutation, AuthorizationGroupMembershipQuery, AuthorizationPermission,
    AuthorizationStorage, BidirectionalRelatedObjectsQuery, CatalogListQuery, CatalogStorage,
    ComputedFieldLifecycleStorage, ComputedObjectEnrichmentQuery, ComputedObjectListQuery,
    ComputedObjectProjection, ComputedObjectStorage, ComputedObjectVisibility, EventArchive,
    EventDeliveryStorage, EventFanoutStorage, EventHealthStorage, EventRetentionStorage,
    HistoryAsOfQuery, HistoryCollectionScope, HistoryListQuery, HistoryStorage, MetricsStorage,
    ObjectAggregateAuthorizationMode, ObjectAggregateAuthorizer, ObjectAggregateStorage,
    ObjectAggregateStorageQuery, ObjectHistoryAsOfQuery, ObjectHistoryListQuery,
    ObjectRelationsTouchingIdsQuery, OperationalStateStorage, RelatedObjectsForRootsQuery,
    RelationGraphQuery, RelationIdsQuery, RelationListQuery, RelationQueryStorage,
    RelationTouchingQuery, RetainedEvent, STORAGE_CONTRACT_VERSION, StorageBackendKind,
    StorageComputedFieldDefinitionInput, StorageComputedFieldDefinitionPatch,
    StorageComputedFieldRebuildRequest, StorageComputedFieldVisibility, StorageError,
    StorageObject, StorageObjectAggregateAuthorizationCandidate,
    StorageObjectAggregateAuthorizationTarget, StorageObjectAggregateSort,
    StorageObjectAggregateSpec, StorageObjectAggregateTarget, StoragePersonalComputedFieldCreate,
    StoragePersonalComputedFieldDelete, StoragePersonalComputedFieldListQuery,
    StoragePersonalComputedFieldUpdate, StorageRelatedDirection, StorageRelatedSort,
    StorageSharedComputedFieldCreate, StorageSharedComputedFieldDelete,
    StorageSharedComputedFieldUpdate, StorageTaskCreateRequest, StorageTaskKind,
    StorageTaskListQuery, StorageTaskOutputLookup, StorageTaskPageQuery, StorageTaskScopeSnapshot,
    StorageTaskStatus, StorageVisibility, TaskQueueStorage, TokenRetentionStorage,
    UnifiedSearchQuery, UnifiedSearchStorage,
};
use crate::traits::CanSave;

#[derive(Clone, Copy, Debug)]
pub(crate) enum LifecycleContractImplementation {
    MemoryModel,
    PostgresAdapter,
}

struct AllowAllObjectAggregateAuthorizer;

#[async_trait]
impl ObjectAggregateAuthorizer for AllowAllObjectAggregateAuthorizer {
    async fn authorize_target(
        &self,
        _target: StorageObjectAggregateAuthorizationTarget,
        _required_permissions: Vec<AuthorizationPermission>,
    ) -> Result<bool, StorageError> {
        Ok(true)
    }

    async fn authorize_objects(
        &self,
        candidates: Vec<StorageObjectAggregateAuthorizationCandidate>,
        _required_permissions: Vec<AuthorizationPermission>,
    ) -> Result<Vec<bool>, StorageError> {
        Ok(vec![true; candidates.len()])
    }
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
async fn every_available_storage_backend_supplies_the_complete_task_queue() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let user = crate::tests::create_user_with_params(
        pool.get_ref(),
        &prefix("task_queue_user"),
        "testpassword",
    )
    .await;
    let options = || QueryOptions {
        filters: Vec::new(),
        sort: Vec::new(),
        limit: Some(10),
        cursor: None,
        include_total: true,
    };

    for kind in StorageBackendKind::ALL {
        match kind {
            StorageBackendKind::Postgresql => {
                let backend = StorageHandle::postgres(pool.get_ref().clone());
                let task = backend
                    .create_task(
                        StorageTaskCreateRequest::builder(
                            StorageTaskKind::Import,
                            user.id,
                            serde_json::json!({"items": []}),
                            0,
                        )
                        .idempotency_key(Some(
                            IdempotencyKey::new(prefix("task_queue_key"))
                                .expect("compatibility idempotency key should be valid"),
                        ))
                        .request_hash(Some(prefix("task_queue_hash")))
                        .scope_snapshot(StorageTaskScopeSnapshot::unscoped())
                        .build(10),
                    )
                    .await
                    .expect("certified backend should create a task");
                let task_id = task.id();
                assert_eq!(task.kind(), StorageTaskKind::Import);
                assert_eq!(task.status(), StorageTaskStatus::Queued);

                let access = backend
                    .get_task_access(task_id)
                    .await
                    .expect("certified backend should return task access facts");
                assert_eq!(access.into_parts().0.id(), task_id);

                let (tasks, total) = backend
                    .list_tasks(StorageTaskListQuery::new(
                        Some(user.id),
                        Some(StorageTaskKind::Import),
                        Some(StorageTaskStatus::Queued),
                        options(),
                    ))
                    .await
                    .expect("certified backend should list tasks")
                    .into_parts();
                assert_eq!(total, Some(1));
                assert_eq!(tasks.len(), 1);

                let (events, event_total) = backend
                    .list_task_events(StorageTaskPageQuery::new(task_id, options()))
                    .await
                    .expect("certified backend should list task events")
                    .into_parts();
                assert_eq!(event_total, Some(1));
                assert_eq!(events.len(), 1);

                let (results, result_total) = backend
                    .list_import_task_results(StorageTaskPageQuery::new(task_id, options()))
                    .await
                    .expect("certified backend should list import results")
                    .into_parts();
                assert_eq!(result_total, Some(0));
                assert!(results.is_empty());

                assert!(
                    backend
                        .list_export_output_summaries(vec![task_id])
                        .await
                        .expect("certified backend should list export output summaries")
                        .is_empty()
                );
                assert!(
                    backend
                        .list_backup_output_summaries(vec![task_id])
                        .await
                        .expect("certified backend should list backup output summaries")
                        .is_empty()
                );
                assert!(matches!(
                    backend.get_export_output_summary(task_id).await,
                    Ok(StorageTaskOutputLookup::Missing)
                ));
                assert!(matches!(
                    backend.get_backup_output_summary(task_id).await,
                    Ok(StorageTaskOutputLookup::Missing)
                ));
                assert!(matches!(
                    backend.get_export_output(task_id).await,
                    Ok(StorageTaskOutputLookup::Missing)
                ));
                assert!(matches!(
                    backend.get_backup_output(task_id).await,
                    Ok(StorageTaskOutputLookup::Missing)
                ));

                crate::storage::postgres::with_transaction(
                    pool.get_ref(),
                    async |conn| -> Result<(), crate::errors::ApiError> {
                        use crate::schema::tasks::dsl::{id, tasks};
                        diesel::delete(tasks.filter(id.eq(task_id)))
                            .execute(conn)
                            .await?;
                        Ok(())
                    },
                )
                .await
                .expect("task queue compatibility fixture should be removed");
            }
        }
    }

    user.delete_without_events(pool.get_ref())
        .await
        .expect("task queue compatibility user should be removed");
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
async fn every_available_storage_backend_supplies_catalog_queries() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let needle = prefix("catalog_query");
    let collection = crate::tests::create_collection_fixture(pool.get_ref(), &needle).await;
    let fixture = crate::tests::create_object_fixture(
        pool.get_ref(),
        collection,
        NewHubuumClass {
            name: format!("{needle}_class"),
            collection_id: 0,
            json_schema: None,
            validate_schema: Some(false),
            description: "catalog compatibility class".to_string(),
        },
        vec![NewHubuumObject {
            name: format!("{needle}_object"),
            collection_id: 0,
            hubuum_class_id: 0,
            data: serde_json::json!({"needle": needle}),
            description: "catalog compatibility object".to_string(),
        }],
    )
    .await
    .expect("catalog compatibility fixture should be created");

    for kind in StorageBackendKind::ALL {
        match kind {
            StorageBackendKind::Postgresql => {
                let backend = StorageHandle::postgres(pool.get_ref().clone());
                let request = || {
                    CatalogListQuery::new(
                        QueryOptions {
                            filters: vec![ParsedQueryParam {
                                field: FilterField::Name,
                                operator: SearchOperator::Contains { is_negated: false },
                                value: needle.clone(),
                            }],
                            sort: Vec::new(),
                            limit: Some(10),
                            cursor: None,
                            include_total: true,
                        },
                        StorageVisibility::new(
                            i32::MAX,
                            true,
                            None::<Vec<AuthorizationPermission>>,
                            None,
                        ),
                    )
                };

                let (collections, collection_total) = backend
                    .list_collections(request())
                    .await
                    .expect("certified backend should list collections")
                    .into_parts();
                assert_eq!(collection_total, Some(1));
                assert!(collections.into_iter().any(|row| {
                    let (id, ..) = row.into_parts();
                    id == fixture.collection.collection.id
                }));

                let (classes, class_total) = backend
                    .list_classes(request())
                    .await
                    .expect("certified backend should list classes")
                    .into_parts();
                assert_eq!(class_total, Some(1));
                assert!(classes.into_iter().any(|row| {
                    let (id, ..) = row.into_parts();
                    id == fixture.class.id
                }));

                let (objects, object_total) = backend
                    .list_objects(request())
                    .await
                    .expect("certified backend should list objects")
                    .into_parts();
                assert_eq!(object_total, Some(1));
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
        .expect("catalog compatibility fixture should be removed");
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_computed_object_queries() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let needle = prefix("computed_object_query");
    let collection = crate::tests::create_collection_fixture(pool.get_ref(), &needle).await;
    let fixture = crate::tests::create_object_fixture(
        pool.get_ref(),
        collection,
        NewHubuumClass {
            name: format!("{needle}_class"),
            collection_id: 0,
            json_schema: None,
            validate_schema: Some(false),
            description: "computed-object compatibility class".to_string(),
        },
        vec![NewHubuumObject {
            name: format!("{needle}_object"),
            collection_id: 0,
            hubuum_class_id: 0,
            data: serde_json::json!({"compatibility": needle}),
            description: "computed-object compatibility object".to_string(),
        }],
    )
    .await
    .expect("computed-object compatibility fixture should be created");
    crate::storage::postgres::with_connection(pool.get_ref(), async |connection| {
        diesel::insert_into(crate::schema::computed_field_definitions::table)
            .values(NewComputedFieldDefinition {
                class_id: fixture.class.id,
                visibility: "shared".to_string(),
                owner_user_id: None,
                key: "compatibility".to_string(),
                label: "Compatibility".to_string(),
                description: String::new(),
                operation: serde_json::json!({
                    "type": "first_non_null",
                    "paths": ["/compatibility"]
                }),
                result_type: "string".to_string(),
                enabled: true,
                semantics_version: 1,
                created_by: None,
                updated_by: None,
            })
            .execute(connection)
            .await
    })
    .await
    .expect("computed-object compatibility definition should be inserted");

    for kind in StorageBackendKind::ALL {
        match kind {
            StorageBackendKind::Postgresql => {
                let backend = StorageHandle::postgres(pool.get_ref().clone());
                let (options, passthrough) =
                    parse_query_parameter_with_computed_filters_and_passthrough(
                        &format!("computed.shared.compatibility__equals={needle}&sort=id"),
                        &[],
                    )
                    .expect("computed compatibility query should parse");
                assert!(passthrough.is_empty());
                let visibility = StorageVisibility::new(
                    i32::MAX,
                    true,
                    None::<Vec<AuthorizationPermission>>,
                    None,
                );
                let (rows, total, computed, _) = backend
                    .list_computed_objects(ComputedObjectListQuery::new(
                        fixture.class.id,
                        None,
                        options,
                        ComputedObjectVisibility::storage(visibility),
                        ComputedObjectProjection::All,
                    ))
                    .await
                    .expect("certified backend should query computed objects")
                    .into_parts();
                assert_eq!(total, Some(1));
                assert_eq!(rows.len(), 1);
                assert_eq!(computed.len(), 1);

                let object = &fixture.objects[0];
                let enriched = backend
                    .enrich_objects_with_computed(ComputedObjectEnrichmentQuery::new(
                        vec![StorageObject::new(
                            object.id,
                            object.name.clone(),
                            object.collection_id,
                            object.hubuum_class_id,
                            object.data.clone(),
                            object.description.clone(),
                            object.created_at,
                            object.updated_at,
                            object.revision.get(),
                        )],
                        None,
                    ))
                    .await
                    .expect("certified backend should enrich objects with computed values");
                assert_eq!(enriched.len(), 1);
            }
        }
    }

    fixture
        .cleanup()
        .await
        .expect("computed-object compatibility fixture should be removed");
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_computed_field_lifecycle() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let needle = prefix("computed_field_lifecycle");
    let owner = crate::tests::create_test_user(pool.get_ref()).await;
    let fixture = crate::tests::create_class_fixture(
        pool.get_ref(),
        crate::tests::create_collection_fixture(pool.get_ref(), &needle).await,
        vec![NewHubuumClass {
            name: format!("{needle}_class"),
            collection_id: 0,
            json_schema: None,
            validate_schema: Some(false),
            description: "computed-field lifecycle compatibility class".to_string(),
        }],
    )
    .await
    .expect("computed-field lifecycle compatibility fixture should be created");
    let class_id = fixture.classes[0].id;
    let collection_id = fixture.collection.collection.id;
    let event_context = EventContext::user(owner.id, None, None);
    let definition = |key: &str| {
        StorageComputedFieldDefinitionInput::new(
            key.to_string(),
            "Compatibility".to_string(),
            serde_json::json!({
                "type": "first_non_null",
                "paths": ["/compatibility"]
            }),
            "string".to_string(),
        )
        .with_description("Backend compatibility definition".to_string())
    };

    for kind in StorageBackendKind::ALL {
        match kind {
            StorageBackendKind::Postgresql => {
                let backend = StorageHandle::postgres(pool.get_ref().clone());

                let initial_state = backend
                    .computed_field_state(class_id)
                    .await
                    .expect("certified backend should supply computed-field state");
                assert_eq!(initial_state.class_id(), class_id);

                let (shared, created_state) = backend
                    .create_shared_computed_field(StorageSharedComputedFieldCreate::new(
                        class_id,
                        collection_id,
                        owner.id,
                        definition("compatibility_shared"),
                        event_context.clone(),
                    ))
                    .await
                    .expect("certified backend should create a shared computed field")
                    .into_parts();
                assert_eq!(shared.visibility(), StorageComputedFieldVisibility::Shared);
                assert!(created_state.evaluation_revision() > initial_state.evaluation_revision());

                let shared_rows = backend
                    .list_shared_computed_fields(class_id)
                    .await
                    .expect("certified backend should list shared computed fields");
                assert!(
                    shared_rows
                        .iter()
                        .any(|row| row.metadata().id() == shared.metadata().id())
                );

                let loaded = backend
                    .get_computed_field(shared.metadata().id())
                    .await
                    .expect("certified backend should load a computed field");
                assert_eq!(loaded.key(), "compatibility_shared");

                let (updated_shared, _) = backend
                    .update_shared_computed_field(StorageSharedComputedFieldUpdate::new(
                        class_id,
                        collection_id,
                        shared.metadata().id(),
                        owner.id,
                        StorageComputedFieldDefinitionPatch::new()
                            .with_label(Some("Updated compatibility".to_string())),
                        event_context.clone(),
                    ))
                    .await
                    .expect("certified backend should update a shared computed field")
                    .into_parts();
                assert_eq!(updated_shared.label(), "Updated compatibility");

                let rebuild_state = backend
                    .request_computed_field_rebuild(StorageComputedFieldRebuildRequest::new(
                        class_id,
                        collection_id,
                        Some(owner.id),
                    ))
                    .await
                    .expect("certified backend should request a computed-field rebuild");
                assert_eq!(rebuild_state.class_id(), class_id);

                let personal = backend
                    .create_personal_computed_field(StoragePersonalComputedFieldCreate::new(
                        class_id,
                        owner.id,
                        definition("compatibility_personal"),
                    ))
                    .await
                    .expect("certified backend should create a personal computed field");
                assert_eq!(
                    personal.visibility(),
                    StorageComputedFieldVisibility::Personal { owner_id: owner.id }
                );

                let (personal_rows, total) = backend
                    .list_personal_computed_fields(StoragePersonalComputedFieldListQuery::new(
                        owner.id,
                        Some(class_id),
                        QueryOptions {
                            filters: Vec::new(),
                            sort: Vec::new(),
                            limit: Some(10),
                            cursor: None,
                            include_total: true,
                        },
                    ))
                    .await
                    .expect("certified backend should list personal computed fields")
                    .into_parts();
                assert_eq!(total, Some(1));
                assert_eq!(personal_rows.len(), 1);

                let updated_personal = backend
                    .update_personal_computed_field(StoragePersonalComputedFieldUpdate::new(
                        owner.id,
                        personal.metadata().id(),
                        StorageComputedFieldDefinitionPatch::new()
                            .with_label(Some("Updated personal compatibility".to_string())),
                    ))
                    .await
                    .expect("certified backend should update a personal computed field");
                assert_eq!(updated_personal.label(), "Updated personal compatibility");

                backend
                    .delete_personal_computed_field(StoragePersonalComputedFieldDelete::new(
                        owner.id,
                        personal.metadata().id(),
                    ))
                    .await
                    .expect("certified backend should delete a personal computed field");

                let deleted_state = backend
                    .delete_shared_computed_field(StorageSharedComputedFieldDelete::new(
                        class_id,
                        collection_id,
                        shared.metadata().id(),
                        owner.id,
                        event_context.clone(),
                    ))
                    .await
                    .expect("certified backend should delete a shared computed field");
                assert_eq!(deleted_state.class_id(), class_id);
            }
        }
    }

    fixture
        .cleanup()
        .await
        .expect("computed-field lifecycle fixture should be removed");
    owner
        .delete_without_events(pool.get_ref())
        .await
        .expect("computed-field lifecycle owner should be removed");
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_object_aggregates() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let needle = prefix("object_aggregate");
    let collection = crate::tests::create_collection_fixture(pool.get_ref(), &needle).await;
    let fixture = crate::tests::create_object_fixture(
        pool.get_ref(),
        collection,
        NewHubuumClass {
            name: format!("{needle}_class"),
            collection_id: 0,
            json_schema: None,
            validate_schema: Some(false),
            description: "object-aggregate compatibility class".to_string(),
        },
        vec![NewHubuumObject {
            name: format!("{needle}_object"),
            collection_id: 0,
            hubuum_class_id: 0,
            data: serde_json::json!({"compatibility": true}),
            description: "object-aggregate compatibility object".to_string(),
        }],
    )
    .await
    .expect("object-aggregate compatibility fixture should be created");
    let visibility =
        || StorageVisibility::new(i32::MAX, true, None::<Vec<AuthorizationPermission>>, None);
    let query = |mode| {
        ObjectAggregateStorageQuery::builder(
            StorageObjectAggregateTarget::new(
                fixture.class.id,
                fixture.class.name.clone(),
                fixture.class.collection_id,
            ),
            QueryOptions {
                filters: vec![
                    ParsedQueryParam {
                        field: FilterField::ClassId,
                        operator: SearchOperator::Equals { is_negated: false },
                        value: fixture.class.id.to_string(),
                    },
                    ParsedQueryParam {
                        field: FilterField::CollectionId,
                        operator: SearchOperator::Equals { is_negated: false },
                        value: fixture.class.collection_id.to_string(),
                    },
                ],
                sort: Vec::new(),
                limit: Some(50),
                cursor: None,
                include_total: true,
            },
            StorageObjectAggregateSpec::new(
                ["name".to_string()],
                [],
                StorageObjectAggregateSort::DimensionsAscending,
            ),
            visibility(),
        )
        .required_permissions([
            AuthorizationPermission::ReadObject,
            AuthorizationPermission::ReadCollection,
        ])
        .cursor_max_encoded_bytes(4_096)
        .authorization_mode(mode)
        .build()
        .expect("compatibility aggregate query should be valid")
    };

    for kind in StorageBackendKind::ALL {
        match kind {
            StorageBackendKind::Postgresql => {
                let backend = StorageHandle::postgres(pool.get_ref().clone());
                let storage_page = backend
                    .aggregate_objects(query(ObjectAggregateAuthorizationMode::Storage), None)
                    .await
                    .expect("certified backend should aggregate with storage authorization");
                let (rows, total, next_cursor) = storage_page.into_parts();
                assert_eq!(rows.len(), 1);
                assert_eq!(total, Some(1));
                assert!(next_cursor.is_none());

                let delegated_page = backend
                    .aggregate_objects(
                        query(ObjectAggregateAuthorizationMode::Delegated),
                        Some(&AllowAllObjectAggregateAuthorizer),
                    )
                    .await
                    .expect("certified backend should aggregate with delegated authorization");
                let (rows, total, next_cursor) = delegated_page.into_parts();
                assert_eq!(rows.len(), 1);
                assert_eq!(total, Some(1));
                assert!(next_cursor.is_none());
            }
        }
    }

    fixture
        .cleanup()
        .await
        .expect("object-aggregate compatibility fixture should be removed");
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_relation_queries() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let needle = prefix("relation_query");
    let collection = crate::tests::create_collection_fixture(pool.get_ref(), &needle).await;
    let class_one = NewHubuumClass {
        name: format!("{needle}_class_one"),
        collection_id: collection.collection.id,
        json_schema: None,
        validate_schema: Some(false),
        description: "relation compatibility source class".to_string(),
    }
    .save_without_events(pool.get_ref())
    .await
    .expect("source class should be created");
    let class_two = NewHubuumClass {
        name: format!("{needle}_class_two"),
        collection_id: collection.collection.id,
        json_schema: None,
        validate_schema: Some(false),
        description: "relation compatibility target class".to_string(),
    }
    .save_without_events(pool.get_ref())
    .await
    .expect("target class should be created");
    let class_relation = NewHubuumClassRelation {
        from_hubuum_class_id: class_one.id,
        to_hubuum_class_id: class_two.id,
        forward_template_alias: None,
        reverse_template_alias: None,
        from_max_relations: None,
        to_max_relations: None,
    }
    .save_without_events(pool.get_ref())
    .await
    .expect("class relation should be created");
    let object_one = NewHubuumObject {
        name: format!("{needle}_object_one"),
        collection_id: collection.collection.id,
        hubuum_class_id: class_one.id,
        data: serde_json::json!({}),
        description: "relation compatibility source object".to_string(),
    }
    .save_without_events(pool.get_ref())
    .await
    .expect("source object should be created");
    let object_two = NewHubuumObject {
        name: format!("{needle}_object_two"),
        collection_id: collection.collection.id,
        hubuum_class_id: class_two.id,
        data: serde_json::json!({}),
        description: "relation compatibility target object".to_string(),
    }
    .save_without_events(pool.get_ref())
    .await
    .expect("target object should be created");
    let object_relation = NewHubuumObjectRelation {
        from_hubuum_object_id: object_one.id,
        to_hubuum_object_id: object_two.id,
        class_relation_id: class_relation.id,
    }
    .save_without_events(pool.get_ref())
    .await
    .expect("object relation should be created");

    for kind in StorageBackendKind::ALL {
        match kind {
            StorageBackendKind::Postgresql => {
                let backend = StorageHandle::postgres(pool.get_ref().clone());
                let visibility = || {
                    StorageVisibility::new(
                        i32::MAX,
                        true,
                        None::<Vec<AuthorizationPermission>>,
                        None,
                    )
                };
                let options = || QueryOptions {
                    filters: Vec::new(),
                    sort: Vec::new(),
                    limit: Some(50),
                    cursor: None,
                    include_total: true,
                };

                let (class_relations, class_total) = backend
                    .list_class_relations(RelationListQuery::new(options(), visibility()))
                    .await
                    .expect("certified backend should list class relations")
                    .into_parts();
                assert!(class_total.is_some_and(|total| total >= 1));
                assert!(class_relations.into_iter().any(|row| {
                    let (id, ..) = row.into_parts();
                    id == class_relation.id
                }));

                let (object_relations, object_total) = backend
                    .list_object_relations(RelationListQuery::new(options(), visibility()))
                    .await
                    .expect("certified backend should list object relations")
                    .into_parts();
                assert!(object_total.is_some_and(|total| total >= 1));
                assert!(object_relations.into_iter().any(|row| {
                    let (id, ..) = row.into_parts();
                    id == object_relation.id
                }));

                let (touching_classes, _) = backend
                    .list_class_relations_touching(RelationTouchingQuery::new(
                        class_one.id,
                        options(),
                        visibility(),
                    ))
                    .await
                    .expect("certified backend should list class relations touching an id")
                    .into_parts();
                assert_eq!(touching_classes.len(), 1);

                let (touching_objects, _) = backend
                    .list_object_relations_touching(RelationTouchingQuery::new(
                        object_one.id,
                        options(),
                        visibility(),
                    ))
                    .await
                    .expect("certified backend should list object relations touching an id")
                    .into_parts();
                assert_eq!(touching_objects.len(), 1);

                let class_ids = [class_one.id, class_two.id];
                assert_eq!(
                    backend
                        .class_relations_touching_ids(RelationIdsQuery::new(
                            class_ids,
                            visibility(),
                        ))
                        .await
                        .expect("certified backend should query class relations touching ids")
                        .len(),
                    1
                );
                assert_eq!(
                    backend
                        .class_relations_between_ids(
                            RelationIdsQuery::new(class_ids, visibility(),)
                        )
                        .await
                        .expect("certified backend should query class relations between ids")
                        .len(),
                    1
                );

                let object_ids = [object_one.id, object_two.id];
                assert_eq!(
                    backend
                        .object_relations_touching_ids(ObjectRelationsTouchingIdsQuery::new(
                            [object_one.id],
                            10,
                            visibility(),
                        ))
                        .await
                        .expect("certified backend should query object relations touching ids")
                        .len(),
                    1
                );
                assert!(
                    backend
                        .object_relations_touching_ids(
                            ObjectRelationsTouchingIdsQuery::new(
                                [object_one.id],
                                10,
                                visibility(),
                            )
                            .excluding_relation_ids([object_relation.id]),
                        )
                        .await
                        .expect("certified backend should exclude previously visited relations")
                        .is_empty()
                );
                assert_eq!(
                    backend
                        .object_relations_between_ids(RelationIdsQuery::new(
                            object_ids,
                            visibility(),
                        ))
                        .await
                        .expect("certified backend should query object relations between ids")
                        .len(),
                    1
                );

                let (related_classes, _) = backend
                    .related_classes(RelationGraphQuery::new(
                        class_one.id,
                        options(),
                        visibility(),
                    ))
                    .await
                    .expect("certified backend should traverse related classes")
                    .into_parts();
                assert!(!related_classes.is_empty());

                let (related_objects, _) = backend
                    .related_objects(RelationGraphQuery::new(
                        object_one.id,
                        options(),
                        visibility(),
                    ))
                    .await
                    .expect("certified backend should traverse related objects")
                    .into_parts();
                assert!(!related_objects.is_empty());

                let included = backend
                    .related_objects_for_roots(
                        RelatedObjectsForRootsQuery::new(
                            [object_one.id],
                            class_two.id,
                            visibility(),
                        )
                        .class_relation_id(Some(class_relation.id))
                        .direction(StorageRelatedDirection::Any)
                        .sort(StorageRelatedSort::Path)
                        .max_depth(1)
                        .limit(10),
                    )
                    .await
                    .expect("certified backend should traverse directional root graphs");
                assert_eq!(included.len(), 1);

                let bidirectional = backend
                    .bidirectionally_related_objects_for_roots(
                        BidirectionalRelatedObjectsQuery::new(
                            [object_one.id],
                            1,
                            10,
                            false,
                            visibility(),
                        ),
                    )
                    .await
                    .expect("certified backend should traverse bidirectional root graphs");
                assert_eq!(bidirectional.len(), 1);
            }
        }
    }

    collection
        .cleanup()
        .await
        .expect("relation compatibility collection should be removed");
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
                        StorageVisibility::new(
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
