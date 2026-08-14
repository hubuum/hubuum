use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};

use actix_web::{App, http, test, web::Data};
use async_trait::async_trait;
use diesel::{ExpressionMethods, QueryDsl};
use diesel_async::RunQueryDsl;
use hubuum_task_core::IdempotencyKey;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::events::{
    Action, EntityType, EventContext, EventFanoutSettings, EventRetentionSettings,
    MutationProvenance,
};
use crate::models::TokenRetentionSettings;
use crate::models::identity::LOCAL_IDENTITY_SCOPE;
use crate::models::search::{
    FilterField, ParsedQueryParam, QueryOptions, SearchOperator,
    parse_query_parameter_with_computed_filters_and_passthrough,
};
use crate::models::{
    CollectionHistory, CollectionID, CollectionKey, ExportTemplateHistory, GroupID,
    HubuumClassHistory, HubuumObjectHistory, ImportAtomicity, ImportClassInput,
    ImportCollectionInput, ImportMode, NewHubuumClass, NewHubuumClassRelation, NewHubuumObject,
    NewHubuumObjectRelation, RemoteTargetHistory,
};
use crate::pagination::prepare_db_pagination;
use crate::permissions::{AppContext, LocalPermissionBackend};
use crate::services::Services;
use crate::storage::StorageHandle;
use crate::storage::postgres::PostgresPool;
use crate::storage::postgres::operations::computed_field_rows::NewComputedFieldDefinitionRow as NewComputedFieldDefinition;
use crate::storage::{
    ApplicationImportOperation, AuditEventStorage, AuthenticationCredential, AuthenticationStorage,
    AuthenticationTokenScopeQuery, AuthorizationCollectionAccessQuery,
    AuthorizationCollectionGrantListQuery, AuthorizationCollectionGroupsPageQuery,
    AuthorizationCollectionGroupsQuery, AuthorizationCollectionVisibilityQuery,
    AuthorizationCollectionsAccessQuery, AuthorizationCollectionsQuery, AuthorizationGrantDelete,
    AuthorizationGrantKey, AuthorizationGrantMutation, AuthorizationGroupCollectionQuery,
    AuthorizationGroupMembershipQuery, AuthorizationPermission, AuthorizationPermissionSetQuery,
    AuthorizationPrincipalCollectionPageQuery, AuthorizationPrincipalCollectionQuery,
    AuthorizationResourceIds, AuthorizationStorage, BackupSnapshotStorage,
    BidirectionalRelatedObjectsQuery, CatalogListQuery, CatalogStorage,
    CollectionAuthorizationStorage, ComputedFieldLifecycleStorage, ComputedObjectEnrichmentQuery,
    ComputedObjectListQuery, ComputedObjectProjection, ComputedObjectStorage,
    ComputedObjectVisibility, EventArchive, EventDeliveryAdministrationStorage,
    EventDeliveryStorage, EventFanoutStorage, EventHealthStorage, EventRetentionStorage,
    EventSubscriptionStorage, ExportQueryStorage, ExportTemplateStorage, GroupStorage,
    HistoryAsOfQuery, HistoryCollectionScope, HistoryListQuery, HistoryStorage, IdentityStorage,
    ImportStorage, InventoryStorage, MetricsStorage, ObjectAggregateAuthorizationMode,
    ObjectAggregateAuthorizer, ObjectAggregateStorage, ObjectAggregateStorageQuery,
    ObjectHistoryAsOfQuery, ObjectHistoryListQuery, ObjectRelationsTouchingIdsQuery,
    OperationalStateStorage, PrincipalStorage, RelatedObjectsForRootsQuery, RelationGraphQuery,
    RelationIdsQuery, RelationListQuery, RelationQueryStorage, RelationTouchingQuery,
    RemoteTargetStorage, RestoreStorage, RetainedEvent, StorageAuditEventFilters,
    StorageAuditEventListQuery, StorageBackendKind, StorageBackupTaskArtifact, StorageCallSite,
    StorageCollectionCreate, StorageCollectionUpdate, StorageComputedFieldDefinitionInput,
    StorageComputedFieldDefinitionPatch, StorageComputedFieldRebuildRequest,
    StorageComputedFieldVisibility, StorageDefaultAdminBootstrap, StorageError, StorageErrorKind,
    StorageEventDeliveryListQuery, StorageEventSinkCreate, StorageEventSinkDelete,
    StorageEventSinkListQuery, StorageEventSinkUpdate, StorageEventSubscriptionCreate,
    StorageEventSubscriptionDelete, StorageEventSubscriptionListQuery,
    StorageEventSubscriptionUpdate, StorageExecution, StorageExportTaskArtifact,
    StorageExportTemplateCreate, StorageExportTemplateDefinition, StorageExportTemplateDelete,
    StorageExportTemplateListQuery, StorageExportTemplateReplace, StorageGroupCreate,
    StorageGroupListQuery, StorageGroupUpdate, StorageImportPlanItem, StorageImportResult,
    StorageLocalPasswordReset, StorageObject, StorageObjectAggregateAuthorizationCandidate,
    StorageObjectAggregateAuthorizationTarget, StorageObjectAggregateSort,
    StorageObjectAggregateSpec, StorageObjectAggregateTarget, StoragePersonalComputedFieldCreate,
    StoragePersonalComputedFieldDelete, StoragePersonalComputedFieldListQuery,
    StoragePersonalComputedFieldUpdate, StoragePrincipalGroupListQuery,
    StoragePrincipalSettingsMutation, StorageQueryBudget, StorageRecordMetadata,
    StorageRelatedDirection, StorageRelatedSort, StorageRemoteCallArtifactOutcome,
    StorageRemoteCallArtifactResponse, StorageRemoteCallArtifactTarget,
    StorageRemoteCallTaskArtifact, StorageRemoteTargetCreate, StorageRemoteTargetDefinition,
    StorageRemoteTargetDelete, StorageRemoteTargetInvocation, StorageRemoteTargetListQuery,
    StorageRemoteTargetPatch, StorageRemoteTargetPolicy, StorageRemoteTargetTransport,
    StorageRemoteTargetUpdate, StorageRestoreArtifactSummary, StorageRestoreFailure,
    StorageRestoreInitiator, StorageRestoreJobStatus, StorageRestoreStageCreate,
    StorageRevisionPrecondition, StorageServiceAccountCreate, StorageServiceAccountListQuery,
    StorageServiceAccountMutation, StorageServiceAccountUpdate, StorageSharedComputedFieldCreate,
    StorageSharedComputedFieldDelete, StorageSharedComputedFieldUpdate, StorageTaskClaimToken,
    StorageTaskCompletion, StorageTaskCompletionArtifact, StorageTaskCreateRequest,
    StorageTaskEventAppend, StorageTaskEventInput, StorageTaskFailure, StorageTaskKind,
    StorageTaskLease, StorageTaskLeaseDuration, StorageTaskListQuery, StorageTaskOutputLookup,
    StorageTaskPageQuery, StorageTaskResultCounts, StorageTaskScopeSnapshot,
    StorageTaskStateUpdate, StorageTaskStatus, StorageTokenCreate, StorageTokenHashRevoke,
    StorageTokenIssuancePolicy, StorageTokenListQuery, StorageTokenListState, StorageTokenRenew,
    StorageTokenRevoke, StorageUserCreate, StorageUserDelete, StorageUserListQuery,
    StorageUserPasswordUpdate, StorageUserUpdate, StorageVisibility, TaskExecutionStorage,
    TaskQueueStorage, TokenRetentionStorage, TokenStorage, UnifiedSearchQuery,
    UnifiedSearchStorage, UserStorage, WorkerNotificationStorage,
};
use crate::traits::{CanDelete, CanSave};

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

/// Construct every backend advertised by application composition.
///
/// Keeping the selector here makes the compatibility suite backend-agnostic:
/// adding a selectable backend requires one fixture construction change, and
/// every test below starts exercising it automatically.
fn available_backends() -> impl Iterator<Item = StorageHandle> {
    let postgres_pool = pool();
    StorageBackendKind::ALL.into_iter().map(move |kind| {
        let backend = backend_for_kind(kind, postgres_pool.get_ref());
        assert_eq!(backend.descriptor().kind(), kind);
        backend
    })
}

fn backend_for_kind(kind: StorageBackendKind, postgres_pool: &PostgresPool) -> StorageHandle {
    match kind {
        StorageBackendKind::Postgresql => StorageHandle::postgres(postgres_pool.clone()),
    }
}

struct BackendApplicationFixture {
    backend: StorageHandle,
    administrator: crate::models::User,
    bearer_token: String,
}

async fn backend_application_fixture(
    kind: StorageBackendKind,
    postgres_pool: &PostgresPool,
) -> BackendApplicationFixture {
    match kind {
        StorageBackendKind::Postgresql => {
            let administrator = crate::tests::create_test_admin(postgres_pool).await;
            let bearer_token = administrator
                .create_token(postgres_pool)
                .await
                .expect("backend compatibility administrator token should be created")
                .get_token();
            BackendApplicationFixture {
                backend: backend_for_kind(kind, postgres_pool),
                administrator,
                bearer_token,
            }
        }
    }
}

impl BackendApplicationFixture {
    async fn cleanup(self, postgres_pool: &PostgresPool) {
        self.administrator
            .delete_without_events(postgres_pool)
            .await
            .expect("backend compatibility administrator should be removed");
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_metrics_snapshots() {
    let _permit = postgres_permit().await;

    for backend in available_backends() {
        let pool_state = backend.metrics_pool_state();
        assert!(pool_state.capacity().max_connections() > 0);
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

#[actix_web::test]
async fn postgres_rolls_back_a_compound_collection_create_at_an_injected_failure() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let backend = StorageHandle::postgres(pool.get_ref().clone());
    let group = backend
        .create_group(
            StorageGroupCreate::new(
                None,
                prefix("collection_failpoint_group"),
                Some("collection rollback owner".to_string()),
            ),
            None,
        )
        .await
        .expect("collection rollback owner group should be created");
    let collection_name = prefix("collection_failpoint");
    let command = StorageCollectionCreate::new(
        collection_name.clone(),
        "must be rolled back",
        GroupID::new(group.id())
            .expect("fixture group id should be valid")
            .id(),
        Some(CollectionID::new(1).expect("root id should be valid").id()),
    );

    let error = crate::storage::postgres::with_failpoint(
        crate::storage::postgres::PostgresFailpoint::CollectionCreateAfterRecords,
        backend
            .collection_store()
            .create_collection(command, Some(&EventContext::system())),
    )
    .await
    .err()
    .expect("injected failure should abort collection creation");
    assert_eq!(error.kind(), StorageErrorKind::Database);

    let persisted = crate::storage::postgres::with_connection(pool.get_ref(), async |conn| {
        use crate::schema::collections::dsl::{collections, name};
        collections
            .filter(name.eq(&collection_name))
            .count()
            .get_result::<i64>(conn)
            .await
    })
    .await
    .expect("collection rollback should remain queryable");
    assert_eq!(persisted, 0, "all collection records must roll back");

    backend
        .delete_group(group.id(), None)
        .await
        .expect("collection rollback owner group should be removed");
}

#[actix_web::test]
async fn postgres_rolls_back_task_finalization_at_an_injected_failure() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let backend = StorageHandle::postgres(pool.get_ref().clone());
    let user = crate::tests::create_user_with_params(
        pool.get_ref(),
        &prefix("task_failpoint_user"),
        "testpassword",
    )
    .await;
    let task = backend
        .create_task(
            StorageTaskCreateRequest::builder(
                StorageTaskKind::Import,
                user.id,
                serde_json::json!({"failpoint": true}),
                1,
            )
            .idempotency_key(Some(
                IdempotencyKey::new(prefix("task_failpoint_key"))
                    .expect("failpoint idempotency key should be valid"),
            ))
            .request_hash(Some(prefix("task_failpoint_hash")))
            .scope_snapshot(StorageTaskScopeSnapshot::unscoped())
            .build(10),
        )
        .await
        .expect("task rollback fixture should be created");
    let claim_token = uuid::Uuid::new_v4();
    crate::storage::postgres::with_connection(pool.get_ref(), async |conn| {
        use crate::schema::tasks::dsl::{id, lease_expires_at, lease_token, status, tasks};
        diesel::update(tasks.filter(id.eq(task.id())))
            .set((
                status.eq(StorageTaskStatus::Validating.as_str()),
                lease_token.eq(Some(claim_token)),
                lease_expires_at.eq(Some(
                    chrono::Utc::now().naive_utc()
                        + chrono::Duration::try_minutes(1).expect("valid failpoint lease"),
                )),
            ))
            .execute(conn)
            .await
    })
    .await
    .expect("task rollback fixture should receive a live claim");
    let lease = StorageTaskLease::new(
        task.id(),
        StorageTaskClaimToken::new(claim_token.to_string()),
    );

    let error = crate::storage::postgres::with_failpoint(
        crate::storage::postgres::PostgresFailpoint::TaskFinalizeAfterEvent,
        backend.complete_task(StorageTaskCompletion::new(
            StorageTaskStateUpdate::new(
                lease,
                StorageTaskStatus::Succeeded,
                StorageTaskResultCounts::new(1, 1, 0),
            ),
            StorageTaskEventInput::new("succeeded", "Must be rolled back"),
        )),
    )
    .await
    .expect_err("injected failure should abort task finalization");
    assert_eq!(error.kind(), StorageErrorKind::Database);

    let (persisted, _) = backend
        .get_task_access(task.id())
        .await
        .expect("rolled-back task should remain readable")
        .into_parts();
    assert_eq!(persisted.status(), StorageTaskStatus::Validating);
    let events = backend
        .list_task_events(StorageTaskPageQuery::new(
            task.id(),
            QueryOptions {
                filters: Vec::new(),
                sort: Vec::new(),
                limit: Some(10),
                cursor: None,
                include_total: true,
            },
        ))
        .await
        .expect("rolled-back task events should remain readable");
    assert_eq!(
        events.into_parts().0.len(),
        1,
        "terminal event must roll back"
    );

    crate::storage::postgres::with_connection(pool.get_ref(), async |conn| {
        use crate::schema::tasks::dsl::{id, tasks};
        diesel::delete(tasks.filter(id.eq(task.id())))
            .execute(conn)
            .await
    })
    .await
    .expect("task rollback fixture should be removed");
    user.delete_without_events(pool.get_ref())
        .await
        .expect("task rollback user should be removed");
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_consistent_inventory_counts() {
    let _permit = postgres_permit().await;

    for backend in available_backends() {
        let counts = backend
            .inventory_counts()
            .await
            .expect("certified backend should supply inventory counts");
        let grouped_objects = counts
            .objects_by_class()
            .iter()
            .map(|row| row.count())
            .sum::<i64>();

        assert_eq!(grouped_objects, counts.total_objects());
        assert!(counts.total_classes() >= counts.objects_by_class().len() as i64);
        assert!(counts.total_collections() >= 1);
        assert!(
            counts
                .objects_by_class()
                .windows(2)
                .all(|rows| rows[0].class_id() < rows[1].class_id()),
            "per-class counts must use stable class-id ordering"
        );
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_complete_group_behavior() {
    let _permit = postgres_permit().await;
    let pool = pool();

    for backend in available_backends() {
        let initial_name = prefix("group_contract");
        let renamed = prefix("group_contract_renamed");
        let created = backend
            .create_group(
                StorageGroupCreate::new(
                    None,
                    initial_name,
                    Some("storage compatibility group".to_string()),
                ),
                None,
            )
            .await
            .expect("certified backend should create groups");

        let loaded = backend
            .load_group(created.id())
            .await
            .expect("certified backend should load groups");
        assert_eq!(loaded.id(), created.id());
        assert_eq!(
            backend
                .group_identity_scope_name(created.id())
                .await
                .expect("certified backend should resolve group identity scopes"),
            LOCAL_IDENTITY_SCOPE
        );

        let updated = backend
            .update_group(
                created.id(),
                StorageGroupUpdate::new(Some(renamed.clone())),
                None,
            )
            .await
            .expect("certified backend should update groups");
        assert_eq!(updated.name(), renamed);

        let list_options = QueryOptions {
            filters: Vec::new(),
            sort: Vec::new(),
            limit: None,
            cursor: None,
            include_total: true,
        };
        let (listed, total_count) = backend
            .list_groups(StorageGroupListQuery::new(
                list_options.clone(),
                Some(list_options),
            ))
            .await
            .expect("certified backend should list and count groups")
            .into_parts();
        assert!(listed.iter().any(|group| group.id() == created.id()));
        assert!(total_count.is_some_and(|count| count >= listed.len() as i64));

        let user = crate::tests::create_user_with_params(
            pool.get_ref(),
            &prefix("group_contract_user"),
            "testpassword",
        )
        .await;
        backend
            .add_group_member(user.id, created.id(), None)
            .await
            .expect("certified backend should add group members");

        let members = backend
            .group_members(created.id())
            .await
            .expect("certified backend should list group members");
        assert!(members.iter().any(|member| member.id() == user.id));
        assert_eq!(
            backend
                .group_member_principal(user.id)
                .await
                .expect("certified backend should load a membership principal")
                .id(),
            user.id
        );

        let query_options = QueryOptions {
            filters: Vec::new(),
            sort: Vec::new(),
            limit: Some(10),
            cursor: None,
            include_total: true,
        };
        let page = backend
            .group_members_page(created.id(), query_options.clone())
            .await
            .expect("certified backend should page group members");
        assert!(page.iter().any(|(_, member)| member.id() == user.id));
        assert_eq!(
            backend
                .count_group_members(created.id(), query_options.clone())
                .await
                .expect("certified backend should count group members"),
            1
        );

        backend
            .remove_group_member(user.id, created.id(), None)
            .await
            .expect("certified backend should remove group members");
        assert_eq!(
            backend
                .count_group_members(created.id(), query_options)
                .await
                .expect("certified backend should recount group members"),
            0
        );
        assert_eq!(
            backend
                .delete_group(created.id(), None)
                .await
                .expect("certified backend should delete groups"),
            1
        );
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_complete_principal_behavior() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let user = crate::tests::create_user_with_params(
        pool.get_ref(),
        &prefix("principal_contract_user"),
        "testpassword",
    )
    .await;
    let event_context = EventContext::user(user.id, None, None);

    for backend in available_backends() {
        let loaded = backend
            .load_principal(user.id)
            .await
            .expect("certified backend should load principals");
        assert_eq!(loaded.id(), user.id);

        let initial = backend
            .load_principal_settings(user.id)
            .await
            .expect("certified backend should load principal settings");
        assert_eq!(initial.document(), &serde_json::json!({}));

        let replaced = backend
            .mutate_principal_settings(
                user.id,
                StoragePrincipalSettingsMutation::Replace(serde_json::json!({
                    "theme": "light",
                    "notifications": {"email": true}
                })),
                &event_context,
            )
            .await
            .expect("certified backend should replace principal settings");
        assert_eq!(replaced.document()["theme"], "light");

        let merged = backend
            .mutate_principal_settings(
                user.id,
                StoragePrincipalSettingsMutation::MergePatch(serde_json::json!({
                    "notifications": {"push": true}
                })),
                &event_context,
            )
            .await
            .expect("certified backend should merge principal settings");
        assert_eq!(merged.document()["notifications"]["email"], true);
        assert_eq!(merged.document()["notifications"]["push"], true);

        let patched = backend
            .mutate_principal_settings(
                user.id,
                StoragePrincipalSettingsMutation::JsonPatch(serde_json::json!([
                    {"op": "replace", "path": "/theme", "value": "dark"}
                ])),
                &event_context,
            )
            .await
            .expect("certified backend should apply principal settings JSON Patch");
        assert_eq!(patched.document()["theme"], "dark");

        let reset = backend
            .mutate_principal_settings(
                user.id,
                StoragePrincipalSettingsMutation::Reset,
                &event_context,
            )
            .await
            .expect("certified backend should reset principal settings");
        assert_eq!(reset.document(), &serde_json::json!({}));
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
    let token = user
        .create_token(pool.get_ref())
        .await
        .expect("authentication compatibility token should be created");

    for backend in available_backends() {
        let authenticated = backend
            .authenticate_bearer_token(AuthenticationCredential::new(token.storage_hash()))
            .await
            .expect("certified backend should validate active bearer credentials");
        assert_eq!(authenticated.principal_id(), user.id);
        assert!(!authenticated.is_scoped());

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

    user.delete_without_events(pool.get_ref())
        .await
        .expect("authentication compatibility fixture should be removed");
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_complete_identity_operations() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let username = prefix("identity_contract_user");
    let user =
        crate::tests::create_user_with_params(pool.get_ref(), &username, "testpassword").await;
    let _token = user
        .create_token(pool.get_ref())
        .await
        .expect("identity compatibility token should be created");
    let owner_group = crate::tests::create_test_group(pool.get_ref()).await;
    owner_group
        .add_member_without_events(pool.get_ref(), &user)
        .await
        .expect("identity compatibility membership should be created");

    for backend in available_backends() {
        assert!(
            !backend
                .default_admin_bootstrap_required()
                .await
                .expect("seeded certified backend should report bootstrap state")
        );
        assert!(
            !backend
                .bootstrap_default_admin(StorageDefaultAdminBootstrap::new(
                    "unused-contract-admin-group",
                    "unused-contract-password-hash",
                ))
                .await
                .expect("certified backend should coordinate administrator bootstrap")
        );
        let local_scope = backend
            .ensure_identity_scope(crate::storage::StorageIdentityScopeEnsure::new(
                LOCAL_IDENTITY_SCOPE,
                crate::models::LOCAL_PROVIDER_KIND,
            ))
            .await
            .expect("certified backend should reconcile identity scopes");
        assert_eq!(
            backend
                .identity_scope_name(local_scope.id())
                .await
                .expect("certified backend should resolve one identity scope"),
            LOCAL_IDENTITY_SCOPE
        );
        assert_eq!(
            backend
                .identity_scope_names(vec![local_scope.id()])
                .await
                .expect("certified backend should resolve identity scopes"),
            vec![(local_scope.id(), LOCAL_IDENTITY_SCOPE.to_string())]
        );

        let membership = backend
            .load_principal_group(user.id, owner_group.id)
            .await
            .expect("certified backend should load effective memberships");
        assert_eq!(membership.principal_id(), user.id);
        let group_options = prepare_db_pagination::<crate::models::Group>(&QueryOptions {
            filters: Vec::new(),
            sort: Vec::new(),
            limit: Some(20),
            cursor: None,
            include_total: true,
        })
        .expect("identity compatibility group query should be valid");
        let (groups, group_total) = backend
            .list_principal_groups(StoragePrincipalGroupListQuery::new(user.id, group_options))
            .await
            .expect("certified backend should list principal groups")
            .into_parts();
        assert!(group_total.is_some_and(|total| total >= 1));
        assert!(groups.into_iter().any(|group| group.id() == owner_group.id));
        assert!(
            backend
                .is_human_owner_group_member(user.id, owner_group.id)
                .await
                .expect("certified backend should evaluate human ownership")
        );

        let event_context = EventContext::user(user.id, None, None);
        let contract_username = prefix("complete_user_contract");
        let contract_user = backend
            .create_user(StorageUserCreate::new(
                None,
                &contract_username,
                "complete-user-contract-password-hash",
                Some("Complete Contract".to_string()),
                Some("complete-contract@example.invalid".to_string()),
                Some(event_context.clone()),
            ))
            .await
            .expect("certified backend should create users");
        let contract_user_id = contract_user.into_parts().0;
        assert_eq!(
            backend
                .load_user(contract_user_id)
                .await
                .expect("certified backend should load users")
                .into_parts()
                .0,
            contract_user_id
        );
        assert_eq!(
            backend
                .load_user_by_name(LOCAL_IDENTITY_SCOPE.to_string(), contract_username.clone(),)
                .await
                .expect("certified backend should resolve scoped user names")
                .into_parts()
                .0,
            contract_user_id
        );
        assert_eq!(
            backend
                .load_user_point(contract_user_id)
                .await
                .expect("certified backend should load user points")
                .into_parts()
                .0,
            contract_user_id
        );
        let user_options = prepare_db_pagination::<crate::models::UserWithName>(&QueryOptions {
            filters: vec![ParsedQueryParam {
                field: FilterField::Id,
                operator: SearchOperator::Equals { is_negated: false },
                value: contract_user_id.to_string(),
            }],
            sort: Vec::new(),
            limit: Some(100),
            cursor: None,
            include_total: true,
        })
        .expect("identity compatibility user query should be valid");
        let (users, user_total) = backend
            .list_users(StorageUserListQuery::new(user_options))
            .await
            .expect("certified backend should list users")
            .into_parts();
        assert!(user_total.is_some_and(|total| total >= 1));
        assert!(users.into_iter().any(|item| {
            let (user, ..) = item.into_parts();
            user.into_parts().0 == contract_user_id
        }));
        backend
            .update_user(StorageUserUpdate::new(
                contract_user_id,
                None,
                Some("Updated Contract".to_string()),
                None,
                Some(event_context.clone()),
            ))
            .await
            .expect("certified backend should update users");
        backend
            .set_user_password(StorageUserPasswordUpdate::new(
                contract_user_id,
                "updated-complete-user-contract-password-hash",
            ))
            .await
            .expect("certified backend should replace local passwords");

        let token_policy = StorageTokenIssuancePolicy::new(24, 24);
        let first_hash = prefix("complete_token_hash");
        let first_token = backend
            .create_token(
                StorageTokenCreate::new(contract_user_id, &first_hash, token_policy)
                    .event_context(Some(event_context.clone())),
            )
            .await
            .expect("certified backend should create tokens");
        let first_token_id = first_token.id();
        assert_eq!(
            backend
                .load_token_metadata(contract_user_id, first_token_id)
                .await
                .expect("certified backend should load token metadata")
                .id(),
            first_token_id
        );
        let batch = backend
            .load_token_metadata_batch(vec![first_token_id, first_token_id])
            .await
            .expect("certified backend should preserve token batch order");
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0].id(), batch[1].id());

        let second_hash = prefix("complete_renewed_token_hash");
        let renewed = backend
            .renew_token(StorageTokenRenew::new(
                first_token_id,
                contract_user_id,
                &second_hash,
                None,
                token_policy,
                Some(event_context.clone()),
            ))
            .await
            .expect("certified backend should renew tokens");
        assert_ne!(renewed.id(), first_token_id);
        assert_eq!(
            backend
                .revoke_token(StorageTokenRevoke::new(
                    first_token_id,
                    contract_user_id,
                    Some(event_context.clone()),
                ))
                .await
                .expect("certified backend should revoke principal-scoped tokens"),
            1
        );
        assert_eq!(
            backend
                .revoke_token_by_hash(StorageTokenHashRevoke::new(
                    Some(contract_user_id),
                    second_hash,
                ))
                .await
                .expect("certified backend should revoke HMAC-keyed tokens"),
            1
        );
        let third_hash = prefix("complete_revoke_all_token_hash");
        backend
            .create_token(StorageTokenCreate::new(
                contract_user_id,
                third_hash,
                token_policy,
            ))
            .await
            .expect("certified backend should create a token for bulk revocation");
        assert_eq!(
            backend
                .revoke_all_principal_tokens(contract_user_id)
                .await
                .expect("certified backend should revoke all principal tokens"),
            1
        );
        backend
            .anonymize_user(contract_user_id)
            .await
            .expect("certified backend should anonymize users");
        assert_eq!(
            backend
                .delete_user(StorageUserDelete::new(
                    contract_user_id,
                    Some(event_context.clone()),
                ))
                .await
                .expect("certified backend should delete users"),
            1
        );

        let token_options = prepare_db_pagination::<crate::models::PrincipalToken>(&QueryOptions {
            filters: Vec::new(),
            sort: Vec::new(),
            limit: Some(20),
            cursor: None,
            include_total: true,
        })
        .expect("identity compatibility token query should be valid");
        let (tokens, token_total) = backend
            .list_retained_tokens(StorageTokenListQuery::new(
                user.id,
                token_options,
                StorageTokenListState::Active,
            ))
            .await
            .expect("certified backend should list retained tokens")
            .into_parts();
        assert_eq!(token_total, Some(1));
        assert_eq!(tokens[0].principal_id(), user.id);
        assert_eq!(
            backend
                .reset_local_password(StorageLocalPasswordReset::new(
                    &username,
                    "identity-contract-password-hash",
                ))
                .await
                .expect("certified backend should reset local credentials"),
            1
        );

        let service_account_name = prefix("identity_contract_sa");
        let created = backend
            .create_service_account(StorageServiceAccountCreate::new(
                &service_account_name,
                "identity contract",
                owner_group.id,
                Some(user.id),
                event_context.clone(),
            ))
            .await
            .expect("certified backend should create service accounts");
        let loaded = backend
            .load_service_account(created.id())
            .await
            .expect("certified backend should load service accounts");
        assert_eq!(loaded.owner_group_id(), owner_group.id);
        let point = backend
            .load_service_account_point(created.id())
            .await
            .expect("certified backend should load service-account points");
        assert_eq!(point.into_parts().2, service_account_name);

        let service_account_options =
            prepare_db_pagination::<crate::models::ServiceAccountWithName>(&QueryOptions {
                filters: Vec::new(),
                sort: Vec::new(),
                limit: Some(100),
                cursor: None,
                include_total: true,
            })
            .expect("identity compatibility service-account query should be valid");
        let (accounts, account_total) = backend
            .list_manageable_service_accounts(StorageServiceAccountListQuery::new(
                user.id,
                true,
                service_account_options,
            ))
            .await
            .expect("certified backend should list manageable service accounts")
            .into_parts();
        assert!(account_total.is_some_and(|total| total >= 1));
        assert!(accounts.into_iter().any(|account| {
            let (account, _, _, _) = account.into_parts();
            account.id() == created.id()
        }));

        let updated = backend
            .update_service_account(StorageServiceAccountUpdate::new(
                created.id(),
                Some("updated identity contract".to_string()),
                None,
                event_context.clone(),
            ))
            .await
            .expect("certified backend should update service accounts");
        assert_eq!(updated.description(), "updated identity contract");
        assert!(
            !backend
                .principal_is_disabled(created.id())
                .await
                .expect("certified backend should read principal lifecycle")
        );
        backend
            .disable_service_account(StorageServiceAccountMutation::new(
                created.id(),
                event_context.clone(),
            ))
            .await
            .expect("certified backend should disable service accounts");
        assert!(
            backend
                .principal_is_disabled(created.id())
                .await
                .expect("certified backend should observe disabled principals")
        );

        let external_scope = prefix("identity_contract_scope");
        let external_name = prefix("identity_contract_external");
        let external = backend
            .sync_external_user(
                crate::storage::StorageExternalUserSync::builder(
                    &external_scope,
                    "compatibility_provider",
                    prefix("identity_contract_subject"),
                    &external_name,
                )
                .groups(vec![crate::storage::StorageExternalGroup::new(
                    prefix("identity_contract_group_key"),
                    prefix("identity_contract_group"),
                    None,
                )])
                .build(),
            )
            .await
            .expect("certified backend should synchronize external identities");
        let external_id = external.into_parts().0;
        let external_state = backend
            .external_principal_state(external_id)
            .await
            .expect("certified backend should load external identity state")
            .expect("synchronized external identity should have refresh state");
        assert_eq!(external_state.identity_scope(), external_scope);
        backend
            .mark_external_sync_attempted(external_id)
            .await
            .expect("certified backend should record external sync attempts");

        backend
            .delete_service_account(StorageServiceAccountMutation::new(
                created.id(),
                event_context,
            ))
            .await
            .expect("certified backend should delete service accounts");
    }

    user.delete_without_events(pool.get_ref())
        .await
        .expect("identity compatibility user should be removed");
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_execution_context() {
    for backend in available_backends() {
        let evaluations = Arc::new(AtomicUsize::new(0));

        let evaluated = Arc::clone(&evaluations);
        backend
            .run_with_call_site(StorageCallSite::Readiness, async move {
                evaluated.fetch_add(1, Ordering::SeqCst);
            })
            .await;

        let evaluated = Arc::clone(&evaluations);
        backend
            .run_with_call_site_send(StorageCallSite::TaskLease, async move {
                evaluated.fetch_add(1, Ordering::SeqCst);
            })
            .await;

        let evaluated = Arc::clone(&evaluations);
        backend
            .run_with_mutation_provenance(Some(MutationProvenance::system()), async move {
                evaluated.fetch_add(1, Ordering::SeqCst);
            })
            .await;

        let evaluated = Arc::clone(&evaluations);
        backend
            .run_with_revision_precondition(
                Some(
                    StorageRevisionPrecondition::new("collection:1", vec![1])
                        .expect("compatibility precondition should be valid"),
                ),
                async move {
                    evaluated.fetch_add(1, Ordering::SeqCst);
                },
            )
            .await;

        assert_eq!(AtomicUsize::load(evaluations.as_ref(), Ordering::SeqCst), 4);
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_the_export_query_scope() {
    for backend in available_backends() {
        let evaluations = Arc::new(AtomicUsize::new(0));
        let evaluated = Arc::clone(&evaluations);
        let output = backend
            .run_export_queries(StorageQueryBudget::from_millis(250), async move {
                evaluated.fetch_add(1, Ordering::SeqCst);
                "complete"
            })
            .await;

        assert_eq!(output, "complete");
        assert_eq!(AtomicUsize::load(evaluations.as_ref(), Ordering::SeqCst), 1);
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_the_complete_import_contract() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let preflight_name = prefix("import_preflight_collection");
    let best_effort_name = prefix("import_best_effort_collection");
    let rollback_name = prefix("import_rollback_collection");
    let collection_input = |name: &str, reference: &str| ImportCollectionInput {
        ref_: Some(reference.to_string()),
        name: name.to_string(),
        description: "storage compatibility import".to_string(),
        parent_collection_ref: None,
        parent_collection_key: None,
        condition: None,
        timestamps: None,
    };

    for backend in available_backends() {
        let root = backend
            .import_root_collection()
            .await
            .expect("certified backend should resolve the import root");
        let root_id = root.id();
        let root_name = root.name().to_string();
        assert_eq!(
            backend
                .import_collection_by_id(root_id)
                .await
                .expect("certified backend should look up import collections by id")
                .map(|collection| collection.id()),
            Some(root_id)
        );
        let root_key = crate::services::import_boundary::collection_key_to_storage(CollectionKey {
            name: root_name.clone(),
            path: Some(Vec::new()),
        });
        assert!(
            backend
                .import_collection_by_key(&root_key)
                .await
                .expect("certified backend should look up import collections by path")
                .is_some()
        );
        assert!(
            backend
                .import_collections_by_name(&root_name)
                .await
                .expect("certified backend should look up import collections by name")
                .iter()
                .any(|collection| collection.id() == root_id)
        );
        assert!(
            backend
                .import_collection_child_by_name(root_id, &preflight_name)
                .await
                .expect("certified backend should look up import children")
                .is_none()
        );
        assert!(
            backend
                .import_class_by_name(root_id, &prefix("missing_import_class"))
                .await
                .expect("certified backend should look up import classes")
                .is_none()
        );
        assert!(
            backend
                .import_classes_by_names(root_id, &[])
                .await
                .expect("certified backend should batch import class lookups")
                .is_empty()
        );
        assert!(
            backend
                .import_object_by_name(i32::MAX, &prefix("missing_import_object"))
                .await
                .expect("certified backend should look up import objects")
                .is_none()
        );
        assert!(
            backend
                .import_objects_by_names(i32::MAX, &[])
                .await
                .expect("certified backend should batch import object lookups")
                .is_empty()
        );
        assert!(
            !backend
                .import_class_relation_exists(i32::MAX - 1, i32::MAX)
                .await
                .expect("certified backend should look up import class relations")
        );
        assert!(
            !backend
                .import_object_relation_exists(i32::MAX - 1, i32::MAX)
                .await
                .expect("certified backend should look up import object relations")
        );
        assert!(
            !backend
                .import_group_exists(LOCAL_IDENTITY_SCOPE, &prefix("missing_import_group"),)
                .await
                .expect("certified backend should look up import groups")
        );

        let preflight_plan = vec![StorageImportPlanItem::new(
            0,
            crate::services::import_boundary::import_operation_to_storage(
                ApplicationImportOperation::CreateCollection(collection_input(
                    &preflight_name,
                    "collection:preflight",
                )),
            )
            .expect("valid collection input should cross the storage boundary"),
        )];
        let (preflight, aborted) = backend
            .preflight_import(
                preflight_plan.clone(),
                crate::services::import_boundary::import_mode_to_storage(ImportMode::default()),
            )
            .await
            .expect("certified backend should preflight an import")
            .into_parts();
        assert!(!aborted);
        assert_eq!(preflight.len(), 1);
        assert!(
            preflight
                .into_iter()
                .next()
                .unwrap()
                .into_parts()
                .2
                .is_none()
        );
        assert!(
            backend
                .import_collection_child_by_name(root_id, &preflight_name)
                .await
                .expect("preflight rollback should remain queryable")
                .is_none(),
            "import preflight must roll back every mutation"
        );

        backend
            .apply_import_strict(preflight_plan)
            .await
            .expect("certified backend should atomically apply a strict import");

        let rollback_plan = vec![
            StorageImportPlanItem::new(
                0,
                crate::services::import_boundary::import_operation_to_storage(
                    ApplicationImportOperation::CreateCollection(collection_input(
                        &rollback_name,
                        "collection:rollback",
                    )),
                )
                .expect("valid collection input should cross the storage boundary"),
            ),
            StorageImportPlanItem::new(
                1,
                crate::services::import_boundary::import_operation_to_storage(
                    ApplicationImportOperation::CreateClass(ImportClassInput {
                        ref_: Some("class:rollback_failure".to_string()),
                        name: prefix("import_rollback_class"),
                        description: "must fail".to_string(),
                        json_schema: None,
                        validate_schema: Some(false),
                        collection_ref: Some("collection:missing".to_string()),
                        collection_key: None,
                        condition: None,
                        timestamps: None,
                    }),
                )
                .expect("valid class input should cross the storage boundary"),
            ),
        ];
        assert!(backend.apply_import_strict(rollback_plan).await.is_err());
        assert!(
            backend
                .import_collection_child_by_name(root_id, &rollback_name)
                .await
                .expect("strict rollback should remain queryable")
                .is_none(),
            "strict import must roll back earlier successful items"
        );

        let best_effort = backend
            .apply_import_best_effort(
                vec![
                    StorageImportPlanItem::new(
                        0,
                        crate::services::import_boundary::import_operation_to_storage(
                            ApplicationImportOperation::CreateCollection(collection_input(
                                &best_effort_name,
                                "collection:best_effort",
                            )),
                        )
                        .expect("valid collection input should cross the storage boundary"),
                    ),
                    StorageImportPlanItem::new(
                        1,
                        crate::services::import_boundary::import_operation_to_storage(
                            ApplicationImportOperation::CreateClass(ImportClassInput {
                                ref_: Some("class:best_effort_failure".to_string()),
                                name: prefix("import_best_effort_class"),
                                description: "must fail".to_string(),
                                json_schema: None,
                                validate_schema: Some(false),
                                collection_ref: Some("collection:missing".to_string()),
                                collection_key: None,
                                condition: None,
                                timestamps: None,
                            }),
                        )
                        .expect("valid class input should cross the storage boundary"),
                    ),
                ],
                crate::services::import_boundary::import_mode_to_storage(ImportMode {
                    atomicity: Some(ImportAtomicity::BestEffort),
                    ..ImportMode::default()
                }),
            )
            .await
            .expect("certified backend should apply a best-effort import");
        let (best_effort, aborted) = best_effort.into_parts();
        assert!(!aborted);
        assert_eq!(best_effort.len(), 2);
        assert!(best_effort[0].error().is_none());
        assert!(best_effort[1].error().is_some());

        for name in [&preflight_name, &best_effort_name] {
            let collection = backend
                .import_collection_child_by_name(root_id, name)
                .await
                .expect("committed import collection should remain queryable")
                .expect("committed import collection should exist");
            crate::services::storage_boundary::collection_from_storage(collection)
                .expect("backend collection should satisfy application invariants")
                .delete_without_events(pool.get_ref())
                .await
                .expect("import compatibility fixture should be removed");
        }
    }
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

    for backend in available_backends() {
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

        backend
            .record_import_results(vec![
                StorageImportResult::builder(task_id, "compatibility", "verify", "succeeded")
                    .item_ref(Some("compatibility:item".to_string()))
                    .build(),
            ])
            .await
            .expect("certified backend should persist import results");
        let (results, result_total) = backend
            .list_import_task_results(StorageTaskPageQuery::new(task_id, options()))
            .await
            .expect("certified backend should return persisted import results")
            .into_parts();
        assert_eq!(result_total, Some(1));
        assert_eq!(results.len(), 1);

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

    user.delete_without_events(pool.get_ref())
        .await
        .expect("task queue compatibility user should be removed");
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_the_complete_task_state_machine() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let user = crate::tests::create_user_with_params(
        pool.get_ref(),
        &prefix("task_execution_user"),
        "testpassword",
    )
    .await;

    for backend in available_backends() {
        let mut fixture_ids = Vec::new();
        for task_kind in StorageTaskKind::ALL {
            let task = backend
                .create_task(
                    StorageTaskCreateRequest::builder(
                        task_kind,
                        user.id,
                        serde_json::json!({"compatibility": true}),
                        1,
                    )
                    .idempotency_key(Some(
                        IdempotencyKey::new(prefix(&format!(
                            "task_execution_{}",
                            task_kind.as_str()
                        )))
                        .expect("compatibility idempotency key should be valid"),
                    ))
                    .request_hash(Some(prefix(&format!(
                        "task_execution_hash_{}",
                        task_kind.as_str()
                    ))))
                    .scope_snapshot(StorageTaskScopeSnapshot::unscoped())
                    .build(10),
                )
                .await
                .expect("certified backend should create an executable task");
            fixture_ids.push(task.id());
        }
        crate::storage::postgres::with_connection(pool.get_ref(), async |conn| {
            use crate::schema::tasks::dsl::{created_at, id, tasks};
            diesel::update(tasks.filter(id.eq_any(&fixture_ids)))
                .set(
                    created_at.eq(chrono::NaiveDate::from_ymd_opt(2000, 1, 1)
                        .expect("compatibility date")
                        .and_hms_opt(0, 0, 0)
                        .expect("compatibility time")),
                )
                .execute(conn)
                .await
        })
        .await
        .expect("compatibility tasks should be made claim-first");

        let lease_duration = StorageTaskLeaseDuration::from_milliseconds(60_000)
            .expect("compatibility lease duration should be valid");
        assert!(
            backend
                .recover_expired_task_leases(0)
                .await
                .expect("certified backend should recover expired claims")
                .is_empty()
        );

        let mut completed_ids = HashSet::new();
        let mut completed_kinds = HashSet::new();
        for completed_index in 0..StorageTaskKind::ALL.len() {
            let claimed = backend
                .claim_next_task(lease_duration)
                .await
                .expect("certified backend should claim the next task")
                .expect("a compatibility task should be claimable");
            assert!(fixture_ids.contains(&claimed.task().id()));
            assert!(completed_ids.insert(claimed.task().id()));
            assert!(completed_kinds.insert(claimed.task().kind()));
            if completed_index == 0 {
                assert!(
                    backend
                        .renew_task_lease(claimed.lease().clone(), lease_duration)
                        .await
                        .expect("certified backend should renew a live claim")
                );
                backend
                    .append_task_event(StorageTaskEventAppend::new(
                        claimed.lease().clone(),
                        StorageTaskEventInput::new("running", "Compatibility event"),
                    ))
                    .await
                    .expect("certified backend should append a claim-owned event");
            }
            backend
                .update_task_state(StorageTaskStateUpdate::new(
                    claimed.lease().clone(),
                    StorageTaskStatus::Running,
                    StorageTaskResultCounts::new(0, 0, 0),
                ))
                .await
                .expect("certified backend should update claimed task state");
            let artifact = compatibility_completion_artifact(claimed.task().kind());
            backend
                .complete_task(
                    StorageTaskCompletion::new(
                        StorageTaskStateUpdate::new(
                            claimed.lease().clone(),
                            StorageTaskStatus::Succeeded,
                            StorageTaskResultCounts::new(1, 1, 0),
                        ),
                        StorageTaskEventInput::new("succeeded", "Compatibility completed"),
                    )
                    .artifact(artifact),
                )
                .await
                .expect("certified backend should complete a claimed task");
            match claimed.task().kind() {
                StorageTaskKind::Export => assert!(matches!(
                    backend.get_export_output(claimed.task().id()).await,
                    Ok(StorageTaskOutputLookup::Available(_))
                )),
                StorageTaskKind::Backup => assert!(matches!(
                    backend.get_backup_output(claimed.task().id()).await,
                    Ok(StorageTaskOutputLookup::Available(_))
                )),
                StorageTaskKind::Import
                | StorageTaskKind::Reindex
                | StorageTaskKind::RemoteCall => {}
            }
        }
        assert_eq!(completed_kinds.len(), StorageTaskKind::ALL.len());

        let mut failure_fixture_ids = Vec::new();
        for task_kind in StorageTaskKind::ALL {
            let task = backend
                .create_task(
                    StorageTaskCreateRequest::builder(
                        task_kind,
                        user.id,
                        serde_json::json!({"compatibility_failure": true}),
                        1,
                    )
                    .idempotency_key(Some(
                        IdempotencyKey::new(prefix(&format!(
                            "task_execution_failure_{}",
                            task_kind.as_str()
                        )))
                        .expect("compatibility idempotency key should be valid"),
                    ))
                    .request_hash(Some(prefix(&format!(
                        "task_execution_failure_hash_{}",
                        task_kind.as_str()
                    ))))
                    .scope_snapshot(StorageTaskScopeSnapshot::unscoped())
                    .build(10),
                )
                .await
                .expect("certified backend should create a failure fixture");
            failure_fixture_ids.push(task.id());
            fixture_ids.push(task.id());
        }
        crate::storage::postgres::with_connection(pool.get_ref(), async |conn| {
            use crate::schema::tasks::dsl::{created_at, id, tasks};
            diesel::update(tasks.filter(id.eq_any(&failure_fixture_ids)))
                .set(
                    created_at.eq(chrono::NaiveDate::from_ymd_opt(1999, 1, 1)
                        .expect("compatibility date")
                        .and_hms_opt(0, 0, 0)
                        .expect("compatibility time")),
                )
                .execute(conn)
                .await
        })
        .await
        .expect("failure fixtures should be made claim-first");
        let failed = backend
            .claim_next_task(lease_duration)
            .await
            .expect("certified backend should claim a failure fixture")
            .expect("a compatibility failure fixture should be claimable");
        assert!(failure_fixture_ids.contains(&failed.task().id()));
        backend
            .fail_task(StorageTaskFailure::new(
                failed.lease().clone(),
                "Compatibility failure",
                StorageTaskEventInput::new("failed", "Compatibility failure"),
            ))
            .await
            .expect("certified backend should fail a claimed task");

        backend
            .purge_expired_export_outputs()
            .await
            .expect("certified backend should purge expired export outputs");
        backend
            .purge_expired_backup_outputs()
            .await
            .expect("certified backend should purge expired backup outputs");

        crate::storage::postgres::with_transaction(
            pool.get_ref(),
            async |conn| -> Result<(), crate::errors::ApiError> {
                use crate::schema::tasks::dsl::{id, tasks};
                diesel::delete(tasks.filter(id.eq_any(&fixture_ids)))
                    .execute(conn)
                    .await?;
                Ok(())
            },
        )
        .await
        .expect("task execution compatibility fixtures should be removed");
    }

    user.delete_without_events(pool.get_ref())
        .await
        .expect("task execution compatibility user should be removed");
}

fn compatibility_completion_artifact(kind: StorageTaskKind) -> StorageTaskCompletionArtifact {
    let output_expires_at =
        chrono::Utc::now().naive_utc() + chrono::Duration::try_hours(1).expect("valid duration");
    match kind {
        StorageTaskKind::Import | StorageTaskKind::Reindex => StorageTaskCompletionArtifact::None,
        StorageTaskKind::Export => StorageTaskCompletionArtifact::Export(
            StorageExportTaskArtifact::builder(
                "application/json",
                serde_json::json!({"compatibility": true}),
                serde_json::json!([]),
                output_expires_at,
            )
            .output(Some(serde_json::json!({"compatible": true})), None)
            .build(),
        ),
        StorageTaskKind::Backup => StorageTaskCompletionArtifact::Backup(
            StorageBackupTaskArtifact::new(b"{}".to_vec(), 2, "0".repeat(64), output_expires_at),
        ),
        StorageTaskKind::RemoteCall => {
            StorageTaskCompletionArtifact::RemoteCall(StorageRemoteCallTaskArtifact::new(
                StorageRemoteCallArtifactTarget::new(
                    None,
                    "collection",
                    1,
                    "GET",
                    "https://compatibility.invalid",
                ),
                StorageRemoteCallArtifactResponse::new(
                    Some(200),
                    Some(serde_json::json!({})),
                    Some("compatible".to_string()),
                ),
                StorageRemoteCallArtifactOutcome::new(1, true, None),
            ))
        }
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_backup_snapshots() {
    let _permit = postgres_permit().await;

    for backend in available_backends() {
        let (state, history) = backend
            .snapshot_backup(false)
            .await
            .expect("certified backend should supply a state-only backup snapshot")
            .into_parts();
        assert_eq!(
            state.len(),
            crate::models::backup::BACKUP_STATE_SECTIONS.len()
        );
        for section in crate::models::backup::BACKUP_STATE_SECTIONS {
            assert!(state.contains_key(*section));
        }
        assert!(history.is_none());

        let (state, history) = backend
            .snapshot_backup(true)
            .await
            .expect("certified backend should supply a history-inclusive backup snapshot")
            .into_parts();
        assert_eq!(
            state.len(),
            crate::models::backup::BACKUP_STATE_SECTIONS.len()
        );
        for section in crate::models::backup::BACKUP_STATE_SECTIONS {
            assert!(state.contains_key(*section));
        }
        let history = history.expect("history was requested");
        assert_eq!(
            history.len(),
            crate::models::backup::backup_history_sections().count()
        );
        for section in crate::models::backup::backup_history_sections() {
            assert!(history.contains_key(section));
        }
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_export_template_lifecycle() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let owner = crate::tests::create_test_user(pool.get_ref()).await;
    let fixture = crate::tests::create_collection_fixture(
        pool.get_ref(),
        &prefix("export_template_collection"),
    )
    .await;
    let collection_id = fixture.collection.id;
    let class = NewHubuumClass {
        name: prefix("export_template_class"),
        collection_id,
        json_schema: None,
        validate_schema: Some(false),
        description: "export-template compatibility class".to_string(),
    }
    .save_without_events(pool.get_ref())
    .await
    .expect("export-template compatibility class should be created");
    let event_context = EventContext::user(owner.id, None, None);

    for backend in available_backends() {
        let name = prefix("export_template");
        let definition = StorageExportTemplateDefinition::new(
            "compatibility fragment",
            "text/plain",
            "Hello {{ object.name }}",
            "fragment",
        );
        let created = backend
            .create_export_template(StorageExportTemplateCreate::new(
                collection_id,
                name.clone(),
                definition,
                Some(event_context.clone()),
            ))
            .await
            .expect("certified backend should create an export template");
        let (metadata, created_collection_id, created_name, _) = created.into_parts();
        let template_id = metadata.id();
        assert_eq!(created_collection_id, collection_id);
        assert_eq!(created_name, name);

        let loaded = backend
            .get_export_template(template_id)
            .await
            .expect("certified backend should load an export template");
        assert_eq!(loaded.into_parts().0.id(), template_id);

        let (templates, total) = backend
            .list_export_templates(StorageExportTemplateListQuery::within_collections(
                vec![collection_id],
                QueryOptions {
                    filters: Vec::new(),
                    sort: Vec::new(),
                    limit: Some(10),
                    cursor: None,
                    include_total: true,
                },
            ))
            .await
            .expect("certified backend should list export templates")
            .into_parts();
        assert_eq!(total, Some(1));
        assert_eq!(templates.len(), 1);

        let siblings = backend
            .list_export_templates_in_collection(collection_id, Some(template_id))
            .await
            .expect("certified backend should list collection template siblings");
        assert!(siblings.is_empty());

        assert_eq!(
            backend
                .export_template_class_collection_id(class.id)
                .await
                .expect("certified backend should resolve template class ownership"),
            Some(collection_id)
        );

        let replacement_name = format!("{name}_updated");
        let replaced = backend
            .replace_export_template(StorageExportTemplateReplace::new(
                template_id,
                collection_id,
                replacement_name.clone(),
                StorageExportTemplateDefinition::new(
                    "updated compatibility fragment",
                    "text/plain",
                    "Updated {{ object.name }}",
                    "fragment",
                ),
                Some(event_context.clone()),
            ))
            .await
            .expect("certified backend should replace an export template");
        assert_eq!(replaced.into_parts().2, replacement_name);

        backend
            .delete_export_template(StorageExportTemplateDelete::new(
                template_id,
                Some(event_context.clone()),
            ))
            .await
            .expect("certified backend should delete an export template");
        assert!(backend.get_export_template(template_id).await.is_err());
    }

    class
        .delete_without_events(pool.get_ref())
        .await
        .expect("export-template compatibility class should be removed");
    fixture
        .cleanup()
        .await
        .expect("export-template compatibility collection should be removed");
    owner
        .delete_without_events(pool.get_ref())
        .await
        .expect("export-template compatibility owner should be removed");
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_remote_target_lifecycle() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let owner = crate::tests::create_test_user(pool.get_ref()).await;
    let fixture = crate::tests::create_collection_fixture(
        pool.get_ref(),
        &prefix("remote_target_collection"),
    )
    .await;
    let collection_id = fixture.collection.id;
    let event_context = EventContext::user(owner.id, None, None);

    for backend in available_backends() {
        let name = prefix("remote_target");
        let created = backend
            .create_remote_target(StorageRemoteTargetCreate::new(
                collection_id,
                name.clone(),
                StorageRemoteTargetDefinition::new(
                    "Compatibility remote target",
                    StorageRemoteTargetTransport::new(
                        "get",
                        "https://compatibility.invalid/collections/{{ collection.id }}",
                        serde_json::json!({}),
                        None,
                        serde_json::json!({"type": "none"}),
                        1_000,
                    ),
                    StorageRemoteTargetPolicy::new(None, vec!["collection".to_string()], true),
                ),
                event_context.clone(),
            ))
            .await
            .expect("certified backend should create a remote target");
        let target_id = created.metadata().id();
        assert_eq!(created.collection_id(), collection_id);

        let loaded = backend
            .get_remote_target(target_id)
            .await
            .expect("certified backend should load a remote target");
        assert_eq!(loaded.metadata().id(), target_id);

        let (targets, total) = backend
            .list_remote_targets(StorageRemoteTargetListQuery::new(
                vec![collection_id],
                QueryOptions {
                    filters: Vec::new(),
                    sort: Vec::new(),
                    limit: Some(10),
                    cursor: None,
                    include_total: true,
                },
            ))
            .await
            .expect("certified backend should list remote targets")
            .into_parts();
        assert_eq!(total, Some(1));
        assert!(
            targets
                .iter()
                .any(|target| target.metadata().id() == target_id)
        );

        let updated = backend
            .update_remote_target(StorageRemoteTargetUpdate::new(
                target_id,
                StorageRemoteTargetPatch::new()
                    .with_name(Some(format!("{name}_updated")))
                    .with_enabled(Some(false)),
                event_context.clone(),
            ))
            .await
            .expect("certified backend should update a remote target");
        let (metadata, _, updated_name, definition) = updated.into_parts();
        let (_, _, policy) = definition.into_parts();
        let (_, allowed_subject_types, enabled) = policy.into_parts();
        assert_eq!(metadata.id(), target_id);
        assert_eq!(updated_name, format!("{name}_updated"));
        assert_eq!(allowed_subject_types, ["collection"]);
        assert!(!enabled);

        backend
            .record_remote_target_invocation(StorageRemoteTargetInvocation::new(
                target_id,
                12345,
                "collection",
                collection_id,
                event_context.clone(),
            ))
            .await
            .expect("certified backend should record remote-target invocation provenance");
        backend
            .delete_remote_target(StorageRemoteTargetDelete::new(
                target_id,
                event_context.clone(),
            ))
            .await
            .expect("certified backend should delete a remote target");
        assert!(backend.get_remote_target(target_id).await.is_err());
    }

    fixture
        .cleanup()
        .await
        .expect("remote-target compatibility fixture should be removed");
    owner
        .delete_without_events(pool.get_ref())
        .await
        .expect("remote-target compatibility user should be removed");
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_restore_lifecycle_and_coordination() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let now = chrono::Utc::now().naive_utc();
    let instance_id = uuid::Uuid::new_v4();
    let mut staged_ids = Vec::new();

    for backend in available_backends() {
        let label = prefix("restore");
        let job = backend
            .stage_restore(StorageRestoreStageCreate::new(
                StorageRestoreInitiator::new(None, "compatibility", label.clone()),
                b"{}".to_vec(),
                StorageRestoreArtifactSummary::new(2, "a".repeat(64)),
                "b".repeat(64),
                serde_json::json!({"compatible": true}),
                now + chrono::Duration::try_hours(1).expect("valid duration"),
            ))
            .await
            .expect("certified backend should stage a restore artifact");
        let job_id = job.summary().id();
        staged_ids.push(job_id);
        assert_eq!(job.summary().status(), StorageRestoreJobStatus::Validated);

        let loaded = backend
            .get_restore_job(job_id)
            .await
            .expect("certified backend should load staged restore bytes");
        let (loaded_summary, document, capability_hash) = loaded.into_parts();
        assert_eq!(loaded_summary.id(), job_id);
        assert_eq!(document, b"{}".to_vec());
        assert_eq!(capability_hash, "b".repeat(64));

        let status = backend
            .get_restore_status(job_id)
            .await
            .expect("certified backend should load document-free restore status");
        let (status_summary, status_capability_hash, validation) = status.into_parts();
        assert_eq!(status_summary.status(), StorageRestoreJobStatus::Validated);
        assert_eq!(status_capability_hash, "b".repeat(64));
        assert_eq!(validation, serde_json::json!({"compatible": true}));

        let snapshot = backend
            .restore_coordinator_snapshot()
            .await
            .expect("certified backend should read restore coordination state");
        assert!(snapshot.maintenance_state().is_normal());
        assert_eq!(snapshot.restore_job_id(), None);

        let local_idle = || true;
        let tick = backend
            .tick_restore_coordinator(instance_id, &local_idle, false)
            .await
            .expect("certified backend should publish a coordinator heartbeat");
        assert!(tick.maintenance_state().is_normal());
        let (generation, instances) = backend
            .restore_drain_state(
                tick.backend_now() - chrono::Duration::try_minutes(1).expect("valid duration"),
            )
            .await
            .expect("certified backend should report live restore coordinators")
            .into_parts();
        let instance = instances
            .into_iter()
            .find(|instance| instance.instance_id() == instance_id)
            .expect("compatibility coordinator should be visible");
        assert_eq!(instance.maintenance_generation(), generation);
        assert!(!instance.is_drained());
        backend
            .remove_restore_instance(instance_id)
            .await
            .expect("certified backend should remove coordinator membership");

        backend
            .fail_restore_and_resume(StorageRestoreFailure::new(job_id, "compatibility failure"))
            .await
            .expect("certified backend should atomically fail a restore");
        let failed = backend
            .get_restore_job(job_id)
            .await
            .expect("failed restore should remain queryable");
        let (failed_summary, failed_document, _) = failed.into_parts();
        assert_eq!(failed_summary.status(), StorageRestoreJobStatus::Failed);
        assert!(failed_document.is_empty());

        backend
            .resume_maintenance_without_restore()
            .await
            .expect("orphaned-maintenance recovery should be idempotent");
        backend
            .resume_terminal_restore(job_id)
            .await
            .expect("terminal-restore recovery should be idempotent");

        let expired_label = prefix("expired_restore");
        let expired = backend
            .stage_restore(StorageRestoreStageCreate::new(
                StorageRestoreInitiator::new(None, "compatibility", expired_label.clone()),
                b"{}".to_vec(),
                StorageRestoreArtifactSummary::new(2, "c".repeat(64)),
                "d".repeat(64),
                serde_json::json!({"compatible": true}),
                now - chrono::Duration::try_minutes(1).expect("valid duration"),
            ))
            .await
            .expect("certified backend should stage an expiring restore artifact");
        let expired_id = expired.summary().id();
        staged_ids.push(expired_id);
        assert!(
            backend
                .expire_restore_stage(expired_id)
                .await
                .expect("certified backend should expire a validated restore")
        );
        let expired_status = backend
            .get_restore_status(expired_id)
            .await
            .expect("expired restore should remain queryable");
        let (expired_summary, _, _) = expired_status.into_parts();
        assert_eq!(expired_summary.status(), StorageRestoreJobStatus::Expired);
    }

    crate::storage::postgres::with_connection(pool.get_ref(), async |conn| {
        use crate::schema::restore_jobs::dsl::{id, restore_jobs};

        diesel::delete(restore_jobs.filter(id.eq_any(staged_ids)))
            .execute(conn)
            .await
    })
    .await
    .expect("restore compatibility fixtures should be removed");
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_collection_lifecycle() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let group = crate::tests::create_test_group(pool.get_ref()).await;

    for backend in available_backends() {
        let command = StorageCollectionCreate::new(
            prefix("collection_lifecycle"),
            "collection lifecycle",
            group.id,
            None,
        );
        let collections = backend.collection_store();
        let created = backend
            .collection_store()
            .create_collection(command, None)
            .await
            .expect("certified backend should create collections");
        let updated = collections
            .update_collection(
                created.id(),
                StorageCollectionUpdate::new(
                    None,
                    Some("updated collection lifecycle".to_string()),
                ),
                Some(&EventContext::system()),
            )
            .await
            .expect("certified backend should update collections");
        assert_eq!(updated.description(), "updated collection lifecycle");
        let moved = collections
            .move_collection(created.id(), 1, None)
            .await
            .expect("certified backend should move collections");
        assert_eq!(moved.parent_collection_id(), Some(1));
        collections
            .delete_collection(created.id(), Some(&EventContext::system()))
            .await
            .expect("certified backend should delete collections");
    }

    group
        .delete_without_events(pool.get_ref())
        .await
        .expect("collection record compatibility group should be removed");
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
    let collection = crate::tests::create_collection_fixture(
        pool.get_ref(),
        &prefix("authorization_collection"),
    )
    .await;
    let needle = prefix("authorization_resource");
    let fixture = crate::tests::create_object_fixture(
        pool.get_ref(),
        collection,
        NewHubuumClass {
            name: format!("{needle}_class"),
            collection_id: 0,
            json_schema: None,
            validate_schema: Some(false),
            description: "authorization compatibility class".to_string(),
        },
        vec![NewHubuumObject {
            name: format!("{needle}_object"),
            collection_id: 0,
            hubuum_class_id: 0,
            data: serde_json::json!({}),
            description: "authorization compatibility object".to_string(),
        }],
    )
    .await
    .expect("authorization compatibility resource fixture should be created");
    let collection_id = fixture.collection_id();

    for backend in available_backends() {
        let principal = backend
            .load_authorization_principal(user.id)
            .await
            .expect("certified backend should supply authorization principal facts");
        assert!(principal.group_ids().contains(&group.id));

        let membership =
            AuthorizationGroupMembershipQuery::new(user.id, &group.groupname, LOCAL_IDENTITY_SCOPE);
        assert!(
            backend
                .authorization_principal_is_group_member(membership)
                .await
                .expect("certified backend should query group membership")
        );

        let classes = backend
            .load_authorization_classes(AuthorizationResourceIds::new([
                fixture.class.id,
                fixture.class.id,
            ]))
            .await
            .expect("certified backend should project authorization class facts");
        assert_eq!(classes.len(), 1);
        assert_eq!(classes[0].id(), fixture.class.id);
        assert_eq!(classes[0].collection_id(), collection_id);

        let objects = backend
            .load_authorization_objects(AuthorizationResourceIds::new([
                fixture.objects[0].id,
                fixture.objects[0].id,
            ]))
            .await
            .expect("certified backend should project authorization object facts");
        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0].id(), fixture.objects[0].id);
        assert_eq!(objects[0].collection_id(), collection_id);
        assert_eq!(objects[0].class_id(), fixture.class.id);
        assert_eq!(objects[0].name(), fixture.objects[0].name);

        let access_query = || {
            AuthorizationCollectionAccessQuery::new(
                user.id,
                collection_id,
                [AuthorizationPermission::ReadCollection],
            )
        };
        let batch_access_query = || {
            AuthorizationCollectionsAccessQuery::new(
                user.id,
                [collection_id, collection_id],
                [AuthorizationPermission::ReadCollection],
            )
        };
        assert!(
            !backend
                .authorize_local_collection(access_query())
                .await
                .expect("missing local grant should deny")
        );
        assert!(
            !backend
                .authorize_local_collections(batch_access_query())
                .await
                .expect("missing local batch grant should deny")
        );

        let key = AuthorizationGrantKey::new(collection_id, group.id);
        backend
            .apply_local_collection_grant(
                AuthorizationGrantMutation::new(
                    key,
                    [AuthorizationPermission::ReadCollection],
                    false,
                )
                .event_context(EventContext::system()),
            )
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
        let (permission_collection_id, permission_revision, permission_grants) = backend
            .load_local_collection_permission_set(AuthorizationPermissionSetQuery::new(
                collection_id,
                Some(group.id),
            ))
            .await
            .expect("certified backend should load revisioned permission sets")
            .into_parts();
        assert_eq!(permission_collection_id, collection_id);
        assert!(permission_revision > 0);
        assert_eq!(permission_grants.len(), 1);
        assert_eq!(permission_grants[0].group_id(), group.id);
        assert!(
            backend
                .authorize_local_collection(access_query())
                .await
                .expect("applied local grant should authorize")
        );
        assert!(
            backend
                .authorize_local_collections(batch_access_query())
                .await
                .expect("applied local grant should authorize the batch")
        );

        let page_options = || QueryOptions {
            filters: Vec::new(),
            sort: Vec::new(),
            limit: None,
            cursor: None,
            include_total: true,
        };
        let principal_query = || AuthorizationPrincipalCollectionQuery::new(user.id, collection_id);

        let principal_permissions = backend
            .principal_collection_permissions(principal_query())
            .await
            .expect("certified backend should project principal collection grants");
        assert!(
            principal_permissions
                .iter()
                .cloned()
                .any(|row| row.into_parts().0.id() == group.id)
        );

        let all_permissions = backend
            .principal_all_collection_permissions(user.id)
            .await
            .expect("certified backend should project all principal collection grants");
        assert!(all_permissions.iter().cloned().any(|row| {
            let (_, row_group, collection) = row.into_parts();
            collection.id() == collection_id && row_group.id() == group.id
        }));

        let (principal_page, principal_total) = backend
            .principal_collection_permissions_page(AuthorizationPrincipalCollectionPageQuery::new(
                principal_query(),
                page_options(),
            ))
            .await
            .expect("certified backend should page principal collection grants")
            .into_parts();
        assert!(principal_total >= 1);
        assert!(!principal_page.is_empty());

        let effective_principal = backend
            .effective_principal_collection_permissions(principal_query())
            .await
            .expect("certified backend should project effective principal grants");
        assert!(
            effective_principal
                .iter()
                .cloned()
                .any(|row| row.into_parts().4.id() == group.id)
        );

        let visible = backend
            .visible_collections(AuthorizationCollectionVisibilityQuery::new(
                user.id,
                AuthorizationPermission::ReadCollection,
                None,
            ))
            .await
            .expect("certified backend should project visible collections");
        assert!(
            visible
                .iter()
                .any(|collection| collection.id() == collection_id)
        );

        let group_query = AuthorizationGroupCollectionQuery::new(
            collection_id,
            group.id,
            AuthorizationPermission::ReadCollection,
        );
        assert!(
            backend
                .group_has_collection_permission(group_query)
                .await
                .expect("certified backend should test group collection grants")
        );

        let effective_group = backend
            .effective_group_collection_permissions(collection_id, group.id)
            .await
            .expect("certified backend should project effective group grants");
        assert!(!effective_group.is_empty());

        let groups_query = || {
            AuthorizationCollectionGroupsQuery::new(
                collection_id,
                AuthorizationPermission::ReadCollection,
            )
        };
        let groups = backend
            .groups_with_collection_permission(groups_query())
            .await
            .expect("certified backend should list groups with collection grants");
        assert!(groups.iter().any(|candidate| candidate.id() == group.id));

        let (groups_page, groups_total) = backend
            .groups_with_collection_permission_page(AuthorizationCollectionGroupsPageQuery::new(
                groups_query(),
                page_options(),
            ))
            .await
            .expect("certified backend should page groups with collection grants")
            .into_parts();
        assert!(groups_total >= 1);
        assert!(!groups_page.is_empty());

        let grant_query = || {
            AuthorizationCollectionGrantListQuery::new(
                collection_id,
                [AuthorizationPermission::ReadCollection],
                page_options(),
            )
        };
        let grants = backend
            .list_collection_group_permissions(grant_query())
            .await
            .expect("certified backend should list collection grants");
        assert!(
            grants
                .iter()
                .cloned()
                .any(|row| row.into_parts().0.id() == group.id)
        );

        let (grant_page, grant_total) = backend
            .list_collection_group_permissions_page(grant_query())
            .await
            .expect("certified backend should page collection grants")
            .into_parts();
        assert!(grant_total >= 1);
        assert!(!grant_page.is_empty());

        let grant = backend
            .collection_group_permission(collection_id, group.id)
            .await
            .expect("certified backend should load a collection grant");
        assert_eq!(grant.collection_id(), collection_id);
        assert_eq!(grant.group_id(), group.id);

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
                .any(|collection| collection.id() == collection_id)
        );

        let page = backend
            .list_local_collection_grants(AuthorizationCollectionGrantListQuery::new(
                collection_id,
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
                .any(|collection| collection.id() == collection_id)
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
                && collection.id() == collection_id
        }));

        backend
            .revoke_local_collection_grant(
                AuthorizationGrantMutation::new(
                    key,
                    [AuthorizationPermission::ReadCollection],
                    false,
                )
                .event_context(EventContext::system()),
            )
            .await
            .expect("certified backend should revoke selected local permissions");
        assert!(
            !backend
                .authorize_local_collection(access_query())
                .await
                .expect("revoked local grant should deny")
        );
        backend
            .revoke_all_local_collection_grants(
                AuthorizationGrantDelete::new(key).event_context(EventContext::system()),
            )
            .await
            .expect("certified backend should remove the local grant row");
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

    for backend in available_backends() {
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
                .expect("certified backend should query remote-target history as of a point")
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

    for backend in available_backends() {
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
                StorageVisibility::new(i32::MAX, true, None::<Vec<AuthorizationPermission>>, None),
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

    for backend in available_backends() {
        let (options, passthrough) = parse_query_parameter_with_computed_filters_and_passthrough(
            &format!("computed.shared.compatibility__equals={needle}&sort=id"),
            &[],
        )
        .expect("computed compatibility query should parse");
        assert!(passthrough.is_empty());
        let visibility =
            StorageVisibility::new(i32::MAX, true, None::<Vec<AuthorizationPermission>>, None);
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
                    StorageRecordMetadata::new(
                        object.id,
                        object.created_at,
                        object.updated_at,
                        object.revision.get(),
                    ),
                    object.name.clone(),
                    object.collection_id,
                    object.hubuum_class_id,
                    object.data.clone(),
                    object.description.clone(),
                )],
                None,
            ))
            .await
            .expect("certified backend should enrich objects with computed values");
        assert_eq!(enriched.len(), 1);
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

    for backend in available_backends() {
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
        let rebuild_task_id = rebuild_state
            .active_task_id()
            .expect("rebuild request should identify its task");
        let claim_token = uuid::Uuid::new_v4();
        crate::storage::postgres::with_connection(pool.get_ref(), async |conn| {
            use crate::schema::tasks::dsl::{id, lease_expires_at, lease_token, status, tasks};
            diesel::update(tasks.filter(id.eq(rebuild_task_id)))
                .set((
                    status.eq(StorageTaskStatus::Validating.as_str()),
                    lease_token.eq(Some(claim_token)),
                    lease_expires_at.eq(Some(
                        chrono::Utc::now().naive_utc()
                            + chrono::Duration::try_minutes(1).expect("valid compatibility lease"),
                    )),
                ))
                .execute(conn)
                .await
        })
        .await
        .expect("compatibility rebuild should receive a live backend claim");
        let rebuilt = backend
            .execute_computed_field_rebuild(StorageTaskLease::new(
                rebuild_task_id,
                StorageTaskClaimToken::new(claim_token.to_string()),
            ))
            .await
            .expect("certified backend should execute a claimed computed-field rebuild");
        assert_eq!(rebuilt.status(), StorageTaskStatus::Succeeded);
        let ready_state = backend
            .computed_field_state(class_id)
            .await
            .expect("certified backend should expose the completed rebuild state");
        assert_eq!(ready_state.rebuild_status(), "ready");
        assert_eq!(ready_state.active_task_id(), None);

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

    for backend in available_backends() {
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

    for backend in available_backends() {
        let visibility =
            || StorageVisibility::new(i32::MAX, true, None::<Vec<AuthorizationPermission>>, None);
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
                .class_relations_touching_ids(RelationIdsQuery::new(class_ids, visibility(),))
                .await
                .expect("certified backend should query class relations touching ids")
                .len(),
            1
        );
        assert_eq!(
            backend
                .class_relations_between_ids(RelationIdsQuery::new(class_ids, visibility(),))
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
                    ObjectRelationsTouchingIdsQuery::new([object_one.id], 10, visibility(),)
                        .excluding_relation_ids([object_relation.id]),
                )
                .await
                .expect("certified backend should exclude previously visited relations")
                .is_empty()
        );
        assert_eq!(
            backend
                .object_relations_between_ids(RelationIdsQuery::new(object_ids, visibility(),))
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
                RelatedObjectsForRootsQuery::new([object_one.id], class_two.id, visibility())
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
            .bidirectionally_related_objects_for_roots(BidirectionalRelatedObjectsQuery::new(
                [object_one.id],
                1,
                10,
                false,
                visibility(),
            ))
            .await
            .expect("certified backend should traverse bidirectional root graphs");
        assert_eq!(bidirectional.len(), 1);
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

    for backend in available_backends() {
        let request = || {
            UnifiedSearchQuery::new(
                needle.clone(),
                10,
                StorageVisibility::new(i32::MAX, true, None::<Vec<AuthorizationPermission>>, None),
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

    fixture
        .cleanup()
        .await
        .expect("unified search compatibility fixture should be removed");
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_operational_state() {
    let _permit = postgres_permit().await;

    for backend in available_backends() {
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
        let storage = backend
            .storage_snapshot()
            .await
            .expect("certified backend should expose database diagnostics");
        assert!(storage.active_sessions() >= 0);
        assert!(storage.storage_bytes() > 0);
        let task_queue = backend
            .task_queue_snapshot()
            .await
            .expect("certified backend should expose task queue diagnostics");
        assert!(task_queue.statuses().total() >= 0);
        assert!(task_queue.total_task_events() >= 0);
        let export_health = backend
            .export_template_health()
            .await
            .expect("certified backend should aggregate export-template health");
        assert!(export_health.iter().all(|row| row.runs() > 0));
        let audit_entries = backend
            .export_templates_for_audit()
            .await
            .expect("certified backend should supply the template audit set");
        assert!(audit_entries.windows(2).all(|entries| {
            (entries[0].collection_id(), entries[0].id())
                <= (entries[1].collection_id(), entries[1].id())
        }));
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_event_health() {
    let _permit = postgres_permit().await;

    for backend in available_backends() {
        backend
            .event_delivery_health()
            .await
            .expect("certified backend should expose event delivery health");
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_complete_event_administration() {
    let _permit = postgres_permit().await;
    let options = || QueryOptions {
        filters: Vec::new(),
        sort: Vec::new(),
        limit: Some(50),
        cursor: None,
        include_total: true,
    };
    let fanout_settings = EventFanoutSettings::new(1_000, 30_000)
        .expect("compatibility fan-out settings should be valid");

    for backend in available_backends() {
        let event_context = EventContext::system();
        let sink_name = prefix("event_admin_sink");
        let sink = backend
            .create_event_sink(
                StorageEventSinkCreate::builder(sink_name, "webhook", event_context.clone())
                    .configuration(serde_json::json!({}))
                    .enabled(true)
                    .build(),
            )
            .await
            .expect("certified backend should create event sinks");
        let sink_id = sink.id();

        assert!(
            backend
                .enabled_event_sink_count()
                .await
                .expect("certified backend should count enabled event sinks")
                >= 1
        );
        assert_eq!(
            backend
                .load_event_sink(sink_id)
                .await
                .expect("certified backend should load event sinks")
                .id(),
            sink_id
        );
        let (sinks, sink_total) = backend
            .list_event_sinks(StorageEventSinkListQuery::new(options()))
            .await
            .expect("certified backend should list event sinks")
            .into_parts();
        assert!(!sinks.is_empty());
        assert!(sink_total.is_some_and(|total| total >= 1));
        let updated_sink = backend
            .update_event_sink(
                StorageEventSinkUpdate::new(sink_id, event_context.clone())
                    .name(Some(prefix("event_admin_sink_updated"))),
            )
            .await
            .expect("certified backend should update event sinks");
        assert!(updated_sink.revision() > sink.revision());

        let subscription = backend
            .create_event_subscription(
                StorageEventSubscriptionCreate::builder(
                    1,
                    sink_id,
                    prefix("event_admin_subscription"),
                    event_context.clone(),
                )
                .description("storage compatibility event subscription")
                .entity_types(vec![EntityType::EventSubscription.as_str().to_string()])
                .actions(vec![Action::Created.as_str().to_string()])
                .routing(serde_json::json!({}))
                .enabled(true)
                .build(),
            )
            .await
            .expect("certified backend should create event subscriptions");
        let subscription_id = subscription.id();
        assert_eq!(
            backend
                .load_event_subscription(1, subscription_id)
                .await
                .expect("certified backend should load scoped subscriptions")
                .collection_id(),
            1
        );
        let (subscriptions, subscription_total) = backend
            .list_event_subscriptions(StorageEventSubscriptionListQuery::new(1, options()))
            .await
            .expect("certified backend should list scoped subscriptions")
            .into_parts();
        assert!(!subscriptions.is_empty());
        assert!(subscription_total.is_some_and(|total| total >= 1));
        let updated_subscription = backend
            .update_event_subscription(
                StorageEventSubscriptionUpdate::new(1, subscription_id, event_context.clone())
                    .description(Some(
                        "updated storage compatibility subscription".to_string(),
                    )),
            )
            .await
            .expect("certified backend should update event subscriptions");
        assert!(updated_subscription.revision() > subscription.revision());

        let (audit_events, audit_total) = backend
            .list_audit_events(StorageAuditEventListQuery::new(
                vec![1],
                false,
                StorageAuditEventFilters::new()
                    .entity_type(Some(EntityType::EventSubscription))
                    .entity_id(Some(subscription_id)),
                options(),
            ))
            .await
            .expect("certified backend should list event audit records")
            .into_parts();
        assert!(audit_events.len() >= 2);
        assert!(audit_total.is_some_and(|total| total >= 2));

        let mut delivery = None;
        for _ in 0..20 {
            let (deliveries, total) = backend
                .list_event_deliveries(
                    StorageEventDeliveryListQuery::new(options())
                        .subscription_id(Some(subscription_id)),
                )
                .await
                .expect("certified backend should list event deliveries")
                .into_parts();
            assert!(total.is_some());
            if let Some(row) = deliveries.into_iter().next() {
                delivery = Some(row);
                break;
            }
            backend
                .process_event_fanout_batch(fanout_settings)
                .await
                .expect("certified backend should fan out lifecycle events");
        }
        let delivery =
            delivery.expect("event-administration compatibility event should produce a delivery");
        let delivery_id = delivery.id();
        let dead = backend
            .mark_event_delivery_dead(delivery_id)
            .await
            .expect("certified backend should dead-letter event deliveries");
        assert_eq!(dead.status(), "dead");
        let pending = backend
            .release_event_delivery_for_retry(delivery_id)
            .await
            .expect("certified backend should release event deliveries for retry");
        assert_eq!(pending.status(), "pending");
        assert_eq!(
            backend
                .load_event_delivery(delivery_id)
                .await
                .expect("certified backend should load event deliveries")
                .id(),
            delivery_id
        );

        backend
            .delete_event_subscription(StorageEventSubscriptionDelete::new(
                1,
                subscription_id,
                event_context.clone(),
            ))
            .await
            .expect("event-subscription compatibility fixture should be removed");
        backend
            .delete_event_sink(StorageEventSinkDelete::new(sink_id, event_context))
            .await
            .expect("event-sink compatibility fixture should be removed");
    }
}

#[actix_web::test]
async fn every_available_storage_backend_processes_event_fanout() {
    let _permit = postgres_permit().await;
    let settings = EventFanoutSettings::new(10, 30_000)
        .expect("compatibility fan-out settings should be valid");

    for backend in available_backends() {
        backend
            .process_event_fanout_batch(settings)
            .await
            .expect("certified backend should process event fan-out");
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

    for backend in available_backends() {
        let summary = backend
            .process_event_retention_batch(settings, &DiscardArchive)
            .await
            .expect("certified backend should process event retention");

        assert!(!summary.did_work());
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

    for backend in available_backends() {
        let purged = backend
            .purge_expired_tokens(settings)
            .await
            .expect("certified backend should execute token retention");

        assert_eq!(purged, 0);
    }
}

#[actix_web::test]
async fn every_available_storage_backend_composes_through_services_and_http() {
    let _permit = postgres_permit().await;
    let postgres_pool = pool();
    let config = crate::tests::integration_test_config()
        .expect("backend compatibility configuration should be valid");

    for kind in StorageBackendKind::ALL {
        let fixture = backend_application_fixture(kind, postgres_pool.get_ref()).await;
        let backend = fixture.backend.clone();
        fn accepts_event_delivery_contract(_backend: &impl EventDeliveryStorage) {}
        fn accepts_worker_notification_contract(_backend: &impl WorkerNotificationStorage) {}
        fn accepts_event_administration_contract(
            _backend: &(
                 impl AuditEventStorage + EventSubscriptionStorage + EventDeliveryAdministrationStorage
             ),
        ) {
        }
        accepts_event_delivery_contract(&backend);
        accepts_worker_notification_contract(&backend);
        accepts_event_administration_contract(&backend);
        let descriptor = backend.descriptor();
        assert_eq!(descriptor.kind(), kind);

        let services = Services::from_storage(backend.clone());
        let root = services
            .collections()
            .get(CollectionID::new(1).expect("valid root collection id"))
            .await
            .expect("certified backend should serve lifecycle operations");
        assert_eq!(root.id, 1);

        let permissions = Arc::new(LocalPermissionBackend::new(
            backend.clone(),
            config.admin_groupname.clone(),
        ));
        let app = test::init_service(
            App::new()
                .wrap(actix_web::middleware::from_fn(
                    crate::middlewares::actor_context,
                ))
                .app_data(Data::new(AppContext::new(backend, permissions)))
                .configure(crate::api::config),
        )
        .await;

        let ready = test::TestRequest::get().uri("/readyz").to_request();
        let ready = test::call_service(&app, ready).await;
        assert_eq!(ready.status(), http::StatusCode::OK);

        let authorization = (
            http::header::AUTHORIZATION,
            format!("Bearer {}", fixture.bearer_token),
        );
        let point = test::TestRequest::get()
            .uri("/api/v1/collections/1")
            .insert_header(authorization.clone())
            .to_request();
        let point = test::call_service(&app, point).await;
        assert_eq!(point.status(), http::StatusCode::OK);
        let point: crate::models::Collection = test::read_body_json(point).await;
        assert_eq!(point.id, 1);

        let list = test::TestRequest::get()
            .uri("/api/v1/collections?limit=10")
            .insert_header(authorization)
            .to_request();
        let list = test::call_service(&app, list).await;
        assert_eq!(list.status(), http::StatusCode::OK);
        let listed: Vec<crate::models::Collection> = test::read_body_json(list).await;
        assert!(listed.iter().any(|collection| collection.id == 1));

        drop(app);
        fixture.cleanup(postgres_pool.get_ref()).await;
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
