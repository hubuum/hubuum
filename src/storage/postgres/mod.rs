mod backup_snapshot;
mod computed_fields;
mod error;
#[doc(hidden)]
pub mod operations;
mod remote_targets;
mod restores;
mod runtime;
mod task_execution;
mod task_queue;

pub use runtime::*;

use async_trait::async_trait;

use crate::events::{EventContext, EventFanoutSettings, EventRetentionSettings};
use crate::models::search::QueryOptions;
use crate::models::{
    ClassSelector, Collection, CollectionID, HubuumClass, HubuumClassRelationID, HubuumObject,
    MaintenanceState, NewCollectionWithAssignee, NewHubuumClass, NewHubuumClassRelation,
    NewHubuumObject, ObjectDataPatchDocument, ObjectRelationCreateSelector, ObjectRelationSelector,
    ObjectSelector, PreparedClassRelation, PreparedObjectRelation, ResolvedClassRelationTarget,
    ResolvedClassTarget, ResolvedObjectRelationTarget, ResolvedObjectTarget,
    TokenRetentionSettings, UpdateCollection, UpdateHubuumClass, UpdateHubuumObject,
};
use crate::storage::postgres::operations::GetCollection;
use crate::storage::postgres::operations::class::{
    CreateClassRecord, DeleteResolvedClassRecord, ResolveClassSelectorRecord,
    UpdateResolvedClassRecord,
};
use crate::storage::postgres::operations::collection::{
    DeleteCollectionRecord, SaveCollectionWithAssigneeRecord, UpdateCollectionRecord,
    collection_ancestors_from_backend, collection_children_from_backend,
    move_collection_record_from_backend,
};
use crate::storage::postgres::operations::object::{
    CreateObjectInResolvedClassRecord, DeleteResolvedObjectRecord, PatchObjectDataRecord,
    ResolveObjectSelectorRecord, UpdateResolvedObjectRecord,
};
use crate::storage::postgres::operations::relations::{
    CreatePreparedClassRelationRecord, CreatePreparedObjectRelationRecord,
    DeleteResolvedClassRelationRecord, DeleteResolvedObjectRelationRecord,
    PrepareClassRelationRecord, PrepareObjectRelationRecord, ResolveClassRelationTargetRecord,
    ResolveObjectRelationTargetRecord,
};

use super::{
    AuthenticationIdentity, AuthenticationStorage, AuthenticationTokenScope,
    AuthenticationTokenScopeQuery, AuthorizationCollection, AuthorizationCollectionAccessQuery,
    AuthorizationCollectionGrantListQuery, AuthorizationCollectionsQuery, AuthorizationGrant,
    AuthorizationGrantKey, AuthorizationGrantMutation, AuthorizationGroup,
    AuthorizationGroupGrantPage, AuthorizationGroupMembershipQuery, AuthorizationPolicySnapshotRow,
    AuthorizationPrincipal, AuthorizationStorage, BidirectionalRelatedObjectsQuery,
    CatalogListQuery, CatalogPage, CatalogStorage, ClassRelationStore, ClassStore, CollectionStore,
    ComputedObjectEnrichmentQuery, ComputedObjectListQuery, ComputedObjectPage,
    ComputedObjectStorage, EventArchive, EventDeliveryBatch, EventDeliveryClaim,
    EventDeliveryHealthSnapshot, EventDeliveryStorage, EventFanoutStorage, EventHealthStorage,
    EventMetricsSnapshot, EventRetentionStorage, EventRetentionSummary,
    ExportTemplateHistoryRecord, HistoryAsOfQuery, HistoryCollectionScope, HistoryListQuery,
    HistoryPage, HistoryPrincipalName, HistoryStorage, InventoryGaugeSnapshot, MetricsStorage,
    ObjectAggregateAuthorizer, ObjectAggregateStorage, ObjectAggregateStorageQuery,
    ObjectHistoryAsOfQuery, ObjectHistoryListQuery, ObjectHistoryRecord, ObjectRelationStore,
    ObjectRelationsTouchingIdsQuery, ObjectStore, OperationalStateStorage, ReadinessSnapshot,
    RelatedObjectsForRootsQuery, RelationGraphQuery, RelationIdsQuery, RelationListQuery,
    RelationPage, RelationQueryStorage, RelationTouchingQuery, RemoteTargetHistoryRecord,
    StorageClass, StorageClassGraphRow, StorageClassRelation, StorageCollection,
    StorageComputedObject, StorageError, StorageIdentity, StorageObject,
    StorageObjectAggregatePage, StorageObjectGraphRow, StorageObjectRelation, StoragePoolState,
    StorageRelatedObjectForRootRow, StorageRelatedObjectIncludeRow, TaskGaugeSnapshot,
    TokenRetentionStorage, UnifiedSearchClass, UnifiedSearchCollection, UnifiedSearchObject,
    UnifiedSearchQuery, UnifiedSearchStorage,
};
use super::{ClassHistoryRecord, CollectionHistoryRecord};
use error::map_postgres_error;

/// Canonical production storage adapter.
#[derive(Clone)]
pub(crate) struct PostgresStorage {
    pool: PostgresPool,
}

impl PostgresStorage {
    pub(crate) fn new(pool: PostgresPool) -> Self {
        Self { pool }
    }

    pub(crate) fn pool(&self) -> &PostgresPool {
        &self.pool
    }
}

impl StorageIdentity for PostgresStorage {
    fn storage_name(&self) -> &'static str {
        "postgresql"
    }
}

#[async_trait]
impl AuthenticationStorage for PostgresStorage {
    async fn load_authentication_identity(
        &self,
        principal_id: i32,
    ) -> Result<AuthenticationIdentity, StorageError> {
        operations::authentication::load_authentication_identity(&self.pool, principal_id)
            .await
            .map_err(map_postgres_error)
    }

    async fn load_authentication_token_scope(
        &self,
        query: AuthenticationTokenScopeQuery,
    ) -> Result<Option<AuthenticationTokenScope>, StorageError> {
        operations::authentication::load_authentication_token_scope(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }
}

#[async_trait]
impl AuthorizationStorage for PostgresStorage {
    async fn load_authorization_principal(
        &self,
        principal_id: i32,
    ) -> Result<AuthorizationPrincipal, StorageError> {
        operations::authorization::load_authorization_principal(&self.pool, principal_id)
            .await
            .map_err(map_postgres_error)
    }

    async fn authorization_principal_is_group_member(
        &self,
        query: AuthorizationGroupMembershipQuery,
    ) -> Result<bool, StorageError> {
        operations::authorization::authorization_principal_is_group_member(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn authorize_local_collection(
        &self,
        query: AuthorizationCollectionAccessQuery,
    ) -> Result<bool, StorageError> {
        operations::authorization::authorize_local_collection(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn local_authorized_collections(
        &self,
        query: AuthorizationCollectionsQuery,
    ) -> Result<Vec<AuthorizationCollection>, StorageError> {
        operations::authorization::local_authorized_collections(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn list_authorization_collection_candidates(
        &self,
    ) -> Result<Vec<AuthorizationCollection>, StorageError> {
        operations::authorization::list_authorization_collection_candidates(&self.pool)
            .await
            .map_err(map_postgres_error)
    }

    async fn list_authorization_group_candidates(
        &self,
        query_options: QueryOptions,
    ) -> Result<Vec<AuthorizationGroup>, StorageError> {
        operations::authorization::list_authorization_group_candidates(&self.pool, query_options)
            .await
            .map_err(map_postgres_error)
    }

    async fn authorization_policy_snapshot(
        &self,
    ) -> Result<Vec<AuthorizationPolicySnapshotRow>, StorageError> {
        operations::authorization::authorization_policy_snapshot(&self.pool)
            .await
            .map_err(map_postgres_error)
    }

    async fn list_local_collection_grants(
        &self,
        query: AuthorizationCollectionGrantListQuery,
    ) -> Result<AuthorizationGroupGrantPage, StorageError> {
        operations::authorization::list_local_collection_grants(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn get_local_collection_grant(
        &self,
        key: AuthorizationGrantKey,
    ) -> Result<Option<AuthorizationGrant>, StorageError> {
        operations::authorization::get_local_collection_grant(&self.pool, key)
            .await
            .map_err(map_postgres_error)
    }

    async fn apply_local_collection_grant(
        &self,
        mutation: AuthorizationGrantMutation,
    ) -> Result<AuthorizationGrant, StorageError> {
        operations::authorization::apply_local_collection_grant(&self.pool, mutation)
            .await
            .map_err(map_postgres_error)
    }

    async fn revoke_local_collection_grant(
        &self,
        mutation: AuthorizationGrantMutation,
    ) -> Result<AuthorizationGrant, StorageError> {
        operations::authorization::revoke_local_collection_grant(&self.pool, mutation)
            .await
            .map_err(map_postgres_error)
    }

    async fn revoke_all_local_collection_grants(
        &self,
        key: AuthorizationGrantKey,
    ) -> Result<(), StorageError> {
        operations::authorization::revoke_all_local_collection_grants(&self.pool, key)
            .await
            .map_err(map_postgres_error)
    }
}

fn history_collection_filter(
    scope: &HistoryCollectionScope,
) -> operations::history::HistoryCollectionFilter<'_> {
    match scope {
        HistoryCollectionScope::All => operations::history::HistoryCollectionFilter::All,
        HistoryCollectionScope::Visible(collection_ids) => {
            operations::history::HistoryCollectionFilter::Visible(collection_ids)
        }
    }
}

#[async_trait]
impl HistoryStorage for PostgresStorage {
    async fn resolve_history_principal_names(
        &self,
        principal_ids: Vec<i32>,
    ) -> Result<Vec<HistoryPrincipalName>, StorageError> {
        operations::history::resolve_principal_name_rows(&self.pool, principal_ids)
            .await
            .map(|rows| {
                rows.into_iter()
                    .map(operations::history::principal_name_to_storage)
                    .collect()
            })
            .map_err(map_postgres_error)
    }

    async fn list_collection_history(
        &self,
        query: HistoryListQuery,
    ) -> Result<HistoryPage<CollectionHistoryRecord>, StorageError> {
        let (entity_id, query_options, scope) = query.into_parts();
        operations::history::collection_history_paginated_with_total_count(
            entity_id,
            &self.pool,
            &query_options,
            history_collection_filter(&scope),
        )
        .await
        .map(|(rows, total)| {
            HistoryPage::new(
                rows.into_iter()
                    .map(operations::history::collection_history_to_storage)
                    .collect(),
                total,
            )
        })
        .map_err(map_postgres_error)
    }

    async fn collection_history_as_of(
        &self,
        query: HistoryAsOfQuery,
    ) -> Result<Option<CollectionHistoryRecord>, StorageError> {
        let (entity_id, at) = query.into_parts();
        operations::history::collection_as_of(entity_id, at, &self.pool)
            .await
            .map(|row| row.map(operations::history::collection_history_to_storage))
            .map_err(map_postgres_error)
    }

    async fn list_class_history(
        &self,
        query: HistoryListQuery,
    ) -> Result<HistoryPage<ClassHistoryRecord>, StorageError> {
        let (entity_id, query_options, scope) = query.into_parts();
        operations::history::class_history_paginated_with_total_count(
            entity_id,
            &self.pool,
            &query_options,
            history_collection_filter(&scope),
        )
        .await
        .map(|(rows, total)| {
            HistoryPage::new(
                rows.into_iter()
                    .map(operations::history::class_history_to_storage)
                    .collect(),
                total,
            )
        })
        .map_err(map_postgres_error)
    }

    async fn class_history_as_of(
        &self,
        query: HistoryAsOfQuery,
    ) -> Result<Option<ClassHistoryRecord>, StorageError> {
        let (entity_id, at) = query.into_parts();
        operations::history::class_as_of(entity_id, at, &self.pool)
            .await
            .map(|row| row.map(operations::history::class_history_to_storage))
            .map_err(map_postgres_error)
    }

    async fn list_object_history(
        &self,
        query: ObjectHistoryListQuery,
    ) -> Result<HistoryPage<ObjectHistoryRecord>, StorageError> {
        let (object_id, class_id, query_options, scope) = query.into_parts();
        operations::history::object_history_paginated_with_total_count(
            object_id,
            class_id,
            &self.pool,
            &query_options,
            history_collection_filter(&scope),
        )
        .await
        .map(|(rows, total)| {
            HistoryPage::new(
                rows.into_iter()
                    .map(operations::history::object_history_to_storage)
                    .collect(),
                total,
            )
        })
        .map_err(map_postgres_error)
    }

    async fn object_history_as_of(
        &self,
        query: ObjectHistoryAsOfQuery,
    ) -> Result<Option<ObjectHistoryRecord>, StorageError> {
        let (object_id, class_id, at) = query.into_parts();
        operations::history::object_as_of(object_id, class_id, at, &self.pool)
            .await
            .map(|row| row.map(operations::history::object_history_to_storage))
            .map_err(map_postgres_error)
    }

    async fn list_export_template_history(
        &self,
        query: HistoryListQuery,
    ) -> Result<HistoryPage<ExportTemplateHistoryRecord>, StorageError> {
        let (entity_id, query_options, scope) = query.into_parts();
        operations::history::export_template_history_paginated_with_total_count(
            entity_id,
            &self.pool,
            &query_options,
            history_collection_filter(&scope),
        )
        .await
        .map(|(rows, total)| {
            HistoryPage::new(
                rows.into_iter()
                    .map(operations::history::export_template_history_to_storage)
                    .collect(),
                total,
            )
        })
        .map_err(map_postgres_error)
    }

    async fn export_template_history_as_of(
        &self,
        query: HistoryAsOfQuery,
    ) -> Result<Option<ExportTemplateHistoryRecord>, StorageError> {
        let (entity_id, at) = query.into_parts();
        operations::history::export_template_as_of(entity_id, at, &self.pool)
            .await
            .map(|row| row.map(operations::history::export_template_history_to_storage))
            .map_err(map_postgres_error)
    }

    async fn list_remote_target_history(
        &self,
        query: HistoryListQuery,
    ) -> Result<HistoryPage<RemoteTargetHistoryRecord>, StorageError> {
        let (entity_id, query_options, scope) = query.into_parts();
        operations::history::remote_target_history_paginated_with_total_count(
            entity_id,
            &self.pool,
            &query_options,
            history_collection_filter(&scope),
        )
        .await
        .map(|(rows, total)| {
            HistoryPage::new(
                rows.into_iter()
                    .map(operations::history::remote_target_history_to_storage)
                    .collect(),
                total,
            )
        })
        .map_err(map_postgres_error)
    }

    async fn remote_target_history_as_of(
        &self,
        query: HistoryAsOfQuery,
    ) -> Result<Option<RemoteTargetHistoryRecord>, StorageError> {
        let (entity_id, at) = query.into_parts();
        operations::history::remote_target_as_of(entity_id, at, &self.pool)
            .await
            .map(|row| row.map(operations::history::remote_target_history_to_storage))
            .map_err(map_postgres_error)
    }
}

#[async_trait]
impl CatalogStorage for PostgresStorage {
    async fn list_collections(
        &self,
        query: CatalogListQuery,
    ) -> Result<CatalogPage<StorageCollection>, StorageError> {
        operations::catalog::list_collections(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn list_classes(
        &self,
        query: CatalogListQuery,
    ) -> Result<CatalogPage<StorageClass>, StorageError> {
        operations::catalog::list_classes(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn list_objects(
        &self,
        query: CatalogListQuery,
    ) -> Result<CatalogPage<StorageObject>, StorageError> {
        operations::catalog::list_objects(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }
}

#[async_trait]
impl ComputedObjectStorage for PostgresStorage {
    async fn list_computed_objects(
        &self,
        query: ComputedObjectListQuery,
    ) -> Result<ComputedObjectPage, StorageError> {
        operations::computed_objects::list_computed_objects(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn enrich_objects_with_computed(
        &self,
        query: ComputedObjectEnrichmentQuery,
    ) -> Result<Vec<StorageComputedObject>, StorageError> {
        operations::computed_objects::enrich_computed_objects(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }
}

#[async_trait]
impl ObjectAggregateStorage for PostgresStorage {
    async fn aggregate_objects(
        &self,
        query: ObjectAggregateStorageQuery,
        authorizer: Option<&dyn ObjectAggregateAuthorizer>,
    ) -> Result<StorageObjectAggregatePage, StorageError> {
        operations::user::aggregate_objects(&self.pool, query, authorizer)
            .await
            .map_err(map_postgres_error)
    }
}

#[async_trait]
impl RelationQueryStorage for PostgresStorage {
    async fn list_class_relations(
        &self,
        query: RelationListQuery,
    ) -> Result<RelationPage<StorageClassRelation>, StorageError> {
        operations::relation_query::list_class_relations(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn list_object_relations(
        &self,
        query: RelationListQuery,
    ) -> Result<RelationPage<StorageObjectRelation>, StorageError> {
        operations::relation_query::list_object_relations(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn list_class_relations_touching(
        &self,
        query: RelationTouchingQuery,
    ) -> Result<RelationPage<StorageClassRelation>, StorageError> {
        operations::relation_query::list_class_relations_touching(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn list_object_relations_touching(
        &self,
        query: RelationTouchingQuery,
    ) -> Result<RelationPage<StorageObjectRelation>, StorageError> {
        operations::relation_query::list_object_relations_touching(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn class_relations_touching_ids(
        &self,
        query: RelationIdsQuery,
    ) -> Result<Vec<StorageClassRelation>, StorageError> {
        operations::relation_query::class_relations_touching_ids(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn class_relations_between_ids(
        &self,
        query: RelationIdsQuery,
    ) -> Result<Vec<StorageClassRelation>, StorageError> {
        operations::relation_query::class_relations_between_ids(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn object_relations_between_ids(
        &self,
        query: RelationIdsQuery,
    ) -> Result<Vec<StorageObjectRelation>, StorageError> {
        operations::relation_query::object_relations_between_ids(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn object_relations_touching_ids(
        &self,
        query: ObjectRelationsTouchingIdsQuery,
    ) -> Result<Vec<StorageObjectRelation>, StorageError> {
        operations::relation_query::object_relations_touching_ids(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn related_classes(
        &self,
        query: RelationGraphQuery,
    ) -> Result<RelationPage<StorageClassGraphRow>, StorageError> {
        operations::relation_query::related_classes(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn related_objects(
        &self,
        query: RelationGraphQuery,
    ) -> Result<RelationPage<StorageObjectGraphRow>, StorageError> {
        operations::relation_query::related_objects(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn related_objects_for_roots(
        &self,
        query: RelatedObjectsForRootsQuery,
    ) -> Result<Vec<StorageRelatedObjectIncludeRow>, StorageError> {
        operations::relation_query::related_objects_for_roots(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn bidirectionally_related_objects_for_roots(
        &self,
        query: BidirectionalRelatedObjectsQuery,
    ) -> Result<Vec<StorageRelatedObjectForRootRow>, StorageError> {
        operations::relation_query::bidirectionally_related_objects_for_roots(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }
}

#[async_trait]
impl UnifiedSearchStorage for PostgresStorage {
    async fn search_unified_collections(
        &self,
        query: UnifiedSearchQuery,
    ) -> Result<Vec<UnifiedSearchCollection>, StorageError> {
        operations::ranked_search::search_collections(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn search_unified_classes(
        &self,
        query: UnifiedSearchQuery,
    ) -> Result<Vec<UnifiedSearchClass>, StorageError> {
        operations::ranked_search::search_classes(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn search_unified_objects(
        &self,
        query: UnifiedSearchQuery,
    ) -> Result<Vec<UnifiedSearchObject>, StorageError> {
        operations::ranked_search::search_objects(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }
}

#[async_trait]
impl MetricsStorage for PostgresStorage {
    fn metrics_pool_state(&self) -> StoragePoolState {
        let state = self.pool.state();
        let max_connections = self.pool.config().max_size;
        let in_use_connections = state.connections.saturating_sub(state.idle_connections);
        StoragePoolState {
            max_connections,
            total_connections: state.connections,
            available_connections: max_connections.saturating_sub(in_use_connections),
            idle_connections: state.idle_connections,
            in_use_connections,
            pending_acquisitions: state.statistics.pending_gets(),
            acquisitions_started: state.statistics.get_started,
            acquisitions_direct: state.statistics.get_direct,
            acquisitions_waited: state.statistics.get_waited,
            acquisitions_timed_out: state.statistics.get_timed_out,
            acquisition_wait_time_ms: u64::try_from(state.statistics.get_wait_time.as_millis())
                .unwrap_or(u64::MAX),
            connections_created: state.statistics.connections_created,
            connections_closed_broken: state.statistics.connections_closed_broken,
            connections_closed_invalid: state.statistics.connections_closed_invalid,
            connections_closed_max_lifetime: state.statistics.connections_closed_max_lifetime,
            connections_closed_idle_timeout: state.statistics.connections_closed_idle_timeout,
        }
    }

    async fn metrics_inventory_snapshot(&self) -> Result<InventoryGaugeSnapshot, StorageError> {
        operations::metrics::load_inventory_gauge_snapshot(&self.pool)
            .await
            .map_err(map_postgres_error)
    }

    async fn metrics_task_snapshot(&self) -> Result<TaskGaugeSnapshot, StorageError> {
        operations::metrics::load_task_gauge_snapshot(&self.pool)
            .await
            .map_err(map_postgres_error)
    }

    async fn metrics_event_snapshot(&self) -> Result<EventMetricsSnapshot, StorageError> {
        operations::event_observability::load_event_metrics_snapshot(&self.pool)
            .await
            .map_err(map_postgres_error)
    }
}

#[async_trait]
impl OperationalStateStorage for PostgresStorage {
    async fn readiness_snapshot(&self) -> Result<ReadinessSnapshot, StorageError> {
        operations::probe::load_readiness_snapshot(&self.pool)
            .await
            .map_err(map_postgres_error)
    }

    async fn maintenance_state(&self) -> Result<MaintenanceState, StorageError> {
        operations::maintenance::load_maintenance_state(&self.pool)
            .await
            .map_err(map_postgres_error)
    }
}

#[async_trait]
impl EventHealthStorage for PostgresStorage {
    async fn event_delivery_health(&self) -> Result<EventDeliveryHealthSnapshot, StorageError> {
        operations::event_observability::load_event_delivery_health(&self.pool)
            .await
            .map_err(map_postgres_error)
    }
}

#[async_trait]
impl EventDeliveryStorage for PostgresStorage {
    async fn claim_event_delivery_batch(
        &self,
        settings: crate::events::EventDeliverySettings,
    ) -> Result<EventDeliveryBatch, StorageError> {
        operations::event_delivery::claim_event_delivery_batch_from_storage(&self.pool, settings)
            .await
            .map_err(map_postgres_error)
    }

    async fn mark_event_delivery_succeeded(
        &self,
        claim: &EventDeliveryClaim,
    ) -> Result<(), StorageError> {
        operations::event_delivery::mark_event_delivery_claim_succeeded(&self.pool, claim)
            .await
            .map_err(map_postgres_error)
    }

    async fn mark_event_delivery_failed(
        &self,
        claim: &EventDeliveryClaim,
        settings: crate::events::EventDeliverySettings,
        error: &str,
    ) -> Result<(), StorageError> {
        operations::event_delivery::mark_event_delivery_claim_failed(
            &self.pool, claim, settings, error,
        )
        .await
        .map_err(map_postgres_error)
    }
}

#[async_trait]
impl EventFanoutStorage for PostgresStorage {
    async fn process_event_fanout_batch(
        &self,
        settings: EventFanoutSettings,
    ) -> Result<usize, StorageError> {
        operations::event_fanout::process_event_fanout_batch(&self.pool, settings)
            .await
            .map_err(map_postgres_error)
    }
}

#[async_trait]
impl EventRetentionStorage for PostgresStorage {
    async fn process_event_retention_batch(
        &self,
        settings: EventRetentionSettings,
        archive: &dyn EventArchive,
    ) -> Result<EventRetentionSummary, StorageError> {
        operations::event_retention::process_event_retention_batch(&self.pool, settings, archive)
            .await
            .map_err(map_postgres_error)
    }
}

#[async_trait]
impl TokenRetentionStorage for PostgresStorage {
    async fn purge_expired_tokens(
        &self,
        settings: TokenRetentionSettings,
    ) -> Result<usize, StorageError> {
        operations::token_retention::purge_expired_token_batch(&self.pool, settings)
            .await
            .map_err(map_postgres_error)
    }
}

#[async_trait]
impl CollectionStore for PostgresStorage {
    async fn get_collection(&self, id: CollectionID) -> Result<Collection, StorageError> {
        id.collection_from_backend(&self.pool)
            .await
            .map_err(map_postgres_error)
    }

    async fn create_collection(
        &self,
        command: NewCollectionWithAssignee,
        context: &EventContext,
    ) -> Result<Collection, StorageError> {
        command
            .save_collection_with_assignee_record(&self.pool, Some(context))
            .await
            .map_err(map_postgres_error)
    }

    async fn update_collection(
        &self,
        id: CollectionID,
        changes: UpdateCollection,
        context: &EventContext,
    ) -> Result<Collection, StorageError> {
        changes
            .update_collection_record(&self.pool, id.id(), Some(context))
            .await
            .map_err(map_postgres_error)
    }

    async fn delete_collection(
        &self,
        id: CollectionID,
        context: &EventContext,
    ) -> Result<(), StorageError> {
        id.delete_collection_record(&self.pool, Some(context))
            .await
            .map_err(map_postgres_error)
    }

    async fn collection_children(&self, id: CollectionID) -> Result<Vec<Collection>, StorageError> {
        collection_children_from_backend(&self.pool, id)
            .await
            .map_err(map_postgres_error)
    }

    async fn collection_ancestors(
        &self,
        id: CollectionID,
    ) -> Result<Vec<Collection>, StorageError> {
        collection_ancestors_from_backend(&self.pool, id)
            .await
            .map_err(map_postgres_error)
    }

    async fn move_collection(
        &self,
        id: CollectionID,
        new_parent_id: CollectionID,
        context: &EventContext,
    ) -> Result<Collection, StorageError> {
        move_collection_record_from_backend(&self.pool, id.id(), new_parent_id.id(), Some(context))
            .await
            .map_err(map_postgres_error)
    }
}

#[async_trait]
impl ClassStore for PostgresStorage {
    async fn resolve_class(
        &self,
        selector: ClassSelector,
    ) -> Result<ResolvedClassTarget, StorageError> {
        let class = selector
            .resolve_class_selector_record(&self.pool)
            .await
            .map_err(map_postgres_error)?;
        Ok(ResolvedClassTarget::new(selector, class))
    }

    async fn create_class(
        &self,
        command: NewHubuumClass,
        context: &EventContext,
    ) -> Result<HubuumClass, StorageError> {
        command.validate_schema().map_err(map_postgres_error)?;
        command
            .create_class_record(&self.pool, Some(context))
            .await
            .map_err(map_postgres_error)
    }

    async fn update_class(
        &self,
        target: &ResolvedClassTarget,
        changes: UpdateHubuumClass,
        context: &EventContext,
    ) -> Result<HubuumClass, StorageError> {
        changes
            .update_resolved_class_record(&self.pool, target, context)
            .await
            .map_err(map_postgres_error)
    }

    async fn delete_class(
        &self,
        target: &ResolvedClassTarget,
        context: &EventContext,
    ) -> Result<(), StorageError> {
        target
            .delete_resolved_class_record(&self.pool, context)
            .await
            .map_err(map_postgres_error)
    }
}

#[async_trait]
impl ClassRelationStore for PostgresStorage {
    async fn prepare_class_relation(
        &self,
        command: NewHubuumClassRelation,
    ) -> Result<PreparedClassRelation, StorageError> {
        command
            .prepare_class_relation_record(&self.pool)
            .await
            .map_err(map_postgres_error)
    }

    async fn resolve_class_relation(
        &self,
        id: HubuumClassRelationID,
    ) -> Result<ResolvedClassRelationTarget, StorageError> {
        id.resolve_class_relation_target_record(&self.pool)
            .await
            .map_err(map_postgres_error)
    }

    async fn create_class_relation(
        &self,
        prepared: &PreparedClassRelation,
        context: &EventContext,
    ) -> Result<ResolvedClassRelationTarget, StorageError> {
        let relation = prepared
            .create_prepared_class_relation_record(&self.pool, context)
            .await
            .map_err(map_postgres_error)?;
        ResolvedClassRelationTarget::new(
            relation,
            prepared.from_class().clone(),
            prepared.to_class().clone(),
        )
        .map_err(map_postgres_error)
    }

    async fn delete_class_relation(
        &self,
        target: &ResolvedClassRelationTarget,
        context: &EventContext,
    ) -> Result<(), StorageError> {
        target
            .delete_resolved_class_relation_record(&self.pool, context)
            .await
            .map_err(map_postgres_error)
    }
}

#[async_trait]
impl ObjectRelationStore for PostgresStorage {
    async fn prepare_object_relation(
        &self,
        selector: ObjectRelationCreateSelector,
    ) -> Result<PreparedObjectRelation, StorageError> {
        selector
            .prepare_object_relation_record(&self.pool)
            .await
            .map_err(map_postgres_error)
    }

    async fn resolve_object_relation(
        &self,
        selector: ObjectRelationSelector,
    ) -> Result<ResolvedObjectRelationTarget, StorageError> {
        selector
            .resolve_object_relation_target_record(&self.pool)
            .await
            .map_err(map_postgres_error)
    }

    async fn create_object_relation(
        &self,
        prepared: &PreparedObjectRelation,
        context: &EventContext,
    ) -> Result<ResolvedObjectRelationTarget, StorageError> {
        let relation = prepared
            .create_prepared_object_relation_record(&self.pool, context)
            .await
            .map_err(map_postgres_error)?;
        ResolvedObjectRelationTarget::new(
            relation,
            prepared.from_object().clone(),
            prepared.to_object().clone(),
            prepared.class_relation().clone(),
        )
        .map_err(map_postgres_error)
    }

    async fn delete_object_relation(
        &self,
        target: &ResolvedObjectRelationTarget,
        context: &EventContext,
    ) -> Result<(), StorageError> {
        target
            .delete_resolved_object_relation_record(&self.pool, context)
            .await
            .map_err(map_postgres_error)
    }
}

#[async_trait]
impl ObjectStore for PostgresStorage {
    async fn resolve_object(
        &self,
        selector: ObjectSelector,
    ) -> Result<ResolvedObjectTarget, StorageError> {
        let (class, object) = selector
            .resolve_object_selector_record(&self.pool)
            .await
            .map_err(map_postgres_error)?;
        Ok(ResolvedObjectTarget::new(selector, class, object))
    }

    async fn create_object(
        &self,
        class: &ResolvedClassTarget,
        command: NewHubuumObject,
        context: &EventContext,
    ) -> Result<HubuumObject, StorageError> {
        command
            .create_object_in_resolved_class_record(&self.pool, class, context)
            .await
            .map_err(map_postgres_error)
    }

    async fn update_object(
        &self,
        target: &ResolvedObjectTarget,
        changes: UpdateHubuumObject,
        context: &EventContext,
    ) -> Result<HubuumObject, StorageError> {
        changes
            .update_resolved_object_record(&self.pool, target, context)
            .await
            .map_err(map_postgres_error)
    }

    async fn patch_object_data(
        &self,
        target: &ResolvedObjectTarget,
        patch: ObjectDataPatchDocument,
        context: &EventContext,
    ) -> Result<HubuumObject, StorageError> {
        patch
            .patch_object_data_record(&self.pool, target, context)
            .await
            .map_err(map_postgres_error)
    }

    async fn delete_object(
        &self,
        target: &ResolvedObjectTarget,
        context: &EventContext,
    ) -> Result<(), StorageError> {
        target
            .delete_resolved_object_record(&self.pool, context)
            .await
            .map_err(map_postgres_error)
    }
}
