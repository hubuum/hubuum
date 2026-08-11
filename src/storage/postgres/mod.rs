mod backup_snapshot;
mod computed_fields;
mod error;
mod export_templates;
mod imports;
#[cfg(feature = "embedded-migrations")]
mod migrations;
#[cfg(test)]
pub(crate) use imports::{RuntimeState, execute_planned_item, resolve_object_runtime};
#[doc(hidden)]
pub mod operations;
mod remote_targets;
mod restores;
mod runtime;
mod task_execution;
mod task_queue;

pub use runtime::*;

use async_trait::async_trait;
use std::future::Future;
use std::pin::Pin;

use crate::events::{
    EventContext, EventFanoutSettings, EventRetentionSettings, MutationProvenance,
};
use crate::models::output::{EffectiveGroupPermission, GroupPermission};
use crate::models::search::QueryOptions;
use crate::models::{
    ClassIdSet, ClassSelector, Collection, CollectionID, Group, GroupID, HubuumClass,
    HubuumClassID, HubuumClassRelation, HubuumClassRelationID, HubuumObject, HubuumObjectID,
    HubuumObjectRelation, HubuumObjectRelationID, MaintenanceState, NewCollectionWithAssignee,
    NewGroup, NewHubuumClass, NewHubuumClassRelation, NewHubuumObject, NewHubuumObjectRelation,
    ObjectDataPatchDocument, ObjectRelationCreateSelector, ObjectRelationSelector, ObjectSelector,
    Permission, PreparedClassRelation, PreparedObjectRelation, Principal, PrincipalGroup,
    PrincipalID, ResolvedClassRelationTarget, ResolvedClassTarget, ResolvedObjectRelationTarget,
    ResolvedObjectTarget, TokenRetentionSettings, UpdateCollection, UpdateGroup, UpdateHubuumClass,
    UpdateHubuumObject,
};
use crate::storage::postgres::operations::GetCollection;
use crate::storage::postgres::operations::class::{
    ClassCollectionLookup, CreateClassRecord, DeleteClassRecord, DeleteResolvedClassRecord,
    LoadClassRecord, ResolveClassSelectorRecord, UpdateClassRecord, UpdateResolvedClassRecord,
    load_class_names,
};
use crate::storage::postgres::operations::collection::{
    DeleteCollectionRecord, SaveCollectionWithAssigneeRecord, UpdateCollectionRecord,
    collection_ancestors_from_backend, collection_children_from_backend,
    effective_group_on_from_backend, effective_principal_on_from_backend,
    group_can_on_from_backend, group_on_from_backend, groups_can_on_from_backend,
    groups_can_on_paginated_with_total_count_from_backend, groups_on_from_backend,
    groups_on_paginated_with_total_count_from_backend, move_collection_record_from_backend,
    principal_all_permissions_from_backend, principal_on_from_backend,
    principal_on_paginated_with_total_count_from_backend, user_can_on_any_from_backend,
};
use crate::storage::postgres::operations::group::{
    DeleteGroupRecord, GroupMembersBackend, LoadGroupRecord, SaveGroupRecord, UpdateGroupRecord,
};
use crate::storage::postgres::operations::object::{
    CreateObjectInResolvedClassRecord, DeleteObjectRecord, DeleteResolvedObjectRecord,
    LoadObjectRecord, ObjectClassLookup, ObjectCollectionLookup, PatchObjectDataRecord,
    ResolveObjectSelectorRecord, SaveObjectRecord, UpdateObjectRecord, UpdateResolvedObjectRecord,
    ValidateObjectRecord,
};
use crate::storage::postgres::operations::relations::{
    CreatePreparedClassRelationRecord, CreatePreparedObjectRelationRecord,
    DeleteClassRelationRecord, DeleteObjectRelationRecord, DeleteResolvedClassRelationRecord,
    DeleteResolvedObjectRelationRecord, PrepareClassRelationRecord, PrepareObjectRelationRecord,
    ResolveClassRelationTargetRecord, ResolveObjectRelationTargetRecord, SaveClassRelationRecord,
    SaveObjectRelationRecord,
};

use super::{
    AuditEventStorage, AuthenticatedToken, AuthenticationCredential, AuthenticationIdentity,
    AuthenticationStorage, AuthenticationTokenScope, AuthenticationTokenScopeQuery,
    AuthorizationClassResource, AuthorizationCollection, AuthorizationCollectionAccessQuery,
    AuthorizationCollectionGrantListQuery, AuthorizationCollectionsAccessQuery,
    AuthorizationCollectionsQuery, AuthorizationGrant, AuthorizationGrantDelete,
    AuthorizationGrantKey, AuthorizationGrantMutation, AuthorizationGroup,
    AuthorizationGroupGrantPage, AuthorizationGroupMembershipQuery, AuthorizationObjectResource,
    AuthorizationPermissionSet, AuthorizationPermissionSetQuery, AuthorizationPolicySnapshotRow,
    AuthorizationPrincipal, AuthorizationResourceIds, AuthorizationStorage,
    BidirectionalRelatedObjectsQuery, CatalogListQuery, CatalogPage, CatalogStorage,
    ClassRecordStorage, ClassRelationStore, ClassStore, CollectionGrantListQuery,
    CollectionGroupPermissionQuery, CollectionGroupsPageQuery, CollectionGroupsQuery,
    CollectionPermissionStorage, CollectionPrincipalPageQuery, CollectionPrincipalQuery,
    CollectionRecordStorage, CollectionStore, CollectionVisibilityQuery,
    ComputedObjectEnrichmentQuery, ComputedObjectListQuery, ComputedObjectPage,
    ComputedObjectStorage, EventArchive, EventDeliveryAdministrationStorage, EventDeliveryBatch,
    EventDeliveryClaim, EventDeliveryHealthSnapshot, EventDeliveryStorage, EventFanoutStorage,
    EventHealthStorage, EventMetricsSnapshot, EventRetentionStorage, EventRetentionSummary,
    EventSubscriptionStorage, ExportQueryStorage, ExportTemplateHistoryRecord, GroupStorage,
    HistoryAsOfQuery, HistoryCollectionScope, HistoryListQuery, HistoryPage, HistoryPrincipalName,
    HistoryStorage, IdentityStorage, InventoryGaugeSnapshot, InventoryStorage, MetricsStorage,
    ObjectAggregateAuthorizer, ObjectAggregateStorage, ObjectAggregateStorageQuery,
    ObjectHistoryAsOfQuery, ObjectHistoryListQuery, ObjectHistoryRecord, ObjectRecordStorage,
    ObjectRelationStore, ObjectRelationsTouchingIdsQuery, ObjectStore,
    OperationalExportTemplateAuditEntry, OperationalExportTemplateHealth, OperationalStateStorage,
    OperationalStorageSnapshot, OperationalTaskQueueSnapshot, ReadinessSnapshot,
    RelatedObjectsForRootsQuery, RelationGraphQuery, RelationIdsQuery, RelationListQuery,
    RelationPage, RelationQueryStorage, RelationTouchingQuery, RemoteTargetHistoryRecord,
    StorageAuditEvent, StorageAuditEventListQuery, StorageCallSite, StorageClass,
    StorageClassGraphRow, StorageClassRelation, StorageCollection, StorageComputedObject,
    StorageDefaultAdminBootstrap, StorageError, StorageEventDelivery,
    StorageEventDeliveryListQuery, StorageEventPage, StorageEventSink, StorageEventSinkCreate,
    StorageEventSinkDelete, StorageEventSinkListQuery, StorageEventSinkUpdate,
    StorageEventSubscription, StorageEventSubscriptionCreate, StorageEventSubscriptionDelete,
    StorageEventSubscriptionListQuery, StorageEventSubscriptionUpdate, StorageExecution,
    StorageExternalPrincipalState, StorageExternalUserSync, StorageIdentity, StorageIdentityPage,
    StorageIdentityScope, StorageIdentityScopeEnsure, StorageInventoryCounts,
    StorageLocalPasswordReset, StorageObject, StorageObjectAggregatePage, StorageObjectGraphRow,
    StorageObjectRelation, StoragePoolState, StoragePrincipalGroup, StorageQueryBudget,
    StorageRelatedObjectForRootRow, StorageRelatedObjectIncludeRow, StorageRevisionPrecondition,
    StorageServiceAccount, StorageServiceAccountCreate, StorageServiceAccountListItem,
    StorageServiceAccountListQuery, StorageServiceAccountMutation, StorageServiceAccountPoint,
    StorageServiceAccountUpdate, StorageSyncedHuman, StorageTokenListQuery, StorageTokenMetadata,
    TaskGaugeSnapshot, TokenRetentionStorage, UnifiedSearchClass, UnifiedSearchCollection,
    UnifiedSearchObject, UnifiedSearchQuery, UnifiedSearchStorage,
};
use super::{ClassHistoryRecord, CollectionHistoryRecord};
use error::map_postgres_error;

/// Canonical production storage adapter.
#[derive(Clone)]
pub(crate) struct PostgresStorage {
    pool: PostgresPool,
}

#[cfg(feature = "embedded-migrations")]
pub(in crate::storage) use migrations::run_embedded_migrations;

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

impl ExportQueryStorage for PostgresStorage {
    fn run_export_queries<'a, F, R>(
        &'a self,
        budget: Option<StorageQueryBudget>,
        future: F,
    ) -> Pin<Box<dyn Future<Output = R> + 'a>>
    where
        F: Future<Output = R> + 'a,
        R: 'a,
    {
        Box::pin(runtime::with_export_query_budget_scope(budget, future))
    }
}

impl StorageExecution for PostgresStorage {
    fn run_with_call_site<'a, F, R>(
        &'a self,
        call_site: StorageCallSite,
        future: F,
    ) -> Pin<Box<dyn Future<Output = R> + 'a>>
    where
        F: Future<Output = R> + 'a,
        R: 'a,
    {
        Box::pin(runtime::with_storage_call_site_scope(call_site, future))
    }

    fn run_with_call_site_send<'a, F, R>(
        &'a self,
        call_site: StorageCallSite,
        future: F,
    ) -> Pin<Box<dyn Future<Output = R> + Send + 'a>>
    where
        F: Future<Output = R> + Send + 'a,
        R: Send + 'a,
    {
        Box::pin(runtime::with_storage_call_site_scope(call_site, future))
    }

    fn run_with_mutation_provenance<'a, F, R>(
        &'a self,
        provenance: Option<MutationProvenance>,
        future: F,
    ) -> Pin<Box<dyn Future<Output = R> + 'a>>
    where
        F: Future<Output = R> + 'a,
        R: 'a,
    {
        Box::pin(runtime::with_mutation_provenance_scope(provenance, future))
    }

    fn run_with_revision_precondition<'a, F, R>(
        &'a self,
        precondition: Option<StorageRevisionPrecondition>,
        future: F,
    ) -> Pin<Box<dyn Future<Output = R> + 'a>>
    where
        F: Future<Output = R> + 'a,
        R: 'a,
    {
        Box::pin(runtime::with_revision_precondition_scope(
            precondition,
            future,
        ))
    }
}

#[async_trait]
impl AuthenticationStorage for PostgresStorage {
    async fn authenticate_bearer_token(
        &self,
        credential: AuthenticationCredential,
    ) -> Result<AuthenticatedToken, StorageError> {
        operations::authentication::authenticate_bearer_token(&self.pool, credential)
            .await
            .map_err(map_postgres_error)
    }

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
impl IdentityStorage for PostgresStorage {
    async fn default_admin_bootstrap_required(&self) -> Result<bool, StorageError> {
        operations::bootstrap::default_admin_bootstrap_required(&self.pool)
            .await
            .map_err(map_postgres_error)
    }

    async fn bootstrap_default_admin(
        &self,
        request: StorageDefaultAdminBootstrap,
    ) -> Result<bool, StorageError> {
        operations::bootstrap::bootstrap_default_admin(&self.pool, request)
            .await
            .map_err(map_postgres_error)
    }

    async fn reset_local_password(
        &self,
        request: StorageLocalPasswordReset,
    ) -> Result<usize, StorageError> {
        operations::identity_operations::reset_local_password(&self.pool, request)
            .await
            .map_err(map_postgres_error)
    }

    async fn ensure_identity_scope(
        &self,
        request: StorageIdentityScopeEnsure,
    ) -> Result<StorageIdentityScope, StorageError> {
        operations::identity_operations::ensure_identity_scope(&self.pool, request)
            .await
            .map_err(map_postgres_error)
    }

    async fn identity_scope_name(&self, scope_id: i32) -> Result<String, StorageError> {
        operations::identity_operations::identity_scope_name(&self.pool, scope_id)
            .await
            .map_err(map_postgres_error)
    }

    async fn identity_scope_names(
        &self,
        scope_ids: Vec<i32>,
    ) -> Result<Vec<(i32, String)>, StorageError> {
        operations::identity_operations::identity_scope_names(&self.pool, scope_ids)
            .await
            .map_err(map_postgres_error)
    }

    async fn load_principal_group(
        &self,
        principal_id: i32,
        group_id: i32,
    ) -> Result<StoragePrincipalGroup, StorageError> {
        operations::identity_operations::load_principal_group(&self.pool, principal_id, group_id)
            .await
            .map_err(map_postgres_error)
    }

    async fn list_retained_tokens(
        &self,
        query: StorageTokenListQuery,
    ) -> Result<StorageIdentityPage<StorageTokenMetadata>, StorageError> {
        operations::identity_operations::list_retained_tokens(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn is_human_owner_group_member(
        &self,
        principal_id: i32,
        owner_group_id: i32,
    ) -> Result<bool, StorageError> {
        operations::identity_operations::is_human_owner_group_member(
            &self.pool,
            principal_id,
            owner_group_id,
        )
        .await
        .map_err(map_postgres_error)
    }

    async fn principal_is_disabled(&self, principal_id: i32) -> Result<bool, StorageError> {
        operations::identity_operations::principal_is_disabled(&self.pool, principal_id)
            .await
            .map_err(map_postgres_error)
    }

    async fn load_service_account(
        &self,
        service_account_id: i32,
    ) -> Result<StorageServiceAccount, StorageError> {
        operations::identity_operations::load_service_account(&self.pool, service_account_id)
            .await
            .map_err(map_postgres_error)
    }

    async fn load_service_account_point(
        &self,
        service_account_id: i32,
    ) -> Result<StorageServiceAccountPoint, StorageError> {
        operations::identity_operations::load_service_account_point(&self.pool, service_account_id)
            .await
            .map_err(map_postgres_error)
    }

    async fn list_manageable_service_accounts(
        &self,
        query: StorageServiceAccountListQuery,
    ) -> Result<StorageIdentityPage<StorageServiceAccountListItem>, StorageError> {
        operations::identity_operations::list_manageable_service_accounts(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn create_service_account(
        &self,
        request: StorageServiceAccountCreate,
    ) -> Result<StorageServiceAccount, StorageError> {
        operations::identity_operations::create_service_account(&self.pool, request)
            .await
            .map_err(map_postgres_error)
    }

    async fn update_service_account(
        &self,
        request: StorageServiceAccountUpdate,
    ) -> Result<StorageServiceAccount, StorageError> {
        operations::identity_operations::update_service_account(&self.pool, request)
            .await
            .map_err(map_postgres_error)
    }

    async fn disable_service_account(
        &self,
        request: StorageServiceAccountMutation,
    ) -> Result<StorageServiceAccount, StorageError> {
        operations::identity_operations::disable_service_account(&self.pool, request)
            .await
            .map_err(map_postgres_error)
    }

    async fn delete_service_account(
        &self,
        request: StorageServiceAccountMutation,
    ) -> Result<(), StorageError> {
        operations::identity_operations::delete_service_account(&self.pool, request)
            .await
            .map_err(map_postgres_error)
    }

    async fn external_principal_state(
        &self,
        principal_id: i32,
    ) -> Result<Option<StorageExternalPrincipalState>, StorageError> {
        operations::identity_operations::external_principal_state(&self.pool, principal_id)
            .await
            .map_err(map_postgres_error)
    }

    async fn mark_external_sync_attempted(&self, principal_id: i32) -> Result<(), StorageError> {
        operations::identity_operations::mark_external_sync_attempted(&self.pool, principal_id)
            .await
            .map_err(map_postgres_error)
    }

    async fn sync_external_user(
        &self,
        request: StorageExternalUserSync,
    ) -> Result<StorageSyncedHuman, StorageError> {
        operations::identity_operations::sync_external_user(&self.pool, request)
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

    async fn load_authorization_classes(
        &self,
        query: AuthorizationResourceIds,
    ) -> Result<Vec<AuthorizationClassResource>, StorageError> {
        operations::authorization::load_authorization_classes(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn load_authorization_objects(
        &self,
        query: AuthorizationResourceIds,
    ) -> Result<Vec<AuthorizationObjectResource>, StorageError> {
        operations::authorization::load_authorization_objects(&self.pool, query)
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

    async fn authorize_local_collections(
        &self,
        query: AuthorizationCollectionsAccessQuery,
    ) -> Result<bool, StorageError> {
        operations::authorization::authorize_local_collections(&self.pool, query)
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

    async fn load_local_collection_permission_set(
        &self,
        query: AuthorizationPermissionSetQuery,
    ) -> Result<AuthorizationPermissionSet, StorageError> {
        operations::authorization::load_local_collection_permission_set(&self.pool, query)
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
        request: AuthorizationGrantDelete,
    ) -> Result<(), StorageError> {
        operations::authorization::revoke_all_local_collection_grants(&self.pool, request)
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
impl InventoryStorage for PostgresStorage {
    async fn inventory_counts(&self) -> Result<StorageInventoryCounts, StorageError> {
        operations::inventory::load_inventory_counts(&self.pool)
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

    async fn storage_snapshot(&self) -> Result<OperationalStorageSnapshot, StorageError> {
        operations::meta::load_storage_snapshot(&self.pool)
            .await
            .map_err(map_postgres_error)
    }

    async fn task_queue_snapshot(&self) -> Result<OperationalTaskQueueSnapshot, StorageError> {
        operations::meta::load_task_queue_snapshot(&self.pool)
            .await
            .map_err(map_postgres_error)
    }

    async fn export_template_health(
        &self,
    ) -> Result<Vec<OperationalExportTemplateHealth>, StorageError> {
        operations::meta::load_export_template_health(&self.pool)
            .await
            .map_err(map_postgres_error)
    }

    async fn export_templates_for_audit(
        &self,
    ) -> Result<Vec<OperationalExportTemplateAuditEntry>, StorageError> {
        operations::meta::load_export_templates_for_audit(&self.pool)
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
impl AuditEventStorage for PostgresStorage {
    async fn list_audit_events(
        &self,
        query: StorageAuditEventListQuery,
    ) -> Result<StorageEventPage<StorageAuditEvent>, StorageError> {
        operations::event_administration::list_audit_events(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }
}

#[async_trait]
impl EventSubscriptionStorage for PostgresStorage {
    async fn enabled_event_sink_count(&self) -> Result<i64, StorageError> {
        operations::event_administration::enabled_event_sink_count(&self.pool)
            .await
            .map_err(map_postgres_error)
    }

    async fn list_event_sinks(
        &self,
        query: StorageEventSinkListQuery,
    ) -> Result<StorageEventPage<StorageEventSink>, StorageError> {
        operations::event_administration::list_event_sinks(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn load_event_sink(&self, sink_id: i32) -> Result<StorageEventSink, StorageError> {
        operations::event_administration::load_event_sink(&self.pool, sink_id)
            .await
            .map_err(map_postgres_error)
    }

    async fn create_event_sink(
        &self,
        request: StorageEventSinkCreate,
    ) -> Result<StorageEventSink, StorageError> {
        operations::event_administration::create_event_sink(&self.pool, request)
            .await
            .map_err(map_postgres_error)
    }

    async fn update_event_sink(
        &self,
        request: StorageEventSinkUpdate,
    ) -> Result<StorageEventSink, StorageError> {
        operations::event_administration::update_event_sink(&self.pool, request)
            .await
            .map_err(map_postgres_error)
    }

    async fn delete_event_sink(&self, request: StorageEventSinkDelete) -> Result<(), StorageError> {
        operations::event_administration::delete_event_sink(&self.pool, request)
            .await
            .map_err(map_postgres_error)
    }

    async fn list_event_subscriptions(
        &self,
        query: StorageEventSubscriptionListQuery,
    ) -> Result<StorageEventPage<StorageEventSubscription>, StorageError> {
        operations::event_administration::list_event_subscriptions(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn load_event_subscription(
        &self,
        collection_id: i32,
        subscription_id: i32,
    ) -> Result<StorageEventSubscription, StorageError> {
        operations::event_administration::load_event_subscription(
            &self.pool,
            collection_id,
            subscription_id,
        )
        .await
        .map_err(map_postgres_error)
    }

    async fn create_event_subscription(
        &self,
        request: StorageEventSubscriptionCreate,
    ) -> Result<StorageEventSubscription, StorageError> {
        operations::event_administration::create_event_subscription(&self.pool, request)
            .await
            .map_err(map_postgres_error)
    }

    async fn update_event_subscription(
        &self,
        request: StorageEventSubscriptionUpdate,
    ) -> Result<StorageEventSubscription, StorageError> {
        operations::event_administration::update_event_subscription(&self.pool, request)
            .await
            .map_err(map_postgres_error)
    }

    async fn delete_event_subscription(
        &self,
        request: StorageEventSubscriptionDelete,
    ) -> Result<(), StorageError> {
        operations::event_administration::delete_event_subscription(&self.pool, request)
            .await
            .map_err(map_postgres_error)
    }
}

#[async_trait]
impl EventDeliveryAdministrationStorage for PostgresStorage {
    async fn list_event_deliveries(
        &self,
        query: StorageEventDeliveryListQuery,
    ) -> Result<StorageEventPage<StorageEventDelivery>, StorageError> {
        operations::event_administration::list_event_deliveries(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn load_event_delivery(
        &self,
        delivery_id: i64,
    ) -> Result<StorageEventDelivery, StorageError> {
        operations::event_administration::load_event_delivery(&self.pool, delivery_id)
            .await
            .map_err(map_postgres_error)
    }

    async fn release_event_delivery_for_retry(
        &self,
        delivery_id: i64,
    ) -> Result<StorageEventDelivery, StorageError> {
        operations::event_administration::release_event_delivery_for_retry(&self.pool, delivery_id)
            .await
            .map_err(map_postgres_error)
    }

    async fn mark_event_delivery_dead(
        &self,
        delivery_id: i64,
    ) -> Result<StorageEventDelivery, StorageError> {
        operations::event_administration::mark_event_delivery_dead(&self.pool, delivery_id)
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
impl GroupStorage for PostgresStorage {
    async fn load_group(&self, group_id: i32) -> Result<Group, StorageError> {
        GroupID::new(group_id)
            .map_err(map_postgres_error)?
            .load_group_record(&self.pool)
            .await
            .map_err(map_postgres_error)
    }

    async fn group_identity_scope_name(&self, group_id: i32) -> Result<String, StorageError> {
        GroupID::new(group_id).map_err(map_postgres_error)?;
        operations::group::group_identity_scope_name(&self.pool, group_id)
            .await
            .map_err(map_postgres_error)
    }

    async fn create_group(
        &self,
        command: &NewGroup,
        context: Option<&EventContext>,
    ) -> Result<Group, StorageError> {
        command
            .save_group_record(&self.pool, context)
            .await
            .map_err(map_postgres_error)
    }

    async fn update_group(
        &self,
        group_id: i32,
        update: &UpdateGroup,
        context: Option<&EventContext>,
    ) -> Result<Group, StorageError> {
        let group_id = GroupID::new(group_id).map_err(map_postgres_error)?;
        update
            .update_group_record(group_id.id(), &self.pool, context)
            .await
            .map_err(map_postgres_error)
    }

    async fn delete_group(
        &self,
        group_id: i32,
        context: Option<&EventContext>,
    ) -> Result<usize, StorageError> {
        GroupID::new(group_id)
            .map_err(map_postgres_error)?
            .delete_group_record(&self.pool, context)
            .await
            .map_err(map_postgres_error)
    }

    async fn group_members(&self, group_id: i32) -> Result<Vec<Principal>, StorageError> {
        let group = self.load_group(group_id).await?;
        group
            .load_group_members(&self.pool)
            .await
            .map_err(map_postgres_error)
    }

    async fn group_members_page(
        &self,
        group_id: i32,
        query_options: &QueryOptions,
    ) -> Result<Vec<(PrincipalGroup, Principal)>, StorageError> {
        let group = self.load_group(group_id).await?;
        group
            .load_group_members_paginated(&self.pool, query_options)
            .await
            .map_err(map_postgres_error)
    }

    async fn count_group_members(
        &self,
        group_id: i32,
        query_options: &QueryOptions,
    ) -> Result<i64, StorageError> {
        let group = self.load_group(group_id).await?;
        group
            .count_group_members_paginated(&self.pool, query_options)
            .await
            .map_err(map_postgres_error)
    }

    async fn group_member_principal(&self, principal_id: i32) -> Result<Principal, StorageError> {
        PrincipalID::new(principal_id).map_err(map_postgres_error)?;
        operations::group::group_member_principal(&self.pool, principal_id)
            .await
            .map_err(map_postgres_error)
    }

    async fn add_group_member(
        &self,
        principal_id: i32,
        group_id: i32,
        context: Option<&EventContext>,
    ) -> Result<PrincipalGroup, StorageError> {
        GroupID::new(group_id).map_err(map_postgres_error)?;
        PrincipalID::new(principal_id).map_err(map_postgres_error)?;
        operations::group::save_manual_membership(&self.pool, principal_id, group_id, context)
            .await
            .map_err(map_postgres_error)
    }

    async fn remove_group_member(
        &self,
        principal_id: i32,
        group_id: i32,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError> {
        PrincipalID::new(principal_id).map_err(map_postgres_error)?;
        let group = self.load_group(group_id).await?;
        group
            .remove_group_member_from_backend(principal_id, &self.pool, context)
            .await
            .map_err(map_postgres_error)
    }
}

#[async_trait]
impl CollectionRecordStorage for PostgresStorage {
    async fn create_collection_record(
        &self,
        command: &NewCollectionWithAssignee,
        context: Option<&EventContext>,
    ) -> Result<Collection, StorageError> {
        command
            .save_collection_with_assignee_record(&self.pool, context)
            .await
            .map_err(map_postgres_error)
    }

    async fn update_collection_record(
        &self,
        update: &UpdateCollection,
        collection_id: i32,
        context: Option<&EventContext>,
    ) -> Result<Collection, StorageError> {
        update
            .update_collection_record(&self.pool, collection_id, context)
            .await
            .map_err(map_postgres_error)
    }

    async fn delete_collection_record(
        &self,
        collection_id: i32,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError> {
        CollectionID::new(collection_id)
            .map_err(map_postgres_error)?
            .delete_collection_record(&self.pool, context)
            .await
            .map_err(map_postgres_error)
    }

    async fn move_collection_record(
        &self,
        collection_id: i32,
        new_parent_collection_id: i32,
        context: Option<&EventContext>,
    ) -> Result<Collection, StorageError> {
        move_collection_record_from_backend(
            &self.pool,
            collection_id,
            new_parent_collection_id,
            context,
        )
        .await
        .map_err(map_postgres_error)
    }
}

#[async_trait]
impl CollectionPermissionStorage for PostgresStorage {
    async fn principal_collection_permissions(
        &self,
        query: CollectionPrincipalQuery,
    ) -> Result<Vec<GroupPermission>, StorageError> {
        principal_on_from_backend(
            &self.pool,
            PrincipalID::new(query.principal_id()).map_err(map_postgres_error)?,
            CollectionID::new(query.collection_id()).map_err(map_postgres_error)?,
        )
        .await
        .map_err(map_postgres_error)
    }

    async fn principal_all_collection_permissions(
        &self,
        principal_id: i32,
    ) -> Result<Vec<(Collection, Group, Permission)>, StorageError> {
        principal_all_permissions_from_backend(
            &self.pool,
            PrincipalID::new(principal_id).map_err(map_postgres_error)?,
        )
        .await
        .map_err(map_postgres_error)
    }

    async fn principal_collection_permissions_page(
        &self,
        query: CollectionPrincipalPageQuery,
    ) -> Result<(Vec<GroupPermission>, i64), StorageError> {
        principal_on_paginated_with_total_count_from_backend(
            &self.pool,
            PrincipalID::new(query.principal().principal_id()).map_err(map_postgres_error)?,
            CollectionID::new(query.principal().collection_id()).map_err(map_postgres_error)?,
            query.query_options(),
        )
        .await
        .map_err(map_postgres_error)
    }

    async fn effective_principal_collection_permissions(
        &self,
        query: CollectionPrincipalQuery,
    ) -> Result<Vec<EffectiveGroupPermission>, StorageError> {
        effective_principal_on_from_backend(
            &self.pool,
            PrincipalID::new(query.principal_id()).map_err(map_postgres_error)?,
            CollectionID::new(query.collection_id()).map_err(map_postgres_error)?,
        )
        .await
        .map_err(map_postgres_error)
    }

    async fn visible_collections(
        &self,
        query: CollectionVisibilityQuery,
    ) -> Result<Vec<Collection>, StorageError> {
        user_can_on_any_from_backend(
            &self.pool,
            PrincipalID::new(query.principal_id()).map_err(map_postgres_error)?,
            query.permission(),
            query.scopes(),
        )
        .await
        .map_err(map_postgres_error)
    }

    async fn group_has_collection_permission(
        &self,
        query: CollectionGroupPermissionQuery,
    ) -> Result<bool, StorageError> {
        group_can_on_from_backend(
            &self.pool,
            query.group_id(),
            CollectionID::new(query.collection_id()).map_err(map_postgres_error)?,
            query.permission(),
        )
        .await
        .map_err(map_postgres_error)
    }

    async fn effective_group_collection_permissions(
        &self,
        collection_id: i32,
        group_id: i32,
    ) -> Result<Vec<EffectiveGroupPermission>, StorageError> {
        effective_group_on_from_backend(&self.pool, collection_id, group_id)
            .await
            .map_err(map_postgres_error)
    }

    async fn groups_with_collection_permission(
        &self,
        query: CollectionGroupsQuery,
    ) -> Result<Vec<Group>, StorageError> {
        groups_can_on_from_backend(&self.pool, query.collection_id(), query.permission())
            .await
            .map_err(map_postgres_error)
    }

    async fn groups_with_collection_permission_page(
        &self,
        query: CollectionGroupsPageQuery,
    ) -> Result<(Vec<Group>, i64), StorageError> {
        groups_can_on_paginated_with_total_count_from_backend(
            &self.pool,
            query.groups().collection_id(),
            query.groups().permission(),
            query.query_options(),
        )
        .await
        .map_err(map_postgres_error)
    }

    async fn list_collection_group_permissions(
        &self,
        query: CollectionGrantListQuery,
    ) -> Result<Vec<GroupPermission>, StorageError> {
        groups_on_from_backend(
            &self.pool,
            CollectionID::new(query.collection_id()).map_err(map_postgres_error)?,
            query.permissions().to_vec(),
            query.query_options().clone(),
        )
        .await
        .map_err(map_postgres_error)
    }

    async fn list_collection_group_permissions_page(
        &self,
        query: CollectionGrantListQuery,
    ) -> Result<(Vec<GroupPermission>, i64), StorageError> {
        groups_on_paginated_with_total_count_from_backend(
            &self.pool,
            CollectionID::new(query.collection_id()).map_err(map_postgres_error)?,
            query.permissions().to_vec(),
            query.query_options(),
        )
        .await
        .map_err(map_postgres_error)
    }

    async fn collection_group_permission(
        &self,
        collection_id: i32,
        group_id: i32,
    ) -> Result<Permission, StorageError> {
        group_on_from_backend(&self.pool, collection_id, group_id)
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
impl ClassRecordStorage for PostgresStorage {
    async fn create_class_record(
        &self,
        class: &NewHubuumClass,
        context: Option<&EventContext>,
    ) -> Result<HubuumClass, StorageError> {
        class.validate_schema().map_err(map_postgres_error)?;
        class
            .create_class_record(&self.pool, context)
            .await
            .map_err(map_postgres_error)
    }

    async fn update_class_record(
        &self,
        update: &UpdateHubuumClass,
        class_id: i32,
        context: Option<&EventContext>,
    ) -> Result<HubuumClass, StorageError> {
        update
            .update_class_record(&self.pool, class_id, context)
            .await
            .map_err(map_postgres_error)
    }

    async fn delete_class_record(
        &self,
        class: &HubuumClass,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError> {
        class
            .delete_class_record(&self.pool, context)
            .await
            .map_err(map_postgres_error)
    }

    async fn load_class_record(&self, class_id: i32) -> Result<HubuumClass, StorageError> {
        HubuumClassID::new(class_id)
            .map_err(map_postgres_error)?
            .load_class_record(&self.pool)
            .await
            .map_err(map_postgres_error)
    }

    async fn class_collection(&self, class_id: i32) -> Result<Collection, StorageError> {
        HubuumClassID::new(class_id)
            .map_err(map_postgres_error)?
            .lookup_class_collection(&self.pool)
            .await
            .map_err(map_postgres_error)
    }

    async fn class_names(
        &self,
        class_ids: &ClassIdSet,
    ) -> Result<Vec<(i32, String)>, StorageError> {
        load_class_names(&self.pool, class_ids)
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
        context: Option<&EventContext>,
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
        context: Option<&EventContext>,
    ) -> Result<(), StorageError> {
        target
            .delete_resolved_class_relation_record(&self.pool, context)
            .await
            .map_err(map_postgres_error)
    }

    async fn create_class_relation_from_command(
        &self,
        command: NewHubuumClassRelation,
        context: Option<&EventContext>,
    ) -> Result<HubuumClassRelation, StorageError> {
        command
            .save_class_relation_record(&self.pool, context)
            .await
            .map_err(map_postgres_error)
    }

    async fn delete_class_relation_by_id(
        &self,
        id: HubuumClassRelationID,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError> {
        id.delete_class_relation_record(&self.pool, context)
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
        context: Option<&EventContext>,
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
        context: Option<&EventContext>,
    ) -> Result<(), StorageError> {
        target
            .delete_resolved_object_relation_record(&self.pool, context)
            .await
            .map_err(map_postgres_error)
    }

    async fn create_object_relation_from_command(
        &self,
        command: NewHubuumObjectRelation,
        context: Option<&EventContext>,
    ) -> Result<HubuumObjectRelation, StorageError> {
        command
            .save_object_relation_record(&self.pool, context)
            .await
            .map_err(map_postgres_error)
    }

    async fn delete_object_relation_by_id(
        &self,
        id: HubuumObjectRelationID,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError> {
        id.delete_object_relation_record(&self.pool, context)
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

#[async_trait]
impl ObjectRecordStorage for PostgresStorage {
    async fn validate_object(&self, object: &HubuumObject) -> Result<(), StorageError> {
        object
            .validate_object_record(&self.pool)
            .await
            .map_err(map_postgres_error)
    }

    async fn validate_new_object(&self, object: &NewHubuumObject) -> Result<(), StorageError> {
        object
            .validate_object_record(&self.pool)
            .await
            .map_err(map_postgres_error)
    }

    async fn validate_object_update(
        &self,
        update: &UpdateHubuumObject,
        object_id: i32,
    ) -> Result<(), StorageError> {
        (update, object_id)
            .validate_object_record(&self.pool)
            .await
            .map_err(map_postgres_error)
    }

    async fn save_object_record(
        &self,
        object: &HubuumObject,
        context: Option<&EventContext>,
    ) -> Result<HubuumObject, StorageError> {
        object
            .save_object_record(&self.pool, context)
            .await
            .map_err(map_postgres_error)
    }

    async fn create_object_record(
        &self,
        object: &NewHubuumObject,
        context: Option<&EventContext>,
    ) -> Result<HubuumObject, StorageError> {
        object
            .save_object_record(&self.pool, context)
            .await
            .map_err(map_postgres_error)
    }

    async fn update_object_record(
        &self,
        update: &UpdateHubuumObject,
        object_id: i32,
        context: Option<&EventContext>,
    ) -> Result<HubuumObject, StorageError> {
        update
            .update_object_record(&self.pool, object_id, context)
            .await
            .map_err(map_postgres_error)
    }

    async fn delete_object_record(
        &self,
        object: &HubuumObject,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError> {
        object
            .delete_object_record(&self.pool, context)
            .await
            .map_err(map_postgres_error)
    }

    async fn load_object_record(&self, object_id: i32) -> Result<HubuumObject, StorageError> {
        HubuumObjectID::new(object_id)
            .map_err(map_postgres_error)?
            .load_object_record(&self.pool)
            .await
            .map_err(map_postgres_error)
    }

    async fn object_collection(&self, object_id: i32) -> Result<Collection, StorageError> {
        HubuumObjectID::new(object_id)
            .map_err(map_postgres_error)?
            .lookup_object_collection(&self.pool)
            .await
            .map_err(map_postgres_error)
    }

    async fn object_class(&self, object_id: i32) -> Result<HubuumClass, StorageError> {
        HubuumObjectID::new(object_id)
            .map_err(map_postgres_error)?
            .lookup_object_class(&self.pool)
            .await
            .map_err(map_postgres_error)
    }
}
