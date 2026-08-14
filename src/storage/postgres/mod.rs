mod backup_snapshot;
mod capabilities;
mod computed_fields;
mod error;
mod export_templates;
mod failpoints;
mod imports;
mod notifications;
#[cfg(test)]
pub(crate) use imports::{
    RuntimeState, execute_application_planned_item, execute_planned_item, resolve_object_runtime,
};
#[doc(hidden)]
pub mod operations;
mod remote_targets;
mod restores;
mod runtime;
mod task_execution;
mod task_queue;

#[cfg(test)]
pub(crate) use failpoints::{PostgresFailpoint, with_failpoint};
pub use runtime::*;

use async_trait::async_trait;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use hubuum_storage_core::StorageErrorKind;
use hubuum_storage_postgres::{PostgresRuntime, PostgresTelemetry};

use super::{
    AuditEventStorage, AuthenticatedToken, AuthenticationAttempt, AuthenticationIdentity,
    AuthenticationStorage, AuthenticationTokenScope, AuthenticationTokenScopeQuery,
    AuthorizationClassResource, AuthorizationCollection, AuthorizationCollectionAccessQuery,
    AuthorizationCollectionGrantListQuery, AuthorizationCollectionGroupsPageQuery,
    AuthorizationCollectionGroupsQuery, AuthorizationCollectionVisibilityQuery,
    AuthorizationCollectionsAccessQuery, AuthorizationCollectionsQuery,
    AuthorizationEffectiveGroupGrant, AuthorizationGrant, AuthorizationGrantDelete,
    AuthorizationGrantKey, AuthorizationGrantMutation, AuthorizationGroup,
    AuthorizationGroupCollectionQuery, AuthorizationGroupGrant, AuthorizationGroupGrantPage,
    AuthorizationGroupMembershipQuery, AuthorizationGroupPage, AuthorizationObjectResource,
    AuthorizationPermissionSet, AuthorizationPermissionSetQuery, AuthorizationPolicySnapshotRow,
    AuthorizationPrincipal, AuthorizationPrincipalCollectionPageQuery,
    AuthorizationPrincipalCollectionQuery, AuthorizationResourceIds, AuthorizationStorage,
    BidirectionalRelatedObjectsQuery, CatalogListQuery, CatalogPage, CatalogStorage,
    ClassRelationStore, ClassStore, CollectionAuthorizationStorage, CollectionStore,
    ComputedObjectEnrichmentQuery, ComputedObjectListQuery, ComputedObjectPage,
    ComputedObjectStorage, EventArchive, EventDeliveryAdministrationStorage, EventDeliveryBatch,
    EventDeliveryClaim, EventDeliveryHealthSnapshot, EventDeliveryStorage, EventFanoutStorage,
    EventHealthStorage, EventMetricsSnapshot, EventRetentionStorage, EventRetentionSummary,
    EventSubscriptionStorage, ExportQueryStorage, ExportTemplateHistoryRecord, GroupStorage,
    HistoryAsOfQuery, HistoryListQuery, HistoryPage, HistoryPrincipalName, HistoryStorage,
    IdentityStorage, InventoryGaugeSnapshot, InventoryStorage, MetricsStorage,
    ObjectAggregateAuthorizer, ObjectAggregateStorage, ObjectAggregateStorageQuery,
    ObjectHistoryAsOfQuery, ObjectHistoryListQuery, ObjectHistoryRecord, ObjectRelationStore,
    ObjectRelationsTouchingIdsQuery, ObjectStore, OperationalExportTemplateAuditEntry,
    OperationalExportTemplateHealth, OperationalStateStorage, OperationalStorageSnapshot,
    OperationalTaskQueueSnapshot, PrincipalStorage, ReadinessSnapshot, RelatedObjectsForRootsQuery,
    RelationGraphQuery, RelationIdsQuery, RelationListQuery, RelationPage, RelationQueryStorage,
    RelationTouchingQuery, RemoteTargetHistoryRecord, StorageAuditEvent,
    StorageAuditEventListQuery, StorageCallSite, StorageClass, StorageClassCreate,
    StorageClassGraphRow, StorageClassRecord, StorageClassRelation, StorageClassRelationCreate,
    StorageClassSelector, StorageClassUpdate, StorageCollection, StorageCollectionCreate,
    StorageCollectionUpdate, StorageComputedObject, StorageDefaultAdminBootstrap, StorageError,
    StorageEventDelivery, StorageEventDeliveryListQuery, StorageEventPage, StorageEventSink,
    StorageEventSinkCreate, StorageEventSinkDelete, StorageEventSinkListQuery,
    StorageEventSinkUpdate, StorageEventSubscription, StorageEventSubscriptionCreate,
    StorageEventSubscriptionDelete, StorageEventSubscriptionListQuery,
    StorageEventSubscriptionUpdate, StorageExecution, StorageExternalPrincipalState,
    StorageExternalUserSync, StorageGroupCreate, StorageGroupListQuery, StorageGroupUpdate,
    StorageIdentity, StorageIdentityGroup, StorageIdentityPage, StorageIdentityScope,
    StorageIdentityScopeEnsure, StorageInventoryCounts, StorageLocalPasswordReset, StorageObject,
    StorageObjectAggregatePage, StorageObjectCreate, StorageObjectDataPatch, StorageObjectGraphRow,
    StorageObjectRelation, StorageObjectRelationCreate, StorageObjectRelationCreateSelector,
    StorageObjectRelationSelector, StorageObjectSelector, StorageObjectUpdate,
    StoragePoolAcquisitionState, StoragePoolCapacity, StoragePoolConnectionState, StoragePoolState,
    StoragePreparedClassRelation, StoragePreparedObjectRelation, StoragePrincipal,
    StoragePrincipalGroup, StoragePrincipalGroupListQuery, StoragePrincipalSettings,
    StoragePrincipalSettingsMutation, StorageQueryBudget, StorageRelatedObjectForRootRow,
    StorageRelatedObjectIncludeRow, StorageResolvedClass, StorageResolvedClassRelation,
    StorageResolvedObject, StorageResolvedObjectRelation, StorageRevisionPrecondition,
    StorageServiceAccount, StorageServiceAccountCreate, StorageServiceAccountListItem,
    StorageServiceAccountListQuery, StorageServiceAccountMutation, StorageServiceAccountPoint,
    StorageServiceAccountUpdate, StorageSyncedHuman, StorageTokenCreate, StorageTokenHashRevoke,
    StorageTokenListQuery, StorageTokenMetadata, StorageTokenRenew, StorageTokenRevoke,
    StorageUser, StorageUserCreate, StorageUserDelete, StorageUserListItem, StorageUserListQuery,
    StorageUserPasswordUpdate, StorageUserPoint, StorageUserUpdate, TaskGaugeSnapshot,
    TokenRetentionStorage, TokenStorage, UnifiedSearchClass, UnifiedSearchCollection,
    UnifiedSearchObject, UnifiedSearchQuery, UnifiedSearchStorage, UserStorage,
};
use super::{ClassHistoryRecord, CollectionHistoryRecord};
use crate::events::{
    EventContext, EventFanoutSettings, EventRetentionSettings, MutationProvenance,
};
use crate::models::output::EffectiveGroupPermission;
use crate::models::search::QueryOptions;
use crate::models::{CollectionID, MaintenanceState, PrincipalID, TokenRetentionSettings};
use crate::storage::postgres::operations::collection::{
    effective_group_on_from_backend, effective_principal_on_from_backend,
    group_can_on_from_backend, group_on_from_backend, groups_can_on_from_backend,
    groups_can_on_paginated_with_total_count_from_backend, groups_on_from_backend,
    groups_on_paginated_with_total_count_from_backend, principal_all_permissions_from_backend,
    principal_on_from_backend, principal_on_paginated_with_total_count_from_backend,
    user_can_on_any_from_backend,
};
use error::map_postgres_error;

#[derive(Debug)]
struct ApplicationPostgresTelemetry;

impl PostgresTelemetry for ApplicationPostgresTelemetry {
    fn connection_acquired(&self, call_site: StorageCallSite, duration: Duration) {
        crate::observability::metrics::db_connection_acquired(call_site.as_str(), duration);
    }

    fn connection_acquisition_failed(&self, call_site: StorageCallSite, duration: Duration) {
        crate::observability::metrics::db_connection_acquire_failed(call_site.as_str(), duration);
    }

    fn operation_finished(
        &self,
        call_site: StorageCallSite,
        operation: &'static str,
        duration: Duration,
        error: Option<StorageErrorKind>,
    ) {
        let result = error.map_or(crate::observability::metrics::ResultKind::Ok, |kind| {
            crate::observability::metrics::ResultKind::Error(kind.as_str())
        });
        crate::observability::metrics::db_operation_finished(
            call_site.as_str(),
            operation,
            duration,
            &result,
        );
    }

    fn computed_evaluation(&self, scope: &'static str, error_codes: &[&'static str]) {
        crate::observability::metrics::computed_evaluation_summary(scope, error_codes);
    }
}

/// Canonical production storage adapter.
#[derive(Clone)]
pub(crate) struct PostgresStorage {
    pool: PostgresPool,
    runtime: PostgresRuntime,
    notification_pool_settings: Option<Arc<PostgresPoolSettings>>,
}

#[cfg(feature = "embedded-migrations")]
pub(in crate::storage) use hubuum_storage_postgres::run_embedded_migrations;

impl PostgresStorage {
    pub(crate) fn new(pool: PostgresPool) -> Self {
        let runtime = PostgresRuntime::new(pool.clone())
            .with_telemetry(Arc::new(ApplicationPostgresTelemetry));
        Self {
            pool,
            runtime,
            notification_pool_settings: None,
        }
    }

    pub(crate) fn with_notification_pool_settings(
        pool: PostgresPool,
        notification_pool_settings: PostgresPoolSettings,
    ) -> Self {
        let mut backend = Self::new(pool);
        backend.notification_pool_settings = Some(Arc::new(notification_pool_settings));
        backend
    }

    pub(crate) fn pool(&self) -> &PostgresPool {
        &self.pool
    }

    fn runtime(&self) -> &PostgresRuntime {
        &self.runtime
    }

    fn notification_listener_pool(&self) -> PostgresPool {
        self.notification_pool_settings
            .as_deref()
            .map(runtime::init_postgres_pool_with_settings)
            .unwrap_or_else(|| self.pool.clone())
    }
}
