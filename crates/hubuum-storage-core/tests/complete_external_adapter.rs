//! Generated-shape compile fixture for a downstream complete adapter.
//!
//! Keep implementations behavior-free: this file proves that all required
//! trait signatures are publicly implementable. Constructor coverage lives in
//! the sibling external adapter values fixture.

#![allow(unused_variables)]

use std::future::Future;
use std::pin::Pin;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use hubuum_domain::*;
use hubuum_events_core::*;
use hubuum_query::*;
use hubuum_storage_core::capabilities::{
    backend::*, common::*, events::*, identity::*, operational::*, queries::*, resources::*,
    workflows::*,
};
use uuid::Uuid;

#[derive(Clone)]
struct CompleteExternalAdapter;

fn fixture_result<T>() -> Result<T, StorageError> {
    Err(StorageError::internal("external adapter compile fixture"))
}

#[async_trait]
impl CollectionStorage for CompleteExternalAdapter {
    async fn get_collection(&self, id: CollectionId) -> Result<StorageCollection, StorageError> {
        fixture_result()
    }

    async fn create_collection(
        &self,
        command: StorageCollectionCreate,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageCollection>, StorageError> {
        fixture_result()
    }

    async fn update_collection(
        &self,
        id: CollectionId,
        changes: StorageCollectionUpdate,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageCollection>, StorageError> {
        fixture_result()
    }

    async fn delete_collection(
        &self,
        id: CollectionId,
        context: &EventContext,
    ) -> Result<MutationOutcome<()>, StorageError> {
        fixture_result()
    }

    async fn list_collection_children(
        &self,
        id: CollectionId,
    ) -> Result<Vec<StorageCollection>, StorageError> {
        fixture_result()
    }

    async fn list_collection_ancestors(
        &self,
        id: CollectionId,
    ) -> Result<Vec<StorageCollection>, StorageError> {
        fixture_result()
    }

    async fn move_collection(
        &self,
        id: CollectionId,
        new_parent_id: CollectionId,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageCollection>, StorageError> {
        fixture_result()
    }
}

#[async_trait]
impl ClassStorage for CompleteExternalAdapter {
    async fn resolve_class(
        &self,
        selector: StorageClassSelector,
    ) -> Result<StorageResolvedClass, StorageError> {
        fixture_result()
    }

    async fn create_class(
        &self,
        command: StorageClassCreate,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageClassRecord>, StorageError> {
        fixture_result()
    }

    async fn update_class(
        &self,
        target: &StorageResolvedClass,
        changes: StorageClassUpdate,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageClassRecord>, StorageError> {
        fixture_result()
    }

    async fn delete_class(
        &self,
        target: &StorageResolvedClass,
        context: &EventContext,
    ) -> Result<MutationOutcome<()>, StorageError> {
        fixture_result()
    }

    async fn resolve_class_names(
        &self,
        class_ids: Vec<ClassId>,
    ) -> Result<Vec<(ClassId, String)>, StorageError> {
        fixture_result()
    }
}

#[async_trait]
impl ObjectStorage for CompleteExternalAdapter {
    async fn get_object(&self, object_id: ObjectId) -> Result<StorageResolvedObject, StorageError> {
        fixture_result()
    }

    async fn resolve_object(
        &self,
        selector: StorageObjectSelector,
    ) -> Result<StorageResolvedObject, StorageError> {
        fixture_result()
    }

    async fn create_object(
        &self,
        class: &StorageResolvedClass,
        command: StorageObjectCreate,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageObject>, StorageError> {
        fixture_result()
    }

    async fn update_object(
        &self,
        target: &StorageResolvedObject,
        changes: StorageObjectUpdate,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageObject>, StorageError> {
        fixture_result()
    }

    async fn patch_object_data(
        &self,
        target: &StorageResolvedObject,
        patch: StorageObjectDataPatch,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageObject>, StorageError> {
        fixture_result()
    }

    async fn delete_object(
        &self,
        target: &StorageResolvedObject,
        context: &EventContext,
    ) -> Result<MutationOutcome<()>, StorageError> {
        fixture_result()
    }

    async fn validate_object(&self, object: StorageObject) -> Result<(), StorageError> {
        fixture_result()
    }

    async fn validate_object_create(
        &self,
        command: StorageObjectCreate,
    ) -> Result<(), StorageError> {
        fixture_result()
    }

    async fn validate_object_update(
        &self,
        object_id: ObjectId,
        changes: StorageObjectUpdate,
    ) -> Result<(), StorageError> {
        fixture_result()
    }
}

#[async_trait]
impl ClassRelationStorage for CompleteExternalAdapter {
    async fn prepare_class_relation(
        &self,
        command: StorageClassRelationCreate,
    ) -> Result<StoragePreparedClassRelation, StorageError> {
        fixture_result()
    }

    async fn resolve_class_relation(
        &self,
        id: ClassRelationId,
    ) -> Result<StorageResolvedClassRelation, StorageError> {
        fixture_result()
    }

    async fn create_class_relation(
        &self,
        prepared: &StoragePreparedClassRelation,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageResolvedClassRelation>, StorageError> {
        fixture_result()
    }

    async fn delete_class_relation(
        &self,
        target: &StorageResolvedClassRelation,
        context: &EventContext,
    ) -> Result<MutationOutcome<()>, StorageError> {
        fixture_result()
    }
}

#[async_trait]
impl ObjectRelationStorage for CompleteExternalAdapter {
    async fn prepare_object_relation(
        &self,
        selector: StorageObjectRelationCreateSelector,
    ) -> Result<StoragePreparedObjectRelation, StorageError> {
        fixture_result()
    }

    async fn resolve_object_relation(
        &self,
        selector: StorageObjectRelationSelector,
    ) -> Result<StorageResolvedObjectRelation, StorageError> {
        fixture_result()
    }

    async fn create_object_relation(
        &self,
        prepared: &StoragePreparedObjectRelation,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageResolvedObjectRelation>, StorageError> {
        fixture_result()
    }

    async fn delete_object_relation(
        &self,
        target: &StorageResolvedObjectRelation,
        context: &EventContext,
    ) -> Result<MutationOutcome<()>, StorageError> {
        fixture_result()
    }
}

#[async_trait]
impl AuthenticationStorage for CompleteExternalAdapter {
    async fn authenticate_bearer_token(
        &self,
        attempt: AuthenticationAttempt,
    ) -> Result<AuthenticatedToken, StorageError> {
        fixture_result()
    }

    async fn get_authentication_identity(
        &self,
        principal_id: PrincipalId,
    ) -> Result<AuthenticationIdentity, StorageError> {
        fixture_result()
    }

    async fn get_authentication_token_scope(
        &self,
        query: AuthenticationTokenScopeQuery,
    ) -> Result<Option<AuthenticationTokenScope>, StorageError> {
        fixture_result()
    }
}

#[async_trait]
impl LocalIdentityCredentialStorage for CompleteExternalAdapter {
    async fn is_default_admin_bootstrap_required(&self) -> Result<bool, StorageError> {
        fixture_result()
    }

    async fn bootstrap_default_admin(
        &self,
        request: StorageDefaultAdminBootstrap,
    ) -> Result<bool, StorageError> {
        fixture_result()
    }

    async fn reset_local_password(
        &self,
        request: StorageLocalPasswordReset,
    ) -> Result<MutationOutcome<usize>, StorageError> {
        fixture_result()
    }
}

#[async_trait]
impl IdentityScopeStorage for CompleteExternalAdapter {
    async fn ensure_identity_scope(
        &self,
        request: StorageIdentityScopeEnsure,
    ) -> Result<StorageIdentityScope, StorageError> {
        fixture_result()
    }

    async fn resolve_identity_scope_name(
        &self,
        scope_id: IdentityScopeId,
    ) -> Result<String, StorageError> {
        fixture_result()
    }

    async fn resolve_identity_scope_names(
        &self,
        scope_ids: Vec<IdentityScopeId>,
    ) -> Result<Vec<(IdentityScopeId, String)>, StorageError> {
        fixture_result()
    }
}

#[async_trait]
impl GroupMembershipStorage for CompleteExternalAdapter {
    async fn get_principal_group(
        &self,
        principal_id: PrincipalId,
        group_id: GroupId,
    ) -> Result<StoragePrincipalGroup, StorageError> {
        fixture_result()
    }

    async fn list_principal_groups(
        &self,
        query: StoragePrincipalGroupListQuery,
    ) -> Result<StoragePage<StorageIdentityGroup>, StorageError> {
        fixture_result()
    }

    async fn is_human_owner_group_member(
        &self,
        principal_id: PrincipalId,
        owner_group_id: GroupId,
    ) -> Result<bool, StorageError> {
        fixture_result()
    }

    async fn load_group_member_principals(
        &self,
        group_id: GroupId,
    ) -> Result<Vec<StoragePrincipal>, StorageError> {
        fixture_result()
    }

    async fn list_group_members(
        &self,
        group_id: GroupId,
        query_options: QueryOptions,
    ) -> Result<StoragePage<StorageGroupMember>, StorageError> {
        fixture_result()
    }

    async fn add_group_member(
        &self,
        principal_id: PrincipalId,
        group_id: GroupId,
        context: &EventContext,
    ) -> Result<MutationOutcome<StoragePrincipalGroup>, StorageError> {
        fixture_result()
    }

    async fn remove_group_member(
        &self,
        principal_id: PrincipalId,
        group_id: GroupId,
        context: &EventContext,
    ) -> Result<MutationOutcome<()>, StorageError> {
        fixture_result()
    }
}

#[async_trait]
impl ServiceAccountStorage for CompleteExternalAdapter {
    async fn is_service_account_disabled(
        &self,
        principal_id: PrincipalId,
    ) -> Result<bool, StorageError> {
        fixture_result()
    }

    async fn get_service_account(
        &self,
        service_account_id: ServiceAccountId,
    ) -> Result<StorageServiceAccount, StorageError> {
        fixture_result()
    }

    async fn get_service_account_details(
        &self,
        service_account_id: ServiceAccountId,
    ) -> Result<StorageServiceAccountDetails, StorageError> {
        fixture_result()
    }

    async fn list_manageable_service_accounts(
        &self,
        query: StorageServiceAccountListQuery,
    ) -> Result<StoragePage<StorageServiceAccountListItem>, StorageError> {
        fixture_result()
    }

    async fn create_service_account(
        &self,
        request: StorageServiceAccountCreate,
    ) -> Result<MutationOutcome<StorageServiceAccount>, StorageError> {
        fixture_result()
    }

    async fn update_service_account(
        &self,
        request: StorageServiceAccountUpdate,
    ) -> Result<MutationOutcome<StorageServiceAccount>, StorageError> {
        fixture_result()
    }

    async fn disable_service_account(
        &self,
        request: StorageServiceAccountMutation,
    ) -> Result<MutationOutcome<StorageServiceAccountDisableOutcome>, StorageError> {
        fixture_result()
    }

    async fn delete_service_account(
        &self,
        request: StorageServiceAccountMutation,
    ) -> Result<MutationOutcome<()>, StorageError> {
        fixture_result()
    }
}

#[async_trait]
impl ExternalIdentityStorage for CompleteExternalAdapter {
    async fn get_external_principal_state(
        &self,
        principal_id: PrincipalId,
    ) -> Result<Option<StorageExternalPrincipalState>, StorageError> {
        fixture_result()
    }

    async fn mark_external_sync_attempted(
        &self,
        principal_id: PrincipalId,
    ) -> Result<(), StorageError> {
        fixture_result()
    }

    async fn sync_external_user(
        &self,
        request: StorageExternalUserSync,
    ) -> Result<MutationOutcome<StorageSyncedHuman>, StorageError> {
        fixture_result()
    }
}

#[async_trait]
impl UserStorage for CompleteExternalAdapter {
    async fn get_user(&self, id: UserId) -> Result<StorageUser, StorageError> {
        fixture_result()
    }

    async fn get_user_by_name(
        &self,
        identity_scope: String,
        name: String,
    ) -> Result<StorageUser, StorageError> {
        fixture_result()
    }

    async fn get_user_details(&self, id: UserId) -> Result<StorageUserDetails, StorageError> {
        fixture_result()
    }

    async fn list_users(
        &self,
        query: StorageUserListQuery,
    ) -> Result<StoragePage<StorageUserListItem>, StorageError> {
        fixture_result()
    }

    async fn create_user(
        &self,
        request: StorageUserCreate,
    ) -> Result<MutationOutcome<StorageUser>, StorageError> {
        fixture_result()
    }

    async fn update_user(
        &self,
        request: StorageUserUpdate,
    ) -> Result<MutationOutcome<StorageUser>, StorageError> {
        fixture_result()
    }

    async fn set_user_password(
        &self,
        request: StorageUserPasswordUpdate,
    ) -> Result<MutationOutcome<usize>, StorageError> {
        fixture_result()
    }

    async fn delete_user(
        &self,
        request: StorageUserDelete,
    ) -> Result<MutationOutcome<usize>, StorageError> {
        fixture_result()
    }

    async fn anonymize_user(
        &self,
        request: StorageUserAnonymize,
    ) -> Result<MutationOutcome<()>, StorageError> {
        fixture_result()
    }
}

#[async_trait]
impl TokenStorage for CompleteExternalAdapter {
    async fn list_retained_tokens(
        &self,
        query: StorageTokenListQuery,
    ) -> Result<StoragePage<StorageTokenMetadata>, StorageError> {
        fixture_result()
    }

    async fn create_token(
        &self,
        request: StorageTokenCreate,
    ) -> Result<MutationOutcome<StorageTokenMetadata>, StorageError> {
        fixture_result()
    }

    async fn renew_token(
        &self,
        request: StorageTokenRenew,
    ) -> Result<MutationOutcome<StorageTokenMetadata>, StorageError> {
        fixture_result()
    }

    async fn get_token_metadata(
        &self,
        principal_id: PrincipalId,
        token_id: TokenId,
        observation: StorageTokenObservation,
    ) -> Result<StorageTokenMetadata, StorageError> {
        fixture_result()
    }

    async fn load_token_metadata_by_ids(
        &self,
        token_ids: Vec<TokenId>,
        observation: StorageTokenObservation,
    ) -> Result<Vec<StorageTokenMetadata>, StorageError> {
        fixture_result()
    }

    async fn revoke_token(
        &self,
        request: StorageTokenRevoke,
    ) -> Result<MutationOutcome<usize>, StorageError> {
        fixture_result()
    }

    async fn revoke_token_by_hash(
        &self,
        request: StorageTokenHashRevoke,
    ) -> Result<MutationOutcome<usize>, StorageError> {
        fixture_result()
    }

    async fn revoke_all_principal_tokens(
        &self,
        request: StoragePrincipalTokensRevoke,
    ) -> Result<MutationOutcome<usize>, StorageError> {
        fixture_result()
    }
}

#[async_trait]
impl AuthorizationDataStorage for CompleteExternalAdapter {
    async fn get_authorization_principal(
        &self,
        principal_id: PrincipalId,
    ) -> Result<AuthorizationPrincipal, StorageError> {
        fixture_result()
    }

    async fn is_authorization_principal_group_member(
        &self,
        query: AuthorizationGroupMembershipQuery,
    ) -> Result<bool, StorageError> {
        fixture_result()
    }

    async fn list_authorization_classes(
        &self,
        query: AuthorizationResourceIds,
    ) -> Result<Vec<AuthorizationClassResource>, StorageError> {
        fixture_result()
    }

    async fn list_authorization_objects(
        &self,
        query: AuthorizationResourceIds,
    ) -> Result<Vec<AuthorizationObjectResource>, StorageError> {
        fixture_result()
    }

    async fn authorize_local_collection(
        &self,
        query: AuthorizationCollectionAccessQuery,
    ) -> Result<bool, StorageError> {
        fixture_result()
    }

    async fn authorize_local_collections(
        &self,
        query: AuthorizationCollectionsAccessQuery,
    ) -> Result<bool, StorageError> {
        fixture_result()
    }

    async fn list_local_authorized_collections(
        &self,
        query: AuthorizationCollectionsQuery,
    ) -> Result<Vec<AuthorizationCollection>, StorageError> {
        fixture_result()
    }

    async fn list_authorization_collection_candidates(
        &self,
    ) -> Result<Vec<AuthorizationCollection>, StorageError> {
        fixture_result()
    }

    async fn list_authorization_group_candidates(
        &self,
        query_options: QueryOptions,
    ) -> Result<Vec<AuthorizationGroup>, StorageError> {
        fixture_result()
    }

    async fn get_authorization_policy_snapshot(
        &self,
    ) -> Result<Vec<AuthorizationPolicySnapshotRow>, StorageError> {
        fixture_result()
    }

    async fn list_local_collection_grants(
        &self,
        query: AuthorizationCollectionGrantListQuery,
    ) -> Result<StoragePage<AuthorizationGroupGrant>, StorageError> {
        fixture_result()
    }

    async fn get_local_collection_grant(
        &self,
        key: AuthorizationGrantKey,
    ) -> Result<Option<AuthorizationGrant>, StorageError> {
        fixture_result()
    }

    async fn get_local_collection_permission_set(
        &self,
        query: AuthorizationPermissionSetQuery,
    ) -> Result<AuthorizationPermissionSet, StorageError> {
        fixture_result()
    }

    async fn apply_local_collection_grant(
        &self,
        mutation: AuthorizationGrantMutation,
    ) -> Result<MutationOutcome<AuthorizationGrant>, StorageError> {
        fixture_result()
    }

    async fn revoke_local_collection_grant(
        &self,
        mutation: AuthorizationGrantMutation,
    ) -> Result<MutationOutcome<AuthorizationGrant>, StorageError> {
        fixture_result()
    }

    async fn revoke_all_local_collection_grants(
        &self,
        request: AuthorizationGrantDelete,
    ) -> Result<MutationOutcome<()>, StorageError> {
        fixture_result()
    }
}

#[async_trait]
impl CatalogStorage for CompleteExternalAdapter {
    async fn list_collections(
        &self,
        query: CatalogListQuery,
    ) -> Result<StoragePage<StorageCollection>, StorageError> {
        fixture_result()
    }

    async fn list_classes(
        &self,
        query: CatalogListQuery,
    ) -> Result<StoragePage<StorageClass>, StorageError> {
        fixture_result()
    }

    async fn list_objects(
        &self,
        query: CatalogListQuery,
    ) -> Result<StoragePage<StorageObject>, StorageError> {
        fixture_result()
    }
}

#[async_trait]
impl ComputedFieldStorage for CompleteExternalAdapter {
    async fn get_computed_field_state(
        &self,
        class_id: ClassId,
    ) -> Result<StorageClassComputationState, StorageError> {
        fixture_result()
    }

    async fn list_shared_computed_fields(
        &self,
        class_id: ClassId,
    ) -> Result<Vec<StorageComputedFieldDefinition>, StorageError> {
        fixture_result()
    }

    async fn list_personal_computed_fields(
        &self,
        query: StoragePersonalComputedFieldListQuery,
    ) -> Result<StoragePage<StorageComputedFieldDefinition>, StorageError> {
        fixture_result()
    }

    async fn get_computed_field(
        &self,
        definition_id: ComputedFieldDefinitionId,
    ) -> Result<StorageComputedFieldDefinition, StorageError> {
        fixture_result()
    }

    async fn create_shared_computed_field(
        &self,
        request: StorageSharedComputedFieldCreate,
    ) -> Result<MutationOutcome<StorageComputedFieldMutation>, StorageError> {
        fixture_result()
    }

    async fn update_shared_computed_field(
        &self,
        request: StorageSharedComputedFieldUpdate,
    ) -> Result<MutationOutcome<StorageComputedFieldMutation>, StorageError> {
        fixture_result()
    }

    async fn delete_shared_computed_field(
        &self,
        request: StorageSharedComputedFieldDelete,
    ) -> Result<MutationOutcome<StorageClassComputationState>, StorageError> {
        fixture_result()
    }

    async fn create_personal_computed_field(
        &self,
        request: StoragePersonalComputedFieldCreate,
    ) -> Result<MutationOutcome<StorageComputedFieldDefinition>, StorageError> {
        fixture_result()
    }

    async fn update_personal_computed_field(
        &self,
        request: StoragePersonalComputedFieldUpdate,
    ) -> Result<MutationOutcome<StorageComputedFieldDefinition>, StorageError> {
        fixture_result()
    }

    async fn delete_personal_computed_field(
        &self,
        request: StoragePersonalComputedFieldDelete,
    ) -> Result<MutationOutcome<()>, StorageError> {
        fixture_result()
    }

    async fn request_computed_field_rebuild(
        &self,
        request: StorageComputedFieldRebuildRequest,
    ) -> Result<StorageClassComputationState, StorageError> {
        fixture_result()
    }

    async fn execute_computed_field_rebuild(
        &self,
        lease: StorageTaskLease,
    ) -> Result<StorageTask, StorageError> {
        fixture_result()
    }
}

#[async_trait]
impl ComputedObjectStorage for CompleteExternalAdapter {
    async fn list_computed_objects(
        &self,
        query: ComputedObjectListQuery,
    ) -> Result<ComputedObjectPage, StorageError> {
        fixture_result()
    }

    async fn enrich_objects_with_computed(
        &self,
        query: ComputedObjectEnrichmentQuery,
    ) -> Result<Vec<StorageComputedObject>, StorageError> {
        fixture_result()
    }
}

#[async_trait]
impl ObjectAggregateStorage for CompleteExternalAdapter {
    async fn aggregate_objects(
        &self,
        query: ObjectAggregateStorageQuery,
        authorization: ObjectAggregateAuthorization<'_>,
    ) -> Result<StorageObjectAggregatePage, StorageError> {
        fixture_result()
    }
}

#[async_trait]
impl RelationQueryStorage for CompleteExternalAdapter {
    async fn list_class_relations(
        &self,
        query: RelationListQuery,
    ) -> Result<StoragePage<StorageClassRelation>, StorageError> {
        fixture_result()
    }

    async fn list_object_relations(
        &self,
        query: RelationListQuery,
    ) -> Result<StoragePage<StorageObjectRelation>, StorageError> {
        fixture_result()
    }

    async fn list_class_relations_touching(
        &self,
        query: RelationTouchingQuery,
    ) -> Result<StoragePage<StorageClassRelation>, StorageError> {
        fixture_result()
    }

    async fn list_object_relations_touching(
        &self,
        query: RelationTouchingQuery,
    ) -> Result<StoragePage<StorageObjectRelation>, StorageError> {
        fixture_result()
    }

    async fn list_class_relations_touching_ids(
        &self,
        query: RelationIdsQuery,
    ) -> Result<Vec<StorageClassRelation>, StorageError> {
        fixture_result()
    }

    async fn list_class_relations_between_ids(
        &self,
        query: RelationIdsQuery,
    ) -> Result<Vec<StorageClassRelation>, StorageError> {
        fixture_result()
    }

    async fn list_object_relations_touching_ids(
        &self,
        query: ObjectRelationsTouchingIdsQuery,
    ) -> Result<Vec<StorageObjectRelation>, StorageError> {
        fixture_result()
    }

    async fn list_object_relations_between_ids(
        &self,
        query: RelationIdsQuery,
    ) -> Result<Vec<StorageObjectRelation>, StorageError> {
        fixture_result()
    }

    async fn list_related_classes(
        &self,
        query: RelationGraphQuery,
    ) -> Result<StoragePage<StorageClassGraphRow>, StorageError> {
        fixture_result()
    }

    async fn list_related_objects(
        &self,
        query: RelationGraphQuery,
    ) -> Result<StoragePage<StorageObjectGraphRow>, StorageError> {
        fixture_result()
    }

    async fn list_related_objects_for_roots(
        &self,
        query: RelatedObjectsForRootsQuery,
    ) -> Result<Vec<StorageRelatedObjectIncludeRow>, StorageError> {
        fixture_result()
    }

    async fn list_bidirectionally_related_objects_for_roots(
        &self,
        query: BidirectionalRelatedObjectsQuery,
    ) -> Result<Vec<StorageRelatedObjectForRootRow>, StorageError> {
        fixture_result()
    }
}

#[async_trait]
impl AuditEventStorage for CompleteExternalAdapter {
    async fn list_audit_events(
        &self,
        query: StorageAuditEventListQuery,
    ) -> Result<StoragePage<StorageAuditEvent>, StorageError> {
        fixture_result()
    }
}

#[async_trait]
impl EventConfigurationStorage for CompleteExternalAdapter {
    async fn count_enabled_event_sinks(&self) -> Result<i64, StorageError> {
        fixture_result()
    }

    async fn list_event_sinks(
        &self,
        query: StorageEventSinkListQuery,
    ) -> Result<StoragePage<StorageEventSink>, StorageError> {
        fixture_result()
    }

    async fn get_event_sink(&self, sink_id: EventSinkId) -> Result<StorageEventSink, StorageError> {
        fixture_result()
    }

    async fn create_event_sink(
        &self,
        request: StorageEventSinkCreate,
    ) -> Result<MutationOutcome<StorageEventSink>, StorageError> {
        fixture_result()
    }

    async fn update_event_sink(
        &self,
        request: StorageEventSinkUpdate,
    ) -> Result<MutationOutcome<StorageEventSink>, StorageError> {
        fixture_result()
    }

    async fn delete_event_sink(
        &self,
        request: StorageEventSinkDelete,
    ) -> Result<MutationOutcome<()>, StorageError> {
        fixture_result()
    }

    async fn list_event_subscriptions(
        &self,
        query: StorageEventSubscriptionListQuery,
    ) -> Result<StoragePage<StorageEventSubscription>, StorageError> {
        fixture_result()
    }

    async fn get_event_subscription(
        &self,
        collection_id: CollectionId,
        subscription_id: EventSubscriptionId,
    ) -> Result<StorageEventSubscription, StorageError> {
        fixture_result()
    }

    async fn create_event_subscription(
        &self,
        request: StorageEventSubscriptionCreate,
    ) -> Result<MutationOutcome<StorageEventSubscription>, StorageError> {
        fixture_result()
    }

    async fn update_event_subscription(
        &self,
        request: StorageEventSubscriptionUpdate,
    ) -> Result<MutationOutcome<StorageEventSubscription>, StorageError> {
        fixture_result()
    }

    async fn delete_event_subscription(
        &self,
        request: StorageEventSubscriptionDelete,
    ) -> Result<MutationOutcome<()>, StorageError> {
        fixture_result()
    }
}

#[async_trait]
impl EventDeliveryAdministrationStorage for CompleteExternalAdapter {
    async fn list_event_deliveries(
        &self,
        query: StorageEventDeliveryListQuery,
    ) -> Result<StoragePage<StorageEventDelivery>, StorageError> {
        fixture_result()
    }

    async fn get_event_delivery(
        &self,
        delivery_id: EventDeliveryId,
    ) -> Result<StorageEventDelivery, StorageError> {
        fixture_result()
    }

    async fn release_event_delivery_for_retry(
        &self,
        delivery_id: EventDeliveryId,
    ) -> Result<StorageEventDelivery, StorageError> {
        fixture_result()
    }

    async fn mark_event_delivery_dead(
        &self,
        delivery_id: EventDeliveryId,
    ) -> Result<StorageEventDelivery, StorageError> {
        fixture_result()
    }
}

#[async_trait]
impl EventDeliveryWorkerStorage for CompleteExternalAdapter {
    async fn claim_event_delivery_batch(
        &self,
        settings: hubuum_domain::EventDeliverySettings,
    ) -> Result<EventDeliveryBatch, StorageError> {
        fixture_result()
    }

    async fn mark_event_delivery_succeeded(
        &self,
        claim: &EventDeliveryClaim,
    ) -> Result<(), StorageError> {
        fixture_result()
    }

    async fn mark_event_delivery_failed(
        &self,
        claim: &EventDeliveryClaim,
        settings: hubuum_domain::EventDeliverySettings,
        error: &str,
    ) -> Result<(), StorageError> {
        fixture_result()
    }
}

#[async_trait]
impl EventFanoutStorage for CompleteExternalAdapter {
    async fn process_event_fanout_batch(
        &self,
        settings: EventFanoutSettings,
    ) -> Result<usize, StorageError> {
        fixture_result()
    }
}

#[async_trait]
impl EventHealthStorage for CompleteExternalAdapter {
    async fn get_event_delivery_health(&self) -> Result<EventDeliveryHealthSnapshot, StorageError> {
        fixture_result()
    }
}

#[async_trait]
impl EventRetentionStorage for CompleteExternalAdapter {
    async fn claim_event_retention_batch(
        &self,
        settings: EventRetentionSettings,
    ) -> Result<Option<EventRetentionBatch>, StorageError> {
        fixture_result()
    }

    async fn complete_event_retention_batch(
        &self,
        batch_id: EventRetentionBatchId,
    ) -> Result<EventRetentionSummary, StorageError> {
        fixture_result()
    }
}

#[async_trait]
impl HistoryStorage for CompleteExternalAdapter {
    async fn resolve_history_principal_names(
        &self,
        principal_ids: Vec<PrincipalId>,
    ) -> Result<Vec<HistoryPrincipalName>, StorageError> {
        fixture_result()
    }

    async fn list_collection_history(
        &self,
        query: HistoryListQuery,
    ) -> Result<StoragePage<CollectionHistoryRecord>, StorageError> {
        fixture_result()
    }

    async fn get_collection_history_as_of(
        &self,
        query: HistoryAsOfQuery,
    ) -> Result<Option<CollectionHistoryRecord>, StorageError> {
        fixture_result()
    }

    async fn list_class_history(
        &self,
        query: HistoryListQuery,
    ) -> Result<StoragePage<ClassHistoryRecord>, StorageError> {
        fixture_result()
    }

    async fn get_class_history_as_of(
        &self,
        query: HistoryAsOfQuery,
    ) -> Result<Option<ClassHistoryRecord>, StorageError> {
        fixture_result()
    }

    async fn list_object_history(
        &self,
        query: ObjectHistoryListQuery,
    ) -> Result<StoragePage<ObjectHistoryRecord>, StorageError> {
        fixture_result()
    }

    async fn get_object_history_as_of(
        &self,
        query: ObjectHistoryAsOfQuery,
    ) -> Result<Option<ObjectHistoryRecord>, StorageError> {
        fixture_result()
    }

    async fn list_export_template_history(
        &self,
        query: HistoryListQuery,
    ) -> Result<StoragePage<ExportTemplateHistoryRecord>, StorageError> {
        fixture_result()
    }

    async fn get_export_template_history_as_of(
        &self,
        query: HistoryAsOfQuery,
    ) -> Result<Option<ExportTemplateHistoryRecord>, StorageError> {
        fixture_result()
    }

    async fn list_remote_target_history(
        &self,
        query: HistoryListQuery,
    ) -> Result<StoragePage<RemoteTargetHistoryRecord>, StorageError> {
        fixture_result()
    }

    async fn get_remote_target_history_as_of(
        &self,
        query: HistoryAsOfQuery,
    ) -> Result<Option<RemoteTargetHistoryRecord>, StorageError> {
        fixture_result()
    }
}

#[async_trait]
impl InventoryStorage for CompleteExternalAdapter {
    async fn get_inventory_counts(&self) -> Result<StorageInventoryCounts, StorageError> {
        fixture_result()
    }
}

#[async_trait]
impl MetricsStorage for CompleteExternalAdapter {
    async fn get_inventory_metrics_snapshot(&self) -> Result<InventoryGaugeSnapshot, StorageError> {
        fixture_result()
    }

    async fn get_task_metrics_snapshot(&self) -> Result<TaskGaugeSnapshot, StorageError> {
        fixture_result()
    }

    async fn get_event_metrics_snapshot(&self) -> Result<EventMetricsSnapshot, StorageError> {
        fixture_result()
    }
}

#[async_trait]
impl OperationalStateStorage for CompleteExternalAdapter {
    async fn get_readiness_snapshot(&self) -> Result<ReadinessSnapshot, StorageError> {
        fixture_result()
    }

    async fn get_maintenance_state(&self) -> Result<MaintenanceState, StorageError> {
        fixture_result()
    }

    async fn get_task_queue_snapshot(&self) -> Result<OperationalTaskQueueSnapshot, StorageError> {
        fixture_result()
    }

    async fn load_export_template_health(
        &self,
    ) -> Result<Vec<OperationalExportTemplateHealth>, StorageError> {
        fixture_result()
    }

    async fn list_export_templates_for_audit(
        &self,
    ) -> Result<Vec<OperationalExportTemplateAuditEntry>, StorageError> {
        fixture_result()
    }
}

#[async_trait]
impl TokenRetentionStorage for CompleteExternalAdapter {
    async fn purge_expired_tokens(
        &self,
        settings: TokenRetentionSettings,
    ) -> Result<usize, StorageError> {
        fixture_result()
    }
}

#[async_trait]
impl UnifiedSearchStorage for CompleteExternalAdapter {
    async fn search_collections(
        &self,
        query: UnifiedSearchQuery,
    ) -> Result<Vec<StorageCollection>, StorageError> {
        fixture_result()
    }

    async fn search_classes(
        &self,
        query: UnifiedSearchQuery,
    ) -> Result<Vec<StorageClass>, StorageError> {
        fixture_result()
    }

    async fn search_objects(
        &self,
        query: UnifiedSearchQuery,
    ) -> Result<Vec<StorageObject>, StorageError> {
        fixture_result()
    }
}

#[async_trait]
impl GroupStorage for CompleteExternalAdapter {
    async fn list_groups(
        &self,
        query: StorageGroupListQuery,
    ) -> Result<StoragePage<StorageIdentityGroup>, StorageError> {
        fixture_result()
    }

    async fn get_group(&self, group_id: GroupId) -> Result<StorageIdentityGroup, StorageError> {
        fixture_result()
    }

    async fn resolve_group_identity_scope_name(
        &self,
        group_id: GroupId,
    ) -> Result<String, StorageError> {
        fixture_result()
    }

    async fn create_group(
        &self,
        command: StorageGroupCreate,
        context: &EventContext,
    ) -> Result<crate::MutationOutcome<StorageIdentityGroup>, StorageError> {
        fixture_result()
    }

    async fn update_group(
        &self,
        group_id: GroupId,
        update: StorageGroupUpdate,
        context: &EventContext,
    ) -> Result<crate::MutationOutcome<StorageIdentityGroup>, StorageError> {
        fixture_result()
    }

    async fn delete_group(
        &self,
        group_id: GroupId,
        context: &EventContext,
    ) -> Result<crate::MutationOutcome<usize>, StorageError> {
        fixture_result()
    }
}

#[async_trait]
impl PrincipalStorage for CompleteExternalAdapter {
    async fn get_principal(
        &self,
        principal_id: PrincipalId,
    ) -> Result<StoragePrincipal, StorageError> {
        fixture_result()
    }

    async fn get_principal_settings(
        &self,
        principal_id: PrincipalId,
    ) -> Result<StoragePrincipalSettings, StorageError> {
        fixture_result()
    }

    async fn update_principal_settings(
        &self,
        principal_id: PrincipalId,
        mutation: StoragePrincipalSettingsMutation,
        context: &EventContext,
    ) -> Result<crate::MutationOutcome<StoragePrincipalSettings>, StorageError> {
        fixture_result()
    }
}

#[async_trait]
impl CollectionAuthorizationQueryStorage for CompleteExternalAdapter {
    async fn load_principal_collection_permissions(
        &self,
        query: AuthorizationPrincipalCollectionQuery,
    ) -> Result<Vec<AuthorizationGroupGrant>, StorageError> {
        fixture_result()
    }

    async fn list_all_principal_collection_permissions(
        &self,
        principal_id: PrincipalId,
    ) -> Result<Vec<AuthorizationPolicySnapshotRow>, StorageError> {
        fixture_result()
    }

    async fn list_principal_collection_permissions(
        &self,
        query: AuthorizationPrincipalCollectionPageQuery,
    ) -> Result<StoragePage<AuthorizationGroupGrant>, StorageError> {
        fixture_result()
    }

    async fn list_effective_principal_collection_permissions(
        &self,
        query: AuthorizationPrincipalCollectionQuery,
    ) -> Result<Vec<AuthorizationEffectiveGroupGrant>, StorageError> {
        fixture_result()
    }

    async fn list_visible_collections(
        &self,
        query: AuthorizationCollectionVisibilityQuery,
    ) -> Result<Vec<AuthorizationCollection>, StorageError> {
        fixture_result()
    }

    async fn has_group_collection_permission(
        &self,
        query: AuthorizationGroupCollectionQuery,
    ) -> Result<bool, StorageError> {
        fixture_result()
    }

    async fn list_effective_group_collection_permissions(
        &self,
        collection_id: CollectionId,
        group_id: GroupId,
    ) -> Result<Vec<AuthorizationEffectiveGroupGrant>, StorageError> {
        fixture_result()
    }

    async fn load_groups_with_collection_permission(
        &self,
        query: AuthorizationCollectionGroupsQuery,
    ) -> Result<Vec<AuthorizationGroup>, StorageError> {
        fixture_result()
    }

    async fn list_groups_with_collection_permission(
        &self,
        query: AuthorizationCollectionGroupsPageQuery,
    ) -> Result<StoragePage<AuthorizationGroup>, StorageError> {
        fixture_result()
    }

    async fn load_collection_group_permissions(
        &self,
        query: AuthorizationCollectionGrantListQuery,
    ) -> Result<Vec<AuthorizationGroupGrant>, StorageError> {
        fixture_result()
    }

    async fn list_collection_group_permissions(
        &self,
        query: AuthorizationCollectionGrantListQuery,
    ) -> Result<StoragePage<AuthorizationGroupGrant>, StorageError> {
        fixture_result()
    }

    async fn get_collection_group_permission(
        &self,
        collection_id: CollectionId,
        group_id: GroupId,
    ) -> Result<AuthorizationGrant, StorageError> {
        fixture_result()
    }
}

#[async_trait]
impl RemoteTargetStorage for CompleteExternalAdapter {
    async fn get_remote_target(
        &self,
        target_id: RemoteTargetId,
    ) -> Result<StorageRemoteTarget, StorageError> {
        fixture_result()
    }

    async fn list_remote_targets(
        &self,
        query: StorageRemoteTargetListQuery,
    ) -> Result<StoragePage<StorageRemoteTarget>, StorageError> {
        fixture_result()
    }

    async fn create_remote_target(
        &self,
        request: StorageRemoteTargetCreate,
    ) -> Result<MutationOutcome<StorageRemoteTarget>, StorageError> {
        fixture_result()
    }

    async fn update_remote_target(
        &self,
        request: StorageRemoteTargetUpdate,
    ) -> Result<MutationOutcome<StorageRemoteTarget>, StorageError> {
        fixture_result()
    }

    async fn delete_remote_target(
        &self,
        request: StorageRemoteTargetDelete,
    ) -> Result<MutationOutcome<()>, StorageError> {
        fixture_result()
    }

    async fn record_remote_target_invocation(
        &self,
        request: StorageRemoteTargetInvocation,
    ) -> Result<MutationOutcome<()>, StorageError> {
        fixture_result()
    }
}

#[async_trait]
impl TaskQueueStorage for CompleteExternalAdapter {
    async fn create_task(
        &self,
        request: StorageTaskCreateRequest,
    ) -> Result<StorageTask, StorageError> {
        fixture_result()
    }

    async fn get_task_access(&self, task_id: TaskId) -> Result<StorageTaskAccess, StorageError> {
        fixture_result()
    }

    async fn list_tasks(
        &self,
        query: StorageTaskListQuery,
    ) -> Result<StoragePage<StorageTask>, StorageError> {
        fixture_result()
    }

    async fn list_task_events(
        &self,
        query: StorageTaskPageQuery,
    ) -> Result<StoragePage<StorageTaskEvent>, StorageError> {
        fixture_result()
    }

    async fn list_import_task_results(
        &self,
        query: StorageTaskPageQuery,
    ) -> Result<StoragePage<StorageImportTaskResult>, StorageError> {
        fixture_result()
    }

    async fn list_export_output_summaries(
        &self,
        task_ids: Vec<TaskId>,
    ) -> Result<Vec<StorageExportOutputSummary>, StorageError> {
        fixture_result()
    }

    async fn list_backup_output_summaries(
        &self,
        task_ids: Vec<TaskId>,
    ) -> Result<Vec<StorageBackupOutputSummary>, StorageError> {
        fixture_result()
    }

    async fn get_export_output_summary(
        &self,
        task_id: TaskId,
    ) -> Result<StorageTaskOutputLookup<StorageExportOutputSummary>, StorageError> {
        fixture_result()
    }

    async fn get_backup_output_summary(
        &self,
        task_id: TaskId,
    ) -> Result<StorageTaskOutputLookup<StorageBackupOutputSummary>, StorageError> {
        fixture_result()
    }

    async fn get_export_output(
        &self,
        task_id: TaskId,
    ) -> Result<StorageTaskOutputLookup<StorageExportOutput>, StorageError> {
        fixture_result()
    }

    async fn get_backup_output(
        &self,
        task_id: TaskId,
    ) -> Result<StorageTaskOutputLookup<StorageBackupOutput>, StorageError> {
        fixture_result()
    }
}

#[async_trait]
impl TaskExecutionStorage for CompleteExternalAdapter {
    async fn claim_next_task(
        &self,
        lease_duration: StorageTaskLeaseDuration,
    ) -> Result<Option<StorageTaskClaim>, StorageError> {
        fixture_result()
    }

    async fn renew_task_lease(
        &self,
        lease: StorageTaskLease,
        lease_duration: StorageTaskLeaseDuration,
    ) -> Result<bool, StorageError> {
        fixture_result()
    }

    async fn recover_expired_task_leases(
        &self,
        batch_size: usize,
    ) -> Result<Vec<StorageTask>, StorageError> {
        fixture_result()
    }

    async fn append_task_event(&self, event: StorageTaskEventAppend) -> Result<(), StorageError> {
        fixture_result()
    }

    async fn update_task_state(
        &self,
        update: StorageTaskStateUpdate,
    ) -> Result<StorageTask, StorageError> {
        fixture_result()
    }

    async fn complete_task(
        &self,
        completion: StorageTaskCompletion,
    ) -> Result<StorageTask, StorageError> {
        fixture_result()
    }

    async fn fail_task(&self, failure: StorageTaskFailure) -> Result<StorageTask, StorageError> {
        fixture_result()
    }

    async fn purge_expired_export_outputs(&self) -> Result<usize, StorageError> {
        fixture_result()
    }

    async fn purge_expired_backup_outputs(&self) -> Result<usize, StorageError> {
        fixture_result()
    }
}

#[async_trait]
impl BackupSnapshotStorage for CompleteExternalAdapter {
    async fn capture_backup_snapshot(
        &self,
        include_history: bool,
    ) -> Result<StorageBackupSnapshot, StorageError> {
        fixture_result()
    }
}

#[async_trait]
impl RestoreStorage for CompleteExternalAdapter {
    async fn stage_restore(
        &self,
        request: StorageRestoreStageCreate,
    ) -> Result<StorageRestoreJob, StorageError> {
        fixture_result()
    }

    async fn get_restore_job(
        &self,
        job_id: RestoreJobId,
    ) -> Result<StorageRestoreJob, StorageError> {
        fixture_result()
    }

    async fn get_restore_status(
        &self,
        job_id: RestoreJobId,
    ) -> Result<StorageRestoreStatus, StorageError> {
        fixture_result()
    }

    async fn expire_restore_stage(&self, job_id: RestoreJobId) -> Result<bool, StorageError> {
        fixture_result()
    }

    async fn start_restore_draining(
        &self,
        job_id: RestoreJobId,
    ) -> Result<DateTime<Utc>, StorageError> {
        fixture_result()
    }

    async fn apply_restore(
        &self,
        request: StorageRestoreApply,
    ) -> Result<StorageRestoreCompletion, StorageError> {
        fixture_result()
    }

    async fn fail_restore_and_resume(
        &self,
        request: StorageRestoreFailure,
    ) -> Result<(), StorageError> {
        fixture_result()
    }

    async fn get_restore_coordinator_snapshot(
        &self,
    ) -> Result<StorageRestoreCoordinatorSnapshot, StorageError> {
        fixture_result()
    }

    async fn resume_maintenance_without_restore(&self) -> Result<(), StorageError> {
        fixture_result()
    }

    async fn resume_terminal_restore(&self, job_id: RestoreJobId) -> Result<(), StorageError> {
        fixture_result()
    }

    async fn tick_restore_coordinator(
        &self,
        instance_id: Uuid,
        local_work_is_idle: &(dyn Fn() -> bool + Send + Sync),
        expire_validated_jobs: bool,
    ) -> Result<StorageRestoreCoordinatorSnapshot, StorageError> {
        fixture_result()
    }

    async fn get_restore_drain_state(
        &self,
        heartbeat_cutoff: DateTime<Utc>,
    ) -> Result<StorageRestoreDrainState, StorageError> {
        fixture_result()
    }

    async fn remove_restore_instance(&self, instance_id: Uuid) -> Result<(), StorageError> {
        fixture_result()
    }
}

#[async_trait]
impl ImportStorage for CompleteExternalAdapter {
    async fn get_import_root_collection(&self) -> Result<StorageCollection, StorageError> {
        fixture_result()
    }

    async fn get_import_collection_by_id(
        &self,
        collection_id: CollectionId,
    ) -> Result<Option<StorageCollection>, StorageError> {
        fixture_result()
    }

    async fn get_import_collection_by_key(
        &self,
        key: &StorageImportCollectionKey,
    ) -> Result<Option<StorageCollection>, StorageError> {
        fixture_result()
    }

    async fn list_import_collections_by_name(
        &self,
        name: &str,
    ) -> Result<Vec<StorageCollection>, StorageError> {
        fixture_result()
    }

    async fn get_import_collection_child_by_name(
        &self,
        parent_collection_id: CollectionId,
        name: &str,
    ) -> Result<Option<StorageCollection>, StorageError> {
        fixture_result()
    }

    async fn get_import_class_by_name(
        &self,
        collection_id: CollectionId,
        name: &str,
    ) -> Result<Option<StorageClassRecord>, StorageError> {
        fixture_result()
    }

    async fn list_import_classes_by_names(
        &self,
        collection_id: CollectionId,
        names: &[String],
    ) -> Result<Vec<StorageClassRecord>, StorageError> {
        fixture_result()
    }

    async fn get_import_object_by_name(
        &self,
        class_id: ClassId,
        name: &str,
    ) -> Result<Option<StorageObject>, StorageError> {
        fixture_result()
    }

    async fn list_import_objects_by_names(
        &self,
        class_id: ClassId,
        names: &[String],
    ) -> Result<Vec<StorageObject>, StorageError> {
        fixture_result()
    }

    async fn has_import_class_relation(
        &self,
        left_class_id: ClassId,
        right_class_id: ClassId,
    ) -> Result<bool, StorageError> {
        fixture_result()
    }

    async fn has_import_object_relation(
        &self,
        left_object_id: ObjectId,
        right_object_id: ObjectId,
    ) -> Result<bool, StorageError> {
        fixture_result()
    }

    async fn has_import_group(
        &self,
        identity_scope: &str,
        group_name: &str,
    ) -> Result<bool, StorageError> {
        fixture_result()
    }

    async fn preflight_import(
        &self,
        plan: StorageImportPlan,
        mode: StorageImportMode,
    ) -> Result<StorageImportPreflight, StorageError> {
        fixture_result()
    }

    async fn apply_import_strict(&self, plan: StorageImportPlan) -> Result<(), StorageError> {
        fixture_result()
    }

    async fn apply_import_best_effort(
        &self,
        plan: StorageImportPlan,
        mode: StorageImportMode,
    ) -> Result<StorageImportApply, StorageError> {
        fixture_result()
    }

    async fn record_import_results(
        &self,
        results: Vec<StorageImportResult>,
    ) -> Result<(), StorageError> {
        fixture_result()
    }
}

#[async_trait]
impl ExportTemplateStorage for CompleteExternalAdapter {
    async fn get_export_template(
        &self,
        template_id: ExportTemplateId,
    ) -> Result<StorageExportTemplate, StorageError> {
        fixture_result()
    }

    async fn list_export_templates(
        &self,
        query: StorageExportTemplateListQuery,
    ) -> Result<StoragePage<StorageExportTemplate>, StorageError> {
        fixture_result()
    }

    async fn list_export_templates_in_collection(
        &self,
        collection_id: CollectionId,
        exclude_template_id: Option<ExportTemplateId>,
    ) -> Result<Vec<StorageExportTemplate>, StorageError> {
        fixture_result()
    }

    async fn get_export_template_class_collection_id(
        &self,
        class_id: ClassId,
    ) -> Result<Option<CollectionId>, StorageError> {
        fixture_result()
    }

    async fn create_export_template(
        &self,
        request: StorageExportTemplateCreate,
    ) -> Result<MutationOutcome<StorageExportTemplate>, StorageError> {
        fixture_result()
    }

    async fn replace_export_template(
        &self,
        request: StorageExportTemplateReplace,
    ) -> Result<MutationOutcome<StorageExportTemplate>, StorageError> {
        fixture_result()
    }

    async fn delete_export_template(
        &self,
        request: StorageExportTemplateDelete,
    ) -> Result<MutationOutcome<()>, StorageError> {
        fixture_result()
    }
}

impl ExecutionStorage for CompleteExternalAdapter {
    fn run_in_scope<'a, F, R>(
        &'a self,
        scope: StorageExecutionScope,
        future: F,
    ) -> Pin<Box<dyn Future<Output = R> + 'a>>
    where
        F: Future<Output = R> + 'a,
        R: 'a,
    {
        Box::pin(future)
    }

    fn run_in_scope_send<'a, F, R>(
        &'a self,
        scope: StorageExecutionScope,
        future: F,
    ) -> Pin<Box<dyn Future<Output = R> + Send + 'a>>
    where
        F: Future<Output = R> + Send + 'a,
        R: Send + 'a,
    {
        Box::pin(future)
    }
}

#[async_trait]
impl TransactionStorage for CompleteExternalAdapter {
    async fn with_transaction<F, R>(
        &self,
        event_context: EventContext,
        operation: F,
    ) -> Result<R, StorageError>
    where
        F: for<'transaction> FnOnce(
                &'transaction dyn StorageTransaction,
            ) -> StorageTransactionFuture<'transaction, R>
            + Send,
        R: Send,
    {
        fixture_result()
    }
}

impl StorageBackend for CompleteExternalAdapter {}

#[test]
fn an_external_crate_can_implement_the_complete_backend_contract() {
    fn assert_complete<T: StorageBackend + Clone + 'static>() {}
    assert_complete::<CompleteExternalAdapter>();
}
