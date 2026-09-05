//! Public construction-path coverage for values returned by storage adapters.

use chrono::Utc;
use hubuum_domain::{
    AuthorizationGrantId, ClassId, CollectionId, EventSinkId, EventSubscriptionId,
    ExportTemplateId, GroupId, IdentityScopeId, ImportTaskResultId, MaintenanceState, ObjectId,
    PrincipalId, PrincipalKind, ResourceId, ResourceRevision, RestoreJobId, ServiceAccountId,
    TaskId, UserId,
};
use hubuum_events_core::{Action, ActorKind, EntityType, EventEnvelope, EventId, EventSequence};
use hubuum_storage_core::*;

#[test]
fn every_adapter_returned_value_exposes_a_public_construction_path() {
    let now = Utc::now();
    let collection_id = CollectionId::new(1).unwrap();
    let metadata = StorageRecordMetadata::try_new(
        ResourceId::new(1).unwrap(),
        now,
        now,
        ResourceRevision::INITIAL,
    )
    .unwrap();
    let collection =
        StorageCollection::try_new(metadata, "collection", "description", None).unwrap();
    let class_record =
        StorageClass::builder(metadata, "class", collection_id, "description").build();
    let object = StorageObject::new(
        metadata,
        "object",
        collection_id,
        ClassId::new(1).unwrap(),
        serde_json::json!({}),
        "description",
    );
    let restore_summary = StorageRestoreJobSummary::try_new(
        RestoreJobId::new(1).unwrap(),
        StorageRestoreJobStatus::Validated,
        StorageRestoreInitiator::try_new(None, "local", "administrator").unwrap(),
        StorageRestoreArtifactSummary::try_new(0, "0".repeat(64)).unwrap(),
        None,
        StorageRestoreTimestamps::try_new(now, None, None, now, now).unwrap(),
    )
    .unwrap();
    let service_account = StorageServiceAccount::try_new(
        ServiceAccountId::new(1).unwrap(),
        "description",
        GroupId::new(1).unwrap(),
        Some(PrincipalId::new(1).unwrap()),
        None,
        now,
        now,
    )
    .unwrap();
    let user =
        StorageUser::try_new(UserId::new(1).unwrap(), None, None, None, now, now, None).unwrap();
    let _ = StorageAuthenticatedToken::builder;
    let _ = StorageAuthenticationIdentity::try_new;
    let _ = StorageAuthenticationTokenScope::new;
    let _ = StorageAuthorizationClassResource::new;
    let _ = StorageAuthorizationCollection::try_new(
        collection_id,
        "collection",
        "description",
        now,
        now,
        None,
        ResourceRevision::INITIAL,
    );
    let _ = StorageAuthorizationEffectiveGroupGrant::new;
    let _ = StorageAuthorizationGrant::try_new(
        AuthorizationGrantId::new(1).unwrap(),
        collection_id,
        GroupId::new(1).unwrap(),
        Vec::<StorageAuthorizationPermission>::new(),
        now,
        now,
    );
    let _ = StorageAuthorizationGroup::new;
    let _ = StorageAuthorizationGroupGrant::try_new;
    let _ = StorageAuthorizationObjectResource::new(
        ObjectId::new(1).unwrap(),
        collection_id,
        ClassId::new(1).unwrap(),
        "object",
    );
    let _ = StorageAuthorizationPermissionSet::try_new;
    let _ = StorageAuthorizationPolicySnapshotRow::try_new;
    let _ = StorageAuthorizationPrincipal::new(PrincipalId::new(1).unwrap(), Vec::<GroupId>::new());
    let _ = StorageComputedObjectPage::try_new;
    let _ = StorageEventDeliveryBatch::new;
    let _ = StorageEventDeliveryClaim::try_new;
    let _ = StorageEventDeliverySink::try_new(
        EventSinkId::new(1).unwrap(),
        "sink",
        "webhook",
        serde_json::json!({}),
        None,
    )
    .unwrap();
    let _ = StorageEventDeliverySubscription::try_new(
        EventSubscriptionId::new(1).unwrap(),
        hubuum_domain::CollectionId::new(1).unwrap(),
        "subscription",
        serde_json::json!({}),
    )
    .unwrap();
    let _ = StorageEventDeliveryWorkItem::new;
    let _ = StorageEventDeliveryHealthSnapshot::new;
    let _ = StorageEventMetricsSnapshot::new;
    let _ = StorageEventRetentionBatch::new;
    let _ = StorageEventRetentionSummary::new;
    let _ = StorageCollectionHistoryRecord::try_new;
    let _ = StorageClassHistoryRecord::try_new;
    let _ = StorageExportTemplateHistoryRecord::try_new;
    let _ = StorageHistoryMetadata::try_new;
    let _ = StorageHistoryPrincipalName::new(PrincipalId::new(1).unwrap(), "principal");
    let _ = StorageInventoryGaugeSnapshot::try_new;
    let _ = MaintenanceState::Normal;
    let _ = StorageMutationOutcome::<()>::unchanged;
    let audit = StorageAuditReceipt::new(
        EventSequence::new(1).unwrap(),
        EventId::from(uuid::Uuid::nil()),
        EntityType::Collection,
        Action::Created,
        None,
        Some(ResourceRevision::INITIAL),
    );
    let committed = StorageMutationOutcome::committed((), audit.clone());
    assert!(committed.is_committed());
    assert_eq!(committed.audits().map(StorageAuditReceipts::len), Some(1));
    let audits = StorageAuditReceipts::new(audit.clone(), vec![audit]);
    let committed_with_audits = StorageMutationOutcome::committed_with_audits((), audits);
    assert_eq!(
        committed_with_audits
            .audits()
            .map(StorageAuditReceipts::len),
        Some(2)
    );
    let _ = StorageOperationalExportTemplateAuditEntry::new(
        ExportTemplateId::new(1).unwrap(),
        collection_id,
        "template",
        "{{ object }}",
        "application/json",
    );
    let _ = StorageOperationalExportTemplateHealth::try_new;
    let _ = StorageOperationalTaskQueueSnapshot::try_new;
    let _ = StorageReadinessSnapshot::new;
    let _ = StorageRemoteTargetHistoryRecord::try_new;
    let event = EventEnvelope::builder()
        .id(EventSequence::new(1).unwrap())
        .event_id(uuid::Uuid::nil())
        .occurred_at(now)
        .entity_type(EntityType::Collection)
        .action(Action::Created)
        .actor_kind(ActorKind::System)
        .summary("created collection".to_string())
        .metadata(serde_json::json!({}))
        .schema_version(1)
        .try_build()
        .unwrap();
    let _ = StorageRecordedEvent::new(event, None, None);
    let empty_sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    let _ = StorageBackupOutput::try_new(
        TaskId::new(1).unwrap(),
        Vec::new(),
        0,
        empty_sha256,
        now,
        now,
    )
    .unwrap();
    let _ =
        StorageBackupOutputSummary::try_new(TaskId::new(1).unwrap(), 0, empty_sha256, now).unwrap();
    let _ = StorageBackupSnapshot::try_new;
    let _ =
        StorageClassWithCollection::builder(metadata, "class", collection.clone(), "description")
            .build();
    let _ = StorageClassComputationState::try_new;
    let _ = StorageClassGraphRow::try_new;
    let _ = class_record.clone();
    let _ = StorageClassRelation::try_new;
    let _ = collection;
    let _ = StorageComputedFieldDefinition::new;
    let _ = StorageComputedFieldMutation::new;
    let _ = StorageComputedObject::new;
    let _ = StorageEventDelivery::builder;
    let _ = StorageEventSink::builder(
        EventSinkId::new(1).unwrap(),
        "sink",
        "webhook",
        now,
        now,
        ResourceRevision::INITIAL,
    )
    .try_build()
    .unwrap();
    let _ = StorageEventSubscription::builder(
        EventSubscriptionId::new(1).unwrap(),
        collection_id,
        EventSinkId::new(1).unwrap(),
        "subscription",
        now,
        now,
        ResourceRevision::INITIAL,
    )
    .entity_types(vec![EntityType::Collection])
    .actions(vec![Action::Created])
    .try_build()
    .unwrap();
    let _ = StorageExportOutput::builder(
        TaskId::new(1).unwrap(),
        "application/json",
        serde_json::json!({}),
        serde_json::json!([]),
        now,
        now,
    )
    .output(Some(serde_json::json!({})), None)
    .try_build()
    .unwrap();
    let _ = StorageExportOutputSummary::try_new(
        TaskId::new(1).unwrap(),
        None,
        "application/json",
        0,
        false,
        now,
        StorageTaskDurations::default(),
    )
    .unwrap();
    let _ = StorageExportTemplate::new(
        metadata,
        collection_id,
        "template",
        StorageExportTemplateDefinition::new(
            "description",
            "application/json",
            "{{ object }}",
            "object",
        ),
    );
    let _ = StorageExternalPrincipalState::try_new("scope", "user", "subject", None, None);
    let _ = StorageGroupMember::try_new;
    let _ = StorageIdentityGroup::builder(
        metadata,
        "group",
        "description",
        IdentityScopeId::new(1).unwrap(),
        "local",
    )
    .try_build()
    .unwrap();
    let _ = StorageIdentityScope::try_new(
        IdentityScopeId::new(1).unwrap(),
        "local",
        "local",
        now,
        now,
        ResourceRevision::INITIAL,
    );
    let _ = StorageImportApply::new;
    let _ = StorageImportPreflight::new;
    let _ = StorageImportTaskResult::builder(
        ImportTaskResultId::new(1).unwrap(),
        TaskId::new(1).unwrap(),
        "object",
        "create",
        "created",
        now,
    )
    .build();
    let _ = StorageInventoryCounts::try_new;
    let _ = StorageObjectCountByClass::try_new;
    let _ = object.clone();
    let _ = StorageObjectHistoryRecord::try_new;
    let _ = StorageObjectAggregateCursor::try_new;
    let _ = StorageObjectAggregateMeasureValue::try_new;
    let _ = StorageObjectAggregatePage::try_new;
    let _ = StorageObjectAggregateRow::try_new;
    let _ = StorageObjectGraphRow::try_new;
    let _ = StorageObjectRelation::try_new;
    let _ = StoragePage::<()>::try_new;
    let _ = StoragePreparedClassRelation::try_new;
    let _ = StoragePreparedObjectRelation::try_new;
    let _ = StoragePrincipal::builder(
        metadata,
        PrincipalKind::Human,
        "principal",
        IdentityScopeId::new(1).unwrap(),
    )
    .try_build()
    .unwrap();
    let _ = StoragePrincipalGroup::try_new;
    let _ = StoragePrincipalSettings::try_new;
    let _ = StorageRelatedObjectForRootRow::try_new;
    let _ = StorageRelatedObjectIncludeRow::try_new;
    let _ = StorageRemoteTarget::new(
        metadata,
        collection_id,
        "remote",
        StorageRemoteTargetDefinition::new(
            "description",
            StorageRemoteTargetTransport::try_new(
                StorageRemoteTargetHttpMethod::Post,
                "https://example.invalid",
                serde_json::json!({}),
                None,
                serde_json::json!({}),
                1_000,
            )
            .unwrap(),
            StorageRemoteTargetPolicy::try_new(
                Some(ClassId::new(1).unwrap()),
                vec![StorageRemoteTargetSubjectType::Object],
                true,
            )
            .unwrap(),
        ),
    );
    let _ = StorageResolvedClass::try_new;
    let _ = StorageResolvedClassRelation::try_new;
    let _ = StorageResolvedObject::try_new;
    let _ = StorageResolvedObjectRelation::try_new;
    let _ = StorageRestoreCompletion::try_new;
    let _ = StorageRestoreCoordinatorSnapshot::new;
    let _ = StorageRestoreDrainState::try_new;
    let _ = StorageRestoreJob::try_new(restore_summary.clone(), Vec::new(), "0".repeat(64));
    let _ = StorageRestoreStatus::try_new(restore_summary, "0".repeat(64), serde_json::json!({}));
    let _ = service_account.clone();
    let _ = StorageServiceAccountDetails::new(
        service_account.clone(),
        IdentityScopeId::new(1).unwrap(),
        "service-account",
        ResourceRevision::INITIAL,
    );
    let _ = StorageServiceAccountDisableOutcome::new(service_account.clone(), Vec::new());
    let _ = StorageServiceAccountListItem::new(
        service_account,
        "local",
        "service-account",
        ResourceRevision::INITIAL,
    );
    let _ = StorageSyncedHuman::try_new;
    let _ = StorageTask::builder;
    let _ = StorageTaskBuilder::try_build;
    let _ = StorageTaskProgress::try_new;
    let _ = StorageTaskAccess::new;
    let _ = StorageTaskActiveUpdate::new;
    let _ = StorageTaskClaim::try_new;
    let _ = StorageTaskCompletion::new;
    let _ = StorageTaskResultCounts::try_new;
    let _ = StorageTaskTerminalUpdate::new;
    let _ = StorageTaskEvent::builder(
        EventSequence::new(1).unwrap(),
        TaskId::new(1).unwrap(),
        "progress",
        "message",
        now,
        "system",
    )
    .build();
    let _ = StorageTaskOutputLookup::<StorageBackupOutput>::Missing;
    let _ = StorageTaskOutputLookup::<StorageBackupOutputSummary>::Missing;
    let _ = StorageTaskOutputLookup::<StorageExportOutput>::Missing;
    let _ = StorageTaskOutputLookup::<StorageExportOutputSummary>::Missing;
    let _ = StorageTokenMetadata::builder;
    let _ = StorageTaskGaugeSnapshot::try_new;
    let _ = user.clone();
    let _ = StorageUserDetails::builder(
        UserId::new(1).unwrap(),
        now,
        now,
        IdentityScopeId::new(1).unwrap(),
        "user",
        ResourceRevision::INITIAL,
    )
    .try_build()
    .unwrap();
    let _ = StorageUserListItem::builder(user, "local", "local", "user", ResourceRevision::INITIAL)
        .try_build()
        .unwrap();

    // A public builder entrypoint is insufficient if its private fields cannot
    // be turned into the boundary value. Keep every terminal method nameable
    // from this downstream integration-test crate as well.
    let _ = StorageAuthenticatedTokenBuilder::try_build;
    let _ = StorageClassWithCollectionBuilder::build;
    let _ = StorageClassBuilder::build;
    let _ = StorageEventDeliveryBuilder::try_build;
    let _ = StorageEventSinkBuilder::try_build;
    let _ = StorageEventSubscriptionBuilder::try_build;
    let _ = StorageExportOutputBuilder::try_build;
    let _ = StorageIdentityGroupBuilder::try_build;
    let _ = StorageImportTaskResultBuilder::build;
    let _ = StoragePrincipalBuilder::try_build;
    let _ = StorageTaskBuilder::try_build;
    let _ = StorageTaskEventBuilder::build;
    let _ = StorageTokenMetadataBuilder::try_build;
    let _ = StorageUserListItemBuilder::try_build;
}
