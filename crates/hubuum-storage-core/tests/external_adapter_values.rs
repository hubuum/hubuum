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
    let collection = StorageCollection::new(metadata, "collection", "description", None);
    let class_record =
        StorageClassRecord::builder(metadata, "class", collection_id, "description").build();
    let object = StorageObject::new(
        metadata,
        "object",
        collection_id,
        ClassId::new(1).unwrap(),
        serde_json::json!({}),
        "description",
    );
    let restore_summary = StorageRestoreJobSummary::new(
        RestoreJobId::new(1).unwrap(),
        StorageRestoreJobStatus::Validated,
        StorageRestoreInitiator::try_new(None, "local", "administrator").unwrap(),
        StorageRestoreArtifactSummary::try_new(0, "0".repeat(64)).unwrap(),
        None,
        StorageRestoreTimestamps::try_new(now, None, None, now, now).unwrap(),
    );
    let service_account = StorageServiceAccount::new(
        ServiceAccountId::new(1).unwrap(),
        "description",
        GroupId::new(1).unwrap(),
        Some(PrincipalId::new(1).unwrap()),
        None,
        now,
        now,
    );
    let user = StorageUser::new(UserId::new(1).unwrap(), None, None, None, now, now, None);
    let _ = AuthenticatedToken::builder;
    let _ = AuthenticationIdentity::new;
    let _ = AuthenticationTokenScope::new;
    let _ = AuthorizationClassResource::new;
    let _ = AuthorizationCollection::new(
        collection_id,
        "collection",
        "description",
        now,
        now,
        None,
        ResourceRevision::INITIAL,
    );
    let _ = AuthorizationEffectiveGroupGrant::new;
    let _ = AuthorizationGrant::new(
        AuthorizationGrantId::new(1).unwrap(),
        collection_id,
        GroupId::new(1).unwrap(),
        Vec::<AuthorizationPermission>::new(),
        now,
        now,
    );
    let _ = AuthorizationGroup::new;
    let _ = AuthorizationGroupGrant::new;
    let _ = AuthorizationObjectResource::new(
        ObjectId::new(1).unwrap(),
        collection_id,
        ClassId::new(1).unwrap(),
        "object",
    );
    let _ = AuthorizationPermissionSet::new;
    let _ = AuthorizationPolicySnapshotRow::new;
    let _ = AuthorizationPrincipal::new(PrincipalId::new(1).unwrap(), Vec::<GroupId>::new());
    let _ = ComputedObjectPage::try_new;
    let _ = EventDeliveryBatch::new;
    let _ = EventDeliveryClaim::try_new;
    let _ = EventDeliverySink::try_new(
        EventSinkId::new(1).unwrap(),
        "sink",
        "webhook",
        serde_json::json!({}),
        None,
    )
    .unwrap();
    let _ = EventDeliverySubscription::try_new(
        EventSubscriptionId::new(1).unwrap(),
        "subscription",
        serde_json::json!({}),
    )
    .unwrap();
    let _ = EventDeliveryWorkItem::new;
    let _ = EventDeliveryHealthSnapshot::new;
    let _ = EventMetricsSnapshot::new;
    let _ = EventRetentionBatch::new;
    let _ = EventRetentionSummary::new;
    let _ = CollectionHistoryRecord::new;
    let _ = ClassHistoryRecord::new;
    let _ = ExportTemplateHistoryRecord::new;
    let _ = HistoryMetadata::try_new;
    let _ = HistoryPrincipalName::new(PrincipalId::new(1).unwrap(), "principal");
    let _ = InventoryGaugeSnapshot::new;
    let _ = MaintenanceState::Normal;
    let _ = MutationOutcome::<()>::unchanged;
    let audit = AuditReceipt::new(
        EventSequence::new(1).unwrap(),
        EventId::from(uuid::Uuid::nil()),
        EntityType::Collection,
        Action::Created,
        None,
        Some(ResourceRevision::INITIAL),
    );
    let committed = MutationOutcome::committed((), audit.clone());
    assert!(committed.is_committed());
    assert_eq!(committed.audits().map(AuditReceipts::len), Some(1));
    let audits = AuditReceipts::new(audit.clone(), vec![audit]);
    let committed_with_audits = MutationOutcome::committed_with_audits((), audits);
    assert_eq!(
        committed_with_audits.audits().map(AuditReceipts::len),
        Some(2)
    );
    let _ = OperationalExportTemplateAuditEntry::new(
        ExportTemplateId::new(1).unwrap(),
        collection_id,
        "template",
        "{{ object }}",
        "application/json",
    );
    let _ = OperationalExportTemplateHealth::new;
    let _ = OperationalTaskQueueSnapshot::new;
    let _ = ReadinessSnapshot::new;
    let _ = RemoteTargetHistoryRecord::new;
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
    let _ = StorageBackupOutput::new(TaskId::new(1).unwrap(), Vec::new(), 0, "sha256", now, now);
    let _ = StorageBackupOutputSummary::new(TaskId::new(1).unwrap(), 0, "sha256", now);
    let _ = StorageBackupSnapshot::try_new;
    let _ = StorageClass::builder(metadata, "class", collection.clone(), "description").build();
    let _ = StorageClassComputationState::builder;
    let _ = StorageClassGraphRow::new;
    let _ = class_record.clone();
    let _ = StorageClassRelation::new;
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
    .build();
    let _ = StorageExportOutputSummary::new(
        TaskId::new(1).unwrap(),
        None,
        "application/json",
        0,
        false,
        now,
        StorageTaskDurations::default(),
    );
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
    let _ = StorageExternalPrincipalState::new("scope", "user", "subject", None, None);
    let _ = StorageGroupMember::new;
    let _ = StorageIdentityGroup::builder(
        metadata,
        "group",
        "description",
        IdentityScopeId::new(1).unwrap(),
        "local",
    )
    .build();
    let _ = StorageIdentityScope::new(
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
    let _ = StorageObjectsByClassCount::try_new;
    let _ = object.clone();
    let _ = ObjectHistoryRecord::new;
    let _ = StorageObjectAggregateCursor::try_new;
    let _ = StorageObjectAggregateMeasureValue::try_new;
    let _ = StorageObjectAggregatePage::try_new;
    let _ = StorageObjectAggregateRow::try_new;
    let _ = StorageObjectGraphRow::new;
    let _ = StorageObjectRelation::new;
    let _ = StoragePage::<()>::try_new;
    let _ = StoragePreparedClassRelation::new;
    let _ = StoragePreparedObjectRelation::new;
    let _ = StoragePrincipal::builder(
        metadata,
        PrincipalKind::Human,
        "principal",
        IdentityScopeId::new(1).unwrap(),
    )
    .build();
    let _ = StoragePrincipalGroup::new;
    let _ = StoragePrincipalSettings::new;
    let _ = StorageRelatedObjectForRootRow::new;
    let _ = StorageRelatedObjectIncludeRow::new;
    let _ = StorageRemoteTarget::new(
        metadata,
        collection_id,
        "remote",
        StorageRemoteTargetDefinition::new(
            "description",
            StorageRemoteTargetTransport::try_new(
                StorageRemoteHttpMethod::Post,
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
    let _ = StorageResolvedClass::new;
    let _ = StorageResolvedClassRelation::new;
    let _ = StorageResolvedObject::new;
    let _ = StorageResolvedObjectRelation::new;
    let _ = StorageRestoreCompletion::new;
    let _ = StorageRestoreCoordinatorSnapshot::new;
    let _ = StorageRestoreDrainState::new;
    let _ = StorageRestoreJob::new(restore_summary.clone(), Vec::new(), "capability-hash");
    let _ = StorageRestoreStatus::new(restore_summary, "capability-hash", serde_json::json!({}));
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
    let _ = StorageSyncedHuman::new;
    let _ = StorageTask::builder;
    let _ = StorageTaskAccess::new;
    let _ = StorageTaskClaim::try_new;
    let _ = StorageTaskResultCounts::try_new;
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
    let _ = TaskGaugeSnapshot::new;
    let _ = user.clone();
    let _ = StorageUserDetails::builder(
        UserId::new(1).unwrap(),
        now,
        now,
        IdentityScopeId::new(1).unwrap(),
        "user",
        ResourceRevision::INITIAL,
    )
    .build();
    let _ = StorageUserListItem::builder(user, "local", "local", "user", ResourceRevision::INITIAL)
        .build();

    // A public builder entrypoint is insufficient if its private fields cannot
    // be turned into the boundary value. Keep every terminal method nameable
    // from this downstream integration-test crate as well.
    let _ = AuthenticatedTokenBuilder::build;
    let _ = StorageClassBuilder::build;
    let _ = StorageClassComputationStateBuilder::try_build;
    let _ = StorageClassRecordBuilder::build;
    let _ = StorageEventDeliveryBuilder::try_build;
    let _ = StorageEventSinkBuilder::try_build;
    let _ = StorageEventSubscriptionBuilder::try_build;
    let _ = StorageExportOutputBuilder::build;
    let _ = StorageIdentityGroupBuilder::build;
    let _ = StorageImportTaskResultBuilder::build;
    let _ = StoragePrincipalBuilder::build;
    let _ = StorageTaskBuilder::build;
    let _ = StorageTaskEventBuilder::build;
    let _ = StorageTokenMetadataBuilder::build;
}
