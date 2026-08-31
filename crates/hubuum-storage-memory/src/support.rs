use super::*;

pub(super) fn ordered_ids<T: Ord>(first: T, second: T) -> (T, T) {
    if first < second {
        (first, second)
    } else {
        (second, first)
    }
}

pub(super) fn invalid_contract_value(error: StorageValidationError) -> StorageError {
    StorageError::internal(error.to_string())
}

pub(super) fn page<T>(
    mut rows: Vec<T>,
    options: &QueryOptions,
) -> Result<StoragePage<T>, StorageError> {
    let total = options
        .include_total()
        .then(|| i64::try_from(rows.len()).unwrap_or(i64::MAX));
    if let Some(limit) = options.limit() {
        rows.truncate(limit);
    }
    StoragePage::try_new(rows, total).map_err(invalid_contract_value)
}

pub(super) fn string_filter_matches(
    actual: &str,
    operator: &SearchOperator,
    expected: &str,
) -> bool {
    let (operator, negated) = operator.op_and_neg();
    let actual_lower = actual.to_lowercase();
    let expected_lower = expected.to_lowercase();
    let matched = match operator {
        Operator::Equals => actual == expected,
        Operator::IEquals => actual_lower == expected_lower,
        Operator::Contains => actual.contains(expected),
        Operator::IContains => actual_lower.contains(&expected_lower),
        Operator::StartsWith => actual.starts_with(expected),
        Operator::IStartsWith => actual_lower.starts_with(&expected_lower),
        Operator::EndsWith => actual.ends_with(expected),
        Operator::IEndsWith => actual_lower.ends_with(&expected_lower),
        Operator::In => expected.split(',').any(|value| actual == value.trim()),
        _ => true,
    };
    matched != negated
}

pub(super) fn resource_filters_match(
    options: &QueryOptions,
    id: i32,
    name: &str,
    description: &str,
) -> bool {
    options.filters().as_slice().iter().all(|filter| {
        let actual = match filter.field {
            FilterField::Id => id.to_string(),
            FilterField::Name => name.to_string(),
            FilterField::Description => description.to_string(),
            _ => return true,
        };
        string_filter_matches(&actual, &filter.operator, &filter.value)
    })
}

pub(super) fn authorization_collection(
    collection: &StorageCollection,
) -> Result<StorageAuthorizationCollection, StorageError> {
    StorageAuthorizationCollection::try_new(
        collection.id(),
        collection.name(),
        collection.description(),
        collection.created_at(),
        collection.updated_at(),
        collection.parent_collection_id(),
        collection.revision(),
    )
    .map_err(invalid_contract_value)
}

pub(super) fn authorization_group(
    group: &StorageIdentityGroup,
) -> Result<StorageAuthorizationGroup, StorageError> {
    let identity = StorageAuthorizationGroupIdentity::new(
        group.id(),
        group.name(),
        group.identity_scope_id(),
        group.managed_by(),
        group.external_key().map(ToOwned::to_owned),
    );
    let profile = StorageAuthorizationGroupProfile::try_new(
        group.description(),
        group.created_at(),
        group.updated_at(),
        group.revision(),
    )
    .map_err(invalid_contract_value)?;
    let sync = StorageAuthorizationGroupSyncState::try_new(
        group.last_sync_attempted_at(),
        group.last_sync_success_at(),
    )
    .map_err(invalid_contract_value)?;
    Ok(StorageAuthorizationGroup::new(identity, profile, sync))
}

pub(super) fn principal_group_ids(state: &MemoryState, principal_id: PrincipalId) -> Vec<GroupId> {
    state
        .memberships
        .keys()
        .filter(|(candidate_principal_id, _)| *candidate_principal_id == principal_id.id())
        .map(|(_, group_id)| GroupId::new(*group_id).expect("stored group ids are positive"))
        .collect()
}

pub(super) fn permissions_include(
    available: &[StorageAuthorizationPermission],
    required: &[StorageAuthorizationPermission],
) -> bool {
    required
        .iter()
        .all(|permission| available.contains(permission))
}

pub(super) fn principal_has_collection_permissions(
    state: &MemoryState,
    principal_id: PrincipalId,
    collection_id: CollectionId,
    permissions: &[StorageAuthorizationPermission],
) -> bool {
    let group_ids = principal_group_ids(state, principal_id);
    group_ids.iter().any(|group_id| {
        state
            .authorization_grants
            .get(&(collection_id.id(), group_id.id()))
            .is_some_and(|grant| permissions_include(grant.permissions(), permissions))
    })
}

pub(super) fn authorization_group_grant(
    state: &MemoryState,
    grant: &StorageAuthorizationGrant,
) -> Result<StorageAuthorizationGroupGrant, StorageError> {
    let group = state
        .groups
        .get(&grant.group_id().id())
        .ok_or_else(|| StorageError::internal("authorization grant group is missing"))?;
    StorageAuthorizationGroupGrant::try_new(authorization_group(group)?, grant.clone())
        .map_err(invalid_contract_value)
}

pub(super) fn authorization_policy_row(
    state: &MemoryState,
    grant: &StorageAuthorizationGrant,
) -> Result<StorageAuthorizationPolicySnapshotRow, StorageError> {
    let group = state
        .groups
        .get(&grant.group_id().id())
        .ok_or_else(|| StorageError::internal("authorization grant group is missing"))?;
    let collection = state
        .collections
        .get(&grant.collection_id().id())
        .ok_or_else(|| StorageError::internal("authorization grant collection is missing"))?;
    StorageAuthorizationPolicySnapshotRow::try_new(
        grant.clone(),
        authorization_group(group)?,
        authorization_collection(collection)?,
    )
    .map_err(invalid_contract_value)
}

pub(super) fn authorization_effective_group_grant(
    state: &MemoryState,
    grant: &StorageAuthorizationGrant,
) -> Result<StorageAuthorizationEffectiveGroupGrant, StorageError> {
    let collection = state
        .collections
        .get(&grant.collection_id().id())
        .ok_or_else(|| StorageError::internal("authorization grant collection is missing"))?;
    let group = state
        .groups
        .get(&grant.group_id().id())
        .ok_or_else(|| StorageError::internal("authorization grant group is missing"))?;
    let collection = authorization_collection(collection)?;
    Ok(StorageAuthorizationEffectiveGroupGrant::new(
        collection.clone(),
        collection,
        0,
        false,
        authorization_group(group)?,
        grant.clone(),
    ))
}

pub(super) fn rebuild_event_delivery(
    delivery: &StorageEventDelivery,
    status: EventDeliveryStatus,
    attempts: i32,
    next_attempt_at: DateTime<Utc>,
    last_error: Option<String>,
    locked_until: Option<DateTime<Utc>>,
) -> Result<StorageEventDelivery, StorageError> {
    StorageEventDelivery::builder(
        delivery.id(),
        delivery.event_id(),
        delivery.subscription_id(),
        status,
        next_attempt_at,
        delivery.created_at(),
        Utc::now(),
    )
    .attempts(attempts)
    .last_error(last_error)
    .locked_until(locked_until)
    .try_build()
    .map_err(invalid_contract_value)
}

pub(super) fn event_status_counts(
    deliveries: impl IntoIterator<Item = EventDeliveryStatus>,
) -> Result<StorageEventDeliveryStatusSnapshot, StorageError> {
    let mut total = 0_i64;
    let mut pending = 0_i64;
    let mut in_flight = 0_i64;
    let mut succeeded = 0_i64;
    let mut failed = 0_i64;
    let mut dead = 0_i64;
    for status in deliveries {
        total += 1;
        match status {
            EventDeliveryStatus::Pending => pending += 1,
            EventDeliveryStatus::InFlight => in_flight += 1,
            EventDeliveryStatus::Succeeded => succeeded += 1,
            EventDeliveryStatus::Failed => failed += 1,
            EventDeliveryStatus::Dead => dead += 1,
        }
    }
    StorageEventDeliveryStatusSnapshot::try_new(
        total, pending, in_flight, succeeded, failed, dead, failed,
    )
    .map_err(invalid_contract_value)
}

pub(super) fn history_scope_allows(
    scope: &StorageHistoryCollectionScope,
    collection_id: CollectionId,
) -> bool {
    match scope {
        StorageHistoryCollectionScope::All => true,
        StorageHistoryCollectionScope::Visible(ids) => ids.contains(&collection_id),
    }
}

pub(super) fn history_valid_to(
    state: &MemoryState,
    entry: &MemoryHistoryEntry,
) -> Option<DateTime<Utc>> {
    let variant = std::mem::discriminant(&entry.value);
    state
        .history
        .iter()
        .filter(|candidate| {
            candidate.id != entry.id
                && candidate.value.entity_id() == entry.value.entity_id()
                && std::mem::discriminant(&candidate.value) == variant
                && (candidate.valid_from > entry.valid_from
                    || (candidate.valid_from == entry.valid_from
                        && candidate.id.id() > entry.id.id()))
        })
        .min_by_key(|candidate| (candidate.valid_from, candidate.id.id()))
        .map(|candidate| candidate.valid_from)
}

pub(super) fn transition_restore_record(
    record: &MemoryRestoreRecord,
    status: StorageRestoreJobStatus,
    error: Option<String>,
    confirmed_at: Option<DateTime<Utc>>,
    finished_at: Option<DateTime<Utc>>,
    erase_document: bool,
) -> Result<MemoryRestoreRecord, StorageError> {
    let (summary, mut document, capability_hash) = record.job.clone().into_parts();
    let (id, _, initiator, artifact, _, timestamps) = summary.into_parts();
    let timestamp_parts = timestamps.into_parts();
    let timestamps = StorageRestoreTimestamps::try_new(
        timestamp_parts.expires_at(),
        confirmed_at,
        finished_at,
        timestamp_parts.created_at(),
        Utc::now(),
    )
    .map_err(invalid_contract_value)?;
    let summary =
        StorageRestoreJobSummary::try_new(id, status, initiator, artifact, error, timestamps)
            .map_err(invalid_contract_value)?;
    if erase_document {
        document.clear();
    }
    let job = StorageRestoreJob::try_new(summary, document, capability_hash)
        .map_err(invalid_contract_value)?;
    Ok(MemoryRestoreRecord {
        job,
        validation_summary: record.validation_summary.clone(),
    })
}

pub(super) fn class_with_collection(
    state: &MemoryState,
    class: &StorageClass,
) -> Result<StorageClassWithCollection, StorageError> {
    let collection = state
        .collections
        .get(&class.collection_id().id())
        .cloned()
        .ok_or_else(|| StorageError::internal("class collection is missing"))?;
    let metadata = StorageRecordMetadata::try_new(
        ResourceId::new(class.id().id()).expect("class id is positive"),
        class.created_at(),
        class.updated_at(),
        class.revision(),
    )
    .map_err(invalid_contract_value)?;
    Ok(
        StorageClassWithCollection::builder(
            metadata,
            class.name(),
            collection,
            class.description(),
        )
        .json_schema(class.json_schema().cloned())
        .validate_schema(class.validates_schema())
        .build(),
    )
}

pub(super) fn search_rank(
    name: &str,
    description: &str,
    extended: Option<&str>,
    term: &str,
) -> Option<i32> {
    let name = name.to_lowercase();
    let description = description.to_lowercase();
    let extended = extended.map(str::to_lowercase);
    let term = term.to_lowercase();
    if name == term {
        Some(3)
    } else if name.starts_with(&term) {
        Some(2)
    } else if name.contains(&term)
        || description.contains(&term)
        || extended.as_ref().is_some_and(|value| value.contains(&term))
    {
        Some(1)
    } else {
        None
    }
}

pub(super) fn graph_class(class: &StorageClass) -> Result<StorageGraphClass, StorageError> {
    let metadata = StorageRecordMetadata::try_new(
        ResourceId::new(class.id().id()).expect("class id is positive"),
        class.created_at(),
        class.updated_at(),
        class.revision(),
    )
    .map_err(invalid_contract_value)?;
    Ok(StorageGraphClass::new(
        StorageGraphResource::new(
            metadata,
            class.name().to_string(),
            class.collection_id(),
            class.description().to_string(),
        ),
        class.json_schema().cloned(),
        class.validates_schema(),
    ))
}

pub(super) fn graph_object(object: &StorageObject) -> Result<StorageGraphObject, StorageError> {
    let metadata = StorageRecordMetadata::try_new(
        ResourceId::new(object.id().id()).expect("object id is positive"),
        object.created_at(),
        object.updated_at(),
        object.revision(),
    )
    .map_err(invalid_contract_value)?;
    Ok(StorageGraphObject::new(
        StorageGraphResource::new(
            metadata,
            object.name().to_string(),
            object.collection_id(),
            object.description().to_string(),
        ),
        object.class_id(),
        object.data().clone(),
    ))
}

pub(super) fn ready_computation_state(
    class_id: ClassId,
    revision: i64,
    created_at: DateTime<Utc>,
) -> Result<StorageClassComputationState, StorageError> {
    StorageClassComputationState::builder(
        class_id,
        StorageComputationRevision::try_new(revision).map_err(invalid_contract_value)?,
        StorageComputationRebuildStatus::Ready,
        created_at,
        Utc::now(),
    )
    .try_build()
    .map_err(invalid_contract_value)
}

pub(super) fn updated_computed_field(
    current: &StorageComputedFieldDefinition,
    patch: &StorageComputedFieldDefinitionPatch,
    actor_id: PrincipalId,
) -> Result<StorageComputedFieldDefinition, StorageError> {
    let metadata = current.metadata();
    let metadata = StorageRecordMetadata::try_new(
        metadata.id(),
        metadata.created_at(),
        Utc::now(),
        metadata
            .revision()
            .checked_advance()
            .map_err(|error| StorageError::internal(error.to_string()))?,
    )
    .map_err(invalid_contract_value)?;
    let input = StorageComputedFieldDefinitionInput::new(
        patch.key().unwrap_or(current.key()).to_string(),
        patch.label().unwrap_or(current.label()).to_string(),
        patch
            .operation()
            .cloned()
            .unwrap_or_else(|| current.operation().clone()),
        patch
            .result_type()
            .unwrap_or(current.result_type())
            .to_string(),
    )
    .with_description(
        patch
            .description()
            .unwrap_or(current.description())
            .to_string(),
    )
    .with_enabled(patch.enabled().unwrap_or(current.enabled()));
    Ok(StorageComputedFieldDefinition::new(
        metadata,
        current.class_id(),
        current.visibility(),
        StorageComputedFieldDefinitionContent::new(input, current.semantics_version()),
        StorageComputedFieldProvenance::new(current.created_by(), Some(actor_id)),
    ))
}

pub(super) fn evaluate_computed_definition(
    definition: &StorageComputedFieldDefinition,
    object: &StorageObject,
) -> serde_json::Value {
    definition
        .operation()
        .get("paths")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .find_map(|path| object.data().pointer(path).cloned())
        .unwrap_or(serde_json::Value::Null)
}

pub(super) fn computed_scope(
    state: &MemoryState,
    object: &StorageObject,
    visibility: StorageComputedFieldVisibility,
) -> StorageComputedScope {
    let values = state
        .computed_fields
        .values()
        .filter(|definition| {
            definition.class_id() == object.class_id()
                && definition.visibility() == visibility
                && definition.enabled()
        })
        .map(|definition| {
            (
                definition.key().to_string(),
                evaluate_computed_definition(definition, object),
            )
        })
        .collect();
    StorageComputedScope::new(values, BTreeMap::new())
}

pub(super) fn computed_object(
    state: &MemoryState,
    object: StorageObject,
    personal_owner_id: Option<PrincipalId>,
) -> Result<StorageComputedObject, StorageError> {
    let revision = state
        .computation_states
        .get(&object.class_id().id())
        .map_or(0, |value| value.evaluation_revision().get());
    let shared = StorageSharedComputedScope::new(
        StorageComputationRevision::try_new(revision).map_err(invalid_contract_value)?,
        false,
        computed_scope(state, &object, StorageComputedFieldVisibility::Shared),
    );
    let personal = personal_owner_id.map(|owner_id| {
        computed_scope(
            state,
            &object,
            StorageComputedFieldVisibility::Personal { owner_id },
        )
    });
    Ok(StorageComputedObject::new(object, shared, personal))
}

pub(super) fn export_output_summary(
    output: &StorageExportOutput,
) -> Result<StorageExportOutputSummary, StorageError> {
    StorageExportOutputSummary::try_new(
        output.task_id(),
        output.template_name().map(ToOwned::to_owned),
        output.content_type(),
        output.warning_count(),
        output.truncated(),
        output.output_expires_at(),
        output.durations(),
    )
    .map_err(invalid_contract_value)
}

pub(super) fn backup_output_summary(
    output: &StorageBackupOutput,
) -> Result<StorageBackupOutputSummary, StorageError> {
    StorageBackupOutputSummary::try_new(
        output.task_id(),
        output.byte_size(),
        output.sha256(),
        output.output_expires_at(),
    )
    .map_err(invalid_contract_value)
}

pub(super) fn invalid_task_lease() -> StorageError {
    StorageError::conflict("Task lease is no longer valid")
}

pub(super) fn advanced_principal(
    current: &StoragePrincipal,
    name: impl Into<String>,
    settings: serde_json::Value,
    updated_at: DateTime<Utc>,
) -> Result<StoragePrincipal, StorageError> {
    let revision = current
        .revision()
        .checked_advance()
        .map_err(|error| StorageError::internal(error.to_string()))?;
    let metadata = StorageRecordMetadata::try_new(
        ResourceId::new(current.id().id()).expect("principal id is positive"),
        current.created_at(),
        updated_at,
        revision,
    )
    .map_err(invalid_contract_value)?;
    StoragePrincipal::builder(metadata, current.kind(), name, current.identity_scope_id())
        .provider_managed(current.provider_managed())
        .settings(settings)
        .external_subject(current.external_subject().map(ToOwned::to_owned))
        .last_sync_attempted_at(current.last_sync_attempted_at())
        .last_sync_success_at(current.last_sync_success_at())
        .try_build()
        .map_err(invalid_contract_value)
}

pub(super) fn token_state_matches(
    metadata: &StorageTokenMetadata,
    state: StorageTokenListState,
) -> bool {
    match state {
        StorageTokenListState::Active => metadata.is_active(),
        StorageTokenListState::Expired => metadata.is_expired(),
        StorageTokenListState::Revoked => metadata.revoked_at().is_some(),
        StorageTokenListState::All => true,
    }
}

pub(super) fn empty_event_fanout_snapshot() -> Result<StorageEventFanoutSnapshot, StorageError> {
    StorageEventFanoutSnapshot::try_new(0, 0, 0, None).map_err(invalid_contract_value)
}

pub(super) fn empty_event_queue_snapshot() -> Result<StorageEventQueueSnapshot, StorageError> {
    StorageEventQueueSnapshot::try_new(
        StorageEventDeliveryStatusSnapshot::try_new(0, 0, 0, 0, 0, 0, 0)
            .map_err(invalid_contract_value)?,
        0,
        None,
    )
    .map_err(invalid_contract_value)
}
