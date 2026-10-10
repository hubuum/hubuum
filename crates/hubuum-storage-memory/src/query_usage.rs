use super::*;

fn check_scope(state: &MemoryState, scope: StorageQueryUsageScope) -> Result<(), StorageError> {
    if state
        .classes
        .get(&scope.class_id().id())
        .is_some_and(|class| class.collection_id() == scope.authorized_collection())
    {
        Ok(())
    } else {
        Err(StorageError::not_found(
            "Class was not found in the authorized collection",
        ))
    }
}

fn check_pattern(
    state: &MemoryState,
    scope: StorageQueryUsageScope,
    pattern: &StorageQueryUsagePattern,
    replacing: Option<ResourceId>,
) -> Result<(), StorageError> {
    let records = state
        .query_usage
        .values()
        .filter(|value| value.class_id() == scope.class_id());
    if records.clone().any(|value| {
        Some(value.metadata().id()) != replacing
            && value.pattern().path() == pattern.path()
            && value.pattern().value_type() == pattern.value_type()
    }) {
        return Err(StorageError::conflict(
            "A declaration for this path and value type already exists",
        ));
    }
    if replacing.is_none() && records.count() >= MAX_QUERY_USAGE_DECLARATIONS {
        return Err(StorageError::conflict(
            "Class query usage declaration limit reached",
        ));
    }
    Ok(())
}

fn current(
    state: &MemoryState,
    scope: StorageQueryUsageScope,
    id: ResourceId,
    expected: ResourceRevision,
) -> Result<StorageQueryUsageDeclaration, StorageError> {
    check_scope(state, scope)?;
    let value = state
        .query_usage
        .get(&id.id())
        .filter(|value| value.class_id() == scope.class_id())
        .ok_or_else(|| StorageError::not_found("Query usage declaration was not found"))?;
    if value.metadata().revision() != expected {
        return Err(StorageError::conflict(
            "Query usage declaration revision changed",
        ));
    }
    Ok(value.clone())
}

fn record_event(
    state: &mut MemoryState,
    scope: StorageQueryUsageScope,
    context: &EventContext,
    before: Option<&StorageQueryUsageDeclaration>,
    after: Option<&StorageQueryUsageDeclaration>,
) -> Result<StorageAuditReceipt, StorageError> {
    let value = after.or(before).expect("mutation has a declaration");
    let action = match (before, after) {
        (None, _) => Action::Created,
        (_, None) => Action::Deleted,
        _ => Action::Updated,
    };
    let document = AuditDocument::try_new(
        "Query usage declaration changed",
        before.map(StorageQueryUsageDeclaration::snapshot),
        after.map(StorageQueryUsageDeclaration::snapshot),
        serde_json::json!({"class_id": scope.class_id()}),
    )
    .map_err(|error| StorageError::backend_failure(error.to_string()))?;
    append_memory_event!(
        state,
        EntityType::QueryUsageDeclaration,
        value.metadata().id().id(),
        None,
        Some(scope.authorized_collection()),
        action,
        context,
        document,
        before.map(|value| value.metadata().revision()),
        after.map(|value| value.metadata().revision())
    )
}

pub(super) fn delete_class_query_usage(
    state: &mut MemoryState,
    class: &StorageClass,
    context: &EventContext,
) -> Result<(), StorageError> {
    let scope = StorageQueryUsageScope::new(class.id(), class.collection_id());
    let records = state
        .query_usage
        .values()
        .filter(|value| value.class_id() == class.id())
        .cloned()
        .collect::<Vec<_>>();
    for record in records {
        record_event(state, scope, context, Some(&record), None)?;
        state.query_usage.remove(&record.metadata().id().id());
    }
    Ok(())
}

#[async_trait]
impl QueryUsageStorage for MemoryStorage {
    async fn list_query_usage(
        &self,
        scope: StorageQueryUsageScope,
    ) -> Result<Vec<StorageQueryUsageDeclaration>, StorageError> {
        let state = self.state.read().await;
        check_scope(&state, scope)?;
        Ok(state
            .query_usage
            .values()
            .filter(|value| value.class_id() == scope.class_id())
            .cloned()
            .collect())
    }

    async fn create_query_usage(
        &self,
        request: StorageQueryUsageCreate,
    ) -> Result<StorageMutationOutcome<StorageQueryUsageDeclaration>, StorageError> {
        let mut state = self.state.write().await;
        check_scope(&state, request.scope())?;
        check_pattern(&state, request.scope(), request.pattern(), None)?;
        let id = ResourceId::new(state.next_query_usage_id)
            .map_err(|error| StorageError::internal(error.to_string()))?;
        let next_id = state
            .next_query_usage_id
            .checked_add(1)
            .ok_or_else(|| StorageError::internal("Declaration identifiers exhausted"))?;
        let now = Utc::now();
        let metadata = StorageRecordMetadata::try_new(id, now, now, ResourceRevision::INITIAL)
            .map_err(invalid_contract_value)?;
        let record = StorageQueryUsageDeclaration::new(
            metadata,
            request.scope().class_id(),
            request.pattern().clone(),
            request.context().actor_user_id(),
            request.context().actor_user_id(),
        );
        let receipt = record_event(
            &mut state,
            request.scope(),
            request.context(),
            None,
            Some(&record),
        )?;
        state.next_query_usage_id = next_id;
        state.query_usage.insert(id.id(), record.clone());
        Ok(StorageMutationOutcome::committed(record, receipt))
    }

    async fn replace_query_usage(
        &self,
        request: StorageQueryUsageReplace,
    ) -> Result<StorageMutationOutcome<StorageQueryUsageDeclaration>, StorageError> {
        let mut state = self.state.write().await;
        let before = current(
            &state,
            request.scope(),
            request.id(),
            request.expected_revision(),
        )?;
        check_pattern(
            &state,
            request.scope(),
            request.pattern(),
            Some(request.id()),
        )?;
        if before.pattern() == request.pattern() {
            return Ok(StorageMutationOutcome::unchanged(before));
        }
        let metadata = StorageRecordMetadata::try_new(
            request.id(),
            before.metadata().created_at(),
            Utc::now().max(before.metadata().updated_at()),
            before
                .metadata()
                .revision()
                .checked_advance()
                .map_err(|error| StorageError::internal(error.to_string()))?,
        )
        .map_err(invalid_contract_value)?;
        let after = StorageQueryUsageDeclaration::new(
            metadata,
            before.class_id(),
            request.pattern().clone(),
            before.created_by(),
            request.context().actor_user_id(),
        );
        let receipt = record_event(
            &mut state,
            request.scope(),
            request.context(),
            Some(&before),
            Some(&after),
        )?;
        state.query_usage.insert(request.id().id(), after.clone());
        Ok(StorageMutationOutcome::committed(after, receipt))
    }

    async fn delete_query_usage(
        &self,
        request: StorageQueryUsageDelete,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        let mut state = self.state.write().await;
        let before = current(
            &state,
            request.scope(),
            request.id(),
            request.expected_revision(),
        )?;
        let receipt = record_event(
            &mut state,
            request.scope(),
            request.context(),
            Some(&before),
            None,
        )?;
        state.query_usage.remove(&request.id().id());
        Ok(StorageMutationOutcome::committed((), receipt))
    }
}
