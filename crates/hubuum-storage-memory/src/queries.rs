use super::*;

#[async_trait]
impl CatalogStorage for MemoryStorage {
    async fn list_collections(
        &self,
        query: StorageCatalogListQuery,
    ) -> Result<StoragePage<StorageCollection>, StorageError> {
        let (options, visibility) = query.into_parts();
        let state = self.state.read().await;
        let rows = state
            .collections
            .values()
            .filter(|collection| {
                visibility
                    .resources()
                    .is_none_or(|scope| scope.collection_ids().contains(&collection.id()))
                    && resource_filters_match(
                        &options,
                        collection.id().id(),
                        collection.name(),
                        collection.description(),
                    )
            })
            .cloned()
            .collect();
        page(rows, &options)
    }

    async fn list_classes(
        &self,
        query: StorageCatalogListQuery,
    ) -> Result<StoragePage<StorageClassWithCollection>, StorageError> {
        let (options, visibility) = query.into_parts();
        let state = self.state.read().await;
        let rows = state
            .classes
            .values()
            .filter(|class| {
                visibility.resources().is_none_or(|scope| {
                    scope.class_ids().contains(&class.id())
                        || scope.collection_ids().contains(&class.collection_id())
                }) && resource_filters_match(
                    &options,
                    class.id().id(),
                    class.name(),
                    class.description(),
                )
            })
            .map(|class| class_with_collection(&state, class))
            .collect::<Result<Vec<_>, StorageError>>()?;
        page(rows, &options)
    }

    async fn list_objects(
        &self,
        query: StorageCatalogListQuery,
    ) -> Result<StoragePage<StorageObject>, StorageError> {
        let (options, visibility) = query.into_parts();
        let state = self.state.read().await;
        let rows = state
            .objects
            .values()
            .filter(|object| {
                visibility.resources().is_none_or(|scope| {
                    scope.object_ids().contains(&object.id())
                        || scope.class_ids().contains(&object.class_id())
                        || scope.collection_ids().contains(&object.collection_id())
                }) && resource_filters_match(
                    &options,
                    object.id().id(),
                    object.name(),
                    object.description(),
                )
            })
            .cloned()
            .collect();
        page(rows, &options)
    }
}

#[async_trait]
impl ComputedFieldStorage for MemoryStorage {
    async fn get_computed_field_state(
        &self,
        class_id: ClassId,
    ) -> Result<StorageClassComputationState, StorageError> {
        let state = self.state.read().await;
        if let Some(computation_state) = state.computation_states.get(&class_id.id()) {
            return Ok(computation_state.clone());
        }
        let class = state.classes.get(&class_id.id()).ok_or_else(|| {
            StorageError::not_found(format!("Class {} was not found", class_id.id()))
        })?;
        ready_computation_state(class_id, 0, class.created_at())
    }

    async fn list_shared_computed_fields(
        &self,
        class_id: ClassId,
    ) -> Result<Vec<StorageComputedFieldDefinition>, StorageError> {
        Ok(self
            .state
            .read()
            .await
            .computed_fields
            .values()
            .filter(|definition| {
                definition.class_id() == class_id
                    && definition.visibility() == StorageComputedFieldVisibility::Shared
            })
            .cloned()
            .collect())
    }

    async fn list_personal_computed_fields(
        &self,
        query: StoragePersonalComputedFieldListQuery,
    ) -> Result<StoragePage<StorageComputedFieldDefinition>, StorageError> {
        let (owner_id, class_id, options) = query.into_parts();
        let rows = self
            .state
            .read()
            .await
            .computed_fields
            .values()
            .filter(|definition| {
                definition.visibility() == StorageComputedFieldVisibility::Personal { owner_id }
                    && class_id.is_none_or(|id| definition.class_id() == id)
            })
            .cloned()
            .collect();
        page(rows, &options)
    }

    async fn get_computed_field(
        &self,
        definition_id: ComputedFieldDefinitionId,
    ) -> Result<StorageComputedFieldDefinition, StorageError> {
        self.state
            .read()
            .await
            .computed_fields
            .get(&definition_id.id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!(
                    "Computed field definition {} was not found",
                    definition_id.id()
                ))
            })
    }

    async fn create_shared_computed_field(
        &self,
        request: StorageSharedComputedFieldCreate,
    ) -> Result<StorageMutationOutcome<StorageComputedFieldMutation>, StorageError> {
        let (class_id, collection_id, actor_id, input, context) = request.into_parts();
        let mut state = self.state.write().await;
        let class = state.classes.get(&class_id.id()).cloned().ok_or_else(|| {
            StorageError::not_found(format!("Class {} was not found", class_id.id()))
        })?;
        if class.collection_id() != collection_id {
            return Err(StorageError::not_found(
                "Class was not found in the authorized collection",
            ));
        }
        if state.computed_fields.values().any(|definition| {
            definition.class_id() == class_id
                && definition.visibility() == StorageComputedFieldVisibility::Shared
                && definition.key() == input.key()
        }) {
            return Err(StorageError::conflict(format!(
                "Shared computed field '{}' already exists",
                input.key()
            )));
        }
        let id = state.next_computed_field_id;
        state.next_computed_field_id += 1;
        let now = Utc::now();
        let metadata = StorageRecordMetadata::try_new(
            ResourceId::new(id).expect("memory computed field id is positive"),
            now,
            now,
            ResourceRevision::INITIAL,
        )
        .map_err(invalid_contract_value)?;
        let definition = StorageComputedFieldDefinition::new(
            metadata,
            class_id,
            StorageComputedFieldVisibility::Shared,
            StorageComputedFieldDefinitionContent::new(input),
            StorageComputedFieldProvenance::new(Some(actor_id), Some(actor_id)),
        );
        let previous_revision = state
            .computation_states
            .get(&class_id.id())
            .map_or(0, |value| value.evaluation_revision().get());
        let computation_state = ready_computation_state(class_id, previous_revision + 1, now)?;
        state.computed_fields.insert(id, definition.clone());
        state
            .computation_states
            .insert(class_id.id(), computation_state.clone());
        let receipt = state.append_simple_event(
            EntityType::ComputedFieldDefinition,
            id,
            Some(definition.key()),
            Action::Created,
            &context,
            format!("Computed field '{}' created", definition.key()),
        )?;
        Ok(StorageMutationOutcome::committed(
            StorageComputedFieldMutation::new(definition, computation_state),
            receipt,
        ))
    }

    async fn update_shared_computed_field(
        &self,
        request: StorageSharedComputedFieldUpdate,
    ) -> Result<StorageMutationOutcome<StorageComputedFieldMutation>, StorageError> {
        let (class_id, collection_id, definition_id, actor_id, patch, context) =
            request.into_parts();
        let mut state = self.state.write().await;
        let class = state.classes.get(&class_id.id()).ok_or_else(|| {
            StorageError::not_found(format!("Class {} was not found", class_id.id()))
        })?;
        if class.collection_id() != collection_id {
            return Err(StorageError::not_found(
                "Class was not found in the authorized collection",
            ));
        }
        let current = state
            .computed_fields
            .get(&definition_id.id())
            .cloned()
            .ok_or_else(|| StorageError::not_found("Computed field definition was not found"))?;
        if current.class_id() != class_id
            || current.visibility() != StorageComputedFieldVisibility::Shared
        {
            return Err(StorageError::not_found(
                "Shared computed field definition was not found",
            ));
        }
        let definition = updated_computed_field(&current, &patch, actor_id)?;
        let previous = state
            .computation_states
            .get(&class_id.id())
            .cloned()
            .unwrap_or(ready_computation_state(class_id, 0, class.created_at())?);
        let computation_state = ready_computation_state(
            class_id,
            previous.evaluation_revision().get() + 1,
            previous.created_at(),
        )?;
        state
            .computed_fields
            .insert(definition_id.id(), definition.clone());
        state
            .computation_states
            .insert(class_id.id(), computation_state.clone());
        let receipt = state.append_simple_event(
            EntityType::ComputedFieldDefinition,
            definition_id.id(),
            Some(definition.key()),
            Action::Updated,
            &context,
            format!("Computed field '{}' updated", definition.key()),
        )?;
        Ok(StorageMutationOutcome::committed(
            StorageComputedFieldMutation::new(definition, computation_state),
            receipt,
        ))
    }

    async fn delete_shared_computed_field(
        &self,
        request: StorageSharedComputedFieldDelete,
    ) -> Result<StorageMutationOutcome<StorageClassComputationState>, StorageError> {
        let (class_id, collection_id, definition_id, _, context) = request.into_parts();
        let mut state = self.state.write().await;
        let class = state.classes.get(&class_id.id()).cloned().ok_or_else(|| {
            StorageError::not_found(format!("Class {} was not found", class_id.id()))
        })?;
        if class.collection_id() != collection_id {
            return Err(StorageError::not_found(
                "Class was not found in the authorized collection",
            ));
        }
        let definition = state
            .computed_fields
            .remove(&definition_id.id())
            .ok_or_else(|| StorageError::not_found("Computed field definition was not found"))?;
        if definition.class_id() != class_id
            || definition.visibility() != StorageComputedFieldVisibility::Shared
        {
            state.computed_fields.insert(definition_id.id(), definition);
            return Err(StorageError::not_found(
                "Shared computed field definition was not found",
            ));
        }
        let previous = state
            .computation_states
            .get(&class_id.id())
            .cloned()
            .unwrap_or(ready_computation_state(class_id, 0, class.created_at())?);
        let computation_state = ready_computation_state(
            class_id,
            previous.evaluation_revision().get() + 1,
            previous.created_at(),
        )?;
        state
            .computation_states
            .insert(class_id.id(), computation_state.clone());
        let receipt = state.append_simple_event(
            EntityType::ComputedFieldDefinition,
            definition_id.id(),
            Some(definition.key()),
            Action::Deleted,
            &context,
            format!("Computed field '{}' deleted", definition.key()),
        )?;
        Ok(StorageMutationOutcome::committed(
            computation_state,
            receipt,
        ))
    }

    async fn create_personal_computed_field(
        &self,
        request: StoragePersonalComputedFieldCreate,
    ) -> Result<StorageMutationOutcome<StorageComputedFieldDefinition>, StorageError> {
        let (class_id, owner_id, input, context) = request.into_parts();
        let mut state = self.state.write().await;
        if !state.classes.contains_key(&class_id.id()) {
            return Err(StorageError::not_found(format!(
                "Class {} was not found",
                class_id.id()
            )));
        }
        if !state.principals.contains_key(&owner_id.id()) {
            return Err(StorageError::not_found(format!(
                "Principal {} was not found",
                owner_id.id()
            )));
        }
        if state.computed_fields.values().any(|definition| {
            definition.class_id() == class_id
                && definition.visibility() == StorageComputedFieldVisibility::Personal { owner_id }
                && definition.key() == input.key()
        }) {
            return Err(StorageError::conflict(format!(
                "Personal computed field '{}' already exists",
                input.key()
            )));
        }
        let id = state.next_computed_field_id;
        state.next_computed_field_id += 1;
        let now = Utc::now();
        let metadata = StorageRecordMetadata::try_new(
            ResourceId::new(id).expect("memory computed field id is positive"),
            now,
            now,
            ResourceRevision::INITIAL,
        )
        .map_err(invalid_contract_value)?;
        let definition = StorageComputedFieldDefinition::new(
            metadata,
            class_id,
            StorageComputedFieldVisibility::Personal { owner_id },
            StorageComputedFieldDefinitionContent::new(input),
            StorageComputedFieldProvenance::new(Some(owner_id), Some(owner_id)),
        );
        state.computed_fields.insert(id, definition.clone());
        let receipt = state.append_simple_event(
            EntityType::ComputedFieldDefinition,
            id,
            Some(definition.key()),
            Action::Created,
            &context,
            format!("Computed field '{}' created", definition.key()),
        )?;
        Ok(StorageMutationOutcome::committed(definition, receipt))
    }

    async fn update_personal_computed_field(
        &self,
        request: StoragePersonalComputedFieldUpdate,
    ) -> Result<StorageMutationOutcome<StorageComputedFieldDefinition>, StorageError> {
        let (owner_id, definition_id, patch, context) = request.into_parts();
        let mut state = self.state.write().await;
        let current = state
            .computed_fields
            .get(&definition_id.id())
            .cloned()
            .ok_or_else(|| StorageError::not_found("Computed field definition was not found"))?;
        if current.visibility() != (StorageComputedFieldVisibility::Personal { owner_id }) {
            return Err(StorageError::not_found(
                "Personal computed field definition was not found",
            ));
        }
        let definition = updated_computed_field(&current, &patch, owner_id)?;
        state
            .computed_fields
            .insert(definition_id.id(), definition.clone());
        let receipt = state.append_simple_event(
            EntityType::ComputedFieldDefinition,
            definition_id.id(),
            Some(definition.key()),
            Action::Updated,
            &context,
            format!("Computed field '{}' updated", definition.key()),
        )?;
        Ok(StorageMutationOutcome::committed(definition, receipt))
    }

    async fn delete_personal_computed_field(
        &self,
        request: StoragePersonalComputedFieldDelete,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        let (owner_id, definition_id, context) = request.into_parts();
        let mut state = self.state.write().await;
        let Some(definition) = state.computed_fields.get(&definition_id.id()).cloned() else {
            return Ok(StorageMutationOutcome::unchanged(()));
        };
        if definition.visibility() != (StorageComputedFieldVisibility::Personal { owner_id }) {
            return Err(StorageError::not_found(
                "Personal computed field definition was not found",
            ));
        }
        state.computed_fields.remove(&definition_id.id());
        let receipt = state.append_simple_event(
            EntityType::ComputedFieldDefinition,
            definition_id.id(),
            Some(definition.key()),
            Action::Deleted,
            &context,
            format!("Computed field '{}' deleted", definition.key()),
        )?;
        Ok(StorageMutationOutcome::committed((), receipt))
    }

    async fn request_computed_field_rebuild(
        &self,
        request: StorageComputedFieldRebuildRequest,
    ) -> Result<StorageClassComputationState, StorageError> {
        let (class_id, collection_id, actor_id) = request.into_parts();
        let actor_id = actor_id.unwrap_or_else(|| PrincipalId::new(1).expect("admin id is valid"));
        {
            let state = self.state.read().await;
            let class = state.classes.get(&class_id.id()).ok_or_else(|| {
                StorageError::not_found(format!("Class {} was not found", class_id.id()))
            })?;
            if class.collection_id() != collection_id {
                return Err(StorageError::not_found(
                    "Class was not found in the authorized collection",
                ));
            }
        }
        let task = self
            .create_task(
                StorageTaskCreateRequest::builder(
                    StorageTaskKind::Reindex,
                    actor_id,
                    serde_json::json!({"class_id": class_id.id()}),
                    0,
                )
                .scope_snapshot(StorageTaskScopeSnapshot::unscoped())
                .try_build(100)?,
            )
            .await?;
        let mut state = self.state.write().await;
        let class = state.classes.get(&class_id.id()).cloned().ok_or_else(|| {
            StorageError::not_found(format!("Class {} was not found", class_id.id()))
        })?;
        let previous = state
            .computation_states
            .get(&class_id.id())
            .cloned()
            .unwrap_or(ready_computation_state(class_id, 0, class.created_at())?);
        let computation_state = StorageClassComputationState::try_new(
            class_id,
            previous.evaluation_revision(),
            StorageComputationRebuildState::Rebuilding {
                active_task_id: task.id(),
            },
            previous.created_at(),
            Utc::now(),
        )
        .map_err(invalid_contract_value)?;
        state
            .computed_rebuild_tasks
            .insert(task.id().id(), class_id);
        state
            .computation_states
            .insert(class_id.id(), computation_state.clone());
        Ok(computation_state)
    }

    async fn execute_computed_field_rebuild(
        &self,
        lease: StorageTaskLease,
    ) -> Result<StorageTask, StorageError> {
        let mut state = self.state.write().await;
        let class_id = state
            .computed_rebuild_tasks
            .get(&lease.task_id().id())
            .copied()
            .ok_or_else(|| StorageError::not_found("Computed-field rebuild task was not found"))?;
        let task = state
            .tasks
            .get_mut(&lease.task_id().id())
            .ok_or_else(|| StorageError::not_found("Computed-field rebuild task was not found"))?;
        if task.kind != StorageTaskKind::Reindex || !task.lease_matches(&lease) {
            return Err(invalid_task_lease());
        }
        let now = Utc::now();
        task.status = StorageTaskStatus::Succeeded;
        task.summary = Some("Computed-field rebuild completed".to_string());
        task.updated_at = now;
        task.finished_at = Some(now);
        task.lease_expires_at = None;
        task.claim_token = None;
        task.request_payload = None;
        task.request_redacted_at = Some(now);
        let completed = task.projection()?;
        let previous = state
            .computation_states
            .get(&class_id.id())
            .cloned()
            .ok_or_else(|| StorageError::internal("computed-field rebuild state is missing"))?;
        let computation_state = ready_computation_state(
            class_id,
            previous.evaluation_revision().get(),
            previous.created_at(),
        )?;
        state
            .computation_states
            .insert(class_id.id(), computation_state);
        state.computed_rebuild_tasks.remove(&lease.task_id().id());
        state.append_task_event_record(
            lease.task_id(),
            StorageTaskEventInput::new("succeeded", "Computed-field rebuild completed"),
        )?;
        Ok(completed)
    }
}

#[async_trait]
impl ComputedObjectStorage for MemoryStorage {
    async fn list_computed_objects(
        &self,
        query: StorageComputedObjectListQuery,
    ) -> Result<StorageComputedObjectPage, StorageError> {
        let (class_id, personal_owner_id, options, visibility, projection) = query.into_parts();
        let (requested, _, effective_page_limit) = options.into_parts();
        let state = self.state.read().await;
        let mut rows = state
            .objects
            .values()
            .filter(|object| object.class_id() == class_id)
            .filter(|object| match &visibility {
                StorageComputedObjectVisibility::Storage(visibility) => visibility
                    .resources()
                    .is_none_or(|scope| scope.object_ids().contains(&object.id())),
                StorageComputedObjectVisibility::AuthorizedObjectIds { object_ids, .. } => {
                    object_ids.contains(&object.id())
                }
            })
            .filter(|object| {
                requested.filters().as_slice().iter().all(|filter| {
                    if let Some(computed) = filter.field.computed_query() {
                        let visibility = match computed.scope() {
                            ComputedFieldScope::Shared => StorageComputedFieldVisibility::Shared,
                            ComputedFieldScope::Personal => {
                                let Some(owner_id) = personal_owner_id else {
                                    return false;
                                };
                                StorageComputedFieldVisibility::Personal { owner_id }
                            }
                        };
                        let value = state
                            .computed_fields
                            .values()
                            .find(|definition| {
                                definition.class_id() == object.class_id()
                                    && definition.visibility() == visibility
                                    && definition.key() == computed.key()
                            })
                            .map(|definition| evaluate_computed_definition(definition, object));
                        let actual = value.as_ref().map_or_else(
                            || "null".to_string(),
                            |value| {
                                value
                                    .as_str()
                                    .map(ToOwned::to_owned)
                                    .unwrap_or_else(|| value.to_string())
                            },
                        );
                        return string_filter_matches(&actual, &filter.operator, &filter.value);
                    }
                    let actual = match filter.field {
                        FilterField::Id => object.id().id().to_string(),
                        FilterField::Name => object.name().to_string(),
                        FilterField::ClassId => object.class_id().id().to_string(),
                        FilterField::CollectionId => object.collection_id().id().to_string(),
                        _ => return true,
                    };
                    string_filter_matches(&actual, &filter.operator, &filter.value)
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        rows.sort_by_key(StorageObject::id);
        let total = requested
            .include_total()
            .then(|| i64::try_from(rows.len()).unwrap_or(i64::MAX));
        rows.truncate(effective_page_limit);
        let computed = if projection == StorageComputedObjectProjection::None {
            Vec::new()
        } else {
            rows.iter()
                .cloned()
                .map(|object| computed_object(&state, object, personal_owner_id))
                .collect::<Result<Vec<_>, _>>()?
        };
        StorageComputedObjectPage::try_new(rows, total, computed, requested)
            .map_err(invalid_contract_value)
    }

    async fn enrich_objects_with_computed(
        &self,
        query: StorageComputedObjectEnrichmentQuery,
    ) -> Result<Vec<StorageComputedObject>, StorageError> {
        let (objects, personal_owner_id) = query.into_parts();
        let state = self.state.read().await;
        objects
            .into_iter()
            .map(|object| computed_object(&state, object, personal_owner_id))
            .collect()
    }
}

#[async_trait]
impl ObjectAggregateStorage for MemoryStorage {
    async fn aggregate_objects(
        &self,
        query: StorageObjectAggregateQuery,
        authorization: StorageObjectAggregateAuthorization<'_>,
    ) -> Result<StorageObjectAggregatePage, StorageError> {
        let (collection_name, mut objects) = {
            let state = self.state.read().await;
            let collection = state
                .collections
                .get(&query.target().collection_id().id())
                .ok_or_else(|| StorageError::not_found("Aggregate collection was not found"))?;
            let objects = state
                .objects
                .values()
                .filter(|object| object.class_id() == query.target().class_id())
                .filter(|object| {
                    query.options().filters().as_slice().iter().all(|filter| {
                        let actual = match filter.field {
                            FilterField::Id => object.id().id().to_string(),
                            FilterField::Name => object.name().to_string(),
                            FilterField::ClassId => object.class_id().id().to_string(),
                            FilterField::CollectionId => object.collection_id().id().to_string(),
                            _ => return true,
                        };
                        string_filter_matches(&actual, &filter.operator, &filter.value)
                    })
                })
                .cloned()
                .collect::<Vec<_>>();
            (collection.name().to_string(), objects)
        };
        if let StorageObjectAggregateAuthorization::Delegated(authorizer) = authorization {
            let target_allowed = authorizer
                .authorize_target(
                    StorageObjectAggregateAuthorizationTarget::new(
                        query.target().class_id(),
                        query.target().class_name().to_string(),
                        query.target().collection_id(),
                        collection_name,
                    ),
                    query.required_permissions().to_vec(),
                )
                .await?;
            if !target_allowed {
                return StorageObjectAggregatePage::try_new(
                    Vec::new(),
                    query.options().include_total().then_some(0),
                    None,
                )
                .map_err(invalid_contract_value);
            }
            let decisions = authorizer
                .authorize_objects(
                    objects
                        .iter()
                        .map(|object| {
                            StorageObjectAggregateAuthorizationCandidate::new(
                                object.id(),
                                object.name().to_string(),
                                object.collection_id(),
                                object.class_id(),
                            )
                        })
                        .collect(),
                    query.required_permissions().to_vec(),
                )
                .await?;
            if decisions.len() != objects.len() {
                return Err(StorageError::backend_failure(
                    "Aggregate authorizer returned the wrong decision count",
                ));
            }
            objects = objects
                .into_iter()
                .zip(decisions)
                .filter_map(|(object, allowed)| allowed.then_some(object))
                .collect();
        }

        struct AggregateGroup {
            sort_key: serde_json::Value,
            object_count: i64,
            measures: Vec<Vec<f64>>,
        }

        let state = self.state.read().await;
        let dimension_value = |object: &StorageObject,
                               dimension: &StorageObjectAggregateDimension|
         -> serde_json::Value {
            let value = match dimension {
                StorageObjectAggregateDimension::Scalar(field) => match field {
                    StorageObjectAggregateScalarField::Name => {
                        serde_json::Value::String(object.name().to_string())
                    }
                    StorageObjectAggregateScalarField::Description => {
                        serde_json::Value::String(object.description().to_string())
                    }
                    StorageObjectAggregateScalarField::CollectionId => {
                        serde_json::Value::from(object.collection_id().id())
                    }
                    StorageObjectAggregateScalarField::CreatedAt => {
                        serde_json::Value::String(object.created_at().naive_utc().to_string())
                    }
                    StorageObjectAggregateScalarField::UpdatedAt => {
                        serde_json::Value::String(object.updated_at().naive_utc().to_string())
                    }
                },
                StorageObjectAggregateDimension::JsonData(path) => path
                    .segments()
                    .try_fold(object.data(), |value, segment| value.get(segment))
                    .cloned()
                    .unwrap_or(serde_json::Value::Null),
                StorageObjectAggregateDimension::Computed(selector) => {
                    let visibility = match selector.scope() {
                        ComputedFieldScope::Shared => StorageComputedFieldVisibility::Shared,
                        ComputedFieldScope::Personal => {
                            let Some(owner_id) = query.personal_owner_id() else {
                                return serde_json::json!([3, null]);
                            };
                            StorageComputedFieldVisibility::Personal { owner_id }
                        }
                    };
                    state
                        .computed_fields
                        .values()
                        .find(|definition| {
                            definition.class_id() == object.class_id()
                                && definition.visibility() == visibility
                                && definition.key() == selector.key()
                        })
                        .map(|definition| evaluate_computed_definition(definition, object))
                        .unwrap_or(serde_json::Value::Null)
                }
            };
            if value.is_null() {
                let state = match dimension {
                    StorageObjectAggregateDimension::JsonData(_) => 2,
                    StorageObjectAggregateDimension::Computed(_) => 3,
                    StorageObjectAggregateDimension::Scalar(_) => 1,
                };
                serde_json::json!([state, null])
            } else {
                serde_json::json!([0, value])
            }
        };
        let measure_value = |object: &StorageObject,
                             measure: &StorageObjectAggregateMeasure|
         -> Option<f64> {
            let value = match measure.field() {
                StorageObjectAggregateMeasureField::JsonData(path) => path
                    .segments()
                    .try_fold(object.data(), |value, segment| value.get(segment))
                    .cloned(),
                StorageObjectAggregateMeasureField::Computed(selector) => {
                    let visibility = match selector.scope() {
                        ComputedFieldScope::Shared => StorageComputedFieldVisibility::Shared,
                        ComputedFieldScope::Personal => StorageComputedFieldVisibility::Personal {
                            owner_id: query.personal_owner_id()?,
                        },
                    };
                    state
                        .computed_fields
                        .values()
                        .find(|definition| {
                            definition.class_id() == object.class_id()
                                && definition.visibility() == visibility
                                && definition.key() == selector.key()
                        })
                        .map(|definition| evaluate_computed_definition(definition, object))
                }
            }?;
            value.as_f64()
        };
        let mut groups = BTreeMap::<String, AggregateGroup>::new();
        for object in &objects {
            let sort_key = serde_json::Value::Array(
                query
                    .spec()
                    .dimensions()
                    .iter()
                    .map(|dimension| dimension_value(object, dimension))
                    .collect(),
            );
            let key = sort_key.to_string();
            let group = groups.entry(key).or_insert_with(|| AggregateGroup {
                sort_key,
                object_count: 0,
                measures: vec![Vec::new(); query.spec().measures().len()],
            });
            group.object_count += 1;
            for (values, measure) in group.measures.iter_mut().zip(query.spec().measures()) {
                if let Some(value) = measure_value(object, measure) {
                    values.push(value);
                }
            }
        }
        drop(state);
        let mut rows = groups
            .into_values()
            .map(|group| {
                let measures = group
                    .measures
                    .into_iter()
                    .zip(query.spec().measures())
                    .map(|(values, measure)| {
                        if values.is_empty() {
                            return StorageObjectAggregateMeasureValue::try_new(
                                StorageObjectAggregateMeasureState::Empty,
                                0,
                                group.object_count,
                                None,
                            )
                            .map_err(invalid_contract_value);
                        }
                        let value = match measure.operation() {
                            StorageObjectAggregateMeasureOperation::Sum => values.iter().sum(),
                            StorageObjectAggregateMeasureOperation::Average => {
                                values.iter().sum::<f64>() / values.len() as f64
                            }
                            StorageObjectAggregateMeasureOperation::Min => values
                                .iter()
                                .copied()
                                .reduce(f64::min)
                                .expect("non-empty measure values"),
                            StorageObjectAggregateMeasureOperation::Max => values
                                .iter()
                                .copied()
                                .reduce(f64::max)
                                .expect("non-empty measure values"),
                        };
                        StorageObjectAggregateMeasureValue::try_new(
                            StorageObjectAggregateMeasureState::Value,
                            i64::try_from(values.len()).unwrap_or(i64::MAX),
                            group.object_count - i64::try_from(values.len()).unwrap_or(i64::MAX),
                            serde_json::Number::from_f64(value).map(serde_json::Value::Number),
                        )
                        .map_err(invalid_contract_value)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                StorageObjectAggregateRow::try_new(measures, group.object_count, group.sort_key)
                    .map_err(invalid_contract_value)
            })
            .collect::<Result<Vec<_>, StorageError>>()?;
        match query.spec().sort() {
            StorageObjectAggregateSort::DimensionsAscending => rows.sort_by_key(|row| {
                let (_, _, key) = row.clone().into_parts();
                key.to_string()
            }),
            StorageObjectAggregateSort::DimensionsDescending => rows.sort_by_key(|row| {
                let (_, _, key) = row.clone().into_parts();
                std::cmp::Reverse(key.to_string())
            }),
            StorageObjectAggregateSort::ObjectCountAscending => rows.sort_by_key(|row| {
                let (_, count, _) = row.clone().into_parts();
                count
            }),
            StorageObjectAggregateSort::ObjectCountDescending => rows.sort_by_key(|row| {
                let (_, count, _) = row.clone().into_parts();
                std::cmp::Reverse(count)
            }),
        }
        let total = query
            .options()
            .include_total()
            .then(|| i64::try_from(rows.len()).unwrap_or(i64::MAX));
        let has_more = rows.len() > query.page_limit();
        rows.truncate(query.page_limit());
        let next_cursor = has_more
            .then(|| rows.last())
            .flatten()
            .map(|row| {
                query
                    .spec()
                    .encode_cursor(row, query.cursor_max_encoded_bytes())
            })
            .transpose()?;
        StorageObjectAggregatePage::try_new(rows, total, next_cursor)
            .map_err(invalid_contract_value)
    }
}

#[async_trait]
impl RelationQueryStorage for MemoryStorage {
    async fn list_class_relations(
        &self,
        query: StorageRelationListQuery,
    ) -> Result<StoragePage<StorageClassRelation>, StorageError> {
        let (options, _) = query.into_parts();
        let rows = self
            .state
            .read()
            .await
            .class_relations
            .values()
            .cloned()
            .collect();
        page(rows, &options)
    }

    async fn list_object_relations(
        &self,
        query: StorageRelationListQuery,
    ) -> Result<StoragePage<StorageObjectRelation>, StorageError> {
        let (options, _) = query.into_parts();
        let rows = self
            .state
            .read()
            .await
            .object_relations
            .values()
            .cloned()
            .collect();
        page(rows, &options)
    }

    async fn list_class_relations_touching(
        &self,
        query: StorageRelationTouchingQuery,
    ) -> Result<StoragePage<StorageClassRelation>, StorageError> {
        let (anchor_id, options, _) = query.into_parts();
        let anchor = ClassId::from(anchor_id);
        let rows = self
            .state
            .read()
            .await
            .class_relations
            .values()
            .filter(|relation| {
                relation.from_class_id() == anchor || relation.to_class_id() == anchor
            })
            .cloned()
            .collect();
        page(rows, &options)
    }

    async fn list_object_relations_touching(
        &self,
        query: StorageRelationTouchingQuery,
    ) -> Result<StoragePage<StorageObjectRelation>, StorageError> {
        let (anchor_id, options, _) = query.into_parts();
        let anchor = ObjectId::from(anchor_id);
        let rows = self
            .state
            .read()
            .await
            .object_relations
            .values()
            .filter(|relation| {
                relation.from_object_id() == anchor || relation.to_object_id() == anchor
            })
            .cloned()
            .collect();
        page(rows, &options)
    }

    async fn list_class_relations_touching_ids(
        &self,
        query: StorageRelationIdsQuery,
    ) -> Result<Vec<StorageClassRelation>, StorageError> {
        let (ids, _) = query.into_parts();
        let ids = ids.into_iter().map(ClassId::from).collect::<BTreeSet<_>>();
        Ok(self
            .state
            .read()
            .await
            .class_relations
            .values()
            .filter(|relation| {
                ids.contains(&relation.from_class_id()) || ids.contains(&relation.to_class_id())
            })
            .cloned()
            .collect())
    }

    async fn list_class_relations_between_ids(
        &self,
        query: StorageRelationIdsQuery,
    ) -> Result<Vec<StorageClassRelation>, StorageError> {
        let (ids, _) = query.into_parts();
        let ids = ids.into_iter().map(ClassId::from).collect::<BTreeSet<_>>();
        Ok(self
            .state
            .read()
            .await
            .class_relations
            .values()
            .filter(|relation| {
                ids.contains(&relation.from_class_id()) && ids.contains(&relation.to_class_id())
            })
            .cloned()
            .collect())
    }

    async fn list_object_relations_touching_ids(
        &self,
        query: StorageObjectRelationsTouchingIdsQuery,
    ) -> Result<Vec<StorageObjectRelation>, StorageError> {
        let (object_ids, excluded_relation_ids, max_results, _) = query.into_parts();
        let object_ids = object_ids.into_iter().collect::<BTreeSet<_>>();
        let excluded_relation_ids = excluded_relation_ids.into_iter().collect::<BTreeSet<_>>();
        Ok(self
            .state
            .read()
            .await
            .object_relations
            .values()
            .filter(|relation| {
                let relation_id = ObjectRelationId::from(relation.metadata().id());
                !excluded_relation_ids.contains(&relation_id)
                    && (object_ids.contains(&relation.from_object_id())
                        || object_ids.contains(&relation.to_object_id()))
            })
            .take(max_results)
            .cloned()
            .collect())
    }

    async fn list_object_relations_between_ids(
        &self,
        query: StorageRelationIdsQuery,
    ) -> Result<Vec<StorageObjectRelation>, StorageError> {
        let (ids, _) = query.into_parts();
        let ids = ids.into_iter().map(ObjectId::from).collect::<BTreeSet<_>>();
        Ok(self
            .state
            .read()
            .await
            .object_relations
            .values()
            .filter(|relation| {
                ids.contains(&relation.from_object_id()) && ids.contains(&relation.to_object_id())
            })
            .cloned()
            .collect())
    }

    async fn list_related_classes(
        &self,
        query: StorageRelationGraphQuery,
    ) -> Result<StoragePage<StorageClassGraphRow>, StorageError> {
        let (root_id, options, _) = query.into_parts();
        let root_id = ClassId::from(root_id);
        let state = self.state.read().await;
        let root = state
            .classes
            .get(&root_id.id())
            .ok_or_else(|| StorageError::not_found("Relation graph root class was not found"))?;
        let mut rows = Vec::new();
        for relation in state.class_relations.values().filter(|relation| {
            relation.from_class_id() == root_id || relation.to_class_id() == root_id
        }) {
            let descendant_id = if relation.from_class_id() == root_id {
                relation.to_class_id()
            } else {
                relation.from_class_id()
            };
            let descendant = state
                .classes
                .get(&descendant_id.id())
                .ok_or_else(|| StorageError::internal("related class is missing"))?;
            rows.push(
                StorageClassGraphRow::try_new(
                    graph_class(root)?,
                    graph_class(descendant)?,
                    1,
                    vec![root_id, descendant_id],
                )
                .map_err(invalid_contract_value)?,
            );
        }
        page(rows, &options)
    }

    async fn list_related_objects(
        &self,
        query: StorageRelationGraphQuery,
    ) -> Result<StoragePage<StorageObjectGraphRow>, StorageError> {
        let (root_id, options, _) = query.into_parts();
        let root_id = ObjectId::from(root_id);
        let state = self.state.read().await;
        let root = state
            .objects
            .get(&root_id.id())
            .ok_or_else(|| StorageError::not_found("Relation graph root object was not found"))?;
        let mut rows = Vec::new();
        for relation in state.object_relations.values().filter(|relation| {
            relation.from_object_id() == root_id || relation.to_object_id() == root_id
        }) {
            let descendant_id = if relation.from_object_id() == root_id {
                relation.to_object_id()
            } else {
                relation.from_object_id()
            };
            let descendant = state
                .objects
                .get(&descendant_id.id())
                .ok_or_else(|| StorageError::internal("related object is missing"))?;
            rows.push(
                StorageObjectGraphRow::try_new(
                    graph_object(root)?,
                    graph_object(descendant)?,
                    1,
                    vec![root_id, descendant_id],
                )
                .map_err(invalid_contract_value)?,
            );
        }
        page(rows, &options)
    }

    async fn list_related_objects_for_roots(
        &self,
        query: StorageRelatedObjectsForRootsQuery,
    ) -> Result<Vec<StorageRelatedObjectIncludeRow>, StorageError> {
        let (root_ids, class_id, class_relation_id, direction, _, max_depth, limit, _, _) =
            query.into_parts();
        if max_depth < 1 || limit <= 0 {
            return Ok(Vec::new());
        }
        let state = self.state.read().await;
        let mut rows = Vec::new();
        for root_id in root_ids {
            let Some(root) = state.objects.get(&root_id.id()) else {
                continue;
            };
            for relation in state.object_relations.values().filter(|relation| {
                class_relation_id.is_none_or(|id| relation.class_relation_id() == id)
                    && match direction {
                        StorageRelatedDirection::Any => {
                            relation.from_object_id() == root_id
                                || relation.to_object_id() == root_id
                        }
                        StorageRelatedDirection::Outgoing => relation.from_object_id() == root_id,
                        StorageRelatedDirection::Incoming => relation.to_object_id() == root_id,
                    }
            }) {
                let descendant_id = if relation.from_object_id() == root_id {
                    relation.to_object_id()
                } else {
                    relation.from_object_id()
                };
                let Some(descendant) = state.objects.get(&descendant_id.id()) else {
                    continue;
                };
                if descendant.class_id() != class_id {
                    continue;
                }
                let graph_row = StorageObjectGraphRow::try_new(
                    graph_object(root)?,
                    graph_object(descendant)?,
                    1,
                    vec![root_id, descendant_id],
                )
                .map_err(invalid_contract_value)?;
                rows.push(
                    StorageRelatedObjectIncludeRow::try_new(root_id, graph_row)
                        .map_err(invalid_contract_value)?,
                );
                if rows.len() >= usize::try_from(limit).unwrap_or(usize::MAX) {
                    return Ok(rows);
                }
            }
        }
        Ok(rows)
    }

    async fn list_bidirectionally_related_objects_for_roots(
        &self,
        query: StorageBidirectionalRelatedObjectsQuery,
    ) -> Result<Vec<StorageRelatedObjectForRootRow>, StorageError> {
        let (root_ids, max_depth, per_root_cap, _, _) = query.into_parts();
        if max_depth < 1 || per_root_cap <= 0 {
            return Ok(Vec::new());
        }
        let state = self.state.read().await;
        let mut rows = Vec::new();
        for root_id in root_ids {
            let mut root_count = 0_i32;
            for relation in state.object_relations.values().filter(|relation| {
                relation.from_object_id() == root_id || relation.to_object_id() == root_id
            }) {
                if root_count >= per_root_cap {
                    break;
                }
                let descendant_id = if relation.from_object_id() == root_id {
                    relation.to_object_id()
                } else {
                    relation.from_object_id()
                };
                let Some(descendant) = state.objects.get(&descendant_id.id()) else {
                    continue;
                };
                rows.push(
                    StorageRelatedObjectForRootRow::try_new(
                        root_id,
                        graph_object(descendant)?,
                        1,
                        vec![root_id, descendant_id],
                    )
                    .map_err(invalid_contract_value)?,
                );
                root_count += 1;
            }
        }
        Ok(rows)
    }
}
#[async_trait]
impl UnifiedSearchStorage for MemoryStorage {
    async fn search_collections(
        &self,
        query: StorageUnifiedSearchQuery,
    ) -> Result<StorageCandidatePage<StorageUnifiedSearchCandidate<StorageCollection>>, StorageError>
    {
        let state = self.state.read().await;
        let mut ranked = state
            .collections
            .values()
            .filter(|collection| {
                query
                    .visibility()
                    .resources()
                    .is_none_or(|scope| scope.collection_ids().contains(&collection.id()))
            })
            .filter_map(|collection| {
                search_rank(
                    collection.name(),
                    collection.description(),
                    None,
                    query.search_term(),
                )
                .map(|rank| (rank, collection.clone()))
            })
            .collect::<Vec<_>>();
        ranked.sort_by(|(left_rank, left), (right_rank, right)| {
            right_rank
                .cmp(left_rank)
                .then_with(|| left.name().to_lowercase().cmp(&right.name().to_lowercase()))
                .then_with(|| left.id().cmp(&right.id()))
        });
        let limit = query.page_limit();
        let has_more = ranked.len() > limit.get();
        ranked.truncate(limit.get());
        let rows = ranked
            .into_iter()
            .map(|(rank, collection)| {
                let cursor = StorageUnifiedSearchCursor::new(
                    rank,
                    collection.name().to_lowercase(),
                    ResourceId::new(collection.id().id()).expect("collection id is positive"),
                );
                StorageUnifiedSearchCandidate::new(collection, cursor)
            })
            .collect();
        StorageCandidatePage::try_new(rows, has_more, limit).map_err(invalid_contract_value)
    }

    async fn search_classes(
        &self,
        query: StorageUnifiedSearchQuery,
    ) -> Result<
        StorageCandidatePage<StorageUnifiedSearchCandidate<StorageClassWithCollection>>,
        StorageError,
    > {
        let state = self.state.read().await;
        let mut ranked = state
            .classes
            .values()
            .filter(|class| {
                query.visibility().resources().is_none_or(|scope| {
                    scope.class_ids().contains(&class.id())
                        || scope.collection_ids().contains(&class.collection_id())
                })
            })
            .filter_map(|class| {
                let extended = query.searches_extended_document().then(|| {
                    class
                        .json_schema()
                        .map(ToString::to_string)
                        .unwrap_or_default()
                });
                search_rank(
                    class.name(),
                    class.description(),
                    extended.as_deref(),
                    query.search_term(),
                )
                .map(|rank| (rank, class))
            })
            .map(|(rank, class)| Ok((rank, class_with_collection(&state, class)?)))
            .collect::<Result<Vec<_>, StorageError>>()?;
        ranked.sort_by(|(left_rank, left), (right_rank, right)| {
            right_rank
                .cmp(left_rank)
                .then_with(|| left.name().to_lowercase().cmp(&right.name().to_lowercase()))
                .then_with(|| left.id().cmp(&right.id()))
        });
        let limit = query.page_limit();
        let has_more = ranked.len() > limit.get();
        ranked.truncate(limit.get());
        let rows = ranked
            .into_iter()
            .map(|(rank, class)| {
                let cursor = StorageUnifiedSearchCursor::new(
                    rank,
                    class.name().to_lowercase(),
                    ResourceId::new(class.id().id()).expect("class id is positive"),
                );
                StorageUnifiedSearchCandidate::new(class, cursor)
            })
            .collect();
        StorageCandidatePage::try_new(rows, has_more, limit).map_err(invalid_contract_value)
    }

    async fn search_objects(
        &self,
        query: StorageUnifiedSearchQuery,
    ) -> Result<StorageCandidatePage<StorageUnifiedSearchCandidate<StorageObject>>, StorageError>
    {
        let state = self.state.read().await;
        let mut ranked = state
            .objects
            .values()
            .filter(|object| {
                query.visibility().resources().is_none_or(|scope| {
                    scope.object_ids().contains(&object.id())
                        || scope.class_ids().contains(&object.class_id())
                        || scope.collection_ids().contains(&object.collection_id())
                })
            })
            .filter_map(|object| {
                let extended = query
                    .searches_extended_document()
                    .then(|| object.data().to_string());
                search_rank(
                    object.name(),
                    object.description(),
                    extended.as_deref(),
                    query.search_term(),
                )
                .map(|rank| (rank, object.clone()))
            })
            .collect::<Vec<_>>();
        ranked.sort_by(|(left_rank, left), (right_rank, right)| {
            right_rank
                .cmp(left_rank)
                .then_with(|| left.name().to_lowercase().cmp(&right.name().to_lowercase()))
                .then_with(|| left.id().cmp(&right.id()))
        });
        let limit = query.page_limit();
        let has_more = ranked.len() > limit.get();
        ranked.truncate(limit.get());
        let rows = ranked
            .into_iter()
            .map(|(rank, object)| {
                let cursor = StorageUnifiedSearchCursor::new(
                    rank,
                    object.name().to_lowercase(),
                    ResourceId::new(object.id().id()).expect("object id is positive"),
                );
                StorageUnifiedSearchCandidate::new(object, cursor)
            })
            .collect();
        StorageCandidatePage::try_new(rows, has_more, limit).map_err(invalid_contract_value)
    }
}
