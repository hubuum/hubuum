use super::*;
use hubuum_events_core::EventSubscriptionScope;
use hubuum_storage_core::StorageEventDeliveryConfiguration;
use hubuum_storage_core::StorageEventDeliveryDisposition;
use hubuum_storage_core::{
    EventSinkGrantAction, StorageAuthorizedEventSink, StorageEventSinkGrantChange,
};
use hubuum_storage_core::{StorageEventNotificationInput, StorageEventNotificationSelection};
use std::time::Instant;

#[async_trait]
impl AuditEventStorage for MemoryStorage {
    async fn list_audit_events(
        &self,
        query: StorageAuditEventListQuery,
    ) -> Result<StoragePage<StorageAuditEvent>, StorageError> {
        let state = self.state.read().await;
        let filters = query.filters();
        let mut events = state
            .events
            .iter()
            .filter(|recorded| {
                let (event, _, _) = (*recorded).clone().into_parts();
                let visible = event
                    .collection_id()
                    .map_or(query.include_collection_less(), |id| {
                        query.accessible_collection_ids().contains(&id)
                    });
                visible
                    && filters
                        .entity_type_value()
                        .is_none_or(|value| event.entity_type() == value)
                    && filters
                        .entity_id_value()
                        .is_none_or(|value| event.entity_id() == Some(value))
                    && filters
                        .action_value()
                        .is_none_or(|value| event.action() == value)
                    && filters
                        .actor_kind_value()
                        .is_none_or(|value| event.actor_kind() == value)
                    && filters
                        .actor_user_id_value()
                        .is_none_or(|value| event.actor_user_id() == Some(value))
                    && filters.initiator_user_id_value().is_none_or(|value| {
                        event
                            .provenance()
                            .initiator
                            .as_ref()
                            .map(|principal| principal.principal_id)
                            == Some(value)
                    })
                    && filters
                        .collection_id_value()
                        .is_none_or(|value| event.collection_id() == Some(value))
                    && filters
                        .occurred_after_value()
                        .is_none_or(|value| event.occurred_at() > value)
                    && filters
                        .occurred_before_value()
                        .is_none_or(|value| event.occurred_at() < value)
            })
            .cloned()
            .collect::<Vec<_>>();
        events.sort_by_key(|recorded| {
            let (event, _, _) = recorded.clone().into_parts();
            std::cmp::Reverse(event.id().get())
        });
        let total = query
            .options()
            .include_total()
            .then(|| i64::try_from(events.len()).unwrap_or(i64::MAX));
        if let Some(limit) = query.options().limit() {
            events.truncate(limit);
        }
        StoragePage::try_new(events, total)
            .map_err(|error| StorageError::backend_failure(error.to_string()))
    }
}

#[async_trait]
impl EventConfigurationStorage for MemoryStorage {
    async fn resolve_event_sink_use(
        &self,
        collection_id: CollectionId,
        sink_id: EventSinkId,
    ) -> Result<StorageAuthorizedEventSink, StorageError> {
        let state = self.state.read().await;
        let sink = state
            .event_sinks
            .get(&sink_id.id())
            .cloned()
            .ok_or_else(|| StorageError::not_found("Event sink was not found"))?;
        StorageAuthorizedEventSink::try_new(
            collection_id,
            sink,
            state
                .event_sink_grants
                .contains(&(sink_id.id(), collection_id.id())),
        )
    }
    async fn list_event_sink_collections(
        &self,
        sink_id: EventSinkId,
    ) -> Result<Vec<CollectionId>, StorageError> {
        let state = self.state.read().await;
        if !state.event_sinks.contains_key(&sink_id.id()) {
            return Err(StorageError::not_found("Event sink was not found"));
        }
        state
            .event_sink_grants
            .iter()
            .filter(|(sink, _)| *sink == sink_id.id())
            .map(|(_, collection)| {
                CollectionId::new(*collection)
                    .map_err(|error| StorageError::backend_failure(error.to_string()))
            })
            .collect()
    }
    async fn change_event_sink_grant(
        &self,
        request: StorageEventSinkGrantChange,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        let mut state = self.state.write().await;
        let sink = state
            .event_sinks
            .get(&request.sink_id().id())
            .ok_or_else(|| StorageError::not_found("Event sink was not found"))?;
        if sink.collection_id().is_some() {
            return Err(StorageError::invalid_input(
                "Collection-owned sinks cannot be shared with other collections",
            ));
        }
        if !state
            .collections
            .contains_key(&request.collection_id().id())
        {
            return Err(StorageError::not_found("Collection was not found"));
        }
        let key = (request.sink_id().id(), request.collection_id().id());
        let changed = match request.action() {
            EventSinkGrantAction::Grant => state.event_sink_grants.insert(key),
            EventSinkGrantAction::Revoke => state.event_sink_grants.remove(&key),
        };
        if !changed {
            return Ok(StorageMutationOutcome::unchanged(()));
        }
        let document = AuditDocument::try_new(
            "Event sink collection grant changed",
            None,
            None,
            serde_json::json!({
                "sink_id": request.sink_id().id(),
                "collection_id": request.collection_id().id(),
                "granted": request.action() == EventSinkGrantAction::Grant,
            }),
        )
        .map_err(|error| StorageError::internal(error.to_string()))?;
        let receipt = append_memory_event!(
            state,
            EntityType::EventSink,
            request.sink_id().id(),
            None,
            Some(request.collection_id()),
            Action::Updated,
            request.event_context(),
            document,
            None,
            None,
        )?;
        Ok(StorageMutationOutcome::committed((), receipt))
    }

    async fn count_enabled_event_sinks(&self) -> Result<i64, StorageError> {
        i64::try_from(
            self.state
                .read()
                .await
                .event_sinks
                .values()
                .filter(|sink| sink.enabled())
                .count(),
        )
        .map_err(|_| StorageError::internal("event sink count does not fit i64"))
    }

    async fn list_event_sinks(
        &self,
        query: StorageEventSinkListQuery,
    ) -> Result<StoragePage<StorageEventSink>, StorageError> {
        let state = self.state.read().await;
        page(
            state
                .event_sinks
                .values()
                .filter(|sink| {
                    query.collection_id().is_none_or(|collection| {
                        memory_sink_allowed(&state, collection.into(), sink.id())
                    })
                })
                .cloned()
                .collect(),
            query.options(),
        )
    }

    async fn get_event_sink(&self, sink_id: EventSinkId) -> Result<StorageEventSink, StorageError> {
        self.state
            .read()
            .await
            .event_sinks
            .get(&sink_id.id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!("Event sink {} was not found", sink_id.id()))
            })
    }

    async fn create_event_sink(
        &self,
        request: StorageEventSinkCreate,
    ) -> Result<StorageMutationOutcome<StorageEventSink>, StorageError> {
        let mut state = self.state.write().await;
        if request
            .collection_id()
            .is_some_and(|id| !state.collections.contains_key(&id.id()))
        {
            return Err(StorageError::not_found("Collection was not found"));
        }

        if state
            .event_sinks
            .values()
            .any(|sink| sink.name() == request.name())
        {
            return Err(StorageError::conflict(format!(
                "Event sink '{}' already exists",
                request.name()
            )));
        }
        let id = EventSinkId::new(state.next_event_sink_id)
            .map_err(|error| StorageError::internal(error.to_string()))?;
        state.next_event_sink_id += 1;
        let now = Utc::now();
        let sink = StorageEventSink::builder(
            id,
            request.name(),
            request.kind(),
            now,
            now,
            ResourceRevision::INITIAL,
        )
        .collection_id(request.collection_id())
        .configuration(request.configuration().clone())
        .delivery_policy(request.delivery_policy())
        .secret_ref(request.secret_ref().map(ToOwned::to_owned))
        .enabled(request.enabled())
        .try_build()
        .map_err(invalid_contract_value)?;
        state.event_sinks.insert(id.id(), sink.clone());
        let receipt = append_memory_scoped_simple_event!(
            state,
            EntityType::EventSink,
            id.id(),
            Some(sink.name()),
            sink.collection_id(),
            Action::Created,
            request.event_context(),
            format!("Event sink '{}' created", sink.name()),
        )?;
        Ok(StorageMutationOutcome::committed(sink, receipt))
    }

    async fn update_event_sink(
        &self,
        request: StorageEventSinkUpdate,
    ) -> Result<StorageMutationOutcome<StorageEventSink>, StorageError> {
        let mut state = self.state.write().await;
        let current = state
            .event_sinks
            .get(&request.id().id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!("Event sink {} was not found", request.id().id()))
            })?;
        let name = request.name_value().unwrap_or(current.name());
        let kind = request.kind_value().unwrap_or(current.kind());
        let configuration = request
            .configuration_value()
            .unwrap_or(current.configuration())
            .clone();
        let secret_ref = request.secret_ref_value().map_or_else(
            || current.secret_ref().map(ToOwned::to_owned),
            |value| value.map(ToOwned::to_owned),
        );
        let enabled = request.enabled_value().unwrap_or(current.enabled());
        if name == current.name()
            && kind == current.kind()
            && configuration == *current.configuration()
            && request
                .delivery_policy()
                .is_none_or(|value| value == current.delivery_policy())
            && secret_ref.as_deref() == current.secret_ref()
            && enabled == current.enabled()
        {
            return Ok(StorageMutationOutcome::unchanged(current));
        }
        if state
            .event_sinks
            .values()
            .any(|sink| sink.id() != request.id() && sink.name() == name)
        {
            return Err(StorageError::conflict(format!(
                "Event sink '{name}' already exists"
            )));
        }
        let sink = StorageEventSink::builder(
            current.id(),
            name,
            kind,
            current.created_at(),
            Utc::now(),
            current
                .revision()
                .checked_advance()
                .map_err(|error| StorageError::internal(error.to_string()))?,
        )
        .collection_id(current.collection_id())
        .configuration(configuration)
        .delivery_policy(
            request
                .delivery_policy()
                .unwrap_or(current.delivery_policy()),
        )
        .secret_ref(secret_ref)
        .enabled(enabled)
        .try_build()
        .map_err(invalid_contract_value)?;
        if sink.delivery_policy() != current.delivery_policy() {
            let now = Utc::now();
            if let Some((next, blocked)) = state.event_sink_schedule.get_mut(&sink.id().id()) {
                *next = match (
                    current.delivery_policy().min_interval_ms(),
                    sink.delivery_policy().min_interval_ms(),
                ) {
                    (Some(old), Some(new)) => {
                        *next + chrono::Duration::milliseconds(new as i64 - old as i64)
                    }
                    _ => now,
                };
                let eligible = (*next).max(*blocked).max(now);
                let reason = if *blocked > now {
                    Some("provider_rate")
                } else if *next > now {
                    Some("configured_rate")
                } else {
                    None
                };
                let subscriptions = state
                    .event_subscriptions
                    .values()
                    .filter(|subscription| subscription.sink_id() == sink.id())
                    .map(|subscription| subscription.id())
                    .collect::<std::collections::HashSet<_>>();
                for delivery in state.event_deliveries.values_mut().filter(|delivery| {
                    subscriptions.contains(&delivery.subscription_id())
                        && delivery.status() == EventDeliveryStatus::Pending
                        && delivery.deferred_reason().is_some()
                }) {
                    *delivery = delivery_metadata(
                        rebuild_event_delivery(
                            delivery,
                            delivery.status(),
                            delivery.attempts(),
                            eligible,
                            delivery.last_error().map(str::to_owned),
                            delivery.locked_until(),
                        )?,
                        delivery.purpose(),
                        reason.map(str::to_owned),
                    )?;
                }
            }
        }
        state.event_sinks.insert(sink.id().id(), sink.clone());
        let receipt = append_memory_scoped_simple_event!(
            state,
            EntityType::EventSink,
            sink.id().id(),
            Some(sink.name()),
            sink.collection_id(),
            Action::Updated,
            request.event_context(),
            format!("Event sink '{}' updated", sink.name()),
        )?;
        Ok(StorageMutationOutcome::committed(sink, receipt))
    }

    async fn delete_event_sink(
        &self,
        request: StorageEventSinkDelete,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        let mut state = self.state.write().await;
        if state
            .event_subscriptions
            .values()
            .any(|subscription| subscription.sink_id() == request.id())
        {
            return Err(StorageError::conflict("Event sink still has subscriptions"));
        }
        let sink = state
            .event_sinks
            .remove(&request.id().id())
            .ok_or_else(|| {
                StorageError::not_found(format!("Event sink {} was not found", request.id().id()))
            })?;
        state
            .event_sink_grants
            .retain(|(sink, _)| *sink != request.id().id());
        let receipt = append_memory_scoped_simple_event!(
            state,
            EntityType::EventSink,
            sink.id().id(),
            Some(sink.name()),
            sink.collection_id(),
            Action::Deleted,
            request.event_context(),
            format!("Event sink '{}' deleted", sink.name()),
        )?;
        Ok(StorageMutationOutcome::committed((), receipt))
    }

    async fn list_event_subscriptions(
        &self,
        query: StorageEventSubscriptionListQuery,
    ) -> Result<StoragePage<StorageEventSubscription>, StorageError> {
        let rows = self
            .state
            .read()
            .await
            .event_subscriptions
            .values()
            .filter(|subscription| subscription.scope() == query.scope())
            .cloned()
            .collect();
        page(rows, query.options())
    }

    async fn get_event_subscription(
        &self,
        scope: EventSubscriptionScope,
        subscription_id: EventSubscriptionId,
    ) -> Result<StorageEventSubscription, StorageError> {
        self.state
            .read()
            .await
            .event_subscriptions
            .get(&subscription_id.id())
            .filter(|subscription| subscription.scope() == scope)
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!(
                    "Event subscription {} was not found in scope {:?}",
                    subscription_id.id(),
                    scope
                ))
            })
    }

    async fn create_event_subscription(
        &self,
        request: StorageEventSubscriptionCreate,
    ) -> Result<StorageMutationOutcome<StorageEventSubscription>, StorageError> {
        let mut state = self.state.write().await;
        if let Some(id) = request.scope().collection_id()
            && !state.collections.contains_key(&id.id())
        {
            return Err(StorageError::not_found("Collection was not found"));
        }
        if !state.event_sinks.contains_key(&request.sink_id().id()) {
            return Err(StorageError::not_found(format!(
                "Event sink {} was not found",
                request.sink_id().id()
            )));
        }
        ensure_memory_sink_allowed(&state, request.scope(), request.sink_id())?;
        if state.event_subscriptions.values().any(|subscription| {
            subscription.scope() == request.scope() && subscription.name() == request.name()
        }) {
            return Err(StorageError::conflict(format!(
                "Event subscription '{}' already exists",
                request.name()
            )));
        }
        let id = EventSubscriptionId::new(state.next_event_subscription_id)
            .map_err(|error| StorageError::internal(error.to_string()))?;
        state.next_event_subscription_id += 1;
        let now = Utc::now();
        let subscription = StorageEventSubscription::builder(
            id,
            request.scope(),
            request.sink_id(),
            request.name(),
            now,
            now,
            ResourceRevision::INITIAL,
        )
        .description(request.description())
        .entity_types(request.entity_types().to_vec())
        .actions(request.actions().to_vec())
        .filter(request.filter().clone())
        .routing(request.routing().clone())
        .enabled(request.enabled())
        .try_build()
        .map_err(invalid_contract_value)?;
        state
            .event_subscriptions
            .insert(id.id(), subscription.clone());
        let receipt = append_memory_scoped_simple_event!(
            state,
            EntityType::EventSubscription,
            id.id(),
            Some(subscription.name()),
            subscription.scope().collection_id(),
            Action::Created,
            request.event_context(),
            format!("Event subscription '{}' created", subscription.name()),
        )?;
        Ok(StorageMutationOutcome::committed(subscription, receipt))
    }

    async fn update_event_subscription(
        &self,
        request: StorageEventSubscriptionUpdate,
    ) -> Result<StorageMutationOutcome<StorageEventSubscription>, StorageError> {
        let mut state = self.state.write().await;
        let current = state
            .event_subscriptions
            .get(&request.id().id())
            .filter(|subscription| subscription.scope() == request.scope())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!(
                    "Event subscription {} was not found in scope {:?}",
                    request.id().id(),
                    request.scope()
                ))
            })?;
        let sink_id = request.sink_id_value().unwrap_or(current.sink_id());
        if !state.event_sinks.contains_key(&sink_id.id()) {
            return Err(StorageError::not_found(format!(
                "Event sink {} was not found",
                sink_id.id()
            )));
        }
        ensure_memory_sink_allowed(&state, request.scope(), sink_id)?;
        let name = request.name_value().unwrap_or(current.name());
        let description = request.description_value().unwrap_or(current.description());
        let entity_types = request
            .entity_types_value()
            .unwrap_or(current.entity_types());
        let actions = request.actions_value().unwrap_or(current.actions());
        let filter = request.filter_value().unwrap_or(current.filter());
        let routing = request.routing_value().unwrap_or(current.routing());
        let enabled = request.enabled_value().unwrap_or(current.enabled());
        let subscription = StorageEventSubscription::builder(
            current.id(),
            current.scope(),
            sink_id,
            name,
            current.created_at(),
            Utc::now(),
            current
                .revision()
                .checked_advance()
                .map_err(|error| StorageError::internal(error.to_string()))?,
        )
        .description(description)
        .entity_types(entity_types.to_vec())
        .actions(actions.to_vec())
        .filter(filter.clone())
        .routing(routing.clone())
        .enabled(enabled)
        .try_build()
        .map_err(invalid_contract_value)?;
        state
            .event_subscriptions
            .insert(subscription.id().id(), subscription.clone());
        let receipt = append_memory_scoped_simple_event!(
            state,
            EntityType::EventSubscription,
            subscription.id().id(),
            Some(subscription.name()),
            subscription.scope().collection_id(),
            Action::Updated,
            request.event_context(),
            format!("Event subscription '{}' updated", subscription.name()),
        )?;
        Ok(StorageMutationOutcome::committed(subscription, receipt))
    }

    async fn delete_event_subscription(
        &self,
        request: StorageEventSubscriptionDelete,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        let mut state = self.state.write().await;
        let subscription = state
            .event_subscriptions
            .remove(&request.id().id())
            .filter(|subscription| subscription.scope() == request.scope())
            .ok_or_else(|| {
                StorageError::not_found(format!(
                    "Event subscription {} was not found in scope {:?}",
                    request.id().id(),
                    request.scope()
                ))
            })?;
        let delivery_ids = state
            .event_deliveries
            .values()
            .filter(|delivery| delivery.subscription_id() == subscription.id())
            .map(|delivery| delivery.id().id())
            .collect::<Vec<_>>();
        for delivery_id in delivery_ids {
            state.event_deliveries.remove(&delivery_id);
            state.event_delivery_claims.remove(&delivery_id);
        }
        let receipt = append_memory_scoped_simple_event!(
            state,
            EntityType::EventSubscription,
            subscription.id().id(),
            Some(subscription.name()),
            subscription.scope().collection_id(),
            Action::Deleted,
            request.event_context(),
            format!("Event subscription '{}' deleted", subscription.name()),
        )?;
        Ok(StorageMutationOutcome::committed((), receipt))
    }
}

#[async_trait]
impl EventDeliveryAdministrationStorage for MemoryStorage {
    async fn load_event_notification(
        &self,
        selection: StorageEventNotificationSelection,
    ) -> Result<StorageEventNotificationInput, StorageError> {
        memory_notification(&*self.state.read().await, selection)
    }
    async fn enqueue_event_notification_test(
        &self,
        selection: StorageEventNotificationSelection,
        context: EventContext,
    ) -> Result<StorageMutationOutcome<StorageEventDelivery>, StorageError> {
        let mut state = self.state.write().await;
        let input = memory_notification(&state, selection)?;
        let id = EventDeliveryId::new(state.next_event_delivery_id)
            .map_err(|e| StorageError::internal(e.to_string()))?;
        let now = Utc::now();
        let delivery = StorageEventDelivery::builder(
            id,
            input.event().id(),
            selection.subscription_id(),
            EventDeliveryStatus::Pending,
            now,
            now,
            now,
        )
        .purpose(EventDeliveryPurpose::Test)
        .try_build()
        .map_err(invalid_contract_value)?;
        let document = AuditDocument::try_new(
            "Event sink test requested",
            None,
            None,
            serde_json::json!({
                "delivery_id": id.id(),
                "subscription_id": selection.subscription_id().id(),
                "source_event_id": selection.event_id(),
                "purpose": "test",
            }),
        )
        .map_err(|error| StorageError::internal(error.to_string()))?;
        let receipt = append_memory_event!(
            state,
            EntityType::EventSink,
            selection.sink_id().id(),
            Some(input.sink().name()),
            None,
            Action::Invoked,
            &context,
            document,
            None,
            None,
        )?;
        state.next_event_delivery_id += 1;
        state.event_deliveries.insert(id.id(), delivery.clone());
        Ok(StorageMutationOutcome::committed(delivery, receipt))
    }

    async fn list_event_deliveries(
        &self,
        query: StorageEventDeliveryListQuery,
    ) -> Result<StoragePage<StorageEventDelivery>, StorageError> {
        let state = self.state.read().await;
        let rows = state
            .event_deliveries
            .values()
            .filter(|delivery| {
                query
                    .subscription_id_value()
                    .is_none_or(|id| delivery.subscription_id() == id)
            })
            .cloned()
            .collect();
        page(rows, query.options())
    }

    async fn get_event_delivery(
        &self,
        delivery_id: EventDeliveryId,
    ) -> Result<StorageEventDelivery, StorageError> {
        self.state
            .read()
            .await
            .event_deliveries
            .get(&delivery_id.id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!(
                    "Event delivery {} was not found",
                    delivery_id.id()
                ))
            })
    }

    async fn release_event_delivery_for_retry(
        &self,
        delivery_id: EventDeliveryId,
    ) -> Result<StorageEventDelivery, StorageError> {
        let mut state = self.state.write().await;
        let current = state
            .event_deliveries
            .get(&delivery_id.id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!(
                    "Event delivery {} was not found",
                    delivery_id.id()
                ))
            })?;
        let delivery = rebuild_event_delivery(
            &current,
            EventDeliveryStatus::Pending,
            current.attempts(),
            Utc::now(),
            None,
            None,
        )?;
        state.event_delivery_claims.remove(&delivery_id.id());
        state
            .event_deliveries
            .insert(delivery_id.id(), delivery.clone());
        Ok(delivery)
    }

    async fn mark_event_delivery_dead(
        &self,
        delivery_id: EventDeliveryId,
    ) -> Result<StorageEventDelivery, StorageError> {
        let mut state = self.state.write().await;
        let current = state
            .event_deliveries
            .get(&delivery_id.id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!(
                    "Event delivery {} was not found",
                    delivery_id.id()
                ))
            })?;
        if current.status() == EventDeliveryStatus::Succeeded {
            return Err(StorageError::conflict(
                "A succeeded event delivery cannot be marked dead",
            ));
        }
        let delivery = rebuild_event_delivery(
            &current,
            EventDeliveryStatus::Dead,
            current.attempts(),
            current.next_attempt_at(),
            Some("Marked dead by an administrator".to_string()),
            None,
        )?;
        state.event_delivery_claims.remove(&delivery_id.id());
        state
            .event_deliveries
            .insert(delivery_id.id(), delivery.clone());
        Ok(delivery)
    }
}

#[async_trait]
impl EventDeliveryWorkerStorage for MemoryStorage {
    async fn claim_event_delivery_batch(
        &self,
        settings: hubuum_domain::EventDeliverySettings,
    ) -> Result<StorageEventDeliveryBatch, StorageError> {
        let mut state = self.state.write().await;
        let now = Utc::now();
        let locked_until = settings
            .lock_deadline(now.naive_utc())
            .map(|value| DateTime::from_naive_utc_and_offset(value, Utc))
            .ok_or_else(|| StorageError::internal("event delivery lock deadline overflowed"))?;
        let mut candidates = state
            .event_deliveries
            .values()
            .filter(|delivery| {
                (matches!(
                    delivery.status(),
                    EventDeliveryStatus::Pending | EventDeliveryStatus::Failed
                ) && delivery.next_attempt_at() <= now
                    && delivery.attempts() < settings.max_attempts())
                    || (delivery.status() == EventDeliveryStatus::InFlight
                        && delivery
                            .locked_until()
                            .is_some_and(|deadline| deadline < now))
            })
            .cloned()
            .collect::<Vec<_>>();
        candidates.sort_by_key(|delivery| (delivery.next_attempt_at(), delivery.id().id()));
        let mut spaced_sinks = std::collections::HashSet::new();
        candidates.retain(|delivery| {
            let sink = state
                .event_subscriptions
                .get(&delivery.subscription_id().id())
                .and_then(|subscription| state.event_sinks.get(&subscription.sink_id().id()));
            sink.is_none_or(|sink| {
                sink_delivery_deadline(&state, sink).is_none_or(|deadline| deadline <= now)
                    && (sink.delivery_policy().min_interval_ms().is_none()
                        || spaced_sinks.insert(sink.id()))
            })
        });
        candidates.truncate(settings.batch_size());
        let mut work = Vec::with_capacity(candidates.len());
        for current in candidates {
            let attempts = current.attempts();
            let token = Uuid::new_v4();
            let delivery = rebuild_event_delivery(
                &current,
                EventDeliveryStatus::InFlight,
                attempts,
                current.next_attempt_at(),
                None,
                Some(locked_until),
            )?;
            let envelope = state
                .events
                .iter()
                .find_map(|recorded| {
                    let (event, _, _) = recorded.clone().into_parts();
                    (event.id() == delivery.event_id()).then_some(event)
                })
                .ok_or_else(|| StorageError::internal("event delivery event is missing"))?;
            let subscription = state
                .event_subscriptions
                .get(&delivery.subscription_id().id())
                .ok_or_else(|| StorageError::internal("event delivery subscription is missing"))?;
            let sink = state
                .event_sinks
                .get(&subscription.sink_id().id())
                .ok_or_else(|| StorageError::internal("event delivery sink is missing"))?;
            let claim = StorageEventDeliveryClaim::try_new(delivery.id(), attempts, token)
                .map_err(invalid_contract_value)?
                .with_configuration(StorageEventDeliveryConfiguration::new(
                    sink.id(),
                    sink.revision(),
                    subscription.revision(),
                ));
            let envelope = envelope.for_subscription_scope(subscription.scope());
            let delivery_subscription = StorageEventDeliverySubscription::try_new(
                subscription.id(),
                subscription.name(),
                subscription.routing().clone(),
            )
            .map_err(invalid_contract_value)?
            .for_test(current.purpose() == hubuum_domain::EventDeliveryPurpose::Test);
            let delivery_sink = StorageEventDeliverySink::try_new(
                sink.id(),
                sink.name(),
                sink.kind(),
                sink.configuration().clone(),
                sink.secret_ref().map(ToOwned::to_owned),
            )
            .map_err(invalid_contract_value)?;
            state
                .event_delivery_claims
                .insert(delivery.id().id(), token);
            state.event_deliveries.insert(delivery.id().id(), delivery);
            work.push(StorageEventDeliveryWorkItem::new(
                claim,
                envelope,
                delivery_subscription,
                delivery_sink,
            ));
        }
        let next_wakeup_in = if work.is_empty() {
            state
                .event_deliveries
                .values()
                .filter_map(|delivery| {
                    let deadline = match delivery.status() {
                        EventDeliveryStatus::Pending | EventDeliveryStatus::Failed
                            if delivery.attempts() < settings.max_attempts() =>
                        {
                            delivery.next_attempt_at()
                        }
                        EventDeliveryStatus::InFlight => delivery.locked_until()?,
                        _ => return None,
                    };
                    let sink = state
                        .event_subscriptions
                        .get(&delivery.subscription_id().id())
                        .and_then(|subscription| {
                            state.event_sinks.get(&subscription.sink_id().id())
                        })?;
                    let deadline = sink_delivery_deadline(&state, sink)
                        .map_or(deadline, |eligible| deadline.max(eligible));
                    (deadline > now).then_some(deadline)
                })
                .min()
                .and_then(|deadline| (deadline - now).to_std().ok())
        } else {
            None
        };
        Ok(StorageEventDeliveryBatch::new(work, next_wakeup_in))
    }

    async fn begin_event_delivery(
        &self,
        claim: &StorageEventDeliveryClaim,
    ) -> Result<Option<StorageEventDeliveryLease>, StorageError> {
        let check_started = Instant::now();
        let mut state = self.state.write().await;
        let Some(delivery) = state
            .event_deliveries
            .get(&claim.delivery_id().id())
            .cloned()
        else {
            return Ok(None);
        };
        if state.event_delivery_claims.get(&claim.delivery_id().id()) != Some(&claim.token())
            || delivery.status() != EventDeliveryStatus::InFlight
        {
            return Ok(None);
        }
        let now = Utc::now();
        let Some(remaining) = delivery
            .locked_until()
            .and_then(|deadline| (deadline - now).to_std().ok())
        else {
            return Ok(None);
        };
        let sink_id = state
            .event_subscriptions
            .get(&delivery.subscription_id().id())
            .ok_or_else(|| StorageError::not_found("Subscription was removed"))?
            .sink_id()
            .id();
        let subscription = state
            .event_subscriptions
            .get(&delivery.subscription_id().id())
            .ok_or_else(|| StorageError::not_found("Subscription was removed"))?;
        let sink = state
            .event_sinks
            .get(&sink_id)
            .ok_or_else(|| StorageError::not_found("Sink was removed"))?;
        let allowed = (delivery.purpose() == hubuum_domain::EventDeliveryPurpose::Test
            || (sink.enabled() && subscription.enabled()))
            && memory_sink_allowed(&state, subscription.scope(), sink.id());
        let unchanged = claim.configuration().is_some_and(|configuration| {
            configuration
                == StorageEventDeliveryConfiguration::new(
                    sink.id(),
                    sink.revision(),
                    subscription.revision(),
                )
        });
        if !allowed || !unchanged {
            let rejected = rebuild_event_delivery(
                &delivery,
                if allowed {
                    EventDeliveryStatus::Pending
                } else {
                    EventDeliveryStatus::Dead
                },
                delivery.attempts(),
                now,
                (!allowed).then(|| "Sink use revoked or destination disabled".to_string()),
                None,
            )?;
            state.event_deliveries.insert(delivery.id().id(), rejected);
            state.event_delivery_claims.remove(&delivery.id().id());
            return Ok(None);
        }
        let interval = state
            .event_sinks
            .get(&sink_id)
            .ok_or_else(|| StorageError::not_found("Sink was removed"))?
            .delivery_policy()
            .min_interval_ms()
            .unwrap_or(0);
        let (next, blocked) = state
            .event_sink_schedule
            .get(&sink_id)
            .copied()
            .unwrap_or((now, now));
        let next = if interval == 0 { now } else { next };
        if next.max(blocked) > now {
            let deferred = rebuild_event_delivery(
                &delivery,
                EventDeliveryStatus::Pending,
                delivery.attempts(),
                next.max(blocked),
                None,
                None,
            )?;
            let deferred = delivery_metadata(
                deferred,
                delivery.purpose(),
                Some(
                    if blocked > now {
                        "provider_rate"
                    } else {
                        "configured_rate"
                    }
                    .to_string(),
                ),
            )?;
            state.event_deliveries.insert(delivery.id().id(), deferred);
            state.event_delivery_claims.remove(&delivery.id().id());
            return Ok(None);
        }
        state.event_sink_schedule.insert(
            sink_id,
            (
                now + chrono::Duration::milliseconds(interval as i64),
                blocked,
            ),
        );
        let admitted = delivery_metadata(delivery.clone(), delivery.purpose(), None)?;
        state.event_deliveries.insert(delivery.id().id(), admitted);
        StorageEventDeliveryLease::try_new(claim.clone(), check_started, remaining)
            .map(Some)
            .map_err(invalid_contract_value)
    }

    async fn finish_event_delivery(
        &self,
        claim: &StorageEventDeliveryClaim,
        disposition: StorageEventDeliveryDisposition,
    ) -> Result<(), StorageError> {
        let mut state = self.state.write().await;
        let now = Utc::now();
        let current = state
            .event_deliveries
            .get(&claim.delivery_id().id())
            .cloned()
            .ok_or_else(|| StorageError::not_found("Delivery was removed"))?;
        if state.event_delivery_claims.get(&current.id().id()) != Some(&claim.token())
            || current.status() != EventDeliveryStatus::InFlight
            || current
                .locked_until()
                .is_none_or(|deadline| deadline <= now)
        {
            return Err(StorageError::conflict("Event delivery claim is stale"));
        }
        let updated = match disposition {
            StorageEventDeliveryDisposition::Permanent(error) => rebuild_event_delivery(
                &current,
                EventDeliveryStatus::Dead,
                current.attempts() + 1,
                now,
                Some(error),
                None,
            )?,
            StorageEventDeliveryDisposition::RateLimited(delay) => {
                let until = now
                    .checked_add_signed(
                        chrono::Duration::from_std(delay)
                            .map_err(|_| StorageError::invalid_input("Cooldown overflow"))?,
                    )
                    .ok_or_else(|| StorageError::invalid_input("Cooldown overflow"))?;
                let sink_id = state
                    .event_subscriptions
                    .get(&current.subscription_id().id())
                    .ok_or_else(|| StorageError::not_found("Subscription was removed"))?
                    .sink_id()
                    .id();
                let schedule = state
                    .event_sink_schedule
                    .entry(sink_id)
                    .or_insert((now, now));
                schedule.1 = schedule.1.max(until);
                let updated = rebuild_event_delivery(
                    &current,
                    EventDeliveryStatus::Pending,
                    current.attempts(),
                    until,
                    None,
                    None,
                )?;
                delivery_metadata(
                    updated,
                    current.purpose(),
                    Some("provider_rate".to_string()),
                )?
            }
        };
        state.event_deliveries.insert(current.id().id(), updated);
        state.event_delivery_claims.remove(&current.id().id());
        Ok(())
    }

    async fn mark_event_delivery_succeeded(
        &self,
        claim: &StorageEventDeliveryClaim,
    ) -> Result<(), StorageError> {
        let mut state = self.state.write().await;
        if state.event_delivery_claims.get(&claim.delivery_id().id()) != Some(&claim.token()) {
            return Err(StorageError::conflict("Event delivery claim is stale"));
        }
        let current = state
            .event_deliveries
            .get(&claim.delivery_id().id())
            .cloned()
            .ok_or_else(|| StorageError::not_found("Event delivery was not found"))?;
        if current.status() != EventDeliveryStatus::InFlight
            || current.attempts() != claim.attempts()
            || current
                .locked_until()
                .is_none_or(|deadline| deadline <= Utc::now())
        {
            return Err(StorageError::conflict("Event delivery claim is stale"));
        }
        let delivery = rebuild_event_delivery(
            &current,
            EventDeliveryStatus::Succeeded,
            current.attempts(),
            current.next_attempt_at(),
            None,
            None,
        )?;
        state
            .event_delivery_claims
            .remove(&claim.delivery_id().id());
        state
            .event_deliveries
            .insert(claim.delivery_id().id(), delivery);
        Ok(())
    }

    async fn mark_event_delivery_failed(
        &self,
        claim: &StorageEventDeliveryClaim,
        settings: hubuum_domain::EventDeliverySettings,
        error: &str,
    ) -> Result<(), StorageError> {
        let mut state = self.state.write().await;
        if state.event_delivery_claims.get(&claim.delivery_id().id()) != Some(&claim.token()) {
            return Err(StorageError::conflict("Event delivery claim is stale"));
        }
        let current = state
            .event_deliveries
            .get(&claim.delivery_id().id())
            .cloned()
            .ok_or_else(|| StorageError::not_found("Event delivery was not found"))?;
        if current.status() != EventDeliveryStatus::InFlight
            || current.attempts() != claim.attempts()
            || current
                .locked_until()
                .is_none_or(|deadline| deadline <= Utc::now())
        {
            return Err(StorageError::conflict("Event delivery claim is stale"));
        }
        let attempts = current.attempts() + 1;
        let exhausted = attempts >= settings.max_attempts();
        let status = if exhausted {
            EventDeliveryStatus::Dead
        } else {
            EventDeliveryStatus::Failed
        };
        let next_attempt_at = settings
            .retry_deadline(Utc::now().naive_utc(), attempts)
            .map(|value| DateTime::from_naive_utc_and_offset(value, Utc))
            .ok_or_else(|| StorageError::internal("event retry deadline overflowed"))?;
        let delivery = rebuild_event_delivery(
            &current,
            status,
            attempts,
            next_attempt_at,
            Some(error.to_string()),
            None,
        )?;
        state
            .event_delivery_claims
            .remove(&claim.delivery_id().id());
        state
            .event_deliveries
            .insert(claim.delivery_id().id(), delivery);
        Ok(())
    }
}

#[async_trait]
impl EventFanoutStorage for MemoryStorage {
    async fn process_event_fanout_batch(
        &self,
        settings: EventFanoutSettings,
    ) -> Result<StorageEventFanoutOutcome, StorageError> {
        let mut state = self.state.write().await;
        let events = state
            .events
            .iter()
            .filter_map(|recorded| {
                let (event, _, _) = recorded.clone().into_parts();
                (event.id().get() > state.fanout_event_cursor).then_some(event)
            })
            .take(settings.batch_size())
            .collect::<Vec<_>>();
        if events.is_empty() {
            return Ok(StorageEventFanoutOutcome::new(0, Vec::new()));
        }
        let subscriptions = state
            .event_subscriptions
            .values()
            .filter(|subscription| subscription.enabled())
            .cloned()
            .collect::<Vec<_>>();
        for event in &events {
            for subscription in &subscriptions {
                let sink_enabled = state
                    .event_sinks
                    .get(&subscription.sink_id().id())
                    .is_some_and(|sink| sink.enabled());
                let matches = sink_enabled
                    && memory_sink_allowed(&state, subscription.scope(), subscription.sink_id())
                    && subscription.scope().matches(event)
                    && subscription.filter().matches(event)
                    && subscription.entity_types().contains(&event.entity_type())
                    && subscription.actions().contains(&event.action());
                let exists = state.event_deliveries.values().any(|delivery| {
                    delivery.purpose() == EventDeliveryPurpose::Event
                        && delivery.event_id() == event.id()
                        && delivery.subscription_id() == subscription.id()
                });
                if matches && !exists {
                    let id = EventDeliveryId::new(state.next_event_delivery_id)
                        .map_err(|error| StorageError::internal(error.to_string()))?;
                    state.next_event_delivery_id += 1;
                    let now = Utc::now();
                    let delivery = StorageEventDelivery::builder(
                        id,
                        event.id(),
                        subscription.id(),
                        EventDeliveryStatus::Pending,
                        now,
                        now,
                        now,
                    )
                    .try_build()
                    .map_err(invalid_contract_value)?;
                    state.event_deliveries.insert(id.id(), delivery);
                }
            }
        }
        state.fanout_event_cursor = events
            .last()
            .map(|event| event.id().get())
            .unwrap_or(state.fanout_event_cursor);
        let trace_links = events
            .iter()
            .filter_map(|event| event.trace_link().cloned())
            .collect();
        Ok(StorageEventFanoutOutcome::new(events.len(), trace_links))
    }
}

#[async_trait]
impl EventHealthStorage for MemoryStorage {
    async fn get_event_delivery_health(
        &self,
    ) -> Result<StorageEventDeliveryHealthSnapshot, StorageError> {
        let state = self.state.read().await;
        let pending_events = state
            .events
            .iter()
            .filter(|recorded| {
                let (event, _, _) = (*recorded).clone().into_parts();
                event.id().get() > state.fanout_event_cursor
            })
            .count();
        let fanout = StorageEventFanoutSnapshot::try_new(
            i64::try_from(pending_events).unwrap_or(i64::MAX),
            0,
            0,
            (pending_events > 0).then_some(0),
        )
        .map_err(invalid_contract_value)?;
        let counts = event_status_counts(
            state
                .event_deliveries
                .values()
                .map(StorageEventDelivery::status),
        )?;
        let now = Utc::now();
        let retryable = state
            .event_deliveries
            .values()
            .filter(|delivery| {
                delivery.status() == EventDeliveryStatus::Failed
                    && delivery.next_attempt_at() <= now
            })
            .count() as i64;
        let stale_claims = state
            .event_deliveries
            .values()
            .filter(|delivery| {
                delivery.status() == EventDeliveryStatus::InFlight
                    && delivery
                        .locked_until()
                        .is_some_and(|deadline| deadline <= now)
            })
            .count() as i64;
        let oldest_due_age = state
            .event_deliveries
            .values()
            .filter(|delivery| match delivery.status() {
                EventDeliveryStatus::Pending | EventDeliveryStatus::Failed => {
                    delivery.next_attempt_at() <= now
                }
                EventDeliveryStatus::InFlight => delivery
                    .locked_until()
                    .is_some_and(|deadline| deadline <= now),
                _ => false,
            })
            .map(|delivery| delivery.created_at())
            .min()
            .map(|created| (now - created).num_seconds().max(0));
        let counts = StorageEventDeliveryStatusSnapshot::try_new(
            counts.total(),
            counts.pending(),
            counts.in_flight(),
            counts.succeeded(),
            counts.failed(),
            counts.dead(),
            retryable,
        )
        .map_err(invalid_contract_value)?;
        let delivery = StorageEventQueueSnapshot::try_new(counts, stale_claims, oldest_due_age)
            .map_err(invalid_contract_value)?;
        Ok(StorageEventDeliveryHealthSnapshot::new(
            fanout,
            delivery,
            Vec::new(),
            Vec::new(),
        ))
    }
}

#[async_trait]
impl EventRetentionStorage for MemoryStorage {
    async fn claim_event_retention_batch(
        &self,
        settings: EventRetentionSettings,
    ) -> Result<Option<StorageEventRetentionBatch>, StorageError> {
        let cutoff: DateTime<Utc> = settings
            .event_cutoff(Utc::now().naive_utc())
            .map(|value| DateTime::from_naive_utc_and_offset(value, Utc))
            .ok_or_else(|| StorageError::internal("event retention cutoff overflowed"))?;
        let mut state = self.state.write().await;
        let retained = state
            .events
            .iter()
            .filter_map(|recorded| {
                let (event, _, _) = recorded.clone().into_parts();
                (event.occurred_at() < cutoff).then_some(event)
            })
            .take(settings.batch_size())
            .map(|event| {
                let id = event.id();
                serde_json::to_string(&event)
                    .map_err(|error| StorageError::internal(error.to_string()))
                    .and_then(|json| {
                        StorageRetainedEvent::try_new(id, json).map_err(invalid_contract_value)
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        if retained.is_empty() {
            return Ok(None);
        }
        let id = StorageEventRetentionBatchId::new(Uuid::new_v4());
        state.event_retention_batches.insert(
            id.as_uuid(),
            retained.iter().map(|event| event.id().get()).collect(),
        );
        Ok(Some(StorageEventRetentionBatch::new(id, retained)))
    }

    async fn complete_event_retention_batch(
        &self,
        batch_id: StorageEventRetentionBatchId,
    ) -> Result<StorageEventRetentionSummary, StorageError> {
        let mut state = self.state.write().await;
        let Some(event_ids) = state.event_retention_batches.remove(&batch_id.as_uuid()) else {
            return Ok(StorageEventRetentionSummary::default());
        };
        let event_ids = event_ids.into_iter().collect::<BTreeSet<_>>();
        let before_events = state.events.len();
        state.events.retain(|recorded| {
            let (event, _, _) = recorded.clone().into_parts();
            !event_ids.contains(&event.id().get())
        });
        let terminal_delivery_ids = state
            .event_deliveries
            .values()
            .filter(|delivery| {
                event_ids.contains(&delivery.event_id().get())
                    && matches!(
                        delivery.status(),
                        EventDeliveryStatus::Succeeded | EventDeliveryStatus::Dead
                    )
            })
            .map(|delivery| delivery.id().id())
            .collect::<Vec<_>>();
        for delivery_id in &terminal_delivery_ids {
            state.event_deliveries.remove(delivery_id);
            state.event_delivery_claims.remove(delivery_id);
        }
        Ok(StorageEventRetentionSummary::new(
            before_events - state.events.len(),
            terminal_delivery_ids.len(),
        ))
    }
}

#[async_trait]
impl HistoryStorage for MemoryStorage {
    async fn resolve_history_principal_names(
        &self,
        principal_ids: Vec<PrincipalId>,
    ) -> Result<Vec<StorageHistoryPrincipalName>, StorageError> {
        let state = self.state.read().await;
        Ok(principal_ids
            .into_iter()
            .filter_map(|id| {
                state
                    .principals
                    .get(&id.id())
                    .map(|principal| StorageHistoryPrincipalName::new(id, principal.name()))
            })
            .collect())
    }

    async fn list_collection_history(
        &self,
        query: StorageHistoryListQuery,
    ) -> Result<StoragePage<StorageCollectionHistoryRecord>, StorageError> {
        let (entity_id, options, scope) = query.into_parts();
        let state = self.state.read().await;
        let mut entries = state
            .history
            .iter()
            .filter(|entry| {
                entry.value.entity_id() == entity_id.id()
                    && matches!(entry.value, MemoryHistoryValue::Collection(_))
                    && history_scope_allows(&scope, entry.value.collection_id())
            })
            .collect::<Vec<_>>();
        entries.reverse();
        let rows = entries
            .into_iter()
            .map(|entry| match &entry.value {
                MemoryHistoryValue::Collection(record) => {
                    StorageCollectionHistoryRecord::try_new(record.clone(), entry.metadata()?)
                        .map_err(invalid_contract_value)
                }
                _ => unreachable!("collection history filter guarantees the variant"),
            })
            .collect::<Result<Vec<_>, _>>()?;
        page(rows, &options)
    }

    async fn get_collection_history_as_of(
        &self,
        query: StorageHistoryAsOfQuery,
    ) -> Result<Option<StorageCollectionHistoryRecord>, StorageError> {
        let (entity_id, at) = query.into_parts();
        let state = self.state.read().await;
        let entry = state.history.iter().rev().find(|entry| {
            entry.value.entity_id() == entity_id.id()
                && matches!(entry.value, MemoryHistoryValue::Collection(_))
                && entry.valid_from <= at
        });
        let Some(entry) = entry else {
            return Ok(None);
        };
        if entry.operation == StorageHistoryOperation::Delete {
            return Ok(None);
        }
        match &entry.value {
            MemoryHistoryValue::Collection(record) => {
                StorageCollectionHistoryRecord::try_new(record.clone(), entry.metadata()?)
                    .map(Some)
                    .map_err(invalid_contract_value)
            }
            _ => unreachable!("collection history filter guarantees the variant"),
        }
    }

    async fn list_class_history(
        &self,
        query: StorageHistoryListQuery,
    ) -> Result<StoragePage<StorageClassHistoryRecord>, StorageError> {
        let (entity_id, options, scope) = query.into_parts();
        let state = self.state.read().await;
        let mut entries = state
            .history
            .iter()
            .filter(|entry| {
                entry.value.entity_id() == entity_id.id()
                    && matches!(entry.value, MemoryHistoryValue::Class(_))
                    && history_scope_allows(&scope, entry.value.collection_id())
            })
            .collect::<Vec<_>>();
        entries.reverse();
        let rows = entries
            .into_iter()
            .map(|entry| match &entry.value {
                MemoryHistoryValue::Class(record) => {
                    StorageClassHistoryRecord::try_new(record.clone(), entry.metadata()?)
                        .map_err(invalid_contract_value)
                }
                _ => unreachable!("class history filter guarantees the variant"),
            })
            .collect::<Result<Vec<_>, _>>()?;
        page(rows, &options)
    }

    async fn get_class_history_as_of(
        &self,
        query: StorageHistoryAsOfQuery,
    ) -> Result<Option<StorageClassHistoryRecord>, StorageError> {
        let (entity_id, at) = query.into_parts();
        let state = self.state.read().await;
        let entry = state.history.iter().rev().find(|entry| {
            entry.value.entity_id() == entity_id.id()
                && matches!(entry.value, MemoryHistoryValue::Class(_))
                && entry.valid_from <= at
        });
        let Some(entry) = entry else {
            return Ok(None);
        };
        if entry.operation == StorageHistoryOperation::Delete {
            return Ok(None);
        }
        match &entry.value {
            MemoryHistoryValue::Class(record) => {
                StorageClassHistoryRecord::try_new(record.clone(), entry.metadata()?)
                    .map(Some)
                    .map_err(invalid_contract_value)
            }
            _ => unreachable!("class history filter guarantees the variant"),
        }
    }

    async fn list_object_history(
        &self,
        query: StorageObjectHistoryListQuery,
    ) -> Result<StoragePage<StorageObjectHistoryRecord>, StorageError> {
        let (object_id, class_id, options, scope) = query.into_parts();
        let state = self.state.read().await;
        let mut entries = state
            .history
            .iter()
            .filter(|entry| match &entry.value {
                MemoryHistoryValue::Object(record) => {
                    record.id() == object_id
                        && record.class_id() == class_id
                        && history_scope_allows(&scope, record.collection_id())
                }
                _ => false,
            })
            .collect::<Vec<_>>();
        entries.reverse();
        let rows = entries
            .into_iter()
            .map(|entry| match &entry.value {
                MemoryHistoryValue::Object(record) => {
                    StorageObjectHistoryRecord::try_new(record.clone(), entry.metadata()?)
                        .map_err(invalid_contract_value)
                }
                _ => unreachable!("object history filter guarantees the variant"),
            })
            .collect::<Result<Vec<_>, _>>()?;
        page(rows, &options)
    }

    async fn get_object_history_as_of(
        &self,
        query: StorageObjectHistoryAsOfQuery,
    ) -> Result<Option<StorageObjectHistoryRecord>, StorageError> {
        let (object_id, class_id, at) = query.into_parts();
        let state = self.state.read().await;
        let entry = state.history.iter().rev().find(|entry| match &entry.value {
            MemoryHistoryValue::Object(record) => {
                record.id() == object_id && record.class_id() == class_id && entry.valid_from <= at
            }
            _ => false,
        });
        let Some(entry) = entry else {
            return Ok(None);
        };
        if entry.operation == StorageHistoryOperation::Delete {
            return Ok(None);
        }
        match &entry.value {
            MemoryHistoryValue::Object(record) => {
                StorageObjectHistoryRecord::try_new(record.clone(), entry.metadata()?)
                    .map(Some)
                    .map_err(invalid_contract_value)
            }
            _ => unreachable!("object history filter guarantees the variant"),
        }
    }

    async fn list_export_template_history(
        &self,
        query: StorageHistoryListQuery,
    ) -> Result<StoragePage<StorageExportTemplateHistoryRecord>, StorageError> {
        let (entity_id, options, scope) = query.into_parts();
        let state = self.state.read().await;
        let mut entries = state
            .history
            .iter()
            .filter(|entry| {
                entry.value.entity_id() == entity_id.id()
                    && matches!(entry.value, MemoryHistoryValue::ExportTemplate(_))
                    && history_scope_allows(&scope, entry.value.collection_id())
            })
            .collect::<Vec<_>>();
        entries.reverse();
        let rows = entries
            .into_iter()
            .map(|entry| match &entry.value {
                MemoryHistoryValue::ExportTemplate(record) => {
                    StorageExportTemplateHistoryRecord::try_new(record.clone(), entry.metadata()?)
                        .map_err(invalid_contract_value)
                }
                _ => unreachable!("export-template history filter guarantees the variant"),
            })
            .collect::<Result<Vec<_>, _>>()?;
        page(rows, &options)
    }

    async fn get_export_template_history_as_of(
        &self,
        query: StorageHistoryAsOfQuery,
    ) -> Result<Option<StorageExportTemplateHistoryRecord>, StorageError> {
        let (entity_id, at) = query.into_parts();
        let state = self.state.read().await;
        let entry = state.history.iter().rev().find(|entry| {
            entry.value.entity_id() == entity_id.id()
                && matches!(entry.value, MemoryHistoryValue::ExportTemplate(_))
                && entry.valid_from <= at
        });
        let Some(entry) = entry else {
            return Ok(None);
        };
        if entry.operation == StorageHistoryOperation::Delete {
            return Ok(None);
        }
        match &entry.value {
            MemoryHistoryValue::ExportTemplate(record) => {
                StorageExportTemplateHistoryRecord::try_new(record.clone(), entry.metadata()?)
                    .map(Some)
                    .map_err(invalid_contract_value)
            }
            _ => unreachable!("export-template history filter guarantees the variant"),
        }
    }

    async fn list_remote_target_history(
        &self,
        query: StorageHistoryListQuery,
    ) -> Result<StoragePage<StorageRemoteTargetHistoryRecord>, StorageError> {
        let (entity_id, options, scope) = query.into_parts();
        let state = self.state.read().await;
        let mut entries = state
            .history
            .iter()
            .filter(|entry| {
                entry.value.entity_id() == entity_id.id()
                    && matches!(entry.value, MemoryHistoryValue::RemoteTarget(_))
                    && history_scope_allows(&scope, entry.value.collection_id())
            })
            .collect::<Vec<_>>();
        entries.reverse();
        let rows = entries
            .into_iter()
            .map(|entry| match &entry.value {
                MemoryHistoryValue::RemoteTarget(record) => {
                    StorageRemoteTargetHistoryRecord::try_new(record.clone(), entry.metadata()?)
                        .map_err(invalid_contract_value)
                }
                _ => unreachable!("remote-target history filter guarantees the variant"),
            })
            .collect::<Result<Vec<_>, _>>()?;
        page(rows, &options)
    }

    async fn get_remote_target_history_as_of(
        &self,
        query: StorageHistoryAsOfQuery,
    ) -> Result<Option<StorageRemoteTargetHistoryRecord>, StorageError> {
        let (entity_id, at) = query.into_parts();
        let state = self.state.read().await;
        let entry = state.history.iter().rev().find(|entry| {
            entry.value.entity_id() == entity_id.id()
                && matches!(entry.value, MemoryHistoryValue::RemoteTarget(_))
                && entry.valid_from <= at
        });
        let Some(entry) = entry else {
            return Ok(None);
        };
        if entry.operation == StorageHistoryOperation::Delete {
            return Ok(None);
        }
        match &entry.value {
            MemoryHistoryValue::RemoteTarget(record) => {
                StorageRemoteTargetHistoryRecord::try_new(record.clone(), entry.metadata()?)
                    .map(Some)
                    .map_err(invalid_contract_value)
            }
            _ => unreachable!("remote-target history filter guarantees the variant"),
        }
    }
}

fn sink_delivery_deadline(state: &MemoryState, sink: &StorageEventSink) -> Option<DateTime<Utc>> {
    state
        .event_sink_schedule
        .get(&sink.id().id())
        .map(|(next, blocked)| {
            if sink.delivery_policy().min_interval_ms().is_some() {
                (*next).max(*blocked)
            } else {
                *blocked
            }
        })
}

fn delivery_metadata(
    current: StorageEventDelivery,
    purpose: hubuum_domain::EventDeliveryPurpose,
    deferred_reason: Option<String>,
) -> Result<StorageEventDelivery, StorageError> {
    StorageEventDelivery::builder(
        current.id(),
        current.event_id(),
        current.subscription_id(),
        current.status(),
        current.next_attempt_at(),
        current.created_at(),
        current.updated_at(),
    )
    .attempts(current.attempts())
    .last_error(current.last_error().map(str::to_owned))
    .locked_until(current.locked_until())
    .purpose(purpose)
    .deferred_reason(deferred_reason)
    .try_build()
    .map_err(invalid_contract_value)
}

fn memory_notification(
    state: &MemoryState,
    selection: StorageEventNotificationSelection,
) -> Result<StorageEventNotificationInput, StorageError> {
    let sink = state
        .event_sinks
        .get(&selection.sink_id().id())
        .cloned()
        .ok_or_else(|| StorageError::not_found("Event sink not found"))?;
    let subscription = state
        .event_subscriptions
        .get(&selection.subscription_id().id())
        .cloned()
        .ok_or_else(|| StorageError::not_found("Event subscription not found"))?;
    let event = state
        .events
        .iter()
        .map(|event| event.clone().into_parts().0)
        .find(|event| event.event_id() == selection.event_id())
        .ok_or_else(|| StorageError::not_found("Event not found"))?;
    StorageEventNotificationInput::try_new(sink, subscription, event)
}

fn memory_sink_allowed(
    state: &MemoryState,
    scope: EventSubscriptionScope,
    sink_id: EventSinkId,
) -> bool {
    let Some(sink) = state.event_sinks.get(&sink_id.id()) else {
        return false;
    };
    match (scope.collection_id(), sink.collection_id()) {
        (None, None) => true,
        (Some(collection), Some(owner)) => collection == owner,
        (Some(collection), None) => state
            .event_sink_grants
            .contains(&(sink_id.id(), collection.id())),
        (None, Some(_)) => false,
    }
}
fn ensure_memory_sink_allowed(
    state: &MemoryState,
    scope: EventSubscriptionScope,
    sink_id: EventSinkId,
) -> Result<(), StorageError> {
    if memory_sink_allowed(state, scope, sink_id) {
        Ok(())
    } else {
        Err(StorageError::permission_denied(
            "Sink is not available to this collection",
        ))
    }
}
