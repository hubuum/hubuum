use super::*;

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
        page(
            self.state
                .read()
                .await
                .event_sinks
                .values()
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
        .configuration(request.configuration().clone())
        .secret_ref(request.secret_ref().map(ToOwned::to_owned))
        .enabled(request.enabled())
        .try_build()
        .map_err(invalid_contract_value)?;
        state.event_sinks.insert(id.id(), sink.clone());
        let receipt = state.append_simple_event(
            EntityType::EventSink,
            id.id(),
            Some(sink.name()),
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
        .configuration(configuration)
        .secret_ref(secret_ref)
        .enabled(enabled)
        .try_build()
        .map_err(invalid_contract_value)?;
        state.event_sinks.insert(sink.id().id(), sink.clone());
        let receipt = state.append_simple_event(
            EntityType::EventSink,
            sink.id().id(),
            Some(sink.name()),
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
        let receipt = state.append_simple_event(
            EntityType::EventSink,
            sink.id().id(),
            Some(sink.name()),
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
            .filter(|subscription| subscription.collection_id() == query.collection_id())
            .cloned()
            .collect();
        page(rows, query.options())
    }

    async fn get_event_subscription(
        &self,
        collection_id: CollectionId,
        subscription_id: EventSubscriptionId,
    ) -> Result<StorageEventSubscription, StorageError> {
        self.state
            .read()
            .await
            .event_subscriptions
            .get(&subscription_id.id())
            .filter(|subscription| subscription.collection_id() == collection_id)
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!(
                    "Event subscription {} was not found in collection {}",
                    subscription_id.id(),
                    collection_id.id()
                ))
            })
    }

    async fn create_event_subscription(
        &self,
        request: StorageEventSubscriptionCreate,
    ) -> Result<StorageMutationOutcome<StorageEventSubscription>, StorageError> {
        let mut state = self.state.write().await;
        if !state
            .collections
            .contains_key(&request.collection_id().id())
        {
            return Err(StorageError::not_found(format!(
                "Collection {} was not found",
                request.collection_id().id()
            )));
        }
        if !state.event_sinks.contains_key(&request.sink_id().id()) {
            return Err(StorageError::not_found(format!(
                "Event sink {} was not found",
                request.sink_id().id()
            )));
        }
        if state.event_subscriptions.values().any(|subscription| {
            subscription.collection_id() == request.collection_id()
                && subscription.name() == request.name()
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
            request.collection_id(),
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
            subscription.collection_id(),
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
            .filter(|subscription| subscription.collection_id() == request.collection_id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!(
                    "Event subscription {} was not found in collection {}",
                    request.id().id(),
                    request.collection_id().id()
                ))
            })?;
        let sink_id = request.sink_id_value().unwrap_or(current.sink_id());
        if !state.event_sinks.contains_key(&sink_id.id()) {
            return Err(StorageError::not_found(format!(
                "Event sink {} was not found",
                sink_id.id()
            )));
        }
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
            current.collection_id(),
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
            subscription.collection_id(),
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
            .filter(|subscription| subscription.collection_id() == request.collection_id())
            .ok_or_else(|| {
                StorageError::not_found(format!(
                    "Event subscription {} was not found in collection {}",
                    request.id().id(),
                    request.collection_id().id()
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
            subscription.collection_id(),
            Action::Deleted,
            request.event_context(),
            format!("Event subscription '{}' deleted", subscription.name()),
        )?;
        Ok(StorageMutationOutcome::committed((), receipt))
    }
}

#[async_trait]
impl EventDeliveryAdministrationStorage for MemoryStorage {
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
        let candidates = state
            .event_deliveries
            .values()
            .filter(|delivery| {
                matches!(
                    delivery.status(),
                    EventDeliveryStatus::Pending | EventDeliveryStatus::Failed
                ) && delivery.next_attempt_at() <= now
                    && delivery.attempts() < settings.max_attempts()
            })
            .take(settings.batch_size())
            .cloned()
            .collect::<Vec<_>>();
        let mut work = Vec::with_capacity(candidates.len());
        for current in candidates {
            let attempts = current.attempts().saturating_add(1);
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
                .map_err(invalid_contract_value)?;
            let delivery_subscription = StorageEventDeliverySubscription::try_new(
                subscription.id(),
                subscription.name(),
                subscription.routing().clone(),
            )
            .map_err(invalid_contract_value)?;
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
        Ok(StorageEventDeliveryBatch::new(work, None))
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
        {
            return Err(StorageError::conflict("Event delivery claim is stale"));
        }
        let exhausted = current.attempts() >= settings.max_attempts();
        let status = if exhausted {
            EventDeliveryStatus::Dead
        } else {
            EventDeliveryStatus::Failed
        };
        let next_attempt_at = settings
            .retry_deadline(Utc::now().naive_utc(), current.attempts())
            .map(|value| DateTime::from_naive_utc_and_offset(value, Utc))
            .ok_or_else(|| StorageError::internal("event retry deadline overflowed"))?;
        let delivery = rebuild_event_delivery(
            &current,
            status,
            current.attempts(),
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
    ) -> Result<usize, StorageError> {
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
            return Ok(0);
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
                    && event.collection_id() == Some(subscription.collection_id())
                    && subscription.entity_types().contains(&event.entity_type())
                    && subscription.actions().contains(&event.action());
                let exists = state.event_deliveries.values().any(|delivery| {
                    delivery.event_id() == event.id()
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
        Ok(events.len())
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
        let due = counts.pending() + counts.retryable();
        let delivery = StorageEventQueueSnapshot::try_new(counts, 0, (due > 0).then_some(0))
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
                MemoryHistoryValue::Collection(record) => StorageCollectionHistoryRecord::try_new(
                    record.clone(),
                    entry.metadata(history_valid_to(&state, entry))?,
                )
                .map_err(invalid_contract_value),
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
            MemoryHistoryValue::Collection(record) => StorageCollectionHistoryRecord::try_new(
                record.clone(),
                entry.metadata(history_valid_to(&state, entry))?,
            )
            .map(Some)
            .map_err(invalid_contract_value),
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
                MemoryHistoryValue::Class(record) => StorageClassHistoryRecord::try_new(
                    record.clone(),
                    entry.metadata(history_valid_to(&state, entry))?,
                )
                .map_err(invalid_contract_value),
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
            MemoryHistoryValue::Class(record) => StorageClassHistoryRecord::try_new(
                record.clone(),
                entry.metadata(history_valid_to(&state, entry))?,
            )
            .map(Some)
            .map_err(invalid_contract_value),
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
                MemoryHistoryValue::Object(record) => StorageObjectHistoryRecord::try_new(
                    record.clone(),
                    entry.metadata(history_valid_to(&state, entry))?,
                )
                .map_err(invalid_contract_value),
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
            MemoryHistoryValue::Object(record) => StorageObjectHistoryRecord::try_new(
                record.clone(),
                entry.metadata(history_valid_to(&state, entry))?,
            )
            .map(Some)
            .map_err(invalid_contract_value),
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
                    StorageExportTemplateHistoryRecord::try_new(
                        record.clone(),
                        entry.metadata(history_valid_to(&state, entry))?,
                    )
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
                StorageExportTemplateHistoryRecord::try_new(
                    record.clone(),
                    entry.metadata(history_valid_to(&state, entry))?,
                )
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
                    StorageRemoteTargetHistoryRecord::try_new(
                        record.clone(),
                        entry.metadata(history_valid_to(&state, entry))?,
                    )
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
            MemoryHistoryValue::RemoteTarget(record) => StorageRemoteTargetHistoryRecord::try_new(
                record.clone(),
                entry.metadata(history_valid_to(&state, entry))?,
            )
            .map(Some)
            .map_err(invalid_contract_value),
            _ => unreachable!("remote-target history filter guarantees the variant"),
        }
    }
}
