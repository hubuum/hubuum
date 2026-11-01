use super::*;

#[async_trait]
impl InventoryStorage for MemoryStorage {
    async fn get_inventory_counts(&self) -> Result<StorageInventoryCounts, StorageError> {
        let state = self.state.read().await?;
        let mut objects_by_class = BTreeMap::<ClassId, i64>::new();
        for object in state.objects.values() {
            *objects_by_class.entry(object.class_id()).or_default() += 1;
        }
        let objects_by_class = objects_by_class
            .into_iter()
            .map(|(class_id, count)| StorageObjectCountByClass::try_new(class_id, count))
            .collect::<Result<Vec<_>, _>>()
            .map_err(invalid_contract_value)?;
        StorageInventoryCounts::try_new(
            i64::try_from(state.objects.len())
                .map_err(|_| StorageError::internal("object count does not fit i64"))?,
            i64::try_from(state.classes.len())
                .map_err(|_| StorageError::internal("class count does not fit i64"))?,
            i64::try_from(state.collections.len())
                .map_err(|_| StorageError::internal("collection count does not fit i64"))?,
            objects_by_class,
        )
        .map_err(invalid_contract_value)
    }
}

#[async_trait]
impl MetricsStorage for MemoryStorage {
    async fn get_inventory_metrics_snapshot(
        &self,
    ) -> Result<StorageInventoryGaugeSnapshot, StorageError> {
        let counts = self.get_inventory_counts().await?;
        let state = self.state.read().await?;
        StorageInventoryGaugeSnapshot::try_new(
            StorageInventoryMetricsSnapshot::try_new(
                counts.total_collections(),
                counts.total_classes(),
                counts.total_objects(),
                i64::try_from(state.users.len())
                    .map_err(|_| StorageError::internal("user count does not fit i64"))?,
                i64::try_from(state.groups.len())
                    .map_err(|_| StorageError::internal("group count does not fit i64"))?,
                i64::try_from(state.service_accounts.len()).map_err(|_| {
                    StorageError::internal("service-account count does not fit i64")
                })?,
                0,
            )
            .map_err(invalid_contract_value)?,
            Vec::new(),
        )
        .map_err(invalid_contract_value)
    }

    async fn get_task_metrics_snapshot(&self) -> Result<StorageTaskGaugeSnapshot, StorageError> {
        let state = self.state.read().await?;
        let mut counts = Vec::new();
        let mut ages = Vec::new();
        let mut last_terminal = Vec::new();
        for kind in StorageTaskKind::ALL {
            let tasks = state
                .tasks
                .values()
                .filter(|task| task.kind == kind)
                .collect::<Vec<_>>();
            for status in StorageTaskStatus::ALL {
                let count = task_count(tasks.iter().filter(|task| task.status == status).count())?;
                if count == 0 {
                    continue;
                }
                counts.push(
                    StorageTaskGaugeCount::try_new(kind, status, count)
                        .map_err(invalid_contract_value)?,
                );
                if status.is_terminal() {
                    let finished_at = tasks
                        .iter()
                        .filter(|task| task.status == status)
                        .filter_map(|task| task.finished_at)
                        .max()
                        .ok_or_else(|| {
                            StorageError::internal("Terminal task has no finish timestamp")
                        })?;
                    last_terminal.push(
                        StorageTaskGaugeLastTerminal::try_new(kind, status, finished_at)
                            .map_err(invalid_contract_value)?,
                    );
                }
            }
            ages.push(StorageTaskGaugeAge::new(
                kind,
                tasks
                    .iter()
                    .filter(|task| task.status == StorageTaskStatus::Queued)
                    .map(|task| task.created_at)
                    .min(),
                tasks
                    .iter()
                    .filter(|task| task.status.is_active())
                    .filter_map(|task| task.started_at)
                    .min(),
            ));
        }
        StorageTaskGaugeSnapshot::try_new(counts, ages, last_terminal)
            .map_err(invalid_contract_value)
    }

    async fn get_event_metrics_snapshot(
        &self,
    ) -> Result<StorageEventMetricsSnapshot, StorageError> {
        Ok(StorageEventMetricsSnapshot::new(
            empty_event_fanout_snapshot()?,
            empty_event_queue_snapshot()?,
        ))
    }
}

#[async_trait]
impl OperationalStateStorage for MemoryStorage {
    async fn get_readiness_snapshot(&self) -> Result<StorageReadinessSnapshot, StorageError> {
        Ok(StorageReadinessSnapshot::new(
            true,
            self.get_maintenance_state().await?,
        ))
    }

    async fn get_maintenance_state(&self) -> Result<MaintenanceState, StorageError> {
        let state = self.state.read_control().await;
        Ok(
            if state
                .application_control
                .is_released_for(&state.deletion_control)
            {
                state.maintenance_state
            } else {
                MaintenanceState::RecoveryQuarantine
            },
        )
    }

    async fn get_task_queue_snapshot(
        &self,
    ) -> Result<StorageOperationalTaskQueueSnapshot, StorageError> {
        let state = self.state.read().await?;
        let status_count = |status| {
            task_count(
                state
                    .tasks
                    .values()
                    .filter(|task| task.status == status)
                    .count(),
            )
        };
        let kind_count = |kind| {
            task_count(
                state
                    .tasks
                    .values()
                    .filter(|task| task.kind == kind)
                    .count(),
            )
        };
        StorageOperationalTaskQueueSnapshot::try_new(
            StorageOperationalTaskStatusCounts::try_new(
                task_count(state.tasks.len())?,
                StorageOperationalTaskActiveCounts::try_new(
                    status_count(StorageTaskStatus::Queued)?,
                    status_count(StorageTaskStatus::Validating)?,
                    status_count(StorageTaskStatus::Running)?,
                    status_count(StorageTaskStatus::Waiting)?,
                )
                .map_err(invalid_contract_value)?,
                StorageOperationalTaskTerminalCounts::try_new(
                    status_count(StorageTaskStatus::Succeeded)?,
                    status_count(StorageTaskStatus::Failed)?,
                    status_count(StorageTaskStatus::PartiallySucceeded)?,
                    status_count(StorageTaskStatus::Cancelled)?,
                )
                .map_err(invalid_contract_value)?,
            )
            .map_err(invalid_contract_value)?,
            StorageOperationalTaskKindCounts::try_new(
                kind_count(StorageTaskKind::Import)?,
                kind_count(StorageTaskKind::Export)?,
                kind_count(StorageTaskKind::Reindex)?,
            )
            .map_err(invalid_contract_value)?,
            task_count(state.task_events.values().flatten().count())?,
            task_count(state.import_task_results.values().flatten().count())?,
            state
                .tasks
                .values()
                .filter(|task| task.status == StorageTaskStatus::Queued)
                .map(|task| task.created_at)
                .min(),
            state
                .tasks
                .values()
                .filter(|task| task.status.is_active())
                .filter_map(|task| task.started_at)
                .min(),
        )
        .map_err(invalid_contract_value)
    }

    async fn load_export_template_health(
        &self,
    ) -> Result<Vec<StorageOperationalExportTemplateHealth>, StorageError> {
        Ok(Vec::new())
    }

    async fn load_export_templates_for_audit(
        &self,
    ) -> Result<Vec<StorageOperationalExportTemplateAuditEntry>, StorageError> {
        Ok(Vec::new())
    }
}

fn task_count(count: usize) -> Result<i64, StorageError> {
    i64::try_from(count).map_err(|_| StorageError::internal("Task count does not fit i64"))
}

#[async_trait]
impl TokenRetentionStorage for MemoryStorage {
    async fn purge_expired_tokens(
        &self,
        _settings: TokenRetentionSettings,
    ) -> Result<usize, StorageError> {
        Ok(0)
    }
}
