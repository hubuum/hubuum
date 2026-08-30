use super::*;

#[async_trait]
impl InventoryStorage for MemoryStorage {
    async fn get_inventory_counts(&self) -> Result<StorageInventoryCounts, StorageError> {
        let state = self.state.read().await;
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
        let state = self.state.read().await;
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
        let ages = StorageTaskKind::ALL
            .into_iter()
            .map(|kind| StorageTaskGaugeAge::new(kind, None, None))
            .collect();
        StorageTaskGaugeSnapshot::try_new(Vec::new(), ages, Vec::new())
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
            self.state.read().await.maintenance_state,
        ))
    }

    async fn get_maintenance_state(&self) -> Result<MaintenanceState, StorageError> {
        Ok(self.state.read().await.maintenance_state)
    }

    async fn get_task_queue_snapshot(
        &self,
    ) -> Result<StorageOperationalTaskQueueSnapshot, StorageError> {
        StorageOperationalTaskQueueSnapshot::try_new(
            StorageOperationalTaskStatusCounts::try_new(
                0,
                StorageOperationalTaskActiveCounts::try_new(0, 0, 0)
                    .map_err(invalid_contract_value)?,
                StorageOperationalTaskTerminalCounts::try_new(0, 0, 0, 0)
                    .map_err(invalid_contract_value)?,
            )
            .map_err(invalid_contract_value)?,
            StorageOperationalTaskKindCounts::try_new(0, 0, 0).map_err(invalid_contract_value)?,
            0,
            0,
            None,
            None,
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

#[async_trait]
impl TokenRetentionStorage for MemoryStorage {
    async fn purge_expired_tokens(
        &self,
        _settings: TokenRetentionSettings,
    ) -> Result<usize, StorageError> {
        Ok(0)
    }
}
