use super::*;

impl MemoryState {
    pub(crate) fn record_computed_history(
        &mut self,
        class_id: ClassId,
    ) -> Result<(), StorageError> {
        let definitions = self
            .computed_fields
            .values()
            .filter(|definition| {
                definition.class_id() == class_id
                    && definition.visibility() == StorageComputedFieldVisibility::Shared
            })
            .map(StorageHistoricalComputedField::from_shared)
            .collect::<Result<Vec<_>, _>>()
            .map_err(invalid_contract_value)?;
        let exists = self.classes.contains_key(&class_id.id());
        let previous = self
            .computed_history
            .iter_mut()
            .rev()
            .find(|record| record.class_id() == class_id && record.valid_to().is_none());
        let mut boundary = DateTime::from_timestamp_micros(Utc::now().timestamp_micros())
            .expect("current time fits microsecond precision");
        if let Some(previous) = previous {
            if exists
                && previous.definitions() == definitions
                && previous.evaluator_version() == HISTORICAL_COMPUTED_EVALUATOR_VERSION
            {
                return Ok(());
            }
            boundary = boundary.max(previous.valid_from() + Duration::microseconds(1));
            previous
                .close_at(boundary)
                .map_err(invalid_contract_value)?;
        }
        if exists {
            let id = HistoryRecordId::new(self.next_history_id)
                .map_err(|error| StorageError::internal(error.to_string()))?;
            let record = StorageComputedFieldHistory::builder(class_id, id, boundary)
                .logical_identity(Some(
                    self.logical_identities
                        .computed_history(class_id, &definitions),
                ))
                .definitions(definitions)
                .build()
                .map_err(invalid_contract_value)?;
            self.next_history_id += 1;
            self.computed_history.push(record);
        }
        Ok(())
    }
}
