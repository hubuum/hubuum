use super::*;

#[async_trait]
impl HistoryStorage for MemoryStorage {
    async fn get_computed_field_history(
        &self,
        query: StorageComputedFieldHistoryQuery,
    ) -> Result<Option<StorageHistoricalComputedFields>, StorageError> {
        let state = self.state.read().await?;
        state
            .computed_history
            .iter()
            .rev()
            .find(|record| {
                record.class_id() == query.class_id() && record.is_effective_at(query.at())
            })
            .cloned()
            .map(|record| record.select(query).map_err(invalid_contract_value))
            .transpose()
    }

    async fn list_relation_history(
        &self,
        query: StorageRelationHistoryQuery,
    ) -> Result<StoragePage<StorageRelationHistoryEntry>, StorageError> {
        let state = self.state.read().await?;
        crate::backup::list_relation_history(&state, &query)
    }

    async fn list_history_markers(
        &self,
        query: StorageHistoryNavigationQuery,
    ) -> Result<StoragePage<StorageHistoryMarker>, StorageError> {
        let state = self.state.read().await?;
        let options = query.options();
        let mut entries = state
            .history
            .iter()
            .filter(|entry| {
                history_matches_resource(entry, query.resource())
                    && history_scope_allows(query.scope(), entry.value.collection_id())
                    && query.time_window().contains(entry.valid_from)
                    && query
                        .selected()
                        .contains(StorageHistoryPosition::new(entry.valid_from, entry.id))
            })
            .collect::<Vec<_>>();
        entries.sort_by_key(|entry| (entry.valid_from, entry.id.id()));
        if options.sort()[0].descending {
            entries.reverse();
        }
        let total = options.include_total().then_some(entries.len() as i64);
        if let Some(values) = options
            .cursor_values(options.sort())
            .map_err(|error| StorageError::invalid_input(error.to_string()))?
        {
            let [CursorValue::DateTime(at), CursorValue::Integer(id)] = values.as_ref() else {
                return Err(StorageError::invalid_input("invalid history cursor values"));
            };
            entries.retain(|entry| {
                let key = (entry.valid_from.naive_utc(), entry.id.id());
                if options.sort()[0].descending {
                    key < (*at, *id)
                } else {
                    key > (*at, *id)
                }
            });
        }
        entries.truncate(options.limit().expect("bounded navigation query"));
        let rows = entries
            .into_iter()
            .map(|entry| {
                let name = match &entry.value {
                    MemoryHistoryValue::Class(record) => record.name(),
                    MemoryHistoryValue::Object(record) => record.name(),
                    _ => unreachable!("resource matched"),
                };
                Ok(StorageHistoryMarker::new(
                    query.resource(),
                    entry.value.collection_id(),
                    name.to_owned(),
                    entry.metadata()?,
                ))
            })
            .collect::<Result<Vec<_>, StorageError>>()?;
        StoragePage::try_new(rows, total).map_err(invalid_contract_value)
    }

    async fn get_history_entry(
        &self,
        query: StorageHistoryEntryQuery,
    ) -> Result<Option<StorageHistoryEntry>, StorageError> {
        let state = self.state.read().await?;
        state
            .history
            .iter()
            .find(|entry| {
                entry.id == query.history_id() && history_matches_resource(entry, query.resource())
            })
            .map(|entry| match &entry.value {
                MemoryHistoryValue::Class(record) => {
                    StorageClassHistoryRecord::try_new(record.clone(), entry.metadata()?)
                        .map(StorageHistoryEntry::Class)
                        .map_err(invalid_contract_value)
                }
                MemoryHistoryValue::Object(record) => {
                    StorageObjectHistoryRecord::try_new(record.clone(), entry.metadata()?)
                        .map(StorageHistoryEntry::Object)
                        .map_err(invalid_contract_value)
                }
                _ => unreachable!("resource matched"),
            })
            .transpose()
    }

    async fn resolve_history_principal_names(
        &self,
        principal_ids: Vec<PrincipalIdentity>,
    ) -> Result<Vec<StorageHistoryPrincipalName>, StorageError> {
        let state = self.state.read().await?;
        let requested = principal_ids.into_iter().collect::<BTreeSet<_>>();
        let mut names = Vec::new();
        for principal in state.principals.values() {
            let evidence = state
                .logical_identities
                .record(StorageIdentityTarget::Principal(principal.id()))?;
            if let Some(StorageIdentitySubject::Principal {
                identity: IdentityReference::Known { identity },
            }) = evidence.subject()
                && requested.contains(&identity)
            {
                names.push(StorageHistoryPrincipalName::new(identity, principal.name()));
            }
        }
        names.sort_by_key(StorageHistoryPrincipalName::identity);
        Ok(names)
    }

    async fn list_collection_history(
        &self,
        query: StorageHistoryListQuery,
    ) -> Result<StoragePage<StorageCollectionHistoryRecord>, StorageError> {
        let (entity_id, options, scope) = query.into_parts();
        let state = self.state.read().await?;
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
        let state = self.state.read().await?;
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
        let state = self.state.read().await?;
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
        let state = self.state.read().await?;
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
        let state = self.state.read().await?;
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

    async fn list_object_history_by_name(
        &self,
        query: StorageObjectHistoryNameQuery,
    ) -> Result<StoragePage<StorageObjectHistoryRecord>, StorageError> {
        use crate::object_filters::ObjectFilters;
        use std::collections::BinaryHeap;

        let options = query.options();
        let filters = ObjectFilters::new(options.filters())?;
        let sorts = options.sort();
        let boundary = options
            .cursor_values(sorts)
            .map_err(|error| StorageError::invalid_input(error.to_string()))?
            .map(|values| {
                values
                    .iter()
                    .zip(sorts.iter())
                    .map(|(value, sort)| {
                        let CursorValue::Integer(value) = value else {
                            return Err(StorageError::invalid_input(
                                "History cursor requires integer values",
                            ));
                        };
                        Ok(if sort.descending {
                            -i128::from(*value)
                        } else {
                            i128::from(*value)
                        })
                    })
                    .collect::<Result<Vec<_>, StorageError>>()
            })
            .transpose()?;
        let state = self.state.read().await?;
        let limit = options.limit().unwrap_or(usize::MAX);
        let mut selected = BinaryHeap::new();
        let mut total = 0;
        for (index, entry) in state.history.iter().enumerate() {
            let MemoryHistoryValue::Object(record) = &entry.value else {
                continue;
            };
            if record.name() != query.name()
                || record.class_id() != query.class_id()
                || !filters.matches(record)
            {
                continue;
            }
            let visible = match state.objects.get(&record.id().id()) {
                Some(live) => {
                    history_scope_allows(query.collection_scope(), live.collection_id())
                        && history_scope_allows(query.collection_scope(), record.collection_id())
                }
                None => query.include_deleted(),
            };
            if !visible {
                continue;
            }
            total += 1;
            let key = sorts
                .iter()
                .map(|sort| {
                    let value = if sort.field == FilterField::Revision {
                        record.revision().get()
                    } else {
                        entry.id.id()
                    };
                    if sort.descending {
                        -i128::from(value)
                    } else {
                        i128::from(value)
                    }
                })
                .collect::<Vec<_>>();
            if boundary.as_ref().is_some_and(|boundary| key <= *boundary) {
                continue;
            }
            selected.push((key, index));
            if selected.len() > limit {
                selected.pop();
            }
        }
        let rows = selected
            .into_sorted_vec()
            .into_iter()
            .map(|(_, index)| {
                let entry = &state.history[index];
                let MemoryHistoryValue::Object(record) = &entry.value else {
                    unreachable!()
                };
                StorageObjectHistoryRecord::try_new(record.clone(), entry.metadata()?)
                    .map_err(invalid_contract_value)
            })
            .collect::<Result<Vec<_>, _>>()?;
        StoragePage::try_new(rows, options.include_total().then_some(total))
            .map_err(invalid_contract_value)
    }

    async fn list_class_objects_as_of(
        &self,
        query: StorageClassObjectsAsOfQuery,
    ) -> Result<StoragePage<StorageObjectHistoryRecord>, StorageError> {
        use crate::object_filters::ObjectFilters;
        use std::collections::BinaryHeap;

        let options = query.options();
        let filters = ObjectFilters::new(options.filters())?;
        let sorts = options.sort();
        let boundary = options
            .cursor_values(sorts)
            .map_err(|error| StorageError::invalid_input(error.to_string()))?
            .map(|values| {
                values
                    .iter()
                    .zip(sorts.iter())
                    .map(|(value, sort)| {
                        let CursorValue::Integer(value) = value else {
                            return Err(StorageError::invalid_input(
                                "History cursor requires integer values",
                            ));
                        };
                        Ok(if sort.descending {
                            -i128::from(*value)
                        } else {
                            i128::from(*value)
                        })
                    })
                    .collect::<Result<Vec<_>, StorageError>>()
            })
            .transpose()?;
        let state = self.state.read().await?;
        let limit = options.limit().unwrap_or(usize::MAX);
        let mut selected = BinaryHeap::new();
        let mut total = 0;
        for (index, entry) in state.history.iter().enumerate() {
            let MemoryHistoryValue::Object(record) = &entry.value else {
                continue;
            };
            if entry.operation == StorageHistoryOperation::Delete
                || entry.valid_from > query.at()
                || entry.valid_to.is_some_and(|end| end <= query.at())
                || record.class_id() != query.class_id()
                || !filters.matches(record)
            {
                continue;
            }
            let visible = match state.objects.get(&record.id().id()) {
                Some(live) => {
                    history_scope_allows(query.collection_scope(), live.collection_id())
                        && history_scope_allows(query.collection_scope(), record.collection_id())
                }
                None => query.include_deleted(),
            };
            if !visible {
                continue;
            }
            total += 1;
            let key = sorts
                .iter()
                .map(|sort| {
                    let value = if sort.field == FilterField::Revision {
                        record.revision().get()
                    } else {
                        entry.id.id()
                    };
                    if sort.descending {
                        -i128::from(value)
                    } else {
                        i128::from(value)
                    }
                })
                .collect::<Vec<_>>();
            if boundary.as_ref().is_some_and(|boundary| key <= *boundary) {
                continue;
            }
            selected.push((key, index));
            if selected.len() > limit {
                selected.pop();
            }
        }
        let rows = selected
            .into_sorted_vec()
            .into_iter()
            .map(|(_, index)| {
                let entry = &state.history[index];
                let MemoryHistoryValue::Object(record) = &entry.value else {
                    unreachable!()
                };
                StorageObjectHistoryRecord::try_new(record.clone(), entry.metadata()?)
                    .map_err(invalid_contract_value)
            })
            .collect::<Result<Vec<_>, _>>()?;
        StoragePage::try_new(rows, options.include_total().then_some(total))
            .map_err(invalid_contract_value)
    }

    async fn get_object_history_as_of(
        &self,
        query: StorageObjectHistoryAsOfQuery,
    ) -> Result<Option<StorageObjectHistoryRecord>, StorageError> {
        let (object_id, class_id, at) = query.into_parts();
        let state = self.state.read().await?;
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
        let state = self.state.read().await?;
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
        let state = self.state.read().await?;
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
        let state = self.state.read().await?;
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
        let state = self.state.read().await?;
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

fn history_matches_resource(entry: &MemoryHistoryEntry, resource: StorageHistoryResource) -> bool {
    match (&entry.value, resource) {
        (MemoryHistoryValue::Class(record), StorageHistoryResource::Class(id)) => record.id() == id,
        (
            MemoryHistoryValue::Object(record),
            StorageHistoryResource::Object {
                class_id,
                object_id,
            },
        ) => record.id() == object_id && record.class_id() == class_id,
        _ => false,
    }
}
