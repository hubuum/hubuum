use std::collections::BinaryHeap;
use std::ops::Bound::{Excluded, Included, Unbounded};

use super::*;

fn metadata(row: &Row<'_>) -> Result<StorageHistoryMetadata, StorageError> {
    let operation = match row.text("operation")? {
        "create" => StorageHistoryOperation::Create,
        "update" => StorageHistoryOperation::Update,
        "delete" => StorageHistoryOperation::Delete,
        _ => return Err(invalid("operation")),
    };
    Ok(StorageHistoryMetadata::try_new(
        operation,
        row.time("valid_from")?,
        row.optional_time("valid_to")?,
        HistoryRecordId::new(row.number("history_entry_id")?)
            .map_err(|_| invalid("history_entry_id"))?,
        row.revision()?,
    )
    .map_err(invalid_contract_value)?
    .actor(
        row.optional_integer("actor_principal_id")?
            .map(PrincipalId::new)
            .transpose()
            .map_err(|_| invalid("actor_principal_id"))?,
        row.optional_text("actor_kind")?,
    )
    .initiator_principal_id(
        row.optional_integer("initiator_principal_id")?
            .map(PrincipalId::new)
            .transpose()
            .map_err(|_| invalid("initiator_principal_id"))?,
    )
    .task_id(
        row.optional_integer("task_id")?
            .map(TaskId::new)
            .transpose()
            .map_err(|_| invalid("task_id"))?,
    )
    .with_logical_identity(
        serde_json::from_value(row.value("logical_identity")?.clone())
            .map_err(|_| invalid("logical_identity"))?,
    ))
}

pub(crate) fn list_relation_history(
    state: &MemoryState,
    query: &StorageRelationHistoryQuery,
) -> Result<StoragePage<StorageRelationHistoryEntry>, StorageError> {
    let (section, from_field, to_field, root) = match query.resource() {
        StorageRelationHistoryResource::Class(id) => (
            StorageBackupHistorySection::ClassRelationHistory,
            "from_class_id",
            "to_class_id",
            id.id(),
        ),
        StorageRelationHistoryResource::Object(id) => (
            StorageBackupHistorySection::ObjectRelationHistory,
            "from_object_id",
            "to_object_id",
            id.id(),
        ),
    };
    let options = query.options();
    let descending = options.sort()[0].descending;
    let key = |id: i64| if descending { -id } else { id };
    let boundary = options
        .cursor_values(options.sort())
        .map_err(|error| StorageError::invalid_input(error.to_string()))?
        .map(|values| match values[0] {
            CursorValue::Integer(id) => key(id),
            _ => unreachable!("validated query"),
        });
    let rows = state
        .relation_history
        .get(&section)
        .map(Vec::as_slice)
        .unwrap_or_default();
    let mut selected = BinaryHeap::new();
    let mut total = 0;
    for (index, value) in rows.iter().enumerate() {
        let row = Row(value);
        if row.integer(from_field)? != root && row.integer(to_field)? != root {
            continue;
        }
        if let StorageRelationHistorySelection::At(at) = query.selection()
            && (row.text("operation")? == "delete"
                || row.time("valid_from")? > at
                || row.optional_time("valid_to")?.is_some_and(|end| at >= end))
        {
            continue;
        }
        total += 1;
        let key = key(row.number("history_entry_id")?);
        if boundary.is_some_and(|boundary| key <= boundary) {
            continue;
        }
        selected.push((key, index));
        if selected.len() > options.limit().expect("validated limit") {
            selected.pop();
        }
    }
    let records = selected
        .into_sorted_vec()
        .into_iter()
        .map(|(_, index)| {
            let row = Row(&rows[index]);
            let relation = match query.resource() {
                StorageRelationHistoryResource::Class(_) => {
                    StorageHistoricalRelation::Class(resources::class_relation(&row)?)
                }
                StorageRelationHistoryResource::Object(_) => {
                    StorageHistoricalRelation::Object(resources::object_relation(&row)?)
                }
            };
            StorageRelationHistoryRecord::try_new(relation, metadata(&row)?)
                .map_err(invalid_contract_value)
        })
        .collect::<Result<Vec<_>, _>>()?;

    // One scan of retained endpoint history, with only this bounded page's references
    // in memory. Repeated root endpoints share the same lookup and timestamp.
    let mut requests =
        BTreeMap::<StorageRelationHistoryResource, BTreeMap<DateTime<Utc>, Vec<usize>>>::new();
    for (index, record) in records.iter().enumerate() {
        let at = match query.selection() {
            StorageRelationHistorySelection::Changes => record.metadata().valid_from(),
            StorageRelationHistorySelection::At(at) => at,
        };
        for (side, endpoint) in record.relation().endpoints().into_iter().enumerate() {
            requests
                .entry(endpoint)
                .or_default()
                .entry(at)
                .or_default()
                .push(2 * index + side);
        }
    }
    let mut endpoints: Vec<Option<&MemoryHistoryEntry>> = vec![None; 2 * records.len()];
    for entry in &state.history {
        if entry.operation == StorageHistoryOperation::Delete {
            continue;
        }
        let resource = match &entry.value {
            MemoryHistoryValue::Class(row) => StorageRelationHistoryResource::Class(row.id()),
            MemoryHistoryValue::Object(row) => StorageRelationHistoryResource::Object(row.id()),
            _ => continue,
        };
        let Some(times) = requests.get(&resource) else {
            continue;
        };
        for slots in times
            .range((
                Included(entry.valid_from),
                entry.valid_to.map_or(Unbounded, Excluded),
            ))
            .map(|(_, slots)| slots)
        {
            for &slot in slots {
                let captured = records[slot / 2].endpoint_identities()[slot % 2];
                let actual = entry
                    .logical_identity
                    .subject()
                    .and_then(|subject| subject.record_identity())
                    .map(|identity| identity.resource());
                if captured.is_none() || captured != actual {
                    continue;
                }
                if endpoints[slot].is_none_or(|previous| previous.id < entry.id) {
                    endpoints[slot] = Some(entry);
                }
            }
        }
    }
    let rows = records
        .into_iter()
        .enumerate()
        .map(|(index, record)| {
            StorageRelationHistoryEntry::try_new(
                record,
                query.selection(),
                endpoints[2 * index].map(endpoint_snapshot).transpose()?,
                endpoints[2 * index + 1]
                    .map(endpoint_snapshot)
                    .transpose()?,
            )
            .map_err(invalid_contract_value)
        })
        .collect::<Result<Vec<_>, _>>()?;
    StoragePage::try_new(rows, options.include_total().then_some(total))
        .map_err(invalid_contract_value)
}

fn endpoint_snapshot(
    entry: &MemoryHistoryEntry,
) -> Result<StorageHistoryEndpointSnapshot, StorageError> {
    let (resource, collection, name) = match &entry.value {
        MemoryHistoryValue::Class(row) => (
            StorageHistoryResource::Class(row.id()),
            row.collection_id(),
            row.name(),
        ),
        MemoryHistoryValue::Object(row) => (
            StorageHistoryResource::Object {
                class_id: row.class_id(),
                object_id: row.id(),
            },
            row.collection_id(),
            row.name(),
        ),
        _ => unreachable!("selected endpoint"),
    };
    StorageHistoryEndpointSnapshot::try_new(
        resource,
        collection,
        name.to_owned(),
        entry.metadata()?,
    )
    .map_err(invalid_contract_value)
}
