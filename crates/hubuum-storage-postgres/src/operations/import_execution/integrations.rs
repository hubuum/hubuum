use super::{
    ImportReferenceChange, ImportRuntime, assert_upsert_condition, ensure_class_collection,
    import_timestamp_pair, imported_timestamps, resolve_class, resolve_collection,
    with_imported_timestamp_override,
};
use crate::{PostgresConnection, PostgresRevision, PostgresStorageError};
use chrono::NaiveDateTime;
use diesel::prelude::{ExpressionMethods, OptionalExtension, QueryDsl};
use diesel_async::RunQueryDsl;
use hubuum_storage_core::{
    StorageErrorKind, StorageImportEventSink, StorageImportEventSinkKey,
    StorageImportEventSubscription, StorageImportExportTemplate, StorageImportRemoteTarget,
    StorageRemoteTargetSubjectType,
};
use hubuum_templates::{TemplateAutoEscape, TemplateLimits, validate_template_composition};

const IMPORT_TEMPLATE_FUEL: u64 = 50_000;
const IMPORT_TEMPLATE_RECURSION_LIMIT: usize = 64;

pub(super) async fn upsert_export_template(
    connection: &mut PostgresConnection,
    state: &ImportRuntime,
    input: StorageImportExportTemplate,
    overwrite: bool,
) -> Result<(), PostgresStorageError> {
    let parts = input.into_parts();
    let collection = resolve_collection(
        connection,
        state,
        parts.collection_ref.as_deref(),
        parts.collection_key.as_ref(),
    )
    .await?;
    let class_id = if parts.class_ref.is_some() || parts.class_key.is_some() {
        let class = resolve_class(
            connection,
            state,
            parts.class_ref.as_deref(),
            parts.class_key.as_ref(),
        )
        .await?;
        ensure_class_collection(&class, &collection, "Export template")?;
        Some(class.id().id())
    } else {
        None
    };
    validate_import_template_composition(
        connection,
        state,
        collection.id().id(),
        &parts.name,
        &parts.template,
        &parts.content_type,
    )
    .await?;
    let existing = crate::schema::export_templates::table
        .filter(crate::schema::export_templates::collection_id.eq(collection.id().id()))
        .filter(crate::schema::export_templates::name.eq(&parts.name))
        .select((
            crate::schema::export_templates::id,
            crate::schema::export_templates::created_at,
            crate::schema::export_templates::updated_at,
            crate::schema::export_templates::revision,
        ))
        .for_update()
        .first::<(i32, NaiveDateTime, NaiveDateTime, PostgresRevision)>(connection)
        .await
        .optional()?;
    assert_upsert_condition(existing.map(|row| row.3), parts.condition)?;
    if existing.is_some() && !overwrite {
        return Err(PostgresStorageError::conflict(format!(
            "Export template '{}' already exists in the collection",
            parts.name
        )));
    }
    match existing {
        Some((id, old_created_at, old_updated_at, _)) => {
            let (created_at, updated_at) = parts
                .timestamps
                .map(import_timestamp_pair)
                .unwrap_or((old_created_at, old_updated_at));
            with_imported_timestamp_override(connection, async |connection| {
                diesel::update(
                    crate::schema::export_templates::table
                        .filter(crate::schema::export_templates::id.eq(id)),
                )
                .set((
                    crate::schema::export_templates::description.eq(parts.description),
                    crate::schema::export_templates::content_type.eq(parts.content_type),
                    crate::schema::export_templates::template.eq(parts.template),
                    crate::schema::export_templates::kind.eq(parts.kind),
                    crate::schema::export_templates::scope_kind.eq(parts.scope_kind),
                    crate::schema::export_templates::class_id.eq(class_id),
                    crate::schema::export_templates::default_query.eq(parts.default_query),
                    crate::schema::export_templates::include.eq(parts.include),
                    crate::schema::export_templates::relation_context.eq(parts.relation_context),
                    crate::schema::export_templates::default_missing_data_policy
                        .eq(parts.default_missing_data_policy),
                    crate::schema::export_templates::default_limits.eq(parts.default_limits),
                    crate::schema::export_templates::created_at.eq(created_at),
                    crate::schema::export_templates::updated_at.eq(updated_at),
                ))
                .execute(connection)
                .await?;
                Ok(())
            })
            .await?;
        }
        None => {
            let (created_at, updated_at) = imported_timestamps(parts.timestamps);
            diesel::insert_into(crate::schema::export_templates::table)
                .values((
                    crate::schema::export_templates::collection_id.eq(collection.id().id()),
                    crate::schema::export_templates::name.eq(parts.name),
                    crate::schema::export_templates::description.eq(parts.description),
                    crate::schema::export_templates::content_type.eq(parts.content_type),
                    crate::schema::export_templates::template.eq(parts.template),
                    crate::schema::export_templates::kind.eq(parts.kind),
                    crate::schema::export_templates::scope_kind.eq(parts.scope_kind),
                    crate::schema::export_templates::class_id.eq(class_id),
                    crate::schema::export_templates::default_query.eq(parts.default_query),
                    crate::schema::export_templates::include.eq(parts.include),
                    crate::schema::export_templates::relation_context.eq(parts.relation_context),
                    crate::schema::export_templates::default_missing_data_policy
                        .eq(parts.default_missing_data_policy),
                    crate::schema::export_templates::default_limits.eq(parts.default_limits),
                    crate::schema::export_templates::created_at.eq(created_at),
                    crate::schema::export_templates::updated_at.eq(updated_at),
                ))
                .execute(connection)
                .await?;
        }
    }
    Ok(())
}

async fn validate_import_template_composition(
    connection: &mut PostgresConnection,
    state: &ImportRuntime,
    collection_id: i32,
    name: &str,
    template: &str,
    content_type: &str,
) -> Result<(), PostgresStorageError> {
    let mut sources = crate::schema::export_templates::table
        .filter(crate::schema::export_templates::collection_id.eq(collection_id))
        .order(crate::schema::export_templates::id.asc())
        .select((
            crate::schema::export_templates::name,
            crate::schema::export_templates::template,
        ))
        .load::<(String, String)>(connection)
        .await?;
    for candidate in &state.export_templates {
        let candidate = candidate.clone().into_parts();
        let resolved = resolve_collection(
            connection,
            state,
            candidate.collection_ref.as_deref(),
            candidate.collection_key.as_ref(),
        )
        .await;
        match resolved {
            Ok(collection) if collection.id().id() == collection_id => {
                sources.push((candidate.name, candidate.template));
            }
            Ok(_) => {}
            Err(error)
                if matches!(
                    error.kind(),
                    StorageErrorKind::InvalidInput | StorageErrorKind::NotFound
                ) => {}
            Err(error) => return Err(error),
        }
    }
    let context = serde_json::json!({
        "items": [],
        "meta": {
            "count": 0,
            "truncated": false,
            "scope": { "kind": "objects_in_class", "class_id": 0, "object_id": 0 },
            "content_type": content_type,
        },
        "warnings": [],
        "request": {
            "scope": { "kind": "objects_in_class", "class_id": 0, "object_id": 0 },
            "query": "",
        },
        "source": {
            "id": 0,
            "name": "",
            "description": "",
            "collection_id": 0,
            "hubuum_class_id": 0,
            "data": {},
            "path": [],
            "path_objects": [],
            "related": {},
            "reachable": {},
            "paths": {},
        },
    });
    let auto_escape = if content_type == "text/html" {
        TemplateAutoEscape::Html
    } else {
        TemplateAutoEscape::None
    };
    validate_template_composition(
        name,
        template,
        &sources,
        &context,
        auto_escape,
        TemplateLimits::new(IMPORT_TEMPLATE_RECURSION_LIMIT, IMPORT_TEMPLATE_FUEL),
    )
    .await
    .map_err(|error| {
        PostgresStorageError::invalid_input(format!(
            "Invalid export template composition '{name}': {error}"
        ))
    })
}

pub(super) async fn upsert_remote_target(
    connection: &mut PostgresConnection,
    state: &ImportRuntime,
    input: StorageImportRemoteTarget,
    overwrite: bool,
) -> Result<(), PostgresStorageError> {
    let parts = input.into_parts();
    let collection = resolve_collection(
        connection,
        state,
        parts.collection_ref.as_deref(),
        parts.collection_key.as_ref(),
    )
    .await?;
    let class_id = if parts.class_ref.is_some() || parts.class_key.is_some() {
        let class = resolve_class(
            connection,
            state,
            parts.class_ref.as_deref(),
            parts.class_key.as_ref(),
        )
        .await?;
        ensure_class_collection(&class, &collection, "Remote target")?;
        Some(class.id().id())
    } else {
        None
    };
    let existing = crate::schema::remote_targets::table
        .filter(crate::schema::remote_targets::collection_id.eq(collection.id().id()))
        .filter(crate::schema::remote_targets::name.eq(&parts.name))
        .select((
            crate::schema::remote_targets::id,
            crate::schema::remote_targets::created_at,
            crate::schema::remote_targets::updated_at,
            crate::schema::remote_targets::revision,
        ))
        .for_update()
        .first::<(i32, NaiveDateTime, NaiveDateTime, PostgresRevision)>(connection)
        .await
        .optional()?;
    assert_upsert_condition(existing.map(|row| row.3), parts.condition)?;
    if existing.is_some() && !overwrite {
        return Err(PostgresStorageError::conflict(format!(
            "Remote target '{}' already exists in the collection",
            parts.name
        )));
    }
    let allowed_subject_types = serde_json::to_value(
        parts
            .allowed_subject_types
            .into_iter()
            .map(StorageRemoteTargetSubjectType::as_str)
            .collect::<Vec<_>>(),
    )
    .map_err(|error| PostgresStorageError::invalid_input(error.to_string()))?;
    match existing {
        Some((id, old_created_at, old_updated_at, _)) => {
            let (created_at, updated_at) = parts
                .timestamps
                .map(import_timestamp_pair)
                .unwrap_or((old_created_at, old_updated_at));
            with_imported_timestamp_override(connection, async |connection| {
                diesel::update(
                    crate::schema::remote_targets::table
                        .filter(crate::schema::remote_targets::id.eq(id)),
                )
                .set((
                    crate::schema::remote_targets::class_id.eq(class_id),
                    crate::schema::remote_targets::description.eq(parts.description),
                    crate::schema::remote_targets::method.eq(parts.method.as_str()),
                    crate::schema::remote_targets::url_template.eq(parts.url_template),
                    crate::schema::remote_targets::headers_template.eq(parts.headers_template),
                    crate::schema::remote_targets::body_template.eq(parts.body_template),
                    crate::schema::remote_targets::auth_config.eq(parts.auth_config),
                    crate::schema::remote_targets::allowed_subject_types.eq(allowed_subject_types),
                    crate::schema::remote_targets::timeout_ms.eq(parts.timeout_ms),
                    crate::schema::remote_targets::enabled.eq(parts.enabled),
                    crate::schema::remote_targets::created_at.eq(created_at),
                    crate::schema::remote_targets::updated_at.eq(updated_at),
                ))
                .execute(connection)
                .await?;
                Ok(())
            })
            .await?;
        }
        None => {
            let (created_at, updated_at) = imported_timestamps(parts.timestamps);
            diesel::insert_into(crate::schema::remote_targets::table)
                .values((
                    crate::schema::remote_targets::collection_id.eq(collection.id().id()),
                    crate::schema::remote_targets::class_id.eq(class_id),
                    crate::schema::remote_targets::name.eq(parts.name),
                    crate::schema::remote_targets::description.eq(parts.description),
                    crate::schema::remote_targets::method.eq(parts.method.as_str()),
                    crate::schema::remote_targets::url_template.eq(parts.url_template),
                    crate::schema::remote_targets::headers_template.eq(parts.headers_template),
                    crate::schema::remote_targets::body_template.eq(parts.body_template),
                    crate::schema::remote_targets::auth_config.eq(parts.auth_config),
                    crate::schema::remote_targets::allowed_subject_types.eq(allowed_subject_types),
                    crate::schema::remote_targets::timeout_ms.eq(parts.timeout_ms),
                    crate::schema::remote_targets::enabled.eq(parts.enabled),
                    crate::schema::remote_targets::created_at.eq(created_at),
                    crate::schema::remote_targets::updated_at.eq(updated_at),
                ))
                .execute(connection)
                .await?;
        }
    }
    Ok(())
}

pub(super) async fn execute_event_sink(
    connection: &mut PostgresConnection,
    input: StorageImportEventSink,
    overwrite: bool,
) -> Result<Option<ImportReferenceChange>, PostgresStorageError> {
    let parts = input.into_parts();
    let existing = crate::schema::event_sinks::table
        .filter(crate::schema::event_sinks::name.eq(&parts.name))
        .select((
            crate::schema::event_sinks::id,
            crate::schema::event_sinks::created_at,
            crate::schema::event_sinks::updated_at,
            crate::schema::event_sinks::revision,
            crate::schema::event_sinks::collection_id,
        ))
        .for_update()
        .first::<(
            i32,
            NaiveDateTime,
            NaiveDateTime,
            PostgresRevision,
            Option<i32>,
        )>(connection)
        .await
        .optional()?;
    if existing.is_some_and(|row| row.4.is_some()) {
        return Err(PostgresStorageError::invalid_input(
            "Global integration imports cannot replace collection-owned sinks",
        ));
    }
    assert_upsert_condition(existing.map(|row| row.3), parts.condition)?;
    if existing.is_some() && !overwrite {
        return Err(PostgresStorageError::conflict(format!(
            "Event sink '{}' already exists",
            parts.name
        )));
    }
    let sink_id = match existing {
        Some((id, old_created_at, old_updated_at, _, _)) => {
            let (created_at, updated_at) = parts
                .timestamps
                .map(import_timestamp_pair)
                .unwrap_or((old_created_at, old_updated_at));
            with_imported_timestamp_override(connection, async |connection| {
                diesel::update(
                    crate::schema::event_sinks::table.filter(crate::schema::event_sinks::id.eq(id)),
                )
                .set((
                    crate::schema::event_sinks::kind.eq(parts.kind),
                    crate::schema::event_sinks::config.eq(parts.config),
                    crate::schema::event_sinks::delivery_policy
                        .eq(serde_json::to_value(parts.delivery_policy)
                            .map_err(|e| PostgresStorageError::invalid_input(e.to_string()))?),
                    crate::schema::event_sinks::secret_ref.eq(parts.secret_ref),
                    crate::schema::event_sinks::enabled.eq(parts.enabled),
                    crate::schema::event_sinks::created_at.eq(created_at),
                    crate::schema::event_sinks::updated_at.eq(updated_at),
                ))
                .execute(connection)
                .await?;
                Ok(id)
            })
            .await?
        }
        None => {
            let (created_at, updated_at) = imported_timestamps(parts.timestamps);
            diesel::insert_into(crate::schema::event_sinks::table)
                .values((
                    crate::schema::event_sinks::name.eq(parts.name),
                    crate::schema::event_sinks::kind.eq(parts.kind),
                    crate::schema::event_sinks::config.eq(parts.config),
                    crate::schema::event_sinks::delivery_policy
                        .eq(serde_json::to_value(parts.delivery_policy)
                            .map_err(|e| PostgresStorageError::invalid_input(e.to_string()))?),
                    crate::schema::event_sinks::secret_ref.eq(parts.secret_ref),
                    crate::schema::event_sinks::enabled.eq(parts.enabled),
                    crate::schema::event_sinks::created_at.eq(created_at),
                    crate::schema::event_sinks::updated_at.eq(updated_at),
                ))
                .returning(crate::schema::event_sinks::id)
                .get_result::<i32>(connection)
                .await?
        }
    };
    Ok(parts
        .reference
        .map(|reference| ImportReferenceChange::EventSink(reference, sink_id)))
}

pub(super) async fn upsert_event_subscription(
    connection: &mut PostgresConnection,
    state: &ImportRuntime,
    input: StorageImportEventSubscription,
    overwrite: bool,
) -> Result<(), PostgresStorageError> {
    let parts = input.into_parts();
    let collection = resolve_collection(
        connection,
        state,
        parts.collection_ref.as_deref(),
        parts.collection_key.as_ref(),
    )
    .await?;
    let sink_id = resolve_event_sink(
        connection,
        state,
        parts.sink_ref.as_deref(),
        parts.sink_key.as_ref(),
    )
    .await?;
    // Integration imports are restricted to unscoped administrators. Their
    // subscription declaration also provisions the direct collection grant.
    diesel::insert_into(crate::schema::event_sink_collection_grants::table)
        .values((
            crate::schema::event_sink_collection_grants::sink_id.eq(sink_id),
            crate::schema::event_sink_collection_grants::collection_id.eq(collection.id().id()),
        ))
        .on_conflict_do_nothing()
        .execute(connection)
        .await?;
    validate_event_subscription(
        &parts.entity_types,
        &parts.actions,
        &parts.filter,
        &parts.routing,
    )?;
    let existing = crate::schema::event_subscriptions::table
        .filter(crate::schema::event_subscriptions::collection_id.eq(collection.id().id()))
        .filter(crate::schema::event_subscriptions::name.eq(&parts.name))
        .select((
            crate::schema::event_subscriptions::id,
            crate::schema::event_subscriptions::created_at,
            crate::schema::event_subscriptions::updated_at,
            crate::schema::event_subscriptions::revision,
        ))
        .for_update()
        .first::<(i32, NaiveDateTime, NaiveDateTime, PostgresRevision)>(connection)
        .await
        .optional()?;
    assert_upsert_condition(existing.map(|row| row.3), parts.condition)?;
    if existing.is_some() && !overwrite {
        return Err(PostgresStorageError::conflict(format!(
            "Event subscription '{}' already exists in the collection",
            parts.name
        )));
    }
    let entity_types = serde_json::to_value(parts.entity_types)
        .map_err(|error| PostgresStorageError::invalid_input(error.to_string()))?;
    let actions = serde_json::to_value(parts.actions)
        .map_err(|error| PostgresStorageError::invalid_input(error.to_string()))?;
    match existing {
        Some((id, old_created_at, old_updated_at, _)) => {
            let (created_at, updated_at) = parts
                .timestamps
                .map(import_timestamp_pair)
                .unwrap_or((old_created_at, old_updated_at));
            with_imported_timestamp_override(connection, async |connection| {
                diesel::update(
                    crate::schema::event_subscriptions::table
                        .filter(crate::schema::event_subscriptions::id.eq(id)),
                )
                .set((
                    crate::schema::event_subscriptions::sink_id.eq(sink_id),
                    crate::schema::event_subscriptions::description.eq(parts.description),
                    crate::schema::event_subscriptions::entity_types.eq(entity_types),
                    crate::schema::event_subscriptions::actions.eq(actions),
                    crate::schema::event_subscriptions::filter.eq(parts.filter),
                    crate::schema::event_subscriptions::routing.eq(parts.routing),
                    crate::schema::event_subscriptions::enabled.eq(parts.enabled),
                    crate::schema::event_subscriptions::created_at.eq(created_at),
                    crate::schema::event_subscriptions::updated_at.eq(updated_at),
                ))
                .execute(connection)
                .await?;
                Ok(())
            })
            .await?;
        }
        None => {
            let (created_at, updated_at) = imported_timestamps(parts.timestamps);
            diesel::insert_into(crate::schema::event_subscriptions::table)
                .values((
                    crate::schema::event_subscriptions::collection_id.eq(collection.id().id()),
                    crate::schema::event_subscriptions::sink_id.eq(sink_id),
                    crate::schema::event_subscriptions::name.eq(parts.name),
                    crate::schema::event_subscriptions::description.eq(parts.description),
                    crate::schema::event_subscriptions::entity_types.eq(entity_types),
                    crate::schema::event_subscriptions::actions.eq(actions),
                    crate::schema::event_subscriptions::filter.eq(parts.filter),
                    crate::schema::event_subscriptions::routing.eq(parts.routing),
                    crate::schema::event_subscriptions::enabled.eq(parts.enabled),
                    crate::schema::event_subscriptions::created_at.eq(created_at),
                    crate::schema::event_subscriptions::updated_at.eq(updated_at),
                ))
                .execute(connection)
                .await?;
        }
    }
    Ok(())
}

async fn resolve_event_sink(
    connection: &mut PostgresConnection,
    state: &ImportRuntime,
    reference: Option<&str>,
    key: Option<&StorageImportEventSinkKey>,
) -> Result<i32, PostgresStorageError> {
    if let Some(reference) = reference
        && let Some(id) = state.event_sinks_by_ref.get(reference)
    {
        return Ok(*id);
    }
    let name = key
        .cloned()
        .map(StorageImportEventSinkKey::into_parts)
        .map(|parts| parts.name)
        .ok_or_else(|| {
            PostgresStorageError::invalid_input(
                "Event-sink reference was not resolved and no sink_key was supplied",
            )
        })?;
    crate::schema::event_sinks::table
        .filter(crate::schema::event_sinks::name.eq(name))
        .filter(crate::schema::event_sinks::collection_id.is_null())
        .select(crate::schema::event_sinks::id)
        .first::<i32>(connection)
        .await
        .optional()?
        .ok_or_else(|| PostgresStorageError::not_found("Event sink was not found"))
}

fn validate_event_subscription(
    entity_types: &[String],
    actions: &[String],
    filter: &serde_json::Value,
    routing: &serde_json::Value,
) -> Result<(), PostgresStorageError> {
    if entity_types.is_empty() {
        return Err(PostgresStorageError::invalid_input(
            "Event subscription entity_types must not be empty",
        ));
    }
    if actions.is_empty() {
        return Err(PostgresStorageError::invalid_input(
            "Event subscription actions must not be empty",
        ));
    }
    serde_json::from_value::<hubuum_events_core::EventSubscriptionFilter>(filter.clone()).map_err(
        |error| {
            PostgresStorageError::invalid_input(format!(
                "Invalid event subscription filter: {error}"
            ))
        },
    )?;
    if !routing.is_object() {
        return Err(PostgresStorageError::invalid_input(
            "Event subscription routing must be an object",
        ));
    }
    Ok(())
}
