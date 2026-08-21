//! PostgreSQL implementation of computed-field definition and rebuild lifecycle.

use std::time::Instant;

use chrono::{NaiveDateTime, Utc};
use diesel::prelude::{ExpressionMethods, OptionalExtension, QueryDsl};
use diesel::sql_types::{BigInt, Integer, Text, Timestamp};
use diesel::{Insertable, Queryable, QueryableByName, Selectable, SelectableHelper};
use diesel_async::RunQueryDsl;
use hubuum_computed_fields::{
    Definition, FieldKey, MAX_PERSONAL_DEFINITIONS, MAX_SHARED_DEFINITIONS, Operation, ResultType,
    SEMANTICS_VERSION,
};
use hubuum_domain::{ClassId, PrincipalId, TaskId};
use hubuum_events_core::{Action, EntityType, EventContext, MutationProvenance, NewEvent};
use hubuum_query::{FilterField, QueryOptions, SortParam};
use hubuum_storage_core::{
    MutationOutcome, StorageClassComputationState, StorageComputationRebuildStatus,
    StorageComputationRevision, StorageComputedFieldDefinition,
    StorageComputedFieldDefinitionInput, StorageComputedFieldDefinitionPatch,
    StorageComputedFieldMutation, StorageComputedFieldRebuildRequest, StoragePage,
    StoragePersonalComputedFieldCreate, StoragePersonalComputedFieldDelete,
    StoragePersonalComputedFieldListQuery, StoragePersonalComputedFieldUpdate,
    StorageSharedComputedFieldCreate, StorageSharedComputedFieldDelete,
    StorageSharedComputedFieldUpdate, StorageTask, StorageTaskCompletion, StorageTaskEventInput,
    StorageTaskKind, StorageTaskLease, StorageTaskResultCounts, StorageTaskStateUpdate,
    StorageTaskStatus,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::cursor::{CursorSqlField, CursorSqlType};
use crate::operations::class::ClassRow;
use crate::operations::computed_definition::{
    ComputedDefinitionRow, PERSONAL_VISIBILITY, SHARED_VISIBILITY,
};
use crate::operations::computed_materialization::{
    ObjectMaterializationInput, acquire_computed_class_exclusive_lock,
    acquire_computed_class_shared_lock, rebuild_objects,
};
use crate::operations::event_record::append_event;
use crate::operations::object::ObjectRow;
use crate::operations::task_execution;
use crate::operations::task_rows::TaskRow;
use crate::revision::RevisionOwner;
use crate::runtime::assert_locked_revision_precondition;
use crate::worker_notifications::notify_task_queue;
use crate::{PostgresConnection, PostgresRuntime, PostgresStorageError};

const REINDEX_PAYLOAD_TYPE: &str = "computed_fields";
const DATABASE_UTC_NOW_QUERY: &str = "SELECT clock_timestamp() AT TIME ZONE 'UTC' AS now";

#[derive(Clone, Queryable, Selectable)]
#[diesel(table_name = crate::schema::class_computation_state)]
struct ComputationStateRow {
    class_id: i32,
    evaluation_revision: i64,
    rebuild_status: String,
    active_task_id: Option<i32>,
    last_error: Option<String>,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
}

impl ComputationStateRow {
    fn ready_without_definitions(class_id: i32) -> Self {
        let now = Utc::now().naive_utc();
        Self {
            class_id,
            evaluation_revision: 0,
            rebuild_status: "ready".to_string(),
            active_task_id: None,
            last_error: None,
            created_at: now,
            updated_at: now,
        }
    }

    fn into_storage(self) -> Result<StorageClassComputationState, PostgresStorageError> {
        let rebuild_status = self
            .rebuild_status
            .parse::<StorageComputationRebuildStatus>()
            .map_err(|error| {
                PostgresStorageError::invalid_persisted_value("computation rebuild status", error)
            })?;
        StorageClassComputationState::builder(
            ClassId::new(self.class_id)?,
            StorageComputationRevision::new(self.evaluation_revision)?,
            rebuild_status,
            self.created_at.and_utc(),
            self.updated_at.and_utc(),
        )
        .active_task(self.active_task_id.map(TaskId::new).transpose()?)
        .last_error(self.last_error)
        .try_build()
        .map_err(|error| PostgresStorageError::invalid_persisted_value("computation state", error))
    }
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::computed_field_definitions)]
struct NewComputedDefinitionRow {
    class_id: i32,
    visibility: String,
    owner_user_id: Option<i32>,
    key: String,
    label: String,
    description: String,
    operation: Value,
    result_type: String,
    enabled: bool,
    semantics_version: i16,
    created_by: Option<i32>,
    updated_by: Option<i32>,
}

impl NewComputedDefinitionRow {
    fn shared(
        class_id: i32,
        actor_id: i32,
        input: StorageComputedFieldDefinitionInput,
    ) -> Result<Self, PostgresStorageError> {
        validate_positive("class", class_id)?;
        validate_positive("actor", actor_id)?;
        validate_definition(&input)?;
        Ok(Self::new(
            class_id,
            SHARED_VISIBILITY,
            None,
            actor_id,
            input,
        ))
    }

    fn personal(
        class_id: i32,
        owner_id: i32,
        input: StorageComputedFieldDefinitionInput,
    ) -> Result<Self, PostgresStorageError> {
        validate_positive("class", class_id)?;
        validate_positive("owner", owner_id)?;
        validate_definition(&input)?;
        Ok(Self::new(
            class_id,
            PERSONAL_VISIBILITY,
            Some(owner_id),
            owner_id,
            input,
        ))
    }

    fn new(
        class_id: i32,
        visibility: &str,
        owner_user_id: Option<i32>,
        actor_id: i32,
        input: StorageComputedFieldDefinitionInput,
    ) -> Self {
        Self {
            class_id,
            visibility: visibility.to_string(),
            owner_user_id,
            key: input.key().to_string(),
            label: input.label().to_string(),
            description: input.description().to_string(),
            operation: input.operation().clone(),
            result_type: input.result_type().to_string(),
            enabled: input.enabled(),
            semantics_version: SEMANTICS_VERSION,
            created_by: Some(actor_id),
            updated_by: Some(actor_id),
        }
    }
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::tasks)]
struct NewInternalTaskRow {
    kind: String,
    status: String,
    submitted_by: Option<i32>,
    idempotency_key: Option<String>,
    request_hash: Option<String>,
    request_payload: Option<Value>,
    summary: Option<String>,
    total_items: i32,
    processed_items: i32,
    success_items: i32,
    failed_items: i32,
    submitted_token_id: Option<i32>,
    submitted_token_scoped: bool,
    submitted_token_scopes: Value,
    request_redacted_at: Option<NaiveDateTime>,
    started_at: Option<NaiveDateTime>,
    finished_at: Option<NaiveDateTime>,
    initiator_user_id: Option<i32>,
}

#[derive(QueryableByName)]
struct ReturnedTaskId {
    #[diesel(sql_type = Integer)]
    id: i32,
}

#[derive(QueryableByName)]
struct ObjectBoundary {
    #[diesel(sql_type = Integer)]
    upper_bound: i32,
    #[diesel(sql_type = BigInt)]
    total_items: i64,
}

#[derive(QueryableByName)]
struct DatabaseTimeRow {
    #[diesel(sql_type = Timestamp)]
    now: NaiveDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ComputedReindexPayload {
    #[serde(rename = "type")]
    payload_type: String,
    class_id: i32,
    target_revision: i64,
    object_upper_bound: i32,
}

impl ComputedReindexPayload {
    fn new(class_id: i32, target_revision: i64, object_upper_bound: i32) -> Self {
        Self {
            payload_type: REINDEX_PAYLOAD_TYPE.to_string(),
            class_id,
            target_revision,
            object_upper_bound,
        }
    }

    fn validate(&self) -> Result<(), PostgresStorageError> {
        if self.payload_type != REINDEX_PAYLOAD_TYPE
            || self.class_id <= 0
            || self.target_revision < 0
            || self.object_upper_bound < 0
        {
            return Err(PostgresStorageError::invalid_input(
                "Computed-field reindex task payload is invalid",
            ));
        }
        Ok(())
    }
}

struct ValidatedDefinitionPatch {
    key: String,
    label: String,
    description: String,
    operation: Value,
    result_type: String,
    enabled: bool,
    value_affecting: bool,
}

enum ReindexBatch {
    Superseded,
    Rows {
        last_id: i32,
        count: i32,
        error_codes: Vec<Vec<&'static str>>,
    },
    Complete,
}

pub async fn get_computed_field_state(
    runtime: &PostgresRuntime,
    class_id: i32,
) -> Result<StorageClassComputationState, PostgresStorageError> {
    validate_positive("class", class_id)?;
    let state = runtime
        .with_connection(async move |connection| {
            use crate::schema::class_computation_state::dsl as state;
            state::class_computation_state
                .filter(state::class_id.eq(class_id))
                .select(ComputationStateRow::as_select())
                .first::<ComputationStateRow>(connection)
                .await
                .optional()
        })
        .await?
        .unwrap_or_else(|| ComputationStateRow::ready_without_definitions(class_id));
    state.into_storage()
}

pub async fn list_shared_computed_fields(
    runtime: &PostgresRuntime,
    class_id: i32,
) -> Result<Vec<StorageComputedFieldDefinition>, PostgresStorageError> {
    validate_positive("class", class_id)?;
    runtime
        .with_connection(async move |connection| {
            use crate::schema::computed_field_definitions::dsl as definitions;
            definitions::computed_field_definitions
                .filter(definitions::class_id.eq(class_id))
                .filter(definitions::visibility.eq(SHARED_VISIBILITY))
                .order(definitions::id.asc())
                .select(ComputedDefinitionRow::as_select())
                .load::<ComputedDefinitionRow>(connection)
                .await
        })
        .await?
        .into_iter()
        .map(ComputedDefinitionRow::into_storage)
        .collect()
}

pub async fn list_personal_computed_fields(
    runtime: &PostgresRuntime,
    query: StoragePersonalComputedFieldListQuery,
) -> Result<StoragePage<StorageComputedFieldDefinition>, PostgresStorageError> {
    let (owner_id, class_id, mut options) = query.into_parts();
    let owner_id = owner_id.id();
    let class_id = class_id.map(|id| id.id());
    normalize_list_options(&mut options);
    let count_options = options.clone();
    let total = if options.include_total() {
        Some(
            runtime
                .with_connection(
                    async move |connection| -> Result<i64, PostgresStorageError> {
                        apply_personal_definition_filters(
                            personal_definition_query(owner_id, class_id),
                            &count_options,
                        )?
                        .count()
                        .get_result::<i64>(connection)
                        .await
                        .map_err(PostgresStorageError::from)
                    },
                )
                .await?,
        )
    } else {
        None
    };
    let definitions = runtime
        .with_connection(
            async move |connection| -> Result<Vec<ComputedDefinitionRow>, PostgresStorageError> {
                let mut storage_query = apply_personal_definition_filters(
                    personal_definition_query(owner_id, class_id),
                    &options,
                )?;
                let fields = computed_cursor_fields(&options)?;
                crate::apply_query_options_with_fields!(storage_query, options, fields);
                Ok(storage_query
                    .select(ComputedDefinitionRow::as_select())
                    .load::<ComputedDefinitionRow>(connection)
                    .await?)
            },
        )
        .await?
        .into_iter()
        .map(ComputedDefinitionRow::into_storage)
        .collect::<Result<Vec<_>, _>>()?;
    StoragePage::try_new(definitions, total).map_err(PostgresStorageError::from)
}

pub async fn get_computed_field(
    runtime: &PostgresRuntime,
    definition_id: i32,
) -> Result<StorageComputedFieldDefinition, PostgresStorageError> {
    validate_positive("computed-field definition", definition_id)?;
    let definition = runtime
        .with_connection(async move |connection| {
            use crate::schema::computed_field_definitions::dsl as definitions;
            definitions::computed_field_definitions
                .filter(definitions::id.eq(definition_id))
                .select(ComputedDefinitionRow::as_select())
                .first::<ComputedDefinitionRow>(connection)
                .await
        })
        .await?;
    definition.into_storage()
}

pub async fn create_shared_computed_field(
    runtime: &PostgresRuntime,
    request: StorageSharedComputedFieldCreate,
) -> Result<MutationOutcome<StorageComputedFieldMutation>, PostgresStorageError> {
    let (class_id, authorized_collection_id, actor_id, definition, context) = request.into_parts();
    let class_id = class_id.id();
    let authorized_collection_id = authorized_collection_id.id();
    let actor_id = actor_id.id();
    let input = NewComputedDefinitionRow::shared(class_id, actor_id, definition)?;
    let (definition, state, audit) = runtime
        .with_transaction(async move |connection| {
            acquire_computed_class_exclusive_lock(connection, class_id).await?;
            let class =
                locked_class_in_collection(connection, class_id, authorized_collection_id).await?;
            use crate::schema::computed_field_definitions::dsl as definitions;
            let count = definitions::computed_field_definitions
                .filter(definitions::class_id.eq(class_id))
                .filter(definitions::visibility.eq(SHARED_VISIBILITY))
                .count()
                .get_result::<i64>(connection)
                .await?;
            if count >= MAX_SHARED_DEFINITIONS as i64 {
                return Err(PostgresStorageError::invalid_input(format!(
                    "A class may have at most {MAX_SHARED_DEFINITIONS} shared computed fields"
                )));
            }
            let definition = diesel::insert_into(definitions::computed_field_definitions)
                .values(input)
                .returning(ComputedDefinitionRow::as_returning())
                .get_result::<ComputedDefinitionRow>(connection)
                .await?;
            let state = advance_revision_and_enqueue(connection, class_id, Some(actor_id)).await?;
            let event = computed_field_event(
                &definition,
                &class,
                Action::Created,
                &context,
                format!("Shared computed field '{}' created", definition.key()),
            )?
            .with_after(definition.snapshot());
            let audit = append_event(connection, &event)
                .await?
                .into_audit_receipt()?;
            Ok::<_, PostgresStorageError>((definition, state, audit))
        })
        .await?;
    Ok(MutationOutcome::committed(
        StorageComputedFieldMutation::new(definition.into_storage()?, state.into_storage()?),
        audit,
    ))
}

pub async fn update_shared_computed_field(
    runtime: &PostgresRuntime,
    request: StorageSharedComputedFieldUpdate,
) -> Result<MutationOutcome<StorageComputedFieldMutation>, PostgresStorageError> {
    let (class_id, authorized_collection_id, definition_id, actor_id, patch, context) =
        request.into_parts();
    let class_id = class_id.id();
    let authorized_collection_id = authorized_collection_id.id();
    let definition_id = definition_id.id();
    let actor_id = actor_id.id();
    let (definition, state, audit) = runtime
        .with_transaction(async move |connection| {
            acquire_computed_class_exclusive_lock(connection, class_id).await?;
            let class =
                locked_class_in_collection(connection, class_id, authorized_collection_id).await?;
            let current = locked_definition(connection, definition_id).await?;
            assert_locked_revision_precondition(
                connection,
                &RevisionOwner::ComputedField.key(current.id()),
                current.revision(),
            )
            .await?;
            if current.class_id() != class_id || !current.is_shared() {
                return Err(PostgresStorageError::not_found(format!(
                    "Shared computed field {definition_id} was not found in class {class_id}"
                )));
            }
            let validated = validate_patch(&current, &patch)?;
            if !definition_changes(&current, &validated) {
                return Ok((
                    current,
                    ensure_computation_state(connection, class_id).await?,
                    None,
                ));
            }
            let definition =
                apply_definition_patch(connection, &current, &validated, actor_id).await?;
            let state = if validated.value_affecting {
                advance_revision_and_enqueue(connection, class_id, Some(actor_id)).await?
            } else {
                ensure_computation_state(connection, class_id).await?
            };
            let event = computed_field_event(
                &definition,
                &class,
                Action::Updated,
                &context,
                format!("Shared computed field '{}' updated", definition.key()),
            )?
            .with_before(current.snapshot())
            .with_after(definition.snapshot());
            let audit = append_event(connection, &event)
                .await?
                .into_audit_receipt()?;
            Ok::<_, PostgresStorageError>((definition, state, Some(audit)))
        })
        .await?;
    let value =
        StorageComputedFieldMutation::new(definition.into_storage()?, state.into_storage()?);
    Ok(match audit {
        Some(audit) => MutationOutcome::committed(value, audit),
        None => MutationOutcome::unchanged(value),
    })
}

pub async fn delete_shared_computed_field(
    runtime: &PostgresRuntime,
    request: StorageSharedComputedFieldDelete,
) -> Result<MutationOutcome<StorageClassComputationState>, PostgresStorageError> {
    let (class_id, authorized_collection_id, definition_id, actor_id, context) =
        request.into_parts();
    let class_id = class_id.id();
    let authorized_collection_id = authorized_collection_id.id();
    let definition_id = definition_id.id();
    let actor_id = actor_id.id();
    let (state, audit) = runtime
        .with_transaction(async move |connection| {
            acquire_computed_class_exclusive_lock(connection, class_id).await?;
            let class =
                locked_class_in_collection(connection, class_id, authorized_collection_id).await?;
            let current = locked_definition(connection, definition_id).await?;
            assert_locked_revision_precondition(
                connection,
                &RevisionOwner::ComputedField.key(current.id()),
                current.revision(),
            )
            .await?;
            if current.class_id() != class_id || !current.is_shared() {
                return Err(PostgresStorageError::not_found(format!(
                    "Shared computed field {definition_id} was not found in class {class_id}"
                )));
            }
            use crate::schema::computed_field_definitions::dsl as definitions;
            diesel::delete(
                definitions::computed_field_definitions.filter(definitions::id.eq(definition_id)),
            )
            .execute(connection)
            .await?;
            let state = advance_revision_and_enqueue(connection, class_id, Some(actor_id)).await?;
            let event = computed_field_event(
                &current,
                &class,
                Action::Deleted,
                &context,
                format!("Shared computed field '{}' deleted", current.key()),
            )?
            .with_before(current.snapshot());
            let audit = append_event(connection, &event)
                .await?
                .into_audit_receipt()?;
            Ok::<_, PostgresStorageError>((state, audit))
        })
        .await?;
    Ok(MutationOutcome::committed(state.into_storage()?, audit))
}

pub async fn create_personal_computed_field(
    runtime: &PostgresRuntime,
    request: StoragePersonalComputedFieldCreate,
) -> Result<StorageComputedFieldDefinition, PostgresStorageError> {
    let (class_id, owner_id, definition) = request.into_parts();
    let class_id = class_id.id();
    let owner_id = owner_id.id();
    let input = NewComputedDefinitionRow::personal(class_id, owner_id, definition)?;
    let definition = runtime
        .with_transaction(async move |connection| {
            acquire_personal_definition_scope_lock(connection, class_id, owner_id).await?;
            load_class(connection, class_id).await?;
            use crate::schema::computed_field_definitions::dsl as definitions;
            let count = definitions::computed_field_definitions
                .filter(definitions::class_id.eq(class_id))
                .filter(definitions::owner_user_id.eq(Some(owner_id)))
                .filter(definitions::visibility.eq(PERSONAL_VISIBILITY))
                .count()
                .get_result::<i64>(connection)
                .await?;
            if count >= MAX_PERSONAL_DEFINITIONS as i64 {
                return Err(PostgresStorageError::invalid_input(format!(
                    "A user may have at most {MAX_PERSONAL_DEFINITIONS} personal computed fields per class"
                )));
            }
            diesel::insert_into(definitions::computed_field_definitions)
                .values(input)
                .returning(ComputedDefinitionRow::as_returning())
                .get_result::<ComputedDefinitionRow>(connection)
                .await
                .map_err(PostgresStorageError::from)
        })
        .await?;
    definition.into_storage()
}

pub async fn update_personal_computed_field(
    runtime: &PostgresRuntime,
    request: StoragePersonalComputedFieldUpdate,
) -> Result<StorageComputedFieldDefinition, PostgresStorageError> {
    let (owner_id, definition_id, patch) = request.into_parts();
    let owner_id = owner_id.id();
    let definition_id = definition_id.id();
    let definition = runtime
        .with_transaction(async move |connection| {
            let current = locked_definition(connection, definition_id).await?;
            assert_locked_revision_precondition(
                connection,
                &RevisionOwner::ComputedField.key(current.id()),
                current.revision(),
            )
            .await?;
            if !current.is_personal_for(owner_id) {
                return Err(PostgresStorageError::not_found(format!(
                    "Personal computed field {definition_id} was not found"
                )));
            }
            let validated = validate_patch(&current, &patch)?;
            if !definition_changes(&current, &validated) {
                return Ok(current);
            }
            apply_definition_patch(connection, &current, &validated, owner_id).await
        })
        .await?;
    definition.into_storage()
}

pub async fn delete_personal_computed_field(
    runtime: &PostgresRuntime,
    request: StoragePersonalComputedFieldDelete,
) -> Result<(), PostgresStorageError> {
    let (owner_id, definition_id) = request.into_parts();
    let owner_id = owner_id.id();
    let definition_id = definition_id.id();
    runtime
        .with_transaction(async move |connection| {
            let current = locked_definition(connection, definition_id).await?;
            assert_locked_revision_precondition(
                connection,
                &RevisionOwner::ComputedField.key(current.id()),
                current.revision(),
            )
            .await?;
            if !current.is_personal_for(owner_id) {
                return Err(PostgresStorageError::not_found(format!(
                    "Personal computed field {definition_id} was not found"
                )));
            }
            use crate::schema::computed_field_definitions::dsl as definitions;
            diesel::delete(
                definitions::computed_field_definitions.filter(definitions::id.eq(definition_id)),
            )
            .execute(connection)
            .await?;
            Ok::<_, PostgresStorageError>(())
        })
        .await
}

pub async fn request_computed_field_rebuild(
    runtime: &PostgresRuntime,
    request: StorageComputedFieldRebuildRequest,
) -> Result<StorageClassComputationState, PostgresStorageError> {
    let (class_id, authorized_collection_id, actor_id) = request.into_parts();
    let class_id = class_id.id();
    let authorized_collection_id = authorized_collection_id.id();
    let actor_id = actor_id.map(|id| id.id());
    let state = runtime
        .with_transaction(async move |connection| {
            acquire_computed_class_exclusive_lock(connection, class_id).await?;
            locked_class_in_collection(connection, class_id, authorized_collection_id).await?;
            let state = ensure_computation_state(connection, class_id).await?;
            if let Some(task_id) = state.active_task_id
                && active_rebuild_matches(connection, task_id, class_id, state.evaluation_revision)
                    .await?
            {
                return Ok(state);
            }
            enqueue_rebuild(connection, class_id, state.evaluation_revision, actor_id).await
        })
        .await?;
    state.into_storage()
}

/// Advance one class revision and enqueue its rebuild inside a caller-owned
/// PostgreSQL transaction.
#[doc(hidden)]
pub async fn advance_revision_and_enqueue_on_connection(
    connection: &mut PostgresConnection,
    class_id: i32,
    actor_id: Option<i32>,
) -> Result<StorageClassComputationState, PostgresStorageError> {
    validate_positive("class", class_id)?;
    if let Some(actor_id) = actor_id {
        validate_positive("actor", actor_id)?;
    }
    acquire_computed_class_exclusive_lock(connection, class_id).await?;
    advance_revision_and_enqueue(connection, class_id, actor_id)
        .await?
        .into_storage()
}

/// Enqueue fresh rebuilds for restored shared definitions in the caller's
/// restore transaction.
#[doc(hidden)]
pub async fn enqueue_restored_computed_rebuilds_on_connection(
    connection: &mut PostgresConnection,
) -> Result<(), PostgresStorageError> {
    use crate::schema::computed_field_definitions::dsl as definitions;
    let class_ids = definitions::computed_field_definitions
        .filter(definitions::visibility.eq(SHARED_VISIBILITY))
        .select(definitions::class_id)
        .distinct()
        .order(definitions::class_id.asc())
        .load::<i32>(connection)
        .await?;
    for class_id in class_ids {
        acquire_computed_class_exclusive_lock(connection, class_id).await?;
        advance_revision_and_enqueue(connection, class_id, None).await?;
    }
    Ok(())
}

pub async fn execute_computed_field_rebuild(
    runtime: &PostgresRuntime,
    lease: StorageTaskLease,
) -> Result<StorageTask, PostgresStorageError> {
    let started = Instant::now();
    let claimed = task_execution::claimed_task(&lease)?;
    let task = task_execution::find_task(runtime, claimed.id).await?;
    if task.kind != StorageTaskKind::Reindex.as_str() {
        return Err(PostgresStorageError::invalid_input(format!(
            "Task {} is not a computed-field rebuild",
            task.id
        )));
    }
    runtime
        .with_connection(async move |connection| {
            task_execution::live_claimed_task(connection, claimed).await
        })
        .await?;
    let payload = task
        .request_payload
        .clone()
        .ok_or_else(|| PostgresStorageError::invalid_input("Reindex task payload is missing"))
        .and_then(|payload| {
            serde_json::from_value::<ComputedReindexPayload>(payload)
                .map_err(|error| PostgresStorageError::invalid_input(error.to_string()))
        })?;
    payload.validate()?;

    let mut cursor = 0;
    let mut processed = 0_i32;
    loop {
        match process_reindex_batch(runtime, claimed, &payload, cursor).await? {
            ReindexBatch::Superseded => {
                let completed = task_execution::complete_task(
                    runtime,
                    StorageTaskCompletion::new(
                        StorageTaskStateUpdate::new(
                            lease,
                            StorageTaskStatus::Cancelled,
                            successful_counts(processed),
                        )
                        .summary(Some("Computed-field rebuild superseded".to_string()))
                        .started_at(task.started_at.map(|timestamp| timestamp.and_utc())),
                        StorageTaskEventInput::new(
                            StorageTaskStatus::Cancelled.as_str(),
                            "Computed-field rebuild superseded",
                        ),
                    ),
                )
                .await?;
                runtime.record_computed_rebuild_finished("cancelled", started.elapsed());
                return Ok(completed);
            }
            ReindexBatch::Rows {
                last_id,
                count,
                error_codes,
            } => {
                for codes in &error_codes {
                    runtime.record_computed_evaluation("shared", codes);
                }
                runtime.record_computed_rebuild_batch(count as usize);
                cursor = last_id;
                processed = processed.saturating_add(count);
                task_execution::update_task_state(
                    runtime,
                    StorageTaskStateUpdate::new(
                        lease.clone(),
                        StorageTaskStatus::Running,
                        successful_counts(processed),
                    )
                    .summary(Some(format!(
                        "Rebuilt {processed} of {} objects",
                        task.total_items
                    )))
                    .started_at(task.started_at.map(|timestamp| timestamp.and_utc())),
                )
                .await?;
            }
            ReindexBatch::Complete => {
                runtime.record_computed_rebuild_batch(0);
                break;
            }
        }
    }

    let (status, finalized) = runtime
        .with_transaction(async move |connection| {
            task_execution::live_claimed_task(connection, claimed).await?;
            acquire_computed_class_shared_lock(connection, payload.class_id).await?;
            use crate::schema::class_computation_state::dsl as state;
            let changed = diesel::update(
                state::class_computation_state
                    .filter(state::class_id.eq(payload.class_id))
                    .filter(state::evaluation_revision.eq(payload.target_revision))
                    .filter(state::active_task_id.eq(Some(task.id))),
            )
            .set((
                state::rebuild_status.eq("ready"),
                state::active_task_id.eq::<Option<i32>>(None),
                state::last_error.eq::<Option<String>>(None),
                state::updated_at.eq(diesel::dsl::now),
            ))
            .execute(connection)
            .await?;
            let (status, summary) = if changed == 1 {
                (
                    StorageTaskStatus::Succeeded,
                    format!("Computed-field rebuild completed for {processed} objects"),
                )
            } else {
                (
                    StorageTaskStatus::Cancelled,
                    "Computed-field rebuild superseded before completion".to_string(),
                )
            };
            let finalized = task_execution::complete_task_on_connection(
                connection,
                StorageTaskStateUpdate::new(lease, status, successful_counts(processed))
                    .summary(Some(summary.clone()))
                    .started_at(task.started_at.map(|timestamp| timestamp.and_utc())),
                StorageTaskEventInput::new(status.as_str(), summary),
            )
            .await?;
            Ok::<_, PostgresStorageError>((status, finalized))
        })
        .await?;
    task_execution::record_task_terminal(runtime, &finalized);
    runtime.record_computed_rebuild_finished(status.as_str(), started.elapsed());
    tracing::info!(
        message = "Computed-field rebuild finished",
        backend = "postgresql",
        task_id = finalized.id,
        class_id = payload.class_id,
        revision = payload.target_revision,
        processed,
    );
    finalized.into_storage()
}

async fn process_reindex_batch(
    runtime: &PostgresRuntime,
    claimed: task_execution::ClaimedTask,
    payload: &ComputedReindexPayload,
    cursor: i32,
) -> Result<ReindexBatch, PostgresStorageError> {
    let payload = payload.clone();
    runtime
        .with_transaction(
            async move |connection| -> Result<ReindexBatch, PostgresStorageError> {
                task_execution::live_claimed_task(connection, claimed).await?;
                use crate::schema::hubuumobject::dsl as objects;
                let rows = objects::hubuumobject
                    .filter(objects::hubuum_class_id.eq(payload.class_id))
                    .filter(objects::id.gt(cursor))
                    .filter(objects::id.le(payload.object_upper_bound))
                    .order(objects::id.asc())
                    .limit(runtime.computed_reindex_batch_size() as i64)
                    .for_update()
                    .select(ObjectRow::as_select())
                    .load::<ObjectRow>(connection)
                    .await?;
                acquire_computed_class_shared_lock(connection, payload.class_id).await?;
                let state = ensure_computation_state(connection, payload.class_id).await?;
                if state.evaluation_revision != payload.target_revision
                    || state.active_task_id != Some(claimed.id)
                {
                    return Ok(ReindexBatch::Superseded);
                }
                let Some(last_id) = rows.last().map(|row| row.id) else {
                    return Ok(ReindexBatch::Complete);
                };
                let inputs = rows
                    .iter()
                    .map(|row| {
                        ObjectMaterializationInput::new(row.id, row.hubuum_class_id, &row.data)
                    })
                    .collect::<Vec<_>>();
                let summaries = rebuild_objects(
                    connection,
                    payload.class_id,
                    payload.target_revision,
                    &inputs,
                )
                .await?;
                task_execution::live_claimed_task(connection, claimed).await?;
                Ok(ReindexBatch::Rows {
                    last_id,
                    count: i32::try_from(rows.len()).unwrap_or(i32::MAX),
                    error_codes: summaries
                        .into_iter()
                        .map(|summary| summary.error_codes().to_vec())
                        .collect(),
                })
            },
        )
        .await
}

fn successful_counts(processed: i32) -> StorageTaskResultCounts {
    StorageTaskResultCounts::new(processed, processed, 0)
}

fn personal_definition_query(
    owner_id: i32,
    class_id: Option<i32>,
) -> crate::schema::computed_field_definitions::BoxedQuery<'static, diesel::pg::Pg> {
    use crate::schema::computed_field_definitions::dsl as definitions;
    let mut query = definitions::computed_field_definitions
        .filter(definitions::owner_user_id.eq(Some(owner_id)))
        .filter(definitions::visibility.eq(PERSONAL_VISIBILITY))
        .into_boxed();
    if let Some(class_id) = class_id {
        query = query.filter(definitions::class_id.eq(class_id));
    }
    query
}

fn apply_personal_definition_filters<'query>(
    mut query: crate::schema::computed_field_definitions::BoxedQuery<'query, diesel::pg::Pg>,
    options: &QueryOptions,
) -> Result<
    crate::schema::computed_field_definitions::BoxedQuery<'query, diesel::pg::Pg>,
    PostgresStorageError,
> {
    use crate::schema::computed_field_definitions::dsl as definitions;
    for parameter in options.filters() {
        match parameter.field {
            FilterField::Id => {
                crate::postgres_integer_filter!(query, parameter, definitions::id)
            }
            FilterField::Name => {
                crate::postgres_string_filter!(query, parameter, definitions::key)
            }
            FilterField::Description => {
                crate::postgres_string_filter!(query, parameter, definitions::description)
            }
            FilterField::ClassId => {
                crate::postgres_integer_filter!(query, parameter, definitions::class_id)
            }
            FilterField::CreatedAt => {
                crate::postgres_datetime_filter!(query, parameter, definitions::created_at)
            }
            FilterField::UpdatedAt => {
                crate::postgres_datetime_filter!(query, parameter, definitions::updated_at)
            }
            FilterField::Revision => {
                crate::postgres_revision_filter!(query, parameter, definitions::revision)
            }
            ref field => {
                return Err(PostgresStorageError::invalid_input(format!(
                    "Field '{field}' is not searchable for computed fields"
                )));
            }
        }
    }
    Ok(query)
}

fn normalize_list_options(options: &mut QueryOptions) {
    if options.sort().is_empty() {
        options.set_sort(
            vec![SortParam {
                field: FilterField::Id,
                descending: false,
            }]
            .try_into()
            .expect("the fixed computed-field default sort must be valid"),
        );
    }
}

fn computed_cursor_fields(
    options: &QueryOptions,
) -> Result<Vec<CursorSqlField>, PostgresStorageError> {
    options
        .sort()
        .iter()
        .map(|sort| {
            Ok(match sort.field {
                FilterField::Id => CursorSqlField {
                    column: "computed_field_definitions.id",
                    sql_type: CursorSqlType::Integer,
                    nullable: false,
                },
                FilterField::Name => CursorSqlField {
                    column: "computed_field_definitions.key",
                    sql_type: CursorSqlType::String,
                    nullable: false,
                },
                FilterField::ClassId => CursorSqlField {
                    column: "computed_field_definitions.class_id",
                    sql_type: CursorSqlType::Integer,
                    nullable: false,
                },
                FilterField::CreatedAt => CursorSqlField {
                    column: "computed_field_definitions.created_at",
                    sql_type: CursorSqlType::DateTime,
                    nullable: false,
                },
                FilterField::UpdatedAt => CursorSqlField {
                    column: "computed_field_definitions.updated_at",
                    sql_type: CursorSqlType::DateTime,
                    nullable: false,
                },
                FilterField::Revision => CursorSqlField {
                    column: "computed_field_definitions.revision",
                    sql_type: CursorSqlType::BigInt,
                    nullable: false,
                },
                ref field => {
                    return Err(PostgresStorageError::invalid_input(format!(
                        "Field '{field}' is not orderable for computed fields"
                    )));
                }
            })
        })
        .collect()
}

fn validate_definition(
    definition: &StorageComputedFieldDefinitionInput,
) -> Result<(), PostgresStorageError> {
    validated_definition(
        definition.key(),
        definition.label(),
        definition.description(),
        definition.operation(),
        definition.result_type(),
        definition.enabled(),
    )
}

fn validated_definition(
    key: &str,
    label: &str,
    description: &str,
    operation: &Value,
    result_type: &str,
    enabled: bool,
) -> Result<(), PostgresStorageError> {
    let operation = serde_json::from_value::<Operation>(operation.clone())
        .map_err(|error| PostgresStorageError::invalid_input(error.to_string()))?;
    let key = FieldKey::new(key.to_string())
        .map_err(|error| PostgresStorageError::invalid_input(error.to_string()))?;
    let result_type = result_type_from_storage(result_type)?;
    Definition::new(key, label, description, operation, result_type, enabled)
        .map(|_| ())
        .map_err(|error| PostgresStorageError::invalid_input(error.to_string()))
}

fn result_type_from_storage(value: &str) -> Result<ResultType, PostgresStorageError> {
    match value {
        "string" => Ok(ResultType::String),
        "number" => Ok(ResultType::Number),
        "integer" => Ok(ResultType::Integer),
        "boolean" => Ok(ResultType::Boolean),
        "object" => Ok(ResultType::Object),
        "array" => Ok(ResultType::Array),
        _ => Err(PostgresStorageError::invalid_input(format!(
            "Unknown computed result type '{value}'"
        ))),
    }
}

fn validate_patch(
    current: &ComputedDefinitionRow,
    patch: &StorageComputedFieldDefinitionPatch,
) -> Result<ValidatedDefinitionPatch, PostgresStorageError> {
    let key = patch.key().unwrap_or(current.key());
    let label = patch.label().unwrap_or(current.label());
    let description = patch.description().unwrap_or(current.description());
    let operation = patch.operation().unwrap_or(current.operation());
    let result_type = patch.result_type().unwrap_or(current.result_type_name());
    let enabled = patch.enabled().unwrap_or(current.enabled());
    validated_definition(key, label, description, operation, result_type, enabled)?;
    Ok(ValidatedDefinitionPatch {
        key: key.to_string(),
        label: label.to_string(),
        description: description.to_string(),
        operation: operation.clone(),
        result_type: result_type.to_string(),
        enabled,
        value_affecting: key != current.key()
            || operation != current.operation()
            || result_type != current.result_type_name()
            || enabled != current.enabled(),
    })
}

fn definition_changes(current: &ComputedDefinitionRow, patch: &ValidatedDefinitionPatch) -> bool {
    patch.key != current.key()
        || patch.label != current.label()
        || patch.description != current.description()
        || patch.operation != *current.operation()
        || patch.result_type != current.result_type_name()
        || patch.enabled != current.enabled()
}

async fn apply_definition_patch(
    connection: &mut PostgresConnection,
    current: &ComputedDefinitionRow,
    patch: &ValidatedDefinitionPatch,
    actor_id: i32,
) -> Result<ComputedDefinitionRow, PostgresStorageError> {
    use crate::schema::computed_field_definitions::dsl as definitions;
    diesel::update(definitions::computed_field_definitions.filter(definitions::id.eq(current.id())))
        .set((
            definitions::key.eq(&patch.key),
            definitions::label.eq(&patch.label),
            definitions::description.eq(&patch.description),
            definitions::operation.eq(&patch.operation),
            definitions::result_type.eq(&patch.result_type),
            definitions::enabled.eq(patch.enabled),
            definitions::updated_by.eq(Some(actor_id)),
            definitions::updated_at.eq(diesel::dsl::now),
        ))
        .returning(ComputedDefinitionRow::as_returning())
        .get_result::<ComputedDefinitionRow>(connection)
        .await
        .map_err(PostgresStorageError::from)
}

async fn acquire_personal_definition_scope_lock(
    connection: &mut PostgresConnection,
    class_id: i32,
    owner_id: i32,
) -> Result<(), PostgresStorageError> {
    validate_positive("class", class_id)?;
    validate_positive("owner", owner_id)?;
    diesel::sql_query("SELECT pg_advisory_xact_lock($1, $2)")
        .bind::<Integer, _>(class_id)
        .bind::<Integer, _>(-owner_id)
        .execute(connection)
        .await?;
    Ok(())
}

async fn ensure_computation_state(
    connection: &mut PostgresConnection,
    class_id: i32,
) -> Result<ComputationStateRow, PostgresStorageError> {
    use crate::schema::class_computation_state::dsl as state;
    diesel::insert_into(state::class_computation_state)
        .values(state::class_id.eq(class_id))
        .on_conflict(state::class_id)
        .do_nothing()
        .execute(connection)
        .await?;
    state::class_computation_state
        .filter(state::class_id.eq(class_id))
        .select(ComputationStateRow::as_select())
        .first::<ComputationStateRow>(connection)
        .await
        .map_err(PostgresStorageError::from)
}

async fn locked_definition(
    connection: &mut PostgresConnection,
    definition_id: i32,
) -> Result<ComputedDefinitionRow, PostgresStorageError> {
    use crate::schema::computed_field_definitions::dsl as definitions;
    definitions::computed_field_definitions
        .filter(definitions::id.eq(definition_id))
        .for_update()
        .select(ComputedDefinitionRow::as_select())
        .first::<ComputedDefinitionRow>(connection)
        .await
        .map_err(PostgresStorageError::from)
}

async fn load_class(
    connection: &mut PostgresConnection,
    class_id: i32,
) -> Result<ClassRow, PostgresStorageError> {
    use crate::schema::hubuumclass::dsl as classes;
    classes::hubuumclass
        .filter(classes::id.eq(class_id))
        .select(ClassRow::as_select())
        .first::<ClassRow>(connection)
        .await
        .map_err(PostgresStorageError::from)
}

async fn locked_class_in_collection(
    connection: &mut PostgresConnection,
    class_id: i32,
    authorized_collection_id: i32,
) -> Result<ClassRow, PostgresStorageError> {
    use crate::schema::hubuumclass::dsl as classes;
    let class = classes::hubuumclass
        .filter(classes::id.eq(class_id))
        .for_share()
        .select(ClassRow::as_select())
        .first::<ClassRow>(connection)
        .await?;
    if class.collection_id != authorized_collection_id {
        return Err(PostgresStorageError::conflict(format!(
            "Class {class_id} moved from collection {authorized_collection_id}; authorize the request again"
        )));
    }
    Ok(class)
}

fn computed_field_event(
    definition: &ComputedDefinitionRow,
    class: &ClassRow,
    action: Action,
    context: &EventContext,
    summary: String,
) -> Result<NewEvent, PostgresStorageError> {
    NewEvent::new(
        EntityType::ComputedFieldDefinition,
        action,
        context.actor_kind(),
        summary,
    )
    .map_err(|error| PostgresStorageError::internal(error.to_string()))
    .and_then(|event| {
        Ok(event
            .with_context(context)
            .with_entity_id(hubuum_events_core::EventEntityId::new(definition.id())?)
            .with_entity_name(definition.key())
            .with_collection_id(hubuum_domain::CollectionId::new(class.collection_id)?)
            .with_metadata(json!({ "class_id": class.id })))
    })
}

async fn active_rebuild_matches(
    connection: &mut PostgresConnection,
    task_id: i32,
    class_id: i32,
    revision: i64,
) -> Result<bool, PostgresStorageError> {
    use crate::schema::tasks::dsl as tasks;
    let active_statuses = [
        StorageTaskStatus::Queued.as_str(),
        StorageTaskStatus::Validating.as_str(),
        StorageTaskStatus::Running.as_str(),
    ];
    let payload = tasks::tasks
        .filter(tasks::id.eq(task_id))
        .filter(tasks::status.eq_any(active_statuses))
        .select(tasks::request_payload)
        .first::<Option<Value>>(connection)
        .await
        .optional()?
        .flatten()
        .and_then(|value| serde_json::from_value::<ComputedReindexPayload>(value).ok());
    Ok(payload.is_some_and(|payload| {
        payload.payload_type == REINDEX_PAYLOAD_TYPE
            && payload.class_id == class_id
            && payload.target_revision == revision
    }))
}

async fn advance_revision_and_enqueue(
    connection: &mut PostgresConnection,
    class_id: i32,
    actor_id: Option<i32>,
) -> Result<ComputationStateRow, PostgresStorageError> {
    use crate::schema::class_computation_state::dsl as state;
    ensure_computation_state(connection, class_id).await?;
    let revision =
        diesel::update(state::class_computation_state.filter(state::class_id.eq(class_id)))
            .set((
                state::evaluation_revision.eq(state::evaluation_revision + 1),
                state::updated_at.eq(diesel::dsl::now),
            ))
            .returning(state::evaluation_revision)
            .get_result::<i64>(connection)
            .await?;
    enqueue_rebuild(connection, class_id, revision, actor_id).await
}

async fn enqueue_rebuild(
    connection: &mut PostgresConnection,
    class_id: i32,
    revision: i64,
    actor_id: Option<i32>,
) -> Result<ComputationStateRow, PostgresStorageError> {
    cancel_queued_reindex_tasks(connection, class_id, actor_id).await?;
    let boundary = object_boundary(connection, class_id).await?;
    let total_items = i32::try_from(boundary.total_items).unwrap_or(i32::MAX);
    let task = insert_internal_task(
        connection,
        serde_json::to_value(ComputedReindexPayload::new(
            class_id,
            revision,
            boundary.upper_bound,
        ))
        .map_err(|error| PostgresStorageError::internal(error.to_string()))?,
        total_items,
        actor_id,
    )
    .await?;
    use crate::schema::class_computation_state::dsl as state;
    diesel::update(state::class_computation_state.filter(state::class_id.eq(class_id)))
        .set((
            state::rebuild_status.eq("rebuilding"),
            state::active_task_id.eq(Some(task.id)),
            state::last_error.eq::<Option<String>>(None),
            state::updated_at.eq(diesel::dsl::now),
        ))
        .returning(ComputationStateRow::as_returning())
        .get_result::<ComputationStateRow>(connection)
        .await
        .map_err(PostgresStorageError::from)
}

async fn object_boundary(
    connection: &mut PostgresConnection,
    class_id: i32,
) -> Result<ObjectBoundary, PostgresStorageError> {
    diesel::sql_query(
        "SELECT COALESCE(MAX(id), 0)::int AS upper_bound, COUNT(*)::bigint AS total_items \
         FROM hubuumobject WHERE hubuum_class_id=$1",
    )
    .bind::<Integer, _>(class_id)
    .get_result::<ObjectBoundary>(connection)
    .await
    .map_err(PostgresStorageError::from)
}

async fn cancel_queued_reindex_tasks(
    connection: &mut PostgresConnection,
    class_id: i32,
    actor_id: Option<i32>,
) -> Result<(), PostgresStorageError> {
    let queued = diesel::sql_query(
        "SELECT id FROM tasks \
         WHERE kind='reindex' AND status='queued' \
           AND request_payload->>'type'='computed_fields' \
           AND request_payload->>'class_id'=$1",
    )
    .bind::<Text, _>(class_id.to_string())
    .load::<ReturnedTaskId>(connection)
    .await?;
    let task_ids = queued.into_iter().map(|task| task.id).collect::<Vec<_>>();
    if task_ids.is_empty() {
        return Ok(());
    }
    let terminal_at = database_now(connection).await?;
    use crate::schema::tasks::dsl as tasks;
    let cancelled = diesel::update(
        tasks::tasks
            .filter(tasks::id.eq_any(task_ids))
            .filter(tasks::status.eq(StorageTaskStatus::Queued.as_str())),
    )
    .set((
        tasks::status.eq(StorageTaskStatus::Cancelled.as_str()),
        tasks::summary.eq(Some(
            "Superseded by a newer computed-field rebuild".to_string(),
        )),
        tasks::finished_at.eq(Some(terminal_at)),
        tasks::request_payload.eq::<Option<Value>>(None),
        tasks::request_redacted_at.eq(Some(terminal_at)),
        tasks::lease_token.eq::<Option<Uuid>>(None),
        tasks::lease_expires_at.eq::<Option<NaiveDateTime>>(None),
        tasks::updated_at.eq(terminal_at),
    ))
    .returning(TaskRow::as_returning())
    .get_results::<TaskRow>(connection)
    .await?;
    for task in &cancelled {
        let initiator_user_id = task.initiator_user_id.map(PrincipalId::new).transpose()?;
        let task_id = TaskId::new(task.id)?;
        let provenance = match actor_id {
            Some(actor_id) => MutationProvenance::user_for_task(
                PrincipalId::new(actor_id)?,
                initiator_user_id,
                task_id,
            ),
            None => MutationProvenance::system_for_task(initiator_user_id, task_id),
        };
        append_event(
            connection,
            &task_event(
                task,
                Action::Cancelled,
                "Computed-field rebuild superseded",
                &provenance,
            )?,
        )
        .await?;
    }
    Ok(())
}

async fn insert_internal_task(
    connection: &mut PostgresConnection,
    payload: Value,
    total_items: i32,
    actor_id: Option<i32>,
) -> Result<TaskRow, PostgresStorageError> {
    let task = diesel::insert_into(crate::schema::tasks::table)
        .values(NewInternalTaskRow {
            kind: StorageTaskKind::Reindex.as_str().to_string(),
            status: StorageTaskStatus::Queued.as_str().to_string(),
            submitted_by: actor_id,
            idempotency_key: None,
            request_hash: None,
            request_payload: Some(payload),
            summary: None,
            total_items,
            processed_items: 0,
            success_items: 0,
            failed_items: 0,
            submitted_token_id: None,
            submitted_token_scoped: false,
            submitted_token_scopes: json!([]),
            request_redacted_at: None,
            started_at: None,
            finished_at: None,
            initiator_user_id: actor_id,
        })
        .returning(TaskRow::as_returning())
        .get_result::<TaskRow>(connection)
        .await?;
    let initiator_user_id = task.initiator_user_id.map(PrincipalId::new).transpose()?;
    let task_id = TaskId::new(task.id)?;
    let provenance = match actor_id {
        Some(actor_id) => MutationProvenance::user_for_task(
            PrincipalId::new(actor_id)?,
            initiator_user_id,
            task_id,
        ),
        None => MutationProvenance::system_for_task(initiator_user_id, task_id),
    };
    append_event(
        connection,
        &task_event(&task, Action::Queued, "Internal task queued", &provenance)?,
    )
    .await?;
    notify_task_queue(connection, task.id).await?;
    tracing::info!(
        message = "Internal task queued",
        backend = "postgresql",
        task_id = task.id,
        task_kind = task.kind,
        submitted_by = ?task.submitted_by,
        total_items = task.total_items,
    );
    Ok(task)
}

fn task_event(
    task: &TaskRow,
    action: Action,
    summary: &str,
    provenance: &MutationProvenance,
) -> Result<NewEvent, PostgresStorageError> {
    NewEvent::new(EntityType::Task, action, provenance.actor_kind(), summary)
        .map_err(|error| PostgresStorageError::internal(error.to_string()))
        .and_then(|event| {
            Ok(event
                .with_entity_id(hubuum_events_core::EventEntityId::new(task.id)?)
                .with_metadata(json!({
                    "task_id": task.id,
                    "task_kind": task.kind,
                }))
                .with_mutation_provenance(provenance))
        })
}

async fn database_now(
    connection: &mut PostgresConnection,
) -> Result<NaiveDateTime, PostgresStorageError> {
    diesel::sql_query(DATABASE_UTC_NOW_QUERY)
        .get_result::<DatabaseTimeRow>(connection)
        .await
        .map(|row| row.now)
        .map_err(PostgresStorageError::from)
}

fn validate_positive(label: &str, value: i32) -> Result<(), PostgresStorageError> {
    if value <= 0 {
        Err(PostgresStorageError::invalid_input(format!(
            "{label} id must be greater than zero"
        )))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use hubuum_query::{ParsedQueryParam, SearchOperator};

    use super::*;

    #[test]
    fn computed_cursor_mapping_covers_the_public_sort_contract() {
        let options = QueryOptions::new(
            Vec::new(),
            [
                FilterField::Id,
                FilterField::Name,
                FilterField::ClassId,
                FilterField::CreatedAt,
                FilterField::UpdatedAt,
                FilterField::Revision,
            ]
            .into_iter()
            .map(|field| SortParam {
                field,
                descending: false,
            })
            .collect::<Vec<_>>(),
            Some(10),
            None,
            false,
        )
        .unwrap();

        assert_eq!(computed_cursor_fields(&options).unwrap().len(), 6);
    }

    #[test]
    fn invalid_result_types_are_rejected_at_the_adapter_boundary() {
        let error = result_type_from_storage("mystery").unwrap_err();

        assert_eq!(
            error.kind(),
            hubuum_storage_core::StorageErrorKind::InvalidInput
        );
    }

    #[test]
    fn personal_definition_filters_reject_fields_outside_the_contract() {
        let options = QueryOptions::new(
            vec![ParsedQueryParam {
                field: FilterField::Permissions,
                operator: SearchOperator::Equals { is_negated: false },
                value: "read".to_string(),
            }],
            Vec::new(),
            None,
            None,
            false,
        )
        .unwrap();

        let error = apply_personal_definition_filters(personal_definition_query(1, None), &options)
            .err()
            .expect("permission filters must not be accepted for computed fields");

        assert_eq!(
            error.kind(),
            hubuum_storage_core::StorageErrorKind::InvalidInput
        );
        assert!(error.to_string().contains("permissions"));
    }

    #[test]
    fn computed_reindex_payload_rejects_cross_workflow_inputs() {
        let payload = ComputedReindexPayload {
            payload_type: "restore".to_string(),
            class_id: 1,
            target_revision: 1,
            object_upper_bound: 1,
        };

        let error = payload.validate().unwrap_err();

        assert_eq!(
            error.kind(),
            hubuum_storage_core::StorageErrorKind::InvalidInput
        );
    }
}
