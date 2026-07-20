use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::Instant;

use diesel::sql_types::{BigInt, Integer, Text};
use hubuum_computed_fields::{
    EvaluationLimits, EvaluationResult, MAX_PERSONAL_DEFINITIONS, MAX_SHARED_DEFINITIONS, evaluate,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{info, warn};

use crate::db::prelude::*;
use crate::db::traits::search::{JsonSqlPredicate, dynamic_sql_predicate};
use crate::db::traits::task::{
    TaskBackend, TaskStateUpdate, emit_internal_task_event, insert_internal_queued_task,
};
use crate::db::{DbConnection, DbPool, with_connection, with_transaction};
use crate::errors::ApiError;
use crate::events::{Action, EntityType, EventContext, NewEvent, emit_event};
use crate::models::search::{
    ComputedFieldScope, ComputedQueryValueType, FilterField, Operator, ParsedQueryParam,
    ParsedQueryParamExt, QueryOptions, SQLComponent, SQLValue, SortParam,
};
use crate::models::{
    COMPUTED_FIELD_VISIBILITY_PERSONAL, COMPUTED_FIELD_VISIBILITY_SHARED, ClassComputationState,
    ComputedFieldDefinition, ComputedFieldDefinitionPatch, ComputedFieldDefinitionRequest,
    ComputedFieldErrorResponse, ComputedFieldMutationResponse, ComputedObjectScopesResponse,
    ComputedResultType, ComputedScopeResponse, HubuumClass, HubuumObject,
    HubuumObjectComputedResponse, NewObjectComputedData, NewTaskEventRecord, ObjectComputedData,
    SharedComputedScopeResponse, TaskKind, TaskRecord, TaskStatus, ValidatedComputedFieldPatch,
};
use crate::pagination::{CursorSqlField, CursorSqlMapping, CursorSqlType};

mod materialization;
mod query;
mod rebuild;

pub(crate) use materialization::materialize_object_in_transaction;
pub use materialization::{
    enrich_objects_with_computed, enrich_objects_with_computed_query_snapshot,
    preview_computed_definition, source_data_sha256,
};
use materialization::{evaluate_definitions, shared_definitions_conn, upsert_materialized};
pub use query::{ComputedQuerySnapshot, resolve_computed_query_fields};
pub(crate) use query::{computed_filter_predicate, object_cursor_sql_fields};
#[cfg(test)]
use query::{validate_computed_filter_count, validate_computed_query_count};
pub(crate) use rebuild::{
    enqueue_restored_computed_rebuilds, mark_recovered_computed_reindex_failed,
};
pub use rebuild::{
    execute_computed_reindex_task, mark_computed_reindex_failed, request_class_rebuild,
};

const COMPUTED_CLASS_LOCK_NAMESPACE: i32 = 1_133_113;
const REINDEX_PAYLOAD_TYPE: &str = "computed_fields";
pub const MAX_FILTERS_WITH_COMPUTED: usize = 2;
pub const MAX_SORT_FIELDS_WITH_COMPUTED: usize = 2;

fn reindex_batch_size() -> i64 {
    crate::config::get_config()
        .map(|config| config.computed_reindex_batch_size)
        .unwrap_or(crate::config::DEFAULT_COMPUTED_REINDEX_BATCH_SIZE)
        .min(crate::config::MAX_COMPUTED_REINDEX_BATCH_SIZE) as i64
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComputedReindexPayload {
    #[serde(rename = "type")]
    pub payload_type: String,
    pub class_id: i32,
    pub target_revision: i64,
    pub object_upper_bound: i32,
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

    fn validate(&self) -> Result<(), ApiError> {
        if self.payload_type != REINDEX_PAYLOAD_TYPE
            || self.class_id <= 0
            || self.target_revision < 0
            || self.object_upper_bound < 0
        {
            return Err(ApiError::BadRequest(
                "Computed-field reindex task payload is invalid".to_string(),
            ));
        }
        Ok(())
    }
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

pub(crate) async fn acquire_computed_class_shared_lock(
    conn: &mut DbConnection,
    class_id: i32,
) -> Result<(), ApiError> {
    diesel::sql_query("SELECT pg_advisory_xact_lock_shared($1, $2)")
        .bind::<Integer, _>(COMPUTED_CLASS_LOCK_NAMESPACE)
        .bind::<Integer, _>(class_id)
        .execute(conn)
        .await?;
    Ok(())
}

async fn acquire_computed_class_exclusive_lock(
    conn: &mut DbConnection,
    class_id: i32,
) -> Result<(), ApiError> {
    diesel::sql_query("SELECT pg_advisory_xact_lock($1, $2)")
        .bind::<Integer, _>(COMPUTED_CLASS_LOCK_NAMESPACE)
        .bind::<Integer, _>(class_id)
        .execute(conn)
        .await?;
    Ok(())
}

async fn acquire_personal_definition_scope_lock(
    conn: &mut DbConnection,
    class_id: i32,
    owner_id: i32,
) -> Result<(), ApiError> {
    if class_id <= 0 || owner_id <= 0 {
        return Err(ApiError::BadRequest(
            "Computed-field class and owner ids must be greater than zero".to_string(),
        ));
    }
    // The negative owner key keeps this two-id lock domain separate from the
    // positive namespace/class pair used by shared materialization locks.
    diesel::sql_query("SELECT pg_advisory_xact_lock($1, $2)")
        .bind::<Integer, _>(class_id)
        .bind::<Integer, _>(-owner_id)
        .execute(conn)
        .await?;
    Ok(())
}

async fn ensure_computation_state(
    conn: &mut DbConnection,
    target_class_id: i32,
) -> Result<ClassComputationState, ApiError> {
    use crate::schema::class_computation_state::dsl::{class_computation_state, class_id};

    diesel::insert_into(class_computation_state)
        .values(class_id.eq(target_class_id))
        .on_conflict(class_id)
        .do_nothing()
        .execute(conn)
        .await?;
    Ok(class_computation_state
        .filter(class_id.eq(target_class_id))
        .select(ClassComputationState::as_select())
        .first(conn)
        .await?)
}

pub async fn class_computation_state_for(
    pool: &DbPool,
    target_class_id: i32,
) -> Result<ClassComputationState, ApiError> {
    use crate::schema::class_computation_state::dsl::{class_computation_state, class_id};

    Ok(with_connection(pool, async |conn| {
        class_computation_state
            .filter(class_id.eq(target_class_id))
            .select(ClassComputationState::as_select())
            .first(conn)
            .await
            .optional()
    })
    .await?
    .unwrap_or_else(|| ClassComputationState::ready_without_definitions(target_class_id)))
}

pub async fn list_shared_definitions(
    pool: &DbPool,
    target_class_id: i32,
) -> Result<Vec<ComputedFieldDefinition>, ApiError> {
    use crate::schema::computed_field_definitions::dsl::{
        class_id, computed_field_definitions, id, visibility,
    };

    with_connection(pool, async |conn| {
        computed_field_definitions
            .filter(class_id.eq(target_class_id))
            .filter(visibility.eq(COMPUTED_FIELD_VISIBILITY_SHARED))
            .order(id.asc())
            .select(ComputedFieldDefinition::as_select())
            .load(conn)
            .await
    })
    .await
}

pub async fn list_personal_definitions_page(
    pool: &DbPool,
    owner_id: i32,
    class_filter: Option<i32>,
    query_options: &QueryOptions,
) -> Result<(Vec<ComputedFieldDefinition>, i64), ApiError> {
    use crate::schema::computed_field_definitions::dsl::{
        class_id, computed_field_definitions, owner_user_id, visibility,
    };

    let count = crate::pagination::exact_count_or_skipped(query_options, async || {
        with_connection(pool, async |conn| {
            let mut query = computed_field_definitions
                .filter(owner_user_id.eq(Some(owner_id)))
                .filter(visibility.eq(COMPUTED_FIELD_VISIBILITY_PERSONAL))
                .into_boxed();
            if let Some(target_class_id) = class_filter {
                query = query.filter(class_id.eq(target_class_id));
            }
            query.count().get_result::<i64>(conn).await
        })
        .await
    })
    .await?;

    let rows = with_connection(pool, async |conn| -> Result<_, ApiError> {
        let mut query = computed_field_definitions
            .filter(owner_user_id.eq(Some(owner_id)))
            .filter(visibility.eq(COMPUTED_FIELD_VISIBILITY_PERSONAL))
            .into_boxed();
        if let Some(target_class_id) = class_filter {
            query = query.filter(class_id.eq(target_class_id));
        }
        crate::apply_query_options!(query, query_options, ComputedFieldDefinition);
        Ok(query
            .select(ComputedFieldDefinition::as_select())
            .load(conn)
            .await?)
    })
    .await?;
    Ok((rows, count))
}

pub async fn get_computed_definition(
    pool: &DbPool,
    definition_id: i32,
) -> Result<ComputedFieldDefinition, ApiError> {
    use crate::schema::computed_field_definitions::dsl::{computed_field_definitions, id};
    with_connection(pool, async |conn| {
        computed_field_definitions
            .filter(id.eq(definition_id))
            .select(ComputedFieldDefinition::as_select())
            .first(conn)
            .await
    })
    .await
}

fn computed_field_event(
    definition: &ComputedFieldDefinition,
    class: &HubuumClass,
    action: Action,
    context: &EventContext,
    summary: String,
) -> Result<NewEvent, ApiError> {
    Ok(NewEvent::new(
        EntityType::ComputedFieldDefinition,
        action,
        context.actor_kind(),
        summary,
    )?
    .with_context(context)
    .with_entity_id(definition.id)
    .with_entity_name(definition.key.clone())
    .with_collection_id(class.collection_id)
    .with_metadata(serde_json::json!({ "class_id": class.id })))
}

async fn class_record(
    conn: &mut DbConnection,
    target_class_id: i32,
) -> Result<HubuumClass, ApiError> {
    use crate::schema::hubuumclass::dsl::{hubuumclass, id};
    Ok(hubuumclass
        .filter(id.eq(target_class_id))
        .first::<HubuumClass>(conn)
        .await?)
}

async fn locked_class_record_in_collection(
    conn: &mut DbConnection,
    target_class_id: i32,
    authorized_collection_id: i32,
) -> Result<HubuumClass, ApiError> {
    use crate::schema::hubuumclass::dsl::{hubuumclass, id};
    let class = hubuumclass
        .filter(id.eq(target_class_id))
        .for_share()
        .first::<HubuumClass>(conn)
        .await?;
    if class.collection_id != authorized_collection_id {
        return Err(ApiError::Conflict(format!(
            "Class {target_class_id} moved from collection {authorized_collection_id}; authorize the request again"
        )));
    }
    Ok(class)
}

async fn cancel_queued_reindex_tasks(
    conn: &mut DbConnection,
    target_class_id: i32,
    actor_id: Option<i32>,
) -> Result<(), ApiError> {
    let cancelled = diesel::sql_query(
        "UPDATE tasks SET status='cancelled', summary='Superseded by a newer computed-field rebuild', \
         finished_at=(clock_timestamp() AT TIME ZONE 'UTC'), request_payload=NULL, \
         request_redacted_at=(clock_timestamp() AT TIME ZONE 'UTC'), \
         updated_at=(clock_timestamp() AT TIME ZONE 'UTC') \
         WHERE kind='reindex' AND status='queued' \
           AND request_payload->>'type'='computed_fields' \
           AND request_payload->>'class_id'=$1 \
         RETURNING id",
    )
    .bind::<Text, _>(target_class_id.to_string())
    .load::<ReturnedTaskId>(conn)
    .await?;

    for task in cancelled {
        emit_internal_task_event(
            conn,
            &NewTaskEventRecord {
                task_id: task.id,
                event_type: TaskStatus::Cancelled.as_str().to_string(),
                message: "Computed-field rebuild superseded".to_string(),
                data: None,
            },
            actor_id,
            TaskKind::Reindex,
        )
        .await?;
    }
    Ok(())
}

async fn object_boundary(
    conn: &mut DbConnection,
    target_class_id: i32,
) -> Result<ObjectBoundary, ApiError> {
    Ok(diesel::sql_query(
        "SELECT COALESCE(MAX(id), 0)::int AS upper_bound, COUNT(*)::bigint AS total_items \
         FROM hubuumobject WHERE hubuum_class_id=$1",
    )
    .bind::<Integer, _>(target_class_id)
    .get_result(conn)
    .await?)
}

async fn enqueue_rebuild(
    conn: &mut DbConnection,
    target_class_id: i32,
    target_revision: i64,
    actor_id: Option<i32>,
) -> Result<ClassComputationState, ApiError> {
    use crate::schema::class_computation_state::dsl::{
        active_task_id, class_computation_state, class_id, last_error, rebuild_status, updated_at,
    };

    cancel_queued_reindex_tasks(conn, target_class_id, actor_id).await?;
    let boundary = object_boundary(conn, target_class_id).await?;
    let total_items = i32::try_from(boundary.total_items).unwrap_or(i32::MAX);
    let task = insert_internal_queued_task(
        conn,
        TaskKind::Reindex,
        serde_json::to_value(ComputedReindexPayload::new(
            target_class_id,
            target_revision,
            boundary.upper_bound,
        ))?,
        total_items,
        actor_id,
    )
    .await?;

    Ok(
        diesel::update(class_computation_state.filter(class_id.eq(target_class_id)))
            .set((
                rebuild_status.eq("rebuilding"),
                active_task_id.eq(Some(task.id)),
                last_error.eq::<Option<String>>(None),
                updated_at.eq(diesel::dsl::now),
            ))
            .returning(ClassComputationState::as_returning())
            .get_result(conn)
            .await?,
    )
}

async fn advance_revision_and_enqueue(
    conn: &mut DbConnection,
    target_class_id: i32,
    actor_id: Option<i32>,
) -> Result<ClassComputationState, ApiError> {
    use crate::schema::class_computation_state::dsl::{
        class_computation_state, class_id, evaluation_revision, updated_at,
    };

    ensure_computation_state(conn, target_class_id).await?;
    let revision = diesel::update(class_computation_state.filter(class_id.eq(target_class_id)))
        .set((
            evaluation_revision.eq(evaluation_revision + 1),
            updated_at.eq(diesel::dsl::now),
        ))
        .returning(evaluation_revision)
        .get_result::<i64>(conn)
        .await?;
    enqueue_rebuild(conn, target_class_id, revision, actor_id).await
}

pub async fn create_shared_definition(
    pool: &DbPool,
    target_class_id: i32,
    authorized_collection_id: i32,
    actor_id: i32,
    request: ComputedFieldDefinitionRequest,
    context: &EventContext,
) -> Result<ComputedFieldMutationResponse, ApiError> {
    let input = request.into_new_shared(target_class_id, actor_id)?;
    with_transaction(pool, async |conn| -> Result<_, ApiError> {
        acquire_computed_class_exclusive_lock(conn, target_class_id).await?;
        let class =
            locked_class_record_in_collection(conn, target_class_id, authorized_collection_id)
                .await?;
        use crate::schema::computed_field_definitions::dsl::{
            class_id, computed_field_definitions, visibility,
        };
        let count = computed_field_definitions
            .filter(class_id.eq(target_class_id))
            .filter(visibility.eq(COMPUTED_FIELD_VISIBILITY_SHARED))
            .count()
            .get_result::<i64>(conn)
            .await?;
        if count >= MAX_SHARED_DEFINITIONS as i64 {
            return Err(ApiError::BadRequest(format!(
                "A class may have at most {MAX_SHARED_DEFINITIONS} shared computed fields"
            )));
        }
        let definition = diesel::insert_into(computed_field_definitions)
            .values(input)
            .returning(ComputedFieldDefinition::as_returning())
            .get_result(conn)
            .await?;
        let state = advance_revision_and_enqueue(conn, target_class_id, Some(actor_id)).await?;
        let event = computed_field_event(
            &definition,
            &class,
            Action::Created,
            context,
            format!("Shared computed field '{}' created", definition.key),
        )?
        .with_after(serde_json::to_value(&definition)?);
        emit_event(conn, &event).await?;
        Ok(ComputedFieldMutationResponse { definition, state })
    })
    .await
}

async fn locked_definition(
    conn: &mut DbConnection,
    definition_id: i32,
) -> Result<ComputedFieldDefinition, ApiError> {
    use crate::schema::computed_field_definitions::dsl::{computed_field_definitions, id};
    computed_field_definitions
        .filter(id.eq(definition_id))
        .for_update()
        .select(ComputedFieldDefinition::as_select())
        .first(conn)
        .await
        .map_err(ApiError::from)
}

async fn apply_definition_patch(
    conn: &mut DbConnection,
    current: &ComputedFieldDefinition,
    patch: &ValidatedComputedFieldPatch,
    actor_id: i32,
) -> Result<ComputedFieldDefinition, ApiError> {
    use crate::schema::computed_field_definitions::dsl::{
        computed_field_definitions, description, enabled, id, key, label, operation, result_type,
        revision, updated_at, updated_by,
    };
    Ok(
        diesel::update(computed_field_definitions.filter(id.eq(current.id)))
            .set((
                key.eq(&patch.key),
                label.eq(&patch.label),
                description.eq(&patch.description),
                operation.eq(&patch.operation),
                result_type.eq(&patch.result_type),
                enabled.eq(patch.enabled),
                revision.eq(revision + 1),
                updated_by.eq(Some(actor_id)),
                updated_at.eq(diesel::dsl::now),
            ))
            .returning(ComputedFieldDefinition::as_returning())
            .get_result(conn)
            .await?,
    )
}

pub async fn update_shared_definition(
    pool: &DbPool,
    target_class_id: i32,
    authorized_collection_id: i32,
    definition_id: i32,
    actor_id: i32,
    patch: ComputedFieldDefinitionPatch,
    context: &EventContext,
) -> Result<ComputedFieldMutationResponse, ApiError> {
    with_transaction(pool, async |conn| -> Result<_, ApiError> {
        acquire_computed_class_exclusive_lock(conn, target_class_id).await?;
        let class =
            locked_class_record_in_collection(conn, target_class_id, authorized_collection_id)
                .await?;
        let current = locked_definition(conn, definition_id).await?;
        if current.class_id != target_class_id || !current.is_shared() {
            return Err(ApiError::NotFound(format!(
                "Shared computed field {definition_id} was not found in class {target_class_id}"
            )));
        }
        if current.revision != patch.expected_revision {
            return Err(ApiError::Conflict(format!(
                "Computed field revision is {}; expected {}",
                current.revision, patch.expected_revision
            )));
        }
        let validated = patch.validate_against(&current)?;
        let changed = validated.key != current.key
            || validated.label != current.label
            || validated.description != current.description
            || validated.operation != current.operation
            || validated.result_type != current.result_type
            || validated.enabled != current.enabled;
        if !changed {
            return Ok(ComputedFieldMutationResponse {
                definition: current,
                state: ensure_computation_state(conn, target_class_id).await?,
            });
        }
        let definition = apply_definition_patch(conn, &current, &validated, actor_id).await?;
        let state = if validated.value_affecting {
            advance_revision_and_enqueue(conn, target_class_id, Some(actor_id)).await?
        } else {
            ensure_computation_state(conn, target_class_id).await?
        };
        let event = computed_field_event(
            &definition,
            &class,
            Action::Updated,
            context,
            format!("Shared computed field '{}' updated", definition.key),
        )?
        .with_before(serde_json::to_value(&current)?)
        .with_after(serde_json::to_value(&definition)?);
        emit_event(conn, &event).await?;
        Ok(ComputedFieldMutationResponse { definition, state })
    })
    .await
}

pub async fn delete_shared_definition(
    pool: &DbPool,
    target_class_id: i32,
    authorized_collection_id: i32,
    definition_id: i32,
    actor_id: i32,
    expected_revision: i64,
    context: &EventContext,
) -> Result<ClassComputationState, ApiError> {
    with_transaction(pool, async |conn| -> Result<_, ApiError> {
        acquire_computed_class_exclusive_lock(conn, target_class_id).await?;
        let class =
            locked_class_record_in_collection(conn, target_class_id, authorized_collection_id)
                .await?;
        let current = locked_definition(conn, definition_id).await?;
        if current.class_id != target_class_id || !current.is_shared() {
            return Err(ApiError::NotFound(format!(
                "Shared computed field {definition_id} was not found in class {target_class_id}"
            )));
        }
        if current.revision != expected_revision {
            return Err(ApiError::Conflict(format!(
                "Computed field revision is {}; expected {expected_revision}",
                current.revision
            )));
        }
        use crate::schema::computed_field_definitions::dsl::{computed_field_definitions, id};
        diesel::delete(computed_field_definitions.filter(id.eq(definition_id)))
            .execute(conn)
            .await?;
        let state = advance_revision_and_enqueue(conn, target_class_id, Some(actor_id)).await?;
        let event = computed_field_event(
            &current,
            &class,
            Action::Deleted,
            context,
            format!("Shared computed field '{}' deleted", current.key),
        )?
        .with_before(serde_json::to_value(&current)?);
        emit_event(conn, &event).await?;
        Ok(state)
    })
    .await
}

pub async fn create_personal_definition(
    pool: &DbPool,
    target_class_id: i32,
    owner_id: i32,
    request: ComputedFieldDefinitionRequest,
) -> Result<ComputedFieldDefinition, ApiError> {
    let input = request.into_new_personal(target_class_id, owner_id)?;
    with_transaction(pool, async |conn| -> Result<_, ApiError> {
        acquire_personal_definition_scope_lock(conn, target_class_id, owner_id).await?;
        let _ = class_record(conn, target_class_id).await?;
        use crate::schema::computed_field_definitions::dsl::{
            class_id, computed_field_definitions, owner_user_id, visibility,
        };
        let count = computed_field_definitions
            .filter(class_id.eq(target_class_id))
            .filter(owner_user_id.eq(Some(owner_id)))
            .filter(visibility.eq(COMPUTED_FIELD_VISIBILITY_PERSONAL))
            .count()
            .get_result::<i64>(conn)
            .await?;
        if count >= MAX_PERSONAL_DEFINITIONS as i64 {
            return Err(ApiError::BadRequest(format!(
                "A user may have at most {MAX_PERSONAL_DEFINITIONS} personal computed fields per class"
            )));
        }
        Ok(diesel::insert_into(computed_field_definitions)
            .values(input)
            .returning(ComputedFieldDefinition::as_returning())
            .get_result(conn)
            .await?)
    })
    .await
}

pub async fn update_personal_definition(
    pool: &DbPool,
    owner_id: i32,
    definition_id: i32,
    patch: ComputedFieldDefinitionPatch,
) -> Result<ComputedFieldDefinition, ApiError> {
    with_transaction(pool, async |conn| -> Result<_, ApiError> {
        let current = locked_definition(conn, definition_id).await?;
        if !current.is_personal_for(owner_id) {
            return Err(ApiError::NotFound(format!(
                "Personal computed field {definition_id} was not found"
            )));
        }
        if current.revision != patch.expected_revision {
            return Err(ApiError::Conflict(format!(
                "Computed field revision is {}; expected {}",
                current.revision, patch.expected_revision
            )));
        }
        let validated = patch.validate_against(&current)?;
        let changed = validated.key != current.key
            || validated.label != current.label
            || validated.description != current.description
            || validated.operation != current.operation
            || validated.result_type != current.result_type
            || validated.enabled != current.enabled;
        if !changed {
            return Ok(current);
        }
        apply_definition_patch(conn, &current, &validated, owner_id).await
    })
    .await
}

pub async fn delete_personal_definition(
    pool: &DbPool,
    owner_id: i32,
    definition_id: i32,
    expected_revision: i64,
) -> Result<(), ApiError> {
    with_transaction(pool, async |conn| -> Result<_, ApiError> {
        let current = locked_definition(conn, definition_id).await?;
        if !current.is_personal_for(owner_id) {
            return Err(ApiError::NotFound(format!(
                "Personal computed field {definition_id} was not found"
            )));
        }
        if current.revision != expected_revision {
            return Err(ApiError::Conflict(format!(
                "Computed field revision is {}; expected {expected_revision}",
                current.revision
            )));
        }
        use crate::schema::computed_field_definitions::dsl::{computed_field_definitions, id};
        diesel::delete(computed_field_definitions.filter(id.eq(definition_id)))
            .execute(conn)
            .await?;
        Ok(())
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::search::{
        parse_query_parameter, parse_query_parameter_with_computed_filters_and_passthrough,
    };

    #[test]
    fn canonical_hash_ignores_object_key_order() {
        let left = serde_json::json!({"a": 1, "b": {"c": 2, "d": 3}});
        let right = serde_json::json!({"b": {"d": 3, "c": 2}, "a": 1});

        assert_eq!(
            source_data_sha256(&left).unwrap(),
            source_data_sha256(&right).unwrap()
        );
    }

    #[test]
    fn canonical_hash_preserves_array_order() {
        assert_ne!(
            source_data_sha256(&serde_json::json!([1, 2])).unwrap(),
            source_data_sha256(&serde_json::json!([2, 1])).unwrap()
        );
    }

    #[test]
    fn computed_query_request_size_is_bounded_before_sql_resolution() {
        let sorts = parse_query_parameter("sort=computed.shared.first,computed.shared.second,name")
            .unwrap()
            .sort;

        let error = validate_computed_query_count(sorts.len()).unwrap_err();

        assert_eq!(
            error.to_string(),
            "Computed sorting supports at most 2 explicit sort fields per request"
        );
    }

    #[test]
    fn computed_filter_count_is_bounded_before_sql_resolution() {
        let (query, _) = parse_query_parameter_with_computed_filters_and_passthrough(
            "computed.shared.first=1&computed.shared.first__gte=0&computed.personal.second=2",
            &[],
        )
        .unwrap();
        let computed_filter_count = query
            .filters
            .iter()
            .filter(|filter| filter.field.computed_query().is_some())
            .count();

        let error = validate_computed_filter_count(computed_filter_count).unwrap_err();

        assert_eq!(
            error.to_string(),
            "Computed filtering supports at most 2 computed filter parameters per request"
        );
    }
}
