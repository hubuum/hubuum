use super::*;

pub async fn request_class_rebuild(
    pool: &DbPool,
    target_class_id: i32,
    authorized_collection_id: i32,
    actor_id: Option<i32>,
) -> Result<ClassComputationState, ApiError> {
    with_transaction(pool, async |conn| -> Result<_, ApiError> {
        acquire_computed_class_exclusive_lock(conn, target_class_id).await?;
        let _ = locked_class_record_in_collection(conn, target_class_id, authorized_collection_id)
            .await?;
        let state = ensure_computation_state(conn, target_class_id).await?;
        if let Some(task_id_value) = state.active_task_id {
            use crate::schema::tasks::dsl::{id, request_payload, status, tasks};
            let active = tasks
                .filter(id.eq(task_id_value))
                .filter(status.eq_any([
                    TaskStatus::Queued.as_str(),
                    TaskStatus::Validating.as_str(),
                    TaskStatus::Running.as_str(),
                ]))
                .select(request_payload)
                .first::<Option<serde_json::Value>>(conn)
                .await
                .optional()?
                .flatten()
                .and_then(|value| serde_json::from_value::<ComputedReindexPayload>(value).ok())
                .is_some_and(|payload| {
                    payload.payload_type == REINDEX_PAYLOAD_TYPE
                        && payload.class_id == target_class_id
                        && payload.target_revision == state.evaluation_revision
                });
            if active {
                return Ok(state);
            }
        }
        enqueue_rebuild(conn, target_class_id, state.evaluation_revision, actor_id).await
    })
    .await
}

pub(crate) async fn enqueue_restored_computed_rebuilds(
    conn: &mut DbConnection,
) -> Result<(), ApiError> {
    use crate::schema::computed_field_definitions::dsl::{
        class_id, computed_field_definitions, visibility,
    };
    let class_ids = computed_field_definitions
        .filter(visibility.eq(COMPUTED_FIELD_VISIBILITY_SHARED))
        .select(class_id)
        .distinct()
        .order(class_id.asc())
        .load::<i32>(conn)
        .await?;
    for target_class_id in class_ids {
        acquire_computed_class_exclusive_lock(conn, target_class_id).await?;
        advance_revision_and_enqueue(conn, target_class_id, None).await?;
    }
    Ok(())
}

enum ReindexBatch {
    Superseded,
    Rows { last_id: i32, count: i32 },
    Complete,
}

async fn process_reindex_batch(
    pool: &DbPool,
    task: &TaskRecord,
    payload: &ComputedReindexPayload,
    cursor: i32,
) -> Result<ReindexBatch, ApiError> {
    with_transaction(pool, async |conn| -> Result<_, ApiError> {
        use crate::schema::hubuumobject::dsl::{hubuum_class_id, hubuumobject, id};
        let objects = hubuumobject
            .filter(hubuum_class_id.eq(payload.class_id))
            .filter(id.gt(cursor))
            .filter(id.le(payload.object_upper_bound))
            .order(id.asc())
            .limit(reindex_batch_size())
            .for_update()
            .load::<HubuumObject>(conn)
            .await?;
        acquire_computed_class_shared_lock(conn, payload.class_id).await?;
        let state = ensure_computation_state(conn, payload.class_id).await?;
        if state.evaluation_revision != payload.target_revision
            || state.active_task_id != Some(task.id)
        {
            return Ok(ReindexBatch::Superseded);
        }
        if objects.is_empty() {
            return Ok(ReindexBatch::Complete);
        }
        let definitions = shared_definitions_conn(conn, payload.class_id).await?;
        for object in &objects {
            if definitions.is_empty() {
                use crate::schema::object_computed_data::dsl::{object_computed_data, object_id};
                diesel::delete(object_computed_data.filter(object_id.eq(object.id)))
                    .execute(conn)
                    .await?;
            } else {
                let result = evaluate_definitions(
                    &object.data,
                    &definitions,
                    MAX_SHARED_DEFINITIONS,
                    "shared",
                )?;
                upsert_materialized(conn, object, payload.target_revision, result).await?;
            }
        }
        Ok(ReindexBatch::Rows {
            last_id: objects.last().expect("non-empty batch").id,
            count: objects.len() as i32,
        })
    })
    .await
}

pub async fn execute_computed_reindex_task(
    pool: &DbPool,
    task: &TaskRecord,
) -> Result<(), ApiError> {
    let started = Instant::now();
    let payload: ComputedReindexPayload = serde_json::from_value(
        task.request_payload
            .clone()
            .ok_or_else(|| ApiError::BadRequest("Reindex task payload is missing".to_string()))?,
    )?;
    payload.validate()?;
    let mut cursor = 0;
    let mut processed = 0;
    loop {
        match process_reindex_batch(pool, task, &payload, cursor).await? {
            ReindexBatch::Superseded => {
                task.finalize_terminal(
                    pool,
                    TaskStateUpdate {
                        status: TaskStatus::Cancelled,
                        summary: Some("Computed-field rebuild superseded".to_string()),
                        processed_items: processed,
                        success_items: processed,
                        failed_items: 0,
                        started_at: task.started_at,
                        finished_at: None,
                    },
                    NewTaskEventRecord {
                        task_id: task.id,
                        event_type: TaskStatus::Cancelled.as_str().to_string(),
                        message: "Computed-field rebuild superseded".to_string(),
                        data: None,
                    },
                )
                .await?;
                crate::observability::metrics::computed_rebuild_finished(
                    "cancelled",
                    started.elapsed(),
                );
                return Ok(());
            }
            ReindexBatch::Rows { last_id, count } => {
                crate::observability::metrics::computed_rebuild_batch(count as usize);
                cursor = last_id;
                processed = processed.saturating_add(count);
                task.update_state(
                    pool,
                    TaskStateUpdate {
                        status: TaskStatus::Running,
                        summary: Some(format!(
                            "Rebuilt {processed} of {} objects",
                            task.total_items
                        )),
                        processed_items: processed,
                        success_items: processed,
                        failed_items: 0,
                        started_at: task.started_at,
                        finished_at: None,
                    },
                )
                .await?;
            }
            ReindexBatch::Complete => {
                crate::observability::metrics::computed_rebuild_batch(0);
                break;
            }
        }
    }

    let ready = with_transaction(pool, async |conn| -> Result<bool, ApiError> {
        acquire_computed_class_shared_lock(conn, payload.class_id).await?;
        use crate::schema::class_computation_state::dsl::{
            active_task_id, class_computation_state, class_id, evaluation_revision, last_error,
            rebuild_status, updated_at,
        };
        let changed = diesel::update(
            class_computation_state
                .filter(class_id.eq(payload.class_id))
                .filter(evaluation_revision.eq(payload.target_revision))
                .filter(active_task_id.eq(Some(task.id))),
        )
        .set((
            rebuild_status.eq("ready"),
            active_task_id.eq::<Option<i32>>(None),
            last_error.eq::<Option<String>>(None),
            updated_at.eq(diesel::dsl::now),
        ))
        .execute(conn)
        .await?;
        Ok(changed == 1)
    })
    .await?;
    let (status, summary) = if ready {
        (
            TaskStatus::Succeeded,
            format!("Computed-field rebuild completed for {processed} objects"),
        )
    } else {
        (
            TaskStatus::Cancelled,
            "Computed-field rebuild superseded before completion".to_string(),
        )
    };
    task.finalize_terminal(
        pool,
        TaskStateUpdate {
            status,
            summary: Some(summary.clone()),
            processed_items: processed,
            success_items: processed,
            failed_items: 0,
            started_at: task.started_at,
            finished_at: None,
        },
        NewTaskEventRecord {
            task_id: task.id,
            event_type: status.as_str().to_string(),
            message: summary,
            data: None,
        },
    )
    .await?;
    crate::observability::metrics::computed_rebuild_finished(status.as_str(), started.elapsed());
    info!(
        message = "Computed-field rebuild finished",
        task_id = task.id,
        class_id = payload.class_id,
        revision = payload.target_revision,
        processed
    );
    Ok(())
}

pub async fn mark_computed_reindex_failed(
    pool: &DbPool,
    task: &TaskRecord,
    stored_error: &str,
) -> Result<(), ApiError> {
    let Some(payload) = task
        .request_payload
        .clone()
        .and_then(|value| serde_json::from_value::<ComputedReindexPayload>(value).ok())
    else {
        return Ok(());
    };
    with_connection(pool, async |conn| {
        use crate::schema::class_computation_state::dsl::{
            active_task_id, class_computation_state, class_id, evaluation_revision, last_error,
            rebuild_status, updated_at,
        };
        diesel::update(
            class_computation_state
                .filter(class_id.eq(payload.class_id))
                .filter(evaluation_revision.eq(payload.target_revision))
                .filter(active_task_id.eq(Some(task.id))),
        )
        .set((
            rebuild_status.eq("failed"),
            active_task_id.eq::<Option<i32>>(None),
            last_error.eq(Some(stored_error.chars().take(512).collect::<String>())),
            updated_at.eq(diesel::dsl::now),
        ))
        .execute(conn)
        .await
    })
    .await?;
    crate::observability::metrics::computed_rebuild_finished("failed", std::time::Duration::ZERO);
    Ok(())
}

pub(crate) async fn mark_recovered_computed_reindex_failed(
    conn: &mut DbConnection,
    task_id: i32,
    stored_error: &str,
) -> Result<(), ApiError> {
    use crate::schema::class_computation_state::dsl::{
        active_task_id, class_computation_state, last_error, rebuild_status, updated_at,
    };
    diesel::update(class_computation_state.filter(active_task_id.eq(Some(task_id))))
        .set((
            rebuild_status.eq("failed"),
            active_task_id.eq::<Option<i32>>(None),
            last_error.eq(Some(stored_error.chars().take(512).collect::<String>())),
            updated_at.eq(diesel::dsl::now),
        ))
        .execute(conn)
        .await?;
    Ok(())
}
