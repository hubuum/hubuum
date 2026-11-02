use super::*;

pub(crate) async fn execute_export_task<C>(
    backend: &C,
    task: &ClaimedTask,
    subject: &ExecutionPrincipal,
    scopes: Option<&TokenScope>,
) -> Result<TaskStatus, ApiError>
where
    C: AuthorizationContext,
{
    let pool = backend;
    let payload = task
        .request_payload
        .clone()
        .ok_or_else(|| ApiError::BadRequest("Export task payload is missing".to_string()))?;
    let payload: StoredExportTaskPayload = serde_json::from_value(payload)?;
    let template = resolve_template(pool, payload.template_id).await?;
    if let Some(template) = &template {
        crate::can!(
            backend,
            subject,
            scopes,
            [Permissions::ReadTemplate],
            template
        );
    }
    let runtime = prepare_export_runtime(pool, payload.export, template).await?;
    validate_export_submission(&runtime)?;
    let exporter = PermissionAwareExport::new(backend, subject, scopes).await?;
    let metric_template_id = runtime
        .template
        .as_ref()
        .map(|template| ExportTemplateID::new(template.id))
        .transpose()?;
    let total_metric =
        metrics::export_phase_timer(metrics::ExportMetricPhase::Total, metric_template_id);
    append_task_event(
        pool,
        task,
        NewTaskEventRecord {
            event_type: "running".to_string(),
            message: "Export execution started".to_string(),
            data: None,
        },
    )
    .await?;
    update_task_state(
        pool,
        task,
        TaskStateChange::new(TaskStatus::Running, TaskResultCounts::default())
            .started_at(task.started_at),
    )
    .await?;

    let ExportRenderInput {
        execution,
        timings,
        total_metric,
    } = collect_export_items(&exporter, task, &runtime, total_metric).await?;

    append_task_event(
        pool,
        task,
        NewTaskEventRecord {
            event_type: "running".to_string(),
            message: "Rendering export output".to_string(),
            data: None,
        },
    )
    .await?;

    let render_metric =
        metrics::export_phase_timer(metrics::ExportMetricPhase::Render, metric_template_id);
    crate::tasks::control::checkpoint()?;
    // Await the bounded render job so its child has been reaped before the
    // worker acknowledges a stop and releases its claim.
    let artifact = build_export_artifact(&runtime, execution, timings).await?;
    crate::tasks::control::checkpoint()?;
    let mut timings = artifact.timings;
    if let Err(error) = enforce_export_stage_timeout(render_metric.elapsed(), "template rendering")
    {
        render_metric.finish(metrics::ExportMetricOutcome::Timeout);
        total_metric.finish(metrics::ExportMetricOutcome::Timeout);
        return Err(error);
    }
    let render_elapsed = render_metric.finish(metrics::ExportMetricOutcome::Success);
    let total_elapsed = total_metric.elapsed();
    timings.render = render_elapsed;
    timings.total = total_elapsed;
    log_export_stage_metrics(task.id, &runtime, timings);
    let artifact = ExportArtifact {
        timings,
        ..artifact
    };
    finish_export_task(pool, task, artifact, total_metric).await
}

struct ExportRenderInput {
    execution: ExportExecution,
    timings: ExportExecutionTimings,
    total_metric: metrics::ExportPhaseTimer,
}

async fn collect_export_items<C, S>(
    exporter: &PermissionAwareExport<'_, C, S>,
    task: &ClaimedTask,
    runtime: &ExportRuntime,
    total_metric: metrics::ExportPhaseTimer,
) -> Result<ExportRenderInput, ApiError>
where
    C: AuthorizationContext,
    S: AuthzSubject,
{
    let pool = exporter.backend;
    let metric_template_id = runtime
        .template
        .as_ref()
        .map(|template| ExportTemplateID::new(template.id))
        .transpose()?;
    let mut timings = ExportExecutionTimings::default();
    append_task_event(
        pool,
        task,
        NewTaskEventRecord {
            event_type: "running".to_string(),
            message: "Query execution started".to_string(),
            data: Some(serde_json::json!({
                "scope": runtime.scope.kind().as_str(),
                "content_type": runtime.content_type().as_mime(),
            })),
        },
    )
    .await?;

    // Export-scoped, in-flight storage query budget. The selected adapter owns
    // how it enforces the limit without affecting bookkeeping writes outside
    // these scopes.
    let query_budget = export_query_budget();
    let export_storage = storage_handle(pool);
    let mut query_options = prepare_query_options(&runtime.export)?;
    let relation_hydration = resolve_relation_hydration_plan(runtime, &mut query_options)?;
    let query_metric =
        metrics::export_phase_timer(metrics::ExportMetricPhase::Query, metric_template_id);
    let (items, mut warnings, truncated) = export_storage
        .run_in_scope(
            StorageExecutionScope::default().with_query_budget(query_budget),
            execute_scope(exporter, runtime.scope, query_options),
        )
        .await?;
    let mut items = items;
    export_storage
        .run_in_scope(
            StorageExecutionScope::default().with_query_budget(query_budget),
            apply_export_includes(exporter, &runtime.export, &mut items),
        )
        .await?;
    if let Err(error) = enforce_export_stage_timeout(query_metric.elapsed(), "query execution") {
        query_metric.finish(metrics::ExportMetricOutcome::Timeout);
        total_metric.finish(metrics::ExportMetricOutcome::Timeout);
        return Err(error);
    }
    let query_elapsed = query_metric.finish(metrics::ExportMetricOutcome::Success);
    timings.query = query_elapsed;

    if relation_hydration
        .as_ref()
        .is_some_and(|plan| plan.enabled_for_scope)
    {
        append_task_event(
            pool,
            task,
            NewTaskEventRecord {
                event_type: "running".to_string(),
                message: "Hydrating relation-aware template context".to_string(),
                data: relation_hydration.as_ref().map(|plan| {
                    serde_json::json!({
                        "depth_limit": plan.depth_limit,
                    })
                }),
            },
        )
        .await?;
    }

    add_truncation_warning(&mut warnings, truncated);
    let hydration_metric =
        metrics::export_phase_timer(metrics::ExportMetricPhase::Hydration, metric_template_id);
    let (template_items, source) = export_storage
        .run_in_scope(
            StorageExecutionScope::default().with_query_budget(query_budget),
            build_template_items(exporter, runtime, &items, relation_hydration),
        )
        .await?;
    if let Err(error) =
        enforce_export_stage_timeout(hydration_metric.elapsed(), "relation hydration")
    {
        hydration_metric.finish(metrics::ExportMetricOutcome::Timeout);
        total_metric.finish(metrics::ExportMetricOutcome::Timeout);
        return Err(error);
    }
    let hydration_elapsed = hydration_metric.finish(metrics::ExportMetricOutcome::Success);
    timings.hydration = hydration_elapsed;
    let template_export = runtime.template.is_some();
    let item_count = if template_export {
        template_items.len()
    } else {
        items.len()
    };
    let execution_items = if template_export {
        drop(items);
        Vec::new()
    } else {
        items
    };
    let execution = ExportExecution {
        meta: ExportMeta {
            count: item_count,
            truncated,
            scope: runtime.export.scope.clone(),
            content_type: runtime.content_type(),
        },
        items: execution_items,
        template_items,
        warnings,
        source,
    };

    Ok(ExportRenderInput {
        execution,
        timings,
        total_metric,
    })
}

async fn finish_export_task(
    pool: &impl crate::storage::StorageContext,
    task: &ClaimedTask,
    artifact: ExportArtifact,
    total_metric: metrics::ExportPhaseTimer,
) -> Result<TaskStatus, ApiError> {
    let total_elapsed = artifact.timings.total;
    let metric_scope = artifact.meta.scope.kind.as_str();
    let metric_content_type = artifact.output.content_type().as_mime();
    let metric_truncated = artifact.meta.truncated;
    let metric_warning_count = artifact.warnings.len();

    append_task_event(
        pool,
        task,
        NewTaskEventRecord {
            event_type: "running".to_string(),
            message: "Persisting export output".to_string(),
            data: None,
        },
    )
    .await?;

    let finished = complete_task(
        pool,
        task,
        TaskStateChange::new(
            TaskStatus::Succeeded,
            TaskResultCounts::from_outcomes(1, 0)?,
        )
        .summary("Export completed successfully")
        .started_at(task.started_at),
        NewTaskEventRecord {
            event_type: TaskStatus::Succeeded.as_str().to_string(),
            message: format!("Export completed successfully in {total_elapsed:?}"),
            data: Some(serde_json::json!({
                "content_type": artifact.output.content_type().as_mime(),
                "template_name": artifact.template_name.clone(),
                "warning_count": artifact.warnings.len(),
                "truncated": artifact.meta.truncated,
                "total_duration_ms": artifact.timings.total_millis(),
                "query_duration_ms": artifact.timings.query_millis(),
                "hydration_duration_ms": artifact.timings.hydration_millis(),
                "render_duration_ms": artifact.timings.render_millis(),
            })),
        },
        StorageTaskCompletionPayload::Export(artifact_to_storage(artifact)?),
    )
    .await?;

    if finished.status == TaskStatus::Cancelled.as_str() {
        return Ok(TaskStatus::Cancelled);
    }
    // Only label the total phase successful after the output and terminal task
    // state have been persisted. If finalization fails, the timer's drop path
    // records an error outcome instead.
    total_metric.finish(metrics::ExportMetricOutcome::Success);
    metrics::export_completed(metric_scope, metric_content_type);
    if metric_truncated {
        metrics::export_truncated(metric_scope, metric_content_type);
    }
    if metric_warning_count > 0 {
        metrics::export_warnings(metric_scope, metric_content_type, metric_warning_count);
    }

    TaskStatus::from_db(&finished.status)
}
