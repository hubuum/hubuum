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

    let max_bytes = runtime
        .export
        .limits
        .as_ref()
        .and_then(|limits| limits.max_output_bytes)
        .unwrap_or_else(configured_export_max_output_bytes);
    let retention = get_config()
        .map(|config| config.export_output_retention_hours)
        .unwrap_or(DEFAULT_EXPORT_OUTPUT_RETENTION_HOURS);
    let expires_at = FutureRetention::from_hours(retention, "export_output_retention_hours")
        .and_then(|retention| retention.expires_at(chrono::Utc::now().naive_utc()))
        .map_err(ApiError::BadRequest)?
        .and_utc();
    let spool_bytes = crate::services::artifacts::spool_limit();
    let output = crate::services::artifacts::TaskOutput::begin(
        backend,
        task,
        max_bytes,
        spool_bytes.max(max_bytes as u64),
        expires_at,
    )
    .await?;
    let result = async {
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
        let artifact =
            build_export_artifact(&runtime, execution, timings, &output, expires_at).await?;
        crate::tasks::control::checkpoint()?;
        let mut timings = artifact.timings;
        if let Err(error) =
            enforce_export_stage_timeout(render_metric.elapsed(), "template rendering")
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
        if let Some(template) = &runtime.template {
            crate::can!(
                backend,
                subject,
                scopes,
                [Permissions::ReadTemplate],
                template
            );
        }
        finish_export_task(pool, task, artifact, total_metric).await
    }
    .await;
    if result.is_err() || matches!(result, Ok(TaskStatus::Cancelled)) {
        let _ = output.store.abort(&output.identity).await;
    }
    result
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
    if runtime.template.is_some() {
        append_task_event(
            pool,
            task,
            NewTaskEventRecord {
                event_type: "running".to_string(),
                message: "Hydrating relation-aware template context".to_string(),
                data: None,
            },
        )
        .await?;
    }
    let query_metric =
        metrics::export_phase_timer(metrics::ExportMetricPhase::Query, metric_template_id);
    let mut rows = hubuum_templates::TemplateRows::new(crate::services::artifacts::spool_limit())
        .map_err(|error| ApiError::PayloadTooLarge(error.to_string()))?;
    let max_items = query_options.limit().unwrap_or(1).saturating_sub(1).max(1);
    normalize_page_order(runtime.scope, &mut query_options)?;
    let mut collected = 0;
    let mut warnings = Vec::new();
    let mut source = None;
    let mut related_items = Vec::new();
    let mut related_bytes = 0;
    let mut hydration_budget = HydrationBudget::new(max_hydrated_template_objects());
    let truncated = loop {
        crate::tasks::control::checkpoint()?;
        let page_size = 4.min(max_items - collected);
        query_options.set_limit(Some(page_size + 1))?;
        let (mut items, page_warnings, more) = export_storage
            .run_in_scope(
                StorageExecutionScope::default().with_query_budget(query_budget),
                execute_scope(exporter, runtime.scope, query_options.clone()),
            )
            .await?;
        let continuation = items
            .last()
            .map(|last| page_continuation(runtime.scope, last, &query_options))
            .transpose()?;
        collected += items.len();
        warnings.extend(page_warnings);
        export_storage
            .run_in_scope(
                StorageExecutionScope::default().with_query_budget(query_budget),
                apply_export_includes(exporter, &runtime.export, &mut items),
            )
            .await?;
        if runtime.template.is_some()
            && matches!(runtime.scope, ValidatedExportScope::RelatedObjects { .. })
        {
            // Related-object templates expose one aggregate source graph. Bound
            // that graph explicitly; ordinary exports never retain prior pages.
            for item in items {
                related_bytes += encoded_row_size(&item)?;
                if related_bytes > 8 * 1024 * 1024 {
                    return Err(ApiError::PayloadTooLarge(
                        "Related template source exceeds 8 MiB".into(),
                    ));
                }
                related_items.push(item);
            }
        } else if runtime.template.is_some() {
            let (hydrated, _) = export_storage
                .run_in_scope(
                    StorageExecutionScope::default().with_query_budget(query_budget),
                    build_template_page(
                        exporter,
                        runtime,
                        &items,
                        relation_hydration,
                        &mut hydration_budget,
                    ),
                )
                .await?;
            for item in hydrated {
                rows.push(&item)
                    .map_err(|error| ApiError::PayloadTooLarge(error.to_string()))?;
            }
        } else {
            for item in items {
                rows.push(&item)
                    .map_err(|error| ApiError::PayloadTooLarge(error.to_string()))?;
            }
        }
        enforce_export_stage_timeout(query_metric.elapsed(), "query execution and hydration")?;
        if !more || collected == max_items {
            break more;
        }
        query_options.set_continuation(continuation.ok_or_else(|| {
            ApiError::InternalServerError("Export pagination did not advance".into())
        })?);
    };
    if runtime.template.is_some()
        && matches!(runtime.scope, ValidatedExportScope::RelatedObjects { .. })
    {
        let (hydrated, root) = export_storage
            .run_in_scope(
                StorageExecutionScope::default().with_query_budget(query_budget),
                build_template_page(
                    exporter,
                    runtime,
                    &related_items,
                    relation_hydration,
                    &mut hydration_budget,
                ),
            )
            .await?;
        for item in hydrated {
            rows.push(&item)
                .map_err(|error| ApiError::PayloadTooLarge(error.to_string()))?;
        }
        source = root;
    }
    timings.query = query_metric.finish(metrics::ExportMetricOutcome::Success);
    add_truncation_warning(&mut warnings, truncated);
    let execution = ExportExecution {
        meta: ExportMeta {
            count: rows.len() as usize,
            truncated,
            scope: runtime.export.scope.clone(),
            content_type: runtime.content_type(),
        },
        rows,
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
    let metric_content_type = artifact.content_type.as_mime();
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
                "content_type": artifact.content_type.as_mime(),
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

fn encoded_row_size(row: &serde_json::Value) -> Result<usize, ApiError> {
    let mut sink = hubuum_budget::LimitedWriter::new(
        std::io::sink(),
        hubuum_budget::ByteLimit::new(hubuum_templates::MAX_SPOOL_ROW_BYTES),
    );
    serde_json::to_writer(&mut sink, row)
        .map_err(|_| ApiError::PayloadTooLarge("Export row exceeds 4 MiB".into()))?;
    Ok(sink.budget().used())
}

fn normalize_page_order(
    scope: ValidatedExportScope,
    options: &mut QueryOptions,
) -> Result<(), ApiError> {
    use crate::pagination::normalized_sorts;
    let sorts = match scope {
        ValidatedExportScope::Collections => normalized_sorts::<Collection>(options.sort())?,
        ValidatedExportScope::Classes => normalized_sorts::<HubuumClassExpanded>(options.sort())?,
        ValidatedExportScope::ObjectsInClass(_) => {
            normalized_sorts::<HubuumObject>(options.sort())?
        }
        ValidatedExportScope::ClassRelations => {
            normalized_sorts::<HubuumClassRelation>(options.sort())?
        }
        ValidatedExportScope::ObjectRelations => {
            normalized_sorts::<HubuumObjectRelation>(options.sort())?
        }
        ValidatedExportScope::RelatedObjects { .. } => {
            normalized_sorts::<HubuumObjectWithPath>(options.sort())?
        }
    };
    let mut prepared = sorts;
    let tie_breaker = (prepared.len() > hubuum_query::MAX_QUERY_SORT_FIELDS)
        .then(|| prepared.pop())
        .flatten();
    options.set_sort(prepared.try_into()?);
    if let Some(sort) = tie_breaker {
        options.sort_mut().append_tie_breaker(sort)?;
    }
    Ok(())
}

fn page_continuation(
    scope: ValidatedExportScope,
    item: &serde_json::Value,
    options: &QueryOptions,
) -> Result<hubuum_query::QueryContinuation, ApiError> {
    use crate::pagination::{CursorBoundary, CursorPaginated};
    fn boundary<T: CursorPaginated + serde::de::DeserializeOwned>(
        item: &serde_json::Value,
        options: &QueryOptions,
    ) -> Result<hubuum_query::QueryContinuation, ApiError> {
        CursorBoundary::from_item(&serde_json::from_value::<T>(item.clone())?, options.sort())?
            .into_continuation()
    }
    match scope {
        ValidatedExportScope::Collections => boundary::<Collection>(item, options),
        ValidatedExportScope::Classes => boundary::<HubuumClassExpanded>(item, options),
        ValidatedExportScope::ObjectsInClass(_) => boundary::<HubuumObject>(item, options),
        ValidatedExportScope::ClassRelations => boundary::<HubuumClassRelation>(item, options),
        ValidatedExportScope::ObjectRelations => boundary::<HubuumObjectRelation>(item, options),
        ValidatedExportScope::RelatedObjects { .. } => {
            boundary::<HubuumObjectWithPath>(item, options)
        }
    }
}
