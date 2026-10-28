use super::*;

pub(crate) struct ExportTaskSubmission {
    export: ExportRequest,
    template: Option<ExportTemplate>,
    idempotency_key: Option<IdempotencyKey>,
    scope_snapshot: StorageTaskScopeSnapshot,
}

impl ExportTaskSubmission {
    pub(crate) fn for_token(
        export: ExportRequest,
        token_id: TokenID,
        scopes: Option<&TokenScope>,
    ) -> Self {
        Self {
            export,
            template: None,
            idempotency_key: None,
            scope_snapshot: task_scope_snapshot(Some(token_id), scopes),
        }
    }

    pub(crate) fn template(mut self, template: ExportTemplate) -> Self {
        self.template = Some(template);
        self
    }

    pub(crate) fn idempotency_key(mut self, idempotency_key: Option<IdempotencyKey>) -> Self {
        self.idempotency_key = idempotency_key;
        self
    }
}

pub(crate) async fn submit_export_task<S: AuthzSubject>(
    pool: &impl crate::storage::StorageContext,
    subject: &S,
    submission: ExportTaskSubmission,
) -> Result<TaskRecord, ApiError> {
    let ExportTaskSubmission {
        export,
        template,
        idempotency_key,
        scope_snapshot,
    } = submission;
    let task_payload = StoredExportTaskPayload {
        export,
        template_id: template.as_ref().map(|template| template.id),
    };
    let payload = serde_json::to_value(&task_payload)?;
    let hash = request_hash(&payload)?;

    let runtime = prepare_export_runtime(pool, task_payload.export.clone(), template).await?;
    validate_export_submission(&runtime)?;
    let task_payload = runtime_to_task_payload(&runtime)?;

    find_or_create_export_task(
        pool,
        PrincipalID::new(subject.principal_id())?,
        scope_snapshot,
        idempotency_key,
        serde_json::to_value(task_payload)?,
        hash,
    )
    .await
}

pub(super) async fn prepare_export_runtime(
    pool: &impl crate::storage::StorageContext,
    export: ExportRequest,
    template: Option<ExportTemplate>,
) -> Result<ExportRuntime, ApiError> {
    let scope = export.scope.validate()?;
    validate_export_include(&export, scope)?;

    let collection_templates = match &template {
        Some(template) => {
            CollectionID::new(template.collection_id)?
                .export_templates(pool, None)
                .await?
        }
        None => Vec::new(),
    };
    Ok(ExportRuntime {
        scope,
        missing_data_policy: export
            .missing_data_policy
            .unwrap_or(ExportMissingDataPolicy::Strict),
        template,
        collection_templates,
        export,
    })
}

pub(super) async fn resolve_template(
    pool: &impl crate::storage::StorageContext,
    template_id: Option<i32>,
) -> Result<Option<ExportTemplate>, ApiError> {
    let Some(template_id) = template_id else {
        return Ok(None);
    };

    ExportTemplateID::new(template_id)?
        .instance(pool)
        .await
        .map(Some)
}

pub(super) fn validate_export_submission(runtime: &ExportRuntime) -> Result<(), ApiError> {
    if runtime.export.relation_context.is_some()
        && runtime
            .export
            .include
            .as_ref()
            .and_then(|include| include.related_objects.as_ref())
            .is_some_and(|related_objects| !related_objects.is_empty())
    {
        return Err(ApiError::BadRequest(
            "include.related_objects cannot be combined with relation_context".to_string(),
        ));
    }

    let mut query_options = prepare_query_options(&runtime.export)?;
    let _ = resolve_relation_hydration_plan(runtime, &mut query_options)?;
    Ok(())
}

pub(super) fn runtime_to_task_payload(
    runtime: &ExportRuntime,
) -> Result<StoredExportTaskPayload, ApiError> {
    validate_export_submission(runtime)?;
    let mut export = runtime.export.clone();
    let max_items = prepare_query_options(&export)?
        .limit()
        .map(|limit| limit.saturating_sub(1));
    let max_output_bytes = export
        .limits
        .as_ref()
        .and_then(|limits| limits.max_output_bytes)
        .unwrap_or_else(configured_export_max_output_bytes);
    export.limits = Some(crate::models::ExportLimits {
        max_items,
        max_output_bytes: Some(max_output_bytes),
    });
    export.missing_data_policy = Some(runtime.missing_data_policy);
    Ok(StoredExportTaskPayload {
        export,
        template_id: runtime.template.as_ref().map(|template| template.id),
    })
}

pub(super) async fn find_or_create_export_task(
    pool: &impl crate::storage::StorageContext,
    submitted_by: PrincipalID,
    snapshot: StorageTaskScopeSnapshot,
    idempotency_key: Option<IdempotencyKey>,
    payload: serde_json::Value,
    request_hash_value: String,
) -> Result<TaskRecord, ApiError> {
    submit_task(
        pool,
        TaskSubmission::new(TaskKind::Export, submitted_by, payload, 1)
            .idempotency_key(idempotency_key)
            .request_hash(Some(request_hash_value))
            .scope_snapshot(snapshot),
    )
    .await
}
