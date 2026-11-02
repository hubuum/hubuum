use super::*;

pub(super) async fn build_export_artifact(
    runtime: &ExportRuntime,
    execution: ExportExecution,
    timings: ExportExecutionTimings,
) -> Result<ExportArtifact, ApiError> {
    match runtime.content_type() {
        ExportContentType::ApplicationJson => {
            build_json_export_artifact(runtime, execution, timings)
        }
        ExportContentType::TextPlain => {
            build_text_export_artifact(runtime, execution, timings, TextExportFormat::Plain).await
        }
        ExportContentType::TextHtml => {
            build_text_export_artifact(runtime, execution, timings, TextExportFormat::Html).await
        }
        ExportContentType::TextCsv => {
            build_text_export_artifact(runtime, execution, timings, TextExportFormat::Csv).await
        }
    }
}

pub(super) fn build_json_export_artifact(
    runtime: &ExportRuntime,
    execution: ExportExecution,
    timings: ExportExecutionTimings,
) -> Result<ExportArtifact, ApiError> {
    let response = ExportJsonResponse {
        items: execution.items,
        meta: execution.meta.clone(),
        warnings: execution.warnings.clone(),
    };

    enforce_json_output_limit(&response, &runtime.export)?;

    Ok(ExportArtifact {
        output: ExportArtifactOutput::Json(response),
        meta: execution.meta,
        warnings: execution.warnings,
        template_name: None,
        timings,
    })
}

pub(super) async fn build_text_export_artifact(
    runtime: &ExportRuntime,
    execution: ExportExecution,
    timings: ExportExecutionTimings,
    format: TextExportFormat,
) -> Result<ExportArtifact, ApiError> {
    let template = required_template(runtime, runtime.content_type())?;
    let context = export_template_context(&runtime.export, &execution);
    let max_output_bytes = runtime
        .export
        .limits
        .as_ref()
        .and_then(|limits| limits.max_output_bytes)
        .unwrap_or_else(configured_export_max_output_bytes);
    let (rendered, template_warnings) = render_template(
        template,
        &runtime.collection_templates,
        &context,
        runtime.content_type(),
        runtime.missing_data_policy,
        max_output_bytes,
    )
    .await?;
    let mut warnings = execution.warnings;
    warnings.extend(template_warnings);

    Ok(ExportArtifact {
        output: ExportArtifactOutput::Text {
            format,
            body: rendered,
        },
        meta: execution.meta,
        warnings,
        template_name: Some(template.name.clone()),
        timings,
    })
}

pub(super) fn required_template(
    runtime: &ExportRuntime,
    content_type: ExportContentType,
) -> Result<&ExportTemplate, ApiError> {
    runtime.template.as_ref().ok_or_else(|| {
        ApiError::BadRequest(format!(
            "Output type '{}' requires running an executable export template",
            content_type.as_mime()
        ))
    })
}
