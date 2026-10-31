use super::*;
use hubuum_artifacts::{ArtifactError, stage_producer};
use std::sync::Arc;

pub(super) async fn build_export_artifact(
    runtime: &ExportRuntime,
    execution: ExportExecution,
    timings: ExportExecutionTimings,
    output: &crate::services::artifacts::TaskOutput,
    availability: StorageOutputAvailability,
) -> Result<ExportArtifact, ApiError> {
    let content_type = runtime.content_type();
    let mut warnings = execution.warnings.clone();
    let meta = execution.meta.clone();
    let template_name = runtime
        .template
        .as_ref()
        .map(|template| template.name.clone());
    let (proof, ()) = if content_type == ExportContentType::ApplicationJson {
        #[derive(Serialize)]
        struct Response {
            items: hubuum_templates::TemplateRows,
            meta: ExportMeta,
            warnings: Vec<ExportWarning>,
        }
        let response = Response {
            items: execution.rows,
            meta: execution.meta,
            warnings: execution.warnings,
        };
        stage_producer(
            output.store.as_ref(),
            output.identity.clone(),
            output.limits,
            move |sink| {
                serde_json::to_writer(sink, &response)
                    .map_err(|error| ArtifactError::Backend(error.to_string()))
            },
        )
        .await
    } else {
        let template = runtime.template.as_ref().ok_or_else(|| {
            ApiError::BadRequest("Text output requires an executable template".into())
        })?;
        let context = export_template_context(&runtime.export, &execution);
        let rendered = render_template_spooled(
            template,
            &runtime.collection_templates,
            &context,
            Arc::new(execution.rows),
            runtime.missing_data_policy,
            output.limits.artifact_bytes() as usize,
        )
        .await?;
        warnings.extend(
            rendered
                .missing_values()
                .iter()
                .map(|missing| ExportWarning {
                    code: "template_missing_value".into(),
                    message: format!(
                        "Template '{}' rendered one or more missing values",
                        missing.template_name()
                    ),
                    path: missing.clone().into_path(),
                }),
        );
        // Rendering has reaped the child and released its input spool. Only the
        // bounded output file overlaps durable staging; neither is in memory.
        stage_producer(
            output.store.as_ref(),
            output.identity.clone(),
            output.limits,
            move |sink| {
                rendered
                    .copy_to(sink)
                    .map_err(|error| ArtifactError::Backend(error.to_string()))
            },
        )
        .await
    }
    .map_err(crate::services::artifacts::error)?;
    Ok(ExportArtifact {
        proof,
        content_type,
        meta,
        warnings,
        template_name,
        timings,
        availability,
    })
}
