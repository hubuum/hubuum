use super::*;

pub(super) fn artifact_to_storage(
    artifact: ExportArtifact,
) -> Result<StorageExportTaskArtifact, ApiError> {
    let retention_hours = get_config()
        .map(|config| config.export_output_retention_hours)
        .unwrap_or(DEFAULT_EXPORT_OUTPUT_RETENTION_HOURS);
    let output_expires_at =
        FutureRetention::from_hours(retention_hours, "export_output_retention_hours")
            .and_then(|retention| retention.expires_at(chrono::Utc::now().naive_utc()))
            .map_err(ApiError::BadRequest)?;
    let content_type = artifact.output.content_type();
    let content = match artifact.output {
        ExportArtifactOutput::Json(output) => {
            StorageExportTaskArtifactContent::Json(serde_json::to_value(output)?)
        }
        ExportArtifactOutput::Text { body, .. } => StorageExportTaskArtifactContent::Text(body),
    };
    StorageExportTaskArtifact::builder(
        content_type.as_mime(),
        content,
        serde_json::to_value(&artifact.meta)?,
        serde_json::to_value(&artifact.warnings)?,
        output_expires_at.and_utc(),
    )
    .template_name(artifact.template_name)
    .warning_state(
        i32::try_from(artifact.warnings.len()).unwrap_or(i32::MAX),
        artifact.meta.truncated,
    )
    .durations(
        StorageTaskDurations::try_new(
            artifact.timings.total_millis(),
            artifact.timings.query_millis(),
            artifact.timings.hydration_millis(),
            artifact.timings.render_millis(),
        )
        .map_err(|error| ApiError::from(error.into_request_error()))?,
    )
    .try_build()
    .map_err(Into::into)
}
