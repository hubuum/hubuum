use super::*;

pub(super) fn artifact_to_storage(
    artifact: ExportArtifact,
) -> Result<StorageExportTaskArtifact, ApiError> {
    let content_type = artifact.content_type;
    let content = StorageExportTaskArtifactContent::Verified(artifact.proof);
    StorageExportTaskArtifact::builder(
        content_type.as_mime(),
        content,
        serde_json::to_value(&artifact.meta)?,
        serde_json::to_value(&artifact.warnings)?,
        artifact.expires_at,
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
