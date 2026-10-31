//! Authorized sampling, bounded inference, and durable report publication.
use hubuum_budget::{ByteLimit, LimitedWriter};
use std::io;

use chrono::{Duration, Utc};
use hubuum_domain::{ClassId, ObjectId};
use hubuum_storage_core::{StorageSchemaProfileArtifact, StorageTaskCompletionPayload};
use hubuum_task_core::{IdempotencyKey, TaskExecutionContext};
use json_schema_profiler::{
    Document, EvaluationOptions, EvaluationStatus, Profiler, SchemaOptions, Suggestion, ValueLimits,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::errors::ApiError;
use crate::extractors::Authenticated;
use crate::models::class_fields::FieldDiscoveryTruncation;
use crate::models::schema_profiling::{
    ProfileSuggestionStatus, SchemaProfileReport, SchemaProfileRequest,
};
use crate::models::{
    HubuumClass, HubuumObject, NewTaskEventRecord, Permissions, TaskKind, TaskRecord,
    TaskResultCounts, TaskStatus, TokenScope,
};
use crate::permissions::visibility::authorize_all_candidates;
use crate::permissions::{AppContext, PrincipalRef};
use crate::restores::MaintenanceActivityGuard;
use crate::services::authentication::ExecutionPrincipal;
use crate::services::authorization_resources::schema_compliance_authorization_resources;
use crate::services::class_fields::AuthorizedObjectSample;
use crate::services::tasks::{
    ClaimedTask, TaskStateChange, TaskSubmission, complete_task, submit_task, task_scope_snapshot,
    update_task_state,
};
use crate::tasks::{control, request_hash};
use crate::traits::{SelfAccessors, UserPermissions, scope_allows};

#[derive(Serialize, Deserialize)]
struct ProfileTaskPayload {
    class_id: ClassId,
    options: SchemaProfileRequest,
}

pub(crate) async fn submit(
    context: &AppContext,
    requestor: &Authenticated,
    class: &HubuumClass,
    options: SchemaProfileRequest,
    key: Option<IdempotencyKey>,
) -> Result<TaskRecord, ApiError> {
    let payload = serde_json::to_value(ProfileTaskPayload {
        class_id: ClassId::new(class.id)?,
        options,
    })?;
    let hash = request_hash(&payload)?;
    submit_task(
        context,
        TaskSubmission::new(
            TaskKind::SchemaProfiling,
            requestor.principal.id(),
            payload,
            1,
        )
        .idempotency_key(key)
        .request_hash(Some(hash))
        .scope_snapshot(task_scope_snapshot(
            Some(requestor.token_meta.id()),
            requestor.scopes(),
        )),
    )
    .await
}

pub(crate) async fn execute(
    context: &AppContext,
    task: &ClaimedTask,
    principal: &ExecutionPrincipal,
    scopes: Option<&TokenScope>,
) -> Result<TaskStatus, ApiError> {
    let payload: ProfileTaskPayload = serde_json::from_value(
        task.request_payload
            .clone()
            .ok_or_else(|| ApiError::BadRequest("Schema profiling payload is missing".into()))?,
    )?;
    let class = payload.class_id.instance(context).await?;
    crate::can!(context, principal, scopes, [Permissions::ReadClass], class);
    update_task_state(
        context,
        task,
        TaskStateChange::new(TaskStatus::Running, TaskResultCounts::default())
            .started_at(task.started_at),
    )
    .await?;
    control::checkpoint()?;
    let sample = AuthorizedObjectSample::load(
        context,
        principal,
        scopes,
        payload.class_id,
        payload.options.sample_limit(),
    )
    .await?;
    control::checkpoint()?;
    let execution = control::execution_context();
    let activity = MaintenanceActivityGuard::begin();
    // Await completion, including cooperative stops, before releasing the task lease.
    let output = tokio::task::spawn_blocking(move || {
        let _activity = activity;
        profile_sample(payload, sample, execution)
    })
    .await
    .map_err(|_| ApiError::InternalServerError("Schema profiling worker failed".into()))??;
    control::checkpoint()?;
    let artifact = StorageSchemaProfileArtifact::new(output, Utc::now() + Duration::hours(24))
        .map_err(|error| ApiError::BadRequest(error.to_string()))?;
    let finished = complete_task(
        context,
        task,
        TaskStateChange::new(
            TaskStatus::Succeeded,
            TaskResultCounts::from_outcomes(1, 0)?,
        )
        .summary("Schema profiling report ready")
        .started_at(task.started_at),
        NewTaskEventRecord {
            event_type: "succeeded".into(),
            message: "Schema profiling report ready".into(),
            data: None,
        },
        StorageTaskCompletionPayload::SchemaProfiling(artifact),
    )
    .await?;
    TaskStatus::from_db(&finished.status)
}

fn profile_sample(
    payload: ProfileTaskPayload,
    sample: AuthorizedObjectSample,
    execution: Option<TaskExecutionContext>,
) -> Result<Value, ApiError> {
    let (objects, truncation) = sample.into_parts();
    let object_ids: Vec<_> = objects.iter().map(|object| object.id).collect();
    let report = infer(
        payload.class_id,
        payload.options,
        &objects,
        truncation,
        execution.as_ref(),
    )?;
    Ok(json!({"report": report, "object_ids": object_ids}))
}

fn infer(
    class: ClassId,
    options: SchemaProfileRequest,
    objects: &[HubuumObject],
    truncation: Option<FieldDiscoveryTruncation>,
    execution: Option<&TaskExecutionContext>,
) -> Result<SchemaProfileReport, ApiError> {
    let check = || -> Result<(), ApiError> {
        if let Some(execution) = execution {
            execution.check()?;
        }
        Ok(())
    };
    let mut report = SchemaProfileReport {
        class_id: class.id(),
        sampled: true,
        sampled_objects: objects.len(),
        effective_sample_limit: options.sample_limit().get(),
        truncation,
        options: options.clone().into(),
        status: ProfileSuggestionStatus::Insufficient,
        schema: None,
        profile: None,
        findings: vec![],
        replay: None,
        sample_valid: None,
    };
    if objects.is_empty() {
        return Ok(report);
    }
    let mut profiler = Profiler::new(options.profiler_options());
    let mut input_budget = LimitedWriter::new(io::sink(), ByteLimit::new(32 * 1024 * 1024));
    for object in objects {
        check()?;
        serde_json::to_writer(&mut input_budget, &object.data).map_err(|_| {
            ApiError::BadRequest(
                "Schema profiling sample exceeds 32 MiB; lower sample_limit".into(),
            )
        })?;
        profiler.observe(&object.data).map_err(profile_error)?;
    }
    check()?;
    let profile = profiler.finish().map_err(profile_error)?;
    let suggestion = profile
        .suggest_with(options.suggestion_options())
        .map_err(profile_error)?;
    report.profile = Some(serde_json::to_value(&profile)?);
    let blocked = matches!(suggestion, Suggestion::Blocked(_));
    match suggestion {
        Suggestion::Candidate(candidate) => {
            check()?;
            let compiled = candidate
                .compile(SchemaOptions::default())
                .map_err(profile_error)?;
            let replay = compiled
                .try_evaluate(
                    objects.iter().map(|object| {
                        check()?;
                        Ok::<_, ApiError>(Document::new(&object.data))
                    }),
                    EvaluationOptions::default()
                        .with_document_limit(options.sample_limit().get())
                        .with_value_limits(
                            ValueLimits::new(100_000, 32, 32 * 1024 * 1024)
                                .map_err(profile_error)?,
                        )
                        .with_report_bytes(512 * 1024)
                        .map_err(profile_error)?,
                )
                .map_err(|error| error.into_parts().1)?;
            check()?;
            if !matches!(replay.status(), EvaluationStatus::Complete) {
                return Err(profile_error("candidate replay did not complete"));
            }
            report.status = ProfileSuggestionStatus::Candidate;
            report.schema = Some(candidate.schema().clone());
            report.findings = candidate
                .findings()
                .iter()
                .map(serde_json::to_value)
                .collect::<Result<_, _>>()?;
            report.sample_valid = Some(replay.all_valid());
            report.replay = Some(serde_json::to_value(replay)?);
        }
        Suggestion::Blocked(findings) | Suggestion::Insufficient(findings) => {
            report.status = if blocked {
                ProfileSuggestionStatus::Blocked
            } else {
                ProfileSuggestionStatus::Insufficient
            };
            report.findings = findings
                .iter()
                .map(serde_json::to_value)
                .collect::<Result<_, _>>()?;
        }
    }
    Ok(report)
}
fn profile_error(error: impl std::fmt::Display) -> ApiError {
    ApiError::BadRequest(format!("Schema profiling failed: {error}"))
}
/// A report is returned only while every sampled object is still readable.
pub(crate) async fn authorize_output(
    context: &AppContext,
    requestor: &Authenticated,
    task: &TaskRecord,
    output: &Value,
) -> Result<(), ApiError> {
    if let Some(scopes) = requestor.scopes()
        && (!task.submitted_token_scoped
            || !scopes.covers(&TokenScope::from_snapshot_json(
                &task.submitted_token_scopes,
            )?))
    {
        return Err(ApiError::Forbidden(
            "Profiling output requires a token covering the submitting token's scope".into(),
        ));
    }
    let ids: Vec<ObjectId> = serde_json::from_value(output["object_ids"].clone())?;
    if ids.is_empty() {
        return Ok(());
    }
    let permissions = vec![Permissions::ReadCollection, Permissions::ReadObject];
    if !scope_allows(requestor.scopes(), &permissions) {
        return Err(ApiError::Forbidden("Permission denied".into()));
    }
    let expected = ids.len();
    let resources =
        schema_compliance_authorization_resources(context, ids.into_iter().map(|id| id.id()))
            .await?;
    if resources.len() != expected {
        return Err(ApiError::NotFound(
            "A sampled object no longer exists".into(),
        ));
    }
    let principal = PrincipalRef::load(context, &requestor.principal).await?;
    let authorized = authorize_all_candidates(
        context.permission_backend(),
        &principal,
        resources.into_values().collect(),
        requestor.scopes(),
        permissions,
        Clone::clone,
    )
    .await?;
    if authorized.len() != expected {
        return Err(ApiError::Forbidden(
            "Permission denied for a sampled object".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::ResourceRevision;
    use rstest::rstest;
    fn object(data: Value) -> HubuumObject {
        HubuumObject {
            id: 1,
            name: "sample".into(),
            collection_id: 1,
            hubuum_class_id: 1,
            data,
            description: String::new(),
            created_at: Utc::now().naive_utc(),
            updated_at: Utc::now().naive_utc(),
            revision: ResourceRevision::new(1).unwrap(),
        }
    }
    fn report(options: Value, documents: Vec<Value>) -> Value {
        let objects: Vec<_> = documents.into_iter().map(object).collect();
        serde_json::to_value(
            infer(
                ClassId::new(1).unwrap(),
                serde_json::from_value(options).unwrap(),
                &objects,
                None,
                None,
            )
            .unwrap(),
        )
        .unwrap()
    }
    #[rstest]
    #[case("strict", "blocked")]
    #[case("balanced", "candidate")]
    #[case("expansive", "candidate")]
    fn mixed_type_inference_obeys_the_selected_preset(#[case] preset: &str, #[case] status: &str) {
        let report = report(
            json!({"preset":preset}),
            vec![json!({"size":4}), json!({"size":"4"})],
        );
        assert_eq!(report["status"], status);
        if status == "candidate" {
            assert_eq!(report["sample_valid"], true);
        }
    }
    #[test]
    fn empty_sample_reports_insufficient_evidence() {
        let report = report(json!({}), vec![]);
        assert_eq!(report["status"], "insufficient");
        assert!(report["schema"].is_null());
    }
    #[test]
    fn missing_and_null_properties_preserve_distinct_evidence() {
        let report = report(
            json!({"preset":"strict"}),
            vec![
                json!({"nullable":null,"sometimes":1}),
                json!({"nullable":3}),
            ],
        );
        assert_eq!(report["schema"]["required"], json!(["nullable"]));
        assert_eq!(report["sample_valid"], true);
    }
    #[test]
    fn inference_preserves_arbitrary_precision_number_classification() {
        let value: Value =
            serde_json::from_str("{\"value\":1.00000000000000000000000000000000001}").unwrap();
        let report = report(json!({}), vec![value]);
        assert_eq!(report["schema"]["properties"]["value"]["type"], "number");
    }
    #[rstest]
    #[case::at_number_limit(1024, true)]
    #[case::over_number_limit(1025, false)]
    #[case::oversized_number(1100, false)]
    fn candidate_requires_complete_replay(
        #[case] digits: usize,
        #[case] complete: bool,
        #[values(0, 1)] preceding_documents: usize,
    ) {
        let mut objects = vec![object(json!({"value":1})); preceding_documents];
        objects.push(object(
            serde_json::from_str(&format!("{{\"value\":{}}}", "1".repeat(digits))).unwrap(),
        ));
        let result = infer(
            ClassId::new(1).unwrap(),
            serde_json::from_value(json!({})).unwrap(),
            &objects,
            None,
            None,
        );
        if complete {
            let report = result.unwrap();
            assert_eq!(report.sample_valid, Some(true));
            assert_eq!(report.replay.unwrap()["processed"], objects.len());
        } else {
            assert!(matches!(result, Err(ApiError::BadRequest(message))
                if message == "Schema profiling failed: candidate replay did not complete"));
        }
    }
    #[test]
    fn profiling_honors_a_stop_before_observing_data() {
        let (execution, stop) =
            TaskExecutionContext::new(std::time::Duration::from_secs(5)).unwrap();
        stop.request_stop(hubuum_task_core::TaskStopReason::Cancelled);
        let error = infer(
            ClassId::new(1).unwrap(),
            serde_json::from_value(json!({})).unwrap(),
            &[object(json!({"x":1}))],
            None,
            Some(&execution),
        )
        .err()
        .unwrap();
        assert!(matches!(error, ApiError::TaskStopped(_)));
    }
}
