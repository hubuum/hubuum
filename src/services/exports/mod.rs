mod query;
use query::*;
mod hydration;
use hydration::*;
mod render;
use render::*;
mod artifact;
use artifact::*;
mod execution;
pub(crate) use execution::execute_export_task;
mod submission;
use hubuum_domain::ClassId;
pub(crate) use submission::{ExportTaskSubmission, submit_export_task};
use submission::{prepare_export_runtime, resolve_template, validate_export_submission};
mod pagination;
use pagination::authorized_storage_page;

use crate::services::authentication::ExecutionPrincipal;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::config::{
    DEFAULT_EXPORT_MAX_OUTPUT_BYTES, DEFAULT_EXPORT_OUTPUT_RETENTION_HOURS,
    DEFAULT_EXPORT_STAGE_TIMEOUT_MS, DEFAULT_EXPORT_STORAGE_QUERY_BUDGET_MS,
    DEFAULT_EXPORT_TEMPLATE_MAX_OBJECTS, get_config,
};
use crate::errors::ApiError;
use crate::models::retention::FutureRetention;
use crate::models::search::{
    FilterField, ParsedQueryParam, QueryOptions, QueryParamsExt, SearchOperator,
    parse_query_parameter,
};
use crate::models::{
    ClassIdSet, Collection, CollectionExportTemplates, CollectionID, ExportContentType,
    ExportIncludeRelatedDirection, ExportIncludeRelatedQuery, ExportIncludeRelatedSort, ExportMeta,
    ExportMissingDataPolicy, ExportRequest, ExportTemplate, ExportTemplateID, ExportWarning,
    HubuumClassExpanded, HubuumClassRelation, HubuumObject, HubuumObjectID, HubuumObjectRelation,
    HubuumObjectWithPath, NewTaskEventRecord, Permissions, PermissionsList, PrincipalID,
    RELATED_INCLUDE_DEFAULT_LIMIT, RELATED_INCLUDE_DEFAULT_MAX_DEPTH, RelatedObjectForRootRow,
    RelatedObjectGraphRow, RelatedObjectIncludeRow, TaskKind, TaskRecord, TaskResultCounts,
    TaskStatus, TokenID, TokenScope, ValidatedExportScope,
};
use crate::observability::metrics;
use crate::pagination::page_limits_or_defaults;
use crate::permissions::{
    AuthorizationContext, AuthorizationMode, AuthzTarget, PermissionBackend, PermissionDecision,
    PermissionRequest, PrincipalRef, ResourceRef,
};
use crate::services::authorization_resources::{
    class_relation_authorization_resources, object_authorization_resources,
    object_relation_authorization_resources,
};
use crate::services::catalog as catalog_service;
use crate::services::relation_queries;
use crate::services::tasks::{
    ClaimedTask, TaskStateChange, TaskSubmission, append_task_event, complete_task,
    task_scope_snapshot, update_task_state,
};
use crate::storage::{
    ExecutionStorage, StorageExecutionScope, StorageExportTaskArtifact,
    StorageExportTaskArtifactContent, StorageQueryBudget, StorageTaskCompletionPayload,
    StorageTaskDurations, StorageTaskScopeSnapshot, storage_handle,
};
use crate::tasks::request_hash;
use crate::traits::{
    AuthzSubject, SelfAccessors, UserPermissions, scope_allows, scope_allows_resource,
};
use crate::utilities::exporting::render_template_spooled;

use crate::models::traits::check_if_object_in_class;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredExportTaskPayload {
    export: ExportRequest,
    template_id: Option<i32>,
}

struct ExportArtifact {
    proof: hubuum_artifacts::VerifiedArtifact,
    content_type: ExportContentType,
    meta: ExportMeta,
    warnings: Vec<ExportWarning>,
    template_name: Option<String>,
    timings: ExportExecutionTimings,
    expires_at: chrono::DateTime<chrono::Utc>,
}

struct ExportRuntime {
    export: ExportRequest,
    scope: ValidatedExportScope,
    missing_data_policy: ExportMissingDataPolicy,
    template: Option<ExportTemplate>,
    collection_templates: Vec<ExportTemplate>,
}

impl ExportRuntime {
    fn content_type(&self) -> ExportContentType {
        self.template
            .as_ref()
            .map_or(ExportContentType::ApplicationJson, |template| {
                template.content_type
            })
    }
}

struct ExportExecution {
    rows: hubuum_templates::TemplateRows,
    warnings: Vec<ExportWarning>,
    meta: ExportMeta,
    source: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Copy, Default)]
struct ExportExecutionTimings {
    total: Duration,
    query: Duration,
    hydration: Duration,
    render: Duration,
}

impl ExportExecutionTimings {
    fn total_millis(self) -> i32 {
        duration_to_millis_i32(self.total)
    }

    fn query_millis(self) -> i32 {
        duration_to_millis_i32(self.query)
    }

    fn hydration_millis(self) -> i32 {
        duration_to_millis_i32(self.hydration)
    }

    fn render_millis(self) -> i32 {
        duration_to_millis_i32(self.render)
    }
}

#[derive(Debug, Clone, Serialize)]
struct HydratedTemplateObject {
    id: i32,
    name: String,
    collection_id: i32,
    hubuum_class_id: i32,
    data: serde_json::Value,
    description: String,
    created_at: chrono::NaiveDateTime,
    updated_at: chrono::NaiveDateTime,
    path: Vec<i32>,
    path_objects: Vec<HydratedTemplatePathObject>,
    related: BTreeMap<String, Vec<HydratedTemplateObject>>,
    relation_labels: BTreeMap<String, String>,
    reachable: BTreeMap<String, Vec<HydratedTemplateObject>>,
    paths: BTreeMap<String, Vec<HydratedTemplateObject>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HydratedTemplatePathObject {
    id: i32,
    name: String,
    collection_id: i32,
    hubuum_class_id: i32,
}

#[derive(Clone, Copy)]
struct RelationHydrationPlan {
    depth_limit: i32,
    enabled_for_scope: bool,
}

struct ObjectNeighborhood {
    objects_by_id: BTreeMap<i32, HubuumObjectWithPath>,
    aliases_by_object_id: BTreeMap<i32, BTreeMap<String, Vec<i32>>>,
    class_relations_by_pair: BTreeMap<(i32, i32), crate::models::HubuumClassRelation>,
    class_names_by_id: BTreeMap<i32, String>,
}

struct HydrationBudget {
    max_objects: usize,
    hydrated_objects: usize,
    row_bytes: usize,
}

impl HydrationBudget {
    fn new(max_objects: usize) -> Self {
        Self {
            max_objects,
            hydrated_objects: 0,
            row_bytes: 0,
        }
    }

    fn remaining(&self) -> usize {
        self.max_objects.saturating_sub(self.hydrated_objects)
    }

    fn remaining_related_capacity(&self) -> Result<usize, ApiError> {
        if self.remaining() == 0 {
            return Err(ApiError::BadRequest(format!(
                "Hydrated template object limit exceeded ({} >= {})",
                self.hydrated_objects, self.max_objects
            )));
        }

        Ok(self.remaining().saturating_sub(1))
    }

    fn charge_object(&mut self, object: &HubuumObjectWithPath) -> Result<(), ApiError> {
        let mut writer = hubuum_budget::LimitedWriter::new(
            std::io::sink(),
            hubuum_budget::ByteLimit::new(
                hubuum_templates::MAX_SPOOL_ROW_BYTES.saturating_sub(self.row_bytes),
            ),
        );
        serde_json::to_writer(&mut writer, object)
            .map_err(|_| ApiError::PayloadTooLarge("Hydrated template row exceeds 4 MiB".into()))?;
        self.row_bytes += writer.budget().used();
        Ok(())
    }

    fn count_object(&mut self) -> Result<(), ApiError> {
        self.hydrated_objects = self.hydrated_objects.saturating_add(1);
        if self.hydrated_objects > self.max_objects {
            return Err(ApiError::BadRequest(format!(
                "Hydrated template object limit exceeded ({} > {})",
                self.hydrated_objects, self.max_objects
            )));
        }

        Ok(())
    }
}

// Reproduces the per-root capacity check the old per-root query path applied:
// `remaining_related_capacity()` reserves one slot for the root, the query fetched
// `cap + 1` rows, and a root over `cap` errored with the fetched count (`cap + 1`).
// Roots are processed in `items` order so the shared budget shrinks exactly as before.
fn take_related_within_budget(
    budget: &HydrationBudget,
    mut related: Vec<HubuumObjectWithPath>,
) -> Result<Vec<HubuumObjectWithPath>, ApiError> {
    let max_related_objects = budget.remaining_related_capacity()?;
    related.truncate(max_related_objects.saturating_add(1));
    if related.len() > max_related_objects {
        return Err(ApiError::BadRequest(format!(
            "Hydrated template object limit exceeded ({} related objects > {} remaining related capacity)",
            related.len(),
            max_related_objects
        )));
    }
    Ok(related)
}

#[derive(Debug, Clone)]
struct ReachableTemplateTarget {
    target_id: i32,
    path: Vec<i32>,
    remaining_depth: i32,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct ObjectGraphEdge {
    first_object_id: i32,
    second_object_id: i32,
}

impl ObjectGraphEdge {
    fn new(left: i32, right: i32) -> Result<Self, ApiError> {
        let left = HubuumObjectID::new(left)?.id();
        let right = HubuumObjectID::new(right)?.id();
        if left <= right {
            Ok(Self {
                first_object_id: left,
                second_object_id: right,
            })
        } else {
            Ok(Self {
                first_object_id: right,
                second_object_id: left,
            })
        }
    }
}

struct AuthorizedObjectGraph {
    object_ids: HashSet<i32>,
    edges: HashSet<ObjectGraphEdge>,
}

impl AuthorizedObjectGraph {
    fn allows_path(&self, path: &[i32]) -> Result<bool, ApiError> {
        if path.is_empty() {
            return Err(ApiError::InternalServerError(
                "Export graph query returned an empty object path".to_string(),
            ));
        }

        for object_id in path {
            let object_id = HubuumObjectID::new(*object_id)?.id();
            if !self.object_ids.contains(&object_id) {
                return Ok(false);
            }
        }
        for edge in path.windows(2) {
            if !self
                .edges
                .contains(&ObjectGraphEdge::new(edge[0], edge[1])?)
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn retain_allowed<T>(
        &self,
        candidates: Vec<T>,
        path: impl for<'candidate> Fn(&'candidate T) -> &'candidate [i32],
    ) -> Result<Vec<T>, ApiError> {
        let mut authorized = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            if self.allows_path(path(&candidate))? {
                authorized.push(candidate);
            }
        }
        Ok(authorized)
    }
}

struct AuthorizationCandidates<T>(Vec<(T, ResourceRef)>);

impl<T> AuthorizationCandidates<T> {
    fn new(candidates: Vec<T>, resources: Vec<ResourceRef>) -> Result<Self, ApiError> {
        if candidates.len() != resources.len() {
            return Err(ApiError::InternalServerError(
                "Export authorization candidate/resource count mismatch".to_string(),
            ));
        }
        Ok(Self(candidates.into_iter().zip(resources).collect()))
    }

    fn retain_scoped(self, scopes: Option<&TokenScope>) -> Self {
        Self(
            self.0
                .into_iter()
                .filter(|(_, resource)| scope_allows_resource(scopes, resource))
                .collect(),
        )
    }

    fn permission_requests(&self, permissions: &PermissionsList) -> Vec<PermissionRequest> {
        self.0
            .iter()
            .map(|(_, resource)| PermissionRequest {
                resource: resource.clone(),
                permissions: permissions.as_slice().to_vec(),
            })
            .collect()
    }

    fn into_values(self) -> Vec<T> {
        self.0.into_iter().map(|(candidate, _)| candidate).collect()
    }

    fn retain_decisions(self, decisions: Vec<PermissionDecision>) -> Result<Vec<T>, ApiError> {
        if decisions.len() != self.0.len() {
            return Err(ApiError::InternalServerError(
                "Permission backend returned an unexpected number of export decisions".to_string(),
            ));
        }
        Ok(self
            .0
            .into_iter()
            .zip(decisions)
            .filter_map(|((candidate, _), decision)| {
                (decision == PermissionDecision::Allow).then_some(candidate)
            })
            .collect())
    }
}

fn query_permissions(
    query: &QueryOptions,
    required: &[Permissions],
) -> Result<PermissionsList, ApiError> {
    let mut permissions = query.filters().permissions()?;
    permissions.ensure_contains(required);
    Ok(permissions)
}

enum ExportAuthorization<'a> {
    LocalSql {
        is_admin: bool,
    },
    External {
        backend: &'a dyn PermissionBackend,
        principal: PrincipalRef,
    },
}

impl ExportAuthorization<'_> {
    fn local_is_admin(&self) -> Option<bool> {
        match self {
            Self::LocalSql { is_admin } => Some(*is_admin),
            Self::External { .. } => None,
        }
    }

    fn external(&self) -> Option<(&dyn PermissionBackend, &PrincipalRef)> {
        match self {
            Self::LocalSql { .. } => None,
            Self::External { backend, principal } => Some((*backend, principal)),
        }
    }
}

fn retain_per_root<T>(
    candidates: Vec<T>,
    per_root_limit: i32,
    root_id: impl Fn(&T) -> i32,
) -> Vec<T> {
    let limit = usize::try_from(per_root_limit).unwrap_or(usize::MAX);
    let mut counts = HashMap::<i32, usize>::new();
    candidates
        .into_iter()
        .filter(|candidate| {
            let count = counts.entry(root_id(candidate)).or_default();
            if *count >= limit {
                return false;
            }
            *count += 1;
            true
        })
        .collect()
}

fn canonicalize_related_include_paths(
    mut candidates: Vec<RelatedObjectIncludeRow>,
    include: ExportIncludeRelatedQuery,
) -> Vec<RelatedObjectIncludeRow> {
    candidates.sort_by(|left, right| {
        left.root_object_id
            .cmp(&right.root_object_id)
            .then_with(|| left.descendant_object_id.cmp(&right.descendant_object_id))
            .then_with(|| left.depth.cmp(&right.depth))
            .then_with(|| left.path.cmp(&right.path))
    });
    candidates.dedup_by(|left, right| {
        left.root_object_id == right.root_object_id
            && left.descendant_object_id == right.descendant_object_id
    });
    candidates.sort_by(|left, right| {
        left.root_object_id
            .cmp(&right.root_object_id)
            .then_with(|| match include.sort {
                ExportIncludeRelatedSort::Path => left
                    .path
                    .cmp(&right.path)
                    .then_with(|| left.descendant_object_id.cmp(&right.descendant_object_id)),
                ExportIncludeRelatedSort::Name => left
                    .descendant_name
                    .cmp(&right.descendant_name)
                    .then_with(|| left.descendant_object_id.cmp(&right.descendant_object_id))
                    .then_with(|| left.path.cmp(&right.path)),
                ExportIncludeRelatedSort::CreatedAt => left
                    .descendant_created_at
                    .cmp(&right.descendant_created_at)
                    .then_with(|| left.descendant_object_id.cmp(&right.descendant_object_id))
                    .then_with(|| left.path.cmp(&right.path)),
            })
    });
    retain_per_root(candidates, include.limit, |candidate| {
        candidate.root_object_id
    })
}

fn canonicalize_related_object_paths(
    mut candidates: Vec<RelatedObjectForRootRow>,
    per_root_cap: i32,
) -> Vec<RelatedObjectForRootRow> {
    candidates.sort_by(|left, right| {
        left.root_object_id
            .cmp(&right.root_object_id)
            .then_with(|| left.descendant_object_id.cmp(&right.descendant_object_id))
            .then_with(|| left.depth.cmp(&right.depth))
            .then_with(|| left.path.cmp(&right.path))
    });
    candidates.dedup_by(|left, right| {
        left.root_object_id == right.root_object_id
            && left.descendant_object_id == right.descendant_object_id
    });
    retain_per_root(candidates, per_root_cap, |candidate| {
        candidate.root_object_id
    })
}

/// Permission-aware query capability for one export task.
///
/// Local authorization stays inside the existing SQL visibility queries. An
/// external permission backend loads unfiltered candidates and authorizes them
/// before applying the export limit, so pagination cannot hide later rows that
/// the principal is allowed to read.
struct PermissionAwareExport<'a, C: ?Sized, S: ?Sized> {
    backend: &'a C,
    subject: &'a S,
    scopes: Option<&'a TokenScope>,
    authorization: ExportAuthorization<'a>,
}

fn validate_export_include(
    export: &ExportRequest,
    scope: ValidatedExportScope,
) -> Result<(), ApiError> {
    let Some(include) = &export.include else {
        return Ok(());
    };

    let Some(related_objects) = &include.related_objects else {
        return Ok(());
    };

    if !related_objects.is_empty() && !matches!(scope, ValidatedExportScope::ObjectsInClass(_)) {
        return Err(ApiError::BadRequest(
            "include.related_objects is only supported for scope 'objects_in_class'".to_string(),
        ));
    }

    include.validate_related_objects()
}

fn add_truncation_warning(warnings: &mut Vec<ExportWarning>, truncated: bool) {
    if truncated {
        warnings.push(ExportWarning {
            code: "truncated".to_string(),
            message: "The export was truncated to the configured max_items limit".to_string(),
            path: None,
        });
    }
}

fn duration_to_millis_i32(duration: Duration) -> i32 {
    i32::try_from(duration.as_millis()).unwrap_or(i32::MAX)
}

/// The export-scoped backend read budget to apply to export queries,
/// or `None` when disabled (`export_storage_query_budget_ms() == 0`).
///
/// This is the in-flight backend query limit that complements the
/// post-completion wall-clock budget enforced by [`enforce_export_stage_timeout`].
fn export_query_budget() -> Option<StorageQueryBudget> {
    let milliseconds = get_config()
        .map(|config| config.export_storage_query_budget_ms())
        .unwrap_or(DEFAULT_EXPORT_STORAGE_QUERY_BUDGET_MS);
    StorageQueryBudget::from_millis(milliseconds)
}

/// Post-completion rejection guard for a export stage.
///
/// This is **not** an in-flight interrupt: it is called after a stage has already
/// finished and rejects the export if the stage took longer than the configured
/// budget. It bounds how long a stage is *accepted* to have taken, not how long
/// it is *allowed to run*. In-flight protection comes from the MiniJinja fuel
/// budget, `export_template_max_objects`, output byte caps, and the configured
/// storage query budgets.
fn enforce_export_stage_timeout(elapsed: Duration, stage_name: &str) -> Result<(), ApiError> {
    crate::tasks::control::checkpoint()?;
    let stage_timeout_ms = get_config()
        .map(|config| config.export_stage_timeout_ms)
        .unwrap_or(DEFAULT_EXPORT_STAGE_TIMEOUT_MS);
    if elapsed.as_millis() > u128::from(stage_timeout_ms) {
        return Err(ApiError::BadRequest(format!(
            "Export {stage_name} exceeded the configured time budget ({}ms > {}ms)",
            elapsed.as_millis(),
            stage_timeout_ms
        )));
    }
    Ok(())
}

fn log_export_stage_metrics(
    task_id: i32,
    runtime: &ExportRuntime,
    timings: ExportExecutionTimings,
) {
    tracing::info!(
        message = "Export execution timings recorded",
        task_id = task_id,
        scope = runtime.scope.kind().as_str(),
        content_type = runtime.content_type().as_mime(),
        template_name = runtime
            .template
            .as_ref()
            .map(|template| template.name.as_str()),
        total_duration_ms = timings.total_millis(),
        query_duration_ms = timings.query_millis(),
        hydration_duration_ms = timings.hydration_millis(),
        render_duration_ms = timings.render_millis()
    );
}

fn export_template_context(
    export: &ExportRequest,
    execution: &ExportExecution,
) -> serde_json::Value {
    json!({

        "meta": &execution.meta,
        "warnings": &execution.warnings,
        "request": export,
        "source": &execution.source,
    })
}

fn prepare_query_options(export: &ExportRequest) -> Result<QueryOptions, ApiError> {
    let mut query_options = parse_query_parameter(export.query.as_deref().unwrap_or_default())?;
    if query_options.cursor().is_some() {
        return Err(ApiError::BadRequest(
            "Exports do not support cursor pagination".to_string(),
        ));
    }

    validate_export_limits(export)?;

    let page_limits = page_limits_or_defaults();
    let configured_limit = export
        .limits
        .as_ref()
        .and_then(|limits| limits.max_items)
        .unwrap_or(page_limits.default_limit());
    let requested_limit = query_options.limit().unwrap_or(configured_limit);
    let effective_limit = page_limits.clamp(requested_limit.min(configured_limit));

    query_options.set_limit(Some(effective_limit.saturating_add(1)))?;
    Ok(query_options)
}

fn validate_export_limits(export: &ExportRequest) -> Result<(), ApiError> {
    let Some(limits) = &export.limits else {
        return Ok(());
    };

    if let Some(0) = limits.max_items {
        return Err(ApiError::BadRequest(
            "max_items must be greater than 0".to_string(),
        ));
    }

    if let Some(0) = limits.max_output_bytes {
        return Err(ApiError::BadRequest(
            "max_output_bytes must be greater than 0".to_string(),
        ));
    }

    if let Some(max_output_bytes) = limits.max_output_bytes {
        let server_max_output_bytes = configured_export_max_output_bytes();
        if max_output_bytes > server_max_output_bytes {
            return Err(ApiError::BadRequest(format!(
                "max_output_bytes ({max_output_bytes}) exceeds server maximum ({server_max_output_bytes})"
            )));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests;
