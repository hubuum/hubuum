use std::collections::{BTreeMap, btree_map::Entry};

use chrono::{DateTime, Utc};
use hubuum_domain::{CollectionId, ObjectId, SchemaReference, TaskId};
use hubuum_storage_core::{
    SchemaEvolutionStorage, StorageSchemaRepairReport, StorageSchemaRepairReportWrite,
    StorageSchemaReportBudget, StorageSchemaWork,
};
use hubuum_templates::{MissingDataPolicy, TemplateAutoEscape, TemplateExecution, TemplateLimits};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    config::{
        DEFAULT_EXPORT_MAX_OUTPUT_BYTES, DEFAULT_EXPORT_TEMPLATE_FUEL,
        DEFAULT_EXPORT_TEMPLATE_RECURSION_LIMIT, get_config,
    },
    errors::ApiError,
    models::{ExportContentType, ExportMissingDataPolicy, ExportTemplate, schema_evolution::*},
    storage::{StorageContext, storage_handle},
    utilities::exporting::render_template,
};

const MAX_CONTEXT_BYTES: usize = 4 * 1024 * 1024;
const CONTENT: &str = include_str!("repair_content.html");
const DOCUMENT_START: &str = "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta http-equiv=\"Content-Security-Policy\" content=\"default-src 'none'; style-src 'unsafe-inline'; img-src data:; base-uri 'none'; form-action 'none'\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"><title>Schema repair report</title><style>body{font:16px system-ui,sans-serif;line-height:1.5;max-width:1100px;margin:2rem auto;padding:0 1rem;color:#182230}h1,h2,h3{line-height:1.2}article{border-top:1px solid #b9c3ce;margin-top:2rem;padding-top:1rem}code,pre{overflow-wrap:anywhere;white-space:pre-wrap}table{border-collapse:collapse}th,td{padding:.4rem .8rem;text-align:left;border:1px solid #ccd3dc}.notice{background:#fff3d3;padding:1rem}.issue{margin:1rem 0;padding-left:1rem;border-left:3px solid #a33721}dt{font-weight:bold}dd{margin-bottom:.5rem}a{color:#12549a}</style></head><body>";
const DOCUMENT_END: &str = "</body></html>";

pub struct RepairReportLayout {
    template: ExportTemplate,
    siblings: Vec<ExportTemplate>,
}

impl RepairReportLayout {
    pub fn try_new(
        template: ExportTemplate,
        siblings: Vec<ExportTemplate>,
    ) -> Result<Self, ApiError> {
        if template.content_type != ExportContentType::TextHtml
            || siblings
                .iter()
                .any(|sibling| sibling.collection_id != template.collection_id)
        {
            return Err(ApiError::BadRequest(
                "Repair report layouts require HTML and templates from the same collection".into(),
            ));
        }
        Ok(Self { template, siblings })
    }
}

pub struct RepairReportGeneration {
    work: StorageSchemaWork,
    class_name: String,
    authorized_collection: CollectionId,
    request: SchemaRepairReportRequest,
    layout: Option<RepairReportLayout>,
}

impl RepairReportGeneration {
    pub fn new(
        work: StorageSchemaWork,
        class_name: String,
        authorized_collection: CollectionId,
        request: SchemaRepairReportRequest,
    ) -> Self {
        Self {
            work,
            class_name,
            authorized_collection,
            request,
            layout: None,
        }
    }

    pub fn with_layout(mut self, layout: Option<RepairReportLayout>) -> Self {
        self.layout = layout;
        self
    }
}

pub async fn generate_repair_report(
    context: &impl StorageContext,
    generation: RepairReportGeneration,
) -> Result<StorageSchemaRepairReport, ApiError> {
    let RepairReportGeneration {
        work,
        class_name,
        authorized_collection,
        request,
        layout,
    } = generation;
    let storage = storage_handle(context);
    let task_id = work.task_id();
    let (limit, recursion, fuel) = get_config()
        .map(|config| {
            (
                config
                    .export_max_output_bytes
                    .min(StorageSchemaRepairReport::MAX_BYTES),
                config.export_template_recursion_limit,
                config.export_template_fuel,
            )
        })
        .unwrap_or((
            DEFAULT_EXPORT_MAX_OUTPUT_BYTES,
            DEFAULT_EXPORT_TEMPLATE_RECURSION_LIMIT,
            DEFAULT_EXPORT_TEMPLATE_FUEL,
        ));
    // Leave room inside the isolated worker's input ceiling for templates and
    // serialization framing. Enforce this while storage enumerates findings,
    // before materializing the response or its duplicate layout projection.
    let input_limit = limit.min(MAX_CONTEXT_BYTES);
    let budget = StorageSchemaReportBudget::new(input_limit)
        .map_err(|error| ApiError::BadRequest(error.to_string()))?;
    let analysis = super::get_work_with_budget(context, task_id, budget).await?;
    let generated_at = Utc::now();
    let render_context = report_context(
        &analysis,
        &class_name,
        &request.object_url_template,
        generated_at,
    )?;
    let limits = TemplateLimits::new(recursion, fuel).with_max_output_bytes(limit);
    let content = render_content(&render_context, limits).await?;
    let body = if let Some(layout) = layout {
        let slot = format!("SCHEMA_REPAIR_CONTENT_{}", Uuid::new_v4().simple());
        let mut layout_context = render_context;
        layout_context["report_content"] = json!(slot);
        let (rendered, _) = render_template(
            &layout.template,
            &layout.siblings,
            &layout_context,
            ExportContentType::TextHtml,
            ExportMissingDataPolicy::Strict,
            limit,
        )
        .await?;
        if rendered.matches(&slot).count() != 1 {
            return Err(ApiError::BadRequest(
                "Repair report layout must render report_content exactly once; no report was saved"
                    .into(),
            ));
        }
        if rendered.len().saturating_add(content.len()) > limit.saturating_add(slot.len()) {
            return Err(report_limit(limit));
        }
        rendered.replacen(&slot, &content, 1)
    } else {
        content
    };
    let html = wrap_document(body, limit)?;
    let report = StorageSchemaRepairReport::try_new(&work, generated_at, html)
        .map_err(|error| ApiError::BadRequest(error.to_string()))?;
    storage
        .save_schema_repair_report(StorageSchemaRepairReportWrite::new(
            report.clone(),
            authorized_collection,
        ))
        .await?;
    Ok(report)
}

pub async fn retained_repair_report(
    context: &impl StorageContext,
    source: SchemaReference,
    task_id: TaskId,
) -> Result<StorageSchemaRepairReport, ApiError> {
    let report = storage_handle(context)
        .get_schema_repair_report(task_id)
        .await?;
    if report.target() != source {
        return Err(ApiError::NotFound(
            "Schema report was not found in this class".into(),
        ));
    }
    Ok(report)
}

fn report_context(
    analysis: &SchemaWorkResponse,
    class_name: &str,
    urls: &SchemaObjectUrlTemplate,
    generated_at: DateTime<Utc>,
) -> Result<Value, ApiError> {
    if !matches!(analysis.kind, SchemaWorkKind::Impact) {
        return Err(ApiError::BadRequest(
            "Only schema impact analyses have repair reports".into(),
        ));
    }
    // The layout exposes analysis and objects, including their saved snapshots.
    // Charge both copies and expanded URLs before retaining each object.
    let mut budget = StorageSchemaReportBudget::new(MAX_CONTEXT_BYTES)
        .map_err(|error| ApiError::BadRequest(error.to_string()))?;
    budget.charge(&(analysis, class_name, generated_at))?;
    let mut objects = BTreeMap::new();
    if let Some(impact) = &analysis.impact {
        for group in &impact.failures {
            for id in &group.samples {
                let object = json!({"id":id,"url":urls.object_url(ObjectId::new(*id)?),"reason":group.reason,"snapshot":null});
                budget.charge(&object)?;
                objects.insert(*id, object);
            }
        }
        for finding in &impact.findings {
            let object = json!({"id":finding.object_id,"url":urls.object_url(ObjectId::new(finding.object_id)?),"reason":finding.reason,"snapshot":finding.snapshot});
            budget.charge(&object)?;
            objects.insert(finding.object_id, object);
        }
    }
    for id in &analysis.invalid_samples {
        if let Entry::Vacant(entry) = objects.entry(*id) {
            let object = json!({"id":id,"url":urls.object_url(ObjectId::new(*id)?),"reason":null,"snapshot":null});
            budget.charge(&object)?;
            entry.insert(object);
        }
    }
    let omitted_objects = analysis.invalid.saturating_sub(objects.len() as u64);
    let legacy_objects = objects
        .values()
        .filter(|object| object["snapshot"].is_null())
        .count();
    Ok(json!({
        "analysis":analysis,
        "class_name":class_name,
        "generated_at":generated_at,
        "partial": !matches!(analysis.status, SchemaWorkStatus::Complete),
        "retained_objects":objects.len(),
        "omitted_objects":omitted_objects,
        "legacy_objects":legacy_objects,
        "objects":objects.into_values().collect::<Vec<_>>(),
    }))
}

async fn render_content(context: &Value, limits: TemplateLimits) -> Result<String, ApiError> {
    TemplateExecution::new("schema-repair-content.html", CONTENT, limits)
        .auto_escape(TemplateAutoEscape::Html)
        .missing_data(MissingDataPolicy::Strict)
        .render(context)
        .await
        .map(|output| output.into_parts().0)
        .map_err(|error| {
            if error.to_string().contains("output limit") {
                report_limit(limits.max_output_bytes())
            } else {
                ApiError::BadRequest(format!(
                    "Repair report could not be rendered in full; no report was saved: {error}"
                ))
            }
        })
}

fn report_limit(limit: usize) -> ApiError {
    ApiError::PayloadTooLarge(format!(
        "Repair report exceeds the {limit}-byte output limit; no partial report was saved. Increase HUBUUM_EXPORT_MAX_OUTPUT_BYTES (up to 16 MiB) to render the complete report"
    ))
}

fn wrap_document(body: String, limit: usize) -> Result<String, ApiError> {
    if body
        .len()
        .saturating_add(DOCUMENT_START.len())
        .saturating_add(DOCUMENT_END.len())
        > limit
    {
        return Err(report_limit(limit));
    }
    Ok(format!("{DOCUMENT_START}{body}{DOCUMENT_END}"))
}

#[cfg(test)]
mod tests;
