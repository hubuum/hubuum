use super::formatting::{format_count, format_percent, format_saturation};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

use super::{
    Distribution, IMPACT_REPORT_SCHEMA_VERSION, LifecycleReport, LimitMode, ProfileName,
    RelationShapeSummary, ResourceReport, Result, RuntimeIdentity, ScaleAxis, ScaleBenchmarkReport,
    ScenarioReport, invalid_data, write_json,
};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct MetricDelta {
    pub baseline: f64,
    pub comparison: f64,
    pub absolute: f64,
    pub percent: Option<f64>,
    pub per_normalization_unit: f64,
}

impl MetricDelta {
    fn between(baseline: f64, comparison: f64, axis_delta: u64, unit: u64) -> Self {
        let absolute = comparison - baseline;
        Self {
            baseline,
            comparison,
            absolute,
            percent: (baseline != 0.0).then_some(absolute * 100.0 / baseline),
            per_normalization_unit: absolute * unit as f64 / axis_delta as f64,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ScenarioImpact {
    pub name: String,
    pub phase: String,
    pub principal: String,
    pub concurrency: usize,
    pub metrics: BTreeMap<String, MetricDelta>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ScaleImpactReport {
    pub schema_version: u32,
    pub axis: ScaleAxis,
    pub topology: String,
    pub normalization_unit: u64,
    pub baseline_label: String,
    pub comparison_label: String,
    pub baseline_manifest_digest: String,
    pub comparison_manifest_digest: String,
    pub baseline_total: u64,
    pub comparison_total: u64,
    pub axis_delta: u64,
    pub target_region: String,
    pub baseline_target_region_total: u64,
    pub comparison_target_region_total: u64,
    pub baseline_relation_shape: Option<RelationShapeSummary>,
    pub comparison_relation_shape: Option<RelationShapeSummary>,
    pub baseline_objects_per_class: Distribution,
    pub comparison_objects_per_class: Distribution,
    pub profile: ProfileName,
    pub seed: u64,
    pub workload_digest: String,
    pub limit_mode: LimitMode,
    pub runtime: RuntimeIdentity,
    pub scenarios: Vec<ScenarioImpact>,
    pub resources: BTreeMap<String, MetricDelta>,
    pub lifecycle: BTreeMap<String, MetricDelta>,
}

impl ScaleImpactReport {
    pub fn compare(
        baseline: &ScaleBenchmarkReport,
        comparison: &ScaleBenchmarkReport,
        axis: ScaleAxis,
        normalization_unit: Option<u64>,
    ) -> Result<Self> {
        validate_impact_controls(baseline, comparison, axis)?;
        let baseline_total = axis.total(&baseline.manifest.totals);
        let comparison_total = axis.total(&comparison.manifest.totals);
        let axis_delta = comparison_total
            .checked_sub(baseline_total)
            .ok_or_else(|| {
                invalid_data(format!(
                    "comparison {} total must exceed baseline total",
                    axis.as_str()
                ))
            })?;
        if axis_delta == 0 {
            return Err(invalid_data(format!(
                "comparison {} total must exceed baseline total",
                axis.as_str()
            )));
        }
        let normalization_unit = normalization_unit.unwrap_or(axis_delta);
        if normalization_unit == 0 {
            return Err(invalid_data(
                "scale impact normalization unit must be non-zero",
            ));
        }

        let scenarios = compare_scenarios(
            &baseline.scenarios,
            &comparison.scenarios,
            axis_delta,
            normalization_unit,
        )?;
        let resources = compare_resources(
            &baseline.resources,
            &comparison.resources,
            axis_delta,
            normalization_unit,
        );
        let lifecycle = compare_lifecycle(
            &baseline.lifecycle,
            &comparison.lifecycle,
            axis_delta,
            normalization_unit,
        );
        let target_region = axis.target_region_name();
        let baseline_target_region_total =
            axis.manifest_region_total(baseline.manifest.regions.get(target_region).ok_or_else(
                || invalid_data(format!("baseline manifest has no {target_region} region")),
            )?);
        let comparison_target_region_total =
            axis.manifest_region_total(comparison.manifest.regions.get(target_region).ok_or_else(
                || invalid_data(format!("comparison manifest has no {target_region} region")),
            )?);
        let baseline_relation_shape = axis
            .relation_shape_name()
            .map(|name| {
                baseline
                    .manifest
                    .relation_shapes
                    .get(name)
                    .cloned()
                    .ok_or_else(|| {
                        invalid_data(format!("baseline manifest has no '{name}' relation shape"))
                    })
            })
            .transpose()?;
        let comparison_relation_shape = axis
            .relation_shape_name()
            .map(|name| {
                comparison
                    .manifest
                    .relation_shapes
                    .get(name)
                    .cloned()
                    .ok_or_else(|| {
                        invalid_data(format!(
                            "comparison manifest has no '{name}' relation shape"
                        ))
                    })
            })
            .transpose()?;

        Ok(Self {
            schema_version: IMPACT_REPORT_SCHEMA_VERSION,
            axis,
            topology: axis.topology().to_string(),
            normalization_unit,
            baseline_label: baseline.label.clone(),
            comparison_label: comparison.label.clone(),
            baseline_manifest_digest: baseline.manifest.semantic_digest.clone(),
            comparison_manifest_digest: comparison.manifest.semantic_digest.clone(),
            baseline_total,
            comparison_total,
            axis_delta,
            target_region: target_region.to_string(),
            baseline_target_region_total,
            comparison_target_region_total,
            baseline_relation_shape,
            comparison_relation_shape,
            baseline_objects_per_class: baseline.manifest.objects_per_class.clone(),
            comparison_objects_per_class: comparison.manifest.objects_per_class.clone(),
            profile: baseline.manifest.profile,
            seed: baseline.manifest.seed,
            workload_digest: baseline.workload_digest.clone(),
            limit_mode: baseline.limit_mode,
            runtime: baseline.runtime.clone(),
            scenarios,
            resources,
            lifecycle,
        })
    }

    pub fn write(&self, path: &Path) -> Result<()> {
        write_json(path, self)
    }

    pub fn read(path: &Path) -> Result<Self> {
        let report = serde_json::from_slice::<Self>(&fs::read(path)?)?;
        if report.schema_version != IMPACT_REPORT_SCHEMA_VERSION {
            return Err(invalid_data(format!(
                "unsupported scale impact report schema version {}",
                report.schema_version
            )));
        }
        Ok(report)
    }

    pub fn markdown(&self) -> String {
        let mut output = String::from("## Hubuum scale sensitivity\n\n");
        output.push_str(&format!(
            "Changed only `{}` from {} to {} ({:+}); `{}` region {} → {}; topology: {}. Deltas are normalized per +{} {} and remain informational.\n\n",
            self.axis.as_str(),
            self.baseline_total,
            self.comparison_total,
            self.axis_delta,
            self.target_region,
            self.baseline_target_region_total,
            self.comparison_target_region_total,
            self.topology,
            self.normalization_unit,
            self.axis.as_str()
        ));
        if let (Some(baseline), Some(comparison)) = (
            &self.baseline_relation_shape,
            &self.comparison_relation_shape,
        ) {
            output.push_str(&format!(
                "Measured class pair: {} → {} edges; {} → {} of {} unique source/target combinations.\n\n",
                format_count(baseline.edge_count),
                format_count(comparison.edge_count),
                format_saturation(baseline.saturation_basis_points),
                format_saturation(comparison.saturation_basis_points),
                format_count(comparison.pair_capacity)
            ));
        }
        output.push_str(
            "| Scenario | Phase | Baseline p95 ms | Comparison p95 ms | p95 change | p95 per unit | Throughput change |\n",
        );
        output.push_str("| --- | --- | ---: | ---: | ---: | ---: | ---: |\n");
        for scenario in &self.scenarios {
            let p95 = &scenario.metrics["latency_p95_ms"];
            let throughput = &scenario.metrics["requests_per_second"];
            output.push_str(&format!(
                "| {} | {} | {:.2} | {:.2} | {} | {:+.2} ms | {} |\n",
                scenario.name,
                scenario.phase,
                p95.baseline,
                p95.comparison,
                format_percent(p95.percent),
                p95.per_normalization_unit,
                format_percent(throughput.percent)
            ));
        }
        output.push_str("\n| Resource | Baseline | Comparison | Change | Per unit |\n");
        output.push_str("| --- | ---: | ---: | ---: | ---: |\n");
        for (name, delta) in &self.resources {
            output.push_str(&format!(
                "| {} | {:.2} | {:.2} | {} | {:+.2} |\n",
                name,
                delta.baseline,
                delta.comparison,
                format_percent(delta.percent),
                delta.per_normalization_unit
            ));
        }
        output.push_str(
            "\nPositive latency, size, CPU, and duration changes are declines; negative throughput changes are declines. Repeat paired trials before drawing a performance conclusion.\n\n",
        );
        output
    }

    pub fn append_markdown(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut output = OpenOptions::new().create(true).append(true).open(path)?;
        output.write_all(self.markdown().as_bytes())?;
        Ok(())
    }
}

fn validate_impact_controls(
    baseline: &ScaleBenchmarkReport,
    comparison: &ScaleBenchmarkReport,
    axis: ScaleAxis,
) -> Result<()> {
    if !baseline.correctness.passed() || !comparison.correctness.passed() {
        return Err(invalid_data(
            "scale impact comparison requires two correctness-passing reports",
        ));
    }
    if baseline.schema_version != comparison.schema_version
        || baseline.manifest.schema_version != comparison.manifest.schema_version
        || baseline.manifest.profile_version != comparison.manifest.profile_version
        || baseline.manifest.profile != comparison.manifest.profile
        || baseline.manifest.seed != comparison.manifest.seed
        || baseline.workload_version != comparison.workload_version
        || baseline.workload_seed != comparison.workload_seed
        || baseline.workload_digest != comparison.workload_digest
        || baseline.limit_mode != comparison.limit_mode
        || baseline.effective_limits != comparison.effective_limits
        || baseline.runtime != comparison.runtime
        || baseline.scale_axis.is_some()
        || comparison.scale_axis != Some(axis)
    {
        return Err(invalid_data(
            "scale impact reports differ in profile, seed, workload, limits, runtime, or controlled axis",
        ));
    }
    let mut baseline_totals = baseline.manifest.totals.clone();
    let mut comparison_totals = comparison.manifest.totals.clone();
    axis.set_total(&mut baseline_totals, 0);
    axis.set_total(&mut comparison_totals, 0);
    if baseline_totals != comparison_totals {
        return Err(invalid_data(format!(
            "scale impact comparison changed totals other than {}",
            axis.as_str()
        )));
    }
    let mut baseline_regions = baseline.manifest.regions.clone();
    let mut comparison_regions = comparison.manifest.regions.clone();
    let target_region = axis.target_region_name();
    let baseline_target = baseline_regions
        .get_mut(target_region)
        .ok_or_else(|| invalid_data(format!("baseline manifest has no {target_region} region")))?;
    let comparison_target = comparison_regions.get_mut(target_region).ok_or_else(|| {
        invalid_data(format!("comparison manifest has no {target_region} region"))
    })?;
    let baseline_region_total = axis.manifest_region_total(baseline_target);
    let comparison_region_total = axis.manifest_region_total(comparison_target);
    let total_delta = axis
        .total(&comparison.manifest.totals)
        .checked_sub(axis.total(&baseline.manifest.totals));
    let region_delta = comparison_region_total.checked_sub(baseline_region_total);
    if total_delta.is_none() || total_delta != region_delta {
        return Err(invalid_data(format!(
            "scale impact {} total and {target_region}-region deltas differ",
            axis.as_str()
        )));
    }
    axis.set_manifest_region_total(baseline_target, 0);
    axis.set_manifest_region_total(comparison_target, 0);
    let mut baseline_anchors = baseline.manifest.anchors.clone();
    let mut comparison_anchors = comparison.manifest.anchors.clone();
    if axis == ScaleAxis::ObjectHeavyObjects {
        baseline_anchors.remove("secondary_object_id");
        comparison_anchors.remove("secondary_object_id");
        baseline_anchors.remove("deep_graph_object_id");
        comparison_anchors.remove("deep_graph_object_id");
    }
    if baseline_regions != comparison_regions
        || baseline.manifest.overlays != comparison.manifest.overlays
        || baseline_anchors != comparison_anchors
        || baseline.manifest.provisioning != comparison.manifest.provisioning
        || (axis != ScaleAxis::Classes
            && baseline.manifest.classes_per_collection
                != comparison.manifest.classes_per_collection)
        || baseline.manifest.history_revisions != comparison.manifest.history_revisions
        || baseline.manifest.json_payload_bytes != comparison.manifest.json_payload_bytes
        || baseline.manifest.principals != comparison.manifest.principals
    {
        return Err(invalid_data(format!(
            "scale impact comparison changed corpus controls other than {}-region {}",
            target_region,
            axis.as_str(),
        )));
    }
    Ok(())
}

fn compare_scenarios(
    baseline: &[ScenarioReport],
    comparison: &[ScenarioReport],
    axis_delta: u64,
    unit: u64,
) -> Result<Vec<ScenarioImpact>> {
    let mut baseline_by_key = BTreeMap::new();
    for scenario in baseline {
        let key = (
            scenario.name.clone(),
            scenario.phase.clone(),
            scenario.principal.clone(),
            scenario.concurrency,
        );
        if baseline_by_key.insert(key, scenario).is_some() {
            return Err(invalid_data(
                "baseline report contains duplicate scenario keys",
            ));
        }
    }
    let mut impacts = Vec::with_capacity(comparison.len());
    for scenario in comparison {
        let key = (
            scenario.name.clone(),
            scenario.phase.clone(),
            scenario.principal.clone(),
            scenario.concurrency,
        );
        let baseline_scenario = baseline_by_key.remove(&key).ok_or_else(|| {
            invalid_data(format!(
                "comparison scenario '{}'/{} has no controlled baseline",
                scenario.name, scenario.phase
            ))
        })?;
        let mut metrics = BTreeMap::from([
            (
                "latency_maximum_ms".to_string(),
                MetricDelta::between(
                    baseline_scenario.latency.maximum_ms,
                    scenario.latency.maximum_ms,
                    axis_delta,
                    unit,
                ),
            ),
            (
                "latency_p50_ms".to_string(),
                MetricDelta::between(
                    baseline_scenario.latency.p50_ms,
                    scenario.latency.p50_ms,
                    axis_delta,
                    unit,
                ),
            ),
            (
                "latency_p95_ms".to_string(),
                MetricDelta::between(
                    baseline_scenario.latency.p95_ms,
                    scenario.latency.p95_ms,
                    axis_delta,
                    unit,
                ),
            ),
            (
                "latency_p99_ms".to_string(),
                MetricDelta::between(
                    baseline_scenario.latency.p99_ms,
                    scenario.latency.p99_ms,
                    axis_delta,
                    unit,
                ),
            ),
            (
                "requests_per_second".to_string(),
                MetricDelta::between(
                    baseline_scenario.requests_per_second,
                    scenario.requests_per_second,
                    axis_delta,
                    unit,
                ),
            ),
            (
                "pages".to_string(),
                MetricDelta::between(
                    baseline_scenario.pages as f64,
                    scenario.pages as f64,
                    axis_delta,
                    unit,
                ),
            ),
        ]);
        insert_optional_metric(
            &mut metrics,
            "traversal_ms",
            baseline_scenario.traversal_ms,
            scenario.traversal_ms,
            axis_delta,
            unit,
        );
        insert_optional_u64_metric(
            &mut metrics,
            "graph_nodes",
            baseline_scenario.graph_nodes,
            scenario.graph_nodes,
            axis_delta,
            unit,
        );
        insert_optional_u64_metric(
            &mut metrics,
            "graph_edges",
            baseline_scenario.graph_edges,
            scenario.graph_edges,
            axis_delta,
            unit,
        );
        impacts.push(ScenarioImpact {
            name: scenario.name.clone(),
            phase: scenario.phase.clone(),
            principal: scenario.principal.clone(),
            concurrency: scenario.concurrency,
            metrics,
        });
    }
    // A shared baseline may contain probes that are intentionally inapplicable
    // to one comparison axis. Every comparison scenario must still have an
    // exact controlled-baseline match.
    Ok(impacts)
}

fn compare_resources(
    baseline: &ResourceReport,
    comparison: &ResourceReport,
    axis_delta: u64,
    unit: u64,
) -> BTreeMap<String, MetricDelta> {
    let mut metrics = BTreeMap::from([
        (
            "application_cpu_seconds".to_string(),
            MetricDelta::between(
                baseline.application_cpu_seconds,
                comparison.application_cpu_seconds,
                axis_delta,
                unit,
            ),
        ),
        (
            "storage_bytes".to_string(),
            MetricDelta::between(
                baseline.backend.storage_bytes as f64,
                comparison.backend.storage_bytes as f64,
                axis_delta,
                unit,
            ),
        ),
        (
            "peak_application_resident_bytes".to_string(),
            MetricDelta::between(
                baseline.peak_application_resident_bytes as f64,
                comparison.peak_application_resident_bytes as f64,
                axis_delta,
                unit,
            ),
        ),
    ]);
    insert_optional_u64_metric(
        &mut metrics,
        "data_bytes",
        baseline.backend.data_bytes,
        comparison.backend.data_bytes,
        axis_delta,
        unit,
    );
    insert_optional_u64_metric(
        &mut metrics,
        "index_bytes",
        baseline.backend.index_bytes,
        comparison.backend.index_bytes,
        axis_delta,
        unit,
    );
    insert_optional_metric(
        &mut metrics,
        "backend_cpu_seconds",
        baseline.backend.cpu_seconds,
        comparison.backend.cpu_seconds,
        axis_delta,
        unit,
    );
    insert_optional_u64_metric(
        &mut metrics,
        "peak_backend_resident_bytes",
        baseline.backend.peak_resident_bytes,
        comparison.backend.peak_resident_bytes,
        axis_delta,
        unit,
    );
    insert_optional_u64_metric(
        &mut metrics,
        "write_ahead_bytes",
        baseline.backend.write_ahead_bytes,
        comparison.backend.write_ahead_bytes,
        axis_delta,
        unit,
    );
    for (name, baseline_value) in &baseline.backend.metrics {
        if let Some(comparison_value) = comparison.backend.metrics.get(name) {
            metrics.insert(
                format!("backend_{name}"),
                MetricDelta::between(*baseline_value, *comparison_value, axis_delta, unit),
            );
        }
    }
    metrics
}

fn compare_lifecycle(
    baseline: &LifecycleReport,
    comparison: &LifecycleReport,
    axis_delta: u64,
    unit: u64,
) -> BTreeMap<String, MetricDelta> {
    let mut metrics = BTreeMap::from([
        (
            "dataset_generation_ms".to_string(),
            MetricDelta::between(
                baseline.dataset_generation_ms as f64,
                comparison.dataset_generation_ms as f64,
                axis_delta,
                unit,
            ),
        ),
        (
            "dataset_loading_ms".to_string(),
            MetricDelta::between(
                baseline.dataset_loading_ms as f64,
                comparison.dataset_loading_ms as f64,
                axis_delta,
                unit,
            ),
        ),
    ]);
    for (name, baseline_value, comparison_value) in [
        (
            "backup_generation_ms",
            baseline.backup_generation_ms,
            comparison.backup_generation_ms,
        ),
        (
            "backup_artifact_bytes",
            baseline.backup_artifact_bytes,
            comparison.backup_artifact_bytes,
        ),
        ("restore_ms", baseline.restore_ms, comparison.restore_ms),
        (
            "semantic_verification_ms",
            baseline.semantic_verification_ms,
            comparison.semantic_verification_ms,
        ),
        (
            "computed_rebuild_ms",
            baseline.computed_rebuild_ms,
            comparison.computed_rebuild_ms,
        ),
    ] {
        insert_optional_u64_metric(
            &mut metrics,
            name,
            baseline_value,
            comparison_value,
            axis_delta,
            unit,
        );
    }
    metrics
}

fn insert_optional_metric(
    metrics: &mut BTreeMap<String, MetricDelta>,
    name: &str,
    baseline: Option<f64>,
    comparison: Option<f64>,
    axis_delta: u64,
    unit: u64,
) {
    if let (Some(baseline), Some(comparison)) = (baseline, comparison) {
        metrics.insert(
            name.to_string(),
            MetricDelta::between(baseline, comparison, axis_delta, unit),
        );
    }
}

fn insert_optional_u64_metric(
    metrics: &mut BTreeMap<String, MetricDelta>,
    name: &str,
    baseline: Option<u64>,
    comparison: Option<u64>,
    axis_delta: u64,
    unit: u64,
) {
    insert_optional_metric(
        metrics,
        name,
        baseline.map(|value| value as f64),
        comparison.map(|value| value as f64),
        axis_delta,
        unit,
    );
}
