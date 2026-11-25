use super::formatting::{
    format_bytes, format_count, format_percent, format_saturation, short_report_label,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

use super::{
    DepthProbeScope, Distribution, LimitMode, MetricDelta, ProfileName, RelationShapeSummary,
    ResourceTotals, Result, RuntimeIdentity, SENSITIVITY_REPORT_SCHEMA_VERSION, ScaleAxis,
    ScaleBenchmarkReport, ScaleImpactReport, ScenarioImpact, SensitivitySpec, invalid_data,
    write_json,
};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ScaleSensitivityReport {
    pub schema_version: u32,
    pub experiment_version: u32,
    pub baseline_label: String,
    pub baseline_manifest_digest: String,
    pub baseline_totals: ResourceTotals,
    pub profile: ProfileName,
    pub seed: u64,
    pub workload_digest: String,
    pub limit_mode: LimitMode,
    pub runtime: RuntimeIdentity,
    pub axes: Vec<AxisSensitivityReport>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct AxisSensitivityReport {
    pub axis: ScaleAxis,
    pub topology: String,
    pub scenario: String,
    pub phase: String,
    pub traversal_phase: Option<String>,
    pub baseline_total: u64,
    pub target_region: String,
    pub baseline_target_region_total: u64,
    pub baseline_relation_shape: Option<RelationShapeSummary>,
    pub baseline_objects_per_class: Distribution,
    pub depth_probe_scope: Option<DepthProbeScope>,
    pub depth_probes: Vec<DepthProbeSensitivityReport>,
    pub points: Vec<SensitivityPointReport>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct DepthProbeSensitivityReport {
    pub depth: u64,
    pub scenario: String,
    pub phase: String,
    pub points: Vec<DepthProbePointReport>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct DepthProbePointReport {
    pub added_percent: u64,
    pub latency_p95_ms: MetricDelta,
    pub graph_nodes: MetricDelta,
    pub graph_edges: MetricDelta,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct SensitivityPointReport {
    pub added_percent: u64,
    pub added_count: u64,
    pub comparison_total: u64,
    pub comparison_target_region_total: u64,
    pub comparison_relation_shape: Option<RelationShapeSummary>,
    pub comparison_objects_per_class: Distribution,
    pub latency_p95_ms: MetricDelta,
    pub requests_per_second: MetricDelta,
    pub traversal_pages: Option<MetricDelta>,
    pub traversal_ms: Option<MetricDelta>,
    pub storage_bytes: MetricDelta,
    pub index_bytes: Option<MetricDelta>,
}

impl ScaleSensitivityReport {
    pub fn summarize(
        baseline: &ScaleBenchmarkReport,
        impacts: &[ScaleImpactReport],
        spec: &SensitivitySpec,
    ) -> Result<Self> {
        spec.validate()?;
        if !baseline.correctness.passed() {
            return Err(invalid_data(
                "scale sensitivity summary requires a correctness-passing baseline",
            ));
        }
        let mut impacts_by_point = BTreeMap::new();
        for impact in impacts {
            if impact.baseline_manifest_digest != baseline.manifest.semantic_digest
                || impact.profile != baseline.manifest.profile
                || impact.seed != baseline.manifest.seed
                || impact.workload_digest != baseline.workload_digest
                || impact.limit_mode != baseline.limit_mode
                || impact.runtime != baseline.runtime
            {
                return Err(invalid_data(
                    "scale sensitivity impact does not match the controlled baseline",
                ));
            }
            if impacts_by_point
                .insert((impact.axis, impact.axis_delta), impact)
                .is_some()
            {
                return Err(invalid_data(format!(
                    "duplicate scale sensitivity impact for {} +{}",
                    impact.axis.as_str(),
                    impact.axis_delta
                )));
            }
        }

        let applicable_axes = spec.axes_for(baseline.limit_mode).collect::<Vec<_>>();
        if applicable_axes.is_empty() {
            return Err(invalid_data(format!(
                "scale sensitivity specification has no axes for the {:?} limit mode",
                baseline.limit_mode
            )));
        }
        let mut axes = Vec::with_capacity(applicable_axes.len());
        for axis_spec in applicable_axes {
            let baseline_total = axis_spec.axis.total(&baseline.manifest.totals);
            let mut points = Vec::with_capacity(axis_spec.percent_steps.len());
            let mut depth_probes = axis_spec
                .depth_probes
                .iter()
                .map(|probe| DepthProbeSensitivityReport {
                    depth: probe.depth,
                    scenario: probe.scenario.clone(),
                    phase: axis_spec.phase.clone(),
                    points: Vec::with_capacity(axis_spec.percent_steps.len()),
                })
                .collect::<Vec<_>>();
            for added_percent in &axis_spec.percent_steps {
                let added_count =
                    sensitivity_increment(baseline_total, *added_percent, axis_spec.axis)?;
                let impact = impacts_by_point
                    .remove(&(axis_spec.axis, added_count))
                    .ok_or_else(|| {
                        invalid_data(format!(
                            "missing {} +{}% (+{}) scale sensitivity impact",
                            axis_spec.axis.as_str(),
                            added_percent,
                            added_count
                        ))
                    })?;
                let expected_total = baseline_total
                    .checked_add(added_count)
                    .ok_or_else(|| invalid_data("scale sensitivity total overflowed"))?;
                if impact.baseline_total != baseline_total
                    || impact.comparison_total != expected_total
                    || impact.target_region != axis_spec.axis.target_region_name()
                    || impact.baseline_target_region_total
                        != axis_spec.axis.manifest_region_total(
                            baseline
                                .manifest
                                .regions
                                .get(axis_spec.axis.target_region_name())
                                .ok_or_else(|| {
                                    invalid_data(format!(
                                        "baseline manifest has no {} region",
                                        axis_spec.axis.target_region_name()
                                    ))
                                })?,
                        )
                {
                    return Err(invalid_data(format!(
                        "{} +{}% impact has unexpected corpus totals",
                        axis_spec.axis.as_str(),
                        added_percent
                    )));
                }
                let primary = sensitivity_scenario(impact, &axis_spec.scenario, &axis_spec.phase)?;
                let traversal = axis_spec
                    .traversal_phase
                    .as_deref()
                    .map(|phase| sensitivity_scenario(impact, &axis_spec.scenario, phase))
                    .transpose()?;
                for (probe_spec, probe_report) in
                    axis_spec.depth_probes.iter().zip(&mut depth_probes)
                {
                    let scenario =
                        sensitivity_scenario(impact, &probe_spec.scenario, &axis_spec.phase)?;
                    probe_report.points.push(DepthProbePointReport {
                        added_percent: *added_percent,
                        latency_p95_ms: sensitivity_metric(scenario, "latency_p95_ms")?.clone(),
                        graph_nodes: sensitivity_metric(scenario, "graph_nodes")?.clone(),
                        graph_edges: sensitivity_metric(scenario, "graph_edges")?.clone(),
                    });
                }
                points.push(SensitivityPointReport {
                    added_percent: *added_percent,
                    added_count,
                    comparison_total: expected_total,
                    comparison_target_region_total: impact.comparison_target_region_total,
                    comparison_relation_shape: impact.comparison_relation_shape.clone(),
                    comparison_objects_per_class: impact.comparison_objects_per_class.clone(),
                    latency_p95_ms: sensitivity_metric(primary, "latency_p95_ms")?.clone(),
                    requests_per_second: sensitivity_metric(primary, "requests_per_second")?
                        .clone(),
                    traversal_pages: traversal
                        .map(|scenario| sensitivity_metric(scenario, "pages").cloned())
                        .transpose()?,
                    traversal_ms: traversal
                        .map(|scenario| sensitivity_metric(scenario, "traversal_ms").cloned())
                        .transpose()?,
                    storage_bytes: sensitivity_resource(impact, "storage_bytes")?.clone(),
                    index_bytes: impact.resources.get("index_bytes").cloned(),
                });
            }
            axes.push(AxisSensitivityReport {
                axis: axis_spec.axis,
                topology: axis_spec.axis.topology().to_string(),
                scenario: axis_spec.scenario.clone(),
                phase: axis_spec.phase.clone(),
                traversal_phase: axis_spec.traversal_phase.clone(),
                baseline_total,
                target_region: axis_spec.axis.target_region_name().to_string(),
                baseline_target_region_total: axis_spec.axis.manifest_region_total(
                    baseline
                        .manifest
                        .regions
                        .get(axis_spec.axis.target_region_name())
                        .ok_or_else(|| {
                            invalid_data(format!(
                                "baseline manifest has no {} region",
                                axis_spec.axis.target_region_name()
                            ))
                        })?,
                ),
                baseline_relation_shape: axis_spec
                    .axis
                    .relation_shape_name()
                    .map(|name| {
                        baseline
                            .manifest
                            .relation_shapes
                            .get(name)
                            .cloned()
                            .ok_or_else(|| {
                                invalid_data(format!(
                                    "baseline manifest has no '{name}' relation shape"
                                ))
                            })
                    })
                    .transpose()?,
                baseline_objects_per_class: baseline.manifest.objects_per_class.clone(),
                depth_probe_scope: axis_spec.depth_probe_scope,
                depth_probes,
                points,
            });
        }
        if let Some(((axis, delta), _)) = impacts_by_point.first_key_value() {
            return Err(invalid_data(format!(
                "unexpected scale sensitivity impact for {} +{}",
                axis.as_str(),
                delta
            )));
        }

        Ok(Self {
            schema_version: SENSITIVITY_REPORT_SCHEMA_VERSION,
            experiment_version: spec.experiment_version,
            baseline_label: baseline.label.clone(),
            baseline_manifest_digest: baseline.manifest.semantic_digest.clone(),
            baseline_totals: baseline.manifest.totals.clone(),
            profile: baseline.manifest.profile,
            seed: baseline.manifest.seed,
            workload_digest: baseline.workload_digest.clone(),
            limit_mode: baseline.limit_mode,
            runtime: baseline.runtime.clone(),
            axes,
        })
    }

    pub fn write(&self, path: &Path) -> Result<()> {
        write_json(path, self)
    }

    pub fn markdown(&self) -> String {
        let mut output = format!(
            "## {} / {} / page limit {} scale growth\n\n",
            self.runtime.backend.name,
            self.profile.as_str(),
            self.limit_mode.page_limit()
        );
        output.push_str(&format!(
            "Fixed binary `{}`; {} {} backend; dataset `{}`; workload `{}`; page limit {}. Every point starts from a fresh copy of the baseline and changes only its named axis and target region.\n\n",
            short_report_label(&self.baseline_label),
            self.runtime.backend.name,
            self.runtime.backend.version,
            &self.baseline_manifest_digest[..12],
            &self.workload_digest[..12],
            self.limit_mode.page_limit()
        ));
        output.push_str(&format!(
            "Baseline corpus: {} collections · {} classes · {} objects · {} class relations · {} object relations.\n\nAuthorization overlay: {} principals · {} groups · {} memberships · {} grants.\n\n",
            format_count(self.baseline_totals.collections),
            format_count(self.baseline_totals.classes),
            format_count(self.baseline_totals.objects),
            format_count(self.baseline_totals.class_relations),
            format_count(self.baseline_totals.object_relations),
            format_count(self.baseline_totals.principals),
            format_count(self.baseline_totals.groups),
            format_count(self.baseline_totals.memberships),
            format_count(self.baseline_totals.permission_grants)
        ));
        render_curve_summary(&mut output, &self.axes);
        for axis in &self.axes {
            match axis.axis {
                ScaleAxis::Classes => render_class_sensitivity(&mut output, axis),
                ScaleAxis::Objects | ScaleAxis::ObjectHeavyObjects => {
                    render_object_sensitivity(&mut output, axis);
                }
                ScaleAxis::ObjectRelations
                | ScaleAxis::ConcentratedObjectRelations
                | ScaleAxis::DenseObjectRelations => {
                    render_relation_sensitivity(&mut output, axis);
                }
            }
        }
        output.push_str(
            "Each percentage is relative to the stated baseline, not the preceding row. Positive latency and storage changes and negative throughput changes are costs. These single-run measurements are informational; repeat trials before drawing a performance conclusion.\n\n",
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

fn render_curve_summary(output: &mut String, axes: &[AxisSensitivityReport]) {
    output.push_str("### Signals at a glance\n\n");
    output.push_str("Each sequence follows independent growth steps and shows p95 change from the same baseline. Expand an axis for exact corpus, latency, throughput, traversal, and storage measurements.\n\n");
    for axis in axes {
        let growth_steps = axis
            .points
            .iter()
            .map(|point| format!("+{}%", point.added_percent))
            .collect::<Vec<_>>()
            .join(" / ");
        let latency_curve = axis
            .points
            .iter()
            .map(|point| format_percent(point.latency_p95_ms.percent))
            .collect::<Vec<_>>()
            .join(" / ");
        output.push_str(&format!(
            "- **{}** (`{}`) — p95 `{}`",
            axis.axis.section_title(),
            growth_steps,
            latency_curve
        ));
        let page_deltas = axis
            .points
            .iter()
            .filter_map(|point| {
                point
                    .traversal_pages
                    .as_ref()
                    .map(|pages| format!("{:.0}", pages.comparison))
            })
            .collect::<Vec<_>>();
        if let Some(baseline) = axis
            .points
            .iter()
            .find_map(|point| point.traversal_pages.as_ref().map(|pages| pages.baseline))
        {
            output.push_str(&format!(
                "; pages `{baseline:.0} → {}`",
                page_deltas.join(" / ")
            ));
        }
        let saturations = axis
            .points
            .iter()
            .filter_map(|point| point.comparison_relation_shape.as_ref())
            .map(|shape| format_saturation(shape.saturation_basis_points))
            .collect::<Vec<_>>();
        if let Some(baseline) = &axis.baseline_relation_shape {
            output.push_str(&format!(
                "; pair saturation `{} → {}`",
                format_saturation(baseline.saturation_basis_points),
                saturations.join(" / ")
            ));
        }
        if let Some(probe) = axis.depth_probes.last() {
            let latency_curve = probe
                .points
                .iter()
                .map(|point| format_percent(point.latency_p95_ms.percent))
                .collect::<Vec<_>>()
                .join(" / ");
            output.push_str(&format!(
                "; depth {} graph p95 `{latency_curve}`",
                probe.depth
            ));
        }
        output.push_str(".\n");
    }
    output.push('\n');
}

fn sensitivity_increment(baseline: u64, percent: u64, axis: ScaleAxis) -> Result<u64> {
    let scaled = baseline
        .checked_mul(percent)
        .ok_or_else(|| invalid_data("scale sensitivity increment overflowed"))?;
    if percent == 0 || scaled % 100 != 0 {
        return Err(invalid_data(format!(
            "{}% of the {} baseline {} is not a positive exact resource count",
            percent,
            axis.as_str(),
            baseline
        )));
    }
    Ok(scaled / 100)
}

fn sensitivity_scenario<'a>(
    impact: &'a ScaleImpactReport,
    name: &str,
    phase: &str,
) -> Result<&'a ScenarioImpact> {
    impact
        .scenarios
        .iter()
        .find(|scenario| scenario.name == name && scenario.phase == phase)
        .ok_or_else(|| {
            invalid_data(format!(
                "scale sensitivity impact has no '{name}'/'{phase}' scenario"
            ))
        })
}

fn sensitivity_metric<'a>(scenario: &'a ScenarioImpact, name: &str) -> Result<&'a MetricDelta> {
    scenario.metrics.get(name).ok_or_else(|| {
        invalid_data(format!(
            "scale sensitivity scenario '{}'/{} has no '{name}' metric",
            scenario.name, scenario.phase
        ))
    })
}

fn sensitivity_resource<'a>(impact: &'a ScaleImpactReport, name: &str) -> Result<&'a MetricDelta> {
    impact.resources.get(name).ok_or_else(|| {
        invalid_data(format!(
            "scale sensitivity impact has no '{name}' resource metric"
        ))
    })
}

fn render_object_sensitivity(output: &mut String, axis: &AxisSensitivityReport) {
    open_axis_details(output, axis);
    output.push_str(&format!(
        "Growth is placed in the `{}` region. It starts with {} of the corpus's {} objects; the largest class has {} objects.\n\n| Growth | Total objects | Target region | Maximum/class |\n",
        axis.target_region,
        format_count(axis.baseline_target_region_total),
        format_count(axis.baseline_total),
        format_count(axis.baseline_objects_per_class.maximum),
    ));
    output.push_str("| ---: | ---: | ---: | ---: |\n");
    for point in &axis.points {
        output.push_str(&format!(
            "| +{}% (+{}) | {} | {} | {} |\n",
            point.added_percent,
            format_count(point.added_count),
            format_count(point.comparison_total),
            format_count(point.comparison_target_region_total),
            format_count(point.comparison_objects_per_class.maximum),
        ));
    }
    render_measurement_baseline(output, axis);
    render_performance_table(output, axis);
    close_axis_details(output);
}

fn render_depth_probes(output: &mut String, axis: &AxisSensitivityReport) {
    let Some(scope) = axis.depth_probe_scope else {
        return;
    };
    let Some(first_probe) = axis.depth_probes.first() else {
        return;
    };
    let scope_description = match scope {
        DepthProbeScope::FixedLocalGraph => {
            "Added relations are outside the probed chain, so its reachable graph stays fixed. This isolates the cost of searching through a larger global relation corpus."
        }
        DepthProbeScope::GrowingLocalDensity => {
            "Added relations enter the probed chain's region. Returned nodes and edges are shown so global volume cost is not confused with the extra work caused by local fan-out."
        }
    };
    output.push_str(&format!(
        "Multi-hop object graph: {scope_description}\n\n| Maximum depth | Baseline p95"
    ));
    for point in &first_probe.points {
        output.push_str(&format!(" | +{}%", point.added_percent));
    }
    output.push_str(" |\n| ---: | ---: ");
    for _ in &first_probe.points {
        output.push_str("| ---: ");
    }
    output.push_str("|\n");
    for probe in &axis.depth_probes {
        let baseline = probe
            .points
            .first()
            .map(|point| point.latency_p95_ms.baseline)
            .unwrap_or_default();
        output.push_str(&format!("| {} | {baseline:.2} ms ", probe.depth));
        for point in &probe.points {
            output.push_str(&format!(
                "| {} ",
                format_comparison_delta(&point.latency_p95_ms, " ms")
            ));
        }
        output.push_str("|\n");
    }
    output.push('\n');
    output.push_str("Returned nodes/edges by maximum depth:\n\n");
    render_depth_graph_shape(output, "Baseline", &axis.depth_probes, None);
    for (point_index, point) in first_probe.points.iter().enumerate() {
        render_depth_graph_shape(
            output,
            &format!("+{}%", point.added_percent),
            &axis.depth_probes,
            Some(point_index),
        );
        if point_index + 1 == first_probe.points.len() {
            output.push('\n');
        }
    }
}

fn render_depth_graph_shape(
    output: &mut String,
    label: &str,
    probes: &[DepthProbeSensitivityReport],
    point_index: Option<usize>,
) {
    let shape = probes
        .iter()
        .filter_map(|probe| probe.points.get(point_index.unwrap_or_default()))
        .map(|point| {
            if point_index.is_none() {
                format!(
                    "{:.0}/{:.0}",
                    point.graph_nodes.baseline, point.graph_edges.baseline
                )
            } else {
                format!(
                    "{:.0}/{:.0}",
                    point.graph_nodes.comparison, point.graph_edges.comparison
                )
            }
        })
        .collect::<Vec<_>>()
        .join(" / ");
    output.push_str(&format!("- {label}: `{shape}`\n"));
}

fn render_class_sensitivity(output: &mut String, axis: &AxisSensitivityReport) {
    open_axis_details(output, axis);
    output.push_str(&format!(
        "Objects and relations remain fixed while classes are added to the `{}` region. It starts with {} of the corpus's {} classes; objects/class starts at median {} and maximum {}.\n\n| Growth | Total classes | Target region | Objects/class median | Maximum |\n",
        axis.target_region,
        format_count(axis.baseline_target_region_total),
        format_count(axis.baseline_total),
        format_count(axis.baseline_objects_per_class.median),
        format_count(axis.baseline_objects_per_class.maximum),
    ));
    output.push_str("| ---: | ---: | ---: | ---: | ---: |\n");
    for point in &axis.points {
        output.push_str(&format!(
            "| +{}% (+{}) | {} | {} | {} | {} |\n",
            point.added_percent,
            format_count(point.added_count),
            format_count(point.comparison_total),
            format_count(point.comparison_target_region_total),
            format_count(point.comparison_objects_per_class.median),
            format_count(point.comparison_objects_per_class.maximum),
        ));
    }
    render_measurement_baseline(output, axis);
    render_performance_table(output, axis);
    close_axis_details(output);
}

fn render_relation_sensitivity(output: &mut String, axis: &AxisSensitivityReport) {
    open_axis_details(output, axis);
    if let Some(shape) = &axis.baseline_relation_shape {
        output.push_str(&format!(
            "The measured class pair starts with {} edges across {} possible unique source/target combinations ({} saturation).\n\n",
            format_count(shape.edge_count),
            format_count(shape.pair_capacity),
            format_saturation(shape.saturation_basis_points)
        ));
    }
    output.push_str(&format!(
        "Growth is placed in the `{}` region, which starts with {} of the corpus's {} object relations.\n\n| Growth | Total relations | Target region | Measured pair edges | Pair saturation |\n",
        axis.target_region,
        format_count(axis.baseline_target_region_total),
        format_count(axis.baseline_total),
    ));
    output.push_str("| ---: | ---: | ---: | ---: | ---: |\n");
    for point in &axis.points {
        let edge_count = point
            .comparison_relation_shape
            .as_ref()
            .map(|shape| format_count(shape.edge_count))
            .unwrap_or_else(|| "-".to_string());
        let saturation = point
            .comparison_relation_shape
            .as_ref()
            .map(|shape| format_saturation(shape.saturation_basis_points))
            .unwrap_or_else(|| "-".to_string());
        output.push_str(&format!(
            "| +{}% (+{}) | {} | {} | {} | {} |\n",
            point.added_percent,
            format_count(point.added_count),
            format_count(point.comparison_total),
            format_count(point.comparison_target_region_total),
            edge_count,
            saturation,
        ));
    }
    render_measurement_baseline(output, axis);
    render_performance_table(output, axis);
    render_depth_probes(output, axis);
    close_axis_details(output);
}

fn open_axis_details(output: &mut String, axis: &AxisSensitivityReport) {
    output.push_str(&format!(
        "<details>\n<summary><strong>{}</strong> — exact evidence</summary>\n\nPrimary operation: `{}` / `{}`.\n\n",
        axis.axis.section_title(),
        axis.scenario,
        axis.phase
    ));
}

fn close_axis_details(output: &mut String) {
    output.push_str("</details>\n\n");
}

fn render_measurement_baseline(output: &mut String, axis: &AxisSensitivityReport) {
    let Some(first) = axis.points.first() else {
        return;
    };
    let mut values = vec![
        format!("p95 {:.2} ms", first.latency_p95_ms.baseline),
        format!("throughput {:.2}/s", first.requests_per_second.baseline),
    ];
    if let Some(pages) = &first.traversal_pages {
        let unit = if pages.baseline == 1.0 {
            "page"
        } else {
            "pages"
        };
        values.push(format!("traversal {:.0} {unit}", pages.baseline));
    }
    if let Some(traversal_ms) = &first.traversal_ms {
        values.push(format!("traversal {:.2} ms", traversal_ms.baseline));
    }
    values.push(format!(
        "database {}",
        format_bytes(first.storage_bytes.baseline)
    ));
    if let Some(index_bytes) = &first.index_bytes {
        values.push(format!("index {}", format_bytes(index_bytes.baseline)));
    }
    output.push_str(&format!(
        "\nMeasurement baseline: {}.\n\n",
        values.join(" · ")
    ));
}

fn render_performance_table(output: &mut String, axis: &AxisSensitivityReport) {
    let has_traversal = axis
        .points
        .iter()
        .any(|point| point.traversal_pages.is_some() || point.traversal_ms.is_some());
    if has_traversal {
        output.push_str(
            "| Growth | p95 ms | Requests/s | Pages | Traversal ms | Database | Index |\n",
        );
        output.push_str("| ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n");
    } else {
        output.push_str("| Growth | p95 ms | Requests/s | Database | Index |\n");
        output.push_str("| ---: | ---: | ---: | ---: | ---: |\n");
    }
    for point in &axis.points {
        if has_traversal {
            output.push_str(&format!(
                "| +{}% | {} | {} | {} | {} | {} | {} |\n",
                point.added_percent,
                format_comparison_delta(&point.latency_p95_ms, ""),
                format_comparison_delta(&point.requests_per_second, ""),
                point
                    .traversal_pages
                    .as_ref()
                    .map(|delta| format!("{:.0}", delta.comparison))
                    .unwrap_or_else(|| "-".to_string()),
                point
                    .traversal_ms
                    .as_ref()
                    .map(|delta| format_comparison_delta(delta, ""))
                    .unwrap_or_else(|| "-".to_string()),
                format_bytes_comparison(&point.storage_bytes),
                point
                    .index_bytes
                    .as_ref()
                    .map(format_bytes_comparison)
                    .unwrap_or_else(|| "-".to_string())
            ));
        } else {
            output.push_str(&format!(
                "| +{}% | {} | {} | {} | {} |\n",
                point.added_percent,
                format_comparison_delta(&point.latency_p95_ms, ""),
                format_comparison_delta(&point.requests_per_second, ""),
                format_bytes_comparison(&point.storage_bytes),
                point
                    .index_bytes
                    .as_ref()
                    .map(format_bytes_comparison)
                    .unwrap_or_else(|| "-".to_string())
            ));
        }
    }
    output.push('\n');
}

fn format_comparison_delta(delta: &MetricDelta, suffix: &str) -> String {
    format!(
        "{:.2}{suffix} ({})",
        delta.comparison,
        format_percent(delta.percent)
    )
}

fn format_bytes_comparison(delta: &MetricDelta) -> String {
    format!(
        "{} ({})",
        format_bytes(delta.comparison),
        format_percent(delta.percent)
    )
}
