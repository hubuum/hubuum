use super::formatting::{format_bytes, format_percent, short_report_label};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

use super::{
    BACKEND_COMPARISON_SCHEMA_VERSION, BackendIdentity, LifecycleReport, LimitMode, ProfileName,
    ResourceReport, Result, ScaleBenchmarkReport, ScenarioReport, float_percentile, invalid_data,
    write_json,
};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct BackendPerformanceSummary {
    pub label: String,
    pub identity: BackendIdentity,
    pub scenarios: Vec<ScenarioReport>,
    pub resources: ResourceReport,
    pub lifecycle: LifecycleReport,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct BackendComparisonReport {
    pub schema_version: u32,
    pub profile: ProfileName,
    pub manifest_digest: String,
    pub workload_version: u32,
    pub workload_digest: String,
    pub limit_mode: LimitMode,
    pub backends: Vec<BackendPerformanceSummary>,
}

impl BackendComparisonReport {
    pub fn compare(reports: &[ScaleBenchmarkReport]) -> Result<Self> {
        let reference = reports
            .first()
            .ok_or_else(|| invalid_data("backend comparison requires at least one report"))?;
        if !reference.correctness.passed() {
            return Err(invalid_data(format!(
                "backend '{}' did not pass correctness checks",
                reference.runtime.backend.name
            )));
        }
        let reference_scenarios = scenario_keys(reference);
        let mut names = BTreeSet::new();
        for report in reports {
            if !report.correctness.passed() {
                return Err(invalid_data(format!(
                    "backend '{}' did not pass correctness checks",
                    report.runtime.backend.name
                )));
            }
            report.manifest.equivalent_to(&reference.manifest)?;
            if report.schema_version != reference.schema_version
                || report.workload_version != reference.workload_version
                || report.workload_seed != reference.workload_seed
                || report.workload_digest != reference.workload_digest
                || report.limit_mode != reference.limit_mode
                || report.scale_axis != reference.scale_axis
                || report.effective_limits != reference.effective_limits
                || report.runtime.runner != reference.runtime.runner
                || report.runtime.process_fresh != reference.runtime.process_fresh
                || report.runtime.database_fresh != reference.runtime.database_fresh
                || report.runtime.deliberate_warmup_requests
                    != reference.runtime.deliberate_warmup_requests
            {
                return Err(invalid_data(
                    "backend reports differ in schema, workload, effective limits, or runner controls",
                ));
            }
            if scenario_keys(report) != reference_scenarios {
                return Err(invalid_data(format!(
                    "backend '{}' reported a different scenario set",
                    report.runtime.backend.name
                )));
            }
            if !names.insert(report.runtime.backend.name.clone()) {
                return Err(invalid_data(format!(
                    "backend '{}' appears more than once",
                    report.runtime.backend.name
                )));
            }
        }
        let mut backends = reports
            .iter()
            .map(|report| BackendPerformanceSummary {
                label: report.label.clone(),
                identity: report.runtime.backend.clone(),
                scenarios: report.scenarios.clone(),
                resources: report.resources.clone(),
                lifecycle: report.lifecycle.clone(),
            })
            .collect::<Vec<_>>();
        backends.sort_by(|left, right| left.identity.name.cmp(&right.identity.name));
        Ok(Self {
            schema_version: BACKEND_COMPARISON_SCHEMA_VERSION,
            profile: reference.manifest.profile,
            manifest_digest: reference.manifest.semantic_digest.clone(),
            workload_version: reference.workload_version,
            workload_digest: reference.workload_digest.clone(),
            limit_mode: reference.limit_mode,
            backends,
        })
    }

    pub fn write(&self, path: &Path) -> Result<()> {
        write_json(path, self)
    }

    pub fn markdown(&self) -> String {
        let mut output = String::from("## Hubuum storage-backend comparison\n\n");
        output.push_str(&format!(
            "Same `{}` corpus `{}` and workload `{}` across {} backend(s). Values are informational and are not normalized across different physical storage models.\n\n",
            self.profile.as_str(),
            &self.manifest_digest[..12],
            &self.workload_digest[..12],
            self.backends.len()
        ));
        output.push_str(
            "| Backend | Version | Scenario | Phase | p95 ms | Requests/s | Failures |\n",
        );
        output.push_str("| --- | --- | --- | --- | ---: | ---: | ---: |\n");
        for backend in &self.backends {
            for scenario in &backend.scenarios {
                output.push_str(&format!(
                    "| {} | {} | {} | {} | {:.2} | {:.2} | {} |\n",
                    backend.identity.name,
                    backend.identity.version,
                    scenario.name,
                    scenario.phase,
                    scenario.latency.p95_ms,
                    scenario.requests_per_second,
                    scenario.failures + scenario.timeouts
                ));
            }
        }
        output.push_str("\n<details><summary>Backend resources and lifecycle</summary>\n\n");
        output.push_str(
            "| Backend | Storage bytes | Data bytes | Index bytes | Application CPU s | Backend CPU s | Load ms |\n",
        );
        output.push_str("| --- | ---: | ---: | ---: | ---: | ---: | ---: |\n");
        for backend in &self.backends {
            output.push_str(&format!(
                "| {} | {} | {} | {} | {:.2} | {} | {} |\n",
                backend.identity.name,
                backend.resources.backend.storage_bytes,
                format_optional_bytes(backend.resources.backend.data_bytes),
                format_optional_bytes(backend.resources.backend.index_bytes),
                backend.resources.application_cpu_seconds,
                backend
                    .resources
                    .backend
                    .cpu_seconds
                    .map(|value| format!("{value:.2}"))
                    .unwrap_or_else(|| "-".to_string()),
                backend.lifecycle.dataset_loading_ms
            ));
        }
        output.push_str("\n</details>\n");
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

fn scenario_keys(report: &ScaleBenchmarkReport) -> BTreeSet<(&str, &str)> {
    report
        .scenarios
        .iter()
        .map(|scenario| (scenario.name.as_str(), scenario.phase.as_str()))
        .collect()
}

#[derive(Clone, Debug, PartialEq)]
pub struct ScaleAssessment {
    markdown: String,
    failures: Vec<String>,
}

impl ScaleAssessment {
    pub fn assess(head: &ScaleBenchmarkReport, base: Option<&ScaleBenchmarkReport>) -> Self {
        let mut failures = Vec::new();
        if !head.correctness.passed() {
            failures.push("head correctness checks failed".to_string());
        }
        if let Some(base) = base {
            if let Err(error) = head.manifest.equivalent_to(&base.manifest) {
                failures.push(error.to_string());
            }
            if head.workload_version != base.workload_version
                || head.workload_seed != base.workload_seed
                || head.workload_digest != base.workload_digest
                || head.limit_mode != base.limit_mode
                || head.scale_axis != base.scale_axis
            {
                failures.push(
                    "base and head workload specifications or limit modes differ".to_string(),
                );
            }
            if head.runtime.backend != base.runtime.backend {
                failures.push(
                    "base and head backend identities or effective settings differ".to_string(),
                );
            }
        }
        let markdown = render_assessment(head, base, &failures);
        Self { markdown, failures }
    }

    pub fn markdown(&self) -> &str {
        &self.markdown
    }

    pub fn append_markdown(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut output = OpenOptions::new().create(true).append(true).open(path)?;
        output.write_all(self.markdown.as_bytes())?;
        Ok(())
    }

    pub fn ensure_passed(&self) -> Result<()> {
        if self.failures.is_empty() {
            Ok(())
        } else {
            Err(invalid_data(self.failures.join("; ")))
        }
    }
}

fn render_assessment(
    head: &ScaleBenchmarkReport,
    base: Option<&ScaleBenchmarkReport>,
    failures: &[String],
) -> String {
    let base_scenarios = base
        .map(|report| {
            report
                .scenarios
                .iter()
                .map(|scenario| ((scenario.name.as_str(), scenario.phase.as_str()), scenario))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();
    let paired_scenarios = head
        .scenarios
        .iter()
        .filter_map(|scenario| {
            base_scenarios
                .get(&(scenario.name.as_str(), scenario.phase.as_str()))
                .copied()
                .map(|base_scenario| (base_scenario, scenario))
        })
        .collect::<Vec<_>>();
    let median_p95_change = median_percent_change(
        &paired_scenarios
            .iter()
            .filter_map(|(base, head)| percent_change(base.latency.p95_ms, head.latency.p95_ms))
            .collect::<Vec<_>>(),
    );
    let median_throughput_change = median_percent_change(
        &paired_scenarios
            .iter()
            .filter_map(|(base, head)| {
                percent_change(base.requests_per_second, head.requests_per_second)
            })
            .collect::<Vec<_>>(),
    );
    let scenario_failures = head
        .scenarios
        .iter()
        .map(|scenario| scenario.failures + scenario.timeouts)
        .sum::<u64>();
    let mode = match head.limit_mode {
        LimitMode::Standard => "standard",
        LimitMode::Extended => "extended",
    };
    let mut output = format!(
        "## {} / {} / {mode}\n\n",
        head.runtime.backend.name,
        head.manifest.profile.as_str()
    );
    if let Some(base) = base {
        output.push_str(&format!(
            "Base `{}` → PR `{}`; dataset `{}`; {} paired scenario/phase rows.\n\n",
            short_report_label(&base.label),
            short_report_label(&head.label),
            &head.manifest.semantic_digest[..12],
            paired_scenarios.len()
        ));
    } else {
        output.push_str(&format!(
            "Run `{}`; dataset `{}`; {} scenario/phase rows.\n\n",
            short_report_label(&head.label),
            &head.manifest.semantic_digest[..12],
            head.scenarios.len()
        ));
    }
    output.push_str(&format!(
        "| Correctness | Paired rows | Median p95 change | Median throughput change | Storage size change | Index size change |\n\
         | --- | ---: | ---: | ---: | ---: | ---: |\n\
         | {} | {} | {} | {} | {} | {} |\n\n",
        if failures.is_empty() && scenario_failures == 0 {
            "passed"
        } else {
            "failed"
        },
        paired_scenarios.len(),
        format_percent(median_p95_change),
        format_percent(median_throughput_change),
        format_percent(base.and_then(|base| percent_change(
            base.resources.backend.storage_bytes as f64,
            head.resources.backend.storage_bytes as f64,
        ))),
        format_percent(base.and_then(|base| {
            base.resources
                .backend
                .index_bytes
                .zip(head.resources.backend.index_bytes)
                .and_then(|(base, head)| percent_change(base as f64, head as f64))
        }))
    ));
    output.push_str(
        "Timing and resource differences are informational. A single paired run is evidence, not a regression threshold. The summary changes are medians of the paired per-scenario changes.\n\n",
    );
    output.push_str(&format!(
        "<details><summary>Scenario timing ({} rows)</summary>\n\n",
        head.scenarios.len()
    ));
    output.push_str(
        "| Scenario | Phase | Base p95 ms | PR p95 ms | p95 change | Base rps | PR rps | Throughput change | Failures |\n",
    );
    output.push_str("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n");
    for scenario in &head.scenarios {
        let base_scenario = base_scenarios
            .get(&(scenario.name.as_str(), scenario.phase.as_str()))
            .copied();
        let base_p95 = base_scenario
            .map(|value| format!("{:.2}", value.latency.p95_ms))
            .unwrap_or_else(|| "-".to_string());
        let base_throughput = base_scenario
            .map(|value| format!("{:.2}", value.requests_per_second))
            .unwrap_or_else(|| "-".to_string());
        output.push_str(&format!(
            "| {} | {} | {} | {:.2} | {} | {} | {:.2} | {} | {} |\n",
            scenario.name,
            scenario.phase,
            base_p95,
            scenario.latency.p95_ms,
            format_percent(
                base_scenario
                    .and_then(|base| percent_change(base.latency.p95_ms, scenario.latency.p95_ms,))
            ),
            base_throughput,
            scenario.requests_per_second,
            format_percent(base_scenario.and_then(|base| percent_change(
                base.requests_per_second,
                scenario.requests_per_second,
            ))),
            scenario.failures + scenario.timeouts
        ));
    }
    output.push_str("\n</details>\n\n");
    output.push_str("<details><summary>Resource and lifecycle comparison</summary>\n\n");
    output.push_str("| Metric | Base | PR | Change |\n");
    output.push_str("| --- | ---: | ---: | ---: |\n");
    render_comparison_row(
        &mut output,
        "Storage size",
        base.map(|value| value.resources.backend.storage_bytes as f64),
        head.resources.backend.storage_bytes as f64,
        format_bytes,
    );
    if let Some(head_data_bytes) = head.resources.backend.data_bytes {
        render_comparison_row(
            &mut output,
            "Data size",
            base.and_then(|value| value.resources.backend.data_bytes)
                .map(|value| value as f64),
            head_data_bytes as f64,
            format_bytes,
        );
    }
    if let Some(head_index_bytes) = head.resources.backend.index_bytes {
        render_comparison_row(
            &mut output,
            "Index size",
            base.and_then(|value| value.resources.backend.index_bytes)
                .map(|value| value as f64),
            head_index_bytes as f64,
            format_bytes,
        );
    }
    render_comparison_row(
        &mut output,
        "Peak application RSS",
        base.map(|value| value.resources.peak_application_resident_bytes as f64),
        head.resources.peak_application_resident_bytes as f64,
        format_bytes,
    );
    render_comparison_row(
        &mut output,
        "Application CPU",
        base.map(|value| value.resources.application_cpu_seconds),
        head.resources.application_cpu_seconds,
        |value| format!("{value:.2} s"),
    );
    render_comparison_row(
        &mut output,
        "Dataset loading",
        base.map(|value| value.lifecycle.dataset_loading_ms as f64),
        head.lifecycle.dataset_loading_ms as f64,
        |value| format!("{value:.0} ms"),
    );
    if let Some(bytes) = head.lifecycle.backup_size_lower_bound_bytes {
        render_comparison_row(
            &mut output,
            "Backup size lower bound",
            base.and_then(|value| value.lifecycle.backup_size_lower_bound_bytes)
                .map(|value| value as f64),
            bytes as f64,
            |value| format!("at least {}", format_bytes(value)),
        );
    }
    if let Some(head_backup_ms) = head.lifecycle.backup_generation_ms {
        render_comparison_row(
            &mut output,
            "Backup generation",
            base.and_then(|value| value.lifecycle.backup_generation_ms)
                .map(|value| value as f64),
            head_backup_ms as f64,
            |value| format!("{value:.0} ms"),
        );
    }
    if let Some(head_backup_bytes) = head.lifecycle.backup_artifact_bytes {
        render_comparison_row(
            &mut output,
            "Backup artifact",
            base.and_then(|value| value.lifecycle.backup_artifact_bytes)
                .map(|value| value as f64),
            head_backup_bytes as f64,
            format_bytes,
        );
    }
    output.push_str("\n</details>\n\n");
    if failures.is_empty() {
        output.push_str("Correctness checks passed.\n\n");
    } else {
        output.push_str("Correctness checks failed:\n\n");
        for failure in failures {
            output.push_str(&format!("- {failure}\n"));
        }
        output.push('\n');
    }
    output.push_str(&format!(
        "Lifecycle outcome: `{}`; backend `{}`; storage {} bytes; peak application RSS {} bytes.\n\n",
        head.lifecycle.outcome,
        head.runtime.backend.name,
        head.resources.backend.storage_bytes,
        head.resources.peak_application_resident_bytes
    ));
    output
}

fn percent_change(baseline: f64, comparison: f64) -> Option<f64> {
    (baseline != 0.0).then_some((comparison / baseline - 1.0) * 100.0)
}

fn median_percent_change(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut values = values.to_vec();
    values.sort_by(f64::total_cmp);
    Some(float_percentile(&values, 50))
}

fn format_optional_bytes(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string())
}

fn render_comparison_row(
    output: &mut String,
    name: &str,
    baseline: Option<f64>,
    comparison: f64,
    formatter: impl Fn(f64) -> String,
) {
    output.push_str(&format!(
        "| {name} | {} | {} | {} |\n",
        baseline.map(&formatter).unwrap_or_else(|| "-".to_string()),
        formatter(comparison),
        format_percent(baseline.and_then(|baseline| percent_change(baseline, comparison)))
    ));
}
