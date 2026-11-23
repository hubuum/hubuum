use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use super::{
    DatasetManifest, LimitMode, ProfileName, ResourceTotals, Result, SENSITIVITY_V1, ScaleAxis,
    ScaleProfile, WORKLOAD_V1, encode_digest, invalid_data, write_json,
};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WorkloadSpec {
    pub schema_version: u32,
    pub workload_version: u32,
    pub seed: u64,
    pub warmup_requests: usize,
    pub single_client_samples: usize,
    pub moderate_concurrency: usize,
    pub higher_concurrency: usize,
    pub concurrent_samples: usize,
    pub mixed_samples: usize,
    pub request_timeout_seconds: u64,
    pub scenarios: Vec<WorkloadScenario>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WorkloadScenario {
    pub name: String,
    pub principal: String,
    pub path: String,
    pub weight: u64,
    #[serde(default = "enabled")]
    pub include_in_mixed: bool,
    #[serde(default)]
    pub scale_axes: Vec<ScaleAxis>,
    pub warmup_requests: Option<usize>,
    pub single_client_samples: Option<usize>,
    #[serde(default)]
    pub traverse: bool,
    #[serde(default)]
    pub verify_sparse_visibility: bool,
}

impl WorkloadScenario {
    pub fn applies_to(&self, scale_axis: Option<ScaleAxis>) -> bool {
        self.scale_axes.is_empty()
            || scale_axis.is_none()
            || scale_axis.is_some_and(|axis| self.scale_axes.contains(&axis))
    }
}

const fn enabled() -> bool {
    true
}

impl WorkloadSpec {
    pub fn bundled() -> Result<Self> {
        let workload = toml::from_str::<Self>(WORKLOAD_V1)?;
        workload.validate()?;
        Ok(workload)
    }

    pub fn read(path: &Path) -> Result<Self> {
        let workload = toml::from_str::<Self>(&fs::read_to_string(path)?)?;
        workload.validate()?;
        Ok(workload)
    }

    pub fn validate(&self) -> Result<()> {
        if self.schema_version != 1 || self.workload_version == 0 || self.seed == 0 {
            return Err(invalid_data("unsupported or incomplete workload identity"));
        }
        if self.scenarios.is_empty()
            || !self
                .scenarios
                .iter()
                .any(|scenario| scenario.include_in_mixed)
            || self.single_client_samples == 0
            || self.moderate_concurrency == 0
            || self.higher_concurrency < self.moderate_concurrency
            || self.concurrent_samples < self.higher_concurrency
            || self.mixed_samples == 0
        {
            return Err(invalid_data("workload sampling settings are invalid"));
        }
        let mut names = BTreeSet::new();
        for scenario in &self.scenarios {
            let scale_axes = scenario.scale_axes.iter().copied().collect::<BTreeSet<_>>();
            if scenario.name.is_empty()
                || !names.insert(&scenario.name)
                || scenario.weight == 0
                || scale_axes.len() != scenario.scale_axes.len()
                || scenario.warmup_requests == Some(0)
                || scenario.single_client_samples == Some(0)
                || !["admin", "tenant", "sparse"].contains(&scenario.principal.as_str())
                || !scenario.path.starts_with("/api/")
            {
                return Err(invalid_data(format!(
                    "invalid workload scenario '{}'",
                    scenario.name
                )));
            }
        }
        Ok(())
    }

    pub fn digest(&self) -> Result<String> {
        Ok(encode_digest(
            Sha256::digest(serde_json::to_vec(self)?).as_slice(),
        ))
    }

    pub fn render_path(
        &self,
        scenario: &WorkloadScenario,
        manifest: &DatasetManifest,
        limit_mode: LimitMode,
    ) -> Result<String> {
        let mut values = manifest
            .anchors
            .iter()
            .map(|(key, value)| (key.clone(), value.to_string()))
            .collect::<BTreeMap<_, _>>();
        values.insert(
            "page_limit".to_string(),
            limit_mode.page_limit().to_string(),
        );
        values.insert(
            "candidate_page_limit".to_string(),
            limit_mode.candidate_page_limit().to_string(),
        );
        values.insert(
            "graph_depth".to_string(),
            limit_mode.graph_depth().to_string(),
        );
        render_template(&scenario.path, &values)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct SensitivitySpec {
    pub schema_version: u32,
    pub experiment_version: u32,
    pub axes: Vec<SensitivityAxisSpec>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct SensitivityAxisSpec {
    pub axis: ScaleAxis,
    pub percent_steps: Vec<u64>,
    #[serde(default = "all_limit_modes")]
    pub limit_modes: Vec<LimitMode>,
    pub scenario: String,
    pub phase: String,
    pub traversal_phase: Option<String>,
    #[serde(default)]
    pub depth_probes: Vec<DepthProbeSpec>,
    pub depth_probe_scope: Option<DepthProbeScope>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct DepthProbeSpec {
    pub depth: u64,
    pub scenario: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DepthProbeScope {
    FixedLocalGraph,
    GrowingLocalDensity,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct SensitivityPlan {
    pub schema_version: u32,
    pub experiment_version: u32,
    pub profile: ProfileName,
    pub limit_mode: LimitMode,
    pub baseline_totals: ResourceTotals,
    pub points: Vec<SensitivityPlanPoint>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct SensitivityPlanPoint {
    pub axis: ScaleAxis,
    pub added_percent: u64,
    pub added_count: u64,
    pub comparison_total: u64,
}

impl SensitivitySpec {
    pub fn bundled() -> Result<Self> {
        let spec = toml::from_str::<Self>(SENSITIVITY_V1)?;
        spec.validate()?;
        Ok(spec)
    }

    pub fn read(path: &Path) -> Result<Self> {
        let spec = toml::from_str::<Self>(&fs::read_to_string(path)?)?;
        spec.validate()?;
        Ok(spec)
    }

    pub fn validate(&self) -> Result<()> {
        if self.schema_version != 1 || self.experiment_version == 0 || self.axes.is_empty() {
            return Err(invalid_data(
                "unsupported or incomplete scale sensitivity specification",
            ));
        }
        let mut axes = BTreeSet::new();
        for axis in &self.axes {
            if !axes.insert(axis.axis) {
                return Err(invalid_data(format!(
                    "scale sensitivity axis '{}' appears more than once",
                    axis.axis.as_str()
                )));
            }
            if axis.scenario.trim().is_empty() || axis.phase.trim().is_empty() {
                return Err(invalid_data(format!(
                    "scale sensitivity axis '{}' requires a scenario and phase",
                    axis.axis.as_str()
                )));
            }
            let limit_modes = axis.limit_modes.iter().copied().collect::<BTreeSet<_>>();
            if limit_modes.is_empty() || limit_modes.len() != axis.limit_modes.len() {
                return Err(invalid_data(format!(
                    "scale sensitivity axis '{}' requires distinct limit modes",
                    axis.axis.as_str()
                )));
            }
            if axis
                .traversal_phase
                .as_ref()
                .is_some_and(|phase| phase.trim().is_empty())
            {
                return Err(invalid_data(format!(
                    "scale sensitivity axis '{}' has an empty traversal phase",
                    axis.axis.as_str()
                )));
            }
            if axis.depth_probes.is_empty() != axis.depth_probe_scope.is_none() {
                return Err(invalid_data(format!(
                    "scale sensitivity axis '{}' must set both depth probes and their scope",
                    axis.axis.as_str()
                )));
            }
            let mut depths = BTreeSet::new();
            let mut depth_scenarios = BTreeSet::new();
            for probe in &axis.depth_probes {
                if probe.depth == 0
                    || probe.scenario.trim().is_empty()
                    || !depths.insert(probe.depth)
                    || !depth_scenarios.insert(probe.scenario.as_str())
                {
                    return Err(invalid_data(format!(
                        "scale sensitivity axis '{}' requires distinct positive depth probes and scenarios",
                        axis.axis.as_str()
                    )));
                }
            }
            if axis
                .depth_probes
                .windows(2)
                .any(|probes| probes[0].depth >= probes[1].depth)
            {
                return Err(invalid_data(format!(
                    "scale sensitivity axis '{}' requires increasing depth probes",
                    axis.axis.as_str()
                )));
            }
            if axis.percent_steps.is_empty()
                || axis.percent_steps.contains(&0)
                || axis
                    .percent_steps
                    .windows(2)
                    .any(|steps| steps[0] >= steps[1])
            {
                return Err(invalid_data(format!(
                    "scale sensitivity axis '{}' requires increasing non-zero percentage steps",
                    axis.axis.as_str()
                )));
            }
        }
        Ok(())
    }

    pub fn plan(&self, profile: &ScaleProfile, limit_mode: LimitMode) -> Result<SensitivityPlan> {
        self.validate()?;
        profile.validate()?;
        let applicable_axes = self.axes_for(limit_mode).collect::<Vec<_>>();
        if applicable_axes.is_empty() {
            return Err(invalid_data(format!(
                "scale sensitivity specification has no axes for the {limit_mode:?} limit mode"
            )));
        }
        let mut points = Vec::new();
        for axis_spec in applicable_axes {
            let baseline_total = axis_spec.axis.total(&profile.totals);
            for added_percent in &axis_spec.percent_steps {
                let scaled = baseline_total
                    .checked_mul(*added_percent)
                    .ok_or_else(|| invalid_data("scale sensitivity increment overflowed"))?;
                if scaled % 100 != 0 {
                    return Err(invalid_data(format!(
                        "{}% of the {} baseline {} is not an exact resource count",
                        added_percent,
                        axis_spec.axis.as_str(),
                        baseline_total
                    )));
                }
                let added_count = scaled / 100;
                let comparison = profile
                    .clone()
                    .with_increment(axis_spec.axis, added_count)?;
                points.push(SensitivityPlanPoint {
                    axis: axis_spec.axis,
                    added_percent: *added_percent,
                    added_count,
                    comparison_total: axis_spec.axis.total(&comparison.totals),
                });
            }
        }
        Ok(SensitivityPlan {
            schema_version: self.schema_version,
            experiment_version: self.experiment_version,
            profile: profile.name,
            limit_mode,
            baseline_totals: profile.totals.clone(),
            points,
        })
    }

    pub(super) fn axes_for(
        &self,
        limit_mode: LimitMode,
    ) -> impl Iterator<Item = &SensitivityAxisSpec> {
        self.axes
            .iter()
            .filter(move |axis| axis.limit_modes.contains(&limit_mode))
    }
}

fn all_limit_modes() -> Vec<LimitMode> {
    vec![LimitMode::Standard, LimitMode::Extended]
}

impl SensitivityPlan {
    pub fn write(&self, path: &Path) -> Result<()> {
        write_json(path, self)
    }
}

fn render_template(template: &str, values: &BTreeMap<String, String>) -> Result<String> {
    let mut output = String::with_capacity(template.len());
    let mut remaining = template;
    while let Some(start) = remaining.find('{') {
        output.push_str(&remaining[..start]);
        let after_start = &remaining[start + 1..];
        let end = after_start.find('}').ok_or_else(|| {
            invalid_data(format!("unclosed workload placeholder in '{template}'"))
        })?;
        let name = &after_start[..end];
        let value = values.get(name).ok_or_else(|| {
            invalid_data(format!("workload path references unknown anchor '{name}'"))
        })?;
        output.push_str(value);
        remaining = &after_start[end + 1..];
    }
    output.push_str(remaining);
    if output.contains('}') {
        return Err(invalid_data(format!(
            "unmatched workload placeholder in '{template}'"
        )));
    }
    Ok(output)
}
