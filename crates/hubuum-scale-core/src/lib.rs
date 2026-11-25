//! Deterministic data and report model for the stateful scale benchmark.
//!
//! The benchmark is intentionally config-free at this layer. Profile and
//! workload documents are immutable inputs, and reports contain only aggregate
//! measurements and stable fixture identifiers.

mod workload;
pub use workload::{
    DepthProbeScope, DepthProbeSpec, SensitivityAxisSpec, SensitivityPlan, SensitivityPlanPoint,
    SensitivitySpec, WorkloadScenario, WorkloadSpec,
};
mod impact;
pub use impact::{MetricDelta, ScaleImpactReport, ScenarioImpact};
mod sensitivity;
pub use sensitivity::{
    AxisSensitivityReport, DepthProbePointReport, DepthProbeSensitivityReport,
    ScaleSensitivityReport, SensitivityPointReport,
};
mod comparison;
mod formatting;
pub use comparison::{BackendComparisonReport, BackendPerformanceSummary, ScaleAssessment};

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;
use std::str::FromStr;

use async_trait::async_trait;
use hubuum_storage_core::MAX_STORAGE_CANDIDATE_PAGE_SIZE;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const LARGE_PROFILE: &str = include_str!("../../../scale-benchmarks/profiles/large.toml");
const HUGE_PROFILE: &str = include_str!("../../../scale-benchmarks/profiles/huge.toml");
const WORKLOAD_V1: &str = include_str!("../../../scale-benchmarks/workloads/v1.toml");
const SENSITIVITY_V1: &str = include_str!("../../../scale-benchmarks/sensitivity-v1.toml");
const PROFILE_SCHEMA_VERSION: u32 = 1;
const DATASET_SCHEMA_VERSION: u32 = 3;
const REPORT_SCHEMA_VERSION: u32 = 3;
const IMPACT_REPORT_SCHEMA_VERSION: u32 = 5;
const SENSITIVITY_REPORT_SCHEMA_VERSION: u32 = 3;
const BACKEND_COMPARISON_SCHEMA_VERSION: u32 = 1;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProfileName {
    Large,
    Huge,
}

impl ProfileName {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Large => "large",
            Self::Huge => "huge",
        }
    }
}

impl FromStr for ProfileName {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "large" => Ok(Self::Large),
            "huge" => Ok(Self::Huge),
            _ => Err(format!(
                "unknown scale profile '{value}'; expected large or huge"
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ScaleAxis {
    Classes,
    Objects,
    ObjectHeavyObjects,
    ObjectRelations,
    ConcentratedObjectRelations,
    DenseObjectRelations,
}

impl ScaleAxis {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Classes => "classes",
            Self::Objects => "objects",
            Self::ObjectHeavyObjects => "object_heavy_objects",
            Self::ObjectRelations => "object_relations",
            Self::ConcentratedObjectRelations => "concentrated_object_relations",
            Self::DenseObjectRelations => "dense_object_relations",
        }
    }

    pub const fn topology(self) -> &'static str {
        match self {
            Self::Classes => {
                "balanced-region classes added while object and relation totals remain fixed"
            }
            Self::Objects => "balanced-region objects distributed across existing classes",
            Self::ObjectHeavyObjects => {
                "object-heavy-region objects concentrated into its existing classes"
            }
            Self::ObjectRelations => {
                "balanced-region spread relations across existing class relations"
            }
            Self::ConcentratedObjectRelations => {
                "object-heavy-region relations concentrated under one class relation"
            }
            Self::DenseObjectRelations => {
                "class-heavy-region relations approaching unique class-pair capacity"
            }
        }
    }

    const fn target_region_name(self) -> &'static str {
        match self {
            Self::Classes | Self::Objects | Self::ObjectRelations => "balanced",
            Self::ObjectHeavyObjects | Self::ConcentratedObjectRelations => "object_heavy",
            Self::DenseObjectRelations => "class_heavy",
        }
    }

    const fn total(self, totals: &ResourceTotals) -> u64 {
        match self {
            Self::Classes => totals.classes,
            Self::Objects | Self::ObjectHeavyObjects => totals.objects,
            Self::ObjectRelations
            | Self::ConcentratedObjectRelations
            | Self::DenseObjectRelations => totals.object_relations,
        }
    }

    const fn set_total(self, totals: &mut ResourceTotals, value: u64) {
        match self {
            Self::Classes => totals.classes = value,
            Self::Objects | Self::ObjectHeavyObjects => totals.objects = value,
            Self::ObjectRelations
            | Self::ConcentratedObjectRelations
            | Self::DenseObjectRelations => {
                totals.object_relations = value;
            }
        }
    }

    const fn region_total(self, regions: &RegionSpecs) -> u64 {
        match self {
            Self::Classes => regions.balanced.classes,
            Self::Objects => regions.balanced.objects,
            Self::ObjectHeavyObjects => regions.object_heavy.objects,
            Self::ObjectRelations => regions.balanced.object_relations,
            Self::ConcentratedObjectRelations => regions.object_heavy.object_relations,
            Self::DenseObjectRelations => regions.class_heavy.object_relations,
        }
    }

    const fn set_region_total(self, regions: &mut RegionSpecs, value: u64) {
        match self {
            Self::Classes => regions.balanced.classes = value,
            Self::Objects => regions.balanced.objects = value,
            Self::ObjectHeavyObjects => regions.object_heavy.objects = value,
            Self::ObjectRelations => regions.balanced.object_relations = value,
            Self::ConcentratedObjectRelations => regions.object_heavy.object_relations = value,
            Self::DenseObjectRelations => regions.class_heavy.object_relations = value,
        }
    }

    const fn set_manifest_region_total(self, region: &mut RegionSpec, value: u64) {
        match self {
            Self::Classes => region.classes = value,
            Self::Objects | Self::ObjectHeavyObjects => region.objects = value,
            Self::ObjectRelations
            | Self::ConcentratedObjectRelations
            | Self::DenseObjectRelations => {
                region.object_relations = value;
            }
        }
    }

    const fn manifest_region_total(self, region: &RegionSpec) -> u64 {
        match self {
            Self::Classes => region.classes,
            Self::Objects | Self::ObjectHeavyObjects => region.objects,
            Self::ObjectRelations
            | Self::ConcentratedObjectRelations
            | Self::DenseObjectRelations => region.object_relations,
        }
    }

    const fn section_title(self) -> &'static str {
        match self {
            Self::Classes => "Class-count growth",
            Self::Objects => "Balanced object growth",
            Self::ObjectHeavyObjects => "Object-heavy growth",
            Self::ObjectRelations => "Spread object-relation growth",
            Self::ConcentratedObjectRelations => "Concentrated object-relation growth",
            Self::DenseObjectRelations => "Dense class-pair relation growth",
        }
    }

    const fn relation_shape_name(self) -> Option<&'static str> {
        match self {
            Self::ObjectRelations => Some("balanced_spread_class_relation"),
            Self::ConcentratedObjectRelations => Some("concentrated_class_relation"),
            Self::DenseObjectRelations => Some("spread_class_relation"),
            Self::Classes | Self::Objects | Self::ObjectHeavyObjects => None,
        }
    }
}

impl FromStr for ScaleAxis {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "classes" => Ok(Self::Classes),
            "objects" => Ok(Self::Objects),
            "object-heavy-objects" | "object_heavy_objects" => Ok(Self::ObjectHeavyObjects),
            "object-relations" | "object_relations" => Ok(Self::ObjectRelations),
            "concentrated-object-relations" | "concentrated_object_relations" => {
                Ok(Self::ConcentratedObjectRelations)
            }
            "dense-object-relations" | "dense_object_relations" => Ok(Self::DenseObjectRelations),
            _ => Err(format!(
                "unknown scale axis '{value}'; expected classes, objects, object-heavy-objects, object-relations, concentrated-object-relations, or dense-object-relations"
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum LimitMode {
    Standard,
    Extended,
}

impl LimitMode {
    pub const fn page_limit(self) -> usize {
        match self {
            Self::Standard => 250,
            Self::Extended => 1_500,
        }
    }

    pub const fn candidate_page_limit(self) -> usize {
        let page_limit = self.page_limit();
        if page_limit < MAX_STORAGE_CANDIDATE_PAGE_SIZE {
            page_limit
        } else {
            MAX_STORAGE_CANDIDATE_PAGE_SIZE
        }
    }

    pub const fn graph_depth(self) -> usize {
        match self {
            Self::Standard => 5,
            Self::Extended => 8,
        }
    }

    pub const fn settings(self) -> EffectiveWorkloadLimits {
        match self {
            Self::Standard => EffectiveWorkloadLimits {
                default_page_limit: 100,
                maximum_page_limit: 250,
                maximum_graph_depth: 100,
                maximum_related_objects_per_include: 50,
                maximum_export_output_bytes: 262_144,
            },
            Self::Extended => EffectiveWorkloadLimits {
                default_page_limit: 250,
                maximum_page_limit: 1_500,
                maximum_graph_depth: 200,
                maximum_related_objects_per_include: 50,
                maximum_export_output_bytes: 4_194_304,
            },
        }
    }
}

impl FromStr for LimitMode {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "standard" => Ok(Self::Standard),
            "extended" => Ok(Self::Extended),
            _ => Err(format!(
                "unknown scale limit mode '{value}'; expected standard or extended"
            )),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ResourceTotals {
    pub collections: u64,
    pub classes: u64,
    pub objects: u64,
    pub class_relations: u64,
    pub object_relations: u64,
    pub principals: u64,
    pub groups: u64,
    pub memberships: u64,
    pub permission_grants: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct RegionSpec {
    pub collections: u64,
    pub classes: u64,
    pub objects: u64,
    pub class_relations: u64,
    pub object_relations: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct RegionSpecs {
    pub object_heavy: RegionSpec,
    pub class_heavy: RegionSpec,
    pub balanced: RegionSpec,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct OverlaySpec {
    pub history_resources: u64,
    pub history_typical_revisions: u64,
    pub history_moderate_resources: u64,
    pub history_moderate_revisions: u64,
    pub history_heavy_resources: u64,
    pub history_heavy_revisions: u64,
    pub computed_classes: u64,
    pub templates: u64,
    pub remote_targets: u64,
    pub event_sinks: u64,
    pub event_subscriptions: u64,
    pub event_deliveries: u64,
    pub terminal_tasks: u64,
    pub audit_events: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct InvariantSpec {
    pub minimum_hot_class_objects: u64,
    pub maximum_median_objects_per_class: u64,
    pub minimum_hub_object_degree: u64,
    pub minimum_concentrated_relation_count: u64,
    pub minimum_heavy_history_revisions: u64,
    pub maximum_sparse_visibility_percent: f64,
    pub class_component_size: u64,
    pub adversarial_component_size: u64,
    pub maximum_graph_depth: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ProvisioningLimits {
    #[serde(default = "default_lifecycle_memory")]
    pub lifecycle_max_resident_bytes: u64,
    #[serde(default = "default_lifecycle_timeout")]
    pub lifecycle_stage_timeout_seconds: u64,
    #[serde(default = "default_capture_rows")]
    pub backup_max_capture_rows: u64,
    pub backup_max_output_bytes: u64,
    pub restore_max_upload_bytes: u64,
    pub db_statement_timeout_ms: u64,
    pub db_pool_size: u64,
}

fn default_lifecycle_memory() -> u64 {
    6 * 1024 * 1024 * 1024
}
fn default_lifecycle_timeout() -> u64 {
    1800
}

fn default_capture_rows() -> u64 {
    1_000_000
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ScaleProfile {
    pub schema_version: u32,
    pub profile_version: u32,
    pub name: ProfileName,
    pub seed: u64,
    pub totals: ResourceTotals,
    pub regions: RegionSpecs,
    pub overlays: OverlaySpec,
    pub invariants: InvariantSpec,
    pub provisioning: ProvisioningLimits,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct LoadReport {
    pub backend: String,
    pub profile: ProfileName,
    pub seed: u64,
    pub generation_ms: u64,
    pub loading_ms: u64,
    pub manifest: DatasetManifest,
}

impl ScaleProfile {
    pub fn bundled(name: ProfileName) -> Result<Self> {
        let text = match name {
            ProfileName::Large => LARGE_PROFILE,
            ProfileName::Huge => HUGE_PROFILE,
        };
        let profile = toml::from_str::<Self>(text)?;
        profile.validate()?;
        Ok(profile)
    }

    pub fn read(path: &Path) -> Result<Self> {
        let profile = toml::from_str::<Self>(&fs::read_to_string(path)?)?;
        profile.validate()?;
        Ok(profile)
    }

    pub fn with_seed(mut self, seed: u64) -> Result<Self> {
        if seed == 0 {
            return Err(invalid_data("dataset seed must be non-zero"));
        }
        self.seed = seed;
        Ok(self)
    }

    pub fn with_increment(mut self, axis: ScaleAxis, amount: u64) -> Result<Self> {
        if amount == 0 {
            return Err(invalid_data("scale increment must be non-zero"));
        }
        let total = axis
            .total(&self.totals)
            .checked_add(amount)
            .ok_or_else(|| invalid_data("scale total overflowed"))?;
        let region_total = axis
            .region_total(&self.regions)
            .checked_add(amount)
            .ok_or_else(|| invalid_data("scale region total overflowed"))?;
        axis.set_total(&mut self.totals, total);
        axis.set_region_total(&mut self.regions, region_total);
        self.validate()?;
        Ok(self)
    }

    pub fn validate(&self) -> Result<()> {
        if self.schema_version != PROFILE_SCHEMA_VERSION {
            return Err(invalid_data(format!(
                "unsupported scale profile schema version {}",
                self.schema_version
            )));
        }
        if self.profile_version == 0 || self.seed == 0 {
            return Err(invalid_data("profile version and seed must be non-zero"));
        }
        for (name, region) in [
            ("object_heavy", &self.regions.object_heavy),
            ("class_heavy", &self.regions.class_heavy),
            ("balanced", &self.regions.balanced),
        ] {
            if region.collections == 0 || region.classes == 0 {
                return Err(invalid_data(format!(
                    "profile region '{name}' requires non-zero collections and classes"
                )));
            }
        }
        for (label, actual, expected) in [
            (
                "collections",
                self.region_sum(|region| region.collections),
                self.totals.collections,
            ),
            (
                "classes",
                self.region_sum(|region| region.classes),
                self.totals.classes,
            ),
            (
                "objects",
                self.region_sum(|region| region.objects),
                self.totals.objects,
            ),
            (
                "class relations",
                self.region_sum(|region| region.class_relations),
                self.totals.class_relations,
            ),
            (
                "object relations",
                self.region_sum(|region| region.object_relations),
                self.totals.object_relations,
            ),
        ] {
            if actual != expected {
                return Err(invalid_data(format!(
                    "profile {label} regions total {actual}, expected {expected}"
                )));
            }
        }
        if self.regions.object_heavy.classes < 2
            || self.regions.class_heavy.classes < self.invariants.class_component_size
            || self.invariants.class_component_size < 4
            || self.invariants.adversarial_component_size > self.invariants.class_component_size
        {
            return Err(invalid_data(
                "profile graph regions cannot realize required shapes",
            ));
        }
        if self.overlays.history_heavy_revisions < self.invariants.minimum_heavy_history_revisions
            || self.overlays.computed_classes < 10
            || self.overlays.computed_classes > self.regions.balanced.classes
            || self.overlays.history_resources > self.totals.objects
            || self.overlays.history_heavy_resources + self.overlays.history_moderate_resources
                > self.overlays.history_resources
            || self.overlays.event_sinks == 0
            || self.overlays.event_subscriptions == 0
            || self.overlays.event_deliveries == 0
        {
            return Err(invalid_data(
                "profile overlays do not satisfy required coverage",
            ));
        }
        if self.totals.principals < 1_000 || self.totals.groups < 100 {
            return Err(invalid_data(
                "scale profiles require at least 1,000 principals and hundreds of groups",
            ));
        }
        if self.provisioning.lifecycle_max_resident_bytes == 0
            || self.provisioning.lifecycle_stage_timeout_seconds == 0
            || self.provisioning.backup_max_capture_rows == 0
            || self.provisioning.backup_max_output_bytes == 0
            || self.provisioning.restore_max_upload_bytes == 0
            || self.provisioning.db_pool_size == 0
            || self.provisioning.db_pool_size > u64::from(u32::MAX)
        {
            return Err(invalid_data("profile provisioning limits are invalid"));
        }
        Ok(())
    }

    fn region_sum(&self, value: impl Fn(&RegionSpec) -> u64) -> u64 {
        value(&self.regions.object_heavy)
            + value(&self.regions.class_heavy)
            + value(&self.regions.balanced)
    }

    pub fn class_plan(&self) -> Vec<ClassPlan> {
        let mut plans = Vec::with_capacity(self.totals.classes as usize);
        let mut next_class_id = 1_u64;
        let mut next_collection_id = 1_u64;

        append_object_heavy_classes(
            &mut plans,
            &self.regions.object_heavy,
            next_class_id,
            next_collection_id,
        );
        next_class_id += self.regions.object_heavy.classes;
        next_collection_id += self.regions.object_heavy.collections;
        append_sparse_classes(
            &mut plans,
            DatasetRegion::ClassHeavy,
            &self.regions.class_heavy,
            next_class_id,
            next_collection_id,
        );
        next_class_id += self.regions.class_heavy.classes;
        next_collection_id += self.regions.class_heavy.collections;
        append_balanced_classes(
            &mut plans,
            &self.regions.balanced,
            next_class_id,
            next_collection_id,
        );

        let mut next_object_id = 1_u64;
        for plan in &mut plans {
            plan.first_object_id = (plan.object_count > 0).then_some(next_object_id);
            next_object_id += plan.object_count;
        }
        plans
    }

    pub fn manifest(&self) -> Result<DatasetManifest> {
        self.validate()?;
        let plans = self.class_plan();
        let relation_plans = class_relation_plan(self)?;
        let objects_per_class = Distribution::from_values(
            &plans
                .iter()
                .map(|plan| plan.object_count)
                .collect::<Vec<_>>(),
        );
        let classes_per_collection = class_collection_distribution(self, &plans);
        let relation_counts = object_relation_counts(self, &plans, &relation_plans)?;
        let relations_per_class_relation = Distribution::from_values(&relation_counts);
        let anchors = manifest_anchors(self, &plans)?;
        let relation_shapes =
            relation_shape_summaries(&plans, &relation_plans, &relation_counts, &anchors)?;
        let sparse_collection_count = (self.totals.collections / 100).max(1);
        let sparse_visibility =
            sparse_collection_count as f64 * 100.0 / self.totals.collections as f64;
        let regions = BTreeMap::from([
            (
                "object_heavy".to_string(),
                self.regions.object_heavy.clone(),
            ),
            ("class_heavy".to_string(), self.regions.class_heavy.clone()),
            ("balanced".to_string(), self.regions.balanced.clone()),
            (
                "history_heavy".to_string(),
                RegionSpec {
                    collections: 0,
                    classes: 0,
                    objects: self.overlays.history_resources,
                    class_relations: 0,
                    object_relations: 0,
                },
            ),
            (
                "authorization_adversarial".to_string(),
                RegionSpec {
                    collections: sparse_collection_count,
                    classes: 0,
                    objects: 0,
                    class_relations: 0,
                    object_relations: 0,
                },
            ),
        ]);
        let graph_shapes = GraphShapeSummary {
            class_components: ceil_div(self.totals.classes, self.invariants.class_component_size),
            largest_class_component: self.invariants.class_component_size,
            object_components: self.totals.classes,
            largest_object_component: self.regions.object_heavy.objects,
            representative_class_depth: 4,
            maximum_class_depth: self.invariants.maximum_graph_depth,
            representative_object_depth: 4,
            maximum_object_depth: self.invariants.maximum_graph_depth,
            adversarial_component_classes: self.invariants.adversarial_component_size,
        };
        let history_revisions = Distribution {
            minimum: 1,
            median: self.overlays.history_typical_revisions,
            p95: self.overlays.history_moderate_revisions,
            p99: self.overlays.history_moderate_revisions,
            maximum: self.overlays.history_heavy_revisions,
        };
        let json_payload_bytes = Distribution {
            minimum: 96,
            median: 160,
            p95: 640,
            p99: 2_176,
            maximum: 8_320,
        };
        let object_relation_degree = Distribution {
            minimum: 0,
            median: 4,
            p95: 18,
            p99: 64,
            maximum: self
                .invariants
                .minimum_hub_object_degree
                .max(self.regions.object_heavy.object_relations / 20),
        };
        let principals = BTreeMap::from([
            (
                "admin".to_string(),
                PrincipalVisibility {
                    visible_percent: 100.0,
                    distribution: "unscoped".to_string(),
                },
            ),
            (
                "tenant".to_string(),
                PrincipalVisibility {
                    visible_percent: 25.0,
                    distribution: "distributed_collection_grants".to_string(),
                },
            ),
            (
                "sparse".to_string(),
                PrincipalVisibility {
                    visible_percent: sparse_visibility,
                    distribution: "interleaved_across_identifier_and_sort_ranges".to_string(),
                },
            ),
        ]);

        let mut manifest = DatasetManifest {
            schema_version: DATASET_SCHEMA_VERSION,
            profile_version: self.profile_version,
            profile: self.name,
            seed: self.seed,
            semantic_digest: String::new(),
            totals: self.totals.clone(),
            overlays: self.overlays.clone(),
            regions,
            objects_per_class,
            classes_per_collection,
            object_relation_degree,
            object_relations_per_class_relation: relations_per_class_relation,
            relation_shapes,
            graph_shapes,
            history_revisions,
            json_payload_bytes,
            principals,
            anchors,
            provisioning: self.provisioning.clone(),
        };
        manifest.semantic_digest = semantic_digest(&manifest)?;
        manifest.validate(self)?;
        Ok(manifest)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DatasetRegion {
    ObjectHeavy,
    ClassHeavy,
    Balanced,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClassPlan {
    pub id: u64,
    pub collection_id: u64,
    pub region: DatasetRegion,
    pub object_count: u64,
    pub first_object_id: Option<u64>,
}

#[derive(Clone, Copy, Debug)]
pub struct ClassRelationPlan {
    pub id: u64,
    pub from_class_id: u64,
    pub to_class_id: u64,
    pub region: DatasetRegion,
}

fn append_object_heavy_classes(
    plans: &mut Vec<ClassPlan>,
    region: &RegionSpec,
    first_class_id: u64,
    first_collection_id: u64,
) {
    let hot_count = region.objects / 2;
    let secondary_count = region.objects / 6;
    let remaining = region.objects - hot_count - secondary_count;
    let remaining_classes = region.classes - 2;
    for offset in 0..region.classes {
        let object_count = match offset {
            0 => hot_count,
            1 => secondary_count,
            _ => distributed_count(remaining, remaining_classes, offset - 2),
        };
        plans.push(ClassPlan {
            id: first_class_id + offset,
            collection_id: first_collection_id + offset % region.collections,
            region: DatasetRegion::ObjectHeavy,
            object_count,
            first_object_id: None,
        });
    }
}

fn append_sparse_classes(
    plans: &mut Vec<ClassPlan>,
    region_name: DatasetRegion,
    region: &RegionSpec,
    first_class_id: u64,
    first_collection_id: u64,
) {
    let populated = region.classes * 3 / 4;
    let high_cardinality = (region.classes / 4).max(1);
    for offset in 0..region.classes {
        let object_count = if offset < populated {
            distributed_count(region.objects, populated, offset)
        } else {
            0
        };
        plans.push(ClassPlan {
            id: first_class_id + offset,
            collection_id: if offset < high_cardinality || region.collections == 1 {
                first_collection_id
            } else {
                first_collection_id + 1 + (offset - high_cardinality) % (region.collections - 1)
            },
            region: region_name,
            object_count,
            first_object_id: None,
        });
    }
}

fn append_balanced_classes(
    plans: &mut Vec<ClassPlan>,
    region: &RegionSpec,
    first_class_id: u64,
    first_collection_id: u64,
) {
    for offset in 0..region.classes {
        plans.push(ClassPlan {
            id: first_class_id + offset,
            collection_id: first_collection_id + offset % region.collections,
            region: DatasetRegion::Balanced,
            object_count: distributed_count(region.objects, region.classes, offset),
            first_object_id: None,
        });
    }
}

fn distributed_count(total: u64, buckets: u64, offset: u64) -> u64 {
    total / buckets + u64::from(offset < total % buckets)
}

const fn ceil_div(numerator: u64, denominator: u64) -> u64 {
    numerator.div_ceil(denominator)
}

fn class_collection_distribution(profile: &ScaleProfile, plans: &[ClassPlan]) -> Distribution {
    let mut counts = vec![0_u64; profile.totals.collections as usize];
    for plan in plans {
        counts[(plan.collection_id - 1) as usize] += 1;
    }
    Distribution::from_values(&counts)
}

fn object_relation_counts(
    profile: &ScaleProfile,
    classes: &[ClassPlan],
    relations: &[ClassRelationPlan],
) -> Result<Vec<u64>> {
    let mut counts = vec![0_u64; profile.totals.class_relations as usize];
    let first_class = classes
        .first()
        .ok_or_else(|| invalid_data("object-heavy region has no first class"))?;
    let second_class = classes
        .get(1)
        .ok_or_else(|| invalid_data("object-heavy region has no second class"))?;
    let hub_edges = profile.invariants.minimum_hub_object_degree;
    let remaining_edges = profile
        .regions
        .object_heavy
        .object_relations
        .saturating_sub(hub_edges);
    let remaining_capacity = u128::from(first_class.object_count.saturating_sub(1))
        * u128::from(second_class.object_count);
    if hub_edges > second_class.object_count
        || profile.regions.object_heavy.object_relations < hub_edges
        || u128::from(remaining_edges) > remaining_capacity
    {
        return Err(invalid_data(format!(
            "object-heavy relation request cannot realize {hub_edges} unique hub edges and {} remaining edges",
            remaining_edges
        )));
    }
    counts[0] = profile.regions.object_heavy.object_relations;

    for (region_name, requested) in [
        (
            DatasetRegion::ClassHeavy,
            profile.regions.class_heavy.object_relations,
        ),
        (
            DatasetRegion::Balanced,
            profile.regions.balanced.object_relations,
        ),
    ] {
        let eligible = relations
            .iter()
            .filter(|relation| relation.region == region_name)
            .filter_map(|relation| {
                let source = classes.get((relation.from_class_id - 1) as usize)?;
                let target = classes.get((relation.to_class_id - 1) as usize)?;
                (source.object_count > 0 && target.object_count > 0)
                    .then_some((relation, source, target))
            })
            .collect::<Vec<_>>();
        if requested > 0 && eligible.is_empty() {
            return Err(invalid_data(format!(
                "{region_name:?} requests object relations but has no eligible class pairs"
            )));
        }
        for (slot, (relation, source, target)) in eligible.iter().enumerate() {
            let assigned = distributed_count(requested, eligible.len() as u64, slot as u64);
            let capacity = u128::from(source.object_count) * u128::from(target.object_count);
            if u128::from(assigned) > capacity {
                return Err(invalid_data(format!(
                    "{region_name:?} class relation {} requests {assigned} unique object edges but its class pair capacity is {capacity}",
                    relation.id
                )));
            }
            counts[(relation.id - 1) as usize] = assigned;
        }
    }
    Ok(counts)
}

fn relation_shape_summaries(
    classes: &[ClassPlan],
    relations: &[ClassRelationPlan],
    counts: &[u64],
    anchors: &BTreeMap<String, i64>,
) -> Result<BTreeMap<String, RelationShapeSummary>> {
    let mut summaries = BTreeMap::new();
    for name in [
        "concentrated_class_relation_id",
        "spread_class_relation_id",
        "balanced_spread_class_relation_id",
    ] {
        let relation_id = u64::try_from(
            *anchors
                .get(name)
                .ok_or_else(|| invalid_data(format!("manifest has no '{name}' anchor")))?,
        )
        .map_err(|_| invalid_data(format!("manifest anchor '{name}' is not positive")))?;
        let relation_index = relation_id
            .checked_sub(1)
            .ok_or_else(|| invalid_data(format!("manifest anchor '{name}' is not positive")))?
            as usize;
        let relation = relations
            .get(relation_index)
            .ok_or_else(|| invalid_data(format!("manifest anchor '{name}' is out of range")))?;
        let source_objects = classes
            .get((relation.from_class_id - 1) as usize)
            .ok_or_else(|| invalid_data("relation source class is out of range"))?
            .object_count;
        let target_objects = classes
            .get((relation.to_class_id - 1) as usize)
            .ok_or_else(|| invalid_data("relation target class is out of range"))?
            .object_count;
        let pair_capacity = source_objects
            .checked_mul(target_objects)
            .ok_or_else(|| invalid_data("relation class-pair capacity overflowed"))?;
        if pair_capacity == 0 {
            return Err(invalid_data(format!(
                "manifest anchor '{name}' has zero class-pair capacity"
            )));
        }
        let edge_count = *counts
            .get(relation_index)
            .ok_or_else(|| invalid_data(format!("manifest anchor '{name}' has no edge count")))?;
        let saturation_basis_points =
            u64::try_from(u128::from(edge_count) * 10_000 / u128::from(pair_capacity))
                .map_err(|_| invalid_data("relation saturation overflowed"))?;
        summaries.insert(
            name.trim_end_matches("_id").to_string(),
            RelationShapeSummary {
                class_relation_id: relation_id,
                source_objects,
                target_objects,
                edge_count,
                pair_capacity,
                saturation_basis_points,
            },
        );
    }
    Ok(summaries)
}

pub fn class_relation_plan(profile: &ScaleProfile) -> Result<Vec<ClassRelationPlan>> {
    let mut output = Vec::with_capacity(profile.totals.class_relations as usize);
    let mut next_id = 1_u64;
    let mut first_class = 1_u64;
    for (region_name, region) in [
        (DatasetRegion::ObjectHeavy, &profile.regions.object_heavy),
        (DatasetRegion::ClassHeavy, &profile.regions.class_heavy),
        (DatasetRegion::Balanced, &profile.regions.balanced),
    ] {
        let component_size = profile.invariants.class_component_size;
        let mut candidates = Vec::new();
        for component_start in
            (first_class..first_class + region.classes).step_by(component_size as usize)
        {
            let component_end =
                (component_start + component_size).min(first_class + region.classes);
            for from in component_start..component_end {
                for to in from + 1..component_end {
                    let component = (from - first_class) / component_size;
                    if region_name != DatasetRegion::ClassHeavy || component != 1 || to == from + 1
                    {
                        candidates.push((from, to));
                    }
                }
            }
        }
        candidates.sort_by_key(|(from, to)| {
            let component = (*from - first_class) / component_size;
            let anchor_priority = u64::from(
                !((region_name == DatasetRegion::ObjectHeavy
                    && *from == first_class
                    && *to == first_class + 1)
                    || (region_name == DatasetRegion::ClassHeavy && component <= 1)),
            );
            (
                anchor_priority,
                mix(profile.seed ^ from.rotate_left(17) ^ to.rotate_left(31)),
                *from,
                *to,
            )
        });
        for (from, to) in candidates.into_iter().take(region.class_relations as usize) {
            output.push(ClassRelationPlan {
                id: next_id,
                from_class_id: from,
                to_class_id: to,
                region: region_name,
            });
            next_id += 1;
        }
        first_class += region.classes;
    }
    if output.len() != profile.totals.class_relations as usize {
        return Err(invalid_data(format!(
            "profile requests {} class relations, but its bounded graph regions can realize only {}",
            profile.totals.class_relations,
            output.len()
        )));
    }
    Ok(output)
}

fn mix(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn manifest_anchors(profile: &ScaleProfile, plans: &[ClassPlan]) -> Result<BTreeMap<String, i64>> {
    let hot = &plans[0];
    let secondary = &plans[1];
    let class_heavy = &plans[profile.regions.object_heavy.classes as usize];
    let deep_graph = plans
        .get(
            (profile.regions.object_heavy.classes + profile.invariants.class_component_size)
                as usize,
        )
        .ok_or_else(|| invalid_data("class-heavy region has no deep graph component"))?;
    let empty_class_heavy = &plans[(profile.regions.object_heavy.classes
        + profile.regions.class_heavy.classes * 3 / 4) as usize];
    let balanced = &plans
        [(profile.regions.object_heavy.classes + profile.regions.class_heavy.classes) as usize];
    let hot_first = hot
        .first_object_id
        .ok_or_else(|| invalid_data("hot class has no object anchor"))?;
    let secondary_first = secondary
        .first_object_id
        .ok_or_else(|| invalid_data("secondary object-heavy class has no anchor"))?;
    let deep_graph_first = deep_graph
        .first_object_id
        .ok_or_else(|| invalid_data("deep graph class has no object anchor"))?;
    Ok(BTreeMap::from([
        ("root_collection_id".to_string(), 1),
        ("nested_collection_id".to_string(), 3),
        (
            "object_heavy_collection_id".to_string(),
            hot.collection_id as i64,
        ),
        (
            "class_heavy_collection_id".to_string(),
            class_heavy.collection_id as i64,
        ),
        (
            "balanced_collection_id".to_string(),
            balanced.collection_id as i64,
        ),
        ("hot_class_id".to_string(), hot.id as i64),
        ("secondary_hot_class_id".to_string(), secondary.id as i64),
        ("sparse_class_id".to_string(), class_heavy.id as i64),
        ("empty_class_id".to_string(), empty_class_heavy.id as i64),
        ("medium_class_id".to_string(), balanced.id as i64),
        ("computed_class_id".to_string(), (balanced.id + 9) as i64),
        ("adversarial_class_id".to_string(), class_heavy.id as i64),
        ("deep_graph_class_id".to_string(), deep_graph.id as i64),
        ("deep_graph_object_id".to_string(), deep_graph_first as i64),
        ("history_class_id".to_string(), hot.id as i64),
        ("hub_object_id".to_string(), hot_first as i64),
        ("ordinary_object_id".to_string(), (hot_first + 1) as i64),
        ("history_object_id".to_string(), (hot_first + 2) as i64),
        (
            "heavy_history_object_id".to_string(),
            (hot_first + 3) as i64,
        ),
        ("secondary_object_id".to_string(), secondary_first as i64),
        ("concentrated_class_relation_id".to_string(), 1),
        (
            "spread_class_relation_id".to_string(),
            (profile.regions.object_heavy.class_relations + 1) as i64,
        ),
        (
            "balanced_spread_class_relation_id".to_string(),
            (profile.regions.object_heavy.class_relations
                + profile.regions.class_heavy.class_relations
                + 1) as i64,
        ),
        ("admin_principal_id".to_string(), 1),
        ("admin_group_id".to_string(), 1),
        ("terminal_task_id".to_string(), 1),
        ("event_delivery_id".to_string(), 1),
    ]))
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct Distribution {
    pub minimum: u64,
    pub median: u64,
    pub p95: u64,
    pub p99: u64,
    pub maximum: u64,
}

impl Distribution {
    pub fn from_values(values: &[u64]) -> Self {
        if values.is_empty() {
            return Self {
                minimum: 0,
                median: 0,
                p95: 0,
                p99: 0,
                maximum: 0,
            };
        }
        let mut values = values.to_vec();
        values.sort_unstable();
        Self {
            minimum: values[0],
            median: percentile(&values, 50),
            p95: percentile(&values, 95),
            p99: percentile(&values, 99),
            maximum: *values.last().expect("non-empty distribution"),
        }
    }
}

fn percentile(values: &[u64], percent: usize) -> u64 {
    let index = ((values.len() - 1) * percent).div_ceil(100);
    values[index]
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct GraphShapeSummary {
    pub class_components: u64,
    pub largest_class_component: u64,
    pub object_components: u64,
    pub largest_object_component: u64,
    pub representative_class_depth: u64,
    pub maximum_class_depth: u64,
    pub representative_object_depth: u64,
    pub maximum_object_depth: u64,
    pub adversarial_component_classes: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct PrincipalVisibility {
    pub visible_percent: f64,
    pub distribution: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct RelationShapeSummary {
    pub class_relation_id: u64,
    pub source_objects: u64,
    pub target_objects: u64,
    pub edge_count: u64,
    pub pair_capacity: u64,
    pub saturation_basis_points: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct DatasetManifest {
    pub schema_version: u32,
    pub profile_version: u32,
    pub profile: ProfileName,
    pub seed: u64,
    pub semantic_digest: String,
    pub totals: ResourceTotals,
    pub overlays: OverlaySpec,
    pub regions: BTreeMap<String, RegionSpec>,
    pub objects_per_class: Distribution,
    pub classes_per_collection: Distribution,
    pub object_relation_degree: Distribution,
    pub object_relations_per_class_relation: Distribution,
    pub relation_shapes: BTreeMap<String, RelationShapeSummary>,
    pub graph_shapes: GraphShapeSummary,
    pub history_revisions: Distribution,
    pub json_payload_bytes: Distribution,
    pub principals: BTreeMap<String, PrincipalVisibility>,
    pub anchors: BTreeMap<String, i64>,
    pub provisioning: ProvisioningLimits,
}

impl DatasetManifest {
    pub fn validate(&self, profile: &ScaleProfile) -> Result<()> {
        if self.schema_version != DATASET_SCHEMA_VERSION
            || self.profile_version != profile.profile_version
            || self.profile != profile.name
            || self.seed != profile.seed
            || self.totals != profile.totals
        {
            return Err(invalid_data(
                "manifest identity or totals do not match its profile",
            ));
        }
        if self.objects_per_class.maximum < profile.invariants.minimum_hot_class_objects {
            return Err(invalid_data("manifest has no required hot class"));
        }
        if self.objects_per_class.median > profile.invariants.maximum_median_objects_per_class {
            return Err(invalid_data(
                "manifest flattened the class-heavy object skew",
            ));
        }
        if self.object_relation_degree.maximum < profile.invariants.minimum_hub_object_degree {
            return Err(invalid_data(
                "manifest has no required high-degree hub object",
            ));
        }
        if self.object_relations_per_class_relation.maximum
            < profile.invariants.minimum_concentrated_relation_count
        {
            return Err(invalid_data(
                "manifest has no required concentrated class relation",
            ));
        }
        for name in [
            "concentrated_class_relation",
            "spread_class_relation",
            "balanced_spread_class_relation",
        ] {
            let shape = self
                .relation_shapes
                .get(name)
                .ok_or_else(|| invalid_data(format!("manifest has no '{name}' relation shape")))?;
            if shape.class_relation_id == 0
                || shape.pair_capacity == 0
                || shape.edge_count > shape.pair_capacity
                || shape.saturation_basis_points > 10_000
            {
                return Err(invalid_data(format!(
                    "manifest relation shape '{name}' is invalid"
                )));
            }
        }
        if self.history_revisions.maximum < profile.invariants.minimum_heavy_history_revisions {
            return Err(invalid_data("manifest has no history-heavy resource"));
        }
        let sparse = self
            .principals
            .get("sparse")
            .ok_or_else(|| invalid_data("manifest has no sparse benchmark principal"))?;
        if sparse.visible_percent > profile.invariants.maximum_sparse_visibility_percent {
            return Err(invalid_data(
                "sparse principal visibility exceeds the profile limit",
            ));
        }
        for region in [
            "object_heavy",
            "class_heavy",
            "balanced",
            "history_heavy",
            "authorization_adversarial",
        ] {
            if !self.regions.contains_key(region) {
                return Err(invalid_data(format!(
                    "manifest is missing region '{region}'"
                )));
            }
        }
        let expected_digest = semantic_digest(self)?;
        if self.semantic_digest != expected_digest {
            return Err(invalid_data(
                "manifest semantic digest does not match its contents",
            ));
        }
        Ok(())
    }

    pub fn equivalent_to(&self, other: &Self) -> Result<()> {
        if self.profile_version != other.profile_version
            || self.profile != other.profile
            || self.seed != other.seed
            || self.semantic_digest != other.semantic_digest
            || self.totals != other.totals
            || self.regions != other.regions
            || self.anchors != other.anchors
        {
            return Err(invalid_data(
                "base and head dataset manifests are not semantically equivalent",
            ));
        }
        Ok(())
    }

    pub fn write(&self, path: &Path) -> Result<()> {
        write_json(path, self)
    }

    pub fn read(path: &Path) -> Result<Self> {
        Ok(serde_json::from_slice(&fs::read(path)?)?)
    }
}

fn semantic_digest(manifest: &DatasetManifest) -> Result<String> {
    let mut canonical = manifest.clone();
    canonical.semantic_digest.clear();
    let encoded = serde_json::to_vec(&canonical)?;
    Ok(encode_digest(Sha256::digest(encoded).as_slice()))
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct EffectiveWorkloadLimits {
    pub default_page_limit: usize,
    pub maximum_page_limit: usize,
    pub maximum_graph_depth: usize,
    pub maximum_related_objects_per_include: usize,
    pub maximum_export_output_bytes: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct LatencyDistribution {
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub maximum_ms: f64,
}

impl LatencyDistribution {
    pub fn from_samples(samples: &[f64]) -> Self {
        if samples.is_empty() {
            return Self {
                p50_ms: 0.0,
                p95_ms: 0.0,
                p99_ms: 0.0,
                maximum_ms: 0.0,
            };
        }
        let mut values = samples.to_vec();
        values.sort_by(f64::total_cmp);
        Self {
            p50_ms: float_percentile(&values, 50),
            p95_ms: float_percentile(&values, 95),
            p99_ms: float_percentile(&values, 99),
            maximum_ms: *values.last().expect("non-empty latency samples"),
        }
    }
}

fn float_percentile(values: &[f64], percent: usize) -> f64 {
    values[((values.len() - 1) * percent).div_ceil(100)]
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ScenarioReport {
    pub name: String,
    pub phase: String,
    pub principal: String,
    pub concurrency: usize,
    pub requests: u64,
    pub successful_requests: u64,
    pub failures: u64,
    pub timeouts: u64,
    pub status_counts: BTreeMap<u16, u64>,
    pub requests_per_second: f64,
    pub latency: LatencyDistribution,
    pub response_bytes: u64,
    pub response_items: u64,
    pub graph_nodes: Option<u64>,
    pub graph_edges: Option<u64>,
    pub pages: u64,
    pub traversal_ms: Option<f64>,
    pub traversal_first_page_ms: Option<f64>,
    pub traversal_middle_page_ms: Option<f64>,
    pub traversal_final_page_ms: Option<f64>,
    pub duplicate_rows: u64,
    pub missing_rows: u64,
    pub unauthorized_rows: u64,
    pub authorization_candidates: Option<u64>,
    pub authorized_rows: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CorrectnessReport {
    pub request_failures: u64,
    pub traversal_duplicates: u64,
    pub traversal_missing: u64,
    pub unauthorized_rows: u64,
    pub manifest_mismatches: u64,
    pub lifecycle_failures: u64,
}

impl CorrectnessReport {
    pub const fn passed(&self) -> bool {
        self.request_failures == 0
            && self.traversal_duplicates == 0
            && self.traversal_missing == 0
            && self.unauthorized_rows == 0
            && self.manifest_mismatches == 0
            && self.lifecycle_failures == 0
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct BackendIdentity {
    pub name: String,
    pub version: String,
    pub settings: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct BackendResourceReport {
    pub cpu_seconds: Option<f64>,
    pub peak_resident_bytes: Option<u64>,
    pub storage_bytes: u64,
    pub data_bytes: Option<u64>,
    pub index_bytes: Option<u64>,
    pub write_ahead_bytes: Option<u64>,
    pub metrics: BTreeMap<String, f64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackendPreparation {
    pub identity: BackendIdentity,
    pub database_fresh: bool,
    pub sparse_collection_ids: BTreeSet<i64>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct BenchmarkPrincipal {
    role: String,
    username: String,
    password: String,
}

impl BenchmarkPrincipal {
    pub fn new(
        role: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Result<Self> {
        let principal = Self {
            role: role.into(),
            username: username.into(),
            password: password.into(),
        };
        if principal.role.trim().is_empty()
            || principal.username.trim().is_empty()
            || principal.password.is_empty()
        {
            return Err(invalid_data(
                "benchmark principal role, username, and password must be non-empty",
            ));
        }
        Ok(principal)
    }

    pub fn role(&self) -> &str {
        &self.role
    }

    pub fn username(&self) -> &str {
        &self.username
    }

    pub fn password(&self) -> &str {
        &self.password
    }
}

/// Adapter-owned scale operations used by the shared benchmark frontend.
///
/// Logical workload execution stays in the frontend. Each storage adapter owns
/// fixture loading, physical-resource probes, and any backend-specific setup.
#[async_trait]
pub trait ScaleBenchmarkBackend: Send + Sync {
    type ResourceBaseline: Send;

    fn name(&self) -> &'static str;

    /// Environment required to select this backend in the production server.
    /// Values may contain credentials and must never be copied into reports.
    fn server_environment(&self) -> BTreeMap<String, String>;

    /// Stable workload principals loaded by this adapter. Credentials are
    /// process-local inputs and must never be copied into reports.
    fn benchmark_principals(&self) -> Vec<BenchmarkPrincipal>;

    async fn load_dataset(&self, profile: &ScaleProfile) -> Result<LoadReport>;

    async fn verify_dataset(
        &self,
        profile: &ScaleProfile,
        manifest: &DatasetManifest,
    ) -> Result<()>;

    /// Count a lower bound on serialized temporal state/history bytes without
    /// retaining the corpus. May stop as soon as the supplied ceiling is exceeded.
    /// This is a size preflight, not a substitute for backup validation.
    async fn backup_size_lower_bound(&self, ceiling_bytes: u64) -> Result<u64>;

    async fn prepare_measurement(&self) -> Result<BackendPreparation>;

    async fn mark_computed_ready(&self) -> Result<()>;

    async fn begin_resource_measurement(&self) -> Result<Self::ResourceBaseline>;

    async fn finish_resource_measurement(
        &self,
        baseline: Self::ResourceBaseline,
    ) -> Result<BackendResourceReport>;
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct RuntimeIdentity {
    pub runner: String,
    pub backend: BackendIdentity,
    pub process_fresh: bool,
    pub database_fresh: bool,
    pub deliberate_warmup_requests: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ResourceReport {
    pub application_cpu_seconds: f64,
    pub peak_application_resident_bytes: u64,
    pub backend: BackendResourceReport,
    pub storage_metric_deltas: BTreeMap<String, f64>,
    pub pool_metric_deltas: BTreeMap<String, f64>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct LifecycleStageResources {
    pub elapsed_ms: u64,
    pub peak_resident_bytes: u64,
    pub termination: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct LifecycleReport {
    #[serde(default)]
    pub stages: BTreeMap<String, LifecycleStageResources>,
    pub dataset_generation_ms: u64,
    pub dataset_loading_ms: u64,
    #[serde(default)]
    pub backup_preflight_ms: Option<u64>,
    #[serde(default)]
    pub backup_size_lower_bound_bytes: Option<u64>,
    pub backup_generation_ms: Option<u64>,
    pub backup_artifact_bytes: Option<u64>,
    pub backup_logical_rows: Option<u64>,
    pub backup_section_counts: BTreeMap<String, u64>,
    pub offline_verification_ms: Option<u64>,
    pub restore_ms: Option<u64>,
    pub semantic_verification_ms: Option<u64>,
    pub computed_rebuild_ms: Option<u64>,
    pub outcome: String,
    pub supported_ceiling_bytes: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ScaleBenchmarkReport {
    pub schema_version: u32,
    pub label: String,
    pub manifest: DatasetManifest,
    pub workload_version: u32,
    pub workload_seed: u64,
    pub workload_digest: String,
    pub limit_mode: LimitMode,
    pub scale_axis: Option<ScaleAxis>,
    pub effective_limits: EffectiveWorkloadLimits,
    pub runtime: RuntimeIdentity,
    pub scenarios: Vec<ScenarioReport>,
    pub correctness: CorrectnessReport,
    pub resources: ResourceReport,
    pub lifecycle: LifecycleReport,
}

impl ScaleBenchmarkReport {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        label: impl Into<String>,
        manifest: DatasetManifest,
        workload: &WorkloadSpec,
        limit_mode: LimitMode,
        scale_axis: Option<ScaleAxis>,
        runtime: RuntimeIdentity,
        scenarios: Vec<ScenarioReport>,
        correctness: CorrectnessReport,
        resources: ResourceReport,
        lifecycle: LifecycleReport,
    ) -> Result<Self> {
        Ok(Self {
            schema_version: REPORT_SCHEMA_VERSION,
            label: label.into(),
            manifest,
            workload_version: workload.workload_version,
            workload_seed: workload.seed,
            workload_digest: workload.digest()?,
            limit_mode,
            scale_axis,
            effective_limits: limit_mode.settings(),
            runtime,
            scenarios,
            correctness,
            resources,
            lifecycle,
        })
    }

    pub fn write(&self, path: &Path) -> Result<()> {
        write_json(path, self)
    }

    pub fn read(path: &Path) -> Result<Self> {
        let report = serde_json::from_slice::<Self>(&fs::read(path)?)?;
        if report.schema_version != REPORT_SCHEMA_VERSION {
            return Err(invalid_data(format!(
                "unsupported scale report schema version {}",
                report.schema_version
            )));
        }
        Ok(report)
    }
}

pub fn validate_traversal(
    item_ids: &[i64],
    expected_total: Option<u64>,
    allowed_ids: Option<&BTreeSet<i64>>,
) -> CorrectnessReport {
    let unique = item_ids.iter().copied().collect::<BTreeSet<_>>();
    let duplicates = item_ids.len().saturating_sub(unique.len()) as u64;
    let missing = expected_total
        .map(|expected| expected.saturating_sub(unique.len() as u64))
        .unwrap_or_default();
    let unauthorized = allowed_ids
        .map(|allowed| unique.difference(allowed).count() as u64)
        .unwrap_or_default();
    CorrectnessReport {
        request_failures: 0,
        traversal_duplicates: duplicates,
        traversal_missing: missing,
        unauthorized_rows: unauthorized,
        manifest_mismatches: 0,
        lifecycle_failures: 0,
    }
}

fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let temporary = path.with_extension("tmp");
    fs::write(&temporary, serde_json::to_vec_pretty(value)?)?;
    fs::rename(temporary, path)?;
    Ok(())
}

pub fn invalid_data(message: impl Into<String>) -> Error {
    Box::new(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        message.into(),
    ))
}

fn encode_digest(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

#[cfg(test)]
mod tests;
