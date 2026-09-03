//! Deterministic data and report model for the stateful scale benchmark.
//!
//! The benchmark is intentionally config-free at this layer. Profile and
//! workload documents are immutable inputs, and reports contain only aggregate
//! measurements and stable fixture identifiers.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
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
    pub backup_max_output_bytes: u64,
    pub restore_max_upload_bytes: u64,
    pub db_statement_timeout_ms: u64,
    pub db_pool_size: u64,
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
        if self.provisioning.backup_max_output_bytes == 0
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

    fn axes_for(&self, limit_mode: LimitMode) -> impl Iterator<Item = &SensitivityAxisSpec> {
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

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct LifecycleReport {
    pub dataset_generation_ms: u64,
    pub dataset_loading_ms: u64,
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

fn format_saturation(basis_points: u64) -> String {
    if basis_points < 100 {
        format!("{:.2}%", basis_points as f64 / 100.0)
    } else {
        format!("{:.1}%", basis_points as f64 / 100.0)
    }
}

fn format_count(value: u64) -> String {
    let digits = value.to_string();
    let mut output = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, character) in digits.chars().enumerate() {
        if index != 0 && (digits.len() - index).is_multiple_of(3) {
            output.push(',');
        }
        output.push(character);
    }
    output
}

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

fn format_percent(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:+.1}%"))
        .unwrap_or_else(|| "-".to_string())
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

fn short_report_label(label: &str) -> String {
    label.chars().take(12).collect()
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

fn format_bytes(value: f64) -> String {
    format!("{:.1} MiB", value / 1_048_576.0)
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
mod tests {
    use super::*;

    #[test]
    fn bundled_profiles_are_deterministic_and_preserve_scale() {
        let large = ScaleProfile::bundled(ProfileName::Large).unwrap();
        let huge = ScaleProfile::bundled(ProfileName::Huge).unwrap();
        let first = large.manifest().unwrap();
        let second = large.manifest().unwrap();

        assert_eq!(first, second);
        assert_eq!(first.schema_version, 3);
        assert_eq!(first.semantic_digest.len(), 64);
        assert_eq!(huge.totals.objects, large.totals.objects * 4);
        assert_eq!(
            huge.totals.object_relations,
            large.totals.object_relations * 4
        );
        assert_eq!(
            huge.overlays.event_deliveries,
            large.overlays.event_deliveries * 4
        );
        assert!(
            huge.provisioning.restore_max_upload_bytes
                > large.provisioning.restore_max_upload_bytes
        );
        assert!(huge.manifest().unwrap().semantic_digest != first.semantic_digest);
    }

    #[test]
    fn scale_increments_are_arbitrary_and_change_one_declared_region() {
        let baseline = ScaleProfile::bundled(ProfileName::Large).unwrap();
        let classes = baseline
            .clone()
            .with_increment(ScaleAxis::Classes, 400)
            .unwrap();
        let objects = baseline
            .clone()
            .with_increment(ScaleAxis::Objects, 2_500)
            .unwrap();
        let object_heavy = baseline
            .clone()
            .with_increment(ScaleAxis::ObjectHeavyObjects, 2_500)
            .unwrap();
        let relations = baseline
            .clone()
            .with_increment(ScaleAxis::ObjectRelations, 125_000)
            .unwrap();
        let concentrated = baseline
            .clone()
            .with_increment(ScaleAxis::ConcentratedObjectRelations, 125_000)
            .unwrap();
        let dense = baseline
            .clone()
            .with_increment(ScaleAxis::DenseObjectRelations, 100_000)
            .unwrap();

        assert_eq!(classes.totals.classes, baseline.totals.classes + 400);
        assert_eq!(
            classes.regions.balanced.classes,
            baseline.regions.balanced.classes + 400
        );
        assert_eq!(classes.totals.objects, baseline.totals.objects);
        assert_eq!(
            classes.totals.object_relations,
            baseline.totals.object_relations
        );
        assert_eq!(objects.totals.objects, baseline.totals.objects + 2_500);
        assert_eq!(
            objects.regions.balanced.objects,
            baseline.regions.balanced.objects + 2_500
        );
        assert_eq!(objects.regions.class_heavy, baseline.regions.class_heavy);
        assert_eq!(
            object_heavy.regions.object_heavy.objects,
            baseline.regions.object_heavy.objects + 2_500
        );
        assert_eq!(object_heavy.regions.balanced, baseline.regions.balanced);
        assert_eq!(
            relations.totals.object_relations,
            baseline.totals.object_relations + 125_000
        );
        assert_eq!(
            relations.regions.balanced.object_relations,
            baseline.regions.balanced.object_relations + 125_000
        );
        assert_eq!(relations.regions.class_heavy, baseline.regions.class_heavy);
        assert_eq!(
            concentrated.regions.object_heavy.object_relations,
            baseline.regions.object_heavy.object_relations + 125_000
        );
        assert_eq!(concentrated.regions.balanced, baseline.regions.balanced);
        assert_eq!(
            dense.regions.class_heavy.object_relations,
            baseline.regions.class_heavy.object_relations + 100_000
        );
        assert_eq!(dense.regions.balanced, baseline.regions.balanced);
        classes.manifest().unwrap();
        object_heavy.manifest().unwrap();
        relations.manifest().unwrap();
        concentrated.manifest().unwrap();
        dense.manifest().unwrap();
    }

    #[test]
    fn calibrated_sensitivity_plan_uses_signal_bearing_independent_steps() {
        let profile = ScaleProfile::bundled(ProfileName::Large).unwrap();
        let spec = SensitivitySpec::bundled().unwrap();
        let plan = spec.plan(&profile, LimitMode::Standard).unwrap();
        let extended = spec.plan(&profile, LimitMode::Extended).unwrap();

        assert_eq!(plan.points.len(), 18);
        assert_eq!(plan.limit_mode, LimitMode::Standard);
        assert_eq!(
            plan.points[0],
            SensitivityPlanPoint {
                axis: ScaleAxis::Classes,
                added_percent: 20,
                added_count: 800,
                comparison_total: 4_800,
            }
        );
        assert_eq!(plan.points[5].comparison_total, 500_000);
        assert_eq!(plan.points[6].axis, ScaleAxis::ObjectHeavyObjects);
        assert_eq!(plan.points[9].added_count, 200_000);
        assert_eq!(plan.points[14].comparison_total, 2_000_000);
        assert_eq!(plan.points[15].axis, ScaleAxis::DenseObjectRelations);
        assert_eq!(plan.points[15].added_count, 100_000);
        assert_eq!(plan.points[17].added_count, 550_000);
        assert_eq!(extended.limit_mode, LimitMode::Extended);
        assert_eq!(extended.points.len(), 6);
        assert!(
            extended
                .points
                .iter()
                .all(|point| matches!(point.axis, ScaleAxis::Objects | ScaleAxis::ObjectRelations))
        );
    }

    #[test]
    fn sensitivity_spec_rejects_non_increasing_steps() {
        let mut spec = SensitivitySpec::bundled().unwrap();
        spec.axes[0].percent_steps = vec![20, 20];

        let error = spec.validate().unwrap_err();

        assert!(error.to_string().contains("increasing non-zero"));
    }

    #[test]
    fn relation_manifest_models_eligible_pairs_and_rejects_saturation() {
        let baseline = ScaleProfile::bundled(ProfileName::Large).unwrap();
        let classes = baseline.class_plan();
        let relations = class_relation_plan(&baseline).unwrap();
        let counts = object_relation_counts(&baseline, &classes, &relations).unwrap();
        let manifest = baseline.manifest().unwrap();

        assert_eq!(counts.iter().sum::<u64>(), baseline.totals.object_relations);
        assert_eq!(
            Distribution::from_values(&counts),
            manifest.object_relations_per_class_relation
        );
        let spread = &manifest.relation_shapes["spread_class_relation"];
        assert_eq!(spread.edge_count, 45);
        assert_eq!(spread.pair_capacity, 196);
        assert_eq!(spread.saturation_basis_points, 2_295);

        let mut saturated = baseline;
        saturated.totals.object_relations += 1_000_000;
        saturated.regions.class_heavy.object_relations += 1_000_000;
        let error = saturated.manifest().unwrap_err();
        assert!(error.to_string().contains("class pair capacity"));
    }

    #[test]
    fn limit_modes_preserve_production_defaults_and_declare_elevated_values() {
        let standard = LimitMode::Standard.settings();
        let extended = LimitMode::Extended.settings();

        assert_eq!(standard.default_page_limit, 100);
        assert_eq!(standard.maximum_page_limit, 250);
        assert_eq!(standard.maximum_graph_depth, 100);
        assert_eq!(standard.maximum_export_output_bytes, 262_144);
        assert_eq!(LimitMode::Standard.candidate_page_limit(), 250);
        assert_eq!(
            LimitMode::Extended.candidate_page_limit(),
            MAX_STORAGE_CANDIDATE_PAGE_SIZE
        );
        assert!(extended.maximum_page_limit > standard.maximum_page_limit);
        assert!(extended.maximum_graph_depth > standard.maximum_graph_depth);
        assert!(extended.maximum_export_output_bytes > standard.maximum_export_output_bytes);
    }

    #[test]
    fn manifest_contains_every_named_skew_and_anchor() {
        let profile = ScaleProfile::bundled(ProfileName::Large).unwrap();
        let manifest = profile.manifest().unwrap();

        assert_eq!(manifest.objects_per_class.maximum, 60_000);
        assert!(manifest.objects_per_class.median <= 20);
        assert!(manifest.classes_per_collection.maximum >= 750);
        assert!(manifest.object_relation_degree.maximum >= 10_000);
        assert!(manifest.object_relations_per_class_relation.maximum >= 600_000);
        assert_eq!(manifest.history_revisions.maximum, 48);
        assert!(manifest.principals["sparse"].visible_percent <= 1.0);
        for anchor in [
            "hot_class_id",
            "sparse_class_id",
            "empty_class_id",
            "medium_class_id",
            "hub_object_id",
            "heavy_history_object_id",
            "adversarial_class_id",
            "deep_graph_class_id",
            "deep_graph_object_id",
            "balanced_spread_class_relation_id",
        ] {
            assert!(manifest.anchors.contains_key(anchor));
        }
    }

    #[test]
    fn acceptance_rejects_flattened_shape_even_when_totals_match() {
        let profile = ScaleProfile::bundled(ProfileName::Large).unwrap();
        let mut manifest = profile.manifest().unwrap();
        manifest.objects_per_class = Distribution {
            minimum: 62,
            median: 62,
            p95: 63,
            p99: 63,
            maximum: 63,
        };
        manifest.semantic_digest = semantic_digest(&manifest).unwrap();

        let error = manifest.validate(&profile).unwrap_err();
        assert!(error.to_string().contains("hot class"));
    }

    #[test]
    fn workload_paths_resolve_only_declared_anchors() {
        let profile = ScaleProfile::bundled(ProfileName::Large).unwrap();
        let manifest = profile.manifest().unwrap();
        let workload = WorkloadSpec::bundled().unwrap();

        for limit_mode in [LimitMode::Standard, LimitMode::Extended] {
            for scenario in &workload.scenarios {
                let path = workload
                    .render_path(scenario, &manifest, limit_mode)
                    .unwrap();
                assert!(path.starts_with("/api/"));
                assert!(!path.contains(['{', '}']));
                if scenario.name == "unified-search" {
                    assert!(path.ends_with(&format!(
                        "limit_per_kind={}",
                        limit_mode.candidate_page_limit()
                    )));
                }
            }
        }
        assert!(
            workload
                .scenarios
                .iter()
                .find(|scenario| scenario.name == "relations-balanced-spread-class-relation")
                .unwrap()
                .traverse
        );
        for depth in [1, 2, 4] {
            let scenario = workload
                .scenarios
                .iter()
                .find(|scenario| scenario.name == format!("object-graph-depth-{depth}"))
                .unwrap();
            assert!(!scenario.include_in_mixed);
            assert_eq!(scenario.warmup_requests, Some(1));
            assert_eq!(scenario.single_client_samples, Some(3));
            assert!(scenario.path.ends_with(&format!("depth__lte={depth}")));
            assert!(scenario.applies_to(None));
            assert!(scenario.applies_to(Some(ScaleAxis::ObjectRelations)));
            assert_eq!(
                scenario.applies_to(Some(ScaleAxis::DenseObjectRelations)),
                depth <= 2
            );
            assert!(!scenario.applies_to(Some(ScaleAxis::Objects)));
        }
        assert!(
            workload
                .scenarios
                .iter()
                .all(|scenario| scenario.name != "object-graph-depth-7")
        );
    }

    #[test]
    fn traversal_validation_detects_duplicates_missing_and_unauthorized_rows() {
        let allowed = BTreeSet::from([1, 2, 3]);
        let result = validate_traversal(&[1, 2, 2, 4], Some(4), Some(&allowed));

        assert_eq!(result.traversal_duplicates, 1);
        assert_eq!(result.traversal_missing, 1);
        assert_eq!(result.unauthorized_rows, 1);
        assert!(!result.passed());
    }

    fn report(label: &str) -> ScaleBenchmarkReport {
        let profile = ScaleProfile::bundled(ProfileName::Large).unwrap();
        let workload = WorkloadSpec::bundled().unwrap();
        ScaleBenchmarkReport::new(
            label,
            profile.manifest().unwrap(),
            &workload,
            LimitMode::Standard,
            None,
            RuntimeIdentity {
                runner: "test".to_string(),
                backend: BackendIdentity {
                    name: "postgres".to_string(),
                    version: "test".to_string(),
                    settings: BTreeMap::new(),
                },
                process_fresh: true,
                database_fresh: true,
                deliberate_warmup_requests: 3,
            },
            vec![ScenarioReport {
                name: "point".to_string(),
                phase: "warm".to_string(),
                principal: "admin".to_string(),
                concurrency: 1,
                requests: 1,
                successful_requests: 1,
                failures: 0,
                timeouts: 0,
                status_counts: BTreeMap::from([(200, 1)]),
                requests_per_second: 10.0,
                latency: LatencyDistribution::from_samples(&[5.0]),
                response_bytes: 100,
                response_items: 1,
                graph_nodes: None,
                graph_edges: None,
                pages: 1,
                traversal_ms: None,
                traversal_first_page_ms: None,
                traversal_middle_page_ms: None,
                traversal_final_page_ms: None,
                duplicate_rows: 0,
                missing_rows: 0,
                unauthorized_rows: 0,
                authorization_candidates: None,
                authorized_rows: None,
            }],
            CorrectnessReport {
                request_failures: 0,
                traversal_duplicates: 0,
                traversal_missing: 0,
                unauthorized_rows: 0,
                manifest_mismatches: 0,
                lifecycle_failures: 0,
            },
            ResourceReport {
                application_cpu_seconds: 1.0,
                peak_application_resident_bytes: 10,
                backend: BackendResourceReport {
                    cpu_seconds: None,
                    peak_resident_bytes: None,
                    storage_bytes: 20,
                    data_bytes: Some(15),
                    index_bytes: Some(5),
                    write_ahead_bytes: None,
                    metrics: BTreeMap::new(),
                },
                storage_metric_deltas: BTreeMap::new(),
                pool_metric_deltas: BTreeMap::new(),
            },
            LifecycleReport {
                dataset_generation_ms: 1,
                dataset_loading_ms: 2,
                backup_generation_ms: None,
                backup_artifact_bytes: None,
                backup_logical_rows: None,
                backup_section_counts: BTreeMap::new(),
                offline_verification_ms: None,
                restore_ms: None,
                semantic_verification_ms: None,
                computed_rebuild_ms: None,
                outcome: "not_run".to_string(),
                supported_ceiling_bytes: 268_435_456,
            },
        )
        .unwrap()
    }

    fn report_for_sensitivity(label: &str, profile: &ScaleProfile) -> ScaleBenchmarkReport {
        let mut report = report(label);
        report.manifest = profile.manifest().unwrap();
        let prototype = &report.scenarios[0];
        let mut scenarios = Vec::new();
        for name in [
            "classes-fragmented-global",
            "unified-search",
            "objects-hot-class",
            "relations-balanced-spread-class-relation",
            "relations-concentrated-class-relation",
            "relations-spread-class-relation",
        ] {
            let mut scenario = prototype.clone();
            scenario.name = name.to_string();
            scenario.phase = "warm_single_client".to_string();
            scenarios.push(scenario);
        }
        for name in [
            "objects-hot-class",
            "relations-balanced-spread-class-relation",
        ] {
            let mut traversal = prototype.clone();
            traversal.name = name.to_string();
            traversal.phase = "complete_cursor_traversal".to_string();
            traversal.traversal_ms = Some(10.0);
            scenarios.push(traversal);
        }
        for depth in [1_u64, 2, 4] {
            let mut graph = prototype.clone();
            graph.name = format!("object-graph-depth-{depth}");
            graph.phase = "warm_single_client".to_string();
            graph.graph_nodes = Some(depth * 3 + 1);
            graph.graph_edges = Some(depth * 3);
            scenarios.push(graph);
        }
        report.scenarios = scenarios;
        report
    }

    #[test]
    fn assessment_keeps_performance_informational() {
        let base = report("base");
        let mut head = report("head");
        head.scenarios[0].latency = LatencyDistribution::from_samples(&[5_000.0]);

        let assessment = ScaleAssessment::assess(&head, Some(&base));

        assessment.ensure_passed().unwrap();
        assert!(assessment.markdown().contains("Base `base` → PR `head`"));
        assert!(assessment.markdown().contains("Median p95 change"));
        assert!(assessment.markdown().contains("+99900.0%"));
        assert!(assessment.markdown().contains("<details>"));
        assert!(assessment.markdown().contains("Resource and lifecycle"));
        assert!(
            assessment
                .markdown()
                .contains("single paired run is evidence")
        );
    }

    #[test]
    fn assessment_fails_on_correctness_or_corpus_drift() {
        let base = report("base");
        let mut head = report("head");
        head.correctness.traversal_duplicates = 1;
        head.manifest.seed += 1;

        let error = ScaleAssessment::assess(&head, Some(&base))
            .ensure_passed()
            .unwrap_err();
        assert!(error.to_string().contains("correctness"));
        assert!(error.to_string().contains("not semantically equivalent"));
    }

    #[test]
    fn impact_normalizes_to_the_measured_step_when_no_unit_is_requested() {
        let baseline = report("baseline");
        let mut comparison = report("comparison");
        comparison.manifest = ScaleProfile::bundled(ProfileName::Large)
            .unwrap()
            .with_increment(ScaleAxis::Objects, 2_500)
            .unwrap()
            .manifest()
            .unwrap();
        comparison.scenarios[0].latency = LatencyDistribution::from_samples(&[7.0]);
        comparison.scenarios[0].requests_per_second = 8.0;
        comparison.resources.backend.storage_bytes = 30;
        comparison.scale_axis = Some(ScaleAxis::Objects);

        let impact =
            ScaleImpactReport::compare(&baseline, &comparison, ScaleAxis::Objects, None).unwrap();
        let latency = &impact.scenarios[0].metrics["latency_p95_ms"];

        assert_eq!(impact.axis_delta, 2_500);
        assert_eq!(impact.normalization_unit, 2_500);
        assert_eq!(latency.absolute, 2.0);
        assert_eq!(latency.percent, Some(40.0));
        assert_eq!(latency.per_normalization_unit, 2.0);
        assert_eq!(impact.scenarios[0].metrics["pages"].comparison, 1.0);
        assert!(impact.markdown().contains("Repeat paired trials"));
    }

    #[test]
    fn sensitivity_summary_validates_the_calibrated_matrix_and_states_exact_growth() {
        let profile = ScaleProfile::bundled(ProfileName::Large).unwrap();
        let baseline = report_for_sensitivity("fixed-binary-baseline", &profile);
        let spec = SensitivitySpec::bundled().unwrap();
        let plan = spec.plan(&profile, LimitMode::Standard).unwrap();
        let mut impacts = Vec::new();
        for point in plan.points {
            let comparison_profile = profile
                .clone()
                .with_increment(point.axis, point.added_count)
                .unwrap();
            let mut comparison = report_for_sensitivity(
                &format!("{}-{}", point.axis.as_str(), point.added_percent),
                &comparison_profile,
            );
            comparison.scale_axis = Some(point.axis);
            comparison.resources.backend.storage_bytes += point.added_count;
            comparison.resources.backend.index_bytes = Some(5 + point.added_count / 2);
            let axis_spec = spec
                .axes
                .iter()
                .find(|axis| axis.axis == point.axis)
                .unwrap();
            let primary = comparison
                .scenarios
                .iter_mut()
                .find(|scenario| {
                    scenario.name == axis_spec.scenario && scenario.phase == axis_spec.phase
                })
                .unwrap();
            primary.latency =
                LatencyDistribution::from_samples(&[5.0 + point.added_percent as f64 / 10.0]);
            if let Some(traversal_phase) = axis_spec.traversal_phase.as_deref() {
                let traversal = comparison
                    .scenarios
                    .iter_mut()
                    .find(|scenario| {
                        scenario.name == axis_spec.scenario && scenario.phase == traversal_phase
                    })
                    .unwrap();
                traversal.pages = match point.added_percent {
                    20 => 1,
                    50 => 2,
                    _ => 3,
                };
                traversal.traversal_ms = Some(10.0 + point.added_percent as f64 / 10.0);
            }
            let workload = WorkloadSpec::bundled().unwrap();
            comparison.scenarios.retain(|report| {
                workload
                    .scenarios
                    .iter()
                    .find(|scenario| scenario.name == report.name)
                    .is_none_or(|scenario| scenario.applies_to(Some(point.axis)))
            });
            impacts.push(
                ScaleImpactReport::compare(&baseline, &comparison, point.axis, None).unwrap(),
            );
        }

        let summary = ScaleSensitivityReport::summarize(&baseline, &impacts, &spec).unwrap();
        let markdown = summary.markdown();

        assert_eq!(summary.axes.len(), 6);
        assert_eq!(summary.axes[3].depth_probes.len(), 3);
        assert_eq!(summary.axes[5].depth_probes.len(), 2);
        assert_eq!(
            summary.axes[3].points[1]
                .traversal_pages
                .as_ref()
                .unwrap()
                .comparison,
            2.0
        );
        assert!(markdown.contains("Baseline corpus:"));
        assert!(markdown.contains("Signals at a glance"));
        assert_eq!(markdown.matches("<details>").count(), 6);
        assert_eq!(markdown.matches("</details>").count(), 6);
        assert!(markdown.contains("Measurement baseline:"));
        assert!(markdown.contains("| Growth | p95 ms | Requests/s |"));
        assert!(markdown.contains("## postgres / large / page limit 250 scale growth"));
        assert!(markdown.contains("Class-count growth"));
        assert!(markdown.contains("Object-heavy growth"));
        assert!(markdown.contains("Concentrated object-relation growth"));
        assert!(markdown.contains("Dense class-pair relation growth"));
        assert!(markdown.contains("Pair saturation"));
        assert!(markdown.contains("85.2%"));
        assert!(markdown.contains("250,000"));
        assert!(markdown.contains("+20% (+50,000)"));
        assert!(markdown.contains("300,000"));
        assert!(markdown.contains("+100% (+1,000,000)"));
        assert!(markdown.contains("pages `1 → 1 / 2 / 3`"));
        assert!(markdown.contains("depth 4 graph p95"));
        assert!(markdown.contains("Returned nodes/edges by maximum depth"));
        assert!(markdown.contains("reachable graph stays fixed"));
        assert!(markdown.contains("extra work caused by local fan-out"));
        assert!(markdown.contains("Each percentage is relative to the stated baseline"));

        let mut extended_summary = summary.clone();
        extended_summary.limit_mode = LimitMode::Extended;
        assert!(
            extended_summary
                .markdown()
                .contains("## postgres / large / page limit 1500 scale growth")
        );
    }

    #[test]
    fn impact_rejects_experiments_that_change_multiple_axes() {
        let baseline = report("baseline");
        let mut comparison = report("comparison");
        comparison.manifest = ScaleProfile::bundled(ProfileName::Large)
            .unwrap()
            .with_increment(ScaleAxis::Objects, 2_500)
            .unwrap()
            .with_increment(ScaleAxis::ObjectRelations, 125_000)
            .unwrap()
            .manifest()
            .unwrap();
        comparison.scale_axis = Some(ScaleAxis::Objects);

        let error = ScaleImpactReport::compare(&baseline, &comparison, ScaleAxis::Objects, None)
            .unwrap_err();

        assert!(error.to_string().contains("totals other than objects"));
    }

    #[test]
    fn backend_comparison_accepts_matching_logical_runs() {
        let postgres = report("postgres-run");
        let mut memory = report("memory-run");
        memory.runtime.backend.name = "memory".to_string();
        memory.runtime.backend.version = "1".to_string();
        memory.scenarios[0].latency = LatencyDistribution::from_samples(&[2.5]);

        let comparison = BackendComparisonReport::compare(&[postgres, memory]).unwrap();

        assert_eq!(comparison.backends[0].identity.name, "memory");
        assert_eq!(comparison.backends[1].identity.name, "postgres");
        assert!(comparison.markdown().contains("Same `large` corpus"));
        assert!(comparison.markdown().contains("| memory | 1 | point |"));
    }

    #[test]
    fn serialized_report_contains_no_credential_fields() {
        let encoded = serde_json::to_string(&report("head")).unwrap();

        for forbidden in ["password", "database_url", "bearer", "token"] {
            assert!(!encoded.to_ascii_lowercase().contains(forbidden));
        }
    }
}
