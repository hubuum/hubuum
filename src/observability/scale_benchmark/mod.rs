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

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const LARGE_PROFILE: &str = include_str!("../../../scale-benchmarks/profiles/large.toml");
const HUGE_PROFILE: &str = include_str!("../../../scale-benchmarks/profiles/huge.toml");
const WORKLOAD_V1: &str = include_str!("../../../scale-benchmarks/workloads/v1.toml");
const DATASET_SCHEMA_VERSION: u32 = 1;
const REPORT_SCHEMA_VERSION: u32 = 1;

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;

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

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
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

    pub fn validate(&self) -> Result<()> {
        if self.schema_version != DATASET_SCHEMA_VERSION {
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
        let objects_per_class = Distribution::from_values(
            &plans
                .iter()
                .map(|plan| plan.object_count)
                .collect::<Vec<_>>(),
        );
        let classes_per_collection = class_collection_distribution(self, &plans);
        let relations_per_class_relation = relation_distribution(self);
        let anchors = manifest_anchors(self, &plans)?;
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

fn relation_distribution(profile: &ScaleProfile) -> Distribution {
    let mut counts = Vec::with_capacity(profile.totals.class_relations as usize);
    for (region, concentrates_relations) in [
        (&profile.regions.object_heavy, true),
        (&profile.regions.class_heavy, false),
        (&profile.regions.balanced, false),
    ] {
        for offset in 0..region.class_relations {
            let count = if offset == 0 && region.object_relations > 0 {
                if concentrates_relations {
                    region.object_relations
                } else {
                    distributed_count(region.object_relations, region.class_relations, offset)
                }
            } else if concentrates_relations {
                0
            } else {
                distributed_count(region.object_relations, region.class_relations, offset)
            };
            counts.push(count);
        }
    }
    Distribution::from_values(&counts)
}

fn manifest_anchors(profile: &ScaleProfile, plans: &[ClassPlan]) -> Result<BTreeMap<String, i64>> {
    let hot = &plans[0];
    let secondary = &plans[1];
    let class_heavy = &plans[profile.regions.object_heavy.classes as usize];
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
    #[serde(default)]
    pub traverse: bool,
    #[serde(default)]
    pub verify_sparse_visibility: bool,
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
            if scenario.name.is_empty()
                || !names.insert(&scenario.name)
                || scenario.weight == 0
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
            "graph_depth".to_string(),
            limit_mode.graph_depth().to_string(),
        );
        render_template(&scenario.path, &values)
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
pub struct RuntimeIdentity {
    pub runner: String,
    pub postgres_version: String,
    pub process_fresh: bool,
    pub database_fresh: bool,
    pub deliberate_warmup_requests: usize,
    pub database_settings: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ResourceReport {
    pub application_cpu_seconds: f64,
    pub postgres_cpu_seconds: Option<f64>,
    pub peak_application_resident_bytes: u64,
    pub peak_postgres_resident_bytes: Option<u64>,
    pub database_bytes: u64,
    pub table_bytes: u64,
    pub index_bytes: u64,
    pub wal_bytes: Option<u64>,
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
            {
                failures.push(
                    "base and head workload specifications or limit modes differ".to_string(),
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
    let mut output = String::from("## Hubuum scale benchmark\n\n");
    output.push_str(&format!(
        "Profile `{}` / `{:?}` limits; dataset `{}`; performance is informational.\n\n",
        head.manifest.profile.as_str(),
        head.limit_mode,
        &head.manifest.semantic_digest[..12]
    ));
    output.push_str(
        "| Scenario | Phase | Head p95 ms | Base p95 ms | Change | Head rps | Failures |\n",
    );
    output.push_str("| --- | --- | ---: | ---: | ---: | ---: | ---: |\n");
    let base_scenarios = base
        .map(|report| {
            report
                .scenarios
                .iter()
                .map(|scenario| ((scenario.name.as_str(), scenario.phase.as_str()), scenario))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();
    for scenario in &head.scenarios {
        let base_scenario = base_scenarios
            .get(&(scenario.name.as_str(), scenario.phase.as_str()))
            .copied();
        let base_p95 = base_scenario
            .map(|value| format!("{:.2}", value.latency.p95_ms))
            .unwrap_or_else(|| "-".to_string());
        let change = base_scenario
            .filter(|value| value.latency.p95_ms > 0.0)
            .map(|value| {
                format!(
                    "{:+.1}%",
                    (scenario.latency.p95_ms / value.latency.p95_ms - 1.0) * 100.0
                )
            })
            .unwrap_or_else(|| "-".to_string());
        output.push_str(&format!(
            "| {} | {} | {:.2} | {} | {} | {:.2} | {} |\n",
            scenario.name,
            scenario.phase,
            scenario.latency.p95_ms,
            base_p95,
            change,
            scenario.requests_per_second,
            scenario.failures + scenario.timeouts
        ));
    }
    output.push('\n');
    if failures.is_empty() {
        output.push_str("Correctness checks passed. Timing differences do not fail this run.\n\n");
    } else {
        output.push_str("Correctness checks failed:\n\n");
        for failure in failures {
            output.push_str(&format!("- {failure}\n"));
        }
        output.push('\n');
    }
    output.push_str(&format!(
        "Lifecycle outcome: `{}`; database {} bytes; peak application RSS {} bytes.\n\n",
        head.lifecycle.outcome,
        head.resources.database_bytes,
        head.resources.peak_application_resident_bytes
    ));
    output
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

fn invalid_data(message: impl Into<String>) -> Error {
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

#[cfg(feature = "scale-benchmark")]
mod loader;
#[cfg(feature = "scale-benchmark")]
mod runner;

#[cfg(feature = "scale-benchmark")]
pub use loader::{LoadReport, load_dataset, verify_loaded_dataset};
#[cfg(feature = "scale-benchmark")]
pub use runner::{MeasureOptions, measure_scale_benchmark};

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
    fn limit_modes_preserve_production_defaults_and_declare_elevated_values() {
        let standard = LimitMode::Standard.settings();
        let extended = LimitMode::Extended.settings();

        assert_eq!(standard.default_page_limit, 100);
        assert_eq!(standard.maximum_page_limit, 250);
        assert_eq!(standard.maximum_graph_depth, 100);
        assert_eq!(standard.maximum_export_output_bytes, 262_144);
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

        for scenario in &workload.scenarios {
            let path = workload
                .render_path(scenario, &manifest, LimitMode::Standard)
                .unwrap();
            assert!(path.starts_with("/api/"));
            assert!(!path.contains(['{', '}']));
        }
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
            RuntimeIdentity {
                runner: "test".to_string(),
                postgres_version: "test".to_string(),
                process_fresh: true,
                database_fresh: true,
                deliberate_warmup_requests: 3,
                database_settings: BTreeMap::new(),
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
                postgres_cpu_seconds: None,
                peak_application_resident_bytes: 10,
                peak_postgres_resident_bytes: None,
                database_bytes: 20,
                table_bytes: 15,
                index_bytes: 5,
                wal_bytes: None,
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

    #[test]
    fn assessment_keeps_performance_informational() {
        let base = report("base");
        let mut head = report("head");
        head.scenarios[0].latency = LatencyDistribution::from_samples(&[5_000.0]);

        ScaleAssessment::assess(&head, Some(&base))
            .ensure_passed()
            .unwrap();
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
    fn serialized_report_contains_no_credential_fields() {
        let encoded = serde_json::to_string(&report("head")).unwrap();

        for forbidden in ["password", "database_url", "bearer", "token"] {
            assert!(!encoded.to_ascii_lowercase().contains(forbidden));
        }
    }
}
