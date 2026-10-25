//! Bounded observations of an authorized object sample, without schema inference.
use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, Write};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

use crate::errors::ApiError;

pub(crate) const MAX_FIELD_CANDIDATES: usize = 1024;
const MAX_FIELDS: usize = 1024;
const MAX_WORK: usize = 100_000;
const MAX_PATH_BYTES: usize = 512;
const MAX_RESPONSE_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug)]
struct SampleLimit(usize);

impl SampleLimit {
    fn new(value: usize) -> Result<Self, ApiError> {
        if !(1..=250).contains(&value) {
            return Err(ApiError::BadRequest(
                "limit must be between 1 and 250".into(),
            ));
        }
        Ok(Self(value))
    }
}

#[derive(Clone, Copy, Debug)]
struct PathDepth(usize);

impl PathDepth {
    fn new(value: usize) -> Result<Self, ApiError> {
        if value > 32 {
            return Err(ApiError::BadRequest(
                "depth must be between 0 and 32".into(),
            ));
        }
        Ok(Self(value))
    }
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(try_from = "DiscoveryOptionsWire")]
pub struct FieldDiscoveryOptions {
    limit: SampleLimit,
    depth: PathDepth,
    containers: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DiscoveryOptionsWire {
    limit: Option<usize>,
    depth: Option<usize>,
    containers: Option<bool>,
}

impl TryFrom<DiscoveryOptionsWire> for FieldDiscoveryOptions {
    type Error = ApiError;
    fn try_from(value: DiscoveryOptionsWire) -> Result<Self, Self::Error> {
        Self::new(
            value.limit.unwrap_or(100),
            value.depth.unwrap_or(6),
            value.containers.unwrap_or(false),
        )
    }
}

impl FieldDiscoveryOptions {
    pub fn new(limit: usize, depth: usize, containers: bool) -> Result<Self, ApiError> {
        Ok(Self {
            limit: SampleLimit::new(limit)?,
            depth: PathDepth::new(depth)?,
            containers,
        })
    }

    pub(crate) const fn limit(self) -> usize {
        self.limit.0
    }
}

/// Property names and array traversal remain distinct even when display labels collide.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, ToSchema)]
#[serde(tag = "kind", content = "name", rename_all = "snake_case")]
pub enum FieldPathSegment {
    Property(String),
    ArrayItem,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum FieldSource {
    Data,
    SharedComputed,
    PersonalComputed,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ObservedFieldType {
    Null,
    Bool,
    Number,
    String,
    Array,
    Object,
}

impl From<&Value> for ObservedFieldType {
    fn from(value: &Value) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::Bool(_) => Self::Bool,
            Value::Number(_) => Self::Number,
            Value::String(_) => Self::String,
            Value::Array(_) => Self::Array,
            Value::Object(_) => Self::Object,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum FieldDiscoveryTruncation {
    SampleLimit,
    CandidateLimit,
    Depth,
    WorkLimit,
    FieldLimit,
    PathLimit,
    ResponseLimit,
}

#[derive(Serialize, ToSchema)]
pub struct DiscoveredField {
    field: String,
    source: FieldSource,
    path: Vec<FieldPathSegment>,
    /// Number of value occurrences; repeated array items count separately.
    observation_count: usize,
    /// Number of sampled objects contributing at least one value at this path.
    object_count: usize,
    types: BTreeMap<ObservedFieldType, usize>,
    example: Option<String>,
    example_truncated: bool,
    error_count: usize,
    #[serde(skip)]
    last_object: Option<usize>,
}

impl DiscoveredField {
    fn new(field: String, source: FieldSource, path: Vec<FieldPathSegment>) -> Self {
        Self {
            field,
            source,
            path,
            observation_count: 0,
            object_count: 0,
            types: BTreeMap::new(),
            example: None,
            example_truncated: false,
            error_count: 0,
            last_object: None,
        }
    }

    fn observe(&mut self, object: usize, value: &Value) {
        self.observation_count += 1;
        *self.types.entry(value.into()).or_default() += 1;
        if self.last_object != Some(object) {
            self.object_count += 1;
            self.last_object = Some(object);
        }
        if self.example.is_none() {
            let (example, truncated) = preview(value);
            self.example = Some(example);
            self.example_truncated = truncated;
        }
    }
}

#[derive(Serialize, ToSchema)]
pub struct ClassFieldInventory {
    sampled: bool,
    sampled_objects: usize,
    effective_sample_limit: usize,
    max_depth: usize,
    include_containers: bool,
    truncation: BTreeSet<FieldDiscoveryTruncation>,
    fields: Vec<DiscoveredField>,
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum FieldIdentity {
    Data(Vec<FieldPathSegment>),
    Shared(String),
    Personal(String),
}

pub(crate) struct FieldInventoryBuilder {
    options: FieldDiscoveryOptions,
    fields: BTreeMap<FieldIdentity, DiscoveredField>,
    truncation: BTreeSet<FieldDiscoveryTruncation>,
    remaining_work: usize,
    stopped: bool,
}

impl FieldInventoryBuilder {
    pub(crate) fn new(options: FieldDiscoveryOptions) -> Self {
        Self {
            options,
            fields: BTreeMap::new(),
            truncation: BTreeSet::new(),
            remaining_work: MAX_WORK,
            stopped: false,
        }
    }

    pub(crate) fn has_computed_definitions(&self) -> bool {
        !self.fields.is_empty()
    }

    pub(crate) fn truncate(&mut self, reason: FieldDiscoveryTruncation) {
        self.truncation.insert(reason);
    }

    pub(crate) fn add_definition(&mut self, key: &str, personal: bool) {
        let (identity, source, prefix) = if personal {
            (
                FieldIdentity::Personal(key.into()),
                FieldSource::PersonalComputed,
                "P",
            )
        } else {
            (
                FieldIdentity::Shared(key.into()),
                FieldSource::SharedComputed,
                "S",
            )
        };
        self.fields
            .entry(identity)
            .or_insert_with(|| DiscoveredField::new(format!("{prefix}:{key}"), source, Vec::new()));
    }

    pub(crate) fn observe_computed(
        &mut self,
        object: usize,
        key: &str,
        personal: bool,
        value: &Value,
        has_error: bool,
    ) {
        let identity = if personal {
            FieldIdentity::Personal(key.into())
        } else {
            FieldIdentity::Shared(key.into())
        };
        if let Some(field) = self.fields.get_mut(&identity) {
            field.observe(object, value);
            field.error_count += usize::from(has_error);
        }
    }

    pub(crate) fn observe_data(&mut self, object: usize, value: &Value) {
        self.visit(object, value, &mut Vec::new(), 0);
    }

    fn visit(
        &mut self,
        object: usize,
        value: &Value,
        path: &mut Vec<FieldPathSegment>,
        path_bytes: usize,
    ) {
        if self.stopped {
            return;
        }
        if self.remaining_work == 0 {
            self.truncate(FieldDiscoveryTruncation::WorkLimit);
            self.stopped = true;
            return;
        }
        self.remaining_work -= 1;
        if !path.is_empty()
            && (self.options.containers || !matches!(value, Value::Object(_) | Value::Array(_)))
        {
            let identity = FieldIdentity::Data(path.clone());
            if !self.fields.contains_key(&identity) && self.fields.len() >= MAX_FIELDS {
                self.truncate(FieldDiscoveryTruncation::FieldLimit);
                self.stopped = true;
                return;
            }
            self.fields
                .entry(identity)
                .or_insert_with(|| {
                    let mut display = String::from("data");
                    for segment in path.iter() {
                        match segment {
                            FieldPathSegment::Property(name) => {
                                display.push('.');
                                display.push_str(name);
                            }
                            FieldPathSegment::ArrayItem => display.push_str("[*]"),
                        }
                    }
                    DiscoveredField::new(display, FieldSource::Data, path.clone())
                })
                .observe(object, value);
        }
        if path.len() >= self.options.depth.0 {
            if matches!(value, Value::Object(values) if !values.is_empty())
                || matches!(value, Value::Array(values) if !values.is_empty())
            {
                self.truncate(FieldDiscoveryTruncation::Depth);
            }
            return;
        }
        match value {
            Value::Object(values) => {
                for (key, value) in values {
                    if self.stopped {
                        break;
                    }
                    if key.len() > MAX_PATH_BYTES.saturating_sub(path_bytes) {
                        self.truncate(FieldDiscoveryTruncation::PathLimit);
                        // Rejected keys still consume traversal work.
                        if self.remaining_work == 0 {
                            self.stopped = true;
                            self.truncate(FieldDiscoveryTruncation::WorkLimit);
                            break;
                        }
                        self.remaining_work -= 1;
                        continue;
                    }
                    path.push(FieldPathSegment::Property(key.clone()));
                    self.visit(object, value, path, path_bytes + key.len());
                    path.pop();
                }
            }
            Value::Array(values) => {
                path.push(FieldPathSegment::ArrayItem);
                for value in values {
                    if self.stopped {
                        break;
                    }
                    self.visit(object, value, path, path_bytes);
                }
                path.pop();
            }
            _ => {}
        }
    }

    pub(crate) fn finish(
        mut self,
        sampled_objects: usize,
    ) -> Result<ClassFieldInventory, ApiError> {
        let mut fields = self.fields.into_values().collect::<Vec<_>>();
        // Keep the bounded enabled selector set even when data paths exhaust
        // the response budget. Restore the documented display order afterward.
        fields.sort_by(|a, b| {
            (a.source == FieldSource::Data, a.source, &a.field, &a.path).cmp(&(
                b.source == FieldSource::Data,
                b.source,
                &b.field,
                &b.path,
            ))
        });
        // Reserve a fixed allowance for the bounded response envelope and all
        // truncation flags. Per-field serialization accounts for JSON escaping.
        let mut bytes = 4096;
        let mut keep = 0;
        for field in &fields {
            bytes += serde_json::to_vec(field)
                .map_err(|error| ApiError::InternalServerError(error.to_string()))?
                .len()
                + 1;
            if bytes > MAX_RESPONSE_BYTES {
                self.truncation
                    .insert(FieldDiscoveryTruncation::ResponseLimit);
                break;
            }
            keep += 1;
        }
        fields.truncate(keep);
        fields.sort_by(|a, b| (a.source, &a.field, &a.path).cmp(&(b.source, &b.field, &b.path)));
        Ok(ClassFieldInventory {
            sampled: true,
            sampled_objects,
            effective_sample_limit: self.options.limit.0,
            max_depth: self.options.depth.0,
            include_containers: self.options.containers,
            truncation: self.truncation,
            fields,
        })
    }
}

// Capture only enough UTF-8 bytes for the CLI's 72-character first-value preview.
// Serialization stops as soon as this buffer fills, including for containers.
#[derive(Default)]
struct PreviewWriter {
    bytes: Vec<u8>,
    clipped: bool,
}
impl Write for PreviewWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let count = bytes.len().min(289 - self.bytes.len());
        if count < bytes.len() {
            self.clipped = true;
        }
        if count == 0 && !bytes.is_empty() {
            return Err(io::Error::other("preview limit"));
        }
        self.bytes.extend_from_slice(&bytes[..count]);
        Ok(count)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn preview_value(writer: &mut PreviewWriter, value: &Value) -> io::Result<()> {
    if let Value::String(value) = value {
        writer.write_all(value.as_bytes())
    } else {
        serde_json::to_writer(writer, value).map_err(io::Error::other)
    }
}

fn preview(value: &Value) -> (String, bool) {
    let mut writer = PreviewWriter::default();
    if let Value::Object(values) = value {
        for (index, (key, value)) in values.iter().enumerate() {
            let result = (|| {
                if index > 0 {
                    writer.write_all(b", ")?;
                }
                writer.write_all(key.as_bytes())?;
                writer.write_all(b"=")?;
                preview_value(&mut writer, value)
            })();
            if result.is_err() {
                break;
            }
        }
    } else {
        let _ = preview_value(&mut writer, value);
    }
    let text = String::from_utf8_lossy(&writer.bytes);
    let truncated = writer.clipped || text.chars().count() > 72;
    if truncated {
        (
            format!("{}...", text.chars().take(69).collect::<String>()),
            true,
        )
    } else {
        (text.into_owned(), false)
    }
}

#[cfg(test)]
mod tests;
