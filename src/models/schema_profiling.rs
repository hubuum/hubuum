//! Validated schema inference options owned by the HTTP API.
use json_schema_profiler::{
    InferencePolicy, Limits, OutputLimits, ProfilerOptions, SuggestionOptions,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::errors::ApiError;
use crate::models::class_fields::SampleLimit;

/// Strict rejects mixed types and closes objects; balanced preserves observed
/// unions and required fields; expansive makes fields optional and objects open.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ProfilePreset {
    Strict,
    #[default]
    Balanced,
    Expansive,
}
impl ProfilePreset {
    fn inference(self) -> InferencePolicy {
        match self {
            Self::Strict => InferencePolicy::strict(),
            Self::Balanced => InferencePolicy::balanced(),
            Self::Expansive => InferencePolicy::expansive(),
        }
    }
}

/// Omitted options use a 100-object sample and balanced inference.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[serde(default, deny_unknown_fields)]
#[schema(as = SchemaProfileRequest)]
pub struct ProfileRequestWire {
    #[schema(minimum = 1, maximum = 250, default = 100)]
    sample_limit: usize,
    preset: ProfilePreset,
}
impl Default for ProfileRequestWire {
    fn default() -> Self {
        Self {
            sample_limit: 100,
            preset: ProfilePreset::Balanced,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(try_from = "ProfileRequestWire", into = "ProfileRequestWire")]
pub struct SchemaProfileRequest {
    sample_limit: SampleLimit,
    preset: ProfilePreset,
}
impl From<SchemaProfileRequest> for ProfileRequestWire {
    fn from(value: SchemaProfileRequest) -> Self {
        Self {
            sample_limit: value.sample_limit.get(),
            preset: value.preset,
        }
    }
}
impl TryFrom<ProfileRequestWire> for SchemaProfileRequest {
    type Error = ApiError;
    fn try_from(wire: ProfileRequestWire) -> Result<Self, ApiError> {
        Ok(Self {
            sample_limit: SampleLimit::new(wire.sample_limit).map_err(|_| {
                ApiError::BadRequest("sample_limit must be between 1 and 250".into())
            })?,
            preset: wire.preset,
        })
    }
}
impl SchemaProfileRequest {
    pub(crate) fn sample_limit(&self) -> SampleLimit {
        self.sample_limit
    }
    pub(crate) fn profiler_options(&self) -> ProfilerOptions {
        ProfilerOptions::default().with_limits(
            Limits::builder()
                .documents(self.sample_limit.get())
                .document_nodes(100_000)
                .depth(32)
                .profile_paths(1024)
                .profile_path_bytes(256 * 1024)
                .report_bytes(1024 * 1024)
                .build()
                .expect("fixed limits and validated sample size"),
        )
    }
    pub(crate) fn suggestion_options(&self) -> SuggestionOptions {
        SuggestionOptions::new(self.preset.inference()).with_output_limits(
            OutputLimits::new(512 * 1024, 1024 * 1024, 4096).expect("fixed nonzero output limits"),
        )
    }
}

#[derive(Serialize, ToSchema)]
pub struct SchemaProfileReport {
    pub class_id: i32,
    /// Always true: inference describes a deterministic authorized convenience sample.
    pub sampled: bool,
    pub sampled_objects: usize,
    pub effective_sample_limit: usize,
    pub truncation: Option<crate::models::class_fields::FieldDiscoveryTruncation>,
    pub options: ProfileRequestWire,
    pub status: ProfileSuggestionStatus,
    pub schema: Option<serde_json::Value>,
    /// Profiler 0.0.1 structural counts and document evidence. No scalar values are retained.
    pub profile: Option<serde_json::Value>,
    pub findings: Vec<serde_json::Value>,
    /// Validation of exactly the in-memory sample used for profiling, when a candidate exists.
    pub replay: Option<serde_json::Value>,
    pub sample_valid: Option<bool>,
}
#[derive(Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ProfileSuggestionStatus {
    Candidate,
    Blocked,
    Insufficient,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use serde_json::json;
    #[rstest]
    #[case(json!({"sample_limit":0}))]
    #[case(json!({"sample_limit":251}))]
    #[case(json!({"sample_limit":-1}))]
    #[case(json!({"unknown":true}))]
    #[case(json!({"preset":"unknown"}))]
    #[case(json!({"enums":{}}))]
    fn invalid_options_are_rejected_at_deserialization(#[case] value: serde_json::Value) {
        assert!(serde_json::from_value::<SchemaProfileRequest>(value).is_err());
    }
    #[test]
    fn validated_options_survive_task_payload_roundtrip() {
        let options: SchemaProfileRequest =
            serde_json::from_value(json!({"sample_limit":250,"preset":"strict"})).unwrap();
        let stored = serde_json::to_value(&options).unwrap();
        let restored: SchemaProfileRequest = serde_json::from_value(stored.clone()).unwrap();
        assert_eq!(serde_json::to_value(restored).unwrap(), stored);
    }
}
