use hubuum_domain::{ExportTemplateId, ObjectId};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Absolute frontend URL containing exactly one `{object_id}` placeholder.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[serde(try_from = "String", into = "String")]
#[schema(value_type = String, example = "https://inventory.example/objects/{object_id}")]
pub struct SchemaObjectUrlTemplate(String);

impl SchemaObjectUrlTemplate {
    pub fn try_new(value: String) -> Result<Self, String> {
        let error = "object_url_template must be an absolute HTTP(S) URL without credentials, containing exactly one {object_id} placeholder";
        if value.len() > 2048
            || value.chars().any(char::is_whitespace)
            || value.matches("{object_id}").count() != 1
        {
            return Err(error.into());
        }
        let url = Url::parse(&value.replace("{object_id}", "1")).map_err(|_| error.to_owned())?;
        if !matches!(url.scheme(), "http" | "https")
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
        {
            return Err(error.into());
        }
        Ok(Self(value))
    }

    pub fn object_url(&self, object_id: ObjectId) -> String {
        self.0.replace("{object_id}", &object_id.to_string())
    }
}

impl TryFrom<String> for SchemaObjectUrlTemplate {
    type Error = String;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}
impl From<SchemaObjectUrlTemplate> for String {
    fn from(value: SchemaObjectUrlTemplate) -> Self {
        value.0
    }
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SchemaRepairReportRequest {
    pub object_url_template: SchemaObjectUrlTemplate,
    /// Optional stored HTML layout. It must render `report_content` exactly once.
    pub template_id: Option<ExportTemplateId>,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SchemaRepairReportQuery {
    /// Serve the same retained HTML as a download instead of inline.
    #[serde(default)]
    pub download: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case("javascript:alert({object_id})")]
    #[case("/objects/{object_id}")]
    #[case("https://user:password@example.test/{object_id}")]
    #[case("https://example.test/{object_id}/{object_id}")]
    #[case("https://example.test/objects")]
    #[case("https://example.test/\n{object_id}")]
    fn frontend_urls_reject_unsafe_or_unusable_templates(#[case] value: &str) {
        assert!(SchemaObjectUrlTemplate::try_new(value.into()).is_err());
    }

    #[test]
    fn frontend_urls_preserve_subpaths_and_hash_routes() {
        let template = SchemaObjectUrlTemplate::try_new(
            "https://example.test/inventory/#/objects/{object_id}".into(),
        )
        .unwrap();
        assert_eq!(
            template.object_url(ObjectId::new(123).unwrap()),
            "https://example.test/inventory/#/objects/123"
        );
    }
}
