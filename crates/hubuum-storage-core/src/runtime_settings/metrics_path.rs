use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct MetricsPath(String);

impl MetricsPath {
    pub fn new(path: impl Into<String>) -> Result<Self, String> {
        let path = path.into();
        if !path.starts_with('/') || path.len() == 1 {
            return Err("metrics_path must be an absolute non-root path".to_string());
        }
        if path.ends_with('/') || path.contains("//") {
            return Err("metrics_path must not contain empty path segments".to_string());
        }
        if path
            .chars()
            .any(|character| character.is_whitespace() || character.is_control())
            || path.contains(['{', '}', '*', '?', '#'])
        {
            return Err("metrics_path must be a literal HTTP path".to_string());
        }
        if path == "/api"
            || path.starts_with("/api/")
            || path == "/swagger-ui"
            || path.starts_with("/swagger-ui/")
            || matches!(
                path.as_str(),
                "/healthz" | "/readyz" | "/api-doc/openapi.json"
            )
        {
            return Err(format!(
                "metrics_path '{path}' collides with an existing application route"
            ));
        }
        Ok(Self(path))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for MetricsPath {
    type Err = String;

    fn from_str(path: &str) -> Result<Self, Self::Err> {
        Self::new(path)
    }
}

impl TryFrom<String> for MetricsPath {
    type Error = String;

    fn try_from(path: String) -> Result<Self, Self::Error> {
        Self::new(path)
    }
}

impl From<MetricsPath> for String {
    fn from(path: MetricsPath) -> Self {
        path.0
    }
}
