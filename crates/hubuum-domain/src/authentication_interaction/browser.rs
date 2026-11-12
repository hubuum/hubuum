use std::fmt;

use serde::{Deserialize, Serialize};
use url::Url;

use super::InteractionValidationError;

/// A bounded, immutable handoff selected from deployment-owned registrations.
/// It carries no credential; the receiving BFF must match the nonce to its session.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "ReturnDocument", into = "ReturnDocument")]
pub struct InteractionReturnTarget {
    id: String,
    uri: String,
    nonce: String,
}

impl InteractionReturnTarget {
    pub fn try_new(
        id: String,
        uri: String,
        nonce: String,
    ) -> Result<Self, InteractionValidationError> {
        let url = Url::parse(&uri).map_err(|_| InteractionValidationError::InvalidReturnTarget)?;
        let loopback = url.host_str().is_some_and(|host| {
            host == "localhost"
                || host
                    .trim_matches(['[', ']'])
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
        });
        if id.is_empty()
            || id.len() > 64
            || !id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
            || uri.len() > 2048
            || url.host_str().is_none()
            || !(url.scheme() == "https" || url.scheme() == "http" && loopback)
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || !(32..=128).contains(&nonce.len())
            || !nonce
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.~".contains(&b))
        {
            return Err(InteractionValidationError::InvalidReturnTarget);
        }
        Ok(Self {
            id,
            uri: url.to_string(),
            nonce,
        })
    }
    pub fn id(&self) -> &str {
        &self.id
    }
    pub fn uri(&self) -> &str {
        &self.uri
    }
    pub fn nonce(&self) -> &str {
        &self.nonce
    }
}
impl fmt::Debug for InteractionReturnTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InteractionReturnTarget")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReturnDocument {
    id: String,
    uri: String,
    nonce: String,
}
impl TryFrom<ReturnDocument> for InteractionReturnTarget {
    type Error = InteractionValidationError;
    fn try_from(value: ReturnDocument) -> Result<Self, Self::Error> {
        Self::try_new(value.id, value.uri, value.nonce)
    }
}
impl From<InteractionReturnTarget> for ReturnDocument {
    fn from(value: InteractionReturnTarget) -> Self {
        Self {
            id: value.id,
            uri: value.uri,
            nonce: value.nonce,
        }
    }
}

/// Server-authored information shown before a browser approves an interaction.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "PromptDocument", into = "PromptDocument")]
pub struct InteractionBrowserPrompt {
    provider: crate::IdentityScopeName,
    details: String,
}
impl InteractionBrowserPrompt {
    pub fn new(
        provider: crate::IdentityScopeName,
        details: String,
    ) -> Result<Self, InteractionValidationError> {
        if details.is_empty()
            || details.len() > 8192
            || details.chars().any(|c| c.is_control() && c != '\n')
        {
            return Err(InteractionValidationError::InvalidPrompt);
        }
        Ok(Self { provider, details })
    }
    pub fn provider(&self) -> &crate::IdentityScopeName {
        &self.provider
    }
    pub fn details(&self) -> &str {
        &self.details
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PromptDocument {
    provider: crate::IdentityScopeName,
    details: String,
}
impl TryFrom<PromptDocument> for InteractionBrowserPrompt {
    type Error = InteractionValidationError;
    fn try_from(value: PromptDocument) -> Result<Self, Self::Error> {
        Self::new(value.provider, value.details)
    }
}
impl From<InteractionBrowserPrompt> for PromptDocument {
    fn from(value: InteractionBrowserPrompt) -> Self {
        Self {
            provider: value.provider,
            details: value.details,
        }
    }
}

/// Bounded ciphertext; only the initiating client capability can open it.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct InteractionSealedOperation(String);
impl InteractionSealedOperation {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for InteractionSealedOperation {
    type Error = InteractionValidationError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.is_empty() || value.len() > 131_072 {
            return Err(InteractionValidationError::InvalidPurpose);
        }
        Ok(Self(value))
    }
}
impl From<InteractionSealedOperation> for String {
    fn from(value: InteractionSealedOperation) -> Self {
        value.0
    }
}
impl fmt::Debug for InteractionSealedOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<encrypted>")
    }
}

/// Bounded successful LDAP preview, retained with its verified subject only.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "VerificationDocument", into = "VerificationDocument")]
pub struct InteractionVerificationDetails {
    admitted: bool,
    groups: Vec<(String, String)>,
}
impl InteractionVerificationDetails {
    pub fn new(
        admitted: bool,
        groups: Vec<(String, String)>,
    ) -> Result<Self, InteractionValidationError> {
        if groups.len() > 1000
            || groups.iter().any(|(key, name)| {
                [key, name]
                    .iter()
                    .any(|v| v.is_empty() || v.len() > 255 || v.chars().any(char::is_control))
            })
        {
            return Err(InteractionValidationError::InvalidSubject);
        }
        Ok(Self { admitted, groups })
    }
    pub const fn admitted(&self) -> bool {
        self.admitted
    }
    pub fn groups(&self) -> &[(String, String)] {
        &self.groups
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct VerificationDocument {
    admitted: bool,
    groups: Vec<(String, String)>,
}
impl TryFrom<VerificationDocument> for InteractionVerificationDetails {
    type Error = InteractionValidationError;
    fn try_from(v: VerificationDocument) -> Result<Self, Self::Error> {
        Self::new(v.admitted, v.groups)
    }
}
impl From<InteractionVerificationDetails> for VerificationDocument {
    fn from(v: InteractionVerificationDetails) -> Self {
        Self {
            admitted: v.admitted,
            groups: v.groups,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case::short_nonce("web", "https://web.example/callback", "short")]
    #[case::query(
        "web",
        "https://web.example/callback?secret=1",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    )]
    #[case::userinfo(
        "web",
        "https://user@web.example/callback",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    )]
    #[case::remote_http(
        "web",
        "http://web.example/callback",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    )]
    #[case::fragment(
        "web",
        "https://web.example/callback#secret",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    )]
    fn invalid_return_document_cannot_be_deserialized(
        #[case] id: &str,
        #[case] uri: &str,
        #[case] nonce: &str,
    ) {
        let value = serde_json::json!({"id":id, "uri":uri, "nonce":nonce});
        assert!(serde_json::from_value::<InteractionReturnTarget>(value).is_err());
    }

    #[test]
    fn browser_prompt_revalidates_persisted_bounds() {
        let value = serde_json::json!({"provider":"local","details":"x".repeat(8193)});
        assert!(serde_json::from_value::<InteractionBrowserPrompt>(value).is_err());
    }
}
