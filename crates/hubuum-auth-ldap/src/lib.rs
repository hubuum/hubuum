#![allow(async_fn_in_trait)]

use hubuum_auth_core::{
    AuthProviderError, AuthenticatedExternalUser, ExternalGroup, ExternalIdentityProvider,
    ExternalUserProfile, IdentityScopeName,
};
use ldap3::{LdapConnAsync, LdapConnSettings, Scope, SearchEntry};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::time::Duration;
use url::Url;

#[derive(Clone, Serialize, Deserialize)]
pub struct LdapScopeConfig {
    pub scope: String,
    pub url: String,
    pub bind_dn: Option<String>,
    pub bind_password: Option<String>,
    #[serde(default = "default_connect_timeout_seconds")]
    pub connect_timeout_seconds: u64,
    #[serde(default = "default_operation_timeout_seconds")]
    pub operation_timeout_seconds: u64,
    pub user_base_dn: String,
    pub user_filter: String,
    #[serde(default = "default_subtree")]
    pub user_scope: LdapSearchScope,
    #[serde(default = "default_uid_attr")]
    pub username_attribute: String,
    #[serde(default = "default_dn_attr")]
    pub subject_attribute: String,
    pub display_name_attribute: Option<String>,
    pub email_attribute: Option<String>,
    #[serde(default)]
    pub group_attributes: Vec<String>,
    #[serde(default)]
    pub group_filters: Vec<String>,
    #[serde(default)]
    pub group_rules: Vec<GroupMappingRuleConfig>,
}

impl fmt::Debug for LdapScopeConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LdapScopeConfig")
            .field("scope", &self.scope)
            .field("url", &self.url)
            .field("bind_dn", &self.bind_dn)
            .field(
                "bind_password",
                &self.bind_password.as_ref().map(|_| "<redacted>"),
            )
            .field("connect_timeout_seconds", &self.connect_timeout_seconds)
            .field("operation_timeout_seconds", &self.operation_timeout_seconds)
            .field("user_base_dn", &self.user_base_dn)
            .field("user_filter", &self.user_filter)
            .field("user_scope", &self.user_scope)
            .field("username_attribute", &self.username_attribute)
            .field("subject_attribute", &self.subject_attribute)
            .field("display_name_attribute", &self.display_name_attribute)
            .field("email_attribute", &self.email_attribute)
            .field("group_attributes", &self.group_attributes)
            .field("group_filters", &self.group_filters)
            .field("group_rules", &self.group_rules)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LdapSearchScope {
    Base,
    One,
    Subtree,
}

impl From<LdapSearchScope> for Scope {
    fn from(value: LdapSearchScope) -> Self {
        match value {
            LdapSearchScope::Base => Scope::Base,
            LdapSearchScope::One => Scope::OneLevel,
            LdapSearchScope::Subtree => Scope::Subtree,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupMappingRuleConfig {
    pub pattern: String,
    pub name: String,
    pub key: Option<String>,
    pub description: Option<String>,
}

#[derive(Clone)]
struct GroupMappingRule {
    pattern: Regex,
    name: String,
    key: Option<String>,
    description: Option<String>,
}

pub struct LdapIdentityProvider {
    scope_name: IdentityScopeName,
    config: LdapScopeConfig,
    group_filters: Vec<Regex>,
    rules: Vec<GroupMappingRule>,
    starttls: bool,
}

impl LdapIdentityProvider {
    pub fn new(config: LdapScopeConfig) -> Result<Self, AuthProviderError> {
        let scope_name = IdentityScopeName::new(config.scope.clone())?;
        let parsed_url = Url::parse(&config.url)
            .map_err(|e| AuthProviderError::Config(format!("invalid ldap url: {e}")))?;
        if parsed_url.host_str().is_none() {
            return Err(AuthProviderError::Config(
                "ldap url must include a host".to_string(),
            ));
        }
        let starttls = match parsed_url.scheme() {
            "ldap" => true,
            "ldaps" => false,
            scheme => {
                return Err(AuthProviderError::Config(format!(
                    "ldap url scheme must be 'ldap' or 'ldaps', got '{scheme}'"
                )));
            }
        };
        if config.user_filter.trim().is_empty() {
            return Err(AuthProviderError::Config(
                "ldap user_filter must not be empty".to_string(),
            ));
        }
        if !config.user_filter.contains("{username}") {
            return Err(AuthProviderError::Config(
                "ldap user_filter must include {username}".to_string(),
            ));
        }
        if config.connect_timeout_seconds == 0 {
            return Err(AuthProviderError::Config(
                "ldap connect_timeout_seconds must be positive".to_string(),
            ));
        }
        if config.operation_timeout_seconds == 0 {
            return Err(AuthProviderError::Config(
                "ldap operation_timeout_seconds must be positive".to_string(),
            ));
        }
        if config.bind_dn.is_some() != config.bind_password.is_some() {
            return Err(AuthProviderError::Config(
                "ldap bind_dn and bind_password must be configured together".to_string(),
            ));
        }
        let group_filters = config
            .group_filters
            .iter()
            .map(|filter| {
                Regex::new(filter).map_err(|e| {
                    AuthProviderError::Config(format!("invalid ldap group filter '{filter}': {e}"))
                })
            })
            .collect::<Result<Vec<_>, AuthProviderError>>()?;
        let rules = config
            .group_rules
            .iter()
            .map(|rule| {
                Ok(GroupMappingRule {
                    pattern: Regex::new(&rule.pattern)
                        .map_err(|e| AuthProviderError::Config(e.to_string()))?,
                    name: rule.name.clone(),
                    key: rule.key.clone(),
                    description: rule.description.clone(),
                })
            })
            .collect::<Result<Vec<_>, AuthProviderError>>()?;
        Ok(Self {
            scope_name,
            config,
            group_filters,
            rules,
            starttls,
        })
    }

    async fn ldap(&self) -> Result<ldap3::Ldap, AuthProviderError> {
        let settings = LdapConnSettings::new()
            .set_conn_timeout(self.connect_timeout())
            .set_starttls(self.starttls);
        let (conn, ldap) = LdapConnAsync::with_settings(settings, &self.config.url)
            .await
            .map_err(|e| AuthProviderError::Unavailable(e.to_string()))?;
        ldap3::drive!(conn);
        Ok(ldap)
    }

    fn connect_timeout(&self) -> Duration {
        Duration::from_secs(self.config.connect_timeout_seconds)
    }

    fn operation_timeout(&self) -> Duration {
        Duration::from_secs(self.config.operation_timeout_seconds)
    }

    fn user_filter(&self, username: &str) -> String {
        self.config
            .user_filter
            .replace("{username}", &escape_filter_value(username))
    }

    fn search_attributes(&self) -> Vec<String> {
        let mut attrs = BTreeSet::new();
        attrs.insert(self.config.username_attribute.clone());
        attrs.insert(self.config.subject_attribute.clone());
        if let Some(attr) = &self.config.display_name_attribute {
            attrs.insert(attr.clone());
        }
        if let Some(attr) = &self.config.email_attribute {
            attrs.insert(attr.clone());
        }
        for attr in &self.config.group_attributes {
            attrs.insert(attr.clone());
        }
        attrs.into_iter().collect()
    }

    async fn bind_service(&self, ldap: &mut ldap3::Ldap) -> Result<(), AuthProviderError> {
        match (&self.config.bind_dn, &self.config.bind_password) {
            (Some(dn), Some(password)) => ldap
                .with_timeout(self.operation_timeout())
                .simple_bind(dn, password)
                .await
                .map_err(|e| AuthProviderError::Unavailable(e.to_string()))?
                .success()
                .map_err(|e| AuthProviderError::Config(format!("ldap service bind failed: {e}")))
                .map(|_| ()),
            (None, None) => Ok(()),
            _ => Err(AuthProviderError::Config(
                "ldap bind_dn and bind_password must be configured together".to_string(),
            )),
        }
    }

    async fn load_user_by_filter(
        &self,
        filter: &str,
    ) -> Result<(String, SearchEntry), AuthProviderError> {
        let mut ldap = self.ldap().await?;
        self.bind_service(&mut ldap).await?;
        let attrs = self.search_attributes();
        let (entries, _) = ldap
            .with_timeout(self.operation_timeout())
            .search(
                &self.config.user_base_dn,
                self.config.user_scope.into(),
                filter,
                attrs,
            )
            .await
            .map_err(|e| AuthProviderError::Unavailable(e.to_string()))?
            .success()
            .map_err(|e| AuthProviderError::Protocol(e.to_string()))?;

        if entries.len() != 1 {
            return Err(AuthProviderError::AuthenticationFailed);
        }
        let entry = SearchEntry::construct(entries.into_iter().next().unwrap());
        Ok((entry.dn.clone(), entry))
    }

    fn profile_from_entry(
        &self,
        username: &str,
        dn: &str,
        entry: &SearchEntry,
    ) -> Result<ExternalUserProfile, AuthProviderError> {
        let subject = if self.subject_is_dn() {
            dn.to_string()
        } else {
            first_attr(entry, &self.config.subject_attribute)
                .ok_or_else(|| AuthProviderError::Protocol("ldap user missing subject".into()))?
        };
        let name = first_attr(entry, &self.config.username_attribute)
            .unwrap_or_else(|| username.to_string());
        let proper_name = self
            .config
            .display_name_attribute
            .as_deref()
            .and_then(|attr| first_attr(entry, attr));
        let email = self
            .config
            .email_attribute
            .as_deref()
            .and_then(|attr| first_attr(entry, attr));
        Ok(ExternalUserProfile {
            subject,
            name,
            proper_name,
            email,
        })
    }

    fn groups_from_entry(&self, entry: &SearchEntry) -> Vec<ExternalGroup> {
        let mut groups = BTreeMap::<String, ExternalGroup>::new();
        for attr in &self.config.group_attributes {
            let Some(values) = attr_values(entry, attr) else {
                continue;
            };
            for value in values {
                for rule in &self.rules {
                    if let Some(captures) = rule.pattern.captures(value) {
                        let name = expand_template(&rule.name, &captures);
                        if name.trim().is_empty() {
                            continue;
                        }
                        if !self.group_filters.is_empty()
                            && !self
                                .group_filters
                                .iter()
                                .any(|filter| filter.is_match(&name))
                        {
                            continue;
                        }
                        let key = rule
                            .key
                            .as_deref()
                            .map(|template| expand_template(template, &captures))
                            .unwrap_or_else(|| value.to_string());
                        let description = rule
                            .description
                            .as_deref()
                            .map(|template| expand_template(template, &captures));
                        groups.entry(key.clone()).or_insert(ExternalGroup {
                            key,
                            name,
                            description,
                        });
                    }
                }
            }
        }
        groups.into_values().collect()
    }

    async fn bind_user(&self, dn: &str, password: &str) -> Result<(), AuthProviderError> {
        if password.is_empty() {
            return Err(AuthProviderError::AuthenticationFailed);
        }
        let mut ldap = self.ldap().await?;
        ldap.with_timeout(self.operation_timeout())
            .simple_bind(dn, password)
            .await
            .map_err(|e| AuthProviderError::Unavailable(e.to_string()))?
            .success()
            .map_err(|_| AuthProviderError::AuthenticationFailed)?;
        Ok(())
    }

    fn subject_is_dn(&self) -> bool {
        self.config.subject_attribute.eq_ignore_ascii_case("dn")
    }
}

impl ExternalIdentityProvider for LdapIdentityProvider {
    fn scope_name(&self) -> &IdentityScopeName {
        &self.scope_name
    }

    async fn authenticate(
        &self,
        username: &str,
        password: &str,
    ) -> Result<AuthenticatedExternalUser, AuthProviderError> {
        let filter = self.user_filter(username);
        let (dn, entry) = self.load_user_by_filter(&filter).await?;
        self.bind_user(&dn, password).await?;
        Ok(AuthenticatedExternalUser {
            profile: self.profile_from_entry(username, &dn, &entry)?,
            groups: self.groups_from_entry(&entry),
        })
    }

    async fn refresh_user(
        &self,
        subject: &str,
    ) -> Result<AuthenticatedExternalUser, AuthProviderError> {
        let filter = if self.subject_is_dn() {
            "(objectClass=*)".to_string()
        } else {
            format!(
                "({}={})",
                self.config.subject_attribute,
                escape_filter_value(subject)
            )
        };
        let (dn, entry) = if self.subject_is_dn() {
            let mut ldap = self.ldap().await?;
            self.bind_service(&mut ldap).await?;
            let attrs = self.search_attributes();
            let (entries, _) = ldap
                .with_timeout(self.operation_timeout())
                .search(subject, Scope::Base, &filter, attrs)
                .await
                .map_err(|e| AuthProviderError::Unavailable(e.to_string()))?
                .success()
                .map_err(|e| AuthProviderError::Protocol(e.to_string()))?;
            if entries.len() != 1 {
                return Err(AuthProviderError::AuthenticationFailed);
            }
            let entry = SearchEntry::construct(entries.into_iter().next().unwrap());
            (entry.dn.clone(), entry)
        } else {
            self.load_user_by_filter(&filter).await?
        };
        let username = first_attr(&entry, &self.config.username_attribute)
            .unwrap_or_else(|| subject.to_string());
        Ok(AuthenticatedExternalUser {
            profile: self.profile_from_entry(&username, &dn, &entry)?,
            groups: self.groups_from_entry(&entry),
        })
    }
}

fn first_attr(entry: &SearchEntry, attr: &str) -> Option<String> {
    attr_values(entry, attr)
        .and_then(|values| values.first())
        .cloned()
}

fn attr_values<'a>(entry: &'a SearchEntry, attr: &str) -> Option<&'a [String]> {
    entry
        .attrs
        .iter()
        .find_map(|(name, values)| name.eq_ignore_ascii_case(attr).then_some(values.as_slice()))
}

fn expand_template(template: &str, captures: &regex::Captures<'_>) -> String {
    let mut output = String::new();
    captures.expand(template, &mut output);
    output
}

fn escape_filter_value(value: &str) -> String {
    let mut escaped = String::new();
    for ch in value.chars() {
        match ch {
            '*' => escaped.push_str("\\2a"),
            '(' => escaped.push_str("\\28"),
            ')' => escaped.push_str("\\29"),
            '\\' => escaped.push_str("\\5c"),
            '\0' => escaped.push_str("\\00"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

fn default_subtree() -> LdapSearchScope {
    LdapSearchScope::Subtree
}

fn default_uid_attr() -> String {
    "uid".to_string()
}

fn default_dn_attr() -> String {
    "dn".to_string()
}

fn default_connect_timeout_seconds() -> u64 {
    5
}

fn default_operation_timeout_seconds() -> u64 {
    10
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn ldap_config(url: &str) -> LdapScopeConfig {
        LdapScopeConfig {
            scope: "directory".into(),
            url: url.into(),
            bind_dn: None,
            bind_password: None,
            connect_timeout_seconds: default_connect_timeout_seconds(),
            operation_timeout_seconds: default_operation_timeout_seconds(),
            user_base_dn: "dc=example,dc=org".into(),
            user_filter: "(uid={username})".into(),
            user_scope: LdapSearchScope::Subtree,
            username_attribute: "uid".into(),
            subject_attribute: "dn".into(),
            display_name_attribute: None,
            email_attribute: None,
            group_attributes: Vec::new(),
            group_filters: Vec::new(),
            group_rules: Vec::new(),
        }
    }

    #[test]
    fn ldap_url_enables_starttls() {
        let provider = LdapIdentityProvider::new(ldap_config("ldap://localhost")).unwrap();
        assert!(provider.starttls);
    }

    #[test]
    fn ldaps_url_uses_implicit_tls() {
        let provider = LdapIdentityProvider::new(ldap_config("ldaps://localhost")).unwrap();
        assert!(!provider.starttls);
    }

    #[test]
    fn non_ldap_url_is_rejected() {
        let error = LdapIdentityProvider::new(ldap_config("http://localhost"))
            .err()
            .unwrap();
        assert!(matches!(&error, AuthProviderError::Config(_)));
    }

    #[test]
    fn ldap_url_without_host_is_rejected() {
        let error = LdapIdentityProvider::new(ldap_config("ldap:directory"))
            .err()
            .unwrap();
        assert!(matches!(&error, AuthProviderError::Config(_)));
    }

    #[test]
    fn bind_dn_without_password_is_rejected_during_construction() {
        let error = LdapIdentityProvider::new(LdapScopeConfig {
            bind_dn: Some("cn=service,dc=example,dc=org".into()),
            ..ldap_config("ldaps://localhost")
        })
        .err()
        .unwrap();

        assert!(matches!(&error, AuthProviderError::Config(_)));
        assert_eq!(
            error.to_string(),
            "provider configuration error: ldap bind_dn and bind_password must be configured together"
        );
    }

    #[test]
    fn bind_password_without_dn_is_rejected_during_construction() {
        let error = LdapIdentityProvider::new(LdapScopeConfig {
            bind_password: Some("secret".into()),
            ..ldap_config("ldaps://localhost")
        })
        .err()
        .unwrap();

        assert!(matches!(&error, AuthProviderError::Config(_)));
        assert_eq!(
            error.to_string(),
            "provider configuration error: ldap bind_dn and bind_password must be configured together"
        );
    }

    #[test]
    fn profile_attributes_are_matched_case_insensitively() {
        let provider = LdapIdentityProvider::new(LdapScopeConfig {
            username_attribute: "uid".into(),
            subject_attribute: "entryUUID".into(),
            display_name_attribute: Some("cn".into()),
            email_attribute: Some("mail".into()),
            ..ldap_config("ldaps://localhost")
        })
        .unwrap();
        let entry = SearchEntry {
            dn: "uid=alice,ou=people,dc=example,dc=org".into(),
            attrs: HashMap::from([
                ("UID".into(), vec!["alice".into()]),
                ("entryuuid".into(), vec!["stable-subject".into()]),
                ("CN".into(), vec!["Alice Example".into()]),
                ("MAIL".into(), vec!["alice@example.org".into()]),
            ]),
            bin_attrs: HashMap::new(),
        };

        let profile = provider
            .profile_from_entry("fallback", &entry.dn, &entry)
            .unwrap();

        assert_eq!(profile.subject, "stable-subject");
        assert_eq!(profile.name, "alice");
        assert_eq!(profile.proper_name.as_deref(), Some("Alice Example"));
        assert_eq!(profile.email.as_deref(), Some("alice@example.org"));
    }

    #[test]
    fn group_mapping_uses_configured_regexes() {
        let provider = LdapIdentityProvider::new(LdapScopeConfig {
            group_attributes: vec!["memberOf".into()],
            group_rules: vec![GroupMappingRuleConfig {
                pattern: "^cn=([^,]+),ou=groups,dc=example,dc=org$".into(),
                name: "$1".into(),
                key: Some("example:$1".into()),
                description: Some("Example group $1".into()),
            }],
            ..ldap_config("ldap://localhost")
        })
        .unwrap();

        let entry = SearchEntry {
            dn: "uid=alice,ou=people,dc=example,dc=org".into(),
            attrs: HashMap::from([(
                "MEMBEROF".into(),
                vec![
                    "cn=admin,ou=groups,dc=example,dc=org".into(),
                    "cn=ignored,ou=other,dc=example,dc=org".into(),
                ],
            )]),
            bin_attrs: HashMap::new(),
        };
        let groups = provider.groups_from_entry(&entry);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].key, "example:admin");
        assert_eq!(groups[0].name, "admin");
    }

    #[test]
    fn group_filters_include_mapped_names_matching_any_configured_regex() {
        let provider = LdapIdentityProvider::new(LdapScopeConfig {
            group_attributes: vec!["memberOf".into()],
            group_filters: vec!["-editors$".into(), "^admin$".into()],
            group_rules: vec![GroupMappingRuleConfig {
                pattern: "^cn=([^,]+),ou=groups,dc=example,dc=org$".into(),
                name: "$1".into(),
                key: None,
                description: None,
            }],
            ..ldap_config("ldap://localhost")
        })
        .unwrap();
        let entry = SearchEntry {
            dn: "uid=alice,ou=people,dc=example,dc=org".into(),
            attrs: HashMap::from([(
                "memberOf".into(),
                vec![
                    "cn=admin,ou=groups,dc=example,dc=org".into(),
                    "cn=hubuum-editors,ou=groups,dc=example,dc=org".into(),
                    "cn=irrelevant,ou=groups,dc=example,dc=org".into(),
                ],
            )]),
            bin_attrs: HashMap::new(),
        };

        let groups = provider.groups_from_entry(&entry);

        assert_eq!(
            groups
                .iter()
                .map(|group| group.name.as_str())
                .collect::<Vec<_>>(),
            vec!["admin", "hubuum-editors"]
        );
    }

    #[test]
    fn invalid_group_filter_is_rejected_during_construction() {
        let error = LdapIdentityProvider::new(LdapScopeConfig {
            group_filters: vec!["[".into()],
            ..ldap_config("ldaps://localhost")
        })
        .err()
        .unwrap();

        assert!(matches!(&error, AuthProviderError::Config(_)));
        assert!(error.to_string().contains("invalid ldap group filter '['"));
    }
}
