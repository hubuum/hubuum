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
    rules: Vec<GroupMappingRule>,
}

impl LdapIdentityProvider {
    pub fn new(config: LdapScopeConfig) -> Result<Self, AuthProviderError> {
        let scope_name = IdentityScopeName::new(config.scope.clone())?;
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
            rules,
        })
    }

    async fn ldap(&self) -> Result<ldap3::Ldap, AuthProviderError> {
        let settings = LdapConnSettings::new().set_conn_timeout(self.connect_timeout());
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
        let subject = if self.config.subject_attribute == "dn" {
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
            let Some(values) = entry.attrs.get(attr) else {
                continue;
            };
            for value in values {
                for rule in &self.rules {
                    if let Some(captures) = rule.pattern.captures(value) {
                        let name = expand_template(&rule.name, &captures);
                        if name.trim().is_empty() {
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
        let filter = if self.config.subject_attribute == "dn" {
            "(objectClass=*)".to_string()
        } else {
            format!(
                "({}={})",
                self.config.subject_attribute,
                escape_filter_value(subject)
            )
        };
        let (dn, entry) = if self.config.subject_attribute == "dn" {
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
    entry
        .attrs
        .get(attr)
        .and_then(|values| values.first())
        .cloned()
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

    #[test]
    fn group_mapping_uses_configured_regexes() {
        let provider = LdapIdentityProvider::new(LdapScopeConfig {
            scope: "directory".into(),
            url: "ldap://localhost".into(),
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
            group_attributes: vec!["memberOf".into()],
            group_rules: vec![GroupMappingRuleConfig {
                pattern: "^cn=([^,]+),ou=groups,dc=example,dc=org$".into(),
                name: "$1".into(),
                key: Some("example:$1".into()),
                description: Some("Example group $1".into()),
            }],
        })
        .unwrap();

        let entry = SearchEntry {
            dn: "uid=alice,ou=people,dc=example,dc=org".into(),
            attrs: BTreeMap::from([(
                "memberOf".into(),
                vec![
                    "cn=admin,ou=groups,dc=example,dc=org".into(),
                    "cn=ignored,ou=other,dc=example,dc=org".into(),
                ],
            )]),
            bin_attrs: BTreeMap::new(),
        };
        let groups = provider.groups_from_entry(&entry);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].key, "example:admin");
        assert_eq!(groups[0].name, "admin");
    }
}
