use hubuum_events_core::EventSubscriptionFilter;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::errors::ApiError;
use crate::models::event_subscription::{validate_sink_parts, validate_subscription_parts};
use crate::models::export_template::{
    ExportTemplateImportRef, validate_import_export_template,
    validate_import_export_template_composition,
};
use crate::models::remote_target::validate_target_parts;
use crate::models::{
    EventSinkKind, ExportContentType, ExportInclude, ExportLimits, ExportMissingDataPolicy,
    ExportRelationContext, ExportScopeKind, ExportTemplateKind, Permissions, RemoteAuthConfig,
    RemoteHttpMethod, RemoteTargetSubjectType,
};

pub const CURRENT_IMPORT_VERSION: i32 = 1;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ImportAtomicity {
    Strict,
    BestEffort,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ImportCollisionPolicy {
    Abort,
    Overwrite,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ImportPermissionPolicy {
    Abort,
    Continue,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct ImportMode {
    pub atomicity: Option<ImportAtomicity>,
    pub collision_policy: Option<ImportCollisionPolicy>,
    pub permission_policy: Option<ImportPermissionPolicy>,
}

impl Default for ImportMode {
    fn default() -> Self {
        Self {
            atomicity: Some(ImportAtomicity::Strict),
            collision_policy: Some(ImportCollisionPolicy::Abort),
            permission_policy: Some(ImportPermissionPolicy::Abort),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct CollectionKey {
    pub name: String,
    pub path: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct GroupKey {
    pub identity_scope: Option<String>,
    pub groupname: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct IdentityScopeKey {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct PrincipalKey {
    pub identity_scope: Option<String>,
    pub name: String,
}

impl PrincipalKey {
    pub fn identity_scope_name(&self) -> &str {
        self.identity_scope
            .as_deref()
            .unwrap_or(crate::models::identity::LOCAL_IDENTITY_SCOPE)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct EventSinkKey {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct RestoreTimestamps {
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
}

impl RestoreTimestamps {
    pub fn validate(&self) -> Result<(), ApiError> {
        if self.updated_at < self.created_at {
            return Err(ApiError::BadRequest(
                "Imported updated_at must not be earlier than created_at".to_string(),
            ));
        }
        Ok(())
    }
}

impl GroupKey {
    pub fn identity_scope_name(&self) -> &str {
        self.identity_scope
            .as_deref()
            .unwrap_or(crate::models::identity::LOCAL_IDENTITY_SCOPE)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct ClassKey {
    pub name: String,
    pub collection_ref: Option<String>,
    pub collection_key: Option<CollectionKey>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct ObjectKey {
    pub name: String,
    pub class_ref: Option<String>,
    pub class_key: Option<ClassKey>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct ImportIdentityScopeInput {
    #[serde(rename = "ref")]
    pub ref_: Option<String>,
    pub name: String,
    pub provider_kind: String,
    pub timestamps: Option<RestoreTimestamps>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct ImportGroupInput {
    #[serde(rename = "ref")]
    pub ref_: Option<String>,
    pub groupname: String,
    pub description: String,
    pub identity_scope_ref: Option<String>,
    pub identity_scope_key: Option<IdentityScopeKey>,
    pub managed_by: String,
    pub external_key: Option<String>,
    pub last_sync_attempted_at: Option<chrono::NaiveDateTime>,
    pub last_sync_success_at: Option<chrono::NaiveDateTime>,
    pub timestamps: Option<RestoreTimestamps>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ImportPrincipalSubtype {
    Human {
        password: Option<String>,
        password_hash: Option<String>,
        proper_name: Option<String>,
        email: Option<String>,
        anonymized_at: Option<chrono::NaiveDateTime>,
    },
    ServiceAccount {
        description: String,
        owner_group_ref: Option<String>,
        owner_group_key: Option<GroupKey>,
        created_by_ref: Option<String>,
        created_by_key: Option<PrincipalKey>,
        disabled_at: Option<chrono::NaiveDateTime>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct ImportPrincipalInput {
    #[serde(rename = "ref")]
    pub ref_: Option<String>,
    pub name: String,
    pub identity_scope_ref: Option<String>,
    pub identity_scope_key: Option<IdentityScopeKey>,
    pub provider_managed: bool,
    #[serde(default = "empty_json_object")]
    pub settings: serde_json::Value,
    pub external_subject: Option<String>,
    pub last_sync_attempted_at: Option<chrono::NaiveDateTime>,
    pub last_sync_success_at: Option<chrono::NaiveDateTime>,
    #[serde(flatten)]
    pub subtype: ImportPrincipalSubtype,
    pub timestamps: Option<RestoreTimestamps>,
}

impl ImportPrincipalInput {
    pub fn validate_credentials(&self) -> Result<(), ApiError> {
        let ImportPrincipalSubtype::Human {
            password,
            password_hash,
            ..
        } = &self.subtype
        else {
            return Ok(());
        };
        if password.is_some() && password_hash.is_some() {
            return Err(ApiError::BadRequest(
                "A human principal import accepts password or password_hash, not both".to_string(),
            ));
        }
        if let Some(hash) = password_hash {
            let parsed = argon2::PasswordHash::new(hash).map_err(|_| {
                ApiError::BadRequest(
                    "Imported password_hash must be a valid Argon2 password hash".to_string(),
                )
            })?;
            if !matches!(
                parsed.algorithm.as_str(),
                "argon2d" | "argon2i" | "argon2id"
            ) {
                return Err(ApiError::BadRequest(
                    "Imported password_hash must use an Argon2 algorithm".to_string(),
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct ImportMembershipSourceInput {
    pub source: String,
    pub source_scope_ref: Option<String>,
    pub source_scope_key: Option<IdentityScopeKey>,
    pub source_key: String,
    pub timestamps: Option<RestoreTimestamps>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct ImportGroupMembershipInput {
    #[serde(rename = "ref")]
    pub ref_: Option<String>,
    pub principal_ref: Option<String>,
    pub principal_key: Option<PrincipalKey>,
    pub group_ref: Option<String>,
    pub group_key: Option<GroupKey>,
    #[serde(default)]
    pub sources: Vec<ImportMembershipSourceInput>,
    pub timestamps: Option<RestoreTimestamps>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct ImportCollectionInput {
    #[serde(rename = "ref")]
    pub ref_: Option<String>,
    pub name: String,
    pub description: String,
    pub parent_collection_ref: Option<String>,
    pub parent_collection_key: Option<CollectionKey>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct ImportClassInput {
    #[serde(rename = "ref")]
    pub ref_: Option<String>,
    pub name: String,
    pub description: String,
    pub json_schema: Option<serde_json::Value>,
    pub validate_schema: Option<bool>,
    pub collection_ref: Option<String>,
    pub collection_key: Option<CollectionKey>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct ImportObjectInput {
    #[serde(rename = "ref")]
    pub ref_: Option<String>,
    pub name: String,
    pub description: String,
    pub data: serde_json::Value,
    pub class_ref: Option<String>,
    pub class_key: Option<ClassKey>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct ImportClassRelationInput {
    #[serde(rename = "ref")]
    pub ref_: Option<String>,
    pub from_class_ref: Option<String>,
    pub from_class_key: Option<ClassKey>,
    pub to_class_ref: Option<String>,
    pub to_class_key: Option<ClassKey>,
    pub forward_template_alias: Option<String>,
    pub reverse_template_alias: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct ImportObjectRelationInput {
    #[serde(rename = "ref")]
    pub ref_: Option<String>,
    pub from_object_ref: Option<String>,
    pub from_object_key: Option<ObjectKey>,
    pub to_object_ref: Option<String>,
    pub to_object_key: Option<ObjectKey>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct ImportCollectionPermissionInput {
    #[serde(rename = "ref")]
    pub ref_: Option<String>,
    pub collection_ref: Option<String>,
    pub collection_key: Option<CollectionKey>,
    pub group_key: GroupKey,
    pub permissions: Vec<Permissions>,
    pub replace_existing: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct ImportExportTemplateInput {
    #[serde(rename = "ref")]
    pub ref_: Option<String>,
    pub collection_ref: Option<String>,
    pub collection_key: Option<CollectionKey>,
    pub class_ref: Option<String>,
    pub class_key: Option<ClassKey>,
    pub name: String,
    pub description: String,
    pub content_type: ExportContentType,
    pub template: String,
    pub kind: ExportTemplateKind,
    pub scope_kind: Option<ExportScopeKind>,
    pub default_query: Option<String>,
    pub include: Option<ExportInclude>,
    pub relation_context: Option<ExportRelationContext>,
    pub default_missing_data_policy: Option<ExportMissingDataPolicy>,
    pub default_limits: Option<ExportLimits>,
    pub timestamps: Option<RestoreTimestamps>,
}

impl ImportExportTemplateInput {
    fn template_ref(&self) -> ExportTemplateImportRef<'_> {
        ExportTemplateImportRef {
            name: &self.name,
            template: &self.template,
            content_type: self.content_type,
            kind: self.kind,
            scope_kind: self.scope_kind,
            has_class: self.class_ref.is_some() || self.class_key.is_some(),
            default_query: self.default_query.as_deref(),
            include: self.include.as_ref(),
            relation_context: self.relation_context.as_ref(),
            default_missing_data_policy: self.default_missing_data_policy,
            default_limits: self.default_limits.as_ref(),
        }
    }

    fn validate(&self) -> Result<(), ApiError> {
        validate_import_export_template(self.template_ref())
    }

    pub(crate) fn validate_composition(
        &self,
        collection_templates: &[(String, String)],
    ) -> Result<(), ApiError> {
        validate_import_export_template_composition(self.template_ref(), collection_templates)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct ImportRemoteTargetInput {
    #[serde(rename = "ref")]
    pub ref_: Option<String>,
    pub collection_ref: Option<String>,
    pub collection_key: Option<CollectionKey>,
    pub class_ref: Option<String>,
    pub class_key: Option<ClassKey>,
    pub name: String,
    pub description: String,
    pub method: RemoteHttpMethod,
    pub url_template: String,
    #[serde(default = "empty_json_object")]
    pub headers_template: serde_json::Value,
    pub body_template: Option<String>,
    #[serde(default)]
    pub auth_config: RemoteAuthConfig,
    pub allowed_subject_types: Vec<RemoteTargetSubjectType>,
    pub timeout_ms: i32,
    pub enabled: bool,
    pub timestamps: Option<RestoreTimestamps>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct ImportEventSinkInput {
    #[serde(rename = "ref")]
    pub ref_: Option<String>,
    pub name: String,
    pub kind: EventSinkKind,
    #[serde(default = "empty_json_object")]
    pub config: serde_json::Value,
    pub secret_ref: Option<String>,
    pub enabled: bool,
    pub timestamps: Option<RestoreTimestamps>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct ImportEventSubscriptionInput {
    #[serde(rename = "ref")]
    pub ref_: Option<String>,
    pub collection_ref: Option<String>,
    pub collection_key: Option<CollectionKey>,
    pub sink_ref: Option<String>,
    pub sink_key: Option<EventSinkKey>,
    pub name: String,
    pub description: String,
    pub entity_types: Vec<String>,
    pub actions: Vec<String>,
    #[serde(default = "empty_json_object")]
    pub filter: serde_json::Value,
    #[serde(default = "empty_json_object")]
    pub routing: serde_json::Value,
    pub enabled: bool,
    pub timestamps: Option<RestoreTimestamps>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default, ToSchema)]
pub struct ImportGraph {
    #[serde(default)]
    pub identity_scopes: Vec<ImportIdentityScopeInput>,
    #[serde(default)]
    pub groups: Vec<ImportGroupInput>,
    #[serde(default)]
    pub principals: Vec<ImportPrincipalInput>,
    #[serde(default)]
    pub group_memberships: Vec<ImportGroupMembershipInput>,
    #[serde(default)]
    pub collections: Vec<ImportCollectionInput>,
    #[serde(default)]
    pub classes: Vec<ImportClassInput>,
    #[serde(default)]
    pub objects: Vec<ImportObjectInput>,
    #[serde(default)]
    pub class_relations: Vec<ImportClassRelationInput>,
    #[serde(default)]
    pub object_relations: Vec<ImportObjectRelationInput>,
    #[serde(default)]
    pub collection_permissions: Vec<ImportCollectionPermissionInput>,
    #[serde(default)]
    pub export_templates: Vec<ImportExportTemplateInput>,
    #[serde(default)]
    pub remote_targets: Vec<ImportRemoteTargetInput>,
    #[serde(default)]
    pub event_sinks: Vec<ImportEventSinkInput>,
    #[serde(default)]
    pub event_subscriptions: Vec<ImportEventSubscriptionInput>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct ImportRequest {
    pub version: i32,
    pub dry_run: Option<bool>,
    pub mode: Option<ImportMode>,
    pub graph: ImportGraph,
}

fn validate_required_selector(
    reference_present: bool,
    key_present: bool,
    reference_name: &str,
    key_name: &str,
) -> Result<(), ApiError> {
    if reference_present == key_present {
        return Err(ApiError::BadRequest(format!(
            "Exactly one of {reference_name} or {key_name} must be provided"
        )));
    }
    Ok(())
}

fn validate_optional_selector(
    reference_present: bool,
    key_present: bool,
    reference_name: &str,
    key_name: &str,
) -> Result<(), ApiError> {
    if reference_present && key_present {
        return Err(ApiError::BadRequest(format!(
            "At most one of {reference_name} or {key_name} may be provided"
        )));
    }
    Ok(())
}

fn validate_class_key(key: &ClassKey) -> Result<(), ApiError> {
    validate_required_selector(
        key.collection_ref.is_some(),
        key.collection_key.is_some(),
        "class_key.collection_ref",
        "class_key.collection_key",
    )
}

impl ImportGraph {
    fn validate_extended_selectors(&self) -> Result<(), ApiError> {
        for group in &self.groups {
            validate_required_selector(
                group.identity_scope_ref.is_some(),
                group.identity_scope_key.is_some(),
                "identity_scope_ref",
                "identity_scope_key",
            )?;
        }
        for principal in &self.principals {
            validate_required_selector(
                principal.identity_scope_ref.is_some(),
                principal.identity_scope_key.is_some(),
                "identity_scope_ref",
                "identity_scope_key",
            )?;
            if let ImportPrincipalSubtype::ServiceAccount {
                owner_group_ref,
                owner_group_key,
                created_by_ref,
                created_by_key,
                ..
            } = &principal.subtype
            {
                validate_required_selector(
                    owner_group_ref.is_some(),
                    owner_group_key.is_some(),
                    "owner_group_ref",
                    "owner_group_key",
                )?;
                validate_optional_selector(
                    created_by_ref.is_some(),
                    created_by_key.is_some(),
                    "created_by_ref",
                    "created_by_key",
                )?;
            }
        }
        for membership in &self.group_memberships {
            validate_required_selector(
                membership.principal_ref.is_some(),
                membership.principal_key.is_some(),
                "principal_ref",
                "principal_key",
            )?;
            validate_required_selector(
                membership.group_ref.is_some(),
                membership.group_key.is_some(),
                "group_ref",
                "group_key",
            )?;
            for source in &membership.sources {
                validate_required_selector(
                    source.source_scope_ref.is_some(),
                    source.source_scope_key.is_some(),
                    "source_scope_ref",
                    "source_scope_key",
                )?;
            }
        }
        for template in &self.export_templates {
            validate_required_selector(
                template.collection_ref.is_some(),
                template.collection_key.is_some(),
                "collection_ref",
                "collection_key",
            )?;
            validate_optional_selector(
                template.class_ref.is_some(),
                template.class_key.is_some(),
                "class_ref",
                "class_key",
            )?;
            if let Some(key) = &template.class_key {
                validate_class_key(key)?;
            }
        }
        for target in &self.remote_targets {
            validate_required_selector(
                target.collection_ref.is_some(),
                target.collection_key.is_some(),
                "collection_ref",
                "collection_key",
            )?;
            validate_optional_selector(
                target.class_ref.is_some(),
                target.class_key.is_some(),
                "class_ref",
                "class_key",
            )?;
            if let Some(key) = &target.class_key {
                validate_class_key(key)?;
            }
        }
        for subscription in &self.event_subscriptions {
            validate_required_selector(
                subscription.collection_ref.is_some(),
                subscription.collection_key.is_some(),
                "collection_ref",
                "collection_key",
            )?;
            validate_required_selector(
                subscription.sink_ref.is_some(),
                subscription.sink_key.is_some(),
                "sink_ref",
                "sink_key",
            )?;
        }
        Ok(())
    }

    fn validate_extended_timestamps(&self) -> Result<(), ApiError> {
        let timestamps = self
            .identity_scopes
            .iter()
            .filter_map(|item| item.timestamps.as_ref())
            .chain(
                self.groups
                    .iter()
                    .filter_map(|item| item.timestamps.as_ref()),
            )
            .chain(
                self.principals
                    .iter()
                    .filter_map(|item| item.timestamps.as_ref()),
            )
            .chain(
                self.group_memberships
                    .iter()
                    .filter_map(|item| item.timestamps.as_ref()),
            )
            .chain(
                self.export_templates
                    .iter()
                    .filter_map(|item| item.timestamps.as_ref()),
            )
            .chain(
                self.remote_targets
                    .iter()
                    .filter_map(|item| item.timestamps.as_ref()),
            )
            .chain(
                self.event_sinks
                    .iter()
                    .filter_map(|item| item.timestamps.as_ref()),
            )
            .chain(
                self.event_subscriptions
                    .iter()
                    .filter_map(|item| item.timestamps.as_ref()),
            );
        for timestamps in timestamps {
            timestamps.validate()?;
        }
        for timestamps in self
            .group_memberships
            .iter()
            .flat_map(|membership| &membership.sources)
            .filter_map(|source| source.timestamps.as_ref())
        {
            timestamps.validate()?;
        }
        Ok(())
    }
}

impl ImportRequest {
    pub fn validate(&self) -> Result<(), ApiError> {
        if self.version != CURRENT_IMPORT_VERSION {
            return Err(ApiError::BadRequest(format!(
                "Unsupported import version '{}'; expected {}",
                self.version, CURRENT_IMPORT_VERSION
            )));
        }
        self.graph.validate_extended_selectors()?;
        self.graph.validate_extended_timestamps()?;
        for principal in &self.graph.principals {
            principal.validate_credentials()?;
        }
        for template in &self.graph.export_templates {
            template.validate()?;
        }
        for target in &self.graph.remote_targets {
            validate_target_parts(
                (target.class_ref.is_some() || target.class_key.is_some()).then_some(1),
                &target.url_template,
                &target.headers_template,
                target.body_template.as_deref(),
                &target.auth_config,
                &target.allowed_subject_types,
                target.timeout_ms,
            )?;
        }
        for sink in &self.graph.event_sinks {
            validate_sink_parts(sink.kind, &sink.config, sink.secret_ref.as_deref())?;
        }
        for subscription in &self.graph.event_subscriptions {
            let filter =
                serde_json::from_value::<EventSubscriptionFilter>(subscription.filter.clone())
                    .map_err(|error| {
                        ApiError::BadRequest(format!("Invalid event subscription filter: {error}"))
                    })?;
            validate_subscription_parts(
                &subscription.entity_types,
                &subscription.actions,
                &filter,
                &subscription.routing,
            )?;
        }
        Ok(())
    }

    pub fn total_items(&self) -> i32 {
        (self.graph.collections.len()
            + self.graph.identity_scopes.len()
            + self.graph.groups.len()
            + self.graph.principals.len()
            + self.graph.group_memberships.len()
            + self.graph.classes.len()
            + self.graph.objects.len()
            + self.graph.class_relations.len()
            + self.graph.object_relations.len()
            + self.graph.collection_permissions.len()
            + self.graph.export_templates.len()
            + self.graph.remote_targets.len()
            + self.graph.event_sinks.len()
            + self.graph.event_subscriptions.len()) as i32
    }

    pub fn dry_run(&self) -> bool {
        self.dry_run.unwrap_or(false)
    }

    pub fn mode(&self) -> ImportMode {
        match &self.mode {
            None => ImportMode::default(),
            Some(provided) => {
                let default = ImportMode::default();
                ImportMode {
                    atomicity: provided.atomicity.or(default.atomicity),
                    collision_policy: provided.collision_policy.or(default.collision_policy),
                    permission_policy: provided.permission_policy.or(default.permission_policy),
                }
            }
        }
    }
}

fn empty_json_object() -> serde_json::Value {
    serde_json::json!({})
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::{
        IdentityScopeKey, ImportAtomicity, ImportCollisionPolicy, ImportEventSinkInput,
        ImportExportTemplateInput, ImportGraph, ImportMode, ImportPermissionPolicy,
        ImportPrincipalInput, ImportPrincipalSubtype, ImportRemoteTargetInput, ImportRequest,
        RestoreTimestamps, validate_optional_selector, validate_required_selector,
    };
    use crate::models::{
        CollectionKey, EventSinkKind, ExportContentType, ExportTemplateKind, RemoteAuthConfig,
        RemoteHttpMethod, RemoteTargetSubjectType,
    };

    #[test]
    fn test_import_request_mode_fills_missing_fields_with_defaults() {
        let request = ImportRequest {
            version: 1,
            dry_run: Some(false),
            mode: Some(ImportMode {
                atomicity: Some(ImportAtomicity::BestEffort),
                collision_policy: None,
                permission_policy: None,
            }),
            graph: ImportGraph::default(),
        };

        let mode = request.mode();
        assert_eq!(mode.atomicity, Some(ImportAtomicity::BestEffort));
        assert_eq!(mode.collision_policy, Some(ImportCollisionPolicy::Abort));
        assert_eq!(mode.permission_policy, Some(ImportPermissionPolicy::Abort));
    }

    fn human_principal(
        password: Option<&str>,
        password_hash: Option<&str>,
    ) -> ImportPrincipalInput {
        ImportPrincipalInput {
            ref_: None,
            name: "imported-user".to_string(),
            identity_scope_ref: None,
            identity_scope_key: Some(IdentityScopeKey {
                name: "local".to_string(),
            }),
            provider_managed: false,
            settings: serde_json::json!({}),
            external_subject: None,
            last_sync_attempted_at: None,
            last_sync_success_at: None,
            subtype: ImportPrincipalSubtype::Human {
                password: password.map(str::to_string),
                password_hash: password_hash.map(str::to_string),
                proper_name: None,
                email: None,
                anonymized_at: None,
            },
            timestamps: None,
        }
    }

    #[rstest]
    #[case::plain_password(Some("secret"), None, true)]
    #[case::both(
        Some("secret"),
        Some("$argon2id$v=19$m=1,t=1,p=1$c2FsdA$aGFzaA"),
        false
    )]
    #[case::invalid_hash(None, Some("not-a-password-hash"), false)]
    fn imported_human_credentials_are_mutually_exclusive_and_argon2_only(
        #[case] password: Option<&str>,
        #[case] password_hash: Option<&str>,
        #[case] expected_valid: bool,
    ) {
        assert_eq!(
            human_principal(password, password_hash)
                .validate_credentials()
                .is_ok(),
            expected_valid
        );
    }

    #[rstest]
    #[case::reference(true, false, true)]
    #[case::key(false, true, true)]
    #[case::missing(false, false, false)]
    #[case::ambiguous(true, true, false)]
    fn required_extended_selectors_are_exclusive(
        #[case] reference_present: bool,
        #[case] key_present: bool,
        #[case] expected_valid: bool,
    ) {
        assert_eq!(
            validate_required_selector(reference_present, key_present, "item_ref", "item_key")
                .is_ok(),
            expected_valid
        );
    }

    #[rstest]
    #[case::omitted(false, false, true)]
    #[case::reference(true, false, true)]
    #[case::key(false, true, true)]
    #[case::ambiguous(true, true, false)]
    fn optional_extended_selectors_are_exclusive(
        #[case] reference_present: bool,
        #[case] key_present: bool,
        #[case] expected_valid: bool,
    ) {
        assert_eq!(
            validate_optional_selector(reference_present, key_present, "item_ref", "item_key")
                .is_ok(),
            expected_valid
        );
    }

    #[rstest]
    #[case::same("2026-07-14T10:00:00", "2026-07-14T10:00:00", true)]
    #[case::later("2026-07-14T10:00:00", "2026-07-14T11:00:00", true)]
    #[case::earlier("2026-07-14T10:00:00", "2026-07-14T09:00:00", false)]
    fn imported_timestamps_follow_creation_order(
        #[case] created_at: &str,
        #[case] updated_at: &str,
        #[case] expected_valid: bool,
    ) {
        let timestamps = RestoreTimestamps {
            created_at: created_at.parse().expect("created_at test timestamp"),
            updated_at: updated_at.parse().expect("updated_at test timestamp"),
        };

        assert_eq!(timestamps.validate().is_ok(), expected_valid);
    }

    fn request_with_graph(graph: ImportGraph) -> ImportRequest {
        ImportRequest {
            version: super::CURRENT_IMPORT_VERSION,
            dry_run: Some(false),
            mode: None,
            graph,
        }
    }

    #[test]
    fn import_rejects_malformed_export_templates() {
        let request = request_with_graph(ImportGraph {
            export_templates: vec![ImportExportTemplateInput {
                ref_: None,
                collection_ref: Some("collection:1".to_string()),
                collection_key: None,
                class_ref: None,
                class_key: None,
                name: "broken.txt".to_string(),
                description: "Broken template".to_string(),
                content_type: ExportContentType::TextPlain,
                template: "{% if".to_string(),
                kind: ExportTemplateKind::Fragment,
                scope_kind: None,
                default_query: None,
                include: None,
                relation_context: None,
                default_missing_data_policy: None,
                default_limits: None,
                timestamps: None,
            }],
            ..ImportGraph::default()
        });

        assert!(request.validate().is_err());
    }

    #[test]
    fn import_rejects_ambiguous_extended_selectors() {
        let request = request_with_graph(ImportGraph {
            export_templates: vec![ImportExportTemplateInput {
                ref_: None,
                collection_ref: Some("collection:1".to_string()),
                collection_key: Some(CollectionKey {
                    name: "root".to_string(),
                    path: Some(Vec::new()),
                }),
                class_ref: None,
                class_key: None,
                name: "valid.txt".to_string(),
                description: "Valid template with an ambiguous selector".to_string(),
                content_type: ExportContentType::TextPlain,
                template: "valid".to_string(),
                kind: ExportTemplateKind::Fragment,
                scope_kind: None,
                default_query: None,
                include: None,
                relation_context: None,
                default_missing_data_policy: None,
                default_limits: None,
                timestamps: None,
            }],
            ..ImportGraph::default()
        });

        assert!(request.validate().is_err());
    }

    #[test]
    fn import_rejects_invalid_remote_target_auth() {
        let request = request_with_graph(ImportGraph {
            remote_targets: vec![ImportRemoteTargetInput {
                ref_: None,
                collection_ref: Some("collection:1".to_string()),
                collection_key: None,
                class_ref: None,
                class_key: None,
                name: "invalid-auth".to_string(),
                description: "Invalid auth target".to_string(),
                method: RemoteHttpMethod::Get,
                url_template: "https://example.invalid".to_string(),
                headers_template: serde_json::json!({}),
                body_template: None,
                auth_config: RemoteAuthConfig::BearerSecret {
                    secret: "invalid secret reference".to_string(),
                },
                allowed_subject_types: vec![RemoteTargetSubjectType::Collection],
                timeout_ms: 1_000,
                enabled: true,
                timestamps: None,
            }],
            ..ImportGraph::default()
        });

        assert!(request.validate().is_err());
    }

    #[test]
    fn import_rejects_invalid_event_sink_configuration() {
        let request = request_with_graph(ImportGraph {
            event_sinks: vec![ImportEventSinkInput {
                ref_: None,
                name: "invalid-sink".to_string(),
                kind: EventSinkKind::Webhook,
                config: serde_json::json!([]),
                secret_ref: None,
                enabled: true,
                timestamps: None,
            }],
            ..ImportGraph::default()
        });

        assert!(request.validate().is_err());
    }
}
