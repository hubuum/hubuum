use std::fmt;

use async_trait::async_trait;
use hubuum_domain::{ClassId, CollectionId, ExportTemplateId};
use hubuum_events_core::EventContext;
use hubuum_query::QueryOptions;
use serde_json::Value;

use crate::{StorageError, StorageMutationOutcome, StoragePage, StorageRecordMetadata};

/// Backend-neutral definition of an export template.
///
/// String discriminants are validated by the application domain before a
/// mutation reaches storage and again while persisted records are converted
/// back into domain values. JSON fields remain opaque to storage adapters.
#[derive(Clone, PartialEq)]
pub struct StorageExportTemplateDefinition {
    description: String,
    content_type: String,
    template: String,
    kind: String,
    scope_kind: Option<String>,
    class_id: Option<ClassId>,
    default_query: Option<String>,
    include: Option<Value>,
    relation_context: Option<Value>,
    default_missing_data_policy: Option<String>,
    default_limits: Option<Value>,
}

pub struct StorageExportTemplateDefinitionParts {
    description: String,
    content_type: String,
    template: String,
    kind: String,
    scope_kind: Option<String>,
    class_id: Option<ClassId>,
    default_query: Option<String>,
    include: Option<Value>,
    relation_context: Option<Value>,
    default_missing_data_policy: Option<String>,
    default_limits: Option<Value>,
}

impl StorageExportTemplateDefinitionParts {
    #[must_use]
    pub fn description(&self) -> &str {
        &self.description
    }
    #[must_use]
    pub fn content_type(&self) -> &str {
        &self.content_type
    }
    #[must_use]
    pub fn template(&self) -> &str {
        &self.template
    }
    #[must_use]
    pub fn kind(&self) -> &str {
        &self.kind
    }
    #[must_use]
    pub fn scope_kind(&self) -> Option<&str> {
        self.scope_kind.as_deref()
    }
    #[must_use]
    pub const fn class_id(&self) -> Option<ClassId> {
        self.class_id
    }
    #[must_use]
    pub fn default_query(&self) -> Option<&str> {
        self.default_query.as_deref()
    }
    #[must_use]
    pub const fn include(&self) -> Option<&Value> {
        self.include.as_ref()
    }
    #[must_use]
    pub const fn relation_context(&self) -> Option<&Value> {
        self.relation_context.as_ref()
    }
    #[must_use]
    pub fn default_missing_data_policy(&self) -> Option<&str> {
        self.default_missing_data_policy.as_deref()
    }
    #[must_use]
    pub const fn default_limits(&self) -> Option<&Value> {
        self.default_limits.as_ref()
    }
}

impl StorageExportTemplateDefinition {
    #[must_use]
    pub fn new(
        description: impl Into<String>,
        content_type: impl Into<String>,
        template: impl Into<String>,
        kind: impl Into<String>,
    ) -> Self {
        Self {
            description: description.into(),
            content_type: content_type.into(),
            template: template.into(),
            kind: kind.into(),
            scope_kind: None,
            class_id: None,
            default_query: None,
            include: None,
            relation_context: None,
            default_missing_data_policy: None,
            default_limits: None,
        }
    }

    #[must_use]
    pub fn with_scope(mut self, scope_kind: Option<String>, class_id: Option<ClassId>) -> Self {
        self.scope_kind = scope_kind;
        self.class_id = class_id;
        self
    }

    #[must_use]
    pub fn with_default_query(mut self, value: Option<String>) -> Self {
        self.default_query = value;
        self
    }

    #[must_use]
    pub fn with_include(mut self, value: Option<Value>) -> Self {
        self.include = value;
        self
    }

    #[must_use]
    pub fn with_relation_context(mut self, value: Option<Value>) -> Self {
        self.relation_context = value;
        self
    }

    #[must_use]
    pub fn with_default_missing_data_policy(mut self, value: Option<String>) -> Self {
        self.default_missing_data_policy = value;
        self
    }

    #[must_use]
    pub fn with_default_limits(mut self, value: Option<Value>) -> Self {
        self.default_limits = value;
        self
    }

    #[must_use]
    pub fn into_parts(self) -> StorageExportTemplateDefinitionParts {
        StorageExportTemplateDefinitionParts {
            description: self.description,
            content_type: self.content_type,
            template: self.template,
            kind: self.kind,
            scope_kind: self.scope_kind,
            class_id: self.class_id,
            default_query: self.default_query,
            include: self.include,
            relation_context: self.relation_context,
            default_missing_data_policy: self.default_missing_data_policy,
            default_limits: self.default_limits,
        }
    }
}

impl fmt::Debug for StorageExportTemplateDefinition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageExportTemplateDefinition")
            .field("content_type", &self.content_type)
            .field("kind", &self.kind)
            .field("scope_kind", &self.scope_kind)
            .field("has_class_binding", &self.class_id.is_some())
            .field("has_default_query", &self.default_query.is_some())
            .field("has_include", &self.include.is_some())
            .field("has_relation_context", &self.relation_context.is_some())
            .field(
                "has_default_missing_data_policy",
                &self.default_missing_data_policy.is_some(),
            )
            .field("has_default_limits", &self.default_limits.is_some())
            .field("content", &"[redacted]")
            .finish()
    }
}

/// Persisted export-template projection returned through the storage boundary.
#[derive(Clone, PartialEq)]
pub struct StorageExportTemplate {
    metadata: StorageRecordMetadata,
    collection_id: CollectionId,
    name: String,
    definition: StorageExportTemplateDefinition,
}

impl StorageExportTemplate {
    #[must_use]
    pub fn new(
        metadata: StorageRecordMetadata,
        collection_id: CollectionId,
        name: impl Into<String>,
        definition: StorageExportTemplateDefinition,
    ) -> Self {
        Self {
            metadata,
            collection_id,
            name: name.into(),
            definition,
        }
    }

    #[must_use]
    pub const fn metadata(&self) -> StorageRecordMetadata {
        self.metadata
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        StorageRecordMetadata,
        CollectionId,
        String,
        StorageExportTemplateDefinition,
    ) {
        (
            self.metadata,
            self.collection_id,
            self.name,
            self.definition,
        )
    }
}

impl fmt::Debug for StorageExportTemplate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageExportTemplate")
            .field("revision", &self.metadata.revision())
            .field("collection_id", &"[redacted]")
            .field("definition", &self.definition)
            .field("name", &"[redacted]")
            .finish()
    }
}

/// List request for either collection-scoped visibility or unscoped candidates.
#[derive(Clone, PartialEq)]
pub struct StorageExportTemplateListQuery {
    collection_ids: Option<Vec<CollectionId>>,
    options: QueryOptions,
}

impl StorageExportTemplateListQuery {
    #[must_use]
    pub const fn within_collections(
        collection_ids: Vec<CollectionId>,
        options: QueryOptions,
    ) -> Self {
        Self {
            collection_ids: Some(collection_ids),
            options,
        }
    }

    #[must_use]
    pub const fn candidates(options: QueryOptions) -> Self {
        Self {
            collection_ids: None,
            options,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (Option<Vec<CollectionId>>, QueryOptions) {
        (self.collection_ids, self.options)
    }
}

impl fmt::Debug for StorageExportTemplateListQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageExportTemplateListQuery")
            .field(
                "collection_count",
                &self.collection_ids.as_ref().map(Vec::len),
            )
            .field("filter_count", &self.options.filters().len())
            .field("sort_count", &self.options.sort().len())
            .field("limit", &self.options.limit())
            .field("has_cursor", &self.options.cursor().is_some())
            .field("include_total", &self.options.include_total())
            .finish()
    }
}

/// Atomic create command including mandatory audit provenance.
#[derive(Clone, PartialEq)]
pub struct StorageExportTemplateCreate {
    collection_id: CollectionId,
    name: String,
    definition: StorageExportTemplateDefinition,
    event_context: EventContext,
}

impl StorageExportTemplateCreate {
    #[must_use]
    pub fn new(
        collection_id: CollectionId,
        name: impl Into<String>,
        definition: StorageExportTemplateDefinition,
        event_context: EventContext,
    ) -> Self {
        Self {
            collection_id,
            name: name.into(),
            definition,
            event_context,
        }
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        CollectionId,
        String,
        StorageExportTemplateDefinition,
        EventContext,
    ) {
        (
            self.collection_id,
            self.name,
            self.definition,
            self.event_context,
        )
    }
}

impl fmt::Debug for StorageExportTemplateCreate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageExportTemplateCreate")
            .field("collection_id", &"[redacted]")
            .field("definition", &self.definition)
            .field("name", &"[redacted]")
            .field("event_context", &"[redacted]")
            .finish()
    }
}

/// Atomic full replacement used after the application resolves and validates a PATCH.
#[derive(Clone, PartialEq)]
pub struct StorageExportTemplateReplace {
    template_id: ExportTemplateId,
    collection_id: CollectionId,
    name: String,
    definition: StorageExportTemplateDefinition,
    event_context: EventContext,
}

impl StorageExportTemplateReplace {
    #[must_use]
    pub fn new(
        template_id: ExportTemplateId,
        collection_id: CollectionId,
        name: impl Into<String>,
        definition: StorageExportTemplateDefinition,
        event_context: EventContext,
    ) -> Self {
        Self {
            template_id,
            collection_id,
            name: name.into(),
            definition,
            event_context,
        }
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        ExportTemplateId,
        CollectionId,
        String,
        StorageExportTemplateDefinition,
        EventContext,
    ) {
        (
            self.template_id,
            self.collection_id,
            self.name,
            self.definition,
            self.event_context,
        )
    }
}

impl fmt::Debug for StorageExportTemplateReplace {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageExportTemplateReplace")
            .field("template_id", &"[redacted]")
            .field("collection_id", &"[redacted]")
            .field("definition", &self.definition)
            .field("name", &"[redacted]")
            .field("event_context", &"[redacted]")
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct StorageExportTemplateDelete {
    template_id: ExportTemplateId,
    event_context: EventContext,
}

impl StorageExportTemplateDelete {
    #[must_use]
    pub const fn new(template_id: ExportTemplateId, event_context: EventContext) -> Self {
        Self {
            template_id,
            event_context,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (ExportTemplateId, EventContext) {
        (self.template_id, self.event_context)
    }
}

impl fmt::Debug for StorageExportTemplateDelete {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageExportTemplateDelete")
            .field("template_id", &"[redacted]")
            .field("event_context", &"[redacted]")
            .finish()
    }
}

/// Complete export-template lifecycle required from every selectable backend.
#[async_trait]
pub trait ExportTemplateStorage: Send + Sync {
    async fn get_export_template(
        &self,
        template_id: ExportTemplateId,
    ) -> Result<StorageExportTemplate, StorageError>;

    async fn list_export_templates(
        &self,
        query: StorageExportTemplateListQuery,
    ) -> Result<StoragePage<StorageExportTemplate>, StorageError>;

    async fn list_export_templates_in_collection(
        &self,
        collection_id: CollectionId,
        exclude_template_id: Option<ExportTemplateId>,
    ) -> Result<Vec<StorageExportTemplate>, StorageError>;

    async fn create_export_template(
        &self,
        request: StorageExportTemplateCreate,
    ) -> Result<StorageMutationOutcome<StorageExportTemplate>, StorageError>;

    async fn replace_export_template(
        &self,
        request: StorageExportTemplateReplace,
    ) -> Result<StorageMutationOutcome<StorageExportTemplate>, StorageError>;

    async fn delete_export_template(
        &self,
        request: StorageExportTemplateDelete,
    ) -> Result<StorageMutationOutcome<()>, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_output_redacts_template_content() {
        let definition = StorageExportTemplateDefinition::new(
            "secret description",
            "text/plain",
            "secret template body",
            "fragment",
        )
        .with_default_query(Some("secret query".to_string()))
        .with_include(Some(serde_json::json!({"secret": true})));
        let request = StorageExportTemplateCreate::new(
            CollectionId::new(7).unwrap(),
            "secret template name",
            definition,
            EventContext::user(hubuum_domain::PrincipalId::new(3).unwrap(), None, None),
        );

        let debug = format!("{request:?}");

        for secret in [
            "secret description",
            "secret template body",
            "secret template name",
            "secret query",
        ] {
            assert!(!debug.contains(secret));
        }
    }
}
