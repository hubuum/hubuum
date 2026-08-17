use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime, Utc};
use hubuum_query::QueryOptions;
use serde_json::Value;
use std::num::NonZeroI64;

use crate::StorageError;

/// Collection visibility applied by the adapter before counting or paging.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HistoryCollectionScope {
    All,
    Visible(Vec<i32>),
}

/// Backend-neutral request for one resource's temporal history page.
#[derive(Clone)]
pub struct HistoryListQuery {
    entity_id: i32,
    query_options: QueryOptions,
    collection_scope: HistoryCollectionScope,
}

impl HistoryListQuery {
    #[must_use]
    pub const fn new(
        entity_id: i32,
        query_options: QueryOptions,
        collection_scope: HistoryCollectionScope,
    ) -> Self {
        Self {
            entity_id,
            query_options,
            collection_scope,
        }
    }

    #[must_use]
    pub const fn entity_id(&self) -> i32 {
        self.entity_id
    }

    #[must_use]
    pub const fn query_options(&self) -> &QueryOptions {
        &self.query_options
    }

    #[must_use]
    pub const fn collection_scope(&self) -> &HistoryCollectionScope {
        &self.collection_scope
    }

    #[must_use]
    pub fn into_parts(self) -> (i32, QueryOptions, HistoryCollectionScope) {
        (self.entity_id, self.query_options, self.collection_scope)
    }
}

/// Backend-neutral request for one object's temporal history page.
#[derive(Clone)]
pub struct ObjectHistoryListQuery {
    object_id: i32,
    class_id: i32,
    query_options: QueryOptions,
    collection_scope: HistoryCollectionScope,
}

impl ObjectHistoryListQuery {
    #[must_use]
    pub const fn new(
        object_id: i32,
        class_id: i32,
        query_options: QueryOptions,
        collection_scope: HistoryCollectionScope,
    ) -> Self {
        Self {
            object_id,
            class_id,
            query_options,
            collection_scope,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (i32, i32, QueryOptions, HistoryCollectionScope) {
        (
            self.object_id,
            self.class_id,
            self.query_options,
            self.collection_scope,
        )
    }
}

/// Point-in-time lookup shared by non-object history resources.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HistoryAsOfQuery {
    entity_id: i32,
    at: DateTime<Utc>,
}

impl HistoryAsOfQuery {
    #[must_use]
    pub const fn new(entity_id: i32, at: DateTime<Utc>) -> Self {
        Self { entity_id, at }
    }

    #[must_use]
    pub const fn into_parts(self) -> (i32, DateTime<Utc>) {
        (self.entity_id, self.at)
    }
}

/// Point-in-time lookup for an object constrained by its class route.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObjectHistoryAsOfQuery {
    object_id: i32,
    class_id: i32,
    at: DateTime<Utc>,
}

impl ObjectHistoryAsOfQuery {
    #[must_use]
    pub const fn new(object_id: i32, class_id: i32, at: DateTime<Utc>) -> Self {
        Self {
            object_id,
            class_id,
            at,
        }
    }

    #[must_use]
    pub const fn into_parts(self) -> (i32, i32, DateTime<Utc>) {
        (self.object_id, self.class_id, self.at)
    }
}

/// Common temporal and provenance columns for one history row.
#[derive(Clone, PartialEq, Eq)]
pub struct HistoryMetadata {
    operation: String,
    valid_from: DateTime<Utc>,
    valid_to: Option<DateTime<Utc>>,
    actor_id: Option<i32>,
    history_id: i64,
    actor_kind: Option<String>,
    initiator_principal_id: Option<i32>,
    task_id: Option<i32>,
    revision: NonZeroI64,
}

impl HistoryMetadata {
    #[must_use]
    pub fn new(
        operation: impl Into<String>,
        valid_from: DateTime<Utc>,
        valid_to: Option<DateTime<Utc>>,
        history_id: i64,
        revision: NonZeroI64,
    ) -> Self {
        Self {
            operation: operation.into(),
            valid_from,
            valid_to,
            actor_id: None,
            history_id,
            actor_kind: None,
            initiator_principal_id: None,
            task_id: None,
            revision,
        }
    }

    #[must_use]
    pub fn actor(mut self, actor_id: Option<i32>, actor_kind: Option<String>) -> Self {
        self.actor_id = actor_id;
        self.actor_kind = actor_kind;
        self
    }

    #[must_use]
    pub const fn initiator_principal_id(mut self, initiator_principal_id: Option<i32>) -> Self {
        self.initiator_principal_id = initiator_principal_id;
        self
    }

    #[must_use]
    pub const fn task_id(mut self, task_id: Option<i32>) -> Self {
        self.task_id = task_id;
        self
    }

    #[must_use]
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        String,
        DateTime<Utc>,
        Option<DateTime<Utc>>,
        Option<i32>,
        i64,
        Option<String>,
        Option<i32>,
        Option<i32>,
        i64,
    ) {
        (
            self.operation,
            self.valid_from,
            self.valid_to,
            self.actor_id,
            self.history_id,
            self.actor_kind,
            self.initiator_principal_id,
            self.task_id,
            self.revision.get(),
        )
    }
}

/// One resolved principal name used to enrich history provenance.
#[derive(Clone, PartialEq, Eq)]
pub struct HistoryPrincipalName {
    principal_id: i32,
    name: String,
}

impl HistoryPrincipalName {
    #[must_use]
    pub fn new(principal_id: i32, name: impl Into<String>) -> Self {
        Self {
            principal_id,
            name: name.into(),
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (i32, String) {
        (self.principal_id, self.name)
    }
}

/// Backend-neutral page with the total computed under the same visibility.
#[derive(Clone, PartialEq, Eq)]
pub struct HistoryPage<T> {
    items: Vec<T>,
    total_count: i64,
}

impl<T> HistoryPage<T> {
    #[must_use]
    pub const fn new(items: Vec<T>, total_count: i64) -> Self {
        Self { items, total_count }
    }

    #[must_use]
    pub fn into_parts(self) -> (Vec<T>, i64) {
        (self.items, self.total_count)
    }
}

/// One collection revision returned by [`HistoryStorage`].
#[derive(Clone, PartialEq, Eq)]
pub struct CollectionHistoryRecord {
    id: i32,
    name: String,
    description: String,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    parent_collection_id: Option<i32>,
    metadata: HistoryMetadata,
}

impl CollectionHistoryRecord {
    #[must_use]
    pub fn new(
        id: i32,
        name: String,
        description: String,
        created_at: NaiveDateTime,
        updated_at: NaiveDateTime,
        parent_collection_id: Option<i32>,
        metadata: HistoryMetadata,
    ) -> Self {
        Self {
            id,
            name,
            description,
            created_at,
            updated_at,
            parent_collection_id,
            metadata,
        }
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        i32,
        String,
        String,
        NaiveDateTime,
        NaiveDateTime,
        Option<i32>,
        HistoryMetadata,
    ) {
        (
            self.id,
            self.name,
            self.description,
            self.created_at,
            self.updated_at,
            self.parent_collection_id,
            self.metadata,
        )
    }
}

/// One class revision returned by [`HistoryStorage`].
#[derive(Clone, PartialEq, Eq)]
pub struct ClassHistoryRecord {
    id: i32,
    name: String,
    collection_id: i32,
    json_schema: Option<Value>,
    validate_schema: bool,
    description: String,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    metadata: HistoryMetadata,
}

impl ClassHistoryRecord {
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: i32,
        name: String,
        collection_id: i32,
        json_schema: Option<Value>,
        validate_schema: bool,
        description: String,
        created_at: NaiveDateTime,
        updated_at: NaiveDateTime,
        metadata: HistoryMetadata,
    ) -> Self {
        Self {
            id,
            name,
            collection_id,
            json_schema,
            validate_schema,
            description,
            created_at,
            updated_at,
            metadata,
        }
    }

    #[must_use]
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        i32,
        String,
        i32,
        Option<Value>,
        bool,
        String,
        NaiveDateTime,
        NaiveDateTime,
        HistoryMetadata,
    ) {
        (
            self.id,
            self.name,
            self.collection_id,
            self.json_schema,
            self.validate_schema,
            self.description,
            self.created_at,
            self.updated_at,
            self.metadata,
        )
    }
}

/// One object revision returned by [`HistoryStorage`].
#[derive(Clone, PartialEq, Eq)]
pub struct ObjectHistoryRecord {
    id: i32,
    name: String,
    collection_id: i32,
    class_id: i32,
    data: Value,
    description: String,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    metadata: HistoryMetadata,
}

impl ObjectHistoryRecord {
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: i32,
        name: String,
        collection_id: i32,
        class_id: i32,
        data: Value,
        description: String,
        created_at: NaiveDateTime,
        updated_at: NaiveDateTime,
        metadata: HistoryMetadata,
    ) -> Self {
        Self {
            id,
            name,
            collection_id,
            class_id,
            data,
            description,
            created_at,
            updated_at,
            metadata,
        }
    }

    #[must_use]
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        i32,
        String,
        i32,
        i32,
        Value,
        String,
        NaiveDateTime,
        NaiveDateTime,
        HistoryMetadata,
    ) {
        (
            self.id,
            self.name,
            self.collection_id,
            self.class_id,
            self.data,
            self.description,
            self.created_at,
            self.updated_at,
            self.metadata,
        )
    }
}

/// One export-template revision returned by [`HistoryStorage`].
#[derive(Clone, PartialEq, Eq)]
pub struct ExportTemplateHistoryRecord {
    id: i32,
    collection_id: i32,
    name: String,
    description: String,
    content_type: String,
    template: String,
    kind: String,
    scope_kind: Option<String>,
    class_id: Option<i32>,
    default_query: Option<String>,
    include: Option<Value>,
    relation_context: Option<Value>,
    default_missing_data_policy: Option<String>,
    default_limits: Option<Value>,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    metadata: HistoryMetadata,
}

impl ExportTemplateHistoryRecord {
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: i32,
        collection_id: i32,
        name: String,
        description: String,
        content_type: String,
        template: String,
        kind: String,
        scope_kind: Option<String>,
        class_id: Option<i32>,
        default_query: Option<String>,
        include: Option<Value>,
        relation_context: Option<Value>,
        default_missing_data_policy: Option<String>,
        default_limits: Option<Value>,
        created_at: NaiveDateTime,
        updated_at: NaiveDateTime,
        metadata: HistoryMetadata,
    ) -> Self {
        Self {
            id,
            collection_id,
            name,
            description,
            content_type,
            template,
            kind,
            scope_kind,
            class_id,
            default_query,
            include,
            relation_context,
            default_missing_data_policy,
            default_limits,
            created_at,
            updated_at,
            metadata,
        }
    }

    #[must_use]
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        i32,
        i32,
        String,
        String,
        String,
        String,
        String,
        Option<String>,
        Option<i32>,
        Option<String>,
        Option<Value>,
        Option<Value>,
        Option<String>,
        Option<Value>,
        NaiveDateTime,
        NaiveDateTime,
        HistoryMetadata,
    ) {
        (
            self.id,
            self.collection_id,
            self.name,
            self.description,
            self.content_type,
            self.template,
            self.kind,
            self.scope_kind,
            self.class_id,
            self.default_query,
            self.include,
            self.relation_context,
            self.default_missing_data_policy,
            self.default_limits,
            self.created_at,
            self.updated_at,
            self.metadata,
        )
    }
}

/// One remote-target revision returned by [`HistoryStorage`].
///
/// Deliberately does not implement `Debug`: transport templates and
/// authentication configuration may contain secrets.
#[derive(Clone, PartialEq, Eq)]
pub struct RemoteTargetHistoryRecord {
    id: i32,
    collection_id: i32,
    class_id: Option<i32>,
    name: String,
    description: String,
    method: String,
    url_template: String,
    headers_template: Value,
    body_template: Option<String>,
    auth_config: Value,
    allowed_subject_types: Value,
    timeout_ms: i32,
    enabled: bool,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    metadata: HistoryMetadata,
}

impl RemoteTargetHistoryRecord {
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: i32,
        collection_id: i32,
        class_id: Option<i32>,
        name: String,
        description: String,
        method: String,
        url_template: String,
        headers_template: Value,
        body_template: Option<String>,
        auth_config: Value,
        allowed_subject_types: Value,
        timeout_ms: i32,
        enabled: bool,
        created_at: NaiveDateTime,
        updated_at: NaiveDateTime,
        metadata: HistoryMetadata,
    ) -> Self {
        Self {
            id,
            collection_id,
            class_id,
            name,
            description,
            method,
            url_template,
            headers_template,
            body_template,
            auth_config,
            allowed_subject_types,
            timeout_ms,
            enabled,
            created_at,
            updated_at,
            metadata,
        }
    }

    #[must_use]
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        i32,
        i32,
        Option<i32>,
        String,
        String,
        String,
        String,
        Value,
        Option<String>,
        Value,
        Value,
        i32,
        bool,
        NaiveDateTime,
        NaiveDateTime,
        HistoryMetadata,
    ) {
        (
            self.id,
            self.collection_id,
            self.class_id,
            self.name,
            self.description,
            self.method,
            self.url_template,
            self.headers_template,
            self.body_template,
            self.auth_config,
            self.allowed_subject_types,
            self.timeout_ms,
            self.enabled,
            self.created_at,
            self.updated_at,
            self.metadata,
        )
    }
}

/// Complete temporal-history capability required of every selectable backend.
#[async_trait]
pub trait HistoryStorage: Send + Sync {
    async fn resolve_history_principal_names(
        &self,
        principal_ids: Vec<i32>,
    ) -> Result<Vec<HistoryPrincipalName>, StorageError>;

    async fn list_collection_history(
        &self,
        query: HistoryListQuery,
    ) -> Result<HistoryPage<CollectionHistoryRecord>, StorageError>;

    async fn collection_history_as_of(
        &self,
        query: HistoryAsOfQuery,
    ) -> Result<Option<CollectionHistoryRecord>, StorageError>;

    async fn list_class_history(
        &self,
        query: HistoryListQuery,
    ) -> Result<HistoryPage<ClassHistoryRecord>, StorageError>;

    async fn class_history_as_of(
        &self,
        query: HistoryAsOfQuery,
    ) -> Result<Option<ClassHistoryRecord>, StorageError>;

    async fn list_object_history(
        &self,
        query: ObjectHistoryListQuery,
    ) -> Result<HistoryPage<ObjectHistoryRecord>, StorageError>;

    async fn object_history_as_of(
        &self,
        query: ObjectHistoryAsOfQuery,
    ) -> Result<Option<ObjectHistoryRecord>, StorageError>;

    async fn list_export_template_history(
        &self,
        query: HistoryListQuery,
    ) -> Result<HistoryPage<ExportTemplateHistoryRecord>, StorageError>;

    async fn export_template_history_as_of(
        &self,
        query: HistoryAsOfQuery,
    ) -> Result<Option<ExportTemplateHistoryRecord>, StorageError>;

    async fn list_remote_target_history(
        &self,
        query: HistoryListQuery,
    ) -> Result<HistoryPage<RemoteTargetHistoryRecord>, StorageError>;

    async fn remote_target_history_as_of(
        &self,
        query: HistoryAsOfQuery,
    ) -> Result<Option<RemoteTargetHistoryRecord>, StorageError>;
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[test]
    fn history_queries_own_their_visibility_boundary() {
        let query = HistoryListQuery::new(
            17,
            QueryOptions::new(Vec::new(), Vec::new(), Some(25), None, true).unwrap(),
            HistoryCollectionScope::Visible(vec![3, 5]),
        );

        let (entity_id, options, scope) = query.into_parts();
        assert_eq!(entity_id, 17);
        assert_eq!(options.limit(), Some(25));
        assert_eq!(scope, HistoryCollectionScope::Visible(vec![3, 5]));
    }

    #[test]
    fn history_metadata_preserves_complete_provenance() {
        let valid_from = Utc.with_ymd_and_hms(2026, 8, 10, 12, 0, 0).unwrap();
        let metadata = HistoryMetadata::new("U", valid_from, None, 41, NonZeroI64::new(7).unwrap())
            .actor(Some(11), Some("human".to_string()))
            .initiator_principal_id(Some(13))
            .task_id(Some(17));

        assert_eq!(
            metadata.into_parts(),
            (
                "U".to_string(),
                valid_from,
                None,
                Some(11),
                41,
                Some("human".to_string()),
                Some(13),
                Some(17),
                7,
            )
        );
    }
}
