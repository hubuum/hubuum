use hubuum_query::TraversalBudget;
use std::fmt;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use hubuum_domain::{
    ClassId, ClassRelationId, CollectionId, ObjectId, ObjectRelationId, ResourceId,
    ResourceRevision, normalize_template_alias,
};
use hubuum_query::QueryOptions;
use serde_json::Value;

use crate::{
    StorageClassSchemaPolicy, StorageError, StoragePage, StorageRecordMetadata,
    StorageValidationError, StorageVisibility,
};

fn validate_graph_path<T>(
    depth: i32,
    path: &[T],
    ancestor: T,
    descendant: T,
) -> Result<(), StorageValidationError>
where
    T: Copy + Eq + std::hash::Hash,
{
    let depth = usize::try_from(depth).map_err(|_| {
        StorageValidationError::invalid("relation graph depth must be greater than zero")
    })?;
    if depth == 0
        || path.len() != depth.saturating_add(1)
        || path.first().copied() != Some(ancestor)
        || path.last().copied() != Some(descendant)
        || path
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>()
            .len()
            != path.len()
    {
        return Err(StorageValidationError::invalid(
            "relation graph depth, endpoints, and path are inconsistent",
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageClassRelation {
    metadata: StorageRecordMetadata,
    from_class_id: ClassId,
    to_class_id: ClassId,
    forward_template_alias: Option<String>,
    reverse_template_alias: Option<String>,
    from_max_relations: Option<i32>,
    to_max_relations: Option<i32>,
}

impl StorageClassRelation {
    pub fn try_new(
        metadata: StorageRecordMetadata,
        from_class_id: ClassId,
        to_class_id: ClassId,
    ) -> Result<Self, StorageValidationError> {
        if from_class_id >= to_class_id {
            return Err(StorageValidationError::invalid(
                "class relation endpoints must be distinct and canonically ordered",
            ));
        }
        Ok(Self {
            metadata,
            from_class_id,
            to_class_id,
            forward_template_alias: None,
            reverse_template_alias: None,
            from_max_relations: None,
            to_max_relations: None,
        })
    }

    pub fn try_with_template_aliases(
        mut self,
        forward: Option<String>,
        reverse: Option<String>,
    ) -> Result<Self, StorageValidationError> {
        for alias in [forward.as_deref(), reverse.as_deref()]
            .into_iter()
            .flatten()
        {
            let normalized = normalize_template_alias(alias)
                .map_err(|error| StorageValidationError::invalid(error.into_message()))?;
            if normalized != alias {
                return Err(StorageValidationError::invalid(
                    "class relation template aliases must use their canonical form",
                ));
            }
        }
        self.forward_template_alias = forward;
        self.reverse_template_alias = reverse;
        Ok(self)
    }

    pub fn try_with_relation_limits(
        mut self,
        from: Option<i32>,
        to: Option<i32>,
    ) -> Result<Self, StorageValidationError> {
        if from.is_some_and(|value| value <= 0) || to.is_some_and(|value| value <= 0) {
            return Err(StorageValidationError::invalid(
                "class relation limits must be greater than zero",
            ));
        }
        self.from_max_relations = from;
        self.to_max_relations = to;
        Ok(self)
    }

    #[must_use]
    pub const fn metadata(&self) -> StorageRecordMetadata {
        self.metadata
    }

    #[must_use]
    pub const fn from_class_id(&self) -> ClassId {
        self.from_class_id
    }

    #[must_use]
    pub const fn to_class_id(&self) -> ClassId {
        self.to_class_id
    }

    #[must_use]
    pub fn forward_template_alias(&self) -> Option<&str> {
        self.forward_template_alias.as_deref()
    }

    #[must_use]
    pub fn reverse_template_alias(&self) -> Option<&str> {
        self.reverse_template_alias.as_deref()
    }

    #[must_use]
    pub const fn from_max_relations(&self) -> Option<i32> {
        self.from_max_relations
    }

    #[must_use]
    pub const fn to_max_relations(&self) -> Option<i32> {
        self.to_max_relations
    }

    #[must_use]
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        ClassRelationId,
        ClassId,
        ClassId,
        Option<String>,
        Option<String>,
        DateTime<Utc>,
        DateTime<Utc>,
        Option<i32>,
        Option<i32>,
        ResourceRevision,
    ) {
        let (id, created_at, updated_at, revision) = self.metadata.into_parts();
        (
            ClassRelationId::from(id),
            self.from_class_id,
            self.to_class_id,
            self.forward_template_alias,
            self.reverse_template_alias,
            created_at,
            updated_at,
            self.from_max_relations,
            self.to_max_relations,
            revision,
        )
    }

    /// Return the canonical class-relation snapshot stored in audit documents.
    #[must_use]
    pub fn audit_snapshot(&self) -> Value {
        serde_json::json!({
            "id": ClassRelationId::from(self.metadata.id()).id(),
            "from_hubuum_class_id": self.from_class_id.id(),
            "to_hubuum_class_id": self.to_class_id.id(),
            "forward_template_alias": self.forward_template_alias,
            "reverse_template_alias": self.reverse_template_alias,
            "from_max_relations": self.from_max_relations,
            "to_max_relations": self.to_max_relations,
            "created_at": self.metadata.created_at().naive_utc(),
            "updated_at": self.metadata.updated_at().naive_utc(),
            "revision": self.metadata.revision().get(),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageObjectRelation {
    metadata: StorageRecordMetadata,
    from_object_id: ObjectId,
    to_object_id: ObjectId,
    class_relation_id: ClassRelationId,
}

impl StorageObjectRelation {
    pub fn try_new(
        metadata: StorageRecordMetadata,
        from_object_id: ObjectId,
        to_object_id: ObjectId,
        class_relation_id: ClassRelationId,
    ) -> Result<Self, StorageValidationError> {
        if from_object_id >= to_object_id {
            return Err(StorageValidationError::invalid(
                "object relation endpoints must be distinct and canonically ordered",
            ));
        }
        Ok(Self {
            metadata,
            from_object_id,
            to_object_id,
            class_relation_id,
        })
    }

    #[must_use]
    pub const fn metadata(&self) -> StorageRecordMetadata {
        self.metadata
    }

    #[must_use]
    pub const fn from_object_id(&self) -> ObjectId {
        self.from_object_id
    }

    #[must_use]
    pub const fn to_object_id(&self) -> ObjectId {
        self.to_object_id
    }

    #[must_use]
    pub const fn class_relation_id(&self) -> ClassRelationId {
        self.class_relation_id
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        ObjectRelationId,
        ObjectId,
        ObjectId,
        ClassRelationId,
        DateTime<Utc>,
        DateTime<Utc>,
        ResourceRevision,
    ) {
        let (id, created_at, updated_at, revision) = self.metadata.into_parts();
        (
            ObjectRelationId::from(id),
            self.from_object_id,
            self.to_object_id,
            self.class_relation_id,
            created_at,
            updated_at,
            revision,
        )
    }

    /// Return the canonical object-relation snapshot stored in audit documents.
    #[must_use]
    pub fn audit_snapshot(&self) -> Value {
        serde_json::json!({
            "id": ObjectRelationId::from(self.metadata.id()).id(),
            "from_hubuum_object_id": self.from_object_id.id(),
            "to_hubuum_object_id": self.to_object_id.id(),
            "class_relation_id": self.class_relation_id.id(),
            "created_at": self.metadata.created_at().naive_utc(),
            "updated_at": self.metadata.updated_at().naive_utc(),
            "revision": self.metadata.revision().get(),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageGraphResource {
    metadata: StorageRecordMetadata,
    name: String,
    collection_id: CollectionId,
    description: String,
}

impl StorageGraphResource {
    #[must_use]
    pub fn new(
        metadata: StorageRecordMetadata,
        name: String,
        collection_id: CollectionId,
        description: String,
    ) -> Self {
        Self {
            metadata,
            name,
            collection_id,
            description,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (StorageRecordMetadata, String, CollectionId, String) {
        (
            self.metadata,
            self.name,
            self.collection_id,
            self.description,
        )
    }

    #[must_use]
    pub const fn metadata(&self) -> StorageRecordMetadata {
        self.metadata
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct StorageGraphClass {
    resource: StorageGraphResource,
    schema_policy: StorageClassSchemaPolicy,
}

impl StorageGraphClass {
    #[must_use]
    pub fn new(resource: StorageGraphResource, schema_policy: StorageClassSchemaPolicy) -> Self {
        Self {
            resource,
            schema_policy,
        }
    }

    #[must_use]
    pub fn id(&self) -> ClassId {
        ClassId::from(self.resource.metadata().id())
    }

    #[must_use]
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        ClassId,
        String,
        CollectionId,
        Option<Value>,
        bool,
        String,
        DateTime<Utc>,
        DateTime<Utc>,
        ResourceRevision,
    ) {
        let (metadata, name, collection_id, description) = self.resource.into_parts();
        let (id, created_at, updated_at, revision) = metadata.into_parts();
        let (json_schema, validate_schema) = self.schema_policy.into_parts();
        (
            ClassId::from(id),
            name,
            collection_id,
            json_schema,
            validate_schema,
            description,
            created_at,
            updated_at,
            revision,
        )
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct StorageGraphObject {
    resource: StorageGraphResource,
    class_id: ClassId,
    data: Value,
}

impl StorageGraphObject {
    #[must_use]
    pub fn new(resource: StorageGraphResource, class_id: ClassId, data: Value) -> Self {
        Self {
            resource,
            class_id,
            data,
        }
    }

    #[must_use]
    pub fn id(&self) -> ObjectId {
        ObjectId::from(self.resource.metadata().id())
    }

    #[must_use]
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        ObjectId,
        String,
        CollectionId,
        ClassId,
        String,
        Value,
        DateTime<Utc>,
        DateTime<Utc>,
        ResourceRevision,
    ) {
        let (metadata, name, collection_id, description) = self.resource.into_parts();
        let (id, created_at, updated_at, revision) = metadata.into_parts();
        (
            ObjectId::from(id),
            name,
            collection_id,
            self.class_id,
            description,
            self.data,
            created_at,
            updated_at,
            revision,
        )
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct StorageClassGraphRow {
    ancestor: StorageGraphClass,
    descendant: StorageGraphClass,
    depth: i32,
    path: Vec<ClassId>,
}

impl StorageClassGraphRow {
    pub fn try_new(
        ancestor: StorageGraphClass,
        descendant: StorageGraphClass,
        depth: i32,
        path: Vec<ClassId>,
    ) -> Result<Self, StorageValidationError> {
        validate_graph_path(depth, &path, ancestor.id(), descendant.id())?;
        Ok(Self {
            ancestor,
            descendant,
            depth,
            path,
        })
    }

    #[must_use]
    pub fn into_parts(self) -> (StorageGraphClass, StorageGraphClass, i32, Vec<ClassId>) {
        (self.ancestor, self.descendant, self.depth, self.path)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct StorageObjectGraphRow {
    ancestor: StorageGraphObject,
    descendant: StorageGraphObject,
    depth: i32,
    path: Vec<ObjectId>,
}

impl StorageObjectGraphRow {
    pub fn try_new(
        ancestor: StorageGraphObject,
        descendant: StorageGraphObject,
        depth: i32,
        path: Vec<ObjectId>,
    ) -> Result<Self, StorageValidationError> {
        validate_graph_path(depth, &path, ancestor.id(), descendant.id())?;
        Ok(Self {
            ancestor,
            descendant,
            depth,
            path,
        })
    }

    #[must_use]
    pub fn into_parts(self) -> (StorageGraphObject, StorageGraphObject, i32, Vec<ObjectId>) {
        (self.ancestor, self.descendant, self.depth, self.path)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct StorageRelatedObjectIncludeRow {
    root_object_id: ObjectId,
    row: StorageObjectGraphRow,
}

impl StorageRelatedObjectIncludeRow {
    pub fn try_new(
        root_object_id: ObjectId,
        row: StorageObjectGraphRow,
    ) -> Result<Self, StorageValidationError> {
        if row.ancestor.id() != root_object_id {
            return Err(StorageValidationError::invalid(
                "related-object include root must match the graph ancestor",
            ));
        }
        Ok(Self {
            root_object_id,
            row,
        })
    }

    #[must_use]
    pub fn into_parts(self) -> (ObjectId, StorageObjectGraphRow) {
        (self.root_object_id, self.row)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct StorageRelatedObjectForRootRow {
    root_object_id: ObjectId,
    descendant: StorageGraphObject,
    depth: i32,
    path: Vec<ObjectId>,
}

impl StorageRelatedObjectForRootRow {
    pub fn try_new(
        root_object_id: ObjectId,
        descendant: StorageGraphObject,
        depth: i32,
        path: Vec<ObjectId>,
    ) -> Result<Self, StorageValidationError> {
        validate_graph_path(depth, &path, root_object_id, descendant.id())?;
        Ok(Self {
            root_object_id,
            descendant,
            depth,
            path,
        })
    }

    #[must_use]
    pub fn into_parts(self) -> (ObjectId, StorageGraphObject, i32, Vec<ObjectId>) {
        (self.root_object_id, self.descendant, self.depth, self.path)
    }
}

/// Relation-query page retained as a domain-specific API name.
#[derive(Clone, PartialEq)]
pub struct StorageRelationListQuery {
    options: QueryOptions,
    visibility: StorageVisibility,
}

impl StorageRelationListQuery {
    #[must_use]
    pub const fn new(options: QueryOptions, visibility: StorageVisibility) -> Self {
        Self {
            options,
            visibility,
        }
    }

    #[must_use]
    pub const fn options(&self) -> &QueryOptions {
        &self.options
    }

    #[must_use]
    pub fn into_parts(self) -> (QueryOptions, StorageVisibility) {
        (self.options, self.visibility)
    }
}

impl fmt::Debug for StorageRelationListQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        query_debug(
            formatter,
            "StorageRelationListQuery",
            &self.options,
            &self.visibility,
        )
    }
}

#[derive(Clone, PartialEq)]
pub struct StorageRelationTouchingQuery {
    anchor_id: ResourceId,
    options: QueryOptions,
    visibility: StorageVisibility,
}

impl StorageRelationTouchingQuery {
    #[must_use]
    pub const fn new(
        anchor_id: ResourceId,
        options: QueryOptions,
        visibility: StorageVisibility,
    ) -> Self {
        Self {
            anchor_id,
            options,
            visibility,
        }
    }

    #[must_use]
    pub const fn options(&self) -> &QueryOptions {
        &self.options
    }

    #[must_use]
    pub fn into_parts(self) -> (ResourceId, QueryOptions, StorageVisibility) {
        (self.anchor_id, self.options, self.visibility)
    }
}

impl fmt::Debug for StorageRelationTouchingQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        query_debug(
            formatter,
            "StorageRelationTouchingQuery",
            &self.options,
            &self.visibility,
        )
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct StorageRelationIdsQuery {
    ids: Vec<ResourceId>,
    visibility: StorageVisibility,
}

impl StorageRelationIdsQuery {
    #[must_use]
    pub fn new(ids: impl IntoIterator<Item = ResourceId>, visibility: StorageVisibility) -> Self {
        let mut ids = ids.into_iter().collect::<Vec<_>>();
        ids.sort_unstable();
        ids.dedup();
        Self { ids, visibility }
    }

    #[must_use]
    pub fn into_parts(self) -> (Vec<ResourceId>, StorageVisibility) {
        (self.ids, self.visibility)
    }
}

impl fmt::Debug for StorageRelationIdsQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageRelationIdsQuery")
            .field("id_count", &self.ids.len())
            .field("visibility", &self.visibility)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct StorageObjectRelationsTouchingIdsQuery {
    object_ids: Vec<ObjectId>,
    excluded_relation_ids: Vec<ObjectRelationId>,
    max_results: usize,
    visibility: StorageVisibility,
}

impl StorageObjectRelationsTouchingIdsQuery {
    #[must_use]
    pub fn new(
        object_ids: impl IntoIterator<Item = ObjectId>,
        max_results: usize,
        visibility: StorageVisibility,
    ) -> Self {
        let mut object_ids = object_ids.into_iter().collect::<Vec<_>>();
        object_ids.sort_unstable();
        object_ids.dedup();
        Self {
            object_ids,
            excluded_relation_ids: Vec::new(),
            max_results,
            visibility,
        }
    }

    #[must_use]
    pub fn excluding_relation_ids(
        mut self,
        relation_ids: impl IntoIterator<Item = ObjectRelationId>,
    ) -> Self {
        self.excluded_relation_ids = relation_ids.into_iter().collect();
        self.excluded_relation_ids.sort_unstable();
        self.excluded_relation_ids.dedup();
        self
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        Vec<ObjectId>,
        Vec<ObjectRelationId>,
        usize,
        StorageVisibility,
    ) {
        (
            self.object_ids,
            self.excluded_relation_ids,
            self.max_results,
            self.visibility,
        )
    }
}

impl fmt::Debug for StorageObjectRelationsTouchingIdsQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageObjectRelationsTouchingIdsQuery")
            .field("object_id_count", &self.object_ids.len())
            .field(
                "excluded_relation_id_count",
                &self.excluded_relation_ids.len(),
            )
            .field("max_results", &self.max_results)
            .field("visibility", &self.visibility)
            .finish()
    }
}

#[derive(Clone, PartialEq)]
pub struct StorageRelationGraphQuery {
    root_id: ResourceId,
    traversal: TraversalBudget,
    options: QueryOptions,
    visibility: StorageVisibility,
}

impl StorageRelationGraphQuery {
    #[must_use]
    pub const fn new(
        root_id: ResourceId,
        traversal: TraversalBudget,
        options: QueryOptions,
        visibility: StorageVisibility,
    ) -> Self {
        Self {
            root_id,
            traversal,
            options,
            visibility,
        }
    }

    #[must_use]
    pub const fn options(&self) -> &QueryOptions {
        &self.options
    }

    #[must_use]
    pub fn into_parts(self) -> (ResourceId, TraversalBudget, QueryOptions, StorageVisibility) {
        (self.root_id, self.traversal, self.options, self.visibility)
    }
}

impl fmt::Debug for StorageRelationGraphQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        query_debug(
            formatter,
            "StorageRelationGraphQuery",
            &self.options,
            &self.visibility,
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageRelatedDirection {
    Any,
    Outgoing,
    Incoming,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageRelatedSort {
    Path,
    Name,
    CreatedAt,
}

#[derive(Clone, PartialEq, Eq)]
pub struct StorageRelatedObjectsForRootsQuery {
    root_object_ids: Vec<ObjectId>,
    class_id: ClassId,
    class_relation_id: Option<ClassRelationId>,
    direction: StorageRelatedDirection,
    sort: StorageRelatedSort,
    traversal: TraversalBudget,
    limit: i32,
    preserve_alternative_paths: bool,
    visibility: StorageVisibility,
}

impl StorageRelatedObjectsForRootsQuery {
    #[must_use]
    pub fn new(
        root_object_ids: impl IntoIterator<Item = ObjectId>,
        class_id: ClassId,
        visibility: StorageVisibility,
    ) -> Self {
        Self {
            root_object_ids: root_object_ids.into_iter().collect(),
            class_id,
            class_relation_id: None,
            direction: StorageRelatedDirection::Any,
            sort: StorageRelatedSort::Path,
            traversal: TraversalBudget::new(1, hubuum_query::MAX_TRAVERSAL_WORK_ROWS)
                .expect("default traversal budget is valid"),
            limit: 1,
            preserve_alternative_paths: false,
            visibility,
        }
    }

    #[must_use]
    pub const fn class_relation_id(mut self, class_relation_id: Option<ClassRelationId>) -> Self {
        self.class_relation_id = class_relation_id;
        self
    }

    #[must_use]
    pub const fn direction(mut self, direction: StorageRelatedDirection) -> Self {
        self.direction = direction;
        self
    }

    #[must_use]
    pub const fn sort(mut self, sort: StorageRelatedSort) -> Self {
        self.sort = sort;
        self
    }

    #[must_use]
    pub const fn traversal_budget(mut self, traversal: TraversalBudget) -> Self {
        self.traversal = traversal;
        self
    }

    #[must_use]
    pub const fn limit(mut self, limit: i32) -> Self {
        self.limit = limit;
        self
    }

    #[must_use]
    pub const fn preserve_alternative_paths(mut self, preserve: bool) -> Self {
        self.preserve_alternative_paths = preserve;
        self
    }

    #[must_use]
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        Vec<ObjectId>,
        ClassId,
        Option<ClassRelationId>,
        StorageRelatedDirection,
        StorageRelatedSort,
        TraversalBudget,
        i32,
        bool,
        StorageVisibility,
    ) {
        (
            self.root_object_ids,
            self.class_id,
            self.class_relation_id,
            self.direction,
            self.sort,
            self.traversal,
            self.limit,
            self.preserve_alternative_paths,
            self.visibility,
        )
    }
}

impl fmt::Debug for StorageRelatedObjectsForRootsQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageRelatedObjectsForRootsQuery")
            .field("root_count", &self.root_object_ids.len())
            .field("direction", &self.direction)
            .field("sort", &self.sort)
            .field("traversal", &self.traversal)
            .field("limit", &self.limit)
            .field(
                "preserve_alternative_paths",
                &self.preserve_alternative_paths,
            )
            .field("visibility", &self.visibility)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct StorageBidirectionalRelatedObjectsQuery {
    root_object_ids: Vec<ObjectId>,
    traversal: TraversalBudget,
    per_root_cap: i32,
    preserve_alternative_paths: bool,
    visibility: StorageVisibility,
}

impl StorageBidirectionalRelatedObjectsQuery {
    #[must_use]
    pub fn new(
        root_object_ids: impl IntoIterator<Item = ObjectId>,
        traversal: TraversalBudget,
        per_root_cap: i32,
        preserve_alternative_paths: bool,
        visibility: StorageVisibility,
    ) -> Self {
        Self {
            root_object_ids: root_object_ids.into_iter().collect(),
            traversal,
            per_root_cap,
            preserve_alternative_paths,
            visibility,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (Vec<ObjectId>, TraversalBudget, i32, bool, StorageVisibility) {
        (
            self.root_object_ids,
            self.traversal,
            self.per_root_cap,
            self.preserve_alternative_paths,
            self.visibility,
        )
    }
}

impl fmt::Debug for StorageBidirectionalRelatedObjectsQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageBidirectionalRelatedObjectsQuery")
            .field("root_count", &self.root_object_ids.len())
            .field("traversal", &self.traversal)
            .field("per_root_cap", &self.per_root_cap)
            .field(
                "preserve_alternative_paths",
                &self.preserve_alternative_paths,
            )
            .field("visibility", &self.visibility)
            .finish()
    }
}

fn query_debug(
    formatter: &mut fmt::Formatter<'_>,
    name: &str,
    options: &QueryOptions,
    visibility: &StorageVisibility,
) -> fmt::Result {
    formatter
        .debug_struct(name)
        .field("filter_count", &options.filters().len())
        .field("sort_count", &options.sort().len())
        .field("limit", &options.limit())
        .field("has_cursor", &options.cursor().is_some())
        .field("include_total", &options.include_total())
        .field("visibility", visibility)
        .finish()
}

/// Mandatory backend contract for relation listing and graph traversal.
#[async_trait]
pub trait RelationQueryStorage: Send + Sync {
    async fn list_class_relations(
        &self,
        query: StorageRelationListQuery,
    ) -> Result<StoragePage<StorageClassRelation>, StorageError>;

    async fn list_object_relations(
        &self,
        query: StorageRelationListQuery,
    ) -> Result<StoragePage<StorageObjectRelation>, StorageError>;

    async fn list_class_relations_touching(
        &self,
        query: StorageRelationTouchingQuery,
    ) -> Result<StoragePage<StorageClassRelation>, StorageError>;

    async fn list_object_relations_touching(
        &self,
        query: StorageRelationTouchingQuery,
    ) -> Result<StoragePage<StorageObjectRelation>, StorageError>;

    async fn list_class_relations_touching_ids(
        &self,
        query: StorageRelationIdsQuery,
    ) -> Result<Vec<StorageClassRelation>, StorageError>;

    async fn list_class_relations_between_ids(
        &self,
        query: StorageRelationIdsQuery,
    ) -> Result<Vec<StorageClassRelation>, StorageError>;

    async fn list_object_relations_touching_ids(
        &self,
        query: StorageObjectRelationsTouchingIdsQuery,
    ) -> Result<Vec<StorageObjectRelation>, StorageError>;

    async fn list_object_relations_between_ids(
        &self,
        query: StorageRelationIdsQuery,
    ) -> Result<Vec<StorageObjectRelation>, StorageError>;

    async fn list_related_classes(
        &self,
        query: StorageRelationGraphQuery,
    ) -> Result<StoragePage<StorageClassGraphRow>, StorageError>;

    async fn list_related_objects(
        &self,
        query: StorageRelationGraphQuery,
    ) -> Result<StoragePage<StorageObjectGraphRow>, StorageError>;

    async fn list_related_objects_for_roots(
        &self,
        query: StorageRelatedObjectsForRootsQuery,
    ) -> Result<Vec<StorageRelatedObjectIncludeRow>, StorageError>;

    async fn list_bidirectionally_related_objects_for_roots(
        &self,
        query: StorageBidirectionalRelatedObjectsQuery,
    ) -> Result<Vec<StorageRelatedObjectForRootRow>, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metadata() -> StorageRecordMetadata {
        let now = Utc::now();
        StorageRecordMetadata::try_new(
            ResourceId::new(1).unwrap(),
            now,
            now,
            ResourceRevision::INITIAL,
        )
        .unwrap()
    }

    #[test]
    fn persisted_relations_require_distinct_canonical_endpoints() {
        let class_id = ClassId::new(2).unwrap();
        let object_id = ObjectId::new(3).unwrap();

        assert!(StorageClassRelation::try_new(metadata(), class_id, class_id).is_err());
        assert!(
            StorageObjectRelation::try_new(
                metadata(),
                object_id,
                object_id,
                ClassRelationId::new(1).unwrap(),
            )
            .is_err()
        );
    }

    #[test]
    fn persisted_relation_aliases_must_be_canonical() {
        let relation = StorageClassRelation::try_new(
            metadata(),
            ClassId::new(1).unwrap(),
            ClassId::new(2).unwrap(),
        )
        .unwrap();

        assert!(
            relation
                .try_with_template_aliases(Some("HostName".to_string()), None)
                .is_err()
        );
    }

    #[test]
    fn query_debug_redacts_ids_filters_and_cursors() {
        let options = QueryOptions::new(
            vec![hubuum_query::ParsedQueryParam::from_parts(
                hubuum_query::FilterField::Name,
                hubuum_query::SearchOperator::Equals { is_negated: false },
                "secret relation",
            )],
            Vec::new(),
            Some(10),
            Some("secret cursor".to_string()),
            true,
        )
        .unwrap();
        let visibility = StorageVisibility::new(
            hubuum_domain::PrincipalId::new(42).unwrap(),
            true,
            None::<[crate::StorageAuthorizationPermission; 0]>,
            None,
        );
        let debug = format!(
            "{:?}",
            StorageRelationTouchingQuery::new(ResourceId::new(73).unwrap(), options, visibility)
        );

        assert!(debug.contains("filter_count: 1"));
        assert!(debug.contains("has_cursor: true"));
        assert!(!debug.contains("secret relation"));
        assert!(!debug.contains("secret cursor"));
        assert!(!debug.contains("42"));
        assert!(!debug.contains("73"));
    }

    #[test]
    fn touching_ids_debug_redacts_endpoint_and_exclusion_ids() {
        let visibility = StorageVisibility::new(
            hubuum_domain::PrincipalId::new(42).unwrap(),
            true,
            None::<[crate::StorageAuthorizationPermission; 0]>,
            None,
        );
        let query = StorageObjectRelationsTouchingIdsQuery::new(
            [ObjectId::new(73).unwrap()],
            20,
            visibility,
        )
        .excluding_relation_ids([ObjectRelationId::new(99).unwrap()]);

        let debug = format!("{query:?}");

        assert!(debug.contains("object_id_count: 1"));
        assert!(debug.contains("excluded_relation_id_count: 1"));
        assert!(debug.contains("max_results: 20"));
        for id in [42, 73, 99] {
            assert!(!debug.contains(&id.to_string()));
        }
    }
}
