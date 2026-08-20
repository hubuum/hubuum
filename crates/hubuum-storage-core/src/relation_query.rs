use std::fmt;

use async_trait::async_trait;
use chrono::NaiveDateTime;
use hubuum_domain::{
    ClassId, ClassRelationId, CollectionId, ObjectId, ObjectRelationId, ResourceId,
    ResourceRevision,
};
use hubuum_query::QueryOptions;
use serde_json::Value;

use crate::{StorageError, StoragePage, StorageRecordMetadata, StorageVisibility};

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
    #[must_use]
    pub fn new(
        metadata: StorageRecordMetadata,
        from_class_id: ClassId,
        to_class_id: ClassId,
    ) -> Self {
        Self {
            metadata,
            from_class_id,
            to_class_id,
            forward_template_alias: None,
            reverse_template_alias: None,
            from_max_relations: None,
            to_max_relations: None,
        }
    }

    #[must_use]
    pub fn with_template_aliases(
        mut self,
        forward: Option<String>,
        reverse: Option<String>,
    ) -> Self {
        self.forward_template_alias = forward;
        self.reverse_template_alias = reverse;
        self
    }

    #[must_use]
    pub const fn with_relation_limits(mut self, from: Option<i32>, to: Option<i32>) -> Self {
        self.from_max_relations = from;
        self.to_max_relations = to;
        self
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
        NaiveDateTime,
        NaiveDateTime,
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
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageObjectRelation {
    metadata: StorageRecordMetadata,
    from_object_id: ObjectId,
    to_object_id: ObjectId,
    class_relation_id: ClassRelationId,
}

impl StorageObjectRelation {
    #[must_use]
    pub const fn new(
        metadata: StorageRecordMetadata,
        from_object_id: ObjectId,
        to_object_id: ObjectId,
        class_relation_id: ClassRelationId,
    ) -> Self {
        Self {
            metadata,
            from_object_id,
            to_object_id,
            class_relation_id,
        }
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
        NaiveDateTime,
        NaiveDateTime,
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
}

#[derive(Clone, Debug, PartialEq)]
pub struct StorageGraphClass {
    resource: StorageGraphResource,
    json_schema: Option<Value>,
    validate_schema: bool,
}

impl StorageGraphClass {
    #[must_use]
    pub fn new(
        resource: StorageGraphResource,
        json_schema: Option<Value>,
        validate_schema: bool,
    ) -> Self {
        Self {
            resource,
            json_schema,
            validate_schema,
        }
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
        NaiveDateTime,
        NaiveDateTime,
        ResourceRevision,
    ) {
        let (metadata, name, collection_id, description) = self.resource.into_parts();
        let (id, created_at, updated_at, revision) = metadata.into_parts();
        (
            ClassId::from(id),
            name,
            collection_id,
            self.json_schema,
            self.validate_schema,
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
        NaiveDateTime,
        NaiveDateTime,
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
    #[must_use]
    pub fn new(
        ancestor: StorageGraphClass,
        descendant: StorageGraphClass,
        depth: i32,
        path: Vec<ClassId>,
    ) -> Self {
        Self {
            ancestor,
            descendant,
            depth,
            path,
        }
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
    #[must_use]
    pub fn new(
        ancestor: StorageGraphObject,
        descendant: StorageGraphObject,
        depth: i32,
        path: Vec<ObjectId>,
    ) -> Self {
        Self {
            ancestor,
            descendant,
            depth,
            path,
        }
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
    #[must_use]
    pub const fn new(root_object_id: ObjectId, row: StorageObjectGraphRow) -> Self {
        Self {
            root_object_id,
            row,
        }
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
    #[must_use]
    pub fn new(
        root_object_id: ObjectId,
        descendant: StorageGraphObject,
        depth: i32,
        path: Vec<ObjectId>,
    ) -> Self {
        Self {
            root_object_id,
            descendant,
            depth,
            path,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (ObjectId, StorageGraphObject, i32, Vec<ObjectId>) {
        (self.root_object_id, self.descendant, self.depth, self.path)
    }
}

/// Relation-query page retained as a domain-specific API name.
#[derive(Clone, PartialEq)]
pub struct RelationListQuery {
    options: QueryOptions,
    visibility: StorageVisibility,
}

impl RelationListQuery {
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

impl fmt::Debug for RelationListQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        query_debug(
            formatter,
            "RelationListQuery",
            &self.options,
            &self.visibility,
        )
    }
}

#[derive(Clone, PartialEq)]
pub struct RelationTouchingQuery {
    anchor_id: ResourceId,
    options: QueryOptions,
    visibility: StorageVisibility,
}

impl RelationTouchingQuery {
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

impl fmt::Debug for RelationTouchingQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        query_debug(
            formatter,
            "RelationTouchingQuery",
            &self.options,
            &self.visibility,
        )
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct RelationIdsQuery {
    ids: Vec<ResourceId>,
    visibility: StorageVisibility,
}

impl RelationIdsQuery {
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

impl fmt::Debug for RelationIdsQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RelationIdsQuery")
            .field("id_count", &self.ids.len())
            .field("visibility", &self.visibility)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ObjectRelationsTouchingIdsQuery {
    object_ids: Vec<ObjectId>,
    excluded_relation_ids: Vec<ObjectRelationId>,
    max_results: usize,
    visibility: StorageVisibility,
}

impl ObjectRelationsTouchingIdsQuery {
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

impl fmt::Debug for ObjectRelationsTouchingIdsQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ObjectRelationsTouchingIdsQuery")
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
pub struct RelationGraphQuery {
    root_id: ResourceId,
    options: QueryOptions,
    visibility: StorageVisibility,
}

impl RelationGraphQuery {
    #[must_use]
    pub const fn new(
        root_id: ResourceId,
        options: QueryOptions,
        visibility: StorageVisibility,
    ) -> Self {
        Self {
            root_id,
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
        (self.root_id, self.options, self.visibility)
    }
}

impl fmt::Debug for RelationGraphQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        query_debug(
            formatter,
            "RelationGraphQuery",
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
pub struct RelatedObjectsForRootsQuery {
    root_object_ids: Vec<ObjectId>,
    class_id: ClassId,
    class_relation_id: Option<ClassRelationId>,
    direction: StorageRelatedDirection,
    sort: StorageRelatedSort,
    max_depth: i32,
    limit: i32,
    preserve_alternative_paths: bool,
    visibility: StorageVisibility,
}

impl RelatedObjectsForRootsQuery {
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
            max_depth: 1,
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
    pub const fn max_depth(mut self, max_depth: i32) -> Self {
        self.max_depth = max_depth;
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
        i32,
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
            self.max_depth,
            self.limit,
            self.preserve_alternative_paths,
            self.visibility,
        )
    }
}

impl fmt::Debug for RelatedObjectsForRootsQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RelatedObjectsForRootsQuery")
            .field("root_count", &self.root_object_ids.len())
            .field("direction", &self.direction)
            .field("sort", &self.sort)
            .field("max_depth", &self.max_depth)
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
pub struct BidirectionalRelatedObjectsQuery {
    root_object_ids: Vec<ObjectId>,
    max_depth: i32,
    per_root_cap: i32,
    preserve_alternative_paths: bool,
    visibility: StorageVisibility,
}

impl BidirectionalRelatedObjectsQuery {
    #[must_use]
    pub fn new(
        root_object_ids: impl IntoIterator<Item = ObjectId>,
        max_depth: i32,
        per_root_cap: i32,
        preserve_alternative_paths: bool,
        visibility: StorageVisibility,
    ) -> Self {
        Self {
            root_object_ids: root_object_ids.into_iter().collect(),
            max_depth,
            per_root_cap,
            preserve_alternative_paths,
            visibility,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (Vec<ObjectId>, i32, i32, bool, StorageVisibility) {
        (
            self.root_object_ids,
            self.max_depth,
            self.per_root_cap,
            self.preserve_alternative_paths,
            self.visibility,
        )
    }
}

impl fmt::Debug for BidirectionalRelatedObjectsQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BidirectionalRelatedObjectsQuery")
            .field("root_count", &self.root_object_ids.len())
            .field("max_depth", &self.max_depth)
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
        query: RelationListQuery,
    ) -> Result<StoragePage<StorageClassRelation>, StorageError>;

    async fn list_object_relations(
        &self,
        query: RelationListQuery,
    ) -> Result<StoragePage<StorageObjectRelation>, StorageError>;

    async fn list_class_relations_touching(
        &self,
        query: RelationTouchingQuery,
    ) -> Result<StoragePage<StorageClassRelation>, StorageError>;

    async fn list_object_relations_touching(
        &self,
        query: RelationTouchingQuery,
    ) -> Result<StoragePage<StorageObjectRelation>, StorageError>;

    async fn class_relations_touching_ids(
        &self,
        query: RelationIdsQuery,
    ) -> Result<Vec<StorageClassRelation>, StorageError>;

    async fn class_relations_between_ids(
        &self,
        query: RelationIdsQuery,
    ) -> Result<Vec<StorageClassRelation>, StorageError>;

    async fn object_relations_touching_ids(
        &self,
        query: ObjectRelationsTouchingIdsQuery,
    ) -> Result<Vec<StorageObjectRelation>, StorageError>;

    async fn object_relations_between_ids(
        &self,
        query: RelationIdsQuery,
    ) -> Result<Vec<StorageObjectRelation>, StorageError>;

    async fn list_related_classes(
        &self,
        query: RelationGraphQuery,
    ) -> Result<StoragePage<StorageClassGraphRow>, StorageError>;

    async fn list_related_objects(
        &self,
        query: RelationGraphQuery,
    ) -> Result<StoragePage<StorageObjectGraphRow>, StorageError>;

    async fn list_related_objects_for_roots(
        &self,
        query: RelatedObjectsForRootsQuery,
    ) -> Result<Vec<StorageRelatedObjectIncludeRow>, StorageError>;

    async fn list_bidirectionally_related_objects_for_roots(
        &self,
        query: BidirectionalRelatedObjectsQuery,
    ) -> Result<Vec<StorageRelatedObjectForRootRow>, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;

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
            None::<[crate::AuthorizationPermission; 0]>,
            None,
        );
        let debug = format!(
            "{:?}",
            RelationTouchingQuery::new(ResourceId::new(73).unwrap(), options, visibility)
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
            None::<[crate::AuthorizationPermission; 0]>,
            None,
        );
        let query =
            ObjectRelationsTouchingIdsQuery::new([ObjectId::new(73).unwrap()], 20, visibility)
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
