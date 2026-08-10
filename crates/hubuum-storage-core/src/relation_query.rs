use std::fmt;

use async_trait::async_trait;
use chrono::NaiveDateTime;
use hubuum_query::QueryOptions;
use serde_json::Value;

use crate::{StorageError, StorageVisibility};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageRecordMetadata {
    id: i32,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    revision: i64,
}

impl StorageRecordMetadata {
    #[must_use]
    pub const fn new(
        id: i32,
        created_at: NaiveDateTime,
        updated_at: NaiveDateTime,
        revision: i64,
    ) -> Self {
        Self {
            id,
            created_at,
            updated_at,
            revision,
        }
    }

    #[must_use]
    pub const fn into_parts(self) -> (i32, NaiveDateTime, NaiveDateTime, i64) {
        (self.id, self.created_at, self.updated_at, self.revision)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageClassRelation {
    metadata: StorageRecordMetadata,
    from_class_id: i32,
    to_class_id: i32,
    forward_template_alias: Option<String>,
    reverse_template_alias: Option<String>,
    from_max_relations: Option<i32>,
    to_max_relations: Option<i32>,
}

impl StorageClassRelation {
    #[must_use]
    pub fn new(metadata: StorageRecordMetadata, from_class_id: i32, to_class_id: i32) -> Self {
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
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        i32,
        i32,
        i32,
        Option<String>,
        Option<String>,
        NaiveDateTime,
        NaiveDateTime,
        Option<i32>,
        Option<i32>,
        i64,
    ) {
        let (id, created_at, updated_at, revision) = self.metadata.into_parts();
        (
            id,
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
    from_object_id: i32,
    to_object_id: i32,
    class_relation_id: i32,
}

impl StorageObjectRelation {
    #[must_use]
    pub const fn new(
        metadata: StorageRecordMetadata,
        from_object_id: i32,
        to_object_id: i32,
        class_relation_id: i32,
    ) -> Self {
        Self {
            metadata,
            from_object_id,
            to_object_id,
            class_relation_id,
        }
    }

    #[must_use]
    pub const fn into_parts(self) -> (i32, i32, i32, i32, NaiveDateTime, NaiveDateTime, i64) {
        let (id, created_at, updated_at, revision) = self.metadata.into_parts();
        (
            id,
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
    collection_id: i32,
    description: String,
}

impl StorageGraphResource {
    #[must_use]
    pub fn new(
        metadata: StorageRecordMetadata,
        name: String,
        collection_id: i32,
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
    pub fn into_parts(self) -> (StorageRecordMetadata, String, i32, String) {
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
        i32,
        String,
        i32,
        Option<Value>,
        bool,
        String,
        NaiveDateTime,
        NaiveDateTime,
        i64,
    ) {
        let (metadata, name, collection_id, description) = self.resource.into_parts();
        let (id, created_at, updated_at, revision) = metadata.into_parts();
        (
            id,
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
    class_id: i32,
    data: Value,
}

impl StorageGraphObject {
    #[must_use]
    pub fn new(resource: StorageGraphResource, class_id: i32, data: Value) -> Self {
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
        i32,
        String,
        i32,
        i32,
        String,
        Value,
        NaiveDateTime,
        NaiveDateTime,
        i64,
    ) {
        let (metadata, name, collection_id, description) = self.resource.into_parts();
        let (id, created_at, updated_at, revision) = metadata.into_parts();
        (
            id,
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
    path: Vec<i32>,
}

impl StorageClassGraphRow {
    #[must_use]
    pub fn new(
        ancestor: StorageGraphClass,
        descendant: StorageGraphClass,
        depth: i32,
        path: Vec<i32>,
    ) -> Self {
        Self {
            ancestor,
            descendant,
            depth,
            path,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (StorageGraphClass, StorageGraphClass, i32, Vec<i32>) {
        (self.ancestor, self.descendant, self.depth, self.path)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct StorageObjectGraphRow {
    ancestor: StorageGraphObject,
    descendant: StorageGraphObject,
    depth: i32,
    path: Vec<i32>,
}

impl StorageObjectGraphRow {
    #[must_use]
    pub fn new(
        ancestor: StorageGraphObject,
        descendant: StorageGraphObject,
        depth: i32,
        path: Vec<i32>,
    ) -> Self {
        Self {
            ancestor,
            descendant,
            depth,
            path,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (StorageGraphObject, StorageGraphObject, i32, Vec<i32>) {
        (self.ancestor, self.descendant, self.depth, self.path)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct StorageRelatedObjectIncludeRow {
    root_object_id: i32,
    row: StorageObjectGraphRow,
}

impl StorageRelatedObjectIncludeRow {
    #[must_use]
    pub const fn new(root_object_id: i32, row: StorageObjectGraphRow) -> Self {
        Self {
            root_object_id,
            row,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (i32, StorageObjectGraphRow) {
        (self.root_object_id, self.row)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct StorageRelatedObjectForRootRow {
    root_object_id: i32,
    descendant: StorageGraphObject,
    depth: i32,
    path: Vec<i32>,
}

impl StorageRelatedObjectForRootRow {
    #[must_use]
    pub fn new(
        root_object_id: i32,
        descendant: StorageGraphObject,
        depth: i32,
        path: Vec<i32>,
    ) -> Self {
        Self {
            root_object_id,
            descendant,
            depth,
            path,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (i32, StorageGraphObject, i32, Vec<i32>) {
        (self.root_object_id, self.descendant, self.depth, self.path)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct RelationPage<T> {
    rows: Vec<T>,
    total: Option<i64>,
}

impl<T> RelationPage<T> {
    #[must_use]
    pub const fn new(rows: Vec<T>, total: Option<i64>) -> Self {
        Self { rows, total }
    }

    #[must_use]
    pub fn into_parts(self) -> (Vec<T>, Option<i64>) {
        (self.rows, self.total)
    }
}

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
    anchor_id: i32,
    options: QueryOptions,
    visibility: StorageVisibility,
}

impl RelationTouchingQuery {
    #[must_use]
    pub const fn new(anchor_id: i32, options: QueryOptions, visibility: StorageVisibility) -> Self {
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
    pub fn into_parts(self) -> (i32, QueryOptions, StorageVisibility) {
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
    ids: Vec<i32>,
    visibility: StorageVisibility,
}

impl RelationIdsQuery {
    #[must_use]
    pub fn new(ids: impl IntoIterator<Item = i32>, visibility: StorageVisibility) -> Self {
        let mut ids = ids.into_iter().collect::<Vec<_>>();
        ids.sort_unstable();
        ids.dedup();
        Self { ids, visibility }
    }

    #[must_use]
    pub fn into_parts(self) -> (Vec<i32>, StorageVisibility) {
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
    object_ids: Vec<i32>,
    excluded_relation_ids: Vec<i32>,
    max_results: usize,
    visibility: StorageVisibility,
}

impl ObjectRelationsTouchingIdsQuery {
    #[must_use]
    pub fn new(
        object_ids: impl IntoIterator<Item = i32>,
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
    pub fn excluding_relation_ids(mut self, relation_ids: impl IntoIterator<Item = i32>) -> Self {
        self.excluded_relation_ids = relation_ids.into_iter().collect();
        self.excluded_relation_ids.sort_unstable();
        self.excluded_relation_ids.dedup();
        self
    }

    #[must_use]
    pub fn into_parts(self) -> (Vec<i32>, Vec<i32>, usize, StorageVisibility) {
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
    root_id: i32,
    options: QueryOptions,
    visibility: StorageVisibility,
}

impl RelationGraphQuery {
    #[must_use]
    pub const fn new(root_id: i32, options: QueryOptions, visibility: StorageVisibility) -> Self {
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
    pub fn into_parts(self) -> (i32, QueryOptions, StorageVisibility) {
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
    root_object_ids: Vec<i32>,
    class_id: i32,
    class_relation_id: Option<i32>,
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
        root_object_ids: impl IntoIterator<Item = i32>,
        class_id: i32,
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
    pub const fn class_relation_id(mut self, class_relation_id: Option<i32>) -> Self {
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
        Vec<i32>,
        i32,
        Option<i32>,
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
    root_object_ids: Vec<i32>,
    max_depth: i32,
    per_root_cap: i32,
    preserve_alternative_paths: bool,
    visibility: StorageVisibility,
}

impl BidirectionalRelatedObjectsQuery {
    #[must_use]
    pub fn new(
        root_object_ids: impl IntoIterator<Item = i32>,
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
    pub fn into_parts(self) -> (Vec<i32>, i32, i32, bool, StorageVisibility) {
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
        .field("filter_count", &options.filters.len())
        .field("sort_count", &options.sort.len())
        .field("limit", &options.limit)
        .field("has_cursor", &options.cursor.is_some())
        .field("include_total", &options.include_total)
        .field("visibility", visibility)
        .finish()
}

/// Mandatory backend contract for relation listing and graph traversal.
#[async_trait]
pub trait RelationQueryStorage: Send + Sync {
    async fn list_class_relations(
        &self,
        query: RelationListQuery,
    ) -> Result<RelationPage<StorageClassRelation>, StorageError>;

    async fn list_object_relations(
        &self,
        query: RelationListQuery,
    ) -> Result<RelationPage<StorageObjectRelation>, StorageError>;

    async fn list_class_relations_touching(
        &self,
        query: RelationTouchingQuery,
    ) -> Result<RelationPage<StorageClassRelation>, StorageError>;

    async fn list_object_relations_touching(
        &self,
        query: RelationTouchingQuery,
    ) -> Result<RelationPage<StorageObjectRelation>, StorageError>;

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

    async fn related_classes(
        &self,
        query: RelationGraphQuery,
    ) -> Result<RelationPage<StorageClassGraphRow>, StorageError>;

    async fn related_objects(
        &self,
        query: RelationGraphQuery,
    ) -> Result<RelationPage<StorageObjectGraphRow>, StorageError>;

    async fn related_objects_for_roots(
        &self,
        query: RelatedObjectsForRootsQuery,
    ) -> Result<Vec<StorageRelatedObjectIncludeRow>, StorageError>;

    async fn bidirectionally_related_objects_for_roots(
        &self,
        query: BidirectionalRelatedObjectsQuery,
    ) -> Result<Vec<StorageRelatedObjectForRootRow>, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_debug_redacts_ids_filters_and_cursors() {
        let options = QueryOptions {
            filters: vec![hubuum_query::ParsedQueryParam {
                field: hubuum_query::FilterField::Name,
                operator: hubuum_query::SearchOperator::Equals { is_negated: false },
                value: "secret relation".to_string(),
            }],
            sort: Vec::new(),
            limit: Some(10),
            cursor: Some("secret cursor".to_string()),
            include_total: true,
        };
        let visibility =
            StorageVisibility::new(42, true, None::<[crate::AuthorizationPermission; 0]>, None);
        let debug = format!("{:?}", RelationTouchingQuery::new(73, options, visibility));

        assert!(debug.contains("filter_count: 1"));
        assert!(debug.contains("has_cursor: true"));
        assert!(!debug.contains("secret relation"));
        assert!(!debug.contains("secret cursor"));
        assert!(!debug.contains("42"));
        assert!(!debug.contains("73"));
    }

    #[test]
    fn touching_ids_debug_redacts_endpoint_and_exclusion_ids() {
        let visibility =
            StorageVisibility::new(42, true, None::<[crate::AuthorizationPermission; 0]>, None);
        let query =
            ObjectRelationsTouchingIdsQuery::new([73], 20, visibility).excluding_relation_ids([99]);

        let debug = format!("{query:?}");

        assert!(debug.contains("object_id_count: 1"));
        assert!(debug.contains("excluded_relation_id_count: 1"));
        assert!(debug.contains("max_results: 20"));
        for id in [42, 73, 99] {
            assert!(!debug.contains(&id.to_string()));
        }
    }
}
