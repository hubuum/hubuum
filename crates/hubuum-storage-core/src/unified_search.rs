use std::fmt;

use async_trait::async_trait;
use chrono::NaiveDateTime;
use serde_json::Value;

use crate::{AuthorizationPermission, StorageError};

/// Normalized collection, class, and object boundary for a scoped search.
#[derive(Clone, PartialEq, Eq)]
pub struct UnifiedSearchResourceScope {
    collection_ids: Vec<i32>,
    class_ids: Vec<i32>,
    object_ids: Vec<i32>,
}

impl UnifiedSearchResourceScope {
    #[must_use]
    pub fn new(
        collection_ids: impl IntoIterator<Item = i32>,
        class_ids: impl IntoIterator<Item = i32>,
        object_ids: impl IntoIterator<Item = i32>,
    ) -> Self {
        Self {
            collection_ids: normalized_ids(collection_ids),
            class_ids: normalized_ids(class_ids),
            object_ids: normalized_ids(object_ids),
        }
    }

    #[must_use]
    pub fn collection_ids(&self) -> &[i32] {
        &self.collection_ids
    }

    #[must_use]
    pub fn class_ids(&self) -> &[i32] {
        &self.class_ids
    }

    #[must_use]
    pub fn object_ids(&self) -> &[i32] {
        &self.object_ids
    }
}

impl fmt::Debug for UnifiedSearchResourceScope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UnifiedSearchResourceScope")
            .field("collection_count", &self.collection_ids.len())
            .field("class_count", &self.class_ids.len())
            .field("object_count", &self.object_ids.len())
            .finish()
    }
}

fn normalized_ids(ids: impl IntoIterator<Item = i32>) -> Vec<i32> {
    let mut ids = ids.into_iter().collect::<Vec<_>>();
    ids.sort_unstable();
    ids.dedup();
    ids
}

/// Principal and token boundaries which the adapter must apply before paging.
///
/// An absent permission or resource dimension is unrestricted. A present but
/// empty permission dimension denies every permission and therefore fails
/// closed for all search kinds.
#[derive(Clone, PartialEq, Eq)]
pub struct UnifiedSearchVisibility {
    principal_id: i32,
    is_admin: bool,
    permissions: Option<Vec<AuthorizationPermission>>,
    resources: Option<UnifiedSearchResourceScope>,
}

impl UnifiedSearchVisibility {
    #[must_use]
    pub fn new(
        principal_id: i32,
        is_admin: bool,
        permissions: Option<impl IntoIterator<Item = AuthorizationPermission>>,
        resources: Option<UnifiedSearchResourceScope>,
    ) -> Self {
        let permissions = permissions.map(|permissions| {
            let mut permissions = permissions.into_iter().collect::<Vec<_>>();
            permissions.sort_unstable();
            permissions.dedup();
            permissions
        });
        Self {
            principal_id,
            is_admin,
            permissions,
            resources,
        }
    }

    #[must_use]
    pub const fn principal_id(&self) -> i32 {
        self.principal_id
    }

    #[must_use]
    pub const fn is_admin(&self) -> bool {
        self.is_admin
    }

    #[must_use]
    pub fn permissions(&self) -> Option<&[AuthorizationPermission]> {
        self.permissions.as_deref()
    }

    #[must_use]
    pub const fn resources(&self) -> Option<&UnifiedSearchResourceScope> {
        self.resources.as_ref()
    }

    #[must_use]
    pub fn allows_permissions(&self, required: &[AuthorizationPermission]) -> bool {
        self.permissions.as_ref().is_none_or(|allowed| {
            required
                .iter()
                .all(|permission| allowed.contains(permission))
        })
    }
}

impl fmt::Debug for UnifiedSearchVisibility {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UnifiedSearchVisibility")
            .field("principal_id", &"[redacted]")
            .field("is_admin", &self.is_admin)
            .field("permission_count", &self.permissions.as_ref().map(Vec::len))
            .field("resources", &self.resources)
            .finish()
    }
}

/// Backend-neutral decoded cursor for one ranked search kind.
#[derive(Clone, PartialEq, Eq)]
pub struct UnifiedSearchCursor {
    rank: i32,
    normalized_name: String,
    id: i32,
}

impl UnifiedSearchCursor {
    #[must_use]
    pub fn new(rank: i32, normalized_name: impl Into<String>, id: i32) -> Self {
        Self {
            rank,
            normalized_name: normalized_name.into(),
            id,
        }
    }

    #[must_use]
    pub const fn rank(&self) -> i32 {
        self.rank
    }

    #[must_use]
    pub fn normalized_name(&self) -> &str {
        &self.normalized_name
    }

    #[must_use]
    pub const fn id(&self) -> i32 {
        self.id
    }
}

impl fmt::Debug for UnifiedSearchCursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UnifiedSearchCursor")
            .field("rank", &self.rank)
            .field("normalized_name", &"[redacted]")
            .field("id", &"[redacted]")
            .finish()
    }
}

/// One operation-shaped ranked search request.
#[derive(Clone, PartialEq, Eq)]
pub struct UnifiedSearchQuery {
    search_term: String,
    limit: usize,
    search_extended_document: bool,
    cursor: Option<UnifiedSearchCursor>,
    visibility: UnifiedSearchVisibility,
}

impl UnifiedSearchQuery {
    #[must_use]
    pub fn new(
        search_term: impl Into<String>,
        limit: usize,
        visibility: UnifiedSearchVisibility,
    ) -> Self {
        Self {
            search_term: search_term.into(),
            limit,
            search_extended_document: false,
            cursor: None,
            visibility,
        }
    }

    #[must_use]
    pub const fn search_extended_document(mut self, enabled: bool) -> Self {
        self.search_extended_document = enabled;
        self
    }

    #[must_use]
    pub fn cursor(mut self, cursor: Option<UnifiedSearchCursor>) -> Self {
        self.cursor = cursor;
        self
    }

    #[must_use]
    pub fn search_term(&self) -> &str {
        &self.search_term
    }

    #[must_use]
    pub const fn limit(&self) -> usize {
        self.limit
    }

    #[must_use]
    pub const fn searches_extended_document(&self) -> bool {
        self.search_extended_document
    }

    #[must_use]
    pub const fn search_cursor(&self) -> Option<&UnifiedSearchCursor> {
        self.cursor.as_ref()
    }

    #[must_use]
    pub const fn visibility(&self) -> &UnifiedSearchVisibility {
        &self.visibility
    }
}

impl fmt::Debug for UnifiedSearchQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UnifiedSearchQuery")
            .field("search_term", &"[redacted]")
            .field("limit", &self.limit)
            .field("search_extended_document", &self.search_extended_document)
            .field("cursor", &self.cursor)
            .field("visibility", &self.visibility)
            .finish()
    }
}

/// Collection projection returned by unified search.
#[derive(Clone, PartialEq, Eq)]
pub struct UnifiedSearchCollection {
    id: i32,
    name: String,
    description: String,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    parent_collection_id: Option<i32>,
    revision: i64,
}

impl UnifiedSearchCollection {
    #[must_use]
    pub fn new(
        id: i32,
        name: impl Into<String>,
        description: impl Into<String>,
        created_at: NaiveDateTime,
        updated_at: NaiveDateTime,
        parent_collection_id: Option<i32>,
        revision: i64,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            description: description.into(),
            created_at,
            updated_at,
            parent_collection_id,
            revision,
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
        i64,
    ) {
        (
            self.id,
            self.name,
            self.description,
            self.created_at,
            self.updated_at,
            self.parent_collection_id,
            self.revision,
        )
    }
}

/// Class projection returned by unified search, including its collection.
#[derive(Clone, PartialEq)]
pub struct UnifiedSearchClass {
    id: i32,
    name: String,
    collection: UnifiedSearchCollection,
    json_schema: Option<Value>,
    validate_schema: bool,
    description: String,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    revision: i64,
}

impl UnifiedSearchClass {
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: i32,
        name: impl Into<String>,
        collection: UnifiedSearchCollection,
        json_schema: Option<Value>,
        validate_schema: bool,
        description: impl Into<String>,
        created_at: NaiveDateTime,
        updated_at: NaiveDateTime,
        revision: i64,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            collection,
            json_schema,
            validate_schema,
            description: description.into(),
            created_at,
            updated_at,
            revision,
        }
    }

    #[must_use]
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        i32,
        String,
        UnifiedSearchCollection,
        Option<Value>,
        bool,
        String,
        NaiveDateTime,
        NaiveDateTime,
        i64,
    ) {
        (
            self.id,
            self.name,
            self.collection,
            self.json_schema,
            self.validate_schema,
            self.description,
            self.created_at,
            self.updated_at,
            self.revision,
        )
    }
}

/// Object projection returned by unified search.
#[derive(Clone, PartialEq)]
pub struct UnifiedSearchObject {
    id: i32,
    name: String,
    collection_id: i32,
    class_id: i32,
    data: Value,
    description: String,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    revision: i64,
}

impl UnifiedSearchObject {
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: i32,
        name: impl Into<String>,
        collection_id: i32,
        class_id: i32,
        data: Value,
        description: impl Into<String>,
        created_at: NaiveDateTime,
        updated_at: NaiveDateTime,
        revision: i64,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            collection_id,
            class_id,
            data,
            description: description.into(),
            created_at,
            updated_at,
            revision,
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
        i64,
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
            self.revision,
        )
    }
}

/// Mandatory backend contract for the three ranked unified-search projections.
#[async_trait]
pub trait UnifiedSearchStorage: Send + Sync {
    async fn search_unified_collections(
        &self,
        query: UnifiedSearchQuery,
    ) -> Result<Vec<UnifiedSearchCollection>, StorageError>;

    async fn search_unified_classes(
        &self,
        query: UnifiedSearchQuery,
    ) -> Result<Vec<UnifiedSearchClass>, StorageError>;

    async fn search_unified_objects(
        &self,
        query: UnifiedSearchQuery,
    ) -> Result<Vec<UnifiedSearchObject>, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scoped_permissions_fail_closed() {
        let visibility = UnifiedSearchVisibility::new(
            42,
            true,
            Some([AuthorizationPermission::ReadCollection]),
            None,
        );

        assert!(visibility.allows_permissions(&[AuthorizationPermission::ReadCollection]));
        assert!(!visibility.allows_permissions(&[
            AuthorizationPermission::ReadCollection,
            AuthorizationPermission::ReadObject,
        ]));
    }

    #[test]
    fn resource_scope_normalizes_identifiers() {
        let scope = UnifiedSearchResourceScope::new([3, 1, 3], [8, 4, 8], [7, 2, 7]);

        assert_eq!(scope.collection_ids(), &[1, 3]);
        assert_eq!(scope.class_ids(), &[4, 8]);
        assert_eq!(scope.object_ids(), &[2, 7]);
    }

    #[test]
    fn debug_output_redacts_search_and_principal_values() {
        let visibility =
            UnifiedSearchVisibility::new(42, false, None::<[AuthorizationPermission; 0]>, None);
        let query = UnifiedSearchQuery::new("secret asset", 10, visibility)
            .cursor(Some(UnifiedSearchCursor::new(2, "secret asset", 99)));

        let debug = format!("{query:?}");
        assert!(!debug.contains("secret asset"));
        assert!(!debug.contains("42"));
        assert!(!debug.contains("99"));
    }
}
