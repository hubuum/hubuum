use std::fmt;

use async_trait::async_trait;
use hubuum_query::QueryOptions;

use crate::{StorageClass, StorageCollection, StorageError, StorageObject, StorageVisibility};

/// A validated list/filter/count request for one catalog resource kind.
///
/// Cursor normalization and public request validation happen in the
/// application layer. The selected backend owns filter execution, visibility
/// pushdown, page selection, and the optional matching-row count.
#[derive(Clone, PartialEq)]
pub struct CatalogListQuery {
    options: QueryOptions,
    visibility: StorageVisibility,
}

impl CatalogListQuery {
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
    pub const fn visibility(&self) -> &StorageVisibility {
        &self.visibility
    }

    #[must_use]
    pub fn into_parts(self) -> (QueryOptions, StorageVisibility) {
        (self.options, self.visibility)
    }
}

impl fmt::Debug for CatalogListQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CatalogListQuery")
            .field("filter_count", &self.options.filters.len())
            .field("sort_count", &self.options.sort.len())
            .field("limit", &self.options.limit)
            .field("has_cursor", &self.options.cursor.is_some())
            .field("include_total", &self.options.include_total)
            .field("visibility", &self.visibility)
            .finish()
    }
}

/// One backend-selected catalog page and its optional exact total.
#[derive(Clone, Debug, PartialEq)]
pub struct CatalogPage<T> {
    rows: Vec<T>,
    total: Option<i64>,
}

impl<T> CatalogPage<T> {
    #[must_use]
    pub const fn new(rows: Vec<T>, total: Option<i64>) -> Self {
        Self { rows, total }
    }

    #[must_use]
    pub fn into_parts(self) -> (Vec<T>, Option<i64>) {
        (self.rows, self.total)
    }
}

/// Mandatory backend contract for ordinary collection, class, and object
/// listing, filtering, cursor paging, and optional exact counts.
#[async_trait]
pub trait CatalogStorage: Send + Sync {
    async fn list_collections(
        &self,
        query: CatalogListQuery,
    ) -> Result<CatalogPage<StorageCollection>, StorageError>;

    async fn list_classes(
        &self,
        query: CatalogListQuery,
    ) -> Result<CatalogPage<StorageClass>, StorageError>;

    async fn list_objects(
        &self,
        query: CatalogListQuery,
    ) -> Result<CatalogPage<StorageObject>, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AuthorizationPermission, StorageVisibility};

    #[test]
    fn debug_output_reports_only_bounded_query_shape() {
        let options = QueryOptions {
            filters: vec![hubuum_query::ParsedQueryParam {
                field: hubuum_query::FilterField::Name,
                operator: hubuum_query::SearchOperator::Equals { is_negated: false },
                value: "secret catalog name".to_string(),
            }],
            sort: Vec::new(),
            limit: Some(20),
            cursor: Some("secret cursor".to_string()),
            include_total: true,
        };
        let visibility = StorageVisibility::new(
            42,
            false,
            Some([AuthorizationPermission::ReadCollection]),
            None,
        );

        let debug = format!("{:?}", CatalogListQuery::new(options, visibility));

        assert!(debug.contains("filter_count: 1"));
        assert!(debug.contains("has_cursor: true"));
        assert!(!debug.contains("secret catalog name"));
        assert!(!debug.contains("secret cursor"));
        assert!(!debug.contains("42"));
    }
}
