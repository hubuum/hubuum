use std::fmt;

use async_trait::async_trait;
use hubuum_query::QueryOptions;

use crate::{
    StorageClass, StorageCollection, StorageError, StorageObject, StoragePage, StorageVisibility,
};

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
            .field("filter_count", &self.options.filters().len())
            .field("sort_count", &self.options.sort().len())
            .field("limit", &self.options.limit())
            .field("has_cursor", &self.options.cursor().is_some())
            .field("include_total", &self.options.include_total())
            .field("visibility", &self.visibility)
            .finish()
    }
}

/// Mandatory backend contract for ordinary collection, class, and object
/// listing, filtering, cursor paging, and optional exact counts.
#[async_trait]
pub trait CatalogStorage: Send + Sync {
    async fn list_collections(
        &self,
        query: CatalogListQuery,
    ) -> Result<StoragePage<StorageCollection>, StorageError>;

    async fn list_classes(
        &self,
        query: CatalogListQuery,
    ) -> Result<StoragePage<StorageClass>, StorageError>;

    async fn list_objects(
        &self,
        query: CatalogListQuery,
    ) -> Result<StoragePage<StorageObject>, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AuthorizationPermission, StorageVisibility};

    #[test]
    fn debug_output_reports_only_bounded_query_shape() {
        let options = QueryOptions::new(
            vec![hubuum_query::ParsedQueryParam::from_parts(
                hubuum_query::FilterField::Name,
                hubuum_query::SearchOperator::Equals { is_negated: false },
                "secret catalog name",
            )],
            Vec::new(),
            Some(20),
            Some("secret cursor".to_string()),
            true,
        )
        .unwrap();
        let visibility = StorageVisibility::new(
            hubuum_domain::PrincipalId::new(42).unwrap(),
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
