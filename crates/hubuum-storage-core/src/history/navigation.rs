use chrono::{DateTime, Utc};
use hubuum_domain::{ClassId, CollectionId, HistoryRecordId, ObjectId};
use hubuum_query::{FilterField, QueryOptions};

use super::{
    StorageClassHistoryRecord, StorageHistoryCollectionScope, StorageHistoryMetadata,
    StorageObjectHistoryRecord,
};
use crate::StorageValidationError;

/// History identities are scoped by both table and resource, including the object's class.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageHistoryResource {
    Class(ClassId),
    Object {
        class_id: ClassId,
        object_id: ObjectId,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageHistoryEntryQuery {
    resource: StorageHistoryResource,
    history_id: HistoryRecordId,
}

impl StorageHistoryEntryQuery {
    pub const fn new(resource: StorageHistoryResource, history_id: HistoryRecordId) -> Self {
        Self {
            resource,
            history_id,
        }
    }

    pub const fn resource(&self) -> StorageHistoryResource {
        self.resource
    }
    pub const fn history_id(&self) -> HistoryRecordId {
        self.history_id
    }
}

#[derive(Clone)]
pub enum StorageHistoryEntry {
    Class(StorageClassHistoryRecord),
    Object(StorageObjectHistoryRecord),
}

/// A timestamp window over entry creation times: [from, until).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StorageHistoryWindow {
    from: Option<DateTime<Utc>>,
    until: Option<DateTime<Utc>>,
}

impl StorageHistoryWindow {
    pub fn try_new(
        from: Option<DateTime<Utc>>,
        until: Option<DateTime<Utc>>,
    ) -> Result<Self, StorageValidationError> {
        if matches!((from, until), (Some(from), Some(until)) if from >= until) {
            return Err(StorageValidationError::invalid(
                "history 'from' must precede 'until'",
            ));
        }
        Ok(Self { from, until })
    }
    pub const fn from(&self) -> Option<DateTime<Utc>> {
        self.from
    }
    pub const fn until(&self) -> Option<DateTime<Utc>> {
        self.until
    }
    pub fn contains(&self, at: DateTime<Utc>) -> bool {
        self.from.is_none_or(|from| at >= from) && self.until.is_none_or(|until| at < until)
    }
}

/// An exact entry position preserves sub-millisecond precision and timestamp ties.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageHistoryPosition {
    at: DateTime<Utc>,
    history_id: HistoryRecordId,
}

impl StorageHistoryPosition {
    pub const fn new(at: DateTime<Utc>, history_id: HistoryRecordId) -> Self {
        Self { at, history_id }
    }
    pub const fn at(&self) -> DateTime<Utc> {
        self.at
    }
    pub const fn history_id(&self) -> HistoryRecordId {
        self.history_id
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StorageHistorySelection {
    #[default]
    All,
    Entry(HistoryRecordId),
    AtOrBefore(DateTime<Utc>),
    After(DateTime<Utc>),
    BeforeEntry(StorageHistoryPosition),
    AfterEntry(StorageHistoryPosition),
}

impl StorageHistorySelection {
    pub fn contains(self, position: StorageHistoryPosition) -> bool {
        let key = (position.at, position.history_id.id());
        match self {
            Self::All => true,
            Self::Entry(id) => position.history_id == id,
            Self::AtOrBefore(at) => position.at <= at,
            Self::After(at) => position.at > at,
            Self::BeforeEntry(anchor) => key < (anchor.at, anchor.history_id.id()),
            Self::AfterEntry(anchor) => key > (anchor.at, anchor.history_id.id()),
        }
    }
}

/// A bounded, validated metadata query. Adapters must project without JSON snapshots.
#[derive(Clone)]
pub struct StorageHistoryNavigationQuery {
    resource: StorageHistoryResource,
    options: QueryOptions,
    scope: StorageHistoryCollectionScope,
    window: StorageHistoryWindow,
    selection: StorageHistorySelection,
}

impl StorageHistoryNavigationQuery {
    pub fn try_new(
        resource: StorageHistoryResource,
        options: QueryOptions,
        scope: StorageHistoryCollectionScope,
    ) -> Result<Self, StorageValidationError> {
        let sorts = options.sort();
        if !options.filters().is_empty()
            || options.structured_filter().is_some()
            || options.limit().is_none()
            || sorts.len() != 2
            || sorts[0].field != FilterField::ValidFrom
            || sorts[1].field != FilterField::HistoryId
            || sorts[0].descending != sorts[1].descending
        {
            return Err(StorageValidationError::invalid(
                "history navigation requires a bounded page ordered by valid_from and history_id in the same direction, without generic filters",
            ));
        }
        Ok(Self {
            resource,
            options,
            scope,
            window: StorageHistoryWindow::default(),
            selection: StorageHistorySelection::All,
        })
    }
    pub fn window(mut self, window: StorageHistoryWindow) -> Self {
        self.window = window;
        self
    }
    pub fn selection(mut self, selection: StorageHistorySelection) -> Self {
        self.selection = selection;
        self
    }
    pub const fn resource(&self) -> StorageHistoryResource {
        self.resource
    }
    pub const fn options(&self) -> &QueryOptions {
        &self.options
    }
    pub const fn scope(&self) -> &StorageHistoryCollectionScope {
        &self.scope
    }
    pub const fn time_window(&self) -> StorageHistoryWindow {
        self.window
    }
    pub const fn selected(&self) -> StorageHistorySelection {
        self.selection
    }
}

/// Lightweight navigation data plus the historical attributes needed for authorization.
#[derive(Clone)]
pub struct StorageHistoryMarker {
    resource: StorageHistoryResource,
    collection_id: CollectionId,
    name: String,
    metadata: StorageHistoryMetadata,
}

impl StorageHistoryMarker {
    pub fn new(
        resource: StorageHistoryResource,
        collection_id: CollectionId,
        name: String,
        metadata: StorageHistoryMetadata,
    ) -> Self {
        Self {
            resource,
            collection_id,
            name,
            metadata,
        }
    }
    pub fn into_parts(
        self,
    ) -> (
        StorageHistoryResource,
        CollectionId,
        String,
        StorageHistoryMetadata,
    ) {
        (self.resource, self.collection_id, self.name, self.metadata)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hubuum_query::SortParam;
    use rstest::rstest;

    #[rstest]
    #[case::equal(0)]
    #[case::reversed(-1)]
    fn history_window_rejects_empty_or_reversed_bounds(#[case] micros: i64) {
        let at = DateTime::parse_from_rfc3339("2026-01-01T00:00:00.123456Z")
            .unwrap()
            .to_utc();
        assert!(
            StorageHistoryWindow::try_new(
                Some(at),
                Some(at + chrono::Duration::microseconds(micros))
            )
            .is_err()
        );
    }

    #[rstest]
    #[case::before(7, true, false)]
    #[case::same(8, false, false)]
    #[case::after(9, false, true)]
    fn history_position_resolves_timestamp_ties(
        #[case] id: i64,
        #[case] before: bool,
        #[case] after: bool,
    ) {
        let at = DateTime::parse_from_rfc3339("2026-01-01T00:00:00.123456Z")
            .unwrap()
            .to_utc();
        let anchor = StorageHistoryPosition::new(at, HistoryRecordId::new(8).unwrap());
        let position = StorageHistoryPosition::new(at, HistoryRecordId::new(id).unwrap());
        assert_eq!(
            StorageHistorySelection::BeforeEntry(anchor).contains(position),
            before
        );
        assert_eq!(
            StorageHistorySelection::AfterEntry(anchor).contains(position),
            after
        );
    }

    #[rstest]
    #[case::unbounded(None, vec![SortParam::new(FilterField::ValidFrom, true), SortParam::new(FilterField::HistoryId, true)])]
    #[case::unstable(Some(10), vec![SortParam::new(FilterField::ValidFrom, true)])]
    #[case::mixed(Some(10), vec![SortParam::new(FilterField::ValidFrom, true), SortParam::new(FilterField::HistoryId, false)])]
    fn history_navigation_rejects_invalid_page_shapes(
        #[case] limit: Option<usize>,
        #[case] sorts: Vec<SortParam>,
    ) {
        let options = QueryOptions::new(vec![], sorts, limit, None, false).unwrap();
        assert!(
            StorageHistoryNavigationQuery::try_new(
                StorageHistoryResource::Class(ClassId::new(1).unwrap()),
                options,
                StorageHistoryCollectionScope::All
            )
            .is_err()
        );
    }
}
