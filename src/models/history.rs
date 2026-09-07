use crate::permissions::ClassResourceEndpoint;
use crate::permissions::ResourceRef;

use super::{
    CollectionHistory, ExportTemplateHistory, HubuumClassHistory, HubuumObjectHistory,
    RemoteTargetHistory,
};

/// Common durable provenance columns stored on temporal-history rows.
pub trait TemporalHistoryProvenance {
    fn actor_id(&self) -> Option<i32>;
    fn actor_kind(&self) -> Option<&str>;
    fn initiator_user_id(&self) -> Option<i32>;
    fn task_id(&self) -> Option<i32>;
}

macro_rules! impl_temporal_history_provenance {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl TemporalHistoryProvenance for $ty {
                fn actor_id(&self) -> Option<i32> {
                    self.actor_id
                }

                fn actor_kind(&self) -> Option<&str> {
                    self.actor_kind.as_deref()
                }

                fn initiator_user_id(&self) -> Option<i32> {
                    self.initiator_user_id
                }

                fn task_id(&self) -> Option<i32> {
                    self.task_id
                }
            }
        )+
    };
}

impl_temporal_history_provenance!(
    CollectionHistory,
    ExportTemplateHistory,
    HubuumClassHistory,
    HubuumObjectHistory,
    RemoteTargetHistory,
);

/// The permission-relevant identity of one historical resource version.
///
/// History rows retain attributes such as collection, class, and name after a
/// live resource moves or is renamed. Authorization must therefore use the
/// stored version rather than reconstructing every decision from the current
/// live row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryAuthorizationSnapshot {
    resource: ResourceRef,
}

impl HistoryAuthorizationSnapshot {
    pub fn collection(id: i32, name: String) -> Self {
        Self {
            resource: ResourceRef::named_collection(id, Some(name)),
        }
    }

    pub fn class(id: i32, collection_id: i32, name: String) -> Self {
        Self {
            resource: ResourceRef::class(id, collection_id, Some(name)),
        }
    }

    pub fn object(id: i32, collection_id: i32, class_id: i32, name: String) -> Self {
        Self {
            resource: ResourceRef::object(
                id,
                ClassResourceEndpoint::new(collection_id, class_id),
                Some(name),
            ),
        }
    }

    pub fn template(id: i32, collection_id: i32, name: String) -> Self {
        Self {
            resource: ResourceRef::template(id, collection_id, Some(name)),
        }
    }

    pub fn remote_target(id: i32, collection_id: i32, name: String) -> Self {
        Self {
            resource: ResourceRef::remote_target(id, collection_id, Some(name)),
        }
    }

    pub fn into_resource(self) -> ResourceRef {
        self.resource
    }
}

impl From<&CollectionHistory> for HistoryAuthorizationSnapshot {
    fn from(row: &CollectionHistory) -> Self {
        Self::collection(row.id, row.name.clone())
    }
}

impl From<&HubuumClassHistory> for HistoryAuthorizationSnapshot {
    fn from(row: &HubuumClassHistory) -> Self {
        Self::class(row.id, row.collection_id, row.name.clone())
    }
}

impl From<&HubuumObjectHistory> for HistoryAuthorizationSnapshot {
    fn from(row: &HubuumObjectHistory) -> Self {
        Self::object(
            row.id,
            row.collection_id,
            row.hubuum_class_id,
            row.name.clone(),
        )
    }
}

impl From<&ExportTemplateHistory> for HistoryAuthorizationSnapshot {
    fn from(row: &ExportTemplateHistory) -> Self {
        Self::template(row.id, row.collection_id, row.name.clone())
    }
}

impl From<&RemoteTargetHistory> for HistoryAuthorizationSnapshot {
    fn from(row: &RemoteTargetHistory) -> Self {
        Self::remote_target(row.id, row.collection_id, row.name.clone())
    }
}
