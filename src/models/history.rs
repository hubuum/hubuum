use crate::permissions::{ResourceAttrs, ResourceKind, ResourceRef};

use super::{
    CollectionHistory, ExportTemplateHistory, HubuumClassHistory, HubuumObjectHistory,
    RemoteTargetHistory,
};

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
            resource: ResourceRef {
                kind: ResourceKind::Collection,
                id,
                attrs: ResourceAttrs {
                    collection_id: Some(id),
                    name: Some(name),
                    ..Default::default()
                },
            },
        }
    }

    pub fn class(id: i32, collection_id: i32, name: String) -> Self {
        Self {
            resource: ResourceRef {
                kind: ResourceKind::Class,
                id,
                attrs: ResourceAttrs {
                    collection_id: Some(collection_id),
                    name: Some(name),
                    ..Default::default()
                },
            },
        }
    }

    pub fn object(id: i32, collection_id: i32, class_id: i32, name: String) -> Self {
        Self {
            resource: ResourceRef {
                kind: ResourceKind::Object,
                id,
                attrs: ResourceAttrs {
                    collection_id: Some(collection_id),
                    class_id: Some(class_id),
                    name: Some(name),
                    ..Default::default()
                },
            },
        }
    }

    pub fn template(id: i32, collection_id: i32, name: String) -> Self {
        Self {
            resource: ResourceRef {
                kind: ResourceKind::Template,
                id,
                attrs: ResourceAttrs {
                    collection_id: Some(collection_id),
                    name: Some(name),
                    ..Default::default()
                },
            },
        }
    }

    pub fn remote_target(id: i32, collection_id: i32, name: String) -> Self {
        Self {
            resource: ResourceRef {
                kind: ResourceKind::RemoteTarget,
                id,
                attrs: ResourceAttrs {
                    collection_id: Some(collection_id),
                    name: Some(name),
                    ..Default::default()
                },
            },
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
