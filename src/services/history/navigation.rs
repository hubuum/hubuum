use chrono::{DateTime, Utc};
use hubuum_domain::{HistoryRecordId, ResourceId};
use std::future::Future;

use super::access::{DeletedHistoryAccess, HistoryAccess, HistoryIdentity};
use super::{AppHistoryMetadata, class_from_storage, history_total, object_from_storage};
use crate::errors::ApiError;
use crate::models::search::QueryOptions;
use crate::models::{
    HistoricalRelation, HistoryAuthorizationSnapshot, HistoryDirection, HistoryMarker,
    HistoryTimelineParameters, HubuumClassHistory, HubuumObjectHistory, Permissions, TokenScope,
};
use crate::pagination::prepare_db_pagination;
use crate::permissions::AppContext;
use crate::storage::{
    HistoryStorage, StorageHistoryCollectionScope, StorageHistoryEntry, StorageHistoryEntryQuery,
    StorageHistoryListQuery, StorageHistoryMarker, StorageHistoryNavigationQuery,
    StorageHistoryPosition, StorageHistoryResource, StorageHistorySelection, StorageHistoryWindow,
    StorageObjectHistoryListQuery, StorageRelationHistoryQuery, StorageRelationHistoryResource,
    StorageRelationHistorySelection, storage_handle,
};
use crate::traits::{AuthzSubject, CursorPaginated};
use hubuum_storage_core::{
    AuthorizationDataStorage, StorageAuthorizationResource, StorageAuthorizationResourceKey,
    StorageAuthorizationResourcesQuery,
};

enum HistoryVisibility {
    DeletedAdministrator(DeletedHistoryAccess),
    Collections(StorageHistoryCollectionScope),
    Policy,
}

/// Carries the successful live-resource (or deleted-resource administrator) gate.
pub(crate) struct HistoryReader<'a> {
    access: HistoryAccess<'a>,
    resource: StorageHistoryResource,
    permission: Permissions,
    visibility: HistoryVisibility,
}

pub(crate) enum HistoryEntry {
    Class(HubuumClassHistory),
    Object(HubuumObjectHistory),
}

pub(crate) enum HistoryRows {
    Class(Vec<HubuumClassHistory>),
    Object(Vec<HubuumObjectHistory>),
}

fn unavailable() -> ApiError {
    ApiError::NotFound("History entry unavailable".into())
}

impl<'a> HistoryReader<'a> {
    pub(crate) async fn relationships(
        &self,
        selection: StorageRelationHistorySelection,
        options: QueryOptions,
    ) -> Result<(Vec<HistoricalRelation>, i64), ApiError> {
        self.require_retained_root().await?;
        let resource = match self.resource {
            StorageHistoryResource::Class(id) => StorageRelationHistoryResource::Class(id),
            StorageHistoryResource::Object { object_id, .. } => {
                StorageRelationHistoryResource::Object(object_id)
            }
        };
        let query = StorageRelationHistoryQuery::try_new(resource, selection, options)
            .map_err(|error| error.into_request_error())?;
        super::relations::list(&self.access, query).await
    }

    pub(crate) async fn authorize(
        context: &'a AppContext,
        subject: &(impl AuthzSubject + ?Sized),
        scopes: Option<&'a TokenScope>,
        resource: StorageHistoryResource,
    ) -> Result<Self, ApiError> {
        let permission = match resource {
            StorageHistoryResource::Class(_) => Permissions::ReadClass,
            StorageHistoryResource::Object { .. } => Permissions::ReadObject,
        };
        let key = match resource {
            StorageHistoryResource::Class(id) => StorageAuthorizationResourceKey::Class(id),
            StorageHistoryResource::Object { object_id, .. } => {
                StorageAuthorizationResourceKey::Object(object_id)
            }
        };
        let facts = storage_handle(context)
            .load_authorization_resources(StorageAuthorizationResourcesQuery::new([key]))
            .await?
            .into_iter()
            .next();
        let snapshot = match (facts, resource) {
            (
                Some(StorageAuthorizationResource::Class {
                    resource: class,
                    name,
                }),
                StorageHistoryResource::Class(id),
            ) if class.id() == id => Some(HistoryAuthorizationSnapshot::class(
                id.id(),
                class.collection_id().id(),
                name,
            )),
            (
                Some(StorageAuthorizationResource::Object(object)),
                StorageHistoryResource::Object {
                    class_id,
                    object_id,
                },
            ) if object.id() == object_id && object.class_id() == class_id => {
                Some(HistoryAuthorizationSnapshot::object(
                    object_id.id(),
                    object.collection_id().id(),
                    class_id.id(),
                    object.name().to_owned(),
                ))
            }
            (None, _) => None,
            // A mismatched live identity is not a deleted resource. In particular,
            // an administrator must still use the live object's current class.
            _ => return Err(unavailable()),
        };
        let access = HistoryAccess::load(context, subject, scopes).await?;
        let visibility = match access.identity(snapshot) {
            Some(HistoryIdentity::Live(snapshot)) => {
                access
                    .authorize(permission, snapshot.into_resource())
                    .await?;
                if access.uses_storage_visibility() {
                    HistoryVisibility::Collections(access.candidate_collections(permission).await?)
                } else {
                    HistoryVisibility::Policy
                }
            }
            Some(HistoryIdentity::Deleted(proof)) => HistoryVisibility::DeletedAdministrator(proof),
            None => return Err(unavailable()),
        };
        Ok(Self {
            access,
            resource,
            permission,
            visibility,
        })
    }

    /// Authorization and retained-data availability are separate facts. Point
    /// readers must be able to report a missing snapshot after the access gate.
    /// Relationship reads need this check before dropping an object's class ID.
    async fn require_retained_root(&self) -> Result<(), ApiError> {
        if !matches!(self.visibility, HistoryVisibility::DeletedAdministrator(_)) {
            return Ok(());
        }
        let options = QueryOptions::new(
            Vec::new(),
            HistoryDirection::Older.sorts(),
            Some(1),
            None,
            false,
        )?;
        let query = StorageHistoryNavigationQuery::try_new(
            self.resource,
            options,
            StorageHistoryCollectionScope::All,
        )
        .map_err(|error| error.into_request_error())?;
        if storage_handle(self.access.context())
            .list_history_markers(query)
            .await?
            .into_parts()
            .0
            .is_empty()
        {
            return Err(unavailable());
        }
        Ok(())
    }

    async fn page<T, Fetch, FetchFuture, Snapshot>(
        &self,
        options: &QueryOptions,
        fetch: Fetch,
        snapshot: Snapshot,
    ) -> Result<(Vec<T>, i64), ApiError>
    where
        T: CursorPaginated,
        Fetch: Fn(QueryOptions, StorageHistoryCollectionScope) -> FetchFuture,
        FetchFuture: Future<Output = Result<(Vec<T>, Option<i64>), ApiError>>,
        Snapshot: Fn(&T) -> HistoryAuthorizationSnapshot,
    {
        if matches!(self.visibility, HistoryVisibility::Policy) {
            self.access
                .page(
                    self.permission,
                    |options| async {
                        fetch(options, StorageHistoryCollectionScope::All)
                            .await
                            .map(|(rows, _)| rows)
                    },
                    options,
                    snapshot,
                )
                .await
        } else {
            let scope = match &self.visibility {
                HistoryVisibility::Collections(scope) => scope.clone(),
                _ => StorageHistoryCollectionScope::All,
            };
            let (rows, total) = fetch(prepare_db_pagination::<T>(options)?, scope).await?;
            if rows.is_empty() {
                self.require_retained_root().await?;
            }
            Ok((rows, history_total(total, options)?))
        }
    }

    pub(crate) async fn list(
        &self,
        options: &QueryOptions,
    ) -> Result<(HistoryRows, i64), ApiError> {
        let storage = storage_handle(self.access.context());
        match self.resource {
            StorageHistoryResource::Class(id) => {
                let (rows, total) = self
                    .page(
                        options,
                        |options, scope| async {
                            let (rows, total) = storage
                                .list_class_history(StorageHistoryListQuery::new(
                                    ResourceId::new(id.id())?,
                                    options,
                                    scope,
                                ))
                                .await?
                                .into_parts();
                            Ok((
                                rows.into_iter()
                                    .map(class_from_storage)
                                    .collect::<Result<_, _>>()?,
                                total,
                            ))
                        },
                        |row| HistoryAuthorizationSnapshot::from(row),
                    )
                    .await?;
                Ok((HistoryRows::Class(rows), total))
            }
            StorageHistoryResource::Object {
                class_id,
                object_id,
            } => {
                let (rows, total) = self
                    .page(
                        options,
                        |options, scope| async {
                            let (rows, total) = storage
                                .list_object_history(StorageObjectHistoryListQuery::new(
                                    object_id, class_id, options, scope,
                                ))
                                .await?
                                .into_parts();
                            Ok((
                                rows.into_iter()
                                    .map(object_from_storage)
                                    .collect::<Result<_, _>>()?,
                                total,
                            ))
                        },
                        |row| HistoryAuthorizationSnapshot::from(row),
                    )
                    .await?;
                Ok((HistoryRows::Object(rows), total))
            }
        }
    }

    async fn markers(
        &self,
        options: &QueryOptions,
        window: StorageHistoryWindow,
        selection: StorageHistorySelection,
    ) -> Result<(Vec<HistoryMarker>, i64), ApiError> {
        self.page(
            options,
            |options, scope| async {
                let query = StorageHistoryNavigationQuery::try_new(self.resource, options, scope)
                    .map_err(|error| error.into_request_error())?
                    .window(window)
                    .selection(selection);
                let (rows, total) = storage_handle(self.access.context())
                    .list_history_markers(query)
                    .await?
                    .into_parts();
                Ok((rows.into_iter().map(marker_from_storage).collect(), total))
            },
            |row| HistoryAuthorizationSnapshot::from(row),
        )
        .await
    }

    async fn anchor(&self, id: HistoryRecordId) -> Result<HistoryMarker, ApiError> {
        let options = QueryOptions::new(
            Vec::new(),
            HistoryDirection::Older.sorts(),
            Some(1),
            None,
            false,
        )?;
        self.markers(
            &options,
            StorageHistoryWindow::default(),
            StorageHistorySelection::Entry(id),
        )
        .await?
        .0
        .into_iter()
        .next()
        .ok_or_else(unavailable)
    }

    pub(crate) async fn timeline(
        &self,
        params: HistoryTimelineParameters,
    ) -> Result<(Vec<HistoryMarker>, i64, QueryOptions), ApiError> {
        if params.at.is_some() && params.anchor.is_some() {
            return Err(ApiError::BadRequest(
                "Use either 'at' or 'anchor', not both".into(),
            ));
        }
        let window = StorageHistoryWindow::try_new(params.from, params.until)
            .map_err(|error| ApiError::BadRequest(error.to_string()))?;
        let options = QueryOptions::new(
            Vec::new(),
            params.direction.sorts(),
            params.limit,
            params.cursor,
            params.include_total,
        )?;
        // Validate even when no visible rows exist.
        prepare_db_pagination::<HistoryMarker>(&options)?;
        let selection = match (params.anchor, params.at, params.direction) {
            (Some(id), _, direction) => {
                let anchor = self.anchor(id).await?;
                let position = StorageHistoryPosition::new(anchor.valid_from, id);
                match direction {
                    HistoryDirection::Older => StorageHistorySelection::BeforeEntry(position),
                    HistoryDirection::Newer => StorageHistorySelection::AfterEntry(position),
                }
            }
            (_, Some(at), HistoryDirection::Older) => StorageHistorySelection::AtOrBefore(at),
            (_, Some(at), HistoryDirection::Newer) => StorageHistorySelection::After(at),
            _ => StorageHistorySelection::All,
        };
        let (rows, total) = self.markers(&options, window, selection).await?;
        Ok((rows, total, options))
    }

    pub(crate) async fn as_of(&self, at: DateTime<Utc>) -> Result<HistoryEntry, ApiError> {
        let (entry, snapshot) = match self.resource {
            StorageHistoryResource::Class(id) => {
                let row = super::class_as_of(id.id(), at, self.access.context())
                    .await?
                    .ok_or_else(unavailable)?;
                let snapshot = HistoryAuthorizationSnapshot::from(&row);
                (HistoryEntry::Class(row), snapshot)
            }
            StorageHistoryResource::Object {
                class_id,
                object_id,
            } => {
                let row =
                    super::object_as_of(object_id.id(), class_id.id(), at, self.access.context())
                        .await?
                        .ok_or_else(unavailable)?;
                let snapshot = HistoryAuthorizationSnapshot::from(&row);
                (HistoryEntry::Object(row), snapshot)
            }
        };
        if !matches!(self.visibility, HistoryVisibility::DeletedAdministrator(_)) {
            self.access
                .authorize(self.permission, snapshot.into_resource())
                .await?;
        }
        Ok(entry)
    }

    pub(crate) async fn entry(&self, id: HistoryRecordId) -> Result<HistoryEntry, ApiError> {
        // Resolve historical visibility using metadata before loading a potentially
        // large snapshot. Missing and inaccessible identities take the same path.
        self.anchor(id).await?;
        let row = storage_handle(self.access.context())
            .get_history_entry(StorageHistoryEntryQuery::new(self.resource, id))
            .await?
            .ok_or_else(unavailable)?;
        match row {
            StorageHistoryEntry::Class(row) => class_from_storage(row).map(HistoryEntry::Class),
            StorageHistoryEntry::Object(row) => object_from_storage(row).map(HistoryEntry::Object),
        }
    }
}

fn marker_from_storage(row: StorageHistoryMarker) -> HistoryMarker {
    let (resource, collection_id, name, metadata) = row.into_parts();
    let (id, hubuum_class_id) = match resource {
        StorageHistoryResource::Class(id) => (id.id(), None),
        StorageHistoryResource::Object {
            class_id,
            object_id,
        } => (object_id.id(), Some(class_id.id())),
    };
    let metadata = AppHistoryMetadata::from(metadata);
    HistoryMarker {
        id,
        collection_id: collection_id.id(),
        hubuum_class_id,
        name,
        history_id: metadata.history_id,
        revision: metadata.revision,
        op: metadata.operation,
        valid_from: metadata.valid_from,
        valid_to: metadata.valid_to,
        actor_id: metadata.actor_id,
        actor_kind: metadata.actor_kind,
        initiator_user_id: metadata.initiator_principal_id,
        task_id: metadata.task_id,
    }
}
