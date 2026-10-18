use hubuum_domain::HistoryRecordId;

use super::access::{
    authorize_history_page, authorize_history_snapshot, can_read_deleted_history,
    readable_history_collection_ids,
};
use super::{AppHistoryMetadata, class_from_storage, history_total, object_from_storage};
use crate::errors::ApiError;
use crate::models::search::QueryOptions;
use crate::models::{
    HistoryAuthorizationSnapshot, HistoryDirection, HistoryMarker, HistoryTimelineParameters,
    HubuumClassHistory, HubuumObjectHistory, Permissions, TokenScope,
};
use crate::pagination::prepare_db_pagination;
use crate::permissions::AppContext;
use crate::storage::{
    HistoryStorage, StorageHistoryCollectionScope, StorageHistoryEntry, StorageHistoryEntryQuery,
    StorageHistoryMarker, StorageHistoryNavigationQuery, StorageHistoryPosition,
    StorageHistoryResource, StorageHistorySelection, StorageHistoryWindow, storage_handle,
};
use crate::traits::AuthzSubject;
use hubuum_storage_core::{
    AuthorizationDataStorage, StorageAuthorizationResource, StorageAuthorizationResourceKey,
    StorageAuthorizationResourcesQuery,
};

enum HistoryVisibility {
    DeletedAdministrator,
    Collections(StorageHistoryCollectionScope),
    Policy,
}

/// Carries the successful live-resource (or deleted-resource administrator) gate.
pub(crate) struct HistoryReader<'a, S> {
    context: &'a AppContext,
    subject: &'a S,
    scopes: Option<&'a TokenScope>,
    resource: StorageHistoryResource,
    permission: Permissions,
    visibility: HistoryVisibility,
}

pub(crate) enum HistoryEntry {
    Class(HubuumClassHistory),
    Object(HubuumObjectHistory),
}

fn unavailable() -> ApiError {
    ApiError::NotFound("History entry unavailable".into())
}

impl<'a, S: AuthzSubject> HistoryReader<'a, S> {
    pub(crate) async fn authorize(
        context: &'a AppContext,
        subject: &'a S,
        scopes: Option<&'a TokenScope>,
        resource: StorageHistoryResource,
    ) -> Result<Self, ApiError> {
        let permission = match resource {
            StorageHistoryResource::Class(_) => Permissions::ReadClass,
            StorageHistoryResource::Object { .. } => Permissions::ReadObject,
        };
        let live = async {
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
                .next()
                .ok_or_else(unavailable)?;
            let snapshot = match (facts, resource) {
                (
                    StorageAuthorizationResource::Class {
                        resource: class,
                        name,
                    },
                    StorageHistoryResource::Class(id),
                ) if class.id() == id => {
                    HistoryAuthorizationSnapshot::class(id.id(), class.collection_id().id(), name)
                }
                (
                    StorageAuthorizationResource::Object(object),
                    StorageHistoryResource::Object {
                        class_id,
                        object_id,
                    },
                ) if object.id() == object_id && object.class_id() == class_id => {
                    HistoryAuthorizationSnapshot::object(
                        object_id.id(),
                        object.collection_id().id(),
                        class_id.id(),
                        object.name().to_owned(),
                    )
                }
                _ => return Err(unavailable()),
            };
            authorize_history_snapshot(context, subject, scopes, permission, snapshot).await
        }
        .await;
        let visibility = match live {
            Ok(())
                if context
                    .permission_backend()
                    .supports_storage_visibility_filtering() =>
            {
                let ids =
                    readable_history_collection_ids(context, subject, scopes, permission).await?;
                HistoryVisibility::Collections(
                    super::HistoryCollectionFilter::Visible(&ids).try_into()?,
                )
            }
            Ok(()) => HistoryVisibility::Policy,
            Err(ApiError::NotFound(_))
                if can_read_deleted_history(context, subject, scopes.is_some()).await? =>
            {
                HistoryVisibility::DeletedAdministrator
            }
            Err(error) => return Err(error),
        };
        Ok(Self {
            context,
            subject,
            scopes,
            resource,
            permission,
            visibility,
        })
    }

    async fn markers(
        &self,
        options: &QueryOptions,
        window: StorageHistoryWindow,
        selection: StorageHistorySelection,
    ) -> Result<(Vec<HistoryMarker>, i64), ApiError> {
        let scope = match &self.visibility {
            HistoryVisibility::Collections(scope) => scope.clone(),
            _ => StorageHistoryCollectionScope::All,
        };
        let fetch = |options| async {
            let query =
                StorageHistoryNavigationQuery::try_new(self.resource, options, scope.clone())
                    .map_err(|error| ApiError::BadRequest(error.to_string()))?
                    .window(window)
                    .selection(selection);
            let (rows, total) = storage_handle(self.context)
                .list_history_markers(query)
                .await?
                .into_parts();
            Ok::<_, ApiError>((
                rows.into_iter()
                    .map(marker_from_storage)
                    .collect::<Vec<_>>(),
                total,
            ))
        };
        if matches!(self.visibility, HistoryVisibility::Policy) {
            authorize_history_page(
                self.context,
                self.subject,
                self.scopes,
                self.permission,
                |options| async { fetch(options).await.map(|(rows, _)| rows) },
                options,
                |row| HistoryAuthorizationSnapshot::from(row),
            )
            .await
        } else {
            let (rows, total) = fetch(prepare_db_pagination::<HistoryMarker>(options)?).await?;
            Ok((rows, history_total(total, options)?))
        }
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
        // Unknown/deleted resources do not turn arbitrary IDs into successful empty timelines.
        if rows.is_empty() && matches!(self.visibility, HistoryVisibility::DeletedAdministrator) {
            let probe = QueryOptions::new(
                Vec::new(),
                HistoryDirection::Older.sorts(),
                Some(1),
                None,
                false,
            )?;
            if self
                .markers(
                    &probe,
                    StorageHistoryWindow::default(),
                    StorageHistorySelection::All,
                )
                .await?
                .0
                .is_empty()
            {
                return Err(unavailable());
            }
        }
        Ok((rows, total, options))
    }

    pub(crate) async fn entry(&self, id: HistoryRecordId) -> Result<HistoryEntry, ApiError> {
        let row = storage_handle(self.context)
            .get_history_entry(StorageHistoryEntryQuery::new(self.resource, id))
            .await?
            .ok_or_else(unavailable)?;
        let (entry, snapshot) = match row {
            StorageHistoryEntry::Class(row) => {
                let row = class_from_storage(row)?;
                let snapshot = HistoryAuthorizationSnapshot::from(&row);
                (HistoryEntry::Class(row), snapshot)
            }
            StorageHistoryEntry::Object(row) => {
                let row = object_from_storage(row)?;
                let snapshot = HistoryAuthorizationSnapshot::from(&row);
                (HistoryEntry::Object(row), snapshot)
            }
        };
        if !matches!(self.visibility, HistoryVisibility::DeletedAdministrator) {
            authorize_history_snapshot(
                self.context,
                self.subject,
                self.scopes,
                self.permission,
                snapshot,
            )
            .await
            .map_err(|error| match error {
                ApiError::Forbidden(_) => unavailable(),
                other => other,
            })?;
        }
        Ok(entry)
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
