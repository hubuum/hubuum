use std::collections::HashMap;

use hubuum_domain::{CollectionId, ResourceId};

use crate::errors::ApiError;
use crate::models::collection::user_can_on_any;
use crate::models::{
    HistoryAuthorizationSnapshot, HubuumObjectHistory, Permissions, TokenScope,
    search::QueryOptions,
};
use crate::pagination::prepare_db_pagination;
use crate::permissions::visibility::{
    authorize_all_candidates, filter_authorized_cursor_page_from_storage,
};
use crate::permissions::{AppContext, ClassResourceEndpoint, PrincipalRef, ResourceRef};
use crate::storage::{
    AuthorizationDataStorage, HistoryStorage, StorageAuthorizationResourceIds,
    StorageClassObjectsAsOfQuery, StorageHandle, StorageHistoryCollectionScope,
    StorageObjectHistoryNameQuery, StorageObjectHistoryRecord, StoragePage, storage_handle,
};
use crate::traits::{AuthzSubject, scope_allows};

use super::{history_total, object_from_storage};

#[derive(Clone)]
enum ObjectHistoryQuery {
    Name(StorageObjectHistoryNameQuery),
    Population(StorageClassObjectsAsOfQuery),
}

impl ObjectHistoryQuery {
    fn options(&self) -> &QueryOptions {
        match self {
            Self::Name(query) => query.options(),
            Self::Population(query) => query.options(),
        }
    }
    fn visibility(self, scope: StorageHistoryCollectionScope, include_deleted: bool) -> Self {
        match self {
            Self::Name(query) => Self::Name(query.visibility(scope, include_deleted)),
            Self::Population(query) => Self::Population(query.visibility(scope, include_deleted)),
        }
    }
    fn with_options(self, options: QueryOptions) -> Result<Self, ApiError> {
        match self {
            Self::Name(query) => query.with_options(options).map(Self::Name),
            Self::Population(query) => query.with_options(options).map(Self::Population),
        }
        .map_err(|error| ApiError::from(error.into_request_error()))
    }
    async fn fetch(
        self,
        storage: &StorageHandle,
    ) -> Result<StoragePage<StorageObjectHistoryRecord>, ApiError> {
        Ok(match self {
            Self::Name(query) => storage.list_object_history_by_name(query).await?,
            Self::Population(query) => storage.list_class_objects_as_of(query).await?,
        })
    }
    fn matches_live_class(&self, class_id: i32, historical_class_id: i32) -> bool {
        // Population reads authorize the current object even if its class changed.
        matches!(self, Self::Population(_)) || class_id == historical_class_id
    }
}

pub async fn class_objects_as_of(
    context: &AppContext,
    subject: &(impl AuthzSubject + ?Sized),
    scopes: Option<&TokenScope>,
    query: StorageClassObjectsAsOfQuery,
) -> Result<(Vec<HubuumObjectHistory>, i64), ApiError> {
    authorized_object_history(
        context,
        subject,
        scopes,
        ObjectHistoryQuery::Population(query),
    )
    .await
}

/// Enumerate matching snapshots without letting one identity authorize another.
pub async fn object_history_by_name(
    context: &AppContext,
    subject: &(impl AuthzSubject + ?Sized),
    scopes: Option<&TokenScope>,
    query: StorageObjectHistoryNameQuery,
) -> Result<(Vec<HubuumObjectHistory>, i64), ApiError> {
    authorized_object_history(context, subject, scopes, ObjectHistoryQuery::Name(query)).await
}

async fn authorized_object_history(
    context: &AppContext,
    subject: &(impl AuthzSubject + ?Sized),
    scopes: Option<&TokenScope>,
    query: ObjectHistoryQuery,
) -> Result<(Vec<HubuumObjectHistory>, i64), ApiError> {
    if !scope_allows(scopes, &[Permissions::ReadObject]) {
        return Err(ApiError::Forbidden("Permission denied".to_string()));
    }
    let principal = PrincipalRef::load(context, subject).await?;
    let permission_backend = context.permission_backend();
    let include_deleted = scopes.is_none() && permission_backend.is_admin(&principal).await?;
    let collections = if permission_backend.supports_storage_visibility_filtering() {
        // Token resource boundaries are applied to each concrete current and
        // historical object below, including object-only tokens.
        StorageHistoryCollectionScope::Visible(
            user_can_on_any(context, subject, Permissions::ReadObject, None)
                .await?
                .into_iter()
                .map(|collection| CollectionId::new(collection.id))
                .collect::<Result<Vec<_>, _>>()?,
        )
    } else {
        StorageHistoryCollectionScope::All
    };
    let query = query.visibility(collections, include_deleted);
    let storage = storage_handle(context);
    if permission_backend.supports_storage_visibility_filtering() && scopes.is_none() {
        // The adapter can enforce both collection checks in the same snapshot
        // as COUNT/LIMIT. External policies and resource-scoped tokens require
        // the bounded candidate path below.
        let options = prepare_db_pagination::<HubuumObjectHistory>(query.options())?;
        let (rows, total) = query
            .with_options(options.clone())?
            .fetch(&storage)
            .await?
            .into_parts();
        return Ok((
            rows.into_iter()
                .map(object_from_storage)
                .collect::<Result<_, _>>()?,
            history_total(total, &options)?,
        ));
    }
    let page = filter_authorized_cursor_page_from_storage(
        permission_backend,
        query.options(),
        |options| {
            let query = query.clone();
            let storage = &storage;
            async move {
                query
                    .with_options(options)?
                    .fetch(storage)
                    .await?
                    .into_parts()
                    .0
                    .into_iter()
                    .map(object_from_storage)
                    .collect()
            }
        },
        |candidates: Vec<HubuumObjectHistory>| {
            let storage = &storage;
            let principal = &principal;
            let query = &query;
            async move {
                let current = storage
                    .list_authorization_objects(StorageAuthorizationResourceIds::new(
                        candidates
                            .iter()
                            .map(|row| ResourceId::new(row.id))
                            .collect::<Result<Vec<_>, _>>()?,
                    ))
                    .await?
                    .into_iter()
                    .map(|object| (object.id().id(), object))
                    .collect::<HashMap<_, _>>();
                let mut deleted = Vec::new();
                let mut live = Vec::new();
                for (index, row) in candidates.into_iter().enumerate() {
                    match current.get(&row.id).filter(|object| {
                        query.matches_live_class(object.class_id().id(), row.hubuum_class_id)
                    }) {
                        Some(object) => live.push((
                            index,
                            row,
                            ResourceRef::object(
                                object.id().id(),
                                ClassResourceEndpoint::new(
                                    object.collection_id().id(),
                                    object.class_id().id(),
                                ),
                                Some(object.name().to_owned()),
                            ),
                        )),
                        None if include_deleted => deleted.push((index, row)),
                        None => {}
                    }
                }
                let live = authorize_all_candidates(
                    permission_backend,
                    principal,
                    live,
                    scopes,
                    vec![Permissions::ReadObject],
                    |(_, _, resource)| resource.clone(),
                )
                .await?;
                let live = authorize_all_candidates(
                    permission_backend,
                    principal,
                    live,
                    scopes,
                    vec![Permissions::ReadObject],
                    |(_, row, _)| HistoryAuthorizationSnapshot::from(row).into_resource(),
                )
                .await?;
                deleted.extend(live.into_iter().map(|(index, row, _)| (index, row)));
                deleted.sort_unstable_by_key(|(index, _)| *index);
                Ok(deleted.into_iter().map(|(_, row)| row).collect())
            }
        },
    )
    .await?;
    Ok((page.rows, page.total_count))
}
