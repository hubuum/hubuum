use std::collections::HashMap;

use hubuum_domain::ResourceId;
use hubuum_storage_core::{
    ExecutionStorage, HistoryObjectPopulationGap, LogicalIdentityStorage, StorageErrorKind,
    StorageIdentityResourceKind, StorageIdentityTarget,
};

use crate::errors::ApiError;
use crate::models::{
    HistoryAuthorizationSnapshot, HubuumObjectHistory, Permissions, TokenScope,
    search::QueryOptions,
};
use crate::pagination::prepare_db_pagination;
use crate::permissions::visibility::{
    authorize_all_candidates, filter_authorized_cursor_page_from_storage,
};
use crate::permissions::{AppContext, ClassResourceEndpoint, ResourceRef};
use crate::storage::{
    AuthorizationDataStorage, HistoryStorage, StorageAuthorizationResourceIds,
    StorageClassObjectsAsOfQuery, StorageHandle, StorageHistoryCollectionScope,
    StorageObjectHistoryNameQuery, StorageObjectHistoryRecord, StoragePage, storage_handle,
};
use crate::traits::{AuthzSubject, scope_allows};

use super::access::{HistoryAccess, HistoryIdentity};
use super::{history_total, object_from_storage, original_as_of_query};

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
    async fn bind_original(self, context: &AppContext) -> Result<Self, ApiError> {
        Ok(match self {
            Self::Name(query) => Self::Name(query),
            Self::Population(query) => {
                // Preserve an earlier canonical selection. A fresh integer-ID
                // lookup cannot replace that identity; adapters must reject a
                // stale/reused ID before population or absence projection.
                if query.expected_resource().is_some() {
                    return Ok(Self::Population(query));
                }
                let original = original_as_of_query(
                    context,
                    StorageIdentityResourceKind::Class,
                    ResourceId::new(query.class_id().id())?,
                    query.at(),
                )
                .await?
                .expected_resource();
                Self::Population(match original {
                    Some(original) => query.for_resource(original),
                    None => query,
                })
            }
        })
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
        access: &HistoryAccess<'_>,
    ) -> Result<StoragePage<StorageObjectHistoryRecord>, ApiError> {
        let result = match &self {
            Self::Name(query) => storage.list_object_history_by_name(query.clone()).await,
            Self::Population(query) => storage.list_class_objects_as_of(query.clone()).await,
        };
        let error = match result {
            Ok(page) => return Ok(page),
            Err(error) => error,
        };
        if error.api_request_rejection().is_some() || error.task_capacity_rejection().is_some() {
            return Err(error.into());
        }
        let Some(gap) = error.history_population_gap_evidence() else {
            return Err(error.into());
        };
        let Self::Population(query) = &self else {
            return Err(ApiError::ServiceUnavailable(
                "Retained gap differs from the original history operation".into(),
            ));
        };
        authorize_population_gap(access, storage, query, gap).await?;
        Err(ApiError::NotFound(
            "Historical object population contains an expired version".into(),
        ))
    }
}

/// The evidence contains original policy inputs, never caller authorization.
/// No current name may stand in for missing or contradictory retained facts.
async fn authorize_population_gap(
    access: &HistoryAccess<'_>,
    storage: &StorageHandle,
    query: &StorageClassObjectsAsOfQuery,
    gap: &HistoryObjectPopulationGap,
) -> Result<(), ApiError> {
    let original_scope = storage.current_execution_scope();
    if !gap.matches_query(query)
        || original_scope
            .history_read_override()
            .is_none_or(|context| context.epoch() != gap.epoch())
    {
        return Err(ApiError::ServiceUnavailable(
            "Retained population differs from the original productive read".into(),
        ));
    }
    let facts = gap.facts();
    let object_id = gap
        .remapping()
        .map_or(facts.object_id(), |proof| proof.object_id());
    let collection_id = gap
        .remapping()
        .map_or(facts.collection_id(), |proof| proof.collection_id());
    let class_id = gap
        .remapping()
        .map_or(facts.class_id(), |proof| proof.class_id());
    let current = storage
        .list_authorization_objects(StorageAuthorizationResourceIds::new(vec![ResourceId::new(
            object_id.id(),
        )?]))
        .await?
        .into_iter()
        .next();
    if current.is_none() && access.deleted().is_none() {
        return Err(ApiError::Forbidden("Permission denied".into()));
    }
    let class = facts.attribution().class().ok_or_else(|| {
        ApiError::ServiceUnavailable("Original retained class is unavailable".into())
    })?;
    for (kind, id, expected) in [
        (
            StorageIdentityResourceKind::Object,
            object_id.id(),
            facts.target().resource(),
        ),
        (
            StorageIdentityResourceKind::Collection,
            collection_id.id(),
            facts.attribution().collection(),
        ),
        (StorageIdentityResourceKind::Class, class_id.id(), class),
    ] {
        let evidence = storage
            .resolve_logical_identity(StorageIdentityTarget::Resource {
                kind,
                id: ResourceId::new(id)?,
            })
            .await;
        let actual = match evidence {
            Ok(evidence) => evidence
                .subject()
                .and_then(|subject| subject.record_identity())
                .map(|target| target.resource()),
            Err(error) if error.kind() == StorageErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        let deleted_object = kind == StorageIdentityResourceKind::Object
            && current.is_none()
            && access.deleted().is_some();
        if actual != Some(expected) && !(actual.is_none() && deleted_object) {
            return Err(ApiError::ServiceUnavailable(
                "Original retained resource correspondence changed".into(),
            ));
        }
    }
    let current_shape = current.as_ref().map(|record| {
        (
            record.collection_id(),
            record.class_id(),
            record.name().to_owned(),
        )
    });
    if let Some(current) = current {
        if gap.remapping().is_some()
            && (current.collection_id() != collection_id || current.class_id() != class_id)
        {
            return Err(ApiError::ServiceUnavailable(
                "Original remapped object relationships changed".into(),
            ));
        }
        access
            .authorize(
                Permissions::ReadObject,
                ResourceRef::object(
                    current.id().id(),
                    ClassResourceEndpoint::new(
                        current.collection_id().id(),
                        current.class_id().id(),
                    ),
                    Some(current.name().to_owned()),
                ),
            )
            .await?;
    }
    access
        .authorize(
            Permissions::ReadObject,
            HistoryAuthorizationSnapshot::object(
                object_id.id(),
                collection_id.id(),
                class_id.id(),
                facts.name().to_owned(),
            )
            .into_resource(),
        )
        .await?;
    // Re-enter the original productive scope after external policy I/O. This
    // validates the captured app/history/API owners; it does not refresh them.
    if gap.remapping().is_some() {
        let final_shape = storage
            .list_authorization_objects(StorageAuthorizationResourceIds::new(vec![
                ResourceId::new(object_id.id())?,
            ]))
            .await?
            .into_iter()
            .next()
            .map(|record| {
                (
                    record.collection_id(),
                    record.class_id(),
                    record.name().to_owned(),
                )
            });
        if final_shape != current_shape {
            return Err(ApiError::ServiceUnavailable(
                "Current authorization shape changed during policy evaluation".into(),
            ));
        }
        // Re-run the bounded native correspondence gate in the SAME original
        // productive scope, after external policy I/O. Never refresh an owner.
        let Err(error) = storage.list_class_objects_as_of(query.clone()).await else {
            return Err(ApiError::ServiceUnavailable(
                "Retained population correspondence changed during authorization".into(),
            ));
        };
        if error.api_request_rejection().is_some() || error.task_capacity_rejection().is_some() {
            return Err(error.into());
        }
        if error
            .history_population_gap_evidence()
            .is_some_and(|current| {
                current.epoch() == gap.epoch()
                    && current.facts() == gap.facts()
                    && current.remapping() == gap.remapping()
                    && current.matches_query(query)
            })
        {
            return Ok(());
        }
        return Err(ApiError::ServiceUnavailable(
            "Retained population correspondence changed during authorization".into(),
        ));
    }
    for (kind, id, expected) in [
        (
            StorageIdentityResourceKind::Object,
            object_id.id(),
            facts.target().resource(),
        ),
        (
            StorageIdentityResourceKind::Collection,
            collection_id.id(),
            facts.attribution().collection(),
        ),
        (StorageIdentityResourceKind::Class, class_id.id(), class),
    ] {
        let evidence = storage
            .resolve_logical_identity(StorageIdentityTarget::Resource {
                kind,
                id: ResourceId::new(id)?,
            })
            .await;
        let deleted_object = kind == StorageIdentityResourceKind::Object
            && access.deleted().is_some()
            && gap.remapping().is_none();
        match evidence {
            Ok(evidence)
                if !evidence.has_unknown_restore_source()
                    && evidence
                        .subject()
                        .and_then(|s| s.record_identity())
                        .is_some_and(|record| record.resource() == expected) => {}
            Err(error) if deleted_object && error.kind() == StorageErrorKind::NotFound => {}
            _ => {
                return Err(ApiError::ServiceUnavailable(
                    "Original retained correspondence changed during authorization".into(),
                ));
            }
        }
    }
    Ok(())
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
    let access = HistoryAccess::load(context, subject, scopes).await?;
    let principal = access.principal();
    let permission_backend = access.backend();
    let include_deleted = access.deleted().is_some();
    let collections = access
        .candidate_collections(Permissions::ReadObject)
        .await?;
    let query = query
        .bind_original(context)
        .await?
        .visibility(collections, include_deleted);
    let storage = storage_handle(context);
    if access.uses_storage_visibility() {
        // The adapter can enforce both collection checks in the same snapshot
        // as COUNT/LIMIT. External policies and resource-scoped tokens require
        // the bounded candidate path below.
        let options = prepare_db_pagination::<HubuumObjectHistory>(query.options())?;
        let (rows, total) = query
            .with_options(options.clone())?
            .fetch(&storage, &access)
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
            let access = &access;
            async move {
                query
                    .with_options(options)?
                    .fetch(storage, access)
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
            let access = &access;
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
                    match access.identity(current.get(&row.id)) {
                        Some(HistoryIdentity::Live(object)) => live.push((
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
                        Some(HistoryIdentity::Deleted(_)) => deleted.push((index, row)),
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
