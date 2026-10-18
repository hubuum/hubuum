use std::collections::HashMap;

use hubuum_domain::{ClassRelationId, ObjectRelationId};
use hubuum_storage_core::StorageAuthorizationResourceKey as Key;

use super::AppHistoryMetadata;
use crate::errors::ApiError;
use crate::models::search::{FilterField, SortParam};
use crate::models::{
    HistoricalEndpointContext, HistoricalEndpointUnavailableReason, HistoricalRelation,
    HistoricalRelationEndpoint, HistoricalRelationValue, Permissions, TokenScope,
};
use crate::permissions::visibility::{
    authorize_all_candidates, filter_authorized_cursor_page_from_storage,
};
use crate::permissions::{
    AppContext, ClassResourceEndpoint, ObjectResourceEndpoint, PrincipalRef, ResourceRef,
};
use crate::services::authorization_resources::load_current_authorization_resources;
use crate::services::relation_queries::{
    class_relation_from_storage, object_relation_from_storage,
};
use crate::storage::{
    HistoryStorage, StorageHistoricalRelation, StorageHistoryEndpointSnapshot,
    StorageHistoryResource, StorageRelationHistoryEntry, StorageRelationHistoryQuery,
    StorageRelationHistoryResource, storage_handle,
};
use crate::traits::{AuthzSubject, CursorPaginated, CursorValue, scope_allows};

struct Candidate(StorageRelationHistoryEntry);

impl CursorPaginated for Candidate {
    fn supports_sort(field: &FilterField) -> bool {
        HistoricalRelation::supports_sort(field)
    }
    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        if Self::supports_sort(field) {
            Ok(CursorValue::Integer(
                self.0.record().metadata().history_entry_id().id(),
            ))
        } else {
            Err(ApiError::BadRequest(
                "Relationship history supports only history_id ordering".into(),
            ))
        }
    }
    fn default_sort() -> Vec<SortParam> {
        HistoricalRelation::default_sort()
    }
    fn tie_breaker_sort() -> Vec<SortParam> {
        HistoricalRelation::tie_breaker_sort()
    }
}

fn endpoint_key(resource: StorageRelationHistoryResource) -> Key {
    match resource {
        StorageRelationHistoryResource::Class(id) => Key::Class(id),
        StorageRelationHistoryResource::Object(id) => Key::Object(id),
    }
}

fn relation_key(relation: &StorageHistoricalRelation) -> Key {
    match relation {
        StorageHistoricalRelation::Class(row) => {
            Key::ClassRelation(ClassRelationId::from(row.metadata().id()))
        }
        StorageHistoricalRelation::Object(row) => {
            Key::ObjectRelation(ObjectRelationId::from(row.metadata().id()))
        }
    }
}

fn historical_endpoint(endpoint: &StorageHistoryEndpointSnapshot) -> ResourceRef {
    match endpoint.resource() {
        StorageHistoryResource::Class(id) => ResourceRef::class(
            id.id(),
            endpoint.collection_id().id(),
            Some(endpoint.name().to_owned()),
        ),
        StorageHistoryResource::Object {
            class_id,
            object_id,
        } => ResourceRef::object(
            object_id.id(),
            ClassResourceEndpoint::new(endpoint.collection_id().id(), class_id.id()),
            Some(endpoint.name().to_owned()),
        ),
    }
}

fn historical_relation(entry: &StorageRelationHistoryEntry) -> Option<ResourceRef> {
    let (from, to) = (entry.from()?, entry.to()?);
    Some(match entry.record().relation() {
        StorageHistoricalRelation::Class(row) => ResourceRef::class_relation(
            Some(row.metadata().id().id()),
            ClassResourceEndpoint::new(from.collection_id().id(), row.from_class_id().id()),
            ClassResourceEndpoint::new(to.collection_id().id(), row.to_class_id().id()),
        ),
        StorageHistoricalRelation::Object(row) => {
            let (
                StorageHistoryResource::Object {
                    class_id: from_class,
                    ..
                },
                StorageHistoryResource::Object {
                    class_id: to_class, ..
                },
            ) = (from.resource(), to.resource())
            else {
                return None;
            };
            ResourceRef::object_relation(
                Some(row.metadata().id().id()),
                ObjectResourceEndpoint::new(
                    from.collection_id().id(),
                    from_class.id(),
                    row.from_object_id().id(),
                ),
                ObjectResourceEndpoint::new(
                    to.collection_id().id(),
                    to_class.id(),
                    row.to_object_id().id(),
                ),
                row.class_relation_id().id(),
            )
        }
    })
}

/// Build conjunctive checks, preserving the existing unscoped-administrator
/// exception only for identities whose live resource no longer exists.
fn checks(
    entry: &StorageRelationHistoryEntry,
    current: &HashMap<Key, ResourceRef>,
    deleted_admin: bool,
    permission: Permissions,
) -> Option<Vec<(Permissions, ResourceRef)>> {
    let relation_permission = match entry.record().relation() {
        StorageHistoricalRelation::Class(_) => Permissions::ReadClassRelation,
        StorageHistoricalRelation::Object(_) => Permissions::ReadObjectRelation,
    };
    let mut checks = Vec::with_capacity(6);
    match current.get(&relation_key(entry.record().relation())) {
        Some(live) => {
            checks.push((relation_permission, live.clone()));
            match historical_relation(entry) {
                Some(historical) => checks.push((relation_permission, historical)),
                None if deleted_admin => {}
                None => return None,
            }
        }
        None if deleted_admin => {}
        None => return None,
    }
    for (identity, historical) in entry
        .record()
        .relation()
        .endpoints()
        .into_iter()
        .zip([entry.from(), entry.to()])
    {
        match current.get(&endpoint_key(identity)) {
            Some(live) => {
                checks.push((permission, live.clone()));
                match historical {
                    Some(historical) => checks.push((permission, historical_endpoint(historical))),
                    None if deleted_admin => {}
                    None => return None,
                }
            }
            None if deleted_admin => {}
            None => return None,
        }
    }
    Some(checks)
}

pub(super) async fn list(
    context: &AppContext,
    subject: &impl AuthzSubject,
    scopes: Option<&TokenScope>,
    query: StorageRelationHistoryQuery,
) -> Result<(Vec<HistoricalRelation>, i64), ApiError> {
    let permission = match query.resource() {
        StorageRelationHistoryResource::Class(_) => Permissions::ReadClass,
        StorageRelationHistoryResource::Object(_) => Permissions::ReadObject,
    };
    let relation_permission = match query.resource() {
        StorageRelationHistoryResource::Class(_) => Permissions::ReadClassRelation,
        StorageRelationHistoryResource::Object(_) => Permissions::ReadObjectRelation,
    };
    if !scope_allows(scopes, &[permission, relation_permission]) {
        return Err(ApiError::Forbidden("Permission denied".into()));
    }
    let backend = context.permission_backend();
    let principal = PrincipalRef::load(context, subject).await?;
    let deleted_admin = scopes.is_none() && backend.is_admin(&principal).await?;
    let storage = storage_handle(context);
    let page = filter_authorized_cursor_page_from_storage(
        backend,
        query.options(),
        |options| {
            let query = query.clone();
            let storage = &storage;
            async move {
                let query = query
                    .with_options(options)
                    .map_err(|error| error.into_request_error())?;
                Ok(storage
                    .list_relation_history(query)
                    .await?
                    .into_parts()
                    .0
                    .into_iter()
                    .map(Candidate)
                    .collect())
            }
        },
        |candidates: Vec<Candidate>| {
            let principal = &principal;
            async move {
                let keys = candidates.iter().flat_map(|candidate| {
                    let relation = candidate.0.record().relation();
                    let [from, to] = relation.endpoints();
                    [relation_key(relation), endpoint_key(from), endpoint_key(to)]
                });
                let current = load_current_authorization_resources(context, keys).await?;
                let candidates = candidates
                    .into_iter()
                    .filter_map(|row| {
                        checks(&row.0, &current, deleted_admin, permission)
                            .map(|checks| (row, checks))
                    })
                    .collect::<Vec<_>>();
                let mut allowed = vec![0usize; candidates.len()];
                // At most two permission groups, each chunked by the common policy
                // helper. Endpoint authorization never incurs one lookup per edge.
                for permission in [relation_permission, permission] {
                    let checks = candidates
                        .iter()
                        .enumerate()
                        .flat_map(|(index, (_, checks))| {
                            checks
                                .iter()
                                .filter(move |(required, _)| *required == permission)
                                .map(move |(_, resource)| (index, resource.clone()))
                        })
                        .collect();
                    for (index, _) in authorize_all_candidates(
                        backend,
                        principal,
                        checks,
                        scopes,
                        vec![permission],
                        |(_, resource)| resource.clone(),
                    )
                    .await?
                    {
                        allowed[index] += 1;
                    }
                }
                Ok(candidates
                    .into_iter()
                    .enumerate()
                    .filter_map(|(index, (row, checks))| {
                        (allowed[index] == checks.len()).then_some(row)
                    })
                    .collect())
            }
        },
    )
    .await?;
    Ok((
        page.rows
            .into_iter()
            .map(|row| project(row.0))
            .collect::<Result<_, _>>()?,
        page.total_count,
    ))
}

fn endpoint_context(value: Option<StorageHistoryEndpointSnapshot>) -> HistoricalEndpointContext {
    match value {
        None => HistoricalEndpointContext::Unavailable {
            reason: HistoricalEndpointUnavailableReason::HistoryUnavailable,
        },
        Some(row) => {
            let (id, class_id) = match row.resource() {
                StorageHistoryResource::Class(id) => (id.id(), None),
                StorageHistoryResource::Object {
                    class_id,
                    object_id,
                } => (object_id.id(), Some(class_id.id())),
            };
            HistoricalEndpointContext::Available {
                snapshot: HistoricalRelationEndpoint {
                    id,
                    hubuum_class_id: class_id,
                    collection_id: row.collection_id().id(),
                    name: row.name().to_owned(),
                    history_id: row.metadata().history_entry_id().id(),
                    revision: row.metadata().revision(),
                    valid_from: row.metadata().valid_from(),
                    valid_to: row.metadata().valid_to(),
                },
            }
        }
    }
}

fn project(entry: StorageRelationHistoryEntry) -> Result<HistoricalRelation, ApiError> {
    let (record, context_at, from, to) = entry.into_parts();
    let (relation, metadata) = record.into_parts();
    let metadata = AppHistoryMetadata::from(metadata);
    let value = match relation {
        StorageHistoricalRelation::Class(row) => {
            HistoricalRelationValue::Class(class_relation_from_storage(row)?)
        }
        StorageHistoricalRelation::Object(row) => {
            HistoricalRelationValue::Object(object_relation_from_storage(row)?)
        }
    };
    Ok(HistoricalRelation {
        value,
        context_at,
        from: endpoint_context(from),
        to: endpoint_context(to),
        history_id: metadata.history_id,
        revision: metadata.revision,
        op: metadata.operation,
        valid_from: metadata.valid_from,
        valid_to: metadata.valid_to,
        actor_id: metadata.actor_id,
        actor_kind: metadata.actor_kind,
        initiator_user_id: metadata.initiator_principal_id,
        task_id: metadata.task_id,
    })
}
