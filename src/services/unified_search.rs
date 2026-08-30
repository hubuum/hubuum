use crate::errors::ApiError;
use crate::models::{
    Collection, HubuumClassExpanded, HubuumObject, TokenScope, UnifiedSearchCursorToken,
    UnifiedSearchSpec,
};
use crate::services::storage_boundary::{
    class_from_storage, collection_from_storage, object_from_storage, resource_id_to_storage,
    visibility,
};
use crate::storage::{
    StorageCandidatePageLimit, StorageContext, StorageUnifiedSearchCursor,
    StorageUnifiedSearchQuery, UnifiedSearchStorage, storage_handle,
};

pub(crate) struct UnifiedSearchCandidatePage<T> {
    pub(crate) items: Vec<UnifiedSearchCandidate<T>>,
    pub(crate) has_more: bool,
}

pub(crate) struct UnifiedSearchCandidate<T> {
    pub(crate) item: T,
    pub(crate) cursor: UnifiedSearchCursorToken,
}

fn candidate_cursor(cursor: StorageUnifiedSearchCursor) -> UnifiedSearchCursorToken {
    UnifiedSearchCursorToken {
        rank: cursor.rank(),
        name: cursor.normalized_name().to_string(),
        id: cursor.id().id(),
    }
}

fn cursor(cursor: Option<&UnifiedSearchCursorToken>) -> Option<StorageUnifiedSearchCursor> {
    cursor.map(|cursor| {
        StorageUnifiedSearchCursor::new(
            cursor.rank,
            cursor.name.clone(),
            resource_id_to_storage(cursor.id),
        )
    })
}

fn query(
    principal_id: i32,
    is_admin: bool,
    scope: Option<&TokenScope>,
    spec: &UnifiedSearchSpec,
    cursor: Option<&UnifiedSearchCursorToken>,
    search_extended_document: bool,
) -> Result<StorageUnifiedSearchQuery, ApiError> {
    let page_limit = StorageCandidatePageLimit::try_new(spec.limit_per_kind)
        .map_err(|error| ApiError::BadRequest(error.to_string()))?;
    Ok(StorageUnifiedSearchQuery::new(
        spec.query.clone(),
        page_limit,
        visibility(principal_id, is_admin, scope)?,
    )
    .search_extended_document(search_extended_document)
    .cursor(self::cursor(cursor)))
}

pub async fn search_collections(
    backend: &impl StorageContext,
    principal_id: i32,
    is_admin: bool,
    scope: Option<&TokenScope>,
    spec: &UnifiedSearchSpec,
) -> Result<UnifiedSearchCandidatePage<Collection>, ApiError> {
    let page = storage_handle(backend)
        .search_collections(query(
            principal_id,
            is_admin,
            scope,
            spec,
            spec.collection_cursor.as_ref(),
            false,
        )?)
        .await?;
    let (items, has_more) = page.into_parts();
    Ok(UnifiedSearchCandidatePage {
        items: items
            .into_iter()
            .map(|candidate| {
                let (item, cursor) = candidate.into_parts();
                Ok(UnifiedSearchCandidate {
                    item: collection_from_storage(item)?,
                    cursor: candidate_cursor(cursor),
                })
            })
            .collect::<Result<Vec<_>, ApiError>>()?,
        has_more,
    })
}

pub async fn search_classes(
    backend: &impl StorageContext,
    principal_id: i32,
    is_admin: bool,
    scope: Option<&TokenScope>,
    spec: &UnifiedSearchSpec,
) -> Result<UnifiedSearchCandidatePage<HubuumClassExpanded>, ApiError> {
    let page = storage_handle(backend)
        .search_classes(query(
            principal_id,
            is_admin,
            scope,
            spec,
            spec.class_cursor.as_ref(),
            spec.search_class_schema,
        )?)
        .await?;
    let (items, has_more) = page.into_parts();
    Ok(UnifiedSearchCandidatePage {
        items: items
            .into_iter()
            .map(|candidate| {
                let (item, cursor) = candidate.into_parts();
                Ok(UnifiedSearchCandidate {
                    item: class_from_storage(item)?,
                    cursor: candidate_cursor(cursor),
                })
            })
            .collect::<Result<Vec<_>, ApiError>>()?,
        has_more,
    })
}

pub async fn search_objects(
    backend: &impl StorageContext,
    principal_id: i32,
    is_admin: bool,
    scope: Option<&TokenScope>,
    spec: &UnifiedSearchSpec,
) -> Result<UnifiedSearchCandidatePage<HubuumObject>, ApiError> {
    let page = storage_handle(backend)
        .search_objects(query(
            principal_id,
            is_admin,
            scope,
            spec,
            spec.object_cursor.as_ref(),
            spec.search_object_data,
        )?)
        .await?;
    let (items, has_more) = page.into_parts();
    Ok(UnifiedSearchCandidatePage {
        items: items
            .into_iter()
            .map(|candidate| {
                let (item, cursor) = candidate.into_parts();
                Ok(UnifiedSearchCandidate {
                    item: object_from_storage(item)?,
                    cursor: candidate_cursor(cursor),
                })
            })
            .collect::<Result<Vec<_>, ApiError>>()?,
        has_more,
    })
}
