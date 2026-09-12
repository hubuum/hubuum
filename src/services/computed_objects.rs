use crate::errors::ApiError;
use crate::models::search::{FilterField, QueryOptions, SortParam};
use crate::models::{
    ComputedFieldErrorResponse, ComputedObjectScopesResponse, ComputedScopeResponse, HubuumObject,
    HubuumObjectComputedResponse, HubuumObjectReadResponse, SharedComputedScopeResponse,
    TokenScope,
};
use crate::pagination::{effective_page_limit, prepare_db_pagination};
use crate::services::storage_boundary::{
    class_id_to_storage, object_from_storage, object_to_storage, principal_id_to_storage,
    visibility as storage_visibility,
};
use crate::storage::{
    ComputedObjectStorage, StorageComputedFieldError, StorageComputedObject,
    StorageComputedObjectEnrichmentQuery, StorageComputedObjectListQuery,
    StorageComputedObjectProjection, StorageComputedObjectQueryOptions,
    StorageComputedObjectVisibility, StorageComputedScope, StorageContext, storage_handle,
};
use crate::traits::{CursorPaginated, CursorValue};

pub(crate) enum ComputedObjectAccess<'a> {
    Storage {
        principal_id: i32,
        is_admin: bool,
        scope: Option<&'a TokenScope>,
    },
}

impl ComputedObjectAccess<'_> {
    fn into_storage(self) -> Result<StorageComputedObjectVisibility, ApiError> {
        match self {
            Self::Storage {
                principal_id,
                is_admin,
                scope,
            } => Ok(StorageComputedObjectVisibility::storage(
                storage_visibility(principal_id, is_admin, scope)?,
            )),
        }
    }
}

pub(crate) struct ComputedObjectListResult {
    pub(crate) objects: Vec<HubuumObject>,
    pub(crate) total: Option<i64>,
    pub(crate) computed: Vec<HubuumObjectComputedResponse>,
    pub(crate) resolved_options: QueryOptions,
}

pub(crate) async fn list_computed_objects(
    backend: &impl StorageContext,
    class_id: i32,
    personal_owner_id: Option<i32>,
    options: QueryOptions,
    access: ComputedObjectAccess<'_>,
    projection: StorageComputedObjectProjection,
) -> Result<ComputedObjectListResult, ApiError> {
    let page_limit = effective_page_limit(&options)?;
    let computed_sorting = options
        .sort()
        .iter()
        .any(|sort| sort.field.computed_query().is_some());
    let execution_options = if computed_sorting {
        prepare_db_pagination::<HubuumObjectComputedResponse>(&options)?
    } else {
        prepare_db_pagination::<HubuumObject>(&options)?
    };
    let prepared_options =
        StorageComputedObjectQueryOptions::try_new(options, execution_options, page_limit)?;
    execute_computed_object_query(
        backend,
        class_id,
        personal_owner_id,
        prepared_options,
        access,
        projection,
    )
    .await
}

async fn execute_computed_object_query(
    backend: &impl StorageContext,
    class_id: i32,
    personal_owner_id: Option<i32>,
    prepared_options: StorageComputedObjectQueryOptions,
    access: ComputedObjectAccess<'_>,
    projection: StorageComputedObjectProjection,
) -> Result<ComputedObjectListResult, ApiError> {
    let (objects, total, computed, resolved_options) = storage_handle(backend)
        .list_computed_objects(StorageComputedObjectListQuery::new(
            class_id_to_storage(class_id),
            personal_owner_id.map(principal_id_to_storage),
            prepared_options,
            access.into_storage()?,
            projection,
        ))
        .await?
        .into_parts();
    Ok(ComputedObjectListResult {
        objects: objects
            .into_iter()
            .map(object_from_storage)
            .collect::<Result<Vec<_>, _>>()?,
        total,
        computed: computed
            .into_iter()
            .map(computed_from_storage)
            .collect::<Result<Vec<_>, _>>()?,
        resolved_options,
    })
}

/// A computed row retains the resolved sort types from the same storage
/// snapshot as its values, including while an external policy is consulted.
pub(crate) struct ComputedObjectCandidate {
    value: HubuumObjectReadResponse,
    sorts: Vec<SortParam>,
}

impl ComputedObjectCandidate {
    pub(crate) fn object(&self) -> &HubuumObject {
        match &self.value {
            HubuumObjectReadResponse::Raw(object) => object,
            HubuumObjectReadResponse::Computed(value) => &value.object,
        }
    }
    pub(crate) fn into_value(self) -> HubuumObjectReadResponse {
        self.value
    }
}

impl CursorPaginated for ComputedObjectCandidate {
    fn supports_sort(field: &FilterField) -> bool {
        HubuumObjectComputedResponse::supports_sort(field)
    }
    fn default_sort() -> Vec<SortParam> {
        HubuumObjectComputedResponse::default_sort()
    }
    fn tie_breaker_sort() -> Vec<SortParam> {
        HubuumObjectComputedResponse::tie_breaker_sort()
    }
    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        let resolved = self
            .sorts
            .iter()
            .find(|sort| sort.field.to_string() == field.to_string());
        let field = resolved.map_or(field, |sort| &sort.field);
        match &self.value {
            HubuumObjectReadResponse::Raw(object) => object.cursor_value(field),
            HubuumObjectReadResponse::Computed(value) => value.cursor_value(field),
        }
    }
}

pub(crate) async fn list_computed_object_candidates(
    backend: &impl StorageContext,
    class_id: i32,
    personal_owner_id: Option<i32>,
    principal_id: i32,
    mut requested: QueryOptions,
    execution: QueryOptions,
    include_computed: bool,
) -> Result<Vec<ComputedObjectCandidate>, ApiError> {
    // Raw computed-filter requests only need SQL predicate evaluation. Keep
    // enrichment for responses or sort keys that actually consume its values.
    let projection = if include_computed
        || execution
            .sort()
            .iter()
            .any(|sort| sort.field.computed_query().is_some())
    {
        StorageComputedObjectProjection::All
    } else {
        StorageComputedObjectProjection::None
    };
    let limit = execution
        .limit()
        .and_then(|limit| limit.checked_sub(1))
        .ok_or_else(|| {
            ApiError::InternalServerError(
                "Computed candidate query requires storage look-ahead".to_string(),
            )
        })?;
    requested.set_limit(Some(limit))?;
    let prepared = StorageComputedObjectQueryOptions::try_new(requested, execution, limit)?;
    let result = execute_computed_object_query(
        backend,
        class_id,
        personal_owner_id,
        prepared,
        ComputedObjectAccess::Storage {
            principal_id,
            is_admin: true,
            scope: None,
        },
        projection,
    )
    .await?;
    let sorts = result
        .resolved_options
        .sort()
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    let values = match projection {
        StorageComputedObjectProjection::None => result
            .objects
            .into_iter()
            .map(HubuumObjectReadResponse::Raw)
            .collect::<Vec<_>>(),
        _ => result
            .computed
            .into_iter()
            .map(HubuumObjectReadResponse::Computed)
            .collect(),
    };
    Ok(values
        .into_iter()
        .map(|value| ComputedObjectCandidate {
            value,
            sorts: sorts.clone(),
        })
        .collect())
}

pub(crate) async fn enrich_objects_with_computed(
    backend: &impl StorageContext,
    objects: Vec<HubuumObject>,
    personal_owner_id: Option<i32>,
) -> Result<Vec<HubuumObjectComputedResponse>, ApiError> {
    storage_handle(backend)
        .enrich_objects_with_computed(StorageComputedObjectEnrichmentQuery::new(
            objects.into_iter().map(object_to_storage).collect(),
            personal_owner_id.map(principal_id_to_storage),
        ))
        .await?
        .into_iter()
        .map(computed_from_storage)
        .collect()
}

fn computed_from_storage(
    object: StorageComputedObject,
) -> Result<HubuumObjectComputedResponse, ApiError> {
    let (object, shared, personal) = object.into_parts();
    let (revision, materialization_stale, shared) = shared.into_parts();
    let (values, errors) = scope_from_storage(shared);
    Ok(HubuumObjectComputedResponse {
        object: object_from_storage(object)?,
        computed: ComputedObjectScopesResponse {
            shared: SharedComputedScopeResponse {
                revision: revision.get(),
                materialization_stale,
                values,
                errors,
            },
            personal: personal.map(|scope| {
                let (values, errors) = scope_from_storage(scope);
                ComputedScopeResponse { values, errors }
            }),
        },
    })
}

fn scope_from_storage(
    scope: StorageComputedScope,
) -> (
    std::collections::BTreeMap<String, serde_json::Value>,
    std::collections::BTreeMap<String, ComputedFieldErrorResponse>,
) {
    let (values, errors) = scope.into_parts();
    (
        values,
        errors
            .into_iter()
            .map(|(key, error)| (key, error_from_storage(error)))
            .collect(),
    )
}

fn error_from_storage(error: StorageComputedFieldError) -> ComputedFieldErrorResponse {
    let (code, path, message) = error.into_parts();
    ComputedFieldErrorResponse {
        code,
        path,
        message,
    }
}
