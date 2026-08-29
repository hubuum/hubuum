use crate::errors::ApiError;
use crate::models::search::QueryOptions;
use crate::models::{
    ComputedFieldErrorResponse, ComputedObjectScopesResponse, ComputedScopeResponse, HubuumObject,
    HubuumObjectComputedResponse, SharedComputedScopeResponse, TokenScope,
};
use crate::pagination::{effective_page_limit, prepare_db_pagination};
use crate::permissions::visibility::AuthorizedObjectIds;
use crate::services::storage_boundary::{
    class_id_to_storage, object_from_storage, object_id_to_storage, object_to_storage,
    principal_id_to_storage, visibility as storage_visibility,
};
use crate::storage::{
    ComputedObjectStorage, StorageComputedFieldError, StorageComputedObject,
    StorageComputedObjectEnrichmentQuery, StorageComputedObjectListQuery,
    StorageComputedObjectProjection, StorageComputedObjectQueryOptions,
    StorageComputedObjectVisibility, StorageComputedScope, StorageContext, storage_handle,
};

pub(crate) enum ComputedObjectAccess<'a> {
    Storage {
        principal_id: i32,
        is_admin: bool,
        scope: Option<&'a TokenScope>,
    },
    AuthorizedObjectIds {
        principal_id: i32,
        object_ids: &'a AuthorizedObjectIds,
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
            Self::AuthorizedObjectIds {
                principal_id,
                object_ids,
            } => Ok(StorageComputedObjectVisibility::authorized_object_ids(
                principal_id_to_storage(principal_id),
                object_ids
                    .as_slice()
                    .iter()
                    .copied()
                    .map(object_id_to_storage),
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
