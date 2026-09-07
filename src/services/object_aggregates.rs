use crate::permissions::ClassResourceEndpoint;
use async_trait::async_trait;

use crate::errors::ApiError;
use crate::models::object_aggregate::{
    ComputedFieldSelector, ObjectAggregateAuthorizationParts, ObjectAggregateDimension,
    ObjectAggregateMeasure, ObjectAggregateMeasureField, ObjectAggregateMeasureOperation,
    ObjectAggregateRequest, ObjectAggregateRequestParts, ObjectAggregateRow,
    ObjectAggregateScalarField, ObjectAggregateSort, ObjectAggregateTargetParts,
};
use crate::models::{ObjectAggregatePage, Permissions};
use crate::pagination::{SKIPPED_TOTAL_COUNT, effective_page_limit};
use crate::permissions::{
    AuthorizationContext, AuthorizationMode, PermissionBackend, PermissionDecision,
    PermissionRequest, PrincipalRef, ResourceRef, permission_from_storage, permission_to_storage,
};
use crate::services::storage_boundary::{
    class_id_to_storage, collection_id_to_storage, principal_id_to_storage, visibility,
};
use crate::storage::{
    ObjectAggregateAuthorizer, ObjectAggregateStorage, StorageAuthorizationPermission,
    StorageComputedFieldSelector, StorageError, StorageObjectAggregateAuthorization,
    StorageObjectAggregateAuthorizationCandidate, StorageObjectAggregateAuthorizationTarget,
    StorageObjectAggregateDimension, StorageObjectAggregateMeasure,
    StorageObjectAggregateMeasureField, StorageObjectAggregateMeasureOperation,
    StorageObjectAggregateMeasureState, StorageObjectAggregateQuery, StorageObjectAggregateRow,
    StorageObjectAggregateScalarField, StorageObjectAggregateSort, StorageObjectAggregateSpec,
    StorageObjectAggregateTarget, storage_handle,
};
use crate::traits::{AuthzSubject, PrincipalIdAccessor};

pub(crate) async fn aggregate_objects(
    backend: &impl AuthorizationContext,
    principal: &(impl PrincipalIdAccessor + ?Sized),
    request: ObjectAggregateRequest,
) -> Result<ObjectAggregatePage, ApiError> {
    let ObjectAggregateRequestParts {
        target,
        query_options,
        spec,
        personal_owner_id,
        authorization,
        cursor_budget,
    } = request.into_parts();
    let ObjectAggregateTargetParts {
        class_id,
        class_name,
        collection_id,
    } = target.into_parts();
    let ObjectAggregateAuthorizationParts {
        required_permissions,
        token_scopes,
    } = authorization.into_parts();
    let response_spec = spec.clone();
    let storage_spec = StorageObjectAggregateSpec::try_new(
        spec.dimensions()
            .iter()
            .map(dimension_to_storage)
            .collect::<Result<Vec<_>, _>>()?,
        spec.measures()
            .iter()
            .map(measure_to_storage)
            .collect::<Result<Vec<_>, _>>()?,
        sort_to_storage(spec.sort()),
    )?;
    let page_limit = effective_page_limit(&query_options)?;
    let permission_backend = match backend.authorization_mode() {
        AuthorizationMode::Delegated(permission_backend)
            if !permission_backend.supports_storage_visibility_filtering() =>
        {
            Some(permission_backend)
        }
        AuthorizationMode::LocalStorage | AuthorizationMode::Delegated(_) => None,
    };
    let is_admin = AuthzSubject::is_admin(principal, backend).await?;
    let visibility = visibility(principal.principal_id(), is_admin, token_scopes.as_ref())?;
    let query = StorageObjectAggregateQuery::builder(
        StorageObjectAggregateTarget::new(
            class_id_to_storage(class_id.id()),
            class_name,
            collection_id_to_storage(collection_id.id()),
        ),
        query_options,
        storage_spec,
        visibility,
    )
    .personal_owner_id(personal_owner_id.map(|owner| principal_id_to_storage(owner.id())))
    .required_permissions(
        required_permissions
            .iter()
            .copied()
            .map(permission_to_storage),
    )
    .page_limit(page_limit)
    .cursor_max_encoded_bytes(cursor_budget.max_encoded_bytes())
    .try_build()?;

    let page = if let Some(permission_backend) = permission_backend {
        let principal = PrincipalRef::load(backend, principal).await?;
        let authorizer = DelegatedObjectAggregateAuthorizer {
            backend: permission_backend,
            principal,
        };
        storage_handle(backend)
            .aggregate_objects(
                query,
                StorageObjectAggregateAuthorization::Delegated(&authorizer),
            )
            .await?
    } else {
        storage_handle(backend)
            .aggregate_objects(query, StorageObjectAggregateAuthorization::Storage)
            .await?
    };
    page_from_storage(page, &response_spec)
}

fn dimension_to_storage(
    dimension: &ObjectAggregateDimension,
) -> Result<StorageObjectAggregateDimension, StorageError> {
    Ok(match dimension {
        ObjectAggregateDimension::Scalar(field) => {
            StorageObjectAggregateDimension::Scalar(scalar_field_to_storage(*field))
        }
        ObjectAggregateDimension::JsonData(path) => {
            StorageObjectAggregateDimension::JsonData(path.clone())
        }
        ObjectAggregateDimension::Computed(selector) => {
            StorageObjectAggregateDimension::Computed(computed_selector_to_storage(selector)?)
        }
    })
}

fn measure_to_storage(
    measure: &ObjectAggregateMeasure,
) -> Result<StorageObjectAggregateMeasure, StorageError> {
    let field = match measure.field() {
        ObjectAggregateMeasureField::JsonData(path) => {
            StorageObjectAggregateMeasureField::JsonData(path.clone())
        }
        ObjectAggregateMeasureField::Computed(selector) => {
            StorageObjectAggregateMeasureField::Computed(computed_selector_to_storage(selector)?)
        }
    };
    Ok(StorageObjectAggregateMeasure::new(
        match measure.operation() {
            ObjectAggregateMeasureOperation::Sum => StorageObjectAggregateMeasureOperation::Sum,
            ObjectAggregateMeasureOperation::Average => {
                StorageObjectAggregateMeasureOperation::Average
            }
            ObjectAggregateMeasureOperation::Min => StorageObjectAggregateMeasureOperation::Min,
            ObjectAggregateMeasureOperation::Max => StorageObjectAggregateMeasureOperation::Max,
        },
        field,
    ))
}

fn computed_selector_to_storage(
    selector: &ComputedFieldSelector,
) -> Result<StorageComputedFieldSelector, StorageError> {
    StorageComputedFieldSelector::try_new(selector.scope(), selector.key())
}

const fn scalar_field_to_storage(
    field: ObjectAggregateScalarField,
) -> StorageObjectAggregateScalarField {
    match field {
        ObjectAggregateScalarField::Name => StorageObjectAggregateScalarField::Name,
        ObjectAggregateScalarField::Description => StorageObjectAggregateScalarField::Description,
        ObjectAggregateScalarField::CollectionId => StorageObjectAggregateScalarField::CollectionId,
        ObjectAggregateScalarField::CreatedAt => StorageObjectAggregateScalarField::CreatedAt,
        ObjectAggregateScalarField::UpdatedAt => StorageObjectAggregateScalarField::UpdatedAt,
    }
}

struct DelegatedObjectAggregateAuthorizer<'a> {
    backend: &'a dyn PermissionBackend,
    principal: PrincipalRef,
}

#[async_trait]
impl ObjectAggregateAuthorizer for DelegatedObjectAggregateAuthorizer<'_> {
    async fn authorize_target(
        &self,
        target: StorageObjectAggregateAuthorizationTarget,
        required_permissions: Vec<StorageAuthorizationPermission>,
    ) -> Result<bool, StorageError> {
        let permissions = required_permissions
            .into_iter()
            .map(permission_from_storage)
            .collect::<Vec<_>>();
        let (object_permissions, invariant_permissions): (Vec<_>, Vec<_>) = permissions
            .into_iter()
            .partition(is_object_specific_permission);
        if object_permissions.is_empty() {
            return Err(StorageError::internal(
                "Object aggregate authorization requires an object permission",
            ));
        }
        let (class_id, class_name, collection_id, collection_name) = target.into_parts();
        let class = ResourceRef::class(class_id.id(), collection_id.id(), Some(class_name));
        let collection = ResourceRef::named_collection(collection_id.id(), Some(collection_name));
        let requests = invariant_permissions
            .into_iter()
            .map(|permission| {
                Ok(PermissionRequest {
                    resource: invariant_resource(permission, &class, &collection)?,
                    permissions: vec![permission],
                })
            })
            .collect::<Result<Vec<_>, StorageError>>()?;
        if requests.is_empty() {
            return Ok(true);
        }
        let expected = requests.len();
        let decisions = self
            .backend
            .authorize_many(&self.principal, requests)
            .await
            .map_err(authorization_error_to_storage)?;
        if decisions.len() != expected {
            return Err(StorageError::internal(
                "Permission backend returned an unexpected number of invariant decisions",
            ));
        }
        Ok(decisions
            .into_iter()
            .all(|decision| decision == PermissionDecision::Allow))
    }

    async fn authorize_objects(
        &self,
        candidates: Vec<StorageObjectAggregateAuthorizationCandidate>,
        required_permissions: Vec<StorageAuthorizationPermission>,
    ) -> Result<Vec<bool>, StorageError> {
        let object_permissions = required_permissions
            .into_iter()
            .map(permission_from_storage)
            .filter(is_object_specific_permission)
            .collect::<Vec<_>>();
        if object_permissions.is_empty() {
            return Err(StorageError::internal(
                "Object aggregate authorization requires an object permission",
            ));
        }
        let requests = candidates
            .into_iter()
            .map(|candidate| {
                let (id, name, collection_id, class_id) = candidate.into_parts();
                PermissionRequest {
                    resource: ResourceRef::object(
                        id.id(),
                        ClassResourceEndpoint::new(collection_id.id(), class_id.id()),
                        Some(name),
                    ),
                    permissions: object_permissions.clone(),
                }
            })
            .collect::<Vec<_>>();
        self.backend
            .authorize_many(&self.principal, requests)
            .await
            .map(|decisions| {
                decisions
                    .into_iter()
                    .map(|decision| decision == PermissionDecision::Allow)
                    .collect()
            })
            .map_err(authorization_error_to_storage)
    }
}

fn invariant_resource(
    permission: Permissions,
    class: &ResourceRef,
    collection: &ResourceRef,
) -> Result<ResourceRef, StorageError> {
    Ok(match permission {
        Permissions::ReadObject | Permissions::UpdateObject | Permissions::DeleteObject => {
            return Err(StorageError::internal(
                "Object-specific permission cannot be preauthorized",
            ));
        }
        Permissions::CreateObject => class.normalized_for_permission(permission),
        Permissions::ReadClass | Permissions::UpdateClass | Permissions::DeleteClass => {
            class.clone()
        }
        Permissions::ReadCollection
        | Permissions::UpdateCollection
        | Permissions::DeleteCollection
        | Permissions::DelegateCollection
        | Permissions::ReadRemoteTarget
        | Permissions::CreateRemoteTarget
        | Permissions::UpdateRemoteTarget
        | Permissions::DeleteRemoteTarget
        | Permissions::ExecuteRemoteTarget
        | Permissions::ReadAudit
        | Permissions::ManageEventSubscription => collection.clone(),
        Permissions::CreateClass
        | Permissions::CreateClassRelation
        | Permissions::ReadClassRelation
        | Permissions::UpdateClassRelation
        | Permissions::DeleteClassRelation
        | Permissions::CreateObjectRelation
        | Permissions::ReadObjectRelation
        | Permissions::UpdateObjectRelation
        | Permissions::DeleteObjectRelation
        | Permissions::ReadTemplate
        | Permissions::CreateTemplate
        | Permissions::UpdateTemplate
        | Permissions::DeleteTemplate => ResourceRef::for_permission_on_collection(
            permission,
            collection
                .collection_id()
                .ok_or_else(|| StorageError::internal("Missing collection identity"))?,
        ),
    })
}

fn is_object_specific_permission(permission: &Permissions) -> bool {
    matches!(
        permission,
        Permissions::ReadObject | Permissions::UpdateObject | Permissions::DeleteObject
    )
}

fn authorization_error_to_storage(error: ApiError) -> StorageError {
    let message = error.to_string();
    match error {
        ApiError::PermissionBackendUnavailable(_) => {
            StorageError::authorization_unavailable(message)
        }
        ApiError::ServiceUnavailable(_) => StorageError::unavailable(message),
        _ => StorageError::internal(message),
    }
}

fn sort_to_storage(sort: ObjectAggregateSort) -> StorageObjectAggregateSort {
    match sort {
        ObjectAggregateSort::DimensionsAscending => StorageObjectAggregateSort::DimensionsAscending,
        ObjectAggregateSort::DimensionsDescending => {
            StorageObjectAggregateSort::DimensionsDescending
        }
        ObjectAggregateSort::ObjectCountAscending => {
            StorageObjectAggregateSort::ObjectCountAscending
        }
        ObjectAggregateSort::ObjectCountDescending => {
            StorageObjectAggregateSort::ObjectCountDescending
        }
    }
}

fn page_from_storage(
    page: crate::storage::StorageObjectAggregatePage,
    spec: &crate::models::ObjectAggregateSpec,
) -> Result<ObjectAggregatePage, ApiError> {
    let (rows, total, next_cursor) = page.into_parts();
    let rows = rows
        .into_iter()
        .map(|row| row_from_storage(row, spec))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ObjectAggregatePage::new(
        rows,
        total.unwrap_or(SKIPPED_TOTAL_COUNT),
        next_cursor,
    ))
}

fn row_from_storage(
    row: StorageObjectAggregateRow,
    spec: &crate::models::ObjectAggregateSpec,
) -> Result<ObjectAggregateRow, ApiError> {
    let (measures, object_count, sort_key) = row.into_parts();
    let measures = measures
        .into_iter()
        .map(|measure| {
            let (state, value_count, skipped_count, value) = measure.into_parts();
            serde_json::json!({
                "state": match state {
                    StorageObjectAggregateMeasureState::Value => "value",
                    StorageObjectAggregateMeasureState::Empty => "empty",
                },
                "value_count": value_count,
                "skipped_count": skipped_count,
                "value": value,
            })
        })
        .collect::<Vec<_>>();
    ObjectAggregateRow::from_database(
        spec,
        serde_json::Value::Array(measures),
        object_count,
        sort_key,
    )
}
