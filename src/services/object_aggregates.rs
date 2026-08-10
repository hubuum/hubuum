use async_trait::async_trait;

use crate::errors::ApiError;
use crate::models::object_aggregate::{
    ObjectAggregateAuthorizationParts, ObjectAggregateRequest, ObjectAggregateRequestParts,
    ObjectAggregateRow, ObjectAggregateSort, ObjectAggregateTargetParts,
};
use crate::models::{ObjectAggregatePage, Permissions};
use crate::pagination::SKIPPED_TOTAL_COUNT;
use crate::permissions::{
    PermissionBackend, PermissionDecision, PermissionRequest, PrincipalRef, ResourceAttrs,
    ResourceKind, ResourceRef, permission_from_storage, permission_to_storage,
};
use crate::services::storage_boundary::visibility;
use crate::storage::{
    AuthorizationPermission, ObjectAggregateAuthorizationMode, ObjectAggregateAuthorizer,
    ObjectAggregateStorage, ObjectAggregateStorageQuery, StorageContext, StorageError,
    StorageErrorKind, StorageObjectAggregateAuthorizationCandidate,
    StorageObjectAggregateAuthorizationTarget, StorageObjectAggregateMeasureState,
    StorageObjectAggregateRow, StorageObjectAggregateSort, StorageObjectAggregateSpec,
    StorageObjectAggregateTarget, storage_handle,
};
use crate::traits::{AuthzSubject, PrincipalIdAccessor};

pub(crate) async fn aggregate_objects(
    backend: &impl StorageContext,
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
    let storage_spec = StorageObjectAggregateSpec::new(
        spec.dimensions()
            .iter()
            .map(|dimension| dimension.canonical()),
        spec.measures().iter().map(|measure| measure.canonical()),
        sort_to_storage(spec.sort()),
    );
    let permission_backend = backend.permission_backend();
    let delegated =
        permission_backend.is_some_and(|backend| !backend.supports_storage_visibility_filtering());
    let is_admin = AuthzSubject::is_admin(principal, backend).await?;
    let visibility = visibility(principal.principal_id(), is_admin, token_scopes.as_ref())?;
    let query = ObjectAggregateStorageQuery::builder(
        StorageObjectAggregateTarget::new(class_id.id(), class_name, collection_id.id()),
        query_options,
        storage_spec,
        visibility,
    )
    .personal_owner_id(personal_owner_id.map(|owner| owner.id()))
    .required_permissions(
        required_permissions
            .iter()
            .copied()
            .map(permission_to_storage),
    )
    .cursor_max_encoded_bytes(cursor_budget.max_encoded_bytes())
    .authorization_mode(if delegated {
        ObjectAggregateAuthorizationMode::Delegated
    } else {
        ObjectAggregateAuthorizationMode::Storage
    })
    .build()?;

    let page = if delegated {
        let permission_backend = permission_backend.ok_or_else(|| {
            ApiError::InternalServerError(
                "Delegated object aggregation requires a permission backend".to_string(),
            )
        })?;
        let principal = PrincipalRef::load(backend, principal).await?;
        let authorizer = DelegatedObjectAggregateAuthorizer {
            backend: permission_backend,
            principal,
        };
        storage_handle(backend)
            .aggregate_objects(query, Some(&authorizer))
            .await?
    } else {
        storage_handle(backend)
            .aggregate_objects(query, None)
            .await?
    };
    page_from_storage(page, &response_spec)
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
        required_permissions: Vec<AuthorizationPermission>,
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
        let class = ResourceRef {
            kind: ResourceKind::Class,
            id: class_id,
            attrs: ResourceAttrs {
                collection_id: Some(collection_id),
                name: Some(class_name),
                ..Default::default()
            },
        };
        let collection = ResourceRef {
            kind: ResourceKind::Collection,
            id: collection_id,
            attrs: ResourceAttrs {
                collection_id: Some(collection_id),
                name: Some(collection_name),
                ..Default::default()
            },
        };
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
        required_permissions: Vec<AuthorizationPermission>,
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
        Permissions::CreateObject => {
            let mut resource = ResourceRef::for_permission_on_collection(permission, collection.id);
            resource.attrs.class_id = Some(class.id);
            resource
        }
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
        | Permissions::DeleteTemplate => {
            ResourceRef::for_permission_on_collection(permission, collection.id)
        }
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
    let kind = match error {
        ApiError::PermissionBackendUnavailable(_) => StorageErrorKind::AuthorizationUnavailable,
        ApiError::ServiceUnavailable(_) => StorageErrorKind::Unavailable,
        _ => StorageErrorKind::Internal,
    };
    StorageError::new(kind, message, None)
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
