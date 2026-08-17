use crate::api::response::ApiResponse;
use crate::api::v1::handlers::classes::{
    computed_personal_owner, object_read_page, scope_object_query_to_class,
};
use crate::errors::ApiError;
use crate::extractors::Authenticated;
use crate::models::search::QueryOptions;
use crate::models::{
    HubuumClass, HubuumClassID, HubuumObject, HubuumObjectComputedResponse,
    HubuumObjectReadResponse, Permissions, TokenScope,
};
use crate::pagination::{
    Page, SKIPPED_TOTAL_COUNT, effective_page_limit, encode_cursor, known_count_or_skipped,
    page_request,
};
use crate::permissions::visibility::{AuthorizedObjectIds, authorize_all_candidates};
use crate::permissions::{AppContext, PrincipalRef, authorize_resources};
use crate::services::catalog as catalog_service;
use crate::services::computed_objects::{
    ComputedObjectAccess, ComputedObjectListResult, list_computed_objects,
};
use crate::services::related_filter_authorization::externally_authorized_related_object_ids;
use crate::storage::ComputedObjectProjection;
use crate::traits::AuthzSubject;
use crate::traits::scope_allows;

enum ComputedListVisibility {
    SqlPushdown,
    Policy(AuthorizedObjectIds),
}

struct ResolvedComputedObjectQuery<'a> {
    context: &'a AppContext,
    requestor: &'a Authenticated,
    params: &'a QueryOptions,
    class_id: i32,
    personal_owner: Option<i32>,
    sorts_by_computed: bool,
}

impl ResolvedComputedObjectQuery<'_> {
    async fn search(
        &self,
        visibility: &ComputedListVisibility,
        include_computed: bool,
    ) -> Result<ComputedObjectListResult, ApiError> {
        let projection = if include_computed {
            ComputedObjectProjection::All
        } else if self.sorts_by_computed {
            ComputedObjectProjection::CursorBoundary {
                page_limit: effective_page_limit(self.params)?,
            }
        } else {
            ComputedObjectProjection::None
        };
        let access = match visibility {
            ComputedListVisibility::SqlPushdown => ComputedObjectAccess::Storage {
                principal_id: self.requestor.principal.id(),
                is_admin: AuthzSubject::is_admin(&self.requestor.principal, self.context).await?,
                scope: self.requestor.scopes(),
            },
            ComputedListVisibility::Policy(object_ids) => {
                ComputedObjectAccess::AuthorizedObjectIds {
                    principal_id: self.requestor.principal.id(),
                    object_ids,
                }
            }
        };
        list_computed_objects(
            self.context,
            self.class_id,
            self.personal_owner,
            self.params.clone(),
            access,
            projection,
        )
        .await
    }

    async fn response(
        &self,
        result: ComputedObjectListResult,
        include_computed: bool,
    ) -> Result<ApiResponse<Vec<HubuumObjectReadResponse>>, ApiError> {
        let total_count = result.total.unwrap_or(SKIPPED_TOTAL_COUNT);
        let resolved_options = result.resolved_options;
        if include_computed {
            let page = crate::pagination::finalize_page(result.computed, &resolved_options)?;
            return object_read_page(page, total_count, effective_page_limit(self.params)?, true);
        }

        if self.sorts_by_computed {
            return self
                .raw_sorted_response(
                    result.objects,
                    result.computed,
                    total_count,
                    &resolved_options,
                )
                .await;
        }

        let page = crate::pagination::finalize_page(result.objects, self.params)?;
        object_read_page(page, total_count, effective_page_limit(self.params)?, true)
    }

    async fn raw_sorted_response(
        &self,
        objects: Vec<HubuumObject>,
        mut computed: Vec<HubuumObjectComputedResponse>,
        total_count: i64,
        resolved_options: &QueryOptions,
    ) -> Result<ApiResponse<Vec<HubuumObjectReadResponse>>, ApiError> {
        let request = page_request::<HubuumObjectComputedResponse>(resolved_options)?;
        let (objects, cursor_boundary) = page_items_and_cursor_boundary(objects, request.limit);
        let next_cursor = if cursor_boundary.is_some() {
            Some(encode_cursor(
                &computed.pop().ok_or_else(|| {
                    ApiError::InternalServerError(
                        "Computed sort cursor boundary was not enriched".to_string(),
                    )
                })?,
                &request.sorts,
            )?)
        } else {
            None
        };
        object_read_page(
            Page {
                items: objects,
                next_cursor,
            },
            total_count,
            request.limit,
            true,
        )
    }
}

async fn can_list_objects_in_class(
    context: &AppContext,
    requestor: &Authenticated,
    class: &HubuumClass,
) -> Result<bool, ApiError> {
    let required = [Permissions::ReadObject, Permissions::ReadCollection];
    if !scope_allows(requestor.scopes(), &required) {
        return Ok(false);
    }

    let has_class_or_collection_scope = requestor
        .scopes()
        .and_then(TokenScope::resource_ids)
        .is_some_and(|ids| ids.has_collection_or_class_entries());
    if has_class_or_collection_scope || requestor.scopes().is_none() {
        return Ok(authorize_resources(
            context.permission_backend(),
            &context,
            &requestor.principal,
            requestor.scopes(),
            vec![Permissions::ReadObject, Permissions::ReadCollection],
            vec![class.authorization_resource()],
        )
        .await
        .is_ok());
    }

    let mut visibility_query = QueryOptions::new(Vec::new(), Vec::new(), Some(1), None, false)?;
    scope_object_query_to_class(&mut visibility_query, &HubuumClassID::new(class.id)?)?;
    let is_admin = crate::traits::AuthzSubject::is_admin(&requestor.principal, context).await?;
    let (visible_objects, _) = catalog_service::list_objects(
        context,
        requestor.principal.id(),
        is_admin,
        requestor.scopes(),
        visibility_query,
    )
    .await?;
    Ok(!visible_objects.is_empty())
}

async fn authorized_object_ids_in_class(
    context: &AppContext,
    requestor: &Authenticated,
    class: &HubuumClassID,
) -> Result<AuthorizedObjectIds, ApiError> {
    let mut visibility_query = QueryOptions::new(Vec::new(), Vec::new(), None, None, false)?;
    scope_object_query_to_class(&mut visibility_query, class)?;
    let (candidates, _) = catalog_service::list_objects(
        context,
        requestor.principal.id(),
        true,
        None,
        visibility_query,
    )
    .await?;
    let principal = PrincipalRef::load(&context, &requestor.principal).await?;
    let authorized = authorize_all_candidates(
        context.permission_backend(),
        &principal,
        candidates,
        requestor.scopes(),
        vec![Permissions::ReadObject],
        HubuumObject::authorization_resource,
    )
    .await?;
    AuthorizedObjectIds::new(authorized.into_iter().map(|object| object.id))
}

async fn computed_list_visibility(
    context: &AppContext,
    requestor: &Authenticated,
    class: &HubuumClass,
    class_id: &HubuumClassID,
    params: &QueryOptions,
) -> Result<Option<ComputedListVisibility>, ApiError> {
    if context
        .permission_backend()
        .supports_storage_visibility_filtering()
    {
        return can_list_objects_in_class(context, requestor, class)
            .await
            .map(|allowed| allowed.then_some(ComputedListVisibility::SqlPushdown));
    }

    let authorized_ids = authorized_object_ids_in_class(context, requestor, class_id).await?;
    let principal = PrincipalRef::load(&context, &requestor.principal).await?;
    let related_ids = externally_authorized_related_object_ids(
        context,
        context.permission_backend(),
        &principal,
        requestor.scopes(),
        params.filters(),
    )
    .await?;
    let authorized_ids = match related_ids {
        Some(related_ids) => authorized_ids.intersection(&related_ids),
        None => authorized_ids,
    };
    Ok((!authorized_ids.is_empty()).then_some(ComputedListVisibility::Policy(authorized_ids)))
}

fn empty_computed_page(
    params: &QueryOptions,
) -> Result<ApiResponse<Vec<HubuumObjectReadResponse>>, ApiError> {
    object_read_page(
        Page::<HubuumObjectComputedResponse> {
            items: Vec::new(),
            next_cursor: None,
        },
        known_count_or_skipped(params, 0),
        effective_page_limit(params)?,
        true,
    )
}

pub(super) async fn list_objects(
    context: &AppContext,
    requestor: &Authenticated,
    class: &HubuumClass,
    params: QueryOptions,
    include_computed: bool,
) -> Result<ApiResponse<Vec<HubuumObjectReadResponse>>, ApiError> {
    if !scope_allows(requestor.scopes(), &[Permissions::ReadObject]) {
        return empty_computed_page(&params);
    }

    let class_id = HubuumClassID::new(class.id)?;
    let Some(visibility) =
        computed_list_visibility(context, requestor, class, &class_id, &params).await?
    else {
        return empty_computed_page(&params);
    };

    let personal_owner = computed_personal_owner(context, requestor, class).await?;
    let computed_sorting = params
        .sort()
        .iter()
        .any(|sort| sort.field.computed_query().is_some());
    let query = ResolvedComputedObjectQuery {
        context,
        requestor,
        params: &params,
        class_id: class.id,
        personal_owner,
        sorts_by_computed: computed_sorting,
    };
    let result = query.search(&visibility, include_computed).await?;
    query.response(result, include_computed).await
}

fn page_items_and_cursor_boundary<T: Clone>(
    mut items: Vec<T>,
    limit: usize,
) -> (Vec<T>, Option<T>) {
    if items.len() <= limit {
        return (items, None);
    }
    items.truncate(limit);
    let boundary = items.last().cloned();
    (items, boundary)
}

#[cfg(test)]
mod tests {
    use super::page_items_and_cursor_boundary;

    #[test]
    fn raw_computed_page_selects_only_its_cursor_boundary() {
        let (items, boundary) = page_items_and_cursor_boundary(vec![1, 2, 3, 4], 2);

        assert_eq!(items, vec![1, 2]);
        assert_eq!(boundary, Some(2));
    }
}
