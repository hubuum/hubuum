use crate::api::response::ApiResponse;
use crate::api::v1::handlers::classes::{
    computed_personal_owner, object_read_page, scope_object_query_to_class,
};
use crate::db::traits::authz::scope_allows;
use crate::db::traits::computed_field::{
    ComputedQuerySnapshot, enrich_objects_with_computed_query_snapshot,
    resolve_computed_query_fields,
};
use crate::db::traits::user::UserSearchBackend;
use crate::db::traits::user::search::{
    count_computed_objects_with_authorized_ids, search_computed_objects_with_authorized_ids,
};
use crate::errors::ApiError;
use crate::extractors::Authenticated;
use crate::models::search::QueryOptions;
use crate::models::{
    HubuumClass, HubuumClassID, HubuumObject, HubuumObjectComputedResponse,
    HubuumObjectReadResponse, Permissions,
};
use crate::pagination::{
    Page, SKIPPED_TOTAL_COUNT, count_query_options, effective_page_limit, encode_cursor,
    known_count_or_skipped, page_request, prepare_db_pagination,
};
use crate::permissions::visibility::{AuthorizedObjectIds, authorize_all_candidates};
use crate::permissions::{
    AppContext, AuthzTarget, PrincipalRef, ResourceAttrs, ResourceKind, ResourceRef,
    authorize_resources,
};
use crate::traits::BackendContext;

enum ComputedListVisibility {
    SqlPushdown,
    Policy(AuthorizedObjectIds),
}

struct ResolvedComputedObjectQuery<'a> {
    pool: &'a AppContext,
    requestor: &'a Authenticated,
    params: &'a QueryOptions,
    personal_owner: Option<i32>,
    snapshot: &'a ComputedQuerySnapshot,
    sorts_by_computed: bool,
}

impl ResolvedComputedObjectQuery<'_> {
    fn search_options(&self) -> Result<QueryOptions, ApiError> {
        if self.sorts_by_computed {
            prepare_db_pagination::<HubuumObjectComputedResponse>(self.params)
        } else {
            prepare_db_pagination::<HubuumObject>(self.params)
        }
    }

    async fn search(
        &self,
        visibility: &ComputedListVisibility,
    ) -> Result<(Vec<HubuumObject>, i64), ApiError> {
        match visibility {
            ComputedListVisibility::SqlPushdown => self.search_with_sql_visibility().await,
            ComputedListVisibility::Policy(authorized_ids) => {
                self.search_with_policy_visibility(authorized_ids).await
            }
        }
    }

    async fn search_with_sql_visibility(&self) -> Result<(Vec<HubuumObject>, i64), ApiError> {
        let total_count = if self.params.include_total {
            self.requestor
                .principal
                .count_objects_with_computed_query_from_backend(
                    self.pool.db_pool(),
                    count_query_options(self.params),
                    self.requestor.scopes(),
                    self.snapshot,
                )
                .await?
        } else {
            SKIPPED_TOTAL_COUNT
        };
        let objects = self
            .requestor
            .principal
            .search_objects_with_computed_query_from_backend(
                self.pool.db_pool(),
                self.search_options()?,
                self.requestor.scopes(),
                self.snapshot,
            )
            .await?;
        Ok((objects, total_count))
    }

    async fn search_with_policy_visibility(
        &self,
        authorized_ids: &AuthorizedObjectIds,
    ) -> Result<(Vec<HubuumObject>, i64), ApiError> {
        let total_count = if self.params.include_total {
            count_computed_objects_with_authorized_ids(
                &self.requestor.principal,
                self.pool.db_pool(),
                count_query_options(self.params),
                self.snapshot,
                authorized_ids,
            )
            .await?
        } else {
            SKIPPED_TOTAL_COUNT
        };
        let objects = search_computed_objects_with_authorized_ids(
            &self.requestor.principal,
            self.pool.db_pool(),
            self.search_options()?,
            self.snapshot,
            authorized_ids,
        )
        .await?;
        Ok((objects, total_count))
    }

    async fn response(
        &self,
        objects: Vec<HubuumObject>,
        total_count: i64,
        include_computed: bool,
    ) -> Result<ApiResponse<Vec<HubuumObjectReadResponse>>, ApiError> {
        if include_computed {
            let enriched = enrich_objects_with_computed_query_snapshot(
                self.pool.db_pool(),
                objects,
                self.personal_owner,
                self.snapshot,
            )
            .await?;
            let page = crate::pagination::finalize_page(enriched, self.params)?;
            return object_read_page(page, total_count, effective_page_limit(self.params)?, true);
        }

        if self.sorts_by_computed {
            return self.raw_sorted_response(objects, total_count).await;
        }

        let page = crate::pagination::finalize_page(objects, self.params)?;
        object_read_page(page, total_count, effective_page_limit(self.params)?, true)
    }

    async fn raw_sorted_response(
        &self,
        objects: Vec<HubuumObject>,
        total_count: i64,
    ) -> Result<ApiResponse<Vec<HubuumObjectReadResponse>>, ApiError> {
        let request = page_request::<HubuumObjectComputedResponse>(self.params)?;
        let (objects, cursor_boundary) = page_items_and_cursor_boundary(objects, request.limit);
        let next_cursor = if let Some(boundary) = cursor_boundary {
            let mut enriched = enrich_objects_with_computed_query_snapshot(
                self.pool.db_pool(),
                vec![boundary],
                self.personal_owner,
                self.snapshot,
            )
            .await?;
            Some(encode_cursor(
                &enriched.pop().ok_or_else(|| {
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
    pool: &AppContext,
    requestor: &Authenticated,
    class: &HubuumClass,
) -> Result<bool, ApiError> {
    let resource = class.to_resource_ref(pool.db_pool()).await?;
    match authorize_resources(
        pool.permission_backend(),
        pool,
        &requestor.principal,
        requestor.scopes(),
        vec![Permissions::ReadObject, Permissions::ReadCollection],
        vec![resource],
    )
    .await
    {
        Ok(()) => Ok(true),
        Err(ApiError::Forbidden(_)) => Ok(false),
        Err(error) => Err(error),
    }
}

async fn authorized_object_ids_in_class(
    pool: &AppContext,
    requestor: &Authenticated,
    class: &HubuumClassID,
) -> Result<AuthorizedObjectIds, ApiError> {
    let mut visibility_query = QueryOptions {
        filters: Vec::new(),
        sort: Vec::new(),
        limit: None,
        cursor: None,
        include_total: false,
    };
    scope_object_query_to_class(&mut visibility_query, class);
    let candidates = requestor
        .principal
        .search_objects_from_backend_with_admin_status(pool, visibility_query, true, None)
        .await?;
    let principal = PrincipalRef::load(pool, &requestor.principal).await?;
    let authorized = authorize_all_candidates(
        pool.permission_backend(),
        &principal,
        candidates,
        vec![Permissions::ReadObject],
        |object| ResourceRef {
            kind: ResourceKind::Object,
            id: object.id,
            attrs: ResourceAttrs {
                collection_id: Some(object.collection_id),
                class_id: Some(object.hubuum_class_id),
                name: Some(object.name.clone()),
                ..Default::default()
            },
        },
    )
    .await?;
    AuthorizedObjectIds::new(authorized.into_iter().map(|object| object.id))
}

async fn computed_list_visibility(
    pool: &AppContext,
    requestor: &Authenticated,
    class: &HubuumClass,
    class_id: &HubuumClassID,
) -> Result<Option<ComputedListVisibility>, ApiError> {
    if pool.permission_backend().supports_sql_visibility_pushdown() {
        return can_list_objects_in_class(pool, requestor, class)
            .await
            .map(|allowed| allowed.then_some(ComputedListVisibility::SqlPushdown));
    }

    let authorized_ids = authorized_object_ids_in_class(pool, requestor, class_id).await?;
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
    pool: &AppContext,
    requestor: &Authenticated,
    class: &HubuumClass,
    mut params: QueryOptions,
    include_computed: bool,
) -> Result<ApiResponse<Vec<HubuumObjectReadResponse>>, ApiError> {
    if !scope_allows(requestor.scopes(), &[Permissions::ReadObject]) {
        return empty_computed_page(&params);
    }

    let class_id = HubuumClassID::new(class.id)?;
    let Some(visibility) = computed_list_visibility(pool, requestor, class, &class_id).await?
    else {
        return empty_computed_page(&params);
    };

    let personal_owner = computed_personal_owner(pool, requestor, class).await?;
    let computed_sorting = params
        .sort
        .iter()
        .any(|sort| sort.field.computed_query().is_some());
    let computed_query_snapshot = resolve_computed_query_fields(
        pool.db_pool(),
        class.id,
        personal_owner,
        &mut params.filters,
        &mut params.sort,
    )
    .await?;

    let query = ResolvedComputedObjectQuery {
        pool,
        requestor,
        params: &params,
        personal_owner,
        snapshot: &computed_query_snapshot,
        sorts_by_computed: computed_sorting,
    };
    let (objects, total_count) = query.search(&visibility).await?;
    query.response(objects, total_count, include_computed).await
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
