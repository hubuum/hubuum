use super::*;
use crate::models::RelatedObjectForRootRow;
use crate::models::permissions::PermissionFilter;
use crate::models::search::{
    DEFAULT_RELATED_FILTER_DEPTH, Operator, ParsedQueryParamExt, ParsedQueryParamSqlExt,
    RelatedClassField, RelatedFilterTarget, RelatedObjectField, SQLComponent, SQLValue,
};
use crate::models::token_scope::TokenScope;
use crate::permissions::visibility::{
    AuthorizedObjectIds, authorize_all_candidates, authorize_resource_permissions,
};
use crate::permissions::{PermissionBackend, PrincipalRef};
use crate::storage::postgres::operations::authz::scope_allows;
use crate::storage::postgres::operations::computed_field::{
    ComputedQuerySnapshot, computed_filter_predicate, object_cursor_sql_fields,
};
use crate::storage::postgres::operations::relations::{
    object_authorization_resources, object_relation_authorization_resources,
};
use crate::storage::postgres::operations::resource_scope::{
    class_scope_predicate, collection_scope_predicate, object_scope_predicate, resource_scope_ids,
};
use crate::storage::postgres::operations::search::{
    JsonPredicateExt, JsonSqlPredicate, dynamic_sql_predicate,
};
use crate::traits::PrincipalIdAccessor;
use crate::traits::{CursorPaginated, CursorSqlMapping};
use crate::utilities::extensions::CustomStringExtensions;
use diesel::BoolExpressionMethods;
use diesel_async::RunQueryDsl;
use std::collections::{BTreeMap, HashSet};

const MAX_EXTERNAL_RELATED_FILTER_TARGETS: usize = 1_000;
const MAX_EXTERNAL_RELATED_FILTER_OBJECTS: usize = 10_000;
const MAX_EXTERNAL_RELATED_FILTER_RELATIONS: usize = 20_000;

#[derive(diesel::QueryableByName)]
struct CountRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    count: i64,
}

#[derive(Debug, Clone)]
struct RawSqlQuerySpec {
    sql: String,
    bind_variables: Vec<SQLValue>,
}

impl RawSqlQuerySpec {
    fn into_count_query(self, alias: &str) -> Self {
        Self {
            sql: format!("SELECT COUNT(*) AS count FROM ({}) AS {alias}", self.sql),
            bind_variables: self.bind_variables,
        }
    }

    fn into_indexed_sql(self) -> Self {
        Self {
            sql: self.sql.replace_question_mark_with_indexed_n(),
            bind_variables: self.bind_variables,
        }
    }
}

pub struct ObjectQueryPlan<'a>(ObjectQueryPlanKind<'a>);

enum ObjectQueryPlanKind<'a> {
    Ordinary(QueryOptions),
    Computed {
        options: QueryOptions,
        snapshot: &'a ComputedQuerySnapshot,
        authorized_object_ids: Option<&'a AuthorizedObjectIds>,
    },
}

#[derive(Clone, Copy)]
enum ObjectQueryMode<'a> {
    Ordinary,
    Computed {
        snapshot: &'a ComputedQuerySnapshot,
        authorized_object_ids: Option<&'a AuthorizedObjectIds>,
    },
}

impl<'a> ObjectQueryMode<'a> {
    fn snapshot(self) -> Result<&'a ComputedQuerySnapshot, ApiError> {
        match self {
            Self::Computed { snapshot, .. } => Ok(snapshot),
            Self::Ordinary => Err(ApiError::BadRequest(
                "Computed object queries require a resolved query plan".to_string(),
            )),
        }
    }

    fn authorized_object_ids(self) -> Option<&'a AuthorizedObjectIds> {
        match self {
            Self::Computed {
                authorized_object_ids,
                ..
            } => authorized_object_ids,
            Self::Ordinary => None,
        }
    }
}

impl<'a> ObjectQueryPlan<'a> {
    fn ordinary(options: QueryOptions) -> Result<Self, ApiError> {
        let has_computed_fields = options
            .filters
            .iter()
            .any(|filter| filter.field.computed_query().is_some())
            || options
                .sort
                .iter()
                .any(|sort| sort.field.computed_query().is_some());
        if has_computed_fields {
            return Err(ApiError::BadRequest(
                "Computed object queries require a resolved query plan".to_string(),
            ));
        }
        Ok(Self(ObjectQueryPlanKind::Ordinary(options)))
    }

    fn computed(options: QueryOptions, snapshot: &'a ComputedQuerySnapshot) -> Self {
        Self(ObjectQueryPlanKind::Computed {
            options,
            snapshot,
            authorized_object_ids: None,
        })
    }

    fn computed_for_authorized_objects(
        options: QueryOptions,
        snapshot: &'a ComputedQuerySnapshot,
        authorized_object_ids: &'a AuthorizedObjectIds,
    ) -> Self {
        Self(ObjectQueryPlanKind::Computed {
            options,
            snapshot,
            authorized_object_ids: Some(authorized_object_ids),
        })
    }

    fn into_parts(self) -> (QueryOptions, ObjectQueryMode<'a>) {
        match self.0 {
            ObjectQueryPlanKind::Ordinary(options) => (options, ObjectQueryMode::Ordinary),
            ObjectQueryPlanKind::Computed {
                options,
                snapshot,
                authorized_object_ids,
            } => (
                options,
                ObjectQueryMode::Computed {
                    snapshot,
                    authorized_object_ids,
                },
            ),
        }
    }
}

struct RelatedFilterGroup<'a> {
    class_filter: &'a ParsedQueryParam,
    object_filters: Vec<(&'a ParsedQueryParam, RelatedObjectField)>,
    max_depth: i32,
}

fn related_filter_groups(
    filters: &[ParsedQueryParam],
) -> Result<Vec<RelatedFilterGroup<'_>>, ApiError> {
    #[derive(Default)]
    struct PendingGroup<'a> {
        class_filter: Option<&'a ParsedQueryParam>,
        object_filters: Vec<(&'a ParsedQueryParam, RelatedObjectField)>,
        max_depth: Option<i32>,
    }

    let mut groups = BTreeMap::<&str, PendingGroup<'_>>::new();
    for filter in filters {
        let Some(field) = filter.field.related_query() else {
            continue;
        };
        let group = groups.entry(field.alias()).or_default();
        match field.target() {
            RelatedFilterTarget::Class(_) => group.class_filter = Some(filter),
            RelatedFilterTarget::Object(object_field) => {
                group.object_filters.push((filter, object_field));
            }
            RelatedFilterTarget::Depth => {
                group.max_depth = Some(filter.value.parse::<i32>().map_err(|_| {
                    ApiError::BadRequest("Related filter depth must be an integer".to_string())
                })?);
            }
        }
    }

    groups
        .into_iter()
        .map(|(alias, group)| {
            Ok(RelatedFilterGroup {
                class_filter: group.class_filter.ok_or_else(|| {
                    ApiError::BadRequest(format!(
                        "Related filter group '{alias}' requires a class selector"
                    ))
                })?,
                object_filters: group.object_filters,
                max_depth: group
                    .max_depth
                    .unwrap_or(i32::from(DEFAULT_RELATED_FILTER_DEPTH)),
            })
        })
        .collect()
}

async fn related_object_filter_predicate<U>(
    user: &U,
    pool: &impl crate::storage::StorageContext,
    filters: &[ParsedQueryParam],
    is_admin: bool,
    scopes: Option<&TokenScope>,
) -> Result<Option<JsonSqlPredicate>, ApiError>
where
    U: UserCollectionAccessors + ?Sized,
{
    let groups = related_filter_groups(filters)?;
    if groups.is_empty() {
        return Ok(None);
    }
    debug!(
        message = "Planning related-object filters",
        authorization = "sql_pushdown",
        group_count = groups.len(),
        max_depths = ?groups.iter().map(|group| group.max_depth).collect::<Vec<_>>()
    );

    let graph_permissions =
        PermissionsList::new([Permissions::ReadObject, Permissions::ReadObjectRelation]);
    let graph_collection_ids = user
        .load_collections_with_permissions_with_admin_status(
            pool,
            &graph_permissions,
            is_admin,
            scopes,
        )
        .await?
        .into_iter()
        .map(|collection| collection.id)
        .collect::<Vec<_>>();

    let class_permissions =
        PermissionsList::new([Permissions::ReadClass, Permissions::ReadCollection]);
    let class_collection_ids = user
        .load_collections_with_permissions_with_admin_status(
            pool,
            &class_permissions,
            is_admin,
            scopes,
        )
        .await?
        .into_iter()
        .map(|collection| collection.id)
        .collect::<Vec<_>>();

    if graph_collection_ids.is_empty() || class_collection_ids.is_empty() {
        return dynamic_sql_predicate(SQLComponent {
            sql: "FALSE".to_string(),
            bind_variables: Vec::new(),
        })
        .map(Some);
    }

    let component = build_related_object_filter_sql(
        &groups,
        &graph_collection_ids,
        &class_collection_ids,
        scopes,
    )?;
    dynamic_sql_predicate(component).map(Some)
}

/// Dependencies and caller scope for external-policy related filtering.
pub(crate) struct ExternalRelatedFilterAuthorization<'a> {
    pool: &'a PostgresPool,
    permission_backend: &'a dyn PermissionBackend,
    principal: &'a PrincipalRef,
    scopes: Option<&'a TokenScope>,
}

impl<'a> ExternalRelatedFilterAuthorization<'a> {
    pub(crate) fn new(
        storage: &'a impl crate::storage::StorageContext,
        permission_backend: &'a dyn PermissionBackend,
        principal: &'a PrincipalRef,
        scopes: Option<&'a TokenScope>,
    ) -> Self {
        Self {
            pool: crate::storage::context::postgres_pool(storage),
            permission_backend,
            principal,
            scopes,
        }
    }
}

/// Resolve related-object matches without relying on SQL permission joins.
///
/// This is the external-policy counterpart to [`related_object_filter_predicate`].
/// Target objects are loaded without local ACL filtering and then authorized.
/// From those targets, a bounded breadth-first traversal authorizes relations
/// and newly encountered objects before adding them to the next frontier. A
/// candidate is retained when a fully visible path reaches a target in every
/// named group.
pub(crate) async fn externally_authorized_related_object_ids<U>(
    user: &U,
    filters: &[ParsedQueryParam],
    authorization: ExternalRelatedFilterAuthorization<'_>,
) -> Result<Option<AuthorizedObjectIds>, ApiError>
where
    U: UserSearchBackend + ?Sized,
{
    let groups = related_filter_groups(filters)?;
    if groups.is_empty() {
        return Ok(None);
    }
    debug!(
        message = "Planning related-object filters",
        authorization = "external_policy",
        group_count = groups.len(),
        max_depths = ?groups.iter().map(|group| group.max_depth).collect::<Vec<_>>()
    );
    if !scope_allows(
        authorization.scopes,
        &[
            Permissions::ReadCollection,
            Permissions::ReadClass,
            Permissions::ReadObject,
            Permissions::ReadObjectRelation,
        ],
    ) {
        return Ok(Some(AuthorizedObjectIds::empty()));
    }

    let mut intersection = None::<HashSet<i32>>;
    for group in groups {
        let Some(target_class) =
            load_related_target_class(authorization.pool, group.class_filter).await?
        else {
            return Ok(Some(AuthorizedObjectIds::empty()));
        };
        let target_class_resource = target_class.authorization_resource();
        let target_class_is_visible = authorize_resource_permissions(
            authorization.permission_backend,
            authorization.principal,
            &target_class_resource,
            authorization.scopes,
            &[Permissions::ReadClass, Permissions::ReadCollection],
        )
        .await?;
        if !target_class_is_visible {
            return Ok(Some(AuthorizedObjectIds::empty()));
        }

        let mut target_query = related_target_query(&group, target_class.id);
        target_query.limit = Some(MAX_EXTERNAL_RELATED_FILTER_TARGETS + 1);
        let target_candidates = user
            .search_objects_from_backend_with_admin_status(
                authorization.pool,
                target_query,
                true,
                None,
            )
            .await?;
        RelatedTraversalResource::TargetObjects.ensure_count(target_candidates.len())?;
        let target_objects = authorize_all_candidates(
            authorization.permission_backend,
            authorization.principal,
            target_candidates,
            authorization.scopes,
            vec![Permissions::ReadObject],
            HubuumObject::authorization_resource,
        )
        .await?;
        let target_ids = target_objects
            .into_iter()
            .map(|object| object.id)
            .collect::<Vec<_>>();
        if target_ids.is_empty() {
            return Ok(Some(AuthorizedObjectIds::empty()));
        }

        let group_matches =
            externally_authorized_related_group_ids(&authorization, &target_ids, group.max_depth)
                .await?;
        intersection = Some(match intersection {
            None => group_matches,
            Some(existing) => existing.intersection(&group_matches).copied().collect(),
        });
        if intersection
            .as_ref()
            .is_some_and(|matches| matches.is_empty())
        {
            break;
        }
    }

    AuthorizedObjectIds::new(intersection.unwrap_or_default()).map(Some)
}

async fn load_related_target_class(
    pool: &impl crate::storage::StorageContext,
    class_filter: &ParsedQueryParam,
) -> Result<Option<HubuumClass>, ApiError> {
    use crate::schema::hubuumclass::dsl::{hubuumclass, id, name};
    use diesel::OptionalExtension;

    let class_field = class_filter
        .field
        .related_query()
        .and_then(|field| match field.target() {
            RelatedFilterTarget::Class(class_field) => Some(class_field),
            _ => None,
        })
        .ok_or_else(|| {
            ApiError::InternalServerError(
                "Related filter group lost its class selector".to_string(),
            )
        })?;

    with_connection(pool, async |conn| match class_field {
        RelatedClassField::Id => {
            let values = class_filter.value_as_integer()?;
            if values.len() != 1 {
                return Err(ApiError::BadRequest(
                    "related.<alias>.class.id requires exactly one integer".to_string(),
                ));
            }
            hubuumclass
                .filter(id.eq(values[0]))
                .first::<HubuumClass>(conn)
                .await
                .optional()
                .map_err(Into::into)
        }
        RelatedClassField::Name => hubuumclass
            .filter(name.eq(&class_filter.value))
            .first::<HubuumClass>(conn)
            .await
            .optional()
            .map_err(Into::into),
    })
    .await
}

fn related_target_query(group: &RelatedFilterGroup<'_>, class_id: i32) -> QueryOptions {
    let mut filters = Vec::with_capacity(group.object_filters.len() + 1);
    filters.push(ParsedQueryParam {
        field: FilterField::ClassId,
        operator: SearchOperator::Equals { is_negated: false },
        value: class_id.to_string(),
    });
    filters.extend(group.object_filters.iter().map(|(filter, field)| {
        let mut filter = (*filter).clone();
        filter.field = match field {
            RelatedObjectField::Id => FilterField::Id,
            RelatedObjectField::Name => FilterField::Name,
            RelatedObjectField::Description => FilterField::Description,
            RelatedObjectField::CollectionId => FilterField::Collections,
            RelatedObjectField::CreatedAt => FilterField::CreatedAt,
            RelatedObjectField::UpdatedAt => FilterField::UpdatedAt,
            RelatedObjectField::Revision => FilterField::Revision,
            RelatedObjectField::JsonData => FilterField::JsonData,
        };
        filter
    }));
    QueryOptions {
        filters,
        sort: Vec::new(),
        limit: None,
        cursor: None,
        include_total: false,
    }
}

#[derive(Clone, Copy)]
enum RelatedTraversalResource {
    TargetObjects,
    Objects,
    ObjectRelations,
}

impl RelatedTraversalResource {
    const fn label(self) -> &'static str {
        match self {
            Self::TargetObjects => "target objects",
            Self::Objects => "objects",
            Self::ObjectRelations => "object relations",
        }
    }

    const fn limit(self) -> usize {
        match self {
            Self::TargetObjects => MAX_EXTERNAL_RELATED_FILTER_TARGETS,
            Self::Objects => MAX_EXTERNAL_RELATED_FILTER_OBJECTS,
            Self::ObjectRelations => MAX_EXTERNAL_RELATED_FILTER_RELATIONS,
        }
    }

    fn ensure_count(self, candidate_count: usize) -> Result<(), ApiError> {
        let limit = self.limit();
        if candidate_count <= limit {
            return Ok(());
        }
        Err(ApiError::BadRequest(format!(
            "Related filter traversal exceeds the {limit} {} safety limit; narrow the target filters or reduce depth",
            self.label()
        )))
    }
}

struct RelatedTraversalBudget {
    examined_objects: usize,
    examined_relations: usize,
}

impl RelatedTraversalBudget {
    fn new(target_count: usize) -> Result<Self, ApiError> {
        RelatedTraversalResource::Objects.ensure_count(target_count)?;
        Ok(Self {
            examined_objects: target_count,
            examined_relations: 0,
        })
    }

    fn relation_query_limit(&self) -> usize {
        RelatedTraversalResource::ObjectRelations
            .limit()
            .saturating_sub(self.examined_relations)
            .saturating_add(1)
    }

    fn record_objects(&mut self, count: usize) -> Result<(), ApiError> {
        self.examined_objects = checked_related_traversal_total(
            RelatedTraversalResource::Objects,
            self.examined_objects,
            count,
        )?;
        Ok(())
    }

    fn record_relations(&mut self, count: usize) -> Result<(), ApiError> {
        self.examined_relations = checked_related_traversal_total(
            RelatedTraversalResource::ObjectRelations,
            self.examined_relations,
            count,
        )?;
        Ok(())
    }
}

fn checked_related_traversal_total(
    resource: RelatedTraversalResource,
    current: usize,
    additional: usize,
) -> Result<usize, ApiError> {
    let total = current.saturating_add(additional);
    resource.ensure_count(total)?;
    Ok(total)
}

async fn load_object_relations_touching_frontier(
    pool: &impl crate::storage::StorageContext,
    frontier: &HashSet<i32>,
    seen_relation_ids: &HashSet<i32>,
    limit: usize,
) -> Result<Vec<HubuumObjectRelation>, ApiError> {
    use crate::schema::hubuumobject_relation::dsl::{
        from_hubuum_object_id, hubuumobject_relation, id, to_hubuum_object_id,
    };
    use diesel::dsl::not;

    if frontier.is_empty() || limit == 0 {
        return Ok(Vec::new());
    }

    let mut frontier = frontier.iter().copied().collect::<Vec<_>>();
    frontier.sort_unstable();
    let mut seen_relation_ids = seen_relation_ids.iter().copied().collect::<Vec<_>>();
    seen_relation_ids.sort_unstable();
    let mut query = hubuumobject_relation
        .filter(
            from_hubuum_object_id
                .eq_any(&frontier)
                .or(to_hubuum_object_id.eq_any(&frontier)),
        )
        .into_boxed();
    if !seen_relation_ids.is_empty() {
        query = query.filter(not(id.eq_any(&seen_relation_ids)));
    }
    let limit = i64::try_from(limit).map_err(|_| {
        ApiError::InternalServerError("Related filter traversal limit overflow".to_string())
    })?;

    with_connection(pool, async |conn| {
        query
            .order(id.asc())
            .limit(limit)
            .load::<HubuumObjectRelation>(conn)
            .await
    })
    .await
}

async fn externally_authorized_related_group_ids(
    authorization: &ExternalRelatedFilterAuthorization<'_>,
    target_ids: &[i32],
    max_depth: i32,
) -> Result<HashSet<i32>, ApiError> {
    let mut visible_objects = target_ids.iter().copied().collect::<HashSet<_>>();
    let mut examined_objects = visible_objects.clone();
    let mut budget = RelatedTraversalBudget::new(examined_objects.len())?;
    let mut frontier = visible_objects.clone();
    let mut seen_relation_ids = HashSet::new();
    let mut matches = HashSet::new();

    for _ in 0..max_depth {
        if frontier.is_empty() {
            break;
        }

        let relation_candidates = load_object_relations_touching_frontier(
            authorization.pool,
            &frontier,
            &seen_relation_ids,
            budget.relation_query_limit(),
        )
        .await?;
        budget.record_relations(relation_candidates.len())?;
        if relation_candidates.is_empty() {
            break;
        }
        seen_relation_ids.extend(relation_candidates.iter().map(|relation| relation.id));

        let relation_resources =
            object_relation_authorization_resources(authorization.pool, &relation_candidates)
                .await?;
        let relation_candidates = relation_candidates
            .into_iter()
            .zip(relation_resources)
            .collect::<Vec<_>>();
        let visible_relations = authorize_all_candidates(
            authorization.permission_backend,
            authorization.principal,
            relation_candidates,
            authorization.scopes,
            vec![Permissions::ReadObjectRelation],
            |(_, resource)| resource.clone(),
        )
        .await?
        .into_iter()
        .map(|(relation, _)| relation)
        .collect::<Vec<_>>();

        let mut new_object_ids = visible_relations
            .iter()
            .flat_map(|relation| [relation.from_hubuum_object_id, relation.to_hubuum_object_id])
            .filter(|object_id| !examined_objects.contains(object_id))
            .collect::<Vec<_>>();
        new_object_ids.sort_unstable();
        new_object_ids.dedup();
        budget.record_objects(new_object_ids.len())?;
        examined_objects.extend(new_object_ids.iter().copied());

        let object_resources =
            object_authorization_resources(authorization.pool, &new_object_ids).await?;
        let object_candidates = new_object_ids
            .into_iter()
            .zip(object_resources)
            .collect::<Vec<_>>();
        let newly_visible_objects = authorize_all_candidates(
            authorization.permission_backend,
            authorization.principal,
            object_candidates,
            authorization.scopes,
            vec![Permissions::ReadObject],
            |(_, resource)| resource.clone(),
        )
        .await?
        .into_iter()
        .map(|(object_id, _)| object_id)
        .collect::<HashSet<_>>();
        visible_objects.extend(newly_visible_objects.iter().copied());

        let mut next_frontier = HashSet::new();
        for relation in visible_relations {
            for (source_id, target_id) in [
                (relation.from_hubuum_object_id, relation.to_hubuum_object_id),
                (relation.to_hubuum_object_id, relation.from_hubuum_object_id),
            ] {
                if source_id != target_id
                    && frontier.contains(&source_id)
                    && visible_objects.contains(&target_id)
                {
                    matches.insert(target_id);
                    if newly_visible_objects.contains(&target_id) {
                        next_frontier.insert(target_id);
                    }
                }
            }
        }
        frontier = next_frontier;
    }

    Ok(matches)
}

pub(crate) async fn search_computed_objects_with_authorized_ids<U>(
    user: &U,
    pool: &impl crate::storage::StorageContext,
    query_options: QueryOptions,
    snapshot: &ComputedQuerySnapshot,
    authorized_object_ids: &AuthorizedObjectIds,
) -> Result<Vec<HubuumObject>, ApiError>
where
    U: UserSearchBackend + ?Sized,
{
    let plan = ObjectQueryPlan::computed_for_authorized_objects(
        query_options,
        snapshot,
        authorized_object_ids,
    );
    user.search_objects_from_backend_with_query_plan(pool, plan, true, None)
        .await
}

pub(crate) async fn count_computed_objects_with_authorized_ids<U>(
    user: &U,
    pool: &impl crate::storage::StorageContext,
    query_options: QueryOptions,
    snapshot: &ComputedQuerySnapshot,
    authorized_object_ids: &AuthorizedObjectIds,
) -> Result<i64, ApiError>
where
    U: UserSearchBackend + ?Sized,
{
    let plan = ObjectQueryPlan::computed_for_authorized_objects(
        query_options,
        snapshot,
        authorized_object_ids,
    );
    user.count_objects_from_backend_with_query_plan(pool, plan, true, None)
        .await
}

macro_rules! bind_raw_sql_query {
    ($spec:expr) => {{
        let spec = $spec.into_indexed_sql();
        let mut query = diesel::sql_query(spec.sql).into_boxed();
        for bind_var in spec.bind_variables {
            query = match bind_var {
                SQLValue::Integer(i) => query.bind::<diesel::sql_types::Integer, _>(i),
                SQLValue::BigInteger(i) => query.bind::<diesel::sql_types::BigInt, _>(i),
                SQLValue::String(s) => query.bind::<diesel::sql_types::Text, _>(s),
                SQLValue::Boolean(b) => query.bind::<diesel::sql_types::Bool, _>(b),
                SQLValue::Date(d) => query.bind::<diesel::sql_types::Timestamp, _>(d),
            };
        }
        query
    }};
}

pub trait UserSearchBackend: UserCollectionAccessors {
    async fn search_collections_from_backend(
        &self,
        pool: &impl crate::storage::StorageContext,
        query_options: QueryOptions,
        scopes: Option<&TokenScope>,
    ) -> Result<Vec<Collection>, ApiError> {
        let is_admin = self.is_admin(pool).await?;
        self.search_collections_from_backend_with_admin_status(
            pool,
            query_options,
            is_admin,
            scopes,
        )
        .await
    }

    async fn count_collections_from_backend(
        &self,
        pool: &impl crate::storage::StorageContext,
        query_options: QueryOptions,
        scopes: Option<&TokenScope>,
    ) -> Result<i64, ApiError> {
        let is_admin = self.is_admin(pool).await?;
        self.count_collections_from_backend_with_admin_status(pool, query_options, is_admin, scopes)
            .await
    }

    async fn search_collections_from_backend_with_admin_status(
        &self,
        pool: &impl crate::storage::StorageContext,
        query_options: QueryOptions,
        is_admin: bool,
        scopes: Option<&TokenScope>,
    ) -> Result<Vec<Collection>, ApiError> {
        // Fail-closed: a scoped token must carry the resource read permission.
        if !scope_allows(scopes, &[Permissions::ReadCollection]) {
            return Ok(Vec::new());
        }
        use crate::schema::collection_closure::dsl::{
            ancestor_collection_id, collection_closure, descendant_collection_id,
        };
        use crate::schema::collections::dsl::{
            collections, created_at as collection_created_at,
            description as collection_description, id as collection_id, name as collection_name,
            revision as collection_revision, updated_at as collection_updated_at,
        };
        use crate::schema::permissions::dsl::{
            collection_id as permissions_collection_id, group_id, permissions,
        };
        let query_params = query_options.filters.clone();

        debug!(
            message = "Searching collections",
            stage = "Starting",
            user_id = self.principal_id(),
            query_params = ?query_params
        );

        // Validate any `permissions` query filters. The requested value does not
        // narrow collection visibility beyond the ReadCollection baseline applied
        // below — a collection is a collection, and ReadCollection is what gates
        // seeing it.
        query_params.permissions()?;

        let mut base_query = if is_admin {
            collections.into_boxed()
        } else {
            let group_id_subquery = self.group_ids_subquery();

            // Visibility requires the principal's groups to actually hold
            // ReadCollection (has_read_collection) on the collection, not merely to
            // have *some* permission row for it.
            let permission_subquery = Permissions::ReadCollection.create_boxed_filter(
                permissions
                    .filter(group_id.eq_any(group_id_subquery))
                    .into_boxed(),
                true,
            );

            collections
                .filter(
                    collection_id.eq_any(
                        permission_subquery
                            .inner_join(
                                collection_closure
                                    .on(permissions_collection_id.eq(ancestor_collection_id)),
                            )
                            .select(descendant_collection_id)
                            .distinct(),
                    ),
                )
                .into_boxed()
        };
        if let Some(scope) = resource_scope_ids(scopes) {
            base_query = base_query.filter(collection_scope_predicate(scope));
        }

        for param in query_params {
            use crate::{date_search, numeric_search, revision_search, string_search};
            let operator = param.operator.clone();
            match param.field {
                FilterField::Id => numeric_search!(base_query, param, operator, collection_id),
                FilterField::CreatedAt => {
                    date_search!(base_query, param, operator, collection_created_at)
                }
                FilterField::UpdatedAt => {
                    date_search!(base_query, param, operator, collection_updated_at)
                }
                FilterField::Revision => {
                    revision_search!(base_query, param, operator, collection_revision)
                }
                FilterField::Name => {
                    string_search!(base_query, param, operator, collection_name)
                }
                FilterField::Description => {
                    string_search!(base_query, param, operator, collection_description)
                }
                FilterField::Permissions => {}
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for collections",
                        param.field
                    )));
                }
            }
        }

        crate::apply_query_options!(base_query, query_options, Collection);

        with_connection(pool, async |conn| {
            base_query
                .select(collections::all_columns())
                .load::<Collection>(conn)
                .await
        })
        .await
    }

    async fn count_collections_from_backend_with_admin_status(
        &self,
        pool: &impl crate::storage::StorageContext,
        query_options: QueryOptions,
        is_admin: bool,
        scopes: Option<&TokenScope>,
    ) -> Result<i64, ApiError> {
        use crate::schema::collection_closure::dsl::{
            ancestor_collection_id, collection_closure, descendant_collection_id,
        };
        use crate::schema::collections::dsl::{
            collections, created_at as collection_created_at,
            description as collection_description, id as collection_id, name as collection_name,
            revision as collection_revision, updated_at as collection_updated_at,
        };
        use crate::schema::permissions::dsl::{
            collection_id as permissions_collection_id, group_id, permissions,
        };

        // Fail-closed: a scoped token must carry the resource read permission.
        if !scope_allows(scopes, &[Permissions::ReadCollection]) {
            return Ok(0);
        }

        let query_params = query_options.filters.clone();
        // Validate any `permissions` query filters. The requested value does not
        // narrow collection visibility beyond the ReadCollection baseline applied
        // below — a collection is a collection, and ReadCollection is what gates
        // seeing it.
        query_params.permissions()?;

        let mut base_query = if is_admin {
            collections.into_boxed()
        } else {
            let group_id_subquery = self.group_ids_subquery();

            // Visibility requires the principal's groups to actually hold
            // ReadCollection (has_read_collection) on the collection, not merely to
            // have *some* permission row for it.
            let permission_subquery = Permissions::ReadCollection.create_boxed_filter(
                permissions
                    .filter(group_id.eq_any(group_id_subquery))
                    .into_boxed(),
                true,
            );

            collections
                .filter(
                    collection_id.eq_any(
                        permission_subquery
                            .inner_join(
                                collection_closure
                                    .on(permissions_collection_id.eq(ancestor_collection_id)),
                            )
                            .select(descendant_collection_id)
                            .distinct(),
                    ),
                )
                .into_boxed()
        };
        if let Some(scope) = resource_scope_ids(scopes) {
            base_query = base_query.filter(collection_scope_predicate(scope));
        }

        for param in query_params {
            use crate::{date_search, numeric_search, revision_search, string_search};
            let operator = param.operator.clone();
            match param.field {
                FilterField::Id => numeric_search!(base_query, param, operator, collection_id),
                FilterField::CreatedAt => {
                    date_search!(base_query, param, operator, collection_created_at)
                }
                FilterField::UpdatedAt => {
                    date_search!(base_query, param, operator, collection_updated_at)
                }
                FilterField::Revision => {
                    revision_search!(base_query, param, operator, collection_revision)
                }
                FilterField::Name => {
                    string_search!(base_query, param, operator, collection_name)
                }
                FilterField::Description => {
                    string_search!(base_query, param, operator, collection_description)
                }
                FilterField::Permissions => {}
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for collections",
                        param.field
                    )));
                }
            }
        }

        with_connection(pool, async |conn| {
            base_query.count().get_result::<i64>(conn).await
        })
        .await
    }

    async fn search_classes_from_backend(
        &self,
        pool: &impl crate::storage::StorageContext,
        query_options: QueryOptions,
        scopes: Option<&TokenScope>,
    ) -> Result<Vec<HubuumClassExpanded>, ApiError> {
        let is_admin = self.is_admin(pool).await?;
        self.search_classes_from_backend_with_admin_status(pool, query_options, is_admin, scopes)
            .await
    }

    async fn count_classes_from_backend(
        &self,
        pool: &impl crate::storage::StorageContext,
        query_options: QueryOptions,
        scopes: Option<&TokenScope>,
    ) -> Result<i64, ApiError> {
        let is_admin = self.is_admin(pool).await?;
        self.count_classes_from_backend_with_admin_status(pool, query_options, is_admin, scopes)
            .await
    }

    async fn search_classes_from_backend_with_admin_status(
        &self,
        pool: &impl crate::storage::StorageContext,
        query_options: QueryOptions,
        is_admin: bool,
        scopes: Option<&TokenScope>,
    ) -> Result<Vec<HubuumClassExpanded>, ApiError> {
        use crate::schema::hubuumclass::dsl::{
            collection_id as class_collection_id, created_at as class_created_at,
            description as class_description, hubuumclass, id as class_id, name as class_name,
            revision as class_revision, updated_at as class_updated_at,
            validate_schema as class_validate_schema,
        };

        let query_params = query_options.filters.clone();

        debug!(
            message = "Searching classes",
            stage = "Starting",
            user_id = self.principal_id(),
            query_params = ?query_params
        );

        let mut permissions_list = query_params.permissions()?;
        permissions_list.ensure_contains(&[Permissions::ReadClass, Permissions::ReadCollection]);

        let collections = self
            .load_collections_with_permissions_with_admin_status(
                pool,
                &permissions_list,
                is_admin,
                scopes,
            )
            .await?;
        let collection_ids: Vec<i32> = collections.iter().map(|n| n.id).collect();

        debug!(
            message = "Searching classes",
            stage = "Collection IDs",
            user_id = self.principal_id(),
            collection_ids = ?collection_ids
        );

        let mut base_query = hubuumclass
            .filter(class_collection_id.eq_any(collection_ids))
            .into_boxed();
        if let Some(scope) = resource_scope_ids(scopes) {
            base_query = base_query.filter(class_scope_predicate(scope));
        }

        let json_schema_queries = query_params.json_schemas()?;
        if !json_schema_queries.is_empty() {
            debug!(
                message = "Searching classes",
                stage = "JSON Schema",
                user_id = self.principal_id(),
                query_params = ?json_schema_queries
            );

            for param in json_schema_queries {
                base_query = base_query.filter(param.as_json_predicate()?);
            }
        }

        for param in query_params {
            use crate::{
                boolean_search, date_search, numeric_search, revision_search, string_search,
            };
            let operator = param.operator.clone();
            match param.field {
                FilterField::Id => numeric_search!(base_query, param, operator, class_id),
                FilterField::Collections => {
                    numeric_search!(base_query, param, operator, class_collection_id)
                }
                FilterField::CreatedAt => {
                    date_search!(base_query, param, operator, class_created_at)
                }
                FilterField::UpdatedAt => {
                    date_search!(base_query, param, operator, class_updated_at)
                }
                FilterField::Revision => {
                    revision_search!(base_query, param, operator, class_revision)
                }
                FilterField::Name => string_search!(base_query, param, operator, class_name),
                FilterField::Description => {
                    string_search!(base_query, param, operator, class_description)
                }
                FilterField::ValidateSchema => {
                    boolean_search!(base_query, param, operator, class_validate_schema)
                }
                FilterField::JsonSchema => {}
                FilterField::Permissions => {}
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for classes",
                        param.field
                    )));
                }
            }
        }

        crate::apply_query_options!(base_query, query_options, HubuumClassExpanded);

        trace_query!(base_query, "Searching classes");

        let result = with_connection(pool, async |conn| {
            base_query
                .select(hubuumclass::all_columns())
                .distinct()
                .load::<HubuumClass>(conn)
                .await
        })
        .await?;

        let collection_map: std::collections::HashMap<i32, Collection> =
            collections.into_iter().map(|n| (n.id, n)).collect();

        Ok(result.expand_collection_from_map(&collection_map))
    }

    async fn count_classes_from_backend_with_admin_status(
        &self,
        pool: &impl crate::storage::StorageContext,
        query_options: QueryOptions,
        is_admin: bool,
        scopes: Option<&TokenScope>,
    ) -> Result<i64, ApiError> {
        use crate::schema::hubuumclass::dsl::{
            collection_id as class_collection_id, created_at as class_created_at,
            description as class_description, hubuumclass, id as class_id, name as class_name,
            revision as class_revision, updated_at as class_updated_at,
            validate_schema as class_validate_schema,
        };

        let query_params = query_options.filters.clone();

        let mut permissions_list = query_params.permissions()?;
        permissions_list.ensure_contains(&[Permissions::ReadClass, Permissions::ReadCollection]);

        let collections = self
            .load_collections_with_permissions_with_admin_status(
                pool,
                &permissions_list,
                is_admin,
                scopes,
            )
            .await?;
        let collection_ids: Vec<i32> = collections.iter().map(|n| n.id).collect();

        let mut base_query = hubuumclass
            .filter(class_collection_id.eq_any(collection_ids))
            .into_boxed();
        if let Some(scope) = resource_scope_ids(scopes) {
            base_query = base_query.filter(class_scope_predicate(scope));
        }

        let json_schema_queries = query_params.json_schemas()?;
        if !json_schema_queries.is_empty() {
            for param in json_schema_queries {
                base_query = base_query.filter(param.as_json_predicate()?);
            }
        }

        for param in query_params {
            use crate::{
                boolean_search, date_search, numeric_search, revision_search, string_search,
            };
            let operator = param.operator.clone();
            match param.field {
                FilterField::Id => numeric_search!(base_query, param, operator, class_id),
                FilterField::Collections => {
                    numeric_search!(base_query, param, operator, class_collection_id)
                }
                FilterField::CreatedAt => {
                    date_search!(base_query, param, operator, class_created_at)
                }
                FilterField::UpdatedAt => {
                    date_search!(base_query, param, operator, class_updated_at)
                }
                FilterField::Revision => {
                    revision_search!(base_query, param, operator, class_revision)
                }
                FilterField::Name => string_search!(base_query, param, operator, class_name),
                FilterField::Description => {
                    string_search!(base_query, param, operator, class_description)
                }
                FilterField::ValidateSchema => {
                    boolean_search!(base_query, param, operator, class_validate_schema)
                }
                FilterField::JsonSchema => {}
                FilterField::Permissions => {}
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for classes",
                        param.field
                    )));
                }
            }
        }

        with_connection(pool, async |conn| {
            base_query
                .select(class_id)
                .distinct()
                .count()
                .get_result::<i64>(conn)
                .await
        })
        .await
    }

    async fn search_objects_from_backend(
        &self,
        pool: &impl crate::storage::StorageContext,
        query_options: QueryOptions,
        scopes: Option<&TokenScope>,
    ) -> Result<Vec<HubuumObject>, ApiError> {
        let is_admin = self.is_admin(pool).await?;
        self.search_objects_from_backend_with_admin_status(pool, query_options, is_admin, scopes)
            .await
    }

    async fn count_objects_from_backend(
        &self,
        pool: &impl crate::storage::StorageContext,
        query_options: QueryOptions,
        scopes: Option<&TokenScope>,
    ) -> Result<i64, ApiError> {
        let is_admin = self.is_admin(pool).await?;
        self.count_objects_from_backend_with_admin_status(pool, query_options, is_admin, scopes)
            .await
    }

    async fn search_objects_from_backend_with_admin_status(
        &self,
        pool: &impl crate::storage::StorageContext,
        query_options: QueryOptions,
        is_admin: bool,
        scopes: Option<&TokenScope>,
    ) -> Result<Vec<HubuumObject>, ApiError> {
        let plan = ObjectQueryPlan::ordinary(query_options)?;
        self.search_objects_from_backend_with_query_plan(pool, plan, is_admin, scopes)
            .await
    }

    async fn search_objects_with_computed_query_from_backend(
        &self,
        pool: &impl crate::storage::StorageContext,
        query_options: QueryOptions,
        scopes: Option<&TokenScope>,
        snapshot: &ComputedQuerySnapshot,
    ) -> Result<Vec<HubuumObject>, ApiError> {
        let is_admin = self.is_admin(pool).await?;
        let plan = ObjectQueryPlan::computed(query_options, snapshot);
        self.search_objects_from_backend_with_query_plan(pool, plan, is_admin, scopes)
            .await
    }

    async fn search_objects_from_backend_with_query_plan(
        &self,
        pool: &impl crate::storage::StorageContext,
        query_plan: ObjectQueryPlan<'_>,
        is_admin: bool,
        scopes: Option<&TokenScope>,
    ) -> Result<Vec<HubuumObject>, ApiError> {
        use crate::schema::hubuumobject::dsl::{
            collection_id as object_collection_id, created_at as object_created_at,
            description as object_description, hubuum_class_id, hubuumobject, id as object_id,
            name as object_name, revision as object_revision, updated_at as object_updated_at,
        };

        let (query_options, query_mode) = query_plan.into_parts();
        let query_params = query_options.filters.clone();

        debug!(
            message = "Searching objects",
            stage = "Starting",
            user_id = self.principal_id(),
            query_params = ?query_params
        );

        let mut permission_list = query_params.permissions()?;
        permission_list.ensure_contains(&[Permissions::ReadObject, Permissions::ReadCollection]);

        let collection_ids: Vec<i32> = self
            .load_collections_with_permissions_with_admin_status(
                pool,
                &permission_list,
                is_admin,
                scopes,
            )
            .await?
            .into_iter()
            .map(|n| n.id)
            .collect();

        debug!(
            message = "Searching objects",
            stage = "Collection IDs",
            user_id = self.principal_id(),
            collection_ids = ?collection_ids
        );

        let mut base_query = hubuumobject
            .filter(object_collection_id.eq_any(collection_ids))
            .into_boxed();
        if let Some(scope) = resource_scope_ids(scopes) {
            base_query = base_query.filter(object_scope_predicate(scope));
        }
        if let Some(authorized_object_ids) = query_mode.authorized_object_ids() {
            base_query = base_query.filter(object_id.eq_any(authorized_object_ids.as_slice()));
        }
        if let Some(predicate) =
            related_object_filter_predicate(self, pool, &query_params, is_admin, scopes).await?
        {
            base_query = base_query.filter(predicate);
        }

        let json_data_queries = query_params.json_datas(FilterField::JsonData)?;
        if !json_data_queries.is_empty() {
            debug!(
                message = "Searching objects",
                stage = "JSON Data",
                user_id = self.principal_id(),
                query_params = ?json_data_queries
            );

            for param in json_data_queries {
                base_query = base_query.filter(param.as_json_predicate()?);
            }
        }

        for param in query_params {
            use crate::{date_search, numeric_search, revision_search, string_search};
            if param.field.related_query().is_some() {
                continue;
            }
            if param.field.computed_query().is_some() {
                let snapshot = query_mode.snapshot()?;
                base_query = base_query.filter(computed_filter_predicate(&param, snapshot)?);
                continue;
            }
            let operator = param.operator.clone();
            match param.field {
                FilterField::Id => numeric_search!(base_query, param, operator, object_id),
                FilterField::Collections => {
                    numeric_search!(base_query, param, operator, object_collection_id)
                }
                FilterField::CreatedAt => {
                    date_search!(base_query, param, operator, object_created_at)
                }
                FilterField::UpdatedAt => {
                    date_search!(base_query, param, operator, object_updated_at)
                }
                FilterField::Revision => {
                    revision_search!(base_query, param, operator, object_revision)
                }
                FilterField::Name => string_search!(base_query, param, operator, object_name),
                FilterField::Description => {
                    string_search!(base_query, param, operator, object_description)
                }
                FilterField::Classes => {
                    numeric_search!(base_query, param, operator, hubuum_class_id)
                }
                FilterField::ClassId => {
                    numeric_search!(base_query, param, operator, hubuum_class_id)
                }
                FilterField::JsonData => {}
                FilterField::Permissions => {}
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for objects",
                        param.field
                    )));
                }
            }
        }

        let computed_sorting = query_options
            .sort
            .iter()
            .any(|sort| sort.field.computed_query().is_some());
        if computed_sorting {
            let snapshot = query_mode.snapshot()?;
            let sql_fields = object_cursor_sql_fields(&query_options.sort, snapshot)?;
            crate::apply_query_options_with_fields!(base_query, query_options, sql_fields);
        } else {
            crate::apply_query_options!(base_query, query_options, HubuumObject);
        }

        trace_query!(base_query, "Searching objects");

        if computed_sorting {
            with_connection(pool, async |conn| {
                base_query
                    .select(hubuumobject::all_columns())
                    .load::<HubuumObject>(conn)
                    .await
            })
            .await
        } else {
            with_connection(pool, async |conn| {
                base_query
                    .select(hubuumobject::all_columns())
                    .distinct()
                    .load::<HubuumObject>(conn)
                    .await
            })
            .await
        }
    }

    async fn count_objects_from_backend_with_admin_status(
        &self,
        pool: &impl crate::storage::StorageContext,
        query_options: QueryOptions,
        is_admin: bool,
        scopes: Option<&TokenScope>,
    ) -> Result<i64, ApiError> {
        let plan = ObjectQueryPlan::ordinary(query_options)?;
        self.count_objects_from_backend_with_query_plan(pool, plan, is_admin, scopes)
            .await
    }

    async fn count_objects_with_computed_query_from_backend(
        &self,
        pool: &impl crate::storage::StorageContext,
        query_options: QueryOptions,
        scopes: Option<&TokenScope>,
        snapshot: &ComputedQuerySnapshot,
    ) -> Result<i64, ApiError> {
        let is_admin = self.is_admin(pool).await?;
        let plan = ObjectQueryPlan::computed(query_options, snapshot);
        self.count_objects_from_backend_with_query_plan(pool, plan, is_admin, scopes)
            .await
    }

    async fn count_objects_from_backend_with_query_plan(
        &self,
        pool: &impl crate::storage::StorageContext,
        query_plan: ObjectQueryPlan<'_>,
        is_admin: bool,
        scopes: Option<&TokenScope>,
    ) -> Result<i64, ApiError> {
        use crate::schema::hubuumobject::dsl::{
            collection_id as object_collection_id, created_at as object_created_at,
            description as object_description, hubuum_class_id, hubuumobject, id as object_id,
            name as object_name, revision as object_revision, updated_at as object_updated_at,
        };

        let (query_options, query_mode) = query_plan.into_parts();
        let query_params = query_options.filters.clone();

        let mut permission_list = query_params.permissions()?;
        permission_list.ensure_contains(&[Permissions::ReadObject, Permissions::ReadCollection]);

        let collection_ids: Vec<i32> = self
            .load_collections_with_permissions_with_admin_status(
                pool,
                &permission_list,
                is_admin,
                scopes,
            )
            .await?
            .into_iter()
            .map(|n| n.id)
            .collect();

        let mut base_query = hubuumobject
            .filter(object_collection_id.eq_any(collection_ids))
            .into_boxed();
        if let Some(scope) = resource_scope_ids(scopes) {
            base_query = base_query.filter(object_scope_predicate(scope));
        }
        if let Some(authorized_object_ids) = query_mode.authorized_object_ids() {
            base_query = base_query.filter(object_id.eq_any(authorized_object_ids.as_slice()));
        }
        if let Some(predicate) =
            related_object_filter_predicate(self, pool, &query_params, is_admin, scopes).await?
        {
            base_query = base_query.filter(predicate);
        }

        let json_data_queries = query_params.json_datas(FilterField::JsonData)?;
        if !json_data_queries.is_empty() {
            for param in json_data_queries {
                base_query = base_query.filter(param.as_json_predicate()?);
            }
        }

        for param in query_params {
            use crate::{date_search, numeric_search, revision_search, string_search};
            if param.field.related_query().is_some() {
                continue;
            }
            if param.field.computed_query().is_some() {
                let snapshot = query_mode.snapshot()?;
                base_query = base_query.filter(computed_filter_predicate(&param, snapshot)?);
                continue;
            }
            let operator = param.operator.clone();
            match param.field {
                FilterField::Id => numeric_search!(base_query, param, operator, object_id),
                FilterField::Collections => {
                    numeric_search!(base_query, param, operator, object_collection_id)
                }
                FilterField::CreatedAt => {
                    date_search!(base_query, param, operator, object_created_at)
                }
                FilterField::UpdatedAt => {
                    date_search!(base_query, param, operator, object_updated_at)
                }
                FilterField::Revision => {
                    revision_search!(base_query, param, operator, object_revision)
                }
                FilterField::Name => string_search!(base_query, param, operator, object_name),
                FilterField::Description => {
                    string_search!(base_query, param, operator, object_description)
                }
                FilterField::Classes => {
                    numeric_search!(base_query, param, operator, hubuum_class_id)
                }
                FilterField::ClassId => {
                    numeric_search!(base_query, param, operator, hubuum_class_id)
                }
                FilterField::JsonData => {}
                FilterField::Permissions => {}
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for objects",
                        param.field
                    )));
                }
            }
        }

        with_connection(pool, async |conn| {
            base_query
                .select(object_id)
                .distinct()
                .count()
                .get_result::<i64>(conn)
                .await
        })
        .await
    }

    async fn search_class_relations_from_backend(
        &self,
        pool: &impl crate::storage::StorageContext,
        query_options: QueryOptions,
        scopes: Option<&TokenScope>,
    ) -> Result<Vec<HubuumClassRelation>, ApiError> {
        let is_admin = self.is_admin(pool).await?;
        self.search_class_relations_from_backend_with_admin_status(
            pool,
            query_options,
            is_admin,
            scopes,
        )
        .await
    }

    async fn class_relations_page_from_backend(
        &self,
        pool: &impl crate::storage::StorageContext,
        query_options: QueryOptions,
        scopes: Option<&TokenScope>,
    ) -> Result<(Vec<HubuumClassRelation>, i64), ApiError> {
        let is_admin = self.is_admin(pool).await?;
        self.class_relations_page_from_backend_with_admin_status(
            pool,
            query_options,
            is_admin,
            scopes,
        )
        .await
    }

    async fn search_class_relations_from_backend_with_admin_status(
        &self,
        pool: &impl crate::storage::StorageContext,
        query_options: QueryOptions,
        is_admin: bool,
        scopes: Option<&TokenScope>,
    ) -> Result<Vec<HubuumClassRelation>, ApiError> {
        let (items, _) = self
            .class_relations_page_from_backend_with_admin_status(
                pool,
                query_options,
                is_admin,
                scopes,
            )
            .await?;
        Ok(items)
    }

    async fn class_relations_page_from_backend_with_admin_status(
        &self,
        pool: &impl crate::storage::StorageContext,
        query_options: QueryOptions,
        is_admin: bool,
        scopes: Option<&TokenScope>,
    ) -> Result<(Vec<HubuumClassRelation>, i64), ApiError> {
        use crate::schema::hubuumclass::dsl::{
            collection_id as class_collection_id, hubuumclass, id as class_id,
        };
        use crate::schema::hubuumclass_relation::dsl::{
            created_at as class_relation_created_at, from_hubuum_class_id, hubuumclass_relation,
            id as class_relation_id, revision as class_relation_revision, to_hubuum_class_id,
            updated_at as class_relation_updated_at,
        };

        let query_params = query_options.filters.clone();

        debug!(
            message = "Searching class relations",
            stage = "Starting",
            user_id = self.principal_id(),
            query_params = ?query_params
        );

        let mut query_params = query_params;
        let mut permissions_list = query_params.permissions()?;
        permissions_list.ensure_contains(&[Permissions::ReadClassRelation]);

        let collection_ids: Vec<i32> = self
            .load_collections_with_permissions_with_admin_status(
                pool,
                &permissions_list,
                is_admin,
                scopes,
            )
            .await?
            .into_iter()
            .map(|n| n.id)
            .collect();

        debug!(
            message = "Searching class relations",
            stage = "Collection IDs",
            user_id = self.principal_id(),
            collection_ids = ?collection_ids
        );

        for param in &[FilterField::ClassFromName, FilterField::ClassToName] {
            if let Some(class_param) = query_params.iter().find(|p| &p.field == param) {
                let qparam = ParsedQueryParam {
                    field: FilterField::Name,
                    operator: class_param.operator.clone(),
                    value: class_param.value.clone(),
                };
                let class_query_options = QueryOptions {
                    filters: vec![qparam],
                    sort: vec![],
                    limit: None,
                    cursor: None,
                    include_total: true,
                };
                let classes = self
                    .search_classes_from_backend_with_admin_status(
                        pool,
                        class_query_options,
                        is_admin,
                        scopes,
                    )
                    .await?;
                let class_ids: Vec<i32> = classes.iter().map(|c| c.id).collect();

                if class_ids.is_empty() {
                    debug!(
                        message = "Searching class relations with class names",
                        stage = "Class IDs",
                        user_id = self.principal_id(),
                        result = "No class IDs found, returning empty result"
                    );
                    return Ok((
                        vec![],
                        crate::pagination::known_count_or_skipped(&query_options, 0),
                    ));
                }

                debug!(
                    message = "Searching class relations with class names",
                    stage = "Class IDs",
                    user_id = self.principal_id(),
                    result = "Found class IDs",
                    class_ids = ?class_ids
                );

                let field = match param {
                    FilterField::ClassFromName => FilterField::ClassFrom,
                    FilterField::ClassToName => FilterField::ClassTo,
                    _ => unreachable!(),
                };

                query_params.push(ParsedQueryParam {
                    field,
                    operator: SearchOperator::Equals { is_negated: false },
                    value: class_ids
                        .iter()
                        .map(|item| item.to_string())
                        .collect::<Vec<_>>()
                        .join(","),
                });
            }
        }

        let build_query = || -> Result<_, ApiError> {
            let mut base_query = hubuumclass_relation
                .filter(
                    from_hubuum_class_id.eq_any(
                        hubuumclass
                            .select(class_id)
                            .filter(class_collection_id.eq_any(&collection_ids)),
                    ),
                )
                .filter(
                    to_hubuum_class_id.eq_any(
                        hubuumclass
                            .select(class_id)
                            .filter(class_collection_id.eq_any(&collection_ids)),
                    ),
                )
                .into_boxed();
            if let Some(scope) = resource_scope_ids(scopes) {
                let scoped_class_query = || {
                    hubuumclass
                        .select(class_id)
                        .filter(class_scope_predicate(scope))
                };
                base_query = base_query
                    .filter(from_hubuum_class_id.eq_any(scoped_class_query()))
                    .filter(to_hubuum_class_id.eq_any(scoped_class_query()));
            }

            for param in &query_params {
                use crate::{date_search, numeric_search, revision_search};
                let operator = param.operator.clone();
                match param.field {
                    FilterField::Id => {
                        numeric_search!(base_query, param, operator, class_relation_id)
                    }
                    FilterField::ClassFrom => {
                        numeric_search!(base_query, param, operator, from_hubuum_class_id)
                    }
                    FilterField::ClassTo => {
                        numeric_search!(base_query, param, operator, to_hubuum_class_id)
                    }
                    FilterField::CreatedAt => {
                        date_search!(base_query, param, operator, class_relation_created_at)
                    }
                    FilterField::UpdatedAt => {
                        date_search!(base_query, param, operator, class_relation_updated_at)
                    }
                    FilterField::Revision => {
                        revision_search!(base_query, param, operator, class_relation_revision)
                    }
                    FilterField::ClassFromName => {}
                    FilterField::ClassToName => {}
                    _ => {
                        return Err(ApiError::BadRequest(format!(
                            "Field '{}' isn't searchable (or does not exist) for class relations",
                            param.field
                        )));
                    }
                }
            }

            Ok(base_query)
        };

        let base_query = build_query()?;
        let total_count = crate::pagination::exact_count_or_skipped(&query_options, async || {
            with_connection(pool, async |conn| {
                base_query
                    .select(class_relation_id)
                    .distinct()
                    .count()
                    .get_result::<i64>(conn)
                    .await
            })
            .await
        })
        .await?;

        let mut base_query = build_query()?;
        crate::apply_query_options!(base_query, query_options, HubuumClassRelation);

        trace_query!(base_query, "Searching class relations");

        let items = with_connection(pool, async |conn| {
            base_query
                .select(hubuumclass_relation::all_columns())
                .distinct()
                .load::<HubuumClassRelation>(conn)
                .await
        })
        .await?;

        Ok((items, total_count))
    }

    async fn class_relations_touching_page_from_backend<K>(
        &self,
        pool: &impl crate::storage::StorageContext,
        class: K,
        query_options: QueryOptions,
        scopes: Option<&TokenScope>,
    ) -> Result<(Vec<HubuumClassRelation>, i64), ApiError>
    where
        K: SelfAccessors<HubuumClass>,
    {
        let is_admin = self.is_admin(pool).await?;
        self.class_relations_touching_page_from_backend_with_admin_status(
            pool,
            class,
            query_options,
            is_admin,
            scopes,
        )
        .await
    }

    async fn class_relations_touching_page_from_backend_with_admin_status<K>(
        &self,
        pool: &impl crate::storage::StorageContext,
        class: K,
        query_options: QueryOptions,
        is_admin: bool,
        scopes: Option<&TokenScope>,
    ) -> Result<(Vec<HubuumClassRelation>, i64), ApiError>
    where
        K: SelfAccessors<HubuumClass>,
    {
        use crate::schema::hubuumclass::dsl::{
            collection_id as class_collection_id, hubuumclass, id as class_id,
        };
        use crate::schema::hubuumclass_relation::dsl::{
            created_at as relation_created_at, from_hubuum_class_id, hubuumclass_relation,
            id as relation_id, revision as relation_revision, to_hubuum_class_id,
            updated_at as relation_updated_at,
        };
        use diesel::BoolExpressionMethods;

        let query_params = query_options.filters.clone();

        let mut permissions_list = query_params.permissions()?;
        permissions_list.ensure_contains(&[Permissions::ReadClassRelation]);

        let collection_ids: Vec<i32> = self
            .load_collections_with_permissions_with_admin_status(
                pool,
                &permissions_list,
                is_admin,
                scopes,
            )
            .await?
            .into_iter()
            .map(|n| n.id)
            .collect();

        let build_query = || -> Result<_, ApiError> {
            let mut base_query = hubuumclass_relation
                .filter(
                    from_hubuum_class_id
                        .eq(class.id())
                        .or(to_hubuum_class_id.eq(class.id())),
                )
                .filter(
                    from_hubuum_class_id.eq_any(
                        hubuumclass
                            .select(class_id)
                            .filter(class_collection_id.eq_any(&collection_ids)),
                    ),
                )
                .filter(
                    to_hubuum_class_id.eq_any(
                        hubuumclass
                            .select(class_id)
                            .filter(class_collection_id.eq_any(&collection_ids)),
                    ),
                )
                .into_boxed();
            if let Some(scope) = resource_scope_ids(scopes) {
                let scoped_class_query = || {
                    hubuumclass
                        .select(class_id)
                        .filter(class_scope_predicate(scope))
                };
                base_query = base_query
                    .filter(from_hubuum_class_id.eq_any(scoped_class_query()))
                    .filter(to_hubuum_class_id.eq_any(scoped_class_query()));
            }

            for param in &query_params {
                use crate::{date_search, numeric_search, revision_search};
                let operator = param.operator.clone();
                match param.field {
                    FilterField::Id => numeric_search!(base_query, param, operator, relation_id),
                    FilterField::ClassFrom => {
                        numeric_search!(base_query, param, operator, from_hubuum_class_id)
                    }
                    FilterField::ClassTo => {
                        numeric_search!(base_query, param, operator, to_hubuum_class_id)
                    }
                    FilterField::CreatedAt => {
                        date_search!(base_query, param, operator, relation_created_at)
                    }
                    FilterField::UpdatedAt => {
                        date_search!(base_query, param, operator, relation_updated_at)
                    }
                    FilterField::Revision => {
                        revision_search!(base_query, param, operator, relation_revision)
                    }
                    _ => {
                        return Err(ApiError::BadRequest(format!(
                            "Field '{}' isn't searchable (or does not exist) for class relations",
                            param.field
                        )));
                    }
                }
            }

            Ok(base_query)
        };

        let base_query = build_query()?;
        let total_count = crate::pagination::exact_count_or_skipped(&query_options, async || {
            with_connection(pool, async |conn| {
                base_query
                    .select(relation_id)
                    .distinct()
                    .count()
                    .get_result::<i64>(conn)
                    .await
            })
            .await
        })
        .await?;

        let mut base_query = build_query()?;
        crate::apply_query_options!(base_query, query_options, HubuumClassRelation);

        trace_query!(
            base_query,
            "Searching direct class relations touching class"
        );

        let items = with_connection(pool, async |conn| {
            base_query
                .select(hubuumclass_relation::all_columns())
                .distinct()
                .load::<HubuumClassRelation>(conn)
                .await
        })
        .await?;

        Ok((items, total_count))
    }

    async fn search_class_relations_between_ids_from_backend(
        &self,
        pool: &impl crate::storage::StorageContext,
        class_ids: &[i32],
        scopes: Option<&TokenScope>,
    ) -> Result<Vec<HubuumClassRelation>, ApiError> {
        let is_admin = self.is_admin(pool).await?;
        self.search_class_relations_between_ids_from_backend_with_admin_status(
            pool, class_ids, is_admin, scopes,
        )
        .await
    }

    async fn search_class_relations_touching_ids_from_backend_with_admin_status(
        &self,
        pool: &impl crate::storage::StorageContext,
        class_ids: &[i32],
        is_admin: bool,
        scopes: Option<&TokenScope>,
    ) -> Result<Vec<HubuumClassRelation>, ApiError> {
        use crate::schema::hubuumclass::dsl::{
            collection_id as class_collection_id, hubuumclass, id as class_id,
        };
        use crate::schema::hubuumclass_relation::dsl::{
            from_hubuum_class_id, hubuumclass_relation, id as relation_id, to_hubuum_class_id,
        };

        if class_ids.is_empty() {
            return Ok(Vec::new());
        }

        let permission_list = [Permissions::ReadClassRelation];
        let collection_ids: Vec<i32> = self
            .load_collections_with_permissions_with_admin_status(
                pool,
                &permission_list,
                is_admin,
                scopes,
            )
            .await?
            .into_iter()
            .map(|collection| collection.id)
            .collect();

        let mut base_query = hubuumclass_relation
            .filter(
                from_hubuum_class_id
                    .eq_any(class_ids)
                    .or(to_hubuum_class_id.eq_any(class_ids)),
            )
            .filter(
                from_hubuum_class_id.eq_any(
                    hubuumclass
                        .select(class_id)
                        .filter(class_collection_id.eq_any(&collection_ids)),
                ),
            )
            .filter(
                to_hubuum_class_id.eq_any(
                    hubuumclass
                        .select(class_id)
                        .filter(class_collection_id.eq_any(&collection_ids)),
                ),
            )
            .into_boxed();
        if let Some(scope) = resource_scope_ids(scopes) {
            let scoped_class_query = || {
                hubuumclass
                    .select(class_id)
                    .filter(class_scope_predicate(scope))
            };
            base_query = base_query
                .filter(from_hubuum_class_id.eq_any(scoped_class_query()))
                .filter(to_hubuum_class_id.eq_any(scoped_class_query()));
        }
        let base_query = base_query.order(relation_id.asc());

        trace_query!(
            base_query,
            "Searching visible class relations touching class IDs"
        );

        with_connection(pool, async |conn| {
            base_query.load::<HubuumClassRelation>(conn).await
        })
        .await
    }

    async fn search_class_relations_between_ids_from_backend_with_admin_status(
        &self,
        pool: &impl crate::storage::StorageContext,
        class_ids: &[i32],
        is_admin: bool,
        scopes: Option<&TokenScope>,
    ) -> Result<Vec<HubuumClassRelation>, ApiError> {
        use crate::schema::hubuumclass::dsl::{
            collection_id as class_collection_id, hubuumclass, id as class_id,
        };
        use crate::schema::hubuumclass_relation::dsl::{
            from_hubuum_class_id, hubuumclass_relation, id as relation_id, to_hubuum_class_id,
        };

        if class_ids.is_empty() {
            return Ok(vec![]);
        }

        let permission_list = [Permissions::ReadClassRelation];
        let collection_ids: Vec<i32> = self
            .load_collections_with_permissions_with_admin_status(
                pool,
                &permission_list,
                is_admin,
                scopes,
            )
            .await?
            .into_iter()
            .map(|n| n.id)
            .collect();

        let mut base_query = hubuumclass_relation
            .filter(from_hubuum_class_id.eq_any(class_ids))
            .filter(to_hubuum_class_id.eq_any(class_ids))
            .filter(
                from_hubuum_class_id.eq_any(
                    hubuumclass
                        .select(class_id)
                        .filter(class_collection_id.eq_any(&collection_ids)),
                ),
            )
            .filter(
                to_hubuum_class_id.eq_any(
                    hubuumclass
                        .select(class_id)
                        .filter(class_collection_id.eq_any(&collection_ids)),
                ),
            )
            .into_boxed();
        if let Some(scope) = resource_scope_ids(scopes) {
            let scoped_class_query = || {
                hubuumclass
                    .select(class_id)
                    .filter(class_scope_predicate(scope))
            };
            base_query = base_query
                .filter(from_hubuum_class_id.eq_any(scoped_class_query()))
                .filter(to_hubuum_class_id.eq_any(scoped_class_query()));
        }
        let base_query = base_query.order(relation_id.asc());

        trace_query!(base_query, "Searching class relations among class IDs");

        with_connection(pool, async |conn| {
            base_query.load::<HubuumClassRelation>(conn).await
        })
        .await
    }

    async fn search_classes_related_to_from_backend<K>(
        &self,
        pool: &impl crate::storage::StorageContext,
        class: K,
        query_options: QueryOptions,
        scopes: Option<&TokenScope>,
    ) -> Result<Vec<ClassGraphRow>, ApiError>
    where
        K: SelfAccessors<HubuumClass>,
    {
        let is_admin = self.is_admin(pool).await?;
        self.search_classes_related_to_from_backend_with_admin_status(
            pool,
            class,
            query_options,
            is_admin,
            scopes,
        )
        .await
    }

    async fn classes_related_to_page_from_backend<K>(
        &self,
        pool: &impl crate::storage::StorageContext,
        class: K,
        query_options: QueryOptions,
        scopes: Option<&TokenScope>,
    ) -> Result<(Vec<ClassGraphRow>, i64), ApiError>
    where
        K: SelfAccessors<HubuumClass>,
    {
        let is_admin = self.is_admin(pool).await?;
        self.classes_related_to_page_from_backend_with_admin_status(
            pool,
            class,
            query_options,
            is_admin,
            scopes,
        )
        .await
    }

    async fn search_classes_related_to_from_backend_with_admin_status<K>(
        &self,
        pool: &impl crate::storage::StorageContext,
        class: K,
        query_options: QueryOptions,
        is_admin: bool,
        scopes: Option<&TokenScope>,
    ) -> Result<Vec<ClassGraphRow>, ApiError>
    where
        K: SelfAccessors<HubuumClass>,
    {
        let Some(base_spec) = build_related_classes_query_spec(
            self,
            pool,
            class,
            query_options.clone(),
            is_admin,
            scopes,
        )
        .await?
        else {
            return Ok(vec![]);
        };
        let spec = apply_raw_sql_pagination::<ClassGraphRow>(base_spec, &query_options)?;

        let query = bind_raw_sql_query!(spec.clone());
        debug!(
            message = "Searching related classes",
            raw_sql = %spec.sql,
            bind_variables = ?spec.bind_variables
        );
        trace_query!(query, "Searching related classes");

        with_connection(pool, async |conn| {
            query.get_results::<ClassGraphRow>(conn).await
        })
        .await
    }

    async fn classes_related_to_page_from_backend_with_admin_status<K>(
        &self,
        pool: &impl crate::storage::StorageContext,
        class: K,
        query_options: QueryOptions,
        is_admin: bool,
        scopes: Option<&TokenScope>,
    ) -> Result<(Vec<ClassGraphRow>, i64), ApiError>
    where
        K: SelfAccessors<HubuumClass>,
    {
        let Some(base_spec) = build_related_classes_query_spec(
            self,
            pool,
            class,
            query_options.clone(),
            is_admin,
            scopes,
        )
        .await?
        else {
            return Ok((
                vec![],
                crate::pagination::known_count_or_skipped(&query_options, 0),
            ));
        };
        let total_count_spec = base_spec.clone().into_count_query("related_classes_count");
        let spec = apply_raw_sql_pagination::<ClassGraphRow>(base_spec, &query_options)?;

        let total_count = crate::pagination::exact_count_or_skipped(&query_options, async || {
            with_connection(pool, async |conn| {
                bind_raw_sql_query!(total_count_spec)
                    .get_result::<CountRow>(conn)
                    .await
                    .map(|row| row.count)
            })
            .await
        })
        .await?;

        let query = bind_raw_sql_query!(spec.clone());
        debug!(
            message = "Searching related classes",
            raw_sql = %spec.sql,
            bind_variables = ?spec.bind_variables
        );
        trace_query!(query, "Searching related classes");
        let items = with_connection(pool, async |conn| {
            query.get_results::<ClassGraphRow>(conn).await
        })
        .await?;

        Ok((items, total_count))
    }

    async fn search_object_relations_from_backend(
        &self,
        pool: &impl crate::storage::StorageContext,
        query_options: QueryOptions,
        scopes: Option<&TokenScope>,
    ) -> Result<Vec<HubuumObjectRelation>, ApiError> {
        let is_admin = self.is_admin(pool).await?;
        self.search_object_relations_from_backend_with_admin_status(
            pool,
            query_options,
            is_admin,
            scopes,
        )
        .await
    }

    async fn object_relations_page_from_backend(
        &self,
        pool: &impl crate::storage::StorageContext,
        query_options: QueryOptions,
        scopes: Option<&TokenScope>,
    ) -> Result<(Vec<HubuumObjectRelation>, i64), ApiError> {
        let is_admin = self.is_admin(pool).await?;
        self.object_relations_page_from_backend_with_admin_status(
            pool,
            query_options,
            is_admin,
            scopes,
        )
        .await
    }

    async fn search_object_relations_from_backend_with_admin_status(
        &self,
        pool: &impl crate::storage::StorageContext,
        query_options: QueryOptions,
        is_admin: bool,
        scopes: Option<&TokenScope>,
    ) -> Result<Vec<HubuumObjectRelation>, ApiError> {
        let (items, _) = self
            .object_relations_page_from_backend_with_admin_status(
                pool,
                query_options,
                is_admin,
                scopes,
            )
            .await?;
        Ok(items)
    }

    async fn object_relations_page_from_backend_with_admin_status(
        &self,
        pool: &impl crate::storage::StorageContext,
        query_options: QueryOptions,
        is_admin: bool,
        scopes: Option<&TokenScope>,
    ) -> Result<(Vec<HubuumObjectRelation>, i64), ApiError> {
        use crate::schema::hubuumobject::dsl::{
            collection_id as object_collection_id, hubuumobject, id as object_id,
        };
        use crate::schema::hubuumobject_relation::dsl::{
            class_relation_id, created_at as relation_created_at, from_hubuum_object_id,
            hubuumobject_relation, id as relation_id, revision as relation_revision,
            to_hubuum_object_id, updated_at as relation_updated_at,
        };

        let query_params = query_options.filters.clone();

        debug!(
            message = "Searching object relations",
            stage = "Starting",
            user_id = self.principal_id(),
            query_params = ?query_params
        );

        let mut permissions_list = query_params.permissions()?;
        permissions_list.ensure_contains(&[Permissions::ReadObjectRelation]);

        let collection_ids: Vec<i32> = self
            .load_collections_with_permissions_with_admin_status(
                pool,
                &permissions_list,
                is_admin,
                scopes,
            )
            .await?
            .into_iter()
            .map(|n| n.id)
            .collect();

        debug!(
            message = "Searching object relations",
            stage = "Collection IDs",
            user_id = self.principal_id(),
            collection_ids = ?collection_ids
        );

        let build_query = || -> Result<_, ApiError> {
            let mut base_query = hubuumobject_relation
                .filter(
                    from_hubuum_object_id.eq_any(
                        hubuumobject
                            .select(object_id)
                            .filter(object_collection_id.eq_any(&collection_ids)),
                    ),
                )
                .filter(
                    to_hubuum_object_id.eq_any(
                        hubuumobject
                            .select(object_id)
                            .filter(object_collection_id.eq_any(&collection_ids)),
                    ),
                )
                .into_boxed();
            if let Some(scope) = resource_scope_ids(scopes) {
                let scoped_object_query = || {
                    hubuumobject
                        .select(object_id)
                        .filter(object_scope_predicate(scope))
                };
                base_query = base_query
                    .filter(from_hubuum_object_id.eq_any(scoped_object_query()))
                    .filter(to_hubuum_object_id.eq_any(scoped_object_query()));
            }

            for param in &query_params {
                use crate::{date_search, numeric_search, revision_search};
                let operator = param.operator.clone();
                match param.field {
                    FilterField::Id => numeric_search!(base_query, param, operator, relation_id),
                    FilterField::ClassRelation => {
                        numeric_search!(base_query, param, operator, class_relation_id)
                    }
                    FilterField::ObjectFrom => {
                        numeric_search!(base_query, param, operator, from_hubuum_object_id)
                    }
                    FilterField::ObjectTo => {
                        numeric_search!(base_query, param, operator, to_hubuum_object_id)
                    }
                    FilterField::CreatedAt => {
                        date_search!(base_query, param, operator, relation_created_at)
                    }
                    FilterField::UpdatedAt => {
                        date_search!(base_query, param, operator, relation_updated_at)
                    }
                    FilterField::Revision => {
                        revision_search!(base_query, param, operator, relation_revision)
                    }
                    _ => {
                        return Err(ApiError::BadRequest(format!(
                            "Field '{}' isn't searchable (or does not exist) for object relations",
                            param.field
                        )));
                    }
                }
            }

            Ok(base_query)
        };

        let base_query = build_query()?;
        let total_count = crate::pagination::exact_count_or_skipped(&query_options, async || {
            with_connection(pool, async |conn| {
                base_query
                    .select(relation_id)
                    .distinct()
                    .count()
                    .get_result::<i64>(conn)
                    .await
            })
            .await
        })
        .await?;

        let mut base_query = build_query()?;
        crate::apply_query_options!(base_query, query_options, HubuumObjectRelation);

        trace_query!(base_query, "Searching object relations");

        let items = with_connection(pool, async |conn| {
            base_query
                .select(hubuumobject_relation::all_columns())
                .distinct()
                .load::<HubuumObjectRelation>(conn)
                .await
        })
        .await?;

        Ok((items, total_count))
    }

    async fn object_relations_touching_page_from_backend<O>(
        &self,
        pool: &impl crate::storage::StorageContext,
        object: O,
        query_options: QueryOptions,
        scopes: Option<&TokenScope>,
    ) -> Result<(Vec<HubuumObjectRelation>, i64), ApiError>
    where
        O: SelfAccessors<HubuumObject>,
    {
        let is_admin = self.is_admin(pool).await?;
        self.object_relations_touching_page_from_backend_with_admin_status(
            pool,
            object,
            query_options,
            is_admin,
            scopes,
        )
        .await
    }

    async fn object_relations_touching_page_from_backend_with_admin_status<O>(
        &self,
        pool: &impl crate::storage::StorageContext,
        object: O,
        query_options: QueryOptions,
        is_admin: bool,
        scopes: Option<&TokenScope>,
    ) -> Result<(Vec<HubuumObjectRelation>, i64), ApiError>
    where
        O: SelfAccessors<HubuumObject>,
    {
        use crate::schema::hubuumobject::dsl::{
            collection_id as object_collection_id, hubuumobject, id as object_id_column,
        };
        use crate::schema::hubuumobject_relation::dsl::{
            class_relation_id, created_at as relation_created_at, from_hubuum_object_id,
            hubuumobject_relation, id as relation_id, revision as relation_revision,
            to_hubuum_object_id, updated_at as relation_updated_at,
        };
        use diesel::BoolExpressionMethods;

        let query_params = query_options.filters.clone();

        debug!(
            message = "Searching direct object relations touching object",
            stage = "Starting",
            user_id = self.principal_id(),
            object_id = object.id(),
            query_params = ?query_params
        );

        let mut permissions_list = query_params.permissions()?;
        permissions_list.ensure_contains(&[Permissions::ReadObjectRelation]);

        let collection_ids: Vec<i32> = self
            .load_collections_with_permissions_with_admin_status(
                pool,
                &permissions_list,
                is_admin,
                scopes,
            )
            .await?
            .into_iter()
            .map(|n| n.id)
            .collect();

        debug!(
            message = "Searching direct object relations touching object",
            stage = "Collection IDs",
            user_id = self.principal_id(),
            object_id = object.id(),
            collection_ids = ?collection_ids
        );

        let build_query = || -> Result<_, ApiError> {
            let mut base_query = hubuumobject_relation
                .filter(
                    from_hubuum_object_id
                        .eq(object.id())
                        .or(to_hubuum_object_id.eq(object.id())),
                )
                .filter(
                    from_hubuum_object_id.eq_any(
                        hubuumobject
                            .select(object_id_column)
                            .filter(object_collection_id.eq_any(&collection_ids)),
                    ),
                )
                .filter(
                    to_hubuum_object_id.eq_any(
                        hubuumobject
                            .select(object_id_column)
                            .filter(object_collection_id.eq_any(&collection_ids)),
                    ),
                )
                .into_boxed();
            if let Some(scope) = resource_scope_ids(scopes) {
                let scoped_object_query = || {
                    hubuumobject
                        .select(object_id_column)
                        .filter(object_scope_predicate(scope))
                };
                base_query = base_query
                    .filter(from_hubuum_object_id.eq_any(scoped_object_query()))
                    .filter(to_hubuum_object_id.eq_any(scoped_object_query()));
            }

            for param in &query_params {
                use crate::{date_search, numeric_search, revision_search};
                let operator = param.operator.clone();
                match param.field {
                    FilterField::Id => numeric_search!(base_query, param, operator, relation_id),
                    FilterField::ClassRelation => {
                        numeric_search!(base_query, param, operator, class_relation_id)
                    }
                    FilterField::ObjectFrom => {
                        numeric_search!(base_query, param, operator, from_hubuum_object_id)
                    }
                    FilterField::ObjectTo => {
                        numeric_search!(base_query, param, operator, to_hubuum_object_id)
                    }
                    FilterField::CreatedAt => {
                        date_search!(base_query, param, operator, relation_created_at)
                    }
                    FilterField::UpdatedAt => {
                        date_search!(base_query, param, operator, relation_updated_at)
                    }
                    FilterField::Revision => {
                        revision_search!(base_query, param, operator, relation_revision)
                    }
                    _ => {
                        return Err(ApiError::BadRequest(format!(
                            "Field '{}' isn't searchable (or does not exist) for object relations",
                            param.field
                        )));
                    }
                }
            }

            Ok(base_query)
        };

        let base_query = build_query()?;
        let total_count = crate::pagination::exact_count_or_skipped(&query_options, async || {
            with_connection(pool, async |conn| {
                base_query
                    .select(relation_id)
                    .distinct()
                    .count()
                    .get_result::<i64>(conn)
                    .await
            })
            .await
        })
        .await?;

        let mut base_query = build_query()?;
        crate::apply_query_options!(base_query, query_options, HubuumObjectRelation);

        trace_query!(
            base_query,
            "Searching direct object relations touching object"
        );

        let items = with_connection(pool, async |conn| {
            base_query
                .select(hubuumobject_relation::all_columns())
                .distinct()
                .load::<HubuumObjectRelation>(conn)
                .await
        })
        .await?;

        Ok((items, total_count))
    }

    async fn search_object_relations_between_ids_from_backend(
        &self,
        pool: &impl crate::storage::StorageContext,
        object_ids: &[i32],
        scopes: Option<&TokenScope>,
    ) -> Result<Vec<HubuumObjectRelation>, ApiError> {
        let is_admin = self.is_admin(pool).await?;
        self.search_object_relations_between_ids_from_backend_with_admin_status(
            pool, object_ids, is_admin, scopes,
        )
        .await
    }

    async fn search_object_relations_between_ids_from_backend_with_admin_status(
        &self,
        pool: &impl crate::storage::StorageContext,
        object_ids: &[i32],
        is_admin: bool,
        scopes: Option<&TokenScope>,
    ) -> Result<Vec<HubuumObjectRelation>, ApiError> {
        use crate::schema::hubuumobject::dsl::{
            collection_id as object_collection_id, hubuumobject, id as object_id_column,
        };
        use crate::schema::hubuumobject_relation::dsl::{
            from_hubuum_object_id, hubuumobject_relation, id, to_hubuum_object_id,
        };

        if object_ids.is_empty() {
            return Ok(vec![]);
        }

        let permission_list = [Permissions::ReadObjectRelation];
        let collection_ids: Vec<i32> = self
            .load_collections_with_permissions_with_admin_status(
                pool,
                &permission_list,
                is_admin,
                scopes,
            )
            .await?
            .into_iter()
            .map(|n| n.id)
            .collect();

        debug!(
            message = "Searching object relations between visible object IDs",
            user_id = self.principal_id(),
            object_ids = ?object_ids,
            collection_ids = ?collection_ids
        );

        let mut base_query = hubuumobject_relation
            .filter(from_hubuum_object_id.eq_any(object_ids))
            .filter(to_hubuum_object_id.eq_any(object_ids))
            .filter(
                from_hubuum_object_id.eq_any(
                    hubuumobject
                        .select(object_id_column)
                        .filter(object_collection_id.eq_any(&collection_ids)),
                ),
            )
            .filter(
                to_hubuum_object_id.eq_any(
                    hubuumobject
                        .select(object_id_column)
                        .filter(object_collection_id.eq_any(&collection_ids)),
                ),
            )
            .into_boxed();
        if let Some(scope) = resource_scope_ids(scopes) {
            let scoped_object_query = || {
                hubuumobject
                    .select(object_id_column)
                    .filter(object_scope_predicate(scope))
            };
            base_query = base_query
                .filter(from_hubuum_object_id.eq_any(scoped_object_query()))
                .filter(to_hubuum_object_id.eq_any(scoped_object_query()));
        }
        let base_query = base_query.order(id.asc());

        trace_query!(base_query, "Searching object relations among object IDs");

        with_connection(pool, async |conn| {
            base_query.load::<HubuumObjectRelation>(conn).await
        })
        .await
    }

    async fn search_objects_related_to_from_backend<O>(
        &self,
        pool: &impl crate::storage::StorageContext,
        object: O,
        query_options: QueryOptions,
        scopes: Option<&TokenScope>,
    ) -> Result<Vec<RelatedObjectGraphRow>, ApiError>
    where
        O: SelfAccessors<HubuumObject> + ClassAccessors,
    {
        let is_admin = self.is_admin(pool).await?;
        self.search_objects_related_to_from_backend_with_admin_status(
            pool,
            object,
            query_options,
            is_admin,
            scopes,
        )
        .await
    }

    async fn objects_related_to_page_from_backend<O>(
        &self,
        pool: &impl crate::storage::StorageContext,
        object: O,
        query_options: QueryOptions,
        scopes: Option<&TokenScope>,
    ) -> Result<(Vec<RelatedObjectGraphRow>, i64), ApiError>
    where
        O: SelfAccessors<HubuumObject> + ClassAccessors,
    {
        let is_admin = self.is_admin(pool).await?;
        self.objects_related_to_page_from_backend_with_admin_status(
            pool,
            object,
            query_options,
            is_admin,
            scopes,
        )
        .await
    }

    async fn search_objects_related_to_from_backend_with_admin_status<O>(
        &self,
        pool: &impl crate::storage::StorageContext,
        object: O,
        query_options: QueryOptions,
        is_admin: bool,
        scopes: Option<&TokenScope>,
    ) -> Result<Vec<RelatedObjectGraphRow>, ApiError>
    where
        O: SelfAccessors<HubuumObject> + ClassAccessors,
    {
        let Some(base_spec) = build_related_objects_query_spec(
            self,
            pool,
            object,
            query_options.clone(),
            is_admin,
            scopes,
        )
        .await?
        else {
            return Ok(vec![]);
        };
        let spec = apply_raw_sql_pagination::<RelatedObjectGraphRow>(base_spec, &query_options)?;

        let query = bind_raw_sql_query!(spec.clone());
        debug!(
            message = "Searching source-relative related objects",
            raw_sql = %spec.sql,
            bind_variables = ?spec.bind_variables
        );
        trace_query!(query, "Searching source-relative related objects");

        with_connection(pool, async |conn| {
            query.get_results::<RelatedObjectGraphRow>(conn).await
        })
        .await
    }

    async fn objects_related_to_page_from_backend_with_admin_status<O>(
        &self,
        pool: &impl crate::storage::StorageContext,
        object: O,
        query_options: QueryOptions,
        is_admin: bool,
        scopes: Option<&TokenScope>,
    ) -> Result<(Vec<RelatedObjectGraphRow>, i64), ApiError>
    where
        O: SelfAccessors<HubuumObject> + ClassAccessors,
    {
        let Some(base_spec) = build_related_objects_query_spec(
            self,
            pool,
            object,
            query_options.clone(),
            is_admin,
            scopes,
        )
        .await?
        else {
            return Ok((
                vec![],
                crate::pagination::known_count_or_skipped(&query_options, 0),
            ));
        };
        let total_count_spec = base_spec.clone().into_count_query("related_objects_count");
        let spec = apply_raw_sql_pagination::<RelatedObjectGraphRow>(base_spec, &query_options)?;

        let total_count = crate::pagination::exact_count_or_skipped(&query_options, async || {
            with_connection(pool, async |conn| {
                bind_raw_sql_query!(total_count_spec)
                    .get_result::<CountRow>(conn)
                    .await
                    .map(|row| row.count)
            })
            .await
        })
        .await?;

        let query = bind_raw_sql_query!(spec.clone());
        debug!(
            message = "Searching source-relative related objects",
            raw_sql = %spec.sql,
            bind_variables = ?spec.bind_variables
        );
        trace_query!(query, "Searching source-relative related objects");
        let items = with_connection(pool, async |conn| {
            query.get_results::<RelatedObjectGraphRow>(conn).await
        })
        .await?;

        Ok((items, total_count))
    }

    async fn related_objects_for_roots_from_backend(
        &self,
        pool: &impl crate::storage::StorageContext,
        root_object_ids: &[i32],
        include: ExportIncludeRelatedQuery,
        scopes: Option<&TokenScope>,
    ) -> Result<Vec<RelatedObjectIncludeRow>, ApiError> {
        let is_admin = self.is_admin(pool).await?;
        self.related_objects_for_roots_from_backend_with_admin_status(
            pool,
            root_object_ids,
            include,
            is_admin,
            scopes,
        )
        .await
    }

    async fn related_objects_for_roots_from_backend_with_admin_status(
        &self,
        pool: &impl crate::storage::StorageContext,
        root_object_ids: &[i32],
        include: ExportIncludeRelatedQuery,
        is_admin: bool,
        scopes: Option<&TokenScope>,
    ) -> Result<Vec<RelatedObjectIncludeRow>, ApiError> {
        related_objects_for_roots_query(
            self,
            pool,
            DirectionalRootGraphQuery {
                root_object_ids,
                include,
                is_admin,
                scopes,
                path_mode: GraphPathMode::Canonical,
            },
        )
        .await
    }

    async fn related_objects_for_roots_preserving_paths_from_backend_with_admin_status(
        &self,
        pool: &impl crate::storage::StorageContext,
        root_object_ids: &[i32],
        include: ExportIncludeRelatedQuery,
        is_admin: bool,
        scopes: Option<&TokenScope>,
    ) -> Result<Vec<RelatedObjectIncludeRow>, ApiError> {
        related_objects_for_roots_query(
            self,
            pool,
            DirectionalRootGraphQuery {
                root_object_ids,
                include,
                is_admin,
                scopes,
                path_mode: GraphPathMode::PreserveAlternatives,
            },
        )
        .await
    }

    async fn bidirectionally_related_objects_for_roots_from_backend(
        &self,
        pool: &impl crate::storage::StorageContext,
        root_object_ids: &[i32],
        max_depth: i32,
        per_root_cap: i32,
        scopes: Option<&TokenScope>,
    ) -> Result<Vec<RelatedObjectForRootRow>, ApiError> {
        let is_admin = self.is_admin(pool).await?;
        self.bidirectionally_related_objects_for_roots_from_backend_with_admin_status(
            pool,
            root_object_ids,
            max_depth,
            per_root_cap,
            is_admin,
            scopes,
        )
        .await
    }

    async fn bidirectionally_related_objects_for_roots_from_backend_with_admin_status(
        &self,
        pool: &impl crate::storage::StorageContext,
        root_object_ids: &[i32],
        max_depth: i32,
        per_root_cap: i32,
        is_admin: bool,
        scopes: Option<&TokenScope>,
    ) -> Result<Vec<RelatedObjectForRootRow>, ApiError> {
        bidirectionally_related_objects_for_roots_query(
            self,
            pool,
            BidirectionalRootGraphQuery {
                root_object_ids,
                max_depth,
                per_root_cap,
                is_admin,
                scopes,
                path_mode: GraphPathMode::Canonical,
            },
        )
        .await
    }

    async fn bidirectionally_related_objects_for_roots_preserving_paths_from_backend_with_admin_status(
        &self,
        pool: &impl crate::storage::StorageContext,
        root_object_ids: &[i32],
        max_depth: i32,
        per_root_cap: i32,
        is_admin: bool,
        scopes: Option<&TokenScope>,
    ) -> Result<Vec<RelatedObjectForRootRow>, ApiError> {
        bidirectionally_related_objects_for_roots_query(
            self,
            pool,
            BidirectionalRootGraphQuery {
                root_object_ids,
                max_depth,
                per_root_cap,
                is_admin,
                scopes,
                path_mode: GraphPathMode::PreserveAlternatives,
            },
        )
        .await
    }
}

struct DirectionalRootGraphQuery<'a> {
    root_object_ids: &'a [i32],
    include: ExportIncludeRelatedQuery,
    is_admin: bool,
    scopes: Option<&'a TokenScope>,
    path_mode: GraphPathMode,
}

async fn related_objects_for_roots_query<U>(
    user: &U,
    pool: &impl crate::storage::StorageContext,
    request: DirectionalRootGraphQuery<'_>,
) -> Result<Vec<RelatedObjectIncludeRow>, ApiError>
where
    U: UserCollectionAccessors + ?Sized,
{
    if request.root_object_ids.is_empty() {
        return Ok(Vec::new());
    }

    let permissions =
        PermissionsList::new([Permissions::ReadObject, Permissions::ReadObjectRelation]);
    let collection_ids: Vec<i32> = user
        .load_collections_with_permissions_with_admin_status(
            pool,
            &permissions,
            request.is_admin,
            request.scopes,
        )
        .await?
        .into_iter()
        .map(|collection| collection.id)
        .collect();

    if collection_ids.is_empty() {
        return Ok(Vec::new());
    }

    let spec = build_root_graph_walk_query(RootGraphWalkSpec {
        root_object_ids: request.root_object_ids,
        collection_ids: &collection_ids,
        scope: request.scopes,
        max_depth: request.include.max_depth,
        per_root_limit: request.include.limit,
        edges: GraphWalkEdges::Directional {
            direction: request.include.direction,
            class_relation_id: request.include.class_relation_id,
        },
        ranking: GraphWalkRanking::ByTargetClass {
            class_id: request.include.class_id,
            sort: request.include.sort,
        },
        projection: GraphWalkProjection::AncestorAndDescendant,
        path_mode: request.path_mode,
    });

    let query = bind_raw_sql_query!(spec.clone());
    debug!(
        message = "Searching batched related objects",
        root_object_count = request.root_object_ids.len(),
        target_class_id = request.include.class_id,
        class_relation_id = request.include.class_relation_id,
        direction = ?request.include.direction,
        sort = ?request.include.sort,
        max_depth = request.include.max_depth,
        per_root_limit = request.include.limit,
        raw_sql = %spec.sql,
        bind_variables = ?spec.bind_variables
    );
    trace_query!(query, "Searching batched related objects");

    with_connection(pool, async |conn| {
        query.get_results::<RelatedObjectIncludeRow>(conn).await
    })
    .await
}

struct BidirectionalRootGraphQuery<'a> {
    root_object_ids: &'a [i32],
    max_depth: i32,
    per_root_cap: i32,
    is_admin: bool,
    scopes: Option<&'a TokenScope>,
    path_mode: GraphPathMode,
}

async fn bidirectionally_related_objects_for_roots_query<U>(
    user: &U,
    pool: &impl crate::storage::StorageContext,
    request: BidirectionalRootGraphQuery<'_>,
) -> Result<Vec<RelatedObjectForRootRow>, ApiError>
where
    U: UserCollectionAccessors + ?Sized,
{
    if request.root_object_ids.is_empty() {
        return Ok(Vec::new());
    }

    let permissions =
        PermissionsList::new([Permissions::ReadObject, Permissions::ReadObjectRelation]);
    let collection_ids: Vec<i32> = user
        .load_collections_with_permissions_with_admin_status(
            pool,
            &permissions,
            request.is_admin,
            request.scopes,
        )
        .await?
        .into_iter()
        .map(|collection| collection.id)
        .collect();

    if collection_ids.is_empty() {
        return Ok(Vec::new());
    }

    let spec = build_root_graph_walk_query(RootGraphWalkSpec {
        root_object_ids: request.root_object_ids,
        collection_ids: &collection_ids,
        scope: request.scopes,
        max_depth: request.max_depth,
        per_root_limit: request.per_root_cap,
        edges: GraphWalkEdges::Bidirectional,
        ranking: GraphWalkRanking::ByDescendant,
        projection: GraphWalkProjection::DescendantOnly,
        path_mode: request.path_mode,
    });

    let query = bind_raw_sql_query!(spec.clone());
    debug!(
        message = "Searching batched bidirectionally related objects",
        root_object_count = request.root_object_ids.len(),
        max_depth = request.max_depth,
        per_root_cap = request.per_root_cap,
        raw_sql = %spec.sql,
        bind_variables = ?spec.bind_variables
    );
    trace_query!(query, "Searching batched bidirectionally related objects");

    with_connection(pool, async |conn| {
        query.get_results::<RelatedObjectForRootRow>(conn).await
    })
    .await
}

fn related_include_object_edges_sql(
    direction: ExportIncludeRelatedDirection,
    class_relation_id: Option<i32>,
    bind_variables: &mut Vec<SQLValue>,
) -> String {
    let mut selects = Vec::new();

    match direction {
        ExportIncludeRelatedDirection::Any | ExportIncludeRelatedDirection::Outgoing => {
            selects.push(related_include_object_edge_select_sql(
                "from_hubuum_object_id",
                "to_hubuum_object_id",
                class_relation_id,
                bind_variables,
            ));
        }
        ExportIncludeRelatedDirection::Incoming => {}
    }

    match direction {
        ExportIncludeRelatedDirection::Any | ExportIncludeRelatedDirection::Incoming => {
            selects.push(related_include_object_edge_select_sql(
                "to_hubuum_object_id",
                "from_hubuum_object_id",
                class_relation_id,
                bind_variables,
            ));
        }
        ExportIncludeRelatedDirection::Outgoing => {}
    }

    selects.join("\n\n    UNION ALL\n\n")
}

fn related_include_object_edge_select_sql(
    source_column: &str,
    target_column: &str,
    class_relation_id: Option<i32>,
    bind_variables: &mut Vec<SQLValue>,
) -> String {
    let class_relation_filter_sql = if let Some(class_relation_id) = class_relation_id {
        bind_variables.push(SQLValue::Integer(class_relation_id));
        "  AND hubuumobject_relation.class_relation_id = ?\n"
    } else {
        ""
    };

    format!(
        r#"    SELECT
        hubuumobject_relation.{source_column} AS source_object_id,
        hubuumobject_relation.{target_column} AS target_object_id
    FROM hubuumobject_relation
    JOIN hubuumobject source_edge_object
      ON source_edge_object.id = hubuumobject_relation.{source_column}
    JOIN hubuumobject target_edge_object
      ON target_edge_object.id = hubuumobject_relation.{target_column}
    WHERE source_edge_object.collection_id IN (SELECT collection_id FROM valid_collections)
      AND target_edge_object.collection_id IN (SELECT collection_id FROM valid_collections)
{class_relation_filter_sql}"#
    )
}

fn related_include_order_sql(sort: ExportIncludeRelatedSort) -> &'static str {
    match sort {
        ExportIncludeRelatedSort::Path => {
            "deduped_walk.path ASC, deduped_walk.descendant_object_id ASC"
        }
        ExportIncludeRelatedSort::Name => {
            "target_object.name ASC, target_object.id ASC, deduped_walk.path ASC"
        }
        ExportIncludeRelatedSort::CreatedAt => {
            "target_object.created_at ASC, target_object.id ASC, deduped_walk.path ASC"
        }
    }
}

/// Edge set for a per-root recursive object-graph walk.
enum GraphWalkEdges {
    /// Both relation directions, unfiltered — used by templated relation hydration.
    Bidirectional,
    /// Direction- and (optionally) class-relation-filtered — used by the export include path.
    Directional {
        direction: ExportIncludeRelatedDirection,
        class_relation_id: Option<i32>,
    },
}

/// How descendants are ranked within each root partition (drives the per-root cap).
enum GraphWalkRanking {
    /// Stable order by descendant id — hydration needs determinism, not a sort option.
    ByDescendant,
    /// Restrict to a target class and order by the include sort option.
    ByTargetClass {
        class_id: i32,
        sort: ExportIncludeRelatedSort,
    },
}

/// Columns emitted by the final SELECT.
enum GraphWalkProjection {
    /// Descendant object only (templated hydration).
    DescendantOnly,
    /// Both the root (ancestor) and descendant objects (include path).
    AncestorAndDescendant,
}

#[derive(Clone, Copy)]
enum GraphPathMode {
    Canonical,
    PreserveAlternatives,
}

impl GraphPathMode {
    fn walk_selection_sql(self) -> &'static str {
        match self {
            Self::Canonical => {
                r#"    SELECT DISTINCT ON (root_object_id, descendant_object_id)
        root_object_id,
        ancestor_object_id,
        descendant_object_id,
        depth,
        path
    FROM graph_walk
    ORDER BY root_object_id ASC, descendant_object_id ASC, depth ASC, path ASC"#
            }
            Self::PreserveAlternatives => {
                r#"    SELECT
        root_object_id,
        ancestor_object_id,
        descendant_object_id,
        depth,
        path
    FROM graph_walk"#
            }
        }
    }
}

/// Parameters for [`build_root_graph_walk_query`]. One builder owns the full SQL and, crucially,
/// the bind-variable ordering shared by both batched per-root graph queries.
struct RootGraphWalkSpec<'a> {
    root_object_ids: &'a [i32],
    collection_ids: &'a [i32],
    scope: Option<&'a TokenScope>,
    max_depth: i32,
    per_root_limit: i32,
    edges: GraphWalkEdges,
    ranking: GraphWalkRanking,
    projection: GraphWalkProjection,
    path_mode: GraphPathMode,
}

fn bidirectional_object_edges_sql() -> &'static str {
    r#"    SELECT from_hubuum_object_id AS source_object_id, to_hubuum_object_id AS target_object_id
    FROM hubuumobject_relation

    UNION ALL

    SELECT to_hubuum_object_id AS source_object_id, from_hubuum_object_id AS target_object_id
    FROM hubuumobject_relation"#
}

/// Builds the recursive per-root object-graph walk shared by `related_objects_for_roots`
/// (include path) and `bidirectionally_related_objects_for_roots` (templated hydration).
///
/// The `root_objects`/`valid_collections`/`graph_walk` CTEs are identical for both callers; only
/// the edge set, optional path deduplication, per-root ranking, and final projection differ. Bind
/// order is fixed here: collection ids, resource-scope ids, root ids, (edge class-relation
/// filter), max_depth ×2, (target class id), per_root_limit.
fn build_root_graph_walk_query(spec: RootGraphWalkSpec) -> RawSqlQuerySpec {
    let mut bind_variables = Vec::<SQLValue>::new();
    let collection_array_sql = sql_integer_array(spec.collection_ids, &mut bind_variables);
    let valid_scope_objects_sql = if let Some(scope) = resource_scope_ids(spec.scope) {
        let collection_scope_sql = sql_integer_array(scope.collection_ids(), &mut bind_variables);
        let class_scope_sql = sql_integer_array(scope.class_ids(), &mut bind_variables);
        let object_scope_sql = sql_integer_array(scope.object_ids(), &mut bind_variables);
        format!(
            "SELECT id AS object_id FROM hubuumobject WHERE collection_id = ANY({collection_scope_sql}) OR hubuum_class_id = ANY({class_scope_sql}) OR id = ANY({object_scope_sql})"
        )
    } else {
        "SELECT id AS object_id FROM hubuumobject".to_string()
    };
    let root_array_sql = sql_integer_array(spec.root_object_ids, &mut bind_variables);

    let object_edges_sql = match spec.edges {
        GraphWalkEdges::Bidirectional => bidirectional_object_edges_sql().to_string(),
        GraphWalkEdges::Directional {
            direction,
            class_relation_id,
        } => related_include_object_edges_sql(direction, class_relation_id, &mut bind_variables),
    };

    bind_variables.push(SQLValue::Integer(spec.max_depth));
    bind_variables.push(SQLValue::Integer(spec.max_depth));

    let deduplicated_walk_sql = spec.path_mode.walk_selection_sql();

    let ranked_walk_sql = match spec.ranking {
        GraphWalkRanking::ByDescendant => r#"    SELECT
        deduped_walk.*,
        row_number() OVER (
            PARTITION BY root_object_id
            ORDER BY descendant_object_id ASC, depth ASC, path ASC
        ) AS related_rank
    FROM deduped_walk"#
            .to_string(),
        GraphWalkRanking::ByTargetClass { class_id, sort } => {
            let related_order_sql = related_include_order_sql(sort);
            bind_variables.push(SQLValue::Integer(class_id));
            format!(
                r#"    SELECT
        deduped_walk.*,
        row_number() OVER (
            PARTITION BY deduped_walk.root_object_id
            ORDER BY {related_order_sql}
        ) AS related_rank
    FROM deduped_walk
    JOIN hubuumobject target_object
      ON target_object.id = deduped_walk.descendant_object_id
    WHERE target_object.hubuum_class_id = ?"#
            )
        }
    };

    bind_variables.push(SQLValue::Integer(spec.per_root_limit));

    let final_select_sql = match spec.projection {
        GraphWalkProjection::DescendantOnly => r#"SELECT
    ranked_walk.root_object_id,
    target_object.id AS descendant_object_id,
    ranked_walk.depth,
    ranked_walk.path,
    target_object.name AS descendant_name,
    target_object.collection_id AS descendant_collection_id,
    target_object.hubuum_class_id AS descendant_class_id,
    target_object.description AS descendant_description,
    target_object.data AS descendant_data,
    target_object.created_at AS descendant_created_at,
    target_object.updated_at AS descendant_updated_at,
    target_object.revision AS descendant_revision
FROM ranked_walk
JOIN hubuumobject target_object
  ON target_object.id = ranked_walk.descendant_object_id
WHERE ranked_walk.related_rank <= ?
  AND target_object.collection_id IN (SELECT collection_id FROM valid_collections)
  AND target_object.id IN (SELECT object_id FROM valid_scope_objects)
ORDER BY ranked_walk.root_object_id ASC, ranked_walk.related_rank ASC"#
            .to_string(),
        GraphWalkProjection::AncestorAndDescendant => r#"SELECT
    ranked_walk.root_object_id,
    source_object.id AS ancestor_object_id,
    target_object.id AS descendant_object_id,
    ranked_walk.depth,
    ranked_walk.path,
    source_object.name AS ancestor_name,
    target_object.name AS descendant_name,
    source_object.collection_id AS ancestor_collection_id,
    target_object.collection_id AS descendant_collection_id,
    source_object.hubuum_class_id AS ancestor_class_id,
    target_object.hubuum_class_id AS descendant_class_id,
    source_object.description AS ancestor_description,
    target_object.description AS descendant_description,
    source_object.data AS ancestor_data,
    target_object.data AS descendant_data,
    source_object.created_at AS ancestor_created_at,
    target_object.created_at AS descendant_created_at,
    source_object.updated_at AS ancestor_updated_at,
    target_object.updated_at AS descendant_updated_at,
    source_object.revision AS ancestor_revision,
    target_object.revision AS descendant_revision
FROM ranked_walk
JOIN hubuumobject source_object
  ON source_object.id = ranked_walk.ancestor_object_id
JOIN hubuumobject target_object
  ON target_object.id = ranked_walk.descendant_object_id
WHERE ranked_walk.related_rank <= ?
  AND source_object.collection_id IN (SELECT collection_id FROM valid_collections)
  AND target_object.collection_id IN (SELECT collection_id FROM valid_collections)
  AND source_object.id IN (SELECT object_id FROM valid_scope_objects)
  AND target_object.id IN (SELECT object_id FROM valid_scope_objects)
ORDER BY ranked_walk.root_object_id ASC, ranked_walk.related_rank ASC"#
            .to_string(),
    };

    let sql = format!(
        r#"
WITH RECURSIVE
valid_collections AS (
    SELECT unnest({collection_array_sql}) AS collection_id
),
valid_scope_objects AS (
    {valid_scope_objects_sql}
),
root_objects AS (
    SELECT scoped_root.root_object_id
    FROM unnest({root_array_sql}) AS scoped_root(root_object_id)
    WHERE scoped_root.root_object_id IN (SELECT object_id FROM valid_scope_objects)
),
object_edges AS (
{object_edges_sql}
),
graph_walk AS (
    SELECT
        root_objects.root_object_id,
        root_objects.root_object_id AS ancestor_object_id,
        object_edges.target_object_id AS descendant_object_id,
        1 AS depth,
        ARRAY[root_objects.root_object_id, object_edges.target_object_id] AS path
    FROM root_objects
    JOIN object_edges
      ON object_edges.source_object_id = root_objects.root_object_id
    JOIN hubuumobject target_object
      ON target_object.id = object_edges.target_object_id
    WHERE ? >= 1
      AND target_object.collection_id IN (SELECT collection_id FROM valid_collections)
      AND target_object.id IN (SELECT object_id FROM valid_scope_objects)

    UNION ALL

    SELECT
        graph_walk.root_object_id,
        graph_walk.ancestor_object_id,
        object_edges.target_object_id AS descendant_object_id,
        graph_walk.depth + 1,
        graph_walk.path || object_edges.target_object_id
    FROM graph_walk
    JOIN object_edges
      ON object_edges.source_object_id = graph_walk.descendant_object_id
    JOIN hubuumobject target_object
      ON target_object.id = object_edges.target_object_id
    WHERE NOT (object_edges.target_object_id = ANY(graph_walk.path))
      AND graph_walk.depth < ?
      AND target_object.collection_id IN (SELECT collection_id FROM valid_collections)
      AND target_object.id IN (SELECT object_id FROM valid_scope_objects)
),
deduped_walk AS (
{deduplicated_walk_sql}
),
ranked_walk AS (
{ranked_walk_sql}
)
{final_select_sql}
"#
    );

    RawSqlQuerySpec {
        sql,
        bind_variables,
    }
}

fn apply_raw_sql_pagination<T>(
    mut spec: RawSqlQuerySpec,
    query_options: &QueryOptions,
) -> Result<RawSqlQuerySpec, ApiError>
where
    T: CursorPaginated + CursorSqlMapping,
{
    use crate::pagination::{cursor_filter_sql, normalized_sorts, order_sql_clause};

    let sorts = normalized_sorts::<T>(&query_options.sort)?;
    let mut where_clauses = Vec::new();
    if let Some(cursor_sql) = cursor_filter_sql::<T>(&sorts, query_options.cursor.as_deref())? {
        where_clauses.push(cursor_sql);
    }

    if !where_clauses.is_empty() {
        if spec.sql.contains("\nWHERE ") {
            spec.sql.push_str("\n  AND ");
        } else {
            spec.sql.push_str("\nWHERE ");
        }
        spec.sql.push_str(&where_clauses.join("\n  AND "));
    }

    let order_by = sorts
        .iter()
        .map(order_sql_clause::<T>)
        .collect::<Result<Vec<_>, _>>()?
        .join(", ");
    spec.sql.push_str(&format!("\nORDER BY {order_by}"));

    if let Some(limit) = query_options.limit {
        spec.sql.push_str(&format!("\nLIMIT {limit}"));
    }

    Ok(spec)
}

async fn build_related_classes_query_spec<U, K>(
    user: &U,
    pool: &impl crate::storage::StorageContext,
    class: K,
    query_options: QueryOptions,
    is_admin: bool,
    scopes: Option<&TokenScope>,
) -> Result<Option<RawSqlQuerySpec>, ApiError>
where
    U: UserCollectionAccessors + ?Sized,
    K: SelfAccessors<HubuumClass>,
{
    let query_params = query_options.filters.clone();

    let mut permissions_list = query_params.permissions()?;
    permissions_list.ensure_contains(&[Permissions::ReadClass, Permissions::ReadClassRelation]);

    let collection_ids: Vec<i32> = user
        .load_collections_with_permissions_with_admin_status(
            pool,
            &permissions_list,
            is_admin,
            scopes,
        )
        .await?
        .into_iter()
        .map(|n| n.id)
        .collect();

    if collection_ids.is_empty() {
        return Ok(None);
    }

    let mut bind_variables = Vec::<SQLValue>::new();
    bind_variables.push(SQLValue::Integer(class.id()));
    let related_depth_upper_bound = related_depth_upper_bound(&query_params)?;
    let collection_array_sql = sql_integer_array(&collection_ids, &mut bind_variables);
    let mut raw_sql = if let Some(max_depth) = related_depth_upper_bound {
        bind_variables.push(SQLValue::Integer(max_depth));
        format!(
            "SELECT related_classes.*, ancestor.revision AS ancestor_revision, descendant.revision AS descendant_revision FROM get_bidirectionally_related_classes(?, {collection_array_sql}, ?) AS related_classes JOIN hubuumclass ancestor ON ancestor.id = related_classes.ancestor_class_id JOIN hubuumclass descendant ON descendant.id = related_classes.descendant_class_id"
        )
    } else {
        format!(
            "SELECT related_classes.*, ancestor.revision AS ancestor_revision, descendant.revision AS descendant_revision FROM get_bidirectionally_related_classes(?, {collection_array_sql}, NULL) AS related_classes JOIN hubuumclass ancestor ON ancestor.id = related_classes.ancestor_class_id JOIN hubuumclass descendant ON descendant.id = related_classes.descendant_class_id"
        )
    };

    let mut where_clauses = Vec::new();
    append_related_class_scope_clause(&mut where_clauses, scopes, &mut bind_variables);
    for param in &query_params {
        let clause = build_related_classes_clause(param, &mut bind_variables)?;
        if let Some(clause) = clause {
            where_clauses.push(clause);
        }
    }

    if !where_clauses.is_empty() {
        raw_sql.push_str("\nWHERE ");
        raw_sql.push_str(&where_clauses.join("\n  AND "));
    }

    Ok(Some(RawSqlQuerySpec {
        sql: raw_sql,
        bind_variables,
    }))
}

async fn build_related_objects_query_spec<U, O>(
    user: &U,
    pool: &impl crate::storage::StorageContext,
    object: O,
    query_options: QueryOptions,
    is_admin: bool,
    scopes: Option<&TokenScope>,
) -> Result<Option<RawSqlQuerySpec>, ApiError>
where
    U: UserCollectionAccessors + ?Sized,
    O: SelfAccessors<HubuumObject> + ClassAccessors,
{
    let query_params = query_options.filters.clone();

    debug!(
        message = "Searching objects related to object",
        stage = "Starting",
        user_id = user.principal_id(),
        object_id = object.id(),
        query_params = ?query_params
    );

    let mut permissions_list = query_params.permissions()?;
    permissions_list.ensure_contains(&[Permissions::ReadObject, Permissions::ReadObjectRelation]);

    let collection_ids: Vec<i32> = user
        .load_collections_with_permissions_with_admin_status(
            pool,
            &permissions_list,
            is_admin,
            scopes,
        )
        .await?
        .into_iter()
        .map(|n| n.id)
        .collect();

    if collection_ids.is_empty() {
        debug!(
            message = "Searching object relations related to object",
            stage = "Collection IDs",
            user_id = user.principal_id(),
            result = "No collection IDs found, returning empty result"
        );
        return Ok(None);
    }

    debug!(
        message = "Searching object relations related to object",
        stage = "Collection IDs",
        user_id = user.principal_id(),
        result = "Found collection IDs",
        collection_ids = ?collection_ids
    );

    let mut bind_variables = Vec::<SQLValue>::new();
    bind_variables.push(SQLValue::Integer(object.id()));
    let related_depth_upper_bound = related_depth_upper_bound(&query_params)?;
    let collection_array_sql = sql_integer_array(&collection_ids, &mut bind_variables);
    let mut raw_sql = if let Some(max_depth) = related_depth_upper_bound {
        bind_variables.push(SQLValue::Integer(max_depth));
        format!(
            "SELECT related_objects.*, ancestor.revision AS ancestor_revision, descendant.revision AS descendant_revision FROM get_bidirectionally_related_objects(?, {collection_array_sql}, ?) AS related_objects JOIN hubuumobject ancestor ON ancestor.id = related_objects.ancestor_object_id JOIN hubuumobject descendant ON descendant.id = related_objects.descendant_object_id"
        )
    } else {
        format!(
            "SELECT related_objects.*, ancestor.revision AS ancestor_revision, descendant.revision AS descendant_revision FROM get_bidirectionally_related_objects(?, {collection_array_sql}, NULL) AS related_objects JOIN hubuumobject ancestor ON ancestor.id = related_objects.ancestor_object_id JOIN hubuumobject descendant ON descendant.id = related_objects.descendant_object_id"
        )
    };

    let mut where_clauses = Vec::new();
    append_related_object_scope_clause(&mut where_clauses, scopes, &mut bind_variables);
    for param in &query_params {
        let clause = build_related_objects_clause(param, &mut bind_variables)?;
        if let Some(clause) = clause {
            where_clauses.push(clause);
        }
    }

    if !where_clauses.is_empty() {
        raw_sql.push_str("\nWHERE ");
        raw_sql.push_str(&where_clauses.join("\n  AND "));
    }

    Ok(Some(RawSqlQuerySpec {
        sql: raw_sql,
        bind_variables,
    }))
}

fn sql_integer_array(values: &[i32], bind_variables: &mut Vec<SQLValue>) -> String {
    let placeholders = values
        .iter()
        .map(|value| {
            bind_variables.push(SQLValue::Integer(*value));
            "?"
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("ARRAY[{placeholders}]::integer[]")
}

fn sql_bigint_array(values: &[i64], bind_variables: &mut Vec<SQLValue>) -> String {
    let placeholders = values
        .iter()
        .map(|value| {
            bind_variables.push(SQLValue::BigInteger(*value));
            "?"
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("ARRAY[{placeholders}]::bigint[]")
}

fn sql_text_array(values: &[String], bind_variables: &mut Vec<SQLValue>) -> String {
    let placeholders = values
        .iter()
        .map(|value| {
            bind_variables.push(SQLValue::String(value.clone()));
            "?"
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("ARRAY[{placeholders}]::text[]")
}

fn build_related_object_filter_sql(
    groups: &[RelatedFilterGroup<'_>],
    graph_collection_ids: &[i32],
    class_collection_ids: &[i32],
    scopes: Option<&TokenScope>,
) -> Result<SQLComponent, ApiError> {
    let mut bind_variables = Vec::new();
    let graph_collections_sql = sql_integer_array(graph_collection_ids, &mut bind_variables);
    let class_collections_sql = sql_integer_array(class_collection_ids, &mut bind_variables);
    let valid_scope_objects_sql = if let Some(scope) = resource_scope_ids(scopes) {
        let collection_scope_sql = sql_integer_array(scope.collection_ids(), &mut bind_variables);
        let class_scope_sql = sql_integer_array(scope.class_ids(), &mut bind_variables);
        let object_scope_sql = sql_integer_array(scope.object_ids(), &mut bind_variables);
        format!(
            "SELECT id AS object_id FROM hubuumobject WHERE collection_id = ANY({collection_scope_sql}) OR hubuum_class_id = ANY({class_scope_sql}) OR id = ANY({object_scope_sql})"
        )
    } else {
        "SELECT id AS object_id FROM hubuumobject".to_string()
    };
    let valid_scope_classes_sql = if let Some(scope) = resource_scope_ids(scopes) {
        let collection_scope_sql = sql_integer_array(scope.collection_ids(), &mut bind_variables);
        let class_scope_sql = sql_integer_array(scope.class_ids(), &mut bind_variables);
        format!(
            "SELECT id AS class_id FROM hubuumclass WHERE collection_id = ANY({collection_scope_sql}) OR id = ANY({class_scope_sql})"
        )
    } else {
        "SELECT id AS class_id FROM hubuumclass".to_string()
    };

    let mut seed_queries = Vec::with_capacity(groups.len());
    for (group_id, group) in groups.iter().enumerate() {
        bind_variables.push(SQLValue::Integer(i32::try_from(group_id).map_err(
            |_| ApiError::InternalServerError("Related filter group index overflow".to_string()),
        )?));
        bind_variables.push(SQLValue::Integer(group.max_depth));

        let class_field = group
            .class_filter
            .field
            .related_query()
            .and_then(|field| match field.target() {
                RelatedFilterTarget::Class(class_field) => Some(class_field),
                _ => None,
            })
            .ok_or_else(|| {
                ApiError::InternalServerError(
                    "Related filter group lost its class selector".to_string(),
                )
            })?;
        let class_clause = match class_field {
            RelatedClassField::Id => {
                let values = group.class_filter.value_as_integer()?;
                if values.len() != 1 {
                    return Err(ApiError::BadRequest(
                        "related.<alias>.class.id requires exactly one integer".to_string(),
                    ));
                }
                bind_variables.push(SQLValue::Integer(values[0]));
                "target_class.id = ?".to_string()
            }
            RelatedClassField::Name => {
                bind_variables.push(SQLValue::String(group.class_filter.value.clone()));
                "target_class.name = ?".to_string()
            }
        };

        let mut target_clauses = vec![class_clause];
        for (filter, field) in &group.object_filters {
            target_clauses.push(related_target_object_clause(
                filter,
                *field,
                &mut bind_variables,
            )?);
        }
        seed_queries.push(format!(
            r#"    SELECT ?::integer AS group_id,
           target_object.id AS seed_id,
           ?::integer AS max_depth
    FROM hubuumobject target_object
    JOIN hubuumclass target_class
      ON target_class.id = target_object.hubuum_class_id
    WHERE target_object.collection_id IN (SELECT collection_id FROM valid_graph_collections)
      AND target_class.collection_id IN (SELECT collection_id FROM valid_class_collections)
      AND target_object.id IN (SELECT object_id FROM valid_scope_objects)
      AND target_class.id IN (SELECT class_id FROM valid_scope_classes)
      AND {}"#,
            target_clauses.join("\n      AND ")
        ));
    }

    bind_variables.push(SQLValue::Integer(i32::try_from(groups.len()).map_err(
        |_| ApiError::InternalServerError("Related filter group count overflow".to_string()),
    )?));

    let sql = format!(
        r#"hubuumobject.id IN (
WITH RECURSIVE
valid_graph_collections AS (
    SELECT unnest({graph_collections_sql}) AS collection_id
),
valid_class_collections AS (
    SELECT unnest({class_collections_sql}) AS collection_id
),
valid_scope_objects AS (
    {valid_scope_objects_sql}
),
valid_scope_classes AS (
    {valid_scope_classes_sql}
),
target_seeds AS (
{}
),
object_edges AS (
    SELECT relation.from_hubuum_object_id AS source_object_id,
           relation.to_hubuum_object_id AS target_object_id
    FROM hubuumobject_relation relation
    JOIN hubuumobject source_object
      ON source_object.id = relation.from_hubuum_object_id
    JOIN hubuumobject target_object
      ON target_object.id = relation.to_hubuum_object_id
    WHERE source_object.collection_id IN (SELECT collection_id FROM valid_graph_collections)
      AND target_object.collection_id IN (SELECT collection_id FROM valid_graph_collections)
      AND source_object.id IN (SELECT object_id FROM valid_scope_objects)
      AND target_object.id IN (SELECT object_id FROM valid_scope_objects)

    UNION ALL

    SELECT relation.to_hubuum_object_id AS source_object_id,
           relation.from_hubuum_object_id AS target_object_id
    FROM hubuumobject_relation relation
    JOIN hubuumobject source_object
      ON source_object.id = relation.to_hubuum_object_id
    JOIN hubuumobject target_object
      ON target_object.id = relation.from_hubuum_object_id
    WHERE source_object.collection_id IN (SELECT collection_id FROM valid_graph_collections)
      AND target_object.collection_id IN (SELECT collection_id FROM valid_graph_collections)
      AND source_object.id IN (SELECT object_id FROM valid_scope_objects)
      AND target_object.id IN (SELECT object_id FROM valid_scope_objects)
),
reachable AS (
    SELECT target_seeds.group_id,
           target_seeds.seed_id,
           object_edges.target_object_id AS object_id,
           1 AS depth,
           target_seeds.max_depth
    FROM target_seeds
    JOIN object_edges
      ON object_edges.source_object_id = target_seeds.seed_id
    WHERE target_seeds.max_depth >= 1

    UNION

    SELECT reachable.group_id,
           reachable.seed_id,
           object_edges.target_object_id AS object_id,
           reachable.depth + 1,
           reachable.max_depth
    FROM reachable
    JOIN object_edges
      ON object_edges.source_object_id = reachable.object_id
    WHERE reachable.depth < reachable.max_depth
)
SELECT reachable.object_id
FROM reachable
WHERE reachable.object_id <> reachable.seed_id
GROUP BY reachable.object_id
HAVING COUNT(DISTINCT reachable.group_id) = ?
)"#,
        seed_queries.join("\n\n    UNION ALL\n\n")
    );

    Ok(SQLComponent {
        sql,
        bind_variables,
    })
}

fn related_target_object_clause(
    param: &ParsedQueryParam,
    field: RelatedObjectField,
    bind_variables: &mut Vec<SQLValue>,
) -> Result<String, ApiError> {
    let column = match field {
        RelatedObjectField::Id => "target_object.id",
        RelatedObjectField::Name => "target_object.name",
        RelatedObjectField::Description => "target_object.description",
        RelatedObjectField::CollectionId => "target_object.collection_id",
        RelatedObjectField::CreatedAt => "target_object.created_at",
        RelatedObjectField::UpdatedAt => "target_object.updated_at",
        RelatedObjectField::Revision => "target_object.revision",
        RelatedObjectField::JsonData => {
            let mut json_param = param.clone();
            json_param.field = FilterField::JsonData;
            let predicate = json_param.as_json_sql_for_field_expr("target_object.data")?;
            bind_variables.extend(predicate.bind_variables);
            return Ok(format!("({})", predicate.sql));
        }
    };

    let (operator, negated) = param.operator.op_and_neg();
    if operator == Operator::IsNull {
        let should_be_null = param.value_as_boolean()? != negated;
        return Ok(format!(
            "{column} IS {}NULL",
            if should_be_null { "" } else { "NOT " }
        ));
    }

    match field {
        RelatedObjectField::Id | RelatedObjectField::CollectionId => {
            related_integer_clause(param, column, bind_variables)
        }
        RelatedObjectField::Revision => related_revision_clause(param, column, bind_variables),
        RelatedObjectField::CreatedAt | RelatedObjectField::UpdatedAt => {
            related_date_clause(param, column, bind_variables)
        }
        RelatedObjectField::Name | RelatedObjectField::Description => {
            related_string_clause(param, column, bind_variables)
        }
        RelatedObjectField::JsonData => unreachable!(),
    }
}

fn wrap_negated(sql: String, negated: bool) -> String {
    if negated { format!("NOT ({sql})") } else { sql }
}

fn related_integer_clause(
    param: &ParsedQueryParam,
    column: &str,
    bind_variables: &mut Vec<SQLValue>,
) -> Result<String, ApiError> {
    let values = param.value_as_integer()?;
    if values.is_empty() {
        return Err(ApiError::BadRequest(format!(
            "Searching on field '{}' requires a value",
            param.field
        )));
    }
    let min = *values.iter().min().unwrap();
    let max = *values.iter().max().unwrap();
    let (operator, negated) = param.operator.op_and_neg();
    let sql = match operator {
        Operator::Equals | Operator::In => {
            if values.len() > 50 {
                return Err(ApiError::OperatorMismatch(format!(
                    "Operator '{operator}' is limited to 50 values, got {}",
                    values.len()
                )));
            }
            let array = sql_integer_array(&values, bind_variables);
            format!("{column} = ANY({array})")
        }
        Operator::Gt => {
            bind_variables.push(SQLValue::Integer(max));
            format!("{column} > ?")
        }
        Operator::Gte => {
            bind_variables.push(SQLValue::Integer(max));
            format!("{column} >= ?")
        }
        Operator::Lt => {
            bind_variables.push(SQLValue::Integer(min));
            format!("{column} < ?")
        }
        Operator::Lte => {
            bind_variables.push(SQLValue::Integer(min));
            format!("{column} <= ?")
        }
        Operator::Between if values.len() == 2 => {
            bind_variables.push(SQLValue::Integer(values[0]));
            bind_variables.push(SQLValue::Integer(values[1]));
            format!("{column} BETWEEN ? AND ?")
        }
        Operator::Between => {
            return Err(ApiError::OperatorMismatch(format!(
                "Operator 'between' requires 2 values for field '{}'",
                param.field
            )));
        }
        _ => {
            return Err(ApiError::OperatorMismatch(format!(
                "Operator '{}' is not implemented for related numeric field '{}'",
                param.operator, param.field
            )));
        }
    };
    Ok(wrap_negated(sql, negated))
}

fn related_revision_clause(
    param: &ParsedQueryParam,
    column: &str,
    bind_variables: &mut Vec<SQLValue>,
) -> Result<String, ApiError> {
    let values = param.value_as_revision()?;
    let (operator, negated) = param.operator.op_and_neg();
    let sql = match operator {
        Operator::Equals if values.len() == 1 => {
            bind_variables.push(SQLValue::BigInteger(values[0]));
            format!("{column} = ?")
        }
        Operator::Equals => {
            return Err(ApiError::OperatorMismatch(format!(
                "Operator '{operator}' requires exactly 1 value for field '{}'",
                param.field
            )));
        }
        Operator::In => {
            let array = sql_bigint_array(&values, bind_variables);
            format!("{column} = ANY({array})")
        }
        Operator::Gt | Operator::Gte | Operator::Lt | Operator::Lte if values.len() == 1 => {
            bind_variables.push(SQLValue::BigInteger(values[0]));
            let sql_operator = match operator {
                Operator::Gt => ">",
                Operator::Gte => ">=",
                Operator::Lt => "<",
                Operator::Lte => "<=",
                _ => unreachable!(),
            };
            format!("{column} {sql_operator} ?")
        }
        Operator::Between if values.len() == 2 => {
            bind_variables.push(SQLValue::BigInteger(values[0]));
            bind_variables.push(SQLValue::BigInteger(values[1]));
            format!("{column} BETWEEN ? AND ?")
        }
        Operator::Gt | Operator::Gte | Operator::Lt | Operator::Lte | Operator::Between => {
            return Err(ApiError::OperatorMismatch(format!(
                "Operator '{operator}' has the wrong number of values for field '{}'",
                param.field
            )));
        }
        _ => {
            return Err(ApiError::OperatorMismatch(format!(
                "Operator '{}' is not implemented for related revision field '{}'",
                param.operator, param.field
            )));
        }
    };
    Ok(wrap_negated(sql, negated))
}

fn related_date_clause(
    param: &ParsedQueryParam,
    column: &str,
    bind_variables: &mut Vec<SQLValue>,
) -> Result<String, ApiError> {
    let values = param.value_as_date()?;
    if values.is_empty() {
        return Err(ApiError::BadRequest(format!(
            "Searching on field '{}' requires a value",
            param.field
        )));
    }
    let min = *values.iter().min().unwrap();
    let max = *values.iter().max().unwrap();
    let (operator, negated) = param.operator.op_and_neg();
    let sql = match operator {
        Operator::Equals | Operator::In => {
            let array = sql_date_array(&values, bind_variables);
            format!("{column} = ANY({array})")
        }
        Operator::Gt => {
            bind_variables.push(SQLValue::Date(max));
            format!("{column} > ?")
        }
        Operator::Gte => {
            bind_variables.push(SQLValue::Date(max));
            format!("{column} >= ?")
        }
        Operator::Lt => {
            bind_variables.push(SQLValue::Date(min));
            format!("{column} < ?")
        }
        Operator::Lte => {
            bind_variables.push(SQLValue::Date(min));
            format!("{column} <= ?")
        }
        Operator::Between if values.len() == 2 => {
            bind_variables.push(SQLValue::Date(values[0]));
            bind_variables.push(SQLValue::Date(values[1]));
            format!("{column} BETWEEN ? AND ?")
        }
        Operator::Between => {
            return Err(ApiError::OperatorMismatch(format!(
                "Operator 'between' requires 2 values for field '{}'",
                param.field
            )));
        }
        _ => {
            return Err(ApiError::OperatorMismatch(format!(
                "Operator '{}' is not implemented for related date field '{}'",
                param.operator, param.field
            )));
        }
    };
    Ok(wrap_negated(sql, negated))
}

fn related_string_clause(
    param: &ParsedQueryParam,
    column: &str,
    bind_variables: &mut Vec<SQLValue>,
) -> Result<String, ApiError> {
    let (operator, negated) = param.operator.op_and_neg();
    let sql = match operator {
        Operator::In => {
            let values = param
                .value
                .split(',')
                .map(ToString::to_string)
                .collect::<Vec<_>>();
            let array = sql_text_array(&values, bind_variables);
            format!("{column} = ANY({array})")
        }
        Operator::Equals
        | Operator::IEquals
        | Operator::Contains
        | Operator::IContains
        | Operator::StartsWith
        | Operator::IStartsWith
        | Operator::EndsWith
        | Operator::IEndsWith
        | Operator::Like
        | Operator::Regex => {
            let (sql_operator, value) = match operator {
                Operator::Equals => ("=", param.value.clone()),
                Operator::IEquals => ("ILIKE", param.value.clone()),
                Operator::Contains => ("LIKE", format!("%{}%", param.value)),
                Operator::IContains => ("ILIKE", format!("%{}%", param.value)),
                Operator::StartsWith => ("LIKE", format!("{}%", param.value)),
                Operator::IStartsWith => ("ILIKE", format!("{}%", param.value)),
                Operator::EndsWith => ("LIKE", format!("%{}", param.value)),
                Operator::IEndsWith => ("ILIKE", format!("%{}", param.value)),
                Operator::Like => ("LIKE", param.value.clone()),
                Operator::Regex => ("~", param.value.clone()),
                _ => unreachable!(),
            };
            bind_variables.push(SQLValue::String(value));
            format!("{column} {sql_operator} ?")
        }
        _ => {
            return Err(ApiError::OperatorMismatch(format!(
                "Operator '{}' is not implemented for related string field '{}'",
                param.operator, param.field
            )));
        }
    };
    Ok(wrap_negated(sql, negated))
}

fn append_related_class_scope_clause(
    where_clauses: &mut Vec<String>,
    scopes: Option<&TokenScope>,
    bind_variables: &mut Vec<SQLValue>,
) {
    let Some(scope) = resource_scope_ids(scopes) else {
        return;
    };
    let collection_sql = sql_integer_array(scope.collection_ids(), bind_variables);
    let class_sql = sql_integer_array(scope.class_ids(), bind_variables);
    where_clauses.push(format!(
        "NOT EXISTS (SELECT 1 FROM unnest(related_classes.path) AS path_class_id JOIN hubuumclass path_class ON path_class.id = path_class_id WHERE NOT (path_class.collection_id = ANY({collection_sql}) OR path_class.id = ANY({class_sql})))"
    ));
}

fn append_related_object_scope_clause(
    where_clauses: &mut Vec<String>,
    scopes: Option<&TokenScope>,
    bind_variables: &mut Vec<SQLValue>,
) {
    let Some(scope) = resource_scope_ids(scopes) else {
        return;
    };
    let collection_sql = sql_integer_array(scope.collection_ids(), bind_variables);
    let class_sql = sql_integer_array(scope.class_ids(), bind_variables);
    let object_sql = sql_integer_array(scope.object_ids(), bind_variables);
    where_clauses.push(format!(
        "NOT EXISTS (SELECT 1 FROM unnest(related_objects.path) AS path_object_id JOIN hubuumobject path_object ON path_object.id = path_object_id WHERE NOT (path_object.collection_id = ANY({collection_sql}) OR path_object.hubuum_class_id = ANY({class_sql}) OR path_object.id = ANY({object_sql})))"
    ));
}

fn related_depth_upper_bound(
    filters: &[crate::models::search::ParsedQueryParam],
) -> Result<Option<i32>, ApiError> {
    use crate::models::search::SearchOperator;

    let mut upper_bound: Option<i32> = None;

    for filter in filters {
        if filter.field != FilterField::Depth {
            continue;
        }

        let values = filter.value_as_integer()?;
        if values.is_empty() {
            continue;
        }

        let min = *values
            .iter()
            .min()
            .ok_or_else(|| ApiError::BadRequest("Depth filter requires a value".to_string()))?;
        let max = *values
            .iter()
            .max()
            .ok_or_else(|| ApiError::BadRequest("Depth filter requires a value".to_string()))?;

        let candidate = match &filter.operator {
            SearchOperator::Equals { is_negated: false } => Some(max),
            SearchOperator::Lt { is_negated: false } => Some(min.saturating_sub(1)),
            SearchOperator::Lte { is_negated: false } => Some(min),
            SearchOperator::Between { is_negated: false } => Some(max),
            _ => None,
        };

        if let Some(candidate) = candidate {
            upper_bound = Some(match upper_bound {
                Some(current) => current.min(candidate),
                None => candidate,
            });
        }
    }

    Ok(upper_bound)
}

fn sql_date_array(values: &[chrono::NaiveDateTime], bind_variables: &mut Vec<SQLValue>) -> String {
    let placeholders = values
        .iter()
        .map(|value| {
            bind_variables.push(SQLValue::Date(*value));
            "?"
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("ARRAY[{placeholders}]::timestamp[]")
}

fn related_classes_column(field: &FilterField) -> Option<&'static str> {
    match field {
        FilterField::Id | FilterField::ClassTo | FilterField::ClassId | FilterField::Classes => {
            Some("related_classes.descendant_class_id")
        }
        FilterField::ClassFrom => Some("related_classes.ancestor_class_id"),
        FilterField::Collections | FilterField::CollectionId | FilterField::CollectionsTo => {
            Some("related_classes.descendant_collection_id")
        }
        FilterField::CollectionsFrom => Some("related_classes.ancestor_collection_id"),
        FilterField::Name | FilterField::NameTo => Some("related_classes.descendant_name"),
        FilterField::NameFrom => Some("related_classes.ancestor_name"),
        FilterField::Description | FilterField::DescriptionTo => {
            Some("related_classes.descendant_description")
        }
        FilterField::DescriptionFrom => Some("related_classes.ancestor_description"),
        FilterField::CreatedAt | FilterField::CreatedAtTo => {
            Some("related_classes.descendant_created_at")
        }
        FilterField::CreatedAtFrom => Some("related_classes.ancestor_created_at"),
        FilterField::UpdatedAt | FilterField::UpdatedAtTo => {
            Some("related_classes.descendant_updated_at")
        }
        FilterField::UpdatedAtFrom => Some("related_classes.ancestor_updated_at"),
        FilterField::Depth => Some("related_classes.depth"),
        FilterField::Path => Some("related_classes.path"),
        _ => None,
    }
}

fn build_related_classes_clause(
    param: &ParsedQueryParam,
    bind_variables: &mut Vec<SQLValue>,
) -> Result<Option<String>, ApiError> {
    use crate::models::search::{DataType, Operator};

    if param.field == FilterField::Permissions {
        return Ok(None);
    }

    let column = related_classes_column(&param.field).ok_or_else(|| {
        ApiError::BadRequest(format!(
            "Field '{}' isn't searchable (or does not exist) for related classes",
            param.field
        ))
    })?;

    let (op, negated) = param.operator.op_and_neg();
    let wrap = |sql: String| {
        if negated { format!("NOT ({sql})") } else { sql }
    };

    let clause = match param.field {
        FilterField::Id
        | FilterField::ClassFrom
        | FilterField::ClassTo
        | FilterField::ClassId
        | FilterField::Classes
        | FilterField::Collections
        | FilterField::CollectionId
        | FilterField::CollectionsFrom
        | FilterField::CollectionsTo
        | FilterField::Depth => {
            if !param.operator.is_applicable_to(DataType::NumericOrDate) {
                return Err(ApiError::OperatorMismatch(format!(
                    "Operator '{:?}' is not applicable to field '{}'",
                    param.operator, param.field
                )));
            }

            let values = param.value_as_integer()?;
            if values.is_empty() {
                return Err(ApiError::BadRequest(format!(
                    "Searching on field '{}' requires a value",
                    param.field
                )));
            }

            let max = *values.iter().max().unwrap();
            let min = *values.iter().min().unwrap();

            match op {
                Operator::Equals => {
                    let array_sql = sql_integer_array(&values, bind_variables);
                    wrap(format!("{column} = ANY({array_sql})"))
                }
                Operator::Gt => {
                    bind_variables.push(SQLValue::Integer(max));
                    wrap(format!("{column} > ?"))
                }
                Operator::Gte => {
                    bind_variables.push(SQLValue::Integer(max));
                    wrap(format!("{column} >= ?"))
                }
                Operator::Lt => {
                    bind_variables.push(SQLValue::Integer(min));
                    wrap(format!("{column} < ?"))
                }
                Operator::Lte => {
                    bind_variables.push(SQLValue::Integer(min));
                    wrap(format!("{column} <= ?"))
                }
                Operator::Between => {
                    if values.len() != 2 {
                        return Err(ApiError::OperatorMismatch(format!(
                            "Operator 'between' requires 2 values (min,max) for field '{}'",
                            param.field
                        )));
                    }
                    bind_variables.push(SQLValue::Integer(values[0]));
                    bind_variables.push(SQLValue::Integer(values[1]));
                    wrap(format!("{column} BETWEEN ? AND ?"))
                }
                _ => {
                    return Err(ApiError::OperatorMismatch(format!(
                        "Operator '{:?}' not implemented for field '{}' (type: numeric)",
                        param.operator, param.field
                    )));
                }
            }
        }
        FilterField::Name
        | FilterField::NameFrom
        | FilterField::NameTo
        | FilterField::Description
        | FilterField::DescriptionFrom
        | FilterField::DescriptionTo => {
            if !param.operator.is_applicable_to(DataType::String) {
                return Err(ApiError::OperatorMismatch(format!(
                    "Operator '{:?}' is not applicable to field '{}'",
                    param.operator, param.field
                )));
            }

            let value = param.value.clone();
            match op {
                Operator::Equals => {
                    bind_variables.push(SQLValue::String(value));
                    wrap(format!("{column} = ?"))
                }
                Operator::IEquals => {
                    bind_variables.push(SQLValue::String(value));
                    wrap(format!("{column} ILIKE ?"))
                }
                Operator::Contains => {
                    bind_variables.push(SQLValue::String(format!("%{value}%")));
                    wrap(format!("{column} LIKE ?"))
                }
                Operator::IContains => {
                    bind_variables.push(SQLValue::String(format!("%{value}%")));
                    wrap(format!("{column} ILIKE ?"))
                }
                Operator::StartsWith => {
                    bind_variables.push(SQLValue::String(format!("{value}%")));
                    wrap(format!("{column} LIKE ?"))
                }
                Operator::IStartsWith => {
                    bind_variables.push(SQLValue::String(format!("{value}%")));
                    wrap(format!("{column} ILIKE ?"))
                }
                Operator::EndsWith => {
                    bind_variables.push(SQLValue::String(format!("%{value}")));
                    wrap(format!("{column} LIKE ?"))
                }
                Operator::IEndsWith => {
                    bind_variables.push(SQLValue::String(format!("%{value}")));
                    wrap(format!("{column} ILIKE ?"))
                }
                Operator::Like => {
                    bind_variables.push(SQLValue::String(value));
                    wrap(format!("{column} LIKE ?"))
                }
                Operator::Regex => {
                    bind_variables.push(SQLValue::String(value));
                    wrap(format!("{column} ~ ?"))
                }
                _ => {
                    return Err(ApiError::OperatorMismatch(format!(
                        "Operator '{:?}' not implemented for field '{}' (type: string)",
                        param.operator, param.field
                    )));
                }
            }
        }
        FilterField::CreatedAt
        | FilterField::CreatedAtFrom
        | FilterField::CreatedAtTo
        | FilterField::UpdatedAt
        | FilterField::UpdatedAtFrom
        | FilterField::UpdatedAtTo => {
            if !param.operator.is_applicable_to(DataType::NumericOrDate) {
                return Err(ApiError::OperatorMismatch(format!(
                    "Operator '{:?}' is not applicable to field '{}'",
                    param.operator, param.field
                )));
            }

            let values = param.value_as_date()?;
            if values.is_empty() {
                return Err(ApiError::BadRequest(format!(
                    "Searching on field '{}' requires a value",
                    param.field
                )));
            }

            let max = *values.iter().max().unwrap();
            let min = *values.iter().min().unwrap();

            match op {
                Operator::Equals => {
                    let array_sql = sql_date_array(&values, bind_variables);
                    wrap(format!("{column} = ANY({array_sql})"))
                }
                Operator::Gt => {
                    bind_variables.push(SQLValue::Date(max));
                    wrap(format!("{column} > ?"))
                }
                Operator::Gte => {
                    bind_variables.push(SQLValue::Date(max));
                    wrap(format!("{column} >= ?"))
                }
                Operator::Lt => {
                    bind_variables.push(SQLValue::Date(min));
                    wrap(format!("{column} < ?"))
                }
                Operator::Lte => {
                    bind_variables.push(SQLValue::Date(min));
                    wrap(format!("{column} <= ?"))
                }
                Operator::Between => {
                    if values.len() != 2 {
                        return Err(ApiError::OperatorMismatch(format!(
                            "Operator 'between' requires 2 values (min,max) for field '{}'",
                            param.field
                        )));
                    }
                    bind_variables.push(SQLValue::Date(values[0]));
                    bind_variables.push(SQLValue::Date(values[1]));
                    wrap(format!("{column} BETWEEN ? AND ?"))
                }
                _ => {
                    return Err(ApiError::OperatorMismatch(format!(
                        "Operator '{:?}' not implemented for field '{}' (type: date)",
                        param.operator, param.field
                    )));
                }
            }
        }
        FilterField::Path => {
            if !param.operator.is_applicable_to(DataType::Array) {
                return Err(ApiError::OperatorMismatch(format!(
                    "Operator '{:?}' is not applicable to field '{}'",
                    param.operator, param.field
                )));
            }

            let values = param.value_as_integer()?;
            let array_sql = sql_integer_array(&values, bind_variables);
            match op {
                Operator::Contains => wrap(format!("{column} @> {array_sql}")),
                Operator::Equals => wrap(format!("{column} = {array_sql}")),
                _ => {
                    return Err(ApiError::OperatorMismatch(format!(
                        "Operator '{:?}' not implemented for field '{}' (type: array)",
                        param.operator, param.field
                    )));
                }
            }
        }
        _ => {
            return Err(ApiError::BadRequest(format!(
                "Field '{}' isn't searchable (or does not exist) for related classes",
                param.field
            )));
        }
    };

    Ok(Some(clause))
}

fn related_objects_column(field: &FilterField) -> Option<&'static str> {
    match field {
        FilterField::ObjectFrom => Some("related_objects.ancestor_object_id"),
        FilterField::Id | FilterField::ObjectTo => Some("related_objects.descendant_object_id"),
        FilterField::ClassFrom => Some("related_objects.ancestor_class_id"),
        FilterField::ClassId | FilterField::Classes | FilterField::ClassTo => {
            Some("related_objects.descendant_class_id")
        }
        FilterField::CollectionsFrom => Some("related_objects.ancestor_collection_id"),
        FilterField::Collections | FilterField::CollectionId | FilterField::CollectionsTo => {
            Some("related_objects.descendant_collection_id")
        }
        FilterField::NameFrom => Some("related_objects.ancestor_name"),
        FilterField::Name | FilterField::NameTo => Some("related_objects.descendant_name"),
        FilterField::DescriptionFrom => Some("related_objects.ancestor_description"),
        FilterField::Description | FilterField::DescriptionTo => {
            Some("related_objects.descendant_description")
        }
        FilterField::CreatedAtFrom => Some("related_objects.ancestor_created_at"),
        FilterField::CreatedAt | FilterField::CreatedAtTo => {
            Some("related_objects.descendant_created_at")
        }
        FilterField::UpdatedAtFrom => Some("related_objects.ancestor_updated_at"),
        FilterField::UpdatedAt | FilterField::UpdatedAtTo => {
            Some("related_objects.descendant_updated_at")
        }
        FilterField::Depth => Some("related_objects.depth"),
        FilterField::Path => Some("related_objects.path"),
        _ => None,
    }
}

fn build_related_objects_clause(
    param: &ParsedQueryParam,
    bind_variables: &mut Vec<SQLValue>,
) -> Result<Option<String>, ApiError> {
    use crate::models::search::{DataType, Operator};

    if param.field == FilterField::Permissions {
        return Ok(None);
    }

    if param.field == FilterField::JsonDataFrom || param.field == FilterField::JsonDataTo {
        let jsonb_field_expr = if param.field == FilterField::JsonDataFrom {
            "related_objects.ancestor_data"
        } else {
            "related_objects.descendant_data"
        };

        let predicate = param.as_json_sql_for_field_expr(jsonb_field_expr)?;
        bind_variables.extend(predicate.bind_variables);
        return Ok(Some(format!("({})", predicate.sql)));
    }

    let column = related_objects_column(&param.field).ok_or_else(|| {
        ApiError::BadRequest(format!(
            "Field '{}' isn't searchable (or does not exist) for object relations",
            param.field
        ))
    })?;

    let (op, negated) = param.operator.op_and_neg();
    let wrap = |sql: String| {
        if negated { format!("NOT ({sql})") } else { sql }
    };

    let clause = match param.field {
        FilterField::ObjectFrom
        | FilterField::Id
        | FilterField::ObjectTo
        | FilterField::ClassFrom
        | FilterField::ClassId
        | FilterField::Classes
        | FilterField::ClassTo
        | FilterField::Collections
        | FilterField::CollectionId
        | FilterField::CollectionsFrom
        | FilterField::CollectionsTo
        | FilterField::Depth => {
            if !param.operator.is_applicable_to(DataType::NumericOrDate) {
                return Err(ApiError::OperatorMismatch(format!(
                    "Operator '{:?}' is not applicable to field '{}'",
                    param.operator, param.field
                )));
            }

            let values = param.value_as_integer()?;
            if values.is_empty() {
                return Err(ApiError::BadRequest(format!(
                    "Searching on field '{}' requires a value",
                    param.field
                )));
            }

            let max = *values.iter().max().unwrap();
            let min = *values.iter().min().unwrap();

            match op {
                Operator::Equals => {
                    if values.len() > 50 {
                        return Err(ApiError::OperatorMismatch(format!(
                            "Operator 'equals' is limited to 50 values, got {} (use between?)",
                            values.len()
                        )));
                    }
                    let array_sql = sql_integer_array(&values, bind_variables);
                    wrap(format!("{column} = ANY({array_sql})"))
                }
                Operator::Gt => {
                    bind_variables.push(SQLValue::Integer(max));
                    wrap(format!("{column} > ?"))
                }
                Operator::Gte => {
                    bind_variables.push(SQLValue::Integer(max));
                    wrap(format!("{column} >= ?"))
                }
                Operator::Lt => {
                    bind_variables.push(SQLValue::Integer(min));
                    wrap(format!("{column} < ?"))
                }
                Operator::Lte => {
                    bind_variables.push(SQLValue::Integer(min));
                    wrap(format!("{column} <= ?"))
                }
                Operator::Between => {
                    if values.len() != 2 {
                        return Err(ApiError::OperatorMismatch(format!(
                            "Operator 'between' requires 2 values (min,max) for field '{}'",
                            param.field
                        )));
                    }
                    bind_variables.push(SQLValue::Integer(values[0]));
                    bind_variables.push(SQLValue::Integer(values[1]));
                    wrap(format!("{column} BETWEEN ? AND ?"))
                }
                _ => {
                    return Err(ApiError::OperatorMismatch(format!(
                        "Operator '{:?}' not implemented for field '{}' (type: numeric)",
                        param.operator, param.field
                    )));
                }
            }
        }
        FilterField::Name
        | FilterField::NameFrom
        | FilterField::NameTo
        | FilterField::Description
        | FilterField::DescriptionFrom
        | FilterField::DescriptionTo => {
            if !param.operator.is_applicable_to(DataType::String) {
                return Err(ApiError::OperatorMismatch(format!(
                    "Operator '{:?}' is not applicable to field '{}'",
                    param.operator, param.field
                )));
            }

            let value = param.value.clone();
            if value.is_empty() {
                return Err(ApiError::BadRequest(format!(
                    "Searching on field '{}' requires a value",
                    param.field
                )));
            }

            match op {
                Operator::Equals => {
                    bind_variables.push(SQLValue::String(value));
                    wrap(format!("{column} = ?"))
                }
                Operator::IEquals => {
                    bind_variables.push(SQLValue::String(value));
                    wrap(format!("{column} ILIKE ?"))
                }
                Operator::Contains => {
                    bind_variables.push(SQLValue::String(format!("%{value}%")));
                    wrap(format!("{column} LIKE ?"))
                }
                Operator::IContains => {
                    bind_variables.push(SQLValue::String(format!("%{value}%")));
                    wrap(format!("{column} ILIKE ?"))
                }
                Operator::StartsWith => {
                    bind_variables.push(SQLValue::String(format!("{value}%")));
                    wrap(format!("{column} LIKE ?"))
                }
                Operator::IStartsWith => {
                    bind_variables.push(SQLValue::String(format!("{value}%")));
                    wrap(format!("{column} ILIKE ?"))
                }
                Operator::EndsWith => {
                    bind_variables.push(SQLValue::String(format!("%{value}")));
                    wrap(format!("{column} LIKE ?"))
                }
                Operator::IEndsWith => {
                    bind_variables.push(SQLValue::String(format!("%{value}")));
                    wrap(format!("{column} ILIKE ?"))
                }
                Operator::Like => {
                    bind_variables.push(SQLValue::String(value));
                    wrap(format!("{column} LIKE ?"))
                }
                Operator::Regex => {
                    bind_variables.push(SQLValue::String(value));
                    wrap(format!("{column} ~ ?"))
                }
                _ => {
                    return Err(ApiError::OperatorMismatch(format!(
                        "Operator '{:?}' not implemented for field '{}' (type: string)",
                        param.operator, param.field
                    )));
                }
            }
        }
        FilterField::CreatedAt
        | FilterField::CreatedAtFrom
        | FilterField::CreatedAtTo
        | FilterField::UpdatedAt
        | FilterField::UpdatedAtFrom
        | FilterField::UpdatedAtTo => {
            if !param.operator.is_applicable_to(DataType::NumericOrDate) {
                return Err(ApiError::OperatorMismatch(format!(
                    "Operator '{:?}' is not applicable to field '{}'",
                    param.operator, param.field
                )));
            }

            let values = param.value_as_date()?;
            if values.is_empty() {
                return Err(ApiError::BadRequest(format!(
                    "Searching on field '{}' requires a value",
                    param.field
                )));
            }

            let max = *values.iter().max().unwrap();
            let min = *values.iter().min().unwrap();

            match op {
                Operator::Equals => {
                    let array_sql = sql_date_array(&values, bind_variables);
                    wrap(format!("{column} = ANY({array_sql})"))
                }
                Operator::Gt => {
                    bind_variables.push(SQLValue::Date(max));
                    wrap(format!("{column} > ?"))
                }
                Operator::Gte => {
                    bind_variables.push(SQLValue::Date(max));
                    wrap(format!("{column} >= ?"))
                }
                Operator::Lt => {
                    bind_variables.push(SQLValue::Date(min));
                    wrap(format!("{column} < ?"))
                }
                Operator::Lte => {
                    bind_variables.push(SQLValue::Date(min));
                    wrap(format!("{column} <= ?"))
                }
                Operator::Between => {
                    if values.len() != 2 {
                        return Err(ApiError::OperatorMismatch(format!(
                            "Operator 'between' requires 2 values (min,max) for field '{}'",
                            param.field
                        )));
                    }
                    bind_variables.push(SQLValue::Date(values[0]));
                    bind_variables.push(SQLValue::Date(values[1]));
                    wrap(format!("{column} BETWEEN ? AND ?"))
                }
                _ => {
                    return Err(ApiError::OperatorMismatch(format!(
                        "Operator '{:?}' not implemented for field '{}' (type: date)",
                        param.operator, param.field
                    )));
                }
            }
        }
        FilterField::Path => {
            if !param.operator.is_applicable_to(DataType::Array) {
                return Err(ApiError::OperatorMismatch(format!(
                    "Operator '{:?}' is not applicable to field '{}'",
                    param.operator, param.field
                )));
            }

            let values = param.value_as_integer()?;
            if values.is_empty() {
                return Err(ApiError::BadRequest(format!(
                    "Searching on field '{}' requires a value",
                    param.field
                )));
            }
            let array_sql = sql_integer_array(&values, bind_variables);
            match op {
                Operator::Contains => wrap(format!("{column} @> {array_sql}")),
                Operator::Equals => wrap(format!("{column} = {array_sql}")),
                _ => {
                    return Err(ApiError::OperatorMismatch(format!(
                        "Operator '{:?}' not implemented for field '{}' (type: array)",
                        param.operator, param.field
                    )));
                }
            }
        }
        _ => {
            return Err(ApiError::BadRequest(format!(
                "Field '{}' isn't searchable (or does not exist) for object relations",
                param.field
            )));
        }
    };

    Ok(Some(clause))
}

impl<T: ?Sized> UserSearchBackend for T where T: UserCollectionAccessors {}

impl User {
    pub async fn search_users(
        &self,
        pool: &impl crate::storage::StorageContext,
        query_options: QueryOptions,
    ) -> Result<Vec<crate::models::UserWithName>, ApiError> {
        use crate::schema::identity_scopes;
        use crate::schema::principals;
        use crate::schema::users::dsl::{created_at, email, id, proper_name, updated_at, users};

        let query_params = query_options.filters.clone();

        debug!(
            message = "Searching users",
            stage = "Starting",
            user_id = self.principal_id(),
            query_params = ?query_params
        );

        let mut base_query = users
            .inner_join(principals::table.on(id.eq(principals::id)))
            .inner_join(
                identity_scopes::table.on(principals::identity_scope_id.eq(identity_scopes::id)),
            )
            .into_boxed();

        for param in query_params {
            let operator = param.operator.clone();
            match param.field {
                FilterField::Id => numeric_search!(base_query, param, operator, id),
                FilterField::Name | FilterField::Username => {
                    string_search!(base_query, param, operator, principals::name)
                }
                FilterField::IdentityScope => {
                    string_search!(base_query, param, operator, identity_scopes::name)
                }
                FilterField::ProperName => {
                    string_search!(base_query, param, operator, proper_name)
                }
                FilterField::Email => string_search!(base_query, param, operator, email),
                FilterField::CreatedAt => date_search!(base_query, param, operator, created_at),
                FilterField::UpdatedAt => date_search!(base_query, param, operator, updated_at),
                FilterField::Revision => {
                    revision_search!(base_query, param, operator, principals::revision)
                }
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for users",
                        param.field
                    )));
                }
            }
        }

        crate::apply_query_options!(base_query, query_options, crate::models::UserWithName);

        trace_query!(base_query, "Searching users");

        let rows = with_connection(pool, async |conn| {
            base_query
                .select((
                    crate::schema::users::all_columns,
                    identity_scopes::name,
                    identity_scopes::provider_kind,
                    principals::name,
                    principals::provider_managed,
                    principals::last_sync_attempted_at,
                    principals::last_sync_success_at,
                    principals::revision,
                ))
                .distinct() // TODO: Is it the joins that makes this required?
                .load::<(
                    User,
                    String,
                    String,
                    String,
                    bool,
                    Option<chrono::NaiveDateTime>,
                    Option<chrono::NaiveDateTime>,
                    crate::models::ResourceRevision,
                )>(conn)
                .await
        })
        .await?;

        Ok(rows
            .into_iter()
            .map(crate::models::UserWithName::from_tuple)
            .collect())
    }

    pub async fn count_users(
        &self,
        pool: &impl crate::storage::StorageContext,
        query_options: QueryOptions,
    ) -> Result<i64, ApiError> {
        use crate::schema::identity_scopes;
        use crate::schema::principals;
        use crate::schema::users::dsl::{created_at, email, id, proper_name, updated_at, users};

        let query_params = query_options.filters.clone();
        let mut base_query = users
            .inner_join(principals::table.on(id.eq(principals::id)))
            .inner_join(
                identity_scopes::table.on(principals::identity_scope_id.eq(identity_scopes::id)),
            )
            .into_boxed();

        for param in query_params {
            let operator = param.operator.clone();
            match param.field {
                FilterField::Id => numeric_search!(base_query, param, operator, id),
                FilterField::Name | FilterField::Username => {
                    string_search!(base_query, param, operator, principals::name)
                }
                FilterField::IdentityScope => {
                    string_search!(base_query, param, operator, identity_scopes::name)
                }
                FilterField::ProperName => {
                    string_search!(base_query, param, operator, proper_name)
                }
                FilterField::Email => string_search!(base_query, param, operator, email),
                FilterField::CreatedAt => date_search!(base_query, param, operator, created_at),
                FilterField::UpdatedAt => date_search!(base_query, param, operator, updated_at),
                FilterField::Revision => {
                    revision_search!(base_query, param, operator, principals::revision)
                }
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for users",
                        param.field
                    )));
                }
            }
        }

        with_connection(pool, async |conn| {
            base_query
                .select(id)
                .distinct()
                .count()
                .get_result::<i64>(conn)
                .await
        })
        .await
    }

    pub async fn search_groups(
        &self,
        pool: &impl crate::storage::StorageContext,
        query_options: QueryOptions,
    ) -> Result<Vec<Group>, ApiError> {
        use crate::schema::groups::dsl::{
            created_at, description, groupname, groups, id, revision, updated_at,
        };
        use crate::schema::identity_scopes;

        let query_params = query_options.filters.clone();

        debug!(
            message = "Searching groups",
            stage = "Starting",
            user_id = self.principal_id(),
            query_params = ?query_params
        );

        let mut base_query = groups.inner_join(identity_scopes::table).into_boxed();

        for param in query_params {
            let operator = param.operator.clone();
            match param.field {
                FilterField::Id => numeric_search!(base_query, param, operator, id),
                FilterField::Name => string_search!(base_query, param, operator, groupname),
                FilterField::Groupname => string_search!(base_query, param, operator, groupname),
                FilterField::IdentityScope => {
                    string_search!(base_query, param, operator, identity_scopes::name)
                }
                FilterField::Description => {
                    string_search!(base_query, param, operator, description)
                }
                FilterField::CreatedAt => date_search!(base_query, param, operator, created_at),
                FilterField::UpdatedAt => date_search!(base_query, param, operator, updated_at),
                FilterField::Revision => revision_search!(base_query, param, operator, revision),
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for groups",
                        param.field
                    )));
                }
            }
        }

        crate::apply_query_options!(base_query, query_options, Group);

        trace_query!(base_query, "Searching groups");

        let result = with_connection(pool, async |conn| {
            base_query
                .select(groups::all_columns())
                .distinct()
                .load::<Group>(conn)
                .await
        })
        .await?;

        Ok(result)
    }

    pub async fn count_groups(
        &self,
        pool: &impl crate::storage::StorageContext,
        query_options: QueryOptions,
    ) -> Result<i64, ApiError> {
        use crate::schema::groups::dsl::{
            created_at, description, groupname, groups, id, revision, updated_at,
        };
        use crate::schema::identity_scopes;

        let query_params = query_options.filters.clone();
        let mut base_query = groups.inner_join(identity_scopes::table).into_boxed();

        for param in query_params {
            let operator = param.operator.clone();
            match param.field {
                FilterField::Id => numeric_search!(base_query, param, operator, id),
                FilterField::Name => string_search!(base_query, param, operator, groupname),
                FilterField::Groupname => string_search!(base_query, param, operator, groupname),
                FilterField::IdentityScope => {
                    string_search!(base_query, param, operator, identity_scopes::name)
                }
                FilterField::Description => {
                    string_search!(base_query, param, operator, description)
                }
                FilterField::CreatedAt => date_search!(base_query, param, operator, created_at),
                FilterField::UpdatedAt => date_search!(base_query, param, operator, updated_at),
                FilterField::Revision => revision_search!(base_query, param, operator, revision),
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for groups",
                        param.field
                    )));
                }
            }
        }

        with_connection(pool, async |conn| {
            base_query
                .select(id)
                .distinct()
                .count()
                .get_result::<i64>(conn)
                .await
        })
        .await
    }
}

#[cfg(test)]
mod related_filter_tests {
    use super::*;
    use crate::models::search::parse_query_parameter_with_computed_and_related_filters_and_passthrough;
    use crate::models::{HubuumObjectID, TokenResourceScope};

    #[test]
    fn related_target_seed_applies_class_resource_scope() {
        let (query, _) = parse_query_parameter_with_computed_and_related_filters_and_passthrough(
            "related.room.class.id=7&related.room.object.name=foo",
            &[],
        )
        .unwrap();
        let groups = related_filter_groups(&query.filters).unwrap();
        let scope = TokenScope::from_request_parts(
            None,
            Some(vec![TokenResourceScope::Object(
                HubuumObjectID::new(11).unwrap(),
            )]),
        )
        .unwrap()
        .unwrap();

        let component = build_related_object_filter_sql(&groups, &[2], &[2], Some(&scope)).unwrap();

        assert!(
            component
                .sql
                .contains("target_class.id IN (SELECT class_id FROM valid_scope_classes)")
        );
        assert!(component.sql.contains(
            "SELECT id AS class_id FROM hubuumclass WHERE collection_id = ANY(ARRAY[]::integer[]) OR id = ANY(ARRAY[]::integer[])"
        ));
    }

    #[test]
    fn related_external_traversal_rejects_work_over_budget() {
        let mut budget = RelatedTraversalBudget::new(1).unwrap();
        let error = budget
            .record_objects(MAX_EXTERNAL_RELATED_FILTER_OBJECTS)
            .unwrap_err();

        assert!(
            matches!(error, ApiError::BadRequest(message) if message.contains("10000 objects"))
        );
    }

    #[test]
    fn related_revision_equality_rejects_multiple_values() {
        let (query, _) = parse_query_parameter_with_computed_and_related_filters_and_passthrough(
            "related.room.class.id=7&related.room.object.revision=1,2",
            &[],
        )
        .unwrap();
        let revision_filter = query
            .filters
            .iter()
            .find(|filter| {
                filter.field.related_query().is_some_and(|field| {
                    field.target() == RelatedFilterTarget::Object(RelatedObjectField::Revision)
                })
            })
            .unwrap();

        let error = related_revision_clause(revision_filter, "target_object.revision", &mut vec![])
            .unwrap_err();

        assert_eq!(
            error,
            ApiError::OperatorMismatch(
                "Operator 'equals' requires exactly 1 value for field 'related.room.object.revision'"
                    .to_string()
            )
        );
    }
}
