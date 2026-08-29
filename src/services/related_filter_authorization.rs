use std::collections::{BTreeMap, HashMap, HashSet};

use tracing::debug;

use crate::errors::ApiError;
use crate::models::search::{
    DEFAULT_RELATED_FILTER_DEPTH, FilterField, ParsedQueryParam, ParsedQueryParamExt, QueryOptions,
    RelatedClassField, RelatedFilterTarget, RelatedObjectField, SearchOperator,
};
use crate::models::{
    HubuumClassExpanded, HubuumObject, HubuumObjectRelation, Permissions, TokenScope,
};
use crate::permissions::visibility::{
    AuthorizedObjectIds, authorize_all_candidates, authorize_resource_permissions,
};
use crate::permissions::{
    PermissionBackend, PrincipalRef, ResourceAttrs, ResourceKind, ResourceRef,
};
use crate::services::catalog;
use crate::services::relation_queries::{self, RelationAccess};
use crate::storage::StorageContext;
use crate::traits::scope_allows;

const MAX_EXTERNAL_RELATED_FILTER_TARGETS: usize = 1_000;
const MAX_EXTERNAL_RELATED_FILTER_OBJECTS: usize = 10_000;
const MAX_EXTERNAL_RELATED_FILTER_RELATIONS: usize = 20_000;

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

pub(crate) async fn externally_authorized_related_object_ids(
    storage: &impl StorageContext,
    permission_backend: &dyn PermissionBackend,
    principal: &PrincipalRef,
    scopes: Option<&TokenScope>,
    filters: &[ParsedQueryParam],
) -> Result<Option<AuthorizedObjectIds>, ApiError> {
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
        scopes,
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
            load_related_target_class(storage, principal.user_id, group.class_filter).await?
        else {
            return Ok(Some(AuthorizedObjectIds::empty()));
        };
        if !authorize_resource_permissions(
            permission_backend,
            principal,
            &class_resource(&target_class),
            scopes,
            &[Permissions::ReadClass, Permissions::ReadCollection],
        )
        .await?
        {
            return Ok(Some(AuthorizedObjectIds::empty()));
        }

        let mut target_query = related_target_query(&group, target_class.id)?;
        target_query.set_limit(Some(MAX_EXTERNAL_RELATED_FILTER_TARGETS + 1))?;
        let (target_candidates, _) =
            catalog::list_objects(storage, principal.user_id, true, None, target_query).await?;
        RelatedTraversalResource::TargetObjects.ensure_count(target_candidates.len())?;
        let target_objects = authorize_all_candidates(
            permission_backend,
            principal,
            target_candidates,
            scopes,
            vec![Permissions::ReadObject],
            object_resource,
        )
        .await?;
        if target_objects.is_empty() {
            return Ok(Some(AuthorizedObjectIds::empty()));
        }

        let group_matches = externally_authorized_related_group_ids(
            storage,
            permission_backend,
            principal,
            scopes,
            target_objects,
            group.max_depth,
        )
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
    storage: &impl StorageContext,
    principal_id: i32,
    class_filter: &ParsedQueryParam,
) -> Result<Option<HubuumClassExpanded>, ApiError> {
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
    let field = match class_field {
        RelatedClassField::Id => {
            let values = class_filter.value_as_integer()?;
            if values.len() != 1 {
                return Err(ApiError::BadRequest(
                    "related.<alias>.class.id requires exactly one integer".to_string(),
                ));
            }
            FilterField::Id
        }
        RelatedClassField::Name => FilterField::Name,
    };
    let (mut classes, _) = catalog::list_classes(
        storage,
        principal_id,
        true,
        None,
        QueryOptions::new(
            vec![ParsedQueryParam {
                field,
                operator: class_filter.operator.clone(),
                value: class_filter.value.clone(),
            }],
            Vec::new(),
            Some(2),
            None,
            false,
        )?,
    )
    .await?;
    if classes.len() > 1 {
        return Err(ApiError::BadRequest(
            "Related class selector matched more than one class".to_string(),
        ));
    }
    Ok(classes.pop())
}

fn related_target_query(
    group: &RelatedFilterGroup<'_>,
    class_id: i32,
) -> Result<QueryOptions, ApiError> {
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
    Ok(QueryOptions::new(filters, Vec::new(), None, None, false)?)
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

async fn externally_authorized_related_group_ids(
    storage: &impl StorageContext,
    permission_backend: &dyn PermissionBackend,
    principal: &PrincipalRef,
    scopes: Option<&TokenScope>,
    target_objects: Vec<HubuumObject>,
    max_depth: i32,
) -> Result<HashSet<i32>, ApiError> {
    let mut objects = target_objects
        .into_iter()
        .map(|object| (object.id, object))
        .collect::<HashMap<_, _>>();
    let mut visible_objects = objects.keys().copied().collect::<HashSet<_>>();
    let mut examined_objects = visible_objects.clone();
    let mut budget = RelatedTraversalBudget::new(examined_objects.len())?;
    let mut frontier = visible_objects.clone();
    let mut seen_relation_ids = HashSet::new();
    let mut matches = HashSet::new();

    for _ in 0..max_depth {
        if frontier.is_empty() {
            break;
        }
        let frontier_ids = sorted_ids(&frontier);
        let excluded_relation_ids = sorted_ids(&seen_relation_ids);
        let mut relation_candidates = relation_queries::list_object_relations_touching_ids(
            storage,
            RelationAccess::new(principal.user_id, true, None),
            &frontier_ids,
            &excluded_relation_ids,
            budget.relation_query_limit(),
        )
        .await?;
        relation_candidates.retain(|relation| seen_relation_ids.insert(relation.id));
        budget.record_relations(relation_candidates.len())?;
        if relation_candidates.is_empty() {
            break;
        }

        let endpoint_ids = relation_candidates
            .iter()
            .flat_map(|relation| [relation.from_hubuum_object_id, relation.to_hubuum_object_id])
            .filter(|object_id| !objects.contains_key(object_id))
            .collect::<HashSet<_>>();
        objects.extend(
            load_objects_by_ids(storage, principal.user_id, &sorted_ids(&endpoint_ids))
                .await?
                .into_iter()
                .map(|object| (object.id, object)),
        );
        let relation_resources = relation_candidates
            .iter()
            .map(|relation| relation_resource(relation, &objects))
            .collect::<Result<Vec<_>, _>>()?;
        let visible_relations = authorize_all_candidates(
            permission_backend,
            principal,
            relation_candidates
                .into_iter()
                .zip(relation_resources)
                .collect::<Vec<_>>(),
            scopes,
            vec![Permissions::ReadObjectRelation],
            |(_, resource)| resource.clone(),
        )
        .await?
        .into_iter()
        .map(|(relation, _)| relation)
        .collect::<Vec<_>>();

        let new_object_ids = visible_relations
            .iter()
            .flat_map(|relation| [relation.from_hubuum_object_id, relation.to_hubuum_object_id])
            .filter(|object_id| !examined_objects.contains(object_id))
            .collect::<HashSet<_>>();
        budget.record_objects(new_object_ids.len())?;
        examined_objects.extend(new_object_ids.iter().copied());
        let object_candidates = sorted_ids(&new_object_ids)
            .into_iter()
            .map(|object_id| {
                objects
                    .get(&object_id)
                    .cloned()
                    .ok_or_else(|| missing_object(object_id))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let newly_visible_objects = authorize_all_candidates(
            permission_backend,
            principal,
            object_candidates,
            scopes,
            vec![Permissions::ReadObject],
            object_resource,
        )
        .await?
        .into_iter()
        .map(|object| object.id)
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

async fn load_objects_by_ids(
    storage: &impl StorageContext,
    principal_id: i32,
    object_ids: &[i32],
) -> Result<Vec<HubuumObject>, ApiError> {
    let mut objects = Vec::with_capacity(object_ids.len());
    for chunk in object_ids.chunks(hubuum_query::MAX_INTEGER_FILTER_VALUES) {
        let (mut rows, _) = catalog::list_objects(
            storage,
            principal_id,
            true,
            None,
            QueryOptions::new(
                vec![ParsedQueryParam {
                    field: FilterField::Id,
                    operator: SearchOperator::In { is_negated: false },
                    value: chunk
                        .iter()
                        .map(i32::to_string)
                        .collect::<Vec<_>>()
                        .join(","),
                }],
                Vec::new(),
                None,
                None,
                false,
            )?,
        )
        .await?;
        objects.append(&mut rows);
    }
    if objects.len() != object_ids.len() {
        return Err(ApiError::InternalServerError(
            "Related filter traversal references missing objects".to_string(),
        ));
    }
    Ok(objects)
}

fn sorted_ids(ids: &HashSet<i32>) -> Vec<i32> {
    let mut ids = ids.iter().copied().collect::<Vec<_>>();
    ids.sort_unstable();
    ids
}

fn class_resource(class: &HubuumClassExpanded) -> ResourceRef {
    ResourceRef {
        kind: ResourceKind::Class,
        id: class.id,
        attrs: ResourceAttrs {
            collection_id: Some(class.collection.id),
            name: Some(class.name.clone()),
            ..Default::default()
        },
    }
}

fn object_resource(object: &HubuumObject) -> ResourceRef {
    ResourceRef {
        kind: ResourceKind::Object,
        id: object.id,
        attrs: ResourceAttrs {
            collection_id: Some(object.collection_id),
            class_id: Some(object.hubuum_class_id),
            name: Some(object.name.clone()),
            ..Default::default()
        },
    }
}

fn relation_resource(
    relation: &HubuumObjectRelation,
    objects: &HashMap<i32, HubuumObject>,
) -> Result<ResourceRef, ApiError> {
    let from = objects
        .get(&relation.from_hubuum_object_id)
        .ok_or_else(|| missing_object(relation.from_hubuum_object_id))?;
    let to = objects
        .get(&relation.to_hubuum_object_id)
        .ok_or_else(|| missing_object(relation.to_hubuum_object_id))?;
    Ok(ResourceRef {
        kind: ResourceKind::ObjectRelation,
        id: relation.id,
        attrs: ResourceAttrs {
            collection_id: (from.collection_id == to.collection_id).then_some(from.collection_id),
            from_collection_id: Some(from.collection_id),
            to_collection_id: Some(to.collection_id),
            from_class_id: Some(from.hubuum_class_id),
            to_class_id: Some(to.hubuum_class_id),
            from_object_id: Some(from.id),
            to_object_id: Some(to.id),
            class_relation_id: Some(relation.class_relation_id),
            ..Default::default()
        },
    })
}

fn missing_object(object_id: i32) -> ApiError {
    ApiError::InternalServerError(format!(
        "Related filter traversal references missing object {object_id}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn traversal_budget_rejects_objects_beyond_the_safety_limit() {
        let mut budget = RelatedTraversalBudget::new(1).unwrap();

        let error = budget
            .record_objects(MAX_EXTERNAL_RELATED_FILTER_OBJECTS)
            .unwrap_err();

        assert!(
            matches!(error, ApiError::BadRequest(message) if message.contains("10000 objects"))
        );
    }

    #[test]
    fn relation_query_limit_requests_the_first_over_budget_row() {
        let mut budget = RelatedTraversalBudget::new(1).unwrap();
        budget
            .record_relations(MAX_EXTERNAL_RELATED_FILTER_RELATIONS)
            .unwrap();

        assert_eq!(budget.relation_query_limit(), 1);
    }
}
