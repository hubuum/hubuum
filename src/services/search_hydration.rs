use std::collections::{BTreeMap, BTreeSet};

use crate::can;
use crate::errors::ApiError;
use crate::models::search::parse_query_parameter;
use crate::models::traits::ResolveClassTarget;
use crate::models::{
    HubuumObject, ObjectSelector, Permissions, SearchHydrationPath, SearchHydrationResponse,
    TokenScope, ValidatedSearchHydration,
};
use crate::permissions::visibility::authorize_all_candidates;
use crate::permissions::{AppContext, ClassResourceEndpoint, PrincipalRef, ResourceRef};
use crate::services::catalog;
use crate::storage::StorageAuthenticationPrincipal;
use crate::traits::{PrincipalIdAccessor, Search, UserPermissions};

/// Hydrate only on explicit inspection. All traversal, intermediate object and
/// edge reads retain their normal authorization, including delegated policies.
pub(crate) async fn hydrate_search_object(
    context: &AppContext,
    principal: &StorageAuthenticationPrincipal,
    scopes: Option<&TokenScope>,
    validated: ValidatedSearchHydration,
) -> Result<SearchHydrationResponse, ApiError> {
    let request = validated.request();
    let root = context
        .object_service()
        .resolve(ObjectSelector::by_id(
            request.root_class_id,
            request.root_object_id,
        ))
        .await?;
    can!(
        context,
        principal,
        scopes,
        [Permissions::ReadObject],
        root.object()
    );
    let class = request
        .class
        .selector()?
        .resolve_class_target(context)
        .await?;
    can!(
        context,
        principal,
        scopes,
        [Permissions::ReadClass],
        class.class()
    );
    let options = parse_query_parameter(&format!(
        "class_id={}&depth__lte={}&limit={}&include_total=false&sort=path.asc,id.asc",
        class.class().id,
        request.depth,
        usize::from(request.limit) + 1,
    ))?;
    let mut hits = principal
        .search_objects_related_to(context, root.object().clone(), options, scopes)
        .await?;
    let truncated = hits.len() > usize::from(request.limit);
    hits.truncate(usize::from(request.limit));
    // Traversal excludes cycles. Still validate the representation before using
    // it to drive bounded secondary reads (at most 1 + 50 * 10 objects).
    let mut object_ids = BTreeSet::new();
    for hit in &hits {
        if hit.path.first() != Some(&root.object().id)
            || hit.path.last() != Some(&hit.descendant_object_id)
            || hit.path.len() < 2
            || hit.path.len() > usize::from(request.depth) + 1
            || hit.path.iter().collect::<BTreeSet<_>>().len() != hit.path.len()
        {
            return Err(ApiError::InternalServerError(
                "Invalid hydration path".into(),
            ));
        }
        object_ids.extend(hit.path.iter().copied());
    }
    let object_ids = object_ids.into_iter().collect::<Vec<_>>();
    let authorization = PrincipalRef::load(context, principal).await?;
    let mut objects = BTreeMap::new();
    for ids in object_ids.chunks(50) {
        let options = parse_query_parameter(&format!(
            "id={}&limit=50&include_total=false",
            ids.iter().map(i32::to_string).collect::<Vec<_>>().join(","),
        ))?;
        // Load a bounded candidate batch and authorize the actual returned
        // contents. Intermediate objects can move since traversal completed.
        let (candidates, _) =
            catalog::list_objects(context, principal.principal_id(), true, None, options).await?;
        let visible = authorize_all_candidates(
            context.permission_backend(),
            &authorization,
            candidates,
            scopes,
            vec![Permissions::ReadObject, Permissions::ReadCollection],
            |object: &HubuumObject| {
                ResourceRef::object(
                    object.id,
                    ClassResourceEndpoint::new(object.collection_id, object.hubuum_class_id),
                    Some(object.name.clone()),
                )
            },
        )
        .await?;
        objects.extend(visible.into_iter().map(|object| (object.id, object)));
    }
    let relations = if object_ids.is_empty() {
        Vec::new()
    } else {
        principal
            .search_object_relations_between_ids(context, &object_ids, scopes)
            .await?
    };
    let mut edges = BTreeMap::new();
    for relation in relations {
        let pair = (
            relation
                .from_hubuum_object_id
                .min(relation.to_hubuum_object_id),
            relation
                .from_hubuum_object_id
                .max(relation.to_hubuum_object_id),
        );
        edges.entry(pair).or_insert(relation);
    }
    let mut paths = Vec::new();
    let mut used_relations = BTreeMap::new();
    for hit in hits {
        if hit.path.iter().any(|id| !objects.contains_key(id)) {
            return Err(changed());
        }
        if objects[&root.object().id].hubuum_class_id != request.root_class_id.id()
            || objects[&hit.descendant_object_id].hubuum_class_id != class.class().id
        {
            return Err(changed());
        }
        let mut relation_ids = Vec::new();
        for pair in hit.path.windows(2) {
            let edge = edges
                .get(&(pair[0].min(pair[1]), pair[0].max(pair[1])))
                .ok_or_else(changed)?;
            relation_ids.push(edge.id);
            used_relations.insert(edge.id, *edge);
        }
        paths.push(SearchHydrationPath {
            object_ids: hit.path,
            relation_ids,
        });
    }
    Ok(SearchHydrationResponse {
        version: 1,
        root_object_id: root.object().id,
        target_class_id: class.class().id,
        depth: request.depth,
        limit: request.limit,
        objects: objects.into_values().collect(),
        relations: used_relations.into_values().collect(),
        paths,
        truncated,
    })
}

fn changed() -> ApiError {
    ApiError::Conflict("Related resources changed while loading. Retry inspection.".into())
}
