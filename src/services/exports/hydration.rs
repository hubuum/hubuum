use super::*;
use hubuum_domain::TemplateAlias;

pub(super) fn resolve_relation_hydration_plan(
    runtime: &ExportRuntime,
    query_options: &mut QueryOptions,
) -> Result<Option<RelationHydrationPlan>, ApiError> {
    let has_template = runtime.template.is_some();
    let scope = runtime.scope;
    let relation_context = runtime.export.relation_context.as_ref();

    if relation_context.is_some() && !has_template {
        return Err(ApiError::BadRequest(
            "relation_context is only supported for templated text exports".to_string(),
        ));
    }

    if relation_context.is_some()
        && !matches!(
            scope,
            ValidatedExportScope::ObjectsInClass(_) | ValidatedExportScope::RelatedObjects { .. }
        )
    {
        return Err(ApiError::BadRequest(
            "relation_context is only supported for objects_in_class and related_objects exports"
                .to_string(),
        ));
    }

    if !has_template {
        return Ok(None);
    }

    match scope {
        ValidatedExportScope::ObjectsInClass(_) => {
            let Some(context) = relation_context else {
                return Ok(None);
            };
            Ok(Some(RelationHydrationPlan {
                depth_limit: validate_relation_depth(context.depth.unwrap_or(2))?,
                enabled_for_scope: true,
            }))
        }
        ValidatedExportScope::RelatedObjects { .. } => {
            let depth_limit = validate_relation_depth(
                relation_context
                    .and_then(|context| context.depth)
                    .unwrap_or(2),
            )?;
            query_options
                .filters_mut()
                .try_retain(|filter| *filter.field() != FilterField::Depth)?;
            query_options.filters_mut().try_push(ParsedQueryParam::new(
                "depth",
                Some(SearchOperator::Lte { is_negated: false }),
                &depth_limit.to_string(),
            )?)?;
            Ok(Some(RelationHydrationPlan {
                depth_limit,
                enabled_for_scope: true,
            }))
        }
        _ => Ok(None),
    }
}

pub(super) fn validate_relation_depth(depth: i32) -> Result<i32, ApiError> {
    if !(1..=2).contains(&depth) {
        return Err(ApiError::BadRequest(
            "Templated relation hydration only supports depth 1 or 2".to_string(),
        ));
    }
    Ok(depth)
}

pub(super) async fn build_template_items<C, S>(
    exporter: &PermissionAwareExport<'_, C, S>,
    runtime: &ExportRuntime,
    items: &[serde_json::Value],
    relation_hydration: Option<RelationHydrationPlan>,
) -> Result<(Vec<serde_json::Value>, Option<serde_json::Value>), ApiError>
where
    C: AuthorizationContext,
    S: crate::traits::Search + ?Sized,
{
    let pool = exporter.pool();
    if runtime.template.is_none() {
        return Ok((items.to_vec(), None));
    }

    let Some(relation_hydration) = relation_hydration else {
        return Ok((items.to_vec(), None));
    };

    if !relation_hydration.enabled_for_scope {
        return Ok((items.to_vec(), None));
    }

    let mut hydration_budget = HydrationBudget::new(max_hydrated_template_objects());

    match runtime.scope {
        ValidatedExportScope::ObjectsInClass(_) => {
            let roots = items
                .iter()
                .cloned()
                .map(serde_json::from_value::<HubuumObject>)
                .collect::<Result<Vec<_>, _>>()?;
            if roots.is_empty() {
                return Ok((Vec::new(), None));
            }

            let root_ids = roots.iter().map(|root| root.id).collect::<Vec<_>>();
            let per_root_cap = i32::try_from(max_hydrated_template_objects()).unwrap_or(i32::MAX);
            let related_rows = exporter
                .bidirectionally_related_objects_for_roots(
                    &root_ids,
                    relation_hydration.depth_limit,
                    per_root_cap,
                )
                .await?;

            // Descendants grouped per root, preserving the query's per-root ordering.
            let mut related_by_root: BTreeMap<i32, Vec<HubuumObjectWithPath>> =
                root_ids.iter().map(|id| (*id, Vec::new())).collect();
            for row in &related_rows {
                if let Some(list) = related_by_root.get_mut(&row.root_object_id) {
                    list.push(row.to_descendant_object_with_path());
                }
            }

            // One relations fetch over the union of all roots + descendants.
            let mut all_object_ids = root_ids.clone();
            for row in &related_rows {
                all_object_ids.push(row.descendant_object_id);
            }
            all_object_ids.sort_unstable();
            all_object_ids.dedup();
            let all_relations = exporter
                .list_object_relations_between_ids(&all_object_ids)
                .await?;

            // One class-metadata fetch over every object in the export.
            let mut all_objects = BTreeMap::<i32, HubuumObjectWithPath>::new();
            for root in &roots {
                let root_with_path = object_with_root_path(root);
                all_objects.insert(root_with_path.id, root_with_path);
            }
            for row in &related_rows {
                let object = row.to_descendant_object_with_path();
                all_objects.entry(object.id).or_insert(object);
            }
            let class_metadata = load_hydration_class_metadata(exporter, &all_objects).await?;

            let mut hydrated_items = Vec::with_capacity(roots.len());
            for root in &roots {
                let root_with_path = object_with_root_path(root);
                let related = related_by_root.remove(&root.id).unwrap_or_default();
                let related = take_related_within_budget(&hydration_budget, related)?;

                let mut neighborhood_ids = related
                    .iter()
                    .map(|object| object.id)
                    .collect::<std::collections::HashSet<_>>();
                neighborhood_ids.insert(root.id);
                let relations = all_relations
                    .iter()
                    .filter(|relation| {
                        neighborhood_ids.contains(&relation.from_hubuum_object_id)
                            && neighborhood_ids.contains(&relation.to_hubuum_object_id)
                    })
                    .cloned()
                    .collect::<Vec<_>>();

                let neighborhood = build_object_neighborhood(
                    root_with_path.clone(),
                    related,
                    relations,
                    &class_metadata,
                )?;
                let hydrated = hydrate_object(
                    &neighborhood,
                    root_with_path.id,
                    vec![root_with_path.id],
                    relation_hydration.depth_limit,
                    &mut hydration_budget,
                )?;
                hydrated_items.push(serde_json::to_value(hydrated)?);
            }

            Ok((hydrated_items, None))
        }
        ValidatedExportScope::RelatedObjects { object_id, .. } => {
            let source_object = object_id.instance(pool).await?;
            let source = object_with_root_path(&source_object);
            let related_objects = items
                .iter()
                .cloned()
                .map(serde_json::from_value::<HubuumObjectWithPath>)
                .collect::<Result<Vec<_>, _>>()?;
            let hydrated = hydrate_related_root(
                exporter,
                source,
                related_objects,
                relation_hydration.depth_limit,
                &mut hydration_budget,
            )
            .await?;
            let source = serde_json::to_value(&hydrated)?;
            Ok((vec![source.clone()], Some(source)))
        }
        _ => Ok((items.to_vec(), None)),
    }
}

pub(super) async fn hydrate_related_root<C, S>(
    exporter: &PermissionAwareExport<'_, C, S>,
    source: HubuumObjectWithPath,
    related_objects: Vec<HubuumObjectWithPath>,
    depth_limit: i32,
    hydration_budget: &mut HydrationBudget,
) -> Result<HydratedTemplateObject, ApiError>
where
    C: AuthorizationContext,
    S: crate::traits::Search + ?Sized,
{
    let max_related_objects = hydration_budget.remaining_related_capacity()?;
    if related_objects.len() > max_related_objects {
        return Err(ApiError::BadRequest(format!(
            "Hydrated template object limit exceeded ({} related objects > {} remaining related capacity)",
            related_objects.len(),
            max_related_objects
        )));
    }

    let object_ids = std::iter::once(source.id)
        .chain(related_objects.iter().map(|object| object.id))
        .collect::<Vec<_>>();
    let relations = exporter
        .list_object_relations_between_ids(&object_ids)
        .await?;

    let mut all_objects = BTreeMap::<i32, HubuumObjectWithPath>::new();
    all_objects.insert(source.id, source.clone());
    for object in &related_objects {
        all_objects
            .entry(object.id)
            .or_insert_with(|| object.clone());
    }
    let class_metadata = load_hydration_class_metadata(exporter, &all_objects).await?;

    let neighborhood =
        build_object_neighborhood(source.clone(), related_objects, relations, &class_metadata)?;
    hydrate_object(
        &neighborhood,
        source.id,
        vec![source.id],
        depth_limit,
        hydration_budget,
    )
}

pub(super) struct HydrationClassMetadata {
    pub(super) resolve_class_names: BTreeMap<i32, String>,
    pub(super) class_relations_by_object_class: BTreeMap<i32, Vec<HubuumClassRelation>>,
}

// One-shot replacement for the per-root ensure_class_names + seed_alias DB work.
// Loads every class relation touching any object class once (sorted by id so the
// normalized-pair last-write-wins is deterministic), and primes class names for both
// object classes AND every relation endpoint class (the adjacent class name is needed
// by relation_alias_for_viewer even when no object of that class is in a neighborhood).
pub(super) async fn load_hydration_class_metadata<C, S>(
    exporter: &PermissionAwareExport<'_, C, S>,
    objects_by_id: &BTreeMap<i32, HubuumObjectWithPath>,
) -> Result<HydrationClassMetadata, ApiError>
where
    C: AuthorizationContext,
    S: crate::traits::Search + ?Sized,
{
    let pool = exporter.pool();
    let object_class_ids =
        ClassIdSet::new(objects_by_id.values().map(|object| object.hubuum_class_id))?;

    let mut class_relations = exporter
        .class_relations_touching_class_ids(object_class_ids.as_slice())
        .await?;
    class_relations.sort_by_key(|relation| relation.id);

    let mut class_relations_by_object_class = BTreeMap::<i32, Vec<HubuumClassRelation>>::new();
    let mut name_ids = object_class_ids.as_slice().to_vec();
    for relation in &class_relations {
        name_ids.push(relation.from_hubuum_class_id.id());
        name_ids.push(relation.to_hubuum_class_id.id());
        if object_class_ids
            .as_slice()
            .binary_search(&relation.from_hubuum_class_id.id())
            .is_ok()
        {
            class_relations_by_object_class
                .entry(relation.from_hubuum_class_id.id())
                .or_default()
                .push(relation.clone());
        }
        if relation.to_hubuum_class_id != relation.from_hubuum_class_id
            && object_class_ids
                .as_slice()
                .binary_search(&relation.to_hubuum_class_id.id())
                .is_ok()
        {
            class_relations_by_object_class
                .entry(relation.to_hubuum_class_id.id())
                .or_default()
                .push(relation.clone());
        }
    }

    let mut resolve_class_names = BTreeMap::new();
    ensure_class_name_ids(pool, &name_ids, &mut resolve_class_names).await?;

    Ok(HydrationClassMetadata {
        resolve_class_names,
        class_relations_by_object_class,
    })
}

pub(super) fn build_object_neighborhood(
    root: HubuumObjectWithPath,
    related_objects: Vec<HubuumObjectWithPath>,
    relations: Vec<HubuumObjectRelation>,
    class_metadata: &HydrationClassMetadata,
) -> Result<ObjectNeighborhood, ApiError> {
    let mut objects_by_id = BTreeMap::new();
    objects_by_id.insert(root.id, root);
    for object in related_objects {
        objects_by_id.insert(object.id, object);
    }

    let resolve_class_names = &class_metadata.resolve_class_names;

    let mut aliases_by_object_id = objects_by_id
        .keys()
        .map(|object_id| (*object_id, BTreeMap::<String, Vec<i32>>::new()))
        .collect::<BTreeMap<_, _>>();
    let mut alias_owners = objects_by_id
        .keys()
        .map(|object_id| (*object_id, BTreeMap::<String, i32>::new()))
        .collect::<BTreeMap<_, _>>();
    let mut class_relations_by_pair = BTreeMap::new();

    seed_alias_buckets(
        &objects_by_id,
        &mut aliases_by_object_id,
        &mut alias_owners,
        &mut class_relations_by_pair,
        &class_metadata.class_relations_by_object_class,
        resolve_class_names,
    )?;

    for relation in relations {
        add_bidirectional_alias_edge(
            &objects_by_id,
            &mut aliases_by_object_id,
            &mut alias_owners,
            &class_relations_by_pair,
            resolve_class_names,
            relation.from_hubuum_object_id,
            relation.to_hubuum_object_id,
        )?;
        add_bidirectional_alias_edge(
            &objects_by_id,
            &mut aliases_by_object_id,
            &mut alias_owners,
            &class_relations_by_pair,
            resolve_class_names,
            relation.to_hubuum_object_id,
            relation.from_hubuum_object_id,
        )?;
    }

    for alias_map in aliases_by_object_id.values_mut() {
        for ids in alias_map.values_mut() {
            ids.sort_unstable_by(|left, right| {
                let left_object = &objects_by_id[left];
                let right_object = &objects_by_id[right];
                left_object
                    .name
                    .cmp(&right_object.name)
                    .then_with(|| left.cmp(right))
            });
            ids.dedup();
        }
    }

    Ok(ObjectNeighborhood {
        objects_by_id,
        aliases_by_object_id,
        class_relations_by_pair,
        class_names_by_id: resolve_class_names.clone(),
    })
}

pub(super) fn seed_alias_buckets(
    objects_by_id: &BTreeMap<i32, HubuumObjectWithPath>,
    aliases_by_object_id: &mut BTreeMap<i32, BTreeMap<String, Vec<i32>>>,
    alias_owners: &mut BTreeMap<i32, BTreeMap<String, i32>>,
    class_relations_by_pair: &mut BTreeMap<(i32, i32), crate::models::HubuumClassRelation>,
    class_relations_by_object_class: &BTreeMap<i32, Vec<HubuumClassRelation>>,
    resolve_class_names: &BTreeMap<i32, String>,
) -> Result<(), ApiError> {
    for object in objects_by_id.values() {
        let Some(class_relations) = class_relations_by_object_class.get(&object.hubuum_class_id)
        else {
            continue;
        };
        for relation in class_relations {
            class_relations_by_pair.insert(
                relation_pair_key(
                    relation.from_hubuum_class_id.id(),
                    relation.to_hubuum_class_id.id(),
                ),
                relation.clone(),
            );
            let adjacent_class_id = if relation.from_hubuum_class_id.id() == object.hubuum_class_id
            {
                relation.to_hubuum_class_id
            } else {
                relation.from_hubuum_class_id
            };

            let alias = relation_alias_for_viewer(
                relation,
                object.hubuum_class_id,
                adjacent_class_id.id(),
                resolve_class_names,
            )?;
            let alias_owner_map = alias_owners.get_mut(&object.id).ok_or_else(|| {
                ApiError::InternalServerError("Missing alias ownership state".to_string())
            })?;
            if let Some(existing_class_id) = alias_owner_map.get(&alias)
                && *existing_class_id != adjacent_class_id.id()
            {
                return Err(ApiError::BadRequest(format!(
                    "Relation alias collision for object '{}' on alias '{}'",
                    object.name, alias
                )));
            }
            alias_owner_map.insert(alias.clone(), adjacent_class_id.id());
            aliases_by_object_id
                .get_mut(&object.id)
                .ok_or_else(|| {
                    ApiError::InternalServerError("Missing alias grouping state".to_string())
                })?
                .entry(alias)
                .or_default();
        }
    }

    Ok(())
}

pub(super) async fn ensure_class_name_ids(
    pool: &impl crate::storage::StorageContext,
    class_ids: &[i32],
    resolve_class_names: &mut BTreeMap<i32, String>,
) -> Result<(), ApiError> {
    let missing = ClassIdSet::new(
        class_ids
            .iter()
            .copied()
            .filter(|class_id| !resolve_class_names.contains_key(class_id)),
    )?;

    if missing.is_empty() {
        return Ok(());
    }

    for (class_id, class_name) in storage_handle(pool)
        .class_store()
        .resolve_class_names(
            missing
                .as_slice()
                .iter()
                .copied()
                .map(ClassId::new)
                .collect::<Result<Vec<_>, _>>()?,
        )
        .await
        .map_err(ApiError::from)?
    {
        resolve_class_names.insert(class_id.id(), class_name);
    }

    for class_id in missing.as_slice() {
        if !resolve_class_names.contains_key(class_id) {
            return Err(ApiError::NotFound(format!("Class {class_id} not found")));
        }
    }

    Ok(())
}

pub(super) fn add_bidirectional_alias_edge(
    objects_by_id: &BTreeMap<i32, HubuumObjectWithPath>,
    aliases_by_object_id: &mut BTreeMap<i32, BTreeMap<String, Vec<i32>>>,
    alias_owners: &mut BTreeMap<i32, BTreeMap<String, i32>>,
    class_relations_by_pair: &BTreeMap<(i32, i32), crate::models::HubuumClassRelation>,
    resolve_class_names: &BTreeMap<i32, String>,
    from_object_id: i32,
    to_object_id: i32,
) -> Result<(), ApiError> {
    let Some(from_object) = objects_by_id.get(&from_object_id) else {
        return Ok(());
    };
    let Some(to_object) = objects_by_id.get(&to_object_id) else {
        return Ok(());
    };
    let alias = reachable_alias_for_classes(
        class_relations_by_pair,
        resolve_class_names,
        from_object.hubuum_class_id,
        to_object.hubuum_class_id,
    )?;

    let alias_owner_map = alias_owners.get_mut(&from_object.id).ok_or_else(|| {
        ApiError::InternalServerError("Missing alias ownership state".to_string())
    })?;
    if let Some(existing_class_id) = alias_owner_map.get(&alias)
        && *existing_class_id != to_object.hubuum_class_id
    {
        return Err(ApiError::BadRequest(format!(
            "Relation alias collision for object '{}' on alias '{}'",
            from_object.name, alias
        )));
    }
    alias_owner_map.insert(alias.clone(), to_object.hubuum_class_id);

    aliases_by_object_id
        .get_mut(&from_object.id)
        .ok_or_else(|| ApiError::InternalServerError("Missing alias grouping state".to_string()))?
        .entry(alias)
        .or_default()
        .push(to_object.id);
    Ok(())
}

pub(super) fn inferred_relation_alias(class_name: &str) -> String {
    pluralize_alias(&normalize_alias_segment(class_name))
}

pub(super) fn relation_pair_key(left: i32, right: i32) -> (i32, i32) {
    if left <= right {
        (left, right)
    } else {
        (right, left)
    }
}

pub(super) fn relation_alias_for_viewer(
    relation: &crate::models::HubuumClassRelation,
    viewer_class_id: i32,
    adjacent_class_id: i32,
    resolve_class_names: &BTreeMap<i32, String>,
) -> Result<String, ApiError> {
    if viewer_class_id == relation.from_hubuum_class_id.id()
        && adjacent_class_id == relation.to_hubuum_class_id.id()
        && let Some(alias) = relation
            .forward_template_alias
            .as_ref()
            .map(TemplateAlias::as_str)
    {
        return Ok(alias.to_string());
    }
    if viewer_class_id == relation.to_hubuum_class_id.id()
        && adjacent_class_id == relation.from_hubuum_class_id.id()
        && let Some(alias) = relation
            .reverse_template_alias
            .as_ref()
            .map(TemplateAlias::as_str)
    {
        return Ok(alias.to_string());
    }

    Ok(inferred_relation_alias(
        resolve_class_names.get(&adjacent_class_id).ok_or_else(|| {
            ApiError::InternalServerError(
                "Missing adjacent class name while hydrating relations".to_string(),
            )
        })?,
    ))
}

pub(super) fn reachable_alias_for_classes(
    class_relations_by_pair: &BTreeMap<(i32, i32), crate::models::HubuumClassRelation>,
    resolve_class_names: &BTreeMap<i32, String>,
    source_class_id: i32,
    target_class_id: i32,
) -> Result<String, ApiError> {
    if let Some(relation) =
        class_relations_by_pair.get(&relation_pair_key(source_class_id, target_class_id))
    {
        return relation_alias_for_viewer(
            relation,
            source_class_id,
            target_class_id,
            resolve_class_names,
        );
    }

    Ok(inferred_relation_alias(
        resolve_class_names.get(&target_class_id).ok_or_else(|| {
            ApiError::InternalServerError(
                "Missing class name while hydrating relations".to_string(),
            )
        })?,
    ))
}

pub(super) fn normalize_alias_segment(class_name: &str) -> String {
    let mut normalized = String::new();
    let mut previous_was_separator = true;

    for character in class_name.chars() {
        if character.is_ascii_alphanumeric() {
            if character.is_ascii_uppercase()
                && !previous_was_separator
                && !normalized.ends_with('_')
            {
                normalized.push('_');
            }
            normalized.push(character.to_ascii_lowercase());
            previous_was_separator = false;
        } else if !normalized.ends_with('_') && !normalized.is_empty() {
            normalized.push('_');
            previous_was_separator = true;
        }
    }

    normalized.trim_matches('_').to_string()
}

pub(super) fn pluralize_alias(alias: &str) -> String {
    if alias.ends_with('y')
        && alias.len() > 1
        && !matches!(
            alias.chars().nth(alias.len() - 2),
            Some('a' | 'e' | 'i' | 'o' | 'u')
        )
    {
        format!("{}ies", &alias[..alias.len() - 1])
    } else if alias.ends_with("ch")
        || alias.ends_with("sh")
        || alias.ends_with('s')
        || alias.ends_with('x')
        || alias.ends_with('z')
    {
        format!("{alias}es")
    } else {
        format!("{alias}s")
    }
}

pub(super) fn hydrate_object(
    neighborhood: &ObjectNeighborhood,
    object_id: i32,
    path: Vec<i32>,
    remaining_depth: i32,
    hydration_budget: &mut HydrationBudget,
) -> Result<HydratedTemplateObject, ApiError> {
    hydration_budget.count_object()?;

    let object = neighborhood.objects_by_id.get(&object_id).ok_or_else(|| {
        ApiError::InternalServerError("Missing object while hydrating template".to_string())
    })?;
    let mut related = BTreeMap::new();
    let mut reachable = BTreeMap::new();
    let mut paths = BTreeMap::new();

    if let Some(alias_groups) = neighborhood.aliases_by_object_id.get(&object_id) {
        for (alias, neighbor_ids) in alias_groups {
            let mut hydrated_neighbors = Vec::new();
            if remaining_depth > 0 {
                for neighbor_id in neighbor_ids {
                    if path.contains(neighbor_id) {
                        continue;
                    }
                    let mut next_path = path.clone();
                    next_path.push(*neighbor_id);
                    hydrated_neighbors.push(hydrate_object(
                        neighborhood,
                        *neighbor_id,
                        next_path,
                        remaining_depth - 1,
                        hydration_budget,
                    )?);
                }
            }
            related.insert(alias.clone(), hydrated_neighbors);
        }
    }

    for (alias, targets) in collect_reachable_targets(
        neighborhood,
        object_id,
        &path,
        remaining_depth,
        hydration_budget.remaining(),
    )? {
        let mut hydrated_targets = Vec::with_capacity(targets.len());
        for target in targets {
            hydrated_targets.push(hydrate_object(
                neighborhood,
                target.target_id,
                target.path,
                target.remaining_depth,
                hydration_budget,
            )?);
        }
        reachable.insert(alias, hydrated_targets);
    }

    for (alias, targets) in collect_path_targets(
        neighborhood,
        object_id,
        &path,
        remaining_depth,
        hydration_budget.remaining(),
    )? {
        let mut hydrated_targets = Vec::with_capacity(targets.len());
        for target in targets {
            hydrated_targets.push(hydrate_object(
                neighborhood,
                target.target_id,
                target.path,
                target.remaining_depth,
                hydration_budget,
            )?);
        }
        paths.insert(alias, hydrated_targets);
    }

    let mut relation_labels = BTreeMap::new();
    for relation in neighborhood.class_relations_by_pair.values() {
        let adjacent = if relation.from_hubuum_class_id.id() == object.hubuum_class_id {
            relation.to_hubuum_class_id
        } else if relation.to_hubuum_class_id.id() == object.hubuum_class_id {
            relation.from_hubuum_class_id
        } else {
            continue;
        };
        let alias = relation_alias_for_viewer(
            relation,
            object.hubuum_class_id,
            adjacent.id(),
            &neighborhood.class_names_by_id,
        )?;
        relation_labels.insert(
            alias,
            relation.label_from(object.hubuum_class_id)?.to_owned(),
        );
    }

    Ok(HydratedTemplateObject {
        id: object.id,
        name: object.name.clone(),
        collection_id: object.collection_id,
        hubuum_class_id: object.hubuum_class_id,
        data: object.data.clone(),
        description: object.description.clone(),
        created_at: object.created_at,
        updated_at: object.updated_at,
        path: path.clone(),
        path_objects: build_path_objects(neighborhood, &path)?,
        related,
        relation_labels,
        reachable,
        paths,
    })
}

pub(super) fn collect_reachable_targets(
    neighborhood: &ObjectNeighborhood,
    object_id: i32,
    path: &[i32],
    remaining_depth: i32,
    max_targets: usize,
) -> Result<BTreeMap<String, Vec<ReachableTemplateTarget>>, ApiError> {
    let mut reachable_by_alias = BTreeMap::<String, Vec<ReachableTemplateTarget>>::new();
    if remaining_depth <= 0 {
        return Ok(reachable_by_alias);
    }

    let mut collected_targets = 0_usize;
    let mut queue = VecDeque::from([(object_id, path.to_vec(), 0_i32)]);
    let mut visited_distances = BTreeMap::from([(object_id, 0_i32)]);
    let mut alias_owners = BTreeMap::<String, i32>::new();

    while let Some((current_id, current_path, current_distance)) = queue.pop_front() {
        if current_distance >= remaining_depth {
            continue;
        }

        for neighbor_id in direct_neighbor_ids(neighborhood, current_id)? {
            if current_path.contains(&neighbor_id) {
                continue;
            }

            let next_distance = current_distance + 1;
            if visited_distances.contains_key(&neighbor_id) {
                continue;
            }

            let neighbor = neighborhood
                .objects_by_id
                .get(&neighbor_id)
                .ok_or_else(|| {
                    ApiError::InternalServerError(
                        "Missing reachable object while hydrating template".to_string(),
                    )
                })?;
            let current = neighborhood.objects_by_id.get(&current_id).ok_or_else(|| {
                ApiError::InternalServerError(
                    "Missing current object while hydrating reachable relations".to_string(),
                )
            })?;
            let alias = reachable_alias_for_classes(
                &neighborhood.class_relations_by_pair,
                &neighborhood.class_names_by_id,
                current.hubuum_class_id,
                neighbor.hubuum_class_id,
            )?;
            if let Some(existing_class_id) = alias_owners.get(&alias)
                && *existing_class_id != neighbor.hubuum_class_id
            {
                let source = neighborhood.objects_by_id.get(&object_id).ok_or_else(|| {
                    ApiError::InternalServerError(
                        "Missing source object while hydrating reachable relations".to_string(),
                    )
                })?;
                return Err(ApiError::BadRequest(format!(
                    "Reachable relation alias collision for object '{}' on alias '{}'",
                    source.name, alias
                )));
            }
            alias_owners.insert(alias.clone(), neighbor.hubuum_class_id);

            count_collected_template_target(&mut collected_targets, max_targets, "reachable")?;
            let mut next_path = current_path.clone();
            next_path.push(neighbor_id);
            visited_distances.insert(neighbor_id, next_distance);
            queue.push_back((neighbor_id, next_path.clone(), next_distance));
            reachable_by_alias
                .entry(alias)
                .or_default()
                .push(ReachableTemplateTarget {
                    target_id: neighbor_id,
                    path: next_path,
                    remaining_depth: remaining_depth - next_distance,
                });
        }
    }

    for targets in reachable_by_alias.values_mut() {
        targets.sort_unstable_by(|left, right| {
            let left_object = &neighborhood.objects_by_id[&left.target_id];
            let right_object = &neighborhood.objects_by_id[&right.target_id];
            left_object
                .name
                .cmp(&right_object.name)
                .then_with(|| left.target_id.cmp(&right.target_id))
        });
    }

    Ok(reachable_by_alias)
}

pub(super) fn count_collected_template_target(
    collected_targets: &mut usize,
    max_targets: usize,
    relation_kind: &str,
) -> Result<(), ApiError> {
    if *collected_targets >= max_targets {
        return Err(ApiError::BadRequest(format!(
            "Hydrated template object limit exceeded while collecting {relation_kind} relation targets ({} >= {} remaining capacity)",
            *collected_targets, max_targets
        )));
    }

    *collected_targets = collected_targets.saturating_add(1);
    Ok(())
}

pub(super) fn collect_path_targets(
    neighborhood: &ObjectNeighborhood,
    object_id: i32,
    path: &[i32],
    remaining_depth: i32,
    max_targets: usize,
) -> Result<BTreeMap<String, Vec<ReachableTemplateTarget>>, ApiError> {
    let mut path_targets = BTreeMap::<String, Vec<ReachableTemplateTarget>>::new();
    if remaining_depth <= 0 {
        return Ok(path_targets);
    }

    let mut collected_targets = 0_usize;
    let mut queue = VecDeque::from([(object_id, path.to_vec(), 0_i32)]);
    let mut alias_owners = BTreeMap::<String, i32>::new();

    while let Some((current_id, current_path, current_distance)) = queue.pop_front() {
        if current_distance >= remaining_depth {
            continue;
        }

        for neighbor_id in direct_neighbor_ids(neighborhood, current_id)? {
            if current_path.contains(&neighbor_id) {
                continue;
            }

            let next_distance = current_distance + 1;
            let neighbor = neighborhood
                .objects_by_id
                .get(&neighbor_id)
                .ok_or_else(|| {
                    ApiError::InternalServerError(
                        "Missing path object while hydrating template".to_string(),
                    )
                })?;
            let current = neighborhood.objects_by_id.get(&current_id).ok_or_else(|| {
                ApiError::InternalServerError(
                    "Missing current object while hydrating path relations".to_string(),
                )
            })?;
            let alias = reachable_alias_for_classes(
                &neighborhood.class_relations_by_pair,
                &neighborhood.class_names_by_id,
                current.hubuum_class_id,
                neighbor.hubuum_class_id,
            )?;
            if let Some(existing_class_id) = alias_owners.get(&alias)
                && *existing_class_id != neighbor.hubuum_class_id
            {
                let source = neighborhood.objects_by_id.get(&object_id).ok_or_else(|| {
                    ApiError::InternalServerError(
                        "Missing source object while hydrating path relations".to_string(),
                    )
                })?;
                return Err(ApiError::BadRequest(format!(
                    "Path relation alias collision for object '{}' on alias '{}'",
                    source.name, alias
                )));
            }
            alias_owners.insert(alias.clone(), neighbor.hubuum_class_id);

            count_collected_template_target(&mut collected_targets, max_targets, "path")?;
            let mut next_path = current_path.clone();
            next_path.push(neighbor_id);
            queue.push_back((neighbor_id, next_path.clone(), next_distance));
            path_targets
                .entry(alias)
                .or_default()
                .push(ReachableTemplateTarget {
                    target_id: neighbor_id,
                    path: next_path,
                    remaining_depth: remaining_depth - next_distance,
                });
        }
    }

    for targets in path_targets.values_mut() {
        targets.sort_unstable_by(|left, right| {
            left.path.len().cmp(&right.path.len()).then_with(|| {
                let left_object = &neighborhood.objects_by_id[&left.target_id];
                let right_object = &neighborhood.objects_by_id[&right.target_id];
                left_object
                    .name
                    .cmp(&right_object.name)
                    .then_with(|| left.target_id.cmp(&right.target_id))
            })
        });
    }

    Ok(path_targets)
}

pub(super) fn max_hydrated_template_objects() -> usize {
    get_config()
        .map(|config| config.export_template_max_objects)
        .unwrap_or(DEFAULT_EXPORT_TEMPLATE_MAX_OBJECTS)
}

pub(super) fn configured_export_max_output_bytes() -> usize {
    get_config()
        .map(|config| config.export_max_output_bytes)
        .unwrap_or(DEFAULT_EXPORT_MAX_OUTPUT_BYTES)
}

pub(super) fn build_path_objects(
    neighborhood: &ObjectNeighborhood,
    path: &[i32],
) -> Result<Vec<HydratedTemplatePathObject>, ApiError> {
    path.iter()
        .map(|object_id| {
            let object = neighborhood.objects_by_id.get(object_id).ok_or_else(|| {
                ApiError::InternalServerError(
                    "Missing object while building hydrated template path".to_string(),
                )
            })?;
            Ok(HydratedTemplatePathObject {
                id: object.id,
                name: object.name.clone(),
                collection_id: object.collection_id,
                hubuum_class_id: object.hubuum_class_id,
            })
        })
        .collect()
}

pub(super) fn direct_neighbor_ids(
    neighborhood: &ObjectNeighborhood,
    object_id: i32,
) -> Result<Vec<i32>, ApiError> {
    let mut neighbor_ids = neighborhood
        .aliases_by_object_id
        .get(&object_id)
        .ok_or_else(|| ApiError::InternalServerError("Missing alias grouping state".to_string()))?
        .values()
        .flatten()
        .copied()
        .collect::<Vec<_>>();

    neighbor_ids.sort_unstable_by(|left, right| {
        let left_object = &neighborhood.objects_by_id[left];
        let right_object = &neighborhood.objects_by_id[right];
        left_object
            .name
            .cmp(&right_object.name)
            .then_with(|| left.cmp(right))
    });
    neighbor_ids.dedup();

    Ok(neighbor_ids)
}

pub(super) fn object_with_root_path(object: &HubuumObject) -> HubuumObjectWithPath {
    HubuumObjectWithPath {
        id: object.id,
        name: object.name.clone(),
        collection_id: object.collection_id,
        hubuum_class_id: object.hubuum_class_id,
        data: object.data.clone(),
        description: object.description.clone(),
        created_at: object.created_at,
        updated_at: object.updated_at,
        revision: object.revision,
        path: vec![object.id],
    }
}
