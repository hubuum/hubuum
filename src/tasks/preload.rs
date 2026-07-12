use std::collections::{HashMap, HashSet};

use super::helpers::{class_to_resolution, collection_to_resolution, object_to_resolution};
use super::resolution::{
    remember_class, remember_collection, remember_object, resolve_class_planning,
    resolve_collection_planning,
};
use super::types::{CollectionResolution, PlanningState};
use crate::db::DbPool;
use crate::db::traits::task_import::{
    lookup_classes_by_collection_and_names, lookup_collections_by_name,
    lookup_objects_by_class_and_names,
};
use crate::models::{ClassKey, ImportRequest, ObjectKey};

fn collect_request_class_keys(request: &ImportRequest) -> Vec<ClassKey> {
    let mut keys = Vec::new();
    for class in &request.graph.classes {
        if class.collection_ref.is_some() || class.collection_key.is_some() {
            keys.push(ClassKey {
                name: class.name.clone(),
                collection_ref: class.collection_ref.clone(),
                collection_key: class.collection_key.clone(),
            });
        }
    }
    for object in &request.graph.objects {
        if let Some(key) = &object.class_key {
            keys.push(key.clone());
        }
    }
    for relation in &request.graph.class_relations {
        if let Some(key) = &relation.from_class_key {
            keys.push(key.clone());
        }
        if let Some(key) = &relation.to_class_key {
            keys.push(key.clone());
        }
    }
    for relation in &request.graph.object_relations {
        for object_key in [&relation.from_object_key, &relation.to_object_key]
            .into_iter()
            .flatten()
        {
            if let Some(class_key) = &object_key.class_key {
                keys.push(class_key.clone());
            }
        }
    }
    keys
}

fn collect_request_object_keys(request: &ImportRequest) -> Vec<ObjectKey> {
    let mut keys = Vec::new();
    for object in &request.graph.objects {
        if object.class_ref.is_some() || object.class_key.is_some() {
            keys.push(ObjectKey {
                name: object.name.clone(),
                class_ref: object.class_ref.clone(),
                class_key: object.class_key.clone(),
            });
        }
    }
    for relation in &request.graph.object_relations {
        if let Some(key) = &relation.from_object_key {
            keys.push(key.clone());
        }
        if let Some(key) = &relation.to_object_key {
            keys.push(key.clone());
        }
    }
    keys
}

async fn preload_collections_for_class_keys(
    pool: &DbPool,
    state: &mut PlanningState,
    class_keys: &[ClassKey],
) -> Result<(), String> {
    let names = class_keys
        .iter()
        .filter_map(|key| {
            key.collection_key
                .as_ref()
                .and_then(|collection| collection.path.is_none().then_some(collection.name.clone()))
        })
        .filter(|name| {
            !state.collections_by_name.contains_key(name)
                && !state.missing_collection_names.contains(name)
        })
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();

    for name in names {
        let collections = lookup_collections_by_name(pool, &name)
            .await
            .map_err(|err| err.to_string())?;
        if collections.is_empty() {
            state.missing_collection_names.insert(name);
            continue;
        }
        for collection in collections {
            remember_collection(state, None, collection_to_resolution(collection));
        }
    }

    for key in class_keys {
        if key
            .collection_key
            .as_ref()
            .and_then(|collection| collection.path.as_ref())
            .is_some()
        {
            let _ = resolve_class_collection_for_preload(pool, state, key).await?;
        }
    }

    Ok(())
}

pub(super) async fn preload_existing_classes(
    pool: &DbPool,
    state: &mut PlanningState,
    request: &ImportRequest,
) -> Result<(), String> {
    let class_keys = collect_request_class_keys(request);
    preload_collections_for_class_keys(pool, state, &class_keys).await?;

    let mut requested = HashMap::<i32, HashSet<String>>::new();
    for key in class_keys {
        let Some(collection) = resolve_class_collection_for_preload(pool, state, &key).await?
        else {
            continue;
        };
        if !collection.exists_in_db {
            state
                .missing_class_keys
                .insert((collection.id, key.name.clone()));
            continue;
        }
        if state
            .classes_by_key
            .contains_key(&(collection.id, key.name.clone()))
            || state
                .missing_class_keys
                .contains(&(collection.id, key.name.clone()))
        {
            continue;
        }
        requested
            .entry(collection.id)
            .or_default()
            .insert(key.name.clone());
    }

    for (collection_id, names) in requested {
        let names = names.into_iter().collect::<Vec<_>>();
        let classes = lookup_classes_by_collection_and_names(pool, collection_id, &names)
            .await
            .map_err(|err| err.to_string())?;
        let found_names = classes
            .iter()
            .map(|class| class.name.clone())
            .collect::<HashSet<_>>();
        for class in classes {
            remember_class(state, None, class_to_resolution(class));
        }
        for name in names {
            if !found_names.contains(&name) {
                state.missing_class_keys.insert((collection_id, name));
            }
        }
    }

    Ok(())
}

async fn resolve_class_collection_for_preload(
    pool: &DbPool,
    state: &mut PlanningState,
    key: &ClassKey,
) -> Result<Option<CollectionResolution>, String> {
    match (key.collection_ref.as_deref(), key.collection_key.as_ref()) {
        (Some(reference), None) => Ok(state.collections_by_ref.get(reference).cloned()),
        (None, Some(key)) => {
            match resolve_collection_planning(pool, state, None, Some(key)).await {
                Ok(collection) => Ok(Some(collection)),
                Err(message) if message.contains("not found") => Ok(None),
                Err(message) => Err(message),
            }
        }
        _ => Ok(None),
    }
}

pub(super) async fn preload_existing_objects(
    pool: &DbPool,
    state: &mut PlanningState,
    request: &ImportRequest,
) -> Result<(), String> {
    let object_keys = collect_request_object_keys(request);
    let mut requested = HashMap::<i32, HashSet<String>>::new();

    for key in object_keys {
        let class = match resolve_class_planning(
            pool,
            state,
            key.class_ref.as_deref(),
            key.class_key.as_ref(),
        )
        .await
        {
            Ok(class) => class,
            Err(_) => continue,
        };

        if !class.exists_in_db {
            state
                .missing_object_keys
                .insert((class.id, key.name.clone()));
            continue;
        }
        if state
            .objects_by_key
            .contains_key(&(class.id, key.name.clone()))
            || state
                .missing_object_keys
                .contains(&(class.id, key.name.clone()))
        {
            continue;
        }
        requested
            .entry(class.id)
            .or_default()
            .insert(key.name.clone());
    }

    for (class_id, names) in requested {
        let names = names.into_iter().collect::<Vec<_>>();
        let objects = lookup_objects_by_class_and_names(pool, class_id, &names)
            .await
            .map_err(|err| err.to_string())?;
        let found_names = objects
            .iter()
            .map(|object| object.name.clone())
            .collect::<HashSet<_>>();
        for object in objects {
            remember_object(state, None, object_to_resolution(object));
        }
        for name in names {
            if !found_names.contains(&name) {
                state.missing_object_keys.insert((class_id, name));
            }
        }
    }

    Ok(())
}
