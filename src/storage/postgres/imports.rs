use std::collections::HashMap;

use async_trait::async_trait;
use diesel_async::AsyncConnection;
use hubuum_storage_postgres::PostgresRevision;

use crate::errors::ApiError;
use crate::models::{
    ClassKey, Collection, CollectionKey, EventSinkKey, GroupKey, HubuumClass, HubuumObject,
    IdentityScopeKey, ImportAtomicity, ImportClassRelationInput, ImportCollectionInput,
    ImportCollisionPolicy, ImportComputedFieldVisibility, ImportExportTemplateInput, ImportMode,
    ImportObjectRelationInput, ImportPermissionPolicy, ImportPrincipalSubtype,
    NewHubuumClassRelation, ObjectKey, PrincipalKey,
};
use crate::services::import_boundary::{
    collection_key_from_storage, import_mode_from_storage, import_operation_from_storage,
};
use crate::storage::postgres::operations::task_rows::NewImportTaskResultRow as NewImportTaskResultRecord;
use crate::storage::{
    ApplicationImportOperation as StorageImportOperation, ImportStorage, StorageClassRecord,
    StorageCollection, StorageError, StorageImportApply, StorageImportApplyItem,
    StorageImportCollectionKey, StorageImportMode,
    StorageImportOperation as StorageImportBoundaryOperation, StorageImportPlanItem,
    StorageImportPreflight, StorageImportPreflightItem, StorageImportResult, StorageObject,
};

use super::error::map_postgres_error;
use super::operations::resource_rows::{
    class_record_to_storage, collection_to_storage, object_to_storage,
};
use super::operations::task::insert_import_results;
use super::operations::task_import::{
    apply_permissions_db, check_class_relation_import_condition_db,
    check_object_relation_import_condition_db, create_class_db, create_class_relation_db,
    create_collection_db, create_object_db, create_object_relation_db,
    load_export_template_sources_db, lookup_class_by_collection_and_name,
    lookup_class_by_collection_and_name_db, lookup_classes_by_collection_and_names,
    lookup_collection_by_id, lookup_collection_by_key, lookup_collection_by_key_db,
    lookup_collection_child_by_name_db, lookup_collections_by_name, lookup_direct_class_relation,
    lookup_event_sink_id_by_name_db, lookup_group_by_name, lookup_group_by_name_db,
    lookup_identity_scope_id_by_name_db, lookup_object_by_class_and_name,
    lookup_object_by_class_and_name_db, lookup_object_relation, lookup_objects_by_class_and_names,
    lookup_principal_id_by_name_db, lookup_root_collection, lookup_root_collection_db,
    update_class_db, update_class_relation_timestamps_db, update_collection_db, update_object_db,
    update_object_relation_timestamps_db, upsert_computed_field_db, upsert_event_sink_db,
    upsert_event_subscription_db, upsert_export_template_db, upsert_group_db,
    upsert_group_membership_db, upsert_identity_scope_db, upsert_principal_db,
    upsert_remote_target_db,
};
use super::{PostgresConnection, PostgresStorage, with_connection, with_transaction};

#[derive(Default)]
pub(crate) struct RuntimeState {
    pub(crate) identity_scopes_by_ref: HashMap<String, i32>,
    pub(crate) groups_by_ref: HashMap<String, i32>,
    pub(crate) principals_by_ref: HashMap<String, i32>,
    pub(crate) collections_by_ref: HashMap<String, Collection>,
    pub(crate) classes_by_ref: HashMap<String, HubuumClass>,
    pub(crate) objects_by_ref: HashMap<String, HubuumObject>,
    pub(crate) event_sinks_by_ref: HashMap<String, i32>,
    pub(crate) import_export_templates: Vec<ImportExportTemplateInput>,
}

impl RuntimeState {
    fn for_plan(items: &[StorageImportPlanItem]) -> Result<Self, ApiError> {
        let mut import_export_templates = Vec::new();
        for item in items {
            if let StorageImportOperation::UpsertExportTemplate { input, .. } =
                import_operation_from_storage(item.operation().clone())?
            {
                import_export_templates.push(input);
            }
        }
        Ok(Self {
            import_export_templates,
            ..Self::default()
        })
    }
}

fn collection_key_label(key: &CollectionKey) -> String {
    match &key.path {
        Some(path) => format!("/{}", path.join("/")),
        None => key.name.clone(),
    }
}

async fn resolve_collection_runtime(
    conn: &mut PostgresConnection,
    runtime: &RuntimeState,
    reference: Option<&str>,
    key: Option<&CollectionKey>,
) -> Result<Collection, ApiError> {
    match (reference, key) {
        (Some(reference), None) => runtime
            .collections_by_ref
            .get(reference)
            .cloned()
            .ok_or_else(|| ApiError::BadRequest(format!("Unknown collection ref '{reference}'"))),
        (None, Some(key)) => lookup_collection_by_key_db(conn, key)
            .await?
            .ok_or_else(|| {
                ApiError::NotFound(format!(
                    "Collection '{}' not found during execution",
                    collection_key_label(key)
                ))
            }),
        _ => Err(ApiError::BadRequest(
            "Exactly one of collection_ref or collection_key must be provided".to_string(),
        )),
    }
}

async fn resolve_collection_parent_runtime(
    conn: &mut PostgresConnection,
    runtime: &RuntimeState,
    input: &ImportCollectionInput,
) -> Result<Collection, ApiError> {
    match (
        input.parent_collection_ref.as_deref(),
        input.parent_collection_key.as_ref(),
    ) {
        (None, None) => lookup_root_collection_db(conn).await,
        (Some(reference), None) => runtime
            .collections_by_ref
            .get(reference)
            .cloned()
            .ok_or_else(|| ApiError::BadRequest(format!("Unknown collection ref '{reference}'"))),
        (None, Some(key)) => lookup_collection_by_key_db(conn, key)
            .await?
            .ok_or_else(|| {
                ApiError::NotFound(format!(
                    "Collection '{}' not found during execution",
                    collection_key_label(key)
                ))
            }),
        (Some(_), Some(_)) => Err(ApiError::BadRequest(
            "At most one of parent_collection_ref or parent_collection_key may be provided"
                .to_string(),
        )),
    }
}

async fn resolve_class_runtime(
    conn: &mut PostgresConnection,
    runtime: &RuntimeState,
    reference: Option<&str>,
    key: Option<&ClassKey>,
) -> Result<HubuumClass, ApiError> {
    match (reference, key) {
        (Some(reference), None) => runtime
            .classes_by_ref
            .get(reference)
            .cloned()
            .ok_or_else(|| ApiError::BadRequest(format!("Unknown class ref '{reference}'"))),
        (None, Some(key)) => {
            let collection = resolve_collection_runtime(
                conn,
                runtime,
                key.collection_ref.as_deref(),
                key.collection_key.as_ref(),
            )
            .await?;
            lookup_class_by_collection_and_name_db(conn, collection.id, &key.name)
                .await?
                .ok_or_else(|| {
                    ApiError::NotFound(format!(
                        "Class '{}' not found in collection '{}' during execution",
                        key.name, collection.name
                    ))
                })
        }
        _ => Err(ApiError::BadRequest(
            "Exactly one of class_ref or class_key must be provided".to_string(),
        )),
    }
}

pub(crate) async fn resolve_object_runtime(
    conn: &mut PostgresConnection,
    runtime: &RuntimeState,
    reference: Option<&str>,
    key: Option<&ObjectKey>,
) -> Result<HubuumObject, ApiError> {
    match (reference, key) {
        (Some(reference), None) => runtime
            .objects_by_ref
            .get(reference)
            .cloned()
            .ok_or_else(|| ApiError::BadRequest(format!("Unknown object ref '{reference}'"))),
        (None, Some(key)) => {
            let class = resolve_class_runtime(
                conn,
                runtime,
                key.class_ref.as_deref(),
                key.class_key.as_ref(),
            )
            .await?;
            lookup_object_by_class_and_name_db(conn, class.id, &key.name)
                .await?
                .ok_or_else(|| {
                    ApiError::NotFound(format!(
                        "Object '{}' not found in class '{}' during execution",
                        key.name, class.name
                    ))
                })
        }
        _ => Err(ApiError::BadRequest(
            "Exactly one of object_ref or object_key must be provided".to_string(),
        )),
    }
}

async fn resolve_identity_scope_runtime(
    conn: &mut PostgresConnection,
    runtime: &RuntimeState,
    reference: Option<&str>,
    key: Option<&IdentityScopeKey>,
) -> Result<i32, ApiError> {
    if let Some(reference) = reference
        && let Some(found_id) = runtime.identity_scopes_by_ref.get(reference)
    {
        return Ok(*found_id);
    }
    let name = key.map(|key| key.name.as_str()).ok_or_else(|| {
        ApiError::BadRequest(
            "Identity-scope reference was not resolved and no identity_scope_key was supplied"
                .to_string(),
        )
    })?;
    lookup_identity_scope_id_by_name_db(conn, name)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("Identity scope '{name}' not found")))
}

async fn validate_import_template_composition(
    conn: &mut PostgresConnection,
    runtime: &RuntimeState,
    input: &ImportExportTemplateInput,
    collection: &Collection,
) -> Result<(), ApiError> {
    let mut sources = load_export_template_sources_db(conn, collection.id).await?;
    for candidate in &runtime.import_export_templates {
        let candidate_collection = resolve_collection_runtime(
            conn,
            runtime,
            candidate.collection_ref.as_deref(),
            candidate.collection_key.as_ref(),
        )
        .await;
        match candidate_collection {
            Ok(candidate_collection) if candidate_collection.id == collection.id => {
                sources.push((candidate.name.clone(), candidate.template.clone()));
            }
            Ok(_) | Err(ApiError::BadRequest(_) | ApiError::NotFound(_)) => {}
            Err(error) => return Err(error),
        }
    }

    input.validate_composition(&sources)
}

async fn resolve_group_runtime(
    conn: &mut PostgresConnection,
    runtime: &RuntimeState,
    reference: Option<&str>,
    key: Option<&GroupKey>,
) -> Result<i32, ApiError> {
    if let Some(reference) = reference
        && let Some(id) = runtime.groups_by_ref.get(reference)
    {
        return Ok(*id);
    }
    let key = key.ok_or_else(|| {
        ApiError::BadRequest(
            "Group reference was not resolved and no group_key was supplied".to_string(),
        )
    })?;
    let scope = key.identity_scope_name();
    lookup_group_by_name_db(conn, scope, &key.groupname)
        .await?
        .map(|group| group.id)
        .ok_or_else(|| ApiError::NotFound(format!("Group '{scope}/{}' not found", key.groupname)))
}

async fn resolve_principal_runtime(
    conn: &mut PostgresConnection,
    runtime: &RuntimeState,
    reference: Option<&str>,
    key: Option<&PrincipalKey>,
) -> Result<i32, ApiError> {
    if let Some(reference) = reference
        && let Some(id) = runtime.principals_by_ref.get(reference)
    {
        return Ok(*id);
    }
    let key = key.ok_or_else(|| {
        ApiError::BadRequest(
            "Principal reference was not resolved and no principal_key was supplied".to_string(),
        )
    })?;
    let scope = key.identity_scope_name();
    lookup_principal_id_by_name_db(conn, scope, &key.name)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("Principal '{scope}/{}' not found", key.name)))
}

async fn resolve_event_sink_runtime(
    conn: &mut PostgresConnection,
    runtime: &RuntimeState,
    reference: Option<&str>,
    key: Option<&EventSinkKey>,
) -> Result<i32, ApiError> {
    if let Some(reference) = reference
        && let Some(found_id) = runtime.event_sinks_by_ref.get(reference)
    {
        return Ok(*found_id);
    }
    let name = key.map(|key| key.name.as_str()).ok_or_else(|| {
        ApiError::BadRequest(
            "Event-sink reference was not resolved and no sink_key was supplied".to_string(),
        )
    })?;
    lookup_event_sink_id_by_name_db(conn, name)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("Event sink '{name}' not found")))
}

async fn resolve_class_relation_runtime(
    conn: &mut PostgresConnection,
    runtime: &RuntimeState,
    input: &ImportClassRelationInput,
) -> Result<(HubuumClass, HubuumClass), ApiError> {
    let from_class = resolve_class_runtime(
        conn,
        runtime,
        input.from_class_ref.as_deref(),
        input.from_class_key.as_ref(),
    )
    .await?;
    let to_class = resolve_class_runtime(
        conn,
        runtime,
        input.to_class_ref.as_deref(),
        input.to_class_key.as_ref(),
    )
    .await?;
    Ok((from_class, to_class))
}

async fn resolve_object_relation_runtime(
    conn: &mut PostgresConnection,
    runtime: &RuntimeState,
    input: &ImportObjectRelationInput,
) -> Result<(HubuumObject, HubuumObject), ApiError> {
    let from_object = resolve_object_runtime(
        conn,
        runtime,
        input.from_object_ref.as_deref(),
        input.from_object_key.as_ref(),
    )
    .await?;
    let to_object = resolve_object_runtime(
        conn,
        runtime,
        input.to_object_ref.as_deref(),
        input.to_object_key.as_ref(),
    )
    .await?;
    Ok((from_object, to_object))
}

async fn observed_revision_for_planned_item(
    conn: &mut PostgresConnection,
    runtime: &RuntimeState,
    execution: &StorageImportBoundaryOperation,
) -> Result<Option<PostgresRevision>, ApiError> {
    use crate::models::ImportComputedFieldVisibility;
    use crate::storage::postgres::prelude::*;

    let execution = import_operation_from_storage(execution.clone())?;
    let revision = match &execution {
        StorageImportOperation::UpsertIdentityScope { input, .. } => {
            use crate::schema::identity_scopes::dsl as s;
            s::identity_scopes
                .filter(s::name.eq(&input.name))
                .select(s::revision)
                .first(conn)
                .await
                .optional()?
        }
        StorageImportOperation::UpsertGroup { input, .. } => {
            use crate::schema::groups::dsl as g;
            let scope_id = resolve_identity_scope_runtime(
                conn,
                runtime,
                input.identity_scope_ref.as_deref(),
                input.identity_scope_key.as_ref(),
            )
            .await?;
            g::groups
                .filter(g::identity_scope_id.eq(scope_id))
                .filter(g::groupname.eq(&input.groupname))
                .select(g::revision)
                .first(conn)
                .await
                .optional()?
        }
        StorageImportOperation::UpsertPrincipal { input, .. } => {
            use crate::schema::principals::dsl as p;
            let scope_id = resolve_identity_scope_runtime(
                conn,
                runtime,
                input.identity_scope_ref.as_deref(),
                input.identity_scope_key.as_ref(),
            )
            .await?;
            p::principals
                .filter(p::identity_scope_id.eq(scope_id))
                .filter(p::name.eq(&input.name))
                .select(p::revision)
                .first(conn)
                .await
                .optional()?
        }
        StorageImportOperation::UpsertGroupMembership { input, .. } => {
            use crate::schema::group_memberships::dsl as m;
            let principal_id = resolve_principal_runtime(
                conn,
                runtime,
                input.principal_ref.as_deref(),
                input.principal_key.as_ref(),
            )
            .await?;
            let group_id = resolve_group_runtime(
                conn,
                runtime,
                input.group_ref.as_deref(),
                input.group_key.as_ref(),
            )
            .await?;
            m::group_memberships
                .filter(m::principal_id.eq(principal_id))
                .filter(m::group_id.eq(group_id))
                .select(m::revision)
                .first(conn)
                .await
                .optional()?
        }
        StorageImportOperation::UpdateCollection { collection_id, .. } => {
            use crate::schema::collections::dsl as c;
            c::collections
                .filter(c::id.eq(collection_id))
                .select(c::revision)
                .first(conn)
                .await
                .optional()?
        }
        StorageImportOperation::UpdateClass { class_id, .. } => {
            use crate::schema::hubuumclass::dsl as c;
            c::hubuumclass
                .filter(c::id.eq(class_id))
                .select(c::revision)
                .first(conn)
                .await
                .optional()?
        }
        StorageImportOperation::UpdateObject { object_id, .. } => {
            use crate::schema::hubuumobject::dsl as o;
            o::hubuumobject
                .filter(o::id.eq(object_id))
                .select(o::revision)
                .first(conn)
                .await
                .optional()?
        }
        StorageImportOperation::UpsertComputedField { input, .. } => {
            use crate::schema::computed_field_definitions::dsl as d;
            let class = resolve_class_runtime(
                conn,
                runtime,
                input.class_ref.as_deref(),
                input.class_key.as_ref(),
            )
            .await?;
            let owner_id = match input.visibility {
                ImportComputedFieldVisibility::Shared => None,
                ImportComputedFieldVisibility::Personal => Some(
                    resolve_principal_runtime(
                        conn,
                        runtime,
                        input.owner_ref.as_deref(),
                        input.owner_key.as_ref(),
                    )
                    .await?,
                ),
            };
            let visibility = match input.visibility {
                ImportComputedFieldVisibility::Shared => "shared",
                ImportComputedFieldVisibility::Personal => "personal",
            };
            d::computed_field_definitions
                .filter(d::class_id.eq(class.id))
                .filter(d::visibility.eq(visibility))
                .filter(d::key.eq(&input.key))
                .filter(d::owner_user_id.is_not_distinct_from(owner_id))
                .select(d::revision)
                .first(conn)
                .await
                .optional()?
        }
        StorageImportOperation::UpdateClassRelationTimestamps { input, .. }
        | StorageImportOperation::CheckClassRelationCondition(input) => {
            use crate::schema::hubuumclass_relation::dsl as r;
            let (from, to) = resolve_class_relation_runtime(conn, runtime, input).await?;
            r::hubuumclass_relation
                .filter(
                    r::from_hubuum_class_id
                        .eq(from.id)
                        .and(r::to_hubuum_class_id.eq(to.id))
                        .or(r::from_hubuum_class_id
                            .eq(to.id)
                            .and(r::to_hubuum_class_id.eq(from.id))),
                )
                .select(r::revision)
                .first(conn)
                .await
                .optional()?
        }
        StorageImportOperation::UpdateObjectRelationTimestamps { input, .. }
        | StorageImportOperation::CheckObjectRelationCondition(input) => {
            use crate::schema::hubuumobject_relation::dsl as r;
            let (from, to) = resolve_object_relation_runtime(conn, runtime, input).await?;
            r::hubuumobject_relation
                .filter(
                    r::from_hubuum_object_id
                        .eq(from.id)
                        .and(r::to_hubuum_object_id.eq(to.id))
                        .or(r::from_hubuum_object_id
                            .eq(to.id)
                            .and(r::to_hubuum_object_id.eq(from.id))),
                )
                .select(r::revision)
                .first(conn)
                .await
                .optional()?
        }
        StorageImportOperation::ApplyCollectionPermissions { input, .. } => {
            use crate::schema::collection_authorization_state::dsl as a;
            let collection = resolve_collection_runtime(
                conn,
                runtime,
                input.collection_ref.as_deref(),
                input.collection_key.as_ref(),
            )
            .await?;
            a::collection_authorization_state
                .filter(a::collection_id.eq(collection.id))
                .select(a::revision)
                .first(conn)
                .await
                .optional()?
        }
        StorageImportOperation::UpsertExportTemplate { input, .. } => {
            use crate::schema::export_templates::dsl as t;
            let collection = resolve_collection_runtime(
                conn,
                runtime,
                input.collection_ref.as_deref(),
                input.collection_key.as_ref(),
            )
            .await?;
            t::export_templates
                .filter(t::collection_id.eq(collection.id))
                .filter(t::name.eq(&input.name))
                .select(t::revision)
                .first(conn)
                .await
                .optional()?
        }
        StorageImportOperation::UpsertRemoteTarget { input, .. } => {
            use crate::schema::remote_targets::dsl as r;
            let collection = resolve_collection_runtime(
                conn,
                runtime,
                input.collection_ref.as_deref(),
                input.collection_key.as_ref(),
            )
            .await?;
            r::remote_targets
                .filter(r::collection_id.eq(collection.id))
                .filter(r::name.eq(&input.name))
                .select(r::revision)
                .first(conn)
                .await
                .optional()?
        }
        StorageImportOperation::UpsertEventSink { input, .. } => {
            use crate::schema::event_sinks::dsl as s;
            s::event_sinks
                .filter(s::name.eq(&input.name))
                .select(s::revision)
                .first(conn)
                .await
                .optional()?
        }
        StorageImportOperation::UpsertEventSubscription { input, .. } => {
            use crate::schema::event_subscriptions::dsl as s;
            let collection = resolve_collection_runtime(
                conn,
                runtime,
                input.collection_ref.as_deref(),
                input.collection_key.as_ref(),
            )
            .await?;
            s::event_subscriptions
                .filter(s::collection_id.eq(collection.id))
                .filter(s::name.eq(&input.name))
                .select(s::revision)
                .first(conn)
                .await
                .optional()?
        }
        StorageImportOperation::CreateCollection(_)
        | StorageImportOperation::CreateClass(_)
        | StorageImportOperation::CreateObject(_)
        | StorageImportOperation::CreateClassRelation(_)
        | StorageImportOperation::CreateObjectRelation(_) => None,
    };
    Ok(revision)
}

pub(crate) async fn execute_planned_item(
    conn: &mut PostgresConnection,
    runtime: &mut RuntimeState,
    execution: &StorageImportBoundaryOperation,
) -> Result<(), ApiError> {
    let execution = import_operation_from_storage(execution.clone())?;
    execute_application_planned_item_inner(conn, runtime, &execution).await
}

#[cfg(test)]
pub(crate) async fn execute_application_planned_item(
    conn: &mut PostgresConnection,
    runtime: &mut RuntimeState,
    execution: &StorageImportOperation,
) -> Result<(), ApiError> {
    execute_application_planned_item_inner(conn, runtime, execution).await
}

async fn execute_application_planned_item_inner(
    conn: &mut PostgresConnection,
    runtime: &mut RuntimeState,
    execution: &StorageImportOperation,
) -> Result<(), ApiError> {
    match execution {
        StorageImportOperation::UpsertIdentityScope { input, overwrite } => {
            let id = upsert_identity_scope_db(conn, input, *overwrite).await?;
            if let Some(reference) = &input.ref_ {
                runtime.identity_scopes_by_ref.insert(reference.clone(), id);
            }
        }
        StorageImportOperation::UpsertGroup { input, overwrite } => {
            let scope_id = resolve_identity_scope_runtime(
                conn,
                runtime,
                input.identity_scope_ref.as_deref(),
                input.identity_scope_key.as_ref(),
            )
            .await?;
            let id = upsert_group_db(conn, input, scope_id, *overwrite).await?;
            if let Some(reference) = &input.ref_ {
                runtime.groups_by_ref.insert(reference.clone(), id);
            }
        }
        StorageImportOperation::UpsertPrincipal { input, overwrite } => {
            let scope_id = resolve_identity_scope_runtime(
                conn,
                runtime,
                input.identity_scope_ref.as_deref(),
                input.identity_scope_key.as_ref(),
            )
            .await?;
            let (owner_group_id, created_by) = match &input.subtype {
                ImportPrincipalSubtype::Human { .. } => (None, None),
                ImportPrincipalSubtype::ServiceAccount {
                    owner_group_ref,
                    owner_group_key,
                    created_by_ref,
                    created_by_key,
                    ..
                } => (
                    Some(
                        resolve_group_runtime(
                            conn,
                            runtime,
                            owner_group_ref.as_deref(),
                            owner_group_key.as_ref(),
                        )
                        .await?,
                    ),
                    if created_by_ref.is_some() || created_by_key.is_some() {
                        Some(
                            resolve_principal_runtime(
                                conn,
                                runtime,
                                created_by_ref.as_deref(),
                                created_by_key.as_ref(),
                            )
                            .await?,
                        )
                    } else {
                        None
                    },
                ),
            };
            let id = upsert_principal_db(
                conn,
                input,
                scope_id,
                owner_group_id,
                created_by,
                *overwrite,
            )
            .await?;
            if let Some(reference) = &input.ref_ {
                runtime.principals_by_ref.insert(reference.clone(), id);
            }
        }
        StorageImportOperation::UpsertGroupMembership { input, overwrite } => {
            let principal_id = resolve_principal_runtime(
                conn,
                runtime,
                input.principal_ref.as_deref(),
                input.principal_key.as_ref(),
            )
            .await?;
            let group_id = resolve_group_runtime(
                conn,
                runtime,
                input.group_ref.as_deref(),
                input.group_key.as_ref(),
            )
            .await?;
            let mut source_scope_ids = Vec::with_capacity(input.sources.len());
            for source in &input.sources {
                source_scope_ids.push(
                    resolve_identity_scope_runtime(
                        conn,
                        runtime,
                        source.source_scope_ref.as_deref(),
                        source.source_scope_key.as_ref(),
                    )
                    .await?,
                );
            }
            upsert_group_membership_db(
                conn,
                input,
                principal_id,
                group_id,
                &source_scope_ids,
                *overwrite,
            )
            .await?;
        }
        StorageImportOperation::CreateCollection(input) => {
            let parent = resolve_collection_parent_runtime(conn, runtime, input).await?;
            let created = create_collection_db(conn, input, Some(parent.id)).await?;
            if let Some(reference) = &input.ref_ {
                runtime
                    .collections_by_ref
                    .insert(reference.clone(), created);
            }
        }
        StorageImportOperation::UpdateCollection {
            collection_id,
            input,
        } => {
            let updated = update_collection_db(conn, *collection_id, input).await?;
            if let Some(reference) = &input.ref_ {
                runtime
                    .collections_by_ref
                    .insert(reference.clone(), updated);
            }
        }
        StorageImportOperation::CreateClass(input) => {
            let collection = resolve_collection_runtime(
                conn,
                runtime,
                input.collection_ref.as_deref(),
                input.collection_key.as_ref(),
            )
            .await?;
            let created = create_class_db(conn, input, collection.id).await?;
            if let Some(reference) = &input.ref_ {
                runtime.classes_by_ref.insert(reference.clone(), created);
            }
        }
        StorageImportOperation::UpdateClass { class_id, input } => {
            let updated = update_class_db(conn, *class_id, input).await?;
            if let Some(reference) = &input.ref_ {
                runtime.classes_by_ref.insert(reference.clone(), updated);
            }
        }
        StorageImportOperation::CreateObject(input) => {
            let class = resolve_class_runtime(
                conn,
                runtime,
                input.class_ref.as_deref(),
                input.class_key.as_ref(),
            )
            .await?;
            let created = create_object_db(conn, input, &class).await?;
            if let Some(reference) = &input.ref_ {
                runtime.objects_by_ref.insert(reference.clone(), created);
            }
        }
        StorageImportOperation::UpdateObject { object_id, input } => {
            let updated = update_object_db(conn, *object_id, input).await?;
            if let Some(reference) = &input.ref_ {
                runtime.objects_by_ref.insert(reference.clone(), updated);
            }
        }
        StorageImportOperation::UpsertComputedField { input, overwrite } => {
            let class = resolve_class_runtime(
                conn,
                runtime,
                input.class_ref.as_deref(),
                input.class_key.as_ref(),
            )
            .await?;
            let owner_id = match input.visibility {
                ImportComputedFieldVisibility::Shared => None,
                ImportComputedFieldVisibility::Personal => Some(
                    resolve_principal_runtime(
                        conn,
                        runtime,
                        input.owner_ref.as_deref(),
                        input.owner_key.as_ref(),
                    )
                    .await?,
                ),
            };
            upsert_computed_field_db(conn, input, class.id, owner_id, *overwrite).await?;
        }
        StorageImportOperation::CreateClassRelation(input) => {
            let (from_class, to_class) =
                resolve_class_relation_runtime(conn, runtime, input).await?;
            create_class_relation_db(
                conn,
                NewHubuumClassRelation {
                    from_hubuum_class_id: from_class.id,
                    to_hubuum_class_id: to_class.id,
                    forward_template_alias: input.forward_template_alias.clone(),
                    reverse_template_alias: input.reverse_template_alias.clone(),
                    from_max_relations: input.from_max_relations,
                    to_max_relations: input.to_max_relations,
                },
                input.timestamps.as_ref(),
                input.condition,
            )
            .await?;
        }
        StorageImportOperation::UpdateClassRelationTimestamps { input, timestamps } => {
            let (from_class, to_class) =
                resolve_class_relation_runtime(conn, runtime, input).await?;
            update_class_relation_timestamps_db(
                conn,
                from_class.id,
                to_class.id,
                timestamps,
                input.condition,
            )
            .await?;
        }
        StorageImportOperation::CheckClassRelationCondition(input) => {
            let (from_class, to_class) =
                resolve_class_relation_runtime(conn, runtime, input).await?;
            check_class_relation_import_condition_db(
                conn,
                from_class.id,
                to_class.id,
                input.condition,
            )
            .await?;
        }
        StorageImportOperation::CreateObjectRelation(input) => {
            let (from_object, to_object) =
                resolve_object_relation_runtime(conn, runtime, input).await?;
            create_object_relation_db(
                conn,
                &from_object,
                &to_object,
                input.timestamps.as_ref(),
                input.condition,
            )
            .await?;
        }
        StorageImportOperation::UpdateObjectRelationTimestamps { input, timestamps } => {
            let (from_object, to_object) =
                resolve_object_relation_runtime(conn, runtime, input).await?;
            update_object_relation_timestamps_db(
                conn,
                &from_object,
                &to_object,
                timestamps,
                input.condition,
            )
            .await?;
        }
        StorageImportOperation::CheckObjectRelationCondition(input) => {
            let (from_object, to_object) =
                resolve_object_relation_runtime(conn, runtime, input).await?;
            check_object_relation_import_condition_db(
                conn,
                &from_object,
                &to_object,
                input.condition,
            )
            .await?;
        }
        StorageImportOperation::ApplyCollectionPermissions { input, overwrite } => {
            let collection = resolve_collection_runtime(
                conn,
                runtime,
                input.collection_ref.as_deref(),
                input.collection_key.as_ref(),
            )
            .await?;
            let identity_scope = input.group_key.identity_scope_name();
            let group = lookup_group_by_name_db(conn, identity_scope, &input.group_key.groupname)
                .await?
                .ok_or_else(|| {
                    ApiError::NotFound(format!(
                        "Group '{}/{}' not found",
                        identity_scope, input.group_key.groupname
                    ))
                })?;
            apply_permissions_db(
                conn,
                collection.id,
                group.id,
                &input.permissions,
                input.replace_existing.unwrap_or(false),
                input.condition,
                *overwrite,
            )
            .await?;
        }
        StorageImportOperation::UpsertExportTemplate { input, overwrite } => {
            let collection = resolve_collection_runtime(
                conn,
                runtime,
                input.collection_ref.as_deref(),
                input.collection_key.as_ref(),
            )
            .await?;
            let class_id = if input.class_ref.is_some() || input.class_key.is_some() {
                let class = resolve_class_runtime(
                    conn,
                    runtime,
                    input.class_ref.as_deref(),
                    input.class_key.as_ref(),
                )
                .await?;
                class.ensure_in_collection(collection.id, "Export template")?;
                Some(class.id)
            } else {
                None
            };
            validate_import_template_composition(conn, runtime, input, &collection).await?;
            upsert_export_template_db(conn, input, collection.id, class_id, *overwrite).await?;
        }
        StorageImportOperation::UpsertRemoteTarget { input, overwrite } => {
            let collection = resolve_collection_runtime(
                conn,
                runtime,
                input.collection_ref.as_deref(),
                input.collection_key.as_ref(),
            )
            .await?;
            let class_id = if input.class_ref.is_some() || input.class_key.is_some() {
                let class = resolve_class_runtime(
                    conn,
                    runtime,
                    input.class_ref.as_deref(),
                    input.class_key.as_ref(),
                )
                .await?;
                class.ensure_in_collection(collection.id, "Remote target")?;
                Some(class.id)
            } else {
                None
            };
            upsert_remote_target_db(conn, input, collection.id, class_id, *overwrite).await?;
        }
        StorageImportOperation::UpsertEventSink { input, overwrite } => {
            let id = upsert_event_sink_db(conn, input, *overwrite).await?;
            if let Some(reference) = &input.ref_ {
                runtime.event_sinks_by_ref.insert(reference.clone(), id);
            }
        }
        StorageImportOperation::UpsertEventSubscription { input, overwrite } => {
            let collection = resolve_collection_runtime(
                conn,
                runtime,
                input.collection_ref.as_deref(),
                input.collection_key.as_ref(),
            )
            .await?;
            let sink_id = resolve_event_sink_runtime(
                conn,
                runtime,
                input.sink_ref.as_deref(),
                input.sink_key.as_ref(),
            )
            .await?;
            upsert_event_subscription_db(conn, input, collection.id, sink_id, *overwrite).await?;
        }
    }

    Ok(())
}

const DRY_RUN_ROLLBACK: &str = "hubuum import dry-run rollback";

fn should_abort_preflight(error: &ApiError, mode: &ImportMode) -> bool {
    if matches!(
        mode.atomicity.unwrap_or(ImportAtomicity::Strict),
        ImportAtomicity::Strict
    ) {
        return true;
    }

    match error {
        ApiError::Forbidden(_) | ApiError::Unauthorized(_) => matches!(
            mode.permission_policy
                .unwrap_or(ImportPermissionPolicy::Abort),
            ImportPermissionPolicy::Abort
        ),
        ApiError::Conflict(_) | ApiError::PreconditionFailed(_, _) => matches!(
            mode.collision_policy
                .unwrap_or(ImportCollisionPolicy::Abort),
            ImportCollisionPolicy::Abort
        ),
        _ => false,
    }
}

fn should_abort_best_effort(error: &ApiError, mode: &ImportMode) -> bool {
    match error {
        ApiError::Forbidden(_) | ApiError::Unauthorized(_) => matches!(
            mode.permission_policy
                .unwrap_or(ImportPermissionPolicy::Abort),
            ImportPermissionPolicy::Abort
        ),
        ApiError::Conflict(_) => matches!(
            mode.collision_policy
                .unwrap_or(ImportCollisionPolicy::Abort),
            ImportCollisionPolicy::Abort
        ),
        _ => false,
    }
}

#[async_trait]
impl ImportStorage for PostgresStorage {
    async fn import_root_collection(&self) -> Result<StorageCollection, StorageError> {
        lookup_root_collection(self.pool())
            .await
            .map(collection_to_storage)
            .map_err(map_postgres_error)
    }

    async fn import_collection_by_id(
        &self,
        collection_id: i32,
    ) -> Result<Option<StorageCollection>, StorageError> {
        lookup_collection_by_id(self.pool(), collection_id)
            .await
            .map(|collection| collection.map(collection_to_storage))
            .map_err(map_postgres_error)
    }

    async fn import_collection_by_key(
        &self,
        key: &StorageImportCollectionKey,
    ) -> Result<Option<StorageCollection>, StorageError> {
        let key = collection_key_from_storage(key.clone());
        lookup_collection_by_key(self.pool(), &key)
            .await
            .map(|collection| collection.map(collection_to_storage))
            .map_err(map_postgres_error)
    }

    async fn import_collections_by_name(
        &self,
        name: &str,
    ) -> Result<Vec<StorageCollection>, StorageError> {
        lookup_collections_by_name(self.pool(), name)
            .await
            .map(|collections| collections.into_iter().map(collection_to_storage).collect())
            .map_err(map_postgres_error)
    }

    async fn import_collection_child_by_name(
        &self,
        parent_collection_id: i32,
        name: &str,
    ) -> Result<Option<StorageCollection>, StorageError> {
        with_connection(self.pool(), async |conn| {
            lookup_collection_child_by_name_db(conn, parent_collection_id, name).await
        })
        .await
        .map(|collection| collection.map(collection_to_storage))
        .map_err(map_postgres_error)
    }

    async fn import_class_by_name(
        &self,
        collection_id: i32,
        name: &str,
    ) -> Result<Option<StorageClassRecord>, StorageError> {
        lookup_class_by_collection_and_name(self.pool(), collection_id, name)
            .await
            .map(|class| class.map(class_record_to_storage))
            .map_err(map_postgres_error)
    }

    async fn import_classes_by_names(
        &self,
        collection_id: i32,
        names: &[String],
    ) -> Result<Vec<StorageClassRecord>, StorageError> {
        lookup_classes_by_collection_and_names(self.pool(), collection_id, names)
            .await
            .map(|classes| classes.into_iter().map(class_record_to_storage).collect())
            .map_err(map_postgres_error)
    }

    async fn import_object_by_name(
        &self,
        class_id: i32,
        name: &str,
    ) -> Result<Option<StorageObject>, StorageError> {
        lookup_object_by_class_and_name(self.pool(), class_id, name)
            .await
            .map(|object| object.map(object_to_storage))
            .map_err(map_postgres_error)
    }

    async fn import_objects_by_names(
        &self,
        class_id: i32,
        names: &[String],
    ) -> Result<Vec<StorageObject>, StorageError> {
        lookup_objects_by_class_and_names(self.pool(), class_id, names)
            .await
            .map(|objects| objects.into_iter().map(object_to_storage).collect())
            .map_err(map_postgres_error)
    }

    async fn import_class_relation_exists(
        &self,
        left_class_id: i32,
        right_class_id: i32,
    ) -> Result<bool, StorageError> {
        lookup_direct_class_relation(self.pool(), left_class_id, right_class_id)
            .await
            .map(|relation| relation.is_some())
            .map_err(map_postgres_error)
    }

    async fn import_object_relation_exists(
        &self,
        left_object_id: i32,
        right_object_id: i32,
    ) -> Result<bool, StorageError> {
        lookup_object_relation(self.pool(), left_object_id, right_object_id)
            .await
            .map(|relation| relation.is_some())
            .map_err(map_postgres_error)
    }

    async fn import_group_exists(
        &self,
        identity_scope: &str,
        group_name: &str,
    ) -> Result<bool, StorageError> {
        lookup_group_by_name(self.pool(), identity_scope, group_name)
            .await
            .map(|group| group.is_some())
            .map_err(map_postgres_error)
    }

    async fn preflight_import(
        &self,
        items: Vec<StorageImportPlanItem>,
        mode: StorageImportMode,
    ) -> Result<StorageImportPreflight, StorageError> {
        with_connection(self.pool(), async move |conn| {
            let mode = import_mode_from_storage(mode);
            let mut outcomes = Vec::with_capacity(items.len());
            let mut aborted = false;
            let mut runtime = RuntimeState::for_plan(&items)?;
            let transaction = conn
                .transaction::<(), ApiError, _>(async |conn| {
                    for item in items {
                        let index = item.index();
                        let observed_revision =
                            observed_revision_for_planned_item(conn, &runtime, item.operation())
                                .await;
                        let (revision, result) = match observed_revision {
                            Ok(revision) => {
                                let result = conn
                                    .transaction::<(), ApiError, _>(async |conn| {
                                        execute_planned_item(conn, &mut runtime, item.operation())
                                            .await
                                    })
                                    .await;
                                (revision, result)
                            }
                            Err(error) => (None, Err(error)),
                        };
                        match result {
                            Ok(()) => outcomes.push(StorageImportPreflightItem::success(
                                index,
                                revision.map(PostgresRevision::get),
                            )),
                            Err(error) => {
                                aborted = should_abort_preflight(&error, &mode);
                                outcomes.push(StorageImportPreflightItem::failure(
                                    index,
                                    revision.map(PostgresRevision::get),
                                    map_postgres_error(error),
                                ));
                                if aborted {
                                    break;
                                }
                            }
                        }
                    }

                    Err(ApiError::InternalServerError(DRY_RUN_ROLLBACK.to_string()))
                })
                .await;

            match transaction {
                Err(ApiError::InternalServerError(message)) if message == DRY_RUN_ROLLBACK => {
                    Ok(StorageImportPreflight::new(outcomes, aborted))
                }
                Err(error) => Err(error),
                Ok(()) => Err(ApiError::InternalServerError(
                    "Import dry run unexpectedly committed".to_string(),
                )),
            }
        })
        .await
        .map_err(map_postgres_error)
    }

    async fn apply_import_strict(
        &self,
        items: Vec<StorageImportPlanItem>,
    ) -> Result<(), StorageError> {
        with_transaction(self.pool(), async move |conn| {
            let mut runtime = RuntimeState::for_plan(&items)?;
            for item in &items {
                execute_planned_item(conn, &mut runtime, item.operation()).await?;
            }
            Ok::<(), ApiError>(())
        })
        .await
        .map_err(map_postgres_error)
    }

    async fn apply_import_best_effort(
        &self,
        items: Vec<StorageImportPlanItem>,
        mode: StorageImportMode,
    ) -> Result<StorageImportApply, StorageError> {
        let mode = import_mode_from_storage(mode);
        let mut runtime = RuntimeState::for_plan(&items).map_err(map_postgres_error)?;
        let mut outcomes = Vec::with_capacity(items.len());
        let mut aborted = false;

        for item in items {
            let index = item.index();
            let result = with_transaction(self.pool(), async |conn| {
                execute_planned_item(conn, &mut runtime, item.operation()).await
            })
            .await;
            match result {
                Ok(()) => outcomes.push(StorageImportApplyItem::success(index)),
                Err(error) => {
                    aborted = should_abort_best_effort(&error, &mode);
                    outcomes.push(StorageImportApplyItem::failure(
                        index,
                        map_postgres_error(error),
                    ));
                    if aborted {
                        break;
                    }
                }
            }
        }

        Ok(StorageImportApply::new(outcomes, aborted))
    }

    async fn record_import_results(
        &self,
        results: Vec<StorageImportResult>,
    ) -> Result<(), StorageError> {
        let results = results
            .into_iter()
            .map(|result| {
                let (task_id, item_ref, entity_kind, action, identifier, outcome, error, details) =
                    result.into_parts();
                NewImportTaskResultRecord {
                    task_id,
                    item_ref,
                    entity_kind,
                    action,
                    identifier,
                    outcome,
                    error,
                    details,
                }
            })
            .collect::<Vec<_>>();
        insert_import_results(self.pool(), &results)
            .await
            .map(|_| ())
            .map_err(map_postgres_error)
    }
}
