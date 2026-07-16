use crate::db::prelude::*;

use crate::db::{DbPool, with_connection};
use crate::errors::ApiError;
use crate::models::event_subscription::validate_subscription_parts;
use crate::models::{
    Collection, CollectionKey, Group, HubuumClass, HubuumClassRelation, HubuumObject,
    HubuumObjectRelation, IdentityScope, ImportClassInput, ImportCollectionInput,
    ImportEventSinkInput, ImportEventSubscriptionInput, ImportExportTemplateInput,
    ImportGroupInput, ImportGroupMembershipInput, ImportIdentityScopeInput, ImportObjectInput,
    ImportPrincipalInput, ImportPrincipalSubtype, ImportRemoteTargetInput, NewHubuumClass,
    NewHubuumClassRelation, NewHubuumObject, NewHubuumObjectRelation, NewPermission, Permission,
    Permissions, PermissionsList, Principal, ServiceAccount, UpdateCollection, UpdateHubuumClass,
    UpdateHubuumObject, UpdatePermission, User,
};
use crate::utilities::aliases::normalize_template_alias;

pub async fn lookup_collections_by_name(
    pool: &DbPool,
    value: &str,
) -> Result<Vec<Collection>, ApiError> {
    use crate::schema::collections::dsl::{collections, name};

    with_connection(pool, async |conn| {
        collections
            .filter(name.eq(value))
            .order(crate::schema::collections::id.asc())
            .load::<Collection>(conn)
            .await
    })
    .await
}

pub async fn lookup_root_collection(pool: &DbPool) -> Result<Collection, ApiError> {
    with_connection(pool, lookup_root_collection_db).await
}

pub async fn lookup_collection_by_key(
    pool: &DbPool,
    key: &CollectionKey,
) -> Result<Option<Collection>, ApiError> {
    with_connection(pool, async |conn| {
        lookup_collection_by_key_db(conn, key).await
    })
    .await
}

pub async fn lookup_collection_by_id(
    pool: &DbPool,
    collection_id: i32,
) -> Result<Option<Collection>, ApiError> {
    use crate::schema::collections::dsl::{collections, id};

    with_connection(pool, async |conn| {
        collections
            .filter(id.eq(collection_id))
            .first::<Collection>(conn)
            .await
            .optional()
    })
    .await
}

pub async fn lookup_class_by_collection_and_name(
    pool: &DbPool,
    collection_id_value: i32,
    class_name: &str,
) -> Result<Option<HubuumClass>, ApiError> {
    use crate::schema::hubuumclass::dsl::{collection_id, hubuumclass, name};

    with_connection(pool, async |conn| {
        hubuumclass
            .filter(collection_id.eq(collection_id_value))
            .filter(name.eq(class_name))
            .first::<HubuumClass>(conn)
            .await
            .optional()
    })
    .await
}

pub async fn lookup_classes_by_collection_and_names(
    pool: &DbPool,
    collection_id_value: i32,
    class_names: &[String],
) -> Result<Vec<HubuumClass>, ApiError> {
    use crate::schema::hubuumclass::dsl::{collection_id, hubuumclass, name};

    if class_names.is_empty() {
        return Ok(Vec::new());
    }

    with_connection(pool, async |conn| {
        hubuumclass
            .filter(collection_id.eq(collection_id_value))
            .filter(name.eq_any(class_names))
            .load::<HubuumClass>(conn)
            .await
    })
    .await
}

pub async fn lookup_object_by_class_and_name(
    pool: &DbPool,
    class_id_value: i32,
    object_name: &str,
) -> Result<Option<HubuumObject>, ApiError> {
    use crate::schema::hubuumobject::dsl::{hubuum_class_id, hubuumobject, name};

    with_connection(pool, async |conn| {
        hubuumobject
            .filter(hubuum_class_id.eq(class_id_value))
            .filter(name.eq(object_name))
            .first::<HubuumObject>(conn)
            .await
            .optional()
    })
    .await
}

pub async fn lookup_objects_by_class_and_names(
    pool: &DbPool,
    class_id_value: i32,
    object_names: &[String],
) -> Result<Vec<HubuumObject>, ApiError> {
    use crate::schema::hubuumobject::dsl::{hubuum_class_id, hubuumobject, name};

    if object_names.is_empty() {
        return Ok(Vec::new());
    }

    with_connection(pool, async |conn| {
        hubuumobject
            .filter(hubuum_class_id.eq(class_id_value))
            .filter(name.eq_any(object_names))
            .load::<HubuumObject>(conn)
            .await
    })
    .await
}

pub async fn lookup_direct_class_relation(
    pool: &DbPool,
    left: i32,
    right: i32,
) -> Result<Option<HubuumClassRelation>, ApiError> {
    use crate::schema::hubuumclass_relation::dsl::{
        from_hubuum_class_id, hubuumclass_relation, to_hubuum_class_id,
    };
    let pair = normalize_pair(left, right);

    with_connection(pool, async |conn| {
        hubuumclass_relation
            .filter(from_hubuum_class_id.eq(pair.0))
            .filter(to_hubuum_class_id.eq(pair.1))
            .first::<HubuumClassRelation>(conn)
            .await
            .optional()
    })
    .await
}

pub async fn lookup_object_relation(
    pool: &DbPool,
    left: i32,
    right: i32,
) -> Result<Option<HubuumObjectRelation>, ApiError> {
    use crate::schema::hubuumobject_relation::dsl::{
        from_hubuum_object_id, hubuumobject_relation, to_hubuum_object_id,
    };
    let pair = normalize_pair(left, right);

    with_connection(pool, async |conn| {
        hubuumobject_relation
            .filter(from_hubuum_object_id.eq(pair.0))
            .filter(to_hubuum_object_id.eq(pair.1))
            .first::<HubuumObjectRelation>(conn)
            .await
            .optional()
    })
    .await
}

pub async fn lookup_group_by_name(
    pool: &DbPool,
    identity_scope: &str,
    value: &str,
) -> Result<Option<Group>, ApiError> {
    use crate::schema::{groups, identity_scopes};

    with_connection(pool, async |conn| {
        groups::table
            .inner_join(identity_scopes::table)
            .filter(groups::groupname.eq(value))
            .filter(identity_scopes::name.eq(identity_scope))
            .select(groups::all_columns)
            .first::<Group>(conn)
            .await
            .optional()
    })
    .await
}

pub async fn lookup_collection_by_name_db(
    conn: &mut crate::db::DbConnection,
    value: &str,
) -> Result<Option<Collection>, ApiError> {
    let matches = lookup_collections_by_name_db(conn, value).await?;
    match matches.as_slice() {
        [] => Ok(None),
        [collection] => Ok(Some(collection.clone())),
        _ => Err(ApiError::BadRequest(format!(
            "Collection name '{value}' is ambiguous; use collection_key.path"
        ))),
    }
}

pub async fn lookup_collections_by_name_db(
    conn: &mut crate::db::DbConnection,
    value: &str,
) -> Result<Vec<Collection>, ApiError> {
    use crate::schema::collections::dsl::{collections, name};

    collections
        .filter(name.eq(value))
        .order(crate::schema::collections::id.asc())
        .load::<Collection>(conn)
        .await
        .map_err(ApiError::from)
}

pub async fn lookup_root_collection_db(
    conn: &mut crate::db::DbConnection,
) -> Result<Collection, ApiError> {
    use crate::schema::collections::dsl::{collections, parent_collection_id};

    collections
        .filter(parent_collection_id.is_null())
        .first::<Collection>(conn)
        .await
        .map_err(ApiError::from)
}

pub async fn lookup_collection_child_by_name_db(
    conn: &mut crate::db::DbConnection,
    parent_id_value: i32,
    child_name: &str,
) -> Result<Option<Collection>, ApiError> {
    use crate::schema::collections::dsl::{collections, name, parent_collection_id};

    collections
        .filter(parent_collection_id.eq(parent_id_value))
        .filter(name.eq(child_name))
        .first::<Collection>(conn)
        .await
        .optional()
        .map_err(ApiError::from)
}

fn validate_collection_key_path(key: &CollectionKey) -> Result<(), ApiError> {
    if let Some(path) = &key.path {
        match path.last() {
            Some(last) if last == &key.name => Ok(()),
            Some(_) => Err(ApiError::BadRequest(format!(
                "collection_key.path must end with collection name '{}'",
                key.name
            ))),
            None if key.name == "root" => Ok(()),
            None => Err(ApiError::BadRequest(
                "collection_key.path may be empty only for the root collection".to_string(),
            )),
        }
    } else {
        Ok(())
    }
}

pub async fn lookup_collection_by_key_db(
    conn: &mut crate::db::DbConnection,
    key: &CollectionKey,
) -> Result<Option<Collection>, ApiError> {
    validate_collection_key_path(key)?;

    let Some(path) = &key.path else {
        return lookup_collection_by_name_db(conn, &key.name).await;
    };

    if path.is_empty() {
        return lookup_root_collection_db(conn).await.map(Some);
    }

    let mut parent = lookup_root_collection_db(conn).await?;
    let mut current = None;
    for segment in path {
        let child = lookup_collection_child_by_name_db(conn, parent.id, segment).await?;
        let Some(child) = child else {
            return Ok(None);
        };
        parent = child.clone();
        current = Some(child);
    }

    Ok(current)
}

pub async fn lookup_class_by_collection_and_name_db(
    conn: &mut crate::db::DbConnection,
    collection_id_value: i32,
    class_name: &str,
) -> Result<Option<HubuumClass>, ApiError> {
    use crate::schema::hubuumclass::dsl::{collection_id, hubuumclass, name};

    hubuumclass
        .filter(collection_id.eq(collection_id_value))
        .filter(name.eq(class_name))
        .first::<HubuumClass>(conn)
        .await
        .optional()
        .map_err(ApiError::from)
}

pub async fn lookup_object_by_class_and_name_db(
    conn: &mut crate::db::DbConnection,
    class_id_value: i32,
    object_name: &str,
) -> Result<Option<HubuumObject>, ApiError> {
    use crate::schema::hubuumobject::dsl::{hubuum_class_id, hubuumobject, name};

    hubuumobject
        .filter(hubuum_class_id.eq(class_id_value))
        .filter(name.eq(object_name))
        .first::<HubuumObject>(conn)
        .await
        .optional()
        .map_err(ApiError::from)
}

pub async fn lookup_group_by_name_db(
    conn: &mut crate::db::DbConnection,
    identity_scope: &str,
    value: &str,
) -> Result<Option<Group>, ApiError> {
    use crate::schema::{groups, identity_scopes};

    groups::table
        .inner_join(identity_scopes::table)
        .filter(groups::groupname.eq(value))
        .filter(identity_scopes::name.eq(identity_scope))
        .select(groups::all_columns)
        .first::<Group>(conn)
        .await
        .optional()
        .map_err(ApiError::from)
}

pub async fn lookup_identity_scope_id_by_name_db(
    conn: &mut crate::db::DbConnection,
    scope_name: &str,
) -> Result<Option<i32>, ApiError> {
    use crate::schema::identity_scopes::dsl::{id, identity_scopes, name};

    identity_scopes
        .filter(name.eq(scope_name))
        .select(id)
        .first::<i32>(conn)
        .await
        .optional()
        .map_err(ApiError::from)
}

pub async fn lookup_principal_id_by_name_db(
    conn: &mut crate::db::DbConnection,
    identity_scope: &str,
    principal_name: &str,
) -> Result<Option<i32>, ApiError> {
    use crate::schema::{identity_scopes, principals};

    principals::table
        .inner_join(identity_scopes::table)
        .filter(principals::name.eq(principal_name))
        .filter(identity_scopes::name.eq(identity_scope))
        .select(principals::id)
        .first::<i32>(conn)
        .await
        .optional()
        .map_err(ApiError::from)
}

pub async fn lookup_event_sink_id_by_name_db(
    conn: &mut crate::db::DbConnection,
    sink_name: &str,
) -> Result<Option<i32>, ApiError> {
    use crate::schema::event_sinks::dsl::{event_sinks, id, name};

    event_sinks
        .filter(name.eq(sink_name))
        .select(id)
        .first::<i32>(conn)
        .await
        .optional()
        .map_err(ApiError::from)
}

pub async fn create_collection_db(
    conn: &mut crate::db::DbConnection,
    input: &ImportCollectionInput,
    parent_collection_id: Option<i32>,
) -> Result<Collection, ApiError> {
    crate::db::traits::collection::insert_collection_row_with_closure(
        conn,
        &input.name,
        &input.description,
        parent_collection_id,
    )
    .await
}

pub async fn update_collection_db(
    conn: &mut crate::db::DbConnection,
    collection_id_value: i32,
    input: &ImportCollectionInput,
) -> Result<Collection, ApiError> {
    use crate::schema::collections::dsl::{collections, id};

    let update = UpdateCollection {
        name: Some(input.name.clone()),
        description: Some(input.description.clone()),
    };

    crate::db::updated_or_current(
        diesel::update(collections.filter(id.eq(collection_id_value)))
            .set(&update)
            .get_result::<Collection>(conn)
            .await
            .optional(),
        async || {
            collections
                .filter(id.eq(collection_id_value))
                .first(conn)
                .await
        },
    )
    .await
    .map_err(ApiError::from)
}

pub async fn create_class_db(
    conn: &mut crate::db::DbConnection,
    input: &ImportClassInput,
    collection_id_value: i32,
) -> Result<HubuumClass, ApiError> {
    use crate::schema::hubuumclass::dsl::hubuumclass;

    let new_class = NewHubuumClass {
        name: input.name.clone(),
        collection_id: collection_id_value,
        json_schema: input.json_schema.clone(),
        validate_schema: input.validate_schema,
        description: input.description.clone(),
    };

    diesel::insert_into(hubuumclass)
        .values(&new_class)
        .get_result::<HubuumClass>(conn)
        .await
        .map_err(ApiError::from)
}

pub async fn update_class_db(
    conn: &mut crate::db::DbConnection,
    class_id_value: i32,
    input: &ImportClassInput,
) -> Result<HubuumClass, ApiError> {
    use crate::schema::hubuumclass::dsl::{hubuumclass, id};

    let update = UpdateHubuumClass {
        name: Some(input.name.clone()),
        collection_id: None,
        json_schema: input.json_schema.clone(),
        validate_schema: input.validate_schema,
        description: Some(input.description.clone()),
    };

    crate::db::updated_or_current(
        diesel::update(hubuumclass.filter(id.eq(class_id_value)))
            .set(&update)
            .get_result::<HubuumClass>(conn)
            .await
            .optional(),
        async || hubuumclass.filter(id.eq(class_id_value)).first(conn).await,
    )
    .await
    .map_err(ApiError::from)
}

pub async fn create_object_db(
    conn: &mut crate::db::DbConnection,
    input: &ImportObjectInput,
    class: &HubuumClass,
) -> Result<HubuumObject, ApiError> {
    use crate::schema::hubuumobject::dsl::hubuumobject;

    let new_object = NewHubuumObject {
        name: input.name.clone(),
        collection_id: class.collection_id,
        hubuum_class_id: class.id,
        data: input.data.clone(),
        description: input.description.clone(),
    };

    let object = diesel::insert_into(hubuumobject)
        .values(&new_object)
        .get_result::<HubuumObject>(conn)
        .await
        .map_err(ApiError::from)?;
    crate::db::traits::computed_field::materialize_object_in_transaction(conn, &object).await?;
    Ok(object)
}

pub async fn update_object_db(
    conn: &mut crate::db::DbConnection,
    object_id_value: i32,
    input: &ImportObjectInput,
) -> Result<HubuumObject, ApiError> {
    use crate::schema::hubuumobject::dsl::{hubuumobject, id};

    let update = UpdateHubuumObject {
        name: Some(input.name.clone()),
        collection_id: None,
        hubuum_class_id: None,
        data: Some(input.data.clone()),
        description: Some(input.description.clone()),
    };

    let object = crate::db::updated_or_current(
        diesel::update(hubuumobject.filter(id.eq(object_id_value)))
            .set(&update)
            .get_result::<HubuumObject>(conn)
            .await
            .optional(),
        async || {
            hubuumobject
                .filter(id.eq(object_id_value))
                .first(conn)
                .await
        },
    )
    .await
    .map_err(ApiError::from)?;
    crate::db::traits::computed_field::materialize_object_in_transaction(conn, &object).await?;
    Ok(object)
}

fn imported_timestamps(
    timestamps: Option<&crate::models::RestoreTimestamps>,
) -> (chrono::NaiveDateTime, chrono::NaiveDateTime) {
    let now = chrono::Utc::now().naive_utc();
    timestamps
        .map(|value| (value.created_at, value.updated_at))
        .unwrap_or((now, now))
}

async fn preserve_imported_timestamps(
    conn: &mut crate::db::DbConnection,
    preserve: bool,
) -> Result<(), ApiError> {
    let value = if preserve { "on" } else { "off" };
    diesel::sql_query("SELECT set_config('hubuum.preserve_imported_timestamps', $1, true)")
        .bind::<diesel::sql_types::Text, _>(value)
        .execute(conn)
        .await?;
    Ok(())
}

pub async fn upsert_identity_scope_db(
    conn: &mut crate::db::DbConnection,
    input: &ImportIdentityScopeInput,
    overwrite: bool,
) -> Result<i32, ApiError> {
    use crate::schema::identity_scopes::dsl::{
        created_at, id, identity_scopes, name, provider_kind, updated_at,
    };
    let existing = identity_scopes
        .filter(name.eq(&input.name))
        .first::<IdentityScope>(conn)
        .await
        .optional()?;
    let row = match existing {
        Some(_) if !overwrite => {
            return Err(ApiError::Conflict(format!(
                "Identity scope '{}' already exists",
                input.name
            )));
        }
        Some(existing) => {
            let (created, updated) = input
                .timestamps
                .as_ref()
                .map(|value| (value.created_at, value.updated_at))
                .unwrap_or((existing.created_at, existing.updated_at));
            conn.transaction::<IdentityScope, ApiError, _>(async |conn| {
                preserve_imported_timestamps(conn, true).await?;
                let row = diesel::update(identity_scopes.filter(id.eq(existing.id)))
                    .set((
                        provider_kind.eq(&input.provider_kind),
                        created_at.eq(created),
                        updated_at.eq(updated),
                    ))
                    .get_result::<IdentityScope>(conn)
                    .await?;
                preserve_imported_timestamps(conn, false).await?;
                Ok(row)
            })
            .await?
        }
        None => {
            let (created, updated) = imported_timestamps(input.timestamps.as_ref());
            diesel::insert_into(identity_scopes)
                .values((
                    name.eq(&input.name),
                    provider_kind.eq(&input.provider_kind),
                    created_at.eq(created),
                    updated_at.eq(updated),
                ))
                .get_result::<IdentityScope>(conn)
                .await?
        }
    };
    Ok(row.id)
}

pub async fn upsert_group_db(
    conn: &mut crate::db::DbConnection,
    input: &ImportGroupInput,
    identity_scope_id_value: i32,
    overwrite: bool,
) -> Result<i32, ApiError> {
    use crate::schema::groups::dsl::*;
    let existing = groups
        .filter(identity_scope_id.eq(identity_scope_id_value))
        .filter(groupname.eq(&input.groupname))
        .first::<Group>(conn)
        .await
        .optional()?;
    let row = match existing {
        Some(_) if !overwrite => {
            return Err(ApiError::Conflict(format!(
                "Group '{}' already exists in its identity scope",
                input.groupname
            )));
        }
        Some(existing) => {
            let (created, updated) = input
                .timestamps
                .as_ref()
                .map(|value| (value.created_at, value.updated_at))
                .unwrap_or((existing.created_at, existing.updated_at));
            conn.transaction::<Group, ApiError, _>(async |conn| {
                preserve_imported_timestamps(conn, true).await?;
                let row = diesel::update(groups.filter(id.eq(existing.id)))
                    .set((
                        description.eq(&input.description),
                        managed_by.eq(&input.managed_by),
                        external_key.eq(&input.external_key),
                        last_sync_attempted_at.eq(input.last_sync_attempted_at),
                        last_sync_success_at.eq(input.last_sync_success_at),
                        created_at.eq(created),
                        updated_at.eq(updated),
                    ))
                    .get_result::<Group>(conn)
                    .await?;
                preserve_imported_timestamps(conn, false).await?;
                Ok(row)
            })
            .await?
        }
        None => {
            let (created, updated) = imported_timestamps(input.timestamps.as_ref());
            diesel::insert_into(groups)
                .values((
                    groupname.eq(&input.groupname),
                    description.eq(&input.description),
                    identity_scope_id.eq(identity_scope_id_value),
                    managed_by.eq(&input.managed_by),
                    external_key.eq(&input.external_key),
                    last_sync_attempted_at.eq(input.last_sync_attempted_at),
                    last_sync_success_at.eq(input.last_sync_success_at),
                    created_at.eq(created),
                    updated_at.eq(updated),
                ))
                .get_result::<Group>(conn)
                .await?
        }
    };
    Ok(row.id)
}

pub async fn upsert_principal_db(
    conn: &mut crate::db::DbConnection,
    input: &ImportPrincipalInput,
    identity_scope_id_value: i32,
    owner_group_id_value: Option<i32>,
    created_by_value: Option<i32>,
    overwrite: bool,
) -> Result<i32, ApiError> {
    input.validate_credentials()?;
    let supplied_password = match &input.subtype {
        ImportPrincipalSubtype::Human {
            password: Some(password),
            password_hash: None,
            ..
        } => Some(
            crate::utilities::auth::hash_password_async(password.clone())
                .await
                .map_err(|error| ApiError::HashError(error.to_string()))?,
        ),
        ImportPrincipalSubtype::Human {
            password: None,
            password_hash,
            ..
        } => password_hash.clone(),
        _ => None,
    };
    use crate::schema::principals::dsl as p;
    let expected_kind = match &input.subtype {
        ImportPrincipalSubtype::Human { .. } => "human",
        ImportPrincipalSubtype::ServiceAccount { .. } => "service_account",
    };
    let existing = p::principals
        .filter(p::identity_scope_id.eq(identity_scope_id_value))
        .filter(p::name.eq(&input.name))
        .first::<Principal>(conn)
        .await
        .optional()?;
    if let Some(existing) = &existing {
        if !overwrite {
            return Err(ApiError::Conflict(format!(
                "Principal '{}' already exists in its identity scope",
                input.name
            )));
        }
        if existing.kind != expected_kind {
            return Err(ApiError::Conflict(format!(
                "Principal '{}' exists with kind '{}' instead of '{}'",
                input.name, existing.kind, expected_kind
            )));
        }
    }
    let principal = match existing {
        Some(existing) => {
            let (created, updated) = input
                .timestamps
                .as_ref()
                .map(|value| (value.created_at, value.updated_at))
                .unwrap_or((existing.created_at, existing.updated_at));
            conn.transaction::<Principal, ApiError, _>(async |conn| {
                preserve_imported_timestamps(conn, true).await?;
                let principal = diesel::update(p::principals.filter(p::id.eq(existing.id)))
                    .set((
                        p::provider_managed.eq(input.provider_managed),
                        p::settings.eq(&input.settings),
                        p::external_subject.eq(&input.external_subject),
                        p::last_sync_attempted_at.eq(input.last_sync_attempted_at),
                        p::last_sync_success_at.eq(input.last_sync_success_at),
                        p::created_at.eq(created),
                        p::updated_at.eq(updated),
                    ))
                    .get_result::<Principal>(conn)
                    .await?;
                preserve_imported_timestamps(conn, false).await?;
                Ok(principal)
            })
            .await?
        }
        None => {
            let (created, updated) = imported_timestamps(input.timestamps.as_ref());
            diesel::insert_into(p::principals)
                .values((
                    p::kind.eq(expected_kind),
                    p::name.eq(&input.name),
                    p::identity_scope_id.eq(identity_scope_id_value),
                    p::provider_managed.eq(input.provider_managed),
                    p::settings.eq(&input.settings),
                    p::external_subject.eq(&input.external_subject),
                    p::last_sync_attempted_at.eq(input.last_sync_attempted_at),
                    p::last_sync_success_at.eq(input.last_sync_success_at),
                    p::created_at.eq(created),
                    p::updated_at.eq(updated),
                ))
                .get_result::<Principal>(conn)
                .await?
        }
    };

    match &input.subtype {
        ImportPrincipalSubtype::Human {
            password: _,
            password_hash: _,
            proper_name,
            email,
            anonymized_at,
        } => {
            use crate::schema::users::dsl as u;
            let existing_user = u::users
                .filter(u::id.eq(principal.id))
                .first::<User>(conn)
                .await
                .optional()?;
            let (created, updated) = input
                .timestamps
                .as_ref()
                .map(|value| (value.created_at, value.updated_at))
                .or_else(|| {
                    existing_user
                        .as_ref()
                        .map(|row| (row.created_at, row.updated_at))
                })
                .unwrap_or_else(|| imported_timestamps(None));
            if let Some(existing_user) = existing_user {
                conn.transaction::<(), ApiError, _>(async |conn| {
                    preserve_imported_timestamps(conn, true).await?;
                    diesel::update(u::users.filter(u::id.eq(principal.id)))
                        .set((
                            u::password.eq(supplied_password.or(existing_user.password)),
                            u::proper_name.eq(proper_name),
                            u::email.eq(email),
                            u::anonymized_at.eq(*anonymized_at),
                            u::created_at.eq(created),
                            u::updated_at.eq(updated),
                        ))
                        .execute(conn)
                        .await?;
                    preserve_imported_timestamps(conn, false).await?;
                    Ok(())
                })
                .await?;
            } else {
                diesel::insert_into(u::users)
                    .values((
                        u::id.eq(principal.id),
                        u::kind.eq("human"),
                        u::password.eq(supplied_password),
                        u::proper_name.eq(proper_name),
                        u::email.eq(email),
                        u::anonymized_at.eq(*anonymized_at),
                        u::created_at.eq(created),
                        u::updated_at.eq(updated),
                    ))
                    .execute(conn)
                    .await?;
            }
        }
        ImportPrincipalSubtype::ServiceAccount {
            description: account_description,
            disabled_at,
            ..
        } => {
            let owner_group_id_value = owner_group_id_value.ok_or_else(|| {
                ApiError::BadRequest("Service-account import requires an owner group".to_string())
            })?;
            use crate::schema::service_accounts::dsl as s;
            let existing_account = s::service_accounts
                .filter(s::id.eq(principal.id))
                .first::<ServiceAccount>(conn)
                .await
                .optional()?;
            let (created, updated) = input
                .timestamps
                .as_ref()
                .map(|value| (value.created_at, value.updated_at))
                .or_else(|| {
                    existing_account
                        .as_ref()
                        .map(|row| (row.created_at, row.updated_at))
                })
                .unwrap_or_else(|| imported_timestamps(None));
            if existing_account.is_some() {
                conn.transaction::<(), ApiError, _>(async |conn| {
                    preserve_imported_timestamps(conn, true).await?;
                    diesel::update(s::service_accounts.filter(s::id.eq(principal.id)))
                        .set((
                            s::description.eq(account_description),
                            s::owner_group_id.eq(owner_group_id_value),
                            s::created_by.eq(created_by_value),
                            s::disabled_at.eq(*disabled_at),
                            s::created_at.eq(created),
                            s::updated_at.eq(updated),
                        ))
                        .execute(conn)
                        .await?;
                    preserve_imported_timestamps(conn, false).await?;
                    Ok(())
                })
                .await?;
            } else {
                diesel::insert_into(s::service_accounts)
                    .values((
                        s::id.eq(principal.id),
                        s::kind.eq("service_account"),
                        s::description.eq(account_description),
                        s::owner_group_id.eq(owner_group_id_value),
                        s::created_by.eq(created_by_value),
                        s::disabled_at.eq(*disabled_at),
                        s::created_at.eq(created),
                        s::updated_at.eq(updated),
                    ))
                    .execute(conn)
                    .await?;
            }
        }
    }
    Ok(principal.id)
}

pub async fn upsert_group_membership_db(
    conn: &mut crate::db::DbConnection,
    input: &ImportGroupMembershipInput,
    principal_id_value: i32,
    group_id_value: i32,
    source_scope_ids: &[i32],
    overwrite: bool,
) -> Result<(), ApiError> {
    use crate::schema::group_membership_sources::dsl as s;
    use crate::schema::group_memberships::dsl as m;

    let existing_membership = m::group_memberships
        .filter(m::principal_id.eq(principal_id_value))
        .filter(m::group_id.eq(group_id_value))
        .select((m::created_at, m::updated_at))
        .first::<(chrono::NaiveDateTime, chrono::NaiveDateTime)>(conn)
        .await
        .optional()?;
    if existing_membership.is_some() && !overwrite {
        return Err(ApiError::Conflict(format!(
            "Principal {principal_id_value} is already a member of group {group_id_value}"
        )));
    }

    conn.transaction::<(), ApiError, _>(async |conn| {
        preserve_imported_timestamps(conn, true).await?;
        let membership_timestamps = input
            .timestamps
            .as_ref()
            .map(|value| (value.created_at, value.updated_at))
            .or(existing_membership)
            .unwrap_or_else(|| imported_timestamps(None));
        match existing_membership {
            Some(_) => {
                diesel::update(
                    m::group_memberships
                        .filter(m::principal_id.eq(principal_id_value))
                        .filter(m::group_id.eq(group_id_value)),
                )
                .set((
                    m::created_at.eq(membership_timestamps.0),
                    m::updated_at.eq(membership_timestamps.1),
                ))
                .execute(conn)
                .await?;
            }
            None => {
                diesel::insert_into(m::group_memberships)
                    .values((
                        m::principal_id.eq(principal_id_value),
                        m::group_id.eq(group_id_value),
                        m::created_at.eq(membership_timestamps.0),
                        m::updated_at.eq(membership_timestamps.1),
                    ))
                    .execute(conn)
                    .await?;
            }
        }

        for (source, source_scope_id_value) in input.sources.iter().zip(source_scope_ids) {
            let existing_source = s::group_membership_sources
                .filter(s::principal_id.eq(principal_id_value))
                .filter(s::group_id.eq(group_id_value))
                .filter(s::source.eq(&source.source))
                .filter(s::source_scope_id.eq(*source_scope_id_value))
                .filter(s::source_key.eq(&source.source_key))
                .select((s::created_at, s::updated_at))
                .first::<(chrono::NaiveDateTime, chrono::NaiveDateTime)>(conn)
                .await
                .optional()?;
            let source_timestamps = source
                .timestamps
                .as_ref()
                .map(|value| (value.created_at, value.updated_at))
                .or(existing_source)
                .unwrap_or_else(|| imported_timestamps(None));
            if existing_source.is_some() {
                diesel::update(
                    s::group_membership_sources
                        .filter(s::principal_id.eq(principal_id_value))
                        .filter(s::group_id.eq(group_id_value))
                        .filter(s::source.eq(&source.source))
                        .filter(s::source_scope_id.eq(*source_scope_id_value))
                        .filter(s::source_key.eq(&source.source_key)),
                )
                .set((
                    s::created_at.eq(source_timestamps.0),
                    s::updated_at.eq(source_timestamps.1),
                ))
                .execute(conn)
                .await?;
            } else {
                diesel::insert_into(s::group_membership_sources)
                    .values((
                        s::principal_id.eq(principal_id_value),
                        s::group_id.eq(group_id_value),
                        s::source.eq(&source.source),
                        s::source_scope_id.eq(*source_scope_id_value),
                        s::source_key.eq(&source.source_key),
                        s::created_at.eq(source_timestamps.0),
                        s::updated_at.eq(source_timestamps.1),
                    ))
                    .execute(conn)
                    .await?;
            }
        }
        preserve_imported_timestamps(conn, false).await?;
        Ok(())
    })
    .await
}

pub async fn load_export_template_sources_db(
    conn: &mut crate::db::DbConnection,
    collection_id_value: i32,
) -> Result<Vec<(String, String)>, ApiError> {
    use crate::schema::export_templates::dsl as t;

    Ok(t::export_templates
        .filter(t::collection_id.eq(collection_id_value))
        .order(t::id.asc())
        .select((t::name, t::template))
        .load::<(String, String)>(conn)
        .await?)
}

pub async fn upsert_export_template_db(
    conn: &mut crate::db::DbConnection,
    input: &ImportExportTemplateInput,
    collection_id_value: i32,
    class_id_value: Option<i32>,
    overwrite: bool,
) -> Result<i32, ApiError> {
    use crate::schema::export_templates::dsl as t;
    let existing = t::export_templates
        .filter(t::collection_id.eq(collection_id_value))
        .filter(t::name.eq(&input.name))
        .select((t::id, t::created_at, t::updated_at))
        .first::<(i32, chrono::NaiveDateTime, chrono::NaiveDateTime)>(conn)
        .await
        .optional()?;
    if existing.is_some() && !overwrite {
        return Err(ApiError::Conflict(format!(
            "Export template '{}' already exists in the collection",
            input.name
        )));
    }
    let include = input
        .include
        .as_ref()
        .map(serde_json::to_value)
        .transpose()?;
    let relation_context = input
        .relation_context
        .as_ref()
        .map(serde_json::to_value)
        .transpose()?;
    let default_limits = input
        .default_limits
        .as_ref()
        .map(serde_json::to_value)
        .transpose()?;
    let scope_kind = input.scope_kind.map(|value| value.as_str().to_string());
    let missing_policy = input
        .default_missing_data_policy
        .map(|value| value.as_str().to_string());
    match existing {
        Some((existing_id, existing_created, existing_updated)) => {
            let (created, updated) = input
                .timestamps
                .as_ref()
                .map(|value| (value.created_at, value.updated_at))
                .unwrap_or((existing_created, existing_updated));
            conn.transaction::<(), ApiError, _>(async |conn| {
                preserve_imported_timestamps(conn, true).await?;
                diesel::update(t::export_templates.filter(t::id.eq(existing_id)))
                    .set((
                        t::description.eq(&input.description),
                        t::content_type.eq(input.content_type.as_mime()),
                        t::template.eq(&input.template),
                        t::kind.eq(input.kind.as_str()),
                        t::scope_kind.eq(scope_kind),
                        t::class_id.eq(class_id_value),
                        t::default_query.eq(&input.default_query),
                        t::include.eq(include),
                        t::relation_context.eq(relation_context),
                        t::default_missing_data_policy.eq(missing_policy),
                        t::default_limits.eq(default_limits),
                        t::created_at.eq(created),
                        t::updated_at.eq(updated),
                    ))
                    .execute(conn)
                    .await?;
                preserve_imported_timestamps(conn, false).await?;
                Ok(())
            })
            .await?;
            Ok(existing_id)
        }
        None => {
            let (created, updated) = imported_timestamps(input.timestamps.as_ref());
            Ok(diesel::insert_into(t::export_templates)
                .values((
                    t::collection_id.eq(collection_id_value),
                    t::name.eq(&input.name),
                    t::description.eq(&input.description),
                    t::content_type.eq(input.content_type.as_mime()),
                    t::template.eq(&input.template),
                    t::kind.eq(input.kind.as_str()),
                    t::scope_kind.eq(scope_kind),
                    t::class_id.eq(class_id_value),
                    t::default_query.eq(&input.default_query),
                    t::include.eq(include),
                    t::relation_context.eq(relation_context),
                    t::default_missing_data_policy.eq(missing_policy),
                    t::default_limits.eq(default_limits),
                    t::created_at.eq(created),
                    t::updated_at.eq(updated),
                ))
                .returning(t::id)
                .get_result::<i32>(conn)
                .await?)
        }
    }
}

pub async fn upsert_remote_target_db(
    conn: &mut crate::db::DbConnection,
    input: &ImportRemoteTargetInput,
    collection_id_value: i32,
    class_id_value: Option<i32>,
    overwrite: bool,
) -> Result<i32, ApiError> {
    use crate::schema::remote_targets::dsl as r;
    let existing = r::remote_targets
        .filter(r::collection_id.eq(collection_id_value))
        .filter(r::name.eq(&input.name))
        .select((r::id, r::created_at, r::updated_at))
        .first::<(i32, chrono::NaiveDateTime, chrono::NaiveDateTime)>(conn)
        .await
        .optional()?;
    if existing.is_some() && !overwrite {
        return Err(ApiError::Conflict(format!(
            "Remote target '{}' already exists in the collection",
            input.name
        )));
    }
    let auth_config = serde_json::to_value(&input.auth_config)?;
    let subject_types = serde_json::to_value(&input.allowed_subject_types)?;
    match existing {
        Some((existing_id, existing_created, existing_updated)) => {
            let (created, updated) = input
                .timestamps
                .as_ref()
                .map(|value| (value.created_at, value.updated_at))
                .unwrap_or((existing_created, existing_updated));
            conn.transaction::<(), ApiError, _>(async |conn| {
                preserve_imported_timestamps(conn, true).await?;
                diesel::update(r::remote_targets.filter(r::id.eq(existing_id)))
                    .set((
                        r::class_id.eq(class_id_value),
                        r::description.eq(&input.description),
                        r::method.eq(input.method.as_str()),
                        r::url_template.eq(&input.url_template),
                        r::headers_template.eq(&input.headers_template),
                        r::body_template.eq(&input.body_template),
                        r::auth_config.eq(auth_config),
                        r::allowed_subject_types.eq(subject_types),
                        r::timeout_ms.eq(input.timeout_ms),
                        r::enabled.eq(input.enabled),
                        r::created_at.eq(created),
                        r::updated_at.eq(updated),
                    ))
                    .execute(conn)
                    .await?;
                preserve_imported_timestamps(conn, false).await?;
                Ok(())
            })
            .await?;
            Ok(existing_id)
        }
        None => {
            let (created, updated) = imported_timestamps(input.timestamps.as_ref());
            Ok(diesel::insert_into(r::remote_targets)
                .values((
                    r::collection_id.eq(collection_id_value),
                    r::class_id.eq(class_id_value),
                    r::name.eq(&input.name),
                    r::description.eq(&input.description),
                    r::method.eq(input.method.as_str()),
                    r::url_template.eq(&input.url_template),
                    r::headers_template.eq(&input.headers_template),
                    r::body_template.eq(&input.body_template),
                    r::auth_config.eq(auth_config),
                    r::allowed_subject_types.eq(subject_types),
                    r::timeout_ms.eq(input.timeout_ms),
                    r::enabled.eq(input.enabled),
                    r::created_at.eq(created),
                    r::updated_at.eq(updated),
                ))
                .returning(r::id)
                .get_result::<i32>(conn)
                .await?)
        }
    }
}

pub async fn upsert_event_sink_db(
    conn: &mut crate::db::DbConnection,
    input: &ImportEventSinkInput,
    overwrite: bool,
) -> Result<i32, ApiError> {
    use crate::schema::event_sinks::dsl as s;
    let existing = s::event_sinks
        .filter(s::name.eq(&input.name))
        .select((s::id, s::created_at, s::updated_at))
        .first::<(i32, chrono::NaiveDateTime, chrono::NaiveDateTime)>(conn)
        .await
        .optional()?;
    if existing.is_some() && !overwrite {
        return Err(ApiError::Conflict(format!(
            "Event sink '{}' already exists",
            input.name
        )));
    }
    match existing {
        Some((existing_id, existing_created, existing_updated)) => {
            let (created, updated) = input
                .timestamps
                .as_ref()
                .map(|value| (value.created_at, value.updated_at))
                .unwrap_or((existing_created, existing_updated));
            conn.transaction::<(), ApiError, _>(async |conn| {
                preserve_imported_timestamps(conn, true).await?;
                diesel::update(s::event_sinks.filter(s::id.eq(existing_id)))
                    .set((
                        s::kind.eq(input.kind.as_str()),
                        s::config.eq(&input.config),
                        s::secret_ref.eq(&input.secret_ref),
                        s::enabled.eq(input.enabled),
                        s::created_at.eq(created),
                        s::updated_at.eq(updated),
                    ))
                    .execute(conn)
                    .await?;
                preserve_imported_timestamps(conn, false).await?;
                Ok(())
            })
            .await?;
            Ok(existing_id)
        }
        None => {
            let (created, updated) = imported_timestamps(input.timestamps.as_ref());
            Ok(diesel::insert_into(s::event_sinks)
                .values((
                    s::name.eq(&input.name),
                    s::kind.eq(input.kind.as_str()),
                    s::config.eq(&input.config),
                    s::secret_ref.eq(&input.secret_ref),
                    s::enabled.eq(input.enabled),
                    s::created_at.eq(created),
                    s::updated_at.eq(updated),
                ))
                .returning(s::id)
                .get_result::<i32>(conn)
                .await?)
        }
    }
}

pub async fn upsert_event_subscription_db(
    conn: &mut crate::db::DbConnection,
    input: &ImportEventSubscriptionInput,
    collection_id_value: i32,
    sink_id_value: i32,
    overwrite: bool,
) -> Result<i32, ApiError> {
    let filter =
        serde_json::from_value::<hubuum_events_core::EventSubscriptionFilter>(input.filter.clone())
            .map_err(|error| {
                ApiError::BadRequest(format!("Invalid event subscription filter: {error}"))
            })?;
    validate_subscription_parts(&input.entity_types, &input.actions, &filter, &input.routing)?;

    use crate::schema::event_subscriptions::dsl as s;
    let existing = s::event_subscriptions
        .filter(s::collection_id.eq(collection_id_value))
        .filter(s::name.eq(&input.name))
        .select((s::id, s::created_at, s::updated_at))
        .first::<(i32, chrono::NaiveDateTime, chrono::NaiveDateTime)>(conn)
        .await
        .optional()?;
    if existing.is_some() && !overwrite {
        return Err(ApiError::Conflict(format!(
            "Event subscription '{}' already exists in the collection",
            input.name
        )));
    }
    let entity_types = serde_json::to_value(&input.entity_types)?;
    let actions = serde_json::to_value(&input.actions)?;
    match existing {
        Some((existing_id, existing_created, existing_updated)) => {
            let (created, updated) = input
                .timestamps
                .as_ref()
                .map(|value| (value.created_at, value.updated_at))
                .unwrap_or((existing_created, existing_updated));
            conn.transaction::<(), ApiError, _>(async |conn| {
                preserve_imported_timestamps(conn, true).await?;
                diesel::update(s::event_subscriptions.filter(s::id.eq(existing_id)))
                    .set((
                        s::sink_id.eq(sink_id_value),
                        s::description.eq(&input.description),
                        s::entity_types.eq(entity_types),
                        s::actions.eq(actions),
                        s::filter.eq(&input.filter),
                        s::routing.eq(&input.routing),
                        s::enabled.eq(input.enabled),
                        s::created_at.eq(created),
                        s::updated_at.eq(updated),
                    ))
                    .execute(conn)
                    .await?;
                preserve_imported_timestamps(conn, false).await?;
                Ok(())
            })
            .await?;
            Ok(existing_id)
        }
        None => {
            let (created, updated) = imported_timestamps(input.timestamps.as_ref());
            Ok(diesel::insert_into(s::event_subscriptions)
                .values((
                    s::collection_id.eq(collection_id_value),
                    s::sink_id.eq(sink_id_value),
                    s::name.eq(&input.name),
                    s::description.eq(&input.description),
                    s::entity_types.eq(entity_types),
                    s::actions.eq(actions),
                    s::filter.eq(&input.filter),
                    s::routing.eq(&input.routing),
                    s::enabled.eq(input.enabled),
                    s::created_at.eq(created),
                    s::updated_at.eq(updated),
                ))
                .returning(s::id)
                .get_result::<i32>(conn)
                .await?)
        }
    }
}

pub async fn create_class_relation_db(
    conn: &mut crate::db::DbConnection,
    left: i32,
    right: i32,
    forward_template_alias: Option<String>,
    reverse_template_alias: Option<String>,
) -> Result<HubuumClassRelation, ApiError> {
    use crate::schema::hubuumclass_relation::dsl::hubuumclass_relation;
    let pair = normalize_pair(left, right);
    let forward_template_alias =
        normalize_template_alias_option(forward_template_alias.as_deref())?;
    let reverse_template_alias =
        normalize_template_alias_option(reverse_template_alias.as_deref())?;
    let new_relation = NewHubuumClassRelation {
        from_hubuum_class_id: pair.0,
        to_hubuum_class_id: pair.1,
        forward_template_alias: if left <= right {
            forward_template_alias.clone()
        } else {
            reverse_template_alias.clone()
        },
        reverse_template_alias: if left <= right {
            reverse_template_alias
        } else {
            forward_template_alias
        },
    };

    diesel::insert_into(hubuumclass_relation)
        .values(&new_relation)
        .get_result::<HubuumClassRelation>(conn)
        .await
        .map_err(ApiError::from)
}

fn normalize_template_alias_option(alias: Option<&str>) -> Result<Option<String>, ApiError> {
    alias.map(normalize_template_alias).transpose()
}

pub async fn create_object_relation_db(
    conn: &mut crate::db::DbConnection,
    from_object: &HubuumObject,
    to_object: &HubuumObject,
) -> Result<HubuumObjectRelation, ApiError> {
    use crate::schema::hubuumclass_relation::dsl::{
        from_hubuum_class_id, hubuumclass_relation, to_hubuum_class_id,
    };
    use crate::schema::hubuumobject_relation::dsl::hubuumobject_relation;
    let class_pair = normalize_pair(from_object.hubuum_class_id, to_object.hubuum_class_id);
    let relation = hubuumclass_relation
        .filter(from_hubuum_class_id.eq(class_pair.0))
        .filter(to_hubuum_class_id.eq(class_pair.1))
        .first::<HubuumClassRelation>(conn)
        .await?;

    let object_pair = normalize_pair(from_object.id, to_object.id);
    let new_relation = NewHubuumObjectRelation {
        from_hubuum_object_id: object_pair.0,
        to_hubuum_object_id: object_pair.1,
        class_relation_id: relation.id,
    };

    diesel::insert_into(hubuumobject_relation)
        .values(&new_relation)
        .get_result::<HubuumObjectRelation>(conn)
        .await
        .map_err(ApiError::from)
}

pub async fn apply_permissions_db(
    conn: &mut crate::db::DbConnection,
    collection_id_value: i32,
    group_id_value: i32,
    permissions: &[Permissions],
    replace_existing: bool,
) -> Result<Permission, ApiError> {
    use crate::schema::permissions::dsl::{
        collection_id, group_id, permissions as permissions_table,
    };

    let existing = permissions_table
        .filter(collection_id.eq(collection_id_value))
        .filter(group_id.eq(group_id_value))
        .first::<Permission>(conn)
        .await
        .optional()?;

    let permission_list = PermissionsList::new(permissions.to_vec());
    match existing {
        Some(_) => {
            let mut update = if replace_existing {
                UpdatePermission {
                    has_read_collection: Some(false),
                    has_update_collection: Some(false),
                    has_delete_collection: Some(false),
                    has_delegate_collection: Some(false),
                    has_create_class: Some(false),
                    has_read_class: Some(false),
                    has_update_class: Some(false),
                    has_delete_class: Some(false),
                    has_create_object: Some(false),
                    has_read_object: Some(false),
                    has_update_object: Some(false),
                    has_delete_object: Some(false),
                    has_create_class_relation: Some(false),
                    has_read_class_relation: Some(false),
                    has_update_class_relation: Some(false),
                    has_delete_class_relation: Some(false),
                    has_create_object_relation: Some(false),
                    has_read_object_relation: Some(false),
                    has_update_object_relation: Some(false),
                    has_delete_object_relation: Some(false),
                    has_read_template: Some(false),
                    has_create_template: Some(false),
                    has_update_template: Some(false),
                    has_delete_template: Some(false),
                    has_read_remote_target: Some(false),
                    has_create_remote_target: Some(false),
                    has_update_remote_target: Some(false),
                    has_delete_remote_target: Some(false),
                    has_execute_remote_target: Some(false),
                    has_read_audit: Some(false),
                    has_manage_event_subscription: Some(false),
                }
            } else {
                UpdatePermission::default()
            };
            apply_permission_list_to_update(&mut update, permissions);

            diesel::update(
                permissions_table
                    .filter(collection_id.eq(collection_id_value))
                    .filter(group_id.eq(group_id_value)),
            )
            .set(&update)
            .get_result::<Permission>(conn)
            .await
            .map_err(ApiError::from)
        }
        None => {
            let new_entry = NewPermission {
                collection_id: collection_id_value,
                group_id: group_id_value,
                has_read_collection: permission_list.contains(&Permissions::ReadCollection),
                has_update_collection: permission_list.contains(&Permissions::UpdateCollection),
                has_delete_collection: permission_list.contains(&Permissions::DeleteCollection),
                has_delegate_collection: permission_list.contains(&Permissions::DelegateCollection),
                has_create_class: permission_list.contains(&Permissions::CreateClass),
                has_read_class: permission_list.contains(&Permissions::ReadClass),
                has_update_class: permission_list.contains(&Permissions::UpdateClass),
                has_delete_class: permission_list.contains(&Permissions::DeleteClass),
                has_create_object: permission_list.contains(&Permissions::CreateObject),
                has_read_object: permission_list.contains(&Permissions::ReadObject),
                has_update_object: permission_list.contains(&Permissions::UpdateObject),
                has_delete_object: permission_list.contains(&Permissions::DeleteObject),
                has_create_class_relation: permission_list
                    .contains(&Permissions::CreateClassRelation),
                has_read_class_relation: permission_list.contains(&Permissions::ReadClassRelation),
                has_update_class_relation: permission_list
                    .contains(&Permissions::UpdateClassRelation),
                has_delete_class_relation: permission_list
                    .contains(&Permissions::DeleteClassRelation),
                has_create_object_relation: permission_list
                    .contains(&Permissions::CreateObjectRelation),
                has_read_object_relation: permission_list
                    .contains(&Permissions::ReadObjectRelation),
                has_update_object_relation: permission_list
                    .contains(&Permissions::UpdateObjectRelation),
                has_delete_object_relation: permission_list
                    .contains(&Permissions::DeleteObjectRelation),
                has_read_template: permission_list.contains(&Permissions::ReadTemplate),
                has_create_template: permission_list.contains(&Permissions::CreateTemplate),
                has_update_template: permission_list.contains(&Permissions::UpdateTemplate),
                has_delete_template: permission_list.contains(&Permissions::DeleteTemplate),
                has_read_remote_target: permission_list.contains(&Permissions::ReadRemoteTarget),
                has_create_remote_target: permission_list
                    .contains(&Permissions::CreateRemoteTarget),
                has_update_remote_target: permission_list
                    .contains(&Permissions::UpdateRemoteTarget),
                has_delete_remote_target: permission_list
                    .contains(&Permissions::DeleteRemoteTarget),
                has_execute_remote_target: permission_list
                    .contains(&Permissions::ExecuteRemoteTarget),
                has_read_audit: permission_list.contains(&Permissions::ReadAudit),
                has_manage_event_subscription: permission_list
                    .contains(&Permissions::ManageEventSubscription),
            };

            diesel::insert_into(permissions_table)
                .values(&new_entry)
                .get_result::<Permission>(conn)
                .await
                .map_err(ApiError::from)
        }
    }
}

fn normalize_pair(left: i32, right: i32) -> (i32, i32) {
    if left <= right {
        (left, right)
    } else {
        (right, left)
    }
}

fn apply_permission_list_to_update(update: &mut UpdatePermission, permissions: &[Permissions]) {
    for permission in permissions {
        match permission {
            Permissions::ReadCollection => update.has_read_collection = Some(true),
            Permissions::UpdateCollection => update.has_update_collection = Some(true),
            Permissions::DeleteCollection => update.has_delete_collection = Some(true),
            Permissions::DelegateCollection => update.has_delegate_collection = Some(true),
            Permissions::CreateClass => update.has_create_class = Some(true),
            Permissions::ReadClass => update.has_read_class = Some(true),
            Permissions::UpdateClass => update.has_update_class = Some(true),
            Permissions::DeleteClass => update.has_delete_class = Some(true),
            Permissions::CreateObject => update.has_create_object = Some(true),
            Permissions::ReadObject => update.has_read_object = Some(true),
            Permissions::UpdateObject => update.has_update_object = Some(true),
            Permissions::DeleteObject => update.has_delete_object = Some(true),
            Permissions::CreateClassRelation => update.has_create_class_relation = Some(true),
            Permissions::ReadClassRelation => update.has_read_class_relation = Some(true),
            Permissions::UpdateClassRelation => update.has_update_class_relation = Some(true),
            Permissions::DeleteClassRelation => update.has_delete_class_relation = Some(true),
            Permissions::CreateObjectRelation => update.has_create_object_relation = Some(true),
            Permissions::ReadObjectRelation => update.has_read_object_relation = Some(true),
            Permissions::UpdateObjectRelation => update.has_update_object_relation = Some(true),
            Permissions::DeleteObjectRelation => update.has_delete_object_relation = Some(true),
            Permissions::ReadTemplate => update.has_read_template = Some(true),
            Permissions::CreateTemplate => update.has_create_template = Some(true),
            Permissions::UpdateTemplate => update.has_update_template = Some(true),
            Permissions::DeleteTemplate => update.has_delete_template = Some(true),
            Permissions::ReadRemoteTarget => update.has_read_remote_target = Some(true),
            Permissions::CreateRemoteTarget => update.has_create_remote_target = Some(true),
            Permissions::UpdateRemoteTarget => update.has_update_remote_target = Some(true),
            Permissions::DeleteRemoteTarget => update.has_delete_remote_target = Some(true),
            Permissions::ExecuteRemoteTarget => update.has_execute_remote_target = Some(true),
            Permissions::ReadAudit => update.has_read_audit = Some(true),
            Permissions::ManageEventSubscription => {
                update.has_manage_event_subscription = Some(true);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::traits::identity::ensure_identity_scope;
    use crate::models::{GroupKey, LDAP_PROVIDER_KIND, NewGroup};
    use crate::tests::TestScope;

    #[actix_rt::test]
    async fn group_lookup_disambiguates_identity_scopes() {
        let scope = TestScope::new();
        let groupname = scope.scoped_name("shared_groupname");
        let local_group = NewGroup {
            identity_scope: None,
            groupname: groupname.clone(),
            description: Some("Local group".to_string()),
        }
        .save_without_events(&scope.pool)
        .await
        .unwrap();

        let external_scope_name = scope.scoped_name("directory");
        let external_scope =
            ensure_identity_scope(&scope.pool, &external_scope_name, LDAP_PROVIDER_KIND)
                .await
                .unwrap();
        let external_group = with_connection(&scope.pool, async |conn| {
            use crate::schema::groups;

            diesel::insert_into(groups::table)
                .values((
                    groups::identity_scope_id.eq(external_scope.id),
                    groups::groupname.eq(&groupname),
                    groups::description.eq("Directory group"),
                    groups::managed_by.eq(LDAP_PROVIDER_KIND),
                    groups::external_key.eq(scope.scoped_name("external_group_key")),
                ))
                .get_result::<Group>(conn)
                .await
        })
        .await
        .unwrap();

        let external_key = GroupKey {
            identity_scope: Some(external_scope_name),
            groupname: groupname.clone(),
        };
        let loaded_external = lookup_group_by_name(
            scope.pool.get_ref(),
            external_key.identity_scope_name(),
            &external_key.groupname,
        )
        .await
        .unwrap()
        .unwrap();
        let local_key = GroupKey {
            identity_scope: None,
            groupname,
        };
        let loaded_local = with_connection(&scope.pool, async |conn| {
            lookup_group_by_name_db(conn, local_key.identity_scope_name(), &local_key.groupname)
                .await
        })
        .await
        .unwrap()
        .unwrap();

        assert_eq!(loaded_external.id, external_group.id);
        assert_eq!(loaded_local.id, local_group.id);
    }
}
