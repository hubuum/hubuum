use diesel::prelude::*;

use crate::db::{DbPool, with_connection};
use crate::errors::ApiError;
use crate::models::{
    Collection, Group, HubuumClass, HubuumClassRelation, HubuumObject, HubuumObjectRelation,
    ImportClassInput, ImportCollectionInput, ImportObjectInput, NewHubuumClass,
    NewHubuumClassRelation, NewHubuumObject, NewHubuumObjectRelation, NewPermission, Permission,
    Permissions, PermissionsList, UpdateCollection, UpdateHubuumClass, UpdateHubuumObject,
    UpdatePermission,
};
use crate::utilities::aliases::normalize_template_alias;

pub async fn lookup_collection_by_name(
    pool: &DbPool,
    value: &str,
) -> Result<Option<Collection>, ApiError> {
    use crate::schema::collections::dsl::{collections, name};

    with_connection(pool, |conn| {
        collections
            .filter(name.eq(value))
            .first::<Collection>(conn)
            .optional()
    })
}

pub async fn lookup_collections_by_names(
    pool: &DbPool,
    values: &[String],
) -> Result<Vec<Collection>, ApiError> {
    use crate::schema::collections::dsl::{collections, name};

    if values.is_empty() {
        return Ok(Vec::new());
    }

    with_connection(pool, |conn| {
        collections
            .filter(name.eq_any(values))
            .load::<Collection>(conn)
    })
}

pub async fn lookup_collection_by_id(
    pool: &DbPool,
    collection_id: i32,
) -> Result<Option<Collection>, ApiError> {
    use crate::schema::collections::dsl::{collections, id};

    with_connection(pool, |conn| {
        collections
            .filter(id.eq(collection_id))
            .first::<Collection>(conn)
            .optional()
    })
}

pub async fn lookup_class_by_collection_and_name(
    pool: &DbPool,
    collection_id_value: i32,
    class_name: &str,
) -> Result<Option<HubuumClass>, ApiError> {
    use crate::schema::hubuumclass::dsl::{collection_id, hubuumclass, name};

    with_connection(pool, |conn| {
        hubuumclass
            .filter(collection_id.eq(collection_id_value))
            .filter(name.eq(class_name))
            .first::<HubuumClass>(conn)
            .optional()
    })
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

    with_connection(pool, |conn| {
        hubuumclass
            .filter(collection_id.eq(collection_id_value))
            .filter(name.eq_any(class_names))
            .load::<HubuumClass>(conn)
    })
}

pub async fn lookup_object_by_class_and_name(
    pool: &DbPool,
    class_id_value: i32,
    object_name: &str,
) -> Result<Option<HubuumObject>, ApiError> {
    use crate::schema::hubuumobject::dsl::{hubuum_class_id, hubuumobject, name};

    with_connection(pool, |conn| {
        hubuumobject
            .filter(hubuum_class_id.eq(class_id_value))
            .filter(name.eq(object_name))
            .first::<HubuumObject>(conn)
            .optional()
    })
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

    with_connection(pool, |conn| {
        hubuumobject
            .filter(hubuum_class_id.eq(class_id_value))
            .filter(name.eq_any(object_names))
            .load::<HubuumObject>(conn)
    })
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

    with_connection(pool, |conn| {
        hubuumclass_relation
            .filter(from_hubuum_class_id.eq(pair.0))
            .filter(to_hubuum_class_id.eq(pair.1))
            .first::<HubuumClassRelation>(conn)
            .optional()
    })
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

    with_connection(pool, |conn| {
        hubuumobject_relation
            .filter(from_hubuum_object_id.eq(pair.0))
            .filter(to_hubuum_object_id.eq(pair.1))
            .first::<HubuumObjectRelation>(conn)
            .optional()
    })
}

pub async fn lookup_group_by_name(pool: &DbPool, value: &str) -> Result<Option<Group>, ApiError> {
    use crate::schema::groups::dsl::{groupname, groups};

    with_connection(pool, |conn| {
        groups
            .filter(groupname.eq(value))
            .first::<Group>(conn)
            .optional()
    })
}

pub fn lookup_collection_by_name_db(
    conn: &mut diesel::PgConnection,
    value: &str,
) -> Result<Option<Collection>, ApiError> {
    use crate::schema::collections::dsl::{collections, name};

    collections
        .filter(name.eq(value))
        .first::<Collection>(conn)
        .optional()
        .map_err(ApiError::from)
}

pub fn lookup_class_by_collection_and_name_db(
    conn: &mut diesel::PgConnection,
    collection_id_value: i32,
    class_name: &str,
) -> Result<Option<HubuumClass>, ApiError> {
    use crate::schema::hubuumclass::dsl::{collection_id, hubuumclass, name};

    hubuumclass
        .filter(collection_id.eq(collection_id_value))
        .filter(name.eq(class_name))
        .first::<HubuumClass>(conn)
        .optional()
        .map_err(ApiError::from)
}

pub fn lookup_object_by_class_and_name_db(
    conn: &mut diesel::PgConnection,
    class_id_value: i32,
    object_name: &str,
) -> Result<Option<HubuumObject>, ApiError> {
    use crate::schema::hubuumobject::dsl::{hubuum_class_id, hubuumobject, name};

    hubuumobject
        .filter(hubuum_class_id.eq(class_id_value))
        .filter(name.eq(object_name))
        .first::<HubuumObject>(conn)
        .optional()
        .map_err(ApiError::from)
}

pub fn lookup_group_by_name_db(
    conn: &mut diesel::PgConnection,
    value: &str,
) -> Result<Option<Group>, ApiError> {
    use crate::schema::groups::dsl::{groupname, groups};

    groups
        .filter(groupname.eq(value))
        .first::<Group>(conn)
        .optional()
        .map_err(ApiError::from)
}

pub fn create_collection_db(
    conn: &mut diesel::PgConnection,
    input: &ImportCollectionInput,
) -> Result<Collection, ApiError> {
    use crate::schema::collections::dsl::collections;

    diesel::insert_into(collections)
        .values((
            crate::schema::collections::name.eq(&input.name),
            crate::schema::collections::description.eq(&input.description),
        ))
        .get_result::<Collection>(conn)
        .map_err(ApiError::from)
}

pub fn update_collection_db(
    conn: &mut diesel::PgConnection,
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
            .optional(),
        || collections.filter(id.eq(collection_id_value)).first(conn),
    )
    .map_err(ApiError::from)
}

pub fn create_class_db(
    conn: &mut diesel::PgConnection,
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
        .map_err(ApiError::from)
}

pub fn update_class_db(
    conn: &mut diesel::PgConnection,
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
            .optional(),
        || hubuumclass.filter(id.eq(class_id_value)).first(conn),
    )
    .map_err(ApiError::from)
}

pub fn create_object_db(
    conn: &mut diesel::PgConnection,
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

    diesel::insert_into(hubuumobject)
        .values(&new_object)
        .get_result::<HubuumObject>(conn)
        .map_err(ApiError::from)
}

pub fn update_object_db(
    conn: &mut diesel::PgConnection,
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

    crate::db::updated_or_current(
        diesel::update(hubuumobject.filter(id.eq(object_id_value)))
            .set(&update)
            .get_result::<HubuumObject>(conn)
            .optional(),
        || hubuumobject.filter(id.eq(object_id_value)).first(conn),
    )
    .map_err(ApiError::from)
}

pub fn create_class_relation_db(
    conn: &mut diesel::PgConnection,
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
        .map_err(ApiError::from)
}

fn normalize_template_alias_option(alias: Option<&str>) -> Result<Option<String>, ApiError> {
    alias.map(normalize_template_alias).transpose()
}

pub fn create_object_relation_db(
    conn: &mut diesel::PgConnection,
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
        .first::<HubuumClassRelation>(conn)?;

    let object_pair = normalize_pair(from_object.id, to_object.id);
    let new_relation = NewHubuumObjectRelation {
        from_hubuum_object_id: object_pair.0,
        to_hubuum_object_id: object_pair.1,
        class_relation_id: relation.id,
    };

    diesel::insert_into(hubuumobject_relation)
        .values(&new_relation)
        .get_result::<HubuumObjectRelation>(conn)
        .map_err(ApiError::from)
}

pub fn apply_permissions_db(
    conn: &mut diesel::PgConnection,
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
