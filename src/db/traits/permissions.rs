use diesel::prelude::*;
use serde::Serialize;

use crate::db::{DbPool, with_connection, with_transaction};
use crate::errors::ApiError;
use crate::models::{
    NewPermission, Permission, PermissionFilter, Permissions, PermissionsList, UpdatePermission,
    User,
};
use crate::traits::{GroupAccessors, GroupMemberships, NamespaceAccessors, SelfAccessors};

use super::user::GroupIdsSubqueryBackend;

pub trait PermissionControllerBackend: Serialize + NamespaceAccessors {
    async fn user_can_all_from_backend<
        U: SelfAccessors<User> + GroupAccessors + GroupMemberships,
    >(
        &self,
        pool: &DbPool,
        user: U,
        permissions_requested: Vec<Permissions>,
    ) -> Result<bool, ApiError> {
        let lookup_table = crate::schema::permissions::dsl::permissions;
        let group_id_field = crate::schema::permissions::dsl::group_id;
        let namespace_id_field = crate::schema::permissions::dsl::namespace_id;

        if user.is_admin(pool).await? {
            return Ok(true);
        }

        let group_id_subquery = user.group_ids_subquery_from_backend();
        let mut base_query = lookup_table
            .into_boxed()
            .filter(namespace_id_field.eq(self.namespace_id(pool).await?))
            .filter(group_id_field.eq_any(group_id_subquery));

        for permission in permissions_requested {
            base_query = permission.create_boxed_filter(base_query, true);
        }

        let result: Option<Permission> =
            with_connection(pool, |conn| base_query.first::<Permission>(conn).optional())?;

        Ok(result.is_some())
    }

    async fn apply_permissions_from_backend(
        &self,
        pool: &DbPool,
        group_id_for_grant: i32,
        permission_list: PermissionsList<Permissions>,
        replace_existing: bool,
    ) -> Result<Permission, ApiError> {
        use crate::schema::permissions::dsl::*;

        let nid = self.namespace_id(pool).await?;

        with_transaction(pool, |conn| -> Result<Permission, ApiError> {
            let existing_entry = permissions
                .filter(namespace_id.eq(nid))
                .filter(group_id.eq(group_id_for_grant))
                .first::<Permission>(conn)
                .optional()?;

            match existing_entry {
                Some(_) => {
                    let mut update_perm = if replace_existing {
                        UpdatePermission {
                            has_read_namespace: Some(false),
                            has_update_namespace: Some(false),
                            has_delete_namespace: Some(false),
                            has_delegate_namespace: Some(false),
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
                        }
                    } else {
                        UpdatePermission::default()
                    };

                    for permission in permission_list.into_iter() {
                        match permission {
                            Permissions::ReadCollection => {
                                update_perm.has_read_namespace = Some(true);
                            }
                            Permissions::UpdateCollection => {
                                update_perm.has_update_namespace = Some(true);
                            }
                            Permissions::DeleteCollection => {
                                update_perm.has_delete_namespace = Some(true);
                            }
                            Permissions::DelegateCollection => {
                                update_perm.has_delegate_namespace = Some(true);
                            }
                            Permissions::CreateClass => {
                                update_perm.has_create_class = Some(true);
                            }
                            Permissions::ReadClass => {
                                update_perm.has_read_class = Some(true);
                            }
                            Permissions::UpdateClass => {
                                update_perm.has_update_class = Some(true);
                            }
                            Permissions::DeleteClass => {
                                update_perm.has_delete_class = Some(true);
                            }
                            Permissions::CreateObject => {
                                update_perm.has_create_object = Some(true);
                            }
                            Permissions::ReadObject => {
                                update_perm.has_read_object = Some(true);
                            }
                            Permissions::UpdateObject => {
                                update_perm.has_update_object = Some(true);
                            }
                            Permissions::DeleteObject => {
                                update_perm.has_delete_object = Some(true);
                            }
                            Permissions::CreateClassRelation => {
                                update_perm.has_create_class_relation = Some(true);
                            }
                            Permissions::ReadClassRelation => {
                                update_perm.has_read_class_relation = Some(true);
                            }
                            Permissions::UpdateClassRelation => {
                                update_perm.has_update_class_relation = Some(true);
                            }
                            Permissions::DeleteClassRelation => {
                                update_perm.has_delete_class_relation = Some(true);
                            }
                            Permissions::CreateObjectRelation => {
                                update_perm.has_create_object_relation = Some(true);
                            }
                            Permissions::ReadObjectRelation => {
                                update_perm.has_read_object_relation = Some(true);
                            }
                            Permissions::UpdateObjectRelation => {
                                update_perm.has_update_object_relation = Some(true);
                            }
                            Permissions::DeleteObjectRelation => {
                                update_perm.has_delete_object_relation = Some(true);
                            }
                        }
                    }

                    Ok(diesel::update(permissions)
                        .filter(namespace_id.eq(nid))
                        .filter(group_id.eq(group_id_for_grant))
                        .set(&update_perm)
                        .get_result(conn)?)
                }
                None => {
                    let new_entry = NewPermission {
                        namespace_id: nid,
                        group_id: group_id_for_grant,
                        has_read_namespace: permission_list.contains(&Permissions::ReadCollection),
                        has_update_namespace: permission_list
                            .contains(&Permissions::UpdateCollection),
                        has_delete_namespace: permission_list
                            .contains(&Permissions::DeleteCollection),
                        has_delegate_namespace: permission_list
                            .contains(&Permissions::DelegateCollection),
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
                        has_read_class_relation: permission_list
                            .contains(&Permissions::ReadClassRelation),
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
                    };

                    Ok(diesel::insert_into(permissions)
                        .values(&new_entry)
                        .get_result(conn)?)
                }
            }
        })
    }

    async fn revoke_permissions_from_backend(
        &self,
        pool: &DbPool,
        group_id_for_revoke: i32,
        permission_list: PermissionsList<Permissions>,
    ) -> Result<Permission, ApiError> {
        use crate::schema::permissions::dsl::*;

        let nid = self.namespace_id(pool).await?;

        with_transaction(pool, |conn| -> Result<Permission, ApiError> {
            permissions
                .filter(namespace_id.eq(nid))
                .filter(group_id.eq(group_id_for_revoke))
                .first::<Permission>(conn)?;

            let mut update_perm = UpdatePermission::default();
            for permission in permission_list.into_iter() {
                match permission {
                    Permissions::ReadCollection => {
                        update_perm.has_read_namespace = Some(false);
                    }
                    Permissions::UpdateCollection => {
                        update_perm.has_update_namespace = Some(false);
                    }
                    Permissions::DeleteCollection => {
                        update_perm.has_delete_namespace = Some(false);
                    }
                    Permissions::DelegateCollection => {
                        update_perm.has_delegate_namespace = Some(false);
                    }
                    Permissions::CreateClass => {
                        update_perm.has_create_class = Some(false);
                    }
                    Permissions::ReadClass => {
                        update_perm.has_read_class = Some(false);
                    }
                    Permissions::UpdateClass => {
                        update_perm.has_update_class = Some(false);
                    }
                    Permissions::DeleteClass => {
                        update_perm.has_delete_class = Some(false);
                    }
                    Permissions::CreateObject => {
                        update_perm.has_create_object = Some(false);
                    }
                    Permissions::ReadObject => {
                        update_perm.has_read_object = Some(false);
                    }
                    Permissions::UpdateObject => {
                        update_perm.has_update_object = Some(false);
                    }
                    Permissions::DeleteObject => {
                        update_perm.has_delete_object = Some(false);
                    }
                    Permissions::CreateClassRelation => {
                        update_perm.has_create_class_relation = Some(false);
                    }
                    Permissions::ReadClassRelation => {
                        update_perm.has_read_class_relation = Some(false);
                    }
                    Permissions::UpdateClassRelation => {
                        update_perm.has_update_class_relation = Some(false);
                    }
                    Permissions::DeleteClassRelation => {
                        update_perm.has_delete_class_relation = Some(false);
                    }
                    Permissions::CreateObjectRelation => {
                        update_perm.has_create_object_relation = Some(false);
                    }
                    Permissions::ReadObjectRelation => {
                        update_perm.has_read_object_relation = Some(false);
                    }
                    Permissions::UpdateObjectRelation => {
                        update_perm.has_update_object_relation = Some(false);
                    }
                    Permissions::DeleteObjectRelation => {
                        update_perm.has_delete_object_relation = Some(false);
                    }
                }
            }

            Ok(diesel::update(permissions)
                .filter(namespace_id.eq(nid))
                .filter(group_id.eq(group_id_for_revoke))
                .set(&update_perm)
                .get_result(conn)?)
        })
    }

    async fn revoke_all_from_backend(
        &self,
        pool: &DbPool,
        group_id_for_revoke: i32,
    ) -> Result<(), ApiError> {
        use crate::schema::permissions::dsl::*;

        let namespace_id_for_revoke = self.namespace_id(pool).await?;
        with_connection(pool, |conn| {
            diesel::delete(permissions)
                .filter(namespace_id.eq(namespace_id_for_revoke))
                .filter(group_id.eq(group_id_for_revoke))
                .execute(conn)
        })?;

        Ok(())
    }
}

impl<T: ?Sized> PermissionControllerBackend for T where T: Serialize + NamespaceAccessors {}
