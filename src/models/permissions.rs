use diesel::prelude::*;
use std::{fmt, fmt::Display, slice};

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{errors::ApiError, schema::permissions};

use super::search::ParsedQueryParam;

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone, Copy, ToSchema)]
pub enum Permissions {
    ReadCollection,
    UpdateCollection,
    DeleteCollection,
    DelegateCollection,
    CreateClass,
    ReadClass,
    UpdateClass,
    DeleteClass,
    CreateObject,
    ReadObject,
    UpdateObject,
    DeleteObject,
    CreateClassRelation,
    ReadClassRelation,
    UpdateClassRelation,
    DeleteClassRelation,
    CreateObjectRelation,
    ReadObjectRelation,
    UpdateObjectRelation,
    DeleteObjectRelation,
    ReadTemplate,
    CreateTemplate,
    UpdateTemplate,
    DeleteTemplate,
    ReadRemoteTarget,
    CreateRemoteTarget,
    UpdateRemoteTarget,
    DeleteRemoteTarget,
    ExecuteRemoteTarget,
    ReadAudit,
}

impl Permissions {
    /// ## Convert a string to a Permissions enum.
    ///
    /// ### Arguments
    ///
    /// * `s` - A string slice to convert to a Permissions enum.
    ///
    /// ### Returns
    ///
    /// * `Result<Permissions, ApiError>` - The Permissions enum if the string is a valid permission.
    pub fn from_string(s: &str) -> Result<Permissions, ApiError> {
        match s {
            "ReadCollection" => Ok(Permissions::ReadCollection),
            "UpdateCollection" => Ok(Permissions::UpdateCollection),
            "DeleteCollection" => Ok(Permissions::DeleteCollection),
            "DelegateCollection" => Ok(Permissions::DelegateCollection),
            "CreateClass" => Ok(Permissions::CreateClass),
            "ReadClass" => Ok(Permissions::ReadClass),
            "UpdateClass" => Ok(Permissions::UpdateClass),
            "DeleteClass" => Ok(Permissions::DeleteClass),
            "CreateObject" => Ok(Permissions::CreateObject),
            "ReadObject" => Ok(Permissions::ReadObject),
            "UpdateObject" => Ok(Permissions::UpdateObject),
            "DeleteObject" => Ok(Permissions::DeleteObject),
            "CreateClassRelation" => Ok(Permissions::CreateClassRelation),
            "ReadClassRelation" => Ok(Permissions::ReadClassRelation),
            "UpdateClassRelation" => Ok(Permissions::UpdateClassRelation),
            "DeleteClassRelation" => Ok(Permissions::DeleteClassRelation),
            "CreateObjectRelation" => Ok(Permissions::CreateObjectRelation),
            "ReadObjectRelation" => Ok(Permissions::ReadObjectRelation),
            "UpdateObjectRelation" => Ok(Permissions::UpdateObjectRelation),
            "DeleteObjectRelation" => Ok(Permissions::DeleteObjectRelation),
            "ReadTemplate" => Ok(Permissions::ReadTemplate),
            "CreateTemplate" => Ok(Permissions::CreateTemplate),
            "UpdateTemplate" => Ok(Permissions::UpdateTemplate),
            "DeleteTemplate" => Ok(Permissions::DeleteTemplate),
            "ReadRemoteTarget" => Ok(Permissions::ReadRemoteTarget),
            "CreateRemoteTarget" => Ok(Permissions::CreateRemoteTarget),
            "UpdateRemoteTarget" => Ok(Permissions::UpdateRemoteTarget),
            "DeleteRemoteTarget" => Ok(Permissions::DeleteRemoteTarget),
            "ExecuteRemoteTarget" => Ok(Permissions::ExecuteRemoteTarget),
            "ReadAudit" => Ok(Permissions::ReadAudit),
            _ => Err(ApiError::BadRequest(format!("Invalid permission: '{s}'"))),
        }
    }
}
impl Display for Permissions {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Permissions::ReadCollection => "ReadCollection",
                Permissions::UpdateCollection => "UpdateCollection",
                Permissions::DeleteCollection => "DeleteCollection",
                Permissions::DelegateCollection => "DelegateCollection",
                Permissions::CreateClass => "CreateClass",
                Permissions::ReadClass => "ReadClass",
                Permissions::UpdateClass => "UpdateClass",
                Permissions::DeleteClass => "DeleteClass",
                Permissions::CreateObject => "CreateObject",
                Permissions::ReadObject => "ReadObject",
                Permissions::UpdateObject => "UpdateObject",
                Permissions::DeleteObject => "DeleteObject",
                Permissions::CreateClassRelation => "CreateClassRelation",
                Permissions::ReadClassRelation => "ReadClassRelation",
                Permissions::UpdateClassRelation => "UpdateClassRelation",
                Permissions::DeleteClassRelation => "DeleteClassRelation",
                Permissions::CreateObjectRelation => "CreateObjectRelation",
                Permissions::ReadObjectRelation => "ReadObjectRelation",
                Permissions::UpdateObjectRelation => "UpdateObjectRelation",
                Permissions::DeleteObjectRelation => "DeleteObjectRelation",
                Permissions::ReadTemplate => "ReadTemplate",
                Permissions::CreateTemplate => "CreateTemplate",
                Permissions::UpdateTemplate => "UpdateTemplate",
                Permissions::DeleteTemplate => "DeleteTemplate",
                Permissions::ReadRemoteTarget => "ReadRemoteTarget",
                Permissions::CreateRemoteTarget => "CreateRemoteTarget",
                Permissions::UpdateRemoteTarget => "UpdateRemoteTarget",
                Permissions::DeleteRemoteTarget => "DeleteRemoteTarget",
                Permissions::ExecuteRemoteTarget => "ExecuteRemoteTarget",
                Permissions::ReadAudit => "ReadAudit",
            }
        )
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PermissionsList<T: Serialize + PartialEq + Clone>(Vec<T>);

impl<T: Serialize + PartialEq + Clone> PermissionsList<T> {
    // Constructor that accepts a generic IntoIterator so we can create a PermissionsList from any
    // collection of items that can be converted into an iterator.
    pub fn new<I: IntoIterator<Item = T>>(items: I) -> Self {
        PermissionsList(items.into_iter().collect())
    }

    pub fn from_query_params(
        query_params: Vec<ParsedQueryParam>,
    ) -> Result<PermissionsList<Permissions>, ApiError> {
        use crate::models::search::QueryParamsExt;
        query_params.permissions()
    }

    pub fn contains(&self, item: &T) -> bool {
        self.0.contains(item)
    }

    pub fn ensure_contains(&mut self, items: &[T]) {
        for item in items {
            if !self.contains(item) {
                self.0.push(item.clone());
            }
        }
    }
    // Method to get an iterator over references to the items in the Vec<T>
    pub fn iter(&self) -> slice::Iter<'_, T> {
        self.0.iter()
    }
}

impl<T: Serialize + PartialEq + Clone + Display> Display for PermissionsList<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let formatted = self
            .0
            .iter()
            .map(|item| item.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        write!(f, "{formatted}")
    }
}

impl<'a, T: Serialize + PartialEq + Clone> IntoIterator for &'a PermissionsList<T> {
    type Item = &'a T;
    type IntoIter = slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

pub trait PermissionFilter<'a, Q> {
    /// ## Create a boxed filter to check if a permission is set to true or false.
    ///
    /// ### Arguments
    ///
    /// * `query` - The query to add the filter to.
    /// * `target` - The value to check for.
    ///
    /// ### Returns
    ///
    /// * `BoxedQuery` - The query with the filter added.
    ///
    /// ## Example
    ///
    /// ```ignore
    /// use crate::models::Permissions;
    /// use crate::models::PermissionFilter;
    /// use crate::schema::permissions::dsl::{permissions, group_id, namespace_id};
    ///
    /// let permissions_list = vec![
    ///  Permissions::ReadCollection,
    ///  Permissions::UpdateCollection
    /// ];
    /// let mut base_query = permissions
    ///   .into_boxed()
    ///   .filter(namespace_id.eq_any(vec![1, 2, 3]))
    ///
    /// for perm in permissions_list {
    ///   base_query = perm.create_boxed_filter(base_query, true);
    /// }
    /// ```
    fn create_boxed_filter(self, query: Q, target: bool) -> Q;
}

impl<'a> PermissionFilter<'a, permissions::BoxedQuery<'a, diesel::pg::Pg>> for Permissions {
    fn create_boxed_filter(
        self,
        query: permissions::BoxedQuery<diesel::pg::Pg>,
        target: bool,
    ) -> permissions::BoxedQuery<diesel::pg::Pg> {
        match self {
            Permissions::ReadCollection => query.filter(permissions::has_read_namespace.eq(target)),
            Permissions::UpdateCollection => {
                query.filter(permissions::has_update_namespace.eq(target))
            }
            Permissions::DeleteCollection => {
                query.filter(permissions::has_delete_namespace.eq(target))
            }
            Permissions::DelegateCollection => {
                query.filter(permissions::has_delegate_namespace.eq(target))
            }
            Permissions::CreateClass => query.filter(permissions::has_create_class.eq(target)),
            Permissions::ReadClass => query.filter(permissions::has_read_class.eq(target)),
            Permissions::UpdateClass => query.filter(permissions::has_update_class.eq(target)),
            Permissions::DeleteClass => query.filter(permissions::has_delete_class.eq(target)),
            Permissions::CreateObject => query.filter(permissions::has_create_object.eq(target)),
            Permissions::ReadObject => query.filter(permissions::has_read_object.eq(target)),
            Permissions::UpdateObject => query.filter(permissions::has_update_object.eq(target)),
            Permissions::DeleteObject => query.filter(permissions::has_delete_object.eq(target)),
            Permissions::CreateClassRelation => {
                query.filter(permissions::has_create_class_relation.eq(target))
            }
            Permissions::ReadClassRelation => {
                query.filter(permissions::has_read_class_relation.eq(target))
            }
            Permissions::UpdateClassRelation => {
                query.filter(permissions::has_update_class_relation.eq(target))
            }
            Permissions::DeleteClassRelation => {
                query.filter(permissions::has_delete_class_relation.eq(target))
            }
            Permissions::CreateObjectRelation => {
                query.filter(permissions::has_create_object_relation.eq(target))
            }
            Permissions::ReadObjectRelation => {
                query.filter(permissions::has_read_object_relation.eq(target))
            }
            Permissions::UpdateObjectRelation => {
                query.filter(permissions::has_update_object_relation.eq(target))
            }
            Permissions::DeleteObjectRelation => {
                query.filter(permissions::has_delete_object_relation.eq(target))
            }
            Permissions::ReadTemplate => query.filter(permissions::has_read_template.eq(target)),
            Permissions::CreateTemplate => {
                query.filter(permissions::has_create_template.eq(target))
            }
            Permissions::UpdateTemplate => {
                query.filter(permissions::has_update_template.eq(target))
            }
            Permissions::DeleteTemplate => {
                query.filter(permissions::has_delete_template.eq(target))
            }
            Permissions::ReadRemoteTarget => {
                query.filter(permissions::has_read_remote_target.eq(target))
            }
            Permissions::CreateRemoteTarget => {
                query.filter(permissions::has_create_remote_target.eq(target))
            }
            Permissions::UpdateRemoteTarget => {
                query.filter(permissions::has_update_remote_target.eq(target))
            }
            Permissions::DeleteRemoteTarget => {
                query.filter(permissions::has_delete_remote_target.eq(target))
            }
            Permissions::ExecuteRemoteTarget => {
                query.filter(permissions::has_execute_remote_target.eq(target))
            }
            Permissions::ReadAudit => query.filter(permissions::has_read_audit.eq(target)),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Queryable, Selectable, Clone, Copy, ToSchema)]
#[diesel(table_name = permissions)]
pub struct Permission {
    pub id: i32,
    pub namespace_id: i32,
    pub group_id: i32,
    pub has_read_namespace: bool,
    pub has_update_namespace: bool,
    pub has_delete_namespace: bool,
    pub has_delegate_namespace: bool,
    pub has_create_class: bool,
    pub has_read_class: bool,
    pub has_update_class: bool,
    pub has_delete_class: bool,
    pub has_create_object: bool,
    pub has_read_object: bool,
    pub has_update_object: bool,
    pub has_delete_object: bool,
    pub has_create_class_relation: bool,
    pub has_read_class_relation: bool,
    pub has_update_class_relation: bool,
    pub has_delete_class_relation: bool,
    pub has_create_object_relation: bool,
    pub has_read_object_relation: bool,
    pub has_update_object_relation: bool,
    pub has_delete_object_relation: bool,
    pub has_read_template: bool,
    pub has_create_template: bool,
    pub has_update_template: bool,
    pub has_delete_template: bool,
    pub has_read_remote_target: bool,
    pub has_create_remote_target: bool,
    pub has_update_remote_target: bool,
    pub has_delete_remote_target: bool,
    pub has_execute_remote_target: bool,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub has_read_audit: bool,
}

impl Permission {
    /// The set of permissions this row grants — the `has_*` flags that are `true`,
    /// mapped to their [`Permissions`] variant.
    pub fn granted(&self) -> Vec<Permissions> {
        [
            (self.has_read_namespace, Permissions::ReadCollection),
            (self.has_update_namespace, Permissions::UpdateCollection),
            (self.has_delete_namespace, Permissions::DeleteCollection),
            (self.has_delegate_namespace, Permissions::DelegateCollection),
            (self.has_create_class, Permissions::CreateClass),
            (self.has_read_class, Permissions::ReadClass),
            (self.has_update_class, Permissions::UpdateClass),
            (self.has_delete_class, Permissions::DeleteClass),
            (self.has_create_object, Permissions::CreateObject),
            (self.has_read_object, Permissions::ReadObject),
            (self.has_update_object, Permissions::UpdateObject),
            (self.has_delete_object, Permissions::DeleteObject),
            (
                self.has_create_class_relation,
                Permissions::CreateClassRelation,
            ),
            (self.has_read_class_relation, Permissions::ReadClassRelation),
            (
                self.has_update_class_relation,
                Permissions::UpdateClassRelation,
            ),
            (
                self.has_delete_class_relation,
                Permissions::DeleteClassRelation,
            ),
            (
                self.has_create_object_relation,
                Permissions::CreateObjectRelation,
            ),
            (
                self.has_read_object_relation,
                Permissions::ReadObjectRelation,
            ),
            (
                self.has_update_object_relation,
                Permissions::UpdateObjectRelation,
            ),
            (
                self.has_delete_object_relation,
                Permissions::DeleteObjectRelation,
            ),
            (self.has_create_template, Permissions::CreateTemplate),
            (self.has_read_template, Permissions::ReadTemplate),
            (self.has_update_template, Permissions::UpdateTemplate),
            (self.has_delete_template, Permissions::DeleteTemplate),
            (
                self.has_create_remote_target,
                Permissions::CreateRemoteTarget,
            ),
            (self.has_read_remote_target, Permissions::ReadRemoteTarget),
            (
                self.has_update_remote_target,
                Permissions::UpdateRemoteTarget,
            ),
            (
                self.has_delete_remote_target,
                Permissions::DeleteRemoteTarget,
            ),
            (
                self.has_execute_remote_target,
                Permissions::ExecuteRemoteTarget,
            ),
            (self.has_read_audit, Permissions::ReadAudit),
        ]
        .into_iter()
        .filter_map(|(set, permission)| set.then_some(permission))
        .collect()
    }
}

// Insertable permission models.
#[derive(Debug, Serialize, Deserialize, Insertable)]
#[diesel(table_name = permissions)]
pub struct NewPermission {
    pub namespace_id: i32,
    pub group_id: i32,
    pub has_read_namespace: bool,
    pub has_update_namespace: bool,
    pub has_delete_namespace: bool,
    pub has_delegate_namespace: bool,
    pub has_create_class: bool,
    pub has_read_class: bool,
    pub has_update_class: bool,
    pub has_delete_class: bool,
    pub has_create_object: bool,
    pub has_read_object: bool,
    pub has_update_object: bool,
    pub has_delete_object: bool,
    pub has_create_class_relation: bool,
    pub has_read_class_relation: bool,
    pub has_update_class_relation: bool,
    pub has_delete_class_relation: bool,
    pub has_create_object_relation: bool,
    pub has_read_object_relation: bool,
    pub has_update_object_relation: bool,
    pub has_delete_object_relation: bool,
    pub has_read_template: bool,
    pub has_create_template: bool,
    pub has_update_template: bool,
    pub has_delete_template: bool,
    pub has_read_remote_target: bool,
    pub has_create_remote_target: bool,
    pub has_update_remote_target: bool,
    pub has_delete_remote_target: bool,
    pub has_execute_remote_target: bool,
    pub has_read_audit: bool,
}

#[derive(Debug, Serialize, Deserialize, AsChangeset, Default)]
#[diesel(table_name = permissions)]
pub struct UpdatePermission {
    pub has_read_namespace: Option<bool>,
    pub has_update_namespace: Option<bool>,
    pub has_delete_namespace: Option<bool>,
    pub has_delegate_namespace: Option<bool>,
    pub has_create_class: Option<bool>,
    pub has_read_class: Option<bool>,
    pub has_update_class: Option<bool>,
    pub has_delete_class: Option<bool>,
    pub has_create_object: Option<bool>,
    pub has_read_object: Option<bool>,
    pub has_update_object: Option<bool>,
    pub has_delete_object: Option<bool>,
    pub has_create_class_relation: Option<bool>,
    pub has_read_class_relation: Option<bool>,
    pub has_update_class_relation: Option<bool>,
    pub has_delete_class_relation: Option<bool>,
    pub has_create_object_relation: Option<bool>,
    pub has_read_object_relation: Option<bool>,
    pub has_update_object_relation: Option<bool>,
    pub has_delete_object_relation: Option<bool>,
    pub has_read_template: Option<bool>,
    pub has_create_template: Option<bool>,
    pub has_update_template: Option<bool>,
    pub has_delete_template: Option<bool>,
    pub has_read_remote_target: Option<bool>,
    pub has_create_remote_target: Option<bool>,
    pub has_update_remote_target: Option<bool>,
    pub has_delete_remote_target: Option<bool>,
    pub has_execute_remote_target: Option<bool>,
    pub has_read_audit: Option<bool>,
}

#[cfg(test)]
mod tests {
    use diesel::prelude::*;

    use super::{PermissionFilter, Permissions};
    use crate::schema::permissions::dsl::permissions;

    #[test]
    fn template_permissions_parse_and_display_round_trip() {
        let fixtures = [
            ("ReadTemplate", Permissions::ReadTemplate),
            ("CreateTemplate", Permissions::CreateTemplate),
            ("UpdateTemplate", Permissions::UpdateTemplate),
            ("DeleteTemplate", Permissions::DeleteTemplate),
        ];

        for (name, permission) in fixtures {
            assert_eq!(Permissions::from_string(name).unwrap(), permission);
            assert_eq!(permission.to_string(), name);
        }
    }

    #[test]
    fn template_permissions_filter_map_to_expected_columns() {
        let fixtures = [
            (Permissions::ReadTemplate, "has_read_template"),
            (Permissions::CreateTemplate, "has_create_template"),
            (Permissions::UpdateTemplate, "has_update_template"),
            (Permissions::DeleteTemplate, "has_delete_template"),
        ];

        for (permission, expected_column) in fixtures {
            let base_query = permissions.into_boxed();
            let filtered = permission.create_boxed_filter(base_query, true);
            let sql = diesel::debug_query::<diesel::pg::Pg, _>(&filtered).to_string();
            assert!(
                sql.contains(expected_column),
                "Expected SQL to contain '{expected_column}', got: {sql}"
            );
        }
    }
}
