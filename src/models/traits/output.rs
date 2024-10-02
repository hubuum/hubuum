use std::collections::HashMap;
use tracing::warn;

use crate::db::DbPool;
use crate::errors::ApiError;
use crate::models::group::Group;
use crate::models::{
    GroupPermission, HubuumClass, HubuumClassExpanded, Namespace, NamespaceID, Permission,
    Permissions, PermissionsList,
};
use crate::traits::SelfAccessors;

pub trait FromTuple<T> {
    fn from_tuple(t: (Group, T)) -> Self;
}

pub trait ExpandNamespace<T> {
    async fn expand_namespace(&self, pool: &crate::db::DbPool) -> Result<T, ApiError>;
}

impl ExpandNamespace<HubuumClassExpanded> for HubuumClass {
    async fn expand_namespace(&self, pool: &DbPool) -> Result<HubuumClassExpanded, ApiError> {
        let namespace = NamespaceID(self.namespace_id).instance(pool).await?;

        Ok(HubuumClassExpanded {
            id: self.id,
            name: self.name.clone(),
            namespace,
            json_schema: self.json_schema.clone(),
            validate_schema: self.validate_schema,
            description: self.description.clone(),
            created_at: self.created_at,
            updated_at: self.updated_at,
        })
    }
}

pub trait ExpandNamespaceFromMap<T> {
    fn expand_namespace_from_map(&self, namespace_map: &HashMap<i32, Namespace>) -> T;
}

impl FromTuple<Permission> for GroupPermission {
    fn from_tuple(t: (Group, Permission)) -> Self {
        GroupPermission {
            group: t.0,
            permission: t.1,
        }
    }
}

impl ExpandNamespaceFromMap<Vec<HubuumClassExpanded>> for Vec<HubuumClass> {
    fn expand_namespace_from_map(
        &self,
        namespace_map: &HashMap<i32, Namespace>,
    ) -> Vec<HubuumClassExpanded> {
        self.iter()
            .map(|class| class.expand_namespace_from_map(namespace_map))
            .collect()
    }
}

impl ExpandNamespaceFromMap<HubuumClassExpanded> for HubuumClass {
    fn expand_namespace_from_map(
        &self,
        namespace_map: &HashMap<i32, Namespace>,
    ) -> HubuumClassExpanded {
        let namespace = match namespace_map.get(&self.namespace_id) {
            Some(namespace) => namespace.clone(),
            None => {
                warn!(
                    message = "Namespace mapping failed",
                    id = self.namespace_id,
                    class = self.name,
                    class_id = self.id
                );
                Namespace {
                    id: self.namespace_id,
                    name: "Unknown".to_string(),
                    description: "Unknown".to_string(),
                    created_at: chrono::NaiveDateTime::default(),
                    updated_at: chrono::NaiveDateTime::default(),
                }
            }
        };

        HubuumClassExpanded {
            id: self.id,
            name: self.name.clone(),
            namespace,
            json_schema: self.json_schema.clone(),
            validate_schema: self.validate_schema,
            description: self.description.clone(),
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }
}
