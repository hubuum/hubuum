use diesel::prelude::*;
use tracing::{debug, trace};

use crate::db::traits::GetNamespace;
use crate::db::{with_connection, with_transaction, DbPool};
use crate::errors::ApiError;
use crate::models::{HubuumClassRelation, NewHubuumObjectRelation};
use crate::models::{HubuumObjectRelation, NewHubuumClassRelation};
use crate::models::{
    HubuumObjectRelationID, Namespace, NamespaceID, NewNamespace, NewNamespaceWithAssignee,
    NewPermission, UpdateNamespace,
};
use crate::traits::{ClassAccessors, ObjectAccessors, SelfAccessors};

impl GetNamespace<(Namespace, Namespace)> for HubuumClassRelation {
    async fn namespace_from_backend(
        &self,
        pool: &DbPool,
    ) -> Result<(Namespace, Namespace), ApiError> {
        use crate::schema::hubuumclass::dsl::{
            hubuumclass, id as class_id, namespace_id as class_namespace_id,
        };
        use crate::schema::namespaces::dsl::{id as namespace_id, namespaces};

        let (from_id, to_id) = self.class_id(pool).await?;

        let namespace_list = with_connection(pool, |conn| {
            hubuumclass
                .filter(class_id.eq_any(&[from_id, to_id]))
                .inner_join(namespaces.on(namespace_id.eq(class_namespace_id)))
                .select(namespaces::all_columns())
                .load::<Namespace>(conn)
        })?;

        if from_id == to_id && namespace_list.len() == 1 {
            trace!("Found same namespace for class relation, returning same namespace twice");
            return Ok((namespace_list[0].clone(), namespace_list[0].clone()));
        } else if namespace_list.len() != 2 {
            debug!(
                "Could not find two namespaces for class relation: {} and {}, found {:?}",
                from_id, to_id, namespace_list
            );
            return Err(ApiError::NotFound(
                format!("Could not find namespaces ({from_id} and {to_id}) for class relation",)
                    .to_string(),
            ));
        }
        Ok((namespace_list[0].clone(), namespace_list[1].clone()))
    }
}

impl GetNamespace<(Namespace, Namespace)> for NewHubuumClassRelation {
    async fn namespace_from_backend(
        &self,
        pool: &DbPool,
    ) -> Result<(Namespace, Namespace), ApiError> {
        use crate::schema::hubuumclass::dsl::{
            hubuumclass, id as class_id, namespace_id as class_namespace_id,
        };
        use crate::schema::namespaces::dsl::{id as namespace_id, namespaces};

        let (from_id, to_id) = self.class_id(pool).await?;

        let namespace_list = with_connection(pool, |conn| {
            hubuumclass
                .filter(class_id.eq_any(&[from_id, to_id]))
                .inner_join(namespaces.on(namespace_id.eq(class_namespace_id)))
                .select(namespaces::all_columns())
                .load::<Namespace>(conn)
        })?;

        if namespace_list.len() == 1 {
            trace!("Found same namespace for class relation, returning same namespace twice");
            return Ok((namespace_list[0].clone(), namespace_list[0].clone()));
        } else if namespace_list.len() != 2 {
            debug!(
                "Could not find two namespaces for class relation: {} and {}, found {:?}",
                from_id, to_id, namespace_list
            );
            return Err(ApiError::NotFound(
                format!("Could not find namespaces ({from_id} and {to_id}) for class relation",)
                    .to_string(),
            ));
        }
        Ok((namespace_list[0].clone(), namespace_list[1].clone()))
    }
}

impl GetNamespace<(Namespace, Namespace)> for HubuumObjectRelation {
    async fn namespace_from_backend(
        &self,
        pool: &DbPool,
    ) -> Result<(Namespace, Namespace), ApiError> {
        use crate::schema::hubuumobject::dsl::{
            hubuumobject, id as object_id, namespace_id as object_namespace_id,
        };
        use crate::schema::namespaces::dsl::{id as namespace_id, namespaces};

        let (from_id, to_id) = self.object_id(pool).await?;

        let namespace_list = with_connection(pool, |conn| {
            hubuumobject
                .filter(object_id.eq_any(&[from_id, to_id]))
                .inner_join(namespaces.on(namespace_id.eq(object_namespace_id)))
                .select(namespaces::all_columns())
                .load::<Namespace>(conn)
        })?;

        if namespace_list.len() == 1 {
            trace!("Found same namespace for object relation, returning same namespace twice");
            return Ok((namespace_list[0].clone(), namespace_list[0].clone()));
        } else if namespace_list.len() != 2 {
            debug!(
                "Could not find two namespaces for object relation: {} and {}, found {:?}",
                from_id, to_id, namespace_list
            );
            return Err(ApiError::NotFound(
                format!("Could not find namespaces ({from_id} and {to_id}) for object relation",)
                    .to_string(),
            ));
        }
        Ok((namespace_list[0].clone(), namespace_list[1].clone()))
    }
}

impl GetNamespace<(Namespace, Namespace)> for NewHubuumObjectRelation {
    async fn namespace_from_backend(
        &self,
        pool: &DbPool,
    ) -> Result<(Namespace, Namespace), ApiError> {
        use crate::schema::hubuumobject::dsl::{
            hubuumobject, id as object_id, namespace_id as object_namespace_id,
        };
        use crate::schema::namespaces::dsl::{id as namespace_id, namespaces};

        let (from_id, to_id) = self.object_id(pool).await?;

        let namespace_list = with_connection(pool, |conn| {
            hubuumobject
                .filter(object_id.eq_any(&[from_id, to_id]))
                .inner_join(namespaces.on(namespace_id.eq(object_namespace_id)))
                .select(namespaces::all_columns())
                .load::<Namespace>(conn)
        })?;

        if namespace_list.len() == 1 {
            trace!("Found same namespace for object relation, returning same namespace twice");
            return Ok((namespace_list[0].clone(), namespace_list[0].clone()));
        } else if namespace_list.len() != 2 {
            debug!(
                "Could not find two namespaces for object relation: {} and {}, found {:?}",
                from_id, to_id, namespace_list
            );
            return Err(ApiError::NotFound(
                format!("Could not find namespaces ({from_id} and {to_id}) for object relation",)
                    .to_string(),
            ));
        }
        Ok((namespace_list[0].clone(), namespace_list[1].clone()))
    }
}

impl GetNamespace<(Namespace, Namespace)> for HubuumObjectRelationID {
    async fn namespace_from_backend(
        &self,
        pool: &DbPool,
    ) -> Result<(Namespace, Namespace), ApiError> {
        use crate::schema::hubuumobject::dsl::{
            hubuumobject, id as object_id, namespace_id as object_namespace_id,
        };
        use crate::schema::namespaces::dsl::{id as namespace_id, namespaces};

        let (from_id, to_id) = self.object_id(pool).await?;

        let namespace_list = with_connection(pool, |conn| {
            hubuumobject
                .filter(object_id.eq_any(&[from_id, to_id]))
                .inner_join(namespaces.on(namespace_id.eq(object_namespace_id)))
                .select(namespaces::all_columns())
                .load::<Namespace>(conn)
        })?;

        if namespace_list.len() == 1 {
            trace!("Found same namespace for object relation, returning same namespace twice");
            return Ok((namespace_list[0].clone(), namespace_list[0].clone()));
        } else if namespace_list.len() != 2 {
            debug!(
                "Could not find two namespaces for object relation: {} and {}, found {:?}",
                from_id, to_id, namespace_list
            );
            return Err(ApiError::NotFound(
                format!("Could not find namespaces ({from_id} and {to_id}) for object relation",)
                    .to_string(),
            ));
        }
        Ok((namespace_list[0].clone(), namespace_list[1].clone()))
    }
}

impl<S> GetNamespace for S
where
    S: SelfAccessors<Namespace>,
{
    async fn namespace_from_backend(&self, pool: &DbPool) -> Result<Namespace, ApiError> {
        use crate::schema::namespaces::dsl::{id, namespaces};

        let namespace = with_connection(pool, |conn| {
            namespaces.filter(id.eq(self.id())).first::<Namespace>(conn)
        })?;

        Ok(namespace)
    }
}

pub trait DeleteNamespaceRecord {
    async fn delete_namespace_record(&self, pool: &DbPool) -> Result<(), ApiError>;
}

impl DeleteNamespaceRecord for Namespace {
    async fn delete_namespace_record(&self, pool: &DbPool) -> Result<(), ApiError> {
        use crate::schema::namespaces::dsl::{id, namespaces};

        with_connection(pool, |conn| {
            diesel::delete(namespaces.filter(id.eq(self.id))).execute(conn)
        })?;
        Ok(())
    }
}

impl DeleteNamespaceRecord for NamespaceID {
    async fn delete_namespace_record(&self, pool: &DbPool) -> Result<(), ApiError> {
        use crate::schema::namespaces::dsl::{id, namespaces};

        with_connection(pool, |conn| {
            diesel::delete(namespaces.filter(id.eq(self.0))).execute(conn)
        })?;
        Ok(())
    }
}

pub trait UpdateNamespaceRecord {
    async fn update_namespace_record(
        &self,
        pool: &DbPool,
        namespace_id: i32,
    ) -> Result<Namespace, ApiError>;
}

impl UpdateNamespaceRecord for UpdateNamespace {
    async fn update_namespace_record(
        &self,
        pool: &DbPool,
        namespace_id: i32,
    ) -> Result<Namespace, ApiError> {
        use crate::schema::namespaces::dsl::{id, namespaces};

        with_connection(pool, |conn| {
            diesel::update(namespaces)
                .filter(id.eq(namespace_id))
                .set(self)
                .get_result::<Namespace>(conn)
        })
    }
}

pub trait SaveNamespaceWithAssigneeRecord {
    async fn save_namespace_with_assignee_record(
        &self,
        pool: &DbPool,
    ) -> Result<Namespace, ApiError>;
}

impl SaveNamespaceWithAssigneeRecord for NewNamespaceWithAssignee {
    async fn save_namespace_with_assignee_record(
        &self,
        pool: &DbPool,
    ) -> Result<Namespace, ApiError> {
        let new_namespace = NewNamespace {
            name: self.name.clone(),
            description: self.description.clone(),
        };

        new_namespace
            .save_namespace_for_group_record(pool, self.group_id)
            .await
    }
}

pub trait SaveNamespaceForGroupRecord {
    async fn save_namespace_for_group_record(
        &self,
        pool: &DbPool,
        group_id: i32,
    ) -> Result<Namespace, ApiError>;
}

impl SaveNamespaceForGroupRecord for NewNamespace {
    async fn save_namespace_for_group_record(
        &self,
        pool: &DbPool,
        group_id: i32,
    ) -> Result<Namespace, ApiError> {
        use crate::schema::namespaces::dsl::namespaces;
        use crate::schema::permissions::dsl::permissions;

        with_transaction(pool, |conn| {
            let namespace = diesel::insert_into(namespaces)
                .values(self)
                .get_result::<Namespace>(conn)?;

            let group_permission = NewPermission {
                namespace_id: namespace.id,
                group_id,
                has_read_namespace: true,
                has_update_namespace: true,
                has_delete_namespace: true,
                has_delegate_namespace: true,
                has_create_class: true,
                has_read_class: true,
                has_update_class: true,
                has_delete_class: true,
                has_create_object: true,
                has_read_object: true,
                has_update_object: true,
                has_delete_object: true,
                has_create_class_relation: true,
                has_read_class_relation: true,
                has_update_class_relation: true,
                has_delete_class_relation: true,
                has_create_object_relation: true,
                has_read_object_relation: true,
                has_update_object_relation: true,
                has_delete_object_relation: true,
            };

            diesel::insert_into(permissions)
                .values(&group_permission)
                .execute(conn)?;

            Ok(namespace)
        })
    }
}
