use super::*;
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

        with_transaction(pool, |conn| -> Result<Namespace, ApiError> {
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
                has_read_template: true,
                has_create_template: true,
                has_update_template: true,
                has_delete_template: true,
            };

            diesel::insert_into(permissions)
                .values(&group_permission)
                .execute(conn)?;

            Ok(namespace)
        })
    }
}
