use super::*;
use crate::events::{Action, EntityType, EventContext, NewEvent, emit_event};

fn namespace_snapshot(namespace: &Namespace) -> serde_json::Value {
    serde_json::json!({
        "id": namespace.id,
        "name": namespace.name,
        "description": namespace.description,
        "created_at": namespace.created_at,
        "updated_at": namespace.updated_at,
    })
}

fn namespace_event(
    namespace: &Namespace,
    action: Action,
    context: &EventContext,
    summary: impl Into<String>,
) -> Result<NewEvent, ApiError> {
    Ok(
        NewEvent::new(EntityType::Namespace, action, context.actor_kind(), summary)?
            .with_context(context)
            .with_entity_id(namespace.id)
            .with_entity_name(namespace.name.clone())
            .with_namespace_id(namespace.id),
    )
}

pub trait DeleteNamespaceRecord {
    async fn delete_namespace_record_without_events(&self, pool: &DbPool) -> Result<(), ApiError>;

    async fn delete_namespace_record(
        &self,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<(), ApiError> {
        let _ = context;
        self.delete_namespace_record_without_events(pool).await
    }
}

impl DeleteNamespaceRecord for Namespace {
    async fn delete_namespace_record_without_events(&self, pool: &DbPool) -> Result<(), ApiError> {
        use crate::schema::namespaces::dsl::{id, namespaces};

        with_connection(pool, |conn| {
            diesel::delete(namespaces.filter(id.eq(self.id))).execute(conn)
        })?;
        Ok(())
    }

    async fn delete_namespace_record(
        &self,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<(), ApiError> {
        let Some(context) = context else {
            return self.delete_namespace_record_without_events(pool).await;
        };

        use crate::schema::namespaces::dsl::{id, namespaces};

        with_transaction(pool, |conn| -> Result<(), ApiError> {
            diesel::delete(namespaces.filter(id.eq(self.id))).execute(conn)?;
            let event = namespace_event(
                self,
                Action::Deleted,
                context,
                format!("Namespace '{}' deleted", self.name),
            )?
            .with_before(namespace_snapshot(self));
            emit_event(conn, &event)?;
            Ok(())
        })
    }
}

impl DeleteNamespaceRecord for NamespaceID {
    async fn delete_namespace_record_without_events(&self, pool: &DbPool) -> Result<(), ApiError> {
        use crate::schema::namespaces::dsl::{id, namespaces};

        with_connection(pool, |conn| {
            diesel::delete(namespaces.filter(id.eq(self.id()))).execute(conn)
        })?;
        Ok(())
    }

    async fn delete_namespace_record(
        &self,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<(), ApiError> {
        let Some(context) = context else {
            return self.delete_namespace_record_without_events(pool).await;
        };

        use crate::schema::namespaces::dsl::{id, namespaces};

        with_transaction(pool, |conn| -> Result<(), ApiError> {
            let namespace = namespaces
                .filter(id.eq(self.id()))
                .first::<Namespace>(conn)?;
            diesel::delete(namespaces.filter(id.eq(namespace.id))).execute(conn)?;
            let event = namespace_event(
                &namespace,
                Action::Deleted,
                context,
                format!("Namespace '{}' deleted", namespace.name),
            )?
            .with_before(namespace_snapshot(&namespace));
            emit_event(conn, &event)?;
            Ok(())
        })
    }
}

pub trait UpdateNamespaceRecord {
    async fn update_namespace_record_without_events(
        &self,
        pool: &DbPool,
        namespace_id: i32,
    ) -> Result<Namespace, ApiError>;

    async fn update_namespace_record(
        &self,
        pool: &DbPool,
        namespace_id: i32,
        context: Option<&EventContext>,
    ) -> Result<Namespace, ApiError> {
        let _ = context;
        self.update_namespace_record_without_events(pool, namespace_id)
            .await
    }
}

impl UpdateNamespaceRecord for UpdateNamespace {
    async fn update_namespace_record_without_events(
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

    async fn update_namespace_record(
        &self,
        pool: &DbPool,
        namespace_id: i32,
        context: Option<&EventContext>,
    ) -> Result<Namespace, ApiError> {
        let Some(context) = context else {
            return self
                .update_namespace_record_without_events(pool, namespace_id)
                .await;
        };

        use crate::schema::namespaces::dsl::{id, namespaces};

        with_transaction(pool, |conn| -> Result<Namespace, ApiError> {
            let before = namespaces
                .filter(id.eq(namespace_id))
                .first::<Namespace>(conn)?;
            let updated = diesel::update(namespaces.filter(id.eq(namespace_id)))
                .set(self)
                .get_result::<Namespace>(conn)?;
            let event = namespace_event(
                &updated,
                Action::Updated,
                context,
                format!("Namespace '{}' updated", updated.name),
            )?
            .with_before(namespace_snapshot(&before))
            .with_after(namespace_snapshot(&updated));
            emit_event(conn, &event)?;
            Ok(updated)
        })
    }
}

pub trait SaveNamespaceWithAssigneeRecord {
    async fn save_namespace_with_assignee_record_without_events(
        &self,
        pool: &DbPool,
    ) -> Result<Namespace, ApiError>;

    async fn save_namespace_with_assignee_record(
        &self,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<Namespace, ApiError> {
        let _ = context;
        self.save_namespace_with_assignee_record_without_events(pool)
            .await
    }
}

impl SaveNamespaceWithAssigneeRecord for NewNamespaceWithAssignee {
    async fn save_namespace_with_assignee_record_without_events(
        &self,
        pool: &DbPool,
    ) -> Result<Namespace, ApiError> {
        let new_namespace = NewNamespace {
            name: self.name.clone(),
            description: self.description.clone(),
        };

        new_namespace
            .save_namespace_for_group_record_without_events(pool, self.group_id)
            .await
    }

    async fn save_namespace_with_assignee_record(
        &self,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<Namespace, ApiError> {
        let new_namespace = NewNamespace {
            name: self.name.clone(),
            description: self.description.clone(),
        };

        new_namespace
            .save_namespace_for_group_record(pool, self.group_id, context)
            .await
    }
}

pub trait SaveNamespaceForGroupRecord {
    async fn save_namespace_for_group_record_without_events(
        &self,
        pool: &DbPool,
        group_id: i32,
    ) -> Result<Namespace, ApiError>;

    async fn save_namespace_for_group_record(
        &self,
        pool: &DbPool,
        group_id: i32,
        context: Option<&EventContext>,
    ) -> Result<Namespace, ApiError> {
        let _ = context;
        self.save_namespace_for_group_record_without_events(pool, group_id)
            .await
    }
}

impl SaveNamespaceForGroupRecord for NewNamespace {
    async fn save_namespace_for_group_record_without_events(
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
                has_read_remote_target: true,
                has_create_remote_target: true,
                has_update_remote_target: true,
                has_delete_remote_target: true,
                has_execute_remote_target: true,
                has_read_audit: true,
                has_manage_event_subscription: true,
            };

            diesel::insert_into(permissions)
                .values(&group_permission)
                .execute(conn)?;

            Ok(namespace)
        })
    }

    async fn save_namespace_for_group_record(
        &self,
        pool: &DbPool,
        group_id: i32,
        context: Option<&EventContext>,
    ) -> Result<Namespace, ApiError> {
        let Some(context) = context else {
            return self
                .save_namespace_for_group_record_without_events(pool, group_id)
                .await;
        };

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
                has_read_remote_target: true,
                has_create_remote_target: true,
                has_update_remote_target: true,
                has_delete_remote_target: true,
                has_execute_remote_target: true,
                has_read_audit: true,
                has_manage_event_subscription: true,
            };

            diesel::insert_into(permissions)
                .values(&group_permission)
                .execute(conn)?;

            let event = namespace_event(
                &namespace,
                Action::Created,
                context,
                format!("Namespace '{}' created", namespace.name),
            )?
            .with_after(namespace_snapshot(&namespace))
            .with_metadata(serde_json::json!({ "assignee_group_id": group_id }));
            emit_event(conn, &event)?;

            Ok(namespace)
        })
    }
}
