use diesel::{ExpressionMethods, QueryDsl};
use diesel_async::RunQueryDsl;
use serde_json::Value;

use super::*;
use crate::schema::{
    class_schema_history, class_schema_revisions, collections, collections_history,
    computed_field_definitions, computed_field_history, event_sinks, event_subscriptions,
    export_templates, export_templates_history, groups, hubuumclass, hubuumclass_history,
    hubuumclass_relation, hubuumclass_relation_history, hubuumobject, hubuumobject_history,
    hubuumobject_relation, hubuumobject_relation_history, identity_scopes, principals,
    query_usage_declarations, remote_targets, remote_targets_history, tasks,
};

#[cfg(all(test, feature = "integration-test-support"))]
mod tests;

#[async_trait]
impl LogicalIdentityStorage for PostgresStorage {
    async fn resolve_logical_identity(
        &self,
        target: StorageIdentityTarget,
    ) -> Result<StorageIdentityEvidence, StorageError> {
        use StorageIdentityHistoryKind as History;
        use StorageIdentityResourceKind as Resource;
        use StorageIdentityTarget as Target;

        self.runtime
            .with_connection(async move |connection| {
                // The closed target vocabulary selects generated Diesel tables
                // and their native key types. Values remain bound parameters.
                macro_rules! read {
                    ($table:ident, $key:ident, $id:expr) => {
                        $table::table
                            .filter($table::$key.eq($id))
                            .select($table::logical_identity)
                            .get_result::<Option<Value>>(connection)
                            .await?
                    };
                }
                let value = match target {
                    Target::Resource { kind, id } => match kind {
                        Resource::Collection => read!(collections, id, id.id()),
                        Resource::Class => read!(hubuumclass, id, id.id()),
                        Resource::Object => read!(hubuumobject, id, id.id()),
                        Resource::ClassRelation => read!(hubuumclass_relation, id, id.id()),
                        Resource::ObjectRelation => read!(hubuumobject_relation, id, id.id()),
                        Resource::ExportTemplate => read!(export_templates, id, id.id()),
                        Resource::RemoteTarget => read!(remote_targets, id, id.id()),
                        Resource::IdentityScope => read!(identity_scopes, id, id.id()),
                        Resource::Group => read!(groups, id, id.id()),
                        Resource::ComputedDefinition => {
                            read!(computed_field_definitions, id, id.id())
                        }
                        Resource::EventSink => read!(event_sinks, id, id.id()),
                        Resource::EventSubscription => read!(event_subscriptions, id, id.id()),
                        Resource::QueryUsageDeclaration => {
                            read!(query_usage_declarations, id, id.id())
                        }
                    },
                    Target::Principal(id) => read!(principals, id, id.id()),
                    Target::Task(id) => read!(tasks, id, id.id()),
                    Target::History { kind, id } => match kind {
                        History::Collection => read!(collections_history, history_id, id.id()),
                        History::Class => read!(hubuumclass_history, history_id, id.id()),
                        History::Object => read!(hubuumobject_history, history_id, id.id()),
                        History::ClassRelation => {
                            read!(hubuumclass_relation_history, history_id, id.id())
                        }
                        History::ObjectRelation => {
                            read!(hubuumobject_relation_history, history_id, id.id())
                        }
                        History::ExportTemplate => {
                            read!(export_templates_history, history_id, id.id())
                        }
                        History::RemoteTarget => read!(remote_targets_history, history_id, id.id()),
                        History::ComputedDefinitions => {
                            read!(computed_field_history, history_id, id.id())
                        }
                        History::ClassSchema => read!(class_schema_history, id, id.id()),
                    },
                    Target::SchemaRevision { class_id, revision } => {
                        class_schema_revisions::table
                            .find((class_id.id(), revision.get()))
                            .select(class_schema_revisions::logical_identity)
                            .get_result::<Option<Value>>(connection)
                            .await?
                    }
                };
                let value = value.ok_or_else(|| {
                    PostgresStorageError::database("Logical identity backfill is incomplete")
                })?;
                let evidence: StorageIdentityEvidence =
                    serde_json::from_value(value).map_err(|error| {
                        PostgresStorageError::invalid_persisted_value(
                            "logical identity evidence",
                            error,
                        )
                    })?;
                crate::validate_persisted("logical identity target", evidence.for_target(target))
            })
            .await
            .map_err(Into::into)
    }
}
