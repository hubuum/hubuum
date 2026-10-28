use diesel::QueryableByName;
use diesel::sql_types::{BigInt, Jsonb, Nullable};
use diesel_async::RunQueryDsl;

use super::*;

#[cfg(all(test, feature = "integration-test-support"))]
mod tests;

#[derive(QueryableByName)]
struct IdentityRow {
    #[diesel(sql_type = Nullable<Jsonb>)]
    logical_identity: Option<serde_json::Value>,
}

fn resource_table(kind: StorageIdentityResourceKind) -> &'static str {
    use StorageIdentityResourceKind as Kind;
    match kind {
        Kind::Collection => "collections",
        Kind::Class => "hubuumclass",
        Kind::Object => "hubuumobject",
        Kind::ClassRelation => "hubuumclass_relation",
        Kind::ObjectRelation => "hubuumobject_relation",
        Kind::ExportTemplate => "export_templates",
        Kind::RemoteTarget => "remote_targets",
        Kind::IdentityScope => "identity_scopes",
        Kind::Group => "groups",
        Kind::ComputedDefinition => "computed_field_definitions",
        Kind::EventSink => "event_sinks",
        Kind::EventSubscription => "event_subscriptions",
        Kind::QueryUsageDeclaration => "query_usage_declarations",
    }
}

#[async_trait]
impl LogicalIdentityStorage for PostgresStorage {
    async fn resolve_logical_identity(
        &self,
        target: StorageIdentityTarget,
    ) -> Result<StorageIdentityEvidence, StorageError> {
        use StorageIdentityHistoryKind as History;
        use StorageIdentityTarget as Target;
        let (table, key, id, revision) = match target {
            Target::Resource { kind, id } => (resource_table(kind), "id", i64::from(id.id()), 0),
            Target::Principal(id) => ("principals", "id", i64::from(id.id()), 0),
            Target::Task(id) => ("tasks", "id", i64::from(id.id()), 0),
            Target::History { kind, id } => {
                let table = match kind {
                    History::Collection => "collections_history",
                    History::Class => "hubuumclass_history",
                    History::Object => "hubuumobject_history",
                    History::ClassRelation => "hubuumclass_relation_history",
                    History::ObjectRelation => "hubuumobject_relation_history",
                    History::ExportTemplate => "export_templates_history",
                    History::RemoteTarget => "remote_targets_history",
                    History::ComputedDefinitions => "computed_field_history",
                    History::ClassSchema => "class_schema_history",
                };
                let key = if kind == History::ClassSchema {
                    "id"
                } else {
                    "history_id"
                };
                (table, key, id.id(), 0)
            }
            Target::SchemaRevision { class_id, revision } => (
                "class_schema_revisions",
                "class_id",
                i64::from(class_id.id()),
                revision.get(),
            ),
        };
        // Identifiers come only from the closed target vocabulary; values are
        // bound, and the evidence is read together in one database snapshot.
        let predicate = if revision == 0 {
            "$2 = 0"
        } else {
            "revision = $2"
        };
        let sql =
            format!("SELECT logical_identity FROM public.{table} WHERE {key} = $1 AND {predicate}");
        let evidence = self
            .runtime
            .with_connection(async move |connection| {
                let row = diesel::sql_query(sql)
                    .bind::<BigInt, _>(id)
                    .bind::<BigInt, _>(revision)
                    .get_result::<IdentityRow>(connection)
                    .await?;
                let value = row.logical_identity.ok_or_else(|| {
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
            .await?;
        Ok(evidence)
    }
}
