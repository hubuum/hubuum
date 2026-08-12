//! Backend persistence for export templates.
//!
//! All Diesel/Postgres query construction for `export_templates` lives here so the model layer
//! (`crate::models::export_template`) stays thin and free of backend details, mirroring the other
//! entities under `crate::storage::postgres::operations`. Instance-scoped CRUD is exposed as self-methods via the record
//! traits below (matching `LoadClassRecord` and friends); collection, search, cross-table, and
//! aggregate queries — which have no single owning instance — stay free functions, as elsewhere in
//! this module. The model owns the domain<->row conversions and all validation.

use crate::storage::postgres::prelude::*;

use crate::api::etag::RevisionOwner;
use crate::errors::ApiError;
use crate::events::{Action, EntityType, EventContext, NewEvent};
use crate::models::ResourceRevision;
use crate::models::export_template::ExportTemplateID;
use crate::models::search::{FilterField, QueryOptions};
use crate::pagination::{
    CursorPaginated, CursorSqlField, CursorSqlMapping, CursorSqlType, CursorValue,
};
use crate::storage::postgres::operations::event_record::emit_event;
use crate::storage::postgres::{with_connection, with_transaction};
use crate::{date_search, numeric_search, revision_search, string_search};

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = crate::schema::export_templates)]
pub(crate) struct ExportTemplateRow {
    pub(crate) id: i32,
    pub(crate) collection_id: i32,
    pub(crate) name: String,
    pub(crate) description: String,
    pub(crate) content_type: String,
    pub(crate) template: String,
    pub(crate) kind: String,
    pub(crate) scope_kind: Option<String>,
    pub(crate) class_id: Option<i32>,
    pub(crate) default_query: Option<String>,
    pub(crate) include: Option<serde_json::Value>,
    pub(crate) relation_context: Option<serde_json::Value>,
    pub(crate) default_missing_data_policy: Option<String>,
    pub(crate) default_limits: Option<serde_json::Value>,
    pub(crate) created_at: chrono::NaiveDateTime,
    pub(crate) updated_at: chrono::NaiveDateTime,
    pub(crate) revision: ResourceRevision,
}

impl ExportTemplateRow {
    pub(crate) fn id(&self) -> i32 {
        self.id
    }

    pub(crate) fn collection_id(&self) -> i32 {
        self.collection_id
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn revision(&self) -> ResourceRevision {
        self.revision
    }

    pub(crate) fn audit_snapshot(&self) -> serde_json::Value {
        serde_json::json!({
            "id": self.id,
            "collection_id": self.collection_id,
            "name": self.name,
            "description": self.description,
            "content_type": self.content_type,
            "template": self.template,
            "kind": self.kind,
            "scope_kind": self.scope_kind,
            "class_id": self.class_id,
            "default_query": self.default_query,
            "include": self.include,
            "relation_context": self.relation_context,
            "default_missing_data_policy": self.default_missing_data_policy,
            "default_limits": self.default_limits,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "revision": self.revision,
        })
    }
}

impl CursorPaginated for ExportTemplateRow {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(
            field,
            FilterField::Id
                | FilterField::Name
                | FilterField::Description
                | FilterField::Collections
                | FilterField::CollectionId
                | FilterField::CreatedAt
                | FilterField::UpdatedAt
                | FilterField::Revision
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        Ok(match field {
            FilterField::Id => CursorValue::Integer(self.id.into()),
            FilterField::Name => CursorValue::String(self.name.clone()),
            FilterField::Description => CursorValue::String(self.description.clone()),
            FilterField::Collections | FilterField::CollectionId => {
                CursorValue::Integer(self.collection_id.into())
            }
            FilterField::CreatedAt => CursorValue::DateTime(self.created_at),
            FilterField::UpdatedAt => CursorValue::DateTime(self.updated_at),
            FilterField::Revision => CursorValue::Integer(self.revision.get()),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{field}' is not orderable for export templates"
                )));
            }
        })
    }

    fn default_sort() -> Vec<crate::models::search::SortParam> {
        vec![crate::models::search::SortParam {
            field: FilterField::Id,
            descending: false,
        }]
    }

    fn tie_breaker_sort() -> Vec<crate::models::search::SortParam> {
        Self::default_sort()
    }
}

impl CursorSqlMapping for ExportTemplateRow {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::Id => CursorSqlField {
                column: "export_templates.id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Name => CursorSqlField {
                column: "export_templates.name",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::Description => CursorSqlField {
                column: "export_templates.description",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::Collections | FilterField::CollectionId => CursorSqlField {
                column: "export_templates.collection_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::CreatedAt => CursorSqlField {
                column: "export_templates.created_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::UpdatedAt => CursorSqlField {
                column: "export_templates.updated_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::Revision => CursorSqlField {
                column: "export_templates.revision",
                sql_type: CursorSqlType::BigInt,
                nullable: false,
            },
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{field}' is not orderable for export templates"
                )));
            }
        })
    }
}

#[derive(Debug, Clone, Insertable)]
#[diesel(table_name = crate::schema::export_templates)]
pub(crate) struct NewExportTemplateRow {
    pub(crate) collection_id: i32,
    pub(crate) name: String,
    pub(crate) description: String,
    pub(crate) content_type: String,
    pub(crate) template: String,
    pub(crate) kind: String,
    pub(crate) scope_kind: Option<String>,
    pub(crate) class_id: Option<i32>,
    pub(crate) default_query: Option<String>,
    pub(crate) include: Option<serde_json::Value>,
    pub(crate) relation_context: Option<serde_json::Value>,
    pub(crate) default_missing_data_policy: Option<String>,
    pub(crate) default_limits: Option<serde_json::Value>,
}

#[derive(Debug, Clone, AsChangeset)]
#[diesel(table_name = crate::schema::export_templates)]
pub(crate) struct UpdateExportTemplateRow {
    pub(crate) collection_id: Option<i32>,
    pub(crate) name: Option<String>,
    pub(crate) description: Option<String>,
    pub(crate) template: Option<String>,
    pub(crate) kind: Option<String>,
    pub(crate) scope_kind: Option<Option<String>>,
    pub(crate) class_id: Option<Option<i32>>,
    pub(crate) default_query: Option<Option<String>>,
    pub(crate) include: Option<Option<serde_json::Value>>,
    pub(crate) relation_context: Option<Option<serde_json::Value>>,
    pub(crate) default_missing_data_policy: Option<Option<String>>,
    pub(crate) default_limits: Option<Option<serde_json::Value>>,
}

impl UpdateExportTemplateRow {
    pub(crate) fn has_changes(&self, current: &ExportTemplateRow) -> bool {
        self.collection_id
            .is_some_and(|value| value != current.collection_id)
            || self
                .name
                .as_ref()
                .is_some_and(|value| value != &current.name)
            || self
                .description
                .as_ref()
                .is_some_and(|value| value != &current.description)
            || self
                .template
                .as_ref()
                .is_some_and(|value| value != &current.template)
            || self
                .kind
                .as_ref()
                .is_some_and(|value| value != &current.kind)
            || self
                .scope_kind
                .as_ref()
                .is_some_and(|value| value != &current.scope_kind)
            || self
                .class_id
                .as_ref()
                .is_some_and(|value| value != &current.class_id)
            || self
                .default_query
                .as_ref()
                .is_some_and(|value| value != &current.default_query)
            || self
                .include
                .as_ref()
                .is_some_and(|value| value != &current.include)
            || self
                .relation_context
                .as_ref()
                .is_some_and(|value| value != &current.relation_context)
            || self
                .default_missing_data_policy
                .as_ref()
                .is_some_and(|value| value != &current.default_missing_data_policy)
            || self
                .default_limits
                .as_ref()
                .is_some_and(|value| value != &current.default_limits)
    }
}

fn export_template_event(
    row: &ExportTemplateRow,
    action: Action,
    context: &EventContext,
    summary: impl Into<String>,
) -> Result<NewEvent, ApiError> {
    Ok(NewEvent::new(
        EntityType::ExportTemplate,
        action,
        context.actor_kind(),
        summary,
    )?
    .with_context(context)
    .with_entity_id(row.id())
    .with_entity_name(row.name().to_string())
    .with_collection_id(row.collection_id()))
}

/// Load the export-template row identified by this id.
pub(crate) trait LoadExportTemplateRecord {
    async fn load_export_template_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<ExportTemplateRow, ApiError>;
}

impl LoadExportTemplateRecord for ExportTemplateID {
    async fn load_export_template_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<ExportTemplateRow, ApiError> {
        use crate::schema::export_templates::dsl::{export_templates, id};

        with_connection(pool, async |conn| {
            export_templates
                .filter(id.eq(self.id()))
                .first::<ExportTemplateRow>(conn)
                .await
        })
        .await
    }
}

/// Insert this new export-template row and return the persisted row.
pub(crate) trait SaveExportTemplateRecord {
    async fn save_export_template_record_without_events(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<ExportTemplateRow, ApiError>;

    async fn save_export_template_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        context: Option<&EventContext>,
    ) -> Result<ExportTemplateRow, ApiError> {
        let _ = context;
        self.save_export_template_record_without_events(pool).await
    }
}

impl SaveExportTemplateRecord for NewExportTemplateRow {
    async fn save_export_template_record_without_events(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<ExportTemplateRow, ApiError> {
        use crate::schema::export_templates::dsl::export_templates;

        with_connection(pool, async |conn| {
            diesel::insert_into(export_templates)
                .values(self)
                .get_result::<ExportTemplateRow>(conn)
                .await
        })
        .await
    }

    async fn save_export_template_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        context: Option<&EventContext>,
    ) -> Result<ExportTemplateRow, ApiError> {
        let Some(context) = context else {
            return self.save_export_template_record_without_events(pool).await;
        };

        use crate::schema::export_templates::dsl::export_templates;

        with_transaction(pool, async |conn| -> Result<ExportTemplateRow, ApiError> {
            let row = diesel::insert_into(export_templates)
                .values(self)
                .get_result::<ExportTemplateRow>(conn)
                .await?;
            let event = export_template_event(
                &row,
                Action::Created,
                context,
                format!("Export template '{}' created", row.name()),
            )?
            .with_after(row.audit_snapshot());
            emit_event(conn, &event).await?;
            Ok(row)
        })
        .await
    }
}

/// Apply this changeset to the export-template row with the given id and return the updated row.
pub(crate) trait UpdateExportTemplateRecord {
    async fn update_export_template_record_without_events(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        template_id: i32,
    ) -> Result<ExportTemplateRow, ApiError>;

    async fn update_export_template_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        template_id: i32,
        context: Option<&EventContext>,
    ) -> Result<ExportTemplateRow, ApiError> {
        let _ = context;
        self.update_export_template_record_without_events(pool, template_id)
            .await
    }
}

impl UpdateExportTemplateRecord for UpdateExportTemplateRow {
    async fn update_export_template_record_without_events(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        template_id: i32,
    ) -> Result<ExportTemplateRow, ApiError> {
        use crate::schema::export_templates::dsl::{export_templates, id};

        with_connection(pool, async |conn| {
            crate::storage::postgres::updated_or_current(
                diesel::update(export_templates.filter(id.eq(template_id)))
                    .set(self)
                    .get_result::<ExportTemplateRow>(conn)
                    .await
                    .optional(),
                async || {
                    export_templates
                        .filter(id.eq(template_id))
                        .first(conn)
                        .await
                },
            )
            .await
        })
        .await
    }

    async fn update_export_template_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        template_id: i32,
        context: Option<&EventContext>,
    ) -> Result<ExportTemplateRow, ApiError> {
        let Some(context) = context else {
            return self
                .update_export_template_record_without_events(pool, template_id)
                .await;
        };

        use crate::schema::export_templates::dsl::{export_templates, id};

        with_transaction(pool, async |conn| -> Result<ExportTemplateRow, ApiError> {
            let before = export_templates
                .filter(id.eq(template_id))
                .for_update()
                .first::<ExportTemplateRow>(conn)
                .await?;
            crate::storage::postgres::assert_locked_revision_precondition(
                conn,
                &RevisionOwner::ExportTemplate.key(before.id()),
                before.revision(),
            )
            .await?;
            if !self.has_changes(&before) {
                return Ok(before);
            }
            let after = diesel::update(export_templates.filter(id.eq(template_id)))
                .set(self)
                .get_result::<ExportTemplateRow>(conn)
                .await?;
            let event = export_template_event(
                &after,
                Action::Updated,
                context,
                format!("Export template '{}' updated", after.name()),
            )?
            .with_before(before.audit_snapshot())
            .with_after(after.audit_snapshot());
            emit_event(conn, &event).await?;
            Ok(after)
        })
        .await
    }
}

/// Delete the export-template row identified by this id.
pub(crate) trait DeleteExportTemplateRecord {
    async fn delete_export_template_record_without_events(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<(), ApiError>;

    async fn delete_export_template_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        context: Option<&EventContext>,
    ) -> Result<(), ApiError> {
        let _ = context;
        self.delete_export_template_record_without_events(pool)
            .await
    }
}

impl DeleteExportTemplateRecord for ExportTemplateID {
    async fn delete_export_template_record_without_events(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<(), ApiError> {
        use crate::schema::export_templates::dsl::{export_templates, id};

        with_connection(pool, async |conn| {
            diesel::delete(export_templates.filter(id.eq(self.id())))
                .execute(conn)
                .await
        })
        .await?;

        Ok(())
    }

    async fn delete_export_template_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        context: Option<&EventContext>,
    ) -> Result<(), ApiError> {
        let Some(context) = context else {
            return self
                .delete_export_template_record_without_events(pool)
                .await;
        };

        use crate::schema::export_templates::dsl::{export_templates, id};

        with_transaction(pool, async |conn| -> Result<(), ApiError> {
            let before = export_templates
                .filter(id.eq(self.id()))
                .for_update()
                .first::<ExportTemplateRow>(conn)
                .await?;
            diesel::delete(export_templates.filter(id.eq(self.id())))
                .execute(conn)
                .await?;
            let event = export_template_event(
                &before,
                Action::Deleted,
                context,
                format!("Export template '{}' deleted", before.name()),
            )?
            .with_before(before.audit_snapshot());
            emit_event(conn, &event).await?;
            Ok(())
        })
        .await
    }
}

/// Load all export-template rows in a collection, optionally excluding one template id.
pub(crate) async fn load_rows_in_collection(
    pool: &crate::storage::postgres::PostgresPool,
    target_collection_id: i32,
    exclude_template_id: Option<i32>,
) -> Result<Vec<ExportTemplateRow>, ApiError> {
    use crate::schema::export_templates::dsl::{collection_id, export_templates, id};

    with_connection(pool, async |conn| {
        let mut query = export_templates
            .into_boxed()
            .filter(collection_id.eq(target_collection_id));
        if let Some(exclude_template_id) = exclude_template_id {
            query = query.filter(id.ne(exclude_template_id));
        }
        query.load::<ExportTemplateRow>(conn).await
    })
    .await
}

/// The collection a class belongs to, or `None` if the class does not exist.
pub(crate) async fn class_collection_id(
    pool: &crate::storage::postgres::PostgresPool,
    target_class_id: i32,
) -> Result<Option<i32>, ApiError> {
    use crate::schema::hubuumclass::dsl::{collection_id, hubuumclass, id};

    with_connection(pool, async |conn| {
        hubuumclass
            .filter(id.eq(target_class_id))
            .select(collection_id)
            .first::<i32>(conn)
            .await
            .optional()
    })
    .await
}

/// Build the filtered (but unsorted, unpaginated) query for listing export templates within the
/// collections the caller may see.
fn build_list_query<'a>(
    allowed_collection_ids: Option<&'a [i32]>,
    query_options: &'a QueryOptions,
) -> Result<crate::schema::export_templates::BoxedQuery<'a, diesel::pg::Pg>, ApiError> {
    use crate::schema::export_templates::dsl::{
        class_id, collection_id, created_at, description, export_templates, id, kind, name,
        revision, updated_at,
    };

    let mut query = export_templates.into_boxed();
    if let Some(allowed_collection_ids) = allowed_collection_ids {
        query = query.filter(collection_id.eq_any(allowed_collection_ids));
    }

    for param in &query_options.filters {
        let operator = param.operator.clone();
        match param.field {
            FilterField::Id => numeric_search!(query, param, operator, id),
            FilterField::Name => string_search!(query, param, operator, name),
            FilterField::Description => string_search!(query, param, operator, description),
            FilterField::Collections | FilterField::CollectionId => {
                numeric_search!(query, param, operator, collection_id)
            }
            FilterField::Kind => string_search!(query, param, operator, kind),
            FilterField::ClassId => numeric_search!(query, param, operator, class_id),
            FilterField::CreatedAt => date_search!(query, param, operator, created_at),
            FilterField::UpdatedAt => date_search!(query, param, operator, updated_at),
            FilterField::Revision => revision_search!(query, param, operator, revision),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' isn't searchable (or does not exist) for export templates",
                    param.field
                )));
            }
        }
    }

    Ok(query)
}

/// List export-template rows (sorted/paginated per `query_options`) together with the total count
/// matching the filters, scoped to the collections the caller may see.
pub(crate) async fn list_rows_with_total_count(
    pool: &crate::storage::postgres::PostgresPool,
    allowed_collection_ids: &[i32],
    query_options: &QueryOptions,
) -> Result<(Vec<ExportTemplateRow>, i64), ApiError> {
    list_rows_with_optional_collection_scope(pool, Some(allowed_collection_ids), query_options)
        .await
}

pub(crate) async fn list_all_rows_with_total_count(
    pool: &crate::storage::postgres::PostgresPool,
    query_options: &QueryOptions,
) -> Result<(Vec<ExportTemplateRow>, i64), ApiError> {
    list_rows_with_optional_collection_scope(pool, None, query_options).await
}

async fn list_rows_with_optional_collection_scope(
    pool: &crate::storage::postgres::PostgresPool,
    allowed_collection_ids: Option<&[i32]>,
    query_options: &QueryOptions,
) -> Result<(Vec<ExportTemplateRow>, i64), ApiError> {
    let query = build_list_query(allowed_collection_ids, query_options)?;
    let total_count = crate::pagination::exact_count_or_skipped(query_options, async || {
        with_connection(pool, async |conn| {
            query.count().get_result::<i64>(conn).await
        })
        .await
    })
    .await?;

    let mut query = build_list_query(allowed_collection_ids, query_options)?;
    crate::apply_query_options!(query, query_options, ExportTemplateRow);
    let rows = with_connection(pool, async |conn| {
        query.load::<ExportTemplateRow>(conn).await
    })
    .await?;

    Ok((rows, total_count))
}
