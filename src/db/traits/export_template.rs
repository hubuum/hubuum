//! Backend persistence for export templates.
//!
//! All Diesel/Postgres query construction for `export_templates` lives here so the model layer
//! (`crate::models::export_template`) stays thin and free of backend details, mirroring the other
//! entities under `src/db/traits/`. Instance-scoped CRUD is exposed as self-methods via the record
//! traits below (matching `LoadClassRecord` and friends); collection, search, cross-table, and
//! aggregate queries — which have no single owning instance — stay free functions, as elsewhere in
//! this module. The model owns the domain<->row conversions and all validation.

use diesel::prelude::*;

use crate::db::{DbPool, with_connection, with_transaction};
use crate::errors::ApiError;
use crate::events::{Action, EntityType, EventContext, NewEvent, emit_event};
use crate::models::collection::CollectionID;
use crate::models::export_template::{
    ExportTemplate, ExportTemplateID, ExportTemplateRow, NewExportTemplateRow,
    UpdateExportTemplateRow,
};
use crate::models::search::{FilterField, QueryOptions};
use crate::{date_search, numeric_search, string_search};

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
        pool: &DbPool,
    ) -> Result<ExportTemplateRow, ApiError>;
}

impl LoadExportTemplateRecord for ExportTemplateID {
    async fn load_export_template_record(
        &self,
        pool: &DbPool,
    ) -> Result<ExportTemplateRow, ApiError> {
        use crate::schema::export_templates::dsl::{export_templates, id};

        with_connection(pool, |conn| {
            export_templates
                .filter(id.eq(self.id()))
                .first::<ExportTemplateRow>(conn)
        })
    }
}

/// Insert this new export-template row and return the persisted row.
pub(crate) trait SaveExportTemplateRecord {
    async fn save_export_template_record_without_events(
        &self,
        pool: &DbPool,
    ) -> Result<ExportTemplateRow, ApiError>;

    async fn save_export_template_record(
        &self,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<ExportTemplateRow, ApiError> {
        let _ = context;
        self.save_export_template_record_without_events(pool).await
    }
}

impl SaveExportTemplateRecord for NewExportTemplateRow {
    async fn save_export_template_record_without_events(
        &self,
        pool: &DbPool,
    ) -> Result<ExportTemplateRow, ApiError> {
        use crate::schema::export_templates::dsl::export_templates;

        with_connection(pool, |conn| {
            diesel::insert_into(export_templates)
                .values(self)
                .get_result::<ExportTemplateRow>(conn)
        })
    }

    async fn save_export_template_record(
        &self,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<ExportTemplateRow, ApiError> {
        let Some(context) = context else {
            return self.save_export_template_record_without_events(pool).await;
        };

        use crate::schema::export_templates::dsl::export_templates;

        with_transaction(pool, |conn| -> Result<ExportTemplateRow, ApiError> {
            let row = diesel::insert_into(export_templates)
                .values(self)
                .get_result::<ExportTemplateRow>(conn)?;
            let event = export_template_event(
                &row,
                Action::Created,
                context,
                format!("Export template '{}' created", row.name()),
            )?
            .with_after(row.audit_snapshot());
            emit_event(conn, &event)?;
            Ok(row)
        })
    }
}

/// Apply this changeset to the export-template row with the given id and return the updated row.
pub(crate) trait UpdateExportTemplateRecord {
    async fn update_export_template_record_without_events(
        &self,
        pool: &DbPool,
        template_id: i32,
    ) -> Result<ExportTemplateRow, ApiError>;

    async fn update_export_template_record(
        &self,
        pool: &DbPool,
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
        pool: &DbPool,
        template_id: i32,
    ) -> Result<ExportTemplateRow, ApiError> {
        use crate::schema::export_templates::dsl::{export_templates, id};

        with_connection(pool, |conn| {
            crate::db::updated_or_current(
                diesel::update(export_templates.filter(id.eq(template_id)))
                    .set(self)
                    .get_result::<ExportTemplateRow>(conn)
                    .optional(),
                || export_templates.filter(id.eq(template_id)).first(conn),
            )
        })
    }

    async fn update_export_template_record(
        &self,
        pool: &DbPool,
        template_id: i32,
        context: Option<&EventContext>,
    ) -> Result<ExportTemplateRow, ApiError> {
        let Some(context) = context else {
            return self
                .update_export_template_record_without_events(pool, template_id)
                .await;
        };

        use crate::schema::export_templates::dsl::{export_templates, id};

        with_transaction(pool, |conn| -> Result<ExportTemplateRow, ApiError> {
            let before = export_templates
                .filter(id.eq(template_id))
                .first::<ExportTemplateRow>(conn)?;
            let after = diesel::update(export_templates.filter(id.eq(template_id)))
                .set(self)
                .get_result::<ExportTemplateRow>(conn)?;
            let event = export_template_event(
                &after,
                Action::Updated,
                context,
                format!("Export template '{}' updated", after.name()),
            )?
            .with_before(before.audit_snapshot())
            .with_after(after.audit_snapshot());
            emit_event(conn, &event)?;
            Ok(after)
        })
    }
}

/// Delete the export-template row identified by this id.
pub(crate) trait DeleteExportTemplateRecord {
    async fn delete_export_template_record_without_events(
        &self,
        pool: &DbPool,
    ) -> Result<(), ApiError>;

    async fn delete_export_template_record(
        &self,
        pool: &DbPool,
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
        pool: &DbPool,
    ) -> Result<(), ApiError> {
        use crate::schema::export_templates::dsl::{export_templates, id};

        with_connection(pool, |conn| {
            diesel::delete(export_templates.filter(id.eq(self.id()))).execute(conn)
        })?;

        Ok(())
    }

    async fn delete_export_template_record(
        &self,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<(), ApiError> {
        let Some(context) = context else {
            return self
                .delete_export_template_record_without_events(pool)
                .await;
        };

        use crate::schema::export_templates::dsl::{export_templates, id};

        with_transaction(pool, |conn| -> Result<(), ApiError> {
            let before = export_templates
                .filter(id.eq(self.id()))
                .first::<ExportTemplateRow>(conn)?;
            diesel::delete(export_templates.filter(id.eq(self.id()))).execute(conn)?;
            let event = export_template_event(
                &before,
                Action::Deleted,
                context,
                format!("Export template '{}' deleted", before.name()),
            )?
            .with_before(before.audit_snapshot());
            emit_event(conn, &event)?;
            Ok(())
        })
    }
}

/// Look up the collection id of the export template identified by this id.
pub(crate) trait ExportTemplateCollectionLookup {
    async fn lookup_export_template_collection_id(
        &self,
        pool: &DbPool,
    ) -> Result<CollectionID, ApiError>;
}

impl ExportTemplateCollectionLookup for ExportTemplateID {
    async fn lookup_export_template_collection_id(
        &self,
        pool: &DbPool,
    ) -> Result<CollectionID, ApiError> {
        use crate::schema::export_templates::dsl::{collection_id, export_templates, id};

        let raw = with_connection(pool, |conn| {
            export_templates
                .filter(id.eq(self.id()))
                .select(collection_id)
                .first::<i32>(conn)
        })?;
        CollectionID::new(raw)
    }
}

/// Load all export-template rows in a collection, optionally excluding one template id.
pub(crate) async fn load_rows_in_collection(
    pool: &DbPool,
    target_collection_id: i32,
    exclude_template_id: Option<i32>,
) -> Result<Vec<ExportTemplateRow>, ApiError> {
    use crate::schema::export_templates::dsl::{collection_id, export_templates, id};

    with_connection(pool, |conn| {
        let mut query = export_templates
            .into_boxed()
            .filter(collection_id.eq(target_collection_id));
        if let Some(exclude_template_id) = exclude_template_id {
            query = query.filter(id.ne(exclude_template_id));
        }
        query.load::<ExportTemplateRow>(conn)
    })
}

/// Load every export-template row.
pub(crate) async fn load_all_rows(pool: &DbPool) -> Result<Vec<ExportTemplateRow>, ApiError> {
    use crate::schema::export_templates::dsl::export_templates;

    with_connection(pool, |conn| {
        export_templates.load::<ExportTemplateRow>(conn)
    })
}

/// Whether a template with `target_name` already exists in the collection, optionally ignoring one
/// template id (used so an update does not conflict with itself).
pub(crate) async fn name_conflict_exists(
    pool: &DbPool,
    target_collection_id: i32,
    target_name: &str,
    exclude_template_id: Option<i32>,
) -> Result<bool, ApiError> {
    use crate::schema::export_templates::dsl::{collection_id, export_templates, id, name};

    let existing = with_connection(pool, |conn| {
        let mut query = export_templates
            .into_boxed()
            .filter(collection_id.eq(target_collection_id))
            .filter(name.eq(target_name));
        if let Some(exclude_template_id) = exclude_template_id {
            query = query.filter(id.ne(exclude_template_id));
        }
        query.first::<ExportTemplateRow>(conn).optional()
    })?;

    Ok(existing.is_some())
}

/// The collection a class belongs to, or `None` if the class does not exist.
pub(crate) async fn class_collection_id(
    pool: &DbPool,
    target_class_id: i32,
) -> Result<Option<i32>, ApiError> {
    use crate::schema::hubuumclass::dsl::{collection_id, hubuumclass, id};

    with_connection(pool, |conn| {
        hubuumclass
            .filter(id.eq(target_class_id))
            .select(collection_id)
            .first::<i32>(conn)
            .optional()
    })
}

/// Build the filtered (but unsorted, unpaginated) query for listing export templates within the
/// collections the caller may see.
fn build_list_query<'a>(
    allowed_collection_ids: &'a [i32],
    query_options: &'a QueryOptions,
) -> Result<crate::schema::export_templates::BoxedQuery<'a, diesel::pg::Pg>, ApiError> {
    use crate::schema::export_templates::dsl::{
        class_id, collection_id, created_at, description, export_templates, id, kind, name,
        updated_at,
    };

    if allowed_collection_ids.is_empty() {
        return Ok(export_templates
            .into_boxed()
            .filter(collection_id.eq_any(allowed_collection_ids)));
    }

    let mut query = export_templates
        .into_boxed()
        .filter(collection_id.eq_any(allowed_collection_ids));

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
    pool: &DbPool,
    allowed_collection_ids: &[i32],
    query_options: &QueryOptions,
) -> Result<(Vec<ExportTemplateRow>, i64), ApiError> {
    let query = build_list_query(allowed_collection_ids, query_options)?;
    let total_count = with_connection(pool, |conn| query.count().get_result::<i64>(conn))?;

    let mut query = build_list_query(allowed_collection_ids, query_options)?;
    crate::apply_query_options!(query, query_options, ExportTemplate);
    let rows = with_connection(pool, |conn| query.load::<ExportTemplateRow>(conn))?;

    Ok((rows, total_count))
}
