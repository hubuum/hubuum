//! Backend persistence for report templates.
//!
//! All Diesel/Postgres query construction for `report_templates` lives here so the model layer
//! (`crate::models::report_template`) stays thin and free of backend details, mirroring the other
//! entities under `src/db/traits/`. Instance-scoped CRUD is exposed as self-methods via the record
//! traits below (matching `LoadClassRecord` and friends); collection, search, cross-table, and
//! aggregate queries — which have no single owning instance — stay free functions, as elsewhere in
//! this module. The model owns the domain<->row conversions and all validation.

use diesel::prelude::*;

use crate::db::{DbPool, with_connection, with_transaction};
use crate::errors::ApiError;
use crate::events::{Action, EntityType, EventContext, NewEvent, emit_event};
use crate::models::namespace::NamespaceID;
use crate::models::report_template::{
    NewReportTemplateRow, ReportTemplate, ReportTemplateID, ReportTemplateRow,
    UpdateReportTemplateRow,
};
use crate::models::search::{FilterField, QueryOptions};
use crate::{date_search, numeric_search, string_search};

fn report_template_event(
    row: &ReportTemplateRow,
    action: Action,
    context: &EventContext,
    summary: impl Into<String>,
) -> Result<NewEvent, ApiError> {
    Ok(NewEvent::new(
        EntityType::ReportTemplate,
        action,
        context.actor_kind(),
        summary,
    )?
    .with_context(context)
    .with_entity_id(row.id())
    .with_entity_name(row.name().to_string())
    .with_namespace_id(row.namespace_id()))
}

/// Load the report-template row identified by this id.
pub(crate) trait LoadReportTemplateRecord {
    async fn load_report_template_record(
        &self,
        pool: &DbPool,
    ) -> Result<ReportTemplateRow, ApiError>;
}

impl LoadReportTemplateRecord for ReportTemplateID {
    async fn load_report_template_record(
        &self,
        pool: &DbPool,
    ) -> Result<ReportTemplateRow, ApiError> {
        use crate::schema::report_templates::dsl::{id, report_templates};

        with_connection(pool, |conn| {
            report_templates
                .filter(id.eq(self.id()))
                .first::<ReportTemplateRow>(conn)
        })
    }
}

/// Insert this new report-template row and return the persisted row.
pub(crate) trait SaveReportTemplateRecord {
    async fn save_report_template_record(
        &self,
        pool: &DbPool,
    ) -> Result<ReportTemplateRow, ApiError>;

    async fn save_report_template_record_with_context(
        &self,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<ReportTemplateRow, ApiError> {
        let _ = context;
        self.save_report_template_record(pool).await
    }
}

impl SaveReportTemplateRecord for NewReportTemplateRow {
    async fn save_report_template_record(
        &self,
        pool: &DbPool,
    ) -> Result<ReportTemplateRow, ApiError> {
        use crate::schema::report_templates::dsl::report_templates;

        with_connection(pool, |conn| {
            diesel::insert_into(report_templates)
                .values(self)
                .get_result::<ReportTemplateRow>(conn)
        })
    }

    async fn save_report_template_record_with_context(
        &self,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<ReportTemplateRow, ApiError> {
        let Some(context) = context else {
            return self.save_report_template_record(pool).await;
        };

        use crate::schema::report_templates::dsl::report_templates;

        with_transaction(pool, |conn| -> Result<ReportTemplateRow, ApiError> {
            let row = diesel::insert_into(report_templates)
                .values(self)
                .get_result::<ReportTemplateRow>(conn)?;
            let event = report_template_event(
                &row,
                Action::Created,
                context,
                format!("Report template '{}' created", row.name()),
            )?
            .with_after(row.audit_snapshot());
            emit_event(conn, &event)?;
            Ok(row)
        })
    }
}

/// Apply this changeset to the report-template row with the given id and return the updated row.
pub(crate) trait UpdateReportTemplateRecord {
    async fn update_report_template_record(
        &self,
        pool: &DbPool,
        template_id: i32,
    ) -> Result<ReportTemplateRow, ApiError>;

    async fn update_report_template_record_with_context(
        &self,
        pool: &DbPool,
        template_id: i32,
        context: Option<&EventContext>,
    ) -> Result<ReportTemplateRow, ApiError> {
        let _ = context;
        self.update_report_template_record(pool, template_id).await
    }
}

impl UpdateReportTemplateRecord for UpdateReportTemplateRow {
    async fn update_report_template_record(
        &self,
        pool: &DbPool,
        template_id: i32,
    ) -> Result<ReportTemplateRow, ApiError> {
        use crate::schema::report_templates::dsl::{id, report_templates};

        with_connection(pool, |conn| {
            diesel::update(report_templates.filter(id.eq(template_id)))
                .set(self)
                .get_result::<ReportTemplateRow>(conn)
        })
    }

    async fn update_report_template_record_with_context(
        &self,
        pool: &DbPool,
        template_id: i32,
        context: Option<&EventContext>,
    ) -> Result<ReportTemplateRow, ApiError> {
        let Some(context) = context else {
            return self.update_report_template_record(pool, template_id).await;
        };

        use crate::schema::report_templates::dsl::{id, report_templates};

        with_transaction(pool, |conn| -> Result<ReportTemplateRow, ApiError> {
            let before = report_templates
                .filter(id.eq(template_id))
                .first::<ReportTemplateRow>(conn)?;
            let after = diesel::update(report_templates.filter(id.eq(template_id)))
                .set(self)
                .get_result::<ReportTemplateRow>(conn)?;
            let event = report_template_event(
                &after,
                Action::Updated,
                context,
                format!("Report template '{}' updated", after.name()),
            )?
            .with_before(before.audit_snapshot())
            .with_after(after.audit_snapshot());
            emit_event(conn, &event)?;
            Ok(after)
        })
    }
}

/// Delete the report-template row identified by this id.
pub(crate) trait DeleteReportTemplateRecord {
    async fn delete_report_template_record(&self, pool: &DbPool) -> Result<(), ApiError>;

    async fn delete_report_template_record_with_context(
        &self,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<(), ApiError> {
        let _ = context;
        self.delete_report_template_record(pool).await
    }
}

impl DeleteReportTemplateRecord for ReportTemplateID {
    async fn delete_report_template_record(&self, pool: &DbPool) -> Result<(), ApiError> {
        use crate::schema::report_templates::dsl::{id, report_templates};

        with_connection(pool, |conn| {
            diesel::delete(report_templates.filter(id.eq(self.id()))).execute(conn)
        })?;

        Ok(())
    }

    async fn delete_report_template_record_with_context(
        &self,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<(), ApiError> {
        let Some(context) = context else {
            return self.delete_report_template_record(pool).await;
        };

        use crate::schema::report_templates::dsl::{id, report_templates};

        with_transaction(pool, |conn| -> Result<(), ApiError> {
            let before = report_templates
                .filter(id.eq(self.id()))
                .first::<ReportTemplateRow>(conn)?;
            diesel::delete(report_templates.filter(id.eq(self.id()))).execute(conn)?;
            let event = report_template_event(
                &before,
                Action::Deleted,
                context,
                format!("Report template '{}' deleted", before.name()),
            )?
            .with_before(before.audit_snapshot());
            emit_event(conn, &event)?;
            Ok(())
        })
    }
}

/// Look up the namespace id of the report template identified by this id.
pub(crate) trait ReportTemplateNamespaceLookup {
    async fn lookup_report_template_namespace_id(
        &self,
        pool: &DbPool,
    ) -> Result<NamespaceID, ApiError>;
}

impl ReportTemplateNamespaceLookup for ReportTemplateID {
    async fn lookup_report_template_namespace_id(
        &self,
        pool: &DbPool,
    ) -> Result<NamespaceID, ApiError> {
        use crate::schema::report_templates::dsl::{id, namespace_id, report_templates};

        let raw = with_connection(pool, |conn| {
            report_templates
                .filter(id.eq(self.id()))
                .select(namespace_id)
                .first::<i32>(conn)
        })?;
        NamespaceID::new(raw)
    }
}

/// Load all report-template rows in a namespace, optionally excluding one template id.
pub(crate) async fn load_rows_in_namespace(
    pool: &DbPool,
    target_namespace_id: i32,
    exclude_template_id: Option<i32>,
) -> Result<Vec<ReportTemplateRow>, ApiError> {
    use crate::schema::report_templates::dsl::{id, namespace_id, report_templates};

    with_connection(pool, |conn| {
        let mut query = report_templates
            .into_boxed()
            .filter(namespace_id.eq(target_namespace_id));
        if let Some(exclude_template_id) = exclude_template_id {
            query = query.filter(id.ne(exclude_template_id));
        }
        query.load::<ReportTemplateRow>(conn)
    })
}

/// Load every report-template row.
pub(crate) async fn load_all_rows(pool: &DbPool) -> Result<Vec<ReportTemplateRow>, ApiError> {
    use crate::schema::report_templates::dsl::report_templates;

    with_connection(pool, |conn| {
        report_templates.load::<ReportTemplateRow>(conn)
    })
}

/// Whether a template with `target_name` already exists in the namespace, optionally ignoring one
/// template id (used so an update does not conflict with itself).
pub(crate) async fn name_conflict_exists(
    pool: &DbPool,
    target_namespace_id: i32,
    target_name: &str,
    exclude_template_id: Option<i32>,
) -> Result<bool, ApiError> {
    use crate::schema::report_templates::dsl::{id, name, namespace_id, report_templates};

    let existing = with_connection(pool, |conn| {
        let mut query = report_templates
            .into_boxed()
            .filter(namespace_id.eq(target_namespace_id))
            .filter(name.eq(target_name));
        if let Some(exclude_template_id) = exclude_template_id {
            query = query.filter(id.ne(exclude_template_id));
        }
        query.first::<ReportTemplateRow>(conn).optional()
    })?;

    Ok(existing.is_some())
}

/// The namespace a class belongs to, or `None` if the class does not exist.
pub(crate) async fn class_namespace_id(
    pool: &DbPool,
    target_class_id: i32,
) -> Result<Option<i32>, ApiError> {
    use crate::schema::hubuumclass::dsl::{hubuumclass, id, namespace_id};

    with_connection(pool, |conn| {
        hubuumclass
            .filter(id.eq(target_class_id))
            .select(namespace_id)
            .first::<i32>(conn)
            .optional()
    })
}

/// Build the filtered (but unsorted, unpaginated) query for listing report templates within the
/// namespaces the caller may see.
fn build_list_query<'a>(
    allowed_namespace_ids: &'a [i32],
    query_options: &'a QueryOptions,
) -> Result<crate::schema::report_templates::BoxedQuery<'a, diesel::pg::Pg>, ApiError> {
    use crate::schema::report_templates::dsl::{
        class_id, created_at, description, id, kind, name, namespace_id, report_templates,
        updated_at,
    };

    if allowed_namespace_ids.is_empty() {
        return Ok(report_templates
            .into_boxed()
            .filter(namespace_id.eq_any(allowed_namespace_ids)));
    }

    let mut query = report_templates
        .into_boxed()
        .filter(namespace_id.eq_any(allowed_namespace_ids));

    for param in &query_options.filters {
        let operator = param.operator.clone();
        match param.field {
            FilterField::Id => numeric_search!(query, param, operator, id),
            FilterField::Name => string_search!(query, param, operator, name),
            FilterField::Description => string_search!(query, param, operator, description),
            FilterField::Namespaces | FilterField::NamespaceId => {
                numeric_search!(query, param, operator, namespace_id)
            }
            FilterField::Kind => string_search!(query, param, operator, kind),
            FilterField::ClassId => numeric_search!(query, param, operator, class_id),
            FilterField::CreatedAt => date_search!(query, param, operator, created_at),
            FilterField::UpdatedAt => date_search!(query, param, operator, updated_at),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' isn't searchable (or does not exist) for report templates",
                    param.field
                )));
            }
        }
    }

    Ok(query)
}

/// List report-template rows (sorted/paginated per `query_options`) together with the total count
/// matching the filters, scoped to the namespaces the caller may see.
pub(crate) async fn list_rows_with_total_count(
    pool: &DbPool,
    allowed_namespace_ids: &[i32],
    query_options: &QueryOptions,
) -> Result<(Vec<ReportTemplateRow>, i64), ApiError> {
    let query = build_list_query(allowed_namespace_ids, query_options)?;
    let total_count = with_connection(pool, |conn| query.count().get_result::<i64>(conn))?;

    let mut query = build_list_query(allowed_namespace_ids, query_options)?;
    crate::apply_query_options!(query, query_options, ReportTemplate);
    let rows = with_connection(pool, |conn| query.load::<ReportTemplateRow>(conn))?;

    Ok((rows, total_count))
}
