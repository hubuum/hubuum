use diesel::prelude::*;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::db::{DbPool, with_connection};
use crate::errors::ApiError;
use crate::models::search::{FilterField, QueryOptions, SortParam};
use crate::models::{Namespace, NamespaceID, ReportContentType};
use crate::pagination::{
    CursorPaginated, CursorSqlField, CursorSqlMapping, CursorSqlType, CursorValue,
};
use crate::schema::report_templates;
use crate::traits::accessors::{IdAccessor, InstanceAdapter, NamespaceAdapter};
use crate::{date_search, numeric_search, string_search};

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = report_templates)]
struct ReportTemplateRow {
    id: i32,
    namespace_id: i32,
    name: String,
    description: String,
    content_type: String,
    template: String,
    created_at: chrono::NaiveDateTime,
    updated_at: chrono::NaiveDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[schema(example = report_template_example)]
pub struct ReportTemplate {
    pub id: i32,
    pub namespace_id: i32,
    pub name: String,
    pub description: String,
    pub content_type: ReportContentType,
    pub template: String,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct ReportTemplateID(pub i32);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[schema(example = new_report_template_example)]
pub struct NewReportTemplate {
    pub namespace_id: i32,
    pub name: String,
    pub description: String,
    pub content_type: ReportContentType,
    pub template: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[schema(example = update_report_template_example)]
pub struct UpdateReportTemplate {
    pub namespace_id: Option<i32>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub template: Option<String>,
}

#[derive(Debug, Clone, Insertable)]
#[diesel(table_name = report_templates)]
struct NewReportTemplateRow {
    namespace_id: i32,
    name: String,
    description: String,
    content_type: String,
    template: String,
}

#[derive(Debug, Clone, AsChangeset, Default)]
#[diesel(table_name = report_templates)]
struct UpdateReportTemplateRow {
    namespace_id: Option<i32>,
    name: Option<String>,
    description: Option<String>,
    template: Option<String>,
}

impl TryFrom<ReportTemplateRow> for ReportTemplate {
    type Error = ApiError;

    fn try_from(row: ReportTemplateRow) -> Result<Self, Self::Error> {
        Ok(Self {
            id: row.id,
            namespace_id: row.namespace_id,
            name: row.name,
            description: row.description,
            content_type: ReportContentType::from_mime(&row.content_type)?,
            template: row.template,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }
}

impl NewReportTemplate {
    fn validate(self) -> Result<NewReportTemplateRow, ApiError> {
        let content_type = self.content_type.ensure_template_output()?.as_mime();

        Ok(NewReportTemplateRow {
            namespace_id: self.namespace_id,
            name: self.name,
            description: self.description,
            content_type: content_type.to_string(),
            template: self.template,
        })
    }
}

impl UpdateReportTemplate {
    fn as_changeset(self) -> UpdateReportTemplateRow {
        UpdateReportTemplateRow {
            namespace_id: self.namespace_id,
            name: self.name,
            description: self.description,
            template: self.template,
        }
    }

    fn is_empty(&self) -> bool {
        self.namespace_id.is_none()
            && self.name.is_none()
            && self.description.is_none()
            && self.template.is_none()
    }
}

pub async fn report_template(pool: &DbPool, template_id: i32) -> Result<ReportTemplate, ApiError> {
    use crate::schema::report_templates::dsl::{id, report_templates};

    let row = with_connection(pool, |conn| {
        report_templates
            .filter(id.eq(template_id))
            .first::<ReportTemplateRow>(conn)
    })?;

    row.try_into()
}

pub async fn create_report_template(
    pool: &DbPool,
    template: NewReportTemplate,
) -> Result<ReportTemplate, ApiError> {
    use crate::schema::report_templates::dsl::report_templates;

    let new_row = template.validate()?;
    let row = with_connection(pool, |conn| {
        diesel::insert_into(report_templates)
            .values(&new_row)
            .get_result::<ReportTemplateRow>(conn)
    })?;

    row.try_into()
}

pub async fn update_report_template(
    pool: &DbPool,
    template_id: i32,
    update: UpdateReportTemplate,
) -> Result<ReportTemplate, ApiError> {
    use crate::schema::report_templates::dsl::{id, name, namespace_id, report_templates};

    let current_row = with_connection(pool, |conn| {
        report_templates
            .filter(id.eq(template_id))
            .first::<ReportTemplateRow>(conn)
    })?;

    if update.is_empty() {
        return current_row.try_into();
    }

    let target_namespace_id = update.namespace_id.unwrap_or(current_row.namespace_id);
    let target_name = update
        .name
        .clone()
        .unwrap_or_else(|| current_row.name.clone());

    let existing_name_conflict = with_connection(pool, |conn| {
        report_templates
            .filter(namespace_id.eq(target_namespace_id))
            .filter(name.eq(&target_name))
            .filter(id.ne(template_id))
            .first::<ReportTemplateRow>(conn)
            .optional()
    })?;

    if existing_name_conflict.is_some() {
        return Err(ApiError::Conflict(format!(
            "Template name '{}' already exists in namespace {}",
            target_name, target_namespace_id
        )));
    }

    let changeset = update.as_changeset();
    let row = with_connection(pool, |conn| {
        diesel::update(report_templates.filter(id.eq(template_id)))
            .set(&changeset)
            .get_result::<ReportTemplateRow>(conn)
    })?;

    row.try_into()
}

pub async fn delete_report_template(pool: &DbPool, template_id: i32) -> Result<(), ApiError> {
    use crate::schema::report_templates::dsl::{id, report_templates};

    with_connection(pool, |conn| {
        diesel::delete(report_templates.filter(id.eq(template_id))).execute(conn)
    })?;

    Ok(())
}

pub async fn list_report_templates(
    pool: &DbPool,
    allowed_namespace_ids: &[i32],
    query_options: &QueryOptions,
) -> Result<Vec<ReportTemplate>, ApiError> {
    use crate::schema::report_templates::dsl::{
        created_at, description, id, name, namespace_id, report_templates, updated_at,
    };

    if allowed_namespace_ids.is_empty() {
        return Ok(Vec::new());
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

    crate::apply_query_options!(query, query_options, ReportTemplate);

    let rows = with_connection(pool, |conn| query.load::<ReportTemplateRow>(conn))?;

    rows.into_iter().map(TryInto::try_into).collect()
}

impl IdAccessor for ReportTemplate {
    fn accessor_id(&self) -> i32 {
        self.id
    }
}

impl InstanceAdapter<ReportTemplate> for ReportTemplate {
    async fn instance_adapter(&self, _pool: &DbPool) -> Result<ReportTemplate, ApiError> {
        Ok(self.clone())
    }
}

impl IdAccessor for ReportTemplateID {
    fn accessor_id(&self) -> i32 {
        self.0
    }
}

impl InstanceAdapter<ReportTemplate> for ReportTemplateID {
    async fn instance_adapter(&self, pool: &DbPool) -> Result<ReportTemplate, ApiError> {
        report_template(pool, self.0).await
    }
}

impl NamespaceAdapter for ReportTemplate {
    async fn namespace_adapter(&self, pool: &DbPool) -> Result<Namespace, ApiError> {
        NamespaceID(self.namespace_id).namespace_adapter(pool).await
    }

    async fn namespace_id_adapter(&self, _pool: &DbPool) -> Result<i32, ApiError> {
        Ok(self.namespace_id)
    }
}

impl NamespaceAdapter for ReportTemplateID {
    async fn namespace_adapter(&self, pool: &DbPool) -> Result<Namespace, ApiError> {
        NamespaceID(self.namespace_id_adapter(pool).await?)
            .namespace_adapter(pool)
            .await
    }

    async fn namespace_id_adapter(&self, pool: &DbPool) -> Result<i32, ApiError> {
        use crate::schema::report_templates::dsl::{id, namespace_id, report_templates};

        with_connection(pool, |conn| {
            report_templates
                .filter(id.eq(self.0))
                .select(namespace_id)
                .first::<i32>(conn)
        })
    }
}

impl CursorPaginated for ReportTemplate {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(
            field,
            FilterField::Id
                | FilterField::Name
                | FilterField::Description
                | FilterField::Namespaces
                | FilterField::NamespaceId
                | FilterField::CreatedAt
                | FilterField::UpdatedAt
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        Ok(match field {
            FilterField::Id => CursorValue::Integer(self.id as i64),
            FilterField::Name => CursorValue::String(self.name.clone()),
            FilterField::Description => CursorValue::String(self.description.clone()),
            FilterField::Namespaces | FilterField::NamespaceId => {
                CursorValue::Integer(self.namespace_id as i64)
            }
            FilterField::CreatedAt => CursorValue::DateTime(self.created_at),
            FilterField::UpdatedAt => CursorValue::DateTime(self.updated_at),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for report templates",
                    field
                )));
            }
        })
    }

    fn default_sort() -> Vec<SortParam> {
        vec![SortParam {
            field: FilterField::Id,
            descending: false,
        }]
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        Self::default_sort()
    }
}

impl CursorSqlMapping for ReportTemplate {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::Id => CursorSqlField {
                column: "report_templates.id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Name => CursorSqlField {
                column: "report_templates.name",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::Description => CursorSqlField {
                column: "report_templates.description",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::Namespaces | FilterField::NamespaceId => CursorSqlField {
                column: "report_templates.namespace_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::CreatedAt => CursorSqlField {
                column: "report_templates.created_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::UpdatedAt => CursorSqlField {
                column: "report_templates.updated_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for report templates",
                    field
                )));
            }
        })
    }
}

#[allow(dead_code)]
fn report_template_example() -> ReportTemplate {
    ReportTemplate {
        id: 1,
        namespace_id: 7,
        name: "owner-report".to_string(),
        description: "Template for owner listing".to_string(),
        content_type: ReportContentType::TextPlain,
        template: "{{#each items}}{{this.name}}={{this.data.owner}}\\n{{/each}}".to_string(),
        created_at: chrono::Utc::now().naive_utc(),
        updated_at: chrono::Utc::now().naive_utc(),
    }
}

#[allow(dead_code)]
fn new_report_template_example() -> NewReportTemplate {
    NewReportTemplate {
        namespace_id: 7,
        name: "owner-report".to_string(),
        description: "Template for owner listing".to_string(),
        content_type: ReportContentType::TextPlain,
        template: "{{#each items}}{{this.name}}={{this.data.owner}}\\n{{/each}}".to_string(),
    }
}

#[allow(dead_code)]
fn update_report_template_example() -> UpdateReportTemplate {
    UpdateReportTemplate {
        namespace_id: Some(9),
        name: Some("owner-report-v2".to_string()),
        description: Some("Updated template description".to_string()),
        template: Some("{{#each items}}{{this.name}}\\n{{/each}}".to_string()),
    }
}
