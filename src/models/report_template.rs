use std::str::FromStr;

use diesel::prelude::*;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::db::{DbPool, with_connection};
use crate::errors::ApiError;
use crate::models::search::{FilterField, QueryOptions, SortParam, parse_query_parameter};
use crate::models::{
    Namespace, NamespaceID, ReportContentType, ReportInclude, ReportLimits,
    ReportMissingDataPolicy, ReportRelationContext, ReportScopeKind,
};
use crate::pagination::{
    CursorPaginated, CursorSqlField, CursorSqlMapping, CursorSqlType, CursorValue,
};
use crate::schema::report_templates;
use crate::traits::accessors::{IdAccessor, InstanceAdapter, NamespaceAdapter};
use crate::utilities::reporting::{validate_related_objects_include, validate_template};
use crate::{date_search, numeric_search, string_search};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReportTemplateKind {
    Report,
    Fragment,
}

impl ReportTemplateKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Report => "report",
            Self::Fragment => "fragment",
        }
    }
}

impl FromStr for ReportTemplateKind {
    type Err = ApiError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "report" => Ok(Self::Report),
            "fragment" => Ok(Self::Fragment),
            _ => Err(ApiError::BadRequest(format!(
                "Unsupported report template kind: '{value}'"
            ))),
        }
    }
}

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = report_templates)]
struct ReportTemplateRow {
    id: i32,
    namespace_id: i32,
    name: String,
    description: String,
    content_type: String,
    template: String,
    kind: String,
    scope_kind: Option<String>,
    class_id: Option<i32>,
    default_query: Option<String>,
    include: Option<serde_json::Value>,
    relation_context: Option<serde_json::Value>,
    default_missing_data_policy: Option<String>,
    default_limits: Option<serde_json::Value>,
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
    pub kind: ReportTemplateKind,
    pub scope_kind: Option<ReportScopeKind>,
    pub class_id: Option<i32>,
    pub default_query: Option<String>,
    pub include: Option<ReportInclude>,
    pub relation_context: Option<ReportRelationContext>,
    pub default_missing_data_policy: Option<ReportMissingDataPolicy>,
    pub default_limits: Option<ReportLimits>,
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
    pub kind: ReportTemplateKind,
    pub scope_kind: Option<ReportScopeKind>,
    pub class_id: Option<i32>,
    pub default_query: Option<String>,
    pub include: Option<ReportInclude>,
    pub relation_context: Option<ReportRelationContext>,
    pub default_missing_data_policy: Option<ReportMissingDataPolicy>,
    pub default_limits: Option<ReportLimits>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[schema(example = update_report_template_example)]
pub struct UpdateReportTemplate {
    pub namespace_id: Option<i32>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub template: Option<String>,
    pub kind: Option<ReportTemplateKind>,
    pub scope_kind: Option<ReportScopeKind>,
    pub class_id: Option<i32>,
    // The nullable report-profile fields use double `Option` so a PATCH can distinguish an
    // omitted field (outer `None` — keep the current value) from an explicit JSON `null`
    // (`Some(None)` — clear the value). A plain `Option` collapses both to `None`.
    // `deserialize_double_option` makes serde populate the outer `Some` only when the key is
    // present, and `skip_serializing_if` keeps omitted fields out of serialized payloads so the
    // distinction survives a serialize/deserialize round-trip (e.g. in tests and examples).
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_double_option"
    )]
    #[schema(value_type = Option<String>)]
    pub default_query: Option<Option<String>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_double_option"
    )]
    #[schema(value_type = Option<ReportInclude>)]
    pub include: Option<Option<ReportInclude>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_double_option"
    )]
    #[schema(value_type = Option<ReportRelationContext>)]
    pub relation_context: Option<Option<ReportRelationContext>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_double_option"
    )]
    #[schema(value_type = Option<ReportMissingDataPolicy>)]
    pub default_missing_data_policy: Option<Option<ReportMissingDataPolicy>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_double_option"
    )]
    #[schema(value_type = Option<ReportLimits>)]
    pub default_limits: Option<Option<ReportLimits>>,
}

/// Deserialize a tri-state PATCH field. serde only invokes a field's `deserialize_with` when the
/// key is present, so this wraps the inner `Option<T>` (which captures `null` vs a value) in an
/// outer `Some`, leaving an omitted key as the `default` outer `None`.
fn deserialize_double_option<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Ok(Some(Option::<T>::deserialize(deserializer)?))
}

#[derive(Debug, Clone, Insertable)]
#[diesel(table_name = report_templates)]
struct NewReportTemplateRow {
    namespace_id: i32,
    name: String,
    description: String,
    content_type: String,
    template: String,
    kind: String,
    scope_kind: Option<String>,
    class_id: Option<i32>,
    default_query: Option<String>,
    include: Option<serde_json::Value>,
    relation_context: Option<serde_json::Value>,
    default_missing_data_policy: Option<String>,
    default_limits: Option<serde_json::Value>,
}

#[derive(Debug, Clone, AsChangeset)]
#[diesel(table_name = report_templates)]
struct UpdateReportTemplateRow {
    namespace_id: Option<i32>,
    name: Option<String>,
    description: Option<String>,
    template: Option<String>,
    kind: Option<String>,
    scope_kind: Option<Option<String>>,
    class_id: Option<Option<i32>>,
    default_query: Option<Option<String>>,
    include: Option<Option<serde_json::Value>>,
    relation_context: Option<Option<serde_json::Value>>,
    default_missing_data_policy: Option<Option<String>>,
    default_limits: Option<Option<serde_json::Value>>,
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
            kind: ReportTemplateKind::from_str(&row.kind)?,
            scope_kind: row
                .scope_kind
                .as_deref()
                .map(ReportScopeKind::from_str)
                .transpose()?,
            class_id: row.class_id,
            default_query: row.default_query,
            include: from_optional_json(row.include)?,
            relation_context: from_optional_json(row.relation_context)?,
            default_missing_data_policy: row
                .default_missing_data_policy
                .as_deref()
                .map(ReportMissingDataPolicy::from_str)
                .transpose()?,
            default_limits: from_optional_json(row.default_limits)?,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }
}

impl NewReportTemplate {
    fn into_row(self) -> Result<NewReportTemplateRow, ApiError> {
        let content_type = self.content_type.ensure_template_output()?.as_mime();

        Ok(NewReportTemplateRow {
            namespace_id: self.namespace_id,
            name: self.name,
            description: self.description,
            content_type: content_type.to_string(),
            template: self.template,
            kind: self.kind.as_str().to_string(),
            scope_kind: self.scope_kind.map(|scope| scope.as_str().to_string()),
            class_id: self.class_id,
            default_query: self.default_query,
            include: to_optional_json(self.include)?,
            relation_context: to_optional_json(self.relation_context)?,
            default_missing_data_policy: self
                .default_missing_data_policy
                .map(|policy| policy.as_str().to_string()),
            default_limits: to_optional_json(self.default_limits)?,
        })
    }
}

impl UpdateReportTemplate {
    fn is_empty(&self) -> bool {
        self.namespace_id.is_none()
            && self.name.is_none()
            && self.description.is_none()
            && self.template.is_none()
            && self.kind.is_none()
            && self.scope_kind.is_none()
            && self.class_id.is_none()
            && self.default_query.is_none()
            && self.include.is_none()
            && self.relation_context.is_none()
            && self.default_missing_data_policy.is_none()
            && self.default_limits.is_none()
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

    let new_row = template.into_row()?;
    ensure_template_name_is_available(pool, new_row.namespace_id, &new_row.name, None).await?;
    validate_report_profile(
        pool,
        new_row.namespace_id,
        ReportProfileRef {
            kind: ReportTemplateKind::from_str(&new_row.kind)?,
            scope_kind: new_row.scope_kind.as_deref(),
            class_id: new_row.class_id,
            default_query: new_row.default_query.as_deref(),
            include: new_row.include.as_ref(),
            relation_context: new_row.relation_context.as_ref(),
            default_missing_data_policy: new_row.default_missing_data_policy.as_deref(),
            default_limits: new_row.default_limits.as_ref(),
        },
    )
    .await?;
    let namespace_templates =
        report_templates_in_namespace(pool, new_row.namespace_id, None).await?;
    validate_template(
        &new_row.name,
        &new_row.template,
        new_row.namespace_id,
        &namespace_templates,
        ReportContentType::from_mime(&new_row.content_type)?,
    )?;
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
    use crate::schema::report_templates::dsl::{id, report_templates};

    let current_row = with_connection(pool, |conn| {
        report_templates
            .filter(id.eq(template_id))
            .first::<ReportTemplateRow>(conn)
    })?;

    if update.is_empty() {
        return current_row.try_into();
    }

    let current = ReportTemplate::try_from(current_row.clone())?;
    let target_namespace_id = update.namespace_id.unwrap_or(current.namespace_id);
    let target_name = update.name.clone().unwrap_or_else(|| current.name.clone());
    let target_description = update
        .description
        .clone()
        .unwrap_or_else(|| current.description.clone());
    let target_template = update
        .template
        .clone()
        .unwrap_or_else(|| current.template.clone());
    let target_kind = update.kind.unwrap_or(current.kind);

    if target_kind == ReportTemplateKind::Fragment && update_report_fields_present(&update) {
        return Err(ApiError::BadRequest(
            "Fragment templates cannot define report execution metadata".to_string(),
        ));
    }

    let (
        target_scope_kind,
        target_class_id,
        target_default_query,
        target_include,
        target_relation_context,
        target_default_missing_data_policy,
        target_default_limits,
    ) = if target_kind == ReportTemplateKind::Fragment {
        (None, None, None, None, None, None, None)
    } else {
        let target_scope_kind = update.scope_kind.or(current.scope_kind);

        // Reconcile scope-dependent fields against the *target* scope. Without this,
        // `update.field.or(current.field)` would carry forward a class_id/include/
        // relation_context that the new scope forbids, making it impossible to PATCH an
        // objects_in_class template into a collection scope. Carried-forward values are
        // dropped when the target scope cannot hold them; an explicitly supplied
        // incompatible value still falls through to validate_report_profile, which rejects
        // it (matching the create path). This mirrors how switching to a fragment clears
        // report metadata.
        let scope_allows_class = target_scope_kind
            .map(ReportScopeKind::requires_class_id)
            .unwrap_or(false);
        let scope_allows_include = target_scope_kind == Some(ReportScopeKind::ObjectsInClass);
        let scope_allows_relation_context = matches!(
            target_scope_kind,
            Some(ReportScopeKind::ObjectsInClass) | Some(ReportScopeKind::RelatedObjects)
        );

        let target_class_id = if scope_allows_class {
            update.class_id.or(current.class_id)
        } else {
            update.class_id
        };
        let target_include =
            resolve_gated_patch(update.include, current.include, scope_allows_include);
        let target_relation_context = resolve_gated_patch(
            update.relation_context,
            current.relation_context,
            scope_allows_relation_context,
        );

        (
            target_scope_kind,
            target_class_id,
            update.default_query.unwrap_or(current.default_query),
            target_include,
            target_relation_context,
            update
                .default_missing_data_policy
                .unwrap_or(current.default_missing_data_policy),
            update.default_limits.unwrap_or(current.default_limits),
        )
    };

    ensure_template_name_is_available(pool, target_namespace_id, &target_name, Some(template_id))
        .await?;
    let include_json = to_optional_json(target_include)?;
    let relation_context_json = to_optional_json(target_relation_context)?;
    let default_limits_json = to_optional_json(target_default_limits)?;
    validate_report_profile(
        pool,
        target_namespace_id,
        ReportProfileRef {
            kind: target_kind,
            scope_kind: target_scope_kind.map(ReportScopeKind::as_str),
            class_id: target_class_id,
            default_query: target_default_query.as_deref(),
            include: include_json.as_ref(),
            relation_context: relation_context_json.as_ref(),
            default_missing_data_policy: target_default_missing_data_policy
                .map(ReportMissingDataPolicy::as_str),
            default_limits: default_limits_json.as_ref(),
        },
    )
    .await?;
    let namespace_templates =
        report_templates_in_namespace(pool, target_namespace_id, Some(template_id)).await?;
    validate_template(
        &target_name,
        &target_template,
        target_namespace_id,
        &namespace_templates,
        current.content_type,
    )?;

    let changeset = UpdateReportTemplateRow {
        namespace_id: Some(target_namespace_id),
        name: Some(target_name),
        description: Some(target_description),
        template: Some(target_template),
        kind: Some(target_kind.as_str().to_string()),
        scope_kind: Some(target_scope_kind.map(|scope| scope.as_str().to_string())),
        class_id: Some(target_class_id),
        default_query: Some(target_default_query),
        include: Some(include_json),
        relation_context: Some(relation_context_json),
        default_missing_data_policy: Some(
            target_default_missing_data_policy.map(|policy| policy.as_str().to_string()),
        ),
        default_limits: Some(default_limits_json),
    };
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

pub async fn report_templates_in_namespace(
    pool: &DbPool,
    target_namespace_id: i32,
    exclude_template_id: Option<i32>,
) -> Result<Vec<ReportTemplate>, ApiError> {
    use crate::schema::report_templates::dsl::{id, namespace_id, report_templates};

    let rows = with_connection(pool, |conn| {
        let mut query = report_templates
            .into_boxed()
            .filter(namespace_id.eq(target_namespace_id));
        if let Some(exclude_template_id) = exclude_template_id {
            query = query.filter(id.ne(exclude_template_id));
        }
        query.load::<ReportTemplateRow>(conn)
    })?;

    rows.into_iter().map(TryInto::try_into).collect()
}

#[allow(dead_code)]
pub async fn list_all_report_templates(pool: &DbPool) -> Result<Vec<ReportTemplate>, ApiError> {
    use crate::schema::report_templates::dsl::report_templates;

    let rows = with_connection(pool, |conn| {
        report_templates.load::<ReportTemplateRow>(conn)
    })?;

    rows.into_iter().map(TryInto::try_into).collect()
}

/// Borrowed view of the report-execution metadata validated together. Bundled so
/// `validate_report_profile` stays within a sensible argument count and both the create and
/// update paths share one shape.
#[derive(Debug, Clone, Copy)]
struct ReportProfileRef<'a> {
    kind: ReportTemplateKind,
    scope_kind: Option<&'a str>,
    class_id: Option<i32>,
    default_query: Option<&'a str>,
    include: Option<&'a serde_json::Value>,
    relation_context: Option<&'a serde_json::Value>,
    default_missing_data_policy: Option<&'a str>,
    default_limits: Option<&'a serde_json::Value>,
}

async fn validate_report_profile(
    pool: &DbPool,
    target_namespace_id: i32,
    profile: ReportProfileRef<'_>,
) -> Result<(), ApiError> {
    let ReportProfileRef {
        kind,
        scope_kind,
        class_id,
        default_query,
        include,
        relation_context,
        default_missing_data_policy,
        default_limits,
    } = profile;

    match kind {
        ReportTemplateKind::Fragment => {
            if scope_kind.is_some() || class_id.is_some() {
                return Err(ApiError::BadRequest(
                    "Fragment templates cannot define report execution metadata".to_string(),
                ));
            }
        }
        ReportTemplateKind::Report => {
            let scope_kind = scope_kind
                .ok_or_else(|| ApiError::BadRequest("Report templates require scope_kind".into()))
                .and_then(ReportScopeKind::from_str)?;

            // `objects_in_class` and `related_objects` are scoped to a single class and
            // require `class_id`; the collection scopes (`namespaces`, `classes`,
            // `class_relations`, `object_relations`) are class-agnostic and must not set it.
            if scope_kind.requires_class_id() {
                let class_id = class_id.ok_or_else(|| {
                    ApiError::BadRequest(format!(
                        "Report templates with scope '{}' require class_id",
                        scope_kind.as_str()
                    ))
                })?;
                if class_id <= 0 {
                    return Err(ApiError::BadRequest(
                        "Report template class_id must be greater than 0".to_string(),
                    ));
                }
                ensure_template_class_in_namespace(pool, target_namespace_id, class_id).await?;
            } else if class_id.is_some() {
                return Err(ApiError::BadRequest(format!(
                    "Report templates with scope '{}' must not set class_id",
                    scope_kind.as_str()
                )));
            }

            if let Some(query) = default_query {
                parse_query_parameter(query)?;
            }

            if include.is_some() && scope_kind != ReportScopeKind::ObjectsInClass {
                return Err(ApiError::BadRequest(
                    "include is only supported for objects_in_class report templates".to_string(),
                ));
            }

            if relation_context.is_some()
                && !matches!(
                    scope_kind,
                    ReportScopeKind::ObjectsInClass | ReportScopeKind::RelatedObjects
                )
            {
                return Err(ApiError::BadRequest(
                    "relation_context is only supported for objects_in_class and related_objects report templates"
                        .to_string(),
                ));
            }
        }
    }

    if include.is_some() && relation_context.is_some() {
        return Err(ApiError::BadRequest(
            "include cannot be combined with relation_context".to_string(),
        ));
    }

    if let Some(include) = include {
        let include: ReportInclude = serde_json::from_value(include.clone())?;
        validate_related_objects_include(&include)?;
    }
    if let Some(relation_context) = relation_context {
        let context: ReportRelationContext = serde_json::from_value(relation_context.clone())?;
        if let Some(depth) = context.depth
            && !(1..=2).contains(&depth)
        {
            return Err(ApiError::BadRequest(
                "Templated relation hydration only supports depth 1 or 2".to_string(),
            ));
        }
    }
    if let Some(policy) = default_missing_data_policy {
        ReportMissingDataPolicy::from_str(policy)?;
    }
    if let Some(limits) = default_limits {
        let _limits: ReportLimits = serde_json::from_value(limits.clone())?;
    }

    Ok(())
}

async fn ensure_template_class_in_namespace(
    pool: &DbPool,
    target_namespace_id: i32,
    target_class_id: i32,
) -> Result<(), ApiError> {
    use crate::schema::hubuumclass::dsl::{hubuumclass, id, namespace_id};

    let class_namespace_id = with_connection(pool, |conn| {
        hubuumclass
            .filter(id.eq(target_class_id))
            .select(namespace_id)
            .first::<i32>(conn)
            .optional()
    })?
    .ok_or_else(|| ApiError::NotFound(format!("Class {target_class_id} not found")))?;

    if class_namespace_id != target_namespace_id {
        return Err(ApiError::BadRequest(format!(
            "Report template class {target_class_id} belongs to namespace {class_namespace_id}, not template namespace {target_namespace_id}"
        )));
    }

    Ok(())
}

fn update_report_fields_present(update: &UpdateReportTemplate) -> bool {
    update.scope_kind.is_some()
        || update.class_id.is_some()
        || update.default_query.is_some()
        || update.include.is_some()
        || update.relation_context.is_some()
        || update.default_missing_data_policy.is_some()
        || update.default_limits.is_some()
}

/// Resolve a tri-state PATCH field whose validity depends on the target scope.
/// When the scope `allowed`s the field, this behaves like a normal tri-state resolve
/// (absent keeps current, `Some(None)` clears, `Some(Some)` sets). When the scope forbids
/// it, a carried-forward current value is dropped, but an explicitly supplied value is kept
/// so `validate_report_profile` can reject it (matching the create path).
fn resolve_gated_patch<T>(
    update: Option<Option<T>>,
    current: Option<T>,
    allowed: bool,
) -> Option<T> {
    if allowed {
        update.unwrap_or(current)
    } else {
        match update {
            Some(Some(value)) => Some(value),
            _ => None,
        }
    }
}

fn to_optional_json<T>(value: Option<T>) -> Result<Option<serde_json::Value>, ApiError>
where
    T: Serialize,
{
    value
        .map(serde_json::to_value)
        .transpose()
        .map_err(Into::into)
}

fn from_optional_json<T>(value: Option<serde_json::Value>) -> Result<Option<T>, ApiError>
where
    T: for<'de> Deserialize<'de>,
{
    value
        .map(serde_json::from_value)
        .transpose()
        .map_err(Into::into)
}

async fn ensure_template_name_is_available(
    pool: &DbPool,
    target_namespace_id: i32,
    target_name: &str,
    exclude_template_id: Option<i32>,
) -> Result<(), ApiError> {
    use crate::schema::report_templates::dsl::{id, name, namespace_id, report_templates};

    let existing_name_conflict = with_connection(pool, |conn| {
        let mut query = report_templates
            .into_boxed()
            .filter(namespace_id.eq(target_namespace_id))
            .filter(name.eq(target_name));
        if let Some(exclude_template_id) = exclude_template_id {
            query = query.filter(id.ne(exclude_template_id));
        }
        query.first::<ReportTemplateRow>(conn).optional()
    })?;

    if existing_name_conflict.is_some() {
        return Err(ApiError::Conflict(format!(
            "Template name '{}' already exists in namespace {}",
            target_name, target_namespace_id
        )));
    }

    Ok(())
}

fn build_report_template_query<'a>(
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

pub async fn list_report_templates_with_total_count(
    pool: &DbPool,
    allowed_namespace_ids: &[i32],
    query_options: &QueryOptions,
) -> Result<(Vec<ReportTemplate>, i64), ApiError> {
    if allowed_namespace_ids.is_empty() {
        return Ok((Vec::new(), 0));
    }

    let query = build_report_template_query(allowed_namespace_ids, query_options)?;
    let total_count = with_connection(pool, |conn| query.count().get_result::<i64>(conn))?;

    let mut query = build_report_template_query(allowed_namespace_ids, query_options)?;
    crate::apply_query_options!(query, query_options, ReportTemplate);
    let rows = with_connection(pool, |conn| query.load::<ReportTemplateRow>(conn))?;

    let items = rows
        .into_iter()
        .map(TryInto::try_into)
        .collect::<Result<Vec<_>, _>>()?;

    Ok((items, total_count))
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
    let example_timestamp = chrono::NaiveDate::from_ymd_opt(2026, 3, 6)
        .and_then(|date| date.and_hms_opt(12, 0, 0))
        .expect("static OpenAPI example timestamp must be valid");

    ReportTemplate {
        id: 1,
        namespace_id: 7,
        name: "owner-report".to_string(),
        description: "Template for owner listing".to_string(),
        content_type: ReportContentType::TextPlain,
        template: "{% for item in items %}{{ item.name }}={{ item.data.owner }}\n{% endfor %}"
            .to_string(),
        kind: ReportTemplateKind::Report,
        scope_kind: Some(ReportScopeKind::ObjectsInClass),
        class_id: Some(42),
        default_query: Some("sort=name".to_string()),
        include: None,
        relation_context: None,
        default_missing_data_policy: Some(ReportMissingDataPolicy::Strict),
        default_limits: Some(ReportLimits {
            max_items: Some(100),
            max_output_bytes: Some(262_144),
        }),
        created_at: example_timestamp,
        updated_at: example_timestamp,
    }
}

#[allow(dead_code)]
fn new_report_template_example() -> NewReportTemplate {
    NewReportTemplate {
        namespace_id: 7,
        name: "owner-report".to_string(),
        description: "Template for owner listing".to_string(),
        content_type: ReportContentType::TextPlain,
        template: "{% for item in items %}{{ item.name }}={{ item.data.owner }}\n{% endfor %}"
            .to_string(),
        kind: ReportTemplateKind::Report,
        scope_kind: Some(ReportScopeKind::ObjectsInClass),
        class_id: Some(42),
        default_query: Some("sort=name".to_string()),
        include: None,
        relation_context: None,
        default_missing_data_policy: Some(ReportMissingDataPolicy::Strict),
        default_limits: Some(ReportLimits {
            max_items: Some(100),
            max_output_bytes: Some(262_144),
        }),
    }
}

#[allow(dead_code)]
fn update_report_template_example() -> UpdateReportTemplate {
    UpdateReportTemplate {
        namespace_id: Some(9),
        name: Some("owner-report-v2".to_string()),
        description: Some("Updated template description".to_string()),
        template: Some("{% for item in items %}{{ item.name }}\n{% endfor %}".to_string()),
        kind: None,
        scope_kind: None,
        class_id: None,
        default_query: Some(Some("sort=name.desc".to_string())),
        include: None,
        relation_context: None,
        default_missing_data_policy: None,
        default_limits: None,
    }
}
