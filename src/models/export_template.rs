use std::str::FromStr;

use crate::db::prelude::*;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::db::DbPool;
use crate::db::traits::export_template::{
    self as backend, DeleteExportTemplateRecord, ExportTemplateCollectionLookup,
    LoadExportTemplateRecord, SaveExportTemplateRecord, UpdateExportTemplateRecord,
};
use crate::errors::ApiError;
use crate::events::EventContext;
use crate::models::search::{FilterField, QueryOptions, SortParam, parse_query_parameter};
use crate::models::{
    Collection, CollectionID, ExportContentType, ExportInclude, ExportLimits,
    ExportMissingDataPolicy, ExportRelationContext, ExportRequest, ExportScope, ExportScopeKind,
    ExportTemplateRunRequest,
};
use crate::pagination::{
    CursorPaginated, CursorSqlField, CursorSqlMapping, CursorSqlType, CursorValue,
};
use crate::permissions::{AuthzTarget, ResourceAttrs, ResourceKind, ResourceRef};
use crate::schema::export_templates;
use crate::traits::BackendContext;
use crate::traits::accessors::{
    CollectionAccessors, CollectionAdapter, IdAccessor, InstanceAdapter, SelfAccessors,
};
use crate::traits::crud::{DeleteAdapter, SaveAdapter, UpdateAdapter};
use crate::utilities::exporting::{
    validate_template, validate_template_sources, validate_template_syntax,
};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExportTemplateKind {
    Export,
    Fragment,
}

impl ExportTemplateKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Export => "export",
            Self::Fragment => "fragment",
        }
    }
}

impl FromStr for ExportTemplateKind {
    type Err = ApiError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "export" => Ok(Self::Export),
            "fragment" => Ok(Self::Fragment),
            _ => Err(ApiError::BadRequest(format!(
                "Unsupported export template kind: '{value}'"
            ))),
        }
    }
}

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = export_templates)]
pub(crate) struct ExportTemplateRow {
    id: i32,
    collection_id: i32,
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
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[schema(example = export_template_example)]
pub struct ExportTemplate {
    pub id: i32,
    pub collection_id: i32,
    pub name: String,
    pub description: String,
    pub content_type: ExportContentType,
    pub template: String,
    pub kind: ExportTemplateKind,
    pub scope_kind: Option<ExportScopeKind>,
    pub class_id: Option<i32>,
    pub default_query: Option<String>,
    pub include: Option<ExportInclude>,
    pub relation_context: Option<ExportRelationContext>,
    pub default_missing_data_policy: Option<ExportMissingDataPolicy>,
    pub default_limits: Option<ExportLimits>,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
}

crate::int_id_newtype! {
    /// Identifier wrapper for a [`ExportTemplate`].
    pub struct ExportTemplateID;
    noun = "export template id";
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[schema(example = new_export_template_example)]
pub struct NewExportTemplate {
    pub collection_id: i32,
    pub name: String,
    pub description: String,
    pub content_type: ExportContentType,
    pub template: String,
    pub kind: ExportTemplateKind,
    pub scope_kind: Option<ExportScopeKind>,
    pub class_id: Option<i32>,
    pub default_query: Option<String>,
    pub include: Option<ExportInclude>,
    pub relation_context: Option<ExportRelationContext>,
    pub default_missing_data_policy: Option<ExportMissingDataPolicy>,
    pub default_limits: Option<ExportLimits>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[schema(example = update_export_template_example)]
pub struct UpdateExportTemplate {
    pub collection_id: Option<i32>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub template: Option<String>,
    pub kind: Option<ExportTemplateKind>,
    pub scope_kind: Option<ExportScopeKind>,
    pub class_id: Option<i32>,
    // The nullable export-profile fields use double `Option` so a PATCH can distinguish an
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
    #[schema(value_type = Option<ExportInclude>)]
    pub include: Option<Option<ExportInclude>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_double_option"
    )]
    #[schema(value_type = Option<ExportRelationContext>)]
    pub relation_context: Option<Option<ExportRelationContext>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_double_option"
    )]
    #[schema(value_type = Option<ExportMissingDataPolicy>)]
    pub default_missing_data_policy: Option<Option<ExportMissingDataPolicy>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_double_option"
    )]
    #[schema(value_type = Option<ExportLimits>)]
    pub default_limits: Option<Option<ExportLimits>>,
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
#[diesel(table_name = export_templates)]
pub(crate) struct NewExportTemplateRow {
    collection_id: i32,
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
#[diesel(table_name = export_templates)]
pub(crate) struct UpdateExportTemplateRow {
    collection_id: Option<i32>,
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

impl TryFrom<ExportTemplateRow> for ExportTemplate {
    type Error = ApiError;

    fn try_from(row: ExportTemplateRow) -> Result<Self, Self::Error> {
        Ok(Self {
            id: row.id,
            collection_id: row.collection_id,
            name: row.name,
            description: row.description,
            content_type: ExportContentType::from_mime(&row.content_type)?,
            template: row.template,
            kind: ExportTemplateKind::from_str(&row.kind)?,
            scope_kind: row
                .scope_kind
                .as_deref()
                .map(ExportScopeKind::from_str)
                .transpose()?,
            class_id: row.class_id,
            default_query: row.default_query,
            include: from_optional_json(row.include)?,
            relation_context: from_optional_json(row.relation_context)?,
            default_missing_data_policy: row
                .default_missing_data_policy
                .as_deref()
                .map(ExportMissingDataPolicy::from_str)
                .transpose()?,
            default_limits: from_optional_json(row.default_limits)?,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }
}

impl NewExportTemplate {
    fn into_row(self) -> Result<NewExportTemplateRow, ApiError> {
        let content_type = self.content_type.ensure_template_output()?.as_mime();

        Ok(NewExportTemplateRow {
            collection_id: self.collection_id,
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

impl UpdateExportTemplate {
    fn is_empty(&self) -> bool {
        self.collection_id.is_none()
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

impl ExportTemplate {
    /// Build the export request to execute this template for a given run. Validates that the
    /// template is an executable export and that the run's `object_id` matches the template scope:
    /// `related_objects` requires one, the other scopes reject one, and `class_id` comes from the
    /// template for the class-bound scopes. Runtime values override the template defaults.
    pub fn build_export_request(
        &self,
        run: ExportTemplateRunRequest,
    ) -> Result<ExportRequest, ApiError> {
        if self.kind != ExportTemplateKind::Export {
            return Err(ApiError::BadRequest(
                "Only export templates can be executed".to_string(),
            ));
        }

        let scope_kind = self.scope_kind.ok_or_else(|| {
            ApiError::BadRequest("Executable export template is missing scope_kind".to_string())
        })?;

        let template_class_id = || {
            self.class_id.ok_or_else(|| {
                ApiError::BadRequest("Executable export template is missing class_id".to_string())
            })
        };
        let reject_object_id = || {
            if run.object_id.is_some() {
                return Err(ApiError::BadRequest(format!(
                    "object_id is not accepted for {} export templates",
                    scope_kind.as_str()
                )));
            }
            Ok(())
        };

        let (class_id, object_id) = match scope_kind {
            ExportScopeKind::ObjectsInClass => {
                reject_object_id()?;
                (Some(template_class_id()?), None)
            }
            ExportScopeKind::RelatedObjects => {
                let object_id = run.object_id.ok_or_else(|| {
                    ApiError::BadRequest(
                        "related_objects export templates require object_id".to_string(),
                    )
                })?;
                (Some(template_class_id()?), Some(object_id))
            }
            ExportScopeKind::Collections
            | ExportScopeKind::Classes
            | ExportScopeKind::ClassRelations
            | ExportScopeKind::ObjectRelations => {
                reject_object_id()?;
                (None, None)
            }
        };

        Ok(ExportRequest {
            scope: ExportScope {
                kind: scope_kind,
                class_id,
                object_id,
            },
            query: run.query.or_else(|| self.default_query.clone()),
            missing_data_policy: run.missing_data_policy.or(self.default_missing_data_policy),
            limits: run.limits.or_else(|| self.default_limits.clone()),
            include: self.include.clone(),
            relation_context: self.relation_context.clone(),
        })
    }

    /// The other export templates sharing this template's collection (this template excluded).
    pub async fn collection_siblings(
        &self,
        pool: &DbPool,
    ) -> Result<Vec<ExportTemplate>, ApiError> {
        self.export_templates(pool, Some(self.id)).await
    }

    /// Every export template across all collections.
    pub async fn list_all(pool: &DbPool) -> Result<Vec<ExportTemplate>, ApiError> {
        let rows = backend::load_all_rows(pool).await?;

        rows.into_iter().map(TryInto::try_into).collect()
    }

    /// List export templates (sorted/paginated per `query_options`) together with the total count
    /// matching the filters, scoped to the collections the caller may see.
    pub async fn list_with_total_count(
        pool: &DbPool,
        allowed_collection_ids: &[i32],
        query_options: &QueryOptions,
    ) -> Result<(Vec<ExportTemplate>, i64), ApiError> {
        if allowed_collection_ids.is_empty() {
            return Ok((
                Vec::new(),
                crate::pagination::known_count_or_skipped(query_options, 0),
            ));
        }

        let (rows, total_count) =
            backend::list_rows_with_total_count(pool, allowed_collection_ids, query_options)
                .await?;

        let items = rows
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()?;

        Ok((items, total_count))
    }

    /// List candidates without applying local permission-table visibility.
    /// External authorization backends use this before filtering the rows
    /// against their own policy decisions.
    pub async fn list_candidates(
        pool: &DbPool,
        query_options: &QueryOptions,
    ) -> Result<Vec<ExportTemplate>, ApiError> {
        let (rows, _) = backend::list_all_rows_with_total_count(pool, query_options).await?;
        rows.into_iter().map(TryInto::try_into).collect()
    }
}

/// List the export templates in a value's collection. Available on anything that resolves to a
/// collection via [`CollectionAccessors`] — `CollectionID`, `Collection`, `ExportTemplate`, classes,
/// objects, and so on. For id-only types whose collection must be looked up (e.g.
/// `ExportTemplateID`) this performs that lookup before listing.
///
/// Defined here, rather than in `models::collection`, so the collection layer stays unaware of export
/// templates: the dependency points from this feature module to the core accessor trait.
pub trait CollectionExportTemplates: CollectionAccessors {
    /// The export templates in this value's collection, optionally excluding one template id (so a
    /// template's own row is not treated as a sibling when validating its body against the set).
    async fn export_templates<C>(
        &self,
        backend: &C,
        exclude_template_id: Option<i32>,
    ) -> Result<Vec<ExportTemplate>, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        let collection_id = self.collection_id(backend).await?.id();
        let rows = crate::db::traits::export_template::load_rows_in_collection(
            backend.db_pool(),
            collection_id,
            exclude_template_id,
        )
        .await?;

        rows.into_iter().map(TryInto::try_into).collect()
    }
}

impl<T: CollectionAccessors> CollectionExportTemplates for T {}

impl SaveAdapter for NewExportTemplate {
    type Output = ExportTemplate;

    async fn save_adapter_without_events(&self, pool: &DbPool) -> Result<ExportTemplate, ApiError> {
        self.save_export_template(pool, None).await
    }

    async fn save_adapter(
        &self,
        pool: &DbPool,
        context: &EventContext,
    ) -> Result<ExportTemplate, ApiError> {
        self.save_export_template(pool, Some(context)).await
    }
}

impl NewExportTemplate {
    async fn save_export_template(
        &self,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<ExportTemplate, ApiError> {
        let new_row = self.clone().into_row()?;
        ensure_template_name_is_available(pool, new_row.collection_id, &new_row.name, None).await?;
        validate_export_profile(
            pool,
            new_row.collection_id,
            ExportProfileRef {
                kind: ExportTemplateKind::from_str(&new_row.kind)?,
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
        let collection_templates = CollectionID::new(new_row.collection_id)?
            .export_templates(pool, None)
            .await?;
        validate_template(
            &new_row.name,
            &new_row.template,
            new_row.collection_id,
            &collection_templates,
            ExportContentType::from_mime(&new_row.content_type)?,
        )?;
        let row = new_row.save_export_template_record(pool, context).await?;

        row.try_into()
    }
}

impl UpdateAdapter for UpdateExportTemplate {
    type Output = ExportTemplate;

    async fn update_adapter_without_events(
        &self,
        pool: &DbPool,
        entry_id: i32,
    ) -> Result<ExportTemplate, ApiError> {
        apply_export_template_update(pool, entry_id, self.clone(), None).await
    }

    async fn update_adapter(
        &self,
        pool: &DbPool,
        entry_id: i32,
        context: &EventContext,
    ) -> Result<ExportTemplate, ApiError> {
        apply_export_template_update(pool, entry_id, self.clone(), Some(context)).await
    }
}

async fn apply_export_template_update(
    pool: &DbPool,
    template_id: i32,
    update: UpdateExportTemplate,
    context: Option<&EventContext>,
) -> Result<ExportTemplate, ApiError> {
    let current_row = ExportTemplateID::new(template_id)?
        .load_export_template_record(pool)
        .await?;

    if update.is_empty() {
        return current_row.try_into();
    }

    let current = ExportTemplate::try_from(current_row.clone())?;
    let target_collection_id = update.collection_id.unwrap_or(current.collection_id);
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

    if target_kind == ExportTemplateKind::Fragment && update_export_fields_present(&update) {
        return Err(ApiError::BadRequest(
            "Fragment templates cannot define export execution metadata".to_string(),
        ));
    }

    let ResolvedExportProfile {
        scope_kind: target_scope_kind,
        class_id: target_class_id,
        default_query: target_default_query,
        include: target_include,
        relation_context: target_relation_context,
        default_missing_data_policy: target_default_missing_data_policy,
        default_limits: target_default_limits,
    } = resolve_export_profile(target_kind, update, &current);

    ensure_template_name_is_available(pool, target_collection_id, &target_name, Some(template_id))
        .await?;
    let include_json = to_optional_json(target_include)?;
    let relation_context_json = to_optional_json(target_relation_context)?;
    let default_limits_json = to_optional_json(target_default_limits)?;
    validate_export_profile(
        pool,
        target_collection_id,
        ExportProfileRef {
            kind: target_kind,
            scope_kind: target_scope_kind.map(ExportScopeKind::as_str),
            class_id: target_class_id,
            default_query: target_default_query.as_deref(),
            include: include_json.as_ref(),
            relation_context: relation_context_json.as_ref(),
            default_missing_data_policy: target_default_missing_data_policy
                .map(ExportMissingDataPolicy::as_str),
            default_limits: default_limits_json.as_ref(),
        },
    )
    .await?;
    let collection_templates = CollectionID::new(target_collection_id)?
        .export_templates(pool, Some(template_id))
        .await?;
    validate_template(
        &target_name,
        &target_template,
        target_collection_id,
        &collection_templates,
        current.content_type,
    )?;

    let changeset = UpdateExportTemplateRow {
        collection_id: Some(target_collection_id),
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
    let row = changeset
        .update_export_template_record(pool, template_id, context)
        .await?;

    row.try_into()
}

/// The export-execution metadata resolved for an update, after applying the patch over the current
/// template and reconciling fields against the target scope.
struct ResolvedExportProfile {
    scope_kind: Option<ExportScopeKind>,
    class_id: Option<i32>,
    default_query: Option<String>,
    include: Option<ExportInclude>,
    relation_context: Option<ExportRelationContext>,
    default_missing_data_policy: Option<ExportMissingDataPolicy>,
    default_limits: Option<ExportLimits>,
}

/// Resolve the target export-execution metadata for an update. A fragment clears everything.
/// Otherwise each field is the patch value falling back to the current value, except that fields the
/// target scope cannot hold (class_id/include/relation_context for the collection scopes, include
/// for the non-`objects_in_class` scopes) drop their carried-forward value. An explicitly supplied
/// incompatible value is preserved so `validate_export_profile` rejects it, matching the create path.
fn resolve_export_profile(
    target_kind: ExportTemplateKind,
    update: UpdateExportTemplate,
    current: &ExportTemplate,
) -> ResolvedExportProfile {
    if target_kind == ExportTemplateKind::Fragment {
        return ResolvedExportProfile {
            scope_kind: None,
            class_id: None,
            default_query: None,
            include: None,
            relation_context: None,
            default_missing_data_policy: None,
            default_limits: None,
        };
    }

    let scope_kind = update.scope_kind.or(current.scope_kind);
    let scope_allows_class = scope_kind
        .map(ExportScopeKind::requires_class_id)
        .unwrap_or(false);
    let scope_allows_include = scope_kind == Some(ExportScopeKind::ObjectsInClass);
    let scope_allows_relation_context = matches!(
        scope_kind,
        Some(ExportScopeKind::ObjectsInClass) | Some(ExportScopeKind::RelatedObjects)
    );

    let class_id = if scope_allows_class {
        update.class_id.or(current.class_id)
    } else {
        update.class_id
    };

    ResolvedExportProfile {
        scope_kind,
        class_id,
        default_query: update
            .default_query
            .unwrap_or_else(|| current.default_query.clone()),
        include: resolve_gated_patch(
            update.include,
            current.include.clone(),
            scope_allows_include,
        ),
        relation_context: resolve_gated_patch(
            update.relation_context,
            current.relation_context.clone(),
            scope_allows_relation_context,
        ),
        default_missing_data_policy: update
            .default_missing_data_policy
            .unwrap_or(current.default_missing_data_policy),
        default_limits: update
            .default_limits
            .unwrap_or_else(|| current.default_limits.clone()),
    }
}

impl DeleteAdapter for ExportTemplateID {
    async fn delete_adapter_without_events(&self, pool: &DbPool) -> Result<(), ApiError> {
        self.delete_export_template_record_without_events(pool)
            .await
    }

    async fn delete_adapter(&self, pool: &DbPool, context: &EventContext) -> Result<(), ApiError> {
        self.delete_export_template_record(pool, Some(context))
            .await
    }
}

/// Borrowed view of the export-execution metadata validated together. Bundled so
/// `validate_export_profile` stays within a sensible argument count and both the create and
/// update paths share one shape.
#[derive(Debug, Clone, Copy)]
struct ExportProfileRef<'a> {
    kind: ExportTemplateKind,
    scope_kind: Option<&'a str>,
    class_id: Option<i32>,
    default_query: Option<&'a str>,
    include: Option<&'a serde_json::Value>,
    relation_context: Option<&'a serde_json::Value>,
    default_missing_data_policy: Option<&'a str>,
    default_limits: Option<&'a serde_json::Value>,
}

#[derive(Clone, Copy)]
pub(crate) struct ExportTemplateImportRef<'a> {
    pub name: &'a str,
    pub template: &'a str,
    pub content_type: ExportContentType,
    pub kind: ExportTemplateKind,
    pub scope_kind: Option<ExportScopeKind>,
    pub has_class: bool,
    pub default_query: Option<&'a str>,
    pub include: Option<&'a ExportInclude>,
    pub relation_context: Option<&'a ExportRelationContext>,
    pub default_missing_data_policy: Option<ExportMissingDataPolicy>,
    pub default_limits: Option<&'a ExportLimits>,
}

pub(crate) fn validate_import_export_template(
    input: ExportTemplateImportRef<'_>,
) -> Result<(), ApiError> {
    input.content_type.ensure_template_output()?;
    let include = input.include.map(serde_json::to_value).transpose()?;
    let relation_context = input
        .relation_context
        .map(serde_json::to_value)
        .transpose()?;
    let default_limits = input.default_limits.map(serde_json::to_value).transpose()?;
    validate_export_profile_shape(&ExportProfileRef {
        kind: input.kind,
        scope_kind: input.scope_kind.map(ExportScopeKind::as_str),
        class_id: input.has_class.then_some(1),
        default_query: input.default_query,
        include: include.as_ref(),
        relation_context: relation_context.as_ref(),
        default_missing_data_policy: input
            .default_missing_data_policy
            .map(ExportMissingDataPolicy::as_str),
        default_limits: default_limits.as_ref(),
    })?;
    validate_template_syntax(input.name, input.template)
}

pub(crate) fn validate_import_export_template_composition(
    input: ExportTemplateImportRef<'_>,
    collection_templates: &[(String, String)],
) -> Result<(), ApiError> {
    validate_import_export_template(input)?;
    validate_template_sources(
        input.name,
        input.template,
        collection_templates,
        input.content_type,
    )
}

async fn validate_export_profile(
    pool: &DbPool,
    target_collection_id: i32,
    profile: ExportProfileRef<'_>,
) -> Result<(), ApiError> {
    if let Some(class_id) = validate_export_profile_shape(&profile)? {
        ensure_template_class_in_collection(pool, target_collection_id, class_id).await?;
    }
    Ok(())
}

fn validate_export_profile_shape(profile: &ExportProfileRef<'_>) -> Result<Option<i32>, ApiError> {
    let class_id = match profile.kind {
        ExportTemplateKind::Fragment => {
            validate_fragment_metadata(profile)?;
            None
        }
        ExportTemplateKind::Export => validate_export_scope_metadata(profile)?,
    };
    validate_common_profile_fields(profile)?;
    Ok(class_id)
}

/// Fragments are reusable partials with no execution metadata.
fn validate_fragment_metadata(profile: &ExportProfileRef<'_>) -> Result<(), ApiError> {
    if profile.scope_kind.is_some() || profile.class_id.is_some() {
        return Err(ApiError::BadRequest(
            "Fragment templates cannot define export execution metadata".to_string(),
        ));
    }

    Ok(())
}

/// Validate the scope/class binding of an executable export template.
fn validate_export_scope_metadata(profile: &ExportProfileRef<'_>) -> Result<Option<i32>, ApiError> {
    let scope_kind = profile
        .scope_kind
        .ok_or_else(|| ApiError::BadRequest("Export templates require scope_kind".into()))
        .and_then(ExportScopeKind::from_str)?;

    // `objects_in_class` and `related_objects` are scoped to a single class and require
    // `class_id`; the collection scopes (`collections`, `classes`, `class_relations`,
    // `object_relations`) are class-agnostic and must not set it.
    let class_id = if scope_kind.requires_class_id() {
        let class_id = profile.class_id.ok_or_else(|| {
            ApiError::BadRequest(format!(
                "Export templates with scope '{}' require class_id",
                scope_kind.as_str()
            ))
        })?;
        if class_id <= 0 {
            return Err(ApiError::BadRequest(
                "Export template class_id must be greater than 0".to_string(),
            ));
        }
        Some(class_id)
    } else if profile.class_id.is_some() {
        return Err(ApiError::BadRequest(format!(
            "Export templates with scope '{}' must not set class_id",
            scope_kind.as_str()
        )));
    } else {
        None
    };

    if let Some(query) = profile.default_query {
        parse_query_parameter(query)?;
    }

    if profile.include.is_some() && scope_kind != ExportScopeKind::ObjectsInClass {
        return Err(ApiError::BadRequest(
            "include is only supported for objects_in_class export templates".to_string(),
        ));
    }

    if profile.relation_context.is_some()
        && !matches!(
            scope_kind,
            ExportScopeKind::ObjectsInClass | ExportScopeKind::RelatedObjects
        )
    {
        return Err(ApiError::BadRequest(
            "relation_context is only supported for objects_in_class and related_objects export templates"
                .to_string(),
        ));
    }

    Ok(class_id)
}

/// Validate the profile fields whose rules are the same for every kind/scope: the
/// include/relation_context exclusivity and the shape of each optional blob.
fn validate_common_profile_fields(profile: &ExportProfileRef<'_>) -> Result<(), ApiError> {
    if profile.include.is_some() && profile.relation_context.is_some() {
        return Err(ApiError::BadRequest(
            "include cannot be combined with relation_context".to_string(),
        ));
    }

    if let Some(include) = profile.include {
        let include: ExportInclude = serde_json::from_value(include.clone())?;
        include.validate_related_objects()?;
    }
    if let Some(relation_context) = profile.relation_context {
        let context: ExportRelationContext = serde_json::from_value(relation_context.clone())?;
        if let Some(depth) = context.depth
            && !(1..=2).contains(&depth)
        {
            return Err(ApiError::BadRequest(
                "Templated relation hydration only supports depth 1 or 2".to_string(),
            ));
        }
    }
    if let Some(policy) = profile.default_missing_data_policy {
        ExportMissingDataPolicy::from_str(policy)?;
    }
    if let Some(limits) = profile.default_limits {
        let _limits: ExportLimits = serde_json::from_value(limits.clone())?;
    }

    Ok(())
}

async fn ensure_template_class_in_collection(
    pool: &DbPool,
    target_collection_id: i32,
    target_class_id: i32,
) -> Result<(), ApiError> {
    let class_collection_id = backend::class_collection_id(pool, target_class_id)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("Class {target_class_id} not found")))?;

    if class_collection_id != target_collection_id {
        return Err(ApiError::BadRequest(format!(
            "Export template class {target_class_id} belongs to collection {class_collection_id}, not template collection {target_collection_id}"
        )));
    }

    Ok(())
}

fn update_export_fields_present(update: &UpdateExportTemplate) -> bool {
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
/// so `validate_export_profile` can reject it (matching the create path).
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
    target_collection_id: i32,
    target_name: &str,
    exclude_template_id: Option<i32>,
) -> Result<(), ApiError> {
    let conflict =
        backend::name_conflict_exists(pool, target_collection_id, target_name, exclude_template_id)
            .await?;

    if conflict {
        return Err(ApiError::Conflict(format!(
            "Template name '{}' already exists in collection {}",
            target_name, target_collection_id
        )));
    }

    Ok(())
}

impl IdAccessor for ExportTemplate {
    fn accessor_id(&self) -> i32 {
        self.id
    }
}

impl InstanceAdapter<ExportTemplate> for ExportTemplate {
    async fn instance_adapter(&self, _pool: &DbPool) -> Result<ExportTemplate, ApiError> {
        Ok(self.clone())
    }
}

impl IdAccessor for ExportTemplateID {
    fn accessor_id(&self) -> i32 {
        self.0
    }
}

impl InstanceAdapter<ExportTemplate> for ExportTemplateID {
    async fn instance_adapter(&self, pool: &DbPool) -> Result<ExportTemplate, ApiError> {
        self.load_export_template_record(pool).await?.try_into()
    }
}

impl CollectionAdapter for ExportTemplate {
    async fn collection_adapter(&self, pool: &DbPool) -> Result<Collection, ApiError> {
        CollectionID::new(self.collection_id)?
            .collection_adapter(pool)
            .await
    }

    async fn collection_id_adapter(&self, _pool: &DbPool) -> Result<CollectionID, ApiError> {
        CollectionID::new(self.collection_id)
    }
}

impl CollectionAdapter for ExportTemplateID {
    async fn collection_adapter(&self, pool: &DbPool) -> Result<Collection, ApiError> {
        self.collection_id_adapter(pool)
            .await?
            .collection_adapter(pool)
            .await
    }

    async fn collection_id_adapter(&self, pool: &DbPool) -> Result<CollectionID, ApiError> {
        self.lookup_export_template_collection_id(pool).await
    }
}

impl CursorPaginated for ExportTemplate {
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
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        Ok(match field {
            FilterField::Id => CursorValue::Integer(self.id as i64),
            FilterField::Name => CursorValue::String(self.name.clone()),
            FilterField::Description => CursorValue::String(self.description.clone()),
            FilterField::Collections | FilterField::CollectionId => {
                CursorValue::Integer(self.collection_id as i64)
            }
            FilterField::CreatedAt => CursorValue::DateTime(self.created_at),
            FilterField::UpdatedAt => CursorValue::DateTime(self.updated_at),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for export templates",
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

impl CursorSqlMapping for ExportTemplate {
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
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for export templates",
                    field
                )));
            }
        })
    }
}

fn export_template_example() -> ExportTemplate {
    let example_timestamp = chrono::NaiveDate::from_ymd_opt(2026, 3, 6)
        .and_then(|date| date.and_hms_opt(12, 0, 0))
        .expect("static OpenAPI example timestamp must be valid");

    ExportTemplate {
        id: 1,
        collection_id: 7,
        name: "owner-export".to_string(),
        description: "Template for owner listing".to_string(),
        content_type: ExportContentType::TextPlain,
        template: "{% for item in items %}{{ item.name }}={{ item.data.owner }}\n{% endfor %}"
            .to_string(),
        kind: ExportTemplateKind::Export,
        scope_kind: Some(ExportScopeKind::ObjectsInClass),
        class_id: Some(42),
        default_query: Some("sort=name".to_string()),
        include: None,
        relation_context: None,
        default_missing_data_policy: Some(ExportMissingDataPolicy::Strict),
        default_limits: Some(ExportLimits {
            max_items: Some(100),
            max_output_bytes: Some(262_144),
        }),
        created_at: example_timestamp,
        updated_at: example_timestamp,
    }
}

fn new_export_template_example() -> NewExportTemplate {
    NewExportTemplate {
        collection_id: 7,
        name: "owner-export".to_string(),
        description: "Template for owner listing".to_string(),
        content_type: ExportContentType::TextPlain,
        template: "{% for item in items %}{{ item.name }}={{ item.data.owner }}\n{% endfor %}"
            .to_string(),
        kind: ExportTemplateKind::Export,
        scope_kind: Some(ExportScopeKind::ObjectsInClass),
        class_id: Some(42),
        default_query: Some("sort=name".to_string()),
        include: None,
        relation_context: None,
        default_missing_data_policy: Some(ExportMissingDataPolicy::Strict),
        default_limits: Some(ExportLimits {
            max_items: Some(100),
            max_output_bytes: Some(262_144),
        }),
    }
}

fn update_export_template_example() -> UpdateExportTemplate {
    UpdateExportTemplate {
        collection_id: Some(9),
        name: Some("owner-export-v2".to_string()),
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

#[derive(serde::Serialize, diesel::Queryable, Clone, Debug, ToSchema)]
#[diesel(table_name = crate::schema::export_templates_history)]
pub struct ExportTemplateHistory {
    pub id: i32,
    pub collection_id: i32,
    pub name: String,
    pub description: String,
    pub content_type: String,
    pub template: String,
    pub kind: String,
    pub scope_kind: Option<String>,
    pub class_id: Option<i32>,
    pub default_query: Option<String>,
    pub include: Option<serde_json::Value>,
    pub relation_context: Option<serde_json::Value>,
    pub default_missing_data_policy: Option<String>,
    pub default_limits: Option<serde_json::Value>,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub op: String,
    pub valid_from: chrono::DateTime<chrono::Utc>,
    pub valid_to: Option<chrono::DateTime<chrono::Utc>>,
    pub actor_id: Option<i32>,
    pub history_id: i64,
}

crate::impl_history_pagination!(ExportTemplateHistory, "export_templates_history");

#[async_trait]
impl AuthzTarget for ExportTemplate {
    async fn to_resource_ref(&self, _pool: &DbPool) -> Result<ResourceRef, ApiError> {
        Ok(ResourceRef {
            kind: ResourceKind::Template,
            id: self.id,
            attrs: ResourceAttrs {
                collection_id: Some(self.collection_id),
                name: Some(self.name.clone()),
                ..Default::default()
            },
        })
    }
}

#[async_trait]
impl AuthzTarget for ExportTemplateID {
    async fn to_resource_ref(&self, pool: &DbPool) -> Result<ResourceRef, ApiError> {
        self.instance(pool).await?.to_resource_ref(pool).await
    }
}
