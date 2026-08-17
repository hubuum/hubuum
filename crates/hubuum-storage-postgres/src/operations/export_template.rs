use chrono::NaiveDateTime;
use diesel::prelude::{ExpressionMethods, QueryDsl};
use diesel::{AsChangeset, Insertable, OptionalExtension, Queryable, Selectable};
use diesel_async::RunQueryDsl;
use hubuum_events_core::{Action, EntityType, EventContext, NewEvent};
use hubuum_query::{FilterField, QueryOptions, SortParam};
use hubuum_storage_core::{
    StorageExportTemplate, StorageExportTemplateCreate, StorageExportTemplateDefinition,
    StorageExportTemplateDelete, StorageExportTemplateListQuery, StorageExportTemplatePage,
    StorageExportTemplateReplace,
};
use serde_json::{Value, json};

use crate::cursor::{CursorSqlField, CursorSqlType};
use crate::revision::{RevisionOwner, record_metadata};
use crate::runtime::assert_locked_revision_precondition;
use crate::{PostgresConnection, PostgresRevision, PostgresRuntime, PostgresStorageError};

use super::event_record::append_event;

#[derive(Clone, Queryable, Selectable)]
#[diesel(table_name = crate::schema::export_templates)]
struct ExportTemplateRow {
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
    include: Option<Value>,
    relation_context: Option<Value>,
    default_missing_data_policy: Option<String>,
    default_limits: Option<Value>,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    revision: PostgresRevision,
}

impl ExportTemplateRow {
    fn into_storage(self) -> StorageExportTemplate {
        StorageExportTemplate::new(
            record_metadata(self.id, self.created_at, self.updated_at, self.revision),
            self.collection_id,
            self.name,
            StorageExportTemplateDefinition::new(
                self.description,
                self.content_type,
                self.template,
                self.kind,
            )
            .with_scope(self.scope_kind, self.class_id)
            .with_default_query(self.default_query)
            .with_include(self.include)
            .with_relation_context(self.relation_context)
            .with_default_missing_data_policy(self.default_missing_data_policy)
            .with_default_limits(self.default_limits),
        )
    }

    fn audit_snapshot(&self) -> Value {
        json!({
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

#[derive(Insertable)]
#[diesel(table_name = crate::schema::export_templates)]
struct NewExportTemplateRow {
    collection_id: i32,
    name: String,
    description: String,
    content_type: String,
    template: String,
    kind: String,
    scope_kind: Option<String>,
    class_id: Option<i32>,
    default_query: Option<String>,
    include: Option<Value>,
    relation_context: Option<Value>,
    default_missing_data_policy: Option<String>,
    default_limits: Option<Value>,
}

impl NewExportTemplateRow {
    fn from_request(request: StorageExportTemplateCreate) -> (Self, Option<EventContext>) {
        let (collection_id, name, definition, event_context) = request.into_parts();
        let definition = ExportTemplateDefinitionParts::from(definition);
        (
            Self {
                collection_id,
                name,
                description: definition.description,
                content_type: definition.content_type,
                template: definition.template,
                kind: definition.kind,
                scope_kind: definition.scope_kind,
                class_id: definition.class_id,
                default_query: definition.default_query,
                include: definition.include,
                relation_context: definition.relation_context,
                default_missing_data_policy: definition.default_missing_data_policy,
                default_limits: definition.default_limits,
            },
            event_context,
        )
    }
}

#[derive(AsChangeset)]
#[diesel(table_name = crate::schema::export_templates)]
struct ReplaceExportTemplateRow {
    collection_id: i32,
    name: String,
    description: String,
    template: String,
    kind: String,
    scope_kind: Option<Option<String>>,
    class_id: Option<Option<i32>>,
    default_query: Option<Option<String>>,
    include: Option<Option<Value>>,
    relation_context: Option<Option<Value>>,
    default_missing_data_policy: Option<Option<String>>,
    default_limits: Option<Option<Value>>,
}

impl ReplaceExportTemplateRow {
    fn from_request(request: StorageExportTemplateReplace) -> (i32, Self, Option<EventContext>) {
        let (template_id, collection_id, name, definition, event_context) = request.into_parts();
        let definition = ExportTemplateDefinitionParts::from(definition);
        (
            template_id,
            Self {
                collection_id,
                name,
                description: definition.description,
                template: definition.template,
                kind: definition.kind,
                scope_kind: Some(definition.scope_kind),
                class_id: Some(definition.class_id),
                default_query: Some(definition.default_query),
                include: Some(definition.include),
                relation_context: Some(definition.relation_context),
                default_missing_data_policy: Some(definition.default_missing_data_policy),
                default_limits: Some(definition.default_limits),
            },
            event_context,
        )
    }

    fn has_changes(&self, current: &ExportTemplateRow) -> bool {
        self.collection_id != current.collection_id
            || self.name != current.name
            || self.description != current.description
            || self.template != current.template
            || self.kind != current.kind
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

struct ExportTemplateDefinitionParts {
    description: String,
    content_type: String,
    template: String,
    kind: String,
    scope_kind: Option<String>,
    class_id: Option<i32>,
    default_query: Option<String>,
    include: Option<Value>,
    relation_context: Option<Value>,
    default_missing_data_policy: Option<String>,
    default_limits: Option<Value>,
}

impl From<StorageExportTemplateDefinition> for ExportTemplateDefinitionParts {
    fn from(definition: StorageExportTemplateDefinition) -> Self {
        let (
            description,
            content_type,
            template,
            kind,
            scope_kind,
            class_id,
            default_query,
            include,
            relation_context,
            default_missing_data_policy,
            default_limits,
        ) = definition.into_parts();
        Self {
            description,
            content_type,
            template,
            kind,
            scope_kind,
            class_id,
            default_query,
            include,
            relation_context,
            default_missing_data_policy,
            default_limits,
        }
    }
}

pub async fn get_export_template(
    runtime: &PostgresRuntime,
    template_id: i32,
) -> Result<StorageExportTemplate, PostgresStorageError> {
    ensure_positive_id(template_id, "Export template")?;
    runtime
        .with_connection(async |connection| {
            load_export_template_row(connection, template_id)
                .await
                .map(ExportTemplateRow::into_storage)
        })
        .await
}

pub async fn list_export_templates(
    runtime: &PostgresRuntime,
    query: StorageExportTemplateListQuery,
) -> Result<StorageExportTemplatePage, PostgresStorageError> {
    let (collection_ids, options) = query.into_parts();
    let options = normalize_query_options(options)?;
    if collection_ids.as_ref().is_some_and(Vec::is_empty) {
        return Ok(StorageExportTemplatePage::new(
            Vec::new(),
            options.include_total().then_some(0),
        ));
    }

    if options.include_total() {
        runtime
            .with_read_only_snapshot(async |connection| {
                let total = build_list_query(collection_ids.as_deref(), &options)?
                    .count()
                    .get_result::<i64>(connection)
                    .await?;
                let templates =
                    load_export_template_rows(connection, collection_ids.as_deref(), &options)
                        .await?;
                Ok::<_, PostgresStorageError>(StorageExportTemplatePage::new(
                    templates,
                    Some(total),
                ))
            })
            .await
    } else {
        runtime
            .with_connection(async |connection| {
                let templates =
                    load_export_template_rows(connection, collection_ids.as_deref(), &options)
                        .await?;
                Ok::<_, PostgresStorageError>(StorageExportTemplatePage::new(templates, None))
            })
            .await
    }
}

pub async fn list_export_templates_in_collection(
    runtime: &PostgresRuntime,
    collection_id: i32,
    exclude_template_id: Option<i32>,
) -> Result<Vec<StorageExportTemplate>, PostgresStorageError> {
    ensure_positive_id(collection_id, "Collection")?;
    if let Some(template_id) = exclude_template_id {
        ensure_positive_id(template_id, "Export template")?;
    }
    runtime
        .with_connection(async |connection| {
            use crate::schema::export_templates::dsl::{
                collection_id as row_collection_id, export_templates, id,
            };

            let mut query = export_templates
                .filter(row_collection_id.eq(collection_id))
                .into_boxed();
            if let Some(template_id) = exclude_template_id {
                query = query.filter(id.ne(template_id));
            }
            query
                .order_by(id.asc())
                .load::<ExportTemplateRow>(connection)
                .await
                .map_err(PostgresStorageError::from)
                .map(|rows| {
                    rows.into_iter()
                        .map(ExportTemplateRow::into_storage)
                        .collect()
                })
        })
        .await
}

pub async fn export_template_class_collection_id(
    runtime: &PostgresRuntime,
    class_id: i32,
) -> Result<Option<i32>, PostgresStorageError> {
    ensure_positive_id(class_id, "Class")?;
    runtime
        .with_connection(async |connection| {
            use crate::schema::hubuumclass::dsl::{collection_id, hubuumclass, id};

            hubuumclass
                .filter(id.eq(class_id))
                .select(collection_id)
                .first::<i32>(connection)
                .await
                .optional()
                .map_err(PostgresStorageError::from)
        })
        .await
}

pub async fn create_export_template(
    runtime: &PostgresRuntime,
    request: StorageExportTemplateCreate,
) -> Result<StorageExportTemplate, PostgresStorageError> {
    let (new_template, event_context) = NewExportTemplateRow::from_request(request);
    ensure_positive_id(new_template.collection_id, "Collection")?;
    if let Some(context) = event_context {
        runtime
            .with_transaction(async |connection| {
                use crate::schema::export_templates::dsl::export_templates;

                let created = diesel::insert_into(export_templates)
                    .values(new_template)
                    .get_result::<ExportTemplateRow>(connection)
                    .await?;
                append_export_template_audit(connection, Action::Created, &context, None, &created)
                    .await?;
                Ok::<_, PostgresStorageError>(created.into_storage())
            })
            .await
    } else {
        runtime
            .with_connection(async |connection| {
                use crate::schema::export_templates::dsl::export_templates;

                diesel::insert_into(export_templates)
                    .values(new_template)
                    .get_result::<ExportTemplateRow>(connection)
                    .await
                    .map(ExportTemplateRow::into_storage)
                    .map_err(PostgresStorageError::from)
            })
            .await
    }
}

pub async fn replace_export_template(
    runtime: &PostgresRuntime,
    request: StorageExportTemplateReplace,
) -> Result<StorageExportTemplate, PostgresStorageError> {
    let (template_id, replacement, event_context) = ReplaceExportTemplateRow::from_request(request);
    ensure_positive_id(template_id, "Export template")?;
    ensure_positive_id(replacement.collection_id, "Collection")?;
    if let Some(context) = event_context {
        runtime
            .with_transaction(async |connection| {
                use crate::schema::export_templates::dsl::{export_templates, id};

                let before = export_templates
                    .filter(id.eq(template_id))
                    .for_update()
                    .first::<ExportTemplateRow>(connection)
                    .await?;
                assert_locked_revision_precondition(
                    connection,
                    &RevisionOwner::ExportTemplate.key(before.id),
                    before.revision,
                )
                .await?;
                if !replacement.has_changes(&before) {
                    return Ok(before.into_storage());
                }
                let updated = diesel::update(export_templates.filter(id.eq(template_id)))
                    .set(replacement)
                    .get_result::<ExportTemplateRow>(connection)
                    .await?;
                append_export_template_audit(
                    connection,
                    Action::Updated,
                    &context,
                    Some(&before),
                    &updated,
                )
                .await?;
                Ok::<_, PostgresStorageError>(updated.into_storage())
            })
            .await
    } else {
        runtime
            .with_connection(async |connection| {
                use crate::schema::export_templates::dsl::{export_templates, id};

                diesel::update(export_templates.filter(id.eq(template_id)))
                    .set(replacement)
                    .get_result::<ExportTemplateRow>(connection)
                    .await
                    .map(ExportTemplateRow::into_storage)
                    .map_err(PostgresStorageError::from)
            })
            .await
    }
}

pub async fn delete_export_template(
    runtime: &PostgresRuntime,
    request: StorageExportTemplateDelete,
) -> Result<(), PostgresStorageError> {
    let (template_id, event_context) = request.into_parts();
    ensure_positive_id(template_id, "Export template")?;
    if let Some(context) = event_context {
        runtime
            .with_transaction(async |connection| {
                use crate::schema::export_templates::dsl::{export_templates, id};

                let before = export_templates
                    .filter(id.eq(template_id))
                    .for_update()
                    .first::<ExportTemplateRow>(connection)
                    .await?;
                assert_locked_revision_precondition(
                    connection,
                    &RevisionOwner::ExportTemplate.key(before.id),
                    before.revision,
                )
                .await?;
                diesel::delete(export_templates.filter(id.eq(template_id)))
                    .execute(connection)
                    .await?;
                append_export_template_audit(
                    connection,
                    Action::Deleted,
                    &context,
                    Some(&before),
                    &before,
                )
                .await
            })
            .await
    } else {
        runtime
            .with_connection(async |connection| {
                use crate::schema::export_templates::dsl::{export_templates, id};

                diesel::delete(export_templates.filter(id.eq(template_id)))
                    .execute(connection)
                    .await
                    .map(|_| ())
                    .map_err(PostgresStorageError::from)
            })
            .await
    }
}

async fn load_export_template_row(
    connection: &mut PostgresConnection,
    template_id: i32,
) -> Result<ExportTemplateRow, PostgresStorageError> {
    use crate::schema::export_templates::dsl::{export_templates, id};

    export_templates
        .filter(id.eq(template_id))
        .first::<ExportTemplateRow>(connection)
        .await
        .map_err(PostgresStorageError::from)
}

async fn load_export_template_rows(
    connection: &mut PostgresConnection,
    collection_ids: Option<&[i32]>,
    options: &QueryOptions,
) -> Result<Vec<StorageExportTemplate>, PostgresStorageError> {
    let mut records = build_list_query(collection_ids, options)?;
    let fields = options
        .sort()
        .iter()
        .map(|sort| export_template_cursor_field(&sort.field))
        .collect::<Result<Vec<_>, _>>()?;
    crate::apply_query_options_with_fields!(records, options, fields);
    records
        .load::<ExportTemplateRow>(connection)
        .await
        .map_err(PostgresStorageError::from)
        .map(|rows| {
            rows.into_iter()
                .map(ExportTemplateRow::into_storage)
                .collect()
        })
}

fn build_list_query<'a>(
    collection_ids: Option<&'a [i32]>,
    options: &'a QueryOptions,
) -> Result<crate::schema::export_templates::BoxedQuery<'a, diesel::pg::Pg>, PostgresStorageError> {
    use crate::schema::export_templates::dsl::{
        class_id, collection_id, created_at, description, export_templates, id, kind, name,
        revision, updated_at,
    };

    let mut query = export_templates.into_boxed();
    if let Some(collection_ids) = collection_ids {
        query = query.filter(collection_id.eq_any(collection_ids));
    }
    for parameter in options.filters() {
        match parameter.field {
            FilterField::Id => crate::postgres_integer_filter!(query, parameter, id),
            FilterField::Name => crate::postgres_string_filter!(query, parameter, name),
            FilterField::Description => {
                crate::postgres_string_filter!(query, parameter, description)
            }
            FilterField::CollectionId | FilterField::Collections => {
                crate::postgres_integer_filter!(query, parameter, collection_id)
            }
            FilterField::Kind => crate::postgres_string_filter!(query, parameter, kind),
            FilterField::ClassId => {
                crate::postgres_integer_filter!(query, parameter, class_id)
            }
            FilterField::CreatedAt => {
                crate::postgres_datetime_filter!(query, parameter, created_at)
            }
            FilterField::UpdatedAt => {
                crate::postgres_datetime_filter!(query, parameter, updated_at)
            }
            FilterField::Revision => {
                crate::postgres_revision_filter!(query, parameter, revision)
            }
            _ => {
                return Err(PostgresStorageError::bad_request(format!(
                    "Field '{}' isn't searchable for export templates",
                    parameter.field
                )));
            }
        }
    }
    Ok(query)
}

fn normalize_query_options(
    mut options: QueryOptions,
) -> Result<QueryOptions, PostgresStorageError> {
    if options.sort().is_empty() {
        options.set_sort(
            vec![SortParam {
                field: FilterField::Id,
                descending: false,
            }]
            .try_into()
            .map_err(|error: hubuum_query::QueryError| {
                PostgresStorageError::bad_request(error.to_string())
            })?,
        );
    }
    for sort in options.sort() {
        export_template_cursor_field(&sort.field)?;
    }
    if !options
        .sort()
        .iter()
        .any(|sort| sort.field == FilterField::Id)
    {
        options
            .sort_mut()
            .append_tie_breaker(SortParam {
                field: FilterField::Id,
                descending: false,
            })
            .map_err(|error| PostgresStorageError::bad_request(error.to_string()))?;
    }
    Ok(options)
}

fn export_template_cursor_field(
    field: &FilterField,
) -> Result<CursorSqlField, PostgresStorageError> {
    Ok(match field {
        FilterField::Id => cursor_field("export_templates.id", CursorSqlType::Integer),
        FilterField::Name => cursor_field("export_templates.name", CursorSqlType::String),
        FilterField::Description => {
            cursor_field("export_templates.description", CursorSqlType::String)
        }
        FilterField::CollectionId | FilterField::Collections => {
            cursor_field("export_templates.collection_id", CursorSqlType::Integer)
        }
        FilterField::CreatedAt => {
            cursor_field("export_templates.created_at", CursorSqlType::DateTime)
        }
        FilterField::UpdatedAt => {
            cursor_field("export_templates.updated_at", CursorSqlType::DateTime)
        }
        FilterField::Revision => cursor_field("export_templates.revision", CursorSqlType::BigInt),
        _ => {
            return Err(PostgresStorageError::bad_request(format!(
                "Field '{field}' is not orderable for export templates"
            )));
        }
    })
}

const fn cursor_field(column: &'static str, sql_type: CursorSqlType) -> CursorSqlField {
    CursorSqlField {
        column,
        sql_type,
        nullable: false,
    }
}

async fn append_export_template_audit(
    connection: &mut PostgresConnection,
    action: Action,
    context: &EventContext,
    before: Option<&ExportTemplateRow>,
    after: &ExportTemplateRow,
) -> Result<(), PostgresStorageError> {
    let event = NewEvent::new(
        EntityType::ExportTemplate,
        action,
        context.actor_kind(),
        format!("Export template '{}' {}", after.name, action.as_str()),
    )
    .map_err(|error| PostgresStorageError::database(error.to_string()))?
    .with_context(context)
    .with_entity_id(hubuum_events_core::EventEntityId::new(after.id)?)
    .with_entity_name(&after.name)
    .with_collection_id(hubuum_domain::CollectionId::new(after.collection_id)?)
    .with_before_opt(before.map(ExportTemplateRow::audit_snapshot))
    .with_after_opt((action != Action::Deleted).then(|| after.audit_snapshot()));
    append_event(connection, &event).await.map(|_| ())
}

fn ensure_positive_id(id: i32, entity: &str) -> Result<(), PostgresStorageError> {
    if id > 0 {
        Ok(())
    } else {
        Err(PostgresStorageError::bad_request(format!(
            "{entity} id must be greater than zero"
        )))
    }
}
