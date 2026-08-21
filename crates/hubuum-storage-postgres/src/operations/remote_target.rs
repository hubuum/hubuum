use std::fmt;

use chrono::NaiveDateTime;
use diesel::prelude::{ExpressionMethods, QueryDsl};
use diesel::{AsChangeset, Insertable, Queryable, Selectable};
use diesel_async::RunQueryDsl;
use hubuum_domain::{ClassId, CollectionId};
use hubuum_events_core::{Action, EntityType, EventContext, NewEvent};
use hubuum_query::{FilterField, QueryOptions};
use hubuum_storage_core::{
    AuditReceipt, MutationOutcome, StoragePage, StorageRemoteHttpMethod, StorageRemoteTarget,
    StorageRemoteTargetCreate, StorageRemoteTargetDefinition, StorageRemoteTargetDelete,
    StorageRemoteTargetInvocation, StorageRemoteTargetListQuery, StorageRemoteTargetPatch,
    StorageRemoteTargetPolicy, StorageRemoteTargetSubjectType, StorageRemoteTargetTransport,
    StorageRemoteTargetTransportParts, StorageRemoteTargetUpdate,
};
use serde_json::{Value, json};

use crate::cursor::{CursorSqlField, CursorSqlType};
use crate::revision::{RevisionOwner, record_metadata};
use crate::runtime::assert_locked_revision_precondition;
use crate::{PostgresConnection, PostgresRevision, PostgresRuntime, PostgresStorageError};

use super::event_record::append_event;

macro_rules! impl_redacted_remote_target_debug {
    ($target:ty, $($field:ident),+ $(,)?) => {
        impl fmt::Debug for $target {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                let mut debug = formatter.debug_struct(stringify!($target));
                $(debug.field(stringify!($field), &self.$field);)+
                debug
                    .field("configuration", &"<redacted>")
                    .finish()
            }
        }
    };
}

#[derive(Clone, Queryable, Selectable)]
#[diesel(table_name = crate::schema::remote_targets)]
struct RemoteTargetRow {
    id: i32,
    collection_id: i32,
    class_id: Option<i32>,
    name: String,
    description: String,
    method: String,
    url_template: String,
    headers_template: Value,
    body_template: Option<String>,
    auth_config: Value,
    allowed_subject_types: Value,
    timeout_ms: i32,
    enabled: bool,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    revision: PostgresRevision,
}

impl_redacted_remote_target_debug!(
    RemoteTargetRow,
    id,
    collection_id,
    class_id,
    name,
    description,
    method,
    allowed_subject_types,
    timeout_ms,
    enabled,
    created_at,
    updated_at,
    revision,
);

impl RemoteTargetRow {
    fn into_storage(self) -> Result<StorageRemoteTarget, PostgresStorageError> {
        let allowed_subject_types = decode_subject_types(self.allowed_subject_types)?;
        Ok(StorageRemoteTarget::new(
            record_metadata(self.id, self.created_at, self.updated_at, self.revision)?,
            CollectionId::new(self.collection_id)?,
            self.name,
            StorageRemoteTargetDefinition::new(
                self.description,
                StorageRemoteTargetTransport::try_new(
                    decode_http_method(&self.method)?,
                    self.url_template,
                    self.headers_template,
                    self.body_template,
                    self.auth_config,
                    self.timeout_ms,
                )?,
                StorageRemoteTargetPolicy::try_new(
                    self.class_id.map(ClassId::new).transpose()?,
                    allowed_subject_types,
                    self.enabled,
                )?,
            ),
        ))
    }

    fn audit_snapshot(&self) -> Value {
        json!({
            "id": self.id,
            "collection_id": self.collection_id,
            "class_id": self.class_id,
            "name": self.name,
            "description": self.description,
            "method": self.method,
            "url_template": self.url_template,
            "headers_template": self.headers_template,
            "body_template": self.body_template,
            "auth_config": "<redacted>",
            "allowed_subject_types": self.allowed_subject_types,
            "timeout_ms": self.timeout_ms,
            "enabled": self.enabled,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "revision": self.revision,
        })
    }
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::remote_targets)]
struct NewRemoteTargetRow {
    collection_id: i32,
    class_id: Option<i32>,
    name: String,
    description: String,
    method: String,
    url_template: String,
    headers_template: Value,
    body_template: Option<String>,
    auth_config: Value,
    allowed_subject_types: Value,
    timeout_ms: i32,
    enabled: bool,
}

impl_redacted_remote_target_debug!(
    NewRemoteTargetRow,
    collection_id,
    class_id,
    name,
    description,
    method,
    allowed_subject_types,
    timeout_ms,
    enabled,
);

impl NewRemoteTargetRow {
    fn from_request(
        request: StorageRemoteTargetCreate,
    ) -> Result<(Self, EventContext), PostgresStorageError> {
        let (collection_id, name, definition, event_context) = request.into_parts();
        let definition = RemoteTargetDefinitionParts::from_definition(definition)?;
        Ok((
            Self {
                collection_id: collection_id.id(),
                class_id: definition.class_id,
                name,
                description: definition.description,
                method: definition.method,
                url_template: definition.url_template,
                headers_template: definition.headers_template,
                body_template: definition.body_template,
                auth_config: definition.auth_config,
                allowed_subject_types: definition.allowed_subject_types,
                timeout_ms: definition.timeout_ms,
                enabled: definition.enabled,
            },
            event_context,
        ))
    }
}

#[derive(AsChangeset)]
#[diesel(table_name = crate::schema::remote_targets)]
struct UpdateRemoteTargetRow {
    collection_id: Option<i32>,
    class_id: Option<Option<i32>>,
    name: Option<String>,
    description: Option<String>,
    method: Option<String>,
    url_template: Option<String>,
    headers_template: Option<Value>,
    body_template: Option<Option<String>>,
    auth_config: Option<Value>,
    allowed_subject_types: Option<Value>,
    timeout_ms: Option<i32>,
    enabled: Option<bool>,
}

impl_redacted_remote_target_debug!(
    UpdateRemoteTargetRow,
    collection_id,
    class_id,
    name,
    description,
    method,
    allowed_subject_types,
    timeout_ms,
    enabled,
);

impl UpdateRemoteTargetRow {
    fn from_patch(patch: StorageRemoteTargetPatch) -> Result<Self, PostgresStorageError> {
        let parts = patch.into_parts();
        Ok(Self {
            collection_id: parts.collection_id().map(|id| id.id()),
            class_id: parts.class_id().map(|value| value.map(|id| id.id())),
            name: parts.name().map(str::to_string),
            description: parts.description().map(str::to_string),
            method: parts.method().map(|method| method.as_str().to_string()),
            url_template: parts.url_template().map(str::to_string),
            headers_template: parts.headers_template().cloned(),
            body_template: parts.body_template().map(|value| value.map(str::to_string)),
            auth_config: parts.auth_config().cloned(),
            allowed_subject_types: parts
                .allowed_subject_types()
                .map(<[StorageRemoteTargetSubjectType]>::to_vec)
                .map(encode_subject_types)
                .transpose()?,
            timeout_ms: parts.timeout_ms(),
            enabled: parts.enabled(),
        })
    }

    fn has_changes(&self, current: &RemoteTargetRow) -> bool {
        self.collection_id
            .is_some_and(|value| value != current.collection_id)
            || self
                .class_id
                .as_ref()
                .is_some_and(|value| value != &current.class_id)
            || self
                .name
                .as_ref()
                .is_some_and(|value| value != &current.name)
            || self
                .description
                .as_ref()
                .is_some_and(|value| value != &current.description)
            || self
                .method
                .as_ref()
                .is_some_and(|value| value != &current.method)
            || self
                .url_template
                .as_ref()
                .is_some_and(|value| value != &current.url_template)
            || self
                .headers_template
                .as_ref()
                .is_some_and(|value| value != &current.headers_template)
            || self
                .body_template
                .as_ref()
                .is_some_and(|value| value != &current.body_template)
            || self
                .auth_config
                .as_ref()
                .is_some_and(|value| value != &current.auth_config)
            || self
                .allowed_subject_types
                .as_ref()
                .is_some_and(|value| value != &current.allowed_subject_types)
            || self
                .timeout_ms
                .is_some_and(|value| value != current.timeout_ms)
            || self.enabled.is_some_and(|value| value != current.enabled)
    }
}

struct RemoteTargetDefinitionParts {
    class_id: Option<i32>,
    description: String,
    method: String,
    url_template: String,
    headers_template: Value,
    body_template: Option<String>,
    auth_config: Value,
    allowed_subject_types: Value,
    timeout_ms: i32,
    enabled: bool,
}

impl RemoteTargetDefinitionParts {
    fn from_definition(
        definition: StorageRemoteTargetDefinition,
    ) -> Result<Self, PostgresStorageError> {
        let (description, transport, policy) = definition.into_parts();
        let StorageRemoteTargetTransportParts {
            method,
            url_template,
            headers_template,
            body_template,
            auth_config,
            timeout_ms,
        } = transport.into_parts();
        let method = method.as_str().to_string();
        let (class_id, allowed_subject_types, enabled) = policy.into_parts();
        Ok(Self {
            class_id: class_id.map(|id| id.id()),
            description,
            method,
            url_template,
            headers_template,
            body_template,
            auth_config,
            allowed_subject_types: encode_subject_types(allowed_subject_types)?,
            timeout_ms,
            enabled,
        })
    }
}

pub async fn get_remote_target(
    runtime: &PostgresRuntime,
    target_id: i32,
) -> Result<StorageRemoteTarget, PostgresStorageError> {
    ensure_positive_target_id(target_id)?;
    runtime
        .with_connection(async |connection| {
            load_remote_target_row(connection, target_id)
                .await?
                .into_storage()
        })
        .await
}

pub async fn list_remote_targets(
    runtime: &PostgresRuntime,
    query: StorageRemoteTargetListQuery,
) -> Result<StoragePage<StorageRemoteTarget>, PostgresStorageError> {
    let (allowed_collection_ids, options) = query.into_parts();
    let allowed_collection_ids = allowed_collection_ids
        .into_iter()
        .map(|collection_id| collection_id.id())
        .collect::<Vec<_>>();
    if options.include_total() {
        runtime
            .with_read_only_snapshot(async |connection| {
                let total = build_list_query(&allowed_collection_ids, &options)?
                    .count()
                    .get_result::<i64>(connection)
                    .await?;
                let targets =
                    load_remote_target_rows(connection, &allowed_collection_ids, &options).await?;
                StoragePage::try_new(targets, Some(total)).map_err(PostgresStorageError::from)
            })
            .await
    } else {
        runtime
            .with_connection(async |connection| {
                let targets =
                    load_remote_target_rows(connection, &allowed_collection_ids, &options).await?;
                StoragePage::try_new(targets, None).map_err(PostgresStorageError::from)
            })
            .await
    }
}

pub async fn create_remote_target(
    runtime: &PostgresRuntime,
    request: StorageRemoteTargetCreate,
) -> Result<MutationOutcome<StorageRemoteTarget>, PostgresStorageError> {
    let (new_target, event_context) = NewRemoteTargetRow::from_request(request)?;
    runtime
        .with_transaction(
            async |connection| -> Result<MutationOutcome<StorageRemoteTarget>, PostgresStorageError> {
            use crate::schema::remote_targets::dsl::remote_targets;

            let created = diesel::insert_into(remote_targets)
                .values(new_target)
                .get_result::<RemoteTargetRow>(connection)
                .await?;
            let audit = append_remote_target_audit(
                connection,
                Action::Created,
                &event_context,
                None,
                &created,
            )
            .await?;
            Ok(MutationOutcome::committed(created.into_storage()?, audit))
            },
        )
        .await
}

pub async fn update_remote_target(
    runtime: &PostgresRuntime,
    request: StorageRemoteTargetUpdate,
) -> Result<MutationOutcome<StorageRemoteTarget>, PostgresStorageError> {
    let (target_id, patch, event_context) = request.into_parts();
    let target_id = target_id.id();
    ensure_positive_target_id(target_id)?;
    let changes = UpdateRemoteTargetRow::from_patch(patch)?;
    runtime
        .with_transaction(
            async |connection| -> Result<MutationOutcome<StorageRemoteTarget>, PostgresStorageError> {
            use crate::schema::remote_targets::dsl::{id, remote_targets};

            let before = remote_targets
                .filter(id.eq(target_id))
                .for_update()
                .first::<RemoteTargetRow>(connection)
                .await?;
            assert_locked_revision_precondition(
                connection,
                &RevisionOwner::RemoteTarget.key(before.id),
                before.revision,
            )
            .await?;
            if !changes.has_changes(&before) {
                return Ok(MutationOutcome::unchanged(before.into_storage()?));
            }
            let updated = diesel::update(remote_targets.filter(id.eq(target_id)))
                .set(changes)
                .get_result::<RemoteTargetRow>(connection)
                .await?;
            let audit = append_remote_target_audit(
                connection,
                Action::Updated,
                &event_context,
                Some(&before),
                &updated,
            )
            .await?;
            Ok(MutationOutcome::committed(updated.into_storage()?, audit))
            },
        )
        .await
}

pub async fn delete_remote_target(
    runtime: &PostgresRuntime,
    request: StorageRemoteTargetDelete,
) -> Result<MutationOutcome<()>, PostgresStorageError> {
    let (target_id, event_context) = request.into_parts();
    let target_id = target_id.id();
    ensure_positive_target_id(target_id)?;
    runtime
        .with_transaction(async |connection| {
            use crate::schema::remote_targets::dsl::{id, remote_targets};

            let before = remote_targets
                .filter(id.eq(target_id))
                .for_update()
                .first::<RemoteTargetRow>(connection)
                .await?;
            assert_locked_revision_precondition(
                connection,
                &RevisionOwner::RemoteTarget.key(before.id),
                before.revision,
            )
            .await?;
            let audit = append_remote_target_audit(
                connection,
                Action::Deleted,
                &event_context,
                Some(&before),
                &before,
            )
            .await?;
            diesel::delete(remote_targets.filter(id.eq(target_id)))
                .execute(connection)
                .await?;
            Ok::<_, PostgresStorageError>(MutationOutcome::committed((), audit))
        })
        .await
}

pub async fn record_remote_target_invocation(
    runtime: &PostgresRuntime,
    request: StorageRemoteTargetInvocation,
) -> Result<MutationOutcome<()>, PostgresStorageError> {
    let (target_id, task_id, subject_type, subject_id, event_context) = request.into_parts();
    let target_id = target_id.id();
    ensure_positive_target_id(target_id)?;
    runtime
        .with_transaction(
            async |connection| -> Result<MutationOutcome<()>, PostgresStorageError> {
                let target = load_remote_target_row(connection, target_id).await?;
                let event = NewEvent::new(
                    EntityType::RemoteTarget,
                    Action::Invoked,
                    event_context.actor_kind(),
                    format!("Remote target '{}' invoked", target.name),
                )
                .map_err(|error| PostgresStorageError::database(error.to_string()))?
                .with_context(&event_context)
                .with_entity_id(hubuum_events_core::EventEntityId::new(target.id)?)
                .with_entity_name(&target.name)
                .with_collection_id(hubuum_domain::CollectionId::new(target.collection_id)?)
                .with_metadata(json!({
                    "task_id": task_id,
                    "subject_type": subject_type.as_str(),
                    "subject_id": subject_id,
                }));
                let audit = append_event(connection, &event)
                    .await?
                    .into_audit_receipt()?;
                Ok(MutationOutcome::committed((), audit))
            },
        )
        .await
}

async fn load_remote_target_row(
    connection: &mut PostgresConnection,
    target_id: i32,
) -> Result<RemoteTargetRow, PostgresStorageError> {
    use crate::schema::remote_targets::dsl::{id, remote_targets};

    remote_targets
        .filter(id.eq(target_id))
        .first::<RemoteTargetRow>(connection)
        .await
        .map_err(PostgresStorageError::from)
}

async fn load_remote_target_rows(
    connection: &mut PostgresConnection,
    allowed_collection_ids: &[i32],
    options: &QueryOptions,
) -> Result<Vec<StorageRemoteTarget>, PostgresStorageError> {
    let mut records = build_list_query(allowed_collection_ids, options)?;
    let fields = options
        .sort()
        .iter()
        .map(|sort| remote_target_cursor_field(&sort.field))
        .collect::<Result<Vec<_>, _>>()?;
    crate::apply_query_options_with_fields!(records, options, fields);
    records
        .load::<RemoteTargetRow>(connection)
        .await?
        .into_iter()
        .map(RemoteTargetRow::into_storage)
        .collect()
}

async fn append_remote_target_audit(
    connection: &mut PostgresConnection,
    action: Action,
    context: &EventContext,
    before: Option<&RemoteTargetRow>,
    after: &RemoteTargetRow,
) -> Result<AuditReceipt, PostgresStorageError> {
    let event = NewEvent::new(
        EntityType::RemoteTarget,
        action,
        context.actor_kind(),
        format!("Remote target '{}' {}", after.name, action.as_str()),
    )
    .map_err(|error| PostgresStorageError::database(error.to_string()))?
    .with_context(context)
    .with_entity_id(hubuum_events_core::EventEntityId::new(after.id)?)
    .with_entity_name(&after.name)
    .with_collection_id(hubuum_domain::CollectionId::new(after.collection_id)?)
    .with_before_opt(before.map(RemoteTargetRow::audit_snapshot))
    .with_after_opt((action != Action::Deleted).then(|| after.audit_snapshot()));
    append_event(connection, &event)
        .await?
        .into_audit_receipt()
        .map_err(Into::into)
}

fn build_list_query<'a>(
    allowed_collection_ids: &'a [i32],
    options: &'a QueryOptions,
) -> Result<crate::schema::remote_targets::BoxedQuery<'a, diesel::pg::Pg>, PostgresStorageError> {
    use crate::schema::remote_targets::dsl::{
        class_id, collection_id, created_at, description, id, method, name, remote_targets,
        revision, updated_at,
    };

    let mut query = remote_targets
        .filter(collection_id.eq_any(allowed_collection_ids))
        .into_boxed();
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
            FilterField::ClassId => {
                crate::postgres_integer_filter!(query, parameter, class_id)
            }
            FilterField::Kind => crate::postgres_string_filter!(query, parameter, method),
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
                return Err(PostgresStorageError::invalid_input(format!(
                    "Field '{}' isn't searchable for remote targets",
                    parameter.field
                )));
            }
        }
    }
    Ok(query)
}

fn remote_target_cursor_field(field: &FilterField) -> Result<CursorSqlField, PostgresStorageError> {
    Ok(match field {
        FilterField::Id => cursor_field("remote_targets.id", CursorSqlType::Integer),
        FilterField::Name => cursor_field("remote_targets.name", CursorSqlType::String),
        FilterField::Description => {
            cursor_field("remote_targets.description", CursorSqlType::String)
        }
        FilterField::CollectionId | FilterField::Collections => {
            cursor_field("remote_targets.collection_id", CursorSqlType::Integer)
        }
        FilterField::CreatedAt => {
            cursor_field("remote_targets.created_at", CursorSqlType::DateTime)
        }
        FilterField::UpdatedAt => {
            cursor_field("remote_targets.updated_at", CursorSqlType::DateTime)
        }
        FilterField::Revision => cursor_field("remote_targets.revision", CursorSqlType::BigInt),
        _ => {
            return Err(PostgresStorageError::invalid_input(format!(
                "Field '{field}' is not orderable for remote targets"
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

fn ensure_positive_target_id(target_id: i32) -> Result<(), PostgresStorageError> {
    if target_id > 0 {
        Ok(())
    } else {
        Err(PostgresStorageError::invalid_input(
            "Remote target id must be greater than zero",
        ))
    }
}

fn encode_subject_types(
    subject_types: Vec<StorageRemoteTargetSubjectType>,
) -> Result<Value, PostgresStorageError> {
    let subject_types = subject_types
        .into_iter()
        .map(StorageRemoteTargetSubjectType::as_str)
        .collect::<Vec<_>>();
    serde_json::to_value(subject_types).map_err(|error| {
        PostgresStorageError::database(format!(
            "Could not serialize remote target subject types: {error}"
        ))
    })
}

fn decode_subject_types(
    subject_types: Value,
) -> Result<Vec<StorageRemoteTargetSubjectType>, PostgresStorageError> {
    let subject_types = serde_json::from_value::<Vec<String>>(subject_types).map_err(|error| {
        PostgresStorageError::database(format!(
            "Could not deserialize remote target subject types: {error}"
        ))
    })?;
    subject_types
        .into_iter()
        .map(|subject_type| {
            subject_type.parse().map_err(|error| {
                PostgresStorageError::database(format!(
                    "Invalid persisted remote target subject type: {error}"
                ))
            })
        })
        .collect()
}

fn decode_http_method(method: &str) -> Result<StorageRemoteHttpMethod, PostgresStorageError> {
    method.parse().map_err(|error| {
        PostgresStorageError::database(format!(
            "Invalid persisted remote target HTTP method: {error}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remote_target_debug_redacts_transport_and_authentication_configuration() {
        let row = NewRemoteTargetRow {
            collection_id: 1,
            class_id: None,
            name: "target".to_string(),
            description: "description".to_string(),
            method: "post".to_string(),
            url_template: "https://secret.invalid/{{ token }}".to_string(),
            headers_template: json!({"authorization": "secret"}),
            body_template: Some("secret".to_string()),
            auth_config: json!({"secret": "secret-ref"}),
            allowed_subject_types: json!(["collection"]),
            timeout_ms: 1_000,
            enabled: true,
        };

        let debug = format!("{row:?}");

        assert!(debug.contains("configuration: \"<redacted>\""));
        assert!(!debug.contains("secret.invalid"));
        assert!(!debug.contains("secret-ref"));
    }

    #[test]
    fn audit_snapshot_redacts_authentication_configuration() {
        let timestamp = chrono::DateTime::from_timestamp(1_700_000_000, 0)
            .unwrap()
            .naive_utc();
        let row = RemoteTargetRow {
            id: 1,
            collection_id: 2,
            class_id: None,
            name: "target".to_string(),
            description: "description".to_string(),
            method: "post".to_string(),
            url_template: "https://example.invalid".to_string(),
            headers_template: json!({}),
            body_template: None,
            auth_config: json!({"secret": "secret-ref"}),
            allowed_subject_types: json!(["collection"]),
            timeout_ms: 1_000,
            enabled: true,
            created_at: timestamp,
            updated_at: timestamp,
            revision: PostgresRevision::INITIAL,
        };

        let snapshot = row.audit_snapshot();

        assert_eq!(snapshot["auth_config"], "<redacted>");
        assert!(!snapshot.to_string().contains("secret-ref"));
    }
}
