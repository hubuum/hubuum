//! PostgreSQL implementation of service-account lifecycle and query contracts.

use chrono::NaiveDateTime;
use diesel::prelude::{ExpressionMethods, QueryDsl};
use diesel::{AsChangeset, JoinOnDsl, Queryable, QueryableByName, Selectable, SelectableHelper};
use diesel_async::RunQueryDsl;
use hubuum_domain::{GroupId, IdentityScopeId, PrincipalId, ServiceAccountId, TaskId};
use hubuum_events_core::{Action, EntityType, EventContext, MutationProvenance, NewEvent};
use hubuum_query::FilterField;
use hubuum_storage_core::{
    MutationOutcome, StoragePage, StorageServiceAccount, StorageServiceAccountCreate,
    StorageServiceAccountDisableOutcome, StorageServiceAccountListItem,
    StorageServiceAccountListQuery, StorageServiceAccountMutation, StorageServiceAccountPoint,
    StorageServiceAccountUpdate,
};
use serde_json::{Value, json};

use crate::cursor::{CursorSqlField, CursorSqlType};
use crate::operations::event_record::append_event;
use crate::revision::RevisionOwner;
use crate::runtime::assert_locked_revision_precondition;
use crate::{PostgresConnection, PostgresRevision, PostgresRuntime, PostgresStorageError};

const LOCAL_IDENTITY_SCOPE: &str = "local";
const SERVICE_ACCOUNT_PRINCIPAL_KIND: &str = "service_account";
const QUEUED_TASK_STATUS: &str = "queued";
const CANCELLED_TASK_STATUS: &str = "cancelled";
const REINDEX_TASK_KIND: &str = "reindex";
const DISABLED_TASK_SUMMARY: &str = "Task cancelled because its submitting principal was disabled";
const DATABASE_UTC_NOW_QUERY: &str = "SELECT clock_timestamp() AT TIME ZONE 'UTC' AS now";

macro_rules! apply_service_account_filters {
    ($query:ident, $options:expr) => {
        for parameter in $options.filters() {
            match parameter.field {
                FilterField::Id => crate::postgres_integer_filter!(
                    $query,
                    parameter,
                    crate::schema::service_accounts::id
                ),
                FilterField::Name => crate::postgres_string_filter!(
                    $query,
                    parameter,
                    crate::schema::principals::name
                ),
                FilterField::IdentityScope => crate::postgres_string_filter!(
                    $query,
                    parameter,
                    crate::schema::identity_scopes::name
                ),
                FilterField::CreatedAt => crate::postgres_datetime_filter!(
                    $query,
                    parameter,
                    crate::schema::service_accounts::created_at
                ),
                FilterField::UpdatedAt => crate::postgres_datetime_filter!(
                    $query,
                    parameter,
                    crate::schema::service_accounts::updated_at
                ),
                FilterField::Revision => crate::postgres_revision_filter!(
                    $query,
                    parameter,
                    crate::schema::principals::revision
                ),
                _ => {
                    return Err(PostgresStorageError::invalid_input(format!(
                        "Field '{}' isn't searchable for service accounts",
                        parameter.field
                    )));
                }
            }
        }
    };
}

#[derive(Clone, Queryable, Selectable)]
#[diesel(table_name = crate::schema::service_accounts)]
pub(crate) struct ServiceAccountRow {
    pub(crate) id: i32,
    #[diesel(column_name = kind)]
    pub(crate) _kind: String,
    pub(crate) description: String,
    pub(crate) owner_group_id: i32,
    pub(crate) created_by: Option<i32>,
    pub(crate) disabled_at: Option<NaiveDateTime>,
    pub(crate) created_at: NaiveDateTime,
    pub(crate) updated_at: NaiveDateTime,
}

impl ServiceAccountRow {
    fn into_storage(self) -> Result<StorageServiceAccount, PostgresStorageError> {
        Ok(StorageServiceAccount::new(
            ServiceAccountId::new(self.id)?,
            self.description,
            GroupId::new(self.owner_group_id)?,
            self.created_by.map(PrincipalId::new).transpose()?,
            self.disabled_at,
            self.created_at,
            self.updated_at,
        ))
    }

    fn snapshot(&self, name: &str, revision: PostgresRevision) -> Value {
        json!({
            "id": self.id,
            "name": name,
            "description": self.description,
            "owner_group_id": self.owner_group_id,
            "created_by": self.created_by,
            "disabled_at": self.disabled_at,
            "revision": revision,
        })
    }
}

#[derive(AsChangeset)]
#[diesel(table_name = crate::schema::service_accounts)]
struct UpdateServiceAccountRow<'value> {
    description: Option<&'value str>,
    owner_group_id: Option<i32>,
}

#[derive(QueryableByName)]
struct DatabaseTimeRow {
    #[diesel(sql_type = diesel::sql_types::Timestamp)]
    now: NaiveDateTime,
}

pub async fn get_service_account(
    runtime: &PostgresRuntime,
    service_account_id: i32,
) -> Result<StorageServiceAccount, PostgresStorageError> {
    validate_positive_id(service_account_id, "service account id")?;
    runtime
        .with_connection(async move |connection| {
            load_service_account_row(connection, service_account_id)
                .await?
                .into_storage()
        })
        .await
}

pub async fn get_service_account_point(
    runtime: &PostgresRuntime,
    service_account_id: i32,
) -> Result<StorageServiceAccountPoint, PostgresStorageError> {
    validate_positive_id(service_account_id, "service account id")?;
    runtime
        .with_connection(async move |connection| {
            let (account, identity_scope_id, name, revision) =
                crate::schema::service_accounts::table
                    .inner_join(
                        crate::schema::principals::table
                            .on(crate::schema::principals::id
                                .eq(crate::schema::service_accounts::id)),
                    )
                    .filter(crate::schema::service_accounts::id.eq(service_account_id))
                    .select((
                        ServiceAccountRow::as_select(),
                        crate::schema::principals::identity_scope_id,
                        crate::schema::principals::name,
                        crate::schema::principals::revision,
                    ))
                    .first::<(ServiceAccountRow, i32, String, PostgresRevision)>(connection)
                    .await?;
            Ok::<_, PostgresStorageError>(StorageServiceAccountPoint::new(
                account.into_storage()?,
                IdentityScopeId::new(identity_scope_id)?,
                name,
                revision.into_domain(),
            ))
        })
        .await
}

pub async fn list_manageable_service_accounts(
    runtime: &PostgresRuntime,
    query: StorageServiceAccountListQuery,
) -> Result<StoragePage<StorageServiceAccountListItem>, PostgresStorageError> {
    let (requestor_id, administrator, options) = query.into_parts();
    let requestor_id = requestor_id.id();
    validate_positive_id(requestor_id, "requestor id")?;
    runtime
        .with_read_only_snapshot(async move |connection| {
            let build_query = || -> Result<_, PostgresStorageError> {
                let mut records =
                    crate::schema::service_accounts::table
                        .inner_join(crate::schema::principals::table.on(
                            crate::schema::principals::id.eq(crate::schema::service_accounts::id),
                        ))
                        .inner_join(
                            crate::schema::identity_scopes::table
                                .on(crate::schema::principals::identity_scope_id
                                    .eq(crate::schema::identity_scopes::id)),
                        )
                        .into_boxed();
                if !administrator {
                    records = records.filter(
                        crate::schema::service_accounts::owner_group_id.eq_any(
                            crate::schema::group_memberships::table
                                .filter(
                                    crate::schema::group_memberships::principal_id.eq(requestor_id),
                                )
                                .select(crate::schema::group_memberships::group_id),
                        ),
                    );
                }
                apply_service_account_filters!(records, options);
                Ok(records)
            };

            let total = if options.include_total() {
                Some(build_query()?.count().get_result::<i64>(connection).await?)
            } else {
                None
            };
            let mut records = build_query()?;
            let fields = options
                .sort()
                .iter()
                .map(|sort| service_account_cursor_field(&sort.field))
                .collect::<Result<Vec<_>, _>>()?;
            crate::apply_query_options_with_fields!(records, options, fields);
            let rows = records
                .select((
                    ServiceAccountRow::as_select(),
                    crate::schema::identity_scopes::name,
                    crate::schema::principals::name,
                    crate::schema::principals::revision,
                ))
                .load::<(ServiceAccountRow, String, String, PostgresRevision)>(connection)
                .await?
                .into_iter()
                .map(|(account, scope, name, revision)| {
                    Ok::<_, PostgresStorageError>(StorageServiceAccountListItem::new(
                        account.into_storage()?,
                        scope,
                        name,
                        revision.into_domain(),
                    ))
                })
                .collect::<Result<Vec<_>, _>>()?;
            StoragePage::try_new(rows, total).map_err(PostgresStorageError::from)
        })
        .await
}

pub async fn create_service_account(
    runtime: &PostgresRuntime,
    request: StorageServiceAccountCreate,
) -> Result<MutationOutcome<StorageServiceAccount>, PostgresStorageError> {
    let (name, description, owner_group_id, created_by, context) = request.into_parts();
    create_service_account_parts(
        runtime,
        name,
        description,
        owner_group_id.id(),
        created_by.map(|principal_id| principal_id.id()),
        context,
    )
    .await
}

async fn create_service_account_parts(
    runtime: &PostgresRuntime,
    name: String,
    description: String,
    owner_group_id: i32,
    created_by: Option<i32>,
    context: EventContext,
) -> Result<MutationOutcome<StorageServiceAccount>, PostgresStorageError> {
    validate_positive_id(owner_group_id, "owner group id")?;
    if let Some(created_by) = created_by {
        validate_positive_id(created_by, "creator id")?;
    }
    runtime
        .with_transaction(async move |connection| {
            let identity_scope_id = local_identity_scope_id(connection).await?;
            let principal_id = diesel::insert_into(crate::schema::principals::table)
                .values((
                    crate::schema::principals::identity_scope_id.eq(identity_scope_id),
                    crate::schema::principals::kind.eq(SERVICE_ACCOUNT_PRINCIPAL_KIND),
                    crate::schema::principals::name.eq(&name),
                ))
                .returning(crate::schema::principals::id)
                .get_result::<i32>(connection)
                .await?;
            let account = diesel::insert_into(crate::schema::service_accounts::table)
                .values((
                    crate::schema::service_accounts::id.eq(principal_id),
                    crate::schema::service_accounts::description.eq(&description),
                    crate::schema::service_accounts::owner_group_id.eq(owner_group_id),
                    crate::schema::service_accounts::created_by.eq(created_by),
                ))
                .get_result::<ServiceAccountRow>(connection)
                .await?;
            let revision = principal_revision(connection, principal_id).await?;
            let event = service_account_event(
                &account,
                &name,
                Action::Created,
                &context,
                format!("Service account '{name}' created"),
            )?
            .with_after(account.snapshot(&name, revision))
            .with_metadata(json!({
                "owner_group_id": account.owner_group_id,
                "created_by": created_by,
            }));
            let audit = append_event(connection, &event)
                .await?
                .into_audit_receipt()?;
            Ok::<_, PostgresStorageError>(MutationOutcome::committed(
                account.into_storage()?,
                audit,
            ))
        })
        .await
}

pub async fn update_service_account(
    runtime: &PostgresRuntime,
    request: StorageServiceAccountUpdate,
) -> Result<MutationOutcome<StorageServiceAccount>, PostgresStorageError> {
    let (service_account_id, description, owner_group_id, context) = request.into_parts();
    let service_account_id = service_account_id.id();
    let owner_group_id = owner_group_id.map(|group_id| group_id.id());
    validate_positive_id(service_account_id, "service account id")?;
    if let Some(owner_group_id) = owner_group_id {
        validate_positive_id(owner_group_id, "owner group id")?;
    }
    runtime
        .with_transaction(async move |connection| {
            let before_revision = lock_principal_revision(connection, service_account_id).await?;
            let before = load_service_account_row(connection, service_account_id).await?;
            let changed = description
                .as_ref()
                .is_some_and(|value| value != &before.description)
                || owner_group_id.is_some_and(|value| value != before.owner_group_id);
            if !changed {
                return Ok(MutationOutcome::unchanged(before.into_storage()?));
            }
            let after = diesel::update(
                crate::schema::service_accounts::table
                    .filter(crate::schema::service_accounts::id.eq(service_account_id)),
            )
            .set(UpdateServiceAccountRow {
                description: description.as_deref(),
                owner_group_id,
            })
            .get_result::<ServiceAccountRow>(connection)
            .await?;
            let name = principal_name(connection, service_account_id).await?;
            let after_revision = principal_revision(connection, service_account_id).await?;
            let event = service_account_event(
                &after,
                &name,
                Action::Updated,
                &context,
                format!("Service account '{name}' updated"),
            )?
            .with_before(before.snapshot(&name, before_revision))
            .with_after(after.snapshot(&name, after_revision))
            .with_metadata(json!({ "owner_group_id": after.owner_group_id }));
            let audit = append_event(connection, &event)
                .await?
                .into_audit_receipt()?;
            Ok::<_, PostgresStorageError>(MutationOutcome::committed(after.into_storage()?, audit))
        })
        .await
}

pub async fn disable_service_account(
    runtime: &PostgresRuntime,
    request: StorageServiceAccountMutation,
) -> Result<MutationOutcome<StorageServiceAccountDisableOutcome>, PostgresStorageError> {
    let (service_account_id, context) = request.into_parts();
    disable_service_account_parts(runtime, service_account_id.id(), context).await
}

async fn disable_service_account_parts(
    runtime: &PostgresRuntime,
    service_account_id: i32,
    context: EventContext,
) -> Result<MutationOutcome<StorageServiceAccountDisableOutcome>, PostgresStorageError> {
    validate_positive_id(service_account_id, "service account id")?;
    runtime
        .with_transaction(async move |connection| {
            let before_revision = lock_principal_revision(connection, service_account_id).await?;
            let before = load_service_account_row(connection, service_account_id).await?;
            if before.disabled_at.is_some() {
                return Ok(MutationOutcome::unchanged(
                    StorageServiceAccountDisableOutcome::new(before.into_storage()?, Vec::new()),
                ));
            }
            let disabled = diesel::update(
                crate::schema::service_accounts::table
                    .filter(crate::schema::service_accounts::id.eq(service_account_id)),
            )
            .set(crate::schema::service_accounts::disabled_at.eq(diesel::dsl::now))
            .get_result::<ServiceAccountRow>(connection)
            .await?;
            let name = principal_name(connection, service_account_id).await?;
            let after_revision = principal_revision(connection, service_account_id).await?;
            let event = service_account_event(
                &disabled,
                &name,
                Action::Disabled,
                &context,
                format!("Service account '{name}' disabled"),
            )?
            .with_before(before.snapshot(&name, before_revision))
            .with_after(disabled.snapshot(&name, after_revision))
            .with_metadata(json!({ "owner_group_id": disabled.owner_group_id }));
            let audit = append_event(connection, &event)
                .await?
                .into_audit_receipt()?;

            crate::operations::token::revoke_all_principal_tokens_on_connection(
                connection,
                service_account_id,
            )
            .await?;
            let cancelled_task_kinds =
                cancel_pending_tasks(connection, service_account_id, &context).await?;
            Ok::<_, PostgresStorageError>(MutationOutcome::committed(
                StorageServiceAccountDisableOutcome::new(
                    disabled.into_storage()?,
                    cancelled_task_kinds,
                ),
                audit,
            ))
        })
        .await
}

pub async fn delete_service_account(
    runtime: &PostgresRuntime,
    request: StorageServiceAccountMutation,
) -> Result<MutationOutcome<()>, PostgresStorageError> {
    let (service_account_id, context) = request.into_parts();
    let service_account_id = service_account_id.id();
    validate_positive_id(service_account_id, "service account id")?;
    runtime
        .with_transaction(async move |connection| {
            let before_revision = lock_principal_revision(connection, service_account_id).await?;
            let account = load_service_account_row(connection, service_account_id).await?;
            let name = principal_name(connection, service_account_id).await?;
            let event = service_account_event(
                &account,
                &name,
                Action::Deleted,
                &context,
                format!("Service account '{name}' deleted"),
            )?
            .with_before(account.snapshot(&name, before_revision))
            .with_metadata(json!({ "owner_group_id": account.owner_group_id }));
            let audit = append_event(connection, &event)
                .await?
                .into_audit_receipt()?;
            diesel::delete(
                crate::schema::principals::table
                    .filter(crate::schema::principals::id.eq(service_account_id)),
            )
            .execute(connection)
            .await?;
            Ok::<_, PostgresStorageError>(MutationOutcome::committed((), audit))
        })
        .await
}

async fn cancel_pending_tasks(
    connection: &mut PostgresConnection,
    principal_id: i32,
    context: &EventContext,
) -> Result<Vec<String>, PostgresStorageError> {
    let task_ids = crate::schema::tasks::table
        .filter(crate::schema::tasks::submitted_by.eq(principal_id))
        .filter(crate::schema::tasks::status.eq(QUEUED_TASK_STATUS))
        .filter(crate::schema::tasks::kind.ne(REINDEX_TASK_KIND))
        .select(crate::schema::tasks::id)
        .load::<i32>(connection)
        .await?;
    if task_ids.is_empty() {
        return Ok(Vec::new());
    }

    let terminal_at = database_now(connection).await?;
    let cancelled = diesel::update(
        crate::schema::tasks::table
            .filter(crate::schema::tasks::id.eq_any(task_ids))
            .filter(crate::schema::tasks::status.eq(QUEUED_TASK_STATUS)),
    )
    .set((
        crate::schema::tasks::status.eq(CANCELLED_TASK_STATUS),
        crate::schema::tasks::summary.eq(Some(DISABLED_TASK_SUMMARY)),
        crate::schema::tasks::finished_at.eq(Some(terminal_at)),
        crate::schema::tasks::request_payload.eq::<Option<Value>>(None),
        crate::schema::tasks::request_redacted_at.eq(Some(terminal_at)),
        crate::schema::tasks::lease_token.eq::<Option<uuid::Uuid>>(None),
        crate::schema::tasks::lease_expires_at.eq::<Option<NaiveDateTime>>(None),
        crate::schema::tasks::updated_at.eq(terminal_at),
    ))
    .returning((
        crate::schema::tasks::id,
        crate::schema::tasks::kind,
        crate::schema::tasks::initiator_user_id,
    ))
    .get_results::<(i32, String, Option<i32>)>(connection)
    .await?;

    for (task_id, task_kind, initiator_user_id) in &cancelled {
        let task_id = TaskId::new(*task_id)?;
        let initiator_user_id = initiator_user_id.map(PrincipalId::new).transpose()?;
        let provenance = match context.actor_user_id() {
            Some(actor_user_id) => {
                MutationProvenance::user_for_task(actor_user_id, initiator_user_id, task_id)
            }
            None => MutationProvenance::system_for_task(initiator_user_id, task_id),
        };
        let event = NewEvent::new(
            EntityType::Task,
            Action::Cancelled,
            provenance.actor_kind(),
            DISABLED_TASK_SUMMARY,
        )
        .map_err(|error| PostgresStorageError::database(error.to_string()))?
        .with_entity_id(hubuum_events_core::EventEntityId::new(task_id.id())?)
        .with_metadata(json!({ "task_id": task_id, "task_kind": task_kind }))
        .with_mutation_provenance(&provenance);
        append_event(connection, &event).await?;
    }

    Ok(cancelled.into_iter().map(|(_, kind, _)| kind).collect())
}

/// Cancel one principal's queued non-reindex work and append system lifecycle
/// events. The returned task kinds let application composition record metrics.
#[doc(hidden)]
#[cfg(feature = "integration-test-support")]
pub async fn cancel_pending_tasks_for_principal(
    runtime: &PostgresRuntime,
    principal_id: i32,
) -> Result<Vec<String>, PostgresStorageError> {
    validate_positive_id(principal_id, "principal id")?;
    runtime
        .with_transaction(async move |connection| {
            let context = EventContext::system();
            cancel_pending_tasks(connection, principal_id, &context).await
        })
        .await
}

async fn database_now(
    connection: &mut PostgresConnection,
) -> Result<NaiveDateTime, PostgresStorageError> {
    diesel::sql_query(DATABASE_UTC_NOW_QUERY)
        .get_result::<DatabaseTimeRow>(connection)
        .await
        .map(|row| row.now)
        .map_err(PostgresStorageError::from)
}

async fn load_service_account_row(
    connection: &mut PostgresConnection,
    service_account_id: i32,
) -> Result<ServiceAccountRow, diesel::result::Error> {
    crate::schema::service_accounts::table
        .filter(crate::schema::service_accounts::id.eq(service_account_id))
        .select(ServiceAccountRow::as_select())
        .first(connection)
        .await
}

async fn local_identity_scope_id(
    connection: &mut PostgresConnection,
) -> Result<i32, diesel::result::Error> {
    crate::schema::identity_scopes::table
        .filter(crate::schema::identity_scopes::name.eq(LOCAL_IDENTITY_SCOPE))
        .select(crate::schema::identity_scopes::id)
        .first(connection)
        .await
}

async fn principal_name(
    connection: &mut PostgresConnection,
    principal_id: i32,
) -> Result<String, diesel::result::Error> {
    crate::schema::principals::table
        .filter(crate::schema::principals::id.eq(principal_id))
        .select(crate::schema::principals::name)
        .first(connection)
        .await
}

async fn principal_revision(
    connection: &mut PostgresConnection,
    principal_id: i32,
) -> Result<PostgresRevision, diesel::result::Error> {
    crate::schema::principals::table
        .filter(crate::schema::principals::id.eq(principal_id))
        .select(crate::schema::principals::revision)
        .first(connection)
        .await
}

async fn lock_principal_revision(
    connection: &mut PostgresConnection,
    principal_id: i32,
) -> Result<PostgresRevision, PostgresStorageError> {
    let revision = crate::schema::principals::table
        .filter(crate::schema::principals::id.eq(principal_id))
        .select(crate::schema::principals::revision)
        .for_update()
        .first::<PostgresRevision>(connection)
        .await?;
    assert_locked_revision_precondition(
        connection,
        &RevisionOwner::Principal.key(principal_id),
        revision,
    )
    .await?;
    Ok(revision)
}

fn service_account_cursor_field(
    field: &FilterField,
) -> Result<CursorSqlField, PostgresStorageError> {
    Ok(match field {
        FilterField::Id => cursor_field("service_accounts.id", CursorSqlType::Integer),
        FilterField::Name => cursor_field("principals.name", CursorSqlType::String),
        FilterField::IdentityScope => cursor_field("identity_scopes.name", CursorSqlType::String),
        FilterField::CreatedAt => {
            cursor_field("service_accounts.created_at", CursorSqlType::DateTime)
        }
        FilterField::UpdatedAt => {
            cursor_field("service_accounts.updated_at", CursorSqlType::DateTime)
        }
        FilterField::Revision => cursor_field("principals.revision", CursorSqlType::BigInt),
        _ => {
            return Err(PostgresStorageError::invalid_input(format!(
                "Field '{field}' is not orderable for service accounts"
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

fn service_account_event(
    account: &ServiceAccountRow,
    name: &str,
    action: Action,
    context: &EventContext,
    summary: String,
) -> Result<NewEvent, PostgresStorageError> {
    NewEvent::new(
        EntityType::ServiceAccount,
        action,
        context.actor_kind(),
        summary,
    )
    .map_err(|error| PostgresStorageError::database(error.to_string()))
    .and_then(|event| {
        Ok(event
            .with_context(context)
            .with_entity_id(hubuum_events_core::EventEntityId::new(account.id)?)
            .with_entity_name(name.to_string()))
    })
}

fn validate_positive_id(id: i32, field: &str) -> Result<(), PostgresStorageError> {
    if id > 0 {
        Ok(())
    } else {
        Err(PostgresStorageError::invalid_input(format!(
            "{field} must be greater than zero"
        )))
    }
}
