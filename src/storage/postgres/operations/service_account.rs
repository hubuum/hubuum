use crate::pagination::{CursorSqlField, CursorSqlMapping, CursorSqlType};
use serde_json::json;

use crate::errors::ApiError;
use crate::events::{Action, EntityType, EventContext, NewEvent};
use crate::models::identity::LOCAL_IDENTITY_SCOPE;
use crate::models::principal::{NewPrincipal, Principal, PrincipalID, PrincipalKind};
use crate::models::search::{FilterField, QueryOptions, SortParam};
use crate::models::{
    NewServiceAccount, ServiceAccount, ServiceAccountID, ServiceAccountWithName, TaskKind,
    TaskStatus, UpdateServiceAccount,
};
use crate::schema::service_accounts;
use crate::storage::postgres::PostgresPool;
use crate::storage::postgres::operations::authz::AuthzSubject as PostgresAuthzSubject;
use crate::storage::postgres::operations::event_record::emit_event;
use crate::storage::postgres::operations::identity::identity_scope_by_name;
use crate::storage::postgres::operations::principal::{
    InsertPrincipalRecord, lock_principal_revision_conn, principal_revision_conn,
};
use crate::storage::postgres::operations::task::{
    QueuedTaskCancellation, cancel_queued_tasks_conn,
};
use crate::storage::postgres::operations::task_rows::TaskRow as TaskRecord;
use crate::storage::postgres::operations::token::revoke_all_tokens_for_principal_conn;
use crate::storage::postgres::prelude::*;
use crate::storage::postgres::{PostgresConnection, with_connection, with_transaction};
use crate::traits::{AuthzSubject, CursorPaginated, CursorValue};

#[derive(Debug, Queryable, Selectable, Clone)]
#[diesel(table_name = crate::schema::service_accounts)]
pub(crate) struct ServiceAccountRow {
    pub(crate) id: i32,
    pub(crate) kind: String,
    pub(crate) description: String,
    pub(crate) owner_group_id: i32,
    pub(crate) created_by: Option<i32>,
    pub(crate) disabled_at: Option<chrono::NaiveDateTime>,
    pub(crate) created_at: chrono::NaiveDateTime,
    pub(crate) updated_at: chrono::NaiveDateTime,
}

impl From<ServiceAccountRow> for ServiceAccount {
    fn from(row: ServiceAccountRow) -> Self {
        Self {
            id: row.id,
            kind: row.kind,
            description: row.description,
            owner_group_id: row.owner_group_id,
            created_by: row.created_by,
            disabled_at: row.disabled_at,
            created_at: row.created_at,
            updated_at: row.updated_at,
        }
    }
}

#[derive(AsChangeset)]
#[diesel(table_name = crate::schema::service_accounts)]
struct UpdateServiceAccountRow<'a> {
    description: Option<&'a String>,
    owner_group_id: Option<i32>,
}

impl<'a> From<&'a UpdateServiceAccount> for UpdateServiceAccountRow<'a> {
    fn from(update: &'a UpdateServiceAccount) -> Self {
        Self {
            description: update.description.as_ref(),
            owner_group_id: update.owner_group_id,
        }
    }
}

struct ServiceAccountWithNameQueryRow(ServiceAccountWithName);

impl CursorPaginated for ServiceAccountWithNameQueryRow {
    fn supports_sort(field: &FilterField) -> bool {
        ServiceAccountWithName::supports_sort(field)
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        self.0.cursor_value(field)
    }

    fn default_sort() -> Vec<SortParam> {
        ServiceAccountWithName::default_sort()
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        ServiceAccountWithName::tie_breaker_sort()
    }
}

impl CursorSqlMapping for ServiceAccountWithNameQueryRow {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::Id => CursorSqlField {
                column: "service_accounts.id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Name => CursorSqlField {
                column: "principals.name",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::IdentityScope => CursorSqlField {
                column: "identity_scopes.name",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::CreatedAt => CursorSqlField {
                column: "service_accounts.created_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::UpdatedAt => CursorSqlField {
                column: "service_accounts.updated_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::Revision => CursorSqlField {
                column: "principals.revision",
                sql_type: CursorSqlType::BigInt,
                nullable: false,
            },
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for service accounts",
                    field
                )));
            }
        })
    }
}

pub trait SaveServiceAccount {
    async fn save(
        &self,
        pool: &PostgresPool,
        created_by: Option<i32>,
        event_context: &EventContext,
    ) -> Result<ServiceAccount, ApiError>;

    #[cfg(any(test, feature = "integration-test-support"))]
    async fn save_without_events(
        &self,
        pool: &PostgresPool,
        created_by: Option<i32>,
    ) -> Result<ServiceAccount, ApiError>;
}

impl SaveServiceAccount for NewServiceAccount {
    async fn save(
        &self,
        pool: &PostgresPool,
        created_by: Option<i32>,
        event_context: &EventContext,
    ) -> Result<ServiceAccount, ApiError> {
        save_service_account(self, pool, created_by, Some(event_context)).await
    }

    #[cfg(any(test, feature = "integration-test-support"))]
    async fn save_without_events(
        &self,
        pool: &PostgresPool,
        created_by: Option<i32>,
    ) -> Result<ServiceAccount, ApiError> {
        save_service_account(self, pool, created_by, None).await
    }
}

async fn save_service_account(
    account: &NewServiceAccount,
    pool: &PostgresPool,
    created_by: Option<i32>,
    event_context: Option<&EventContext>,
) -> Result<ServiceAccount, ApiError> {
    let name = account.name.clone();
    let description = account.description.clone().unwrap_or_default();
    let owner_group_id = account.owner_group_id.id();
    let scope_name = account
        .identity_scope
        .as_deref()
        .unwrap_or(LOCAL_IDENTITY_SCOPE);
    if scope_name != LOCAL_IDENTITY_SCOPE {
        return Err(ApiError::BadRequest(
            "service accounts in non-local identity scopes are managed by their identity provider"
                .to_string(),
        ));
    }
    let local_scope = identity_scope_by_name(pool, LOCAL_IDENTITY_SCOPE).await?;

    with_transaction(pool, async |conn| -> Result<ServiceAccount, ApiError> {
        let principal = NewPrincipal {
            identity_scope_id: local_scope.id,
            kind: PrincipalKind::ServiceAccount.as_str(),
            name: &name,
        }
        .insert(conn)
        .await?;

        let sa: ServiceAccount = diesel::insert_into(service_accounts::table)
            .values((
                service_accounts::id.eq(principal.id),
                service_accounts::description.eq(&description),
                service_accounts::owner_group_id.eq(owner_group_id),
                service_accounts::created_by.eq(created_by),
            ))
            .get_result::<ServiceAccountRow>(conn)
            .await?
            .into();
        let revision = principal_revision_conn(conn, principal.id).await?;
        if let Some(event_context) = event_context {
            let event = NewEvent::new(
                EntityType::ServiceAccount,
                Action::Created,
                event_context.actor_kind(),
                format!("Service account '{name}' created"),
            )?
            .with_context(event_context)
            .with_entity_id(sa.id)
            .with_entity_name(&name)
            .with_after(json!({
                "id": sa.id,
                "name": name,
                "description": sa.description,
                "owner_group_id": sa.owner_group_id,
                "created_by": sa.created_by,
                "disabled_at": sa.disabled_at,
                "revision": revision,
            }))
            .with_metadata(json!({
                "owner_group_id": sa.owner_group_id,
                "created_by": created_by,
            }));
            emit_event(conn, &event).await?;
        }

        Ok(sa)
    })
    .await
}

pub(crate) async fn update_service_account_record(
    update: &UpdateServiceAccount,
    pool: &crate::storage::postgres::PostgresPool,
    service_account_id: i32,
    event_context: Option<&EventContext>,
) -> Result<ServiceAccount, ApiError> {
    use crate::schema::service_accounts::dsl::{id, service_accounts as sa_table};

    with_transaction(pool, async |conn| -> Result<ServiceAccount, ApiError> {
        let before_revision = lock_principal_revision_conn(conn, service_account_id).await?;
        let before = sa_table
            .filter(id.eq(service_account_id))
            .first::<ServiceAccountRow>(conn)
            .await?
            .into();
        if !update.has_changes(&before) {
            return Ok(before);
        }
        let updated: ServiceAccount = diesel::update(sa_table.filter(id.eq(service_account_id)))
            .set(UpdateServiceAccountRow::from(update))
            .get_result::<ServiceAccountRow>(conn)
            .await?
            .into();
        let after_revision = principal_revision_conn(conn, service_account_id).await?;
        if let Some(event_context) = event_context {
            let name = load_principal_name_by_id(conn, updated.id).await?;
            let event = NewEvent::new(
                EntityType::ServiceAccount,
                Action::Updated,
                event_context.actor_kind(),
                format!("Service account '{name}' updated"),
            )?
            .with_context(event_context)
            .with_entity_id(updated.id)
            .with_entity_name(&name)
            .with_before(service_account_snapshot(&before, &name, before_revision))
            .with_after(service_account_snapshot(&updated, &name, after_revision))
            .with_metadata(json!({
                "owner_group_id": updated.owner_group_id,
            }));
            emit_event(conn, &event).await?;
        }
        Ok(updated)
    })
    .await
}

pub trait DisableServiceAccount {
    #[cfg(any(test, feature = "integration-test-support"))]
    async fn disable_without_events(&self, pool: &PostgresPool)
    -> Result<ServiceAccount, ApiError>;

    async fn disable(
        &self,
        pool: &PostgresPool,
        event_context: &EventContext,
    ) -> Result<ServiceAccount, ApiError>;
}

impl DisableServiceAccount for ServiceAccountID {
    #[cfg(any(test, feature = "integration-test-support"))]
    async fn disable_without_events(
        &self,
        pool: &PostgresPool,
    ) -> Result<ServiceAccount, ApiError> {
        disable_service_account(self, pool, None).await
    }

    async fn disable(
        &self,
        pool: &PostgresPool,
        event_context: &EventContext,
    ) -> Result<ServiceAccount, ApiError> {
        disable_service_account(self, pool, Some(event_context)).await
    }
}

async fn disable_service_account(
    account_id: &ServiceAccountID,
    pool: &PostgresPool,
    event_context: Option<&EventContext>,
) -> Result<ServiceAccount, ApiError> {
    let sa_id = account_id.id();
    let (disabled, cancelled_tasks) = with_transaction(pool, async |conn| {
        disable_service_account_conn(conn, sa_id, event_context).await
    })
    .await?;

    record_cancelled_task_metrics(&cancelled_tasks);
    Ok(disabled)
}

async fn disable_service_account_conn(
    conn: &mut PostgresConnection,
    service_account_id: i32,
    event_context: Option<&EventContext>,
) -> Result<(ServiceAccount, Vec<TaskRecord>), ApiError> {
    use crate::schema::service_accounts::dsl::{disabled_at, id, service_accounts as sa_table};

    let before_revision = lock_principal_revision_conn(conn, service_account_id).await?;
    let before: ServiceAccount = sa_table
        .filter(id.eq(service_account_id))
        .first::<ServiceAccountRow>(conn)
        .await?
        .into();
    let disabled = if before.disabled_at.is_some() {
        before
    } else {
        let disabled: ServiceAccount = diesel::update(sa_table.filter(id.eq(service_account_id)))
            .set(disabled_at.eq(diesel::dsl::now))
            .get_result::<ServiceAccountRow>(conn)
            .await?
            .into();
        let after_revision = principal_revision_conn(conn, service_account_id).await?;
        if let Some(event_context) = event_context {
            let name = load_principal_name_by_id(conn, disabled.id).await?;
            let event = NewEvent::new(
                EntityType::ServiceAccount,
                Action::Disabled,
                event_context.actor_kind(),
                format!("Service account '{name}' disabled"),
            )?
            .with_context(event_context)
            .with_entity_id(disabled.id)
            .with_entity_name(&name)
            .with_before(service_account_snapshot(&before, &name, before_revision))
            .with_after(service_account_snapshot(&disabled, &name, after_revision))
            .with_metadata(json!({
                "owner_group_id": disabled.owner_group_id,
            }));
            emit_event(conn, &event).await?;
        }
        disabled
    };

    revoke_all_tokens_for_principal_conn(conn, PrincipalID::new(service_account_id)?).await?;
    let actor = event_context
        .and_then(EventContext::actor_user_id)
        .map(PrincipalID::new)
        .transpose()?;
    let cancelled_tasks = cancel_pending_tasks_for_principal_conn(
        conn,
        service_account_id,
        actor,
        event_context.is_some(),
    )
    .await?;

    Ok((disabled, cancelled_tasks))
}

pub(crate) async fn delete_service_account(
    account_id: &ServiceAccountID,
    pool: &crate::storage::postgres::PostgresPool,
    event_context: Option<&EventContext>,
) -> Result<(), ApiError> {
    use crate::schema::principals::dsl::{id, principals as principals_table};
    let sa_id = account_id.id();
    with_transaction(pool, async |conn| -> Result<(), ApiError> {
        let before_revision = lock_principal_revision_conn(conn, sa_id).await?;
        let sa = load_service_account_by_id_conn(conn, sa_id).await?;
        if let Some(event_context) = event_context {
            let name = load_principal_name_by_id(conn, sa_id).await?;
            let event = NewEvent::new(
                EntityType::ServiceAccount,
                Action::Deleted,
                event_context.actor_kind(),
                format!("Service account '{name}' deleted"),
            )?
            .with_context(event_context)
            .with_entity_id(sa_id)
            .with_entity_name(&name)
            .with_before(service_account_snapshot(&sa, &name, before_revision))
            .with_metadata(json!({
                "owner_group_id": sa.owner_group_id,
            }));
            emit_event(conn, &event).await?;
        }
        diesel::delete(principals_table.filter(id.eq(sa_id)))
            .execute(conn)
            .await?;
        Ok(())
    })
    .await
}

async fn load_principal_name_by_id(
    conn: &mut PostgresConnection,
    principal_id_value: i32,
) -> Result<String, ApiError> {
    use crate::schema::principals::dsl::{id, name, principals};

    principals
        .filter(id.eq(principal_id_value))
        .select(name)
        .first::<String>(conn)
        .await
        .map_err(ApiError::from)
}

async fn load_service_account_by_id_conn(
    conn: &mut PostgresConnection,
    service_account_id: i32,
) -> Result<ServiceAccount, ApiError> {
    use crate::schema::service_accounts::dsl::{id, service_accounts as sa_table};
    sa_table
        .filter(id.eq(service_account_id))
        .first::<ServiceAccountRow>(conn)
        .await
        .map(Into::into)
        .map_err(ApiError::from)
}

fn service_account_snapshot(
    sa: &ServiceAccount,
    name: &str,
    revision: PostgresRevision,
) -> serde_json::Value {
    json!({
        "id": sa.id,
        "name": name,
        "description": sa.description,
        "owner_group_id": sa.owner_group_id,
        "created_by": sa.created_by,
        "disabled_at": sa.disabled_at,
        "revision": revision,
    })
}

/// Is `principal_id` a **human** member of `owner_group_id`?
pub async fn is_human_owner_group_member(
    pool: &crate::storage::postgres::PostgresPool,
    principal_id: i32,
    owner_group_id: i32,
) -> Result<bool, ApiError> {
    use crate::schema::group_memberships;
    use crate::schema::principals;
    use diesel::dsl::{exists, select};

    with_connection(pool, async |conn| {
        select(exists(
            group_memberships::table
                .inner_join(
                    principals::table.on(principals::id.eq(group_memberships::principal_id)),
                )
                .filter(group_memberships::group_id.eq(owner_group_id))
                .filter(group_memberships::principal_id.eq(principal_id))
                .filter(principals::kind.eq(PrincipalKind::Human.as_str())),
        ))
        .get_result(conn)
        .await
    })
    .await
}

pub async fn principal_is_disabled(
    pool: &crate::storage::postgres::PostgresPool,
    principal: &Principal,
) -> Result<bool, ApiError> {
    if !principal.is_service_account() {
        return Ok(false);
    }
    let sa = load_service_account_by_id(pool, principal.id).await?;
    Ok(sa.is_disabled())
}

/// Soft-revoke all tokens belonging to a principal (used when disabling an SA).
pub async fn revoke_all_tokens_for_principal(
    pool: &crate::storage::postgres::PostgresPool,
    principal_id_value: i32,
) -> Result<usize, ApiError> {
    let principal_id = PrincipalID::new(principal_id_value)?;
    with_connection(pool, async |conn| {
        revoke_all_tokens_for_principal_conn(conn, principal_id).await
    })
    .await
}

pub async fn cancel_pending_tasks_for_principal(
    pool: &crate::storage::postgres::PostgresPool,
    principal_id_value: i32,
) -> Result<usize, ApiError> {
    let cancelled_tasks = with_transaction(pool, async |conn| {
        cancel_pending_tasks_for_principal_conn(conn, principal_id_value, None, true).await
    })
    .await?;

    record_cancelled_task_metrics(&cancelled_tasks);
    Ok(cancelled_tasks.len())
}

async fn cancel_pending_tasks_for_principal_conn(
    conn: &mut PostgresConnection,
    principal_id_value: i32,
    actor: Option<PrincipalID>,
    emit_events: bool,
) -> Result<Vec<TaskRecord>, ApiError> {
    use crate::schema::tasks::dsl::{id, kind, status, submitted_by, tasks};

    let task_ids = tasks
        .filter(submitted_by.eq(principal_id_value))
        .filter(status.eq(TaskStatus::Queued.as_str()))
        .filter(kind.ne(TaskKind::Reindex.as_str()))
        .select(id)
        .load::<i32>(conn)
        .await?;
    let cancellation =
        QueuedTaskCancellation::new("Task cancelled because its submitting principal was disabled")
            .with_actor(actor)
            .with_event_emission(emit_events);
    cancel_queued_tasks_conn(conn, &task_ids, &cancellation).await
}

fn record_cancelled_task_metrics(cancelled_tasks: &[TaskRecord]) {
    for task in cancelled_tasks {
        crate::observability::metrics::task_completed(
            &task.kind,
            TaskStatus::Cancelled.as_str(),
            None,
        );
    }
}

pub async fn service_accounts_owned_by_group(
    pool: &crate::storage::postgres::PostgresPool,
    owner_group: i32,
) -> Result<Vec<(i32, String)>, ApiError> {
    use crate::schema::principals;
    use crate::schema::service_accounts;
    with_connection(pool, async |conn| {
        service_accounts::table
            .inner_join(principals::table.on(principals::id.eq(service_accounts::id)))
            .filter(service_accounts::owner_group_id.eq(owner_group))
            .select((service_accounts::id, principals::name))
            .load::<(i32, String)>(conn)
            .await
    })
    .await
}

pub async fn load_service_account_by_id(
    pool: &crate::storage::postgres::PostgresPool,
    service_account_id: i32,
) -> Result<ServiceAccount, ApiError> {
    use crate::schema::service_accounts::dsl::{id, service_accounts as sa_table};
    with_connection(pool, async |conn| {
        sa_table
            .filter(id.eq(service_account_id))
            .first::<ServiceAccountRow>(conn)
            .await
            .map(Into::into)
    })
    .await
}

pub async fn search_manageable_service_accounts<S>(
    pool: &crate::storage::postgres::PostgresPool,
    requestor: &S,
    is_admin: bool,
    query_options: QueryOptions,
) -> Result<Vec<ServiceAccountWithName>, ApiError>
where
    S: AuthzSubject + ?Sized,
{
    use crate::schema::identity_scopes;
    use crate::schema::principals;
    use crate::schema::service_accounts::dsl::{
        created_at, id, owner_group_id, service_accounts, updated_at,
    };
    use crate::{apply_query_options, date_search, numeric_search, string_search};

    let mut base_query = service_accounts
        .inner_join(principals::table.on(principals::id.eq(id)))
        .inner_join(
            identity_scopes::table.on(principals::identity_scope_id.eq(identity_scopes::id)),
        )
        .into_boxed();
    if !is_admin {
        base_query = base_query.filter(owner_group_id.eq_any(requestor.group_ids_subquery()));
    }

    for param in query_options.filters.clone() {
        let operator = param.operator.clone();
        match param.field {
            FilterField::Id => numeric_search!(base_query, param, operator, id),
            FilterField::Name => {
                string_search!(base_query, param, operator, principals::name)
            }
            FilterField::IdentityScope => {
                string_search!(base_query, param, operator, identity_scopes::name)
            }
            FilterField::CreatedAt => date_search!(base_query, param, operator, created_at),
            FilterField::UpdatedAt => date_search!(base_query, param, operator, updated_at),
            FilterField::Revision => {
                crate::revision_search!(base_query, param, operator, principals::revision)
            }
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' isn't searchable for service accounts",
                    param.field
                )));
            }
        }
    }

    apply_query_options!(base_query, query_options, ServiceAccountWithNameQueryRow);

    let rows = with_connection(pool, async |conn| {
        base_query
            .select((
                ServiceAccountRow::as_select(),
                identity_scopes::name,
                principals::name,
                principals::revision,
            ))
            .load::<(ServiceAccountRow, String, String, PostgresRevision)>(conn)
            .await
    })
    .await?;

    Ok(rows
        .into_iter()
        .map(|(account, scope, name, revision)| {
            ServiceAccountWithName::from_tuple((
                account.into(),
                scope,
                name,
                revision.into_domain(),
            ))
        })
        .collect())
}

pub async fn count_manageable_service_accounts<S>(
    pool: &crate::storage::postgres::PostgresPool,
    requestor: &S,
    is_admin: bool,
    query_options: QueryOptions,
) -> Result<i64, ApiError>
where
    S: AuthzSubject + ?Sized,
{
    use crate::schema::identity_scopes;
    use crate::schema::principals;
    use crate::schema::service_accounts::dsl::{
        created_at, id, owner_group_id, service_accounts, updated_at,
    };
    use crate::{date_search, numeric_search, revision_search, string_search};

    let mut base_query = service_accounts
        .inner_join(principals::table.on(principals::id.eq(id)))
        .inner_join(
            identity_scopes::table.on(principals::identity_scope_id.eq(identity_scopes::id)),
        )
        .into_boxed();
    if !is_admin {
        base_query = base_query.filter(owner_group_id.eq_any(requestor.group_ids_subquery()));
    }

    for param in query_options.filters.clone() {
        let operator = param.operator.clone();
        match param.field {
            FilterField::Id => numeric_search!(base_query, param, operator, id),
            FilterField::Name => {
                string_search!(base_query, param, operator, principals::name)
            }
            FilterField::IdentityScope => {
                string_search!(base_query, param, operator, identity_scopes::name)
            }
            FilterField::CreatedAt => date_search!(base_query, param, operator, created_at),
            FilterField::UpdatedAt => date_search!(base_query, param, operator, updated_at),
            FilterField::Revision => {
                revision_search!(base_query, param, operator, principals::revision)
            }
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' isn't searchable for service accounts",
                    param.field
                )));
            }
        }
    }

    with_connection(pool, async |conn| {
        base_query.count().get_result::<i64>(conn).await
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{tasks, tokens};
    use crate::storage::postgres::operations::task_rows::{
        NewTaskRow as NewTaskRecord, TaskRow as TaskRecord,
    };
    use crate::tests::{
        TestContext, create_test_group, create_test_service_account, create_test_user,
        service_account_token,
    };

    fn queued_export_task(submitter_id: i32, idempotency_key: String) -> NewTaskRecord {
        NewTaskRecord {
            kind: TaskKind::Export.as_str().to_string(),
            status: TaskStatus::Queued.as_str().to_string(),
            submitted_by: Some(submitter_id),
            idempotency_key: Some(idempotency_key),
            request_hash: None,
            request_payload: Some(serde_json::json!({"secret": "redact-me"})),
            summary: None,
            total_items: 0,
            processed_items: 0,
            success_items: 0,
            failed_items: 0,
            submitted_token_id: None,
            submitted_token_scoped: false,
            submitted_token_scopes: serde_json::json!([]),
            request_redacted_at: None,
            started_at: None,
            finished_at: None,
        }
    }

    #[tokio::test]
    async fn cancelling_pending_tasks_records_a_terminal_timestamp() {
        let context = TestContext::new().await;
        let submitter_id = create_test_user(&context.pool).await.id;

        let cancelled_task = with_transaction(
            &context.pool,
            async |conn| -> Result<TaskRecord, ApiError> {
                let task = diesel::insert_into(tasks::table)
                    .values(queued_export_task(
                        submitter_id,
                        context.scoped_name("cancel-terminal-timestamp"),
                    ))
                    .get_result::<TaskRecord>(conn)
                    .await?;

                let cancelled =
                    cancel_pending_tasks_for_principal_conn(conn, submitter_id, None, true).await?;
                assert_eq!(cancelled.len(), 1);
                assert_eq!(cancelled[0].kind, TaskKind::Export.as_str());

                Ok(tasks::table.find(task.id).first::<TaskRecord>(conn).await?)
            },
        )
        .await
        .unwrap();

        assert_eq!(cancelled_task.status, TaskStatus::Cancelled.as_str());
        assert!(cancelled_task.finished_at.is_some());
        assert!(cancelled_task.request_payload.is_none());
        assert!(cancelled_task.request_redacted_at.is_some());
        assert!(cancelled_task.updated_at >= cancelled_task.created_at);
    }

    #[tokio::test]
    async fn disabling_service_account_atomically_revokes_tokens_and_cancels_tasks() {
        let context = TestContext::new().await;
        let owner_group = create_test_group(&context.pool).await;
        let service_account = create_test_service_account(&context.pool, &owner_group, None).await;
        let _token = service_account_token(&context.pool, &service_account, None, None).await;

        let cancelled_task = with_transaction(
            &context.pool,
            async |conn| -> Result<TaskRecord, ApiError> {
                let task = diesel::insert_into(tasks::table)
                    .values(queued_export_task(
                        service_account.id,
                        context.scoped_name("atomic-disable"),
                    ))
                    .get_result::<TaskRecord>(conn)
                    .await?;

                let (disabled, cancelled) =
                    disable_service_account_conn(conn, service_account.id, None).await?;
                assert!(disabled.disabled_at.is_some());
                assert_eq!(cancelled.len(), 1);
                assert_eq!(cancelled[0].id, task.id);

                let revoked_at = tokens::table
                    .filter(tokens::principal_id.eq(service_account.id))
                    .select(tokens::revoked_at)
                    .first::<Option<chrono::NaiveDateTime>>(conn)
                    .await?;
                assert!(revoked_at.is_some());

                Ok(tasks::table.find(task.id).first::<TaskRecord>(conn).await?)
            },
        )
        .await
        .unwrap();

        assert_eq!(cancelled_task.status, TaskStatus::Cancelled.as_str());
        assert!(cancelled_task.finished_at.is_some());
        assert!(cancelled_task.request_payload.is_none());
        assert!(cancelled_task.request_redacted_at.is_some());
    }
}
