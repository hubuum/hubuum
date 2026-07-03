use diesel::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::json;
use utoipa::ToSchema;

use crate::db::{DbPool, with_connection, with_transaction};
use crate::errors::ApiError;
use crate::events::{Action, EntityType, EventContext, NewEvent, emit_event};
use crate::models::principal::{NewPrincipal, Principal, PrincipalKind};
use crate::models::search::{FilterField, QueryOptions, SortParam};
use crate::schema::service_accounts;
use crate::traits::accessors::{IdAccessor, InstanceAdapter};
use crate::traits::{
    AuthzSubject, BackendContext, CursorPaginated, CursorSqlField, CursorSqlMapping, CursorSqlType,
    CursorValue,
};

/// A non-human principal used by automation/integrations. Its id is the
/// principal id and its name lives on `principals.name`; this row carries the
/// service-account-specific lifecycle (owner group, disabled state).
#[derive(
    Serialize, Deserialize, Queryable, Selectable, Insertable, PartialEq, Debug, Clone, ToSchema,
)]
#[diesel(table_name = service_accounts)]
pub struct ServiceAccount {
    pub id: i32,
    pub kind: String,
    pub description: String,
    pub owner_group_id: i32,
    pub created_by: Option<i32>,
    pub disabled_at: Option<chrono::NaiveDateTime>,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
}

impl ServiceAccount {
    pub fn is_disabled(&self) -> bool {
        self.disabled_at.is_some()
    }
}

impl IdAccessor for ServiceAccount {
    fn accessor_id(&self) -> i32 {
        self.id
    }
}

impl InstanceAdapter<ServiceAccount> for ServiceAccount {
    async fn instance_adapter(&self, _pool: &DbPool) -> Result<ServiceAccount, ApiError> {
        Ok(self.clone())
    }
}

/// Public response shape, combining the service-account row with its principal
/// name (the name lives on `principals`).
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, ToSchema)]
pub struct ServiceAccountResponse {
    pub id: i32,
    pub name: String,
    pub description: String,
    pub owner_group_id: i32,
    pub created_by: Option<i32>,
    pub disabled_at: Option<chrono::NaiveDateTime>,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
}

impl ServiceAccountResponse {
    pub fn from_parts(sa: &ServiceAccount, name: String) -> Self {
        Self {
            id: sa.id,
            name,
            description: sa.description.clone(),
            owner_group_id: sa.owner_group_id,
            created_by: sa.created_by,
            disabled_at: sa.disabled_at,
            created_at: sa.created_at,
            updated_at: sa.updated_at,
        }
    }
}

/// List/search projection: the `service_accounts` row plus the principal name
/// (the name lives on `principals`). Drives cursor pagination without smuggling a
/// non-table field into the `ServiceAccount` Diesel mapping.
#[derive(Debug, Clone)]
pub struct ServiceAccountWithName {
    pub service_account: ServiceAccount,
    pub name: String,
}

impl ServiceAccountWithName {
    pub fn from_tuple(t: (ServiceAccount, String)) -> Self {
        Self {
            service_account: t.0,
            name: t.1,
        }
    }
}

impl From<ServiceAccountWithName> for ServiceAccountResponse {
    fn from(value: ServiceAccountWithName) -> Self {
        ServiceAccountResponse::from_parts(&value.service_account, value.name)
    }
}

impl CursorPaginated for ServiceAccountWithName {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(
            field,
            FilterField::Id | FilterField::Name | FilterField::CreatedAt | FilterField::UpdatedAt
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        Ok(match field {
            FilterField::Id => CursorValue::Integer(self.service_account.id as i64),
            FilterField::Name => CursorValue::String(self.name.clone()),
            FilterField::CreatedAt => CursorValue::DateTime(self.service_account.created_at),
            FilterField::UpdatedAt => CursorValue::DateTime(self.service_account.updated_at),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for service accounts",
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

impl CursorSqlMapping for ServiceAccountWithName {
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
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for service accounts",
                    field
                )));
            }
        })
    }
}

/// Request body to create a service account.
#[derive(Deserialize, Serialize, Debug, ToSchema)]
#[schema(example = new_service_account_example)]
pub struct NewServiceAccount {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    pub owner_group_id: i32,
}

impl NewServiceAccount {
    pub async fn save<C>(
        &self,
        backend: &C,
        created_by: Option<i32>,
        event_context: &EventContext,
    ) -> Result<ServiceAccount, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.save_impl(backend, created_by, Some(event_context))
            .await
    }

    /// Persist without emitting domain events.
    ///
    /// This helper is available only in tests and is intended for fixture
    /// construction and event-system tests. Normal application code should use
    /// [`NewServiceAccount::save`] so event subscribers observe the change.
    #[cfg(test)]
    pub async fn save_without_events<C>(
        &self,
        backend: &C,
        created_by: Option<i32>,
    ) -> Result<ServiceAccount, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.save_impl(backend, created_by, None).await
    }

    /// Create the principal (kind=service_account) and service_account rows in a
    /// single transaction (principal-first id allocation).
    async fn save_impl<C>(
        &self,
        backend: &C,
        created_by: Option<i32>,
        event_context: Option<&EventContext>,
    ) -> Result<ServiceAccount, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        let name = self.name.clone();
        let description = self.description.clone().unwrap_or_default();
        let owner_group_id = self.owner_group_id;

        with_transaction(
            backend.db_pool(),
            |conn| -> Result<ServiceAccount, ApiError> {
                let principal = NewPrincipal {
                    kind: PrincipalKind::ServiceAccount.as_str(),
                    name: &name,
                }
                .insert(conn)?;

                let sa = diesel::insert_into(service_accounts::table)
                    .values((
                        service_accounts::id.eq(principal.id),
                        service_accounts::description.eq(&description),
                        service_accounts::owner_group_id.eq(owner_group_id),
                        service_accounts::created_by.eq(created_by),
                    ))
                    .get_result::<ServiceAccount>(conn)?;
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
                    }))
                    .with_metadata(json!({
                        "owner_group_id": sa.owner_group_id,
                        "created_by": created_by,
                    }));
                    emit_event(conn, &event)?;
                }

                Ok(sa)
            },
        )
    }
}

/// Mutable fields on a service account.
#[derive(Deserialize, Serialize, AsChangeset, Debug, ToSchema)]
#[diesel(table_name = service_accounts)]
pub struct UpdateServiceAccount {
    pub description: Option<String>,
    pub owner_group_id: Option<i32>,
}

impl UpdateServiceAccount {
    pub async fn save<C>(
        &self,
        service_account_id: i32,
        backend: &C,
        event_context: &EventContext,
    ) -> Result<ServiceAccount, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        use crate::schema::service_accounts::dsl::{id, service_accounts as sa_table};
        with_transaction(
            backend.db_pool(),
            |conn| -> Result<ServiceAccount, ApiError> {
                let before = sa_table
                    .filter(id.eq(service_account_id))
                    .first::<ServiceAccount>(conn)?;
                let updated = diesel::update(sa_table.filter(id.eq(service_account_id)))
                    .set(self)
                    .get_result::<ServiceAccount>(conn)?;
                let name = load_principal_name_by_id(conn, updated.id)?;
                let event = NewEvent::new(
                    EntityType::ServiceAccount,
                    Action::Updated,
                    event_context.actor_kind(),
                    format!("Service account '{name}' updated"),
                )?
                .with_context(event_context)
                .with_entity_id(updated.id)
                .with_entity_name(&name)
                .with_before(service_account_snapshot(&before, &name))
                .with_after(service_account_snapshot(&updated, &name))
                .with_metadata(json!({
                    "owner_group_id": updated.owner_group_id,
                }));
                emit_event(conn, &event)?;
                Ok(updated)
            },
        )
    }
}

crate::int_id_newtype! {
    /// Identifier wrapper for a [`ServiceAccount`].
    pub struct ServiceAccountID;
    noun = "service account id";
}

impl IdAccessor for ServiceAccountID {
    fn accessor_id(&self) -> i32 {
        self.0
    }
}

impl InstanceAdapter<ServiceAccount> for ServiceAccountID {
    async fn instance_adapter(&self, pool: &DbPool) -> Result<ServiceAccount, ApiError> {
        load_service_account_by_id(pool, self.id()).await
    }
}

impl ServiceAccountID {
    pub async fn service_account<C>(&self, backend: &C) -> Result<ServiceAccount, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        load_service_account_by_id(backend.db_pool(), self.id()).await
    }

    /// Disable this service account without emitting domain events.
    ///
    /// This helper is available only in tests and is intended for fixture
    /// cleanup and event-system tests. Normal application code should use
    /// [`ServiceAccountID::disable`] so event subscribers observe the change.
    #[cfg(test)]
    pub async fn disable_without_events<C>(&self, backend: &C) -> Result<ServiceAccount, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.disable_impl(backend, None).await
    }

    /// Mark the service account disabled. Token soft-revocation and queued-task
    /// handling are performed by the caller/handler.
    pub async fn disable<C>(
        &self,
        backend: &C,
        event_context: &EventContext,
    ) -> Result<ServiceAccount, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.disable_impl(backend, Some(event_context)).await
    }

    async fn disable_impl<C>(
        &self,
        backend: &C,
        event_context: Option<&EventContext>,
    ) -> Result<ServiceAccount, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        use crate::schema::service_accounts::dsl::{disabled_at, id, service_accounts as sa_table};
        let sa_id = self.id();
        with_transaction(
            backend.db_pool(),
            |conn| -> Result<ServiceAccount, ApiError> {
                let before = sa_table
                    .filter(id.eq(sa_id))
                    .first::<ServiceAccount>(conn)?;
                let disabled = diesel::update(sa_table.filter(id.eq(sa_id)))
                    .set(disabled_at.eq(diesel::dsl::now))
                    .get_result::<ServiceAccount>(conn)?;
                if let Some(event_context) = event_context {
                    let name = load_principal_name_by_id(conn, disabled.id)?;
                    let event = NewEvent::new(
                        EntityType::ServiceAccount,
                        Action::Disabled,
                        event_context.actor_kind(),
                        format!("Service account '{name}' disabled"),
                    )?
                    .with_context(event_context)
                    .with_entity_id(disabled.id)
                    .with_entity_name(&name)
                    .with_before(service_account_snapshot(&before, &name))
                    .with_after(service_account_snapshot(&disabled, &name))
                    .with_metadata(json!({
                        "owner_group_id": disabled.owner_group_id,
                    }));
                    emit_event(conn, &event)?;
                }
                Ok(disabled)
            },
        )
    }

    /// Delete the service account by removing its principal row (cascades to the
    /// service_accounts row, group memberships, and tokens).
    pub async fn delete<C>(
        &self,
        backend: &C,
        event_context: &EventContext,
    ) -> Result<usize, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        use crate::schema::principals::dsl::{id, principals as principals_table};
        let sa_id = self.id();
        with_transaction(backend.db_pool(), |conn| -> Result<usize, ApiError> {
            let sa = load_service_account_by_id_conn(conn, sa_id)?;
            let name = load_principal_name_by_id(conn, sa_id)?;
            let event = NewEvent::new(
                EntityType::ServiceAccount,
                Action::Deleted,
                event_context.actor_kind(),
                format!("Service account '{name}' deleted"),
            )?
            .with_context(event_context)
            .with_entity_id(sa_id)
            .with_entity_name(&name)
            .with_before(service_account_snapshot(&sa, &name))
            .with_metadata(json!({
                "owner_group_id": sa.owner_group_id,
            }));
            emit_event(conn, &event)?;
            diesel::delete(principals_table.filter(id.eq(sa_id)))
                .execute(conn)
                .map_err(ApiError::from)
        })
    }
}

fn load_principal_name_by_id(
    conn: &mut PgConnection,
    principal_id_value: i32,
) -> Result<String, ApiError> {
    use crate::schema::principals::dsl::{id, name, principals};

    principals
        .filter(id.eq(principal_id_value))
        .select(name)
        .first::<String>(conn)
        .map_err(ApiError::from)
}

fn load_service_account_by_id_conn(
    conn: &mut PgConnection,
    service_account_id: i32,
) -> Result<ServiceAccount, ApiError> {
    use crate::schema::service_accounts::dsl::{id, service_accounts as sa_table};
    sa_table
        .filter(id.eq(service_account_id))
        .first::<ServiceAccount>(conn)
        .map_err(ApiError::from)
}

fn service_account_snapshot(sa: &ServiceAccount, name: &str) -> serde_json::Value {
    json!({
        "id": sa.id,
        "name": name,
        "description": sa.description,
        "owner_group_id": sa.owner_group_id,
        "created_by": sa.created_by,
        "disabled_at": sa.disabled_at,
    })
}

/// Is `principal_id` a **human** member of `owner_group_id`?
///
/// This is the management-authz primitive for service accounts: only human
/// owner-group members (and admins, checked separately) may manage an SA, its
/// tokens, or its tasks. A service account placed in its own owner group does
/// NOT gain self-management (least privilege).
pub async fn is_human_owner_group_member(
    pool: &DbPool,
    principal_id: i32,
    owner_group_id: i32,
) -> Result<bool, ApiError> {
    use crate::schema::group_memberships;
    use crate::schema::principals;
    use diesel::dsl::{exists, select};

    with_connection(pool, |conn| {
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
    })
}

/// Whether `principal` is currently barred from acting because it is a disabled
/// service account.
///
/// Single source for the "a disabled service account may not act" rule shared by
/// task execution and credential minting (token validation enforces the same
/// rule at the SQL layer in [`crate::db::traits::Status::is_valid`]). Human
/// principals are never disabled, so this is always `false` for them. Callers map
/// `true` to whatever status code fits their context (e.g. 409 on mint, failure
/// on a queued task).
pub async fn principal_is_disabled(pool: &DbPool, principal: &Principal) -> Result<bool, ApiError> {
    if !principal.is_service_account() {
        return Ok(false);
    }
    let sa = load_service_account_by_id(pool, principal.id).await?;
    Ok(sa.is_disabled())
}

/// Soft-revoke all tokens belonging to a principal (used when disabling an SA).
pub async fn revoke_all_tokens_for_principal(
    pool: &DbPool,
    principal_id_value: i32,
) -> Result<usize, ApiError> {
    use crate::schema::tokens::dsl::{principal_id, revoked_at, tokens};
    with_connection(pool, |conn| {
        diesel::update(
            tokens
                .filter(principal_id.eq(principal_id_value))
                .filter(revoked_at.is_null()),
        )
        .set(revoked_at.eq(diesel::dsl::now))
        .execute(conn)
    })
}

/// Cancel a principal's **queued** (not-yet-claimed) tasks when disabling an SA,
/// so pending work does not linger awaiting a worker claim.
///
/// Only `queued` tasks are cancelled. A task in `validating`/`running` has already
/// been claimed by a worker and may be performing side effects; marking it
/// `cancelled` would mislabel work we cannot actually stop. Such in-flight tasks
/// are instead caught by the worker's disabled-SA gate before dispatch (or, if
/// already executing, run to completion). The claim transition is
/// `queued -> validating` under a row lock, so a task cannot be both cancelled
/// here and claimed by a worker.
pub async fn cancel_pending_tasks_for_principal(
    pool: &DbPool,
    principal_id_value: i32,
) -> Result<usize, ApiError> {
    use crate::models::TaskStatus;
    use crate::schema::tasks::dsl::{status, submitted_by, tasks};
    with_connection(pool, |conn| {
        diesel::update(
            tasks
                .filter(submitted_by.eq(principal_id_value))
                .filter(status.eq(TaskStatus::Queued.as_str())),
        )
        .set(status.eq(TaskStatus::Cancelled.as_str()))
        .execute(conn)
    })
}

/// Service accounts owned by a group, as `(id, principal name)` pairs. Used to
/// give a clear `409 Conflict` (instead of an opaque FK error) when deleting a
/// group that still owns service accounts.
pub async fn service_accounts_owned_by_group(
    pool: &DbPool,
    owner_group: i32,
) -> Result<Vec<(i32, String)>, ApiError> {
    use crate::schema::principals;
    use crate::schema::service_accounts;
    with_connection(pool, |conn| {
        service_accounts::table
            .inner_join(principals::table.on(principals::id.eq(service_accounts::id)))
            .filter(service_accounts::owner_group_id.eq(owner_group))
            .select((service_accounts::id, principals::name))
            .load::<(i32, String)>(conn)
    })
}

/// Load a service account by id.
pub async fn load_service_account_by_id(
    pool: &DbPool,
    service_account_id: i32,
) -> Result<ServiceAccount, ApiError> {
    use crate::schema::service_accounts::dsl::{id, service_accounts as sa_table};
    with_connection(pool, |conn| {
        sa_table
            .filter(id.eq(service_account_id))
            .first::<ServiceAccount>(conn)
    })
}

/// Paginated listing of the service accounts a caller may manage, with principal
/// names. The manageability rule is pushed into SQL — an admin sees every account;
/// any other (human) caller sees only accounts whose `owner_group_id` is one of
/// their groups — replacing the previous load-all + per-row authorization scan.
pub async fn search_manageable_service_accounts<S>(
    pool: &DbPool,
    requestor: &S,
    is_admin: bool,
    query_options: QueryOptions,
) -> Result<Vec<ServiceAccountWithName>, ApiError>
where
    S: AuthzSubject + ?Sized,
{
    use crate::schema::principals;
    use crate::schema::service_accounts::dsl::{
        created_at, id, owner_group_id, service_accounts, updated_at,
    };
    use crate::{apply_query_options, date_search, numeric_search, string_search};

    let mut base_query = service_accounts
        .inner_join(principals::table.on(principals::id.eq(id)))
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
            FilterField::CreatedAt => date_search!(base_query, param, operator, created_at),
            FilterField::UpdatedAt => date_search!(base_query, param, operator, updated_at),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' isn't searchable for service accounts",
                    param.field
                )));
            }
        }
    }

    apply_query_options!(base_query, query_options, ServiceAccountWithName);

    let rows = with_connection(pool, |conn| {
        base_query
            .select((ServiceAccount::as_select(), principals::name))
            .load::<(ServiceAccount, String)>(conn)
    })?;

    Ok(rows
        .into_iter()
        .map(ServiceAccountWithName::from_tuple)
        .collect())
}

/// Count of the service accounts a caller may manage (matching filters).
pub async fn count_manageable_service_accounts<S>(
    pool: &DbPool,
    requestor: &S,
    is_admin: bool,
    query_options: QueryOptions,
) -> Result<i64, ApiError>
where
    S: AuthzSubject + ?Sized,
{
    use crate::schema::principals;
    use crate::schema::service_accounts::dsl::{
        created_at, id, owner_group_id, service_accounts, updated_at,
    };
    use crate::{date_search, numeric_search, string_search};

    let mut base_query = service_accounts
        .inner_join(principals::table.on(principals::id.eq(id)))
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
            FilterField::CreatedAt => date_search!(base_query, param, operator, created_at),
            FilterField::UpdatedAt => date_search!(base_query, param, operator, updated_at),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' isn't searchable for service accounts",
                    param.field
                )));
            }
        }
    }

    with_connection(pool, |conn| base_query.count().get_result::<i64>(conn))
}

#[allow(dead_code)]
fn new_service_account_example() -> NewServiceAccount {
    NewServiceAccount {
        name: "dns-sync".to_string(),
        description: Some("Production DNS importer".to_string()),
        owner_group_id: 1,
    }
}
