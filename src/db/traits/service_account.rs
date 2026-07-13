use serde_json::json;

use crate::db::prelude::*;
use crate::db::traits::identity::identity_scope_by_name;
use crate::db::traits::principal::InsertPrincipalRecord;
use crate::db::{DbConnection, DbPool, with_connection, with_transaction};
use crate::errors::ApiError;
use crate::events::{Action, EntityType, EventContext, NewEvent, emit_event};
use crate::models::identity::LOCAL_IDENTITY_SCOPE;
use crate::models::principal::{NewPrincipal, Principal, PrincipalKind};
use crate::models::search::{FilterField, QueryOptions};
use crate::models::{
    NewServiceAccount, ServiceAccount, ServiceAccountID, ServiceAccountWithName, TaskStatus,
    UpdateServiceAccount,
};
use crate::schema::service_accounts;
use crate::traits::accessors::InstanceAdapter;
use crate::traits::crud::{DeleteAdapter, UpdateAdapter};
use crate::traits::{AuthzSubject, BackendContext};

pub trait SaveServiceAccount {
    async fn save<C>(
        &self,
        backend: &C,
        created_by: Option<i32>,
        event_context: &EventContext,
    ) -> Result<ServiceAccount, ApiError>
    where
        C: BackendContext + ?Sized;

    #[cfg(test)]
    async fn save_without_events<C>(
        &self,
        backend: &C,
        created_by: Option<i32>,
    ) -> Result<ServiceAccount, ApiError>
    where
        C: BackendContext + ?Sized;
}

impl SaveServiceAccount for NewServiceAccount {
    async fn save<C>(
        &self,
        backend: &C,
        created_by: Option<i32>,
        event_context: &EventContext,
    ) -> Result<ServiceAccount, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        save_service_account(self, backend, created_by, Some(event_context)).await
    }

    #[cfg(test)]
    async fn save_without_events<C>(
        &self,
        backend: &C,
        created_by: Option<i32>,
    ) -> Result<ServiceAccount, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        save_service_account(self, backend, created_by, None).await
    }
}

async fn save_service_account<C>(
    account: &NewServiceAccount,
    backend: &C,
    created_by: Option<i32>,
    event_context: Option<&EventContext>,
) -> Result<ServiceAccount, ApiError>
where
    C: BackendContext + ?Sized,
{
    let name = account.name.clone();
    let description = account.description.clone().unwrap_or_default();
    let owner_group_id = account.owner_group_id;
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
    let local_scope = identity_scope_by_name(backend.db_pool(), LOCAL_IDENTITY_SCOPE).await?;

    with_transaction(
        backend.db_pool(),
        async |conn| -> Result<ServiceAccount, ApiError> {
            let principal = NewPrincipal {
                identity_scope_id: local_scope.id,
                kind: PrincipalKind::ServiceAccount.as_str(),
                name: &name,
            }
            .insert(conn)
            .await?;

            let sa = diesel::insert_into(service_accounts::table)
                .values((
                    service_accounts::id.eq(principal.id),
                    service_accounts::description.eq(&description),
                    service_accounts::owner_group_id.eq(owner_group_id),
                    service_accounts::created_by.eq(created_by),
                ))
                .get_result::<ServiceAccount>(conn)
                .await?;
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
                emit_event(conn, &event).await?;
            }

            Ok(sa)
        },
    )
    .await
}

impl UpdateAdapter for UpdateServiceAccount {
    type Output = ServiceAccount;

    async fn update_adapter_without_events(
        &self,
        pool: &DbPool,
        service_account_id: i32,
    ) -> Result<ServiceAccount, ApiError> {
        update_service_account_record(self, pool, service_account_id, None).await
    }

    async fn update_adapter(
        &self,
        pool: &DbPool,
        service_account_id: i32,
        event_context: &EventContext,
    ) -> Result<ServiceAccount, ApiError> {
        update_service_account_record(self, pool, service_account_id, Some(event_context)).await
    }
}

async fn update_service_account_record(
    update: &UpdateServiceAccount,
    pool: &DbPool,
    service_account_id: i32,
    event_context: Option<&EventContext>,
) -> Result<ServiceAccount, ApiError> {
    use crate::schema::service_accounts::dsl::{id, service_accounts as sa_table};

    with_transaction(pool, async |conn| -> Result<ServiceAccount, ApiError> {
        let before = sa_table
            .filter(id.eq(service_account_id))
            .for_update()
            .first::<ServiceAccount>(conn)
            .await?;
        if !update.has_changes(&before) {
            return Ok(before);
        }
        let updated = diesel::update(sa_table.filter(id.eq(service_account_id)))
            .set(update)
            .get_result::<ServiceAccount>(conn)
            .await?;
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
            .with_before(service_account_snapshot(&before, &name))
            .with_after(service_account_snapshot(&updated, &name))
            .with_metadata(json!({
                "owner_group_id": updated.owner_group_id,
            }));
            emit_event(conn, &event).await?;
        }
        Ok(updated)
    })
    .await
}

impl InstanceAdapter<ServiceAccount> for ServiceAccountID {
    async fn instance_adapter(&self, pool: &DbPool) -> Result<ServiceAccount, ApiError> {
        load_service_account_by_id(pool, self.id()).await
    }
}

pub trait DisableServiceAccount {
    #[cfg(test)]
    async fn disable_without_events<C>(&self, backend: &C) -> Result<ServiceAccount, ApiError>
    where
        C: BackendContext + ?Sized;

    async fn disable<C>(
        &self,
        backend: &C,
        event_context: &EventContext,
    ) -> Result<ServiceAccount, ApiError>
    where
        C: BackendContext + ?Sized;
}

impl DisableServiceAccount for ServiceAccountID {
    #[cfg(test)]
    async fn disable_without_events<C>(&self, backend: &C) -> Result<ServiceAccount, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        disable_service_account(self, backend, None).await
    }

    async fn disable<C>(
        &self,
        backend: &C,
        event_context: &EventContext,
    ) -> Result<ServiceAccount, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        disable_service_account(self, backend, Some(event_context)).await
    }
}

impl DeleteAdapter for ServiceAccountID {
    async fn delete_adapter_without_events(&self, pool: &DbPool) -> Result<(), ApiError> {
        delete_service_account(self, pool, None).await
    }

    async fn delete_adapter(&self, pool: &DbPool, context: &EventContext) -> Result<(), ApiError> {
        delete_service_account(self, pool, Some(context)).await
    }
}

async fn disable_service_account<C>(
    account_id: &ServiceAccountID,
    backend: &C,
    event_context: Option<&EventContext>,
) -> Result<ServiceAccount, ApiError>
where
    C: BackendContext + ?Sized,
{
    use crate::schema::service_accounts::dsl::{disabled_at, id, service_accounts as sa_table};
    let sa_id = account_id.id();
    with_transaction(
        backend.db_pool(),
        async |conn| -> Result<ServiceAccount, ApiError> {
            let before = sa_table
                .filter(id.eq(sa_id))
                .for_update()
                .first::<ServiceAccount>(conn)
                .await?;
            if before.disabled_at.is_some() {
                return Ok(before);
            }
            let disabled = diesel::update(sa_table.filter(id.eq(sa_id)))
                .set(disabled_at.eq(diesel::dsl::now))
                .get_result::<ServiceAccount>(conn)
                .await?;
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
                .with_before(service_account_snapshot(&before, &name))
                .with_after(service_account_snapshot(&disabled, &name))
                .with_metadata(json!({
                    "owner_group_id": disabled.owner_group_id,
                }));
                emit_event(conn, &event).await?;
            }
            Ok(disabled)
        },
    )
    .await
}

async fn delete_service_account(
    account_id: &ServiceAccountID,
    pool: &DbPool,
    event_context: Option<&EventContext>,
) -> Result<(), ApiError> {
    use crate::schema::principals::dsl::{id, principals as principals_table};
    let sa_id = account_id.id();
    with_transaction(pool, async |conn| -> Result<(), ApiError> {
        principals_table
            .filter(id.eq(sa_id))
            .for_update()
            .select(id)
            .first::<i32>(conn)
            .await?;
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
            .with_before(service_account_snapshot(&sa, &name))
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
    conn: &mut DbConnection,
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
    conn: &mut DbConnection,
    service_account_id: i32,
) -> Result<ServiceAccount, ApiError> {
    use crate::schema::service_accounts::dsl::{id, service_accounts as sa_table};
    sa_table
        .filter(id.eq(service_account_id))
        .first::<ServiceAccount>(conn)
        .await
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
pub async fn is_human_owner_group_member(
    pool: &DbPool,
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
    with_connection(pool, async |conn| {
        diesel::update(
            tokens
                .filter(principal_id.eq(principal_id_value))
                .filter(revoked_at.is_null()),
        )
        .set(revoked_at.eq(diesel::dsl::now))
        .execute(conn)
        .await
    })
    .await
}

pub async fn cancel_pending_tasks_for_principal(
    pool: &DbPool,
    principal_id_value: i32,
) -> Result<usize, ApiError> {
    use crate::schema::tasks::dsl::{kind, status, submitted_by, tasks};
    let cancelled_kinds = with_connection(pool, async |conn| {
        diesel::update(
            tasks
                .filter(submitted_by.eq(principal_id_value))
                .filter(status.eq(TaskStatus::Queued.as_str())),
        )
        .set(status.eq(TaskStatus::Cancelled.as_str()))
        .returning(kind)
        .get_results::<String>(conn)
        .await
    })
    .await?;

    for task_kind in &cancelled_kinds {
        crate::observability::metrics::task_completed(
            task_kind,
            TaskStatus::Cancelled.as_str(),
            None,
        );
    }

    Ok(cancelled_kinds.len())
}

pub async fn service_accounts_owned_by_group(
    pool: &DbPool,
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
    pool: &DbPool,
    service_account_id: i32,
) -> Result<ServiceAccount, ApiError> {
    use crate::schema::service_accounts::dsl::{id, service_accounts as sa_table};
    with_connection(pool, async |conn| {
        sa_table
            .filter(id.eq(service_account_id))
            .first::<ServiceAccount>(conn)
            .await
    })
    .await
}

pub async fn search_manageable_service_accounts<S>(
    pool: &DbPool,
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
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' isn't searchable for service accounts",
                    param.field
                )));
            }
        }
    }

    apply_query_options!(base_query, query_options, ServiceAccountWithName);

    let rows = with_connection(pool, async |conn| {
        base_query
            .select((
                ServiceAccount::as_select(),
                identity_scopes::name,
                principals::name,
            ))
            .load::<(ServiceAccount, String, String)>(conn)
            .await
    })
    .await?;

    Ok(rows
        .into_iter()
        .map(ServiceAccountWithName::from_tuple)
        .collect())
}

pub async fn count_manageable_service_accounts<S>(
    pool: &DbPool,
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
    use crate::{date_search, numeric_search, string_search};

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
