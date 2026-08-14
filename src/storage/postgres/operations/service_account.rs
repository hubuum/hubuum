//! Transitional application projections for PostgreSQL service accounts.
//!
//! SQL lifecycle and query ownership lives in the `hubuum-storage-postgres`
//! adapter. This module retains only application-domain fixture traits and the
//! row projection still consumed by task persistence during its extraction.

use hubuum_storage_core::{StorageServiceAccountCreate, StorageServiceAccountMutation};

use crate::errors::ApiError;
use crate::events::EventContext;
use crate::models::identity::LOCAL_IDENTITY_SCOPE;
use crate::models::{NewServiceAccount, ServiceAccount, ServiceAccountID, TaskStatus};
use crate::storage::postgres::PostgresPool;
use crate::storage::postgres::prelude::*;

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
        ensure_local_scope(self)?;
        let request = StorageServiceAccountCreate::new(
            &self.name,
            self.description.clone().unwrap_or_default(),
            self.owner_group_id.id(),
            created_by,
            event_context.clone(),
        );
        let account = hubuum_storage_postgres::operations::service_account::create_service_account(
            &hubuum_storage_postgres::PostgresRuntime::new(pool.clone()),
            request,
        )
        .await
        .map_err(storage_error)?;
        Ok(service_account_from_storage(account))
    }

    #[cfg(any(test, feature = "integration-test-support"))]
    async fn save_without_events(
        &self,
        pool: &PostgresPool,
        created_by: Option<i32>,
    ) -> Result<ServiceAccount, ApiError> {
        ensure_local_scope(self)?;
        let account =
            hubuum_storage_postgres::operations::service_account::create_service_account_without_events(
                &hubuum_storage_postgres::PostgresRuntime::new(pool.clone()),
                self.name.clone(),
                self.description.clone().unwrap_or_default(),
                self.owner_group_id.id(),
                created_by,
            )
            .await
            .map_err(storage_error)?;
        Ok(service_account_from_storage(account))
    }
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
        let outcome = hubuum_storage_postgres::operations::service_account::disable_service_account_without_events(
            &hubuum_storage_postgres::PostgresRuntime::new(pool.clone()),
            self.id(),
        )
        .await
        .map_err(storage_error)?;
        let (account, task_kinds) = outcome.into_parts();
        record_cancelled_task_metrics(task_kinds);
        Ok(service_account_from_storage(account))
    }

    async fn disable(
        &self,
        pool: &PostgresPool,
        event_context: &EventContext,
    ) -> Result<ServiceAccount, ApiError> {
        let request = StorageServiceAccountMutation::new(self.id(), event_context.clone());
        let outcome =
            hubuum_storage_postgres::operations::service_account::disable_service_account(
                &hubuum_storage_postgres::PostgresRuntime::new(pool.clone()),
                request,
            )
            .await
            .map_err(storage_error)?;
        let (account, task_kinds) = outcome.into_parts();
        record_cancelled_task_metrics(task_kinds);
        Ok(service_account_from_storage(account))
    }
}

pub async fn cancel_pending_tasks_for_principal(
    pool: &PostgresPool,
    principal_id: i32,
) -> Result<usize, ApiError> {
    let task_kinds =
        hubuum_storage_postgres::operations::service_account::cancel_pending_tasks_for_principal(
            &hubuum_storage_postgres::PostgresRuntime::new(pool.clone()),
            principal_id,
        )
        .await
        .map_err(storage_error)?;
    let count = task_kinds.len();
    record_cancelled_task_metrics(task_kinds);
    Ok(count)
}

pub async fn load_service_account_by_id(
    pool: &PostgresPool,
    service_account_id: i32,
) -> Result<ServiceAccount, ApiError> {
    let account = hubuum_storage_postgres::operations::service_account::load_service_account(
        &hubuum_storage_postgres::PostgresRuntime::new(pool.clone()),
        service_account_id,
    )
    .await
    .map_err(storage_error)?;
    Ok(service_account_from_storage(account))
}

fn ensure_local_scope(account: &NewServiceAccount) -> Result<(), ApiError> {
    let scope_name = account
        .identity_scope
        .as_deref()
        .unwrap_or(LOCAL_IDENTITY_SCOPE);
    if scope_name == LOCAL_IDENTITY_SCOPE {
        Ok(())
    } else {
        Err(ApiError::BadRequest(
            "service accounts in non-local identity scopes are managed by their identity provider"
                .to_string(),
        ))
    }
}

fn service_account_from_storage(
    account: hubuum_storage_core::StorageServiceAccount,
) -> ServiceAccount {
    ServiceAccount {
        id: account.id(),
        kind: "service_account".to_string(),
        description: account.description().to_string(),
        owner_group_id: account.owner_group_id(),
        created_by: account.created_by(),
        disabled_at: account.disabled_at(),
        created_at: account.created_at(),
        updated_at: account.updated_at(),
    }
}

fn record_cancelled_task_metrics(task_kinds: Vec<String>) {
    for task_kind in task_kinds {
        crate::observability::metrics::task_completed(
            &task_kind,
            TaskStatus::Cancelled.as_str(),
            None,
        );
    }
}

fn storage_error(error: hubuum_storage_postgres::PostgresStorageError) -> ApiError {
    ApiError::from(hubuum_storage_core::StorageError::from(error))
}
