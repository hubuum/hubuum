use async_trait::async_trait;

use crate::events::EventContext;
use crate::models::search::QueryOptions;
use crate::models::{Group, NewGroup, Principal, PrincipalGroup, UpdateGroup};

use super::StorageError;

/// Complete group lifecycle and membership behavior required from every backend.
///
/// The application owns group and membership DTOs. Backends own persistence,
/// coordination, event atomicity, query construction, and implementation errors.
#[async_trait]
pub(crate) trait GroupStorage: Send + Sync {
    async fn load_group(&self, group_id: i32) -> Result<Group, StorageError>;

    async fn group_identity_scope_name(&self, group_id: i32) -> Result<String, StorageError>;

    async fn create_group(
        &self,
        command: &NewGroup,
        context: Option<&EventContext>,
    ) -> Result<Group, StorageError>;

    async fn update_group(
        &self,
        group_id: i32,
        update: &UpdateGroup,
        context: Option<&EventContext>,
    ) -> Result<Group, StorageError>;

    async fn delete_group(
        &self,
        group_id: i32,
        context: Option<&EventContext>,
    ) -> Result<usize, StorageError>;

    async fn group_members(&self, group_id: i32) -> Result<Vec<Principal>, StorageError>;

    async fn group_members_page(
        &self,
        group_id: i32,
        query_options: &QueryOptions,
    ) -> Result<Vec<(PrincipalGroup, Principal)>, StorageError>;

    async fn count_group_members(
        &self,
        group_id: i32,
        query_options: &QueryOptions,
    ) -> Result<i64, StorageError>;

    async fn group_member_principal(&self, principal_id: i32) -> Result<Principal, StorageError>;

    async fn add_group_member(
        &self,
        principal_id: i32,
        group_id: i32,
        context: Option<&EventContext>,
    ) -> Result<PrincipalGroup, StorageError>;

    async fn remove_group_member(
        &self,
        principal_id: i32,
        group_id: i32,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError>;
}
