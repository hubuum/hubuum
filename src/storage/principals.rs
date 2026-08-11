use async_trait::async_trait;

use crate::events::EventContext;
use crate::models::{
    Principal, PrincipalSettings, PrincipalSettingsPatch, PrincipalSettingsResponse,
};

use super::StorageError;

/// Complete principal point and settings behavior required from every backend.
///
/// The application owns principal and settings DTOs. Backends own persistence,
/// concurrency control, audit-event atomicity, and implementation errors.
#[async_trait]
pub(crate) trait PrincipalStorage: Send + Sync {
    async fn load_principal(&self, principal_id: i32) -> Result<Principal, StorageError>;

    async fn load_principal_settings(
        &self,
        principal_id: i32,
    ) -> Result<PrincipalSettingsResponse, StorageError>;

    async fn replace_principal_settings(
        &self,
        principal_id: i32,
        settings: PrincipalSettings,
        context: &EventContext,
    ) -> Result<PrincipalSettingsResponse, StorageError>;

    async fn merge_principal_settings(
        &self,
        principal_id: i32,
        patch: PrincipalSettings,
        context: &EventContext,
    ) -> Result<PrincipalSettingsResponse, StorageError>;

    async fn apply_principal_settings_patch(
        &self,
        principal_id: i32,
        patch: PrincipalSettingsPatch,
        context: &EventContext,
    ) -> Result<PrincipalSettingsResponse, StorageError>;

    async fn reset_principal_settings(
        &self,
        principal_id: i32,
        context: &EventContext,
    ) -> Result<PrincipalSettingsResponse, StorageError>;
}
