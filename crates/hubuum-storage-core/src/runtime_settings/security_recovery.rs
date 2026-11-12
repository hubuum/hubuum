//! Explicit offline recovery over the same revisioned security authority.
use hubuum_domain::PrincipalId;
use hubuum_events_core::EventContext;

use crate::{
    StorageAuthenticationProviders, StorageAuthorizationBackend, StorageRuntimeSettingsAssignment,
    StorageRuntimeSettingsGroup, StorageRuntimeSettingsSnapshot, StorageRuntimeSettingsUpdate,
    StorageRuntimeSettingsValue, StorageValidationError,
};

/// Privileged CLI operation: callers must quiesce writes before invoking it.
/// Adapters atomically retain a local administrator, revoke external credentials,
/// discard pending authority and commit both security groups with an audit trail.
#[derive(Clone, Debug)]
pub struct StorageSecurityRecovery {
    administrator: PrincipalId,
    recovery_group: String,
    updates: [StorageRuntimeSettingsUpdate; 2],
}

impl StorageSecurityRecovery {
    pub fn try_new(
        administrator: PrincipalId,
        recovery_group: String,
        snapshot: &StorageRuntimeSettingsSnapshot,
    ) -> Result<Self, StorageValidationError> {
        if recovery_group.is_empty()
            || recovery_group.len() > 255
            || recovery_group.chars().any(char::is_control)
        {
            return Err(StorageValidationError::invalid(
                "Invalid local recovery group",
            ));
        }
        let values = [
            StorageRuntimeSettingsValue::Authentication(StorageAuthenticationProviders::try_new(
                Vec::new(),
            )?),
            StorageRuntimeSettingsValue::Authorization(StorageAuthorizationBackend::Local),
        ];
        let updates = values.map(|value| {
            let revision = snapshot.record(value.group()).revision();
            StorageRuntimeSettingsUpdate::new(
                StorageRuntimeSettingsAssignment::Managed(value),
                revision,
                EventContext::system(),
            )
        });
        debug_assert_eq!(
            updates[0].group(),
            StorageRuntimeSettingsGroup::Authentication
        );
        Ok(Self {
            administrator,
            recovery_group,
            updates,
        })
    }
    pub const fn administrator(&self) -> PrincipalId {
        self.administrator
    }
    pub fn recovery_group(&self) -> &str {
        &self.recovery_group
    }
    pub const fn updates(&self) -> &[StorageRuntimeSettingsUpdate; 2] {
        &self.updates
    }
}
