use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use hubuum_storage_core::{
    BackupSnapshotStorage, StorageBackupBudget, StorageBackupHistorySection, StorageBackupReplay,
    StorageBackupSnapshot, StorageErrorKind, StorageIdentityUpgrade, StorageRestoreIdentityContext,
};

use crate::FixtureError;

/// Check the shared exhaustion contract against a fixture known to exceed the
/// supplied byte or row budget. Adapter-native probes must additionally prove
/// early enumeration termination and bounded retained state.
pub async fn verify_backup_budget_rejected(
    backend: &impl BackupSnapshotStorage,
    budget: StorageBackupBudget,
) -> Result<(), FixtureError> {
    match backend.capture_backup_replay(true, budget).await {
        Err(error) if error.kind() == StorageErrorKind::InputTooLarge => Ok(()),
        Err(error) => Err(error.into()),
        Ok(_) => Err("Backup capture accepted a fixture exceeding its budget".into()),
    }
}

/// Adapter/application fixture for the portable destructive recovery contract.
/// Each fixture owns isolated storage. Mutations must observably change the
/// captured state, including a revisioned update and a deletion.
#[async_trait]
pub trait BackupRestoreFixture: Send + Sync {
    async fn capture(&self, include_history: bool) -> Result<StorageBackupSnapshot, FixtureError>;
    async fn restore(
        &self,
        snapshot: StorageBackupSnapshot,
    ) -> Result<StorageRestoreIdentityContext, FixtureError>;
    async fn mutate(&self) -> Result<(), FixtureError>;
    /// Attempt apply with an adapter-native failure after replacement begins.
    /// Resume maintenance before returning the apply error.
    async fn fail_restore(&self, snapshot: StorageBackupSnapshot) -> Result<(), FixtureError>;
}

/// Prove recovery remains composable across history modes and later writes.
/// Expectations belong to this runner, independent of adapter mechanics.
pub async fn verify_backup_restore_contract(
    fixture: &impl BackupRestoreFixture,
    include_source_history: bool,
) -> Result<(), FixtureError> {
    // The source must include advanced revisions and a previously deleted row.
    fixture.mutate().await?;
    let source = fixture.capture(include_source_history).await?;
    fixture.mutate().await?;
    require_changed(&source, &fixture.capture(false).await?)?;
    let context = fixture.restore(source.clone()).await?;
    let restored = fixture.capture(true).await?;
    require_restored(&source, &restored, context)?;

    fixture.mutate().await?;
    let second = fixture.capture(true).await?;
    require_changed(&restored, &second)?;
    fixture.mutate().await?;
    require_changed(&second, &fixture.capture(false).await?)?;
    let context = fixture.restore(second.clone()).await?;
    require_restored(&second, &fixture.capture(true).await?, context)
}

fn canonical(snapshot: &StorageBackupSnapshot) -> StorageBackupSnapshot {
    let (mut state, mut history) = snapshot.clone().into_parts();
    for rows in state
        .values_mut()
        .chain(history.iter_mut().flat_map(|history| history.values_mut()))
    {
        rows.sort_by_cached_key(|row| row.clone().into_value().to_string());
    }
    StorageBackupSnapshot::try_new(state, history).expect("sorting preserves validated snapshots")
}

fn require_changed(
    before: &StorageBackupSnapshot,
    after: &StorageBackupSnapshot,
) -> Result<(), FixtureError> {
    if canonical(before).into_parts().0 == canonical(after).into_parts().0 {
        return Err(std::io::Error::other(
            "Recovery fixture mutation did not change authoritative state",
        )
        .into());
    }
    Ok(())
}

fn require_restored(
    source: &StorageBackupSnapshot,
    restored: &StorageBackupSnapshot,
    context: StorageRestoreIdentityContext,
) -> Result<(), FixtureError> {
    let (source_state, source_history) = canonical(source).into_parts();
    let (restored_state, restored_history) = canonical(restored).into_parts();
    if source_history.is_some() && source_state != restored_state {
        return Err(std::io::Error::other(
            "Restore changed authoritative resource state or revisions",
        )
        .into());
    }
    let restored_history = restored_history
        .ok_or_else(|| std::io::Error::other("History-inclusive capture omitted history"))?;
    if let Some(source_history) = source_history {
        for (section, source_rows) in source_history {
            let restored_rows = &restored_history[&section];
            if section == StorageBackupHistorySection::AuditEvents {
                let source_keys = source_rows
                    .iter()
                    .map(|row| row.clone().into_value().to_string())
                    .collect::<HashSet<_>>();
                let restored_by_key = restored_rows
                    .iter()
                    .map(|row| (row.clone().into_value().to_string(), row))
                    .collect::<HashMap<_, _>>();
                let additions = restored_by_key
                    .iter()
                    .filter(|(key, _)| !source_keys.contains(*key))
                    .map(|(_, row)| *row)
                    .collect::<Vec<_>>();
                if source_keys.len() != source_rows.len()
                    || restored_by_key.len() != restored_rows.len()
                    || !source_keys
                        .iter()
                        .all(|key| restored_by_key.contains_key(key))
                    || additions.len() != 1
                    || additions[0].get("entity_type").and_then(|v| v.as_str()) != Some("restore")
                    || additions[0].get("action").and_then(|v| v.as_str()) != Some("succeeded")
                {
                    return Err(std::io::Error::other(
                        "Restore did not retain audit history plus exactly one success event",
                    )
                    .into());
                }
            } else if &source_rows != restored_rows {
                return Err(std::io::Error::other(format!(
                    "Restore changed retained history section '{section}'"
                ))
                .into());
            }
        }
    } else {
        let expected = StorageBackupReplay::from_snapshot(
            StorageBackupSnapshot::try_new(source_state, None)?,
            StorageBackupBudget::new(256 * 1024 * 1024, 1_000_000)?,
        )?
        .upgrade_logical_identities(StorageIdentityUpgrade::Native)?
        .at_restore_boundary(context)?
        .into_replay()
        .materialize()?;
        // Baselines retain resource identities while assigning new version
        // identities in the confirmed stage's retry-stable namespace.
        require_restored(&expected, restored, context)?;
        let source_history_flag = restored_history[&StorageBackupHistorySection::AuditEvents][0]
            .get("metadata")
            .and_then(|metadata| metadata.get("includes_history"))
            .and_then(|value| value.as_bool());
        if source_history_flag != Some(false) {
            return Err(
                std::io::Error::other("Restore provenance lost the source history mode").into(),
            );
        }
    }
    Ok(())
}

/// A failed replacement must preserve both live rows and retained history.
pub async fn verify_failed_restore_preserves_snapshot(
    fixture: &impl BackupRestoreFixture,
) -> Result<(), FixtureError> {
    let target = fixture.capture(false).await?;
    fixture.mutate().await?;
    let before = fixture.capture(true).await?;
    require_changed(&target, &before)?;
    if fixture.fail_restore(target).await.is_ok() {
        return Err(std::io::Error::other("Injected restore unexpectedly succeeded").into());
    }
    if canonical(&before) != canonical(&fixture.capture(true).await?) {
        return Err(
            std::io::Error::other("Failed restore changed current state or history").into(),
        );
    }
    fixture.mutate().await?;
    require_changed(&before, &fixture.capture(true).await?)
}
