//! Best-effort upstream cleanup after local authority has been fenced.
use super::{RetainedCredentials, configured_adapter, error, unprotect};
use crate::{
    errors::{ApiError, InteractionError},
    permissions::AppContext,
    storage::storage_handle,
};
use hubuum_domain::ResourceRevision;
use hubuum_storage_core::{
    RuntimeSettingsStorage, StorageAuthenticationProvider, StorageAuthenticationProviders,
    StorageProviderIdentityBinding, StorageRuntimeSettingsGroup,
    StorageRuntimeSettingsHistoryQuery, StorageRuntimeSettingsValue, StorageUpstreamSession,
};
use std::{collections::BTreeMap, future::Future, time::Duration};
use tokio::sync::OnceCell;

/// Invoke only after local revocation has committed. Provider failure cannot
/// restore local credentials or delay the local response.
pub(crate) fn revoke_best_effort(records: Vec<StorageUpstreamSession>, context: AppContext) {
    if records.is_empty() {
        return;
    }
    tokio::spawn(async move {
        let storage = storage_handle(&context);
        let mut batches = BTreeMap::new();
        for record in records {
            batches
                .entry(record.current_revision())
                .or_insert_with(Vec::new)
                .push(record);
        }
        for (revision, records) in batches {
            let selection = OnceCell::new();
            dispatch_revocations(records, |record| async {
                let Some(providers) = selection
                    .get_or_init(|| async {
                        match tokio::time::timeout(
                            Duration::from_secs(25),
                            revocation_providers_at(&storage, revision),
                        )
                        .await
                        {
                            Ok(Ok(providers)) => Some(providers),
                            _ => {
                                tracing::warn!(
                                    "Cannot resolve the authentication revision for upstream logout"
                                );
                                None
                            }
                        }
                    })
                    .await
                else {
                    return;
                };
                let provider = providers.providers().iter().find(|provider| {
                    provider.enabled()
                        && provider.id() == record.binding().provider().external_id()
                        && provider.name() == record.scope()
                        && StorageProviderIdentityBinding::from_provider(provider)
                            .is_ok_and(|binding| binding.namespace() == record.namespace())
                });
                if let Some(provider) = provider {
                    revoke_record(provider, record).await;
                } else {
                    tracing::warn!("Cannot resolve the original provider for upstream logout");
                }
            })
            .await;
        }
    });
}

// A later activation can remove the provider after local logout has committed.
// Its append-only settings change preserves the exact preceding assignment,
// including the original client configuration and revocation destination.
async fn revocation_providers_at(
    storage: &impl RuntimeSettingsStorage,
    revision: ResourceRevision,
) -> Result<StorageAuthenticationProviders, ApiError> {
    let group = StorageRuntimeSettingsGroup::Authentication;
    let snapshot = storage.load_runtime_settings().await?;
    let current = snapshot.record(group);
    let historical;
    let assignment = if current.revision() == revision {
        current.assignment()
    } else {
        if current.revision() < revision {
            return Err(InteractionError::ConfigurationChanged.into());
        }
        let next = revision
            .checked_advance()
            .map_err(|_| InteractionError::Invalid)?;
        let query =
            StorageRuntimeSettingsHistoryQuery::try_new(group, next.checked_advance().ok(), 1)
                .map_err(|_| InteractionError::Invalid)?;
        historical = storage
            .list_runtime_settings_changes(query)
            .await?
            .into_iter()
            .find(|change| change.revision() == next)
            .ok_or(InteractionError::ConfigurationChanged)?;
        historical.previous()
    };
    match assignment.value() {
        Some(StorageRuntimeSettingsValue::Authentication(providers)) => Ok(providers.clone()),
        _ => Err(InteractionError::ConfigurationChanged.into()),
    }
}

pub(crate) fn revoke_provider_best_effort(
    records: Vec<StorageUpstreamSession>,
    provider: StorageAuthenticationProvider,
) {
    if !records.is_empty() {
        tokio::spawn(async move {
            dispatch_revocations(records, |record| revoke_record(&provider, record)).await
        });
    }
}

// Share capacity across concurrent logouts, and keep pending work in each batch.
async fn dispatch_revocations<T, F: Future<Output = ()>>(records: Vec<T>, revoke: impl Fn(T) -> F) {
    use futures_util::{StreamExt, stream};
    const CONCURRENCY: usize = 32;
    static REVOCATIONS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(CONCURRENCY);
    let revoke = &revoke;
    stream::iter(records)
        .for_each_concurrent(CONCURRENCY, |record| async move {
            let _permit = REVOCATIONS
                .acquire()
                .await
                .expect("revocation semaphore stays open");
            revoke(record).await;
        })
        .await;
}

async fn revoke_record(provider: &StorageAuthenticationProvider, record: StorageUpstreamSession) {
    bounded_revocation(async {
        let Some(credential) = record.credential() else {
            return Ok::<_, ApiError>(());
        };
        let credentials = RetainedCredentials::restore(
            &unprotect(
                record.binding(),
                &format!("upstream:{}", record.subject()),
                credential.ciphertext(),
            )
            .await?,
        )
        .map_err(error)?;
        configured_adapter(provider)
            .await?
            .revoke(credentials)
            .await
            .map_err(error)?;
        Ok(())
    })
    .await;
}

async fn bounded_revocation(work: impl Future<Output = Result<(), ApiError>>) {
    let result = tokio::time::timeout(Duration::from_secs(25), work).await;
    if !matches!(result, Ok(Ok(()))) {
        tracing::warn!("Upstream grant revocation unavailable");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hubuum_events_core::EventContext;
    use hubuum_storage_core::{
        StorageAuthenticationProviderKind, StorageRuntimeSettingsAssignment,
        StorageRuntimeSettingsUpdate,
    };
    use hubuum_storage_memory::MemoryStorage;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use uuid::Uuid;

    #[tokio::test]
    async fn delayed_logout_resolves_original_definition_after_two_activations() {
        let storage = MemoryStorage::new();
        let provider = StorageAuthenticationProvider::try_new("company", StorageAuthenticationProviderKind::Oidc, serde_json::json!({
            "issuer":"https://original.example","client_id":"original",
            "client_authentication":"client_secret_basic","scopes":["openid"],"signing_algorithms":["RS256"],
            "membership":{"mode":"local_only","refresh_interval_seconds":300,"max_stale_seconds":900},
            "admission":{"mode":"existing_only"}
        })).unwrap().with_id(Uuid::new_v4()).unwrap();
        let original = StorageAuthenticationProviders::try_new(vec![provider]).unwrap();
        let mut revision = ResourceRevision::INITIAL;
        let captured = revision.checked_advance().unwrap();
        for assignment in [
            StorageRuntimeSettingsAssignment::Managed(StorageRuntimeSettingsValue::Authentication(
                original.clone(),
            )),
            StorageRuntimeSettingsAssignment::Managed(StorageRuntimeSettingsValue::Authentication(
                StorageAuthenticationProviders::try_new(vec![]).unwrap(),
            )),
            StorageRuntimeSettingsAssignment::Deployment(
                StorageRuntimeSettingsGroup::Authentication,
            ),
        ] {
            revision = storage
                .update_runtime_settings(StorageRuntimeSettingsUpdate::new(
                    assignment,
                    revision,
                    EventContext::system(),
                ))
                .await
                .unwrap()
                .record()
                .revision();
        }
        assert_eq!(
            revocation_providers_at(&storage, captured).await.unwrap(),
            original
        );
    }

    #[tokio::test]
    async fn overlapping_bulk_logouts_dispatch_every_grant_with_bounded_concurrency() {
        let active = AtomicUsize::new(0);
        let peak = AtomicUsize::new(0);
        let seen: Vec<_> = (0..80).map(|_| AtomicUsize::new(0)).collect();
        let revoke = async |id: usize| {
            let running = active.fetch_add(1, Ordering::Relaxed) + 1;
            peak.fetch_max(running, Ordering::Relaxed);
            tokio::task::yield_now().await;
            seen[id].fetch_add(1, Ordering::Relaxed);
            active.fetch_sub(1, Ordering::Relaxed);
        };
        tokio::join!(
            dispatch_revocations((0..40).collect(), &revoke),
            dispatch_revocations((40..80).collect(), &revoke)
        );
        assert!(peak.load(Ordering::Relaxed) <= 32);
        assert!(
            seen.iter()
                .all(|attempts| attempts.load(Ordering::Relaxed) == 1)
        );
    }
}
