//! Real retained authority transitions alongside the measured worker pipeline.
use super::*;

pub(super) struct Population {
    before: HistoryLifecycleControl,
    pending_keys: Vec<HistoryPolicyKey>,
}

pub(super) async fn prepare(
    fixture: &RestoreContractFixture,
    shape: &str,
    worker_scopes: usize,
) -> Population {
    let storage = fixture.storage();
    let principal = PrincipalIdentity::generate();
    let mut snapshot = storage.capture_history_lifecycle().await.unwrap();
    let mut pending_keys = Vec::new();
    if shape == "policy_count" {
        let key = HistoryPolicyKey::new(
            StorageIdentityHistoryKind::Class,
            HistoryPolicyScope::Deployment,
        )
        .unwrap();
        while snapshot.control().authority().policies().records().len()
            < HistoryPolicyCatalog::MAX_REVISIONS
        {
            let change = HistoryPolicyChange::new(
                key,
                snapshot
                    .control()
                    .authority()
                    .policies()
                    .current_revision(key),
                HistoryPolicyBinding::DisabledExpiry,
                HistoryPolicyApplication::Prospective,
            )
            .unwrap();
            snapshot = storage
                .commit_history_lifecycle(
                    snapshot
                        .control()
                        .apply(
                            HistoryPolicyApplyRequest::new(change, None),
                            principal,
                            snapshot.fence(),
                            snapshot.observed_at(),
                        )
                        .unwrap(),
                )
                .await
                .unwrap();
            let count = snapshot.control().authority().policies().records().len();
            if count % 512 == 0 {
                eprintln!("Qualification authority population: policies={count}");
            }
        }
    } else if shape == "hold_count" {
        let key = HistoryPolicyKey::new(
            StorageIdentityHistoryKind::Class,
            HistoryPolicyScope::Deployment,
        )
        .unwrap();
        for _ in 0..HistoryLifecycleAuthority::MAX_HOLD_REVISIONS / 2 {
            let hold = HistoryRetentionHold::new(
                Uuid::new_v4(),
                key,
                snapshot.observed_at(),
                None,
                principal,
            )
            .unwrap();
            snapshot = storage
                .commit_history_lifecycle(
                    snapshot
                        .control()
                        .create_hold(hold, snapshot.fence())
                        .unwrap(),
                )
                .await
                .unwrap();
        }
    } else {
        assert_eq!(shape, "pending_count");
        for index in 0..HistoryExpiryJournal::MAX_INTENTS - worker_scopes {
            let collection = fixture
                .create_collection(&format!("pending_{index}_{}", Uuid::new_v4()), "initial")
                .await
                .unwrap();
            snapshot = storage.capture_history_lifecycle().await.unwrap();
            let identity = storage
                .resolve_logical_identity(StorageIdentityTarget::Resource {
                    kind: StorageIdentityResourceKind::Collection,
                    id: collection.id().into(),
                })
                .await
                .unwrap()
                .subject()
                .and_then(StorageIdentitySubject::record_identity)
                .unwrap()
                .resource();
            let key = HistoryPolicyKey::new(
                StorageIdentityHistoryKind::Collection,
                HistoryPolicyScope::Collection { identity },
            )
            .unwrap();
            let change = HistoryPolicyChange::new(
                key,
                snapshot
                    .control()
                    .authority()
                    .policies()
                    .current_revision(key),
                HistoryPolicyBinding::Expire {
                    trigger: HistoryExpiryTrigger::Age {
                        age_seconds: HistoryRetentionSeconds::new(0).unwrap(),
                        deleted_age_seconds: None,
                    },
                    minimum_age_seconds: HistoryRetentionSeconds::new(0).unwrap(),
                },
                HistoryPolicyApplication::Prospective,
            )
            .unwrap();
            snapshot = storage
                .commit_history_lifecycle(
                    snapshot
                        .control()
                        .apply(
                            HistoryPolicyApplyRequest::new(change, None),
                            principal,
                            snapshot.fence(),
                            snapshot.observed_at(),
                        )
                        .unwrap(),
                )
                .await
                .unwrap();
            fixture
                .update_collection_description(collection.id(), "eligible pending source".into())
                .await
                .unwrap();
            pending_keys.push(key);
        }
    }
    Population {
        before: snapshot.control().clone(),
        pending_keys,
    }
}

pub(super) async fn admit_pending(
    storage: &impl HistoryLifecycleStorage,
    keys: &[HistoryPolicyKey],
) {
    for key in keys {
        let snapshot = storage.capture_history_lifecycle().await.unwrap();
        let request = HistoryExpiryAdmissionRequest::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            HistoryExpiryOrigin::Manual {
                principal: PrincipalIdentity::generate(),
            },
            HistoryLifecycleScan::new(*key, 1000).unwrap(),
            snapshot.fence(),
        )
        .unwrap();
        let page = storage.admit_history_expiry(request).await.unwrap();
        assert_eq!(page.manifest().unwrap().members().len(), 1);
    }
}
impl Population {
    pub(super) fn pending_keys(&self) -> &[HistoryPolicyKey] {
        &self.pending_keys
    }
    pub(super) fn verify(
        &self,
        after: &HistoryLifecycleSnapshot,
        replicas: &[Value],
        shape: &str,
    ) -> Value {
        let authority = after.control().authority();
        assert_eq!(authority.policies(), self.before.authority().policies());
        assert_eq!(
            authority.hold_revisions(),
            self.before.authority().hold_revisions()
        );
        let policies = authority.policies().records().len();
        let holds = authority.current_holds().count();
        let peak_pending = replicas
            .iter()
            .map(|report| report["max_pending_intents"].as_u64().unwrap())
            .max()
            .unwrap();
        match shape {
            "policy_count" => assert_eq!(policies, HistoryPolicyCatalog::MAX_REVISIONS),
            "hold_count" => assert_eq!(holds * 2, HistoryLifecycleAuthority::MAX_HOLD_REVISIONS),
            "pending_count" => {
                assert_eq!(peak_pending, HistoryExpiryJournal::MAX_INTENTS as u64);
                assert_eq!(
                    authority.expiry().intents().len(),
                    HistoryExpiryJournal::MAX_INTENTS
                );
                assert!(
                    authority
                        .expiry()
                        .intents()
                        .iter()
                        .all(|intent| intent.is_complete())
                );
            }
            _ => unreachable!("authority fixture shape"),
        }
        json!({"policy_revisions":policies,"live_holds":holds,"initial_pending_intents":self.pending_keys.len(),
            "max_pending_intents":peak_pending,"final_intents":authority.expiry().intents().len(),
            "before_control_bytes":self.before.encode().unwrap().len(),"after_control_bytes":after.control().encode().unwrap().len(),
            "policies_preserved":true,"holds_preserved":true,"pending_completed":true})
    }
}
