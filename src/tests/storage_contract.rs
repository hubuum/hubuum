use std::sync::{Arc, LazyLock};

use actix_web::web::Data;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::events::{EventFanoutSettings, EventRetentionSettings};
use crate::models::CollectionID;
use crate::models::TokenRetentionSettings;
use crate::models::identity::LOCAL_IDENTITY_SCOPE;
use crate::models::search::QueryOptions;
use crate::services::Services;
use crate::storage::StorageHandle;
use crate::storage::postgres::PostgresPool;
use crate::storage::{
    AuthenticationStorage, AuthenticationTokenScopeQuery, AuthorizationCollectionAccessQuery,
    AuthorizationCollectionGrantListQuery, AuthorizationCollectionsQuery, AuthorizationGrantKey,
    AuthorizationGrantMutation, AuthorizationGroupMembershipQuery, AuthorizationPermission,
    AuthorizationStorage, EventArchive, EventDeliveryStorage, EventFanoutStorage,
    EventHealthStorage, EventRetentionStorage, MetricsStorage, OperationalStateStorage,
    RetainedEvent, STORAGE_CONTRACT_VERSION, StorageBackendKind, StorageError,
    TokenRetentionStorage,
};

#[derive(Clone, Copy, Debug)]
pub(crate) enum LifecycleContractImplementation {
    MemoryModel,
    PostgresAdapter,
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_metrics_snapshots() {
    let _permit = postgres_permit().await;

    for kind in StorageBackendKind::ALL {
        match kind {
            StorageBackendKind::Postgresql => {
                let backend = StorageHandle::postgres(pool().get_ref().clone());

                let pool_state = backend.metrics_pool_state();
                assert!(pool_state.max_connections > 0);
                backend
                    .metrics_inventory_snapshot()
                    .await
                    .expect("certified backend should supply inventory metrics");
                backend
                    .metrics_task_snapshot()
                    .await
                    .expect("certified backend should supply task metrics");
                backend
                    .metrics_event_snapshot()
                    .await
                    .expect("certified backend should supply event metrics");
            }
        }
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_authentication_projections() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let user = crate::tests::create_user_with_params(
        pool.get_ref(),
        &prefix("authentication_user"),
        "testpassword",
    )
    .await;

    for kind in StorageBackendKind::ALL {
        match kind {
            StorageBackendKind::Postgresql => {
                let backend = StorageHandle::postgres(pool.get_ref().clone());
                let identity = backend
                    .load_authentication_identity(user.id)
                    .await
                    .expect("certified backend should supply authentication identity data");
                let (principal, human) = identity.into_parts();

                assert_eq!(principal.id(), user.id);
                assert!(principal.is_human());
                assert!(human.is_some());

                let scope = backend
                    .load_authentication_token_scope(AuthenticationTokenScopeQuery::new(
                        i32::MAX,
                        true,
                        false,
                    ))
                    .await
                    .expect("certified backend should preserve empty scope dimensions")
                    .expect("an enabled scope dimension should produce a scope snapshot");
                let (permissions, resources) = scope.into_parts();
                assert_eq!(permissions, Some(Vec::new()));
                assert_eq!(resources, None);
            }
        }
    }

    user.delete_without_events(pool.get_ref())
        .await
        .expect("authentication compatibility fixture should be removed");
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_local_authorization_data() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let user = crate::tests::create_user_with_params(
        pool.get_ref(),
        &prefix("authorization_user"),
        "testpassword",
    )
    .await;
    let group = crate::tests::create_test_group(pool.get_ref()).await;
    group
        .add_member_without_events(pool.get_ref(), &user)
        .await
        .expect("authorization compatibility membership should be created");
    let fixture = crate::tests::create_collection_fixture(
        pool.get_ref(),
        &prefix("authorization_collection"),
    )
    .await;

    for kind in StorageBackendKind::ALL {
        match kind {
            StorageBackendKind::Postgresql => {
                let backend = StorageHandle::postgres(pool.get_ref().clone());
                let principal = backend
                    .load_authorization_principal(user.id)
                    .await
                    .expect("certified backend should supply authorization principal facts");
                assert!(principal.group_ids().contains(&group.id));

                let membership = AuthorizationGroupMembershipQuery::new(
                    user.id,
                    &group.groupname,
                    LOCAL_IDENTITY_SCOPE,
                );
                assert!(
                    backend
                        .authorization_principal_is_group_member(membership)
                        .await
                        .expect("certified backend should query group membership")
                );

                let access_query = || {
                    AuthorizationCollectionAccessQuery::new(
                        user.id,
                        fixture.collection.id,
                        [AuthorizationPermission::ReadCollection],
                    )
                };
                assert!(
                    !backend
                        .authorize_local_collection(access_query())
                        .await
                        .expect("missing local grant should deny")
                );

                let key = AuthorizationGrantKey::new(fixture.collection.id, group.id);
                backend
                    .apply_local_collection_grant(AuthorizationGrantMutation::new(
                        key,
                        [AuthorizationPermission::ReadCollection],
                        false,
                    ))
                    .await
                    .expect("certified backend should apply a local grant");
                let grant = backend
                    .get_local_collection_grant(key)
                    .await
                    .expect("certified backend should load a local grant")
                    .expect("applied local grant should exist");
                assert!(
                    grant
                        .permissions()
                        .contains(&AuthorizationPermission::ReadCollection)
                );
                assert!(
                    backend
                        .authorize_local_collection(access_query())
                        .await
                        .expect("applied local grant should authorize")
                );

                let collections = backend
                    .local_authorized_collections(AuthorizationCollectionsQuery::new(
                        user.id,
                        [AuthorizationPermission::ReadCollection],
                    ))
                    .await
                    .expect("certified backend should run reverse authorization queries");
                assert!(
                    collections
                        .iter()
                        .any(|collection| collection.id() == fixture.collection.id)
                );

                let page = backend
                    .list_local_collection_grants(AuthorizationCollectionGrantListQuery::new(
                        fixture.collection.id,
                        [AuthorizationPermission::ReadCollection],
                        QueryOptions {
                            filters: Vec::new(),
                            sort: Vec::new(),
                            limit: None,
                            cursor: None,
                            include_total: true,
                        },
                    ))
                    .await
                    .expect("certified backend should list local grants");
                let (items, total_count) = page.into_parts();
                assert!(total_count >= 1);
                assert!(!items.is_empty());

                backend
                    .revoke_local_collection_grant(AuthorizationGrantMutation::new(
                        key,
                        [AuthorizationPermission::ReadCollection],
                        false,
                    ))
                    .await
                    .expect("certified backend should revoke selected local permissions");
                assert!(
                    !backend
                        .authorize_local_collection(access_query())
                        .await
                        .expect("revoked local grant should deny")
                );
                backend
                    .revoke_all_local_collection_grants(key)
                    .await
                    .expect("certified backend should remove the local grant row");
            }
        }
    }

    group
        .remove_member_without_events(&user, pool.get_ref())
        .await
        .expect("authorization compatibility membership should be removed");
    group
        .delete_without_events(pool.get_ref())
        .await
        .expect("authorization compatibility group should be removed");
    fixture
        .cleanup()
        .await
        .expect("authorization compatibility collection should be removed");
    user.delete_without_events(pool.get_ref())
        .await
        .expect("authorization compatibility user should be removed");
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_operational_state() {
    let _permit = postgres_permit().await;

    for kind in StorageBackendKind::ALL {
        match kind {
            StorageBackendKind::Postgresql => {
                let backend = StorageHandle::postgres(pool().get_ref().clone());
                let state = backend
                    .maintenance_state()
                    .await
                    .expect("certified backend should expose maintenance state");
                let readiness = backend
                    .readiness_snapshot()
                    .await
                    .expect("certified backend should expose readiness state");

                assert_eq!(readiness.maintenance_state(), state);
                assert!(readiness.schema_is_ready());
            }
        }
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_event_health() {
    let _permit = postgres_permit().await;

    for kind in StorageBackendKind::ALL {
        match kind {
            StorageBackendKind::Postgresql => {
                let backend = StorageHandle::postgres(pool().get_ref().clone());
                backend
                    .event_delivery_health()
                    .await
                    .expect("certified backend should expose event delivery health");
            }
        }
    }
}

#[actix_web::test]
async fn every_available_storage_backend_processes_event_fanout() {
    let _permit = postgres_permit().await;
    let settings = EventFanoutSettings::new(10, 30_000)
        .expect("compatibility fan-out settings should be valid");

    for kind in StorageBackendKind::ALL {
        match kind {
            StorageBackendKind::Postgresql => {
                let backend = StorageHandle::postgres(pool().get_ref().clone());
                backend
                    .process_event_fanout_batch(settings)
                    .await
                    .expect("certified backend should process event fan-out");
            }
        }
    }
}

#[actix_web::test]
async fn every_available_storage_backend_processes_event_retention() {
    struct DiscardArchive;

    impl EventArchive for DiscardArchive {
        fn archive(&self, _events: &[RetainedEvent]) -> Result<(), StorageError> {
            Ok(())
        }
    }

    let _permit = postgres_permit().await;
    let settings = EventRetentionSettings::new(10_000, 10_000, 10)
        .expect("compatibility event-retention settings should be valid");

    for kind in StorageBackendKind::ALL {
        match kind {
            StorageBackendKind::Postgresql => {
                let backend = StorageHandle::postgres(pool().get_ref().clone());
                let summary = backend
                    .process_event_retention_batch(settings, &DiscardArchive)
                    .await
                    .expect("certified backend should process event retention");

                assert!(!summary.did_work());
            }
        }
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_token_retention() {
    let _permit = postgres_permit().await;
    let settings = TokenRetentionSettings::builder()
        .retention_days(1_000_000)
        .token_lifetime_hours(24)
        .batch_size(10)
        .build()
        .expect("compatibility retention settings should be valid");

    for kind in StorageBackendKind::ALL {
        match kind {
            StorageBackendKind::Postgresql => {
                let backend = StorageHandle::postgres(pool().get_ref().clone());
                let purged = backend
                    .purge_expired_tokens(settings)
                    .await
                    .expect("certified backend should execute token retention");

                assert_eq!(purged, 0);
            }
        }
    }
}

#[actix_web::test]
async fn every_available_storage_backend_composes_through_the_complete_contract() {
    let _permit = postgres_permit().await;

    for kind in StorageBackendKind::ALL {
        match kind {
            StorageBackendKind::Postgresql => {
                let backend = StorageHandle::postgres(pool().get_ref().clone());
                fn accepts_event_delivery_contract(_backend: &impl EventDeliveryStorage) {}
                accepts_event_delivery_contract(&backend);
                let descriptor = backend.descriptor();
                assert_eq!(descriptor.kind(), kind);
                assert_eq!(descriptor.contract_version(), STORAGE_CONTRACT_VERSION);

                let services = Services::from_lifecycle_storage(backend.lifecycle_storage());
                let root = services
                    .collections()
                    .get(CollectionID::new(1).expect("valid root collection id"))
                    .await
                    .expect("certified backend should serve lifecycle operations");
                assert_eq!(root.id, 1);
            }
        }
    }
}

pub(crate) async fn postgres_permit() -> OwnedSemaphorePermit {
    static LIMITER: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(4)));
    LIMITER
        .clone()
        .acquire_owned()
        .await
        .expect("storage contract semaphore should remain open")
}

pub(crate) fn pool() -> Data<PostgresPool> {
    let config = crate::tests::integration_test_config()
        .expect("integration test config should be initialized");
    Data::new(crate::storage::postgres::init_postgres_pool(
        &config.database_url,
        2,
    ))
}

pub(crate) fn prefix(label: &str) -> String {
    let suffix = crate::utilities::auth::generate_random_password(12).to_ascii_lowercase();
    format!("storage_contract_{label}_{suffix}")
}
