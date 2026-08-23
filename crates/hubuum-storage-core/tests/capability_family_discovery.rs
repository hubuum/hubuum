//! Compile-time checks that each canonical capability family is independently
//! discoverable with only the shared `common` values imported beside it.

mod resources {
    use hubuum_storage_core::capabilities::{common::*, resources::*};

    #[test]
    fn exports_resource_protocol() {
        let _ = std::mem::size_of::<Option<StorageCollection>>();
        let _ = std::mem::size_of::<Option<StorageObject>>();
        let _ = std::mem::size_of::<Option<StorageClassRelation>>();
        let _ = std::mem::size_of::<Option<StorageObjectRelation>>();
        let _ = std::mem::size_of::<Option<MutationOutcome<StorageClassRecord>>>();
    }
}

mod identity {
    use hubuum_storage_core::capabilities::{common::*, identity::*};

    #[test]
    fn exports_identity_protocol() {
        let _ = std::mem::size_of::<Option<AuthenticationTokenScope>>();
        let _ = std::mem::size_of::<Option<AuthorizationPermission>>();
        let _ = std::mem::size_of::<Option<StoragePrincipal>>();
        let _ = std::mem::size_of::<Option<MutationOutcome<StorageIdentityGroup>>>();
    }
}

mod queries {
    use hubuum_storage_core::capabilities::{common::*, queries::*};

    #[test]
    fn exports_query_protocol() {
        let _ = std::mem::size_of::<Option<AuthorizationPermission>>();
        let _ = std::mem::size_of::<Option<StorageClassRecord>>();
        let _ = std::mem::size_of::<Option<StorageComputationRevision>>();
        let _ = std::mem::size_of::<Option<StorageExportTemplate>>();
        let _ = std::mem::size_of::<Option<StorageRemoteTarget>>();
        let _ = std::mem::size_of::<Option<StoragePage<StorageCollection>>>();
    }
}

mod workflows {
    use hubuum_storage_core::capabilities::{common::*, workflows::*};

    #[test]
    fn exports_workflow_protocol() {
        let _ = std::mem::size_of::<Option<StorageError>>();
        let _ = std::mem::size_of::<Option<AuthorizationPermission>>();
        let _ = std::mem::size_of::<Option<StorageClassRecord>>();
        let _ = std::mem::size_of::<Option<StorageCollection>>();
        let _ = std::mem::size_of::<Option<StorageObject>>();
        let _ = std::mem::size_of::<Option<StorageTask>>();
    }
}

mod events {
    use hubuum_storage_core::capabilities::{common::*, events::*};

    #[test]
    fn exports_event_protocol() {
        let _ = std::mem::size_of::<Option<AuditReceipt>>();
        let _ = std::mem::size_of::<Option<EventDeliveryHealthSnapshot>>();
        let _ = std::mem::size_of::<Option<EventDeliveryStatusSnapshot>>();
        let _ = std::mem::size_of::<Option<EventFanoutSnapshot>>();
        let _ = std::mem::size_of::<Option<EventQueueSnapshot>>();
        let _ = std::mem::size_of::<Option<EventSinkHealthSnapshot>>();
        let _ = std::mem::size_of::<Option<EventSinkSnapshot>>();
        let _ = std::mem::size_of::<Option<EventSubscriptionHealthSnapshot>>();
    }
}

mod operational {
    use hubuum_storage_core::capabilities::{common::*, operational::*};

    #[test]
    fn exports_operational_protocol() {
        let _ = std::mem::size_of::<Option<StorageError>>();
        let _ = std::mem::size_of::<Option<StorageQueryBudget>>();
        let _ = std::mem::size_of::<Option<StorageTaskKind>>();
        let _ = std::mem::size_of::<Option<StorageTaskStatus>>();
        let _ = std::mem::size_of::<Option<EventFanoutSnapshot>>();
        let _ = std::mem::size_of::<Option<EventQueueSnapshot>>();
        let _ = std::mem::size_of::<Option<ReadinessSnapshot>>();
    }
}
