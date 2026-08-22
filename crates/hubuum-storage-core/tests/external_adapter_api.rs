//! Compile-time checks for the API an out-of-tree storage adapter consumes.

use chrono::NaiveDateTime;
use hubuum_domain::{IdentityScopeId, PrincipalKind, ResourceId, ResourceRevision};
use hubuum_events_core::EventContext;
use hubuum_query::{QueryOptions, parse_query_parameter};
use hubuum_storage_core::{
    StoragePrincipal, StorageRecordMetadata, TransactionalClassRelations, TransactionalClasses,
    TransactionalCollections, TransactionalObjectRelations, TransactionalObjects,
    capabilities::resources::{
        ClassRelationStorage, ClassStorage, CollectionStorage, ObjectRelationStorage, ObjectStorage,
    },
};

fn collection_port<'a>(
    storage: &'a dyn CollectionStorage,
    context: &'a EventContext,
) -> TransactionalCollections<'a> {
    TransactionalCollections::new(storage, context)
}

fn class_port<'a>(
    storage: &'a dyn ClassStorage,
    context: &'a EventContext,
) -> TransactionalClasses<'a> {
    TransactionalClasses::new(storage, context)
}

fn class_relation_port<'a>(
    storage: &'a dyn ClassRelationStorage,
    context: &'a EventContext,
) -> TransactionalClassRelations<'a> {
    TransactionalClassRelations::new(storage, context)
}

fn object_port<'a>(
    storage: &'a dyn ObjectStorage,
    context: &'a EventContext,
) -> TransactionalObjects<'a> {
    TransactionalObjects::new(storage, context)
}

fn object_relation_port<'a>(
    storage: &'a dyn ObjectRelationStorage,
    context: &'a EventContext,
) -> TransactionalObjectRelations<'a> {
    TransactionalObjectRelations::new(storage, context)
}

#[test]
fn transaction_ports_are_constructible_outside_the_contract_crate() {
    let _ = collection_port;
    let _ = class_port;
    let _ = class_relation_port;
    let _ = object_port;
    let _ = object_relation_port;
}

#[test]
fn query_options_are_readable_and_mutable_without_exposing_their_representation() {
    let mut options = parse_query_parameter("name=router&limit=20&cursor=first").unwrap();

    assert_eq!(options.filters().len(), 1);
    assert_eq!(options.limit(), Some(20));
    assert_eq!(
        options.cursor().map(ToString::to_string).as_deref(),
        Some("first")
    );
    assert!(options.include_total());

    options.set_limit(Some(10));
    options.set_include_total(false);
    options.set_cursor(Some("second".to_string())).unwrap();
    assert_eq!(options.limit(), Some(10));
    assert!(!options.include_total());
    assert_eq!(
        options.cursor().map(ToString::to_string).as_deref(),
        Some("second")
    );

    let _: QueryOptions = options;
}

#[test]
fn principal_records_expose_typed_identity_through_accessors() {
    let timestamp = NaiveDateTime::default();
    let metadata = StorageRecordMetadata::try_new(
        ResourceId::new(7).unwrap(),
        timestamp.and_utc(),
        timestamp.and_utc(),
        ResourceRevision::INITIAL,
    )
    .unwrap();
    let principal = StoragePrincipal::builder(
        metadata,
        PrincipalKind::Human,
        "adapter-user",
        IdentityScopeId::new(3).unwrap(),
    )
    .build();

    assert_eq!(principal.id().id(), 7);
    assert_eq!(principal.identity_scope_id().id(), 3);
    assert_eq!(principal.name(), "adapter-user");
    assert_eq!(principal.revision(), ResourceRevision::INITIAL);
}
