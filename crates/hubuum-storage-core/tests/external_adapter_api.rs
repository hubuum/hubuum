//! Compile-time checks for the API an out-of-tree storage adapter consumes.

use chrono::NaiveDateTime;
use hubuum_domain::{
    CollectionId, EventSinkId, EventSubscriptionId, IdentityScopeId, PrincipalKind, ResourceId,
    ResourceRevision,
};
use hubuum_events_core::{Action, EntityType, EventContext, EventSubscriptionFilter};
use hubuum_query::{QueryOptions, parse_query_parameter};
use hubuum_storage_core::{
    StorageEventSinkCreate, StorageEventSinkDelete, StorageEventSinkUpdate,
    StorageEventSubscriptionCreate, StorageEventSubscriptionDelete, StorageEventSubscriptionUpdate,
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

    options.set_limit(Some(10)).unwrap();
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
    .try_build()
    .unwrap();

    assert_eq!(principal.id().id(), 7);
    assert_eq!(principal.identity_scope_id().id(), 3);
    assert_eq!(principal.name(), "adapter-user");
    assert_eq!(principal.revision(), ResourceRevision::INITIAL);
}

#[test]
fn event_administration_requests_are_validated_and_readable_outside_the_crate() {
    let context = EventContext::system();
    let sink_id = EventSinkId::new(2).unwrap();
    let collection_id = CollectionId::new(3).unwrap();
    let subscription_id = EventSubscriptionId::new(4).unwrap();

    let sink_create = StorageEventSinkCreate::builder("sink", "webhook", context.clone())
        .configuration(serde_json::json!({"url": "https://example.invalid"}))
        .secret_ref(Some("secret".to_string()))
        .enabled(true)
        .try_build()
        .unwrap();
    assert_eq!(sink_create.name(), "sink");
    assert_eq!(sink_create.kind(), "webhook");
    assert!(sink_create.configuration().is_object());
    assert_eq!(sink_create.secret_ref(), Some("secret"));
    assert!(sink_create.enabled());
    let _ = sink_create.event_context();

    let sink_update = StorageEventSinkUpdate::builder(sink_id, context.clone())
        .name(Some("renamed".to_string()))
        .kind(Some("webhook".to_string()))
        .configuration(Some(serde_json::json!({})))
        .secret_ref(Some(None))
        .enabled(Some(false))
        .try_build()
        .unwrap();
    assert_eq!(sink_update.id(), sink_id);
    assert_eq!(sink_update.name_value(), Some("renamed"));
    assert_eq!(sink_update.kind_value(), Some("webhook"));
    assert!(sink_update.configuration_value().is_some());
    assert_eq!(sink_update.secret_ref_value(), Some(None));
    assert_eq!(sink_update.enabled_value(), Some(false));
    let _ = sink_update.event_context();

    let subscription_create = StorageEventSubscriptionCreate::builder(
        collection_id,
        sink_id,
        "subscription",
        context.clone(),
    )
    .description("description")
    .entity_types(vec![EntityType::Collection])
    .actions(vec![Action::Created])
    .filter(EventSubscriptionFilter::default())
    .routing(serde_json::json!({}))
    .enabled(true)
    .try_build()
    .unwrap();
    assert_eq!(subscription_create.collection_id(), collection_id);
    assert_eq!(subscription_create.sink_id(), sink_id);
    assert_eq!(subscription_create.name(), "subscription");
    assert_eq!(subscription_create.description(), "description");
    assert_eq!(subscription_create.entity_types(), [EntityType::Collection]);
    assert_eq!(subscription_create.actions(), [Action::Created]);
    let _ = subscription_create.filter();
    assert!(subscription_create.routing().is_object());
    assert!(subscription_create.enabled());
    let _ = subscription_create.event_context();

    let subscription_update =
        StorageEventSubscriptionUpdate::builder(collection_id, subscription_id, context.clone())
            .sink_id(Some(sink_id))
            .name(Some("renamed".to_string()))
            .description(Some("updated".to_string()))
            .entity_types(Some(vec![EntityType::Collection]))
            .actions(Some(vec![Action::Updated]))
            .filter(Some(EventSubscriptionFilter::default()))
            .routing(Some(serde_json::json!({})))
            .enabled(Some(false))
            .try_build()
            .unwrap();
    assert_eq!(subscription_update.collection_id(), collection_id);
    assert_eq!(subscription_update.id(), subscription_id);
    assert_eq!(subscription_update.sink_id_value(), Some(sink_id));
    assert_eq!(subscription_update.name_value(), Some("renamed"));
    assert_eq!(subscription_update.description_value(), Some("updated"));
    assert_eq!(
        subscription_update.entity_types_value(),
        Some([EntityType::Collection].as_slice())
    );
    assert_eq!(
        subscription_update.actions_value(),
        Some([Action::Updated].as_slice())
    );
    assert!(subscription_update.filter_value().is_some());
    assert!(subscription_update.routing_value().is_some());
    assert_eq!(subscription_update.enabled_value(), Some(false));
    let _ = subscription_update.event_context();

    let sink_delete = StorageEventSinkDelete::new(sink_id, context.clone());
    assert_eq!(sink_delete.id(), sink_id);
    let _ = sink_delete.event_context();
    let subscription_delete =
        StorageEventSubscriptionDelete::new(collection_id, subscription_id, context);
    assert_eq!(subscription_delete.collection_id(), collection_id);
    assert_eq!(subscription_delete.id(), subscription_id);
    let _ = subscription_delete.event_context();
}
