use super::*;

#[test]
fn entity_type_round_trips() {
    let all = [
        EntityType::Collection,
        EntityType::Class,
        EntityType::Object,
        EntityType::ClassRelation,
        EntityType::ObjectRelation,
        EntityType::User,
        EntityType::Group,
        EntityType::UserGroup,
        EntityType::Permission,
        EntityType::Token,
        EntityType::RemoteTarget,
        EntityType::ExportTemplate,
        EntityType::Task,
        EntityType::ServiceAccount,
        EntityType::EventSink,
        EntityType::EventSubscription,
        EntityType::ExternalIdentitySync,
        EntityType::Restore,
    ];
    for t in all {
        assert_eq!(EntityType::parse(t.as_str()).unwrap(), t);
    }
    assert!(EntityType::parse("hubuumclass").is_err());
}

#[test]
fn action_round_trips() {
    let all = [
        Action::Created,
        Action::Updated,
        Action::Deleted,
        Action::Added,
        Action::Removed,
        Action::Granted,
        Action::Revoked,
        Action::Purged,
        Action::Invoked,
        Action::Queued,
        Action::Started,
        Action::Validating,
        Action::Running,
        Action::Succeeded,
        Action::Failed,
        Action::PartiallySucceeded,
        Action::Cancelled,
        Action::Cleanup,
        Action::Disabled,
    ];
    for a in all {
        assert_eq!(Action::parse(a.as_str()).unwrap(), a);
    }
    assert!(Action::parse("patched").is_err());
}

#[test]
fn only_class_relations_support_updated() {
    assert!(is_valid_pair(EntityType::ObjectRelation, Action::Created));
    assert!(is_valid_pair(EntityType::ObjectRelation, Action::Deleted));
    assert!(!is_valid_pair(EntityType::ObjectRelation, Action::Updated));
    assert!(is_valid_pair(EntityType::ClassRelation, Action::Updated));
}

#[test]
fn permission_is_grant_revoke() {
    assert!(is_valid_pair(EntityType::Permission, Action::Granted));
    assert!(is_valid_pair(EntityType::Permission, Action::Revoked));
    assert!(!is_valid_pair(EntityType::Permission, Action::Created));
    assert!(!is_valid_pair(EntityType::Permission, Action::Updated));
}

#[test]
fn token_has_lifecycle_actions_but_no_updated_or_deleted() {
    assert!(is_valid_pair(EntityType::Token, Action::Created));
    assert!(is_valid_pair(EntityType::Token, Action::Revoked));
    assert!(is_valid_pair(EntityType::Token, Action::Purged));
    assert!(!is_valid_pair(EntityType::Token, Action::Updated));
    assert!(!is_valid_pair(EntityType::Token, Action::Deleted));
}

#[test]
fn remote_target_has_invoked() {
    assert!(is_valid_pair(EntityType::RemoteTarget, Action::Invoked));
    assert!(!is_valid_pair(EntityType::Object, Action::Invoked));
}

#[test]
fn user_group_is_add_remove() {
    assert!(is_valid_pair(EntityType::UserGroup, Action::Added));
    assert!(is_valid_pair(EntityType::UserGroup, Action::Removed));
    assert!(!is_valid_pair(EntityType::UserGroup, Action::Created));
}

#[test]
fn task_is_lifecycle_only() {
    assert!(is_valid_pair(EntityType::Task, Action::Queued));
    assert!(is_valid_pair(EntityType::Task, Action::Succeeded));
    assert!(is_valid_pair(EntityType::Task, Action::Cleanup));
    assert!(!is_valid_pair(EntityType::Task, Action::Created));
    assert!(!is_valid_pair(EntityType::Task, Action::Updated));
}

#[test]
fn restore_records_success_only() {
    assert_eq!(
        (
            is_valid_pair(EntityType::Restore, Action::Succeeded),
            is_valid_pair(EntityType::Restore, Action::Failed),
            is_valid_pair(EntityType::Restore, Action::Created),
        ),
        (true, false, false)
    );
}

#[test]
fn actor_kind_round_trips() {
    for k in [ActorKind::User, ActorKind::System, ActorKind::Worker] {
        assert_eq!(ActorKind::parse(k.as_str()).unwrap(), k);
    }
    assert!(ActorKind::parse("anonymous").is_err());
}

#[rstest::rstest]
#[case(serde_json::json!({"task_kind":"backup"}), true)]
#[case(serde_json::json!({"task_kind":"import"}), false)]
#[case(serde_json::json!({}), false)]
#[case(serde_json::json!({"task_kind":"unknown"}), false)]
fn task_kind_filter_requires_recognized_metadata(
    #[case] metadata: serde_json::Value,
    #[case] expected: bool,
) {
    let event = envelope_builder()
        .entity_type(EntityType::Task)
        .action(Action::Failed)
        .metadata(metadata)
        .try_build()
        .unwrap();
    let filter = EventSubscriptionFilter {
        task_kinds: vec![hubuum_domain::TaskKind::Backup],
        ..Default::default()
    };
    assert_eq!(filter.matches(&event), expected);
}

#[test]
fn system_scope_excludes_direct_and_related_collections() {
    assert!(!EventSubscriptionScope::System.matches(&envelope()));
    let event = envelope_builder()
        .collection_id(None)
        .metadata(serde_json::json!({"related_collection_ids":[20]}))
        .try_build()
        .unwrap();
    assert!(!EventSubscriptionScope::System.matches(&event));
    let event = envelope_builder()
        .collection_id(None)
        .metadata(serde_json::json!({}))
        .try_build()
        .unwrap();
    assert!(EventSubscriptionScope::System.matches(&event));
}

#[rstest::rstest]
#[case(serde_json::json!({"subject_is_admin": true}), true)]
#[case(serde_json::json!({"subject_is_admin": false}), false)]
#[case(serde_json::json!({"subject_principal_ids": [7, 8]}), true)]
#[case(serde_json::json!({"subject_principal_ids": [40]}), false)]
#[case(serde_json::json!({"subject_identity_scopes": ["local", "directory"]}), true)]
#[case(serde_json::json!({"subject_identity_scopes": ["local"]}), false)]
#[case(serde_json::json!({"subject_is_admin": true, "subject_principal_ids": [7], "subject_identity_scopes": ["directory"], "actor_user_ids": [40]}), true)]
#[case(serde_json::json!({"subject_is_admin": true, "subject_principal_ids": [7], "subject_identity_scopes": ["local"]}), false)]
fn subject_filters_use_snapshot_with_and_between_fields(
    #[case] filter: serde_json::Value,
    #[case] expected: bool,
) {
    let subject = EventSubject::try_new(
        PrincipalId::new(7).unwrap(),
        "original-name".into(),
        hubuum_domain::PrincipalKind::Human,
        "directory".into(),
        "ldap".into(),
        true,
    )
    .unwrap();
    // The actor is principal 40, independently of subject 7.
    let event = envelope_builder()
        .metadata(subject.successful_login_document().metadata().clone())
        .try_build()
        .unwrap();
    let filter: EventSubscriptionFilter = serde_json::from_value(filter).unwrap();
    filter.validate().unwrap();
    assert_eq!(filter.matches(&event), expected);
}

#[rstest::rstest]
#[case(serde_json::json!({"subject_is_admin": true}))]
#[case(serde_json::json!({"subject_is_admin": false}))]
#[case(serde_json::json!({"subject_principal_ids": [40]}))]
#[case(serde_json::json!({"subject_identity_scopes": ["local"]}))]
fn historical_events_do_not_match_explicit_subject_predicates(#[case] filter: serde_json::Value) {
    let filter: EventSubscriptionFilter = serde_json::from_value(filter).unwrap();
    assert!(!filter.matches(&envelope()));
}

#[rstest::rstest]
fn administrator_filter_compares_both_boolean_values(
    #[values(true, false)] recorded: bool,
    #[values(true, false)] required: bool,
) {
    let subject = EventSubject::try_new(
        PrincipalId::new(7).unwrap(),
        "principal".into(),
        hubuum_domain::PrincipalKind::Human,
        "local".into(),
        "local".into(),
        recorded,
    )
    .unwrap();
    let event = envelope_builder()
        .metadata(subject.successful_login_document().metadata().clone())
        .try_build()
        .unwrap();
    let filter = EventSubscriptionFilter {
        subject_is_admin: Some(required),
        ..Default::default()
    };
    assert_eq!(filter.matches(&event), recorded == required);
}

#[rstest::rstest]
#[case("name", serde_json::json!(" "))]
#[case("identity_scope", serde_json::json!(""))]
#[case("authentication_provider_kind", serde_json::json!(""))]
#[case("principal_id", serde_json::json!(0))]
#[case("kind", serde_json::json!("unknown"))]
#[case("is_admin", serde_json::Value::Null)]
fn subject_snapshot_rejects_incomplete_or_invalid_context(
    #[case] field: &str,
    #[case] value: serde_json::Value,
) {
    let mut subject = serde_json::json!({"principal_id": 7, "name": "principal", "kind": "human", "identity_scope": "local", "authentication_provider_kind": "local", "is_admin": true});
    subject[field] = value;
    assert!(
        envelope_builder()
            .metadata(serde_json::json!({"subject": subject}))
            .try_build()
            .is_err()
    );
}

#[rstest::rstest]
#[case(serde_json::json!({"subject_principal_ids": [7, 7]}))]
#[case(serde_json::json!({"subject_identity_scopes": ["local", "local"]}))]
#[case(serde_json::json!({"subject_identity_scopes": ["  "]}))]
fn subject_filters_reject_invalid_values(#[case] filter: serde_json::Value) {
    let filter: EventSubscriptionFilter = serde_json::from_value(filter).unwrap();
    assert!(filter.validate().is_err());
}

#[rstest::rstest]
#[case(serde_json::json!({"subject_principal_ids": [0]}))]
#[case(serde_json::json!({"subject_principal_ids": [-1]}))]
#[case(serde_json::json!({"subject_is_admin": "false"}))]
fn subject_filters_reject_invalid_wire_types(#[case] filter: serde_json::Value) {
    assert!(serde_json::from_value::<EventSubscriptionFilter>(filter).is_err());
}

#[test]
fn authentication_catalog_is_success_only() {
    assert_eq!(
        EntityType::parse("authentication").unwrap(),
        EntityType::Authentication
    );
    assert_eq!(
        valid_actions(EntityType::Authentication),
        &[Action::Updated, Action::Revoked, Action::Succeeded]
    );
}

#[test]
fn empty_subscription_filter_matches_any_event() {
    assert!(EventSubscriptionFilter::default().matches(&envelope()));
}

#[test]
fn subscription_filter_matches_selected_dimensions() {
    let request_id = Uuid::new_v4();
    let event = envelope_builder()
        .request_id(Some(request_id))
        .try_build()
        .unwrap();
    let filter = EventSubscriptionFilter {
        task_kinds: vec![],
        collection_ids: vec![CollectionId::new(10).unwrap()],
        related_collection_ids: vec![CollectionId::new(20).unwrap()],
        entity_ids: vec![EventEntityId::new(30).unwrap()],
        entity_names: vec!["test entity".to_string()],
        actor_kinds: vec!["user".to_string()],
        actor_user_ids: vec![PrincipalId::new(40).unwrap()],
        initiator_user_ids: vec![],
        request_ids: vec![request_id],
        correlation_ids: vec!["correlation".to_string()],
        ..Default::default()
    };

    assert!(filter.matches(&event));
}

#[test]
fn subscription_filter_rejects_non_matching_dimension() {
    let filter = EventSubscriptionFilter {
        actor_user_ids: vec![PrincipalId::new(999).unwrap()],
        ..EventSubscriptionFilter::default()
    };

    assert!(!filter.matches(&envelope()));
}

#[test]
fn subscription_filter_matches_task_initiator() {
    let event = envelope_builder()
        .provenance(Provenance {
            initiator: Some(ProvenancePrincipal {
                principal_id: PrincipalId::new(77).unwrap(),
                name: Some("submitter".to_string()),
            }),
            ..Provenance::default()
        })
        .try_build()
        .unwrap();
    let filter = EventSubscriptionFilter {
        initiator_user_ids: vec![PrincipalId::new(77).unwrap()],
        ..EventSubscriptionFilter::default()
    };

    assert!(filter.matches(&event));
}

#[test]
fn subscription_filter_rejects_duplicate_task_kinds() {
    let filter = EventSubscriptionFilter {
        task_kinds: vec![hubuum_domain::TaskKind::Backup; 2],
        ..Default::default()
    };
    assert!(matches!(
        filter.validate(),
        Err(EventFilterError::DuplicateValue {
            field: "task_kinds",
            ..
        })
    ));
}

#[test]
fn subscription_filter_validates_values() {
    let filter = EventSubscriptionFilter {
        actor_kinds: vec!["anonymous".to_string()],
        ..EventSubscriptionFilter::default()
    };

    assert!(matches!(
        filter.validate(),
        Err(EventFilterError::InvalidActorKind { .. })
    ));

    let filter = EventSubscriptionFilter {
        task_kinds: vec![],
        collection_ids: vec![
            CollectionId::new(10).unwrap(),
            CollectionId::new(10).unwrap(),
        ],
        ..EventSubscriptionFilter::default()
    };

    assert!(matches!(
        filter.validate(),
        Err(EventFilterError::DuplicateValue { field, .. }) if field == "collection_ids"
    ));
}

fn envelope_builder() -> EventEnvelopeBuilder {
    EventEnvelope::builder()
        .id(EventSequence::new(1).unwrap())
        .event_id(Uuid::new_v4())
        .occurred_at(
            chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap()
                .and_utc(),
        )
        .entity_type(EntityType::Collection)
        .entity_id(Some(EventEntityId::new(30).unwrap()))
        .entity_name(Some("test entity".to_string()))
        .collection_id(Some(CollectionId::new(10).unwrap()))
        .action(Action::Created)
        .actor_user_id(Some(PrincipalId::new(40).unwrap()))
        .actor_kind(ActorKind::User)
        .correlation_id(Some(CorrelationId::new("correlation").unwrap()))
        .summary("summary".to_string())
        .metadata(serde_json::json!({"related_collection_ids": [20, "21"]}))
        .schema_version(1)
}

fn envelope() -> EventEnvelope {
    envelope_builder().try_build().unwrap()
}

#[test]
fn related_collection_delivery_redacts_snapshots() {
    let event = envelope_builder()
        .collection_id(Some(CollectionId::new(1).unwrap()))
        .before(Some(serde_json::json!({"secret": "old"})))
        .after(Some(serde_json::json!({"secret": "new"})))
        .try_build()
        .unwrap();
    let projected = event.for_subscription_scope(CollectionId::new(2).unwrap().into());
    assert!(projected.before().is_none() && projected.after().is_none());
}

#[test]
fn event_envelope_debug_redacts_payload_snapshots() {
    let event = envelope_builder()
        .before(Some(serde_json::json!({"token": "before-secret"})))
        .after(Some(serde_json::json!({"token": "after-secret"})))
        .metadata(serde_json::json!({"token": "metadata-secret"}))
        .try_build()
        .unwrap();

    let debug = format!("{event:?}");

    assert!(debug.contains("<redacted>"));
    assert!(!debug.contains("before-secret"));
    assert!(!debug.contains("after-secret"));
    assert!(!debug.contains("metadata-secret"));
}

#[test]
fn event_envelope_rejects_invalid_catalog_and_payload_values() {
    let invalid_pair = envelope_builder()
        .entity_type(EntityType::ObjectRelation)
        .action(Action::Updated)
        .try_build();
    assert!(invalid_pair.is_err());

    assert!(
        envelope_builder()
            .metadata(serde_json::json!([]))
            .try_build()
            .is_err()
    );
    assert!(
        envelope_builder()
            .before(Some(serde_json::json!("not an object")))
            .try_build()
            .is_err()
    );
    assert!(envelope_builder().schema_version(0).try_build().is_err());
}

#[test]
fn event_envelope_rejects_invalid_or_mismatched_provenance_actor_kind() {
    let invalid = envelope_builder()
        .provenance(Provenance {
            actor: ProvenanceActor {
                kind: Some("anonymous".to_string()),
                principal: None,
            },
            ..Provenance::default()
        })
        .try_build();
    assert!(invalid.is_err());

    let mismatched = envelope_builder()
        .provenance(Provenance {
            actor: ProvenanceActor {
                kind: Some(ActorKind::Worker.as_str().to_string()),
                principal: None,
            },
            ..Provenance::default()
        })
        .try_build();
    assert!(mismatched.is_err());

    let mismatched_principal = envelope_builder()
        .provenance(Provenance {
            actor: ProvenanceActor {
                kind: Some(ActorKind::User.as_str().to_string()),
                principal: Some(ProvenancePrincipal {
                    principal_id: PrincipalId::new(41).unwrap(),
                    name: None,
                }),
            },
            ..Provenance::default()
        })
        .try_build();
    assert!(mismatched_principal.is_err());
}

#[test]
fn event_envelope_canonicalizes_missing_actor_provenance() {
    let event = envelope();

    assert_eq!(event.provenance().actor.kind.as_deref(), Some("user"));
    assert_eq!(
        event
            .provenance()
            .actor
            .principal
            .as_ref()
            .map(|principal| principal.principal_id),
        event.actor_user_id()
    );
}

#[test]
fn event_envelope_serialization_preserves_naive_utc_wire_format() {
    let event = envelope();
    let value = serde_json::to_value(&event).unwrap();

    assert_eq!(value["occurred_at"], "2026-01-01T00:00:00");
    let decoded: EventEnvelope = serde_json::from_value(value).unwrap();
    assert_eq!(decoded.occurred_at(), event.occurred_at());
}

#[test]
fn event_envelope_preserves_the_correlation_id_proof() {
    let correlation_id = CorrelationId::new("validated-correlation").unwrap();
    let context = EventContext::user(
        PrincipalId::new(40).unwrap(),
        None,
        Some(correlation_id.clone()),
    );
    let event = envelope_builder()
        .correlation_id(context.correlation_id().cloned())
        .try_build()
        .unwrap();
    let retained: &CorrelationId = event.correlation_id().unwrap();
    assert_eq!(retained, &correlation_id);
}

#[test]
fn event_envelope_correlation_id_keeps_its_string_wire_format() {
    let event = envelope();
    let value = serde_json::to_value(&event).unwrap();
    assert_eq!(value["correlation_id"], "correlation");
    let decoded: EventEnvelope = serde_json::from_value(value).unwrap();
    assert_eq!(decoded.correlation_id(), event.correlation_id());
}

#[test]
fn event_envelope_deserialization_rejects_invalid_correlation_ids() {
    let mut value = serde_json::to_value(envelope()).unwrap();
    value["correlation_id"] = serde_json::json!("invalid correlation");
    assert!(serde_json::from_value::<EventEnvelope>(value).is_err());
}

#[test]
fn correlation_id_enforces_the_shared_boundary() {
    assert!(CorrelationId::new("request-123").is_ok());
    assert!(matches!(
        CorrelationId::new(""),
        Err(CorrelationIdError::Empty)
    ));
    assert!(matches!(
        CorrelationId::new("contains whitespace"),
        Err(CorrelationIdError::InvalidCharacter)
    ));
    assert!(matches!(
        CorrelationId::new("x".repeat(MAX_CORRELATION_ID_BYTES + 1)),
        Err(CorrelationIdError::TooLong { .. })
    ));
}

#[test]
fn trace_link_accepts_only_bounded_w3c_identifiers() {
    let link =
        TraceLink::new("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7", 1, 0).unwrap();
    assert_eq!(link.trace_flags(), 1);
    assert!(TraceLink::new("0".repeat(32), "00f067aa0ba902b7", 0, 0).is_err());
    assert!(TraceLink::new("4bf92f3577b34da6a3ce929d0e0e4736", "xyz", 0, 0).is_err());
    assert!(TraceLink::new("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7", 2, 0,).is_err());

    let serialized = serde_json::to_value(&link).unwrap();
    assert_eq!(
        serde_json::from_value::<TraceLink>(serialized).unwrap(),
        link
    );
    assert!(
        serde_json::from_value::<TraceLink>(serde_json::json!({
            "trace_id": "not-a-trace-id",
            "span_id": "00f067aa0ba902b7",
            "trace_flags": 1,
            "version": 0
        }))
        .is_err()
    );
}

#[test]
fn event_envelope_keeps_trace_link_out_of_public_json() {
    let link =
        TraceLink::new("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7", 1, 0).unwrap();
    let event = envelope_builder()
        .trace_link(Some(link.clone()))
        .try_build()
        .unwrap();

    assert_eq!(event.trace_link(), Some(&link));
    let value = serde_json::to_value(&event).unwrap();
    assert!(value.get("trace_link").is_none());
    assert!(!value.to_string().contains(link.trace_id()));
}

#[test]
fn mutation_provenance_keeps_trace_link_out_of_serde_representations() {
    let link =
        TraceLink::new("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7", 1, 0).unwrap();
    let provenance = MutationProvenance::system().with_trace_link(Some(link.clone()));

    let value = serde_json::to_value(&provenance).unwrap();

    assert!(!value.to_string().contains(link.trace_id()));
    assert!(
        serde_json::from_value::<MutationProvenance>(value)
            .unwrap()
            .trace_link()
            .is_none()
    );
}

#[test]
fn new_event_validates_catalog_pairs_and_redacts_debug_payloads() {
    assert!(matches!(
        NewEvent::new(
            EntityType::ObjectRelation,
            Action::Updated,
            ActorKind::System,
            "invalid"
        ),
        Err(EventCatalogError::InvalidActionForType { .. })
    ));
    let document = AuditDocument::try_new(
        "created",
        Some(serde_json::json!({"token": "before-secret"})),
        Some(serde_json::json!({"token": "after-secret"})),
        serde_json::json!({"token": "metadata-secret"}),
    )
    .unwrap();
    let event = NewEvent::from_document(
        EntityType::Collection,
        Action::Created,
        ActorKind::User,
        document,
    )
    .unwrap();

    let debug = format!("{event:?}");

    assert!(debug.contains("<redacted>"));
    assert!(!debug.contains("before-secret"));
    assert!(!debug.contains("after-secret"));
    assert!(!debug.contains("metadata-secret"));
}

#[test]
fn audit_document_validates_payload_shapes_and_owns_schema_version() {
    assert!(
        AuditDocument::builder("invalid snapshot")
            .before(serde_json::json!([]))
            .try_build()
            .is_err()
    );
    assert!(
        AuditDocument::builder("invalid metadata")
            .metadata(serde_json::json!(null))
            .try_build()
            .is_err()
    );
    assert!(
        AuditDocument::builder("invalid revision")
            .after(serde_json::json!({"revision": 0}))
            .try_build()
            .is_err()
    );

    let document = AuditDocument::try_new(
        "valid",
        Some(serde_json::json!({"revision": 1})),
        None,
        serde_json::json!({"source": "test"}),
    )
    .unwrap();
    assert_eq!(
        document.schema_version(),
        REVISION_AWARE_AUDIT_DOCUMENT_SCHEMA_VERSION
    );
    assert_eq!(
        AuditDocument::summary("legacy-shaped").schema_version(),
        BASE_AUDIT_DOCUMENT_SCHEMA_VERSION
    );
}

#[test]
fn event_envelope_exposes_its_canonical_audit_document() {
    let envelope = envelope();
    let document = envelope.audit_document();

    assert_eq!(document.summary_text(), envelope.summary());
    assert_eq!(document.before(), envelope.before());
    assert_eq!(document.after(), envelope.after());
    assert_eq!(document.metadata(), envelope.metadata());
    assert_eq!(document.schema_version(), envelope.schema_version());
}

#[test]
fn sink_redaction_preserves_the_url_secret_alias() {
    let redacted = redact_event_sink_config(&serde_json::json!({"url_secret_ref":"ops_webhook"}));
    assert_eq!(redacted["url_secret_ref"], "ops_webhook");
}

#[test]
fn sink_redaction_covers_nested_keys_and_uri_userinfo() {
    let redacted = redact_event_sink_config(&serde_json::json!({
        "url": "https://user:password@example.invalid/events",
        "headers": {"X-API-Key": "secret", "routing_key": "visible"}
    }));

    assert_eq!(redacted["url"], "https://[redacted]@example.invalid/events");
    assert_eq!(redacted["headers"]["X-API-Key"], "[redacted]");
    assert_eq!(redacted["headers"]["routing_key"], "visible");
}
