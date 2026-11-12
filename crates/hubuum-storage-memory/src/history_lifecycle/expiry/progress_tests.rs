use super::*;
use rstest::rstest;
use serde_json::json;
use std::future::{Future, ready};

pub(super) async fn fixture_write<T>(storage: &MemoryStorage, work: impl Future<Output = T>) -> T {
    let scope = storage.current_execution_scope();
    if scope.application_access_override().is_none() {
        return work.await;
    }
    let publication = storage
        .admit_history_publication(HistoryPublicationKind::HttpResponse)
        .await
        .unwrap();
    let result = storage
        .run_in_scope(scope.with_history_read(publication.context()), work)
        .await;
    // These fixtures await synchronous storage work with no detached driver or producer.
    let completed = publication
        .after_drained(ready(Ok(())), ready(Ok(())))
        .await
        .unwrap();
    storage.settle_history_publication(completed).await.unwrap();
    result
}

pub(super) async fn seed(
    storage: &MemoryStorage,
    family: StorageIdentityHistoryKind,
    name: &str,
    description: &str,
) -> HistoryPolicyKey {
    use StorageIdentityHistoryKind as F;
    let event = EventContext::system();
    let (collection, identity) = fixture_write(storage, async {
        let collection = storage
            .create_collection(
                StorageCollectionCreate::new(name, description, GroupId::new(1).unwrap(), None),
                &event,
            )
            .await
            .unwrap()
            .into_value();
        let identity = storage
            .resolve_logical_identity(StorageIdentityTarget::Resource {
                kind: StorageIdentityResourceKind::Collection,
                id: collection.id().into(),
            })
            .await
            .unwrap()
            .subject()
            .unwrap()
            .record_identity()
            .unwrap()
            .resource();
        (collection, identity)
    })
    .await;
    let key = HistoryPolicyKey::new(family, HistoryPolicyScope::Collection { identity }).unwrap();
    let before = storage.capture_history_lifecycle().await.unwrap();
    let proposal = HistoryPolicyChange::new(
        key,
        HistoryPolicyRevision::new(0).unwrap(),
        HistoryPolicyBinding::Expire {
            trigger: HistoryExpiryTrigger::Age {
                age_seconds: HistoryRetentionSeconds::new(0).unwrap(),
                deleted_age_seconds: Some(HistoryRetentionSeconds::new(0).unwrap()),
            },
            minimum_age_seconds: HistoryRetentionSeconds::new(0).unwrap(),
        },
        HistoryPolicyApplication::Prospective,
    )
    .unwrap();
    storage
        .commit_history_lifecycle(
            before
                .control()
                .apply(
                    HistoryPolicyApplyRequest::new(proposal, None),
                    PrincipalIdentity::generate(),
                    before.fence(),
                    before.observed_at() - Duration::seconds(1),
                )
                .unwrap(),
        )
        .await
        .unwrap();
    fixture_write(storage, async {
        match family {
            F::Collection => {
                storage
                    .update_collection(
                        collection.id(),
                        StorageCollectionUpdate::new(None, Some("after".into())),
                        &event,
                    )
                    .await
                    .unwrap()
                    .into_value();
            }
            F::ExportTemplate => {
                let template = storage
                    .create_export_template(StorageExportTemplateCreate::new(
                        collection.id(),
                        name,
                        StorageExportTemplateDefinition::new(
                            "before",
                            "text/plain",
                            "hello",
                            "fragment",
                        ),
                        event.clone(),
                    ))
                    .await
                    .unwrap()
                    .into_value();
                storage
                    .delete_export_template(StorageExportTemplateDelete::new(
                        ExportTemplateId::new(template.metadata().id().id()).unwrap(),
                        event.clone(),
                    ))
                    .await
                    .unwrap()
                    .into_value();
            }
            F::RemoteTarget => {
                let transport = StorageRemoteTargetTransport::try_new(
                    StorageRemoteTargetHttpMethod::Get,
                    "https://example.invalid",
                    json!({}),
                    None,
                    json!({}),
                    1000,
                )
                .unwrap();
                let policy = StorageRemoteTargetPolicy::try_new(
                    None,
                    vec![StorageRemoteTargetSubjectType::Collection],
                    true,
                )
                .unwrap();
                let target = storage
                    .create_remote_target(StorageRemoteTargetCreate::new(
                        collection.id(),
                        name,
                        StorageRemoteTargetDefinition::new("before", transport, policy),
                        event.clone(),
                    ))
                    .await
                    .unwrap()
                    .into_value();
                storage
                    .delete_remote_target(StorageRemoteTargetDelete::new(
                        RemoteTargetId::new(target.metadata().id().id()).unwrap(),
                        event.clone(),
                    ))
                    .await
                    .unwrap()
                    .into_value();
            }
            _ => {
                let first = storage
                    .create_class(
                        StorageClassCreate::builder(format!("{name}_first"), collection.id(), "")
                            .build(),
                        &event,
                    )
                    .await
                    .unwrap()
                    .into_value();
                if matches!(family, F::ClassRelation | F::ObjectRelation) {
                    let second = storage
                        .create_class(
                            StorageClassCreate::builder(
                                format!("{name}_second"),
                                collection.id(),
                                "",
                            )
                            .build(),
                            &event,
                        )
                        .await
                        .unwrap()
                        .into_value();
                    let prepared = storage
                        .prepare_class_relation(
                            StorageClassRelationCreate::builder(first.id(), second.id()).build(),
                        )
                        .await
                        .unwrap();
                    let relation = storage
                        .create_class_relation(&prepared, &event)
                        .await
                        .unwrap()
                        .into_value();
                    if family == F::ObjectRelation {
                        let mut objects = Vec::new();
                        for class in [&first, &second] {
                            let resolved = storage
                                .resolve_class(StorageClassSelector::Id(class.id()))
                                .await
                                .unwrap();
                            objects.push(
                                storage
                                    .create_object(
                                        &resolved,
                                        StorageObjectCreate::new(
                                            class.name(),
                                            collection.id(),
                                            class.id(),
                                            json!({}),
                                            "",
                                        ),
                                        &event,
                                    )
                                    .await
                                    .unwrap()
                                    .into_value(),
                            );
                        }
                        let prepared = storage
                            .prepare_object_relation(StorageObjectRelationCreateSelector::Explicit(
                                StorageObjectRelationCreate::new(
                                    objects[0].id(),
                                    objects[1].id(),
                                    ClassRelationId::new(relation.relation().metadata().id().id())
                                        .unwrap(),
                                ),
                            ))
                            .await
                            .unwrap();
                        storage
                            .create_object_relation(&prepared, &event)
                            .await
                            .unwrap()
                            .into_value();
                    }
                    let resolved = storage
                        .resolve_class(StorageClassSelector::Id(second.id()))
                        .await
                        .unwrap();
                    storage
                        .delete_class(&resolved, &event)
                        .await
                        .unwrap()
                        .into_value();
                } else if family == F::Object {
                    let class = storage
                        .resolve_class(StorageClassSelector::Id(first.id()))
                        .await
                        .unwrap();
                    let object = storage
                        .create_object(
                            &class,
                            StorageObjectCreate::new(
                                name,
                                collection.id(),
                                first.id(),
                                json!({}),
                                "before",
                            ),
                            &event,
                        )
                        .await
                        .unwrap()
                        .into_value();
                    let resolved = storage
                        .resolve_object(StorageObjectSelector::Ids {
                            class_id: first.id(),
                            object_id: object.id(),
                        })
                        .await
                        .unwrap();
                    storage
                        .delete_object(&resolved, &event)
                        .await
                        .unwrap()
                        .into_value();
                } else {
                    let resolved = storage
                        .resolve_class(StorageClassSelector::Id(first.id()))
                        .await
                        .unwrap();
                    storage
                        .delete_class(&resolved, &event)
                        .await
                        .unwrap()
                        .into_value();
                }
            }
        }
    })
    .await;
    key
}

struct Run<'a> {
    storage: &'a MemoryStorage,
    scope: StorageExecutionScope,
    request: HistoryExpiryAdmissionRequest,
    progress: Option<HistoryExpiryScanProgress>,
}

impl<'a> Run<'a> {
    async fn new(
        storage: &'a MemoryStorage,
        runtime: &RuntimeRegistration,
        family: StorageIdentityHistoryKind,
        limit: u32,
    ) -> Self {
        let application = storage
            .capture_application_execution(Some(runtime.owner()))
            .await
            .unwrap();
        let before = storage.capture_history_lifecycle().await.unwrap();
        Self {
            storage,
            scope: StorageExecutionScope::default().with_application_access(application),
            request: HistoryExpiryAdmissionRequest::new(
                Uuid::new_v4(),
                Uuid::new_v4(),
                HistoryExpiryOrigin::Scheduled {
                    initiator: None,
                    policy: None,
                },
                HistoryLifecycleScan::new(
                    HistoryPolicyKey::new(family, HistoryPolicyScope::Deployment).unwrap(),
                    limit,
                )
                .unwrap(),
                before.fence(),
            )
            .unwrap(),
            progress: None,
        }
    }

    async fn page(&mut self) -> HistoryExpiryAdmissionResult {
        if let Some(progress) = &self.progress {
            let current = self.storage.capture_history_lifecycle().await.unwrap();
            self.request = self
                .request
                .next_page(Uuid::new_v4(), current.fence(), progress.clone())
                .unwrap();
        }
        let page = self
            .storage
            .run_in_scope(
                self.scope.clone(),
                self.storage.admit_history_expiry(self.request.clone()),
            )
            .await
            .unwrap();
        assert!(page.scanned() <= self.request.scan().limit());
        self.progress = Some(page.progress().clone());
        page
    }

    async fn pass(&mut self) -> Vec<HistoryExpiryMember> {
        let bound = {
            let state = self.storage.state.read_control().await;
            state.history.len()
                + state.schema_history.len()
                + state.computed_history.len()
                + state.relation_history.values().map(Vec::len).sum::<usize>()
                + 1
        };
        let mut members = Vec::new();
        for _ in 0..bound {
            let page = self.page().await;
            if let Some(manifest) = page.manifest() {
                members.extend_from_slice(manifest.members());
            }
            if matches!(page.progress(), HistoryExpiryScanProgress::PassFinished) {
                return members;
            }
        }
        panic!("memory expiry must finish its finite pass")
    }
}

#[rstest]
#[case::collection(StorageIdentityHistoryKind::Collection)]
#[case::class(StorageIdentityHistoryKind::Class)]
#[case::object(StorageIdentityHistoryKind::Object)]
#[case::class_relation(StorageIdentityHistoryKind::ClassRelation)]
#[case::object_relation(StorageIdentityHistoryKind::ObjectRelation)]
#[case::template(StorageIdentityHistoryKind::ExportTemplate)]
#[case::remote(StorageIdentityHistoryKind::RemoteTarget)]
#[case::computed(StorageIdentityHistoryKind::ComputedDefinitions)]
#[case::schema(StorageIdentityHistoryKind::ClassSchema)]
#[tokio::test]
async fn bounded_pass_revisits_released_prefix_without_reselecting_retained_rows(
    #[case] family: StorageIdentityHistoryKind,
) {
    let storage = MemoryStorage::new();
    let key_a = seed(&storage, family, "held", "before").await;
    let key_b = seed(&storage, family, "eligible", "before").await;
    let principal = PrincipalIdentity::generate();
    let hold = Uuid::new_v4();
    let before = storage.capture_history_lifecycle().await.unwrap();
    storage
        .commit_history_lifecycle(
            before
                .control()
                .create_hold(
                    HistoryRetentionHold::new(hold, key_a, before.observed_at(), None, principal)
                        .unwrap(),
                    before.fence(),
                )
                .unwrap(),
        )
        .await
        .unwrap();
    let runtime = storage.register_history_runtime(None).await.unwrap();
    let mut run = Run::new(&storage, &runtime, family, 1).await;
    let first = run.pass().await;
    let current = storage.capture_history_lifecycle().await.unwrap();
    storage
        .commit_history_lifecycle(
            current
                .control()
                .release_hold(hold, principal, current.fence(), current.observed_at())
                .unwrap(),
        )
        .await
        .unwrap();
    let second = run.pass().await;
    storage.unregister_runtime(runtime).await.unwrap();
    assert!(
        !first.is_empty(),
        "eligible rows behind a held prefix must be reached"
    );
    assert!(first.iter().all(|member| {
        member
            .attributions()
            .iter()
            .any(|scope| scope.matches(key_b.scope()))
    }));
    assert!(
        !second.is_empty(),
        "a new pass must revisit the released prefix"
    );
    assert!(second.iter().all(|member| {
        member
            .attributions()
            .iter()
            .any(|scope| scope.matches(key_a.scope()))
    }));
}

#[tokio::test]
async fn oversized_first_row_does_not_starve_later_memory_work() {
    let storage = MemoryStorage::new();
    let oversized = seed(
        &storage,
        StorageIdentityHistoryKind::Collection,
        "oversized",
        &"x".repeat(17 * 1024 * 1024),
    )
    .await;
    let eligible = seed(
        &storage,
        StorageIdentityHistoryKind::Collection,
        "eligible",
        "before",
    )
    .await;
    let runtime = storage.register_history_runtime(None).await.unwrap();
    let mut run = Run::new(
        &storage,
        &runtime,
        StorageIdentityHistoryKind::Collection,
        1,
    )
    .await;
    let members = run.pass().await;
    storage.unregister_runtime(runtime).await.unwrap();
    assert!(!members.is_empty());
    assert!(members.iter().all(|member| {
        member
            .attributions()
            .iter()
            .any(|scope| scope.matches(eligible.scope()))
    }));
    assert!(members.iter().all(|member| {
        !member
            .attributions()
            .iter()
            .any(|scope| scope.matches(oversized.scope()))
    }));
}

#[tokio::test]
async fn cumulative_byte_limit_defers_rows_to_a_fresh_page() {
    let storage = MemoryStorage::new();
    let first = seed(
        &storage,
        StorageIdentityHistoryKind::Collection,
        "first",
        &"x".repeat(9 * 1024 * 1024),
    )
    .await;
    let second = seed(
        &storage,
        StorageIdentityHistoryKind::Collection,
        "second",
        &"x".repeat(9 * 1024 * 1024),
    )
    .await;
    let runtime = storage.register_history_runtime(None).await.unwrap();
    let mut run = Run::new(
        &storage,
        &runtime,
        StorageIdentityHistoryKind::Collection,
        1000,
    )
    .await;
    let members = run.pass().await;
    storage.unregister_runtime(runtime).await.unwrap();
    for key in [first, second] {
        assert!(
            members.iter().any(|member| member
                .attributions()
                .iter()
                .any(|scope| scope.matches(key.scope()))),
            "each individually bounded row must eventually be selected"
        );
    }
}

#[rstest]
#[case::collection(StorageIdentityHistoryKind::Collection)]
#[case::class(StorageIdentityHistoryKind::Class)]
#[case::object(StorageIdentityHistoryKind::Object)]
#[case::class_relation(StorageIdentityHistoryKind::ClassRelation)]
#[case::object_relation(StorageIdentityHistoryKind::ObjectRelation)]
#[case::template(StorageIdentityHistoryKind::ExportTemplate)]
#[case::remote(StorageIdentityHistoryKind::RemoteTarget)]
#[case::computed(StorageIdentityHistoryKind::ComputedDefinitions)]
#[case::schema(StorageIdentityHistoryKind::ClassSchema)]
#[tokio::test]
async fn appended_rows_do_not_renew_a_memory_pass(#[case] family: StorageIdentityHistoryKind) {
    let storage = MemoryStorage::new();
    seed(&storage, family, "first", "before").await;
    seed(&storage, family, "second", "before").await;
    let runtime = storage.register_history_runtime(None).await.unwrap();
    let mut run = Run::new(&storage, &runtime, family, 1).await;
    let first = run.page().await;
    assert!(matches!(
        first.progress(),
        HistoryExpiryScanProgress::Continue(_)
    ));
    let added = storage
        .run_in_scope(run.scope.clone(), seed(&storage, family, "added", "before"))
        .await;
    let remaining = run.pass().await;
    let following = run.pass().await;
    storage.unregister_runtime(runtime).await.unwrap();
    assert!(!remaining.iter().any(|member| {
        member
            .attributions()
            .iter()
            .any(|scope| scope.matches(added.scope()))
    }));
    assert!(following.iter().any(|member| {
        member
            .attributions()
            .iter()
            .any(|scope| scope.matches(added.scope()))
    }));
}

#[tokio::test]
async fn old_open_version_closing_after_its_page_is_revisited() {
    let storage = MemoryStorage::new();
    seed(
        &storage,
        StorageIdentityHistoryKind::Collection,
        "first",
        "before",
    )
    .await;
    seed(
        &storage,
        StorageIdentityHistoryKind::Collection,
        "second",
        "before",
    )
    .await;
    let (collection, position, target, from) = {
        let state = storage.state.read_control().await;
        let collection = state
            .collections
            .values()
            .find(|collection| collection.name() == "first")
            .unwrap()
            .id();
        let (index,row)=state.history.iter().enumerate().find(|(_,row)|row.valid_to.is_none() && matches!(&row.value,MemoryHistoryValue::Collection(value) if value.id()==collection)).unwrap();
        (
            collection,
            index as u64 + 1,
            row.logical_identity
                .subject()
                .unwrap()
                .record_identity()
                .unwrap(),
            row.valid_from,
        )
    };
    let runtime = storage.register_history_runtime(None).await.unwrap();
    let mut run = Run::new(
        &storage,
        &runtime,
        StorageIdentityHistoryKind::Collection,
        1,
    )
    .await;
    let mut seen = Vec::new();
    loop {
        let page = run.page().await;
        if let Some(manifest) = page.manifest() {
            seen.extend_from_slice(manifest.members());
        }
        let HistoryExpiryScanProgress::Continue(cursor) = page.progress() else {
            panic!("the fixture must have a later source row")
        };
        let request = run.request.clone().continue_from(cursor.clone()).unwrap();
        if request.scan_window(0).unwrap().after() >= position {
            break;
        }
    }
    storage
        .run_in_scope(
            run.scope.clone(),
            fixture_write(
                &storage,
                storage.update_collection(
                    collection,
                    StorageCollectionUpdate::new(None, Some("late closure".into())),
                    &EventContext::system(),
                ),
            ),
        )
        .await
        .unwrap()
        .into_value();
    seen.extend(run.pass().await);
    let following = run.pass().await;
    storage.unregister_runtime(runtime).await.unwrap();
    assert!(!seen.iter().any(|member| member.target() == target));
    let selected = following
        .iter()
        .find(|member| member.target() == target)
        .expect("the original open version must become eligible in a later pass");
    assert_eq!(selected.read_span().valid_from(), from);
    assert!(selected.read_span().valid_to().is_some());
}
