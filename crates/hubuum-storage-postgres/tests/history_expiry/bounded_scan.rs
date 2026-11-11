use super::*;
use chrono::DateTime;
use diesel::sql_types::{BigInt, Integer, Nullable, Timestamptz};
use rstest::rstest;

fn source(family: StorageIdentityHistoryKind) -> (&'static str, &'static str) {
    use StorageIdentityHistoryKind as F;
    let table = match family {
        F::Collection => "collections_history",
        F::Class => "hubuumclass_history",
        F::Object => "hubuumobject_history",
        F::ClassRelation => "hubuumclass_relation_history",
        F::ObjectRelation => "hubuumobject_relation_history",
        F::ExportTemplate => "export_templates_history",
        F::RemoteTarget => "remote_targets_history",
        F::ComputedDefinitions => "computed_field_history",
        F::ClassSchema => "class_schema_history",
    };
    (
        table,
        if family == F::ClassSchema {
            "id"
        } else {
            "history_id"
        },
    )
}

#[derive(QueryableByName)]
struct Count {
    #[diesel(sql_type = BigInt)]
    value: i64,
}

#[derive(QueryableByName)]
struct Fact {
    #[diesel(sql_type = BigInt)]
    position: i64,
    #[diesel(sql_type = Jsonb)]
    logical_identity: Value,
    #[diesel(sql_type = Timestamptz)]
    valid_from: DateTime<Utc>,
    #[diesel(sql_type = Nullable<Timestamptz>)]
    valid_to: Option<DateTime<Utc>>,
}

impl Fact {
    fn target(&self) -> StorageRecordIdentity {
        serde_json::from_value::<StorageIdentityEvidence>(self.logical_identity.clone())
            .unwrap()
            .subject()
            .unwrap()
            .record_identity()
            .unwrap()
    }
}

async fn fact_on(
    connection: &mut PostgresConnection,
    family: StorageIdentityHistoryKind,
    collection: i32,
    open: bool,
) -> Result<Fact, PostgresStorageError> {
    use StorageIdentityHistoryKind as F;
    let (table, position) = source(family);
    let (from, until, interval) = if family == F::ClassSchema {
        ("occurred_at", "NULL::timestamptz", "TRUE")
    } else {
        (
            "valid_from",
            "valid_to",
            if open {
                "valid_to IS NULL"
            } else {
                "valid_to IS NOT NULL"
            },
        )
    };
    let value = if matches!(family, F::ComputedDefinitions | F::ClassSchema) {
        "TRUE"
    } else {
        "op<>'D'"
    };
    // Identify this fixture through original UUID attribution. Native numeric
    // coordinates are not used to manufacture missing historical scope.
    let query = format!(
        "SELECT {position}::bigint AS position,logical_identity,{from} AS valid_from,{until} AS valid_to FROM public.{table} WHERE ({interval}) AND ({value}) AND (logical_identity #>> '{{references,/collection_id,reference,identity}}'=(SELECT logical_identity #>> '{{subject,identity,identity}}' FROM public.collections WHERE id=$1) OR logical_identity #>> '{{references,/lifecycle/from/collection,reference,identity}}'=(SELECT logical_identity #>> '{{subject,identity,identity}}' FROM public.collections WHERE id=$1) OR ($2='collection' AND logical_identity #>> '{{subject,identity,identity}}'=(SELECT logical_identity #>> '{{subject,identity,identity}}' FROM public.collections WHERE id=$1))) ORDER BY {position} DESC LIMIT 1"
    );
    sql_query(query)
        .bind::<Integer, _>(collection)
        .bind::<Text, _>(serde_json::to_value(family).unwrap().as_str().unwrap())
        .get_result(connection)
        .await
        .map_err(Into::into)
}

struct BoundedRun<'a> {
    fixture: &'a Fixture,
    scope: StorageExecutionScope,
    request: HistoryExpiryAdmissionRequest,
    progress: Option<HistoryExpiryScanProgress>,
    admitted: Vec<Uuid>,
}

impl<'a> BoundedRun<'a> {
    async fn new(
        fixture: &'a Fixture,
        runtime: &RuntimeRegistration,
        family: StorageIdentityHistoryKind,
        limit: u32,
    ) -> Result<Self, StorageError> {
        let application = fixture
            .storage
            .capture_application_execution(Some(runtime.owner()))
            .await?;
        let snapshot = fixture.storage.capture_history_lifecycle().await?;
        Ok(Self {
            fixture,
            scope: StorageExecutionScope::default().with_application_access(application),
            request: HistoryExpiryAdmissionRequest::new(
                Uuid::new_v4(),
                Uuid::new_v4(),
                HistoryExpiryOrigin::Scheduled { initiator: None },
                HistoryLifecycleScan::new(
                    HistoryPolicyKey::new(family, HistoryPolicyScope::Deployment).unwrap(),
                    limit,
                )
                .unwrap(),
                snapshot.fence(),
            )
            .unwrap(),
            progress: None,
            admitted: Vec::new(),
        })
    }

    async fn page(&mut self) -> Result<HistoryExpiryAdmissionResult, StorageError> {
        if let Some(progress) = &self.progress {
            let current = self.fixture.storage.capture_history_lifecycle().await?;
            self.request = self
                .request
                .next_page(Uuid::new_v4(), current.fence(), progress.clone())
                .map_err(|error| StorageError::invalid_input(error.to_string()))?;
        }
        let page = self
            .fixture
            .storage
            .run_in_scope(
                self.scope.clone(),
                self.fixture
                    .storage
                    .admit_history_expiry(self.request.clone()),
            )
            .await?;
        if let Some(manifest) = page.manifest() {
            self.admitted.push(manifest.identity());
        }
        if page.scanned() > self.request.scan().limit()
            || page.manifest().is_some_and(|manifest| {
                manifest.members().len() > self.request.scan().limit() as usize
            })
        {
            return Err(StorageError::backend_failure(
                "Native expiry exceeded its bounded page",
            ));
        }
        self.progress = Some(page.progress().clone());
        Ok(page)
    }

    async fn bound(&self) -> Result<i64, StorageError> {
        let (table, _) = source(self.request.scan().key().family());
        with_transaction(&self.fixture.owner_pool, async |connection| {
            owner(connection).await?;
            sql_query(format!("SELECT count(*)+2 AS value FROM public.{table}"))
                .get_result::<Count>(connection)
                .await
                .map(|count| count.value)
                .map_err(PostgresStorageError::from)
        })
        .await
        .map_err(Into::into)
    }

    async fn pass(&mut self) -> Result<Vec<HistoryExpiryMember>, StorageError> {
        let mut members = Vec::new();
        for _ in 0..self.bound().await? {
            let page = self.page().await?;
            if let Some(manifest) = page.manifest() {
                members.extend_from_slice(manifest.members());
            }
            if matches!(page.progress(), HistoryExpiryScanProgress::PassFinished) {
                return Ok(members);
            }
        }
        Err(StorageError::backend_failure(
            "Native expiry did not finish its finite pass",
        ))
    }

    async fn past(&mut self, position: i64) -> Result<Vec<HistoryExpiryMember>, StorageError> {
        let mut members = Vec::new();
        for _ in 0..self.bound().await? {
            let page = self.page().await?;
            if let Some(manifest) = page.manifest() {
                members.extend_from_slice(manifest.members());
            }
            let HistoryExpiryScanProgress::Continue(cursor) = page.progress() else {
                return Err(StorageError::backend_failure(
                    "Fixture needs a later visible row inside this pass",
                ));
            };
            let request = self.request.clone().continue_from(cursor.clone()).unwrap();
            if request.scan_window(0).unwrap().after() >= position as u64 {
                return Ok(members);
            }
        }
        Err(StorageError::backend_failure(
            "Native expiry did not reach the fixture position",
        ))
    }
}

async fn schema_collection_policy(
    fixture: &Fixture,
    family: StorageIdentityHistoryKind,
    collection: i32,
) {
    if family != StorageIdentityHistoryKind::ClassSchema {
        return;
    }
    let evidence = with_transaction(&fixture.owner_pool, async |connection| {
        owner(connection).await?;
        sql_query("SELECT logical_identity AS material FROM public.collections WHERE id=$1")
            .bind::<Integer, _>(collection)
            .get_result::<Material>(connection)
            .await
            .map_err(PostgresStorageError::from)
    })
    .await
    .unwrap();
    let identity = serde_json::from_value::<StorageIdentityEvidence>(evidence.material)
        .unwrap()
        .subject()
        .unwrap()
        .record_identity()
        .unwrap()
        .resource();
    let before = fixture.storage.capture_history_lifecycle().await.unwrap();
    let proposal = HistoryPolicyChange::new(
        HistoryPolicyKey::new(family, HistoryPolicyScope::Collection { identity }).unwrap(),
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
    let change = before
        .control()
        .apply(
            HistoryPolicyApplyRequest::new(proposal, None),
            PrincipalIdentity::generate(),
            before.fence(),
            before.observed_at() - Duration::seconds(30),
        )
        .unwrap();
    fixture
        .storage
        .commit_history_lifecycle(change)
        .await
        .unwrap();
}

async fn live_edges(fixture: &Fixture, family: StorageIdentityHistoryKind, collection: i32) {
    if !matches!(
        family,
        StorageIdentityHistoryKind::ClassRelation | StorageIdentityHistoryKind::ObjectRelation
    ) {
        return;
    }
    with_transaction(&fixture.owner_pool, async |connection| {
        owner(connection).await?;
        sql_query("INSERT INTO public.hubuumclass(name,description,collection_id) VALUES($1,'late endpoint',$2)").bind::<Text,_>(format!("expiry_late_{}",Uuid::new_v4().simple())).bind::<Integer,_>(collection).execute(connection).await?;
        sql_query("INSERT INTO public.hubuumobject(name,description,collection_id,hubuum_class_id) SELECT name,'late endpoint',collection_id,id FROM public.hubuumclass WHERE collection_id=$1 AND NOT EXISTS(SELECT 1 FROM public.hubuumobject o WHERE o.hubuum_class_id=hubuumclass.id)").bind::<Integer,_>(collection).execute(connection).await?;
        insert_edge(connection,collection,StorageIdentityHistoryKind::ClassRelation).await?;
        insert_edge(connection,collection,StorageIdentityHistoryKind::ObjectRelation).await?;
        Ok::<_,PostgresStorageError>(())
    }).await.unwrap();
}

async fn insert_edge(
    connection: &mut PostgresConnection,
    collection: i32,
    family: StorageIdentityHistoryKind,
) -> Result<(), PostgresStorageError> {
    let query = if family == StorageIdentityHistoryKind::ClassRelation {
        "INSERT INTO public.hubuumclass_relation(from_hubuum_class_id,to_hubuum_class_id) SELECT min(id),max(id) FROM public.hubuumclass WHERE collection_id=$1"
    } else {
        "INSERT INTO public.hubuumobject_relation(from_hubuum_object_id,to_hubuum_object_id,class_relation_id) SELECT a.id,b.id,r.id FROM public.hubuumobject a JOIN public.hubuumobject b ON a.id<b.id JOIN public.hubuumclass_relation r ON r.from_hubuum_class_id=a.hubuum_class_id AND r.to_hubuum_class_id=b.hubuum_class_id WHERE a.collection_id=$1 AND b.collection_id=$1"
    };
    sql_query(query)
        .bind::<Integer, _>(collection)
        .execute(connection)
        .await?;
    Ok(())
}

async fn close_live(
    connection: &mut PostgresConnection,
    collection: i32,
    family: StorageIdentityHistoryKind,
) -> Result<(), PostgresStorageError> {
    use StorageIdentityHistoryKind as F;
    let query = match family {
        F::Collection => {
            "UPDATE public.collections SET description=description||' late' WHERE id=$1"
        }
        F::Class => {
            "UPDATE public.hubuumclass SET description=description||' late' WHERE collection_id=$1"
        }
        F::Object => {
            "UPDATE public.hubuumobject SET description=description||' late' WHERE collection_id=$1"
        }
        F::ExportTemplate => {
            "UPDATE public.export_templates SET description=description||' late' WHERE collection_id=$1"
        }
        F::RemoteTarget => {
            "UPDATE public.remote_targets SET description=description||' late' WHERE collection_id=$1"
        }
        F::ClassRelation => {
            "DELETE FROM public.hubuumclass_relation WHERE from_hubuum_class_id IN (SELECT id FROM public.hubuumclass WHERE collection_id=$1)"
        }
        F::ObjectRelation => {
            "DELETE FROM public.hubuumobject_relation WHERE from_hubuum_object_id IN (SELECT id FROM public.hubuumobject WHERE collection_id=$1)"
        }
        F::ComputedDefinitions => "DELETE FROM public.hubuumclass WHERE collection_id=$1",
        F::ClassSchema => unreachable!("schema events have no open interval"),
    };
    sql_query(query)
        .bind::<Integer, _>(collection)
        .execute(connection)
        .await?;
    Ok(())
}

async fn create_closed(
    connection: &mut PostgresConnection,
    collection: i32,
    family: StorageIdentityHistoryKind,
) -> Result<Fact, PostgresStorageError> {
    use StorageIdentityHistoryKind as F;
    match family {
        F::ComputedDefinitions | F::ClassSchema => {
            #[derive(QueryableByName)]
            struct Class {
                #[diesel(sql_type=Integer)]
                id: i32,
            }
            let class = sql_query("INSERT INTO public.hubuumclass(name,description,collection_id) VALUES($1,'late class',$2) RETURNING id").bind::<Text,_>(format!("expiry_pending_{}",Uuid::new_v4().simple())).bind::<Integer,_>(collection).get_result::<Class>(connection).await?;
            sql_query("DELETE FROM public.hubuumclass WHERE id=$1")
                .bind::<Integer, _>(class.id)
                .execute(connection)
                .await?;
            if family == F::ClassSchema {
                sql_query("DELETE FROM public.class_schema_revisions WHERE class_id=$1")
                    .bind::<Integer, _>(class.id)
                    .execute(connection)
                    .await?;
            }
        }
        F::ClassRelation | F::ObjectRelation => {
            close_live(connection, collection, family).await?;
            insert_edge(connection, collection, family).await?;
            close_live(connection, collection, family).await?;
        }
        _ => {
            close_live(connection, collection, family).await?;
            close_live(connection, collection, family).await?;
        }
    }
    fact_on(connection, family, collection, false).await
}

async fn cleanup(
    a: Fixture,
    b: Fixture,
    collection_a: i32,
    collection_b: i32,
    runtime: RuntimeRegistration,
    admitted: Vec<Uuid>,
) {
    a.storage.unregister_runtime(runtime).await.unwrap();
    with_transaction(&a.owner_pool, async |connection| {
        owner(connection).await?;
        sql_query("DELETE FROM hubuum_control.history_expiry_members WHERE intent_uuid=ANY($1)")
            .bind::<Array<SqlUuid>, _>(admitted)
            .execute(connection)
            .await?;
        Ok::<_, PostgresStorageError>(())
    })
    .await
    .unwrap();
    cleanup_expiry_fixture(b, collection_b, Uuid::nil()).await;
    cleanup_expiry_fixture(a, collection_a, Uuid::nil()).await;
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
async fn finite_native_pass_revisits_held_prefix_after_release(
    #[case] family: StorageIdentityHistoryKind,
) {
    let _guard = CONTROL_LOCK.lock().await;
    let (a, key_a, collection_a) = expiry_fixture(family, 32).await;
    let (b, key_b, collection_b) = expiry_fixture(family, 32).await;
    let principal = PrincipalIdentity::generate();
    let hold = Uuid::new_v4();
    let snapshot = a.storage.capture_history_lifecycle().await.unwrap();
    a.storage
        .commit_history_lifecycle(
            snapshot
                .control()
                .create_hold(
                    HistoryRetentionHold::new(hold, key_a, snapshot.observed_at(), None, principal)
                        .unwrap(),
                    snapshot.fence(),
                )
                .unwrap(),
        )
        .await
        .unwrap();
    let runtime = a.storage.register_history_runtime(None).await.unwrap();
    let (outcome, admitted) = {
        let mut run = BoundedRun::new(&a, &runtime, family, 1).await.unwrap();
        let outcome = async {
            let first = run.pass().await?;
            let current = a.storage.capture_history_lifecycle().await?;
            a.storage
                .run_in_scope(
                    run.scope.clone(),
                    a.storage.commit_history_lifecycle(
                        current
                            .control()
                            .release_hold(hold, principal, current.fence(), current.observed_at())
                            .unwrap(),
                    ),
                )
                .await?;
            let second = run.pass().await?;
            Ok::<_, StorageError>((first, second))
        }
        .await;
        (outcome, run.admitted)
    };
    cleanup(a, b, collection_a, collection_b, runtime, admitted).await;
    let (first, second) = outcome.unwrap();
    assert!(
        !first.is_empty(),
        "eligible work behind the held prefix must be reached"
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
async fn finite_native_pass_revisits_late_commit(#[case] family: StorageIdentityHistoryKind) {
    let _guard = CONTROL_LOCK.lock().await;
    let (a, _, collection_a) = expiry_fixture(family, 32).await;
    schema_collection_policy(&a, family, collection_a).await;
    live_edges(&a, family, collection_a).await;
    // Graph mutations serialize through an advisory lock. For those families,
    // reserve a lower native position before the second fixture commits, then
    // assign it to the real trigger-created row in the pending transaction.
    // Only its traversal position changes: UUIDs, attribution and intervals
    // remain the native facts produced by the domain mutation.
    let reserved = if matches!(
        family,
        StorageIdentityHistoryKind::ClassRelation
            | StorageIdentityHistoryKind::ObjectRelation
            | StorageIdentityHistoryKind::ComputedDefinitions
            | StorageIdentityHistoryKind::ClassSchema
    ) {
        let (table, position) = source(family);
        let reserved = with_transaction(&a.owner_pool, async |connection| {
            owner(connection).await?;
            sql_query(format!(
                "SELECT nextval(pg_get_serial_sequence('public.{table}','{position}')) AS value"
            ))
            .get_result::<Count>(connection)
            .await
            .map_err(PostgresStorageError::from)
        })
        .await
        .unwrap()
        .value;
        Some((reserved, expiry_fixture(family, 32).await))
    } else {
        None
    };
    let mut writer = a.owner_pool.get().await.unwrap();
    writer.batch_execute("BEGIN").await.unwrap();
    owner(&mut writer).await.unwrap();
    let mut late = create_closed(&mut writer, collection_a, family)
        .await
        .unwrap();
    // Another transaction commits higher allocated history IDs first.
    let (b, _, collection_b) = if let Some((reserved, fixture)) = reserved {
        let (table, position) = source(family);
        sql_query(format!(
            "UPDATE public.{table} SET {position}=$1 WHERE {position}=$2"
        ))
        .bind::<BigInt, _>(reserved)
        .bind::<BigInt, _>(late.position)
        .execute(&mut writer)
        .await
        .unwrap();
        late.position = reserved;
        fixture
    } else {
        expiry_fixture(family, 32).await
    };
    let runtime = a.storage.register_history_runtime(None).await.unwrap();
    let mut committed = false;
    let (outcome, admitted) = {
        let mut run = BoundedRun::new(&a, &runtime, family, 1).await.unwrap();
        let outcome = async {
            let mut first = run.past(late.position).await?;
            writer
                .batch_execute("COMMIT")
                .await
                .map_err(PostgresStorageError::from)
                .map_err(StorageError::from)?;
            committed = true;
            first.extend(run.pass().await?);
            let second = run.pass().await?;
            Ok::<_, StorageError>((first, second))
        }
        .await;
        (outcome, run.admitted)
    };
    if !committed {
        writer.batch_execute("ROLLBACK").await.unwrap();
    }
    drop(writer);
    cleanup(a, b, collection_a, collection_b, runtime, admitted).await;
    let (first, second) = outcome.unwrap();
    assert!(!first.iter().any(|member| member.target() == late.target()));
    let selected = second
        .iter()
        .find(|member| member.target() == late.target())
        .expect("the next pass must find the late committed version");
    assert_eq!(
        (
            selected.read_span().valid_from(),
            selected.read_span().valid_to()
        ),
        (late.valid_from, late.valid_to)
    );
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
#[tokio::test]
async fn finite_native_pass_revisits_old_version_closing_later(
    #[case] family: StorageIdentityHistoryKind,
) {
    let _guard = CONTROL_LOCK.lock().await;
    let (a, _, collection_a) = expiry_fixture(family, 32).await;
    live_edges(&a, family, collection_a).await;
    let open = with_transaction(&a.owner_pool, async |connection| {
        owner(connection).await?;
        fact_on(connection, family, collection_a, true).await
    })
    .await
    .unwrap();
    let (b, _, collection_b) = expiry_fixture(family, 32).await;
    let runtime = a.storage.register_history_runtime(None).await.unwrap();
    let (outcome, admitted) = {
        let mut run = BoundedRun::new(&a, &runtime, family, 1).await.unwrap();
        let outcome = async {
            let mut first = run.past(open.position).await?;
            with_transaction(&a.owner_pool, async |connection| {
                owner(connection).await?;
                close_live(connection, collection_a, family).await
            })
            .await
            .map_err(StorageError::from)?;
            first.extend(run.pass().await?);
            let second = run.pass().await?;
            Ok::<_, StorageError>((first, second))
        }
        .await;
        (outcome, run.admitted)
    };
    cleanup(a, b, collection_a, collection_b, runtime, admitted).await;
    let (first, second) = outcome.unwrap();
    assert!(!first.iter().any(|member| member.target() == open.target()));
    let selected = second
        .iter()
        .find(|member| member.target() == open.target())
        .expect("the next pass must revisit the original open version");
    assert_eq!(selected.read_span().valid_from(), open.valid_from);
    assert!(selected.read_span().valid_to().is_some());
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
async fn finite_native_pass_keeps_its_boundary_when_rows_are_appended(
    #[case] family: StorageIdentityHistoryKind,
) {
    let _guard = CONTROL_LOCK.lock().await;
    let (a, _, collection_a) = expiry_fixture(family, 32).await;
    schema_collection_policy(&a, family, collection_a).await;
    live_edges(&a, family, collection_a).await;
    let (b, _, collection_b) = expiry_fixture(family, 32).await;
    let runtime = a.storage.register_history_runtime(None).await.unwrap();
    let (outcome, admitted) = {
        let mut run = BoundedRun::new(&a, &runtime, family, 1).await.unwrap();
        let outcome = async {
            let page = run.page().await?;
            let HistoryExpiryScanProgress::Continue(cursor) = page.progress() else {
                return Err(StorageError::backend_failure(
                    "Fixture needs multiple source pages",
                ));
            };
            let request = run.request.clone().continue_from(cursor.clone()).unwrap();
            let boundary = request.scan_window(0).unwrap().through();
            let added = with_transaction(&a.owner_pool, async |connection| {
                owner(connection).await?;
                create_closed(connection, collection_a, family).await
            })
            .await
            .map_err(StorageError::from)?;
            let first = run.pass().await?;
            let second = run.pass().await?;
            Ok::<_, StorageError>((boundary, added, first, second))
        }
        .await;
        (outcome, run.admitted)
    };
    cleanup(a, b, collection_a, collection_b, runtime, admitted).await;
    let (boundary, added, first, second) = outcome.unwrap();
    assert!(added.position as u64 > boundary);
    assert!(!first.iter().any(|member| member.target() == added.target()));
    assert!(
        second
            .iter()
            .any(|member| member.target() == added.target()),
        "the following pass must visit newly appended closed history"
    );
}

async fn payload_versions(
    fixture: &Fixture,
    collections: &[i32],
    width: i32,
) -> Vec<StorageRecordIdentity> {
    let material = with_transaction(&fixture.owner_pool, async |connection| {
        owner(connection).await?;
        sql_query("SELECT coalesce(jsonb_agg(logical_identity),'[]') AS material FROM public.hubuumobject_history WHERE collection_id=ANY($1) AND valid_to IS NOT NULL AND op<>'D' AND octet_length(description)>=$2")
            .bind::<Array<Integer>, _>(collections.to_vec())
            .bind::<Integer, _>(width)
            .get_result::<Material>(connection)
            .await
            .map_err(PostgresStorageError::from)
    })
    .await
    .unwrap()
    .material;
    material
        .as_array()
        .unwrap()
        .iter()
        .map(|identity| {
            serde_json::from_value::<StorageIdentityEvidence>(identity.clone())
                .unwrap()
                .subject()
                .unwrap()
                .record_identity()
                .unwrap()
        })
        .collect()
}

#[tokio::test]
async fn oversized_native_prefix_does_not_starve_another_scope() {
    let _guard = CONTROL_LOCK.lock().await;
    let family = StorageIdentityHistoryKind::Object;
    let (a, _, collection_a) = expiry_fixture(family, 17 * 1024 * 1024).await;
    let (b, key_b, collection_b) = expiry_fixture(family, 32).await;
    let oversized = payload_versions(&a, &[collection_a], 17 * 1024 * 1024).await;
    let runtime = a.storage.register_history_runtime(None).await.unwrap();
    let (outcome, admitted) = {
        let mut run = BoundedRun::new(&a, &runtime, family, 1).await.unwrap();
        let outcome = run.pass().await;
        (outcome, run.admitted)
    };
    cleanup(a, b, collection_a, collection_b, runtime, admitted).await;
    let members = outcome.unwrap();
    assert_eq!(
        oversized.len(),
        2,
        "fixture must include two oversized closed versions"
    );
    assert!(
        !members
            .iter()
            .any(|member| oversized.contains(&member.target())),
        "individually oversized versions must remain unadmitted"
    );
    assert!(members.iter().any(|member| {
        member
            .attributions()
            .iter()
            .any(|scope| scope.matches(key_b.scope()))
    }));
}

#[tokio::test]
async fn cumulative_native_byte_limit_defers_individually_bounded_rows() {
    let _guard = CONTROL_LOCK.lock().await;
    let family = StorageIdentityHistoryKind::Object;
    let width = 9 * 1024 * 1024;
    let (a, _, collection_a) = expiry_fixture(family, width).await;
    let (b, _, collection_b) = expiry_fixture(family, width).await;
    let expected = payload_versions(&a, &[collection_a, collection_b], width).await;
    let runtime = a.storage.register_history_runtime(None).await.unwrap();
    let (outcome, admitted) = {
        let mut run = BoundedRun::new(&a, &runtime, family, 1000).await.unwrap();
        let outcome = run.pass().await;
        (outcome, run.admitted)
    };
    cleanup(a, b, collection_a, collection_b, runtime, admitted).await;
    let members = outcome.unwrap();
    assert_eq!(
        expected.len(),
        4,
        "fixture must include four large closed versions"
    );
    for target in expected {
        assert!(
            members.iter().any(|member| member.target() == target),
            "every individually bounded version must survive a cumulative byte stop"
        );
    }
}
