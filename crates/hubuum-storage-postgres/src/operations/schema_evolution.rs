//! Native schema lifecycle transactions and bounded validation batches.

use diesel::{
    prelude::*,
    sql_types::{BigInt, Bool, Integer, Jsonb, Nullable, Text},
};
use diesel_async::RunQueryDsl;
use hubuum_domain::JsonSchemaLimits;
use hubuum_domain::{
    ClassId, CollectionId, ObjectId, PrincipalId, ResourceRevision, SchemaReference,
    SchemaRevision, TaskId,
};
use hubuum_events_core::{
    Action, AuditDocument, EntityType, EventContext, EventEntityId, MutationProvenance, NewEvent,
};
use hubuum_storage_core::{
    StorageAuditReceipt, StorageMutationOutcome, StorageTaskKind, StorageTaskLease,
    schema_evolution::*,
};
use serde_json::{Value, json};
use std::time::Instant;

use super::{
    class::ClassRow,
    computed_fields::{insert_internal_task, insert_restored_internal_task},
    event_record::append_event,
    object::ObjectRow,
    task_execution,
};
use crate::{PostgresConnection, PostgresRuntime, PostgresStorageError};

#[derive(QueryableByName)]
struct JsonRow {
    #[diesel(sql_type=Jsonb)]
    value: Value,
}
#[derive(QueryableByName)]
struct StateRow {
    #[diesel(sql_type=BigInt)]
    active_revision: i64,
    #[diesel(sql_type=BigInt)]
    object_epoch: i64,
    #[diesel(sql_type=BigInt)]
    object_count: i64,
}
#[derive(QueryableByName)]
struct Boundary {
    #[diesel(sql_type=Integer)]
    upper_bound: i32,
    #[diesel(sql_type=BigInt)]
    total: i64,
}
#[derive(QueryableByName)]
struct Snapshot {
    #[diesel(sql_type=Integer)]
    id: i32,
    #[diesel(sql_type=BigInt)]
    revision: i64,
    #[diesel(sql_type=Nullable<Jsonb>)]
    data: Option<Value>,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name=crate::schema::hubuumobject)]
struct ValidationObject {
    id: i32,
    name: String,
    collection_id: i32,
    hubuum_class_id: i32,
    revision: crate::PostgresRevision,
}
impl From<&ObjectRow> for ValidationObject {
    fn from(object: &ObjectRow) -> Self {
        Self {
            id: object.id,
            name: object.name.clone(),
            collection_id: object.collection_id,
            hubuum_class_id: object.hubuum_class_id,
            revision: object.revision,
        }
    }
}

fn invalid(error: impl std::fmt::Debug) -> PostgresStorageError {
    PostgresStorageError::invalid_persisted_value("schema lifecycle", error)
}
fn policy_name(policy: StorageSchemaActivationPolicy) -> &'static str {
    match policy {
        StorageSchemaActivationPolicy::RejectIncompatible => "reject_incompatible",
        StorageSchemaActivationPolicy::AllowPending => "allow_pending",
    }
}
fn work_name(kind: StorageSchemaWorkKind) -> &'static str {
    match kind {
        StorageSchemaWorkKind::Impact => "impact",
        StorageSchemaWorkKind::Revalidation => "revalidation",
    }
}

async fn lock_class(
    connection: &mut PostgresConnection,
    class_id: ClassId,
) -> Result<ClassRow, PostgresStorageError> {
    use crate::schema::hubuumclass::dsl as classes;
    classes::hubuumclass
        .filter(classes::id.eq(class_id.id()))
        .for_update()
        .select(ClassRow::as_select())
        .first(connection)
        .await
        .map_err(PostgresStorageError::from)
}
fn check_collection(class: &ClassRow, expected: CollectionId) -> Result<(), PostgresStorageError> {
    if class.collection_id != expected.id() {
        return Err(PostgresStorageError::not_found(
            "Class was not found in the authorized collection",
        ));
    }
    Ok(())
}
async fn state_on(
    connection: &mut PostgresConnection,
    class_id: ClassId,
) -> Result<StateRow, PostgresStorageError> {
    diesel::sql_query("SELECT active_revision, object_epoch, object_count FROM class_schema_state WHERE class_id=$1").bind::<Integer,_>(class_id.id()).get_result(connection).await.map_err(PostgresStorageError::from)
}
async fn revision_on(
    schema_limits: JsonSchemaLimits,
    connection: &mut PostgresConnection,
    target: SchemaReference,
) -> Result<StorageSchemaRevision, PostgresStorageError> {
    let row = diesel::sql_query("SELECT to_jsonb(r) AS value FROM class_schema_revisions r WHERE class_id=$1 AND revision=$2").bind::<Integer,_>(target.class_id().id()).bind::<BigInt,_>(target.revision().get()).get_result::<JsonRow>(connection).await?;
    StorageSchemaRevision::from_snapshot_with_limits(row.value, schema_limits).map_err(invalid)
}
async fn active_on(
    schema_limits: JsonSchemaLimits,
    connection: &mut PostgresConnection,
    class_id: ClassId,
) -> Result<StorageSchemaRevision, PostgresStorageError> {
    let row = diesel::sql_query("SELECT to_jsonb(r) AS value FROM class_schema_revisions r JOIN class_schema_state s ON s.class_id=r.class_id AND s.active_revision=r.revision WHERE s.class_id=$1")
        .bind::<Integer, _>(class_id.id()).get_result::<JsonRow>(connection).await?;
    StorageSchemaRevision::from_snapshot_with_limits(row.value, schema_limits).map_err(invalid)
}

pub(crate) async fn schema_event_on(
    connection: &mut PostgresConnection,
    class: &ClassRow,
    action: Action,
    context: &EventContext,
    metadata: Value,
) -> Result<StorageAuditReceipt, PostgresStorageError> {
    let snapshot = json!({"id":class.id,"revision":class.revision,"schema":metadata});
    let document = AuditDocument::try_new(
        "Class schema lifecycle changed",
        (action != Action::Created).then(|| snapshot.clone()),
        (action != Action::Deleted).then_some(snapshot),
        metadata,
    )?;
    let event = NewEvent::from_document(
        EntityType::ClassSchema,
        action,
        context.actor_kind(),
        document,
    )
    .map_err(invalid)?
    .with_context(context)
    .with_entity_id(EventEntityId::new(class.id)?)
    .with_entity_name(&class.name)
    .with_collection_id(CollectionId::new(class.collection_id)?);
    Ok(append_event(connection, &event).await?.into_audit_receipt())
}

pub(crate) async fn record_class_schema_on(
    schema_limits: JsonSchemaLimits,
    connection: &mut PostgresConnection,
    class: &ClassRow,
    context: &EventContext,
) -> Result<(), PostgresStorageError> {
    let active = active_on(schema_limits, connection, ClassId::new(class.id)?).await?;
    let dependent_task = if active.reference().revision() > SchemaRevision::INITIAL {
        super::computed_fields::invalidate_schema_dependency_on(
            connection,
            active.reference(),
            context.actor_user_id().map(PrincipalId::id),
        )
        .await?
    } else {
        None
    };
    schema_event_on(connection,class,Action::Created,context,json!({"schema":active.reference(),"status":"active","activation_policy":"reject_incompatible","dependent_rebuild_task_id":dependent_task})).await?;
    Ok(())
}

pub async fn list_schema_revisions(
    runtime: &PostgresRuntime,
    query: StorageSchemaPage,
) -> Result<Vec<StorageSchemaRevision>, PostgresStorageError> {
    let schema_limits = runtime.schema_limits();
    runtime.with_read_connection(async move |connection| {
        state_on(connection,query.class_id()).await?;
        let rows = diesel::sql_query("SELECT to_jsonb(r) AS value FROM class_schema_revisions r WHERE class_id=$1 AND revision>$2 ORDER BY revision LIMIT $3")
            .bind::<Integer,_>(query.class_id().id()).bind::<BigInt,_>(query.after()).bind::<BigInt,_>(query.limit() as i64).load::<JsonRow>(connection).await?;
        rows.into_iter().map(|row|StorageSchemaRevision::from_snapshot_with_limits(row.value, schema_limits).map_err(invalid)).collect()
    }).await
}

async fn class_state_on(
    schema_limits: JsonSchemaLimits,
    connection: &mut PostgresConnection,
    class_id: ClassId,
) -> Result<StorageClassSchemaState, PostgresStorageError> {
    let row=diesel::sql_query(r#"
        SELECT jsonb_build_object('active',to_jsonb(r),'object_epoch',s.object_epoch,'counts',(
            SELECT jsonb_build_object(
                'valid',count(o.id) FILTER(WHERE r.validate_schema AND e.schema_revision=r.revision AND e.object_revision=o.revision AND e.valid),
                'invalid',count(o.id) FILTER(WHERE r.validate_schema AND e.schema_revision=r.revision AND e.object_revision=o.revision AND NOT e.valid),
                'pending',count(o.id) FILTER(WHERE r.validate_schema AND (e.object_id IS NULL OR e.schema_revision<>r.revision OR e.object_revision<>o.revision)),
                'not_required',count(o.id) FILTER(WHERE NOT r.validate_schema))
            FROM hubuumobject o LEFT JOIN object_schema_evidence e ON e.object_id=o.id AND e.class_id=o.hubuum_class_id
            WHERE o.hubuum_class_id=s.class_id)) AS value
        FROM class_schema_state s JOIN class_schema_revisions r ON r.class_id=s.class_id AND r.revision=s.active_revision
        WHERE s.class_id=$1
    "#).bind::<Integer,_>(class_id.id()).get_result::<JsonRow>(connection).await?;
    let active = StorageSchemaRevision::from_snapshot_with_limits(
        row.value["active"].clone(),
        schema_limits,
    )
    .map_err(invalid)?;
    let counts = serde_json::from_value(row.value["counts"].clone()).map_err(invalid)?;
    let epoch = row.value["object_epoch"]
        .as_u64()
        .ok_or_else(|| invalid("object epoch"))?;
    Ok(StorageClassSchemaState::new(active, counts, epoch))
}
pub async fn get_schema_state(
    runtime: &PostgresRuntime,
    class_id: ClassId,
) -> Result<StorageClassSchemaState, PostgresStorageError> {
    let schema_limits = runtime.schema_limits();
    runtime
        .with_read_connection(async move |connection| {
            class_state_on(schema_limits, connection, class_id).await
        })
        .await
}

pub async fn stage_schema_revision(
    runtime: &PostgresRuntime,
    request: StorageSchemaStage,
) -> Result<StorageMutationOutcome<StorageSchemaRevision>, PostgresStorageError> {
    let schema_limits = runtime.schema_limits();
    request
        .policy()
        .ensure_limits(runtime.schema_limits())
        .map_err(|error| PostgresStorageError::invalid_input(error.to_string()))?;
    runtime.with_transaction(async move |connection| {
        let class=lock_class(connection,request.class_id()).await?; check_collection(&class,request.authorized_collection())?;
        let policy=request.policy().policy();
        let existing=diesel::sql_query("SELECT to_jsonb(r) AS value FROM class_schema_revisions r WHERE class_id=$1 AND status IN ('staged','active') AND json_schema IS NOT DISTINCT FROM $2 AND validate_schema=$3 ORDER BY revision DESC LIMIT 1")
            .bind::<Integer,_>(class.id).bind::<Nullable<Jsonb>,_>(policy.json_schema()).bind::<Bool,_>(policy.validates_schema()).get_result::<JsonRow>(connection).await.optional()?;
        if let Some(row)=existing{return Ok::<_,PostgresStorageError>(StorageMutationOutcome::unchanged(StorageSchemaRevision::from_snapshot_with_limits(row.value, schema_limits).map_err(invalid)?));}
        let row=diesel::sql_query("WITH allocated AS (UPDATE class_schema_state SET last_revision=last_revision+1 WHERE class_id=$1 RETURNING last_revision), inserted AS (INSERT INTO class_schema_revisions(class_id,revision,json_schema,validate_schema,status,created_by) SELECT $1,last_revision,$2,$3,'staged',$4 FROM allocated RETURNING *) SELECT to_jsonb(inserted) AS value FROM inserted")
            .bind::<Integer,_>(class.id).bind::<Nullable<Jsonb>,_>(policy.json_schema()).bind::<Bool,_>(policy.validates_schema()).bind::<Nullable<Integer>,_>(request.context().actor_user_id().map(PrincipalId::id)).get_result::<JsonRow>(connection).await?;
        let revision=StorageSchemaRevision::from_snapshot_with_limits(row.value, schema_limits).map_err(invalid)?;
        let receipt=schema_event_on(connection,&class,Action::Created,request.context(),json!({"schema":revision.reference(),"status":"staged"})).await?;
        Ok::<_,PostgresStorageError>(StorageMutationOutcome::committed(revision,receipt))
    }).await
}

pub async fn abandon_schema_revision(
    runtime: &PostgresRuntime,
    target: SchemaReference,
    authorized_collection: CollectionId,
    context: &EventContext,
) -> Result<StorageMutationOutcome<StorageSchemaRevision>, PostgresStorageError> {
    let schema_limits = runtime.schema_limits();
    let context = context.clone();
    runtime.with_transaction(async move |connection|{
        let class=lock_class(connection,target.class_id()).await?; check_collection(&class,authorized_collection)?;let revision=revision_on(schema_limits, connection,target).await?;
        if revision.status()==StorageSchemaRevisionStatus::Abandoned{return Ok::<_,PostgresStorageError>(StorageMutationOutcome::unchanged(revision));}
        if revision.status()!=StorageSchemaRevisionStatus::Staged{return Err(PostgresStorageError::conflict("Only staged schema revisions may be abandoned"));}
        diesel::sql_query("UPDATE class_schema_revisions SET status='abandoned' WHERE class_id=$1 AND revision=$2").bind::<Integer,_>(class.id).bind::<BigInt,_>(target.revision().get()).execute(connection).await?;
        let revision=revision_on(schema_limits, connection,target).await?;
        let receipt=schema_event_on(connection,&class,Action::Updated,&context,json!({"schema":target,"status":"abandoned"})).await?;
        Ok::<_,PostgresStorageError>(StorageMutationOutcome::committed(revision,receipt))
    }).await
}

async fn work_on(
    connection: &mut PostgresConnection,
    task_id: TaskId,
) -> Result<StorageSchemaWork, PostgresStorageError> {
    let row = diesel::sql_query(
        "SELECT checkpoint AS value FROM schema_validation_work WHERE task_id=$1",
    )
    .bind::<Integer, _>(task_id.id())
    .get_result::<JsonRow>(connection)
    .await?;
    serde_json::from_value(row.value).map_err(invalid)
}
pub(super) async fn mark_schema_work_failed_on(
    connection: &mut PostgresConnection,
    task_id: TaskId,
) -> Result<(), PostgresStorageError> {
    let row = diesel::sql_query(
        "SELECT checkpoint AS value FROM schema_validation_work WHERE task_id=$1 FOR UPDATE",
    )
    .bind::<Integer, _>(task_id.id())
    .get_result::<JsonRow>(connection)
    .await
    .optional()?;
    if let Some(row) = row {
        let mut work: StorageSchemaWork = serde_json::from_value(row.value).map_err(invalid)?;
        let epoch = state_on(connection, work.target().class_id())
            .await?
            .object_epoch;
        work.finish(
            StorageSchemaWorkStatus::Failed,
            u64::try_from(epoch).map_err(invalid)?,
        );
        save_work_on(connection, &work).await?;
    }
    Ok(())
}

async fn save_work_on(
    connection: &mut PostgresConnection,
    work: &StorageSchemaWork,
) -> Result<(), PostgresStorageError> {
    diesel::sql_query(
        "UPDATE schema_validation_work SET checkpoint=$2, active=$3 WHERE task_id=$1",
    )
    .bind::<Integer, _>(work.task_id().id())
    .bind::<Jsonb, _>(serde_json::to_value(work).map_err(invalid)?)
    .bind::<Bool, _>(work.status() == StorageSchemaWorkStatus::Running)
    .execute(connection)
    .await?;
    Ok(())
}
async fn enqueue_on(
    connection: &mut PostgresConnection,
    request: &StorageSchemaWorkRequest,
) -> Result<(StorageSchemaWork, bool), PostgresStorageError> {
    enqueue_with_event_on(connection, request, true).await
}

async fn enqueue_with_event_on(
    connection: &mut PostgresConnection,
    request: &StorageSchemaWorkRequest,
    record_queue_event: bool,
) -> Result<(StorageSchemaWork, bool), PostgresStorageError> {
    let existing=diesel::sql_query("SELECT w.checkpoint AS value FROM schema_validation_work w JOIN tasks t ON t.id=w.task_id WHERE w.class_id=$1 AND w.schema_revision=$2 AND w.kind=$3 AND w.active AND t.status IN ('queued','validating','running') LIMIT 1")
        .bind::<Integer,_>(request.target().class_id().id()).bind::<BigInt,_>(request.target().revision().get()).bind::<Text,_>(work_name(request.kind())).get_result::<JsonRow>(connection).await.optional()?;
    if let Some(row) = existing {
        return Ok((serde_json::from_value(row.value).map_err(invalid)?, false));
    }
    diesel::sql_query("UPDATE schema_validation_work w SET active=false WHERE class_id=$1 AND schema_revision=$2 AND kind=$3 AND active AND EXISTS (SELECT 1 FROM tasks t WHERE t.id=w.task_id AND t.status NOT IN ('queued','validating','running'))")
        .bind::<Integer,_>(request.target().class_id().id()).bind::<BigInt,_>(request.target().revision().get()).bind::<Text,_>(work_name(request.kind())).execute(connection).await?;
    let boundary=diesel::sql_query("SELECT COALESCE((SELECT id FROM hubuumobject WHERE hubuum_class_id=$1 ORDER BY id DESC LIMIT 1),0)::int AS upper_bound, object_count AS total FROM class_schema_state WHERE class_id=$1").bind::<Integer,_>(request.target().class_id().id()).get_result::<Boundary>(connection).await?;
    let state = state_on(connection, request.target().class_id()).await?;
    let payload = json!({"class_id":request.target().class_id(),"schema_revision":request.target().revision(),"kind":request.kind()});
    let total = i32::try_from(boundary.total).unwrap_or(i32::MAX);
    let task = if record_queue_event {
        insert_internal_task(
            connection,
            StorageTaskKind::SchemaValidation,
            payload,
            total,
            request
                .context()
                .actor_user_id()
                .or(request.context().initiator_user_id())
                .map(PrincipalId::id),
        )
        .await?
    } else {
        insert_restored_internal_task(
            connection,
            StorageTaskKind::SchemaValidation,
            payload,
            total,
        )
        .await?
    };
    let work = StorageSchemaWork::start(
        TaskId::new(task.id)?,
        request,
        u64::try_from(state.object_epoch).map_err(invalid)?,
        boundary.upper_bound,
    );
    diesel::sql_query("INSERT INTO schema_validation_work(task_id,class_id,schema_revision,kind,checkpoint) VALUES ($1,$2,$3,$4,$5)").bind::<Integer,_>(task.id).bind::<Integer,_>(request.target().class_id().id()).bind::<BigInt,_>(request.target().revision().get()).bind::<Text,_>(work_name(request.kind())).bind::<Jsonb,_>(serde_json::to_value(&work).map_err(invalid)?).execute(connection).await?;
    Ok((work, true))
}

pub async fn request_schema_work(
    runtime: &PostgresRuntime,
    request: StorageSchemaWorkRequest,
) -> Result<StorageMutationOutcome<StorageSchemaWork>, PostgresStorageError> {
    let schema_limits = runtime.schema_limits();
    runtime.with_transaction(async move |connection|{
        let class=lock_class(connection,request.target().class_id()).await?; check_collection(&class,request.authorized_collection())?;revision_on(schema_limits, connection,request.target()).await?;
        if request.kind()==StorageSchemaWorkKind::Revalidation && active_on(schema_limits, connection,request.target().class_id()).await?.reference()!=request.target(){return Err(PostgresStorageError::conflict("Revalidation must target the active schema"));}
        let (work,created)=enqueue_on(connection,&request).await?;if !created{return Ok::<_,PostgresStorageError>(StorageMutationOutcome::unchanged(work));}
        let receipt=schema_event_on(connection,&class,Action::Updated,request.context(),json!({"schema":request.target(),"task_id":work.task_id(),"work_kind":request.kind()})).await?;
        Ok::<_,PostgresStorageError>(StorageMutationOutcome::committed(work,receipt))
    }).await
}
pub async fn get_schema_work(
    runtime: &PostgresRuntime,
    task_id: TaskId,
) -> Result<StorageSchemaWork, PostgresStorageError> {
    runtime
        .with_read_connection(async move |connection| work_on(connection, task_id).await)
        .await
}

pub async fn activate_schema_revision(
    runtime: &PostgresRuntime,
    request: StorageSchemaActivation,
) -> Result<StorageMutationOutcome<StorageSchemaActivationResult>, PostgresStorageError> {
    let schema_limits = runtime.schema_limits();
    runtime
        .with_transaction(async move |connection| {
            activate_schema_revision_on(schema_limits, connection, request).await
        })
        .await
}

pub(crate) async fn activate_schema_revision_on(
    schema_limits: JsonSchemaLimits,
    connection: &mut PostgresConnection,
    request: StorageSchemaActivation,
) -> Result<StorageMutationOutcome<StorageSchemaActivationResult>, PostgresStorageError> {
    super::computed_materialization::acquire_computed_class_exclusive_lock(
        connection,
        request.target().class_id().id(),
    )
    .await?;
    let class = lock_class(connection, request.target().class_id()).await?;
    check_collection(&class, request.authorized_collection())?;
    // Fence native writers too: their object trigger advances this row even
    // when they do not participate in the application class-lock protocol.
    let state: StateRow = diesel::sql_query("SELECT active_revision, object_epoch, object_count FROM class_schema_state WHERE class_id=$1 FOR UPDATE")
        .bind::<Integer, _>(request.target().class_id().id()).get_result(connection).await?;
    if state.active_revision != request.expected_active().get() {
        return Err(PostgresStorageError::conflict(
            "Active schema revision changed",
        ));
    }
    if state.active_revision == request.target().revision().get() {
        return Ok::<_, PostgresStorageError>(StorageMutationOutcome::unchanged(
            StorageSchemaActivationResult::new(
                active_on(schema_limits, connection, request.target().class_id()).await?,
                None,
            ),
        ));
    }
    let target = revision_on(schema_limits, connection, request.target()).await?;
    if target.status() != StorageSchemaRevisionStatus::Staged
        || target.reference().revision().get() <= state.active_revision
    {
        return Err(PostgresStorageError::conflict(
            "Activation requires a later staged schema revision",
        ));
    }
    if request.policy() == StorageSchemaActivationPolicy::RejectIncompatible
        && state.object_count > 0
    {
        let proof = match request.proof_task() {
            Some(task) => Some(work_on(connection, task).await?),
            None => None,
        };
        if !proof
            .is_some_and(|work| work.proves_compatible(request.target(), state.object_epoch as u64))
        {
            return Err(PostgresStorageError::conflict(
                "A current, completed compatible impact analysis is required",
            ));
        }
    }
    diesel::sql_query(
        "UPDATE class_schema_revisions SET status='retired' WHERE class_id=$1 AND status='active'",
    )
    .bind::<Integer, _>(class.id)
    .execute(connection)
    .await?;
    diesel::sql_query("UPDATE class_schema_revisions SET status='active',activated_at=clock_timestamp(),activation_policy=$3 WHERE class_id=$1 AND revision=$2").bind::<Integer,_>(class.id).bind::<BigInt,_>(request.target().revision().get()).bind::<Text,_>(policy_name(request.policy())).execute(connection).await?;
    diesel::sql_query("UPDATE class_schema_state SET active_revision=$2 WHERE class_id=$1")
        .bind::<Integer, _>(class.id)
        .bind::<BigInt, _>(request.target().revision().get())
        .execute(connection)
        .await?;
    use crate::schema::hubuumclass::dsl as classes;
    let updated = diesel::update(classes::hubuumclass.filter(classes::id.eq(class.id)))
        .set((
            classes::json_schema.eq(target.policy().policy().json_schema()),
            classes::validate_schema.eq(target.policy().policy().validates_schema()),
        ))
        .returning(ClassRow::as_returning())
        .get_result::<ClassRow>(connection)
        .await?;
    let (work, _) = enqueue_on(
        connection,
        &StorageSchemaWorkRequest::new(
            request.authorized_collection(),
            request.target(),
            StorageSchemaWorkKind::Revalidation,
            request.context().clone(),
        ),
    )
    .await?;
    let dependent_task = super::computed_fields::invalidate_schema_dependency_on(
        connection,
        request.target(),
        request.context().actor_user_id().map(PrincipalId::id),
    )
    .await?;
    let receipt=schema_event_on(connection,&updated,Action::Updated,request.context(),json!({"before_schema_revision":state.active_revision,"dependent_rebuild_task_id":dependent_task,"schema":request.target(),"activation_policy":request.policy(),"object_effect":if target.policy().policy().validates_schema(){"pending"}else{"not_required"},"task_id":work.task_id()})).await?;
    Ok::<_, PostgresStorageError>(StorageMutationOutcome::committed(
        StorageSchemaActivationResult::new(
            active_on(schema_limits, connection, request.target().class_id()).await?,
            Some(work.task_id()),
        )
        .with_dependent_rebuild(dependent_task),
        receipt,
    ))
}

async fn evidence_on(
    connection: &mut PostgresConnection,
    object_id: ObjectId,
) -> Result<Option<StorageSchemaEvidence>, PostgresStorageError> {
    let row=diesel::sql_query("SELECT jsonb_build_object('schema',jsonb_build_object('class_id',class_id,'revision',schema_revision),'object_revision',object_revision,'valid',valid,'validated_at',validated_at) AS value FROM object_schema_evidence WHERE object_id=$1").bind::<Integer,_>(object_id.id()).get_result::<JsonRow>(connection).await.optional()?;
    row.map(|row| serde_json::from_value(row.value).map_err(invalid))
        .transpose()
}

pub(crate) async fn record_object_schema_on(
    schema_limits: JsonSchemaLimits,
    connection: &mut PostgresConnection,
    object: &ObjectRow,
    context: &EventContext,
) -> Result<(), PostgresStorageError> {
    let active = active_on(
        schema_limits,
        connection,
        ClassId::new(object.hubuum_class_id)?,
    )
    .await?;
    let status = active.policy().inspect(&object.data);
    store_result_on(
        connection,
        &ValidationObject::from(object),
        &active,
        status,
        context,
    )
    .await
}

async fn store_result_on(
    connection: &mut PostgresConnection,
    object: &ValidationObject,
    active: &StorageSchemaRevision,
    status: StorageComplianceStatus,
    context: &EventContext,
) -> Result<(), PostgresStorageError> {
    let object_id = ObjectId::new(object.id)?;
    let revision = ResourceRevision::new(object.revision.get()).map_err(invalid)?;
    let previous = evidence_on(connection, object_id).await?;
    let before = StorageSchemaEvidence::effective_status(previous.as_ref(), active, revision);
    if previous.as_ref().is_some_and(|evidence| {
        evidence.schema() == active.reference() && evidence.object_revision() == revision
    }) && before == status
    {
        return Ok(());
    }
    if matches!(
        status,
        StorageComplianceStatus::Valid | StorageComplianceStatus::Invalid
    ) {
        diesel::sql_query("INSERT INTO object_schema_evidence(object_id,class_id,schema_revision,object_revision,valid,validated_at) VALUES ($1,$2,$3,$4,$5,clock_timestamp()) ON CONFLICT(object_id) DO UPDATE SET class_id=EXCLUDED.class_id,schema_revision=EXCLUDED.schema_revision,object_revision=EXCLUDED.object_revision,valid=EXCLUDED.valid,validated_at=EXCLUDED.validated_at")
            .bind::<Integer,_>(object.id).bind::<Integer,_>(object.hubuum_class_id).bind::<BigInt,_>(active.reference().revision().get()).bind::<BigInt,_>(revision.get()).bind::<Bool,_>(status==StorageComplianceStatus::Valid).execute(connection).await?;
    } else {
        diesel::sql_query("DELETE FROM object_schema_evidence WHERE object_id=$1")
            .bind::<Integer, _>(object.id)
            .execute(connection)
            .await?;
    }
    if previous.is_none()
        && status == StorageComplianceStatus::NotRequired
        && context.task_id().is_none()
    {
        return Ok(());
    }
    let before_snapshot =
        json!({"id":object.id,"revision":revision,"validation_status":before,"evidence":previous});
    let after_snapshot = json!({"id":object.id,"revision":revision,"schema":active.reference(),"validation_status":status});
    let metadata = json!({"schema":active.reference(),"object_revision":revision,"validation_status":status,"category":if status==StorageComplianceStatus::Invalid{Some("schema_mismatch")}else{None}});
    let action = match status {
        StorageComplianceStatus::Invalid => Action::Failed,
        StorageComplianceStatus::Valid => Action::Succeeded,
        _ => Action::Updated,
    };
    let document = AuditDocument::try_new(
        "Object schema compliance changed",
        Some(before_snapshot),
        Some(after_snapshot),
        metadata,
    )?;
    let event = NewEvent::from_document(
        EntityType::ObjectValidation,
        action,
        context.actor_kind(),
        document,
    )
    .map_err(invalid)?
    .with_context(context)
    .with_entity_id(EventEntityId::new(object.id)?)
    .with_entity_name(&object.name)
    .with_collection_id(CollectionId::new(object.collection_id)?);
    append_event(connection, &event).await?;
    Ok(())
}

async fn finish_work_on(
    connection: &mut PostgresConnection,
    work: &mut StorageSchemaWork,
    status: StorageSchemaWorkStatus,
    context: &EventContext,
) -> Result<(), PostgresStorageError> {
    let epoch = state_on(connection, work.target().class_id())
        .await?
        .object_epoch;
    work.finish(status, epoch as u64);
    save_work_on(connection, work).await?;
    let task_status = if status == StorageSchemaWorkStatus::Complete {
        "succeeded"
    } else {
        "cancelled"
    };
    diesel::sql_query("UPDATE tasks SET status=$2,finished_at=clock_timestamp(),updated_at=clock_timestamp(),lease_token=NULL,lease_expires_at=NULL,request_payload=NULL,request_redacted_at=clock_timestamp(),summary=$3 WHERE id=$1")
        .bind::<Integer,_>(work.task_id().id()).bind::<Text,_>(task_status).bind::<Text,_>("Schema validation finished").execute(connection).await?;
    let action = if status == StorageSchemaWorkStatus::Complete {
        Action::Succeeded
    } else {
        Action::Cancelled
    };
    let document = AuditDocument::try_new(
        "Schema validation finished",
        None,
        None,
        json!({"task_id":work.task_id(),"task_kind":"schema_validation","schema":work.target(),"work_status":status,"examined":work.examined()}),
    )?;
    let event = NewEvent::from_document(EntityType::Task, action, context.actor_kind(), document)
        .map_err(invalid)?
        .with_context(context)
        .with_entity_id(EventEntityId::new(work.task_id().id())?);
    append_event(connection, &event).await?;
    Ok(())
}

pub async fn process_schema_work(
    runtime: &PostgresRuntime,
    lease: StorageTaskLease,
    limits: StorageSchemaBatchLimits,
) -> Result<StorageSchemaWork, PostgresStorageError> {
    let schema_limits = runtime.schema_limits();
    let started = Instant::now();
    let claimed = task_execution::claimed_task(&lease)?;
    let (mut work,revision,snapshots,context)=runtime.with_transaction(async move |connection|{
        let task=task_execution::live_claimed_task(connection,claimed).await?;
        if task.kind!=StorageTaskKind::SchemaValidation.as_str(){return Err(PostgresStorageError::invalid_input("Task is not schema validation"));}
        let work=work_on(connection,TaskId::new(claimed.id)?).await?;
        let revision=revision_on(schema_limits, connection,work.target()).await?;
        // Read only one bounded page. Oversized JSON is never transferred to the worker.
        let snapshots=diesel::sql_query("WITH page AS MATERIALIZED (SELECT id,revision,data,octet_length(data::text) AS bytes FROM hubuumobject WHERE hubuum_class_id=$1 AND id>$2 AND id<=$3 ORDER BY id LIMIT $4), budgeted AS (SELECT *,sum(CASE WHEN bytes<=$5 THEN bytes ELSE 0 END) OVER (ORDER BY id) AS cumulative FROM page) SELECT id,revision,CASE WHEN bytes<=$5 THEN data ELSE NULL END AS data FROM budgeted WHERE cumulative<=$6 ORDER BY id")
            .bind::<Integer,_>(work.target().class_id().id()).bind::<Integer,_>(work.cursor()).bind::<Integer,_>(work.upper_bound()).bind::<BigInt,_>(limits.rows() as i64).bind::<BigInt,_>(limits.object_bytes() as i64).bind::<BigInt,_>(limits.bytes() as i64).load::<Snapshot>(connection).await?;
        let context=EventContext::from_mutation(MutationProvenance::worker(task.initiator_user_id.map(PrincipalId::new).transpose()?,TaskId::new(task.id)?));
        Ok((work,revision,snapshots,context))
    }).await?;
    let results = snapshots
        .into_iter()
        .map(|snapshot| {
            let status = snapshot
                .data
                .as_ref()
                .map(|value| revision.policy().inspect(value));
            (snapshot.id, snapshot.revision, status)
        })
        .collect::<Vec<_>>();
    runtime.with_transaction(async move |connection|{
        lock_class(connection,work.target().class_id()).await?;
        task_execution::live_claimed_task(connection,claimed).await?;
        let persisted=work_on(connection,work.task_id()).await?;
        task_execution::live_claimed_task(connection,claimed).await?;
        if persisted.cursor()!=work.cursor() || persisted.status()!=StorageSchemaWorkStatus::Running{return Err(PostgresStorageError::conflict("Schema work checkpoint changed"));}
        if work.kind()==StorageSchemaWorkKind::Revalidation && active_on(schema_limits, connection,work.target().class_id()).await?.reference()!=work.target(){finish_work_on(connection,&mut work,StorageSchemaWorkStatus::Superseded,&context).await?;}
        else if results.is_empty(){finish_work_on(connection,&mut work,StorageSchemaWorkStatus::Complete,&context).await?;}
        else{
            use crate::schema::hubuumobject::dsl as objects;
            for (id,resource_revision,status) in results{
                let object=objects::hubuumobject.filter(objects::id.eq(id)).filter(objects::hubuum_class_id.eq(work.target().class_id().id())).filter(objects::revision.eq(resource_revision)).for_update().select(ValidationObject::as_select()).first::<ValidationObject>(connection).await.optional()?;
                let stale=object.is_none();
                if let Some(object)=object {
                    if work.kind()==StorageSchemaWorkKind::Revalidation && let Some(status)=status {store_result_on(connection,&object,&revision,status,&context).await?;}
                    else if status==Some(StorageComplianceStatus::Invalid) {impact_mismatch_on(connection,&object,&revision,&context).await?;}
                }
                work.record(ObjectId::new(id)?,status,stale);
            }
            task_execution::live_claimed_task(connection,claimed).await?;
            work.batch_committed(u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX));
            save_work_on(connection,&work).await?;
            let processed=i32::try_from(work.examined()).unwrap_or(i32::MAX);let failed=i32::try_from(work.invalid()+work.uninspectable()).unwrap_or(processed).min(processed);
            diesel::sql_query("UPDATE tasks SET processed_items=$2,success_items=$2-$3,failed_items=$3,total_items=GREATEST(total_items,$2),updated_at=clock_timestamp() WHERE id=$1").bind::<Integer,_>(claimed.id).bind::<Integer,_>(processed).bind::<Integer,_>(failed).execute(connection).await?;
        }
        Ok(work)
    }).await
}

pub async fn cancel_schema_work(
    runtime: &PostgresRuntime,
    task_id: TaskId,
    authorized_collection: CollectionId,
    context: &EventContext,
) -> Result<StorageMutationOutcome<StorageSchemaWork>, PostgresStorageError> {
    let context = context.clone();
    runtime
        .with_transaction(async move |connection| {
            let work = work_on(connection, task_id).await?;
            let class = lock_class(connection, work.target().class_id()).await?;
            check_collection(&class, authorized_collection)?;
            use crate::schema::tasks::dsl as tasks;
            tasks::tasks
                .filter(tasks::id.eq(task_id.id()))
                .for_update()
                .select(tasks::id)
                .first::<i32>(connection)
                .await?;

            let mut work = work_on(connection, task_id).await?;
            if work.status() != StorageSchemaWorkStatus::Running {
                return Ok::<_, PostgresStorageError>(StorageMutationOutcome::unchanged(work));
            }
            finish_work_on(
                connection,
                &mut work,
                StorageSchemaWorkStatus::Cancelled,
                &context,
            )
            .await?;
            let receipt = schema_event_on(
                connection,
                &class,
                Action::Updated,
                &context,
                json!({"schema":work.target(),"task_id":task_id,"work_status":"cancelled"}),
            )
            .await?;
            Ok::<_, PostgresStorageError>(StorageMutationOutcome::committed(work, receipt))
        })
        .await
}

pub async fn list_schema_compliance(
    runtime: &PostgresRuntime,
    query: StorageSchemaPage,
    status: Option<StorageComplianceStatus>,
) -> Result<Vec<StorageObjectCompliance>, PostgresStorageError> {
    let schema_limits = runtime.schema_limits();
    runtime.with_read_connection(async move |connection|{
        let row=diesel::sql_query(r#"
            SELECT jsonb_build_object('active',to_jsonb(r),'objects',(
                SELECT COALESCE(jsonb_agg(page.value ORDER BY page.id),'[]'::jsonb) FROM (
                    SELECT o.id,jsonb_build_object('id',o.id,'revision',o.revision,'evidence',
                        CASE WHEN e.object_id IS NULL THEN NULL ELSE jsonb_build_object(
                            'schema',jsonb_build_object('class_id',e.class_id,'revision',e.schema_revision),
                            'object_revision',e.object_revision,'valid',e.valid,'validated_at',e.validated_at) END) AS value
                    FROM hubuumobject o LEFT JOIN object_schema_evidence e ON e.object_id=o.id AND e.class_id=o.hubuum_class_id
                    WHERE o.hubuum_class_id=s.class_id AND o.id>$2 AND ($3 IS NULL OR (
                        CASE WHEN NOT r.validate_schema THEN 'not_required'
                             WHEN e.schema_revision=r.revision AND e.object_revision=o.revision THEN CASE WHEN e.valid THEN 'valid' ELSE 'invalid' END
                             ELSE 'pending' END)=$3)
                    ORDER BY o.id LIMIT $4
                ) page)) AS value
            FROM class_schema_state s JOIN class_schema_revisions r ON r.class_id=s.class_id AND r.revision=s.active_revision
            WHERE s.class_id=$1
        "#).bind::<Integer,_>(query.class_id().id()).bind::<BigInt,_>(query.after()).bind::<Nullable<Text>,_>(status.map(StorageComplianceStatus::as_str)).bind::<BigInt,_>(query.limit() as i64).get_result::<JsonRow>(connection).await?;
        let active=StorageSchemaRevision::from_snapshot_with_limits(row.value["active"].clone(), schema_limits).map_err(invalid)?;
        row.value["objects"].as_array().ok_or_else(||invalid("compliance page"))?.iter().map(|value|{
            let id=ObjectId::new(i32::try_from(value["id"].as_i64().ok_or_else(||invalid("object id"))?).map_err(invalid)?)?;
            let revision=ResourceRevision::new(value["revision"].as_i64().ok_or_else(||invalid("object revision"))?).map_err(invalid)?;
            let evidence=if value["evidence"].is_null(){None}else{Some(serde_json::from_value(value["evidence"].clone()).map_err(invalid)?)};
            Ok::<_,PostgresStorageError>(StorageObjectCompliance::new(id,revision,&active,evidence))
        }).collect()
    }).await
}

pub(crate) async fn enqueue_restored_schema_work_on(
    connection: &mut PostgresConnection,
) -> Result<(), PostgresStorageError> {
    let rows=diesel::sql_query("SELECT jsonb_build_object('class_id',s.class_id,'revision',s.active_revision) AS value FROM class_schema_state s JOIN class_schema_revisions r ON r.class_id=s.class_id AND r.revision=s.active_revision WHERE r.validate_schema ORDER BY s.class_id").load::<JsonRow>(connection).await?;
    for row in rows {
        let target: SchemaReference = serde_json::from_value(row.value).map_err(invalid)?;
        let collection = CollectionId::new(
            lock_class(connection, target.class_id())
                .await?
                .collection_id,
        )?;
        enqueue_with_event_on(
            connection,
            &StorageSchemaWorkRequest::new(
                collection,
                target,
                StorageSchemaWorkKind::Revalidation,
                EventContext::system(),
            ),
            false,
        )
        .await?;
    }
    Ok(())
}

/// Validate at the import write boundary while the active class is locked.
pub(crate) async fn validate_import_object_on(
    schema_limits: JsonSchemaLimits,
    connection: &mut PostgresConnection,
    class_id: ClassId,
    collection: Option<CollectionId>,
    data: &Value,
) -> Result<(), PostgresStorageError> {
    let class = lock_class(connection, class_id).await?;
    if let Some(collection) = collection {
        check_collection(&class, collection)?;
    }
    let active = active_on(schema_limits, connection, class_id).await?;
    if active.policy().inspect(data) == StorageComplianceStatus::Invalid {
        return Err(PostgresStorageError::invalid_input(
            "Imported object does not satisfy the active schema revision",
        ));
    }
    Ok(())
}

async fn impact_mismatch_on(
    connection: &mut PostgresConnection,
    object: &ValidationObject,
    revision: &StorageSchemaRevision,
    context: &EventContext,
) -> Result<(), PostgresStorageError> {
    let document = AuditDocument::try_new(
        "Schema impact analysis found an incompatible object",
        None,
        None,
        json!({"schema":revision.reference(),"object_revision":object.revision,"category":"schema_mismatch","source":"impact","compliance_changed":false}),
    )?;
    let event = NewEvent::from_document(
        EntityType::ObjectValidation,
        Action::Failed,
        context.actor_kind(),
        document,
    )
    .map_err(invalid)?
    .with_context(context)
    .with_entity_id(EventEntityId::new(object.id)?)
    .with_entity_name(&object.name)
    .with_collection_id(CollectionId::new(object.collection_id)?);
    append_event(connection, &event).await?;
    Ok(())
}

pub async fn schema_compliance_counts(
    runtime: &PostgresRuntime,
) -> Result<StorageComplianceCounts, PostgresStorageError> {
    runtime.with_read_connection(async move |connection|{
        let row=diesel::sql_query("SELECT jsonb_build_object('valid',count(*) FILTER(WHERE c.validate_schema AND e.schema_revision=s.active_revision AND e.object_revision=o.revision AND e.valid),'invalid',count(*) FILTER(WHERE c.validate_schema AND e.schema_revision=s.active_revision AND e.object_revision=o.revision AND NOT e.valid),'pending',count(*) FILTER(WHERE c.validate_schema AND (e.object_id IS NULL OR e.schema_revision<>s.active_revision OR e.object_revision<>o.revision)),'not_required',count(*) FILTER(WHERE NOT c.validate_schema)) AS value FROM hubuumobject o JOIN hubuumclass c ON c.id=o.hubuum_class_id JOIN class_schema_state s ON s.class_id=c.id LEFT JOIN object_schema_evidence e ON e.object_id=o.id AND e.class_id=c.id").get_result::<JsonRow>(connection).await?;
        serde_json::from_value(row.value).map_err(invalid)
    }).await
}

pub(crate) async fn activate_import_schema_on(
    schema_limits: JsonSchemaLimits,
    connection: &mut PostgresConnection,
    class: &ClassRow,
    intent: &StorageImportSchemaActivation,
    policy: &hubuum_storage_core::StorageClassSchemaPolicy,
) -> Result<(), PostgresStorageError> {
    let request = intent.for_class(
        ClassId::new(class.id)?,
        CollectionId::new(class.collection_id)?,
        crate::runtime::ambient_event_context(),
    );
    let target = revision_on(schema_limits, connection, request.target()).await?;
    if target.policy().policy() != policy {
        return Err(PostgresStorageError::invalid_input(
            "Imported class policy must exactly match the selected staged schema revision",
        ));
    }
    activate_schema_revision_on(schema_limits, connection, request)
        .await?
        .into_value();
    Ok(())
}
