use super::*;
use hubuum_domain::{CollectionId, PrincipalId, PrincipalIdentity, TaskId};
use hubuum_events_core::{
    Action, AuditDocument, EntityType, EventContext, EventEntityId, NewEvent,
};
use serde::Serialize;
use serde_json::json;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PendingInvocation {
    target: i32,
    target_identity: Uuid,
    name: String,
    collection: i32,
    collection_identity: Uuid,
    subject_type: String,
    subject: i32,
    context: EventContext,
    #[serde(default)]
    principals: Vec<(PrincipalId, PrincipalIdentity)>,
}

pub(crate) async fn capture_invocation_on(
    connection: &mut PostgresConnection,
    task_id: i32,
    invocation: &StorageTaskRemoteInvocation,
) -> Result<(), PostgresStorageError> {
    let row = diesel::sql_query("SELECT jsonb_build_object('target',t.id,'target_identity',t.logical_identity #>> '{subject,identity,identity}','name',t.name,'collection',c.id,'collection_identity',c.logical_identity #>> '{subject,identity,identity}','subject_type',$2::text,'subject',$3::integer,'context',$4::jsonb) AS material FROM public.remote_targets t JOIN public.collections c ON c.id=t.collection_id WHERE t.id=$1 FOR SHARE OF t,c")
        .bind::<Integer,_>(invocation.target_id().id())
        .bind::<Text,_>(invocation.subject_type().as_str())
        .bind::<Integer,_>(invocation.subject_id().id())
        .bind::<Jsonb,_>(serde_json::to_value(invocation.event_context().clone().with_task_id(TaskId::new(task_id)?))
            .map_err(|_| PostgresStorageError::internal("Cannot encode original task invocation provenance"))?)
        .get_result::<MaterialRow>(connection).await?;
    let mut pending: PendingInvocation = decode(row.material)?;
    let mut ids = std::collections::BTreeSet::new();
    ids.extend(pending.context.actor_user_id());
    ids.extend(pending.context.initiator_user_id());
    for id in ids {
        let row = diesel::sql_query(
            "SELECT logical_identity AS material FROM public.principals WHERE id=$1 FOR SHARE",
        )
        .bind::<Integer, _>(id.id())
        .get_result::<MaterialRow>(connection)
        .await?;
        let evidence: StorageIdentityEvidence = decode(row.material)?;
        let Some(StorageIdentitySubject::Principal {
            identity: IdentityReference::Known { identity },
        }) = evidence.subject()
        else {
            return Err(PostgresStorageError::conflict(
                "Invocation principal identity is unresolved",
            ));
        };
        pending.principals.push((id, identity));
    }
    let encoded = canonical(&pending)?;
    if encoded.len() > 8192 {
        return Err(PostgresStorageError::invalid_input(
            "Task invocation provenance exceeds its bounded admission envelope",
        ));
    }
    diesel::sql_query("UPDATE public.tasks SET admission_event=$2::jsonb WHERE id=$1")
        .bind::<Integer, _>(task_id)
        .bind::<Text, _>(encoded)
        .execute(connection)
        .await?;
    Ok(())
}

pub(crate) async fn emit_invocation_on(
    connection: &mut PostgresConnection,
    task_id: i32,
) -> Result<(), PostgresStorageError> {
    let row = diesel::sql_query("SELECT admission_event AS material FROM public.tasks WHERE id=$1")
        .bind::<Integer, _>(task_id)
        .get_result::<OptionalMaterialRow>(connection)
        .await?;
    let Some(material) = row.material else {
        return Ok(());
    };
    let pending: PendingInvocation = decode(material)?;
    for (id, identity) in &pending.principals {
        diesel::sql_query("SELECT logical_identity AS material FROM public.principals WHERE id=$1 AND logical_identity #>> '{subject,identity,identity}'=$2 FOR SHARE")
            .bind::<Integer,_>(id.id()).bind::<Text,_>(identity.to_string()).get_result::<MaterialRow>(connection).await?;
    }
    // Delayed activation must not rebind an old integer reference. Keep original
    // display/provenance; pin only its captured current identity before append.
    diesel::sql_query("SELECT jsonb_build_object('matched',true) AS material FROM public.remote_targets t JOIN public.collections c ON c.id=$3 WHERE t.id=$1 AND t.logical_identity #>> '{subject,identity,identity}'=$2 AND c.logical_identity #>> '{subject,identity,identity}'=$4 FOR SHARE OF t,c")
        .bind::<Integer,_>(pending.target).bind::<Text,_>(pending.target_identity.to_string())
        .bind::<Integer,_>(pending.collection).bind::<Text,_>(pending.collection_identity.to_string())
        .get_result::<MaterialRow>(connection).await?;
    let document = AuditDocument::try_new(
        format!("Remote target '{}' invoked", pending.name),
        None,
        None,
        json!({"task_id":task_id,"subject_type":pending.subject_type,"subject_id":pending.subject}),
    )?;
    let event = NewEvent::from_document(
        EntityType::RemoteTarget,
        Action::Invoked,
        pending.context.actor_kind(),
        document,
    )
    .map_err(|error| PostgresStorageError::internal(error.to_string()))?
    .with_context(&pending.context)
    .with_entity_id(EventEntityId::new(pending.target)?)
    .with_entity_name(pending.name)
    .with_collection_id(CollectionId::new(pending.collection)?);
    crate::operations::event_record::append_event(connection, &event).await?;
    diesel::sql_query("UPDATE public.tasks SET admission_event=NULL WHERE id=$1")
        .bind::<Integer, _>(task_id)
        .execute(connection)
        .await?;
    Ok(())
}

pub async fn activate_task_submission(
    runtime: &PostgresRuntime,
    request: ProtectedTaskAdmission,
) -> Result<TaskActivationOutcome, PostgresStorageError> {
    runtime.with_transaction(async move |connection| {
        let header=lock_on(connection).await?;
        let ticket=request.ticket();
        let receipt=receipt_on(connection,ticket.receipt().submission()).await?
            .ok_or_else(||PostgresStorageError::conflict("Task admission generation no longer exists"))?;
        if receipt.ticket()!=ticket || header.lineage!=request.checkpoint().lineage()
            || header.epoch!=request.checkpoint().epoch() {
            return Err(PostgresStorageError::conflict("Task admission generation or lineage changed"));
        }
        let application=crate::backend::deletion_control::read_application_on(connection).await?;
        if application.fence()!=request.execution_fence() {
            return Err(PostgresStorageError::conflict("Task admission application generation changed"));
        }
        let Some(task)=find_task_identity_on(connection,ticket.receipt().task()).await? else {
            return Ok(TaskActivationOutcome::DetailsExpired(receipt.expired_projection()));
        };
        let legacy=diesel::sql_query("SELECT receipt_legacy AS changed FROM public.tasks WHERE id=$1")
            .bind::<Integer,_>(task.id).get_result::<ChangedRow>(connection).await?.changed;
        // Fresh activation appends original provenance; a legacy acknowledgement
        // emits no new creation event and needs no live submitter row.
        if !legacy { diesel::sql_query("SELECT p.logical_identity AS material FROM public.principals p WHERE p.id=$1 AND p.logical_identity #>> '{subject,identity,identity}'=$2 FOR SHARE")
            .bind::<Integer,_>(task.submitted_by.ok_or_else(||PostgresStorageError::conflict("Original task submitter is unavailable"))?)
            .bind::<Text,_>(ticket.receipt().submission().principal().to_string())
            .get_result::<MaterialRow>(connection).await?;
            if let Some(initiator)=task.initiator_user_id {
                let identity=diesel::sql_query("SELECT logical_identity AS material FROM public.tasks WHERE id=$1").bind::<Integer,_>(task.id).get_result::<MaterialRow>(connection).await?;
                let evidence:StorageIdentityEvidence=decode(identity.material)?;
                let expected=evidence.principal_reference("/initiator_principal_id").ok_or_else(||PostgresStorageError::conflict("Original task initiator identity is unresolved"))?;
                diesel::sql_query("SELECT logical_identity AS material FROM public.principals WHERE id=$1 AND logical_identity #>> '{subject,identity,identity}'=$2 FOR SHARE").bind::<Integer,_>(initiator).bind::<Text,_>(expected.to_string()).get_result::<MaterialRow>(connection).await?;
            }
        }
        let changed=diesel::sql_query("SELECT public.hubuum_task_receipt_activate($1,$2,$3,$4,$5,$6,$7,$8) AS changed")
            .bind::<Integer,_>(task.id).bind::<SqlUuid,_>(ticket.receipt().generation().value())
            .bind::<SqlUuid,_>(ticket.admitted_incarnation()).bind::<BigInt,_>(ticket.created_revision().value())
            .bind::<BigInt,_>(request.checkpoint().task_revision().value())
            .bind::<SqlUuid,_>(header.lineage).bind::<SqlUuid,_>(header.epoch)
            .bind::<Text,_>(request.checkpoint().checkpoint().as_str()).get_result::<ChangedRow>(connection).await?.changed;
        if changed {
            if !legacy {
                crate::operations::task_queue::emit_queued_on(connection,&task).await?;
                emit_invocation_on(connection,task.id).await?;
            }
            Ok(TaskActivationOutcome::Activated(task.into_storage()?))
        } else {
            Ok(TaskActivationOutcome::AlreadyActive(task.into_storage()?))
        }
    }).await
}
