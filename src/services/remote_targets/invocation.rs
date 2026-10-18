use std::time::Duration;

use hubuum_outbound_http::{OutboundMethod, OutboundRequest, OutboundUrlParts};

use crate::errors::ApiError;
use crate::models::remote_target::CredentialBoundDestination;
use crate::models::{
    Collection, CollectionID, HubuumClassRelationID, Permissions, RemoteHttpMethod,
    RemoteInvocationSubject, RemoteTarget, RemoteTargetSubjectType, RemoteTemplateContext,
    StoredRemoteCallTaskPayload, TokenScope,
};
use crate::permissions::AuthorizationContext;
use crate::storage::StorageContext;
use crate::traits::{
    AuthzSubject, ClassAccessors, CollectionAccessors, ObjectAccessors, SelfAccessors,
    UserPermissions,
};

struct ResolvedRemoteInvocationSubject {
    subject: RemoteInvocationSubject,
    collections: Vec<Collection>,
    required_read_permission: Permissions,
    context: RemoteTemplateContext,
}

/// Authorized target revision and resolved subject, kept together through rendering.
pub(crate) struct AuthorizedRemoteInvocation {
    target: RemoteTarget,
    resolved: ResolvedRemoteInvocationSubject,
    destination: Option<CredentialBoundDestination>,
}

impl AuthorizedRemoteInvocation {
    pub(crate) async fn from_stored(
        backend: &impl AuthorizationContext,
        actor: &impl AuthzSubject,
        scopes: Option<&TokenScope>,
        request: &StoredRemoteCallTaskPayload,
    ) -> Result<Self, ApiError> {
        let target = super::get_remote_target(backend, request.target_id.id()).await?;
        request.validate_target_revision(target.revision)?;
        Self::authorize(backend, actor, scopes, target, &request.subject).await
    }

    pub(crate) fn target(&self) -> &RemoteTarget {
        &self.target
    }
    pub(crate) fn subject_type(&self) -> RemoteTargetSubjectType {
        self.resolved.subject.subject_type()
    }
    pub(crate) fn subject_id(&self) -> i32 {
        self.resolved.subject.subject_id()
    }
    pub(crate) fn template_context(&self) -> RemoteTemplateContext {
        self.resolved.context.clone()
    }

    pub(crate) fn approve_destination(
        &self,
        parts: OutboundUrlParts,
    ) -> Result<AuthorizedRemoteDestination<'_>, ApiError> {
        if let Some(binding) = &self.destination {
            binding.validate_rendered(&parts)?;
        }
        Ok(AuthorizedRemoteDestination {
            invocation: self,
            parts,
        })
    }

    pub(crate) async fn authorize<C>(
        backend: &C,
        actor: &impl AuthzSubject,
        scopes: Option<&TokenScope>,
        target: RemoteTarget,
        subject: &RemoteInvocationSubject,
    ) -> Result<Self, ApiError>
    where
        C: AuthorizationContext,
    {
        let target_collection_id = CollectionID::new(target.collection_id)?;
        crate::can!(
            backend,
            actor,
            scopes,
            [Permissions::ExecuteRemoteTarget],
            target_collection_id
        );

        if !target.enabled {
            return Err(ApiError::BadRequest(
                "Remote target is disabled".to_string(),
            ));
        }

        let resolved = resolve_subject(subject, backend).await?;
        if !target.allows_subject_type(resolved.subject.subject_type()) {
            return Err(ApiError::BadRequest(format!(
                "Remote target does not allow '{}' subjects",
                resolved.subject.subject_type().as_str()
            )));
        }
        if let RemoteInvocationSubject::Object { class_id, .. } = subject
            && target.class_id != Some(class_id.id())
        {
            return Err(ApiError::NotFound(
                "Remote target not found for invocation subject class".to_string(),
            ));
        }
        if !resolved
            .collections
            .iter()
            .any(|collection| collection.id == target.collection_id)
        {
            return Err(ApiError::NotFound(
                "Remote target not found for invocation subject".to_string(),
            ));
        }
        for collection in &resolved.collections {
            crate::can!(
                backend,
                actor,
                scopes,
                [resolved.required_read_permission],
                collection
            );
        }

        let destination = if target.auth_config.uses_server_credentials() {
            Some(CredentialBoundDestination::from_template(
                &target.url_template,
            )?)
        } else {
            None
        };
        Ok(Self {
            target,
            resolved,
            destination,
        })
    }
}

/// The transport request can only use the destination checked for this invocation.
pub(crate) struct AuthorizedRemoteDestination<'a> {
    invocation: &'a AuthorizedRemoteInvocation,
    parts: OutboundUrlParts,
}

impl AuthorizedRemoteDestination<'_> {
    pub(crate) fn url(&self) -> &str {
        self.parts.url()
    }
    pub(crate) fn into_request(self, timeout: Duration) -> OutboundRequest {
        let method = match self.invocation.target.method {
            RemoteHttpMethod::Get => OutboundMethod::Get,
            RemoteHttpMethod::Post => OutboundMethod::Post,
            RemoteHttpMethod::Patch => OutboundMethod::Patch,
            RemoteHttpMethod::Delete => OutboundMethod::Delete,
        };
        OutboundRequest::new(method, self.parts.url(), timeout)
    }
}

async fn resolve_subject(
    subject: &RemoteInvocationSubject,
    backend: &impl StorageContext,
) -> Result<ResolvedRemoteInvocationSubject, ApiError> {
    match subject {
        RemoteInvocationSubject::Collection { collection_id } => {
            let collection = collection_id.collection(backend).await?;
            let context = serde_json::json!({
                "subject_type": subject.subject_type().as_str(),
                "subject": collection.clone(),
                "collection": collection.clone(),
            });
            Ok(ResolvedRemoteInvocationSubject {
                subject: subject.clone(),
                collections: vec![collection],
                required_read_permission: Permissions::ReadCollection,
                context: RemoteTemplateContext::new(context)?,
            })
        }
        RemoteInvocationSubject::Class { class_id } => {
            let class = class_id.class(backend).await?;
            let collection = CollectionID::new(class.collection_id)?
                .collection(backend)
                .await?;
            let context = serde_json::json!({
                "subject_type": subject.subject_type().as_str(),
                "subject": class.clone(),
                "class": class.clone(),
                "collection": collection.clone(),
            });
            Ok(ResolvedRemoteInvocationSubject {
                subject: subject.clone(),
                collections: vec![collection],
                required_read_permission: Permissions::ReadClass,
                context: RemoteTemplateContext::new(context)?,
            })
        }
        RemoteInvocationSubject::Object {
            class_id,
            object_id,
        } => {
            let class = class_id.class(backend).await?;
            let object = object_id.instance(backend).await?;
            if object.hubuum_class_id != class.id {
                return Err(ApiError::NotFound("Object not found in class".to_string()));
            }
            let collection = CollectionID::new(object.collection_id)?
                .collection(backend)
                .await?;
            let context = serde_json::json!({
                "subject_type": subject.subject_type().as_str(),
                "subject": object.clone(),
                "object": object.clone(),
                "class": class.clone(),
                "collection": collection.clone(),
            });
            Ok(ResolvedRemoteInvocationSubject {
                subject: subject.clone(),
                collections: vec![collection],
                required_read_permission: Permissions::ReadObject,
                context: RemoteTemplateContext::new(context)?,
            })
        }
        RemoteInvocationSubject::ClassRelation { relation_id } => {
            let relation = relation_id.instance(backend).await?;
            let (from_class, to_class) = relation_id.class(backend).await?;
            let collections = relation_id.collection(backend).await?;
            let subject_collections =
                unique_collections(vec![collections.0.clone(), collections.1.clone()]);
            let context = serde_json::json!({
                "subject_type": subject.subject_type().as_str(),
                "subject": relation.clone(),
                "class_relation": relation.clone(),
                "from_class": from_class.clone(),
                "to_class": to_class.clone(),
                "collections": [collections.0.clone(), collections.1.clone()],
            });
            Ok(ResolvedRemoteInvocationSubject {
                subject: subject.clone(),
                collections: subject_collections,
                required_read_permission: Permissions::ReadClassRelation,
                context: RemoteTemplateContext::new(context)?,
            })
        }
        RemoteInvocationSubject::ObjectRelation { relation_id } => {
            let relation = relation_id.instance(backend).await?;
            let (from_object, to_object) = relation_id.object(backend).await?;
            let class_relation_id = HubuumClassRelationID::new(relation.class_relation_id)?;
            let class_relation = class_relation_id.instance(backend).await?;
            let (from_class, to_class) = class_relation_id.class(backend).await?;
            let collections = relation_id.collection(backend).await?;
            let subject_collections =
                unique_collections(vec![collections.0.clone(), collections.1.clone()]);
            let context = serde_json::json!({
                "subject_type": subject.subject_type().as_str(),
                "subject": relation.clone(),
                "object_relation": relation.clone(),
                "from_object": from_object.clone(),
                "to_object": to_object.clone(),
                "class_relation": class_relation.clone(),
                "from_class": from_class.clone(),
                "to_class": to_class.clone(),
                "collections": [collections.0.clone(), collections.1.clone()],
            });
            Ok(ResolvedRemoteInvocationSubject {
                subject: subject.clone(),
                collections: subject_collections,
                required_read_permission: Permissions::ReadObjectRelation,
                context: RemoteTemplateContext::new(context)?,
            })
        }
    }
}

fn unique_collections(collections: Vec<Collection>) -> Vec<Collection> {
    let mut seen = std::collections::HashSet::new();
    collections
        .into_iter()
        .filter(|collection| seen.insert(collection.id))
        .collect()
}
