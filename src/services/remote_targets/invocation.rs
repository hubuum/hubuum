use std::time::Duration;

use hubuum_outbound_http::{OutboundMethod, OutboundRequest, OutboundUrlParts};

use crate::errors::ApiError;
use crate::models::remote_target::RemoteTemplateSources;
use crate::models::{
    Collection, CollectionID, HubuumClassRelationID, HubuumObjectID, Permissions, RemoteHttpMethod,
    RemoteInvocationSubject, RemoteTarget, RemoteTargetSubjectType, RemoteTemplateContext,
    StoredRemoteCallTaskPayload, TokenScope,
};
use crate::permissions::{
    AuthorizationContext, AuthorizationMode, ClassResourceEndpoint, ObjectResourceEndpoint,
    ResourceRef, authorize_resources,
};
use crate::traits::{
    AuthzSubject, ClassAccessors, CollectionAccessors, SelfAccessors, UserPermissions,
    scope_allows_resource,
};

struct ResolvedRemoteInvocationSubject {
    subject: RemoteInvocationSubject,
    collections: Vec<Collection>,
    context: RemoteTemplateContext,
}

/// Authorized target revision and resolved subject, kept together through rendering.
pub(crate) struct AuthorizedRemoteInvocation {
    target: RemoteTarget,
    resolved: ResolvedRemoteInvocationSubject,
    templates: RemoteTemplateSources,
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
    pub(crate) fn templates(&self) -> &RemoteTemplateSources {
        &self.templates
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
        self.templates.validate_destination(&parts)?;
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

        let resolved = authorize_subject(subject, backend, actor, scopes).await?;
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
        // Persisted source syntax is checked by rendering; retain structural and
        // credential-origin validation without a second template compilation.
        let templates = RemoteTemplateSources::new(
            &target.url_template,
            &target.headers_template,
            target.body_template.as_deref(),
            &target.auth_config,
        )?;
        Ok(Self {
            target,
            resolved,
            templates,
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

async fn authorize_subject(
    subject: &RemoteInvocationSubject,
    backend: &impl AuthorizationContext,
    actor: &impl AuthzSubject,
    scopes: Option<&TokenScope>,
) -> Result<ResolvedRemoteInvocationSubject, ApiError> {
    match subject {
        RemoteInvocationSubject::Collection { collection_id } => {
            let collection = collection_id.collection(backend).await?;
            crate::can!(
                backend,
                actor,
                scopes,
                [Permissions::ReadCollection],
                collection
            );
            let context = serde_json::json!({
                "subject_type": subject.subject_type().as_str(),
                "subject": collection.clone(),
                "collection": collection.clone(),
            });
            Ok(ResolvedRemoteInvocationSubject {
                subject: subject.clone(),
                collections: vec![collection],
                context: RemoteTemplateContext::new(context)?,
            })
        }
        RemoteInvocationSubject::Class { class_id } => {
            let class = class_id.class(backend).await?;
            crate::can!(backend, actor, scopes, [Permissions::ReadClass], class);
            let collection = CollectionID::new(class.collection_id)?
                .collection(backend)
                .await?;
            crate::can!(
                backend,
                actor,
                scopes,
                [Permissions::ReadCollection],
                collection
            );
            let context = serde_json::json!({
                "subject_type": subject.subject_type().as_str(),
                "subject": class.clone(),
                "class": class.clone(),
                "collection": collection.clone(),
            });
            Ok(ResolvedRemoteInvocationSubject {
                subject: subject.clone(),
                collections: vec![collection],
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
            crate::can!(backend, actor, scopes, [Permissions::ReadObject], object);
            crate::can!(backend, actor, scopes, [Permissions::ReadClass], class);
            let collection = CollectionID::new(object.collection_id)?
                .collection(backend)
                .await?;
            crate::can!(
                backend,
                actor,
                scopes,
                [Permissions::ReadCollection],
                collection
            );
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
                context: RemoteTemplateContext::new(context)?,
            })
        }
        RemoteInvocationSubject::ClassRelation { relation_id } => {
            let relation = relation_id.instance(backend).await?;
            let from_class = relation.from_hubuum_class_id.instance(backend).await?;
            let to_class = relation.to_hubuum_class_id.instance(backend).await?;
            let collections = (
                from_class.collection(backend).await?,
                to_class.collection(backend).await?,
            );
            let subject_collections =
                unique_collections(vec![collections.0.clone(), collections.1.clone()]);
            authorize_resolved_resource(
                backend,
                actor,
                scopes,
                Permissions::ReadClassRelation,
                ResourceRef::class_relation(
                    Some(relation.id.id()),
                    ClassResourceEndpoint::new(from_class.collection_id, from_class.id),
                    ClassResourceEndpoint::new(to_class.collection_id, to_class.id),
                ),
            )
            .await?;
            crate::can!(
                backend,
                actor,
                scopes,
                [Permissions::ReadClass],
                from_class,
                to_class
            );
            crate::can!(
                backend,
                actor,
                scopes,
                [Permissions::ReadCollection],
                collections.0,
                collections.1
            );
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
                context: RemoteTemplateContext::new(context)?,
            })
        }
        RemoteInvocationSubject::ObjectRelation { relation_id } => {
            let relation = relation_id.instance(backend).await?;
            let from_object = HubuumObjectID::new(relation.from_hubuum_object_id)?
                .instance(backend)
                .await?;
            let to_object = HubuumObjectID::new(relation.to_hubuum_object_id)?
                .instance(backend)
                .await?;
            let class_relation_id = HubuumClassRelationID::new(relation.class_relation_id)?;
            let class_relation = class_relation_id.instance(backend).await?;
            let from_class = class_relation
                .from_hubuum_class_id
                .instance(backend)
                .await?;
            let to_class = class_relation.to_hubuum_class_id.instance(backend).await?;
            let collections = (
                from_object.collection(backend).await?,
                to_object.collection(backend).await?,
            );
            let subject_collections =
                unique_collections(vec![collections.0.clone(), collections.1.clone()]);
            authorize_resolved_resource(
                backend,
                actor,
                scopes,
                Permissions::ReadObjectRelation,
                ResourceRef::object_relation(
                    Some(relation.id),
                    ObjectResourceEndpoint::new(
                        from_object.collection_id,
                        from_object.hubuum_class_id,
                        from_object.id,
                    ),
                    ObjectResourceEndpoint::new(
                        to_object.collection_id,
                        to_object.hubuum_class_id,
                        to_object.id,
                    ),
                    relation.class_relation_id,
                ),
            )
            .await?;
            authorize_resolved_resource(
                backend,
                actor,
                scopes,
                Permissions::ReadClassRelation,
                ResourceRef::class_relation(
                    Some(class_relation.id.id()),
                    ClassResourceEndpoint::new(from_class.collection_id, from_class.id),
                    ClassResourceEndpoint::new(to_class.collection_id, to_class.id),
                ),
            )
            .await?;
            crate::can!(
                backend,
                actor,
                scopes,
                [Permissions::ReadObject],
                from_object,
                to_object
            );
            crate::can!(
                backend,
                actor,
                scopes,
                [Permissions::ReadClass],
                from_class,
                to_class
            );
            crate::can!(
                backend,
                actor,
                scopes,
                [Permissions::ReadCollection],
                collections.0,
                collections.1
            );
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
                context: RemoteTemplateContext::new(context)?,
            })
        }
    }
}

/// Preserve the loaded relation identity for external policies and scopes.
/// Local grants use the collection IDs from that same captured resource.
async fn authorize_resolved_resource(
    backend: &impl AuthorizationContext,
    actor: &impl AuthzSubject,
    scopes: Option<&TokenScope>,
    permission: Permissions,
    resource: ResourceRef,
) -> Result<(), ApiError> {
    if !scope_allows_resource(scopes, &resource) {
        return Err(ApiError::Forbidden("Permission denied".to_string()));
    }
    match backend.authorization_mode() {
        AuthorizationMode::Delegated(policy) => {
            authorize_resources(
                policy,
                backend,
                actor,
                scopes,
                vec![permission],
                vec![resource],
            )
            .await
        }
        AuthorizationMode::LocalStorage => {
            let fields = resource.fields();
            let collections = fields
                .collection_id
                .into_iter()
                .chain(fields.from_collection_id)
                .chain(fields.to_collection_id)
                .map(CollectionID::new)
                .collect::<Result<Vec<_>, _>>()?;
            if collections.is_empty() {
                return Err(ApiError::Forbidden("Permission denied".to_string()));
            }
            actor.can(backend, [permission], collections, scopes).await
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
