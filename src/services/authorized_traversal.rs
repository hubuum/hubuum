use std::collections::HashSet;

use crate::errors::ApiError;
use crate::models::search::{
    FilterField, ParsedQueryParam, QueryOptions, QueryParamsExt, SearchOperator,
};
use crate::models::{
    ClassGraphRow, HubuumClassRelation, HubuumObjectRelation, Permissions, RelatedObjectGraphRow,
    TokenScope,
};
use crate::pagination::paginate_in_memory;
use crate::permissions::visibility::authorize_all_candidates;
use crate::permissions::{
    AuthorizationContext, AuthorizationMode, PermissionBackend, PrincipalRef, ResourceRef,
};
use crate::services::authorization_resources::{
    class_authorization_resources, class_relation_authorization_resources,
    object_authorization_resources, object_relation_authorization_resources,
};
use crate::services::relation_queries::{self, RelationAccess};
use crate::storage::StorageContext;
use crate::traits::AuthzSubject;

const MAX_CANDIDATES: usize = 10_000;

#[derive(Clone, Copy)]
enum GraphKind {
    Class,
    Object,
}

impl GraphKind {
    fn read_permission(self) -> Permissions {
        match self {
            Self::Class => Permissions::ReadClass,
            Self::Object => Permissions::ReadObject,
        }
    }

    fn relation_permission(self) -> Permissions {
        match self {
            Self::Class => Permissions::ReadClassRelation,
            Self::Object => Permissions::ReadObjectRelation,
        }
    }
}

/// Proof that every vertex and edge of a retained path passed resource-aware
/// authorization, including token scope. Fields cannot be populated by callers.
struct AuthorizedGraph {
    vertices: HashSet<i32>,
    edges: HashSet<(i32, i32)>,
}

fn edge(left: i32, right: i32) -> (i32, i32) {
    (left.min(right), left.max(right))
}

impl AuthorizedGraph {
    async fn load(
        storage: &impl StorageContext,
        authorization: &ExternalTraversal<'_>,
        kind: GraphKind,
        paths: &[Vec<i32>],
        permissions: &[Permissions],
    ) -> Result<Self, ApiError> {
        ensure_candidate_count(paths.len())?;
        let mut ids = HashSet::new();
        for path in paths {
            if path.is_empty() || path.iter().any(|id| *id <= 0) {
                return Err(ApiError::InternalServerError(
                    "Traversal returned an invalid path".into(),
                ));
            }
            ids.extend(path.iter().copied());
            ensure_candidate_count(ids.len())?;
        }
        let mut ids = ids.into_iter().collect::<Vec<_>>();
        ids.sort_unstable();
        let resources = match kind {
            GraphKind::Class => {
                class_authorization_resources(storage, authorization.principal.user_id, &ids)
                    .await?
            }
            GraphKind::Object => object_authorization_resources(storage, &ids).await?,
        };
        let mut vertex_permissions = permissions.to_vec();
        vertex_permissions.extend([kind.read_permission(), Permissions::ReadCollection]);
        let vertices = authorization
            .authorize(ids.clone(), resources, vertex_permissions)
            .await?
            .into_iter()
            .collect();
        let access = RelationAccess::new(authorization.principal.user_id, true, None);
        let (edges, resources) = match kind {
            GraphKind::Class => {
                let (relations, _) = relation_queries::list_class_relations(
                    storage,
                    access,
                    relation_options(GraphKind::Class, &ids)?,
                )
                .await?;
                ensure_candidate_count(relations.len())?;
                let resources = class_relation_authorization_resources(storage, &relations).await?;
                (
                    relations
                        .iter()
                        .map(|r| edge(r.from_hubuum_class_id, r.to_hubuum_class_id))
                        .collect(),
                    resources,
                )
            }
            GraphKind::Object => {
                let (relations, _) = relation_queries::list_object_relations(
                    storage,
                    access,
                    relation_options(GraphKind::Object, &ids)?,
                )
                .await?;
                ensure_candidate_count(relations.len())?;
                let resources =
                    object_relation_authorization_resources(storage, &relations).await?;
                (
                    relations
                        .iter()
                        .map(|r| edge(r.from_hubuum_object_id, r.to_hubuum_object_id))
                        .collect(),
                    resources,
                )
            }
        };
        let edges = authorization
            .authorize(edges, resources, vec![kind.relation_permission()])
            .await?
            .into_iter()
            .collect();
        Ok(Self { vertices, edges })
    }

    fn retain_allowed<T>(
        &self,
        candidates: Vec<T>,
        path: impl for<'a> Fn(&'a T) -> &'a [i32],
    ) -> Result<Vec<T>, ApiError> {
        let mut allowed = Vec::new();
        for candidate in candidates {
            let path = path(&candidate);
            if path.is_empty() {
                return Err(ApiError::InternalServerError(
                    "Traversal returned an empty path".into(),
                ));
            }
            if path.iter().all(|id| self.vertices.contains(id))
                && path
                    .windows(2)
                    .all(|pair| self.edges.contains(&edge(pair[0], pair[1])))
            {
                allowed.push(candidate);
            }
        }
        Ok(allowed)
    }
}

struct ExternalTraversal<'a> {
    backend: &'a dyn PermissionBackend,
    principal: &'a PrincipalRef,
    scopes: Option<&'a TokenScope>,
}

impl<'a> ExternalTraversal<'a> {
    fn new(
        backend: &'a dyn PermissionBackend,
        principal: &'a PrincipalRef,
        scopes: Option<&'a TokenScope>,
    ) -> Self {
        Self {
            backend,
            principal,
            scopes,
        }
    }

    async fn authorize<T>(
        &self,
        candidates: Vec<T>,
        resources: Vec<ResourceRef>,
        permissions: Vec<Permissions>,
    ) -> Result<Vec<T>, ApiError> {
        if candidates.len() != resources.len() {
            return Err(ApiError::InternalServerError(
                "Traversal authorization candidate/resource count mismatch".into(),
            ));
        }
        Ok(authorize_all_candidates(
            self.backend,
            self.principal,
            candidates.into_iter().zip(resources).collect(),
            self.scopes,
            permissions,
            |(_, resource)| resource.clone(),
        )
        .await?
        .into_iter()
        .map(|(candidate, _)| candidate)
        .collect())
    }
}

fn ensure_candidate_count(count: usize) -> Result<(), ApiError> {
    if count > MAX_CANDIDATES {
        return Err(ApiError::BadRequest(format!(
            "Traversal authorization exceeds {MAX_CANDIDATES} candidates; narrow the query or reduce depth"
        )));
    }
    Ok(())
}

fn relation_options(kind: GraphKind, ids: &[i32]) -> Result<QueryOptions, ApiError> {
    ensure_candidate_count(ids.len())?;
    let (from, to) = match kind {
        GraphKind::Class => (FilterField::ClassFrom, FilterField::ClassTo),
        GraphKind::Object => (FilterField::ObjectFrom, FilterField::ObjectTo),
    };
    // An impossible positive resource id keeps an empty vertex set empty.
    let value = if ids.is_empty() {
        "0".to_string()
    } else {
        ids.iter().map(i32::to_string).collect::<Vec<_>>().join(",")
    };
    Ok(QueryOptions::new(
        [from, to]
            .into_iter()
            .map(|field| ParsedQueryParam {
                field,
                operator: SearchOperator::Equals { is_negated: false },
                value: value.clone(),
            })
            .collect(),
        Vec::new(),
        Some(MAX_CANDIDATES + 1),
        None,
        false,
    )?)
}

fn candidate_query(options: &QueryOptions) -> Result<QueryOptions, ApiError> {
    let mut query = options.clone();
    query.clear_cursor();
    query.set_include_total(false);
    query.set_limit(Some(MAX_CANDIDATES + 1))?;
    Ok(query)
}

pub(crate) async fn list_related_objects<C: AuthorizationContext, S: AuthzSubject + ?Sized>(
    context: &C,
    subject: &S,
    object_id: i32,
    options: QueryOptions,
    scopes: Option<&TokenScope>,
) -> Result<(Vec<RelatedObjectGraphRow>, Option<i64>), ApiError> {
    if let AuthorizationMode::Delegated(backend) = context.authorization_mode() {
        let principal = PrincipalRef::load(context, subject).await?;
        let authorization = ExternalTraversal::new(backend, &principal, scopes);
        let (candidates, _) = relation_queries::list_related_objects(
            context,
            RelationAccess::new(subject.principal_id(), true, None),
            object_id,
            candidate_query(&options)?,
        )
        .await?;
        let permissions = options.filters().permissions()?;
        let graph = AuthorizedGraph::load(
            context,
            &authorization,
            GraphKind::Object,
            &candidates
                .iter()
                .map(|row| row.path.clone())
                .collect::<Vec<_>>(),
            permissions.as_slice(),
        )
        .await?;
        let rows = graph.retain_allowed(candidates, |row| &row.path)?;
        let total = options.include_total().then_some(rows.len() as i64);
        return Ok((paginate_in_memory(rows, &options)?, total));
    }
    relation_queries::list_related_objects(
        context,
        RelationAccess::new(
            subject.principal_id(),
            subject.is_admin(context).await?,
            scopes,
        ),
        object_id,
        options,
    )
    .await
}

pub(crate) async fn list_related_classes<C: AuthorizationContext, S: AuthzSubject + ?Sized>(
    context: &C,
    subject: &S,
    class_id: i32,
    options: QueryOptions,
    scopes: Option<&TokenScope>,
) -> Result<(Vec<ClassGraphRow>, Option<i64>), ApiError> {
    if let AuthorizationMode::Delegated(backend) = context.authorization_mode() {
        let principal = PrincipalRef::load(context, subject).await?;
        let authorization = ExternalTraversal::new(backend, &principal, scopes);
        let (candidates, _) = relation_queries::list_related_classes(
            context,
            RelationAccess::new(subject.principal_id(), true, None),
            class_id,
            candidate_query(&options)?,
        )
        .await?;
        let permissions = options.filters().permissions()?;
        let graph = AuthorizedGraph::load(
            context,
            &authorization,
            GraphKind::Class,
            &candidates
                .iter()
                .map(|row| row.path.clone())
                .collect::<Vec<_>>(),
            permissions.as_slice(),
        )
        .await?;
        let rows = graph.retain_allowed(candidates, |row| &row.path)?;
        let total = options.include_total().then_some(rows.len() as i64);
        return Ok((paginate_in_memory(rows, &options)?, total));
    }
    relation_queries::list_related_classes(
        context,
        RelationAccess::new(
            subject.principal_id(),
            subject.is_admin(context).await?,
            scopes,
        ),
        class_id,
        options,
    )
    .await
}

pub(crate) async fn list_class_relations_between_ids<
    C: AuthorizationContext,
    S: AuthzSubject + ?Sized,
>(
    context: &C,
    subject: &S,
    class_ids: &[i32],
    scopes: Option<&TokenScope>,
) -> Result<Vec<HubuumClassRelation>, ApiError> {
    if let AuthorizationMode::Delegated(backend) = context.authorization_mode() {
        let principal = PrincipalRef::load(context, subject).await?;
        let authorization = ExternalTraversal::new(backend, &principal, scopes);
        let (candidates, _) = relation_queries::list_class_relations(
            context,
            RelationAccess::new(subject.principal_id(), true, None),
            relation_options(GraphKind::Class, class_ids)?,
        )
        .await?;
        ensure_candidate_count(candidates.len())?;
        let paths = candidates
            .iter()
            .map(|r| vec![r.from_hubuum_class_id, r.to_hubuum_class_id])
            .collect::<Vec<_>>();
        let graph =
            AuthorizedGraph::load(context, &authorization, GraphKind::Class, &paths, &[]).await?;
        let resources = class_relation_authorization_resources(context, &candidates).await?;
        let rows = authorization
            .authorize(
                candidates.into_iter().zip(paths).collect(),
                resources,
                vec![Permissions::ReadClassRelation],
            )
            .await?;
        let rows = graph
            .retain_allowed(rows, |(_, path)| path)?
            .into_iter()
            .map(|(row, _)| row)
            .collect::<Vec<_>>();

        return Ok(rows);
    }
    relation_queries::list_class_relations_between_ids(
        context,
        RelationAccess::new(
            subject.principal_id(),
            subject.is_admin(context).await?,
            scopes,
        ),
        class_ids,
    )
    .await
}

pub(crate) async fn list_object_relations_between_ids<
    C: AuthorizationContext,
    S: AuthzSubject + ?Sized,
>(
    context: &C,
    subject: &S,
    object_ids: &[i32],
    scopes: Option<&TokenScope>,
) -> Result<Vec<HubuumObjectRelation>, ApiError> {
    if let AuthorizationMode::Delegated(backend) = context.authorization_mode() {
        let principal = PrincipalRef::load(context, subject).await?;
        let authorization = ExternalTraversal::new(backend, &principal, scopes);
        let (candidates, _) = relation_queries::list_object_relations(
            context,
            RelationAccess::new(subject.principal_id(), true, None),
            relation_options(GraphKind::Object, object_ids)?,
        )
        .await?;
        ensure_candidate_count(candidates.len())?;
        let paths = candidates
            .iter()
            .map(|r| vec![r.from_hubuum_object_id, r.to_hubuum_object_id])
            .collect::<Vec<_>>();
        let graph =
            AuthorizedGraph::load(context, &authorization, GraphKind::Object, &paths, &[]).await?;
        let resources = object_relation_authorization_resources(context, &candidates).await?;
        let rows = authorization
            .authorize(
                candidates.into_iter().zip(paths).collect(),
                resources,
                vec![Permissions::ReadObjectRelation],
            )
            .await?;
        let rows = graph
            .retain_allowed(rows, |(_, path)| path)?
            .into_iter()
            .map(|(row, _)| row)
            .collect::<Vec<_>>();

        return Ok(rows);
    }
    relation_queries::list_object_relations_between_ids(
        context,
        RelationAccess::new(
            subject.principal_id(),
            subject.is_admin(context).await?,
            scopes,
        ),
        object_ids,
    )
    .await
}
