use diesel::prelude::*;
use diesel_async::RunQueryDsl;

use super::ObjectAggregateRouteTarget;
use super::candidate::ObjectAggregateCandidate;
use crate::errors::ApiError;
use crate::storage::postgres::PostgresConnection;
use crate::storage::{
    AuthorizationPermission, ObjectAggregateAuthorizer,
    StorageObjectAggregateAuthorizationCandidate, StorageObjectAggregateAuthorizationTarget,
};

pub(super) struct DelegatedObjectAggregateAuthorization<'a> {
    authorizer: &'a dyn ObjectAggregateAuthorizer,
    required_permissions: Vec<AuthorizationPermission>,
}

impl<'a> DelegatedObjectAggregateAuthorization<'a> {
    pub(super) fn new(
        authorizer: &'a dyn ObjectAggregateAuthorizer,
        required_permissions: Vec<AuthorizationPermission>,
    ) -> Self {
        Self {
            authorizer,
            required_permissions,
        }
    }

    pub(super) async fn authorize_target(
        &self,
        connection: &mut PostgresConnection,
        target: &ObjectAggregateRouteTarget,
    ) -> Result<bool, ApiError> {
        use crate::schema::collections;

        let collection_name = collections::table
            .filter(collections::id.eq(target.collection_id))
            .select(collections::name)
            .first::<String>(connection)
            .await
            .optional()?
            .ok_or_else(|| {
                ApiError::InternalServerError(format!(
                    "Object aggregate target references missing collection {}",
                    target.collection_id
                ))
            })?;
        let target = StorageObjectAggregateAuthorizationTarget::new(
            target.class_id,
            target.class_name.clone(),
            target.collection_id,
            collection_name,
        );
        Ok(self
            .authorizer
            .authorize_target(target, self.required_permissions.clone())
            .await?)
    }

    pub(super) async fn authorize_objects(
        &self,
        candidates: Vec<ObjectAggregateCandidate>,
    ) -> Result<Vec<ObjectAggregateCandidate>, ApiError> {
        if candidates.is_empty() {
            return Ok(candidates);
        }
        let authorization_candidates = candidates
            .iter()
            .map(|candidate| {
                StorageObjectAggregateAuthorizationCandidate::new(
                    candidate.id,
                    candidate.name.clone(),
                    candidate.collection_id,
                    candidate.hubuum_class_id,
                )
            })
            .collect::<Vec<_>>();
        let decisions = self
            .authorizer
            .authorize_objects(authorization_candidates, self.required_permissions.clone())
            .await?;
        if decisions.len() != candidates.len() {
            return Err(ApiError::InternalServerError(
                "Object aggregate authorizer returned an unexpected number of decisions"
                    .to_string(),
            ));
        }
        Ok(candidates
            .into_iter()
            .zip(decisions)
            .filter_map(|(candidate, allowed)| allowed.then_some(candidate))
            .collect())
    }
}
