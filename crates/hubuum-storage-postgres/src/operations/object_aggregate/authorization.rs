use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use hubuum_domain::{ClassId, CollectionId, ObjectId};

use super::ObjectAggregateRouteTarget;
use super::candidate::ObjectAggregateCandidate;
use crate::{PostgresConnection, PostgresStorageError};
use hubuum_storage_core::{
    ObjectAggregateAuthorizer, StorageAuthorizationPermission,
    StorageObjectAggregateAuthorizationCandidate, StorageObjectAggregateAuthorizationTarget,
};

pub(super) struct DelegatedObjectAggregateAuthorization<'a> {
    authorizer: &'a dyn ObjectAggregateAuthorizer,
    required_permissions: Vec<StorageAuthorizationPermission>,
}

impl<'a> DelegatedObjectAggregateAuthorization<'a> {
    pub(super) fn new(
        authorizer: &'a dyn ObjectAggregateAuthorizer,
        required_permissions: Vec<StorageAuthorizationPermission>,
    ) -> Self {
        Self {
            authorizer,
            required_permissions,
        }
    }

    pub(super) async fn load_authorization_target(
        &self,
        connection: &mut PostgresConnection,
        target: &ObjectAggregateRouteTarget,
    ) -> Result<StorageObjectAggregateAuthorizationTarget, PostgresStorageError> {
        use crate::schema::collections;

        let collection_name = collections::table
            .filter(collections::id.eq(target.collection_id))
            .select(collections::name)
            .first::<String>(connection)
            .await
            .optional()?
            .ok_or_else(|| {
                PostgresStorageError::database(format!(
                    "Object aggregate target references missing collection {}",
                    target.collection_id
                ))
            })?;
        Ok(StorageObjectAggregateAuthorizationTarget::new(
            ClassId::new(target.class_id)?,
            target.class_name.clone(),
            CollectionId::new(target.collection_id)?,
            collection_name,
        ))
    }

    pub(super) async fn authorize_target(
        &self,
        target: StorageObjectAggregateAuthorizationTarget,
    ) -> Result<bool, PostgresStorageError> {
        Ok(self
            .authorizer
            .authorize_target(target, self.required_permissions.clone())
            .await?)
    }

    pub(super) async fn authorize_objects(
        &self,
        candidates: Vec<ObjectAggregateCandidate>,
    ) -> Result<Vec<ObjectAggregateCandidate>, PostgresStorageError> {
        if candidates.is_empty() {
            return Ok(candidates);
        }
        let authorization_candidates = candidates
            .iter()
            .map(|candidate| {
                Ok(StorageObjectAggregateAuthorizationCandidate::new(
                    ObjectId::new(candidate.id)?,
                    candidate.name.clone(),
                    CollectionId::new(candidate.collection_id)?,
                    ClassId::new(candidate.hubuum_class_id)?,
                ))
            })
            .collect::<Result<Vec<_>, PostgresStorageError>>()?;
        let decisions = self
            .authorizer
            .authorize_objects(authorization_candidates, self.required_permissions.clone())
            .await?;
        if decisions.len() != candidates.len() {
            return Err(PostgresStorageError::internal(
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
