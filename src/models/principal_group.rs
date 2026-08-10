use crate::models::ResourceRevision;
use crate::models::group::Group;
use crate::models::principal::Principal;
use crate::storage::postgres::operations::group::{
    DeletePrincipalGroupRecord, PrincipalGroupGroupLookup, PrincipalGroupPrincipalLookup,
    SavePrincipalGroupRecord,
};

use crate::errors::ApiError;
use crate::schema::group_memberships;
use crate::storage::StorageContext;

use crate::storage::postgres::prelude::*;
use crate::traits::crud::SaveAdapter;
use serde::{Deserialize, Serialize};

/// A principal's membership in a group. Both human users and service accounts
/// participate through this single table.
#[derive(Serialize, Deserialize, Queryable, Insertable, Associations)]
#[diesel(belongs_to(Principal))]
#[diesel(belongs_to(Group))]
#[diesel(table_name = group_memberships)]
pub struct PrincipalGroup {
    pub principal_id: i32,
    pub group_id: i32,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub revision: ResourceRevision,
}

#[derive(Serialize, Deserialize, Insertable)]
#[diesel(table_name = group_memberships)]
pub struct NewPrincipalGroup {
    pub principal_id: i32,
    pub group_id: i32,
}

impl SaveAdapter for NewPrincipalGroup {
    type Output = PrincipalGroup;

    async fn save_adapter_without_events(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<Self::Output, ApiError> {
        self.save_principal_group_record_without_events(pool).await
    }
}

impl PrincipalGroup {
    pub async fn principal<C>(&self, backend: &C) -> Result<Principal, ApiError>
    where
        C: StorageContext,
    {
        self.load_principal_group_principal(backend).await
    }

    pub async fn group<C>(&self, backend: &C) -> Result<Group, ApiError>
    where
        C: StorageContext,
    {
        self.load_principal_group_group(backend).await
    }

    pub async fn save<C>(&self, backend: &C) -> Result<PrincipalGroup, ApiError>
    where
        C: StorageContext,
    {
        self.save_principal_group_record_without_events(backend)
            .await
    }

    pub async fn delete<C>(&self, backend: &C) -> Result<(), ApiError>
    where
        C: StorageContext,
    {
        self.delete_principal_group_record(backend).await
    }
}
