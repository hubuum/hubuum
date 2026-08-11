use crate::storage::postgres::prelude::*;
use tracing::{debug, trace};

use crate::errors::ApiError;
use crate::models::group::Group;
use crate::models::output::{EffectiveGroupPermission, GroupPermission};
use crate::models::search::{FilterField, QueryOptions, QueryParamsExt};
use crate::models::{
    Collection, CollectionID, HubuumObjectRelationID, NewCollection, NewCollectionWithAssignee,
    Permission, Permissions, PermissionsList, UpdateCollection,
};
use crate::models::{HubuumClassRelation, NewHubuumObjectRelation};
use crate::models::{HubuumObjectRelation, NewHubuumClassRelation};
use crate::storage::postgres::operations::GetCollection;
use crate::storage::postgres::operations::group::GroupRow;
use crate::storage::postgres::operations::permissions::{PermissionFilter, PermissionRow};
use crate::storage::postgres::{with_connection, with_transaction};
use crate::traits::{
    ClassAccessors, CollectionAccessors, GroupAccessors, ObjectAccessors, SelfAccessors,
};

mod permissions;
mod records;
mod relations;

pub use permissions::*;
pub use records::*;
