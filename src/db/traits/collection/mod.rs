use diesel::prelude::*;
use tracing::{debug, trace};

use crate::db::traits::GetCollection;
use crate::db::{DbPool, with_connection, with_transaction};
use crate::errors::ApiError;
use crate::models::group::Group;
use crate::models::output::GroupPermission;
use crate::models::permissions::PermissionFilter;
use crate::models::search::{FilterField, QueryOptions, QueryParamsExt};
use crate::models::{
    Collection, CollectionID, HubuumObjectRelationID, NewCollection, NewCollectionWithAssignee,
    NewPermission, Permission, Permissions, UpdateCollection,
};
use crate::models::{HubuumClassRelation, NewHubuumObjectRelation};
use crate::models::{HubuumObjectRelation, NewHubuumClassRelation};
use crate::traits::{
    ClassAccessors, CollectionAccessors, GroupAccessors, ObjectAccessors, SelfAccessors,
};

mod permissions;
mod records;
mod relations;

pub use permissions::*;
pub use records::*;
