use diesel::prelude::*;
use tracing::{debug, trace};

use crate::db::traits::GetNamespace;
use crate::db::{DbPool, with_connection, with_transaction};
use crate::errors::ApiError;
use crate::models::group::Group;
use crate::models::output::GroupPermission;
use crate::models::permissions::PermissionFilter;
use crate::models::search::{FilterField, QueryOptions, QueryParamsExt};
use crate::models::{HubuumClassRelation, NewHubuumObjectRelation};
use crate::models::{HubuumObjectRelation, NewHubuumClassRelation};
use crate::models::{
    HubuumObjectRelationID, Namespace, NamespaceID, NewNamespace, NewNamespaceWithAssignee,
    NewPermission, Permission, Permissions, UpdateNamespace, User, UserID,
};
use crate::traits::{
    ClassAccessors, GroupAccessors, NamespaceAccessors, ObjectAccessors, SelfAccessors,
};

use super::user::{GroupIdsSubqueryBackend, GroupMemberships};

mod permissions;
mod records;
mod relations;

pub use permissions::*;
pub use records::*;
