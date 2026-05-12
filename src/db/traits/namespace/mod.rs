use diesel::prelude::*;
use tracing::{debug, trace};

use crate::db::traits::GetNamespace;
use crate::db::{DbPool, with_connection, with_transaction};
use crate::errors::ApiError;
use crate::models::group::Group;
use crate::models::output::GroupPermission;
use crate::models::search::QueryOptions;
use crate::models::{HubuumClassRelation, NewHubuumObjectRelation};
use crate::models::{HubuumObjectRelation, NewHubuumClassRelation};
use crate::models::{
    HubuumObjectRelationID, Namespace, NamespaceID, NewNamespace, NewNamespaceWithAssignee,
    NewPermission, Permissions, UpdateNamespace, User, UserID,
};
use crate::traits::{
    ClassAccessors, GroupAccessors, NamespaceAccessors, ObjectAccessors, SelfAccessors,
};

mod permissions;
mod records;
mod relations;

pub use permissions::*;
pub use records::*;
