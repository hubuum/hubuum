#![allow(ambiguous_glob_reexports)] // We have several test modules, should be fine

pub(crate) const REDACTED_DEBUG_VALUE: &str = "<redacted>";

pub(crate) fn redacted_debug_option<T>(value: &Option<T>) -> Option<&'static str> {
    value.as_ref().map(|_| REDACTED_DEBUG_VALUE)
}

pub mod backup;
pub mod class;
pub mod collection;
pub mod computed_field;
pub mod credential;
pub mod event_delivery;
pub mod event_subscription;
pub mod export;
pub mod export_template;
pub mod group;
pub mod history;
pub mod identity;
pub mod import;
pub(crate) mod json_patch;
pub(crate) mod maintenance;
pub mod object;
pub mod object_aggregate;
pub mod object_data_patch;
pub mod output;
pub mod permissions;
pub mod principal;
pub mod principal_group;
pub mod relation;
pub mod remote_target;
pub(crate) mod retention;
pub mod revision;
pub mod search;
pub mod service_account;
pub mod structured_search;
pub mod task;
pub mod token;
pub mod token_retention;
pub mod token_scope;
pub mod unified_search;
pub mod user;

pub mod traits;

pub use crate::models::backup::*;
pub use crate::models::class::*;
pub use crate::models::collection::*;
pub use crate::models::computed_field::*;
pub use crate::models::event_delivery::*;
pub use crate::models::event_subscription::*;
pub use crate::models::export::*;
pub use crate::models::export_template::*;
pub use crate::models::group::*;
pub use crate::models::history::*;
pub use crate::models::identity::*;
pub use crate::models::import::*;
pub(crate) use crate::models::maintenance::*;
pub use crate::models::object::*;
pub use crate::models::object_aggregate::*;
pub use crate::models::object_data_patch::*;
pub use crate::models::output::*;
pub use crate::models::permissions::*;
pub use crate::models::principal::*;
pub use crate::models::principal_group::*;
pub use crate::models::relation::*;
pub use crate::models::remote_target::*;
pub use crate::models::revision::*;
pub use crate::models::service_account::*;
pub use crate::models::structured_search::*;
pub use crate::models::task::*;
pub use crate::models::token::*;
pub use crate::models::token_retention::*;
pub use crate::models::token_scope::*;
pub use crate::models::unified_search::*;
pub use crate::models::user::*;
