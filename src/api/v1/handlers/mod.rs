pub mod classes;
pub mod event_deliveries;
pub mod event_sinks;
pub mod event_subscriptions;
pub mod events;
pub mod groups;
pub mod history;
pub mod imports;
pub mod me;
pub mod namespaces;
pub mod principals;
pub mod relations;
pub mod remote_targets;
pub mod reports;
pub mod search;
pub mod service_accounts;
pub mod tasks;
pub mod templates;
pub mod users;

use tracing::debug;

use crate::db::DbPool;
use crate::errors::ApiError;
use crate::models::{HubuumClass, HubuumObject};
use crate::traits::{ClassAccessors, SelfAccessors};

pub async fn check_if_object_in_class<C, O>(
    pool: &DbPool,
    class: &C,
    object: &O,
) -> Result<(), ApiError>
where
    C: SelfAccessors<HubuumClass>,
    O: SelfAccessors<HubuumObject> + ClassAccessors<HubuumClass>,
{
    let object_class_id = object.class_id(pool).await?.id();

    if object_class_id != class.id() {
        debug!(
            message = "Object class mismatch",
            class_id = class.id(),
            object_id = object.id(),
            object_class = object_class_id
        );
        return Err(ApiError::NotFound(format!(
            "Object {} is not of class {}",
            object.id(),
            class.id()
        )));
    }

    Ok(())
}
