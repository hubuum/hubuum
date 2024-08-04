pub mod classes;
pub mod groups;
pub mod namespaces;
pub mod relations;
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
    let object_class_id = object.class_id(&pool).await?;

    if object_class_id != class.id() {
        debug!(
            message = "Object class mismatch",
            class_id = class.id(),
            object_id = object.id(),
            object_class = object_class_id
        );
        return Err(ApiError::BadRequest(format!(
            "Object {} is not of class {}",
            object.id(),
            class.id()
        )));
    }

    Ok(())
}
