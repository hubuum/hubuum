use diesel::prelude::*;
use std::{fmt, fmt::Display, slice};

use crate::db::DbPool;

use serde::{Deserialize, Serialize};

use crate::{errors::ApiError, schema::hubuumclass_relation, schema::hubuumobject_relation};

use crate::models::{HubuumClassRelation, HubuumClassRelationID, NewHubuumClassRelation};
use crate::traits::{CanDelete, CanSave, SelfAccessors};

impl SelfAccessors<HubuumClassRelation> for HubuumClassRelationID {
    fn id(&self) -> i32 {
        self.0
    }

    async fn instance(&self, pool: &DbPool) -> Result<HubuumClassRelation, ApiError> {
        use crate::schema::hubuumclass_relation::dsl::{hubuumclass_relation, id};
        use diesel::prelude::*;

        let mut conn = pool.get()?;
        let class = hubuumclass_relation
            .filter(id.eq(self.0))
            .first::<HubuumClassRelation>(&mut conn)?;

        Ok(class)
    }
}
impl SelfAccessors<HubuumClassRelation> for HubuumClassRelation {
    fn id(&self) -> i32 {
        self.id
    }

    async fn instance(&self, _pool: &DbPool) -> Result<HubuumClassRelation, ApiError> {
        Ok(self.clone())
    }
}

impl CanDelete for HubuumClassRelation {
    async fn delete(&self, pool: &DbPool) -> Result<(), ApiError> {
        use crate::schema::hubuumclass_relation::dsl::*;

        let mut conn = pool.get()?;
        diesel::delete(hubuumclass_relation.filter(id.eq(self.id))).execute(&mut conn)?;

        Ok(())
    }
}

impl CanSave for NewHubuumClassRelation {
    type Output = HubuumClassRelation;

    async fn save(&self, pool: &DbPool) -> Result<HubuumClassRelation, ApiError> {
        use crate::schema::hubuumclass_relation::dsl::hubuumclass_relation;

        if self.from_hubuum_class_id == self.to_hubuum_class_id {
            return Err(ApiError::BadRequest(
                "from_hubuum_class_id and to_hubuum_class_id cannot be the same".to_string(),
            ));
        }

        // Ensure that the from_hubuum_class_id is always less than the to_hubuum_class_id, this is
        // simply a convention to simplify queries. self is also RO so we'll use these values to create
        // the NewHubuumClassRelation during the insert.
        let (from_hubuum_class_id, to_hubuum_class_id) =
            if self.from_hubuum_class_id < self.to_hubuum_class_id {
                (self.from_hubuum_class_id, self.to_hubuum_class_id)
            } else {
                (self.to_hubuum_class_id, self.from_hubuum_class_id)
            };

        let mut conn = pool.get()?;
        let result = diesel::insert_into(hubuumclass_relation)
            .values(NewHubuumClassRelation {
                from_hubuum_class_id,
                to_hubuum_class_id,
            })
            .get_result(&mut conn)?;

        Ok(result)
    }
}
