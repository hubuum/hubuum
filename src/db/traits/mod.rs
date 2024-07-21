mod active_tokens;
mod class;
mod is_active;
mod namespace;
mod relations;

use crate::errors::ApiError;
use crate::models::{
    HubuumClass, HubuumClassRelation, HubuumClassRelationTransitive, Namespace, UserToken,
};
use crate::traits::SelfAccessors;

use super::{with_connection, DbPool};

/// Trait for checking if a structure is valid/active/etc in the database.
///
/// What the different traits imply may vary depending on the structure. For example, a user simply has to
/// exist in the database to be valid, while a token has to be valid and not expired.
pub trait Status<T> {
    /// Check that a structure is active.
    ///
    /// Validity implies that the structure exists in the database and that it is not expired, disabled,
    /// or otherwise inactive.
    async fn is_valid(&self, pool: &DbPool) -> Result<T, ApiError>;
}

/// Trait for getting all active tokens for a given structure.
///
/// This trait is used to get all active tokens for a given structure. For example, a user may have multiple
/// active tokens, and this trait would allow us to get all of them.
pub trait ActiveTokens {
    /// Get all active tokens for a given structure.
    async fn tokens(&self, pool: &DbPool) -> Result<Vec<UserToken>, ApiError>;
}

/// Trait for getting the namespace(s) of a structure from the backend database.
///
/// By default, this returns the singular namespace of the structure in question.
/// For relations, where we have two namespaces (one for each class or object),
/// the trait is implemented to return a tuple of the two namespaces.
pub trait GetNamespace<T = Namespace> {
    async fn namespace_from_backend(&self, pool: &DbPool) -> Result<T, ApiError>;
}

/// Trait for getting the classes(s) of a structure from the backend database.
///
/// By default, this returns the singular class of the structure in question.
/// For relations, where we have two classes (one for each structure), the
/// trait is implemented to return a tuple of the two namespaces.
pub trait GetClass<T = HubuumClass> {
    async fn class_from_backend(&self, pool: &DbPool) -> Result<T, ApiError>;
}

/// Trait for checking if a relation exists between two classes.
pub trait Relations<C1, C2>
where
    C1: SelfAccessors<HubuumClass> + Clone + Send + Sync,
    C2: SelfAccessors<HubuumClass> + Clone + Send + Sync,
{
    /// Check if a relation exists between two classes.
    async fn relations_between(
        pool: &DbPool,
        from: &C1,
        to: &C2,
    ) -> Result<Vec<HubuumClassRelationTransitive>, ApiError>;
}

/// Traits for checking relations between classes
pub trait ClassRelation<C1, C2>
where
    C1: SelfAccessors<HubuumClass> + Relations<C1, C2> + Clone + Send + Sync,
    C2: SelfAccessors<HubuumClass> + Clone + Send + Sync,
    Self: SelfAccessors<HubuumClass>,
{
    /// Check if a relation exists between self and another class
    async fn relations_to(
        &self,
        pool: &DbPool,
        other: &C2,
    ) -> Result<Vec<HubuumClassRelationTransitive>, ApiError>;

    /// Check if a relation exists between self and another class, boolean
    async fn has_relation_to(&self, pool: &DbPool, other: &C2) -> Result<bool, ApiError> {
        let relations = self.relations_to(pool, other).await?;
        Ok(!relations.is_empty())
    }
}

pub trait SelfRelations<C1>
where
    C1: SelfAccessors<HubuumClass> + Clone + Send + Sync,
    Self: SelfAccessors<HubuumClass>,
{
    async fn transitive_relations(
        &self,
        pool: &DbPool,
    ) -> Result<Vec<HubuumClassRelationTransitive>, ApiError> {
        use crate::schema::hubuumclass_closure::dsl::*;
        use diesel::prelude::*;

        with_connection(pool, |conn| {
            Ok(hubuumclass_closure
                .or_filter(ancestor_class_id.eq(self.id()))
                .or_filter(descendant_class_id.eq(self.id()))
                .load::<HubuumClassRelationTransitive>(conn)?)
        })
    }

    async fn relations(&self, pool: &DbPool) -> Result<Vec<HubuumClassRelation>, ApiError> {
        use crate::schema::hubuumclass_relation::dsl::*;
        use diesel::prelude::*;

        with_connection(pool, |conn| {
            Ok(hubuumclass_relation
                .or_filter(from_hubuum_class_id.eq(self.id()))
                .or_filter(to_hubuum_class_id.eq(self.id()))
                .load::<HubuumClassRelation>(conn)?)
        })
    }
}
