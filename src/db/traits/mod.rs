pub mod active_tokens;
pub mod class;
pub mod group;
pub mod is_active;
pub mod namespace;
pub mod object;
pub mod permissions;
pub mod relations;
pub mod task;
pub mod task_import;
pub mod user;

#[allow(unused_imports)]
pub use user::UserPermissions;

use super::{DbPool, with_connection};
use crate::db::traits::relations::{ObjectRelationMembershipsBackend, SelfRelationsBackend};
use crate::errors::ApiError;
use crate::models::search::{FilterField, ParsedQueryParam, QueryOptions};
use crate::models::{
    HubuumClass, HubuumClassRelation, HubuumClassRelationTransitive, HubuumObject, HubuumObjectID,
    HubuumObjectRelation, HubuumObjectTransitiveLink, Namespace, User, UserToken,
};
use crate::traits::{GroupAccessors, SelfAccessors};

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
    #[allow(dead_code)]
    async fn tokens(&self, pool: &DbPool) -> Result<Vec<UserToken>, ApiError>;
    async fn tokens_paginated(
        &self,
        pool: &DbPool,
        query_options: &QueryOptions,
    ) -> Result<Vec<UserToken>, ApiError>;
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

/// Trait for getting the object(s) of a structure from the backend database.
///
/// By default, this returns the singular object of the structure in question.
/// For relations, where we have two objects (one for each structure), the
/// trait is implemented to return a tuple of the two objects.
#[allow(dead_code)]
pub trait GetObject<T = HubuumObject> {
    async fn object_from_backend(&self, pool: &DbPool) -> Result<T, ApiError>;
}

/// Trait for checking if a relation exists between two classes.
pub trait Relations<C1, C2>
where
    C1: SelfAccessors<HubuumClass> + Clone + Send + Sync,
    C2: SelfAccessors<HubuumClass> + Clone + Send + Sync,
{
    /// Check if a relation exists between two classes.
    #[allow(dead_code)]
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
    #[allow(dead_code)]
    async fn relations_to(
        &self,
        pool: &DbPool,
        other: &C2,
    ) -> Result<Vec<HubuumClassRelationTransitive>, ApiError>;

    async fn relations_to_paginated(
        &self,
        pool: &DbPool,
        other: &C2,
        query_options: &QueryOptions,
    ) -> Result<Vec<HubuumClassRelationTransitive>, ApiError>;

    /// Check if a direct relation exists between self and another class
    async fn direct_relation_to(
        &self,
        pool: &DbPool,
        other: &C2,
    ) -> Result<Option<HubuumClassRelation>, ApiError>;
}

pub trait SelfRelations<C1>
where
    C1: SelfAccessors<HubuumClass> + Clone + Send + Sync,
    Self: SelfAccessors<HubuumClass> + Clone + Send + Sync,
{
    #[allow(dead_code)]
    async fn transitive_relations(
        &self,
        pool: &DbPool,
    ) -> Result<Vec<HubuumClassRelationTransitive>, ApiError> {
        self.transitive_relations_from_backend(pool).await
    }

    async fn transitive_relations_paginated(
        &self,
        pool: &DbPool,
        query_options: &QueryOptions,
    ) -> Result<Vec<HubuumClassRelationTransitive>, ApiError> {
        use crate::schema::hubuumclass_closure::dsl::*;
        use crate::{array_search, numeric_search};
        use diesel::prelude::*;

        let mut base_query = hubuumclass_closure
            .or_filter(ancestor_class_id.eq(self.id()))
            .or_filter(descendant_class_id.eq(self.id()))
            .into_boxed();

        for param in &query_options.filters {
            let operator = param.operator.clone();
            match param.field {
                FilterField::ClassFrom => {
                    numeric_search!(base_query, param, operator, ancestor_class_id)
                }
                FilterField::ClassTo => {
                    numeric_search!(base_query, param, operator, descendant_class_id)
                }
                FilterField::Depth => numeric_search!(base_query, param, operator, depth),
                FilterField::Path => array_search!(base_query, param, operator, path),
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for transitive class relations",
                        param.field
                    )));
                }
            }
        }

        crate::apply_query_options!(base_query, query_options, HubuumClassRelationTransitive);

        with_connection(pool, |conn| {
            base_query
                .select((
                    ancestor_class_id.assume_not_null(),
                    descendant_class_id.assume_not_null(),
                    depth,
                    path,
                ))
                .load::<HubuumClassRelationTransitive>(conn)
        })
    }

    // We typically end up searching, so this interface is rarely used.
    #[allow(dead_code)]
    async fn relations(&self, pool: &DbPool) -> Result<Vec<HubuumClassRelation>, ApiError> {
        self.relations_from_backend(pool).await
    }

    async fn search_relations(
        &self,
        pool: &DbPool,
        query_options: &QueryOptions,
    ) -> Result<Vec<HubuumClassRelation>, ApiError> {
        self.search_relations_from_backend(pool, query_options)
            .await
    }
}

#[allow(dead_code)]
pub trait ObjectRelationsFromUser: SelfAccessors<User> + GroupAccessors
where
    for<'a> &'a Self: GroupAccessors,
{
    async fn get_related_objects<O, C>(
        &self,
        pool: &DbPool,
        source_object: &O,
        target_class: &C,
    ) -> Result<Vec<HubuumObjectTransitiveLink>, ApiError>
    where
        O: SelfAccessors<HubuumObject> + Clone + Send + Sync,
        C: SelfAccessors<HubuumClass> + Clone + Send + Sync;
}

#[allow(dead_code)]
pub trait ObjectRelationMemberships
where
    Self: SelfAccessors<HubuumObject> + Clone + Send + Sync,
{
    async fn is_member_of_class_relation(
        &self,
        pool: &DbPool,
        class_relation: &HubuumClassRelation,
    ) -> Result<bool, ApiError> {
        self.is_member_of_class_relation_from_backend(pool, class_relation)
            .await
    }

    async fn object_relation<O, C>(
        &self,
        pool: &DbPool,
        class: &C,
        target_object: &O,
    ) -> Result<HubuumObjectRelation, ApiError>
    where
        C: SelfAccessors<HubuumClass> + Clone + Send + Sync,
        O: SelfAccessors<HubuumObject> + Clone + Send + Sync,
    {
        self.object_relation_from_backend(pool, class, target_object)
            .await
    }

    async fn related_objects<C>(
        &self,
        pool: &DbPool,
        class: &C,
        query_params: &[ParsedQueryParam],
    ) -> Result<Vec<HubuumObject>, ApiError>
    where
        Self: SelfAccessors<HubuumObject> + Clone + Send + Sync,
        C: SelfAccessors<HubuumClass> + Clone + Send + Sync,
    {
        self.related_objects_from_backend(pool, class, query_params)
            .await
    }
}

impl ObjectRelationMemberships for HubuumObject {}
impl ObjectRelationMemberships for HubuumObjectID {}
