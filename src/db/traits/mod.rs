pub mod active_tokens;
pub mod class;
pub mod is_active;
pub mod namespace;
pub mod object;
pub mod relations;
pub mod user;

#[allow(unused_imports)]
pub use user::UserPermissions;

use crate::errors::ApiError;
use crate::models::search::{FilterField, ParsedQueryParam, QueryOptions};
use crate::models::{
    HubuumClass, HubuumClassRelation, HubuumClassRelationTransitive, HubuumObject, HubuumObjectID,
    HubuumObjectRelation, HubuumObjectTransitiveLink, Namespace, User, UserToken,
};
use crate::traits::{GroupAccessors, SelfAccessors};
use crate::{date_search, numeric_search, string_search, trace_query};

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
    Self: SelfAccessors<HubuumClass>,
{
    async fn transitive_relations(
        &self,
        pool: &DbPool,
    ) -> Result<Vec<HubuumClassRelationTransitive>, ApiError> {
        use crate::schema::hubuumclass_closure::dsl::*;
        use diesel::prelude::*;

        with_connection(pool, |conn| {
            hubuumclass_closure
                .or_filter(ancestor_class_id.eq(self.id()))
                .or_filter(descendant_class_id.eq(self.id()))
                .load::<HubuumClassRelationTransitive>(conn)
        })
    }

    // We typically end up searching, so this interface is rarely used.
    #[allow(dead_code)]
    async fn relations(&self, pool: &DbPool) -> Result<Vec<HubuumClassRelation>, ApiError> {
        use crate::schema::hubuumclass_relation::dsl::*;
        use diesel::prelude::*;

        with_connection(pool, |conn| {
            hubuumclass_relation
                .or_filter(from_hubuum_class_id.eq(self.id()))
                .or_filter(to_hubuum_class_id.eq(self.id()))
                .load::<HubuumClassRelation>(conn)
        })
    }

    async fn search_relations(
        &self,
        pool: &DbPool,
        query_options: &QueryOptions,
    ) -> Result<Vec<HubuumClassRelation>, ApiError> {
        use crate::schema::hubuumclass_relation::dsl::*;
        use diesel::prelude::*;

        let query_params = query_options.filters.clone();
        let mut base_query = hubuumclass_relation.into_boxed();
        for param in query_params {
            let operator = param.operator.clone();
            match param.field {
                FilterField::Id => {
                    numeric_search!(base_query, param, operator, id)
                }
                FilterField::ClassFrom => {
                    numeric_search!(base_query, param, operator, from_hubuum_class_id)
                }
                FilterField::ClassTo => {
                    numeric_search!(base_query, param, operator, to_hubuum_class_id)
                }
                FilterField::CreatedAt => {
                    date_search!(base_query, param, operator, created_at)
                }
                FilterField::UpdatedAt => {
                    date_search!(base_query, param, operator, updated_at)
                }
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for class relations",
                        param.field
                    )))
                }
            }
        }

        trace_query!(base_query, "Searching relations");

        with_connection(pool, |conn| {
            base_query
                .select(hubuumclass_relation::all_columns())
                .distinct() // TODO: Is it the joins that makes this required?
                .load::<HubuumClassRelation>(conn)
        })
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
        use crate::schema::hubuumclass_relation::dsl as class_rel;
        use crate::schema::hubuumobject_relation::dsl as obj_rel;
        use diesel::prelude::*;

        with_connection(pool, |conn| {
            obj_rel::hubuumobject_relation
                .inner_join(class_rel::hubuumclass_relation)
                .filter(
                    obj_rel::from_hubuum_object_id
                        .eq(self.id())
                        .or(obj_rel::to_hubuum_object_id.eq(self.id())),
                )
                .filter(class_rel::id.eq(class_relation.id))
                .select(obj_rel::id)
                .first::<i32>(conn)
                .optional()
        })
        .map(|result| result.is_some())
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
        use crate::schema::hubuumclass_relation::dsl as class_rel;
        use crate::schema::hubuumobject_relation::dsl as obj_rel;
        use diesel::prelude::*;

        let (from, to) = (self.id(), target_object.id());
        let (from, to) = if from > to { (to, from) } else { (from, to) };

        with_connection(pool, |conn| {
            obj_rel::hubuumobject_relation
                .inner_join(class_rel::hubuumclass_relation)
                .filter(
                    obj_rel::from_hubuum_object_id
                        .eq(from)
                        .and(obj_rel::to_hubuum_object_id.eq(to)),
                )
                .filter(
                    class_rel::from_hubuum_class_id
                        .eq(class.id())
                        .or(class_rel::to_hubuum_class_id.eq(class.id())),
                )
                .select(obj_rel::hubuumobject_relation::all_columns())
                .first::<HubuumObjectRelation>(conn)
        })
    }

    async fn related_objects<C>(
        &self,
        pool: &DbPool,
        class: &C,
        query_params: &Vec<ParsedQueryParam>,
    ) -> Result<Vec<HubuumObject>, ApiError>
    where
        Self: SelfAccessors<HubuumObject> + Clone + Send + Sync,
        C: SelfAccessors<HubuumClass> + Clone + Send + Sync,
    {
        use crate::schema::hubuumclass_relation::dsl as class_rel;
        use crate::schema::hubuumobject::dsl as obj;
        use crate::schema::hubuumobject_relation::dsl as obj_rel;
        use diesel::prelude::*;

        let mut base_query = obj::hubuumobject.into_boxed();
        for param in query_params {
            let operator = param.operator.clone();
            match param.field {
                FilterField::CreatedAt => {
                    date_search!(base_query, param, operator, obj::created_at)
                }
                FilterField::UpdatedAt => {
                    date_search!(base_query, param, operator, obj::updated_at)
                }
                FilterField::Namespaces => {
                    numeric_search!(base_query, param, operator, obj::namespace_id)
                }
                FilterField::Description => {
                    string_search!(base_query, param, operator, obj::description)
                }
                FilterField::Name => {
                    string_search!(base_query, param, operator, obj::name)
                }
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for objects",
                        param.field
                    )))
                }
            }
        }

        with_connection(pool, |conn| {
            base_query
                .inner_join(
                    obj_rel::hubuumobject_relation.on(obj::id.eq(obj_rel::from_hubuum_object_id)),
                )
                .inner_join(
                    class_rel::hubuumclass_relation
                        .on(obj_rel::class_relation_id.eq(class_rel::id)),
                )
                .filter(
                    obj_rel::from_hubuum_object_id
                        .eq(self.id())
                        .or(obj_rel::to_hubuum_object_id.eq(self.id())),
                )
                .filter(
                    class_rel::from_hubuum_class_id
                        .eq(class.id())
                        .or(class_rel::to_hubuum_class_id.eq(class.id())),
                )
                .select(obj::hubuumobject::all_columns())
                .distinct()
                .load::<HubuumObject>(conn)
        })
    }
}

impl ObjectRelationMemberships for HubuumObject {}
impl ObjectRelationMemberships for HubuumObjectID {}
