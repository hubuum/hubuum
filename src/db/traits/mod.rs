pub mod active_tokens;
pub mod authz;
pub mod backup;
pub mod bootstrap;
pub mod class;
pub mod collection;
pub mod computed_field;
pub mod event_delivery;
pub mod event_fanout;
pub mod event_observability;
pub mod event_retention;
pub mod event_subscription;
pub mod events;
pub mod export_template;
pub mod external_identity;
pub mod group;
pub mod history;
pub mod identity;
pub mod is_active;
pub mod meta;
pub mod metrics;
pub mod object;
pub mod permissions;
pub mod principal;
pub mod relations;
pub mod remote_target;
pub(crate) mod resource_scope;
pub mod restore;
pub mod search;
pub mod service_account;
pub mod task;
pub mod task_import;
pub mod token;
pub mod user;

pub use user::UserPermissions;

use super::{DbPool, with_connection};
use crate::bind_transitive_filter_params;
use crate::db::traits::relations::{
    ObjectRelationMembershipsBackend, SelfRelationsBackend, max_transitive_depth_from_config,
    parse_transitive_filter_params,
};
use crate::errors::ApiError;
use crate::models::search::{ParsedQueryParam, QueryOptions};
use crate::models::{
    Collection, HubuumClass, HubuumClassRelation, HubuumClassRelationTransitive, HubuumObject,
    HubuumObjectID, HubuumObjectRelation, HubuumObjectTransitiveLink, PrincipalToken, User,
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
    async fn tokens(&self, pool: &DbPool) -> Result<Vec<PrincipalToken>, ApiError>;
    async fn tokens_paginated_with_total_count(
        &self,
        pool: &DbPool,
        query_options: &QueryOptions,
    ) -> Result<(Vec<PrincipalToken>, i64), ApiError>;
}

/// Trait for getting the collection(s) of a structure from the backend database.
///
/// By default, this returns the singular collection of the structure in question.
/// For relations, where we have two collections (one for each class or object),
/// the trait is implemented to return a tuple of the two collections.
pub trait GetCollection<T = Collection> {
    async fn collection_from_backend(&self, pool: &DbPool) -> Result<T, ApiError>;
}

/// Trait for getting the classes(s) of a structure from the backend database.
///
/// By default, this returns the singular class of the structure in question.
/// For relations, where we have two classes (one for each structure), the
/// trait is implemented to return a tuple of the two collections.
pub trait GetClass<T = HubuumClass> {
    async fn class_from_backend(&self, pool: &DbPool) -> Result<T, ApiError>;
}

/// Trait for getting the object(s) of a structure from the backend database.
///
/// By default, this returns the singular object of the structure in question.
/// For relations, where we have two objects (one for each structure), the
/// trait is implemented to return a tuple of the two objects.
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
        use crate::pagination::{cursor_filter_sql, normalized_sorts, order_sql_clause};
        use diesel::sql_query;
        use diesel::sql_types::Integer;

        let filter = parse_transitive_filter_params(query_options)?;
        let sorts = normalized_sorts::<HubuumClassRelationTransitive>(&query_options.sort)?;

        let mut raw_sql = String::from(
            "SELECT ancestor_class_id, descendant_class_id, depth, path
             FROM get_bidirectionally_related_classes(
                 $1, ARRAY[]::INT[], $2, $3, $4, $5, $6, $7, $8
             )
             WHERE (ancestor_class_id = $1 OR descendant_class_id = $1)",
        );

        if let Some(cursor_sql) = cursor_filter_sql::<HubuumClassRelationTransitive>(
            &sorts,
            query_options.cursor.as_deref(),
        )? {
            raw_sql.push_str("\n  AND ");
            raw_sql.push_str(&cursor_sql);
        }

        let order_by = sorts
            .iter()
            .map(order_sql_clause::<HubuumClassRelationTransitive>)
            .collect::<Result<Vec<_>, _>>()?
            .join(", ");
        raw_sql.push_str(&format!("\nORDER BY {order_by}"));

        if let Some(limit) = query_options.limit {
            raw_sql.push_str(&format!("\nLIMIT {limit}"));
        }

        with_connection(pool, async |conn| {
            let query = bind_transitive_filter_params!(
                sql_query(raw_sql)
                    .bind::<Integer, _>(self.id())
                    .bind::<Integer, _>(max_transitive_depth_from_config()),
                filter
            );

            diesel_async::RunQueryDsl::load::<HubuumClassRelationTransitive>(query, conn).await
        })
        .await
    }

    // We typically end up searching, so this interface is rarely used.
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
