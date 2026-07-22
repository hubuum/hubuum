use diesel::prelude::*;

use crate::models::{TokenResourceScopeIds, TokenScope};
use crate::schema::{collections, hubuumclass, hubuumobject};

pub(crate) type CollectionScopePredicate<'a> = diesel::dsl::EqAny<collections::id, &'a [i32]>;
pub(crate) type ClassScopePredicate<'a> = diesel::dsl::Or<
    diesel::dsl::EqAny<hubuumclass::collection_id, &'a [i32]>,
    diesel::dsl::EqAny<hubuumclass::id, &'a [i32]>,
>;
pub(crate) type ObjectScopePredicate<'a> = diesel::dsl::Or<
    diesel::dsl::Or<
        diesel::dsl::EqAny<hubuumobject::collection_id, &'a [i32]>,
        diesel::dsl::EqAny<hubuumobject::hubuum_class_id, &'a [i32]>,
    >,
    diesel::dsl::EqAny<hubuumobject::id, &'a [i32]>,
>;

pub(crate) fn resource_scope_ids(scopes: Option<&TokenScope>) -> Option<TokenResourceScopeIds<'_>> {
    scopes.and_then(TokenScope::resource_ids)
}

pub(crate) fn collection_scope_predicate(
    scope: TokenResourceScopeIds<'_>,
) -> CollectionScopePredicate<'_> {
    collections::id.eq_any(scope.collection_ids())
}

pub(crate) fn class_scope_predicate(scope: TokenResourceScopeIds<'_>) -> ClassScopePredicate<'_> {
    hubuumclass::collection_id
        .eq_any(scope.collection_ids())
        .or(hubuumclass::id.eq_any(scope.class_ids()))
}

pub(crate) fn object_scope_predicate(scope: TokenResourceScopeIds<'_>) -> ObjectScopePredicate<'_> {
    hubuumobject::collection_id
        .eq_any(scope.collection_ids())
        .or(hubuumobject::hubuum_class_id.eq_any(scope.class_ids()))
        .or(hubuumobject::id.eq_any(scope.object_ids()))
}
