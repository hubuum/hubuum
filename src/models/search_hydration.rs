use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::errors::ApiError;
use crate::models::{
    HubuumClassID, HubuumObject, HubuumObjectID, HubuumObjectRelation, StructuredClassSelector,
};

/// Opt-in, lazy hydration of one search-result object's visible neighborhood.
/// This request does not execute or modify the root search. The class selects
/// related endpoints; intermediate path objects may belong to other classes.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SearchHydrationRequest {
    #[schema(minimum = 1, maximum = 1)]
    pub version: u8,
    pub root_class_id: HubuumClassID,
    pub root_object_id: HubuumObjectID,
    pub class: StructuredClassSelector,
    #[schema(minimum = 1, maximum = 10)]
    pub depth: u8,
    /// Maximum visible paths, not an exact count of related objects.
    #[schema(minimum = 1, maximum = 50)]
    pub limit: u8,
}

pub(crate) struct ValidatedSearchHydration(SearchHydrationRequest);

impl TryFrom<SearchHydrationRequest> for ValidatedSearchHydration {
    type Error = ApiError;

    fn try_from(request: SearchHydrationRequest) -> Result<Self, Self::Error> {
        if request.version != 1
            || !(1..=10).contains(&request.depth)
            || !(1..=50).contains(&request.limit)
        {
            return Err(ApiError::BadRequest(
                "Hydration requires version 1, depth 1–10 and path limit 1–50".into(),
            ));
        }
        request.class.validate()?;
        Ok(Self(request))
    }
}

impl ValidatedSearchHydration {
    pub(crate) fn request(&self) -> &SearchHydrationRequest {
        &self.0
    }
}

/// One visible bidirectional path. Relation IDs correspond to successive
/// object ID pairs; an edge's stored orientation need not match traversal.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct SearchHydrationPath {
    pub object_ids: Vec<i32>,
    pub relation_ids: Vec<i32>,
}

/// A bounded visible neighborhood, independent of root search pagination.
/// Objects and relations are unique by ID and include only the returned paths.
/// Class/collection IDs are references, not authorization to read their names.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct SearchHydrationResponse {
    pub version: u8,
    pub root_object_id: i32,
    pub target_class_id: i32,
    pub depth: u8,
    pub limit: u8,
    pub objects: Vec<HubuumObject>,
    pub relations: Vec<HubuumObjectRelation>,
    pub paths: Vec<SearchHydrationPath>,
    /// Additional visible paths exist. No count of hidden resources is exposed.
    /// This version does not paginate hydration; narrow the class/depth or raise
    /// the path limit, up to 50, to inspect a different bounded scope.
    pub truncated: bool,
}

#[cfg(test)]
mod tests {
    use super::{SearchHydrationRequest, ValidatedSearchHydration};
    use rstest::rstest;
    use serde_json::json;

    #[rstest]
    #[case(1, 1, 1, true)]
    #[case(1, 10, 50, true)]
    #[case(2, 1, 1, false)]
    #[case(1, 0, 1, false)]
    #[case(1, 11, 1, false)]
    #[case(1, 1, 0, false)]
    #[case(1, 1, 51, false)]
    fn hydration_validates_bounds(
        #[case] version: u8,
        #[case] depth: u8,
        #[case] limit: u8,
        #[case] valid: bool,
    ) {
        let request: SearchHydrationRequest = serde_json::from_value(json!({
            "version": version, "root_class_id": 1, "root_object_id": 2,
            "class": {"id": 3}, "depth": depth, "limit": limit
        }))
        .unwrap();
        assert_eq!(ValidatedSearchHydration::try_from(request).is_ok(), valid);
    }
}
