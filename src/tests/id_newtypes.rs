//! Validation invariants shared by every `int_id_newtype!`-generated id newtype.
//!
//! All id newtypes are produced by the same macro, so one test over the full set guards the
//! contract: positive ids round-trip, non-positive ids are rejected, and `Deserialize` routes
//! through the validating constructor so `web::Path<XID>` rejects invalid ids at the API edge.

use crate::errors::ApiError;
use crate::models::{
    GroupID, HubuumClassID, HubuumClassRelationID, HubuumObjectID, HubuumObjectRelationID,
    NamespaceID, ReportTemplateID, TaskID, UserID,
};

macro_rules! assert_id_newtype_validates {
    ($($t:ty),+ $(,)?) => {{
        $(
            // Positive ids round-trip through `new` / `id`.
            assert_eq!(<$t>::new(1).unwrap().id(), 1, "{}::new(1)", stringify!($t));
            assert_eq!(<$t>::new(i32::MAX).unwrap().id(), i32::MAX);

            // Non-positive ids are rejected with a 400-class error.
            for invalid in [0, -1, i32::MIN] {
                let err = <$t>::new(invalid).unwrap_err();
                assert!(
                    matches!(err, ApiError::BadRequest(_)),
                    "{}::new({invalid}) should be BadRequest, got {err:?}",
                    stringify!($t)
                );
            }

            // `Deserialize` routes through `new`, so an invalid path/body id never produces a value.
            assert_eq!(serde_json::from_str::<$t>("7").unwrap().id(), 7);
            assert!(serde_json::from_str::<$t>("0").is_err());
            assert!(serde_json::from_str::<$t>("-3").is_err());
        )+
    }};
}

#[test]
fn all_id_newtypes_reject_invalid_ids() {
    assert_id_newtype_validates!(
        HubuumObjectID,
        HubuumClassID,
        HubuumClassRelationID,
        HubuumObjectRelationID,
        UserID,
        NamespaceID,
        GroupID,
        ReportTemplateID,
        TaskID,
    );
}
