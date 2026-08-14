//! Validation invariants shared by every backend-neutral domain id newtype.
//!
//! Tests over both supported integer widths guard the shared contract: positive ids round-trip,
//! non-positive ids are rejected, and `Deserialize` routes through the validating constructor so
//! `web::Path<XID>` rejects invalid ids at the API edge.

use crate::errors::ApiError;
use crate::models::{
    CollectionID, ComputedFieldDefinitionID, EventDeliveryID, EventSinkID, EventSubscriptionID,
    ExportTemplateID, GroupID, HubuumClassID, HubuumClassRelationID, HubuumObjectID,
    HubuumObjectRelationID, NewCollectionWithAssignee, NewServiceAccount, RemoteTargetID,
    RestoreJobID, ServiceAccountID, TaskID, TokenID, UserID,
};

macro_rules! assert_id_newtype_validates {
    ($($t:ty),+ $(,)?) => {{
        $(
            // Positive ids round-trip through `new` / `id`.
            assert_eq!(<$t>::new(1).unwrap().id(), 1, "{}::new(1)", stringify!($t));
            assert_eq!(<$t>::new(i32::MAX).unwrap().id(), i32::MAX);

            // Non-positive ids are rejected with a 400-class error.
            for invalid in [0, -1, i32::MIN] {
                let err: ApiError = <$t>::new(invalid).unwrap_err().into();
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

macro_rules! assert_i64_id_newtype_validates {
    ($($t:ty),+ $(,)?) => {{
        $(
            assert_eq!(<$t>::new(1).unwrap().id(), 1);
            assert_eq!(<$t>::new(i64::MAX).unwrap().id(), i64::MAX);
            for invalid in [0_i64, -1, i64::MIN] {
                let err: ApiError = <$t>::new(invalid).unwrap_err().into();
                assert!(
                    matches!(err, ApiError::BadRequest(_)),
                    "{}::new({invalid}) should be BadRequest, got {err:?}",
                    stringify!($t)
                );
            }
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
        CollectionID,
        GroupID,
        ExportTemplateID,
        RemoteTargetID,
        ServiceAccountID,
        ComputedFieldDefinitionID,
        TaskID,
        EventSinkID,
        EventSubscriptionID,
        TokenID,
    );
}

#[test]
fn all_i64_id_newtypes_reject_invalid_ids() {
    assert_i64_id_newtype_validates!(EventDeliveryID, RestoreJobID);
}

#[test]
fn new_collection_assignee_rejects_a_non_positive_group_id() {
    let request = serde_json::json!({
        "name": "assets",
        "description": "Assets",
        "group_id": 0,
        "parent_collection_id": null
    });

    assert!(serde_json::from_value::<NewCollectionWithAssignee>(request).is_err());
}

#[test]
fn new_service_account_owner_rejects_a_non_positive_group_id() {
    let request = serde_json::json!({
        "name": "dns-sync",
        "description": "DNS automation",
        "owner_group_id": 0
    });

    assert!(serde_json::from_value::<NewServiceAccount>(request).is_err());
}
