use super::*;
use crate::models::{HistoryDirection, HistoryMarker};
use hubuum_domain::HistoryRecordId;
use hubuum_storage_core::{
    StorageHistoryEntry, StorageHistoryEntryQuery, StorageHistoryNavigationQuery,
    StorageHistoryResource, StorageHistorySelection,
};

#[rstest::rstest]
#[case::class(false)]
#[case::object(true)]
#[actix_web::test]
async fn every_backend_navigates_history(#[case] object: bool) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture = create_backend_object_fixture(
            &backend,
            &prefix("timeline_contract"),
            vec![serde_json::json!({"value": 1})],
        )
        .await;
        let resource = if object {
            StorageHistoryResource::Object {
                class_id: fixture.class.id(),
                object_id: fixture.objects[0].id(),
            }
        } else {
            StorageHistoryResource::Class(fixture.class.id())
        };
        let options = QueryOptions::new(
            Vec::new(),
            HistoryDirection::Older.sorts(),
            Some(10),
            None,
            true,
        )
        .unwrap();
        let options = prepare_db_pagination::<HistoryMarker>(&options).unwrap();
        let query = || {
            StorageHistoryNavigationQuery::try_new(
                resource,
                options.clone(),
                StorageHistoryCollectionScope::All,
            )
            .unwrap()
        };
        let (rows, count) = backend
            .list_history_markers(query())
            .await
            .unwrap()
            .into_parts();
        assert_eq!(count, Some(rows.len() as i64));
        assert!(!rows.is_empty());
        let (actual, collection_id, _, metadata) = rows.into_iter().next().unwrap().into_parts();
        assert_eq!(actual, resource);
        assert_eq!(collection_id, fixture.collection.id());
        let id = metadata.into_parts().history_entry_id();
        let entry = backend
            .get_history_entry(StorageHistoryEntryQuery::new(resource, id))
            .await
            .unwrap()
            .unwrap();
        let metadata = match entry {
            StorageHistoryEntry::Class(row) => row.into_parts().1,
            StorageHistoryEntry::Object(row) => row.into_parts().1,
        };
        assert_eq!(metadata.into_parts().history_entry_id(), id);
        assert!(
            backend
                .list_history_markers(query().selection(StorageHistorySelection::Entry(
                    HistoryRecordId::new(i64::MAX).unwrap()
                )))
                .await
                .unwrap()
                .into_parts()
                .0
                .is_empty()
        );
        let wrong = if object {
            StorageHistoryResource::Object {
                class_id: fixture.class.id(),
                object_id: ObjectId::new(i32::MAX).unwrap(),
            }
        } else {
            StorageHistoryResource::Class(ClassId::new(i32::MAX).unwrap())
        };
        assert!(
            backend
                .get_history_entry(StorageHistoryEntryQuery::new(wrong, id))
                .await
                .unwrap()
                .is_none()
        );
        delete_backend_object_fixture(&backend, fixture).await;
    }
}

#[rstest::rstest]
#[case::counted(true)]
#[case::uncounted(false)]
#[actix_web::test]
async fn every_backend_pages_history_with_optional_totals(
    #[values(false, true)] object: bool,
    #[values(false, true)] descending: bool,
    #[case] include_total: bool,
) {
    use crate::models::{HubuumClassID, HubuumObjectID, UpdateHubuumClass, UpdateHubuumObject};
    use crate::traits::CanUpdate;
    use hubuum_query::{CursorValue, FilterField, SortParam, encode_cursor_values};
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture = create_backend_object_fixture(
            &backend,
            &prefix("timeline_pages"),
            vec![serde_json::json!({})],
        )
        .await;
        let resource = if object {
            StorageHistoryResource::Object {
                class_id: fixture.class.id(),
                object_id: fixture.objects[0].id(),
            }
        } else {
            StorageHistoryResource::Class(fixture.class.id())
        };
        for version in ["second", "third"] {
            if object {
                UpdateHubuumObject {
                    name: None,
                    collection_id: None,
                    hubuum_class_id: None,
                    description: Some(version.into()),
                    data: None,
                }
                .update(
                    &backend,
                    HubuumObjectID::new(fixture.objects[0].id().id()).unwrap(),
                    &EventContext::system(),
                )
                .await
                .unwrap();
            } else {
                UpdateHubuumClass {
                    name: None,
                    collection_id: None,
                    description: Some(version.into()),
                    json_schema: None,
                    validate_schema: None,
                }
                .update(
                    &backend,
                    HubuumClassID::new(fixture.class.id().id()).unwrap(),
                    &EventContext::system(),
                )
                .await
                .unwrap();
            }
        }
        let sorts = vec![
            SortParam::new(FilterField::ValidFrom, descending),
            SortParam::new(FilterField::HistoryId, descending),
        ];
        let options =
            QueryOptions::new(vec![], sorts.clone(), Some(1), None, include_total).unwrap();
        let query = StorageHistoryNavigationQuery::try_new(
            resource,
            options.clone(),
            StorageHistoryCollectionScope::All,
        )
        .unwrap();
        let (first, total) = backend
            .list_history_markers(query)
            .await
            .unwrap()
            .into_parts();
        assert_eq!(total, include_total.then_some(3));
        let (_, _, _, metadata) = first.into_iter().next().unwrap().into_parts();
        let metadata = metadata.into_parts();
        let first_id = metadata.history_entry_id();
        let cursor = encode_cursor_values(
            &sorts,
            vec![
                CursorValue::DateTime(metadata.valid_from().naive_utc()),
                CursorValue::Integer(first_id.id()),
            ],
        )
        .unwrap();
        let mut options = options;
        options.set_cursor(Some(cursor)).unwrap();
        options.set_limit(Some(5)).unwrap();
        let query = StorageHistoryNavigationQuery::try_new(
            resource,
            options,
            StorageHistoryCollectionScope::All,
        )
        .unwrap();
        let (next, total) = backend
            .list_history_markers(query)
            .await
            .unwrap()
            .into_parts();
        assert_eq!(total, include_total.then_some(3));
        assert_eq!(next.len(), 2);
        for row in next {
            let id = row.into_parts().3.into_parts().history_entry_id();
            assert!(if descending {
                id.id() < first_id.id()
            } else {
                id.id() > first_id.id()
            });
        }
        delete_backend_object_fixture(&backend, fixture).await;
    }
}
