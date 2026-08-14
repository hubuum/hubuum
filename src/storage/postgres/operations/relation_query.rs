use crate::errors::ApiError;
use crate::models::{HubuumClassID, HubuumObjectID};
use crate::storage::postgres::PostgresPool;
use crate::storage::postgres::operations::relation_rows::{
    class_graph_to_storage, object_graph_to_storage,
};
use crate::storage::postgres::operations::user::UserSearchBackend;
use crate::storage::postgres::operations::visibility::{principal, token_scope};
use crate::storage::{
    RelationGraphQuery, RelationPage, StorageClassGraphRow, StorageObjectGraphRow,
};

pub(crate) async fn related_classes(
    pool: &PostgresPool,
    query: RelationGraphQuery,
) -> Result<RelationPage<StorageClassGraphRow>, ApiError> {
    let include_total = query.options().include_total;
    let (class_id, options, visibility) = query.into_parts();
    let principal = principal(&visibility)?;
    let scope = token_scope(&visibility)?;
    let class_id = HubuumClassID::new(class_id)?;
    let (rows, total) = principal
        .classes_related_to_page_from_backend_with_admin_status(
            pool,
            class_id,
            options,
            visibility.is_admin(),
            scope.as_ref(),
        )
        .await?;
    Ok(RelationPage::new(
        rows.into_iter().map(class_graph_to_storage).collect(),
        include_total.then_some(total),
    ))
}

pub(crate) async fn related_objects(
    pool: &PostgresPool,
    query: RelationGraphQuery,
) -> Result<RelationPage<StorageObjectGraphRow>, ApiError> {
    let include_total = query.options().include_total;
    let (object_id, options, visibility) = query.into_parts();
    let principal = principal(&visibility)?;
    let scope = token_scope(&visibility)?;
    let object_id = HubuumObjectID::new(object_id)?;
    let (rows, total) = principal
        .objects_related_to_page_from_backend_with_admin_status(
            pool,
            object_id,
            options,
            visibility.is_admin(),
            scope.as_ref(),
        )
        .await?;
    Ok(RelationPage::new(
        rows.into_iter().map(object_graph_to_storage).collect(),
        include_total.then_some(total),
    ))
}
