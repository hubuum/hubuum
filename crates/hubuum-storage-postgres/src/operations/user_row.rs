use chrono::NaiveDateTime;
use diesel::{Queryable, Selectable};
use hubuum_domain::UserId;
use hubuum_storage_core::StorageUser;
use serde_json::{Value, json};

use crate::{PostgresRevision, PostgresStorageError};

#[derive(Clone, Queryable, Selectable)]
#[diesel(table_name = crate::schema::users)]
pub(crate) struct UserRow {
    pub(crate) id: i32,
    #[diesel(column_name = kind)]
    pub(crate) _kind: String,
    pub(crate) password: Option<String>,
    pub(crate) proper_name: Option<String>,
    pub(crate) email: Option<String>,
    pub(crate) created_at: NaiveDateTime,
    pub(crate) updated_at: NaiveDateTime,
    pub(crate) anonymized_at: Option<NaiveDateTime>,
}

impl UserRow {
    pub(crate) fn into_storage(self) -> Result<StorageUser, PostgresStorageError> {
        crate::validate_persisted(
            "user",
            StorageUser::try_new(
                UserId::new(self.id)?,
                self.password,
                self.proper_name,
                self.email,
                self.created_at.and_utc(),
                self.updated_at.and_utc(),
                self.anonymized_at.map(|timestamp| timestamp.and_utc()),
            ),
        )
    }

    pub(super) fn snapshot(&self, name: &str, revision: PostgresRevision) -> Value {
        json!({
            "id": self.id,
            "name": name,
            "proper_name": self.proper_name,
            "email": self.email,
            "revision": revision,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        })
    }
}
