use crate::db::{DbPool, with_connection, with_transaction};
use crate::models::user::User;

use diesel::prelude::*;

use crate::errors::ApiError;

pub fn get_user_by_id(pool: &DbPool, user_id: i32) -> Result<User, ApiError> {
    use crate::schema::users::dsl::{id, users};

    with_connection(pool, |conn| {
        users.filter(id.eq(user_id)).first::<User>(conn)
    })
}

/// Sentinel password value set during anonymization — not a valid Argon2 PHC hash,
/// so verification can never succeed.
const ANONYMIZED_PASSWORD: &str = "!anonymized-no-login";

/// GDPR erasure: tombstone a user's principal name and PII
/// (proper_name/email/password), stamp `anonymized_at`, and revoke their tokens,
/// in one transaction. History rows are untouched — they only ever held the
/// integer actor/principal id, now a pseudonym.
pub async fn anonymize_user(pool: &DbPool, target_id: i32) -> Result<(), ApiError> {
    use crate::schema::principals::dsl as p;
    use crate::schema::tokens::dsl as t;
    use crate::schema::users::dsl as u;
    use diesel::prelude::*;

    with_transaction(pool, |conn| -> Result<(), ApiError> {
        let updated = diesel::update(u::users.filter(u::id.eq(target_id)))
            .set((
                u::proper_name.eq::<Option<String>>(None),
                u::email.eq::<Option<String>>(None),
                u::password.eq(ANONYMIZED_PASSWORD),
                u::anonymized_at.eq(diesel::dsl::now),
            ))
            .execute(conn)?;
        if updated == 0 {
            return Err(ApiError::NotFound(format!("User {target_id} not found")));
        }

        diesel::update(p::principals.filter(p::id.eq(target_id)))
            .set(p::name.eq(format!("anonymized-{target_id}")))
            .execute(conn)?;
        diesel::update(
            t::tokens
                .filter(t::principal_id.eq(target_id))
                .filter(t::revoked_at.is_null()),
        )
        .set(t::revoked_at.eq(diesel::dsl::now))
        .execute(conn)?;
        Ok(())
    })
}
