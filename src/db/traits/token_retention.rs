use crate::db::prelude::*;
use chrono::{Duration, NaiveDateTime, Utc};
use diesel::sql_types::{BigInt, Bool, Timestamp};

use crate::config::MIN_TOKEN_RETENTION_PURGE_BATCH_SIZE;
use crate::db::{DbConnection, DbPool, with_transaction};
use crate::errors::ApiError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenRetentionSettings {
    pub retention_days: i64,
    pub token_lifetime_hours: i64,
    pub batch_size: usize,
}

impl TokenRetentionSettings {
    fn validate(self) -> Result<Self, ApiError> {
        if self.retention_days <= 0 {
            return Err(ApiError::BadRequest(
                "token retention days must be greater than 0".to_string(),
            ));
        }
        if self.token_lifetime_hours <= 0 {
            return Err(ApiError::BadRequest(
                "token lifetime hours must be greater than 0".to_string(),
            ));
        }
        if self.batch_size < MIN_TOKEN_RETENTION_PURGE_BATCH_SIZE {
            return Err(ApiError::BadRequest(format!(
                "token retention purge batch size must be at least \
                 {MIN_TOKEN_RETENTION_PURGE_BATCH_SIZE}"
            )));
        }

        Ok(self)
    }
}

const TOKEN_RETENTION_LOCK_KEY: i64 = 4_850_188_191_125_219;

#[derive(Debug, QueryableByName)]
struct AdvisoryLockRow {
    #[diesel(sql_type = Bool)]
    locked: bool,
}

pub(crate) async fn try_acquire_token_retention_lock(
    conn: &mut DbConnection,
) -> Result<bool, ApiError> {
    Ok(
        diesel::sql_query("SELECT pg_try_advisory_xact_lock($1) AS locked")
            .bind::<BigInt, _>(TOKEN_RETENTION_LOCK_KEY)
            .get_result::<AdvisoryLockRow>(conn)
            .await?
            .locked,
    )
}

/// Delete one bounded batch of tokens whose expiry is older than the configured
/// retention window.
///
/// Explicit `expires_at` values remain authoritative. Tokens without one use
/// `issued + token_lifetime_hours` as their effective expiry, matching the
/// authentication predicate. Foreign keys cascade scope-row deletion and set
/// task `submitted_token_id` provenance to null.
pub async fn purge_expired_token_batch(
    pool: &DbPool,
    settings: TokenRetentionSettings,
) -> Result<usize, ApiError> {
    purge_expired_token_batch_at(pool, settings, Utc::now().naive_utc()).await
}

async fn purge_expired_token_batch_at(
    pool: &DbPool,
    settings: TokenRetentionSettings,
    now: NaiveDateTime,
) -> Result<usize, ApiError> {
    let settings = settings.validate()?;

    let retention = Duration::try_days(settings.retention_days).ok_or_else(|| {
        ApiError::BadRequest("token retention days are outside the supported range".to_string())
    })?;
    let lifetime = Duration::try_hours(settings.token_lifetime_hours).ok_or_else(|| {
        ApiError::BadRequest("token lifetime hours are outside the supported range".to_string())
    })?;
    let explicit_expiry_cutoff = now.checked_sub_signed(retention).ok_or_else(|| {
        ApiError::BadRequest("token retention cutoff is outside the supported range".to_string())
    })?;
    let implicit_issue_cutoff = explicit_expiry_cutoff
        .checked_sub_signed(lifetime)
        .ok_or_else(|| {
            ApiError::BadRequest(
                "implicit token retention cutoff is outside the supported range".to_string(),
            )
        })?;
    let batch_size = i64::try_from(settings.batch_size)
        .map_err(|_| ApiError::BadRequest("token purge batch size is too large".to_string()))?;

    with_transaction(pool, async |conn| -> Result<usize, ApiError> {
        if !try_acquire_token_retention_lock(conn).await? {
            return Ok(0);
        }

        Ok(purge_expired_token_batch_conn(
            conn,
            explicit_expiry_cutoff,
            implicit_issue_cutoff,
            batch_size,
        )
        .await?)
    })
    .await
}

async fn purge_expired_token_batch_conn(
    conn: &mut DbConnection,
    explicit_expiry_cutoff: NaiveDateTime,
    implicit_issue_cutoff: NaiveDateTime,
    batch_size: i64,
) -> Result<usize, diesel::result::Error> {
    // Give both partial-index streams an initial share of the batch. Any
    // unused implicit share is then filled from the explicit stream so a
    // one-sided backlog still uses the configured batch size.
    let explicit_share = batch_size / 2 + batch_size % 2;
    let mut deleted =
        purge_explicit_expired_tokens(conn, explicit_expiry_cutoff, explicit_share).await?;
    let remaining = batch_size.saturating_sub(deleted as i64);
    deleted += purge_implicit_expired_tokens(conn, implicit_issue_cutoff, remaining).await?;
    let remaining = batch_size.saturating_sub(deleted as i64);
    deleted += purge_explicit_expired_tokens(conn, explicit_expiry_cutoff, remaining).await?;

    Ok(deleted)
}

async fn purge_explicit_expired_tokens(
    conn: &mut DbConnection,
    expiry_cutoff: NaiveDateTime,
    limit: i64,
) -> Result<usize, diesel::result::Error> {
    if limit == 0 {
        return Ok(0);
    }

    diesel::sql_query(
        "WITH candidates AS (
             SELECT id
             FROM tokens
             WHERE expires_at IS NOT NULL
               AND expires_at <= $1
             ORDER BY expires_at ASC, id ASC
             LIMIT $2
             FOR UPDATE SKIP LOCKED
         )
         DELETE FROM tokens AS expired
         USING candidates
         WHERE expired.id = candidates.id",
    )
    .bind::<Timestamp, _>(expiry_cutoff)
    .bind::<BigInt, _>(limit)
    .execute(conn)
    .await
}

async fn purge_implicit_expired_tokens(
    conn: &mut DbConnection,
    issue_cutoff: NaiveDateTime,
    limit: i64,
) -> Result<usize, diesel::result::Error> {
    if limit == 0 {
        return Ok(0);
    }

    diesel::sql_query(
        "WITH candidates AS (
             SELECT id
             FROM tokens
             WHERE expires_at IS NULL
               AND issued <= $1
             ORDER BY issued ASC, id ASC
             LIMIT $2
             FOR UPDATE SKIP LOCKED
         )
         DELETE FROM tokens AS expired
         USING candidates
         WHERE expired.id = candidates.id",
    )
    .bind::<Timestamp, _>(issue_cutoff)
    .bind::<BigInt, _>(limit)
    .execute(conn)
    .await
}

#[cfg(test)]
mod tests {
    use diesel::sql_types::{Bool, Text};
    use rstest::rstest;

    use crate::db::traits::user::DeleteUserRecord;
    use crate::db::with_connection;
    use crate::models::{Permissions, PrincipalID, PrincipalTokenCreateRequest, Token, TokenScope};
    use crate::schema::{token_scopes, tokens};
    use crate::tests::{TestMutex, create_test_user, lock_test_mutex, test_mutex};

    use super::*;

    const TEST_RETENTION_DAYS: i64 = 10_000;
    const TEST_TOKEN_LIFETIME_HOURS: i64 = 24;
    static TOKEN_RETENTION_TEST_LOCK: TestMutex = test_mutex();

    #[derive(QueryableByName)]
    struct IndexExistsRow {
        #[diesel(sql_type = Bool)]
        exists: bool,
    }

    fn settings(batch_size: usize) -> TokenRetentionSettings {
        TokenRetentionSettings {
            retention_days: TEST_RETENTION_DAYS,
            token_lifetime_hours: TEST_TOKEN_LIFETIME_HOURS,
            batch_size,
        }
    }

    async fn create_token(
        pool: &DbPool,
        principal_id: i32,
        expires_at: Option<NaiveDateTime>,
    ) -> Token {
        PrincipalTokenCreateRequest::new(PrincipalID::new(principal_id).unwrap())
            .expires_at(expires_at)
            .create(pool, None)
            .await
            .unwrap()
    }

    async fn token_exists(pool: &DbPool, token_value: &Token) -> bool {
        let token_hash = token_value.storage_hash();
        with_connection(pool, async |conn| {
            tokens::table
                .filter(tokens::token.eq(token_hash))
                .count()
                .get_result::<i64>(conn)
                .await
        })
        .await
        .unwrap()
            == 1
    }

    #[tokio::test]
    async fn purge_deletes_explicit_expiry_after_retention() {
        let _lock = lock_test_mutex(&TOKEN_RETENTION_TEST_LOCK).await;
        let pool = crate::tests::get_test_pool();
        let user = create_test_user(&pool).await;
        let now = Utc::now().naive_utc();
        let token = create_token(
            &pool,
            user.id,
            Some(now - Duration::days(TEST_RETENTION_DAYS + 1)),
        )
        .await;

        let deleted = purge_expired_token_batch_at(&pool, settings(100), now)
            .await
            .unwrap();

        assert_eq!(deleted, 1);
        assert!(!token_exists(&pool, &token).await);
        user.delete_user_record_without_events(&pool).await.unwrap();
    }

    #[tokio::test]
    async fn purge_retains_explicit_expiry_during_retention() {
        let _lock = lock_test_mutex(&TOKEN_RETENTION_TEST_LOCK).await;
        let pool = crate::tests::get_test_pool();
        let user = create_test_user(&pool).await;
        let now = Utc::now().naive_utc();
        let token = create_token(&pool, user.id, Some(now - Duration::days(1))).await;

        let deleted = purge_expired_token_batch_at(&pool, settings(100), now)
            .await
            .unwrap();

        assert_eq!(deleted, 0);
        assert!(token_exists(&pool, &token).await);
        user.delete_user_record_without_events(&pool).await.unwrap();
    }

    #[tokio::test]
    async fn purge_deletes_implicit_expiry_after_lifetime_and_retention() {
        let _lock = lock_test_mutex(&TOKEN_RETENTION_TEST_LOCK).await;
        let pool = crate::tests::get_test_pool();
        let user = create_test_user(&pool).await;
        let now = Utc::now().naive_utc();
        let token = create_token(&pool, user.id, None).await;
        let token_hash = token.storage_hash();
        let issued_at = now - Duration::days(TEST_RETENTION_DAYS) - Duration::hours(25);
        with_connection(&pool, async move |conn| {
            diesel::update(tokens::table.filter(tokens::token.eq(token_hash)))
                .set((
                    tokens::issued.eq(issued_at),
                    tokens::expires_at.eq::<Option<NaiveDateTime>>(None),
                ))
                .execute(conn)
                .await
        })
        .await
        .unwrap();

        let deleted = purge_expired_token_batch_at(&pool, settings(100), now)
            .await
            .unwrap();

        assert_eq!(deleted, 1);
        assert!(!token_exists(&pool, &token).await);
        user.delete_user_record_without_events(&pool).await.unwrap();
    }

    #[tokio::test]
    async fn purge_respects_batch_size() {
        let _lock = lock_test_mutex(&TOKEN_RETENTION_TEST_LOCK).await;
        let pool = crate::tests::get_test_pool();
        let user = create_test_user(&pool).await;
        let now = Utc::now().naive_utc();
        let expiry = now - Duration::days(TEST_RETENTION_DAYS + 1);
        for _ in 0..=MIN_TOKEN_RETENTION_PURGE_BATCH_SIZE {
            create_token(&pool, user.id, Some(expiry)).await;
        }

        let deleted = purge_expired_token_batch_at(
            &pool,
            settings(MIN_TOKEN_RETENTION_PURGE_BATCH_SIZE),
            now,
        )
        .await
        .unwrap();
        let remaining = with_connection(&pool, async |conn| {
            tokens::table
                .filter(tokens::principal_id.eq(user.id))
                .count()
                .get_result::<i64>(conn)
                .await
        })
        .await
        .unwrap();

        assert_eq!(deleted, MIN_TOKEN_RETENTION_PURGE_BATCH_SIZE);
        assert_eq!(remaining, 1);
        user.delete_user_record_without_events(&pool).await.unwrap();
    }

    #[tokio::test]
    async fn purge_rejects_batch_below_supported_minimum() {
        let pool = crate::tests::get_test_pool();
        let error = purge_expired_token_batch_at(
            &pool,
            settings(MIN_TOKEN_RETENTION_PURGE_BATCH_SIZE - 1),
            Utc::now().naive_utc(),
        )
        .await
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "token retention purge batch size must be at least 10"
        );
    }

    #[rstest]
    #[case(
        -1,
        TEST_TOKEN_LIFETIME_HOURS,
        "token retention days must be greater than 0"
    )]
    #[case(
        0,
        TEST_TOKEN_LIFETIME_HOURS,
        "token retention days must be greater than 0"
    )]
    #[case(
        TEST_RETENTION_DAYS,
        -1,
        "token lifetime hours must be greater than 0"
    )]
    #[case(TEST_RETENTION_DAYS, 0, "token lifetime hours must be greater than 0")]
    #[tokio::test]
    async fn purge_rejects_non_positive_durations_without_deleting_tokens(
        #[case] retention_days: i64,
        #[case] token_lifetime_hours: i64,
        #[case] expected_error: &str,
    ) {
        let _lock = lock_test_mutex(&TOKEN_RETENTION_TEST_LOCK).await;
        let pool = crate::tests::get_test_pool();
        let user = create_test_user(&pool).await;
        let now = Utc::now().naive_utc();
        let token = create_token(&pool, user.id, Some(now + Duration::hours(12))).await;
        let settings = TokenRetentionSettings {
            retention_days,
            token_lifetime_hours,
            batch_size: MIN_TOKEN_RETENTION_PURGE_BATCH_SIZE,
        };

        let error = purge_expired_token_batch_at(&pool, settings, now)
            .await
            .unwrap_err();

        assert_eq!(error.to_string(), expected_error);
        assert!(token_exists(&pool, &token).await);
        user.delete_user_record_without_events(&pool).await.unwrap();
    }

    #[tokio::test]
    async fn purge_batch_advances_both_indexed_expiry_streams() {
        let _lock = lock_test_mutex(&TOKEN_RETENTION_TEST_LOCK).await;
        let pool = crate::tests::get_test_pool();
        let user = create_test_user(&pool).await;
        let now = Utc::now().naive_utc();
        let explicit = create_token(
            &pool,
            user.id,
            Some(now - Duration::days(TEST_RETENTION_DAYS + 1)),
        )
        .await;
        let implicit = create_token(&pool, user.id, None).await;
        let implicit_hash = implicit.storage_hash();
        let implicit_issued = now - Duration::days(TEST_RETENTION_DAYS) - Duration::hours(25);
        with_connection(&pool, async move |conn| {
            diesel::update(tokens::table.filter(tokens::token.eq(implicit_hash)))
                .set((
                    tokens::issued.eq(implicit_issued),
                    tokens::expires_at.eq::<Option<NaiveDateTime>>(None),
                ))
                .execute(conn)
                .await
        })
        .await
        .unwrap();

        let deleted = purge_expired_token_batch_at(
            &pool,
            settings(MIN_TOKEN_RETENTION_PURGE_BATCH_SIZE),
            now,
        )
        .await
        .unwrap();

        assert_eq!(deleted, 2);
        assert!(!token_exists(&pool, &explicit).await);
        assert!(!token_exists(&pool, &implicit).await);
        user.delete_user_record_without_events(&pool).await.unwrap();
    }

    #[tokio::test]
    async fn purge_cascades_token_scope_rows() {
        let _lock = lock_test_mutex(&TOKEN_RETENTION_TEST_LOCK).await;
        let pool = crate::tests::get_test_pool();
        let user = create_test_user(&pool).await;
        let now = Utc::now().naive_utc();
        let scope =
            TokenScope::from_request_parts(Some(vec![Permissions::ReadCollection]), None).unwrap();
        let token = PrincipalTokenCreateRequest::new(PrincipalID::new(user.id).unwrap())
            .expires_at(Some(now - Duration::days(TEST_RETENTION_DAYS + 1)))
            .scope(scope)
            .create(&pool, None)
            .await
            .unwrap();
        let token_hash = token.storage_hash();
        let token_id = with_connection(&pool, async |conn| {
            tokens::table
                .filter(tokens::token.eq(token_hash))
                .select(tokens::id)
                .first::<i32>(conn)
                .await
        })
        .await
        .unwrap();

        purge_expired_token_batch_at(&pool, settings(100), now)
            .await
            .unwrap();

        let scope_rows = with_connection(&pool, async |conn| {
            token_scopes::table
                .filter(token_scopes::token_id.eq(token_id))
                .count()
                .get_result::<i64>(conn)
                .await
        })
        .await
        .unwrap();
        assert_eq!(scope_rows, 0);
        user.delete_user_record_without_events(&pool).await.unwrap();
    }

    #[rstest]
    #[case(
        "idx_tokens_explicit_expiry_retention",
        "tokens",
        "%(expires_at, id)%",
        "%WHERE (expires_at IS NOT NULL)%"
    )]
    #[case(
        "idx_tokens_implicit_expiry_retention",
        "tokens",
        "%(issued, id)%",
        "%WHERE (expires_at IS NULL)%"
    )]
    #[case(
        "idx_tasks_submitted_token_id",
        "tasks",
        "%(submitted_token_id)%",
        "%WHERE (submitted_token_id IS NOT NULL)%"
    )]
    #[tokio::test]
    async fn token_retention_supporting_index_exists(
        #[case] index_name: &str,
        #[case] table_name: &str,
        #[case] columns: &str,
        #[case] predicate: &str,
    ) {
        let pool = crate::tests::get_test_pool();
        let exists = with_connection(&pool, async |conn| {
            diesel::sql_query(
                "SELECT EXISTS (
                    SELECT 1
                    FROM pg_index
                    JOIN pg_class AS index_class
                      ON index_class.oid = pg_index.indexrelid
                    JOIN pg_class AS table_class
                      ON table_class.oid = pg_index.indrelid
                    JOIN pg_namespace
                      ON pg_namespace.oid = table_class.relnamespace
                    WHERE pg_namespace.nspname = 'public'
                      AND table_class.relname = $2
                      AND index_class.relname = $1
                      AND pg_index.indisvalid
                      AND pg_get_indexdef(pg_index.indexrelid) LIKE $3
                      AND pg_get_indexdef(pg_index.indexrelid) LIKE $4
                ) AS exists",
            )
            .bind::<Text, _>(index_name)
            .bind::<Text, _>(table_name)
            .bind::<Text, _>(columns)
            .bind::<Text, _>(predicate)
            .get_result::<IndexExistsRow>(conn)
            .await
        })
        .await
        .unwrap()
        .exists;

        assert!(exists, "{index_name} is missing or does not match");
    }

    #[test]
    fn concurrent_index_migrations_do_not_accept_invalid_same_name_indexes() {
        let migrations = [
            include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/migrations/2026-07-25-000001_token_explicit_expiry_retention_index/up.sql"
            )),
            include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/migrations/2026-07-25-000002_token_implicit_expiry_retention_index/up.sql"
            )),
            include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/migrations/2026-07-25-000003_task_submitted_token_retention_index/up.sql"
            )),
        ];

        for migration in migrations {
            assert!(migration.contains("CREATE INDEX CONCURRENTLY"));
            assert!(
                !migration.contains("IF NOT EXISTS"),
                "a failed concurrent build must not be accepted on retry"
            );
        }
    }
}
