use crate::storage::postgres::prelude::*;
use chrono::{NaiveDateTime, Utc};
use diesel::sql_types::{BigInt, Bool, Integer, Timestamp};

use crate::errors::ApiError;
use crate::events::{Action, ActorKind, EntityType, NewEvent, emit_events};
use crate::models::{PrincipalToken, TokenRetentionSettings, TokenScope};
use crate::schema::tokens;
use crate::storage::postgres::operations::authz::load_token_scopes_for_tokens_conn;
use crate::storage::postgres::operations::maintenance::maintenance_state_conn;
use crate::storage::postgres::operations::token::token_snapshot;
use crate::storage::postgres::{PostgresConnection, with_transaction};

const TOKEN_RETENTION_LOCK_KEY: i64 = 4_850_188_191_125_219;

#[derive(Debug, QueryableByName)]
struct AdvisoryLockRow {
    #[diesel(sql_type = Bool)]
    locked: bool,
}

#[derive(Debug, QueryableByName)]
struct TokenRetentionCandidate {
    #[diesel(sql_type = Integer)]
    id: i32,
}

#[derive(Debug, Clone, Copy)]
enum TokenRetentionBasis {
    Revocation,
    ExplicitExpiry,
    ImplicitExpiry,
}

impl TokenRetentionBasis {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Revocation => "revocation",
            Self::ExplicitExpiry => "explicit_expiry",
            Self::ImplicitExpiry => "implicit_expiry",
        }
    }
}

pub(crate) async fn try_acquire_token_retention_lock(
    conn: &mut PostgresConnection,
) -> Result<bool, ApiError> {
    Ok(
        diesel::sql_query("SELECT pg_try_advisory_xact_lock($1) AS locked")
            .bind::<BigInt, _>(TOKEN_RETENTION_LOCK_KEY)
            .get_result::<AdvisoryLockRow>(conn)
            .await?
            .locked,
    )
}

/// Delete one bounded batch of tokens whose terminal time is older than the
/// configured retention window.
///
/// Terminal time is the earlier of revocation and effective expiry. Explicit
/// `expires_at` values remain authoritative; tokens without one use `issued +
/// token_lifetime_hours`, matching the authentication predicate. Foreign keys
/// cascade scope-row deletion and set task `submitted_token_id` provenance to
/// null.
pub(crate) async fn purge_expired_token_batch(
    pool: &crate::storage::postgres::PostgresPool,
    settings: TokenRetentionSettings,
) -> Result<usize, ApiError> {
    purge_expired_token_batch_at(pool, settings, Utc::now().naive_utc()).await
}

async fn purge_expired_token_batch_at(
    pool: &impl crate::storage::StorageContext,
    settings: TokenRetentionSettings,
    now: NaiveDateTime,
) -> Result<usize, ApiError> {
    let cutoffs = settings.cutoffs(now)?;
    let batch_size = settings.batch_size().as_i64();

    with_transaction(pool, async |conn| -> Result<usize, ApiError> {
        if !maintenance_state_conn(conn).await?.is_normal() {
            return Ok(0);
        }
        if !try_acquire_token_retention_lock(conn).await? {
            return Ok(0);
        }

        purge_expired_token_batch_conn(
            conn,
            cutoffs.explicit_expiry(),
            cutoffs.implicit_issue(),
            batch_size,
        )
        .await
    })
    .await
}

async fn purge_expired_token_batch_conn(
    conn: &mut PostgresConnection,
    explicit_expiry_cutoff: NaiveDateTime,
    implicit_issue_cutoff: NaiveDateTime,
    batch_size: i64,
) -> Result<usize, ApiError> {
    // Give revoked, explicit-expiry, and implicit-expiry index streams an
    // initial share. Then offer every stream the remaining capacity so a
    // one-sided backlog still uses the configured batch size.
    let revoked_share = batch_size / 3 + i64::from(batch_size % 3 > 0);
    let explicit_share = batch_size / 3 + i64::from(batch_size % 3 > 1);
    let implicit_share = batch_size / 3;

    let mut deleted = purge_revoked_tokens(conn, explicit_expiry_cutoff, revoked_share).await?;
    let remaining = batch_size.saturating_sub(deleted as i64);
    deleted +=
        purge_explicit_expired_tokens(conn, explicit_expiry_cutoff, explicit_share.min(remaining))
            .await?;
    let remaining = batch_size.saturating_sub(deleted as i64);
    deleted +=
        purge_implicit_expired_tokens(conn, implicit_issue_cutoff, implicit_share.min(remaining))
            .await?;

    let remaining = batch_size.saturating_sub(deleted as i64);
    deleted += purge_revoked_tokens(conn, explicit_expiry_cutoff, remaining).await?;
    let remaining = batch_size.saturating_sub(deleted as i64);
    deleted += purge_explicit_expired_tokens(conn, explicit_expiry_cutoff, remaining).await?;
    let remaining = batch_size.saturating_sub(deleted as i64);
    deleted += purge_implicit_expired_tokens(conn, implicit_issue_cutoff, remaining).await?;

    Ok(deleted)
}

async fn purge_revoked_tokens(
    conn: &mut PostgresConnection,
    revocation_cutoff: NaiveDateTime,
    limit: i64,
) -> Result<usize, ApiError> {
    if limit == 0 {
        return Ok(0);
    }

    let candidates = diesel::sql_query(
        "SELECT id
         FROM tokens
         WHERE revoked_at IS NOT NULL
           AND revoked_at <= $1
         ORDER BY revoked_at ASC, id ASC
         LIMIT $2
         FOR UPDATE SKIP LOCKED",
    )
    .bind::<Timestamp, _>(revocation_cutoff)
    .bind::<BigInt, _>(limit)
    .load::<TokenRetentionCandidate>(conn)
    .await?;
    purge_selected_tokens(conn, candidates, TokenRetentionBasis::Revocation).await
}

async fn purge_explicit_expired_tokens(
    conn: &mut PostgresConnection,
    expiry_cutoff: NaiveDateTime,
    limit: i64,
) -> Result<usize, ApiError> {
    if limit == 0 {
        return Ok(0);
    }

    let candidates = diesel::sql_query(
        "SELECT id
         FROM tokens
         WHERE expires_at IS NOT NULL
           AND expires_at <= $1
         ORDER BY expires_at ASC, id ASC
         LIMIT $2
         FOR UPDATE SKIP LOCKED",
    )
    .bind::<Timestamp, _>(expiry_cutoff)
    .bind::<BigInt, _>(limit)
    .load::<TokenRetentionCandidate>(conn)
    .await?;
    purge_selected_tokens(conn, candidates, TokenRetentionBasis::ExplicitExpiry).await
}

async fn purge_implicit_expired_tokens(
    conn: &mut PostgresConnection,
    issue_cutoff: NaiveDateTime,
    limit: i64,
) -> Result<usize, ApiError> {
    if limit == 0 {
        return Ok(0);
    }

    let candidates = diesel::sql_query(
        "SELECT id
         FROM tokens
         WHERE expires_at IS NULL
           AND issued <= $1
         ORDER BY issued ASC, id ASC
         LIMIT $2
         FOR UPDATE SKIP LOCKED",
    )
    .bind::<Timestamp, _>(issue_cutoff)
    .bind::<BigInt, _>(limit)
    .load::<TokenRetentionCandidate>(conn)
    .await?;
    purge_selected_tokens(conn, candidates, TokenRetentionBasis::ImplicitExpiry).await
}

async fn purge_selected_tokens(
    conn: &mut PostgresConnection,
    candidates: Vec<TokenRetentionCandidate>,
    basis: TokenRetentionBasis,
) -> Result<usize, ApiError> {
    if candidates.is_empty() {
        return Ok(0);
    }

    let token_ids = candidates
        .into_iter()
        .map(|candidate| candidate.id)
        .collect::<Vec<_>>();
    let retained = tokens::table
        .filter(tokens::id.eq_any(&token_ids))
        .order_by(tokens::id.asc())
        .load::<PrincipalToken>(conn)
        .await?;
    let scopes = load_token_scopes_for_tokens_conn(conn, &retained).await?;
    let events = retained
        .iter()
        .zip(scopes.iter())
        .map(|(token, scope)| token_purge_event(token, scope.as_ref(), basis))
        .collect::<Result<Vec<_>, _>>()?;
    emit_events(conn, &events).await?;

    let deleted = diesel::delete(tokens::table.filter(tokens::id.eq_any(&token_ids)))
        .execute(conn)
        .await?;
    if deleted != token_ids.len() {
        return Err(ApiError::InternalServerError(format!(
            "Token retention selected {} locked rows but deleted {deleted}",
            token_ids.len()
        )));
    }
    Ok(deleted)
}

fn token_purge_event(
    token: &PrincipalToken,
    scope: Option<&TokenScope>,
    basis: TokenRetentionBasis,
) -> Result<NewEvent, ApiError> {
    Ok(NewEvent::new(
        EntityType::Token,
        Action::Purged,
        ActorKind::System,
        format!(
            "Token {} purged after retention for principal {}",
            token.id, token.principal_id
        ),
    )?
    .with_entity_id(token.id)
    .with_entity_name(token.name.clone().unwrap_or_else(|| token.id.to_string()))
    .with_before(token_snapshot(token, scope)?)
    .with_metadata(serde_json::json!({
        "principal_id": token.principal_id,
        "retention_basis": basis.as_str(),
    })))
}

#[cfg(test)]
mod tests {
    use chrono::Duration;
    use diesel::sql_types::{Bool, Text};
    use rstest::rstest;

    use crate::events::{Action, ActorKind, EntityType, Event};
    use crate::models::search::QueryOptions;
    use crate::models::{
        MIN_TOKEN_RETENTION_PURGE_BATCH_SIZE, Permissions, PrincipalID,
        PrincipalTokenCreateRequest, PrincipalTokenMetadata, Token, TokenID, TokenListState,
        TokenScope,
    };
    use crate::schema::{events, token_scopes, tokens};
    use crate::storage::postgres::operations::active_tokens::retained_token_metadata_by_principal_id_paginated_with_total_count;
    use crate::storage::postgres::operations::user::DeleteUserRecord;
    use crate::storage::postgres::with_connection;
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
        TokenRetentionSettings::builder()
            .retention_days(TEST_RETENTION_DAYS)
            .token_lifetime_hours(TEST_TOKEN_LIFETIME_HOURS)
            .batch_size(batch_size)
            .build()
            .unwrap()
    }

    async fn create_token(
        pool: &impl crate::storage::StorageContext,
        principal_id: i32,
        expires_at: Option<NaiveDateTime>,
    ) -> Token {
        let token = PrincipalTokenCreateRequest::new(PrincipalID::new(principal_id).unwrap())
            .create(pool, None)
            .await
            .unwrap();
        if let Some(expires_at) = expires_at {
            set_persisted_expiry(pool, &token, expires_at).await;
        }
        token
    }

    async fn set_persisted_expiry(
        pool: &impl crate::storage::StorageContext,
        token_value: &Token,
        expires_at: NaiveDateTime,
    ) {
        let token_hash = token_value.storage_hash();
        with_connection(pool, async |conn| {
            diesel::update(tokens::table.filter(tokens::token.eq(token_hash)))
                .set(tokens::expires_at.eq(Some(expires_at)))
                .execute(conn)
                .await
        })
        .await
        .unwrap();
    }

    async fn set_revoked_at(
        pool: &impl crate::storage::StorageContext,
        token_value: &Token,
        revoked_at: NaiveDateTime,
    ) {
        let token_hash = token_value.storage_hash();
        with_connection(pool, async |conn| {
            diesel::update(tokens::table.filter(tokens::token.eq(token_hash)))
                .set(tokens::revoked_at.eq(Some(revoked_at)))
                .execute(conn)
                .await
        })
        .await
        .unwrap();
    }

    async fn token_exists(pool: &impl crate::storage::StorageContext, token_value: &Token) -> bool {
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

    async fn wait_until_token_row_is_locked(
        pool: &impl crate::storage::StorageContext,
        token_id: i32,
    ) {
        for _ in 0..100 {
            let result = with_connection(pool, async |conn| {
                diesel::sql_query("SELECT id FROM tokens WHERE id = $1 FOR UPDATE NOWAIT")
                    .bind::<Integer, _>(token_id)
                    .load::<TokenRetentionCandidate>(conn)
                    .await
            })
            .await;
            if result.is_err() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("retained metadata read did not lock token {token_id}");
    }

    #[derive(Debug, Clone, Copy)]
    enum RetainedMetadataRead {
        Point,
        List,
        Batch,
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
        set_revoked_at(&pool, &token, now - Duration::days(1)).await;

        let deleted = purge_expired_token_batch_at(&pool, settings(100), now)
            .await
            .unwrap();

        assert_eq!(deleted, 1);
        assert!(!token_exists(&pool, &token).await);
        user.delete_user_record_without_events(&pool).await.unwrap();
    }

    #[tokio::test]
    async fn purge_deletes_revoked_token_before_its_future_expiry() {
        let _lock = lock_test_mutex(&TOKEN_RETENTION_TEST_LOCK).await;
        let pool = crate::tests::get_test_pool();
        let user = create_test_user(&pool).await;
        let now = Utc::now().naive_utc();
        let token = create_token(&pool, user.id, Some(now + Duration::days(1))).await;
        set_revoked_at(&pool, &token, now - Duration::days(TEST_RETENTION_DAYS + 1)).await;

        let deleted = purge_expired_token_batch_at(&pool, settings(100), now)
            .await
            .unwrap();

        assert_eq!(deleted, 1);
        assert!(!token_exists(&pool, &token).await);
        user.delete_user_record_without_events(&pool).await.unwrap();
    }

    #[tokio::test]
    async fn purge_retains_recently_revoked_token_before_expiry() {
        let _lock = lock_test_mutex(&TOKEN_RETENTION_TEST_LOCK).await;
        let pool = crate::tests::get_test_pool();
        let user = create_test_user(&pool).await;
        let now = Utc::now().naive_utc();
        let token = create_token(&pool, user.id, Some(now + Duration::days(1))).await;
        set_revoked_at(&pool, &token, now - Duration::days(1)).await;

        let deleted = purge_expired_token_batch_at(&pool, settings(100), now)
            .await
            .unwrap();

        assert_eq!(deleted, 0);
        assert!(token_exists(&pool, &token).await);
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
    async fn purge_batch_advances_all_indexed_terminal_streams() {
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
        let revoked = create_token(&pool, user.id, Some(now + Duration::days(1))).await;
        set_revoked_at(
            &pool,
            &revoked,
            now - Duration::days(TEST_RETENTION_DAYS + 1),
        )
        .await;

        let deleted = purge_expired_token_batch_at(
            &pool,
            settings(MIN_TOKEN_RETENTION_PURGE_BATCH_SIZE),
            now,
        )
        .await
        .unwrap();

        assert_eq!(deleted, 3);
        assert!(!token_exists(&pool, &explicit).await);
        assert!(!token_exists(&pool, &implicit).await);
        assert!(!token_exists(&pool, &revoked).await);
        user.delete_user_record_without_events(&pool).await.unwrap();
    }

    #[tokio::test]
    async fn purge_cascades_scope_rows_after_persisting_an_exact_audit_snapshot() {
        let _lock = lock_test_mutex(&TOKEN_RETENTION_TEST_LOCK).await;
        let pool = crate::tests::get_test_pool();
        let user = create_test_user(&pool).await;
        let now = Utc::now().naive_utc();
        let scope =
            TokenScope::from_request_parts(Some(vec![Permissions::ReadCollection]), None).unwrap();
        let token = PrincipalTokenCreateRequest::new(PrincipalID::new(user.id).unwrap())
            .scope(scope)
            .create(&pool, None)
            .await
            .unwrap();
        set_persisted_expiry(&pool, &token, now - Duration::days(TEST_RETENTION_DAYS + 1)).await;
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

        let purge_event = with_connection(&pool, async |conn| {
            events::table
                .filter(events::entity_type.eq(EntityType::Token.as_str()))
                .filter(events::entity_id.eq(Some(token_id)))
                .filter(events::action.eq(Action::Purged.as_str()))
                .first::<Event>(conn)
                .await
        })
        .await
        .unwrap();
        assert_eq!(purge_event.actor_kind, ActorKind::System.as_str());
        assert_eq!(
            purge_event.before.as_ref().unwrap()["scope"]["permissions"],
            serde_json::json!(["ReadCollection"])
        );
        assert_eq!(purge_event.metadata["retention_basis"], "explicit_expiry");
        assert!(purge_event.before.as_ref().unwrap().get("token").is_none());
        user.delete_user_record_without_events(&pool).await.unwrap();
    }

    #[rstest]
    #[case::point(RetainedMetadataRead::Point)]
    #[case::list(RetainedMetadataRead::List)]
    #[case::batch(RetainedMetadataRead::Batch)]
    #[tokio::test]
    async fn retained_metadata_scope_projection_blocks_concurrent_purge(
        #[case] read: RetainedMetadataRead,
    ) {
        let _lock = lock_test_mutex(&TOKEN_RETENTION_TEST_LOCK).await;
        let pool = crate::tests::get_test_pool();
        let user = create_test_user(&pool).await;
        let now = Utc::now().naive_utc();
        let scope =
            TokenScope::from_request_parts(Some(vec![Permissions::ReadCollection]), None).unwrap();
        let raw = PrincipalTokenCreateRequest::new(PrincipalID::new(user.id).unwrap())
            .scope(scope)
            .create(&pool, None)
            .await
            .unwrap();
        let token_hash = raw.storage_hash();
        let persisted_token = with_connection(&pool, async |conn| {
            tokens::table
                .filter(tokens::token.eq(token_hash))
                .first::<PrincipalToken>(conn)
                .await
        })
        .await
        .unwrap();
        let token_id = persisted_token.id;
        set_persisted_expiry(&pool, &raw, now + Duration::days(1)).await;
        set_revoked_at(&pool, &raw, now - Duration::days(TEST_RETENTION_DAYS + 1)).await;

        let blocker_pool = pool.clone();
        let (table_locked_tx, table_locked_rx) = tokio::sync::oneshot::channel();
        let (release_table_tx, release_table_rx) = tokio::sync::oneshot::channel();
        let blocker = tokio::spawn(async move {
            with_transaction(&blocker_pool, async |conn| -> Result<(), ApiError> {
                diesel::sql_query("LOCK TABLE token_scopes IN ACCESS EXCLUSIVE MODE")
                    .execute(conn)
                    .await?;
                table_locked_tx.send(()).unwrap();
                release_table_rx.await.unwrap();
                Ok(())
            })
            .await
            .unwrap();
        });
        table_locked_rx.await.unwrap();

        let reader_pool = pool.clone();
        let principal_id = PrincipalID::new(user.id).unwrap();
        let reader = tokio::spawn(async move {
            match read {
                RetainedMetadataRead::Point => vec![
                    PrincipalTokenMetadata::load_for_principal_token(
                        &reader_pool,
                        principal_id,
                        TokenID::new(token_id).unwrap(),
                    )
                    .await
                    .unwrap(),
                ],
                RetainedMetadataRead::List => {
                    retained_token_metadata_by_principal_id_paginated_with_total_count(
                        principal_id,
                        &reader_pool,
                        &QueryOptions {
                            filters: Vec::new(),
                            sort: Vec::new(),
                            limit: None,
                            cursor: None,
                            include_total: false,
                        },
                        TokenListState::Revoked,
                    )
                    .await
                    .unwrap()
                    .0
                }
                RetainedMetadataRead::Batch => {
                    PrincipalTokenMetadata::load_for_tokens(&reader_pool, &[persisted_token])
                        .await
                        .unwrap()
                }
            }
        });
        wait_until_token_row_is_locked(&pool, token_id).await;

        let deleted_while_reading = purge_expired_token_batch_at(&pool, settings(100), now)
            .await
            .unwrap();
        assert_eq!(deleted_while_reading, 0);
        assert!(token_exists(&pool, &raw).await);

        release_table_tx.send(()).unwrap();
        blocker.await.unwrap();
        let metadata = reader.await.unwrap();
        assert_eq!(metadata.len(), 1);
        assert_eq!(
            metadata[0].scope.as_ref().unwrap().permissions(),
            Some([Permissions::ReadCollection].as_slice())
        );

        let deleted_after_read = purge_expired_token_batch_at(&pool, settings(100), now)
            .await
            .unwrap();
        assert_eq!(deleted_after_read, 1);
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
        "idx_tokens_revoked_retention",
        "tokens",
        "%(revoked_at, id)%",
        "%WHERE (revoked_at IS NOT NULL)%"
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
            include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/migrations/2026-08-04-000001_token_revoked_retention_index/up.sql"
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
