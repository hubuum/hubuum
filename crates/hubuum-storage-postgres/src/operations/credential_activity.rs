use crate::failpoints::{PostgresFaultPoint, reach_fault_point};
use crate::{PostgresConnection, PostgresRuntime, PostgresStorageError};
use chrono::{Duration, NaiveDateTime, Utc};
use diesel::{
    prelude::*,
    sql_types::{BigInt, Bool, Integer, Nullable, Text, Timestamp, Uuid as SqlUuid},
};
use diesel_async::RunQueryDsl;
use hubuum_domain::{PrincipalId, TokenId};
use hubuum_storage_core::{
    AuthSessionId, CredentialActivityPolicy, CredentialIssuanceMethod, ObservedSource,
    StorageActivityQuery, StorageCredentialActivity, StorageCredentialCreation,
    StorageCredentialSource,
};
use uuid::Uuid;

#[derive(QueryableByName)]
struct Owner {
    #[diesel(sql_type=Nullable<SqlUuid>)]
    auth_session_id: Option<Uuid>,
}
#[derive(QueryableByName)]
struct Activity {
    #[diesel(sql_type=BigInt)]
    id: i64,
    #[diesel(sql_type=Nullable<SqlUuid>)]
    session_id: Option<Uuid>,
    #[diesel(sql_type=Nullable<Integer>)]
    issuer_id: Option<i32>,
    #[diesel(sql_type=Nullable<Text>)]
    issuance_method: Option<String>,
    #[diesel(sql_type=Nullable<Timestamp>)]
    issued_at: Option<NaiveDateTime>,
    #[diesel(sql_type=Nullable<Text>)]
    creation_address: Option<String>,
    #[diesel(sql_type=Nullable<Text>)]
    creation_method: Option<String>,
    #[diesel(sql_type=Nullable<Timestamp>)]
    overflow_since: Option<NaiveDateTime>,
}
#[derive(QueryableByName)]
struct Source {
    #[diesel(sql_type=Text)]
    address: String,
    #[diesel(sql_type=Text)]
    address_method: String,
    #[diesel(sql_type=Timestamp)]
    first_seen: NaiveDateTime,
    #[diesel(sql_type=Timestamp)]
    last_seen: NaiveDateTime,
}
#[derive(QueryableByName)]
struct Count {
    #[diesel(sql_type=BigInt)]
    count: i64,
}
async fn owner(
    connection: &mut PostgresConnection,
    token: TokenId,
) -> Result<Option<Uuid>, PostgresStorageError> {
    Ok(
        diesel::sql_query("SELECT auth_session_id FROM tokens WHERE id=$1")
            .bind::<Integer, _>(token.id())
            .get_result::<Owner>(connection)
            .await?
            .auth_session_id,
    )
}
async fn ensure(
    connection: &mut PostgresConnection,
    token: TokenId,
) -> Result<Activity, PostgresStorageError> {
    let session = owner(connection, token).await?;
    diesel::sql_query("INSERT INTO credential_activity(token_id,session_id) VALUES ($1,$2) ON CONFLICT DO NOTHING")
        .bind::<Nullable<Integer>, _>(session.is_none().then_some(token.id()))
        .bind::<Nullable<SqlUuid>, _>(session)
        .execute(connection)
        .await?;
    Ok(diesel::sql_query(
        "SELECT * FROM credential_activity WHERE token_id=$1 OR session_id=$2 FOR UPDATE",
    )
    .bind::<Integer, _>(token.id())
    .bind::<Nullable<SqlUuid>, _>(session)
    .get_result(connection)
    .await?)
}
pub async fn creation(
    runtime: &PostgresRuntime,
    token: TokenId,
    creation: StorageCredentialCreation,
) -> Result<(), PostgresStorageError> {
    runtime
        .with_transaction(async move |connection| -> Result<_, PostgresStorageError> {
            reach_fault_point(PostgresFaultPoint::CredentialActivityBeforeWrite, Some(connection)).await?;
            let activity = ensure(connection, token).await?;
            diesel::sql_query("UPDATE credential_activity SET issuer_id=$2,issuance_method=$3,issued_at=$4,creation_address=$5,creation_method=$6 WHERE id=$1 AND issuance_method IS NULL")
                .bind::<BigInt, _>(activity.id)
                .bind::<Nullable<Integer>, _>(creation.issuer().map(|id| id.id()))
                .bind::<Text, _>(creation.method().as_str())
                .bind::<Timestamp, _>(creation.issued_at().naive_utc())
                .bind::<Nullable<Text>, _>(creation.source().address().map(|ip| ip.to_string()))
                .bind::<Text, _>(creation.source().method())
                .execute(connection)
                .await?;
            Ok(())
        })
        .await
}
pub async fn observe(
    runtime: &PostgresRuntime,
    token: TokenId,
    source: ObservedSource,
    policy: CredentialActivityPolicy,
) -> Result<(), PostgresStorageError> {
    if !policy.enabled() {
        return Ok(());
    }
    // A repeated source inside its throttle window needs no write or row lock.
    // This database read is only an optimization: the locked transaction below
    // always rechecks, including first observations arriving on other replicas.
    #[derive(QueryableByName)]
    struct Recent {
        #[diesel(sql_type = Bool)]
        recent: bool,
    }
    let address = source
        .address()
        .map(|ip| ip.to_string())
        .unwrap_or_default();
    let method = source.method();
    let recent = runtime
        .with_connection(async move |connection| {
            diesel::sql_query("SELECT EXISTS (SELECT 1 FROM credential_sources s JOIN credential_activity a ON a.id=s.activity_id JOIN tokens t ON (a.token_id=t.id OR a.session_id=t.auth_session_id) WHERE t.id=$1 AND s.address=$2 AND s.address_method=$3 AND s.last_seen > (clock_timestamp() AT TIME ZONE 'UTC') - interval '60 seconds') AS recent")
                .bind::<Integer, _>(token.id())
                .bind::<Text, _>(address)
                .bind::<Text, _>(method)
                .get_result::<Recent>(connection)
                .await
        })
        .await?
        .recent;
    if recent {
        return Ok(());
    }
    runtime
        .with_transaction(async move |connection| -> Result<_, PostgresStorageError> {
            reach_fault_point(PostgresFaultPoint::CredentialActivityBeforeWrite, Some(connection)).await?;
            let activity = ensure(connection, token).await?;
            let now = diesel::select(diesel::dsl::sql::<Timestamp>(
                "clock_timestamp() AT TIME ZONE 'UTC'",
            ))
            .get_result::<NaiveDateTime>(connection)
            .await?;
            let cutoff = now - Duration::days(policy.retention_days());
            diesel::sql_query("DELETE FROM credential_sources WHERE activity_id=$1 AND last_seen<$2")
                .bind::<BigInt, _>(activity.id)
                .bind::<Timestamp, _>(cutoff)
                .execute(connection)
                .await?;
            let address = source
                .address()
                .map(|ip| ip.to_string())
                .unwrap_or_default();
            let existing = diesel::sql_query("SELECT * FROM credential_sources WHERE activity_id=$1 AND address=$2 AND address_method=$3")
                .bind::<BigInt, _>(activity.id)
                .bind::<Text, _>(&address)
                .bind::<Text, _>(source.method())
                .get_result::<Source>(connection)
                .await
                .optional()?;
            if let Some(existing) = existing {
                if existing.last_seen <= now - Duration::seconds(60) {
                    diesel::sql_query("UPDATE credential_sources SET last_seen=GREATEST(last_seen,$4) WHERE activity_id=$1 AND address=$2 AND address_method=$3")
                        .bind::<BigInt, _>(activity.id)
                        .bind::<Text, _>(&address)
                        .bind::<Text, _>(source.method())
                        .bind::<Timestamp, _>(now)
                        .execute(connection)
                        .await?;
                }
            } else {
                let count = diesel::sql_query("SELECT COUNT(*) AS count FROM credential_sources WHERE activity_id=$1")
                    .bind::<BigInt, _>(activity.id)
                    .get_result::<Count>(connection)
                    .await?
                    .count;
                if count >= policy.source_cap() as i64 {
                    diesel::sql_query("UPDATE credential_activity SET overflow_since=COALESCE(overflow_since,$2) WHERE id=$1")
                        .bind::<BigInt, _>(activity.id)
                        .bind::<Timestamp, _>(now)
                        .execute(connection)
                        .await?;
                } else {
                    diesel::sql_query("INSERT INTO credential_sources(activity_id,address,address_method,first_seen,last_seen) VALUES ($1,$2,$3,$4,$4)")
                        .bind::<BigInt, _>(activity.id)
                        .bind::<Text, _>(address)
                        .bind::<Text, _>(source.method())
                        .bind::<Timestamp, _>(now)
                        .execute(connection)
                        .await?;
                }
            }
            Ok(())
        })
        .await
}
pub async fn read(
    runtime: &PostgresRuntime,
    query: StorageActivityQuery,
) -> Result<StorageCredentialActivity, PostgresStorageError> {
    runtime
        .with_read_only_snapshot(async move |connection| -> Result<_, PostgresStorageError> {
            let owner = diesel::sql_query("SELECT auth_session_id FROM tokens WHERE id=$1 AND principal_id=$2")
                .bind::<Integer, _>(query.token_id().id())
                .bind::<Integer, _>(query.principal_id().id())
                .get_result::<Owner>(connection)
                .await?;
            let activity = diesel::sql_query("SELECT * FROM credential_activity WHERE token_id=$1 OR session_id=$2")
                .bind::<Integer, _>(query.token_id().id())
                .bind::<Nullable<SqlUuid>, _>(owner.auth_session_id)
                .get_result::<Activity>(connection)
                .await
                .optional()?;
            let Some(activity) = activity else {
                return Ok(StorageCredentialActivity::new(
                    owner.auth_session_id.map(AuthSessionId::new),
                    None,
                    vec![],
                    0,
                    None,
                ));
            };
            let creation = match (
                activity.issuance_method,
                activity.issued_at,
                activity.creation_method,
            ) {
                (Some(method), Some(issued), Some(source_method)) => {
                    Some(StorageCredentialCreation::new(
                        activity.issuer_id.map(PrincipalId::new).transpose()?,
                        CredentialIssuanceMethod::from_persisted(&method).map_err(|e| {
                            PostgresStorageError::invalid_persisted_value("issuance method", e)
                        })?,
                        ObservedSource::from_persisted(
                            activity.creation_address.as_deref(),
                            &source_method,
                        )
                        .map_err(|e| {
                            PostgresStorageError::invalid_persisted_value("creation source", e)
                        })?,
                        issued.and_utc(),
                    ))
                }
                _ => None,
            };
            let cutoff = (Utc::now() - Duration::days(query.policy().retention_days())).naive_utc();
            let total = diesel::sql_query("SELECT COUNT(*) AS count FROM credential_sources WHERE activity_id=$1 AND last_seen >= $2")
                .bind::<BigInt, _>(activity.id)
                .bind::<Timestamp, _>(cutoff)
                .get_result::<Count>(connection)
                .await?
                .count as usize;
            let rows = diesel::sql_query("SELECT * FROM credential_sources WHERE activity_id=$1 AND last_seen >= $2 ORDER BY first_seen,id LIMIT $3 OFFSET $4")
                .bind::<BigInt, _>(activity.id)
                .bind::<Timestamp, _>(cutoff)
                .bind::<BigInt, _>(query.limit() as i64)
                .bind::<BigInt, _>(query.offset() as i64)
                .load::<Source>(connection)
                .await?;
            let sources = rows
                .into_iter()
                .map(|r| {
                    Ok(StorageCredentialSource::new(
                        ObservedSource::from_persisted(
                            (!r.address.is_empty()).then_some(r.address.as_str()),
                            &r.address_method,
                        )
                        .map_err(|e| {
                            PostgresStorageError::invalid_persisted_value("credential source", e)
                        })?,
                        r.first_seen.and_utc(),
                        r.last_seen.and_utc(),
                    ))
                })
                .collect::<Result<Vec<_>, PostgresStorageError>>()?;
            Ok(StorageCredentialActivity::new(
                activity.session_id.map(AuthSessionId::new),
                creation,
                sources,
                total,
                activity.overflow_since.map(|t| t.and_utc()),
            ))
        })
        .await
}
pub async fn purge(
    runtime: &PostgresRuntime,
    policy: CredentialActivityPolicy,
    limit: usize,
) -> Result<usize, PostgresStorageError> {
    let limit = i64::try_from(limit)
        .map_err(|_| PostgresStorageError::invalid_input("activity purge limit out of range"))?;
    runtime
        .with_transaction(async move |connection| -> Result<_, PostgresStorageError> {
            Ok(diesel::sql_query("DELETE FROM credential_sources WHERE id IN (SELECT id FROM credential_sources WHERE last_seen<$1 ORDER BY last_seen,id LIMIT $2 FOR UPDATE SKIP LOCKED)")
                .bind::<Timestamp, _>(
                    (Utc::now() - Duration::days(policy.retention_days())).naive_utc(),
                )
                .bind::<BigInt, _>(limit)
                .execute(connection)
                .await?)
        })
        .await
}
