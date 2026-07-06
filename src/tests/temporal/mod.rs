use crate::db::with_connection;
use crate::models::{NewHubuumClass, UpdateHubuumClass};
use crate::tests::TestScope;
use crate::traits::{CanSave, CanUpdate};
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use diesel::sql_types::{Integer, Text, Timestamp, Timestamptz};

/// Driving INSERT/UPDATE/DELETE on a base table through raw SQL (with the
/// actor GUC set) must produce I/U/D history rows carrying that actor.
#[actix_rt::test]
async fn trigger_records_ops_and_actor() {
    let scope = TestScope::new();
    let pool = scope.pool.clone();
    let collection_fixture = scope.collection_fixture("trigger_actor").await;
    let collection_id = collection_fixture.collection.id;
    let cname = format!("trigger_actor_class_{}", scope.scope_id);

    // All three DML statements in one transaction with the actor GUC set.
    with_connection(&pool, |conn| {
        conn.transaction::<(), diesel::result::Error, _>(|conn| {
            diesel::sql_query("SELECT set_config('hubuum.actor_id', '4242', true)")
                .execute(conn)?;
            diesel::sql_query(
                "INSERT INTO hubuumclass (name, collection_id, validate_schema, description)
                 VALUES ($1, $2, false, 'd')",
            )
            .bind::<Text, _>(&cname)
            .bind::<Integer, _>(collection_id)
            .execute(conn)?;

            let cid: i32 = {
                use crate::schema::hubuumclass::dsl as c;
                c::hubuumclass
                    .filter(c::name.eq(&cname))
                    .select(c::id)
                    .first(conn)?
            };
            diesel::sql_query("UPDATE hubuumclass SET description='d2' WHERE id=$1")
                .bind::<Integer, _>(cid)
                .execute(conn)?;
            diesel::sql_query("DELETE FROM hubuumclass WHERE id=$1")
                .bind::<Integer, _>(cid)
                .execute(conn)?;
            Ok(())
        })
    })
    .unwrap();

    // Read back the history for that class, oldest first.
    type HistRow = (String, DateTime<Utc>, Option<DateTime<Utc>>, Option<i32>);
    let rows: Vec<HistRow> = with_connection(&pool, |conn| {
        use crate::schema::hubuumclass_history::dsl as h;
        // The class itself is deleted; find history by the name snapshot instead.
        h::hubuumclass_history
            .filter(h::name.eq(&cname))
            .order(h::history_id.asc())
            .select((h::op, h::valid_from, h::valid_to, h::actor_id))
            .load(conn)
    })
    .unwrap();

    let ops: Vec<&str> = rows.iter().map(|(op, _, _, _)| op.as_str()).collect();
    assert_eq!(
        ops,
        vec!["I", "U", "D"],
        "expected insert/update/delete history"
    );
    assert!(
        rows.iter().all(|(_, _, _, actor)| *actor == Some(4242)),
        "actor must be 4242 on every row"
    );

    // The DELETE row must be a zero-width tombstone.
    let delete_row = rows.iter().find(|(op, _, _, _)| op == "D").unwrap();
    let (_, valid_from, valid_to, _) = delete_row;
    assert_eq!(
        valid_from,
        valid_to.as_ref().unwrap(),
        "DELETE tombstone must have valid_from == valid_to"
    );

    collection_fixture.cleanup().await.unwrap();
}

/// The generic trigger must insert by column name, not by physical column order.
/// Future migrations may append mirrored columns in different positions; that
/// should not silently shift base values into the wrong history fields.
#[actix_rt::test]
async fn trigger_inserts_history_by_column_name() {
    let scope = TestScope::new();
    let pool = scope.pool.clone();

    let row: ColumnOrderHistoryRow = with_connection(&pool, |conn| {
        conn.transaction::<_, diesel::result::Error, _>(|conn| {
            diesel::sql_query(
                "CREATE TEMP TABLE temporal_column_order (
                    id int PRIMARY KEY,
                    b text NOT NULL,
                    a int NOT NULL
                 )",
            )
            .execute(conn)?;
            diesel::sql_query(
                "CREATE TEMP TABLE temporal_column_order_history (
                    a int NOT NULL,
                    id int NOT NULL,
                    b text NOT NULL,
                    op varchar NOT NULL CHECK (op IN ('I','U','D')),
                    valid_from timestamptz NOT NULL,
                    valid_to timestamptz,
                    actor_id int,
                    history_id bigint NOT NULL
                 )",
            )
            .execute(conn)?;
            diesel::sql_query("CREATE TEMP SEQUENCE temporal_column_order_history_seq")
                .execute(conn)?;
            diesel::sql_query(
                "CREATE TRIGGER temporal_column_order_history_trg
                 AFTER INSERT OR UPDATE OR DELETE ON temporal_column_order
                 FOR EACH ROW EXECUTE FUNCTION hubuum_record_history()",
            )
            .execute(conn)?;
            diesel::sql_query("INSERT INTO temporal_column_order (id, b, a) VALUES (7, 'bee', 42)")
                .execute(conn)?;
            diesel::sql_query(
                "SELECT id AS hist_id, a AS hist_a, b AS hist_b
                 FROM temporal_column_order_history
                 WHERE history_id = 1",
            )
            .get_result(conn)
        })
    })
    .unwrap();

    assert_eq!(
        (row.hist_id, row.hist_a, row.hist_b),
        (7, 42, "bee".to_string())
    );
}

/// `valid_from` should reflect the actual trigger execution time, not the start
/// of a long-running transaction.
#[actix_rt::test]
async fn trigger_timestamp_is_execution_time_not_transaction_start() {
    let scope = TestScope::new();
    let pool = scope.pool.clone();
    let collection_fixture = scope.collection_fixture("clock_timestamp").await;
    let cname = format!("clock_timestamp_class_{}", scope.scope_id);

    let (tx_start, valid_from): (DateTime<Utc>, DateTime<Utc>) = with_connection(&pool, |conn| {
        conn.transaction::<_, diesel::result::Error, _>(|conn| {
            let tx_start = diesel::sql_query("SELECT transaction_timestamp() AS value")
                .get_result::<SingleTimestamp>(conn)?
                .value;
            diesel::sql_query("SELECT pg_sleep(0.02)").execute(conn)?;
            diesel::sql_query(
                "INSERT INTO hubuumclass (name, collection_id, validate_schema, description)
                     VALUES ($1, $2, false, 'd')",
            )
            .bind::<Text, _>(&cname)
            .bind::<Integer, _>(collection_fixture.collection.id)
            .execute(conn)?;
            let valid_from = diesel::sql_query(
                "SELECT valid_from AS value
                     FROM hubuumclass_history
                     WHERE name = $1
                     ORDER BY history_id DESC
                     LIMIT 1",
            )
            .bind::<Text, _>(&cname)
            .get_result::<SingleTimestamp>(conn)?
            .value;
            Ok((tx_start, valid_from))
        })
    })
    .unwrap();

    assert!(
        valid_from > tx_start,
        "history timestamp should advance after transaction start"
    );

    collection_fixture.cleanup().await.unwrap();
}

/// Idempotent updates to temporal domain rows are not data changes. They must
/// not bump `updated_at` or add an artificial UPDATE history row.
#[actix_rt::test]
async fn unchanged_domain_update_is_noop() {
    let scope = TestScope::new();
    let pool = scope.pool.clone();
    let collection_fixture = scope.collection_fixture("noop_update").await;
    let cname = format!("noop_update_class_{}", scope.scope_id);
    let event_context = hubuum_events_core::EventContext::system();

    let class = NewHubuumClass {
        name: cname,
        collection_id: collection_fixture.collection.id,
        json_schema: None,
        validate_schema: Some(false),
        description: "d".into(),
    }
    .save(&pool, &event_context)
    .await
    .unwrap();
    let before_updated_at = class.updated_at;

    with_connection(&pool, |conn| {
        diesel::sql_query("SELECT pg_sleep(0.02)").execute(conn)
    })
    .unwrap();

    let returned = UpdateHubuumClass {
        name: Some(class.name.clone()),
        collection_id: Some(class.collection_id),
        json_schema: None,
        validate_schema: Some(class.validate_schema),
        description: Some(class.description.clone()),
    }
    .update_without_events(&pool, class.id)
    .await
    .unwrap();

    let (after_updated_at, history_count): (chrono::NaiveDateTime, i64) =
        with_connection(&pool, |conn| {
            let after_updated_at =
                diesel::sql_query("SELECT updated_at AS value FROM hubuumclass WHERE id = $1")
                    .bind::<Integer, _>(class.id)
                    .get_result::<SingleNaiveTimestamp>(conn)?
                    .value;
            let history_count = {
                use crate::schema::hubuumclass_history::dsl as h;
                h::hubuumclass_history
                    .filter(h::id.eq(class.id))
                    .count()
                    .get_result::<i64>(conn)?
            };
            Ok::<_, diesel::result::Error>((after_updated_at, history_count))
        })
        .unwrap();

    assert_eq!(returned.updated_at, before_updated_at);
    assert_eq!(after_updated_at, before_updated_at);
    assert_eq!(history_count, 1);

    collection_fixture.cleanup().await.unwrap();
}

#[derive(QueryableByName)]
struct SingleTimestamp {
    #[diesel(sql_type = Timestamptz)]
    value: DateTime<Utc>,
}

#[derive(QueryableByName)]
struct SingleNaiveTimestamp {
    #[diesel(sql_type = Timestamp)]
    value: chrono::NaiveDateTime,
}

#[derive(QueryableByName)]
struct ColumnOrderHistoryRow {
    #[diesel(sql_type = Integer)]
    hist_id: i32,
    #[diesel(sql_type = Integer)]
    hist_a: i32,
    #[diesel(sql_type = Text)]
    hist_b: String,
}

/// Deleting a collection cascades to its classes; the AFTER trigger must still
/// record a 'D' history row for each cascaded class.
#[actix_rt::test]
async fn cascade_delete_records_history() {
    use crate::models::NewHubuumClass;
    use crate::traits::CanSave;

    let scope = TestScope::new();
    let pool = scope.pool.clone();
    let collection_fixture = scope.collection_fixture("cascade_hist").await;
    let cname = format!("cascade_hist_class_{}", scope.scope_id);
    let event_context = hubuum_events_core::EventContext::system();

    let class = NewHubuumClass {
        name: cname.clone(),
        collection_id: collection_fixture.collection.id,
        json_schema: None,
        validate_schema: Some(false),
        description: "d".into(),
    }
    .save(&pool, &event_context)
    .await
    .unwrap();

    collection_fixture.cleanup().await.unwrap(); // cascade-deletes the class

    let ops: Vec<String> = with_connection(&pool, |conn| {
        use crate::schema::hubuumclass_history::dsl as h;
        h::hubuumclass_history
            .filter(h::id.eq(class.id))
            .order(h::history_id.asc())
            .select(h::op)
            .load(conn)
    })
    .unwrap();

    assert_eq!(
        ops,
        vec!["I".to_string(), "D".to_string()],
        "expected insert then cascade delete"
    );
}

use crate::db::with_actor_scope;

#[actix_rt::test]
async fn actor_scope_sets_actor_and_default_is_null() {
    use crate::models::NewHubuumClass;
    use crate::traits::CanSave;

    let scope = TestScope::new();
    let pool = scope.pool.clone();
    let collection_fixture = scope.collection_fixture("actor_scope").await;
    let collection_id = collection_fixture.collection.id;

    // Inside a scope -> actor recorded.
    let in_name = format!("actor_in_{}", scope.scope_id);
    let in_class = with_actor_scope(Some(4242), async {
        let event_context = hubuum_events_core::EventContext::system();
        NewHubuumClass {
            name: in_name.clone(),
            collection_id,
            json_schema: None,
            validate_schema: Some(false),
            description: "d".into(),
        }
        .save(&pool, &event_context)
        .await
    })
    .await
    .unwrap();

    // Outside any scope -> actor NULL.
    let out_name = format!("actor_out_{}", scope.scope_id);
    let event_context = hubuum_events_core::EventContext::system();
    let out_class = NewHubuumClass {
        name: out_name.clone(),
        collection_id,
        json_schema: None,
        validate_schema: Some(false),
        description: "d".into(),
    }
    .save(&pool, &event_context)
    .await
    .unwrap();

    let read_actor = |id: i32| {
        with_connection(&pool, move |conn| {
            use crate::schema::hubuumclass_history::dsl as h;
            h::hubuumclass_history
                .filter(h::id.eq(id))
                .order(h::history_id.desc())
                .select(h::actor_id)
                .first::<Option<i32>>(conn)
        })
        .unwrap()
    };

    assert_eq!(read_actor(in_class.id), Some(4242));
    assert_eq!(read_actor(out_class.id), None);

    collection_fixture.cleanup().await.unwrap();
}

#[actix_rt::test]
async fn anonymize_scrubs_pii_but_keeps_history_actor() {
    use crate::db::{with_actor_scope, with_connection};
    use crate::models::{NewHubuumClass, NewUser};
    use crate::traits::CanSave;
    use crate::utilities::iam::anonymize_user;
    use diesel::prelude::*;

    let scope = TestScope::new();
    let pool = scope.pool.clone();
    let collection_fixture = scope.collection_fixture("anon").await;

    // A user who will make a change and then be anonymized.
    let uname = format!("anon_user_{}", scope.scope_id);
    let user = NewUser {
        name: uname.clone(),
        password: "secret".into(),
        proper_name: Some("Anon User".into()),
        email: Some("a@example.com".into()),
    }
    .save(&pool, None)
    .await
    .unwrap();
    let token = user.create_token(&pool).await.unwrap();
    let _ = token;

    let cname = format!("anon_class_{}", scope.scope_id);
    let class = with_actor_scope(Some(user.id), async {
        let event_context = hubuum_events_core::EventContext::system();
        NewHubuumClass {
            name: cname.clone(),
            collection_id: collection_fixture.collection.id,
            json_schema: None,
            validate_schema: Some(false),
            description: "d".into(),
        }
        .save(&pool, &event_context)
        .await
    })
    .await
    .unwrap();

    anonymize_user(&pool, user.id).await.unwrap();

    // PII scrubbed on the (non-versioned) users row.
    let (proper_name, email, stored_password, anonymized_at): (
        Option<String>,
        Option<String>,
        String,
        Option<chrono::NaiveDateTime>,
    ) = with_connection(&pool, |conn| {
        use crate::schema::users::dsl as u;
        u::users
            .filter(u::id.eq(user.id))
            .select((u::proper_name, u::email, u::password, u::anonymized_at))
            .first(conn)
    })
    .unwrap();
    assert_eq!(proper_name, None);
    assert_eq!(email, None);
    assert!(anonymized_at.is_some());

    let principal_name: String = with_connection(&pool, |conn| {
        use crate::schema::principals::dsl as p;
        p::principals
            .filter(p::id.eq(user.id))
            .select(p::name)
            .first(conn)
    })
    .unwrap();
    assert_eq!(principal_name, format!("anonymized-{}", user.id));

    // Anonymized password cannot authenticate (neither the original password nor empty string).
    assert!(
        !crate::utilities::auth::verify_password("secret", &stored_password).unwrap(),
        "original password must not verify"
    );
    assert!(
        !crate::utilities::auth::verify_password("", &stored_password).unwrap(),
        "empty password must not verify"
    );

    // Tokens revoked.
    let token_count: i64 = with_connection(&pool, |conn| {
        use crate::schema::tokens::dsl as t;
        t::tokens
            .filter(t::principal_id.eq(user.id))
            .filter(t::revoked_at.is_null())
            .count()
            .get_result(conn)
    })
    .unwrap();
    assert_eq!(token_count, 0);

    // History still attributes the change to the (now pseudonymous) id.
    let actor: Option<i32> = with_connection(&pool, |conn| {
        use crate::schema::hubuumclass_history::dsl as h;
        h::hubuumclass_history
            .filter(h::id.eq(class.id))
            .order(h::history_id.desc())
            .select(h::actor_id)
            .first::<Option<i32>>(conn)
    })
    .unwrap();
    assert_eq!(actor, Some(user.id));

    collection_fixture.cleanup().await.unwrap();
}
