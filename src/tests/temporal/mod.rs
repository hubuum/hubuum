use crate::db::with_connection;
use crate::tests::TestScope;
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use diesel::sql_types::{Integer, Text};

/// Driving INSERT/UPDATE/DELETE on a base table through raw SQL (with the
/// actor GUC set) must produce I/U/D history rows carrying that actor.
#[actix_rt::test]
async fn trigger_records_ops_and_actor() {
    let scope = TestScope::new();
    let pool = scope.pool.clone();
    let ns = scope.namespace_fixture("trigger_actor").await;
    let ns_id = ns.namespace.id;
    let cname = format!("trigger_actor_class_{}", scope.scope_id);

    // All three DML statements in one transaction with the actor GUC set.
    with_connection(&pool, |conn| {
        conn.transaction::<(), diesel::result::Error, _>(|conn| {
            diesel::sql_query("SELECT set_config('hubuum.actor_id', '4242', true)").execute(conn)?;
            diesel::sql_query(
                "INSERT INTO hubuumclass (name, namespace_id, validate_schema, description)
                 VALUES ($1, $2, false, 'd')",
            )
            .bind::<Text, _>(&cname)
            .bind::<Integer, _>(ns_id)
            .execute(conn)?;

            let cid: i32 = {
                use crate::schema::hubuumclass::dsl as c;
                c::hubuumclass.filter(c::name.eq(&cname)).select(c::id).first(conn)?
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
    let rows: Vec<(String, Option<DateTime<Utc>>, Option<i32>)> = with_connection(&pool, |conn| {
        use crate::schema::hubuumclass::dsl as c;
        use crate::schema::hubuumclass_history::dsl as h;
        let cid: i32 = c::hubuumclass
            .filter(c::name.eq(&cname))
            .select(c::id)
            .first(conn)
            .optional()?
            .unwrap_or(-1);
        // The class itself is deleted; find history by the name snapshot instead.
        let _ = cid;
        h::hubuumclass_history
            .filter(h::name.eq(&cname))
            .order(h::history_id.asc())
            .select((h::op, h::valid_to, h::actor_id))
            .load(conn)
    })
    .unwrap();

    let ops: Vec<&str> = rows.iter().map(|(op, _, _)| op.as_str()).collect();
    assert_eq!(ops, vec!["I", "U", "D"], "expected insert/update/delete history");
    assert!(rows.iter().all(|(_, _, actor)| *actor == Some(4242)), "actor must be 4242 on every row");

    ns.cleanup().await.unwrap();
}

/// Deleting a namespace cascades to its classes; the AFTER trigger must still
/// record a 'D' history row for each cascaded class.
#[actix_rt::test]
async fn cascade_delete_records_history() {
    use crate::models::NewHubuumClass;
    use crate::traits::CanSave;

    let scope = TestScope::new();
    let pool = scope.pool.clone();
    let ns = scope.namespace_fixture("cascade_hist").await;
    let cname = format!("cascade_hist_class_{}", scope.scope_id);

    let class = NewHubuumClass {
        name: cname.clone(),
        namespace_id: ns.namespace.id,
        json_schema: None,
        validate_schema: Some(false),
        description: "d".into(),
    }
    .save(&pool)
    .await
    .unwrap();

    ns.cleanup().await.unwrap(); // cascade-deletes the class

    let ops: Vec<String> = with_connection(&pool, |conn| {
        use crate::schema::hubuumclass_history::dsl as h;
        h::hubuumclass_history
            .filter(h::id.eq(class.id))
            .order(h::history_id.asc())
            .select(h::op)
            .load(conn)
    })
    .unwrap();

    assert!(ops.contains(&"I".to_string()), "insert should be recorded");
    assert!(ops.contains(&"D".to_string()), "cascade delete should be recorded");
}

use crate::db::with_actor_scope;

#[actix_rt::test]
async fn actor_scope_sets_actor_and_default_is_null() {
    use crate::models::NewHubuumClass;
    use crate::traits::CanSave;

    let scope = TestScope::new();
    let pool = scope.pool.clone();
    let ns = scope.namespace_fixture("actor_scope").await;
    let ns_id = ns.namespace.id;

    // Inside a scope -> actor recorded.
    let in_name = format!("actor_in_{}", scope.scope_id);
    let in_class = with_actor_scope(Some(4242), async {
        NewHubuumClass {
            name: in_name.clone(),
            namespace_id: ns_id,
            json_schema: None,
            validate_schema: Some(false),
            description: "d".into(),
        }
        .save(&pool)
        .await
    })
    .await
    .unwrap();

    // Outside any scope -> actor NULL.
    let out_name = format!("actor_out_{}", scope.scope_id);
    let out_class = NewHubuumClass {
        name: out_name.clone(),
        namespace_id: ns_id,
        json_schema: None,
        validate_schema: Some(false),
        description: "d".into(),
    }
    .save(&pool)
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

    ns.cleanup().await.unwrap();
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
    let ns = scope.namespace_fixture("anon").await;

    // A user who will make a change and then be anonymized.
    let uname = format!("anon_user_{}", scope.scope_id);
    let user = NewUser {
        username: uname.clone(),
        password: "secret".into(),
        email: Some("a@example.com".into()),
    }
    .save(&pool)
    .await
    .unwrap();
    let token = user.create_token(&pool).await.unwrap();
    let _ = token;

    let cname = format!("anon_class_{}", scope.scope_id);
    let class = with_actor_scope(Some(user.id), async {
        NewHubuumClass {
            name: cname.clone(),
            namespace_id: ns.namespace.id,
            json_schema: None,
            validate_schema: Some(false),
            description: "d".into(),
        }
        .save(&pool)
        .await
    })
    .await
    .unwrap();

    anonymize_user(&pool, user.id).await.unwrap();

    // PII scrubbed on the (non-versioned) users row.
    let (username, email, stored_password, anonymized_at): (String, Option<String>, String, Option<chrono::NaiveDateTime>) =
        with_connection(&pool, |conn| {
            use crate::schema::users::dsl as u;
            u::users
                .filter(u::id.eq(user.id))
                .select((u::username, u::email, u::password, u::anonymized_at))
                .first(conn)
        })
        .unwrap();
    assert_eq!(username, format!("anonymized-{}", user.id));
    assert_eq!(email, None);
    assert!(anonymized_at.is_some());

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
        t::tokens.filter(t::user_id.eq(user.id)).count().get_result(conn)
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

    ns.cleanup().await.unwrap();
}
