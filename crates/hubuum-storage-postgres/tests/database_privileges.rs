#![cfg(feature = "integration-test-support")]

use diesel::QueryableByName;
use diesel::sql_types::{BigInt, Bool, Integer, Text};
use diesel_async::{AsyncConnection, RunQueryDsl, SimpleAsyncConnection};
use hubuum_events_core::{Action, ActorKind, EntityType, NewEvent, TraceLink};
use hubuum_storage_postgres::test_support::{
    append_event_on_connection, database_role_tests_enabled, integration_test_database_roles,
    integration_test_migration_pool, integration_test_pool,
};
use hubuum_storage_postgres::{
    DatabaseRoleNames, database_role_reconciliation_sql, inspect_database_privileges,
    with_connection,
};
use rstest::rstest;
use uuid::Uuid;

macro_rules! require_database_role_fixture {
    () => {
        if !database_role_tests_enabled() {
            return;
        }
    };
}

fn role_names() -> DatabaseRoleNames {
    integration_test_database_roles()
}

async fn assert_runtime_rejects(statement: String) {
    let pool = integration_test_pool(2);
    let result = with_connection(&pool, async |connection| {
        diesel::sql_query("BEGIN").execute(&mut *connection).await?;
        let result = diesel::sql_query(statement).execute(&mut *connection).await;
        diesel::sql_query("ROLLBACK")
            .execute(&mut *connection)
            .await?;
        Ok::<_, diesel::result::Error>(result)
    })
    .await
    .expect("privilege probe transaction");
    assert!(
        result.is_err(),
        "runtime unexpectedly executed privileged SQL"
    );
}

#[tokio::test]
async fn generated_runtime_grants_pass_the_catalog_audit() {
    require_database_role_fixture!();
    let pool = integration_test_pool(2);
    let roles = role_names();
    let report = inspect_database_privileges(&pool, roles.runtime(), &roles)
        .await
        .expect("privilege report");

    assert!(
        report.is_safe(),
        "dangerous={:?}, missing={:?}",
        report.dangerous(),
        report.missing()
    );
}

#[tokio::test]
async fn audit_rejects_a_connection_authenticated_as_a_different_role() {
    require_database_role_fixture!();
    let pool = integration_test_migration_pool(1);
    let roles = role_names();
    let report = inspect_database_privileges(&pool, roles.runtime(), &roles)
        .await
        .expect("privilege report");

    assert_eq!(report.connected_role(), roles.migrator().as_str());
    assert!(
        report
            .dangerous()
            .iter()
            .any(|finding| finding.code() == "connected_role_mismatch")
    );
    assert!(!report.is_safe());
}

#[rstest]
#[case("CREATE TABLE public.runtime_must_not_create_objects (id integer)")]
#[case("ALTER TABLE public.groups ADD COLUMN runtime_must_not_alter integer")]
#[case("ALTER TABLE public.groups OWNER TO CURRENT_USER")]
#[case("DROP TABLE public.groups")]
#[case("ALTER TABLE public.groups DISABLE TRIGGER ALL")]
#[tokio::test]
async fn runtime_cannot_change_application_schema(#[case] statement: &str) {
    require_database_role_fixture!();
    assert_runtime_rejects(statement.to_string()).await;
}

#[rstest]
#[case("UPDATE public.collections_history SET valid_to = now()")]
#[case("DELETE FROM public.collections_history")]
#[case("UPDATE public.events SET summary = 'forged'")]
#[case("DELETE FROM public.events")]
#[case(
    "INSERT INTO public.__diesel_schema_migrations(version, run_on) VALUES ('99999999999999', now())"
)]
#[case(
    "INSERT INTO public.restore_success_receipts(id, requested_by_identity_scope, requested_by_name, byte_size, sha256, capability_hash, validation_summary, expires_at, confirmed_at, finished_at, created_at, updated_at) VALUES (999999, 'local', 'forged', 0, repeat('0', 64), repeat('0', 64), '{}'::jsonb, now(), now(), now(), now(), now())"
)]
#[tokio::test]
async fn runtime_cannot_rewrite_integrity_records(#[case] statement: &str) {
    require_database_role_fixture!();
    assert_runtime_rejects(statement.to_string()).await;
}

#[rstest]
#[case::trace_id("trace_id = '6bf92f3577b34da6a3ce929d0e0e4738'")]
#[case::span_id("trace_span_id = '10f067aa0ba902b8'")]
#[case::flags("trace_flags = 0")]
#[case::version("trace_context_version = 1")]
#[case::clear(
    "trace_id = NULL, trace_span_id = NULL, trace_flags = NULL, trace_context_version = NULL"
)]
#[tokio::test]
async fn event_trigger_rejects_trace_rewrites_even_for_the_owner(#[case] assignment: &str) {
    require_database_role_fixture!();
    let pool = integration_test_migration_pool(1);
    let roles = role_names();
    let event = NewEvent::new(
        EntityType::Collection,
        Action::Created,
        ActorKind::System,
        "trace immutability test",
    )
    .unwrap()
    .with_trace_link(
        TraceLink::new("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7", 1, 0).unwrap(),
    );
    let error = with_connection(&pool, async |connection| {
        diesel::sql_query("BEGIN").execute(&mut *connection).await?;
        diesel::sql_query(format!("SET LOCAL ROLE \"{}\"", roles.owner().as_str()))
            .execute(&mut *connection)
            .await?;
        let event = append_event_on_connection(connection, &event).await?;
        let id = event.into_parts().0.id().get();
        let result = diesel::sql_query(format!("UPDATE events SET {assignment} WHERE id = $1"))
            .bind::<BigInt, _>(id)
            .execute(&mut *connection)
            .await;
        diesel::sql_query("ROLLBACK")
            .execute(&mut *connection)
            .await?;
        Ok::<_, hubuum_storage_postgres::PostgresStorageError>(result)
    })
    .await
    .unwrap()
    .unwrap_err();

    assert!(
        error.to_string().contains("events table is append-only"),
        "{error}"
    );
}

#[rstest]
#[case::owner(true)]
#[case::migrator(false)]
#[tokio::test]
async fn runtime_cannot_assume_privileged_roles(#[case] owner: bool) {
    require_database_role_fixture!();
    let roles = role_names();
    let role = if owner {
        roles.owner()
    } else {
        roles.migrator()
    };
    assert_runtime_rejects(format!("SET ROLE \"{}\"", role.as_str())).await;
}

#[tokio::test]
async fn runtime_cannot_grant_itself_owner_membership() {
    require_database_role_fixture!();
    let roles = role_names();
    assert_runtime_rejects(format!(
        "GRANT \"{}\" TO \"{}\"",
        roles.owner().as_str(),
        roles.runtime().as_str()
    ))
    .await;
}

#[tokio::test]
async fn runtime_cannot_grant_itself_table_privileges() {
    require_database_role_fixture!();
    let pool = integration_test_pool(1);
    let roles = role_names();
    let _grant_result = with_connection(&pool, async |connection| {
        diesel::sql_query(format!(
            "GRANT TRUNCATE ON TABLE public.groups TO \"{}\"",
            roles.runtime().as_str()
        ))
        .execute(connection)
        .await
    })
    .await;
    let privilege = with_connection(&pool, async |connection| {
        diesel::sql_query(
            "SELECT pg_catalog.has_table_privilege(\
                current_user, 'public.groups', 'TRUNCATE'\
             ) AS allowed",
        )
        .get_result::<PrivilegeRow>(connection)
        .await
    })
    .await
    .expect("runtime table privilege probe");
    assert!(!privilege.allowed, "runtime granted itself TRUNCATE");
}

#[derive(QueryableByName)]
struct PrivilegeRow {
    #[diesel(sql_type = Bool)]
    allowed: bool,
}

#[derive(QueryableByName)]
struct OwnerRow {
    #[diesel(sql_type = Text)]
    owner_name: String,
}

#[tokio::test]
#[ignore = "run serially by run_tests.sh because reconciliation locks every application table"]
async fn split_role_reconciliation_adopts_existing_single_role_objects() {
    require_database_role_fixture!();
    let roles = role_names();
    let migration_pool = integration_test_migration_pool(1);
    let table_name = format!("single_role_adoption_{}", Uuid::new_v4().simple());

    let (owner_before, owner_after) = with_connection(&migration_pool, async |connection| {
        connection
            .transaction::<_, diesel::result::Error, _>(async |connection| {
                diesel::sql_query(format!(
                    "CREATE TABLE public.\"{table_name}\" (id serial PRIMARY KEY)"
                ))
                .execute(&mut *connection)
                .await?;
                let owner_before = diesel::sql_query(
                    "SELECT pg_catalog.pg_get_userbyid(relation.relowner)::text AS owner_name \
                     FROM pg_catalog.pg_class relation \
                     JOIN pg_catalog.pg_namespace namespace ON namespace.oid = relation.relnamespace \
                     WHERE namespace.nspname = 'public' AND relation.relname = $1",
                )
                .bind::<Text, _>(&table_name)
                .get_result::<OwnerRow>(&mut *connection)
                .await?
                .owner_name;
                connection
                    .batch_execute(&database_role_reconciliation_sql(&roles))
                    .await?;
                let owner_after = diesel::sql_query(
                    "SELECT pg_catalog.pg_get_userbyid(relation.relowner)::text AS owner_name \
                     FROM pg_catalog.pg_class relation \
                     JOIN pg_catalog.pg_namespace namespace ON namespace.oid = relation.relnamespace \
                     WHERE namespace.nspname = 'public' AND relation.relname = $1",
                )
                .bind::<Text, _>(&table_name)
                .get_result::<OwnerRow>(&mut *connection)
                .await?
                .owner_name;
                Ok((owner_before, owner_after))
            })
            .await
    })
    .await
    .expect("single-role adoption transaction");

    assert_eq!(owner_before, roles.migrator().as_str());
    assert_eq!(owner_after, roles.owner().as_str());

    let runtime_pool = integration_test_pool(1);
    with_connection(&runtime_pool, async |connection| {
        diesel::sql_query(format!(
            "INSERT INTO public.\"{table_name}\" (id) VALUES (1)"
        ))
        .execute(&mut *connection)
        .await?;
        diesel::sql_query(format!(
            "UPDATE public.\"{table_name}\" SET id = 2 WHERE id = 1"
        ))
        .execute(&mut *connection)
        .await?;
        diesel::sql_query(format!("DELETE FROM public.\"{table_name}\" WHERE id = 2"))
            .execute(&mut *connection)
            .await
    })
    .await
    .expect("adopted table runtime DML");

    with_connection(&migration_pool, async |connection| {
        connection
            .transaction::<_, diesel::result::Error, _>(async |connection| {
                diesel::sql_query("SELECT set_config('role', $1, true)")
                    .bind::<Text, _>(roles.owner().as_str())
                    .execute(&mut *connection)
                    .await?;
                diesel::sql_query(format!("DROP TABLE public.\"{table_name}\""))
                    .execute(&mut *connection)
                    .await?;
                Ok(())
            })
            .await
    })
    .await
    .expect("adoption fixture cleanup");
}

#[derive(QueryableByName)]
struct SecurityDefinerRow {
    #[diesel(sql_type = Text)]
    function_name: String,
    #[diesel(sql_type = Bool)]
    owner_can_login: bool,
    #[diesel(sql_type = Text)]
    settings: String,
}

#[tokio::test]
async fn privileged_functions_have_non_login_owners_and_fixed_search_paths() {
    require_database_role_fixture!();
    let pool = integration_test_pool(2);
    let rows = with_connection(&pool, async |connection| {
        diesel::sql_query(
            r#"SELECT procedure.proname::text AS function_name,
                      owner.rolcanlogin AS owner_can_login,
                      array_to_string(procedure.proconfig, ',') AS settings
                 FROM pg_catalog.pg_proc procedure
                 JOIN pg_catalog.pg_namespace namespace ON namespace.oid = procedure.pronamespace
                 JOIN pg_catalog.pg_roles owner ON owner.oid = procedure.proowner
                WHERE namespace.nspname = 'public' AND procedure.prosecdef
                ORDER BY procedure.proname"#,
        )
        .load::<SecurityDefinerRow>(connection)
        .await
    })
    .await
    .expect("security-definer catalog rows");

    assert!(
        !rows.is_empty(),
        "expected hardened security-definer functions"
    );
    for row in rows {
        assert!(
            !row.owner_can_login,
            "{} has a login owner",
            row.function_name
        );
        assert_eq!(
            row.settings, "search_path=pg_catalog",
            "{} does not fix its search path",
            row.function_name
        );
    }
}

#[derive(QueryableByName)]
struct InsertedCollection {
    #[diesel(sql_type = Integer)]
    id: i32,
}

#[derive(QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    count: i64,
}

#[tokio::test]
async fn runtime_restore_flag_cannot_suppress_temporal_history() {
    require_database_role_fixture!();
    let pool = integration_test_pool(2);
    let name = format!("runtime-history-guard-{}", Uuid::new_v4());
    let collection_id = with_connection(&pool, async |connection| {
        diesel::sql_query("BEGIN").execute(&mut *connection).await?;
        diesel::sql_query("SELECT set_config('hubuum.restore_history', 'on', true)")
            .execute(&mut *connection)
            .await?;
        let inserted = diesel::sql_query(
            "INSERT INTO public.collections (name, description, parent_collection_id) \
             VALUES (
                 $1,
                 'privilege test',
                 (SELECT id FROM public.collections WHERE parent_collection_id IS NULL LIMIT 1)
             ) RETURNING id",
        )
        .bind::<Text, _>(&name)
        .get_result::<InsertedCollection>(&mut *connection)
        .await?;
        diesel::sql_query("COMMIT")
            .execute(&mut *connection)
            .await?;
        Ok::<_, diesel::result::Error>(inserted.id)
    })
    .await
    .expect("runtime collection insert");

    let count = with_connection(&pool, async |connection| {
        diesel::sql_query(
            "SELECT count(*)::bigint AS count FROM public.collections_history WHERE id = $1",
        )
        .bind::<Integer, _>(collection_id)
        .get_result::<CountRow>(connection)
        .await
    })
    .await
    .expect("history count")
    .count;
    assert_eq!(count, 1);

    with_connection(&pool, async |connection| {
        diesel::sql_query("DELETE FROM public.collections WHERE id = $1")
            .bind::<Integer, _>(collection_id)
            .execute(connection)
            .await
    })
    .await
    .expect("collection cleanup");
}
