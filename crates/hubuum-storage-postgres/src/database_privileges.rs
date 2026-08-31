//! Generated PostgreSQL role grants and catalog-backed privilege diagnostics.

use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::LazyLock;

use diesel::QueryableByName;
use diesel::sql_types::{Bool, Text};
use diesel_async::RunQueryDsl;
use serde::{Deserialize, Serialize};

use crate::{NoopPostgresObserver, PostgresPool, PostgresRuntime, PostgresStorageError};

const MANIFEST_JSON: &str = include_str!("../database-privileges.json");

static MANIFEST: LazyLock<DatabasePrivilegeManifest> = LazyLock::new(|| {
    serde_json::from_str(MANIFEST_JSON).expect("database privilege manifest must be valid JSON")
});

#[derive(Debug, Deserialize)]
struct DatabasePrivilegeManifest {
    schema: String,
    migration_table: String,
    history_table_suffix: String,
    events_table: String,
    event_update_columns: Vec<String>,
    runtime_functions: Vec<String>,
    runtime_function_policy: String,
    restore: String,
    built_in_capabilities: Vec<String>,
}

/// Validated PostgreSQL identifier used by generated role-management SQL.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatabaseRoleName(String);

impl DatabaseRoleName {
    pub fn new(value: impl Into<String>) -> Result<Self, PostgresStorageError> {
        let value = value.into();
        let mut characters = value.chars();
        let valid_start = characters
            .next()
            .is_some_and(|character| character == '_' || character.is_ascii_alphabetic());
        let valid_rest = characters.all(|character| {
            character == '_' || character == '$' || character.is_ascii_alphanumeric()
        });
        if value.len() > 63 || !valid_start || !valid_rest {
            return Err(PostgresStorageError::invalid_input(
                "database role names must be 1-63 ASCII identifier characters and start with a letter or underscore",
            ));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn quoted(&self) -> String {
        format!("\"{}\"", self.0.replace('"', "\"\""))
    }
}

/// Operator-selected names for Hubuum's three supported database roles.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatabaseRoleNames {
    owner: DatabaseRoleName,
    migrator: DatabaseRoleName,
    runtime: DatabaseRoleName,
}

impl DatabaseRoleNames {
    pub fn new(
        owner: impl Into<String>,
        migrator: impl Into<String>,
        runtime: impl Into<String>,
    ) -> Result<Self, PostgresStorageError> {
        let names = Self {
            owner: DatabaseRoleName::new(owner)?,
            migrator: DatabaseRoleName::new(migrator)?,
            runtime: DatabaseRoleName::new(runtime)?,
        };
        if names.owner == names.migrator
            || names.owner == names.runtime
            || names.migrator == names.runtime
        {
            return Err(PostgresStorageError::invalid_input(
                "database owner, migrator, and runtime role names must be distinct",
            ));
        }
        Ok(names)
    }

    #[must_use]
    pub fn owner(&self) -> &DatabaseRoleName {
        &self.owner
    }

    #[must_use]
    pub fn migrator(&self) -> &DatabaseRoleName {
        &self.migrator
    }

    #[must_use]
    pub fn runtime(&self) -> &DatabaseRoleName {
        &self.runtime
    }
}

/// Finding emitted by the runtime database privilege audit.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DatabasePrivilegeFinding {
    code: String,
    object: String,
    detail: String,
}

impl DatabasePrivilegeFinding {
    fn new(code: &str, object: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            code: code.to_string(),
            object: object.into(),
            detail: detail.into(),
        }
    }

    #[must_use]
    pub fn code(&self) -> &str {
        &self.code
    }

    #[must_use]
    pub fn object(&self) -> &str {
        &self.object
    }

    #[must_use]
    pub fn detail(&self) -> &str {
        &self.detail
    }
}

/// Non-secret result of checking one PostgreSQL role against the manifest.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DatabasePrivilegeReport {
    role: String,
    connected_role: String,
    safe: bool,
    dangerous: Vec<DatabasePrivilegeFinding>,
    missing: Vec<DatabasePrivilegeFinding>,
}

impl DatabasePrivilegeReport {
    #[must_use]
    pub fn role(&self) -> &str {
        &self.role
    }

    #[must_use]
    pub fn connected_role(&self) -> &str {
        &self.connected_role
    }

    #[must_use]
    pub const fn is_safe(&self) -> bool {
        self.safe
    }

    #[must_use]
    pub fn dangerous(&self) -> &[DatabasePrivilegeFinding] {
        &self.dangerous
    }

    #[must_use]
    pub fn missing(&self) -> &[DatabasePrivilegeFinding] {
        &self.missing
    }
}

#[derive(QueryableByName)]
struct RoleRow {
    #[diesel(sql_type = Text)]
    role_name: String,
    #[diesel(sql_type = Text)]
    connected_role: String,
    #[diesel(sql_type = Bool)]
    superuser: bool,
    #[diesel(sql_type = Bool)]
    create_role: bool,
    #[diesel(sql_type = Bool)]
    create_database: bool,
    #[diesel(sql_type = Bool)]
    bypass_rls: bool,
    #[diesel(sql_type = Bool)]
    owns_schema: bool,
    #[diesel(sql_type = Bool)]
    creates_in_schema: bool,
    #[diesel(sql_type = Bool)]
    owner_member: bool,
    #[diesel(sql_type = Bool)]
    migrator_member: bool,
}

#[derive(QueryableByName)]
struct ObjectPrivilegeRow {
    #[diesel(sql_type = Text)]
    object_kind: String,
    #[diesel(sql_type = Text)]
    object_name: String,
    #[diesel(sql_type = Bool)]
    owned: bool,
    #[diesel(sql_type = Bool)]
    can_select: bool,
    #[diesel(sql_type = Bool)]
    can_insert: bool,
    #[diesel(sql_type = Bool)]
    can_update: bool,
    #[diesel(sql_type = Bool)]
    can_delete: bool,
    #[diesel(sql_type = Bool)]
    security_definer: bool,
}

#[derive(QueryableByName)]
struct ColumnPrivilegeRow {
    #[diesel(sql_type = Text)]
    column_name: String,
    #[diesel(sql_type = Bool)]
    can_update: bool,
}

/// Return the version-controlled manifest verbatim for tooling and docs.
#[must_use]
pub const fn database_privilege_manifest_json() -> &'static str {
    MANIFEST_JSON
}

/// Generate the idempotent administrator SQL that creates and reconciles all roles.
#[must_use]
pub fn database_role_setup_sql(names: &DatabaseRoleNames) -> String {
    let mut sql = String::new();
    writeln!(
        sql,
        "-- Generated from crates/hubuum-storage-postgres/database-privileges.json."
    )
    .unwrap();
    writeln!(sql, "-- Set passwords or workload-identity mappings separately; this output never contains credentials.").unwrap();
    writeln!(sql, "BEGIN;").unwrap();
    for (role, login) in [
        (names.owner(), false),
        (names.migrator(), true),
        (names.runtime(), true),
    ] {
        writeln!(
            sql,
            "DO $role$ BEGIN IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '{}') THEN CREATE ROLE {} {}; END IF; END $role$;",
            role.as_str(),
            role.quoted(),
            if login { "LOGIN" } else { "NOLOGIN" },
        )
        .unwrap();
        writeln!(
            sql,
            "ALTER ROLE {} {} NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;",
            role.quoted(),
            if login { "LOGIN" } else { "NOLOGIN" },
        )
        .unwrap();
    }
    writeln!(
        sql,
        "GRANT {} TO {};",
        names.owner().quoted(),
        names.migrator().quoted(),
    )
    .unwrap();
    writeln!(
        sql,
        "REVOKE {}, {} FROM {};",
        names.owner().quoted(),
        names.migrator().quoted(),
        names.runtime().quoted(),
    )
    .unwrap();
    sql.push_str(&database_role_reconciliation_sql(names));
    writeln!(sql, "COMMIT;").unwrap();
    sql
}

/// Generate ownership and grants after migrations have created new objects.
#[must_use]
pub fn database_role_reconciliation_sql(names: &DatabaseRoleNames) -> String {
    let manifest = &*MANIFEST;
    assert_eq!(
        manifest.runtime_function_policy, "all_security_invoker_plus_allowlisted_security_definer",
        "unsupported database privilege function policy"
    );
    let owner = names.owner().quoted();
    let runtime = names.runtime().quoted();
    let update_columns = manifest
        .event_update_columns
        .iter()
        .map(|column| format!("\"{column}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let runtime_functions = manifest
        .runtime_functions
        .iter()
        .map(|function| format!("'{function}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut sql = String::new();
    writeln!(
        sql,
        "REVOKE CREATE ON SCHEMA \"{}\" FROM PUBLIC;",
        manifest.schema
    )
    .unwrap();
    writeln!(
        sql,
        "ALTER SCHEMA \"{}\" OWNER TO {owner};",
        manifest.schema
    )
    .unwrap();
    writeln!(
        sql,
        "REVOKE ALL ON SCHEMA \"{}\" FROM {runtime};",
        manifest.schema
    )
    .unwrap();
    writeln!(
        sql,
        "GRANT USAGE ON SCHEMA \"{}\" TO {runtime};",
        manifest.schema
    )
    .unwrap();
    writeln!(
        sql,
        "DO $grants$ DECLARE object RECORD; BEGIN\n  FOR object IN SELECT c.oid, c.relname, c.relkind FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = '{}' AND c.relkind IN ('r', 'p', 'S') LOOP\n    IF object.relkind = 'S' THEN\n      EXECUTE pg_catalog.format('ALTER SEQUENCE %I.%I OWNER TO %I', '{}', object.relname, '{}');\n      EXECUTE pg_catalog.format('REVOKE ALL ON SEQUENCE %I.%I FROM PUBLIC', '{}', object.relname);\n      EXECUTE pg_catalog.format('REVOKE ALL ON SEQUENCE %I.%I FROM %I', '{}', object.relname, '{}');\n      EXECUTE pg_catalog.format('GRANT USAGE, SELECT ON SEQUENCE %I.%I TO %I', '{}', object.relname, '{}');\n    ELSE\n      EXECUTE pg_catalog.format('ALTER TABLE %I.%I OWNER TO %I', '{}', object.relname, '{}');\n      EXECUTE pg_catalog.format('REVOKE ALL ON TABLE %I.%I FROM PUBLIC', '{}', object.relname);\n      EXECUTE pg_catalog.format('REVOKE ALL ON TABLE %I.%I FROM %I', '{}', object.relname, '{}');\n      IF object.relname = '{}' OR object.relname LIKE '%' || '{}' THEN\n        EXECUTE pg_catalog.format('GRANT SELECT ON TABLE %I.%I TO %I', '{}', object.relname, '{}');\n      ELSIF object.relname = '{}' THEN\n        EXECUTE pg_catalog.format('GRANT SELECT, INSERT ON TABLE %I.%I TO %I', '{}', object.relname, '{}');\n        EXECUTE pg_catalog.format('GRANT UPDATE ({}) ON TABLE %I.%I TO %I', '{}', object.relname, '{}');\n      ELSE\n        EXECUTE pg_catalog.format('GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE %I.%I TO %I', '{}', object.relname, '{}');\n      END IF;\n    END IF;\n  END LOOP;\nEND $grants$;",
        manifest.schema,
        manifest.schema,
        names.owner().as_str(),
        manifest.schema,
        manifest.schema,
        names.runtime().as_str(),
        manifest.schema,
        names.runtime().as_str(),
        manifest.schema,
        names.owner().as_str(),
        manifest.schema,
        manifest.schema,
        names.runtime().as_str(),
        manifest.migration_table,
        manifest.history_table_suffix,
        manifest.schema,
        names.runtime().as_str(),
        manifest.events_table,
        manifest.schema,
        names.runtime().as_str(),
        update_columns,
        manifest.schema,
        names.runtime().as_str(),
        manifest.schema,
        names.runtime().as_str(),
    )
    .unwrap();
    writeln!(
        sql,
        "DO $functions$ DECLARE object RECORD; BEGIN\n  FOR object IN SELECT p.oid, p.proname, p.prosecdef, p.oid::pg_catalog.regprocedure AS identity FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = '{}' LOOP\n    EXECUTE pg_catalog.format('ALTER FUNCTION %s OWNER TO %I', object.identity, '{}');\n    EXECUTE pg_catalog.format('REVOKE ALL ON FUNCTION %s FROM PUBLIC', object.identity);\n    EXECUTE pg_catalog.format('REVOKE ALL ON FUNCTION %s FROM %I', object.identity, '{}');\n    IF NOT object.prosecdef OR object.proname IN ({}) THEN\n      EXECUTE pg_catalog.format('GRANT EXECUTE ON FUNCTION %s TO %I', object.identity, '{}');\n    END IF;\n  END LOOP;\nEND $functions$;",
        manifest.schema,
        names.owner().as_str(),
        names.runtime().as_str(),
        runtime_functions,
        names.runtime().as_str(),
    )
    .unwrap();
    writeln!(sql, "ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA \"{}\" REVOKE ALL ON TABLES FROM PUBLIC;", manifest.schema).unwrap();
    writeln!(sql, "ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA \"{}\" GRANT SELECT ON TABLES TO {runtime};", manifest.schema).unwrap();
    writeln!(sql, "ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA \"{}\" REVOKE ALL ON SEQUENCES FROM PUBLIC;", manifest.schema).unwrap();
    writeln!(sql, "ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA \"{}\" GRANT USAGE, SELECT ON SEQUENCES TO {runtime};", manifest.schema).unwrap();
    writeln!(sql, "ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA \"{}\" REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;", manifest.schema).unwrap();
    sql
}

/// Inspect a role against all current database objects using manifest rules.
pub async fn inspect_database_privileges(
    pool: &PostgresPool,
    role: &DatabaseRoleName,
    names: &DatabaseRoleNames,
) -> Result<DatabasePrivilegeReport, PostgresStorageError> {
    let manifest = &*MANIFEST;
    let runtime = PostgresRuntime::new(pool.clone(), Arc::new(NoopPostgresObserver));
    let role_row = runtime.with_connection(async |connection| {
        diesel::sql_query(
            r#"SELECT role.rolname::text AS role_name, current_user::text AS connected_role,
                      role.rolsuper AS superuser,
                      role.rolcreaterole AS create_role, role.rolcreatedb AS create_database,
                      role.rolbypassrls AS bypass_rls,
                      EXISTS (SELECT FROM pg_catalog.pg_namespace n
                              WHERE n.nspname = $2 AND n.nspowner = role.oid) AS owns_schema,
                      pg_catalog.has_schema_privilege(role.oid, $2::text, 'CREATE') AS creates_in_schema,
                      EXISTS (SELECT FROM pg_catalog.pg_roles expected
                              WHERE expected.rolname = $3
                                AND pg_catalog.pg_has_role(role.oid, expected.oid, 'MEMBER')) AS owner_member,
                      EXISTS (SELECT FROM pg_catalog.pg_roles expected
                              WHERE expected.rolname = $4
                                AND pg_catalog.pg_has_role(role.oid, expected.oid, 'MEMBER')) AS migrator_member
               FROM pg_catalog.pg_roles role
              WHERE role.rolname = $1"#,
        )
        .bind::<Text, _>(role.as_str())
        .bind::<Text, _>(&manifest.schema)
        .bind::<Text, _>(names.owner().as_str())
        .bind::<Text, _>(names.migrator().as_str())
        .get_result::<RoleRow>(connection)
        .await
    })
    .await
    .map_err(|error| {
        PostgresStorageError::database(format!(
            "failed to inspect database role '{}': {error}",
            role.as_str()
        ))
    })?;

    let objects = runtime.with_connection(async |connection| {
        diesel::sql_query(
            r#"SELECT 'table'::text AS object_kind, c.relname::text AS object_name,
                      pg_catalog.pg_has_role(role.oid, c.relowner, 'MEMBER') AS owned,
                      pg_catalog.has_table_privilege(role.oid, c.oid, 'SELECT') AS can_select,
                      pg_catalog.has_table_privilege(role.oid, c.oid, 'INSERT') AS can_insert,
                      pg_catalog.has_table_privilege(role.oid, c.oid, 'UPDATE') AS can_update,
                      pg_catalog.has_table_privilege(role.oid, c.oid, 'DELETE') AS can_delete,
                      false AS security_definer
               FROM pg_catalog.pg_class c
               JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
               CROSS JOIN pg_catalog.pg_roles role
              WHERE role.rolname = $1 AND n.nspname = $2 AND c.relkind IN ('r', 'p')
              UNION ALL
             SELECT 'sequence'::text, c.relname::text,
                    pg_catalog.pg_has_role(role.oid, c.relowner, 'MEMBER'),
                    pg_catalog.has_sequence_privilege(role.oid, c.oid, 'SELECT'),
                    pg_catalog.has_sequence_privilege(role.oid, c.oid, 'USAGE'), false, false, false
               FROM pg_catalog.pg_class c
               JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
               CROSS JOIN pg_catalog.pg_roles role
              WHERE role.rolname = $1 AND n.nspname = $2 AND c.relkind = 'S'
              UNION ALL
             SELECT 'function'::text, p.oid::pg_catalog.regprocedure::text,
                    pg_catalog.pg_has_role(role.oid, p.proowner, 'MEMBER'),
                    pg_catalog.has_function_privilege(role.oid, p.oid, 'EXECUTE'), false, false, false,
                    p.prosecdef
               FROM pg_catalog.pg_proc p
               JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
               CROSS JOIN pg_catalog.pg_roles role
              WHERE role.rolname = $1 AND n.nspname = $2"#,
        )
        .bind::<Text, _>(role.as_str())
        .bind::<Text, _>(&manifest.schema)
        .load::<ObjectPrivilegeRow>(connection)
        .await
    })
    .await?;

    let event_columns = runtime
        .with_connection(async |connection| {
            diesel::sql_query(
                r#"SELECT attribute.attname::text AS column_name,
                          pg_catalog.has_column_privilege(
                              role.oid, relation.oid, attribute.attnum, 'UPDATE'
                          ) AS can_update
                     FROM pg_catalog.pg_class relation
                     JOIN pg_catalog.pg_namespace namespace ON namespace.oid = relation.relnamespace
                     JOIN pg_catalog.pg_attribute attribute ON attribute.attrelid = relation.oid
                     CROSS JOIN pg_catalog.pg_roles role
                    WHERE role.rolname = $1 AND namespace.nspname = $2
                      AND relation.relname = $3
                      AND attribute.attnum > 0 AND NOT attribute.attisdropped"#,
            )
            .bind::<Text, _>(role.as_str())
            .bind::<Text, _>(&manifest.schema)
            .bind::<Text, _>(&manifest.events_table)
            .load::<ColumnPrivilegeRow>(connection)
            .await
        })
        .await?;

    let mut dangerous = Vec::new();
    let mut missing = Vec::new();
    if role_row.connected_role != role_row.role_name {
        dangerous.push(DatabasePrivilegeFinding::new(
            "connected_role_mismatch",
            &role_row.connected_role,
            format!(
                "database connection is authenticated as '{}' instead of the audited runtime role '{}'",
                role_row.connected_role, role_row.role_name
            ),
        ));
    }
    for (condition, code, detail) in [
        (
            role_row.superuser,
            "role_superuser",
            "role is a PostgreSQL superuser",
        ),
        (
            role_row.create_role,
            "role_create_role",
            "role can create or alter roles",
        ),
        (
            role_row.create_database,
            "role_create_database",
            "role can create databases",
        ),
        (
            role_row.bypass_rls,
            "role_bypass_rls",
            "role can bypass row-level security",
        ),
        (
            role_row.owns_schema,
            "owns_schema",
            "role owns the application schema",
        ),
        (
            role_row.creates_in_schema,
            "schema_create",
            "role can create objects in the application schema",
        ),
        (
            role_row.owner_member,
            "owner_membership",
            "role is a member of the schema-owner role",
        ),
        (
            role_row.migrator_member,
            "migrator_membership",
            "role is a member of the migrator role",
        ),
    ] {
        if condition {
            dangerous.push(DatabasePrivilegeFinding::new(
                code,
                &manifest.schema,
                detail,
            ));
        }
    }
    for object in objects {
        if object.owned {
            dangerous.push(DatabasePrivilegeFinding::new(
                "owns_object",
                format!("{}:{}", object.object_kind, object.object_name),
                "runtime role owns or can assume the owner of an application object",
            ));
        }
        match object.object_kind.as_str() {
            "table" if object.object_name == manifest.migration_table => {
                if !object.can_select {
                    missing.push(DatabasePrivilegeFinding::new(
                        "missing_migration_read",
                        &object.object_name,
                        "runtime readiness requires SELECT",
                    ));
                }
                if object.can_insert || object.can_update || object.can_delete {
                    dangerous.push(DatabasePrivilegeFinding::new(
                        "migration_table_write",
                        &object.object_name,
                        "runtime role can modify migration records",
                    ));
                }
            }
            "table" if object.object_name.ends_with(&manifest.history_table_suffix) => {
                if !object.can_select {
                    missing.push(DatabasePrivilegeFinding::new(
                        "missing_history_read",
                        &object.object_name,
                        "runtime history APIs require SELECT",
                    ));
                }
                if object.can_update || object.can_delete {
                    dangerous.push(DatabasePrivilegeFinding::new(
                        "history_mutation",
                        &object.object_name,
                        "runtime role can directly update or delete temporal history",
                    ));
                }
            }
            "table" if object.object_name == manifest.events_table => {
                if !object.can_select || !object.can_insert {
                    missing.push(DatabasePrivilegeFinding::new(
                        "missing_event_access",
                        &object.object_name,
                        "runtime requires SELECT and INSERT on audit events",
                    ));
                }
                if object.can_delete || object.can_update {
                    dangerous.push(DatabasePrivilegeFinding::new(
                        "event_broad_mutation",
                        &object.object_name,
                        "runtime has table-level UPDATE or DELETE on append-only events",
                    ));
                }
            }
            "table" => {
                if !(object.can_select
                    && object.can_insert
                    && object.can_update
                    && object.can_delete)
                {
                    missing.push(DatabasePrivilegeFinding::new(
                        "missing_table_dml",
                        &object.object_name,
                        "runtime requires SELECT, INSERT, UPDATE, and DELETE",
                    ));
                }
            }
            "sequence" => {
                if !(object.can_select && object.can_insert) {
                    missing.push(DatabasePrivilegeFinding::new(
                        "missing_sequence_access",
                        &object.object_name,
                        "runtime requires SELECT and USAGE",
                    ));
                }
            }
            "function" => {
                let base_name = object
                    .object_name
                    .split('(')
                    .next()
                    .unwrap_or(&object.object_name);
                let required = !object.security_definer
                    || manifest
                        .runtime_functions
                        .iter()
                        .any(|name| name == base_name);
                if required && !object.can_select {
                    missing.push(DatabasePrivilegeFinding::new(
                        "missing_function_execute",
                        &object.object_name,
                        "runtime requires EXECUTE",
                    ));
                } else if !required && object.can_select {
                    dangerous.push(DatabasePrivilegeFinding::new(
                        "unexpected_function_execute",
                        &object.object_name,
                        "runtime can execute a non-runtime application function directly",
                    ));
                }
            }
            _ => {}
        }
    }
    for column in event_columns {
        let required = manifest
            .event_update_columns
            .iter()
            .any(|required| required == &column.column_name);
        if required && !column.can_update {
            missing.push(DatabasePrivilegeFinding::new(
                "missing_event_column_update",
                format!("{}.{}", manifest.events_table, column.column_name),
                "runtime event workers require column-level UPDATE",
            ));
        } else if !required && column.can_update {
            dangerous.push(DatabasePrivilegeFinding::new(
                "event_immutable_column_update",
                format!("{}.{}", manifest.events_table, column.column_name),
                "runtime can update immutable audit event content",
            ));
        }
    }
    let safe = dangerous.is_empty() && missing.is_empty();
    Ok(DatabasePrivilegeReport {
        role: role_row.role_name,
        connected_role: role_row.connected_role,
        safe,
        dangerous,
        missing,
    })
}

/// Machine-readable details that are not expressible as ordinary SQL grants.
#[must_use]
pub fn database_privilege_capabilities() -> (&'static str, &'static [String]) {
    (&MANIFEST.restore, &MANIFEST.built_in_capabilities)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names() -> DatabaseRoleNames {
        DatabaseRoleNames::new("hubuum_owner", "hubuum_migrator", "hubuum_runtime").unwrap()
    }

    #[test]
    fn role_names_reject_unsafe_sql_identifiers() {
        for value in [
            "",
            "1runtime",
            "runtime; DROP ROLE postgres",
            "runtime-role",
        ] {
            assert!(DatabaseRoleName::new(value).is_err(), "accepted {value:?}");
        }
    }

    #[test]
    fn setup_sql_is_derived_from_the_manifest_and_contains_no_credentials() {
        let sql = database_role_setup_sql(&names());
        assert!(sql.contains("hubuum_complete_event_retention_purge"));
        assert!(sql.contains(
            "GRANT UPDATE (\"dispatched_at\", \"fanout_locked_until\", \"fanout_claim_token\")"
        ));
        assert!(sql.contains("LIKE '%' || '_history'"));
        assert!(sql.contains("REVOKE ALL ON SCHEMA \"public\" FROM \"hubuum_runtime\""));
        assert!(!sql.to_ascii_lowercase().contains(" password '"));
    }

    #[test]
    fn manifest_declares_privileged_restore_and_builtin_capabilities() {
        let (restore, capabilities) = database_privilege_capabilities();
        assert_eq!(restore, "isolated_migrator_executor");
        assert!(capabilities.iter().any(|value| value == "listen_notify"));
        assert_eq!(
            MANIFEST.runtime_function_policy,
            "all_security_invoker_plus_allowlisted_security_definer"
        );
    }
}
