"""Exercise database isolation and cleanup without PostgreSQL or Cargo."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from urllib.parse import urlsplit

from support import ROOT


class RustRunnerTests(unittest.TestCase):
    def run_fixture(self, failure="", *, role_tests=None, arguments=(), inherited=None):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copy(ROOT / "run_tests.sh", root / "run_tests.sh")
            binaries = root / "bin"
            binaries.mkdir()
            stub = binaries / "stub"
            stub.write_text(f"#!{sys.executable}\n" + '''
import json
import os
from pathlib import Path
import sys

name = Path(sys.argv[0]).name
args = sys.argv[1:]
call = {"tool": name, "args": args}
for key in ("HUBUUM_DATABASE_URL", "HUBUUM_MIGRATION_DATABASE_URL",
            "HUBUUM_RESTORE_TEST_DATABASE_URL", "HUBUUM_RESTORE_TEST_MIGRATION_DATABASE_URL",
            "HUBUUM_DATABASE_ROLE_TESTS", "HUBUUM_DATABASE_ROLE_MODE",
            "HUBUUM_DATABASE_OWNER_ROLE", "HUBUUM_DATABASE_MIGRATOR_ROLE",
            "HUBUUM_DATABASE_RUNTIME_ROLE", "PGOPTIONS"):
    call[key] = os.environ.get(key)
with open(os.environ["RUNNER_CALLS"], "a") as output:
    output.write(json.dumps(call) + "\\n")
if name == "cargo":
    restore_migration = "--migrate" in args and (call["HUBUUM_DATABASE_URL"] or "").endswith("_restore")
    application_tests = args == ["test", "--features", "integration-test-support,embedded-migrations"]
    if (os.environ["RUNNER_FAILURE"] == "migration" and restore_migration
            or os.environ["RUNNER_FAILURE"] == "tests" and application_tests):
        sys.exit(17)
if name == "diesel" and os.environ["RUNNER_FAILURE"] == "diesel":
    sys.exit(17)
''')
            stub.chmod(0o755)
            for name in ("cargo", "diesel", "psql", "python3"):
                (binaries / name).symlink_to(stub)
            log = root / "calls.jsonl"
            environment = {
                key: value for key, value in os.environ.items()
                if not key.startswith("HUBUUM_") and key != "PGOPTIONS"
            }
            environment.update({
                "PATH": f"{binaries}:{os.environ['PATH']}",
                "HUBUUM_TEST_DB_PASSWORD": "fixture-password",
                "RUNNER_CALLS": str(log),
                "RUNNER_FAILURE": failure,
            })
            environment.update(inherited or {})
            if role_tests is not None:
                environment["HUBUUM_DATABASE_ROLE_TESTS"] = role_tests
            result = subprocess.run(
                ["bash", "run_tests.sh", *arguments], cwd=root, env=environment,
                capture_output=True, text=True, timeout=30,
            )
            calls = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
            return result, calls

    def test_role_tests_default_to_enabled_and_accept_explicit_true(self):
        for role_tests in (None, "true"):
            with self.subTest(role_tests=role_tests):
                result, calls = self.run_fixture(role_tests=role_tests)
                self.assertEqual(result.returncode, 0, result.stderr)
                tests = [call for call in calls
                         if call["tool"] == "cargo" and call["args"][0] == "test"]
                self.assertEqual(len(tests), 4)
                for call in tests:
                    self.assertEqual(call["HUBUUM_DATABASE_ROLE_TESTS"], "true")
                    self.assertEqual(call["HUBUUM_DATABASE_ROLE_MODE"], "split")
                self.assertTrue(any(
                    call["tool"] == "psql" and "scripts/create-test-database-roles.sql" in call["args"]
                    for call in calls
                ))

    def test_invalid_role_test_setting_fails_before_building_or_provisioning(self):
        for role_tests in ("", "TRUE", "0", "yes", "false "):
            with self.subTest(role_tests=role_tests):
                result, calls = self.run_fixture(role_tests=role_tests)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("HUBUUM_DATABASE_ROLE_TESTS", result.stdout + result.stderr)
                self.assertFalse(any(call["tool"] in {"cargo", "diesel", "psql"} for call in calls))

    def test_single_login_runs_general_suites_with_separate_migrated_restore_database(self):
        result, calls = self.run_fixture(role_tests="false")
        self.assertEqual(result.returncode, 0, result.stderr)
        tests = [call for call in calls if call["tool"] == "cargo" and call["args"][0] == "test"]
        self.assertEqual([call["args"] for call in tests], [
            ["test", "--workspace", "--exclude", "hubuum", "--exclude", "hubuum-storage-postgres"],
            ["test", "-p", "hubuum-storage-postgres", "--features",
             "integration-test-support,scale-benchmark-support,embedded-migrations"],
            ["test", "--features", "integration-test-support,embedded-migrations"],
        ])
        self.assertTrue(any(
            call["tool"] == "python3" and call["args"] == [
                "tests/python/run.py", "integration", "schema-budget",
            ] for call in calls
        ))
        for call in tests:
            self.assertEqual(call["HUBUUM_DATABASE_ROLE_TESTS"], "false")
            self.assertEqual(call["HUBUUM_DATABASE_ROLE_MODE"], "single")
            application = urlsplit(call["HUBUUM_DATABASE_URL"])
            restore = urlsplit(call["HUBUUM_RESTORE_TEST_DATABASE_URL"])
            self.assertEqual(application.username, "postgres")
            self.assertEqual(restore.username, application.username)
            self.assertNotEqual(application.path, restore.path)
            self.assertEqual(call["HUBUUM_DATABASE_URL"], call["HUBUUM_MIGRATION_DATABASE_URL"])
            self.assertEqual(call["HUBUUM_RESTORE_TEST_DATABASE_URL"],
                             call["HUBUUM_RESTORE_TEST_MIGRATION_DATABASE_URL"])
        first_test = calls.index(tests[0])
        for key in ("HUBUUM_DATABASE_URL", "HUBUUM_RESTORE_TEST_DATABASE_URL"):
            database_url = tests[0][key]
            self.assertTrue(any(
                call["tool"] == "cargo" and "--migrate" in call["args"]
                and call["HUBUUM_DATABASE_URL"] == database_url
                and call["HUBUUM_MIGRATION_DATABASE_URL"] == database_url
                for call in calls[:first_test]
            ), f"missing production migration for {key}")

    def test_single_login_clears_inherited_split_role_configuration(self):
        roles = ("HUBUUM_DATABASE_OWNER_ROLE", "HUBUUM_DATABASE_MIGRATOR_ROLE",
                 "HUBUUM_DATABASE_RUNTIME_ROLE")
        inherited = {name: "stale_role" for name in roles}
        inherited.update({
            "HUBUUM_DATABASE_ROLE_MODE": "split",
            "HUBUUM_MIGRATION_DATABASE_URL": "postgres://stale_role:password@localhost/stale",
            "PGOPTIONS": "-c role=stale_role",
        })
        result, calls = self.run_fixture(role_tests="false", inherited=inherited)
        self.assertEqual(result.returncode, 0, result.stderr)
        for call in calls:
            if call["tool"] in {"psql", "diesel"} or (
                    call["tool"] == "cargo" and call["args"][0] in {"run", "test"}):
                self.assertFalse(call["PGOPTIONS"])
            if call["tool"] == "cargo" and call["args"][0] in {"run", "test"}:
                for name in roles:
                    self.assertIsNone(call[name])
        sql = "\n".join(" ".join(call["args"]) for call in calls if call["tool"] == "psql")
        self.assertNotIn("create-test-database-roles.sql", sql)
        self.assertNotIn("DROP ROLE", sql)
        self.assertNotIn("REVOKE", sql)
        self.assertNotIn(" OWNER ", sql)

    def test_single_login_failure_removes_every_created_database_without_roles(self):
        for failure in ("diesel", "migration", "tests"):
            with self.subTest(failure=failure):
                result, calls = self.run_fixture(failure, role_tests="false")
                self.assertNotEqual(result.returncode, 0)
                sql = [call["args"][call["args"].index("-c") + 1] for call in calls
                       if call["tool"] == "psql" and "-c" in call["args"]]
                created = [statement.removeprefix("CREATE DATABASE ").removesuffix(";")
                           for statement in sql if statement.startswith("CREATE DATABASE ")]
                self.assertTrue(created)
                for name in created:
                    self.assertTrue(f"DROP DATABASE {name};" in sql
                                    or f"DROP DATABASE IF EXISTS {name};" in sql,
                                    f"database {name} was not cleaned up after {failure}")
                self.assertFalse(any("DROP ROLE" in statement for statement in sql))

    def test_targeted_arguments_are_forwarded_to_each_general_suite(self):
        arguments = ["some::test", "--", "--exact", "--nocapture"]
        for role_tests in ("true", "false"):
            with self.subTest(role_tests=role_tests):
                result, calls = self.run_fixture(role_tests=role_tests, arguments=arguments)
                self.assertEqual(result.returncode, 0, result.stderr)
                tests = [call for call in calls
                         if call["tool"] == "cargo" and call["args"][0] == "test"]
                self.assertEqual(len(tests), 3)
                for call in tests:
                    self.assertEqual(call["args"][-len(arguments):], arguments)
                self.assertFalse(any("schema-budget" in call["args"] for call in calls))

    def test_restore_contract_uses_separately_migrated_database_with_split_roles(self):
        result, calls = self.run_fixture()
        self.assertEqual(result.returncode, 0, result.stderr)
        tests = next(call for call in calls if call["tool"] == "cargo" and call["args"] == [
            "test", "--features", "integration-test-support,embedded-migrations",
        ])
        application = urlsplit(tests["HUBUUM_DATABASE_URL"])
        runtime = urlsplit(tests["HUBUUM_RESTORE_TEST_DATABASE_URL"])
        migrator = urlsplit(tests["HUBUUM_RESTORE_TEST_MIGRATION_DATABASE_URL"])
        self.assertNotEqual(application.path, runtime.path)
        self.assertEqual(runtime.path, migrator.path)
        self.assertNotEqual(runtime.username, migrator.username)
        self.assertEqual(application.username, runtime.username)
        self.assertTrue(any(
            call["tool"] == "cargo" and "--migrate" in call["args"]
            and call["HUBUUM_DATABASE_URL"] == runtime.geturl()
            and call["HUBUUM_MIGRATION_DATABASE_URL"] == migrator.geturl()
            for call in calls
        ))

    def test_failure_removes_both_databases_before_removing_shared_roles(self):
        for failure in ("migration", "tests"):
            with self.subTest(failure=failure):
                result, calls = self.run_fixture(failure)
                self.assertNotEqual(result.returncode, 0)
                restore = next(call for call in calls if call["tool"] == "cargo"
                               and (call["HUBUUM_DATABASE_URL"] or "").endswith("_restore"))
                restore_name = urlsplit(restore["HUBUUM_DATABASE_URL"]).path.removeprefix("/")
                application_name = restore_name.removesuffix("_restore")
                sql = [call["args"][-1] for call in calls
                       if call["tool"] == "psql" and "-c" in call["args"]]
                for name in (restore_name, application_name):
                    self.assertIn(f"DROP DATABASE IF EXISTS {name};", sql)
                self.assertIn("DROP ROLE IF EXISTS", sql[-1])
