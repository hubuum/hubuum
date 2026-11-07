"""Verify isolated restore provisioning and cleanup around CI test commands."""

import os
from pathlib import Path
import subprocess
import unittest
from unittest.mock import patch
from urllib.parse import urlsplit

from integration.rust_tests import run_tests


class RustCiFixtureTests(unittest.TestCase):
    def exercise(self, failure=None):
        calls = []

        def run(command, **kwargs):
            calls.append((command, kwargs))
            if command[-1] == "--migrate" and (failure == "migration" or
                    failure == "archive_migration" and
                    "/hubuum_archive_" in kwargs["env"]["HUBUUM_DATABASE_URL"]):
                raise subprocess.CalledProcessError(17, command)
            return subprocess.CompletedProcess(command, 23 if command[0] == "cargo" else 0)

        with patch.dict(os.environ, {"HUBUUM_DATABASE_URL": "postgres://root@localhost/application",
                                     "PG_BIN": "/test/postgres/bin"}), patch(
            "integration.rust_tests.subprocess.run", side_effect=run,
        ):
            try:
                code = run_tests(["cargo", "test"], Path("hubuum-admin"),
                                 "postgres://root@localhost:5433/application?sslmode=require")
            except subprocess.CalledProcessError:
                code = 17
        return code, calls

    def test_runtime_and_restore_authority_are_distinct_from_application(self):
        code, calls = self.exercise()
        self.assertEqual(code, 23)
        env = next(kwargs["env"] for command, kwargs in calls if command[0] == "cargo")
        runtime = urlsplit(env["HUBUUM_RESTORE_TEST_DATABASE_URL"])
        migrator = urlsplit(env["HUBUUM_RESTORE_TEST_MIGRATION_DATABASE_URL"])
        self.assertNotEqual(runtime.username, migrator.username)
        self.assertEqual(runtime.path, migrator.path)
        self.assertNotEqual(runtime.path, "/application")
        self.assertEqual((runtime.port, runtime.query), (5433, "sslmode=require"))
        self.assertEqual(env["HUBUUM_DATABASE_URL"], "postgres://root@localhost/application")

    def test_migration_failure_still_removes_database_and_roles(self):
        code, calls = self.exercise("migration")
        self.assertEqual(code, 17)
        self.assertFalse(any(command[0] == "cargo" for command, _ in calls))
        self.assertIn("DROP DATABASE IF EXISTS", calls[-1][1]["input"])
        self.assertEqual(calls[-1][1]["input"].count("DROP ROLE IF EXISTS"), 3)

    def test_archive_owners_have_separate_migrated_storage(self):
        code, calls = self.exercise()
        self.assertEqual(code, 23)
        env = next(kwargs["env"] for command, kwargs in calls if command[0] == "cargo")
        runtime = urlsplit(env["HUBUUM_ARCHIVE_TEST_DATABASE_URL"])
        migrator = urlsplit(env["HUBUUM_ARCHIVE_TEST_MIGRATION_DATABASE_URL"])
        self.assertEqual(runtime.path, migrator.path)
        self.assertNotEqual(runtime.username, migrator.username)
        self.assertNotEqual(runtime.path, urlsplit(env["HUBUUM_RESTORE_TEST_DATABASE_URL"]).path)
        self.assertNotEqual(runtime.path, urlsplit(env["HUBUUM_DATABASE_URL"]).path)
        self.assertTrue(any(
            command[-1] == "--migrate" and
            kwargs["env"]["HUBUUM_DATABASE_URL"] == runtime.geturl() and
            kwargs["env"]["HUBUUM_MIGRATION_DATABASE_URL"] == migrator.geturl()
            for command, kwargs in calls
        ))
        self.assertIn(f"DROP DATABASE IF EXISTS {runtime.path[1:]}", calls[-1][1]["input"])

    def test_archive_migration_failure_cleans_up_both_databases(self):
        code, calls = self.exercise("archive_migration")
        self.assertEqual(code, 17)
        self.assertFalse(any(command[0] == "cargo" for command, _ in calls))
        self.assertEqual(calls[-1][1]["input"].count("DROP DATABASE IF EXISTS"), 2)

    def test_failed_test_command_preserves_its_exit_code_and_cleans_up(self):
        code, calls = self.exercise()
        self.assertEqual(code, 23)
        cleanup = calls[-1][1]["input"]
        self.assertLess(cleanup.index("DROP DATABASE"), cleanup.index("DROP ROLE"))
