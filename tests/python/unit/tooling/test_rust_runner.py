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
    def run_fixture(self, failure=""):
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
            "HUBUUM_RESTORE_TEST_DATABASE_URL", "HUBUUM_RESTORE_TEST_MIGRATION_DATABASE_URL"):
    call[key] = os.environ.get(key)
with open(os.environ["RUNNER_CALLS"], "a") as output:
    output.write(json.dumps(call) + "\\n")
if name == "cargo":
    restore_migration = "--migrate" in args and (call["HUBUUM_DATABASE_URL"] or "").endswith("_restore")
    application_tests = args == ["test", "--features", "integration-test-support,embedded-migrations"]
    if (os.environ["RUNNER_FAILURE"] == "migration" and restore_migration
            or os.environ["RUNNER_FAILURE"] == "tests" and application_tests):
        sys.exit(17)
''')
            stub.chmod(0o755)
            for name in ("cargo", "diesel", "psql", "python3"):
                (binaries / name).symlink_to(stub)
            log = root / "calls.jsonl"
            environment = {
                key: value for key, value in os.environ.items()
                if not key.startswith("HUBUUM_")
            }
            environment.update({
                "PATH": f"{binaries}:{os.environ['PATH']}",
                "HUBUUM_TEST_DB_PASSWORD": "fixture-password",
                "RUNNER_CALLS": str(log),
                "RUNNER_FAILURE": failure,
            })
            result = subprocess.run(
                ["bash", "run_tests.sh"], cwd=root, env=environment,
                capture_output=True, text=True, timeout=30,
            )
            return result, [json.loads(line) for line in log.read_text().splitlines()]

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
