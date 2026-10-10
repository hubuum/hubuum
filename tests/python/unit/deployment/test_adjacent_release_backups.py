"""Exercise the adjacent-release restore transition guard without containers."""

import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from support import SCRIPTS


class BackupTransitionTests(unittest.TestCase):
    def restore(self, source_version, candidate_version):
        script = (SCRIPTS / "test-adjacent-release-upgrade.sh").read_text()
        function = "verify_restore_artifact() {" + script.split(
            "verify_restore_artifact() {", 1
        )[1].split("\n}\n", 1)[0] + "\n}\n"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "source.json").write_text(json.dumps({"backup_version": source_version}))
            (root / "current.json").write_text(json.dumps({"backup_version": candidate_version}))
            result = subprocess.run([
                "bash", "-c", """
set -euo pipefail
test_root="$1"
current_backup_file="$test_root/current.json"
compose_fixture() {
  printf '%s\n' "$*" >> "$test_root/commands"
  printf '%s\n' '{"result":"passed","mode":"isolated_restore","restore_test":{"storage_ready":true,"target_cleanup":"schema_reset"}}'
}
compose=(compose_fixture)
""" + function + """
verify_restore_artifact "$test_root/source.json" postgres://fixture/disposable "$test_root/report.json" reset
""", "fixture", directory,
            ], capture_output=True, text=True, timeout=10)
            report = json.loads((root / "report.json").read_text()) if result.returncode == 0 else None
            commands = (root / "commands").read_text() if (root / "commands").exists() else ""
            return result, report, commands

    def test_supported_predecessors_use_the_candidate_directly(self):
        for source, candidate in ((6, 7), (6, 8), (7, 8), (6, 9), (7, 9), (8, 9), (9, 9)):
            with self.subTest(source=source, candidate=candidate):
                result, report, commands = self.restore(source, candidate)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("candidate-api --verify-backup /verification/source.json", commands)
                self.assertEqual(report["recovery"], {
                    "path": "direct", "source_backup_version": source,
                    "candidate_backup_version": candidate, "candidate_storage_ready": True,
                    "target_cleanup": "schema_reset",
                })

    def test_unreviewed_transitions_do_not_attempt_restore(self):
        for source, candidate in ((5, 9), (9, 8), (8, 10)):
            with self.subTest(source=source, candidate=candidate):
                result, _, commands = self.restore(source, candidate)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(f"untested backup format transition {source} -> {candidate}", result.stderr)
                self.assertEqual(commands, "")
