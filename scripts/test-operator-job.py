#!/usr/bin/env python3
"""Verify operator-job evidence preserves failure and last-success semantics."""
import sys

if sys.version_info < (3, 11):
    sys.exit(
        "Hubuum tooling requires Python 3.11 or newer; found "
        + sys.version.split()[0]
        + ". Install Python 3.11+ and ensure python3 on PATH selects it."
    )

import importlib.util
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("record-operator-job.py")
spec = importlib.util.spec_from_file_location("operator_job", SCRIPT)
job = importlib.util.module_from_spec(spec)
spec.loader.exec_module(job)


class OperatorJobTests(unittest.TestCase):
    def test_failure_preserves_last_success(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory, "job.prom")
            labels = 'deployment="test",operation="restore_verify"'
            job.write_metrics(path, labels, 3600, (0, 1, 1000))
            job.write_metrics(path, labels, 3600, (1, 2, 2000))
            contents = path.read_text()
            self.assertIn(f'hubuum_operator_job_success{{{labels}}} 0', contents)
            self.assertIn(f'hubuum_operator_job_last_success_timestamp_seconds{{{labels}}} 1000', contents)
            self.assertEqual(list(Path(directory).iterdir()), [path])

    def test_never_run_job_does_not_claim_success(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory, "job.prom")
            job.write_metrics(path, 'deployment="test",operation="restore_verify"', 3600)
            self.assertIn('hubuum_operator_job_expected', path.read_text())
            self.assertNotIn('last_success', path.read_text())

    def test_command_failure_is_not_hidden(self):
        with tempfile.TemporaryDirectory() as directory:
            result = subprocess.run([sys.executable, str(SCRIPT), "--directory", directory,
                                     "--deployment", "test", "--operation", "retention", "--",
                                     sys.executable, "-c", "raise SystemExit(7)"], check=False)
            self.assertEqual(result.returncode, 7)
            self.assertIn('hubuum_operator_job_success{deployment="test",operation="retention"} 0',
                          next(Path(directory).glob("*.prom")).read_text())


if __name__ == "__main__":
    unittest.main()
