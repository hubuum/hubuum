"""Operator-package validation and external job regression tests."""
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

from . import ROOT, validate as checker, jobs as job


class ContractDriftTests(unittest.TestCase):
    def test_committed_package(self):
        checker.validate(ROOT)

    def test_metric_label_and_enum_drift_are_rejected(self):
        for old, new in (("hubuum_db_pool_connections", "hubuum_unknown"),
                         ('state=', 'unknown='), ('checked_out', 'invalid_state')):
            with self.subTest(replacement=new), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                shutil.copytree(ROOT / "observability", root / "observability")
                (root / "docs").mkdir()
                shutil.copy(ROOT / "docs/operational-contract.json", root / "docs/operational-contract.json")
                rules = root / "observability/prometheus/alerts.json"
                rules.write_text(rules.read_text().replace(old, new))
                with self.assertRaises(ValueError):
                    checker.validate(root)

    def test_all_dashboard_metrics_are_checked(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / "observability", root / "observability")
            (root / "docs").mkdir()
            shutil.copy(ROOT / "docs/operational-contract.json", root / "docs/operational-contract.json")
            path = root / "observability/dashboards/recovery.json"
            dashboard = json.loads(path.read_text())
            dashboard["panels"][0]["targets"][0]["expr"] = "removed_metric"
            path.write_text(json.dumps(dashboard))
            with self.assertRaisesRegex(ValueError, "Unknown metric: removed_metric"):
                checker.validate(root)

    def test_vector_names_exclude_functions_and_labels(self):
        expression = 'sum by (deployment) (rate(requests_total{outcome="error"}[5m])) or (errors_total / total)'
        self.assertEqual(list(checker.metric_names(expression)), ["requests_total", "errors_total", "total"])


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
            result = subprocess.run([sys.executable, str(ROOT / "scripts/observability.py"), "record-job", "--directory", directory,
                                     "--deployment", "test", "--operation", "retention", "--",
                                     sys.executable, "-c", "raise SystemExit(7)"], check=False)
            self.assertEqual(result.returncode, 7)
            self.assertIn('hubuum_operator_job_success{deployment="test",operation="retention"} 0',
                          next(Path(directory).glob("*.prom")).read_text())
