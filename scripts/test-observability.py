#!/usr/bin/env python3
"""Validate operator assets against the committed metric contract and Prometheus."""
import sys

if sys.version_info < (3, 11):
    sys.exit(
        "Hubuum tooling requires Python 3.11 or newer; found "
        + sys.version.split()[0]
        + ". Install Python 3.11+ and ensure python3 on PATH selects it."
    )

import importlib.util
import json
from pathlib import Path
import shutil
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("operator_check", ROOT / "scripts/check-observability.py")
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)


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


if __name__ == "__main__":
    unittest.main()
