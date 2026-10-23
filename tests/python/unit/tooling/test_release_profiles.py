"""Regression coverage for comparable release benchmark reports."""

import json
from pathlib import Path
import tempfile
import unittest

from support import load_script

profiles = load_script("compare-release-profiles.py")


class ReleaseProfileTests(unittest.TestCase):
    def test_missing_workloads_cannot_produce_a_comparison(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "expected six"):
                profiles.estimates(Path(directory))

    def test_criterion_throughput_uses_nanoseconds(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ("release_serialization/encode", "release_serialization/decode",
                         "release_schema/validate", "release_search/unified_parse",
                         "release_search/filter_parse", "release_template/render"):
                target = root / (name.replace("/", "_") if name.startswith("release_search/") else name) / "new"
                target.mkdir(parents=True)
                (target / "estimates.json").write_text(json.dumps({
                    "mean": {"point_estimate": 2000, "confidence_interval": {
                        "lower_bound": 1900, "upper_bound": 2100, "confidence_level": 0.95}},
                    "median": {"point_estimate": 1950}}))
                (target / "benchmark.json").write_text(json.dumps({
                    "full_id": name, "throughput": {"Bytes": 100}}))
            result = profiles.estimates(root)
            self.assertEqual(result["release_serialization/encode"]["throughput"],
                             {"unit": "Bytes", "per_second": 50_000_000})

    def test_different_corpora_are_not_compared(self):
        with self.assertRaisesRegex(ValueError, "workload sets differ"):
            profiles.comparison({"z": {"workloads": {"a": {"mean_ns": 1}}},
                                 "3": {"workloads": {"b": {"mean_ns": 1}}}})

    def test_latency_sign_reports_faster_as_negative(self):
        result = profiles.comparison({"z": {"workloads": {"a": {"mean_ns": 200}}},
                                      "3": {"workloads": {"a": {"mean_ns": 100}}}})
        self.assertEqual(result["a"]["latency_change_percent"], -50)
