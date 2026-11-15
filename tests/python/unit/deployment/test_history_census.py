"""Acceptance rejects incomplete, over-budget and misclassified census evidence."""
import unittest

from integration.history_census import CEILING, assess


class CensusAcceptanceTests(unittest.TestCase):
    def test_exhaustion_cannot_publish_complete_accounting(self):
        resources = {"rss_trace_verified": True, "samples": 2,
                     "peak_rss_bytes": 100, "peak_hwm_bytes": 100}
        for shape in ("rows_over", "identity_over", "metadata_over"):
            with self.subTest(shape=shape):
                report = {"shape": shape, "outcome": {"status": "complete"},
                          "elapsed_us": 100, "control_unchanged": True, "gate_released": True,
                          "kernel_peak_rss_bytes": 100,
                          "fallback": {"size_only_members": 0, "age_members": 1,
                                       "age_size_witnesses": 0, "pressure_unchanged": True,
                                       "size_only_us": 100, "age_or_size_us": 100},
                          "source_rows": 100_001, "metadata_bytes": 65 * 1024 ** 2,
                          "largest_identity_bytes": 1024 ** 2 + 1}
                self.assertFalse(assess(report, resources)["complete_or_unavailable_pass"])

    def test_supported_probe_requires_complete_bounded_atomic_evidence(self):
        report = {"shape": "supported", "outcome": {"status": "complete"},
                  "elapsed_us": 100, "control_unchanged": True, "gate_released": True,
                  "kernel_peak_rss_bytes": 100,
                  "source_rows": 1000, "metadata_bytes": 1000}
        resources = {"rss_trace_verified": True, "samples": 2,
                     "peak_rss_bytes": 100, "peak_hwm_bytes": 100}
        self.assertTrue(all(assess(report, resources).values()))
        for key, value in (("elapsed_us", 10_000_000), ("control_unchanged", False), ("gate_released", False),
                           ("outcome", {"status": "unavailable"})):
            with self.subTest(key=key):
                self.assertFalse(all(assess({**report, key: value}, resources).values()))
        self.assertFalse(all(assess(report, {**resources, "peak_hwm_bytes": CEILING + 1}).values()))
        self.assertFalse(all(assess(report, {**resources, "rss_trace_verified": False}).values()))

    def test_row_overflow_requires_productive_independent_age_fallback(self):
        report = {"shape": "rows_over", "outcome": {"status": "unavailable"},
                  "elapsed_us": 100, "control_unchanged": True, "gate_released": True,
                  "kernel_peak_rss_bytes": 100, "source_rows": 100_001,
                  "metadata_bytes": 1000,
                  "fallback": {"size_only_members": 0, "age_members": 1,
                               "age_size_witnesses": 0, "pressure_unchanged": True,
                               "size_only_us": 100, "age_or_size_us": 100}}
        resources = {"rss_trace_verified": True, "samples": 2,
                     "peak_rss_bytes": 100, "peak_hwm_bytes": 100}
        self.assertTrue(all(assess(report, resources).values()))
        for key, value in (("size_only_members", 1), ("age_members", 0),
                           ("age_size_witnesses", 1), ("pressure_unchanged", False),
                           ("age_or_size_us", 10_000_000)):
            with self.subTest(key=key):
                changed = {**report, "fallback": {**report["fallback"], key: value}}
                self.assertFalse(assess(changed, resources)["age_fallback_pass"])
