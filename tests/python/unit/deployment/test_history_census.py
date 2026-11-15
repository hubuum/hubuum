"""Acceptance rejects incomplete, over-budget and misclassified census evidence."""
from contextlib import nullcontext
import unittest
from unittest.mock import Mock, call

from integration.history_census import CEILING, assess, fresh_database


class CensusAcceptanceTests(unittest.TestCase):
    def test_each_probe_drops_its_database_even_after_failure(self):
        for failed in (False, True):
            with self.subTest(failed=failed):
                fixture = Mock()
                fixture.url.return_value = "fixture-url"
                with self.assertRaises(RuntimeError) if failed else nullcontext():
                    with fresh_database(fixture, "census_supported_0") as url:
                        self.assertEqual(url, "fixture-url")
                        if failed:
                            raise RuntimeError("probe failed")
                fixture.sql.assert_has_calls([
                    call("postgres", "CREATE DATABASE census_supported_0 TEMPLATE template_fixture"),
                    call("postgres", "DROP DATABASE census_supported_0 WITH (FORCE)"),
                ])

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

    def test_row_overflow_requires_independent_age_and_retained_scope_evidence(self):
        report = {"shape": "rows_over", "outcome": {"status": "unavailable"},
                  "elapsed_us": 100, "control_unchanged": True, "gate_released": True,
                  "kernel_peak_rss_bytes": 100, "source_rows": 100_001,
                  "metadata_bytes": 1000,
                  "fallback": {"size_only_members": 0, "age_members": 1,
                               "age_size_witnesses": 0, "pressure_unchanged": True,
                               "size_only_us": 100, "age_or_size_us": 100,
                               "overlapping_size_sources": 2, "held_copies": 2, "held_closed_copies": 1,
                               "pending_size_members": 1, "governing_partition_verified": True,
                               "pending_preserved": True, "held_and_pending_unselected": True,
                               "holds_preserved": True}}
        resources = {"rss_trace_verified": True, "samples": 2,
                     "peak_rss_bytes": 100, "peak_hwm_bytes": 100}
        self.assertTrue(all(assess(report, resources).values()))
        for key, value, check in (
                ("size_only_members", 1, "age_fallback_pass"),
                ("age_members", 0, "age_fallback_pass"),
                ("age_size_witnesses", 1, "age_fallback_pass"),
                ("pressure_unchanged", False, "age_fallback_pass"),
                ("age_or_size_us", 10_000_000, "age_fallback_pass"),
                ("overlapping_size_sources", 1, "retained_scope_pass"),
                ("held_copies", 0, "retained_scope_pass"),
                ("held_closed_copies", 0, "retained_scope_pass"),
                ("pending_size_members", 0, "retained_scope_pass"),
                ("governing_partition_verified", False, "retained_scope_pass"),
                ("pending_preserved", False, "retained_scope_pass"),
                ("held_and_pending_unselected", False, "retained_scope_pass"),
                ("holds_preserved", False, "retained_scope_pass")):
            with self.subTest(key=key):
                changed = {**report, "fallback": {**report["fallback"], key: value}}
                self.assertFalse(assess(changed, resources)[check])
