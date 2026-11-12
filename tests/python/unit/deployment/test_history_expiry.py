"""Acceptance rejects incomplete resource or physical-input evidence."""
import copy
import unittest

from integration.history_expiry import assess


class HistoryExpiryQualificationTests(unittest.TestCase):
    def fixture(self):
        return ({"shape": "rows", "batches": [{"rows": 1000, "removed_rows": 1000,
                 "input_bytes": 1_000_000, "native_input_bytes": 1_000_000}],
                 "kernel_peak_rss_bytes": 100_000_000, "kernel_child_peak_rss_bytes": 0,
                 "replay_preserved_receipts": True, "quarantine_released": True},
                {"samples": 10, "peak_rss_bytes": 100_000_000, "peak_hwm_bytes": 110_000_000})

    def test_complete_evidence_passes(self):
        self.assertTrue(all(assess(*self.fixture()).values()))

    def test_multiple_periodic_batches_do_not_depend_on_uuid_order(self):
        report, resources = self.fixture()
        report["batches"].insert(0, {"rows": 1, "removed_rows": 1,
                                     "input_bytes": 100, "native_input_bytes": 100})
        self.assertTrue(assess(report, resources)["h4_pass"])

    def test_missing_or_exceeded_physical_evidence_fails(self):
        for field, value in [("native_input_bytes", 999_999), ("rows", 1001),
                             ("input_bytes", 16 * 1024 ** 2 + 1), ("removed_rows", 999)]:
            with self.subTest(field=field):
                report, resources = self.fixture()
                report["batches"][0][field] = value
                self.assertFalse(assess(report, resources)["h4_pass"])

    def test_missing_or_over_limit_rss_and_children_fail(self):
        report, resources = self.fixture()
        for target, field, value in [(resources, "samples", 0),
                                     (resources, "peak_hwm_bytes", 512 * 1024 ** 2 + 1),
                                     (report, "kernel_peak_rss_bytes", 0),
                                     (report, "kernel_child_peak_rss_bytes", 1)]:
            with self.subTest(field=field):
                before = copy.deepcopy(target)
                target[field] = value
                self.assertFalse(assess(report, resources)["h1_pass"])
                target.clear()
                target.update(before)
