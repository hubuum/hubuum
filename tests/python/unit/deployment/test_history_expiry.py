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

    def test_largest_supported_row_requires_native_size_evidence(self):
        report, resources = self.fixture()
        report["shape"] = "large_row"
        report["batches"][0]["largest_claimed_row_bytes"] = 4 * 1024 ** 2 - 4096
        self.assertTrue(all(assess(report, resources).values()))
        report["batches"][0]["largest_claimed_row_bytes"] = 1024
        self.assertFalse(assess(report, resources)["h4_pass"])

    def test_unsupported_backup_row_requires_preservation_and_explicit_task_outcome(self):
        report, resources = self.fixture()
        report.update(shape="oversized", slots=1, oversized_deferred_rows=1,
                      oversized_preserved_rows=1, oversized_reported_rows=1,
                      backup_rejected_oversized=True, replay_preserved_receipts=None,
                      quarantine_released=None)
        self.assertTrue(all(assess(report, resources).values()))
        report["oversized_reported_rows"] = 0
        self.assertFalse(assess(report, resources)["oversized_outcome_pass"])

    def test_maximum_composite_requires_its_real_publisher_assertions(self):
        report, resources = self.fixture()
        report.update(shape="authority", maximum_composite_verified=True)
        self.assertTrue(all(assess(report, resources).values()))
        report["maximum_composite_verified"] = False
        self.assertFalse(all(assess(report, resources).values()))

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
