"""Acceptance rejects incomplete resource or physical-input evidence."""
import copy
from pathlib import Path
import tempfile
import threading
import unittest

from integration.history_expiry import assess, measurement_environment, replica_run


class HistoryExpiryQualificationTests(unittest.TestCase):
    def fixture(self):
        report = {"shape": "rows", "batches": [{"rows": 1000, "removed_rows": 1000,
                 "input_bytes": 1_000_000, "native_input_bytes": 1_000_000}],
                 "kernel_peak_rss_bytes": 100_000_000, "kernel_child_peak_rss_bytes": 0,
                 "replay_preserved_receipts": True, "quarantine_released": True,
                 "slots": 1, "replicas": 1,
                 "replica_reports": [{"replica": 0, "slots": 1, "initial_claimed_slots": 1, "http_workers": 2,
                     "claimed_iterations": 10, "slow_consumer_blocked": True,
                     "slow_consumer_retired": True, "kernel_peak_rss_bytes": 100_000_000,
                     "kernel_child_peak_rss_bytes": 0, "database_round_trip_us": [500] * 20}]}
        resources = {"samples": 10, "peak_rss_bytes": 100_000_000, "peak_hwm_bytes": 110_000_000}
        resources["replicas"] = [dict(resources)]
        return report, resources

    def test_complete_evidence_passes(self):
        self.assertTrue(all(assess(*self.fixture()).values()))

    def test_processes_share_only_the_fixture_hash_key_and_database_authority(self):
        source = {key: "fixture-" + key for key in (
            "HUBUUM_RESTORE_TEST_DATABASE_URL", "HUBUUM_RESTORE_TEST_MIGRATION_DATABASE_URL",
            "HUBUUM_ARCHIVE_TEST_DATABASE_URL", "HUBUUM_ARCHIVE_TEST_MIGRATION_DATABASE_URL")}
        source.update(GITHUB_TOKEN="host-token", HUBUUM_TOKEN_HASH_KEY="host-key",
                      HUBUUM_SECRET_SOURCE="host-provider")
        first = measurement_environment(source, "fixture-primary", "rows.json", "rows", "shared")
        second = measurement_environment(source, "fixture-primary", "rows.json", "rows", "shared")
        self.assertEqual(first["HUBUUM_TOKEN_HASH_KEY"], second["HUBUUM_TOKEN_HASH_KEY"])
        self.assertGreaterEqual(len(first["HUBUUM_TOKEN_HASH_KEY"]), 32)
        self.assertNotEqual(first["HUBUUM_TOKEN_HASH_KEY"], source["HUBUUM_TOKEN_HASH_KEY"])
        self.assertEqual(first["HUBUUM_REQUIRE_STABLE_TOKEN_HASH_KEY"], "true")
        self.assertNotIn("GITHUB_TOKEN", first)
        self.assertNotIn("HUBUUM_SECRET_SOURCE", first)
        self.assertEqual(first["HUBUUM_RESTORE_TEST_DATABASE_URL"], source["HUBUUM_RESTORE_TEST_DATABASE_URL"])

    def test_shared_topology_requires_both_measured_active_replicas(self):
        report, resources = self.fixture()
        report.update(replicas=2, slots=2)
        report["replica_reports"][0]["slots"] = 2
        report["replica_reports"][0]["initial_claimed_slots"] = 2
        report["replica_reports"].append({**report["replica_reports"][0], "replica": 1})
        resources["replicas"].append(dict(resources["replicas"][0]))
        self.assertTrue(all(assess(report, resources).values()))
        for field, value in [("claimed_iterations", 0), ("slow_consumer_blocked", False),
                             ("slow_consumer_retired", False),
                             ("database_round_trip_us", [2001] * 20),
                             ("kernel_peak_rss_bytes", 512 * 1024 ** 2 + 1)]:
            with self.subTest(field=field):
                original = report["replica_reports"][1][field]
                report["replica_reports"][1][field] = value
                self.assertFalse(assess(report, resources)["replica_topology_pass"])
                report["replica_reports"][1][field] = original
        resources["replicas"].pop()
        self.assertFalse(assess(report, resources)["replica_topology_pass"])

    def test_missing_replica_evidence_cannot_qualify_a_worker(self):
        report, resources = self.fixture()
        del report["replica_reports"]
        self.assertFalse(assess(report, resources)["replica_topology_pass"])

    def test_failed_replica_stops_its_coordinator_and_peer(self):
        with tempfile.TemporaryDirectory() as temporary:
            destination = Path(temporary) / "rows-0.json"
            root = destination.with_suffix(".runtime")
            root.mkdir()
            stopped = {name: threading.Event() for name in
                       ("rows-0", "rows-0-replica-0", "rows-0-replica-1")}

            class Fixture:
                name = "owned-fixture"

                def __init__(self):
                    self.containers = []
                    self.removed = []

                def binary(self, binary, arguments, name, **kwargs):
                    if name.endswith("replica-1"):
                        stopped["rows-0"].wait(5)
                    self.containers.append(self.name + "-" + name)
                    if name.endswith("replica-0"):
                        raise RuntimeError("replica failed before readiness")
                    marker = "ready-1" if name.endswith("replica-1") else "prepared"
                    (root / marker).write_bytes(b"")
                    if not stopped[name].wait(5):
                        raise RuntimeError("Peer was not stopped after failure")
                    return "stopped"

                def docker(self, *arguments, **kwargs):
                    self.removed.append(arguments[-1])
                    stopped[arguments[-1].removeprefix(self.name + "-")].set()

            fixture = Fixture()
            with self.assertRaisesRegex(RuntimeError, "replica failed before readiness"):
                replica_run(fixture, Path("driver"), [], "rows-0",
                            {"HUBUUM_TEST_HISTORY_QUALIFICATION_REPLICAS": "2"}, destination)
            self.assertTrue(stopped["rows-0"].is_set())
            self.assertEqual(fixture.containers, [])
            self.assertIn("owned-fixture-rows-0", fixture.removed)

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
