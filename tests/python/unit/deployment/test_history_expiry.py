"""Acceptance rejects incomplete resource or physical-input evidence."""
import copy
import json
from pathlib import Path
import tempfile
import tarfile
import threading
import unittest
from unittest.mock import MagicMock, Mock, patch

from integration.history_expiry import (SHAPES, aggregate_measurements, assess, container_failure_log,
                                       measured_resources, measurement_environment, package_measurements,
                                       replica_run, run, sample_resources, summarize, verify_binary_reuse)
from support import ROOT


class HistoryExpiryQualificationTests(unittest.TestCase):
    def fixture(self):
        report = {"shape": "rows", "batches": [{"rows": 1000, "removed_rows": 1000,
                 "input_bytes": 1_000_000, "native_input_bytes": 1_000_000}],
                 "kernel_peak_rss_bytes": 100_000_000, "kernel_child_peak_rss_bytes": 0,
                 "replay_preserved_receipts": True, "quarantine_released": True,
                 "slots": 1, "replicas": 1,
                 "max_scanned_source_rows": 1000,
                 "replica_reports": [{"replica": 0, "slots": 1, "initial_claimed_slots": 1, "http_workers": 2,
                     "claimed_iterations": 10, "slow_consumer_blocked": True,
                     "max_scanned_source_rows": 1000,
                     "slow_consumer_retired": True, "kernel_peak_rss_bytes": 100_000_000,
                     "kernel_child_peak_rss_bytes": 0, "database_round_trip_us": [500] * 20}]}
        resources = {"samples": 10, "peak_rss_bytes": 100_000_000, "peak_hwm_bytes": 110_000_000,
                     "rss_trace_verified": True, "first_sample_ns": 1, "last_sample_ns": 100}
        resources["replicas"] = [dict(resources)]
        return report, resources

    def test_complete_evidence_passes(self):
        self.assertTrue(all(assess(*self.fixture()).values()))

    def test_row_boundary_counts_scanned_sources_before_eligibility_filtering(self):
        report, resources = self.fixture()
        report["batches"][0].update(rows=998, removed_rows=998)
        self.assertTrue(all(assess(report, resources).values()))
        for scanned in (999, 1001, None):
            with self.subTest(scanned=scanned):
                report["max_scanned_source_rows"] = scanned
                report["replica_reports"][0]["max_scanned_source_rows"] = scanned
                self.assertFalse(assess(report, resources)["h4_pass"])

    def test_failed_warmup_stops_measurement_without_qualifying_partial_evidence(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binary = root / "binary"
            binary.write_bytes(b"fixture")
            args = Mock(output=root / "report", binary=binary, publisher_binary=binary,
                        admin_binary=binary, profile="shared", shape=None)
            fixture = MagicMock()
            fixture.__enter__.return_value = fixture
            with patch("integration.history_expiry.Fixture", return_value=fixture), \
                    patch("integration.history_expiry.run_tests", side_effect=RuntimeError("worker failed")) as tests, \
                    patch("integration.history_expiry.container_failure_log") as diagnostic:
                self.assertEqual(run(args), 1)
            self.assertEqual(tests.call_count, 1)
            diagnostic.assert_called_once()
            fixture.__exit__.assert_called_once()
            summary = json.loads((args.output / "summary.json").read_text())
            self.assertEqual(summary["expected_measured_runs"], 25)
            self.assertEqual(summary["measured_runs"], 0)
            self.assertFalse(summary["h1_h4_worker_qualified"])
            self.assertFalse(summary["full_shared_topology_qualified"])

    def measurement_shards(self, parent):
        roots = []
        for shape in SHAPES:
            root = parent / shape / "history-expiry-report"
            root.mkdir(parents=True)
            roots.append(root)
            (root.parent / "history-expiry-build.txt").write_text("same production build")
            manifest = {"profile": "lifecycle-local-v1", "worker_binary_sha256": "same digest", "shapes": [shape]}
            samples = []
            for repeat in range(6):
                report, _ = self.fixture()
                report.update(shape=shape, worker_seconds=1, publisher_fixture_seconds=1)
                if shape == "bytes":
                    report["batches"][0].update(input_bytes=15 * 1024**2, native_input_bytes=15 * 1024**2)
                elif shape == "large_row":
                    report["batches"][0]["largest_claimed_row_bytes"] = 4 * 1024**2 - 4096
                elif shape == "oversized":
                    report.update(oversized_deferred_rows=1, oversized_preserved_rows=1,
                                  oversized_reported_rows=1, backup_rejected_oversized=True)
                elif shape == "authority":
                    report.pop("replica_reports")
                    report["maximum_composite_verified"] = True
                name = f"{shape}-{repeat}"
                process = name if shape == "authority" else name + "-replica-0"
                (root / (process + "-container.json")).write_text(json.dumps({"resources": {
                    "samples": 2, "peak_rss_bytes": 100_000_000, "peak_hwm_bytes": 110_000_000}}))
                (root / (process + "-rss.jsonl")).write_text("".join(json.dumps({
                    "monotonic_ns": stamp, "rss_bytes": 100_000_000, "hwm_bytes": 110_000_000}) + "\n"
                    for stamp in (1, 100)))
                resources = sample_resources(root, name, report)
                samples.append({"shape": shape, "repeat": repeat, "warmup": repeat == 0,
                                "report": report, "resources": resources, "checks": assess(report, resources)})
                (root / (name + ".json")).write_text(json.dumps(report))
            for name, data in (("manifest", manifest), ("samples", samples),
                               ("summary", summarize(manifest, samples, []))):
                (root / (name + ".json")).write_text(json.dumps(data))
        return roots

    def test_shape_shards_only_qualify_after_complete_raw_evidence_aggregation(self):
        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary)
            roots = self.measurement_shards(parent)
            self.assertTrue(all(json.loads((root / "summary.json").read_text())["selected_shapes_passed"]
                                for root in roots))
            self.assertFalse(any(json.loads((root / "summary.json").read_text())["h1_h4_worker_qualified"]
                                 for root in roots))
            summary = aggregate_measurements(roots, parent / "combined")
            self.assertTrue(summary["h1_h4_worker_qualified"])
            self.assertEqual(summary["measured_runs"], 25)
            self.assertEqual(len(json.loads((parent / "combined/samples.json").read_text())), 30)

    def test_aggregation_rejects_incomplete_duplicate_or_mixed_provenance_shards(self):
        for invalid in ("missing", "duplicate", "manifest", "build", "sample", "raw", "failed"):
            with self.subTest(invalid=invalid), tempfile.TemporaryDirectory() as temporary:
                parent = Path(temporary)
                roots = self.measurement_shards(parent)
                if invalid == "missing":
                    roots.pop()
                elif invalid == "duplicate":
                    roots[-1] = roots[0]
                elif invalid == "build":
                    (roots[0].parent / "history-expiry-build.txt").write_text("different build")
                elif invalid == "raw":
                    (roots[0] / "rows-0-replica-0-rss.jsonl").write_text("{}\n")
                else:
                    name = "manifest" if invalid == "manifest" else "samples"
                    path = roots[0] / (name + ".json")
                    data = json.loads(path.read_text())
                    if invalid == "manifest":
                        data["worker_binary_sha256"] = "different digest"
                    elif invalid == "sample":
                        data.pop()
                    else:
                        data[0]["checks"]["h1_pass"] = False
                    path.write_text(json.dumps(data))
                with self.assertRaises((ValueError, KeyError)):
                    aggregate_measurements(roots, parent / "combined")

    def test_container_failure_log_retains_both_streams_and_redacts_fixture_password(self):
        fixture = Mock(containers=["owned-postgres"], password="fixture-secret")
        with tempfile.TemporaryDirectory() as temporary:
            destination = Path(temporary) / "postgres.log"
            with patch("integration.history_expiry.subprocess.run",
                       return_value=Mock(returncode=0, stdout="startup\n", stderr="deadlock fixture-secret\n")) as run:
                container_failure_log(fixture, destination, "owned-postgres")
            self.assertEqual(destination.read_text(), "startup\ndeadlock [fixture password]\n")
            self.assertEqual(run.call_args.args[0], ["docker", "logs", "--tail", "200", "owned-postgres"])

    def test_reuse_requires_identical_rust_inputs_and_build_recipe(self):
        workflow = (ROOT / ".github/workflows/history-expiry-qualification.yml").read_text()
        verify_binary_reuse(["tests/python/integration/history_expiry.py"], workflow, workflow)
        for changed in ("src/application.rs", "Cargo.lock", "Dockerfile", "crates/new/input.json"):
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                verify_binary_reuse([changed], workflow, workflow)
        with self.assertRaises(ValueError):
            verify_binary_reuse([], workflow, workflow.replace("--release", "--profile dev", 1))

    def test_packaging_retains_failure_evidence_without_private_runtime_material(self):
        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary)
            output = parent / "report"
            output.mkdir()
            (output / "rows-failure.txt").write_text("native failure")
            private = output / "rows.runtime"
            private.mkdir()
            (private / "key").write_text("private fixture key")
            (parent / "history-expiry-build.txt").write_text("original build")
            private.chmod(0)
            archive = parent / "evidence.tar.gz"
            try:
                package_measurements(output, archive)
            finally:
                private.chmod(0o700)
            with tarfile.open(archive) as packed:
                self.assertEqual(set(packed.getnames()),
                                 {"history-expiry-report/rows-failure.txt", "history-expiry-build.txt"})
                self.assertEqual(packed.extractfile("history-expiry-report/rows-failure.txt").read(), b"native failure")
            self.assertEqual(archive.stat().st_mode & 0o777, 0o644)

    def test_raw_memory_trace_must_match_count_peaks_and_monotonic_time(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            summary = {"samples": 2, "peak_rss_bytes": 20, "peak_hwm_bytes": 30}
            (output / "worker-container.json").write_text(json.dumps({"resources": summary}))
            samples = [{"monotonic_ns": 1, "rss_bytes": 10, "hwm_bytes": 30},
                       {"monotonic_ns": 2, "rss_bytes": 20, "hwm_bytes": 30}]
            trace = output / "worker-rss.jsonl"
            trace.write_text("".join(json.dumps(row) + "\n" for row in samples))
            self.assertTrue(measured_resources(output, "worker")["rss_trace_verified"])
            for field, value in [("monotonic_ns", 1), ("rss_bytes", 19), ("hwm_bytes", 1)]:
                with self.subTest(field=field), self.assertRaises(ValueError):
                    invalid = copy.deepcopy(samples)
                    invalid[1][field] = value
                    trace.write_text("".join(json.dumps(row) + "\n" for row in invalid))
                    measured_resources(output, "worker")

    def test_processes_share_only_the_fixture_hash_key_and_database_authority(self):
        source = {key: "fixture-" + key for key in (
            "HUBUUM_RESTORE_TEST_DATABASE_URL", "HUBUUM_RESTORE_TEST_MIGRATION_DATABASE_URL",
            "HUBUUM_ARCHIVE_TEST_DATABASE_URL", "HUBUUM_ARCHIVE_TEST_MIGRATION_DATABASE_URL")}
        source.update(GITHUB_TOKEN="host-token", GH_TOKEN="host-gh-token", HUBUUM_TOKEN_HASH_KEY="host-key",
                      HUBUUM_SECRET_SOURCE="host-provider")
        first = measurement_environment(source, "fixture-primary", "rows.json", "rows", "shared")
        second = measurement_environment(source, "fixture-primary", "rows.json", "rows", "shared")
        self.assertEqual(first["HUBUUM_TOKEN_HASH_KEY"], second["HUBUUM_TOKEN_HASH_KEY"])
        self.assertGreaterEqual(len(first["HUBUUM_TOKEN_HASH_KEY"]), 32)
        self.assertNotEqual(first["HUBUUM_TOKEN_HASH_KEY"], source["HUBUUM_TOKEN_HASH_KEY"])
        self.assertEqual(first["HUBUUM_REQUIRE_STABLE_TOKEN_HASH_KEY"], "true")
        self.assertNotIn("GITHUB_TOKEN", first)
        self.assertNotIn("GH_TOKEN", first)
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
        for field, value in [("rss_trace_verified", False), ("first_sample_ns", 101)]:
            with self.subTest(resource_field=field):
                original = resources["replicas"][1][field]
                resources["replicas"][1][field] = value
                self.assertFalse(assess(report, resources)["replica_topology_pass"])
                resources["replicas"][1][field] = original
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
            with patch("integration.history_expiry.container_failure_log") as diagnostic:
                with self.assertRaisesRegex(RuntimeError, "replica failed before readiness"):
                    replica_run(fixture, Path("driver"), [], "rows-0",
                                {"HUBUUM_TEST_HISTORY_QUALIFICATION_REPLICAS": "2"}, destination)
            self.assertIn("owned-fixture-rows-0-replica-0",
                          [call.args[2] for call in diagnostic.call_args_list])
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
