"""Resource evidence remains meaningful when a real fixture run fails."""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
import json
import subprocess
import tempfile
import unittest

from integration import task_retention as RETENTION


class TaskRetentionMeasurementTests(unittest.TestCase):
    def test_failed_captured_administrator_retains_private_redacted_diagnostics(self):
        with tempfile.TemporaryDirectory() as temporary:
            deployment = SimpleNamespace(root=Path(temporary), password="fixture-password",
                                         token_key="fixture-token-key", host_command=lambda command: command,
                                         host_environment=lambda: {})
            failure = subprocess.CalledProcessError(1, ["hubuum-admin"],
                                                   output="stdout fixture-password\n", stderr="stderr fixture-token-key\n")
            with patch.object(RETENTION.subprocess, "run", side_effect=failure), \
                    self.assertRaisesRegex(RuntimeError, "inspect retained artifacts"):
                RETENTION.run_native(deployment, ["hubuum-admin"], timeout=1, capture_output=True)
            files = list(Path(temporary).glob("failed-hubuum-admin-*.log"))
            self.assertEqual(len(files), 1)
            self.assertEqual(files[0].read_text(encoding="utf-8"), "stdout [redacted]\nstderr [redacted]\n")
            self.assertEqual(files[0].stat().st_mode & 0o777, 0o600)

    def test_native_administration_cannot_inherit_another_migration_database(self):
        deployment = RETENTION.RetentionDeployment.__new__(RETENTION.RetentionDeployment)
        deployment.host_url = "postgres://owned-fixture/source"
        deployment.token_key = "fixture-token-key"
        deployment.repository = Path("/tmp/owned-fixture/repository")
        deployment.key = Path("/tmp/owned-fixture/key")
        deployment.affinity = False
        with patch.dict(RETENTION.os.environ, {
                "HUBUUM_DATABASE_URL": "postgres://unrelated/runtime",
                "HUBUUM_MIGRATION_DATABASE_URL": "postgres://unrelated/privileged",
        }):
            environment = deployment.host_environment()
        self.assertEqual(environment["HUBUUM_DATABASE_URL"], deployment.host_url)
        self.assertEqual(environment["HUBUUM_MIGRATION_DATABASE_URL"], deployment.host_url)

    def test_affinity_plan_has_disjoint_process_budgets_and_reuses_local_role_slot(self):
        with patch.object(RETENTION.os, "sched_getaffinity", return_value=set(range(16))):
            plan = RETENTION.affinity_plan("shared")
        names = ("database", "api-0", "api-1", "worker-0", "worker-1", "administrator")
        masks = [RETENTION.cpu_set(plan[name]) for name in names]
        self.assertEqual([len(mask) for mask in masks], [4, 2, 2, 2, 2, 2])
        self.assertEqual(sum(map(len, masks)), len(set.union(*masks)))
        self.assertEqual(plan["all-2"], plan["api-0"])

    def test_affinity_plan_refuses_insufficient_cpus(self):
        with patch.object(RETENTION.os, "sched_getaffinity", return_value={0, 1}), \
                self.assertRaisesRegex(ValueError, "Not enough"):
            RETENTION.affinity_plan("local")

    def test_memory_parser_keeps_rss_and_kernel_high_water_separate(self):
        status = "Name:\tworker\nVmRSS:\t64 kB\nThreads:\t32\nVmHWM:\t192 kB\n"
        self.assertEqual(RETENTION.parse_proc_memory(status), {
            "VmRSS": 65536, "VmHWM": 196608,
        })

    def test_memory_parser_rejects_uninterpretable_measurements(self):
        for value in ("VmRSS: 12 MB", "VmRSS: -1 kB", "VmHWM: unknown kB"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                RETENTION.parse_proc_memory(value)

    def test_exited_container_cannot_be_registered_as_pid_zero(self):
        deployment = MagicMock()
        deployment.docker.return_value.stdout = "0\n"
        with self.assertRaisesRegex(ValueError, "no live process"):
            RETENTION.Monitor(deployment, Path("unused")).add("stopped")

    def test_failed_admission_retains_resources_and_removes_bearer_file(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "run"
            workload = Path(temporary) / "recipe.json"
            workload.write_text('{"count":1}', encoding="utf-8")
            arguments = SimpleNamespace(
                workload=workload, count=None, image="fixture", scale_binary=Path("scale"),
                admin_binary=Path("admin"), timeout=30, admission_only=True,
                profile="diagnostic", steady_seconds=0,
                recovery_probes=False,
                cpu_affinity=False,
                output_probe=False,
                physical_recovery_probes=False,
                safety_probes=False,
            )
            api = MagicMock(base="http://fixture", token="sensitive-bearer")
            api.get.return_value = {"policy": {"revision": 0, "policy": {
                "deployment": {}, "principal": {}, "collection": {}, "kinds": [],
            }}}
            api.request.return_value = ({"updated": True}, {})
            deployment = MagicMock()
            deployment.__enter__.return_value = deployment
            deployment.admin.side_effect = ["", "", "", '{"preview":{"confirmation":"proof"}}', '{}']
            deployment.login.return_value = api
            deployment.start_measured_api.return_value = (api, "fixture-api")
            deployment.repository = root / "repository"
            deployment.host_url = "postgres://fixture"
            deployment.host_environment.return_value = {}
            deployment.host_command.side_effect = lambda command: command
            deployment.cpu_plan = {}
            deployment.engine = {}
            deployment.containers = []
            deployment.application_containers = []
            monitor = MagicMock()
            monitor.__enter__.return_value = monitor
            monitor.peaks = {"fixture": {"sampled_rss_bytes": 4096}}
            monitor.samples = 3
            monitor.repository_peak_bytes = 100
            monitor.repository_peak_files = 2
            monitor.errors = []
            monitor.enforcement = {}
            with patch.object(RETENTION, "RetentionDeployment", return_value=deployment), \
                    patch.object(RETENTION, "Monitor", return_value=monitor), \
                    patch.object(RETENTION.subprocess, "run", side_effect=subprocess.CalledProcessError(1, "fixture")):
                with self.assertRaisesRegex(RuntimeError, "failed"):
                    RETENTION.measured_run(arguments, root, 1)
            report = json.loads((root / "resources.json").read_text(encoding="utf-8"))
            self.assertFalse(report["workload_completed"])
            self.assertFalse(report["p6_acceptance"])
            self.assertEqual(report["samples"], 3)
            self.assertFalse((root / "tokens.json").exists())
            self.assertNotIn("sensitive-bearer", (root / "resources.json").read_text(encoding="utf-8"))
