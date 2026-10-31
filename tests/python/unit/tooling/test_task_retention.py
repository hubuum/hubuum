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
    def test_direct_endpoint_uses_only_the_managed_network(self):
        deployment = RETENTION.RetentionDeployment.__new__(RETENTION.RetentionDeployment)
        deployment.name = "owned-fixture"
        deployment.direct_container_network = True
        deployment.docker = MagicMock(return_value=SimpleNamespace(stdout=json.dumps({
            "unrelated": {"IPAddress": "10.1.0.1"}, "owned-fixture": {"IPAddress": "10.2.0.2"},
        })))
        self.assertEqual(deployment.host_endpoint("owned-fixture-db", 5432), "10.2.0.2:5432")

    def test_direct_endpoint_rejects_missing_or_unusable_managed_addresses(self):
        deployment = RETENTION.RetentionDeployment.__new__(RETENTION.RetentionDeployment)
        deployment.name = "owned-fixture"
        deployment.direct_container_network = True
        for networks in ({"other": {"IPAddress": "10.2.0.2"}}, *(
                {"owned-fixture": {"IPAddress": address}}
                for address in ("", "0.0.0.0", "127.0.0.1", "224.0.0.1"))):
            with self.subTest(networks=networks), self.assertRaises(ValueError):
                deployment.docker = MagicMock(return_value=SimpleNamespace(stdout=json.dumps(networks)))
                deployment.host_endpoint("owned-fixture-db", 5432)

    def test_default_endpoint_keeps_the_published_port(self):
        deployment = RETENTION.RetentionDeployment.__new__(RETENTION.RetentionDeployment)
        deployment.direct_container_network = False
        deployment.docker = MagicMock(return_value=SimpleNamespace(stdout="127.0.0.1:32999\n"))
        self.assertEqual(deployment.host_endpoint("owned-fixture-db", 5432), "127.0.0.1:32999")
        deployment.docker.assert_called_once_with("port", "owned-fixture-db", "5432/tcp")

    def test_direct_effect_fixture_uses_the_managed_network_gateway(self):
        deployment = RETENTION.RetentionDeployment.__new__(RETENTION.RetentionDeployment)
        deployment.name = "owned-fixture"
        deployment.docker = MagicMock(return_value=SimpleNamespace(stdout=json.dumps({
            "unrelated": {"Gateway": "10.1.0.1"},
            "owned-fixture": {"IPAddress": "10.2.0.2", "Gateway": "10.2.0.1"},
        })))
        self.assertEqual(deployment.network_address("owned-fixture-db", "Gateway"), "10.2.0.1")

    def test_administrator_retries_only_lock_refusal_and_retains_elapsed_attempts(self):
        busy = RuntimeError('Docker run failed (1): Error: ServiceUnavailable("Control repository is busy; retry without bypassing protection")\n')
        with tempfile.TemporaryDirectory() as temporary:
            deployment = RETENTION.RetentionDeployment.__new__(RETENTION.RetentionDeployment)
            deployment.root = Path(temporary)
            deployment._admin_once = MagicMock(side_effect=[busy, "completed"])
            clock = [0.0]

            def sleep(seconds):
                clock[0] += seconds

            with patch.object(RETENTION.time, "monotonic", side_effect=lambda: clock[0]), \
                    patch.object(RETENTION.time, "sleep", side_effect=sleep):
                result = deployment.admin("source", "--task-retention", "set-policy",
                                          "--control-recovery-key", "fixture-secret")
            self.assertEqual(result, "completed")
            self.assertEqual(deployment._admin_once.call_count, 2)
            self.assertEqual(deployment._admin_once.call_args_list[0], deployment._admin_once.call_args_list[1])
            report_path, = deployment.root.glob("admin-contention-*.json")
            report = json.loads(report_path.read_text())
            self.assertEqual(report, {"attempts": 2, "completed": True, "elapsed_ms": 100,
                                      "retry_budget_seconds": 30, "refusals": [
                                          {"attempt": 1, "elapsed_ms": 0, "outcome": "control_repository_busy"},
                                      ]})
            self.assertNotIn("fixture-secret", report_path.read_text())

    def test_administrator_does_not_retry_recovery_required(self):
        failure = RuntimeError('Error: ServiceUnavailable("Recover independently acknowledged control before task administration")')
        with tempfile.TemporaryDirectory() as temporary:
            deployment = RETENTION.RetentionDeployment.__new__(RETENTION.RetentionDeployment)
            deployment.root = Path(temporary)
            deployment._admin_once = MagicMock(side_effect=failure)
            with self.assertRaisesRegex(RuntimeError, "Recover independently"):
                deployment.admin("source", "--task-retention", "set-policy")
            self.assertEqual(deployment._admin_once.call_count, 1)
            self.assertEqual(list(deployment.root.glob("admin-contention-*.json")), [])

    def test_administrator_stops_at_contention_retry_budget(self):
        with tempfile.TemporaryDirectory() as temporary:
            deployment = RETENTION.RetentionDeployment.__new__(RETENTION.RetentionDeployment)
            deployment.root = Path(temporary)
            clock = [0.0]

            def refuse(*args, **kwargs):
                clock[0] += 10
                raise RuntimeError('Error: ServiceUnavailable("Control repository is busy; retry without bypassing protection")')

            deployment._admin_once = MagicMock(side_effect=refuse)
            with patch.object(RETENTION.time, "monotonic", side_effect=lambda: clock[0]), \
                    patch.object(RETENTION.time, "sleep"):
                with self.assertRaisesRegex(RuntimeError, "Control repository is busy"):
                    deployment.admin("source", "--task-retention", "set-policy")
            self.assertEqual(deployment._admin_once.call_count, 3)
            report_path, = deployment.root.glob("admin-contention-*.json")
            self.assertEqual(json.loads(report_path.read_text()), {
                "attempts": 3, "completed": False, "elapsed_ms": 30000,
                "retry_budget_seconds": 30,
                "refusals": [
                    {"attempt": attempt, "elapsed_ms": 10000, "outcome": "control_repository_busy"}
                    for attempt in range(1, 4)
                ],
            })

    def test_administrator_preserves_fatal_failure_after_a_retry(self):
        busy = RuntimeError('Docker run failed (1): engine warning\nError: ServiceUnavailable("Control repository is busy; retry without bypassing protection")')
        fatal = RuntimeError('Error: ServiceUnavailable("Configured key does not own protected task authority")')
        with tempfile.TemporaryDirectory() as temporary:
            deployment = RETENTION.RetentionDeployment.__new__(RETENTION.RetentionDeployment)
            deployment.root = Path(temporary)
            deployment._admin_once = MagicMock(side_effect=[busy, fatal])
            with patch.object(RETENTION.time, "sleep"), \
                    self.assertRaisesRegex(RuntimeError, "Configured key"):
                deployment.admin("source", "--task-retention", "set-policy")
            self.assertEqual(deployment._admin_once.call_count, 2)
            report_path, = deployment.root.glob("admin-contention-*.json")
            self.assertFalse(json.loads(report_path.read_text())["completed"])

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
        with patch.object(RETENTION.os, "sched_getaffinity", return_value=set(range(16)), create=True):
            plan = RETENTION.affinity_plan("shared")
        names = ("database", "api-0", "api-1", "worker-0", "worker-1", "administrator")
        masks = [RETENTION.cpu_set(plan[name]) for name in names]
        self.assertEqual([len(mask) for mask in masks], [4, 2, 2, 2, 2, 2])
        self.assertEqual(sum(map(len, masks)), len(set.union(*masks)))
        self.assertEqual(plan["all-2"], plan["api-0"])

    def test_affinity_plan_refuses_insufficient_cpus(self):
        with patch.object(RETENTION.os, "sched_getaffinity", return_value={0, 1}, create=True), \
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
                direct_container_network=False,
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
