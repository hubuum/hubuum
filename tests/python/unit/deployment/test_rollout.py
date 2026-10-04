"""Run rollout and updater failure paths against a local Compose fixture."""

import json
import shutil
import subprocess
import sys

from support import ROOT
from support.installer import InstallerFixture


ENGINE = r'''
import json
import os
from pathlib import Path
import sys

args = sys.argv[1:]
with open(os.environ["COMMAND_LOG"], "a") as log:
    log.write(json.dumps(args) + "\n")
path = Path(os.environ["ROLLOUT_STATE"])
state = json.loads(path.read_text())
if args[0] == "compose":
    args = args[1:]
    while args[0] in ("--env-file", "-f"):
        args = args[2:]
command = args[0]
containers = state["containers"]
if command == "ps":
    if state.get("discovery_failure") == "ps":
        sys.exit(1)
    if "--help" in args:
        print("--all" if state["provider"] == "docker" else "--quiet")
    else:
        if state["provider"] == "podman" and "-a" in args:
            sys.exit("this podman-compose version does not support -a")
        for service, status in containers.items():
            if status == "running" or "-a" in args or state["provider"] == "podman":
                print("container-" + service)
elif command == "inspect":
    if state.get("discovery_failure") == "inspect":
        sys.exit(1)
    service = args[-1].removeprefix("container-")
    if "Config.Labels" in args[2]:
        print(service)
    elif ".Dependencies" not in args[2]:
        print("unhealthy" if service == state.get("unhealthy") else
              "healthy" if containers[service] == "running" else "exited")
elif command == "run" and args[-1] == "--migration-mode":
    print(state.get("mode", "rolling"))
    sys.exit(state.get("preflight_status", 0))
elif command in ("up", "stop"):
    services = [arg for arg in args[1:] if arg in (
        "caddy", "postgres", "valkey", "hubuum-api", "hubuum-api-standby",
        "hubuum-restore-executor", "prometheus", "grafana")]
    if command == "up" and "prometheus" in services and state.get("monitoring_start_failure"):
        sys.exit(1)
    for service in services:
        containers[service] = "running" if command == "up" else "exited"
    path.write_text(json.dumps(state))
elif command == "exec" and "wget" in args:
    print(json.dumps([{"address": service + ":8080", "fails": 0}
                      for service in ("hubuum-api", "hubuum-api-standby")]))
'''


class RolloutTests(InstallerFixture):
    def setUp(self):
        super().setUp()
        self.state = self.root / "rollout-state.json"
        self.environment["ROLLOUT_STATE"] = str(self.state)
        self.environment.pop("HUBUUM_ROLLOUT_REQUIRE_PREFLIGHT", None)
        self.environment.update({
            "INSTALL_MODE": "backend", "DATABASE_MANAGED": "false",
            "MONITORING_ENABLED": "false", "HUBUUM_ROLLOUT_HEALTH_TIMEOUT_SECONDS": "1",
        })
        self.engine = self.root / "bin/docker"
        self.engine.write_text(f"#!{sys.executable}\n" + ENGINE)
        shutil.copy(self.engine, self.root / "bin/podman")
        shutil.copy(ROOT / "scripts/single-host-rollout.sh", self.scripts)

    def configure(self, provider="docker", status="exited", monitoring=False, **settings):
        containers = {} if status == "missing" else {
            service: status for service in (
                "caddy", "hubuum-api", "hubuum-api-standby", "hubuum-restore-executor")
        }
        if monitoring:
            containers.update({"prometheus": "running", "grafana": "running"})
        self.state.write_text(json.dumps({
            "provider": provider, "containers": containers, **settings,
        }))
        self.commands.write_text("")

    def rollout(self, **environment):
        return subprocess.run(
            ["bash", "-euo", "pipefail", "-c", '''
ENGINE_PATH="$1"
COMPOSE_CMD=("$ENGINE_PATH" compose)
source "$2"
hubuum_rollout
''', "rollout-test", str(self.engine), str(ROOT / "scripts/single-host-rollout.sh")],
            env=self.environment | environment, text=True, capture_output=True, timeout=15,
        )

    def recorded_commands(self):
        return [json.loads(line) for line in self.commands.read_text().splitlines()]

    def assert_preflight_only(self):
        commands = self.recorded_commands()
        self.assertEqual(sum(command[-1] == "--migration-mode" for command in commands), 1)
        mutations = [command for command in commands
                     if any(verb in command for verb in ("run", "up", "start", "stop", "rm"))]
        self.assertEqual(len(mutations), 1, mutations)
        self.assertEqual(mutations[0][-1], "--migration-mode")

    def test_stopped_deployment_rejects_failed_or_invalid_preflight(self):
        for provider in ("docker", "podman"):
            for settings in ({"preflight_status": 1}, {"mode": "invalid"}):
                with self.subTest(provider=provider, settings=settings):
                    self.configure(provider, **settings)
                    result = self.rollout()
                    self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
                    self.assert_preflight_only()

    def test_stopped_deployment_recovers_after_successful_preflight(self):
        for provider in ("docker", "podman"):
            for mode in ("rolling", "offline"):
                with self.subTest(provider=provider, mode=mode):
                    self.configure(provider, mode=mode)
                    result = self.rollout()
                    self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                    commands = self.recorded_commands()
                    self.assertEqual(sum(c[-1] == "--migration-mode" for c in commands), 1)
                    preflight = next(i for i, c in enumerate(commands) if c[-1] == "--migration-mode")
                    stop = next(i for i, c in enumerate(commands) if "stop" in c)
                    migrate = next(i for i, c in enumerate(commands) if c[-1] == "--migrate")
                    self.assertLess(preflight, stop)
                    self.assertLess(stop, migrate)
                    self.assertTrue(all(status == "running" for status in
                                        json.loads(self.state.read_text())["containers"].values()))

    def test_container_discovery_errors_block_rollout(self):
        for failure in ("ps", "inspect"):
            with self.subTest(failure=failure):
                self.configure(discovery_failure=failure)
                self.assertNotEqual(self.rollout().returncode, 0)
                self.assertFalse(any(verb in command for command in self.recorded_commands()
                                     for verb in ("run", "up", "stop")))

    def test_updater_requires_preflight_after_containers_are_removed(self):
        for provider in ("docker", "podman"):
            with self.subTest(provider=provider):
                self.seed_legacy_installation()
                self.configure(provider, status="missing", preflight_status=1)
                result = self.run_script("update-single-host.sh", "--engine", provider, success=False)
                self.assertIn("migration preflight failed", result.stderr)
                self.assert_preflight_only()

    def test_fresh_install_can_bootstrap_before_migrator_roles_exist(self):
        self.configure(status="missing", preflight_status=1)
        result = self.rollout()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertTrue(any(command[-1] == "--migrate" for command in self.recorded_commands()))
        self.assertFalse(any(command[-1] == "--migration-mode" for command in self.recorded_commands()))

    def test_monitoring_failures_abort_initial_and_offline_startup(self):
        for status in ("missing", "exited"):
            for failure in ({"monitoring_start_failure": True},
                            {"unhealthy": "prometheus"}, {"unhealthy": "grafana"}):
                with self.subTest(status=status, failure=failure):
                    self.configure(status=status, mode="offline", monitoring=True, **failure)
                    result = self.rollout(MONITORING_ENABLED="true")
                    self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
                    self.assertFalse(any("up" in command and command[-1] == "caddy"
                                         for command in self.recorded_commands()))
