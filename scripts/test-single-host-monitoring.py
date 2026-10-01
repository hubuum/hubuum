#!/usr/bin/env python3
"""Test the installed operator package; --live uses real monitoring containers."""
import sys

if sys.version_info < (3, 11):
    sys.exit(
        "Hubuum tooling requires Python 3.11 or newer; found "
        + sys.version.split()[0]
        + ". Install Python 3.11+ and ensure python3 on PATH selects it."
    )

import argparse
import base64
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import socket
import ssl
import subprocess
import tempfile
import time
import unittest
import urllib.error
import urllib.request
import urllib.parse

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("tag_tests", ROOT / "scripts/test-single-host-tags.py")
tags = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tags)


class MonitoringTests(tags.InstallerFixture):
    # Share the existing engine/download fixture without rerunning tag tests.
    def setUp(self):
        super().setUp()
        shutil.copytree(ROOT / "observability", self.root / "observability")
        engine = self.root / "bin/docker"
        engine.write_text(engine.read_text() + '\nif [[ "$*" == *" hash-password" ]]; then\n  cat >/dev/null\n  printf \'$2a$14$fixturehash\\n\'\nfi\n')
        shutil.copy(engine, self.root / "bin/podman")
        self.engine = "docker"

    def run_script(self, script, *args, **kwargs):
        # Last --engine takes precedence over the base fixture's Docker default.
        return super().run_script(script, *args, "--engine", self.engine, **kwargs)

    def test_monitoring_is_opt_in(self):
        self.install()
        self.assertNotIn('  prometheus:', (self.installation / "compose.yml").read_text())
        self.assertNotIn('import monitoring', (self.installation / "Caddyfile").read_text())
        self.assertFalse((self.installation / "monitoring").exists())

    def test_supported_routes_and_engines(self):
        for engine in ("docker", "podman"):
            for mode, routing in (("backend", ""), ("all", ""), ("all", "bff"), ("all", "direct"), ("all", "prefixed")):
                with self.subTest(engine=engine, mode=mode, routing=routing):
                    shutil.rmtree(self.installation, ignore_errors=True)
                    self.engine = engine
                    options = ["--mode", mode, "--monitoring"]
                    if routing:
                        options += ["--web", "api.example.invalid", "--shared-host-routing", routing]
                    result = self.run_script("install-single-host.sh", "--web", "web.example.invalid",
                                             "--api", "api.example.invalid", "--email", "admin@example.invalid", *options)
                    values = self.values()
                    hostname = "web.example.invalid" if mode == "all" and not routing else "api.example.invalid"
                    self.assertEqual(values["MONITORING_HOST"], hostname)
                    self.assertNotIn(values["GRAFANA_ADMIN_PASSWORD"], result.stdout + result.stderr)
                    self.assertEqual((self.installation / ".env").stat().st_mode & 0o777, 0o600)
                    caddy = (self.installation / "Caddyfile").read_text()
                    self.assertEqual(caddy.count("import monitoring"), 1)
                    self.assertIn("basic_auth", caddy)
                    self.assertNotIn(values["PROMETHEUS_PASSWORD"], caddy)
                    compose = (self.installation / "compose.yml").read_text()
                    monitoring = compose.split("  prometheus:")[1].split("  caddy:")[0]
                    self.assertNotIn("ports:", monitoring)
                    targets = json.loads((self.installation / "monitoring/prometheus/targets.json").read_text())
                    self.assertEqual([t["targets"] for t in targets], [["hubuum-api:8080"], ["hubuum-api-standby:8080"]])
                    for asset in (ROOT / "observability/manifest.txt").read_text().splitlines():
                        if asset.startswith("dashboards/"):
                            installed = self.installation / "monitoring/grafana" / asset
                        else:
                            name = "hubuum.json" if asset.endswith("/alerts.json") else "hubuum-recording.json"
                            installed = self.installation / "monitoring/prometheus/rules" / name
                        self.assertEqual(installed.read_bytes(), (ROOT / "observability" / asset).read_bytes())

    def test_update_preserves_settings_and_credentials(self):
        self.install("--monitoring")
        before = self.values()
        config = self.installation / "monitoring/prometheus/prometheus.yml"
        config.write_text(config.read_text() + "# operator configuration\n")
        # The update fixture downloads from its local remote. Point that installed
        # script back at the fixture's asset tree, just as a source checkout does.
        self.update()
        after = self.values()
        for key in ("GRAFANA_ADMIN_PASSWORD", "GRAFANA_SECRET_KEY", "PROMETHEUS_PASSWORD", "MONITORING_DEPLOYMENT", "MONITORING_ENABLED", "PROMETHEUS_IMAGE", "GRAFANA_IMAGE"):
            self.assertEqual(after[key], before[key])
        self.assertIn("# operator configuration", config.read_text())
        self.assertIn("prometheus_data:", (self.installation / "compose.yml").read_text())

    def test_existing_installation_can_enable_monitoring(self):
        self.install()
        result = self.run_script("update-single-host.sh", "--monitoring")
        self.assertEqual(self.values()["MONITORING_ENABLED"], "true")
        self.assertIn("  grafana:", (self.installation / "compose.yml").read_text())
        self.assertIn("/grafana/", result.stdout)
        self.assertIn("GRAFANA_ADMIN_PASSWORD", result.stdout)
        self.assertNotIn(self.values()["GRAFANA_ADMIN_PASSWORD"], result.stdout)

    def test_lifecycle_preserves_until_explicit_purge(self):
        for engine in ("docker", "podman"):
            for mode in ("all", "backend"):
                with self.subTest(engine=engine, mode=mode):
                    self.engine = engine
                    self.install("--mode", mode, "--monitoring")
                    self.run_script("install-single-host.sh", "--stop")
                    self.assertIn("down --remove-orphans", self.commands.read_text())
                    self.assertTrue((self.installation / "monitoring").exists())
                    self.run_script("install-single-host.sh", "--uninstall")
                    self.assertTrue((self.installation / ".env").exists())
                    self.run_script("install-single-host.sh", "--uninstall", "--purge")
                    self.assertIn("down -v --remove-orphans", self.commands.read_text())
                    self.assertFalse(self.installation.exists())


def live(engine, without_resource_limits=False):
    """Exercise generated services, subpaths, auth and persistent volumes.

    Hubuum HTTP fixtures provide separate, stable process metrics; production
    Hubuum metrics and API semantics are covered by the Rust integration suite.
    """
    with tempfile.TemporaryDirectory(prefix="hubuum-monitoring-live-") as directory:
        root = Path(directory)
        installation = root / "installation"
        scripts = root / "scripts"
        scripts.mkdir()
        shutil.copytree(ROOT / "observability", root / "observability")
        for name in ("install-single-host.sh", "single-host-monitoring.sh", "update-single-host.sh", "stop-single-host.sh", "uninstall-single-host.sh"):
            source = (ROOT / "scripts" / name).read_text().replace('if [[ "$EUID" -ne 0 ]]; then', 'if false; then')
            (scripts / name).write_text(source)
        (scripts / "single-host-rollout.sh").write_text('hubuum_rollout() { :; }\n')
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        project = f"hubuum-monitoring-{os.getpid()}"
        compose = [engine, "compose", "--project-name", project, "--env-file", str(installation / ".env"), "-f", str(installation / "compose.yml")]

        def run(*args, **kwargs):
            result = subprocess.run(args, check=False, text=True, capture_output=True, **kwargs)
            if result.returncode:
                raise RuntimeError(f"{args[0]} failed: {result.stdout}\n{result.stderr}")
            return result

        def request(path, password=None, payload=None):
            headers = {}
            if password:
                headers["Authorization"] = "Basic " + base64.b64encode(f"admin:{password}".encode()).decode()
            if payload is not None:
                headers["Content-Type"] = "application/json"
            req = urllib.request.Request(f"https://localhost:{port}{path}", headers=headers,
                                         data=json.dumps(payload).encode() if payload is not None else None)
            try:
                with urllib.request.urlopen(req, context=ssl._create_unverified_context(), timeout=10) as response:
                    return response.status, response.read().decode()
            except urllib.error.HTTPError as error:
                return error.code, error.read().decode()

        try:
            run("bash", str(scripts / "install-single-host.sh"), "--dir", str(installation), "--mode", "backend",
                "--api", "localhost", "--email", "test@example.invalid", "--engine", engine, "--monitoring", "--no-pull")
            environment = installation / ".env"
            environment.write_text(environment.read_text().replace("MONITORING_HOST=localhost", f"MONITORING_HOST=localhost:{port}"))
            values = dict(line.split("=", 1) for line in environment.read_text().splitlines() if "=" in line)
            fixture_environment = environment.read_text()
            content = (installation / "compose.yml").read_text()
            # Keep the actual monitoring and Caddy services exactly as generated,
            # apart from ephemeral test ports and removing globally fixed names.
            content = "services:\n" + content[content.index("  prometheus:"):]
            content = re.sub(r"^    container_name:.*\n", "", content, flags=re.M)
            content = content.replace('      - "80:80"\n', '').replace('      - "443:443"', f'      - "127.0.0.1:{port}:443"')
            content = content.replace("    ipam:\n      config:\n        - subnet: 172.30.42.0/24\n", "")
            if without_resource_limits:
                content = re.sub(r"^    (mem_limit|cpus):.*\n", "", content, flags=re.M)
            # Caddy fixtures serve genuine Prometheus exposition independently.
            fixture = '''  hubuum-api: &fixture
    image: ${CADDY_IMAGE}
    command: ["caddy", "run", "--config", "/fixtures/Caddyfile", "--adapter", "caddyfile"]
    volumes:
      - ./fixtures:/fixtures:ro,z
    networks: [hubuum_net]
  hubuum-api-standby:
    <<: *fixture
'''
            content = content.replace("services:\n", "services:\n" + fixture)
            (installation / "compose.yml").write_text(content)
            (installation / "fixtures").mkdir()
            (installation / "fixtures/Caddyfile").write_text(':8080 {\n root * /fixtures\n header /metrics Content-Type "text/plain; version=0.0.4; charset=utf-8"\n file_server\n}\n')
            (installation / "fixtures/metrics").write_text('# TYPE hubuum_runtime_info gauge\nhubuum_runtime_info{role="all"} 1\n# TYPE hubuum_db_pool_connections gauge\nhubuum_db_pool_connections{state="configured"} 10\nhubuum_db_pool_connections{state="checked_out"} 1\n')
            caddy = installation / "Caddyfile"
            caddy.write_text(caddy.read_text().replace("localhost {", "localhost {\n\ttls internal"))
            run(*compose, "up", "-d")
            deadline = time.monotonic() + 180
            while True:
                try:
                    code, body = request("/prometheus/api/v1/targets", values["PROMETHEUS_PASSWORD"])
                    targets = json.loads(body)["data"]["activeTargets"] if code == 200 else []
                    grafana_ready = request("/grafana/api/health")[0] == 200
                    if len(targets) == 2 and all(t["health"] == "up" for t in targets) and grafana_ready:
                        break
                except (OSError, KeyError, ValueError):
                    pass
                if time.monotonic() >= deadline:
                    raise AssertionError("Monitoring did not become healthy")
                time.sleep(1)
            assert request("/prometheus/api/v1/targets")[0] == 401
            assert request("/prometheus/api/v1/targets", "incorrect")[0] == 401
            assert request("/grafana/api/search")[0] == 401
            password = values["GRAFANA_ADMIN_PASSWORD"]
            status, body = request("/grafana/api/search?tag=hubuum", password)
            assert status == 200 and len(json.loads(body)) == 7, body
            status, body = request("/grafana/api/datasources/uid/hubuum-prometheus/health", password)
            assert status == 200 and json.loads(body)["status"] == "OK", body
            status, body = request("/prometheus/api/v1/rules", values["PROMETHEUS_PASSWORD"])
            assert status == 200 and len(json.loads(body)["data"]["groups"]) == 3, body
            # Check every shared-domain routing mode against the actual Caddy
            # parser and live upstreams, restoring the isolated Compose fixture.
            for routing in ("bff", "direct", "prefixed"):
                run("bash", str(scripts / "install-single-host.sh"), "--dir", str(installation),
                    "--engine", engine, "--refresh-config", "--mode", "all", "--web", "localhost",
                    "--api", "localhost", "--shared-host-routing", routing)
                (installation / "compose.yml").write_text(content)
                environment.write_text(fixture_environment)
                caddy.write_text(caddy.read_text().replace("localhost {", "localhost {\n\ttls internal"))
                run(*compose, "exec", "-T", "caddy", "caddy", "reload", "--config", "/etc/caddy/Caddyfile", "--adapter", "caddyfile")
                assert request("/grafana/api/health")[0] == 200, routing
                assert request("/prometheus/api/v1/targets", values["PROMETHEUS_PASSWORD"])[0] == 200, routing
                assert request("/prometheus/api/v1/targets")[0] == 401, routing
            status, body = request("/grafana/api/dashboards/db", password,
                                   {"dashboard": {"uid": "persistence-test", "title": "Persistence test", "panels": [], "schemaVersion": 39}})
            assert status == 200, body
            historic_query = "/prometheus/api/v1/query?" + urllib.parse.urlencode({"query": 'up{job="hubuum"}', "time": time.time()})
            historic = request(historic_query, values["PROMETHEUS_PASSWORD"])
            assert historic[0] == 200 and len(json.loads(historic[1])["data"]["result"]) == 2
            # Recreate the installed services, preserving the DB and TSDB volumes.
            run(*compose, "up", "-d", "--force-recreate", "prometheus", "grafana")
            deadline = time.monotonic() + 90
            while time.monotonic() < deadline:
                try:
                    if request("/grafana/api/dashboards/uid/persistence-test", password)[0] == 200 and request(historic_query, values["PROMETHEUS_PASSWORD"])[0] == 200:
                        break
                except OSError:
                    pass
                time.sleep(1)
            else:
                raise AssertionError("Grafana credentials did not survive recreation")
            restored = request(historic_query, values["PROMETHEUS_PASSWORD"])
            assert json.loads(restored[1])["data"]["result"] == json.loads(historic[1])["data"]["result"], restored
            run(*compose, "down")
            volumes = run(engine, "volume", "ls", "--filter", f"label=com.docker.compose.project={project}", "--format", "{{.Name}}").stdout
            assert "grafana_data" in volumes and "prometheus_data" in volumes, volumes
            run(*compose, "down", "--volumes")
            remaining = run(engine, "volume", "ls", "--filter", f"label=com.docker.compose.project={project}", "--format", "{{.Name}}").stdout
            assert not remaining.strip(), remaining
            print(f"Live monitoring passed using {engine}: authentication, both targets, seven dashboards, rules, and retained volumes")
        except BaseException:
            diagnostics = subprocess.run([*compose, "logs", "--tail", "60"], text=True, capture_output=True, check=False)
            print(diagnostics.stdout + diagnostics.stderr, file=sys.stderr)
            raise
        finally:
            subprocess.run([*compose, "down", "--volumes", "--remove-orphans"], check=False, capture_output=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--engine", choices=("docker", "podman"), default="docker")
    parser.add_argument("--without-resource-limits", action="store_true", help="local rootless test only, when cgroup controllers are unavailable; CI keeps limits")
    args = parser.parse_args()
    if args.live:
        live(args.engine, args.without_resource_limits)
    else:
        unittest.main(argv=[sys.argv[0]])
