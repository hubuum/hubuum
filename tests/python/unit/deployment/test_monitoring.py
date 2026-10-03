"""Generated installer configuration and lifecycle regressions."""

import json
import shutil

from support import ROOT
from support.installer import InstallerFixture


class MonitoringTests(InstallerFixture):
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

    def test_disabled_monitoring_preserves_settings_for_reenable(self):
        for engine in ("docker", "podman"):
            for mode in ("all", "backend"):
                for script, options in (("install-single-host.sh", ()),
                                        ("install-single-host.sh", ("--recreate",)),
                                        ("update-single-host.sh", ())):
                    with self.subTest(engine=engine, mode=mode, script=script, options=options):
                        shutil.rmtree(self.installation, ignore_errors=True)
                        self.engine = engine
                        self.install("--mode", mode, "--monitoring")
                        environment = self.installation / ".env"
                        environment.write_text(environment.read_text().replace(
                            "PROMETHEUS_RETENTION_TIME=31d", "PROMETHEUS_RETENTION_TIME=45d"))
                        before = {key: value for key, value in self.values().items()
                                  if key.startswith(("GRAFANA_", "PROMETHEUS_", "MONITORING_"))
                                  and key != "MONITORING_ENABLED"}
                        environment.write_text(environment.read_text().replace(
                            "MONITORING_ENABLED=true", "MONITORING_ENABLED=false"))
                        self.commands.write_text("")
                        self.run_script(script, *options)
                        disabled = self.values()
                        self.assertEqual(disabled["MONITORING_ENABLED"], "false")
                        for key, value in before.items():
                            self.assertEqual(disabled.get(key), value, key)
                        self.assertEqual(environment.stat().st_mode & 0o777, 0o600)
                        self.assertNotIn("hash-password", self.commands.read_text())
                        self.assertNotIn("  grafana:", (self.installation / "compose.yml").read_text())
                        self.assertNotIn("import monitoring", (self.installation / "Caddyfile").read_text())
                        self.assertIn("  grafana_data:", (self.installation / "compose.yml").read_text())
                        self.run_script(script, "--monitoring")
                        for key, value in before.items():
                            self.assertEqual(self.values().get(key), value, key)

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

    def test_postgres_data_directory_preserves_the_mounted_volume_layout(self):
        for image in ("postgres:17", "postgres:18"):
            with self.subTest(image=image):
                self.install("--postgres-image", image)
                self.update()
                postgres = (self.installation / "compose.yml").read_text().split("  postgres:")[1].split("  hubuum-migrate:")[0]
                self.assertIn("PGDATA: /var/lib/postgresql/data", postgres)
                self.assertIn("postgres_data:/var/lib/postgresql/data", postgres)
                self.assertIn("- /var/lib/postgresql:size=16m,mode=1777", postgres)

    def test_restore_executor_has_a_non_http_liveness_probe(self):
        self.install()
        compose = (self.installation / "compose.yml").read_text()
        executor = compose.split("  hubuum-restore-executor:")[1].split("  hubuum-api:")[0]
        self.assertIn('test: ["CMD-SHELL", "kill -0 1"]', executor)
        self.assertNotIn("wget", executor)
