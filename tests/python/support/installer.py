"""Fake engine and download fixture for installer regression tests."""

from pathlib import Path
import os
import shutil
import subprocess
import tempfile
import unittest

from support import ROOT as REPOSITORY_ROOT


SERVER = "ghcr.io/hubuum/hubuum-server"
FRONTEND = "ghcr.io/hubuum/hubuum-frontend"

class InstallerFixture(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.scripts = self.root / "scripts"
        self.scripts.mkdir()
        self.installation = self.root / "installation"
        self.commands = self.root / "commands.log"
        self.environment = os.environ | {
            "PATH": f"{self.root / 'bin'}:{os.environ['PATH']}",
            "FAKE_REMOTE": str(self.scripts),
            "COMMAND_LOG": str(self.commands),
        }

        # Only bypass the root guard in temporary copies. All deployment files
        # stay in the fixture, and engine/download commands are replaced below.
        for name in (
            "install-single-host.sh",
            "update-single-host.sh",
            "stop-single-host.sh",
            "uninstall-single-host.sh",
            "single-host-monitoring.sh",
        ):
            source = (REPOSITORY_ROOT / "scripts" / name).read_text()
            source = source.replace('if [[ "$EUID" -ne 0 ]]; then', "if false; then")
            (self.scripts / name).write_text(source)
        (self.scripts / "single-host-rollout.sh").write_text(
            'hubuum_rollout() { echo rollout >> "$COMMAND_LOG"; }\n'
        )

        binaries = self.root / "bin"
        binaries.mkdir()
        engine = binaries / "docker"
        engine.write_text(
            '#!/usr/bin/env bash\nset -euo pipefail\n'
            'printf "%s\\n" "$*" >> "$COMMAND_LOG"\n'
            'if [[ "$*" == *" pull"* ]]; then\n'
            '  grep -E "^(BACKEND_IMAGE|FRONTEND_IMAGE)=" .env >> "$COMMAND_LOG"\n'
            'fi\n'
        )
        engine.chmod(0o755)
        shutil.copy(engine, binaries / "podman")
        curl = binaries / "curl"
        curl.write_text(
            '#!/usr/bin/env bash\nset -euo pipefail\n'
            '[[ "$1" == "-fsSL" && "$3" == "-o" ]]\n'
            'cp "$FAKE_REMOTE/${2##*/}" "$4"\n'
        )
        curl.chmod(0o755)

    def run_script(self, script, *arguments, success=True, piped=False):
        script_path = self.scripts / script
        command = ["bash", "-s", "--"] if piped else ["bash", str(script_path)]
        result = subprocess.run(
            [
                *command,
                "--dir", str(self.installation),
                "--engine", "docker", "--no-systemd", *arguments,
            ],
            env=self.environment,
            input=script_path.read_text() if piped else None,
            text=True,
            capture_output=True,
            check=False,
        )
        if success:
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        else:
            self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        return result

    def install(self, *arguments):
        self.run_script(
            "install-single-host.sh", "--web", "web.example.invalid",
            "--api", "api.example.invalid", "--email", "admin@example.invalid",
            *arguments,
        )

    def update(self, *arguments):
        self.run_script("update-single-host.sh", *arguments)

    def values(self):
        return dict(
            line.split("=", 1) for line in (self.installation / ".env").read_text().splitlines()
            if line and not line.startswith("#")
        )

    def assert_images(self, server, frontend):
        values = self.values()
        self.assertEqual(values["BACKEND_IMAGE"].strip('"'), server)
        self.assertEqual(values["FRONTEND_IMAGE"].strip('"'), frontend)
        commands = self.commands.read_text()
        # The persisted choices must be in effect when Compose pulls images.
        self.assertIn(f"BACKEND_IMAGE={values['BACKEND_IMAGE']}\n", commands)
        self.assertIn(f"FRONTEND_IMAGE={values['FRONTEND_IMAGE']}\n", commands)
        self.assertTrue(commands.endswith("rollout\n"), commands)

    def seed_legacy_installation(self, server_tag="main"):
        if self.installation.exists():
            shutil.rmtree(self.installation)
        self.installation.mkdir()
        self.commands.write_text("")
        # Older installers saved full image references, without tag settings,
        # auth mounts, database-role mode, or management-script URL metadata.
        values = {
            "INSTALL_MODE": "all",
            "WEB_FQDN": "web.example.invalid",
            "API_FQDN": "api.example.invalid",
            "LETSENCRYPT_EMAIL": "admin@example.invalid",
            "BUILD_FROM_SOURCE": "false",
            "CONTAINER_ENGINE": "docker",
            "SYSTEMD_SERVICE_NAME": "hubuum-legacy",
            "BACKEND_IMAGE": f"{SERVER}:{server_tag}",
            "FRONTEND_IMAGE": f'"{FRONTEND}:main"',
            "POSTGRES_IMAGE": "docker.io/library/postgres:17-alpine",
            "DATABASE_MANAGED": "true",
            "POSTGRES_DB": "hubuum",
            "POSTGRES_USER": "hubuum",
            "POSTGRES_PASSWORD": "legacy-fixture-password",
            "HUBUUM_DATABASE_URL": '"postgres://hubuum:legacy-fixture-password@postgres:5432/hubuum"',
            "HUBUUM_TOKEN_HASH_KEY": "a" * 64,
            "HUBUUM_BIND_PORT": "8080",
            "HUBUUM_CLIENT_ALLOWLIST": "172.30.42.0/24",
        }
        (self.installation / ".env").write_text(
            "".join(f"{key}={value}\n" for key, value in values.items())
        )
        (self.installation / "compose.yml").write_text(
            "services:\n  hubuum-api:\n    image: ${BACKEND_IMAGE}\n"
            "  hubuum-web:\n    image: ${FRONTEND_IMAGE}\n"
        )
        (self.installation / "Caddyfile").write_text("# Legacy proxy configuration\n")
        for script in self.scripts.iterdir():
            (self.installation / script.name).write_text(
                '#!/usr/bin/env bash\necho "legacy helper was not replaced" >&2\nexit 1\n'
            )
        return values
