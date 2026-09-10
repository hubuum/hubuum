#!/usr/bin/env python3
"""Exercise deployment image selection with local scripts and a fake engine."""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
SERVER = "ghcr.io/hubuum/hubuum-server"
FRONTEND = "ghcr.io/hubuum/hubuum-frontend"


class ImageTagTests(unittest.TestCase):
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

    def test_legacy_installation_accepts_updates_with_and_without_tag_overrides(self):
        for script in ("install-single-host.sh", "update-single-host.sh"):
            for saved_server in ("main", "v0.0.14"):
                for options, server_tag, frontend_tag in (
                    ((), saved_server, "main"),
                    (("--tag", "latest"), "latest", "latest"),
                    (("--server-tag", "v0.0.14"), "v0.0.14", "main"),
                    (("--frontend-tag", "latest"), saved_server, "latest"),
                ):
                    with self.subTest(script=script, saved=saved_server, options=options):
                        self.seed_legacy_installation(saved_server)
                        self.run_script(script, *options)
                        self.assert_images(f"{SERVER}:{server_tag}", f"{FRONTEND}:{frontend_tag}")

    def test_legacy_credentials_and_database_image_survive_upgrade(self):
        for script in ("install-single-host.sh", "update-single-host.sh"):
            for options in ((), ("--tag", "latest")):
                with self.subTest(script=script, options=options):
                    original = self.seed_legacy_installation()
                    self.run_script(script, *options)
                    actual = self.values()
                    for key in (
                        "POSTGRES_PASSWORD", "HUBUUM_DATABASE_URL",
                        "HUBUUM_TOKEN_HASH_KEY", "POSTGRES_IMAGE",
                    ):
                        self.assertEqual(actual[key], original[key], key)

    def test_legacy_installation_accepts_current_scripts_from_stdin(self):
        for script in ("install-single-host.sh", "update-single-host.sh"):
            for options, server_tag in (((), "main"), (("--server-tag", "v0.0.14"), "v0.0.14")):
                with self.subTest(script=script, options=options):
                    self.seed_legacy_installation()
                    self.run_script(script, *options, piped=True)
                    self.assert_images(f"{SERVER}:{server_tag}", f"{FRONTEND}:main")

    def test_legacy_helpers_and_compose_are_upgraded_for_subsequent_updates(self):
        for script in ("install-single-host.sh", "update-single-host.sh"):
            with self.subTest(script=script):
                self.seed_legacy_installation()
                self.run_script(script, "--server-tag", "v0.0.14")
                for helper in self.scripts.iterdir():
                    installed = self.installation / helper.name
                    self.assertEqual(installed.read_text(), helper.read_text())
                    self.assertTrue(os.access(installed, os.X_OK))
                compose = (self.installation / "compose.yml").read_text()
                self.assertIn("  hubuum-api-standby:", compose)
                self.assertIn("  hubuum-restore-executor:", compose)
                # Invoke the refreshed, installed updater on the next run.
                self.commands.write_text("")
                self.run_script(str(self.installation / "update-single-host.sh"))
                self.assert_images(f"{SERVER}:v0.0.14", f"{FRONTEND}:main")

    def test_fresh_install_defaults_to_latest(self):
        self.install()
        self.assert_images(f"{SERVER}:latest", f"{FRONTEND}:latest")

    def test_shared_tag_is_used_by_install_and_update(self):
        for tag in ("latest", "main", "v0.0.14", "_build.1-test", "a" * 128):
            for action in (self.install, self.update):
                with self.subTest(tag=tag, action=action.__name__):
                    action("--tag", tag)
                    self.assert_images(f"{SERVER}:{tag}", f"{FRONTEND}:{tag}")

    def test_component_tags_override_shared_tag_in_either_order(self):
        self.install()
        for action in (self.install, self.update):
            for options in (
                ("--tag", "main", "--server-tag", "v0.0.14", "--frontend-tag", "latest"),
                ("--frontend-tag", "latest", "--backend-tag", "v0.0.14", "--tag", "main"),
            ):
                with self.subTest(action=action.__name__, options=options):
                    action(*options)
                    self.assert_images(f"{SERVER}:v0.0.14", f"{FRONTEND}:latest")

    def test_component_override_leaves_other_image_unchanged_and_persists(self):
        for option, expected_server, expected_frontend in (
            ("--server-tag", "v0.0.14", "main"),
            ("--backend-tag", "v0.0.14", "main"),
            ("--frontend-tag", "main", "v0.0.14"),
        ):
            for action in (self.install, self.update):
                with self.subTest(option=option, action=action.__name__):
                    self.install("--tag", "main")
                    action(option, "v0.0.14")
                    self.commands.write_text("")
                    self.update()
                    self.assert_images(f"{SERVER}:{expected_server}", f"{FRONTEND}:{expected_frontend}")

    def test_existing_image_choices_survive_install_and_update_without_options(self):
        self.install("--server-tag", "v0.0.14", "--frontend-tag", "main")
        for action in (self.install, self.update):
            with self.subTest(action=action.__name__):
                self.commands.write_text("")
                action()
                self.assert_images(f"{SERVER}:v0.0.14", f"{FRONTEND}:main")

    def test_explicit_full_image_overrides_install_tag_options(self):
        image = "registry.example.invalid:5000/custom/server@sha256:" + "a" * 64
        for options in (
            ("--backend-image", image, "--tag", "main", "--server-tag", "latest"),
            ("--tag", "main", "--server-tag", "latest", "--backend-image", image),
        ):
            with self.subTest(options=options):
                self.install(*options)
                self.assert_images(image, f"{FRONTEND}:main")

    def test_tags_preserve_custom_repositories_and_replace_digests(self):
        repository = "registry.example.invalid:5000/custom/server"
        for suffix in ("", ":old", "@sha256:" + "a" * 64, ":old@sha256:" + "a" * 64):
            for action in (self.install, self.update):
                with self.subTest(suffix=suffix, action=action.__name__):
                    self.install("--backend-image", repository + suffix)
                    action("--server-tag", "v0.0.14")
                    self.assert_images(f"{repository}:v0.0.14", f"{FRONTEND}:latest")

    def test_refresh_config_saves_explicit_tags_and_preserves_operator_settings(self):
        self.install("--tag", "main")
        with (self.installation / ".env").open("a") as env:
            env.write("OPERATOR_SETTING=preserved\n")
        self.run_script("install-single-host.sh", "--refresh-config", "--server-tag", "v0.0.14")
        self.update()
        self.assert_images(f"{SERVER}:v0.0.14", f"{FRONTEND}:main")
        self.assertEqual(self.values()["OPERATOR_SETTING"], "preserved")

    def test_backend_only_install_remembers_both_tags(self):
        self.install("--mode", "backend", "--server-tag", "v0.0.14", "--frontend-tag", "main")
        self.update()
        self.assert_images(f"{SERVER}:v0.0.14", f"{FRONTEND}:main")
        self.assertNotIn("  hubuum-web:", (self.installation / "compose.yml").read_text())

    def test_podman_update_uses_saved_tag_choices(self):
        self.install("--tag", "main", "--engine", "podman")
        self.update("--engine", "podman", "--server-tag", "v0.0.14")
        self.assert_images(f"{SERVER}:v0.0.14", f"{FRONTEND}:main")

    def test_invalid_tags_fail_before_touching_deployment(self):
        self.install()
        original = (self.installation / ".env").read_bytes()
        self.commands.write_text("")
        for script in ("install-single-host.sh", "update-single-host.sh"):
            for option in ("--tag", "--server-tag", "--backend-tag", "--frontend-tag"):
                for value in (
                    "", "-bad", ".bad", "a/b", "a:b", "a@b", "two words",
                    "a\nb", "$(id)", "é", "a" * 129,
                ):
                    with self.subTest(script=script, option=option, value=value):
                        result = self.run_script(script, option, value, success=False)
                        self.assertIn(f"{option} requires a valid image tag", result.stderr)
                        self.assertEqual((self.installation / ".env").read_bytes(), original)
                        self.assertEqual(self.commands.read_text(), "")
                with self.subTest(script=script, option=option, value="missing"):
                    result = self.run_script(script, option, success=False)
                    self.assertIn(f"{option} requires a valid image tag", result.stderr)

    def test_source_build_rejects_tag_options_without_changing_configuration(self):
        self.install()
        env = self.installation / ".env"
        env.write_text(env.read_text().replace("BUILD_FROM_SOURCE=false", "BUILD_FROM_SOURCE=true"))
        original = env.read_bytes()
        for script in ("install-single-host.sh", "update-single-host.sh"):
            with self.subTest(script=script):
                result = self.run_script(script, "--tag", "main", success=False)
                self.assertIn("image tag options cannot be used with source builds", result.stderr)
                self.assertEqual(env.read_bytes(), original)


class ReleaseTagTests(unittest.TestCase):
    def test_only_stable_releases_publish_latest(self):
        workflow = (REPOSITORY_ROOT / ".github/workflows/ci.yml").read_text()
        start = workflow.index("          final_tags=(\n")
        end = workflow.index("          # Create and push manifest", start)
        script = workflow[start:end]
        for tag, expected_latest in (("v0.0.14", True), ("v0.0.15-rc.1", False)):
            with self.subTest(tag=tag):
                result = subprocess.run(
                    ["bash", "-eu", "-c", script + '\nprintf "%s\\n" "${final_tags[@]}"'],
                    env=os.environ | {"image": SERVER, "version_tag": tag, "version": tag[1:]},
                    text=True, capture_output=True, check=True,
                )
                tags = result.stdout.splitlines()
                self.assertEqual(f"{SERVER}:latest" in tags, expected_latest)
                self.assertIn(f"{SERVER}:{tag}", tags)


if __name__ == "__main__":
    unittest.main()
