"""Exercise deployment image selection with local scripts and a fake engine."""

import os
import subprocess
import unittest

from support import ROOT as REPOSITORY_ROOT
from support.installer import InstallerFixture, SERVER, FRONTEND


class ImageTagTests(InstallerFixture):
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

    def test_fresh_install_keeps_available_main_defaults(self):
        self.install()
        self.assert_images(f"{SERVER}:main", f"{FRONTEND}:main")

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
                    self.assert_images(f"{repository}:v0.0.14", f"{FRONTEND}:main")

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
