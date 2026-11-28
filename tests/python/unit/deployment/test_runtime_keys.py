"""Exercise installer provisioning and retention of runtime data encryption keys."""

import base64

from support.installer import InstallerFixture


class RuntimeKeyTests(InstallerFixture):
    def test_legacy_upgrade_provisions_one_shared_key(self):
        self.seed_legacy_installation()
        self.update()
        settings = self.values()
        self.assertEqual(settings["HUBUUM_RUNTIME_SETTINGS_ENCRYPTION_KEY_REF"], "deployment")
        key = settings["HUBUUM_RUNTIME_SETTINGS_KEY_DEPLOYMENT"]
        self.assertEqual(len(base64.b64decode(key.removeprefix("base64:"), validate=True)), 32)
        compose = (self.installation / "compose.yml").read_text()
        self.assertIn("HUBUUM_RUNTIME_SETTINGS_KEY_DEPLOYMENT: ${HUBUUM_RUNTIME_SETTINGS_KEY_DEPLOYMENT}", compose)
        self.assertNotIn(key, compose)
        self.assertEqual((self.installation / ".env").stat().st_mode & 0o777, 0o600)

    def test_rotation_preserves_old_key_on_refresh_and_recreate(self):
        self.install()
        path = self.installation / ".env"
        original = self.values()["HUBUUM_RUNTIME_SETTINGS_KEY_DEPLOYMENT"]
        rotated = "0123456789abcdef0123456789abcdef"
        path.write_text(path.read_text().replace(
            "HUBUUM_RUNTIME_SETTINGS_ENCRYPTION_KEY_REF=deployment",
            "HUBUUM_RUNTIME_SETTINGS_ENCRYPTION_KEY_REF=next-key\n"
            f"HUBUUM_RUNTIME_SETTINGS_KEY_NEXT_KEY={rotated}",
        ))
        for args in (("--refresh-config",), ("--recreate",)):
            with self.subTest(args=args):
                self.install(*args)
                settings = self.values()
                self.assertEqual(settings["HUBUUM_RUNTIME_SETTINGS_KEY_DEPLOYMENT"], original)
                self.assertEqual(settings["HUBUUM_RUNTIME_SETTINGS_KEY_NEXT_KEY"], rotated)
                self.assertEqual(settings["HUBUUM_RUNTIME_SETTINGS_ENCRYPTION_KEY_REF"], "next-key")
                self.assertIn("HUBUUM_RUNTIME_SETTINGS_KEY_NEXT_KEY: ${HUBUUM_RUNTIME_SETTINGS_KEY_NEXT_KEY}",
                              (self.installation / "compose.yml").read_text())

    def test_update_refuses_to_replace_missing_active_key(self):
        self.install()
        path = self.installation / ".env"
        damaged = "\n".join(
            line for line in path.read_text().splitlines()
            if not line.startswith("HUBUUM_RUNTIME_SETTINGS_KEY_DEPLOYMENT=")
        ) + "\n"
        path.write_text(damaged)
        result = self.run_script("install-single-host.sh", "--refresh-config", success=False)
        self.assertIn("restore the configured runtime settings encryption key", result.stderr)
        self.assertEqual(path.read_text(), damaged)
