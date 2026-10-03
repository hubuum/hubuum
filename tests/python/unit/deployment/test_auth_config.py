"""Host-file permissions for the non-root API authentication mount."""

from support.installer import InstallerFixture


class AuthConfigTests(InstallerFixture):
    def test_fresh_local_only_placeholder_is_readable_by_the_container(self):
        self.install()
        auth = self.installation / 'auth.toml'
        self.assertEqual(auth.read_bytes(), b'')
        self.assertEqual(auth.stat().st_mode & 0o777, 0o644)

    def test_refresh_repairs_an_older_empty_placeholder(self):
        self.install()
        auth = self.installation / 'auth.toml'
        for operation in (self.install, self.update):
            with self.subTest(operation=operation.__name__):
                auth.chmod(0o600)
                operation()
                self.assertEqual(auth.read_bytes(), b'')
                self.assertEqual(auth.stat().st_mode & 0o777, 0o644)

    def test_refresh_preserves_populated_provider_file_permissions(self):
        self.install()
        auth = self.installation / 'auth.toml'
        contents = '# Operator provider configuration must remain private.\n'
        auth.write_text(contents)
        auth.chmod(0o600)
        for operation in (self.install, self.update):
            with self.subTest(operation=operation.__name__):
                operation()
                self.assertEqual(auth.read_text(), contents)
                self.assertEqual(auth.stat().st_mode & 0o777, 0o600)

    def test_empty_external_configuration_permissions_are_not_changed(self):
        auth = self.root / 'external-auth.toml'
        auth.touch(mode=0o600)
        self.install('--auth-config', str(auth))
        self.update()
        self.assertEqual(auth.stat().st_mode & 0o777, 0o600)
