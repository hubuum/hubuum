"""OAuth deployment fixture setup and secret handling regressions."""
import shutil
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from integration import oauth


class OAuthFixtureTests(unittest.TestCase):
    def test_integration_runner_can_pass_arguments(self):
        with redirect_stdout(StringIO()), self.assertRaises(SystemExit) as error:
            oauth.main(["--help"])
        self.assertEqual(error.exception.code, 0)

    def test_github_secret_is_loaded_from_file_without_evidence_copy(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            secret = root / "client-secret"
            secret.write_text("fixture-secret\n")
            fixture = oauth.Fixture("server:test", root / "evidence", 48479, 48480, "public-client-id", secret)
            try:
                self.assertEqual(fixture.client_secret, "fixture-secret")
                self.assertEqual(list(fixture.evidence.iterdir()), [])
            finally:
                with patch.object(oauth.BaseFixture, "cleanup"):
                    fixture.cleanup()
                shutil.rmtree(fixture.root)

    def test_synthetic_setup_does_not_need_a_personal_secret(self):
        with tempfile.TemporaryDirectory() as root:
            fixture = oauth.Fixture("server:test", Path(root), 48479, 48480)
            try:
                self.assertIsNone(fixture.github_client)
                self.assertTrue(fixture.client_secret)
            finally:
                with patch.object(oauth.BaseFixture, "cleanup"):
                    fixture.cleanup()
                shutil.rmtree(fixture.root)
