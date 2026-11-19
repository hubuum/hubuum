"""OAuth deployment fixture setup and secret handling regressions."""
import shutil
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import Mock, call, patch

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

    def test_github_polling_waits_for_initial_interval_and_keeps_backoff(self):
        fixture = Mock(spec=oauth.Fixture)
        fixture.github_client = "public-client-id"
        fixture.origin = "https://fixture.example"
        fixture.browser = Mock()
        fixture.browser.request.side_effect = [
            (429, {"Retry-After": "17"}, ""),
            (200, {}, '{"state":"pending"}'),
            (200, {}, '{"state":"ready"}'),
        ]
        interaction = {"id": "id", "interval_seconds": 7, "redemption_secret": "secret",
                       "next": {"verification_uri": fixture.origin, "user_code": "code"}}
        timeline = Mock()
        timeline.attach_mock(fixture.browser.request, "poll")
        with patch.object(oauth.time, "sleep", timeline.sleep), redirect_stdout(StringIO()):
            oauth.Fixture.authorize(fixture, interaction)
        self.assertEqual([event[0] for event in timeline.mock_calls],
                         ["sleep", "poll", "sleep", "poll", "sleep", "poll"])
        self.assertEqual(timeline.sleep.call_args_list, [call(7), call(17), call(17)])

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
