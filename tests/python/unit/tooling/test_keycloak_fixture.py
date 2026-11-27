"""Fixture transport, form handling and secret-redaction regressions."""
import base64
import subprocess
import traceback
import unittest
from unittest.mock import Mock, patch

from integration import keycloak


class FixtureTests(unittest.TestCase):
    def test_totp_matches_rfc_6238_sha1_vector(self):
        secret = base64.b32encode(b"12345678901234567890").decode()
        self.assertEqual(keycloak.totp(secret, at=59), "287082")

    def test_form_action_and_csrf_decode_html_entities(self):
        form = keycloak.Forms('<form action="/login?a=1&amp;b=2"><input name="csrf" value="a&amp;b"></form>').forms[0]
        self.assertEqual(form, {"action": "/login?a=1&b=2", "fields": {"csrf": "a&b"}})

    def test_inputs_after_form_do_not_enter_credentials(self):
        forms = keycloak.Forms('<form><input name="csrf" value="safe"></form><input name="password" value="outside">').forms
        self.assertEqual(forms[0]["fields"], {"csrf": "safe"})

    def test_failed_docker_command_does_not_expose_environment(self):
        result = Mock(returncode=1, stdout="secret", stderr="secret")
        with patch.object(keycloak.subprocess, "run", return_value=result):
            with self.assertRaisesRegex(RuntimeError, "docker failed") as error:
                keycloak.command("docker", "secret")
        self.assertNotIn("secret", str(error.exception))

    def test_command_deadline_hides_exception_arguments(self):
        arguments = ["docker", "fixture-secret"]
        expired = subprocess.TimeoutExpired(arguments, 600)
        with patch.object(keycloak.subprocess, "run", side_effect=expired):
            try:
                keycloak.command(*arguments)
            except RuntimeError as error:
                self.assertNotIn("fixture-secret", "".join(traceback.format_exception(error)))
            else:
                self.fail("Timeout must fail the command")
