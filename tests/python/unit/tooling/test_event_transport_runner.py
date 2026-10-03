"""Test integration-fixture failure handling without Docker or network access."""

from unittest.mock import Mock, patch
import io
import subprocess
import sys
import traceback
import unittest

from integration import event_transports as runner


class FixtureFailureTests(unittest.TestCase):
    def test_command_timeouts_do_not_expose_credentials(self):
        arguments = ("docker", "exec", "fixture", "--password", "fixture-secret")
        expired = subprocess.TimeoutExpired(arguments, 5)
        with patch.object(runner.subprocess, "run", side_effect=expired):
            try:
                runner.command(*arguments, timeout=5)
            except RuntimeError as error:
                self.assertEqual(str(error), "docker exec timed out")
                self.assertNotIn("fixture-secret", "".join(traceback.format_exception(error)))
            else:
                self.fail("A command timeout must fail the fixture operation")

    def test_readiness_retries_a_transient_probe_timeout(self):
        probe = Mock(side_effect=[subprocess.TimeoutExpired(["probe"], 5), None])
        with patch.object(runner.time, "sleep"):
            runner.wait_until_ready(probe, "fixture")
        self.assertEqual(probe.call_count, 2)

    def test_readiness_timeouts_remain_bounded_by_the_deadline(self):
        probe = Mock(side_effect=subprocess.TimeoutExpired(["probe"], 5))
        with patch.object(runner.time, "monotonic", side_effect=[0, 0, 121]), \
                patch.object(runner.time, "sleep"):
            with self.assertRaisesRegex(RuntimeError, "fixture.*120 seconds"):
                runner.wait_until_ready(probe, "fixture")

    def test_cleanup_attempts_every_resource_after_a_timeout(self):
        def fixtures(_root, containers, _servers, networks):
            containers.extend(["first", "second"])
            networks.append("network")
            return 0

        success = subprocess.CompletedProcess([], 0)
        expired = subprocess.TimeoutExpired(["docker", "fixture-secret"], 30)
        errors = io.StringIO()
        with patch.object(runner.sys, "platform", "linux"), \
                patch.object(runner, "run", side_effect=fixtures), \
                patch.object(runner.subprocess, "run",
                             side_effect=[success, success, expired, success, success]) as run, \
                patch.object(sys, "stderr", errors):
            self.assertEqual(runner.main(), 1)
        self.assertEqual([call.args[0][-1] for call in run.call_args_list[2:]],
                         ["second", "first", "network"])
        self.assertNotIn("fixture-secret", errors.getvalue())

    def test_unsupported_platform_fails_before_building_or_provisioning(self):
        for platform in ("darwin", "win32"):
            with self.subTest(platform=platform), \
                    patch.object(runner.sys, "platform", platform), \
                    patch.object(runner.subprocess, "run") as command:
                with self.assertRaisesRegex(RuntimeError, "require Linux.*SSL_CERT_FILE"):
                    runner.main([])
                command.assert_not_called()
