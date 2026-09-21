#!/usr/bin/env python3
"""Test integration-fixture failure handling without Docker or network access."""

import sys

if sys.version_info < (3, 11):
    sys.exit(
        "Hubuum tooling requires Python 3.11 or newer; found "
        + sys.version.split()[0]
        + ". Install Python 3.11+ and ensure python3 on PATH selects it."
    )

import importlib.util
import io
from pathlib import Path
import subprocess
import unittest
from unittest.mock import Mock, patch

spec = importlib.util.spec_from_file_location(
    "transport_runner", Path(__file__).with_name("test-event-transports.py")
)
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class FixtureFailureTests(unittest.TestCase):
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
        with patch.object(runner, "run", side_effect=fixtures), \
                patch.object(runner.subprocess, "run",
                             side_effect=[success, success, expired, success, success]) as run, \
                patch.object(sys, "stderr", errors):
            self.assertEqual(runner.main(), 1)
        self.assertEqual([call.args[0][-1] for call in run.call_args_list[2:]],
                         ["second", "first", "network"])
        self.assertNotIn("fixture-secret", errors.getvalue())


if __name__ == "__main__":
    unittest.main()
